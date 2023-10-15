#include "memtable.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "InternalColumnArr.h"
#include "common.h"
#include "filename.h"
#include "io/file.h"
#include "struct/ColumnValue.h"
#include "struct/Vin.h"
#include "util/logging.h"
#include "util/stat.h"
#include "util/util.h"

namespace LindormContest {

void MemTable::Init() {
  columnArrs_ = new ColumnArrWrapper*[columnsNum_];
  for (int i = 0; i < columnsNum_; i++) {
    switch (engine_->columns_type_[i]) {
      case COLUMN_TYPE_STRING:
        columnArrs_[i] = new StringArrWrapper(i);
        break;
      case COLUMN_TYPE_INTEGER:
        columnArrs_[i] = new IntArrWrapper(i);
        break;
      case COLUMN_TYPE_DOUBLE_FLOAT:
        columnArrs_[i] = new DoubleArrWrapper(i);
        break;
      case COLUMN_TYPE_UNINITIALIZED:
        break;
    }
  }
  // vid col
  vid_col_ = new VidArrWrapper(columnsNum_);
  // ts col
  ts_col_ = new TsArrWrapper(columnsNum_ + 1);
  // idx col
  idx_col_ = new IdxArrWrapper(columnsNum_ + 2);

  for (int i = 0; i < kVinNumPerShard; i++) {
    min_ts_[i] = INT64_MAX;
    max_ts_[i] = INT64_MIN;
    mem_latest_row_idx_[i] = -1;
    mem_latest_row_ts_[i] = -1;
  }

  std::string file_name = ShardDataFileName(engine_->dataDirPath, engine_->table_name_, shard_id_);
  File* file = io_manager_->Open(file_name, LIBAIO_FLAG);
  // File* file = io_manager_->Open(file_name, NORMAL_FLAG);
  buffer_ = new AlignedWriteBuffer(file);
}

void MemTable::Add(const Row& row, uint16_t vid) {
  int idx = vid2idx(vid);

  if (mem_latest_row_ts_[idx] < row.timestamp && latest_ts_cache_[idx] < row.timestamp) {
    mem_latest_row_ts_[idx] = row.timestamp;
    mem_latest_row_idx_[idx] = cnt_;
  }

  for (int i = 0; i < columnsNum_; i++) {
    auto iter = row.columns.find(engine_->columns_name_[i]);
    LOG_ASSERT(iter != row.columns.end(), "iter == end");
    const auto& col = iter->second;
    columnArrs_[i]->Add(col, cnt_);
  }

  // TODO: 不用ColumnValue
  // 写入vid列
  ColumnValue vidVal(vid);  // TODO: 这里只需要写入idx
  vid_col_->Add(vidVal, cnt_);

  // 写入ts列
  ColumnValue tsVal(row.timestamp * 1.0);
  ts_col_->Add(tsVal, cnt_);

  // 写入idx列
  ColumnValue idxVal(cnt_);
  idx_col_->Add(idxVal, cnt_);

  updateTs(row.timestamp, vid);

  cnt_++;
  if (cnt_ >= kMemtableRowNum) {
    Flush();
  }
}

void MemTable::sort() {
  // 先对 vid + ts idx这三列进行排序
  uint16_t* vid_datas = vid_col_->GetDataArr();
  int64_t* ts_datas = ts_col_->GetDataArr();
  uint16_t* idx_datas = idx_col_->GetDataArr();

  quickSort(vid_datas, ts_datas, idx_datas, 0, cnt_ - 1);
}

void MemTable::Flush(bool shutdown) {
  if (cnt_ == 0) return;

  // 更新缓存的latest row
  for (int i = 0; i < kVinNumPerShard; i++) {
    if (mem_latest_row_ts_[i] > latest_ts_cache_[i]) {
      latest_ts_cache_[i] = mem_latest_row_ts_[i];
      int idx = mem_latest_row_idx_[i];
      for (int j = 0; j < columnsNum_; j++) {
        columnArrs_[engine_->column_idx_[engine_->columns_name_[j]]]->Get(idx, latest_ts_cols_[i][j]);
      }
    }
    mem_latest_row_ts_[i] = -1;
    mem_latest_row_idx_[i] = -1;
  }

  BlockMeta* meta = block_manager_->NewVinBlockMeta(shard_id_, cnt_, min_ts_, max_ts_);

  // 先对 vid + ts idx这三列进行排序
  sort();

  for (int i = 0; i < columnsNum_; i++) {
    columnArrs_[i]->Flush(buffer_, cnt_, meta);
  }
  vid_col_->Flush(buffer_, cnt_, meta);
  ts_col_->Flush(buffer_, cnt_, meta);
  idx_col_->Flush(buffer_, cnt_, meta);

  if (shutdown) {
    buffer_->flush();
  }

  Reset();
}

/* 
TODO: 现在的time range是通过将当时写入的整个列（包括了总共391个vin）的压缩数据全部读上来，读到临时数组ColumnArrWrapper里面，然后再对
索引列进行二分查找。
1）读放大非常大，几乎是多读了391倍的数据上来
2）临时内存的开销不可控

*/
void MemTable::GetRowsFromTimeRange(uint64_t vid, int64_t lowerInclusive, int64_t upperExclusive,
                                    std::vector<int> colids, std::vector<Row>& results) {
  // 首先看memtable当中是否有符合要求的数据
  int idx = vid2idx(vid);
  if (!(min_ts_[idx] >= upperExclusive || max_ts_[idx] < lowerInclusive)) {
    // 有重合，需要遍历memtable，首先找到vin + ts符合要求的行索引
    std::vector<int> idxs;
    for (int i = 0; i < cnt_; i++) {
      if (vid_col_->GetVal(i) == (int64_t)vid && inRange(ts_col_->GetVal(i), lowerInclusive, upperExclusive)) {
        idxs.emplace_back(i);
      }
    }
    if (!idxs.empty()) {
      RECORD_FETCH_ADD(tr_memtable_blk_query_cnt, idxs.size());
      for (auto idx : idxs) {
        // build res row
        Row resultRow;
        resultRow.timestamp = ts_col_->GetVal(idx);
        memcpy(resultRow.vin.vin, engine_->vid2vin_[vid].c_str(), VIN_LENGTH);
        for (const auto col_id : colids) {
          ColumnValue col;
          columnArrs_[col_id]->Get(idx, col);
          resultRow.columns.insert(std::make_pair(engine_->columns_name_[col_id], std::move(col)));
        }
        results.push_back(std::move(resultRow));
      }
    }
  }

  // 然后读落盘的block
  std::vector<BlockMeta*> blk_metas;
  block_manager_->GetVinBlockMetasByTimeRange(vid, lowerInclusive, upperExclusive, blk_metas);
  std::string file_name = ShardDataFileName(engine_->dataDirPath, engine_->table_name_, shard_id_);
  RandomAccessFile file(file_name);

  if (!blk_metas.empty()) {
    RECORD_FETCH_ADD(tr_disk_blk_query_cnt, blk_metas.size());
    for (auto blk_meta : blk_metas) {
      // 去读对应列的block
      // TODO:这里临时内存的开销太大了
      ColumnArrWrapper* cols[colids.size()];
      VidArrWrapper* tmp_vid_col = new VidArrWrapper(columnsNum_);
      TsArrWrapper* tmp_ts_col = new TsArrWrapper(columnsNum_ + 1);
      IdxArrWrapper* tmp_idx_col = new IdxArrWrapper(columnsNum_ + 2);

      int icol_idx = 0;
      for (const auto col_id : colids) {
        switch (engine_->columns_type_[col_id]) {
          case COLUMN_TYPE_STRING:
            cols[icol_idx] = new StringArrWrapper(col_id);
            break;
          case COLUMN_TYPE_INTEGER:
            cols[icol_idx] = new IntArrWrapper(col_id);
            break;
          case COLUMN_TYPE_DOUBLE_FLOAT:
            cols[icol_idx] = new DoubleArrWrapper(col_id);
            break;
          case COLUMN_TYPE_UNINITIALIZED:
            LOG_ASSERT(false, "error");
            break;
        }
        cols[icol_idx++]->Read(&file, buffer_, blk_meta);
      }
      tmp_vid_col->Read(&file, buffer_, blk_meta);
      tmp_ts_col->Read(&file, buffer_, blk_meta);
      tmp_idx_col->Read(&file, buffer_, blk_meta);

      // 现在vid + ts是有序的，可以直接应用二分查找
      std::vector<uint16_t> idxs;
      std::vector<int64_t> tss;
      findMatchingIndices(tmp_vid_col->GetDataArr(), tmp_ts_col->GetDataArr(), tmp_idx_col->GetDataArr(), blk_meta->num,
                          vid, lowerInclusive, upperExclusive, idxs, tss);

      if (!idxs.empty()) {
        for (int i = 0; i < (int)idxs.size(); i++) {
          // build res row
          Row resultRow;
          resultRow.timestamp = tss[i];
          memcpy(resultRow.vin.vin, engine_->vid2vin_[vid].c_str(), VIN_LENGTH);
          for (size_t k = 0; k < colids.size(); k++) {
            ColumnValue col;
            cols[k]->Get(idxs[i], col);
            resultRow.columns.insert(std::make_pair(engine_->columns_name_[cols[k]->GetColid()], std::move(col)));
          }
          results.push_back(std::move(resultRow));
        }
      }

      for (size_t k = 0; k < colids.size(); k++) {
        delete cols[k];
      }

      delete tmp_vid_col;
      delete tmp_ts_col;
      delete tmp_idx_col;
    }
  }
}

void MemTable::SaveLatestRowCache(File* file) {
  // vin ts columns
  // columns就按照colid顺序存，除了string先存一个4字节的长度，再存字符串之后，其他字段都是固定长度，直接根据类型进行解析即可
  for (int i = 0; i < kVinNumPerShard; i++) {
    // 需要存kVinNumPerShard个row
    if (latest_ts_cache_[i] == -1) {
      continue;
    }
    file->write((char*)&i, sizeof(i));
    file->write((char*)(&latest_ts_cache_[i]), sizeof(latest_ts_cache_[i]));
    for (int k = 0; k < columnsNum_; k++) {
      auto& col = latest_ts_cols_[i][k];
      switch (engine_->columns_type_[k]) {
        case COLUMN_TYPE_STRING: {
          std::pair<int32_t, const char*> lengthStrPair;
          col.getStringValue(lengthStrPair);
          file->write((char*)&lengthStrPair.first, sizeof(lengthStrPair.first));
          file->write(lengthStrPair.second, lengthStrPair.first);
        } break;
        case COLUMN_TYPE_INTEGER: {
          int num;
          col.getIntegerValue(num);
          file->write((char*)&num, sizeof(num));
        } break;
        case COLUMN_TYPE_DOUBLE_FLOAT: {
          double num;
          col.getDoubleFloatValue(num);
          file->write((char*)&num, sizeof(num));
        } break;
        case COLUMN_TYPE_UNINITIALIZED:
          LOG_ASSERT(false, "error");
          break;
      }
    }
  }
}

void MemTable::LoadLatestRowCache(File* file) {
  for (int i = 0; i < kVinNumPerShard; i++) {
    if (file->read((char*)&i, sizeof(i)) == Status::END) break;
    // 需要存kVinNumPerShard个row
    file->read((char*)(&latest_ts_cache_[i]), sizeof(latest_ts_cache_[i]));
    for (int k = 0; k < columnsNum_; k++) {
      auto& col = latest_ts_cols_[i][k];
      switch (engine_->columns_type_[k]) {
        case COLUMN_TYPE_STRING: {
          int32_t len;
          file->read((char*)&len, sizeof(len));
          char buf[len];
          file->read(buf, len);
          col = std::move(ColumnValue(buf, len));
        } break;
        case COLUMN_TYPE_INTEGER: {
          int num;
          file->read((char*)&num, sizeof(num));
          col = std::move(ColumnValue(num));
        } break;
        case COLUMN_TYPE_DOUBLE_FLOAT: {
          double num;
          file->read((char*)&num, sizeof(num));
          col = std::move(ColumnValue(num));
        } break;
        case COLUMN_TYPE_UNINITIALIZED:
          LOG_ASSERT(false, "error");
          break;
      }
    }
  }
}

} // namespace LindormContest
