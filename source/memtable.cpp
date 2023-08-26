#include "memtable.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "InternalColumnArr.h"
#include "common.h"
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
    switch (engine->columnsType[i]) {
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
  vid_col = new VidArrWrapper(columnsNum_);
  // ts col
  ts_col = new TsArrWrapper(columnsNum_ + 1);
  // idx col
  idx_col = new IdxArrWrapper(columnsNum_ + 2);

  for (int i = 0; i < kVinNumPerShard; i++) {
    min_ts_[i] = INT64_MAX;
    max_ts_[i] = INT64_MIN;
    mem_latest_row_idx[i] = -1;
    mem_latest_row_ts[i] = -1;
  }
}

void MemTable::Add(const Row& row, uint16_t vid) {
  int idx = vid2idx(vid);

  if (mem_latest_row_ts[idx] < row.timestamp && latest_ts_cache[idx] < row.timestamp) {
    mem_latest_row_ts[idx] = row.timestamp;
    mem_latest_row_idx[idx] = cnt_;
  }

  for (int i = 0; i < columnsNum_; i++) {
    auto iter = row.columns.find(engine->columnsName[i]);
    LOG_ASSERT(iter != row.columns.end(), "iter == end");
    const auto& col = iter->second;
    columnArrs_[i]->Add(col, cnt_);
  }

  // 写入vid列
  ColumnValue vidVal(vid);
  vid_col->Add(vidVal, cnt_);

  // 写入ts列
  ColumnValue tsVal(row.timestamp * 1.0);
  ts_col->Add(tsVal, cnt_);

  // 写入idx列
  ColumnValue idxVal(cnt_);
  idx_col->Add(idxVal, cnt_);

  updateTs(row.timestamp, vid);

  cnt_++;
  if (cnt_ >= kMemtableRowNum) {
    Flush();
  }
}

void MemTable::sort() {
  // 先对 vid + ts idx这三列进行排序
  uint16_t* vid_datas = vid_col->GetDataArr();
  int64_t* ts_datas = ts_col->GetDataArr();
  uint16_t* idx_datas = idx_col->GetDataArr();

  quickSort(vid_datas, ts_datas, idx_datas, 0, cnt_ - 1);
}

void MemTable::Flush() {
  if (cnt_ == 0) return;

  for (int i = 0; i < kVinNumPerShard; i++) {
    if (mem_latest_row_ts[i] > latest_ts_cache[i]) {
      latest_ts_cache[i] = mem_latest_row_ts[i];
      int idx = mem_latest_row_idx[i];
      for (int j = 0; j < columnsNum_; j++) {
        columnArrs_[engine->col2colid[engine->columnsName[j]]]->Get(idx, latest_ts_cols[i][j]);
      }
    }
    mem_latest_row_ts[i] = -1;
    mem_latest_row_idx[i] = -1;
  }

  BlockMeta* meta = block_manager_->NewVinBlockMeta(shard_id_, cnt_, min_ts_, max_ts_);
  for (int i = 0; i < columnsNum_; i++) {
    // filename = vin + colname
    std::string file_name = ColFileName(engine->dataDirPath, engine->table_name_, shard_id_, engine->columnsName[i]);
    File* file = file_manager_->Open(file_name);

    columnArrs_[i]->Flush(file, cnt_, meta);
  }

  // 先对 vid + ts idx这三列进行排序
  sort();

  {
    std::string file_name = ColFileName(engine->dataDirPath, engine->table_name_, shard_id_, kVidColName);
    File* file = file_manager_->Open(file_name);
    vid_col->Flush(file, cnt_, meta);
  }

  {
    std::string file_name = ColFileName(engine->dataDirPath, engine->table_name_, shard_id_, kTsColName);
    File* file = file_manager_->Open(file_name);
    ts_col->Flush(file, cnt_, meta);
  }

  {
    std::string file_name = ColFileName(engine->dataDirPath, engine->table_name_, shard_id_, kIdxColName);
    File* file = file_manager_->Open(file_name);
    idx_col->Flush(file, cnt_, meta);
  }

  Reset();
}

void MemTable::GetRowsFromTimeRange(uint64_t vid, int64_t lowerInclusive, int64_t upperExclusive,
                                    const std::set<std::string>& requestedColumns, std::vector<Row>& results) {
  // 首先看memtable当中是否有符合要求的数据
  int idx = vid2idx(vid);
  if (!(min_ts_[idx] >= upperExclusive || max_ts_[idx] < lowerInclusive)) {
    // 有重合，需要遍历memtable，首先找到vin + ts符合要求的行索引
    std::vector<int> idxs;
    for (int i = 0; i < cnt_; i++) {
      if (vid_col->GetVal(i) == (int64_t)vid && inRange(ts_col->GetVal(i), lowerInclusive, upperExclusive)) {
        idxs.emplace_back(i);
      }
    }
    if (!idxs.empty()) {
      RECORD_FETCH_ADD(tr_memtable_blk_query_cnt, idxs.size());
      for (auto idx : idxs) {
        // build res row
        Row resultRow;
        resultRow.timestamp = ts_col->GetVal(idx);
        memcpy(resultRow.vin.vin, engine->vid2vin[vid].c_str(), VIN_LENGTH);
        for (const auto& requestedColumn : requestedColumns) {
          ColumnValue col;
          columnArrs_[engine->col2colid[requestedColumn]]->Get(idx, col);
          resultRow.columns.insert(std::make_pair(requestedColumn, std::move(col)));
        }
        results.push_back(std::move(resultRow));
      }
    }
  }

  // 然后读落盘的block
  std::vector<BlockMeta*> blk_metas;
  if (interval_trees[idx] != nullptr) {
    Interval query = {lowerInclusive, upperExclusive};
    std::set<Interval> overlappingIntervals = interval_trees[idx]->searchOverlap(query);
    for (auto& interval : overlappingIntervals) {
      blk_metas.emplace_back(interval.meta);
    }
  } else {
    block_manager_->GetVinBlockMetasByTimeRange(vid, lowerInclusive, upperExclusive, blk_metas);
  }
  if (!blk_metas.empty()) {
    RECORD_FETCH_ADD(tr_disk_blk_query_cnt, blk_metas.size());
    for (auto blk_meta : blk_metas) {
      // 去读对应列的block
      ColumnArrWrapper* cols[requestedColumns.size()];
      VidArrWrapper* tmp_vid_col = new VidArrWrapper(columnsNum_);
      TsArrWrapper* tmp_ts_col = new TsArrWrapper(columnsNum_ + 1);
      IdxArrWrapper* tmp_idx_col = new IdxArrWrapper(columnsNum_ + 2);

      int icol_idx = 0;
      for (const auto& requestedColumn : requestedColumns) {
        auto idx = engine->col2colid[requestedColumn];
        switch (engine->columnsType[idx]) {
          case COLUMN_TYPE_STRING:
            cols[icol_idx] = new StringArrWrapper(idx);
            break;
          case COLUMN_TYPE_INTEGER:
            cols[icol_idx] = new IntArrWrapper(idx);
            break;
          case COLUMN_TYPE_DOUBLE_FLOAT:
            cols[icol_idx] = new DoubleArrWrapper(idx);
            break;
          case COLUMN_TYPE_UNINITIALIZED:
            LOG_ASSERT(false, "error");
            break;
        }
        std::string file_name =
          ColFileName(engine->dataDirPath, engine->table_name_, shard_id_, engine->columnsName[idx]);
        RandomAccessFile file(file_name);
        cols[icol_idx++]->Read(&file, blk_meta);
      }

      {
        std::string file_name = ColFileName(engine->dataDirPath, engine->table_name_, shard_id_, kVidColName);
        RandomAccessFile file(file_name);
        tmp_vid_col->Read(&file, blk_meta);
      }

      {
        std::string file_name = ColFileName(engine->dataDirPath, engine->table_name_, shard_id_, kTsColName);
        RandomAccessFile file(file_name);
        tmp_ts_col->Read(&file, blk_meta);
      }

      {
        std::string file_name = ColFileName(engine->dataDirPath, engine->table_name_, shard_id_, kIdxColName);
        RandomAccessFile file(file_name);
        tmp_idx_col->Read(&file, blk_meta);
      }

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
          memcpy(resultRow.vin.vin, engine->vid2vin[vid].c_str(), VIN_LENGTH);
          for (size_t k = 0; k < requestedColumns.size(); k++) {
            ColumnValue col;
            cols[k]->Get(idxs[i], col);
            resultRow.columns.insert(std::make_pair(engine->columnsName[cols[k]->GetColid()], std::move(col)));
          }
          results.push_back(std::move(resultRow));
        }
      }

      for (size_t k = 0; k < requestedColumns.size(); k++) {
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
    if (latest_ts_cache[i] == -1) {
      continue;
    }
    file->write((char*)&i, sizeof(i));
    file->write((char*)(&latest_ts_cache[i]), sizeof(latest_ts_cache[i]));
    for (int k = 0; k < columnsNum_; k++) {
      auto& col = latest_ts_cols[i][k];
      switch (engine->columnsType[k]) {
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
    file->read((char*)(&latest_ts_cache[i]), sizeof(latest_ts_cache[i]));
    for (int k = 0; k < columnsNum_; k++) {
      auto& col = latest_ts_cols[i][k];
      switch (engine->columnsType[k]) {
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
