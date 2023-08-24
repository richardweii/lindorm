#include "memtable.h"
#include "common.h"
#include "io/file.h"
#include "struct/ColumnValue.h"
#include "struct/Vin.h"
#include "util/logging.h"
#include "util/stat.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace LindormContest {

void MemTable::Init() {
  columnArrs_ = new ColumnArrWrapper *[columnsNum_];
  for (int i = 0; i < columnsNum_; i++) {
    switch (engine->columnsType[i]) {
      case COLUMN_TYPE_STRING: columnArrs_[i] = new StringArrWrapper(i); break;
      case COLUMN_TYPE_INTEGER: columnArrs_[i] = new IntArrWrapper(i); break;
      case COLUMN_TYPE_DOUBLE_FLOAT: columnArrs_[i] = new DoubleArrWrapper(i); break;
      case COLUMN_TYPE_UNINITIALIZED: break;
    }
  }
  // vid col
  vid_col = new VidArrWrapper(columnsNum_);
  // ts col
  ts_col = new TsArrWrapper(columnsNum_ + 1);

  for (int i = 0; i < kVinNumPerShard; i++) {
    min_ts_[i] = INT64_MAX;
    max_ts_[i] = INT64_MIN;
    latest_ts_cache[i] = -1;
    mem_latest_row_idx[i] = -1;
    mem_latest_row_ts[i] = -1;
  }
}

void MemTable::Add(const Row &row, uint16_t vid) {
  int idx = vid2idx(vid);

  if (latest_ts_cache[idx] < row.timestamp) {
    latest_ts_cache[idx] = row.timestamp;
    latest_row_cache[idx] = row;
    // mem_latest_row_ts[idx] = row.timestamp;
    // mem_latest_row_idx[idx] = cnt_;
  }

  for (int i = 0; i < columnsNum_; i++) {
    auto iter = row.columns.find(engine->columnsName[i]);
    LOG_ASSERT(iter != row.columns.end(), "iter == end");
    const auto &col = iter->second;
    columnArrs_[i]->Add(col, cnt_);
  }

  // 写入vid列
  ColumnValue vidVal(vid);
  vid_col->Add(vidVal, cnt_);

  // 写入ts列
  ColumnValue tsVal(row.timestamp * 1.0);
  ts_col->Add(tsVal, cnt_);

  updateTs(row.timestamp, vid);

  cnt_++;
  if (cnt_ >= kMemtableRowNum) {
    Flush();
  }
}

void MemTable::Flush() {
  if (cnt_ == 0)
    return;

  // for (int i = 0; i < kVinNumPerShard; i++) {
  //   if (mem_latest_row_ts[i] > latest_ts_cache[i]) {
  //     latest_ts_cache[i] = mem_latest_row_ts[i];
  //     int idx = mem_latest_row_idx[i];
  //     auto &row = latest_row_cache[i];
  //     memcpy(row.vin.vin, engine->vid2vin[idx2vid(shard_id_, i)].c_str(), VIN_LENGTH);
  //     for (int j = 0; j < columnsNum_; j++) {
  //       row.columns.insert(std::make_pair(engine->columnsName[i], columnArrs_[engine->col2colid[engine->columnsName[i]]]->Get(idx)));
  //     }
  //   }
  // }

  BlockMeta *meta = block_manager_->NewVinBlockMeta(shard_id_, cnt_, min_ts_, max_ts_);
  for (int i = 0; i < columnsNum_; i++) {
    // filename = vin + colname
    std::string file_name = ColFileName(engine->dataDirPath, engine->table_name_, shard_id_, engine->columnsName[i]);
    File *file = file_manager_->Open(file_name);

    columnArrs_[i]->Flush(file, cnt_, meta);
  }

  {
    std::string file_name = ColFileName(engine->dataDirPath, engine->table_name_, shard_id_, kVidColName);
    File *file = file_manager_->Open(file_name);
    vid_col->Flush(file, cnt_, meta);
  }

  {
    std::string file_name = ColFileName(engine->dataDirPath, engine->table_name_, shard_id_, kTsColName);
    File *file = file_manager_->Open(file_name);
    ts_col->Flush(file, cnt_, meta);
  }

  Reset();
}

void MemTable::GetRowsFromTimeRange(uint64_t vid, int64_t lowerInclusive, int64_t upperExclusive, const std::set<std::string> &requestedColumns, std::vector<Row> &results) {
  // 首先看memtable当中是否有符合要求的数据
  int idx = vid2idx(vid);
  if (!(min_ts_[idx] >= upperExclusive || max_ts_[idx] < lowerInclusive)) {
    // 有重合，需要遍历memtable，首先找到vin + ts符合要求的行索引
    std::vector<int> idxs;
    for (int i = 0; i < cnt_; i++) {
      if (vid_col->GetVal(i) == (int64_t) vid && inRange(ts_col->GetVal(i), lowerInclusive, upperExclusive)) {
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
        for (const auto &requestedColumn : requestedColumns) {
          ColumnValue col;
          resultRow.columns.insert(std::make_pair(requestedColumn, columnArrs_[engine->col2colid[requestedColumn]]->Get(idx)));
        }
        results.push_back(std::move(resultRow));
      }
    }
  }

  // 然后读落盘的block
  std::vector<BlockMeta *> blk_metas;
  block_manager_->GetVinBlockMetasByTimeRange(vid, lowerInclusive, upperExclusive, blk_metas);
  if (!blk_metas.empty()) {
    RECORD_FETCH_ADD(tr_disk_blk_query_cnt, blk_metas.size());
    for (auto blk_meta : blk_metas) {
      // 去读对应列的block
      ColumnArrWrapper *cols[requestedColumns.size()];
      VidArrWrapper *tmp_vid_col = new VidArrWrapper(columnsNum_);
      TsArrWrapper *tmp_ts_col = new TsArrWrapper(columnsNum_ + 1);
      int icol_idx = 0;
      for (const auto &requestedColumn : requestedColumns) {
        auto idx = engine->col2colid[requestedColumn];
        switch (engine->columnsType[idx]) {
          case COLUMN_TYPE_STRING: cols[icol_idx] = new StringArrWrapper(idx); break;
          case COLUMN_TYPE_INTEGER: cols[icol_idx] = new IntArrWrapper(idx); break;
          case COLUMN_TYPE_DOUBLE_FLOAT: cols[icol_idx] = new DoubleArrWrapper(idx); break;
          case COLUMN_TYPE_UNINITIALIZED: LOG_ASSERT(false, "error"); break;
        }
        std::string file_name = ColFileName(engine->dataDirPath, engine->table_name_, shard_id_, engine->columnsName[idx]);
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

      std::vector<int> idxs;
      for (int i = 0; i < blk_meta->num; i++) {
        if (tmp_vid_col->GetVal(i) == (int64_t) vid && inRange(tmp_ts_col->GetVal(i), lowerInclusive, upperExclusive)) {
          idxs.emplace_back(i);
        }
      }
      if (!idxs.empty()) {
        for (auto idx : idxs) {
          // build res row
          Row resultRow;
          resultRow.timestamp = tmp_ts_col->GetVal(idx);
          memcpy(resultRow.vin.vin, engine->vid2vin[vid].c_str(), VIN_LENGTH);
          for (size_t k = 0; k < requestedColumns.size(); k++) {
            resultRow.columns.insert(std::make_pair(engine->columnsName[cols[k]->GetColid()], cols[k]->Get(idx)));
          }
          results.push_back(std::move(resultRow));
        }
      }

      for (size_t k = 0; k < requestedColumns.size(); k++) {
        delete cols[k];
      }
      delete tmp_vid_col;
      delete tmp_ts_col;
    }
  }
}

void MemTable::SaveLatestRowCache(File *file) {
  // vin ts columns
  // columns就按照colid顺序存，除了string先存一个4字节的长度，再存字符串之后，其他字段都是固定长度，直接根据类型进行解析即可
  for (int i = 0; i < kVinNumPerShard; i++) {
    // 需要存kVinNumPerShard个row
    if (latest_ts_cache[i] == -1) {
      continue;
    }
    auto &row = latest_row_cache[i];
    file->write((char *) &i, sizeof(i));
    file->write(row.vin.vin, VIN_LENGTH);
    file->write((char *) &row.timestamp, sizeof(row.timestamp));
    for (int k = 0; k < columnsNum_; k++) {
      auto pair = row.columns.find(engine->columnsName[k]);
      LOG_ASSERT(pair != row.columns.end(), "error");
      auto col = pair->second;
      switch (engine->columnsType[k]) {
        case COLUMN_TYPE_STRING: {
          std::pair<int32_t, const char *> lengthStrPair;
          col.getStringValue(lengthStrPair);
          file->write((char *) &lengthStrPair.first, sizeof(lengthStrPair.first));
          file->write(lengthStrPair.second, lengthStrPair.first);
        } break;
        case COLUMN_TYPE_INTEGER: {
          int num;
          col.getIntegerValue(num);
          file->write((char *) &num, sizeof(num));
        } break;
        case COLUMN_TYPE_DOUBLE_FLOAT: {
          double num;
          col.getDoubleFloatValue(num);
          file->write((char *) &num, sizeof(num));
        } break;
        case COLUMN_TYPE_UNINITIALIZED: LOG_ASSERT(false, "error"); break;
      }
    }
  }
}

void MemTable::LoadLatestRowCache(File *file) {
  for (int i = 0; i < kVinNumPerShard; i++) {
    if (file->read((char *) &i, sizeof(i)) == Status::END)
      break;
    // 需要存kVinNumPerShard个row
    auto &row = latest_row_cache[i];
    file->read(row.vin.vin, VIN_LENGTH);
    file->read((char *) &row.timestamp, sizeof(row.timestamp));
    latest_ts_cache[i] = row.timestamp;
    for (int k = 0; k < columnsNum_; k++) {
      switch (engine->columnsType[k]) {
        case COLUMN_TYPE_STRING: {
          int32_t len;
          file->read((char *) &len, sizeof(len));
          char buf[len];
          file->read(buf, len);
          ColumnValue col(buf, len);
          row.columns.emplace(std::make_pair(engine->columnsName[k], col));
        } break;
        case COLUMN_TYPE_INTEGER: {
          int num;
          file->read((char *) &num, sizeof(num));
          ColumnValue col(num);
          row.columns.emplace(std::make_pair(engine->columnsName[k], col));
        } break;
        case COLUMN_TYPE_DOUBLE_FLOAT: {
          double num;
          file->read((char *) &num, sizeof(num));
          ColumnValue col(num);
          row.columns.emplace(std::make_pair(engine->columnsName[k], col));
        } break;
        case COLUMN_TYPE_UNINITIALIZED: LOG_ASSERT(false, "error"); break;
      }
    }
  }
}

} // namespace LindormContest
