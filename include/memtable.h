#pragma once

#include <cstdint>
#include <cstring>
#include <utility>

#include "InternalColumnArr.h"
#include "ShardBlockMetaManager.h"
#include "TSDBEngine.hpp"
#include "TSDBEngineImpl.h"
#include "common.h"
#include "filename.h"
#include "io/file.h"
#include "io/file_manager.h"
#include "status.h"
#include "struct/ColumnValue.h"
#include "struct/Row.h"
#include "struct/Vin.h"
#include "util/aligned_buffer.h"
#include "util/defer.h"
#include "util/logging.h"
#include "util/rwlock.h"

namespace LindormContest {

class MemTable {
public:
  MemTable(int shard_id, TSDBEngineImpl* engine)
      : engine(engine), shard_id_(shard_id), columnsNum_(kColumnNum), cnt_(0), file_manager_(engine->file_manager_) {
    block_manager_ = new ShardBlockMetaManager(columnsNum_ + kExtraColNum);
    for (int i = 0; i < kVinNumPerShard; i++) {
      latest_ts_cache[i] = -1;
    }
  }

  virtual ~MemTable() {
    for (int i = 0; i < columnsNum_; i++) {
      LOG_ASSERT(columnArrs_[i] != nullptr, "nullptr");
      delete columnArrs_[i];
    }

    delete buffer;
    delete[] columnArrs_;
    delete vid_col;
    delete ts_col;
  }

  void Init();

  /**
   * 写入一行数据到memtable，会将其所有列分别加到对应列的数组当中
   */
  void Add(const Row& row, uint16_t vid);

  void GetLatestRow(uint16_t vid, Row& row, std::vector<int>& colids) {
    int idx = vid2idx(vid);

    if (mem_latest_row_ts[idx] > latest_ts_cache[idx]) {
      // 说明最新的row在内存当中
      int mem_lat_idx = mem_latest_row_idx[idx];
      row.timestamp = mem_latest_row_ts[idx];
      memcpy(row.vin.vin, engine->vid2vin[vid].c_str(), VIN_LENGTH);
      for (auto col_id : colids) {
        ColumnValue col_value;
        auto& col_name = engine->columnsName[col_id];
        columnArrs_[col_id]->Get(mem_lat_idx, col_value);
        row.columns.insert(std::make_pair(col_name, std::move(col_value)));
      }
    } else {
      row.timestamp = latest_ts_cache[idx];
      memcpy(row.vin.vin, engine->vid2vin[vid].c_str(), VIN_LENGTH);
      for (auto col_id : colids) {
        ColumnValue col_value;
        auto& col_name = engine->columnsName[col_id];
        row.columns.insert(std::make_pair(col_name, latest_ts_cols[idx][col_id]));
      }
    }

    LOG_ASSERT(row.timestamp != -1, "???");
  }

  void Flush(bool shutdown = false);

  void GetRowsFromTimeRange(uint64_t vid, int64_t lowerInclusive, int64_t upperExclusive, std::vector<int> colids,
                            std::vector<Row>& results);

  // 清空状态
  void Reset() {
    for (int i = 0; i < kVinNumPerShard; i++) {
      min_ts_[i] = INT64_MAX;
      max_ts_[i] = INT64_MIN;
      mem_latest_row_idx[i] = -1;
      mem_latest_row_ts[i] = -1;
    }
    cnt_ = 0;
    for (int i = 0; i < columnsNum_; i++) {
      columnArrs_[i]->Reset();
    }
    vid_col->Reset();
    ts_col->Reset();
  }

  void SaveBlockMeta(File* file) { block_manager_->Save(file, shard_id_); }

  void LoadBlockMeta(File* file) { block_manager_->Load(file, shard_id_); }

  void SaveLatestRowCache(File* file);

  void LoadLatestRowCache(File* file);

private:
  friend class ShardBlockMetaManager;
  void updateTs(const int64_t ts, int vid) {
    int idx = vid2idx(vid);
    if (ts < min_ts_[idx]) {
      min_ts_[idx] = ts;
    }

    if (ts > max_ts_[idx]) {
      max_ts_[idx] = ts;
    }
  }

  bool inRange(int64_t ts, int64_t lowerInclusive, int64_t upperExclusive) {
    return ts >= lowerInclusive && ts < upperExclusive;
  }

  void sort();

  TSDBEngineImpl* engine;
  int shard_id_;

  int columnsNum_; // How many columns is defined in schema for the sole table.
  // std::string *engine->columnsName; // The column's name for each column.
  ColumnArrWrapper** columnArrs_;
  VidArrWrapper* vid_col;
  TsArrWrapper* ts_col;
  IdxArrWrapper* idx_col;

  int cnt_; // 记录这个memtable写了多少行了，由于可能没有写满，然后shutdown刷下去了，所以需要记录一下
  int64_t min_ts_[kVinNumPerShard];
  int64_t max_ts_[kVinNumPerShard];
  FileManager* file_manager_;
  ShardBlockMetaManager* block_manager_;

  // LatestQueryCache
  // Row latest_row_cache[kVinNumPerShard];
  ColumnValue latest_ts_cols[kVinNumPerShard][kColumnNum];
  int64_t latest_ts_cache[kVinNumPerShard];

  // mem latest row idx and ts
  int64_t mem_latest_row_idx[kVinNumPerShard];
  int64_t mem_latest_row_ts[kVinNumPerShard];

  AlignedBuffer* buffer;
};

/**
 * 上层直接调用ShardMemtable的接口即可，封装了具体的分片逻辑
 */
class ShardMemtable {
public:
  ShardMemtable(TSDBEngineImpl* engine) {
    for (int i = 0; i < kShardNum; i++) {
      memtables[i] = new MemTable(i, engine);
    }
  }

  void Init() {
    LOG_INFO("Init ShardMemtable");
    for (int i = 0; i < kShardNum; i++) {
      memtables[i]->Init();
    }
    LOG_INFO("Init ShardMemtable finished");
  }

  void Add(const Row& row, uint16_t vid) {
    int shard_id = Shard(vid);

    rwlcks[shard_id].wlock();
    defer { rwlcks[shard_id].unlock(); };

    memtables[shard_id]->Add(row, vid);
  }

  void GetLatestRow(uint16_t vid, Row& row, std::vector<int>& colids) {
    int shard_id = Shard(vid);

    rwlcks[shard_id].rlock();
    defer { rwlcks[shard_id].unlock(); };

    memtables[shard_id]->GetLatestRow(vid, row, colids);
  }

  void GetRowsFromTimeRange(uint64_t vid, int64_t lowerInclusive, int64_t upperExclusive, std::vector<int> colids,
                            std::vector<Row>& results) {
    int shard_id = Shard(vid);

    rwlcks[shard_id].rlock();
    defer { rwlcks[shard_id].unlock(); };

    memtables[shard_id]->GetRowsFromTimeRange(vid, lowerInclusive, upperExclusive, colids, results);
  }

  void Flush(int shard_id) { memtables[shard_id]->Flush(true); }

  void SaveBlockMeta(int shard_id, File* file) { memtables[shard_id]->SaveBlockMeta(file); }

  void LoadBlockMeta(int shard_id, File* file) { memtables[shard_id]->LoadBlockMeta(file); }

  void SaveLatestRowCache(int shard_id, File* file) { memtables[shard_id]->SaveLatestRowCache(file); }

  void LoadLatestRowCache(int shard_id, File* file) { memtables[shard_id]->LoadLatestRowCache(file); }

private:
  MemTable* memtables[kShardNum];
  RWLock rwlcks[kShardNum];
};

} // namespace LindormContest
