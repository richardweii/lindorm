#pragma once

#include <cstdint>
#include <cstring>
#include <utility>

#include "InternalColumnArr.h"
#include "TSDBEngineImpl.h"
#include "common.h"
#include "filename.h"
#include "io/aligned_buffer.h"
#include "io/file.h"
#include "status.h"
#include "struct/ColumnValue.h"
#include "struct/Row.h"
#include "struct/Vin.h"
#include "util/logging.h"
#include "util/rwlock.h"

namespace LindormContest {

class MemTable {
  friend class ShardImpl;

public:
  MemTable(int shard_id, TSDBEngineImpl* engine) : engine_(engine), shard_id_(shard_id), cnt_(0) {}

  virtual ~MemTable() {
    for (int i = 0; i < kColumnNum; i++) {
      LOG_ASSERT(columnArrs_[i] != nullptr, "nullptr");
      delete columnArrs_[i];
    }

    delete[] columnArrs_;
    delete svid_col_;
    delete ts_col_;
  }

  void Init();

  /**
   * 写入一行数据到memtable，会将其所有列分别加到对应列的数组当中, 满了就返回true
   */
  bool Write(uint16_t vid, const Row& row);

  void GetLatestRow(uint16_t svid, const std::vector<int>& colids, Row& row);

  // 写阶段从memtable中读取time-range
  void GetRowsFromTimeRange(uint64_t vid, int64_t lowerInclusive, int64_t upperExclusive,
                            const std::vector<int>& colids, std::vector<Row>& results);

  // 清空状态
  void Reset();

private:
  friend class BlockMetaManager;

  void updateTs(const int64_t ts, uint16_t svid) {
    if (ts < min_ts_[svid]) {
      min_ts_[svid] = ts;
    }

    if (ts > max_ts_[svid]) {
      max_ts_[svid] = ts;
    }
  }

  bool inRange(int64_t ts, int64_t lowerInclusive, int64_t upperExclusive) {
    return ts >= lowerInclusive && ts < upperExclusive;
  }

  void sort();

  TSDBEngineImpl* engine_;
  int shard_id_;

  // std::string *engine->columnsName; // The column's name for each column.
  ColumnArrWrapper** columnArrs_;
  VidArrWrapper* svid_col_;
  TsArrWrapper* ts_col_;
  IdxArrWrapper* idx_col_;

  int cnt_; // 记录这个memtable写了多少行了，由于可能没有写满，然后shutdown刷下去了，所以需要记录一下
  int64_t min_ts_[kVinNumPerShard];
  int64_t max_ts_[kVinNumPerShard];

  // mem latest row idx and ts
  int64_t mem_latest_row_idx_[kVinNumPerShard];
  int64_t mem_latest_row_ts_[kVinNumPerShard];
};

} // namespace LindormContest
