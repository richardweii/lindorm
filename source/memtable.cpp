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
  columnArrs_ = new ColumnArrWrapper*[kColumnNum];
  for (int i = 0; i < kColumnNum; i++) {
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
  // svid_col_ = new VidArrWrapper(kColumnNum);
  // ts col
  ts_col_ = new TsArrWrapper(kColumnNum);
  // idx col
  // idx_col_ = new IdxArrWrapper(kColumnNum + 2);

  for (int i = 0; i < kVinNumPerShard; i++) {
    min_ts_ = INT64_MAX;
    max_ts_ = INT64_MIN;
    mem_latest_row_idx_ = -1;
    mem_latest_row_ts_ = -1;
    for (int i = 0; i < kColumnNum; i++) {
      max_val_[i] = 0;
      sum_val_[i] = 0;
    }
  }
}

void MemTable::GetRowsFromTimeRange(uint64_t vid, int64_t lowerInclusive, int64_t upperExclusive,
                                    const std::vector<int>& colids, std::vector<Row>& results) {
  // 首先看memtable当中是否有符合要求的数据
  int svid = vid2svid(vid);
  if (!(min_ts_ >= upperExclusive || max_ts_ < lowerInclusive)) {
    // 有重合，需要遍历memtable，首先找到vin + ts符合要求的行索引
    std::vector<int> idxs;
    for (int i = 0; i < cnt_; i++) {
      if (inRange(ts_col_->GetVal(i), lowerInclusive, upperExclusive)) {
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
        results.emplace_back(std::move(resultRow));
      }
    }
  }
}

bool MemTable::Write(uint16_t svid, const Row& row) {
#ifdef ENABLE_STAT
  if (svid2vid(shard_id_, svid) == 0x111) {
    if (print_row_cnt.fetch_add(1) <= 100) {
      print_row(row, svid2vid(shard_id_, svid));
    }
  }
#endif
  //  更新本memtable中的最新row信息
  if (mem_latest_row_ts_ < row.timestamp) {
    mem_latest_row_ts_ = row.timestamp;
    mem_latest_row_idx_ = cnt_;
  }

  int colid = 0;
  for (auto& col : row.columns) {
    LOG_ASSERT(col.first == engine_->columns_name_[colid], "invalid column");
    columnArrs_[colid]->Add(col.second, cnt_);
    auto& col_val = col.second;
    switch (col_val.columnType) {
      case COLUMN_TYPE_INTEGER: {
        int cur;
        col_val.getIntegerValue(cur);

        int max = TO_INT(max_val_[colid]);
        if (cur > max || cnt_ == 0) {
          TO_INT(max_val_[colid]) = cur;
        }

        // int64 防止溢出
        int64_t sum = TO_INT64(sum_val_[colid]);
        TO_INT64(sum_val_[colid]) = cur + sum;
        break;
      }
      case COLUMN_TYPE_DOUBLE_FLOAT: {
        double cur;
        col_val.getDoubleFloatValue(cur);

        double max = TO_DOUBLE(max_val_[colid]);
        if (cur > max || cnt_ == 0) {
          TO_DOUBLE(max_val_[colid]) = cur;
        }

        double sum = TO_DOUBLE(sum_val_[colid]);
        TO_DOUBLE(sum_val_[colid]) = cur + sum;
        break;
      }
      default:
        break;
    }
    colid++;
  }

  // TODO: 不用ColumnValue
  // 写入vid列
  // svid_col_->Add(svid, cnt_);

  // 写入ts列
  ts_col_->Add(row.timestamp, cnt_);

  // 写入idx列
  // idx_col_->Add(cnt_, cnt_);

  updateTs(row.timestamp, svid);
  cnt_++;

  return cnt_ >= kMemtableRowNum;
};

void MemTable::Reset() {
  min_ts_ = INT64_MAX;
  max_ts_ = INT64_MIN;
  mem_latest_row_idx_ = -1;
  mem_latest_row_ts_ = -1;
  cnt_ = 0;
  for (int i = 0; i < kColumnNum; i++) {
    columnArrs_[i]->Reset();
    max_val_[i] = 0;
    sum_val_[i] = 0;
  }
  ts_col_->Reset();
  in_flush_ = false;
}

void MemTable::GetLatestRow(uint16_t svid, const std::vector<int>& colids, Row& row) {
  // 最新的row在内存当中
  int mem_lat_idx = mem_latest_row_idx_;
  row.timestamp = mem_latest_row_ts_;
  memcpy(row.vin.vin, engine_->vid2vin_[svid2vid(shard_id_, svid)].c_str(), VIN_LENGTH);
  for (auto col_id : colids) {
    ColumnValue col_value;
    auto& col_name = engine_->columns_name_[col_id];
    columnArrs_[col_id]->Get(mem_lat_idx, col_value);
    row.columns.insert(std::make_pair(col_name, std::move(col_value)));
  }

  LOG_ASSERT(row.timestamp != -1, "???");
}

} // namespace LindormContest
