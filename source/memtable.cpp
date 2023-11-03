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
  svid_col_ = new VidArrWrapper(kColumnNum);
  // ts col
  ts_col_ = new TsArrWrapper(kColumnNum + 1);
  // idx col
  idx_col_ = new IdxArrWrapper(kColumnNum + 2);

  for (int i = 0; i < kVinNumPerShard; i++) {
    min_ts_[i] = INT64_MAX;
    max_ts_[i] = INT64_MIN;
    mem_latest_row_idx_[i] = -1;
    mem_latest_row_ts_[i] = -1;
  }
}

void MemTable::sort() {
  // 先对 vid + ts idx这三列进行排序
  uint16_t* vid_datas = svid_col_->GetDataArr();
  int64_t* ts_datas = ts_col_->GetDataArr();
  uint16_t* idx_datas = idx_col_->GetDataArr();

  quickSort(vid_datas, ts_datas, idx_datas, 0, cnt_ - 1);
}

void MemTable::GetRowsFromTimeRange(uint64_t vid, int64_t lowerInclusive, int64_t upperExclusive,
                                    const std::vector<int>& colids, std::vector<Row>& results) {
  // 首先看memtable当中是否有符合要求的数据
  int svid = vid2svid(vid);
  if (!(min_ts_[svid] >= upperExclusive || max_ts_[svid] < lowerInclusive)) {
    // 有重合，需要遍历memtable，首先找到vin + ts符合要求的行索引
    std::vector<int> idxs;
    for (int i = 0; i < cnt_; i++) {
      if (svid_col_->GetVal(i) == (int64_t)svid && inRange(ts_col_->GetVal(i), lowerInclusive, upperExclusive)) {
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
  //  更新本memtable中的最新row信息
  if (mem_latest_row_ts_[svid] < row.timestamp) {
    mem_latest_row_ts_[svid] = row.timestamp;
    mem_latest_row_idx_[svid] = cnt_;
  }

  int colid = 0;
  for (auto& col : row.columns) {
    LOG_ASSERT(col.first == engine_->columns_name_[colid], "invalid column");
    columnArrs_[colid++]->Add(col.second, cnt_);
  }

  // TODO: 不用ColumnValue
  // 写入vid列
  svid_col_->Add(svid, cnt_);

  // 写入ts列
  ts_col_->Add(row.timestamp, cnt_);

  // 写入idx列
  idx_col_->Add(cnt_, cnt_);

  updateTs(row.timestamp, svid);
  cnt_++;

  return cnt_ >= kMemtableRowNum;
};

void MemTable::Reset() {
  for (int i = 0; i < kVinNumPerShard; i++) {
    min_ts_[i] = INT64_MAX;
    max_ts_[i] = INT64_MIN;
    mem_latest_row_idx_[i] = -1;
    mem_latest_row_ts_[i] = -1;
  }
  cnt_ = 0;
  for (int i = 0; i < kColumnNum; i++) {
    columnArrs_[i]->Reset();
  }
  svid_col_->Reset();
  ts_col_->Reset();
  in_flush_ = false;
}

void MemTable::GetLatestRow(uint16_t svid, const std::vector<int>& colids, Row& row) {
  // 最新的row在内存当中
  int mem_lat_idx = mem_latest_row_idx_[svid];
  row.timestamp = mem_latest_row_ts_[svid];
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
