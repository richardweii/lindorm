//
// You should modify this file.
//
// Refer TSDBEngineSample.cpp to ensure that you have understood
// the interface semantics correctly.
//

#include "TSDBEngineImpl.h"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <sstream>
#include <utility>

#include "common.h"
#include "filename.h"
#include "io/file.h"
#include "io/file_manager.h"
#include "memtable.h"
#include "struct/Vin.h"
#include "util/likely.h"
#include "util/logging.h"
#include "util/stat.h"
namespace LindormContest {

std::string kTableName = "only_one"; // 目前就一张表，表名预留给复赛
/**
 * This constructor's function signature should not be modified.
 * Our evaluation program will call this constructor.
 * The function's body can be modified.
 */
TSDBEngineImpl::TSDBEngineImpl(const std::string& dataDirPath) : TSDBEngine(dataDirPath) {
  file_manager_ = new FileManager();
  shard_memtable_ = new ShardMemtable(this);
}

void TSDBEngineImpl::loadSchema() {
  LOG_INFO("start Load Schema");
  // Read schema.
  std::ifstream schemaFin;
  schemaFin.open(getDataPath() + "/schema", std::ios::in);
  if (!schemaFin.is_open() || !schemaFin.good()) {
    std::cout << "Connect new database with empty pre-written data" << std::endl;
    schemaFin.close();
    return;
  }
  int magic = 0;
  schemaFin >> magic;
  if (magic != kColumnNum) {
    std::cerr << "Unexpected columns' num: [" << magic << "]" << std::endl;
    schemaFin.close();
    throw std::exception();
  }
  std::cout << "Found pre-written data with columns' num: [" << kColumnNum << "]" << std::endl;

  columns_type_ = new ColumnType[kColumnNum];
  columns_name_ = new std::string[kColumnNum];

  for (int i = 0; i < kColumnNum; ++i) {
    schemaFin >> columns_name_[i];
    int32_t columnTypeInt;
    schemaFin >> columnTypeInt;
    columns_type_[i] = (ColumnType)columnTypeInt;

    column_idx_.emplace(columns_name_[i], i);
  }
  RemoveFile(getDataPath() + "/schema");
  LOG_INFO("Load Schema finished");
}

int TSDBEngineImpl::connect() {
  print_memory_usage();
  loadSchema();

  // load vin2vid
  {
    vin2vid_lck_.wlock();
    std::string filename = Vin2vidFileName(dataDirPath, kTableName);
    if (file_manager_->Exist(filename)) {
      LOG_INFO("start load vin2vid");
      SequentialReadFile file(filename);
      int num;
      file.read((char*)&num, sizeof(num));
      for (int i = 0; i < num; i++) {
        char vin[VIN_LENGTH] = {0};
        uint16_t vid;
        file.read(vin, VIN_LENGTH);
        file.read((char*)&vid, sizeof(vid));
        std::string vin_str(vin, VIN_LENGTH);
        vin2vid_.emplace(std::make_pair(vin_str, vid));
        vid2vin_.emplace(std::make_pair(vid, vin_str));
      }
      LOG_INFO("load vin2vid finished");
      RemoveFile(filename);
    }
    vin2vid_lck_.unlock();
  }

  table_name_ = kTableName;

  // load block meta
  LOG_INFO("start load block meta");
  for (int i = 0; i < kShardNum; i++) {
    std::string filename = ShardMetaFileName(dataDirPath, kTableName, i);
    if (file_manager_->Exist(filename)) {
      SequentialReadFile file(filename);
      shard_memtable_->LoadBlockMeta(i, &file);
      // 删除文件
      RemoveFile(filename);
    }
  }
  LOG_INFO("load block meta finished");

  // load latest row cache
  LOG_INFO("start load latest row cache");
  for (int i = 0; i < kShardNum; i++) {
    std::string filename = LatestRowFileName(dataDirPath, kTableName, i);
    if (file_manager_->Exist(filename)) {
      SequentialReadFile file(filename);
      shard_memtable_->LoadLatestRowCache(i, &file);
      RemoveFile(filename);
    }
  }
  LOG_INFO("load latest row cache finished");

  if (columns_name_ != nullptr) {
    shard_memtable_->Init();
  }

  return 0;
}
int TSDBEngineImpl::createTable(const std::string& tableName, const Schema& schema) {
  LOG_INFO("start create table %s", tableName.c_str());
  LOG_ASSERT(kColumnNum == (int32_t)schema.columnTypeMap.size(), "schema.column.num != 60");
  columns_name_ = new std::string[kColumnNum];
  columns_type_ = new ColumnType[kColumnNum];
  int i = 0;
  for (auto it = schema.columnTypeMap.cbegin(); it != schema.columnTypeMap.cend(); ++it) {
    column_idx_.emplace(it->first, i);
    columns_name_[i] = it->first;
    columns_type_[i++] = it->second;
  }

  table_name_ = kTableName;
  LOG_INFO("create table %s finished", tableName.c_str());

  shard_memtable_->Init();
  return 0;
}

void TSDBEngineImpl::saveSchema() {
  // Persist the schema.
  if (kColumnNum > 0) {
    std::ofstream schemaFout;
    schemaFout.open(getDataPath() + "/schema", std::ios::out);
    schemaFout << kColumnNum;
    schemaFout << " ";
    for (int i = 0; i < kColumnNum; ++i) {
      schemaFout << columns_name_[i] << " ";
      schemaFout << (int32_t)columns_type_[i] << " ";
    }
    schemaFout.close();
  }
}

int TSDBEngineImpl::shutdown() {
  LOG_INFO("start shutdown");
  // Close all resources, assuming all writing and reading process has finished.
  // No mutex is fetched by assumptions.

  // save schema
  saveSchema();

  // flush memtable
  for (int i = 0; i < kShardNum; i++) {
    shard_memtable_->Flush(i);
  }

  print_summary(columns_type_, columns_name_);
  print_memory_usage();

  // save block meta
  for (int i = 0; i < kShardNum; i++) {
    std::string filename = ShardMetaFileName(dataDirPath, kTableName, i);
    File* file = file_manager_->Open(filename, NORMAL_FLAG);
    shard_memtable_->SaveBlockMeta(i, file);
  }

  // save latest row cache
  for (int i = 0; i < kShardNum; i++) {
    std::string filename = LatestRowFileName(dataDirPath, kTableName, i);
    File* file = file_manager_->Open(filename, NORMAL_FLAG);
    shard_memtable_->SaveLatestRowCache(i, file);
  }

  // save vin2vid
  {
    vin2vid_lck_.wlock();
    std::string filename = Vin2vidFileName(dataDirPath, kTableName);
    File* file = file_manager_->Open(filename, NORMAL_FLAG);
    int num = vin2vid_.size();
    file->write((char*)&num, sizeof(num));
    for (auto& pair : vin2vid_) {
      LOG_ASSERT(pair.first.size() == VIN_LENGTH, "size = %zu", pair.first.size());
      file->write(pair.first.c_str(), VIN_LENGTH);
      file->write((char*)&pair.second, sizeof(pair.second));
    }
    vin2vid_lck_.unlock();
  }

  if (columns_type_ != nullptr) {
    delete[] columns_type_;
  }

  if (columns_name_ != nullptr) {
    delete[] columns_name_;
  }

  delete file_manager_;
  delete shard_memtable_;
  return 0;
}

int TSDBEngineImpl::write(const WriteRequest& writeRequest) {
  RECORD_FETCH_ADD(write_cnt, writeRequest.rows.size());

  for (auto& row : writeRequest.rows) {
    uint16_t vid = getVidForWrite(row.vin);
    LOG_ASSERT(vid != UINT16_MAX, "error");
    shard_memtable_->Add(row, vid);
  }

  return 0;
}

int TSDBEngineImpl::executeLatestQuery(const LatestQueryRequest& pReadReq, std::vector<Row>& pReadRes) {
  RECORD_FETCH_ADD(latest_query_cnt, pReadReq.vins.size());
  std::vector<int> colids;
  for (auto& col_name : pReadReq.requestedColumns) {
    colids.emplace_back(column_idx_[col_name]);
  }

  for (const auto& vin : pReadReq.vins) {
    uint16_t vid = getVidForRead(vin);
    if (vid == UINT16_MAX) {
      continue;
    }

    Row row;
    shard_memtable_->GetLatestRow(vid, row, colids);
    pReadRes.push_back(std::move(row));
  }

  return 0;
}

int TSDBEngineImpl::executeTimeRangeQuery(const TimeRangeQueryRequest& trReadReq, std::vector<Row>& trReadRes) {
  RECORD_FETCH_ADD(time_range_query_cnt, 1);
  std::vector<int> colids;
  for (auto& col_name : trReadReq.requestedColumns) {
    colids.emplace_back(column_idx_[col_name]);
  }
  uint16_t vid = getVidForRead(trReadReq.vin);
  if (vid == UINT16_MAX) {
    return 0;
  }

  shard_memtable_->GetRowsFromTimeRange(vid, trReadReq.timeLowerBound, trReadReq.timeUpperBound, colids, trReadRes);

  return 0;
}

int TSDBEngineImpl::executeAggregateQuery(const TimeRangeAggregationRequest& aggregationReq,
                                          std::vector<Row>& aggregationRes) {
  return 0;
}

int TSDBEngineImpl::executeDownsampleQuery(const TimeRangeDownsampleRequest& downsampleReq,
                                           std::vector<Row>& downsampleRes) {
  return 0;
}

TSDBEngineImpl::~TSDBEngineImpl() = default;

uint16_t TSDBEngineImpl::getVidForRead(const Vin& vin) {
  vin2vid_lck_.rlock();
  std::string str(vin.vin, VIN_LENGTH);
  auto it = vin2vid_.find(str);
  if (it == vin2vid_.end()) {
    vin2vid_lck_.unlock();
    LOG_INFO("查找了一个不存在的vin");
    return UINT16_MAX;
  }
  uint16_t vid = it->second;
  vin2vid_lck_.unlock();

  return vid;
}

uint16_t TSDBEngineImpl::getVidForWrite(const Vin& vin) {
  uint16_t vid = UINT16_MAX;
  vin2vid_lck_.rlock();
  std::string str(vin.vin, VIN_LENGTH);
  auto it = vin2vid_.find(str);
  if (LIKELY(it != vin2vid_.cend())) {
    vin2vid_lck_.unlock();
    vid = it->second;
  } else {
    vin2vid_lck_.unlock();
    vin2vid_lck_.wlock();
    it = vin2vid_.find(str);
    if (LIKELY(it == vin2vid_.cend())) {
      vid = vid_cnt_;
#ifdef ENABLE_STAT
      if (vid % 10000 == 9999) {
        LOG_INFO("vid %d", vid);
      }
#endif
      vid2vin_.emplace(std::make_pair(vid_cnt_, str));
      vin2vid_.emplace(std::make_pair(str, vid_cnt_++));
      vin2vid_lck_.unlock();
    } else {
      vid = it->second;
      vin2vid_lck_.unlock();
    }
    LOG_ASSERT(vid != UINT16_MAX, "vid == UINT16_MAX");
  }

  return vid;
}
} // namespace LindormContest