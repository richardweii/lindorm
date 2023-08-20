//
// You should modify this file.
//
// Refer TSDBEngineSample.cpp to ensure that you have understood
// the interface semantics correctly.
//

#include "TSDBEngineImpl.h"
#include "common.h"
#include "filename.h"
#include "io/file.h"
#include "io/file_manager.h"
#include "memtable.h"
#include "struct/Vin.h"
#include "util/likely.h"
#include "util/logging.h"
#include <atomic>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <utility>

namespace LindormContest {

std::string kTableName = "only_one"; // 目前就一张表，表名预留给复赛

/**
 * This constructor's function signature should not be modified.
 * Our evaluation program will call this constructor.
 * The function's body can be modified.
 */
TSDBEngineImpl::TSDBEngineImpl(const std::string &dataDirPath) : TSDBEngine(dataDirPath) {
  file_manager_ = new FileManager();
  shard_memtable_ = new ShardMemtable(this);
}

void TSDBEngineImpl::LoadSchema() {
  LOG_INFO("start Load Schema");
  // Read schema.
  std::ifstream schemaFin;
  schemaFin.open(getDataPath() + "/schema", std::ios::in);
  if (!schemaFin.is_open() || !schemaFin.good()) {
    std::cout << "Connect new database with empty pre-written data" << std::endl;
    schemaFin.close();
    return;
  }

  schemaFin >> columnsNum;
  if (columnsNum <= 0) {
    std::cerr << "Unexpected columns' num: [" << columnsNum << "]" << std::endl;
    schemaFin.close();
    throw std::exception();
  }
  std::cout << "Found pre-written data with columns' num: [" << columnsNum << "]" << std::endl;

  columnsType = new ColumnType[columnsNum];
  columnsName = new std::string[columnsNum];

  for (int i = 0; i < columnsNum; ++i) {
    schemaFin >> columnsName[i];
    int32_t columnTypeInt;
    schemaFin >> columnTypeInt;
    columnsType[i] = (ColumnType) columnTypeInt;

    col2colid.emplace(columnsName[i], i);
  }
  LOG_INFO("Load Schema finished");
}

int TSDBEngineImpl::connect() {
  LoadSchema();

  // load vin2vid
  {
    vin2vid_lck.wlock();
    std::string filename = Vin2vidFileName(dataDirPath, kTableName);
    if (file_manager_->Exist(filename)) {
      LOG_INFO("start load vin2vid");
      SequentialReadFile file(filename);
      int num;
      file.read((char *) &num, sizeof(num));
      for (int i = 0; i < num; i++) {
        char vin[VIN_LENGTH] = {0};
        uint16_t vid;
        file.read(vin, VIN_LENGTH);
        file.read((char *) &vid, sizeof(vid));
        std::string vin_str(vin, VIN_LENGTH);
        vin2vid.emplace(std::make_pair(vin_str, vid));
        vid2vin.emplace(std::make_pair(vid, vin_str));
      }
      LOG_INFO("load vin2vid finished");
    }
    vin2vid_lck.unlock();
  }

  table_name_ = kTableName;

  // load block meta
  LOG_INFO("start load block meta");
  for (int i = 0; i < kShardNum; i++) {
    std::string filename = ShardMetaFileName(dataDirPath, kTableName, i);
    if (file_manager_->Exist(filename)) {
      SequentialReadFile file(filename);
      shard_memtable_->LoadBlockMeta(i, &file);
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
    }
  }
  LOG_INFO("load latest row cache finished");

  if (columnsName != nullptr) {
    shard_memtable_->Init();
  }

  return 0;
}

int TSDBEngineImpl::createTable(const std::string &tableName, const Schema &schema) {
  LOG_INFO("start create table %s", tableName.c_str());
  columnsNum = (int32_t) schema.columnTypeMap.size();
  columnsName = new std::string[columnsNum];
  columnsType = new ColumnType[columnsNum];
  int i = 0;
  for (auto it = schema.columnTypeMap.cbegin(); it != schema.columnTypeMap.cend(); ++it) {
    col2colid.emplace(it->first, i);

    columnsName[i] = it->first;
    columnsType[i++] = it->second;
  }

  table_name_ = kTableName;
  LOG_INFO("create table %s finished", tableName.c_str());

  shard_memtable_->Init();
  return 0;
}

void TSDBEngineImpl::SaveSchema() {
  // Persist the schema.
  if (columnsNum > 0) {
    std::ofstream schemaFout;
    schemaFout.open(getDataPath() + "/schema", std::ios::out);
    schemaFout << columnsNum;
    schemaFout << " ";
    for (int i = 0; i < columnsNum; ++i) {
      schemaFout << columnsName[i] << " ";
      schemaFout << (int32_t) columnsType[i] << " ";
    }
    schemaFout.close();
  }
}

int TSDBEngineImpl::shutdown() {
  LOG_INFO("start shutdown");
  // Close all resources, assuming all writing and reading process has finished.
  // No mutex is fetched by assumptions.

  // save schema
  SaveSchema();

  // flush memtable
  for (int i = 0; i < kShardNum; i++) {
    shard_memtable_->Flush(i);
  }

  // save block meta
  for (int i = 0; i < kShardNum; i++) {
    std::string filename = ShardMetaFileName(dataDirPath, kTableName, i);
    File *file = file_manager_->Open(filename);
    shard_memtable_->SaveBlockMeta(i, file);
  }

  // save latest row cache
  for (int i = 0; i < kShardNum; i++) {
    std::string filename = LatestRowFileName(dataDirPath, kTableName, i);
    File *file = file_manager_->Open(filename);
    shard_memtable_->SaveLatestRowCache(i, file);
  }

  // save vin2vid
  {
    vin2vid_lck.wlock();
    std::string filename = Vin2vidFileName(dataDirPath, kTableName);
    File *file = file_manager_->Open(filename);
    int num = vin2vid.size();
    file->write((char *) &num, sizeof(num));
    for (auto &pair : vin2vid) {
      LOG_ASSERT(pair.first.size() == VIN_LENGTH, "size = %zu", pair.first.size());
      file->write(pair.first.c_str(), VIN_LENGTH);
      file->write((char *) &pair.second, sizeof(pair.second));
    }
    vin2vid_lck.unlock();
  }

  if (columnsType != nullptr) {
    delete[] columnsType;
  }

  if (columnsName != nullptr) {
    delete[] columnsName;
  }

  delete file_manager_;
  delete shard_memtable_;
  return 0;
}

int TSDBEngineImpl::upsert(const WriteRequest &writeRequest) {
  for (auto &row : writeRequest.rows) {
    vin2vid_lck.rlock();
    std::string str(row.vin.vin, VIN_LENGTH);
    auto it = vin2vid.find(str);
    if (LIKELY(it != vin2vid.cend())) {
      vin2vid_lck.unlock();
      // find vid
      shard_memtable_->Add(row, it->second);
    } else {
      vin2vid_lck.unlock();
      vin2vid_lck.wlock();
      int vid = -1;
      it = vin2vid.find(str);
      if (LIKELY(it == vin2vid.cend())) {
        vid = vid_cnt_;
        if (vid % 10000 == 1) {
          LOG_INFO("vid %d", vid);
        }
        if (vid % 10000 == 9999) {
          LOG_INFO("vid %d", vid);
        }
        vid2vin.emplace(std::make_pair(vid_cnt_, str));
        vin2vid.emplace(std::make_pair(str, vid_cnt_++));
        vin2vid_lck.unlock();
      } else {
        vid = it->second;
        vin2vid_lck.unlock();
      }
      LOG_ASSERT(vid != -1, "vid == -1");
      shard_memtable_->Add(row, vid);
    }
  }

  return 0;
}

int TSDBEngineImpl::executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) {
  static std::atomic<int> latest_log_cnt = 0;
  if (latest_log_cnt < 10) {
    LOG_INFO("executeLatestQuery %d, size %zu", latest_log_cnt.load(), pReadReq.vins.size());
  }
  for (const auto &vin : pReadReq.vins) {
    vin2vid_lck.rlock();
    std::string str(vin.vin, VIN_LENGTH);
    auto it = vin2vid.find(str);
    LOG_ASSERT(it != vin2vid.end(), "it == end");
    uint16_t vid = it->second;
    vin2vid_lck.unlock();
    Row row = shard_memtable_->GetLatestRow(vid);
    Row res;
    res.vin = vin;
    res.timestamp = row.timestamp;
    for (const auto &requestedColumn : pReadReq.requestedColumns) {
      auto pair = row.columns.find(requestedColumn);
      LOG_ASSERT(pair != row.columns.end(), "error");
      auto col = pair->second;
      res.columns.insert(std::make_pair(requestedColumn, col));
    }

    pReadRes.push_back(std::move(res));
  }

  if (latest_log_cnt++ < 10) {
    LOG_INFO("executeLatestQuery %d, done", latest_log_cnt.load());
  }
  return 0;
}

int TSDBEngineImpl::executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) {
  static std::atomic<int> time_range_log_cnt = 0;
  if (time_range_log_cnt < 10) {
    LOG_INFO("executeTimeRangeQuery %d, low %zu, upper %zu", time_range_log_cnt.load(), trReadReq.timeLowerBound, trReadReq.timeUpperBound);
  }

  vin2vid_lck.rlock();
  std::string str(trReadReq.vin.vin, VIN_LENGTH);
  auto it = vin2vid.find(str);
  LOG_ASSERT(it != vin2vid.end(), "it == end");
  uint16_t vid = it->second;
  vin2vid_lck.unlock();
  
  shard_memtable_->GetRowsFromTimeRange(vid, trReadReq.timeLowerBound, trReadReq.timeUpperBound, trReadReq.requestedColumns, trReadRes);

  if (time_range_log_cnt++ < 10) {
    LOG_INFO("executeLatestQuery %d done", time_range_log_cnt.load());
  }

  return 0;
}

TSDBEngineImpl::~TSDBEngineImpl() {}

} // namespace LindormContest
