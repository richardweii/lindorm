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
    columnsType[i] = (ColumnType)columnTypeInt;

    col2colid.emplace(columnsName[i], i);
  }
  RemoveFile(getDataPath() + "/schema");
  LOG_INFO("Load Schema finished");
}

int TSDBEngineImpl::connect() {
  print_memory_usage();
  LoadSchema();

  // load vin2vid
  {
    vin2vid_lck.wlock();
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
        vin2vid.emplace(std::make_pair(vin_str, vid));
        vid2vin.emplace(std::make_pair(vid, vin_str));
      }
      LOG_INFO("load vin2vid finished");
      RemoveFile(filename);
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

  if (columnsName != nullptr) {
    shard_memtable_->Init();
  }

  return 0;
}

int TSDBEngineImpl::createTable(const std::string& tableName, const Schema& schema) {
  LOG_INFO("start create table %s", tableName.c_str());
  columnsNum = (int32_t)schema.columnTypeMap.size();
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
      schemaFout << (int32_t)columnsType[i] << " ";
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

  print_summary(columnsType, columnsName);
  print_memory_usage();

  // save block meta
  for (int i = 0; i < kShardNum; i++) {
    std::string filename = ShardMetaFileName(dataDirPath, kTableName, i);
    File* file = file_manager_->Open(filename);
    shard_memtable_->SaveBlockMeta(i, file);
  }

  // save latest row cache
  for (int i = 0; i < kShardNum; i++) {
    std::string filename = LatestRowFileName(dataDirPath, kTableName, i);
    File* file = file_manager_->Open(filename);
    shard_memtable_->SaveLatestRowCache(i, file);
  }

  // save vin2vid
  {
    vin2vid_lck.wlock();
    std::string filename = Vin2vidFileName(dataDirPath, kTableName);
    File* file = file_manager_->Open(filename);
    int num = vin2vid.size();
    file->write((char*)&num, sizeof(num));
    for (auto& pair : vin2vid) {
      LOG_ASSERT(pair.first.size() == VIN_LENGTH, "size = %zu", pair.first.size());
      file->write(pair.first.c_str(), VIN_LENGTH);
      file->write((char*)&pair.second, sizeof(pair.second));
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

// static std::atomic<int> flag = 0;
// int cnt[kVinNum] = {0};
// std::mutex mutex;

int TSDBEngineImpl::upsert(const WriteRequest& writeRequest) {
  // static std::atomic<int> upsert_cnt = 0;
  RECORD_FETCH_ADD(write_cnt, writeRequest.rows.size());

  for (auto& row : writeRequest.rows) {
    uint16_t vid = write_get_vid(row.vin);
    // if (vid == 0 && upsert_cnt++ < 100) {
    //   print_row(row, vid);
    // }
    LOG_ASSERT(vid != UINT16_MAX, "error");
    shard_memtable_->Add(row, vid);
  }

  // if (flag.load() < 10) {
  //   mutex.lock();
  //   if (flag.load() < 10) {
  //     flag += 1;
  //     memset(cnt, 0, sizeof(int)*kVinNum);
  //     for (auto &row : writeRequest.rows) {
  //       uint16_t vid = write_get_vid(row.vin);
  //       cnt[vid]++;
  //     }
  //     printf("*************\n");
  //     for (int i = 0; i < kVinNum; i++) {
  //       if (cnt[i] != 0) {
  //         printf("vid %d cnt %d | ", i, cnt[i]);
  //       }
  //     }
  //     printf("\n*************\n");
  //   }
  //   mutex.unlock();
  // }

  return 0;
}

int TSDBEngineImpl::executeLatestQuery(const LatestQueryRequest& pReadReq, std::vector<Row>& pReadRes) {
  RECORD_FETCH_ADD(latest_query_cnt, pReadReq.vins.size());
  std::vector<int> colids;
  for (auto& col_name : pReadReq.requestedColumns) {
    colids.emplace_back(col2colid[col_name]);
  }

  for (const auto& vin : pReadReq.vins) {
    uint16_t vid = read_get_vid(vin);
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
  uint16_t vid = read_get_vid(trReadReq.vin);
  if (vid == UINT16_MAX) {
    return 0;
  }

  shard_memtable_->GetRowsFromTimeRange(vid, trReadReq.timeLowerBound, trReadReq.timeUpperBound,
                                        trReadReq.requestedColumns, trReadRes);

  return 0;
}

TSDBEngineImpl::~TSDBEngineImpl() {}

} // namespace LindormContest
