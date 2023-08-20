#include "Hasher.hpp"
#include "TSDBEngineImpl.h"
#include "common.h"
#include "struct/ColumnValue.h"
#include "struct/Vin.h"
#include "test.hpp"
#include "util/logging.h"
#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

/**
 * 把文件夹中的文件删除掉
 */
static void clearTempFolder(const std::string dataPath) {
  std::filesystem::path folder_path(dataPath);
  std::filesystem::remove_all(folder_path);
  mkdir(folder_path.c_str(), S_IRUSR | S_IWUSR | S_IXUSR | S_IRWXG | S_IRWXO);
  std::cout << "Successfully deleted all files and folders under " << folder_path << std::endl;
}

// 0 int 1 double 2 string
static int types[] = {0, 0, 1, 0, 1, 0, 2, 1, 1, 0, 1, 0, 1, 0, 1, 0, 2, 1, 1, 0, 0, 1, 1, 0, 1, 0, 1, 1, 1, 0,
                      0, 0, 1, 0, 1, 2, 2, 1, 1, 2, 1, 0, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0, 2, 1, 0, 1, 1, 1, 0};

/**
 * 创建一个60个列的表
 */
static int createTable(LindormContest::TSDBEngine *engine) {
  LindormContest::Schema schema;
  for (int i = 0; i < LindormContest::kColumnNum; i++) {
    std::string col_name = "c" + std::to_string(i);
    if (types[i] == 0) {
      schema.columnTypeMap[col_name] = LindormContest::COLUMN_TYPE_INTEGER;
    } else if (types[i] == 1) {
      schema.columnTypeMap[col_name] = LindormContest::COLUMN_TYPE_DOUBLE_FLOAT;
    } else {
      schema.columnTypeMap[col_name] = LindormContest::COLUMN_TYPE_STRING;
    }
  }

  int ret = engine->createTable("t1", schema);
  if (ret != 0) {
    std::cerr << "Create table 1 failed" << std::endl;
    return -1;
  }

  return 0;
}

static constexpr int kVinNum = 30000;
static constexpr int kRowsPerVin = 10;
static LindormContest::Row rows[kVinNum][kRowsPerVin];

void prepare_data() {
  LOG_INFO("start prepare data...");

  for (int i = 0; i < kVinNum; i++) {
    for (int j = 0; j < kRowsPerVin; j++) {
      auto &row = rows[i][j];
      std::string vin = std::to_string(i);
      memcpy(row.vin.vin, vin.c_str(), vin.size());
      row.timestamp = j;
      // build columns
      for (int k = 0; k < LindormContest::kColumnNum; k++) {
        std::string col_name = "c" + std::to_string(k);
        switch (types[k]) {
          case 0: {
            LindormContest::ColumnValue col(j);
            row.columns.insert(std::make_pair(col_name, col));
            break;
          }
          case 1: {
            LindormContest::ColumnValue col(j * 1.0);
            row.columns.insert(std::make_pair(col_name, col));
            break;
          }
          case 2: {
            std::string s = std::to_string(j);
            LindormContest::ColumnValue col(s);
            LOG_ASSERT(col.columnType == LindormContest::COLUMN_TYPE_STRING, "???");
            row.columns.insert(std::make_pair(col_name, col));
            break;
          }
        }
      }
    }
  }
  LOG_INFO("prepare data finished!");
}

void upsert(LindormContest::TSDBEngine *engine) {
  LOG_INFO("start upsert...");
  // insert
  for (int i = 0; i < kVinNum; i++) {
    for (int j = 0; j < kRowsPerVin; j++) {
      LindormContest::WriteRequest wReq;
      wReq.tableName = "t1";
      wReq.rows.push_back(rows[i][j]);
      int ret = engine->upsert(wReq);
    }
  }
  LOG_INFO("upsert finished!");
}

void parallel_upsert(LindormContest::TSDBEngine *engine) {
  LOG_INFO("start parallel upsert...");

  std::vector<std::thread> threads;
  const int thread_num = 30;
  const int per_thread_vin_num = kVinNum / thread_num;
  for (int t = 0; t < thread_num; t++) {
    threads.emplace_back([=]() {
      for (int j = 0; j < kRowsPerVin; j++) {
        for (int i = t * per_thread_vin_num; i < (t + 1) * per_thread_vin_num; i++) {
          LindormContest::WriteRequest wReq;
          wReq.tableName = "t1";
          // int k = 0;
          // for (; i < (t + 1) * per_thread_vin_num && k < 500; k++, i++) {
          wReq.rows.push_back(rows[i][j]);
          // }
          int ret = engine->upsert(wReq);
        }
      }
    });
  }

  for (auto &th : threads) {
    th.join();
  }

  LOG_INFO("parallel upsert end");
}

void test_latest(LindormContest::TSDBEngine *engine) {
  LOG_INFO("start test latest...");
  // validate executeLatestQuery
  for (int i = 0; i < kVinNum; i++) {
    LindormContest::LatestQueryRequest pReadReq;
    std::vector<LindormContest::Row> pReadRes;
    pReadReq.tableName = "t1";
    for (int j = 0; j < LindormContest::kColumnNum; j++) {
      std::string col_name = "c" + std::to_string(j);
      pReadReq.requestedColumns.insert(col_name);
    }
    LindormContest::Vin vin;
    memcpy(vin.vin, rows[i][0].vin.vin, LindormContest::VIN_LENGTH);
    pReadReq.vins.push_back(vin);
    engine->executeLatestQuery(pReadReq, pReadRes);
    LOG_ASSERT(pReadRes.size() == 1, "executeLatestQuery failed");
    auto row = pReadRes[0];
    for (int k = 0; k < LindormContest::kColumnNum; k++) {
      std::string col_name = "c" + std::to_string(k);
      auto pair = row.columns.find(col_name);
      LOG_ASSERT(pair != row.columns.end(), "error");
      auto col_val = pair->second;
      switch (types[k]) {
        case 0: {
          LOG_ASSERT(col_val.columnType == LindormContest::COLUMN_TYPE_INTEGER, "COLUMN_TYPE_INTEGER");
          int val;
          col_val.getIntegerValue(val);
          LOG_ASSERT(val == kRowsPerVin - 1, "val = %d", val);
        } break;
        case 1: {
          LOG_ASSERT(col_val.columnType == LindormContest::COLUMN_TYPE_DOUBLE_FLOAT, "COLUMN_TYPE_DOUBLE_FLOAT");
          double val;
          col_val.getDoubleFloatValue(val);
          LOG_ASSERT(val == (kRowsPerVin - 1) * 1.0, "val = %f", val);
        } break;
        case 2: {
          LOG_ASSERT(col_val.columnType == LindormContest::COLUMN_TYPE_STRING, "COLUMN_TYPE_STRING");
          std::pair<int32_t, const char *> pair;
          col_val.getStringValue(pair);
          std::string str(pair.second, pair.first);
          LOG_ASSERT(str == std::to_string(kRowsPerVin - 1), "val = %s", str.c_str());
        } break;
      }
    }
  }
  LOG_INFO("test latest PASS");
}

void parallel_test_latest(LindormContest::TSDBEngine *engine) {
  LOG_INFO("start parallel test latest...");
  // validate executeLatestQuery
  std::vector<std::thread> threads;
  const int thread_num = 30;
  const int per_thread_vin_num = kVinNum / thread_num;

  for (int t = 0; t < thread_num; t++) {
    threads.emplace_back([=]() {
      for (int i = t * per_thread_vin_num; i < (t + 1) * per_thread_vin_num; i++) {
        LindormContest::LatestQueryRequest pReadReq;
        std::vector<LindormContest::Row> pReadRes;
        pReadReq.tableName = "t1";
        for (int j = 0; j < LindormContest::kColumnNum; j++) {
          std::string col_name = "c" + std::to_string(j);
          pReadReq.requestedColumns.insert(col_name);
        }
        LindormContest::Vin vin;
        memcpy(vin.vin, rows[i][0].vin.vin, LindormContest::VIN_LENGTH);
        pReadReq.vins.push_back(vin);
        engine->executeLatestQuery(pReadReq, pReadRes);
        LOG_ASSERT(pReadRes.size() == 1, "executeLatestQuery failed");
        auto row = pReadRes[0];
        for (int k = 0; k < LindormContest::kColumnNum; k++) {
          std::string col_name = "c" + std::to_string(k);
          auto pair = row.columns.find(col_name);
          LOG_ASSERT(pair != row.columns.end(), "error");
          auto col_val = pair->second;
          switch (types[k]) {
            case 0: {
              LOG_ASSERT(col_val.columnType == LindormContest::COLUMN_TYPE_INTEGER, "COLUMN_TYPE_INTEGER");
              int val;
              col_val.getIntegerValue(val);
              LOG_ASSERT(val == kRowsPerVin - 1, "val = %d", val);
            } break;
            case 1: {
              LOG_ASSERT(col_val.columnType == LindormContest::COLUMN_TYPE_DOUBLE_FLOAT, "COLUMN_TYPE_DOUBLE_FLOAT");
              double val;
              col_val.getDoubleFloatValue(val);
              LOG_ASSERT(val == (kRowsPerVin - 1) * 1.0, "val = %f", val);
            } break;
            case 2: {
              LOG_ASSERT(col_val.columnType == LindormContest::COLUMN_TYPE_STRING, "COLUMN_TYPE_STRING");
              std::pair<int32_t, const char *> pair;
              col_val.getStringValue(pair);
              std::string str(pair.second, pair.first);
              LOG_ASSERT(str == std::to_string(kRowsPerVin - 1), "val = %s", str.c_str());
            } break;
          }
        }
      }
    });
  }

  for (auto &th : threads) {
    th.join();
  }

  LOG_INFO("parallel test latest PASS");
}

void test_time_range(LindormContest::TSDBEngine *engine) {
  LOG_INFO("start test time range...");
  // validate time range
  for (int i = 0; i < kVinNum; i++) {
    LindormContest::TimeRangeQueryRequest trR;
    LindormContest::Vin vin;
    memcpy(vin.vin, rows[i][0].vin.vin, LindormContest::VIN_LENGTH);
    trR.vin = vin;
    trR.tableName = "t1";
    trR.timeLowerBound = 0;
    trR.timeUpperBound = kRowsPerVin;
    for (int j = 0; j < LindormContest::kColumnNum; j++) {
      std::string col_name = "c" + std::to_string(j);
      trR.requestedColumns.insert(col_name);
    }
    std::vector<LindormContest::Row> trReadRes;
    int ret = engine->executeTimeRangeQuery(trR, trReadRes);
    LOG_ASSERT(trReadRes.size() == kRowsPerVin, "size = %zu", trReadRes.size());
    for (auto &row : trReadRes) {
      auto expect_row = rows[i][row.timestamp];
      LOG_ASSERT(row == rows[i][row.timestamp], "error");
    }
  }
  LOG_INFO("test time range PASS");
}

void parallel_test_time_range(LindormContest::TSDBEngine *engine) {
  LOG_INFO("start parallel test time range...");
  // validate time range
  std::vector<std::thread> threads;
  const int thread_num = 30;
  const int per_thread_vin_num = kVinNum / thread_num;

  for (int t = 0; t < thread_num; t++) {
    threads.emplace_back([=]() {
      for (int i = t * per_thread_vin_num; i < (t + 1) * per_thread_vin_num; i++) {
        LindormContest::TimeRangeQueryRequest trR;
        LindormContest::Vin vin;
        memcpy(vin.vin, rows[i][0].vin.vin, LindormContest::VIN_LENGTH);
        trR.vin = vin;
        trR.tableName = "t1";
        trR.timeLowerBound = 5;
        trR.timeUpperBound = kRowsPerVin;
        for (int j = 0; j < LindormContest::kColumnNum; j++) {
          std::string col_name = "c" + std::to_string(j);
          trR.requestedColumns.insert(col_name);
        }
        std::vector<LindormContest::Row> trReadRes;
        int ret = engine->executeTimeRangeQuery(trR, trReadRes);
        LOG_ASSERT(trReadRes.size() == kRowsPerVin - 5, "size = %zu", trReadRes.size());
        for (auto &row : trReadRes) {
          auto expect_row = rows[i][row.timestamp];
          LOG_ASSERT(row == rows[i][row.timestamp], "error");
        }
      }
    });
  }

  for (auto &th : threads) {
    th.join();
  }

  LOG_INFO("parallel test time range PASS");
}

int main() {
  std::string dataPath = "/tmp/tsdb_test";
  clearTempFolder(dataPath);
  LindormContest::TSDBEngine *engine = new LindormContest::TSDBEngineImpl(dataPath);

  // connect
  int ret = engine->connect();
  LOG_ASSERT(ret == 0, "connect failed");

  // create table
  ret = createTable(engine);
  LOG_ASSERT(ret == 0, "create table failed");

  prepare_data();

  // upsert(engine);
  parallel_upsert(engine);

  // test_latest(engine);
  parallel_test_latest(engine);
  // test_time_range(engine);
  parallel_test_time_range(engine);

  LOG_INFO("start shutdown...");
  engine->shutdown();
  delete engine;
  LOG_INFO("shutdown finished");

  LOG_INFO("start connect...");
  engine = new LindormContest::TSDBEngineImpl(dataPath);
  engine->connect();
  LOG_INFO("start connect finished");

  // test_latest(engine);
  parallel_test_latest(engine);
  // test_time_range(engine);
  parallel_test_time_range(engine);

  LOG_INFO("PASS!!!");
}
