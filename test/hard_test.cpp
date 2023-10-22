#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <thread>

#include "Hasher.hpp"
#include "TSDBEngineImpl.h"
#include "common.h"
#include "struct/ColumnValue.h"
#include "struct/Vin.h"
#include "test.hpp"
#include "util/logging.h"

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
static int createTable(LindormContest::TSDBEngine* engine) {
  LindormContest::Schema schema;
  for (int i = 0; i < LindormContest::kColumnNum; i++) {
    std::string col_name = "column_" + std::to_string(i);
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

static constexpr int kVinNum = LindormContest::kVinNum;
static constexpr int kRowsPerVin = 1000;
static LindormContest::Row rows[kVinNum][kRowsPerVin];

static bool RowEquals(const LindormContest::Row& a, const LindormContest::Row& b) {
  if (a != b) return false;
  auto a_cols = a.columns;
  auto b_cols = b.columns;
  ASSERT(a_cols.size() == b_cols.size(), "a_cols.size %zu, b_cols.size %zu", a_cols.size(), b_cols.size());
  for (auto& col : a_cols) {
    auto& name = col.first;
    auto a_col_val = col.second;
    auto iter = b_cols.find(name);
    ASSERT(iter != b_cols.cend(), "b_cols 没有 col %s", name.c_str());
    auto b_col_val = iter->second;
    ASSERT(a_col_val.getColumnType() == b_col_val.getColumnType(), "column type !=");
    switch (a_col_val.getColumnType()) {
      case LindormContest::COLUMN_TYPE_STRING: {
        std::pair<int32_t, const char*> a_pair;
        a_col_val.getStringValue(a_pair);
        std::string a_str(a_pair.second, a_pair.first);

        std::pair<int32_t, const char*> b_pair;
        b_col_val.getStringValue(b_pair);
        std::string b_str(b_pair.second, b_pair.first);

        ASSERT(a_pair.first == b_pair.first, "a len = %d, b len = %d", a_pair.first, b_pair.first);
        ASSERT(a_str == b_str, "a_str %s b_str %s", a_str.c_str(), b_str.c_str());
      } break;
      case LindormContest::COLUMN_TYPE_INTEGER: {
        int a_val;
        a_col_val.getIntegerValue(a_val);

        int b_val;
        b_col_val.getIntegerValue(b_val);

        ASSERT(a_val == b_val, "a_val %d b_val %d", a_val, b_val);
      } break;
      case LindormContest::COLUMN_TYPE_DOUBLE_FLOAT: {
        double a_val;
        a_col_val.getDoubleFloatValue(a_val);

        double b_val;
        b_col_val.getDoubleFloatValue(b_val);
        ASSERT(a_val == b_val, "a_val %f b_val %f", a_val, b_val);
      } break;
      case LindormContest::COLUMN_TYPE_UNINITIALIZED:
        break;
    }
  }

  return true;
}

char* randstr(char* str, const int len) {
  srand(time(NULL));
  int i;
  for (i = 0; i < len; ++i) {
    switch ((rand() % 3)) {
      case 1:
        str[i] = 'A' + rand() % 26;
        break;
      case 2:
        str[i] = 'a' + rand() % 26;
        break;
      default:
        str[i] = '0' + rand() % 10;
        break;
    }
  }
  str[++i] = '\0';
  return str;
}

std::string RandStr() {
  srand(time(NULL));
  int len = rand() % 20 + 1;
  char buf[128];
  randstr(buf, len);
  return std::string(buf);
}

void prepare_data() {
  LOG_INFO("start prepare data...");

  std::vector<std::thread> threads;

  int thread_num = 8;
  int vin_per_thread = kVinNum / 8;

  for (int thr = 0; thr < thread_num; thr++) {
    threads.emplace_back(
      [vin_per_thread](int idx) {
        int vin = vin_per_thread * idx;
        for (int i = vin; i < vin + vin_per_thread; i++) {
          for (int j = 0; j < kRowsPerVin; j++) {
            auto& row = rows[i][j];
            std::string vin = std::to_string(i);
            memset(row.vin.vin, 0, LindormContest::VIN_LENGTH);
            memcpy(row.vin.vin, vin.c_str(), vin.size());
            row.timestamp = j;
            // build columns
            for (int k = 0; k < LindormContest::kColumnNum; k++) {
              std::string col_name = "column_" + std::to_string(k);
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
                  // std::string s = RandStr();
                  std::string s = std::string(30, (j % 26) + 'a');
                  LindormContest::ColumnValue col(s);
                  EXPECT(col.columnType == LindormContest::COLUMN_TYPE_STRING, "???");
                  row.columns.insert(std::make_pair(col_name, col));
                  break;
                }
              }
            }
          }
        }
      },
      thr);
  }

  for (auto& th : threads) {
    th.join();
  }

  LOG_INFO("prepare data finished!");
}

void parallel_upsert(LindormContest::TSDBEngine* engine) {
  LOG_INFO("start parallel write...");

  std::vector<std::thread> threads;
  const int thread_num = 8;
  const int per_thread_vin_num = kVinNum / thread_num;
  std::vector<int> is(kVinNum);
  std::vector<int> js(kRowsPerVin);
  for (int i = 0; i < kVinNum; i++) {
    is[i] = i;
  }
  for (int j = 0; j < kRowsPerVin; j++) {
    js[j] = j;
  }

  std::mt19937 rng(1);
  std::shuffle(is.begin(), is.end(), rng);
  std::shuffle(js.begin(), js.end(), rng);

  for (int t = 0; t < thread_num; t++) {
    threads.emplace_back(
      [&is, &js, engine](int th) {
        for (int j = 0; j < kRowsPerVin; j++) {
          for (int i = th * per_thread_vin_num; i < (th + 1) * per_thread_vin_num;) {
            LindormContest::WriteRequest wReq;
            wReq.tableName = "t1";
            for (int k = 0; i < (th + 1) * per_thread_vin_num && k < 10; k++, i++) {
              wReq.rows.push_back(rows[is[i]][js[j]]);
            }
            int ret = engine->write(wReq);
          }
        }
      },
      t);
  }

  for (auto& th : threads) {
    th.join();
  }

  dynamic_cast<LindormContest::TSDBEngineImpl*>(engine)->wait_write();

  LOG_INFO("parallel write end");
}

void parallel_test_latest(LindormContest::TSDBEngine* engine) {
  LOG_INFO("start parallel test latest...");
  // validate executeLatestQuery
  std::vector<std::thread> threads;
  const int thread_num = 1;
  const int per_thread_vin_num = kVinNum / thread_num;

  for (int t = 0; t < thread_num; t++) {
    threads.emplace_back([=]() {
      for (int i = t * per_thread_vin_num; i < (t + 1) * per_thread_vin_num; i++) {
        LindormContest::LatestQueryRequest pReadReq;
        std::vector<LindormContest::Row> pReadRes;
        pReadReq.tableName = "t1";
        for (int j = 0; j < LindormContest::kColumnNum; j++) {
          std::string col_name = "column_" + std::to_string(j);
          pReadReq.requestedColumns.insert(col_name);
        }
        LindormContest::Vin vin;
        memcpy(vin.vin, rows[i][0].vin.vin, LindormContest::VIN_LENGTH);
        pReadReq.vins.push_back(vin);
        engine->executeLatestQuery(pReadReq, pReadRes);
        ASSERT(pReadRes.size() == 1, "executeLatestQuery failed");
        auto row = pReadRes[0];
        ASSERT(RowEquals(row, rows[i][kRowsPerVin - 1]), "error");
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }

  LOG_INFO("parallel test latest PASS");
}

void parallel_test_time_range(LindormContest::TSDBEngine* engine) {
  LOG_INFO("start parallel test time range...");
  // validate time range
  std::vector<std::thread> threads;
  const int thread_num = 1;
  const int per_thread_vin_num = kVinNum / thread_num;

  for (int t = 0; t < thread_num; t++) {
    threads.emplace_back([=]() {
      for (int i = t * per_thread_vin_num; i < (t + 1) * per_thread_vin_num; i++) {
        LindormContest::TimeRangeQueryRequest trR;
        LindormContest::Vin vin;
        memcpy(vin.vin, rows[i][0].vin.vin, LindormContest::VIN_LENGTH);
        trR.vin = vin;
        trR.tableName = "t1";
        size_t res_cnt = (rand() % kRowsPerVin);
        trR.timeLowerBound = 0;
        trR.timeUpperBound = res_cnt;
        for (int j = 0; j < LindormContest::kColumnNum; j++) {
          std::string col_name = "column_" + std::to_string(j);
          trR.requestedColumns.insert(col_name);
        }
        std::vector<LindormContest::Row> trReadRes;
        int ret = engine->executeTimeRangeQuery(trR, trReadRes);
        std::sort(trReadRes.begin(), trReadRes.end());
        int64_t ts = 0;
        for (auto& r : trReadRes) {
          EXPECT(r.timestamp == ts,"r.timestamp %ld %ld", r.timestamp, ts);
          ts++; 
        }
        ASSERT(trReadRes.size() == res_cnt, "size = %zu", trReadRes.size());
        for (auto& row : trReadRes) {
          ASSERT(RowEquals(row, rows[i][row.timestamp % kRowsPerVin]), "not equal");
        }
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }

  LOG_INFO("parallel test time range PASS");
}

int main() {
  std::string dataPath = "/tmp/tsdb_test";
  clearTempFolder(dataPath);
  LindormContest::TSDBEngine* engine = new LindormContest::TSDBEngineImpl(dataPath);

  // connect
  int ret = engine->connect();
  LOG_ASSERT(ret == 0, "connect failed");

  // create table
  ret = createTable(engine);
  LOG_ASSERT(ret == 0, "create table failed");

  prepare_data();

  parallel_upsert(engine);

  parallel_test_latest(engine);
  parallel_test_time_range(engine);

  LOG_INFO("start shutdown...");
  engine->shutdown();
  delete engine;
  LOG_INFO("shutdown finished");

  LOG_INFO("start connect...");
  engine = new LindormContest::TSDBEngineImpl(dataPath);
  engine->connect();
  LOG_INFO("start connect finished");

  parallel_test_latest(engine);
  parallel_test_time_range(engine);

  engine->shutdown();

  LOG_INFO("PASS!!!");
}
