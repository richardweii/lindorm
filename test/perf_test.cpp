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
#include "util/stat.h"

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
static constexpr int kRowsPerVin = 10 * 60;

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

// 让测试程序只使用前8个核心
void bind_cores() {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  for (int i = 0; i < 8; i++) {
    CPU_SET(i, &cpuset); // 设置要使用的核心编号
  }
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}

LindormContest::Row get_row(int v, int r) {
  LindormContest::Row row;
  std::string vin = std::to_string(v);
  memset(row.vin.vin, 0, LindormContest::VIN_LENGTH);
  memcpy(row.vin.vin, vin.c_str(), vin.size());
  row.timestamp = r;
  // build columns
  for (int k = 0; k < LindormContest::kColumnNum; k++) {
    std::string col_name = "column_" + std::to_string(k);
    switch (types[k]) {
      case 0: {
        LindormContest::ColumnValue col(r);
        row.columns.insert(std::make_pair(col_name, col));
        break;
      }
      case 1: {
        LindormContest::ColumnValue col(r * 1.0);
        row.columns.insert(std::make_pair(col_name, col));
        break;
      }
      case 2: {
        // std::string s = RandStr();
        std::string s = std::string(30, (r % 26) + 'a');
        LindormContest::ColumnValue col(s);
        EXPECT(col.columnType == LindormContest::COLUMN_TYPE_STRING, "???");
        row.columns.insert(std::make_pair(col_name, col));
        break;
      }
    }
  }
  return row;
}

void parallel_upsert(LindormContest::TSDBEngine* engine) {
  LOG_INFO("start parallel write...");

  std::vector<std::thread> threads;
  const int thread_num = 8;
  const int per_thread_vin_num = (kVinNum + thread_num - 1) / thread_num;
  std::vector<int> is(kVinNum);
  std::vector<int> js(kRowsPerVin);
  for (int i = 0; i < kVinNum; i++) {
    is[i] = i;
  }
  for (int j = 0; j < kRowsPerVin; j++) {
    js[j] = j;
  }

  // std::mt19937 rng(1);
  // std::shuffle(is.begin(), is.end(), rng);
  // std::shuffle(js.begin(), js.end(), rng);

  for (int t = 0; t < thread_num; t++) {
    threads.emplace_back(
      [&is, &js, engine](int th) {
        bind_cores();
        for (int j = 0; j < kRowsPerVin; j++) {
          for (int i = th * per_thread_vin_num; i < (th + 1) * per_thread_vin_num && i < kVinNum;) {
            LindormContest::WriteRequest wReq;
            wReq.tableName = "t1";
            for (int k = 0; i < (th + 1) * per_thread_vin_num && k < 10; k++, i++) {
              wReq.rows.push_back(get_row(is[i], js[j]));
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
  const int thread_num = 16;
  const int per_thread_vin_num = (kVinNum + thread_num - 1) / thread_num;

  Progress pgrs(kVinNum);
  for (int t = 0; t < thread_num; t++) {
    threads.emplace_back([t, engine, &pgrs]() {
      bind_cores();
      for (int i = t * per_thread_vin_num; i < (t + 1) * per_thread_vin_num && i < kVinNum; i++) {
        LindormContest::LatestQueryRequest pReadReq;
        std::vector<LindormContest::Row> pReadRes;
        pReadReq.tableName = "t1";
        for (int j = 0; j < LindormContest::kColumnNum; j++) {
          std::string col_name = "column_" + std::to_string(j);
          pReadReq.requestedColumns.insert(col_name);
        }
        LindormContest::Vin vin;
        memcpy(vin.vin, get_row(i, 0).vin.vin, LindormContest::VIN_LENGTH);
        pReadReq.vins.push_back(vin);
        engine->executeLatestQuery(pReadReq, pReadRes);
        ASSERT(pReadRes.size() == 1, "executeLatestQuery failed");
        auto row = pReadRes[0];
        ASSERT(RowEquals(row, get_row(i, kRowsPerVin - 1)), "error");
        pgrs.wg()->Done();
      }
    });
  }

  pgrs.Wait();
  pgrs.Finish();

  for (auto& th : threads) {
    th.join();
  }

  LOG_INFO("parallel test latest PASS");
}

void parallel_test_time_range(LindormContest::TSDBEngine* engine) {
  LOG_INFO("start parallel test time range...");
  // validate time range
  std::vector<std::thread> threads;
  const int thread_num = 16;
  const int per_thread_vin_num = (kVinNum + thread_num - 1) / thread_num;

  Progress pgrs(kVinNum);
  for (int t = 0; t < thread_num; t++) {
    threads.emplace_back([t, engine, &pgrs]() {
      bind_cores();
      for (int i = t * per_thread_vin_num; i < (t + 1) * per_thread_vin_num && i < kVinNum; i++) {
        LindormContest::TimeRangeQueryRequest trR;
        LindormContest::Vin vin;
        memcpy(vin.vin, get_row(i, 0).vin.vin, LindormContest::VIN_LENGTH);
        trR.vin = vin;
        trR.tableName = "t1";
        auto start = (rand() % (3 * kRowsPerVin / 4));
        size_t res_cnt = (rand() % (kRowsPerVin / 4));
        trR.timeLowerBound = start;
        trR.timeUpperBound = start + res_cnt;
        // for (int j = 0; j < LindormContest::kColumnNum; j++) {
        //   std::string col_name = "column_" + std::to_string(j);
        //   trR.requestedColumns.insert(col_name);
        // }
        std::vector<LindormContest::Row> trReadRes;
        int ret = engine->executeTimeRangeQuery(trR, trReadRes);
        std::sort(trReadRes.begin(), trReadRes.end());
        int64_t ts = start;
        for (auto& r : trReadRes) {
          EXPECT(r.timestamp == ts, "r.timestamp %ld %ld", r.timestamp, ts);
          ts++;
        }
        ASSERT(trReadRes.size() == res_cnt, "size = %zu", trReadRes.size());
        for (auto& row : trReadRes) {
          ASSERT(RowEquals(row, get_row(i, row.timestamp % kRowsPerVin)), "not equal");
        }
        pgrs.wg()->Done();
      }
    });
  }

  pgrs.Wait();
  pgrs.Finish();
  for (auto& th : threads) {
    th.join();
  }

  LOG_INFO("parallel test time range PASS");
}

void parallel_agg(LindormContest::TSDBEngine* engine, LindormContest::Aggregator op, int type) {
  LOG_INFO("start parallel agg op %d, type %d", op, type);
  // validate time range
  std::vector<std::thread> threads;
  const int thread_num = 16;
  const int per_thread_vin_num = (kVinNum + thread_num - 1) / thread_num;

  Progress pgrs(kVinNum);
  for (int t = 0; t < thread_num; t++) {
    threads.emplace_back([t, engine, &pgrs, type, op]() {
      bind_cores();
      for (int i = t * per_thread_vin_num; i < (t + 1) * per_thread_vin_num && i < kVinNum; i++) {
        LindormContest::TimeRangeAggregationRequest tAG;
        LindormContest::Vin vin;
        memcpy(vin.vin, get_row(i, 0).vin.vin, LindormContest::VIN_LENGTH);
        tAG.aggregator = op;
        tAG.vin = vin;
        tAG.tableName = "t1";
        auto start = (rand() % (3 * kRowsPerVin / 4));
        size_t res_cnt = (rand() % (kRowsPerVin / 4)) + 1;
        tAG.timeLowerBound = start;
        tAG.timeUpperBound = start + res_cnt;
        std::string col_name;
        if (type == 0) {
          // int
          col_name = "column_" + std::to_string(0);
          tAG.columnName = col_name;
        } else {
          // double
          col_name = "column_" + std::to_string(2);
          tAG.columnName = col_name;
        }

        std::vector<LindormContest::Row> trReadRes;
        int ret = engine->executeAggregateQuery(tAG, trReadRes);
        ASSERT(trReadRes.size() == 1, "size = %zu", trReadRes.size());
        auto& row = trReadRes[0];

        if (op == LindormContest::MAX) {
          if (type == 0) {
            int got;
            row.columns[col_name].getIntegerValue(got);
            int expected = start + res_cnt - 1;
            ASSERT(got == expected, "expected %d, but got %d", expected, got);
            ASSERT(row.timestamp == start, "expected ts %d, but got %ld", start, row.timestamp);
            ASSERT(::memcmp(row.vin.vin, get_row(i, 0).vin.vin, LindormContest::VIN_LENGTH) == 0, "vin not equal");
          } else if (type == 1) {
            double got;
            row.columns[col_name].getDoubleFloatValue(got);
            double expected = 1.0 * (start + res_cnt - 1);
            ASSERT(got == expected, "expected %f, but got %f", expected, got);
            ASSERT(row.timestamp == start, "expected ts %d, but got %ld", start, row.timestamp);
            ASSERT(::memcmp(row.vin.vin, get_row(i, 0).vin.vin, LindormContest::VIN_LENGTH) == 0, "vin not equal");
          }
        } else if (op == LindormContest::AVG) {
          double got;
          row.columns[col_name].getDoubleFloatValue(got);

          double expected = (start + start + res_cnt - 1) * 1.0 / 2;
          ASSERT(got == expected, "expected %lf, but got %lf", expected, got);
          ASSERT(row.timestamp == start, "expected ts %d, but got %ld", start, row.timestamp);
          ASSERT(::memcmp(row.vin.vin, get_row(i, 0).vin.vin, LindormContest::VIN_LENGTH) == 0, "vin not equal");
        }
        pgrs.wg()->Done();
      }
    });
  }

  pgrs.Wait();
  pgrs.Finish();
  for (auto& th : threads) {
    th.join();
  }

  LOG_INFO("parallel test time range PASS");
}

void parallel_downsample(LindormContest::TSDBEngine* engine, LindormContest::Aggregator op, int type, int cmp_t) {
  LOG_INFO("start parallel downsample op %d, type %d cmp %d", op, type, cmp_t);
  // validate time range
  std::vector<std::thread> threads;
  const int thread_num = 16;
  const int per_thread_vin_num = (kVinNum + thread_num - 1) / thread_num;

  Progress pgrs(kVinNum);
  for (int t = 0; t < thread_num; t++) {
    threads.emplace_back([t, engine, &pgrs, type, op, cmp_t]() {
      bind_cores();
      for (int i = t * per_thread_vin_num; i < (t + 1) * per_thread_vin_num && i < kVinNum; i++) {
        LindormContest::TimeRangeDownsampleRequest tRD;
        LindormContest::Vin vin;
        memcpy(vin.vin, get_row(i, 0).vin.vin, LindormContest::VIN_LENGTH);
        tRD.aggregator = op;
        tRD.vin = vin;
        tRD.tableName = "t1";
        tRD.timeLowerBound = 0;
        tRD.timeUpperBound = kRowsPerVin;
        tRD.interval = 10;
        std::string col_name;
        if (type == 0) {
          // int
          col_name = "column_" + std::to_string(0);
          tRD.columnName = col_name;
        } else {
          // double
          col_name = "column_" + std::to_string(2);
          tRD.columnName = col_name;
        }

        LindormContest::CompareExpression cmp;
        if (cmp_t == 0) {
          // EQUAL
          cmp.compareOp = LindormContest::EQUAL;
        } else {
          // GREATER
          cmp.compareOp = LindormContest::GREATER;
        }

        int filter_val = rand() % kRowsPerVin;
        if (type == 0) {
          cmp.value = LindormContest::ColumnValue(filter_val);
        } else {
          cmp.value = LindormContest::ColumnValue(filter_val * 1.0);
        }
        tRD.columnFilter = cmp;

        std::vector<LindormContest::Row> trReadRes;
        int ret = engine->executeDownsampleQuery(tRD, trReadRes);
        ASSERT(trReadRes.size() == kRowsPerVin / 10, "size = %zu", trReadRes.size());

        std::sort(trReadRes.begin(), trReadRes.end());
        if (op == LindormContest::MAX) {
          if (cmp_t == LindormContest::GREATER) {
            if (type == 0) {
              for (int j = 0; j < trReadRes.size(); j++) {
                auto& r = trReadRes[j];
                int got;
                r.columns.at(col_name).getIntegerValue(got);
                int expected = 10 * (j + 1) - 1;
                expected = expected > filter_val ? expected : LindormContest::kIntNan;
                ASSERT(got == expected, "expect %d, but got %d", expected, got);
                ASSERT(r.timestamp == j * 10, "expected ts %d, but got %ld", j * 10, r.timestamp);
                ASSERT(::memcmp(r.vin.vin, get_row(i, 0).vin.vin, LindormContest::VIN_LENGTH) == 0, "vin not equal");
              }
            } else if (type == 1) {
              for (int j = 0; j < trReadRes.size(); j++) {
                auto& r = trReadRes[j];
                double got;
                r.columns.at(col_name).getDoubleFloatValue(got);
                double expected = (10 * (j + 1) - 1) * 1.0;
                expected = expected > filter_val ? expected : LindormContest::kDoubleNan;
                ASSERT(got == expected, "expect %lf, but got %lf", expected, got);
                ASSERT(r.timestamp == j * 10, "expected ts %d, but got %ld", j * 10, r.timestamp);
                ASSERT(::memcmp(r.vin.vin, get_row(i, 0).vin.vin, LindormContest::VIN_LENGTH) == 0, "vin not equal");
              }
            }
          } else if (cmp_t == LindormContest::EQUAL) {
            if (type == 0) {
              for (int j = 0; j < trReadRes.size(); j++) {
                auto& r = trReadRes[j];
                int got;
                r.columns.at(col_name).getIntegerValue(got);
                int expected = LindormContest::kIntNan;
                if (10 * j <= filter_val && filter_val < 10 * (j + 1)) {
                  expected = filter_val;
                }
                ASSERT(got == expected, "expect %d, but got %d", expected, got);
                ASSERT(r.timestamp == j * 10, "expected ts %d, but got %ld", j * 10, r.timestamp);
                ASSERT(::memcmp(r.vin.vin, get_row(i, 0).vin.vin, LindormContest::VIN_LENGTH) == 0, "vin not equal");
              }
            } else if (type == 1) {
              for (int j = 0; j < trReadRes.size(); j++) {
                auto& r = trReadRes[j];
                double got;
                r.columns.at(col_name).getDoubleFloatValue(got);
                double expected = LindormContest::kDoubleNan;
                if (10 * j <= filter_val && filter_val < 10 * (j + 1)) {
                  expected = filter_val * 1.0;
                }
                ASSERT(got == expected, "expect %lf, but got %lf", expected, got);
                ASSERT(r.timestamp == j * 10, "expected ts %d, but got %ld", j * 10, r.timestamp);
                ASSERT(::memcmp(r.vin.vin, get_row(i, 0).vin.vin, LindormContest::VIN_LENGTH) == 0, "vin not equal");
              }
            }
          }

        } else if (op == LindormContest::AVG) {
          if (cmp_t == LindormContest::GREATER) {
            for (int j = 0; j < trReadRes.size(); j++) {
              auto& r = trReadRes[j];
              double got;
              r.columns.at(col_name).getDoubleFloatValue(got);
              double expected = LindormContest::kDoubleNan;
              if (filter_val < 10 * j) {
                expected = (10 * j + 10 * (j + 1) - 1) * 1.0 / 2;
              } else if (10 * j <= filter_val && filter_val < j * (10 + 1) - 1) {
                expected = (filter_val + 1 + 10 * (j + 1) - 1) * 1.0 / 2;
              }
              expected = expected > filter_val ? expected : LindormContest::kIntNan;
              ASSERT(got == expected, "expect %lf, but got %lf", expected, got);
              ASSERT(r.timestamp == j * 10, "expected ts %d, but got %ld", j * 10, r.timestamp);
              ASSERT(::memcmp(r.vin.vin, get_row(i, 0).vin.vin, LindormContest::VIN_LENGTH) == 0, "vin not equal");
            }
          } else if (cmp_t == LindormContest::EQUAL) {
            for (int j = 0; j < trReadRes.size(); j++) {
              auto& r = trReadRes[j];
              double got;
              r.columns.at(col_name).getDoubleFloatValue(got);
              double expected = LindormContest::kDoubleNan;
              if (10 * j <= filter_val && filter_val < 10 * (j + 1)) {
                expected = filter_val * 1.0;
              }
              ASSERT(got == expected, "expect %lf, but got %lf", expected, got);
              ASSERT(r.timestamp == j * 10, "expected ts %d, but got %ld", j * 10, r.timestamp);
              ASSERT(::memcmp(r.vin.vin, get_row(i, 0).vin.vin, LindormContest::VIN_LENGTH) == 0, "vin not equal");
            }
          }
        }
        pgrs.wg()->Done();
      }
    });
  }

  pgrs.Wait();
  pgrs.Finish();
  for (auto& th : threads) {
    th.join();
  }

  LOG_INFO("parallel test time range PASS");
}

int main(int argc, char** argv) {
  std::string dataPath = "/tmp/tsdb_test";
  bool create = argc == 1;
  if (create) {
    clearTempFolder(dataPath);
  }
  LindormContest::TSDBEngine* engine = nullptr;
  
  if (create) {
    engine = new LindormContest::TSDBEngineImpl(dataPath);

    // connect
    int ret = engine->connect();
    LOG_ASSERT(ret == 0, "connect failed");
    // create table
    ret = createTable(engine);
    LOG_ASSERT(ret == 0, "create table failed");
    {
      auto now = TIME_NOW;
      parallel_upsert(engine);
      auto now2 = TIME_NOW;
      LOG_INFO("============== [RESLUT] ================ Write Use :%ld ms", TIME_DURATION_US(now, now2) / 1000);
    }

    LOG_INFO("start shutdown...");
    engine->shutdown();
    delete engine;
    LOG_INFO("shutdown finished");
  }

  LOG_INFO("start connect...");
  engine = new LindormContest::TSDBEngineImpl(dataPath);
  engine->connect();
  LOG_INFO("start connect finished");

  {
    auto now = TIME_NOW;
    parallel_test_latest(engine);
    auto now2 = TIME_NOW;
    LOG_INFO("============== [RESLUT] ================ parallel_test_latest Use :%ld ms", TIME_DURATION_US(now, now2) / 1000);
  }
  {
    auto now = TIME_NOW;
    parallel_test_time_range(engine);
    auto now2 = TIME_NOW;
    LOG_INFO("============== [RESLUT] ================ parallel_test_time_range Use :%ld ms", TIME_DURATION_US(now, now2) / 1000);
  }
  {
    auto now = TIME_NOW;
    parallel_agg(engine, LindormContest::AVG, 0);
    parallel_agg(engine, LindormContest::AVG, 1);
    parallel_agg(engine, LindormContest::MAX, 0);
    parallel_agg(engine, LindormContest::MAX, 1);
    auto now2 = TIME_NOW;
    LOG_INFO("============== [RESLUT] ================ parallel_agg Use :%ld ms", TIME_DURATION_US(now, now2) / 1000);
  }

  {
    auto now = TIME_NOW;
    parallel_downsample(engine, LindormContest::AVG, 0, 0);
    parallel_downsample(engine, LindormContest::AVG, 0, 0);
    parallel_downsample(engine, LindormContest::AVG, 1, 0);
    parallel_downsample(engine, LindormContest::AVG, 1, 0);
    parallel_downsample(engine, LindormContest::MAX, 0, 1);
    parallel_downsample(engine, LindormContest::MAX, 0, 1);
    parallel_downsample(engine, LindormContest::MAX, 1, 1);
    parallel_downsample(engine, LindormContest::MAX, 1, 1);
    auto now2 = TIME_NOW;
    LOG_INFO("============== [RESLUT] ================ parallel_downsample Use :%ld ms", TIME_DURATION_US(now, now2) / 1000);
  }
  engine->shutdown();
  LOG_INFO("PASS!!!");
}
