#include "util/stat.h"

#include <unistd.h>

#include <atomic>

#include "TSDBEngineImpl.h"
#include "common.h"
#include "util/logging.h"

namespace LindormContest {

std::atomic<int64_t> write_cnt{0};
std::atomic<int64_t> latest_query_cnt{0};
std::atomic<int64_t> time_range_query_cnt{0};
std::atomic<int64_t> agg_query_cnt{0};
std::atomic<int64_t> downsample_query_cnt{0};

std::atomic<int64_t> write_time{0}; // 单位为ms
std::atomic<int64_t> latest_query_time{0};
std::atomic<int64_t> time_range_query_time{0};

std::atomic<int64_t> tr_memtable_blk_query_cnt{0}; // time range遍历的memtable中的block总数
std::atomic<int64_t> disk_blk_access_cnt{0};       // time range遍历的磁盘块的总数

std::atomic<int64_t> origin_szs[kColumnNum + kExtraColNum];
std::atomic<int64_t> compress_szs[kColumnNum + kExtraColNum];

std::atomic<int64_t> cache_hit{0};
std::atomic<int64_t> cache_cnt{0};
std::atomic<int64_t> data_wait_cnt{0};
std::atomic<int64_t> lru_wait_cnt{0};
std::atomic<int64_t> write_wait_cnt{0};
std::atomic<int64_t> flush_wait_cnt{0};

std::atomic<int64_t> alloc_time{0};
std::atomic<int64_t> wait_aio{0};
std::atomic<int64_t> print_row_cnt{0};

std::atomic<int64_t> write_phase_sync{0};

std::string types[] = {
  "NULL",
  "string",
  "Integer",
  "double",
};

void print_file_summary(ColumnType* columnsType, std::string* columnsName) {
  LOG_INFO("*********FILE SUMMARY*********");
  LOG_INFO("write_cnt: %ld, write wait cnt: %ld, flush wait cnt: %ld", write_cnt.load(), write_wait_cnt.load(),
           flush_wait_cnt.load());
  for (int i = 0; i < kColumnNum; i++) {
    LOG_INFO("col %d col_name %s, col_type %s, origin_sz %ld MB, compress_sz %ld MB compress rate is %f", i,
             columnsName[i].c_str(), types[columnsType[i]].c_str(), origin_szs[i].load() / MB,
             compress_szs[i].load() / MB, (compress_szs[i] * 1.0) / (origin_szs[i] * 1.0));
  }
  for (int i = kColumnNum; i < kColumnNum + kExtraColNum; i++) {
    LOG_INFO("col %d, origin_sz %ld MB, compress_sz %ld MB compress rate is %f", i, origin_szs[i].load() / MB,
             compress_szs[i].load() / MB, (compress_szs[i] * 1.0) / (origin_szs[i] * 1.0));
  }
}

void print_performance_statistic() {
  print_memory_usage();
  LOG_INFO("*********PERFORMANCE STAT SUMMARY*********");
  LOG_INFO(
    "\n====================latest_query_cnt: %ld\n====================time_range_query_cnt: "
    "%ld\n====================agg_query_cnt: %ld\n====================downsample_query_cnt: "
    "%ld\n====================ReadCache Hit: %ld, MISS: "
    "%ld, HitRate: %lf\n====================ReadCache data wait :%ld, lru wait %ld\n====================Alloc time: "
    "%ld\n====================wait aio :%ld\n===================disk_blk_access_cnt :%ld",
    latest_query_cnt.load(), time_range_query_cnt.load(), agg_query_cnt.load(), downsample_query_cnt.load(),
    cache_hit.load(), cache_cnt.load() - cache_hit.load(), cache_hit.load() * 1.0 / cache_cnt.load(),
    data_wait_cnt.load(), lru_wait_cnt.load(), alloc_time.load(), wait_aio.load(), disk_blk_access_cnt.load());
  LOG_INFO("*******************************************");
  fflush(stdout);
}

void print_row(const Row& row, uint16_t vid) {
  printf("@@@@@@@@@@@ vid %d, ts: %ld | ", vid, row.timestamp);
  for (auto& col : row.columns) {
    printf("[%s] :", col.first.c_str());
    auto col_val = col.second;
    switch (col.second.getColumnType()) {
      case COLUMN_TYPE_STRING: {
        LOG_ASSERT(col_val.columnType == LindormContest::COLUMN_TYPE_STRING, "COLUMN_TYPE_STRING");
        std::pair<int32_t, const char*> pair;
        col_val.getStringValue(pair);
        std::string str(pair.second, pair.first);
        printf("%s ", str.c_str());
      } break;
      case COLUMN_TYPE_INTEGER: {
        LOG_ASSERT(col_val.columnType == LindormContest::COLUMN_TYPE_INTEGER, "COLUMN_TYPE_INTEGER");
        int val;
        col_val.getIntegerValue(val);
        printf("%d ", val);
      } break;
      case COLUMN_TYPE_DOUBLE_FLOAT: {
        LOG_ASSERT(col_val.columnType == LindormContest::COLUMN_TYPE_DOUBLE_FLOAT, "COLUMN_TYPE_DOUBLE_FLOAT");
        double val;
        col_val.getDoubleFloatValue(val);
        printf("%f ", val);
      } break;
      case COLUMN_TYPE_UNINITIALIZED:
        break;
    }
    printf(" | ");
  }
  printf("@@@@@@@@@@@@\n");
  fflush(stdout);
}

#define VMRSS_LINE 22
static inline float GetMemoryUsage(int pid) {
  char file_name[64] = {0};
  FILE* fd;
  char line_buff[512] = {0};
  sprintf(file_name, "/proc/%d/status", pid);

  fd = fopen(file_name, "r");
  if (nullptr == fd) return 0;

  char name[64];
  int vmrss = 0;
  for (int i = 0; i < VMRSS_LINE - 1; i++) fgets(line_buff, sizeof(line_buff), fd);

  fgets(line_buff, sizeof(line_buff), fd);
  sscanf(line_buff, "%s %d", name, &vmrss);
  fclose(fd);

  // cnvert VmRSS from KB to MB
  return vmrss / 1024.0;
}

void print_memory_usage() {
  int pid = getpid();
  float num = GetMemoryUsage(pid);

  LOG_INFO("use %f MB memory", num);
}

void clear_performance_statistic() {
  disk_blk_access_cnt = 0; // time range遍历的磁盘块的总数
  cache_hit = 0;
  cache_cnt = 0;
  data_wait_cnt = 0;
  lru_wait_cnt = 0;
  alloc_time = 0;
};
} // namespace LindormContest
