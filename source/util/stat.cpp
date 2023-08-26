#include "util/stat.h"
#include "TSDBEngineImpl.h"
#include "common.h"
#include "util/logging.h"
#include <atomic>
#include <unistd.h>

namespace LindormContest {

std::atomic<int64_t> write_cnt{0};
std::atomic<int64_t> latest_query_cnt{0};
std::atomic<int64_t> time_range_query_cnt{0};

std::atomic<int64_t> write_time{0}; // 单位为ms
std::atomic<int64_t> latest_query_time{0};
std::atomic<int64_t> time_range_query_time{0};

std::atomic<int64_t> tr_memtable_blk_query_cnt{0}; // time range遍历的memtable中的block总数
std::atomic<int64_t> tr_disk_blk_query_cnt{0};     // time range遍历的磁盘块的总数

std::atomic<int64_t> origin_szs[kColumnNum + kExtraColNum];
std::atomic<int64_t> compress_szs[kColumnNum + kExtraColNum];
std::string types[] = {
    "NULL",
    "string",
    "Integer",
    "double",
};

void print_summary(ColumnType *columnsType, std::string *columnsName) {
  LOG_INFO("*********STAT SUMMARY*********");
  LOG_INFO("write_cnt: %ld", write_cnt.load());
  LOG_INFO("latest_query_cnt: %ld", latest_query_cnt.load());
  LOG_INFO("time_range_query_cnt: %ld", time_range_query_cnt.load());
  LOG_INFO("tr_memtable_blk_query_cnt: %ld", tr_memtable_blk_query_cnt.load());
  LOG_INFO("tr_disk_blk_query_cnt: %ld", tr_disk_blk_query_cnt.load());
  for (int i = 0; i < kColumnNum; i++) {
    LOG_INFO("col %d col_name %s, col_type %s, origin_sz %ld MB, compress_sz %ld MB compress rate is %f",
             i,
             columnsName[i].c_str(),
             types[columnsType[i]].c_str(),
             origin_szs[i].load() / MB,
             compress_szs[i].load() / MB,
             (compress_szs[i] * 1.0) / (origin_szs[i] * 1.0));
  }
  for (int i = kColumnNum; i < kColumnNum + kExtraColNum; i++) {
    LOG_INFO("col %d, origin_sz %ld MB, compress_sz %ld MB compress rate is %f",
             i,
             origin_szs[i].load() / MB,
             compress_szs[i].load() / MB,
             (compress_szs[i] * 1.0) / (origin_szs[i] * 1.0));
  }
}

void print_row(const Row &row, uint16_t vid) {
  printf("vid %d, ts: %ld | ", vid, row.timestamp);
  for (auto &col : row.columns) {
    printf("col_name %s type: %s ", col.first.c_str(), types[col.second.getColumnType()].c_str());
    printf("val : ");
    auto col_val = col.second;
    switch (col.second.getColumnType()) {
      case COLUMN_TYPE_STRING: {
        LOG_ASSERT(col_val.columnType == LindormContest::COLUMN_TYPE_STRING, "COLUMN_TYPE_STRING");
        std::pair<int32_t, const char *> pair;
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
      case COLUMN_TYPE_UNINITIALIZED: break;
    }
    printf(" | ");
  }
  printf("\n");
  fflush(stdout);
}

#define VMRSS_LINE 22
static inline float GetMemoryUsage(int pid) {
  char file_name[64] = {0};
  FILE *fd;
  char line_buff[512] = {0};
  sprintf(file_name, "/proc/%d/status", pid);

  fd = fopen(file_name, "r");
  if (nullptr == fd)
    return 0;

  char name[64];
  int vmrss = 0;
  for (int i = 0; i < VMRSS_LINE - 1; i++)
    fgets(line_buff, sizeof(line_buff), fd);

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

} // namespace LindormContest
