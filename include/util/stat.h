#pragma once

#include <atomic>
#include <chrono>
#include "struct/Row.h"

namespace LindormContest {

#define RDMA_MR_FLAG (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE)
#define TIME_NOW (std::chrono::high_resolution_clock::now())
#define TIME_DURATION_US(START, END) (std::chrono::duration_cast<std::chrono::microseconds>((END) - (START)).count())
#define TO_MS(_time) (_time / 1000)

extern std::atomic<int64_t> write_cnt;
extern std::atomic<int64_t> latest_query_cnt;
extern std::atomic<int64_t> time_range_query_cnt;

extern std::atomic<int64_t> latest_query_time;
extern std::atomic<int64_t> write_time;
extern std::atomic<int64_t> time_range_query_time;

extern std::atomic<int64_t> tr_memtable_blk_query_cnt;
extern std::atomic<int64_t> tr_disk_blk_query_cnt;

extern std::atomic<int64_t> origin_szs[];
extern std::atomic<int64_t> compress_szs[];

#define ENABLE_STAT
#ifdef ENABLE_STAT
#define RECORD_FETCH_ADD(num, delta) (num.fetch_add(delta))
#define RECORD_ARR_FETCH_ADD(nums, idx, delta) (nums[idx].fetch_add(delta))
#else
#define RECORD_FETCH_ADD(num, delta)
#define RECORD_ARR_FETCH_ADD(nums, idx, delta)
#endif

void print_summary(ColumnType *columnsType, std::string *columnsName);
void print_row(const Row &row, uint16_t vid);
void print_memory_usage();
}