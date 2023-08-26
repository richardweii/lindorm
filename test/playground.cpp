#include <fcntl.h>
#include <malloc.h>
#include <stdio.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include "Hasher.hpp"
#include "TSDBEngineImpl.h"
#include "common.h"
#include "compress.h"
#include "io/file.h"
#include "struct/ColumnValue.h"
#include "struct/Vin.h"
#include "test.hpp"
#include "util/libaio.h"
#include "util/logging.h"
#include "util/util.h"

#define MAX_EVENT 10
#define BUF_LEN 512

void callback(io_context_t ctx, struct iocb* iocb, long res, long res2) { printf("test call\n"); }

int play_libaio() {
  int fd = open("/home/wjp/test.txt", O_RDWR | O_DIRECT, 0);
  io_context_t io_context;
  struct iocb io, *p = &io;
  struct io_event event[MAX_EVENT];
  void* buf;
  posix_memalign(&buf, 512, 512);
  memset(buf, 0, BUF_LEN);
  strcpy((char*)buf, "hello libaio");
  memset(&io_context, 0, sizeof(io_context));

  if (io_setup(10, &io_context)) {
    printf("io_setup error");
    return 0;
  }
  if (fd < 0) {
    printf("open file error");
    return 0;
  }
  io_prep_pwrite(&io, fd, buf, 512, 0);
  io_set_callback(&io, callback);
  if (io_submit(io_context, 1, &p) < 0) {
    printf("io_submit error");
    return 0;
  }

  int num = io_getevents(io_context, 1, MAX_EVENT, event, NULL);
  for (int i = 0; i < num; i++) {
    io_callback_t io_callback = (io_callback_t)event[i].data;
    // LOG_INFO("write %d", event[i].res);
    LOG_ASSERT(event[i].res > 0, "res %zu", event[i].res);
    io_callback(io_context, event[i].obj, event[i].res, event[i].res2);
  }

  close(fd);
  return 0;
}

void play_zstd() {
  srandom(time(NULL));
  constexpr int CNT = 5000;
  constexpr int MAX = 23421;
  int nums[CNT];
  for (int i = 1; i < CNT; i++) {
    nums[i] = nums[i - 1] + (rand() % 30 + 1);
  }

  int origin_sz = sizeof(int) * CNT;
  int compress_buf_sz = LindormContest::max_dest_size_func(origin_sz);
  char* compress_buf = new char[compress_buf_sz];
  int compress_sz = LindormContest::compress_func((char*)nums, origin_sz, compress_buf, compress_buf_sz);

  LOG_INFO("origin_sz %d compress_sz %d, compress_ratio = %f", origin_sz, compress_sz,
           (compress_sz * 1.0) / (origin_sz * 1.0));
}

const size_t ARRAY_SIZE = 5; // Size of the arrays

void play_quick_sort() {
  uint16_t vid[ARRAY_SIZE] = {2, 1, 3, 1, 2};
  int64_t ts[ARRAY_SIZE] = {5, 4, 3, 6, 2};
  uint16_t idx[ARRAY_SIZE] = {3, 1, 4, 2, 5};

  LindormContest::quickSort(vid, ts, idx, 0, ARRAY_SIZE - 1);

  std::cout << "Sorted vid:" << std::endl;
  for (int v : vid) {
    std::cout << v << " ";
  }
  std::cout << std::endl;

  std::cout << "Sorted ts:" << std::endl;
  for (int t : ts) {
    std::cout << t << " ";
  }
  std::cout << std::endl;

  std::cout << "Sorted idx:" << std::endl;
  for (int i : idx) {
    std::cout << i << " ";
  }
  std::cout << std::endl;
}

void play_binary_serch() {
  uint16_t vid[] = {1, 1, 1, 2, 2, 2, 2, 2, 3};
  int64_t ts[] = {2, 4, 6, 1, 2, 3, 4, 5, 3};
  uint16_t idx[] = {0, 1, 2, 4, 3, 5, 6, 7, 8};
  int size = sizeof(vid) / sizeof(vid[0]);

  int target_vid = 2;
  int ts_lower = 2;
  int ts_upper = 5;

  std::vector<uint16_t> found_indices;
  std::vector<int64_t> tss;
  LindormContest::findMatchingIndices(vid, ts, idx, size, target_vid, ts_lower, ts_upper, found_indices, tss);

  if (!found_indices.empty()) {
    std::cout << "Found at indices: ";
    for (size_t index : found_indices) {
      std::cout << index << " ";
    }
    std::cout << std::endl;
  } else {
    std::cout << "Not found." << std::endl;
  }
}

int main() {
  play_binary_serch();
  return 0;
}
