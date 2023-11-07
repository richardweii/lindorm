#include <malloc.h>
#include <stdio.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
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

#define ARR_NUM 256

void printDoubleBinary(double value) {
    uint64_t* ptr = (uint64_t*)&value;
    uint64_t bits = *ptr;

    for (int i = 63; i >= 0; i--) {
        uint64_t bit = (bits >> i) & 1;
        printf("%lu", bit);

        if (i == 63 || i == 52) {
            printf(" "); // 添加空格以分隔符号位、阶码和尾数
        }
    }

    printf("\n");
}

template<typename T>
static int CalculateBitsRequired(T min, T max) {
  // 计算绝对值范围的大小
  T range = std::max(std::abs(min), std::abs(max));
  
  // 使用对数函数来估算所需的比特数，向上取整
  int bitsRequired = static_cast<int>(std::ceil(std::log2(range + 1)));
  
  // 考虑符号位，至少需要1个比特
  return bitsRequired + 1;
}

int main() {
  int a = CalculateBitsRequired(0, 7);
  printf("%d\n", a);
  int arr[ARR_NUM];
  srandom(time(NULL));
  int min = INT32_MAX;
  int max = INT32_MIN;
  for (int i = 0; i < ARR_NUM; i++) {
    arr[i] = i;
    if (i > 100 && i < 150) arr[i] = -i - LindormContest::MAX_DIFF_VAL - 1;
    if (i >= 150) arr[i] = i + LindormContest::MAX_DIFF_VAL + 1 + LindormContest::MAX_DIFF_VAL + 1;
    arr[i] += rand() % 100;
    if (arr[i] < min) min = arr[i];
    if (arr[i] > max) max = arr[i];
    if (i == ARR_NUM - 1) arr[i] = 234124123;
  }

  int origin_sz = sizeof(int) * ARR_NUM;
  char* compress_buf;
  uint64_t compress_size;
  LindormContest::TArrCompress(arr, ARR_NUM, min, max, 4, compress_buf, compress_size, LindormContest::MyColumnType::MyInt32);
  LOG_INFO("my compress ratoo %f\n", compress_size * 1.0 / origin_sz);
  char buf[origin_sz];
  int zstd_size = LindormContest::ZSTDCompress((const char *)arr, origin_sz, buf, origin_sz);
  LOG_INFO("zstd compress ratoo %f\n", zstd_size * 1.0 / origin_sz);


  int arr2[ARR_NUM];
  int cnt;
  LindormContest::TArrDeCompress(arr2, cnt, sizeof(int)*ARR_NUM, compress_buf, compress_size, LindormContest::MyColumnType::MyInt32);
  LOG_ASSERT(cnt == ARR_NUM, "cnt = %d", cnt);
  for (int i = 0; i < cnt; i++) {
    LOG_ASSERT(arr[i] == arr2[i], "i %d expect %d, but got %d", i, arr[i], arr2[i]);
  }
  // char* compress_buf;
  // int cnt;
  // uint64_t compress_size;
  // int64_t arr3[ARR_NUM];
  // int64_t arr4[ARR_NUM];
  // int64_t min3 = INT64_MAX;
  // int64_t max3 = INT64_MIN;
  // for (int i = 0; i < ARR_NUM; i++) {
  //   arr3[i] = 1153593 + rand() % 127;
  //   if (arr3[i] < min3) min3 = arr3[i];
  //   if (arr3[i] > max3) max3 = arr3[i];
  // }
  // LindormContest::TArrCompress(arr3, ARR_NUM, min3, max3, compress_buf, compress_size, LindormContest::MyColumnType::MyDouble);
  // auto ret = LindormContest::TArrDeCompress(arr4, cnt, sizeof(int64_t) * ARR_NUM, compress_buf, compress_size, LindormContest::MyColumnType::MyDouble);
  // for (int i = 0; i < 100; i++) {
  //   if (arr3[i] != arr4[i]) {
  //     assert(0);
  //   }
  // }

  // for (int i = 0; i < 100; i++) {
  //   double d = 25717 + rand() % 100 + ((rand() % 10000000000)*1.0 + 10000000000) / 10000000000;
  //   printDoubleBinary(d);
  // }

  // double double_arr[ARR_NUM];
  // double min;
  // double max;
  // srandom(time(NULL));
  // for (int i = 0; i < ARR_NUM; i++) {
  //   double_arr[i] = 8860312 + rand() % 100 + ((rand() % 10000000000)*1.0 + 10000000000) / 10000000000;
  //   if (i == 0) {
  //     min = double_arr[i];
  //     max = double_arr[i];
  //   } else {
  //     if (min > double_arr[i]) min = double_arr[i];
  //     if (max < double_arr[i]) max = double_arr[i];
  //   }
  // }

  // int origin_sz = sizeof(double) * ARR_NUM;
  // char* compress_buf;
  // uint64_t compress_size;
  // LindormContest::DoubleArrCompress(double_arr, ARR_NUM, min, max, compress_buf, compress_size);
  // LOG_INFO("my compress ratio %f\n", compress_size * 1.0 / origin_sz);

  // double arr2[ARR_NUM];
  // int cnt;
  // LindormContest::DoubleArrDeCompress(arr2, cnt, origin_sz, compress_buf, compress_size);
  // LOG_ASSERT(cnt == ARR_NUM, "cnt %d", cnt);
  // for (int i = 0; i < ARR_NUM; i++) {
  //   LOG_ASSERT(double_arr[i] == arr2[i], "%f <--> %f", double_arr[i], arr2[i]);
  // }

  // double ts_arr[ARR_NUM];
  // double min;
  // double max;
  // srandom(time(NULL));
  // ts_arr[0] = 12323.12123;
  // min = ts_arr[0];
  // max = ts_arr[0];
  // for (int i = 1; i < ARR_NUM; i++) {
  //   ts_arr[i] = ts_arr[i-1] + 1000;
  //   if (min > ts_arr[i]) min = ts_arr[i];
  //   if (max < ts_arr[i]) max = ts_arr[i];
  // }

  // int origin_sz = sizeof(double) * ARR_NUM;
  // char* compress_buf;
  // uint64_t compress_size;
  // LindormContest::TArrCompress(ts_arr, ARR_NUM, min, max, compress_buf, compress_size, LindormContest::MyColumnType::MyDouble);
  // LOG_INFO("my compress ratio %f\n", compress_size * 1.0 / origin_sz);

  // double arr2[ARR_NUM];
  // int cnt;
  // LindormContest::TArrDeCompress(arr2, cnt, sizeof(double) * ARR_NUM, compress_buf, compress_size, LindormContest::MyColumnType::MyDouble);
  // LOG_ASSERT(cnt == ARR_NUM, "cnt %d", cnt);
  // for (int i = 0; i < ARR_NUM; i++) {
  //   LOG_ASSERT(ts_arr[i] == arr2[i], "%ld <--> %ld", ts_arr[i], arr2[i]);
  // }

  // LOG_INFO("PASS");
  // return 0;
}
