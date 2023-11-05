#pragma once

#include <malloc.h>
#include <cassert>
#include <cstdint>
#include <cstring>

#include "struct/ColumnValue.h"
#include "util/logging.h"
#include "util/lz4.h"
#include "util/mem_pool.h"
#include "util/stat.h"
#include "util/zstd.h"
#include "common.h"

#define OUT

namespace LindormContest {

enum class CompressType {
  INT_ALL_EQUALS, // int值全部相同
  INT_DIFFERENCE, // int差分
  ZSTD, // 走zstd
};

inline int ZSTDMaxDestSize(int inputSize) { return ZSTD_compressBound(inputSize); }

inline uint64_t ZSTDCompress(const char* data, uint64_t len, char* compress_buf, uint64_t compress_len) {
  RECORD_FETCH_ADD(zstd_compress_cnt, 1);
  return ZSTD_compress((void*)compress_buf, compress_len, (const void*)data, len, 3);
}

inline int ZSTDDeCompress(const char* src, char* dst, int compressedSize, int dstCapacity) {
  return ZSTD_decompress(dst, dstCapacity, src, compressedSize);
}

int IntAllEqualCompress(int int_arr[], int cnt, int min, int max, char* &buf, uint64_t &size);
int IntAllEqualDeCompress(int int_arr[], int &cnt, char* buf, uint64_t size);
int IntDiffCompress(int int_arr[], int cnt, int min, int max, char* &buf, uint64_t &size);
int IntDiffDeCompress(int int_arr[], int &cnt, char* buf, uint64_t size);
int TsDiffCompress(int64_t ts_arr[], int cnt, int64_t min, int64_t max, char* &buf, uint64_t &compress_size);
int TsDiffDeCompress(int64_t ts_arr[], int &cnt, char* buf, uint64_t compress_size);

/**
 * t_arr是需要压缩的泛型数组，cnt是数组的元素个数，min和max分别是数组的最大值和最小值
 * 
 * buf是输出的压缩后的buffer指针，size是压缩后的大小，col_type代表列类型，用于决定压缩规则
 */
template<typename T>
int TArrCompress(T t_arr[], int cnt, T min, T max, OUT char* &buf, OUT uint64_t &compress_size, MyColumnType col_type);

template<typename T>
int TArrDeCompress(OUT T t_arr[], OUT int &cnt, int origin_sz, char* compress_buf, uint64_t compress_size, MyColumnType col_type);

inline int StringArrCompress(std::string* data, uint16_t lens[], int cnt, OUT char* &buf, OUT uint64_t &compress_sz) {
  uint64_t offset;
  uint64_t writesz1 = cnt * sizeof(lens[0]);
  uint64_t writesz2 = data->size();
  uint64_t input_sz = writesz1 + writesz2;
  auto now = TIME_NOW;
  char* origin = reinterpret_cast<char*>(naive_alloc(input_sz));
  auto now2 = TIME_NOW;
  RECORD_FETCH_ADD(alloc_time, TIME_DURATION_US(now, now2));
  memcpy(origin, lens, writesz1);
  memcpy(origin + writesz1, data->c_str(), writesz2);
  uint64_t compress_buf_sz = ZSTDMaxDestSize(input_sz);
  auto now11 = TIME_NOW;
  char* compress_buf = reinterpret_cast<char*>(naive_alloc(compress_buf_sz));
  auto now111 = TIME_NOW;
  RECORD_FETCH_ADD(alloc_time, TIME_DURATION_US(now11, now111));

  compress_sz = ZSTDCompress(origin, input_sz, compress_buf, compress_buf_sz);
  naive_free(origin);
  buf = compress_buf;
  return 0;
}

inline int StringArrDeCompress(char* origin_buf, int origin_sz, char* compress_data, uint64_t compress_sz) {
  return ZSTDDeCompress(compress_data, origin_buf, compress_sz, origin_sz);
}

inline int DoubleArrCompress(double double_arr[], int cnt, double min, double max,
                              OUT char* &buf, OUT uint64_t& compress_sz) {
  int high_bits = 24; // 后面通过策略控制，目前暂定24比特
  int high_mask = 0xFFFFFF;
  int high[cnt];
  int high_min;
  int high_max;

  int low_bytes = 5;
  uint64_t buf_low_sz = low_bytes * cnt;
  uint64_t buf_sz = 8 * cnt + 8; // 一次性分配好，后面压缩的直接追加在后面就好了
  buf = reinterpret_cast<char*>(naive_alloc(buf_sz));
  for (int i = 0; i < cnt; i++) {
    // 提取double的高位比特
    uint64_t binaryRepresentation = *reinterpret_cast<uint64_t*>(&double_arr[i]);
    high[i] = (binaryRepresentation >> (64-high_bits)) & high_mask;
    memcpy(buf + low_bytes * i + 8, &double_arr[i], low_bytes);
    if (i == 0) {
      high_min = high[i];
      high_max = high[i];
    } else {
      if (high_min > high[i]) high_min = high[i];
      if (high_max < high[i]) high_max = high[i];
    }
  }

  char* buf_high;
  uint64_t high_compress_size;
  TArrCompress(high, cnt, high_min, high_max, buf_high, high_compress_size, MyColumnType::MyInt32);
  memcpy(buf + buf_low_sz + 8, buf_high, high_compress_size);
  compress_sz = buf_low_sz + high_compress_size + 8;

  *reinterpret_cast<int*>(buf) = buf_low_sz;
  *reinterpret_cast<int*>(buf+4) = high_compress_size;

  naive_free(buf_high);

  return 0;
}

inline int DoubleArrDeCompress(OUT double double_arr[], int& cnt, int origin_size, char* compress_buf, uint64_t compress_sz) {
  uint64_t buf_low_size = *reinterpret_cast<int*>(compress_buf);
  uint64_t high_compress_size = *reinterpret_cast<int*>(compress_buf + 4);
  int high_bits = 24; // 后面通过策略控制，目前暂定24比特
  int high_mask = 0xFFFFFF;
  int low_bytes = 5;
  cnt = buf_low_size / low_bytes;
  int high[cnt];
  LOG_ASSERT(compress_sz == buf_low_size + high_compress_size + 8, "error");

  int high_cnt;
  TArrDeCompress(high, high_cnt, sizeof(int) * cnt, compress_buf + 8 + buf_low_size, high_compress_size, MyColumnType::MyInt32);
  LOG_ASSERT(high_cnt == cnt, "high_cnt %d cnt %d", high_cnt, cnt);

  for (int i = 0; i < cnt; i++) {
    uint64_t hhh = high[i];
    uint64_t res_high = (hhh << (64-high_bits));
    uint64_t res_low = 0;
    uint64_t res = 0;
    memcpy(&res_low, compress_buf + low_bytes * i + 8, low_bytes);
    res = res_high | res_low;
    *reinterpret_cast<uint64_t*>(&double_arr[i]) = res;
  }

  return 0;
}

// 上面用完了需要记得释放内存
template<typename T>
int TArrCompress(T t_arr[], int cnt, T min, T max, OUT char* &compress_buf, OUT uint64_t &size, MyColumnType col_type) {
  if (col_type == MyColumnType::MyInt32) {
    if (IntAllEqualCompress((int*)t_arr, cnt, min, max, compress_buf, size) == 0) {
    } else if (IntDiffCompress((int*)t_arr, cnt, min, max, compress_buf, size) == 0) {
    } else {
      // zstd 兜底
      uint64_t compress_buf_sz = ZSTDMaxDestSize(sizeof(int) * cnt) + 1; // +1是因为头部需要额外存一个字节的type
      auto now = TIME_NOW;
      compress_buf = reinterpret_cast<char*>(naive_alloc(compress_buf_sz));
      auto now2 = TIME_NOW;
      RECORD_FETCH_ADD(alloc_time, TIME_DURATION_US(now, now2));
      compress_buf[0] = (char)CompressType::ZSTD;
      size = ZSTDCompress((const char*)t_arr, sizeof(int) * cnt, compress_buf+1, compress_buf_sz-1) + 1;
    }
  } else if (col_type == MyColumnType::MyDouble) {
    DoubleArrCompress((double*)t_arr, cnt, min, max, compress_buf, size);
  } else if (col_type == MyColumnType::MyUInt16) {
    uint64_t compress_buf_sz = ZSTDMaxDestSize(sizeof(T) * cnt) + 1; // +1是因为头部需要额外存一个字节的type
    auto now = TIME_NOW;
    compress_buf = reinterpret_cast<char*>(naive_alloc(compress_buf_sz));
    auto now2 = TIME_NOW;
    RECORD_FETCH_ADD(alloc_time, TIME_DURATION_US(now, now2));
    compress_buf[0] = (char)CompressType::ZSTD;
    size = ZSTDCompress((const char*)t_arr, sizeof(T) * cnt, compress_buf+1, compress_buf_sz-1) + 1;
  } else if (col_type == MyColumnType::MyInt64) {
    if (TsDiffCompress((int64_t*)t_arr, cnt, min, max, compress_buf, size) == 0) {
    } else {
      // zstd 兜底
      uint64_t compress_buf_sz = ZSTDMaxDestSize(sizeof(int64_t) * cnt) + 1; // +1是因为头部需要额外存一个字节的type
      auto now = TIME_NOW;
      compress_buf = reinterpret_cast<char*>(naive_alloc(compress_buf_sz));
      auto now2 = TIME_NOW;
      RECORD_FETCH_ADD(alloc_time, TIME_DURATION_US(now, now2));
      compress_buf[0] = (char)CompressType::ZSTD;
      size = ZSTDCompress((const char*)t_arr, sizeof(int64_t) * cnt, compress_buf+1, compress_buf_sz-1) + 1;
    }
  } else {
    LOG_ASSERT(false, "should not run here");
  }

  return 0;
}

// 上面释放内存
template<typename T>
int TArrDeCompress(OUT T t_arr[], OUT int &cnt, int origin_sz, char* compress_buf, uint64_t compress_size, MyColumnType col_type) {
  if (col_type == MyColumnType::MyInt32) {
    auto compress_type = compress_buf[0];
    if (compress_type == (char)CompressType::INT_ALL_EQUALS) {
      IntAllEqualDeCompress((int*)t_arr, cnt, compress_buf, compress_size);
    } else if (compress_type == (char)CompressType::INT_DIFFERENCE) {
      IntDiffDeCompress((int*)t_arr, cnt, compress_buf, compress_size);
    } else {
      LOG_ASSERT(compress_buf[0] == 2, "no zstd");
      // zstd decompress
      auto ret = ZSTDDeCompress((const char*)compress_buf+1, (char*)t_arr, compress_size-1, origin_sz);
      cnt = ret / sizeof(T);
    }
  } else if (col_type == MyColumnType::MyDouble) {
    DoubleArrDeCompress((double*)t_arr, cnt, origin_sz, compress_buf, compress_size);
  } else if (col_type == MyColumnType::MyUInt16) {
    LOG_ASSERT(compress_buf[0] == 2, "no zstd");
    // zstd decompress
    auto ret = ZSTDDeCompress((const char*)compress_buf+1, (char*)t_arr, compress_size-1, origin_sz);
    cnt = ret / sizeof(T);
  } else if (col_type == MyColumnType::MyInt64) {
    auto compress_type = compress_buf[0];
    if (compress_type == (char)CompressType::INT_DIFFERENCE) {
      TsDiffDeCompress((int64_t*)t_arr, cnt, compress_buf, compress_size);
    } else {
      LOG_ASSERT(compress_buf[0] == 2, "no zstd");
      // zstd decompress
      auto ret = ZSTDDeCompress((const char*)compress_buf+1, (char*)t_arr, compress_size-1, origin_sz);
      cnt = ret / sizeof(T);
    }
  }

  return 0;
}

} // namespace LindormContest
