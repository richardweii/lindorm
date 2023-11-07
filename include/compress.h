#pragma once

#include <malloc.h>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
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
  ALL_EQUALS, // int值全部相同
  DIFFERENCE, // int差分
  ZSTD, // 走zstd
};

int TsDiffCompress(int64_t ts_arr[], int cnt, int64_t min, int64_t max, char* &buf, uint64_t &compress_size);
int TsDiffDeCompress(int64_t ts_arr[], int &cnt, char* buf, uint64_t compress_size);

inline int ZSTDMaxDestSize(int inputSize) { return ZSTD_compressBound(inputSize); }

inline uint64_t ZSTDCompress(const char* data, uint64_t len, char* compress_buf, uint64_t compress_len, int compress_level = 3) {
  RECORD_FETCH_ADD(zstd_compress_cnt, 1);
  return ZSTD_compress((void*)compress_buf, compress_len, (const void*)data, len, compress_level);
}

inline int ZSTDDeCompress(const char* src, char* dst, int compressedSize, int dstCapacity) {
  return ZSTD_decompress(dst, dstCapacity, src, compressedSize);
}

template<typename T>
int AllEqualCompress(T t_arr[], int cnt, T min, T max, char* &buf, uint64_t &compress_size) {
  if (min == max) {
    RECORD_FETCH_ADD(all_equal_compress_cnt, 1);
    // 说明全是相同的值，只需要存储类型第一个值以及个数即可
    compress_size = 1 + sizeof(T) + sizeof(cnt);
    buf = reinterpret_cast<char*>(naive_alloc(compress_size));
    buf[0] = static_cast<char>(CompressType::ALL_EQUALS);
    *reinterpret_cast<T*>(&buf[1]) = min;
    *reinterpret_cast<int*>(&buf[1+sizeof(T)]) = cnt;
    return 0;
  }
  return -1;
}

template<typename T>
int AllEqualDeCompress(T t_arr[], int &cnt, char* buf, uint64_t compress_size) {
  LOG_ASSERT(compress_size == 1 + sizeof(T) + sizeof(cnt), "compress size %lu", compress_size);
  // 读取压缩类型
  int type = static_cast<int>(buf[0]);
  LOG_ASSERT(type == (int)CompressType::ALL_EQUALS, "type is %d", (int)CompressType::ALL_EQUALS);
  T number;
  number = *reinterpret_cast<T*>(&buf[1]);
  cnt = *reinterpret_cast<int*>(&buf[1+sizeof(T)]);
  
  for (int i = 0; i < cnt; i++) {
    t_arr[i] = number;
  }
  return 0;
}

inline int CalculateBitsRequired(uint64_t min, uint64_t max) {
  if (min == std::numeric_limits<uint64_t>::max()) return 0;
  // 计算绝对值范围的大小
  uint64_t range = std::max(min, max);
  
  // 使用对数函数来估算所需的比特数，向上取整
  int bitsRequired = static_cast<int>(std::ceil(std::log2(range + 1)));
  
  // 考虑符号位，至少需要1个比特
  return bitsRequired + 1;
}

/**
 * 分段差分
 */
template<typename T>
int DiffCompress(T t_arr[], int cnt, int diff_cnt, char* &buf, uint64_t &compress_size) {
  if (diff_cnt > 5) return -1;
  RECORD_FETCH_ADD(int_diff_compress_cnt, 1);

  int bitsRequireds[diff_cnt];
  int diff_idx = 0;
  int diff_offset[diff_cnt+1];
  diff_offset[0] = 0;
  // 计算差分范围
  uint64_t min_diff = std::numeric_limits<uint64_t>::max();
  uint64_t max_diff = 0;
  uint64_t last_min_diff = min_diff;
  uint64_t last_max_diff = max_diff;
  for (int i = 1; i < cnt; ++i) {
    uint64_t diff = std::abs(t_arr[i] - t_arr[i - 1]);
    min_diff = std::min(min_diff, diff);
    max_diff = std::max(max_diff, diff);
    if (diff >= MAX_DIFF_VAL) {
      bitsRequireds[diff_idx] = CalculateBitsRequired(last_min_diff, last_max_diff);
      min_diff = std::numeric_limits<uint64_t>::max();
      max_diff = std::numeric_limits<uint64_t>::min();
      diff_offset[diff_idx+1] = i;
      diff_idx++;
    }
    last_min_diff = min_diff;
    last_max_diff = max_diff;
  }
  if (diff_idx+1 != diff_cnt) abort();
  bitsRequireds[diff_idx] = CalculateBitsRequired(min_diff, max_diff);
  diff_offset[diff_idx+1] = cnt;

  int cnts[diff_cnt];
  // 计算缓冲区大小
  // 1字节的压缩类型 + 1字节的diff_cnt + (sizeof(T) + sizeof(uint8_t) + sizeof(uint16_t)) * diff_cnt + 压缩数据
  compress_size = 1 + 1 + (sizeof(T) + sizeof(uint8_t) + sizeof(uint16_t)) * diff_cnt;
  for (int i = 0; i < diff_cnt; i++) {
    auto cnt_i = diff_offset[i+1] - diff_offset[i];
    cnts[i] = cnt_i;
    compress_size += ((cnt_i-1) * bitsRequireds[i] + 7) / 8;
  }

  // 分配缓冲区
  buf = reinterpret_cast<char*>(naive_alloc(compress_size));
  memset(buf, 0, compress_size);
  // 写入压缩类型 (1字节)
  buf[0] = static_cast<char>(CompressType::DIFFERENCE);
  buf[1] = diff_cnt;
  int skip = sizeof(T) + sizeof(uint8_t) + sizeof(uint16_t);
  for (int i = 0; i < diff_cnt; i++) {
    *reinterpret_cast<T*>(buf+2+i*skip) = t_arr[diff_offset[i]];
    *reinterpret_cast<uint8_t*>(buf+2+i*skip+sizeof(T)) = bitsRequireds[i];
    *reinterpret_cast<uint16_t*>(buf+2+i*skip+sizeof(T)+1) = cnts[i];
  }

  // 差分压缩并写入缓冲区
  int currentByte = 2+diff_cnt*skip;
  int currentBit = 0;
  
  for (int k = 0; k < diff_cnt; k++) {
    int start = diff_offset[k];
    int end = diff_offset[k+1];
    if (currentBit != 0) {
      currentBit = 0;
      currentByte++;
    }
    for (int i = start+1; i < end; i++) {
      bool is_neg = t_arr[i] < t_arr[i-1];
      uint64_t diff = std::abs(t_arr[i] - t_arr[i-1]);
      for (int j = 0; j < bitsRequireds[k]; ++j) {
        if (currentBit == 8) {
          currentBit = 0;
          currentByte++;
        }
        if (j == bitsRequireds[k] - 1) {
          // 设置符号位
          if (is_neg) buf[currentByte] |= 0x1 << currentBit;
          else buf[currentByte] |= 0x0 << currentBit;
        } else {
          // 设置缓冲区的相应比特位
          buf[currentByte] |= ((diff >> j) & 1) << currentBit;
        }
        currentBit++;
      }
    }
  }

  return 0;
}

template<typename T>
int DiffDeCompress(T t_arr[], int &cnt, char* buf, uint64_t compress_size) {
  // 读取压缩类型
  int type = static_cast<int>(buf[0]);
  int diff_cnt = *reinterpret_cast<uint8_t*>(buf+1);

  int bitsRequireds[diff_cnt];
  int diff_idx = 0;
  int diff_offset[diff_cnt+1];
  int cnts[diff_cnt];
  T first_val[diff_cnt];

  int skip = sizeof(T) + sizeof(uint8_t) + sizeof(uint16_t);
  for (int i = 0; i < diff_cnt; i++) {
    first_val[i] = *reinterpret_cast<T*>(buf+2+i*skip);
    bitsRequireds[i] = *reinterpret_cast<uint8_t*>(buf+2+i*skip+sizeof(T));
    cnts[i] = *reinterpret_cast<uint16_t*>(buf+2+i*skip+sizeof(T)+1);
  }

  diff_offset[0] = 0;
  cnt = 0;
  for (int i = 1; i <= diff_cnt; i++) {
    diff_offset[i] = diff_offset[i-1] + cnts[i-1];
    cnt += cnts[i-1];
  }

  // 解压数据
  int currentByte = 2+diff_cnt*skip;
  int currentBit = 0;

  for (int k = 0; k < diff_cnt; k++) {
    int start = diff_offset[k];
    int end = diff_offset[k+1];
    if (currentBit != 0) {
      currentBit = 0;
      currentByte++;
    }
    t_arr[start] = first_val[k];
    for (int i = start+1; i < end; i++) {
      T val;
      uint64_t diff = 0;
      bool is_neg = false;
      for (int j = 0; j < bitsRequireds[k]; ++j) {
        if (currentBit == 8) {
          currentBit = 0;
          currentByte++;
        }
        if (j == bitsRequireds[k] - 1) {
          if ((buf[currentByte] >> currentBit) & 1) is_neg = true;
          if (is_neg) val = t_arr[i-1] - diff;
          else val = t_arr[i-1] + diff;
        } else {
          // 读取缓冲区的相应比特位
          diff |= ((buf[currentByte] >> currentBit) & 1) << j;
        }
        currentBit++;
      }
      t_arr[i] = val;
    }
  }

  return 0;
}

// 根据最小值和最大值，找到高位可以压缩的最大位数
// int CalculateHighBits(int min, int max) {

// }

// int HighBitCompress(int t_arr[], int cnt, int min, int max, char* &buf, uint64_t &compress_size) {

// }

/**
 * t_arr是需要压缩的泛型数组，cnt是数组的元素个数，min和max分别是数组的最大值和最小值
 * 
 * buf是输出的压缩后的buffer指针，size是压缩后的大小，col_type代表列类型，用于决定压缩规则
 */
template<typename T>
int TArrCompress(T t_arr[], int cnt, T min, T max, int diff_cnt, OUT char* &buf, OUT uint64_t &compress_size, MyColumnType col_type);

template<typename T>
int TArrDeCompress(OUT T t_arr[], OUT int &cnt, int origin_sz, char* compress_buf, uint64_t compress_size, MyColumnType col_type);

inline int StringArrCompress(std::string* data, uint16_t lens[], int cnt, uint16_t min, uint16_t max, 
                              OUT char* &buf, OUT uint64_t &compress_sz) {
  uint64_t offset;
  uint64_t writesz1 = cnt * sizeof(lens[0]);
  // 对lens进行压缩
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

  compress_sz = ZSTDCompress(origin, input_sz, compress_buf, compress_buf_sz, 3);
  naive_free(origin);
  buf = compress_buf;
  return 0;
}

inline int StringArrDeCompress(char* origin_buf, int origin_sz, char* compress_data, uint64_t compress_sz) {
  return ZSTDDeCompress(compress_data, origin_buf, compress_sz, origin_sz);
}

// inline int StringArrCompress(std::string* data, uint16_t lens[], int cnt, uint16_t min, uint16_t max, 
//                               OUT char* &buf, OUT uint64_t &compress_sz) {
//   // 对lens进行压缩
//   char* lens_compress_buf;
//   uint64_t lens_compress_sz;
//   TArrCompress(lens, cnt, min, max, lens_compress_buf, lens_compress_sz, MyColumnType::MyUInt16);

//   // 直接把最终的缓冲区分配好
//   uint64_t dst_compress_buf_sz = 4 + 4 + lens_compress_sz + ZSTDMaxDestSize(data->size());
//   auto now = TIME_NOW;
//   char* big_buf = reinterpret_cast<char*>(naive_alloc(dst_compress_buf_sz));
//   auto now2 = TIME_NOW;
//   RECORD_FETCH_ADD(alloc_time, TIME_DURATION_US(now, now2));
//   uint64_t string_compress_sz = ZSTDCompress(data->c_str(), data->size(), big_buf + 8 + lens_compress_sz, dst_compress_buf_sz-8-lens_compress_sz, 20);

//   *reinterpret_cast<int*>(big_buf) = lens_compress_sz;
//   *reinterpret_cast<int*>(big_buf + 4) = string_compress_sz;
//   memcpy(big_buf+8, lens_compress_buf, lens_compress_sz);
//   naive_free(lens_compress_buf);

//   buf = big_buf;
//   compress_sz = 8 + lens_compress_sz + string_compress_sz;

//   return 0;
// }

// inline int StringArrDeCompress(char* origin_buf, int origin_sz, char* compress_data, uint64_t compress_sz) {
//   uint64_t lens_compress_sz = *reinterpret_cast<int*>(compress_data);
//   uint64_t dst_compress_buf_sz = *reinterpret_cast<int*>(compress_data+4);

//   int cnt;
//   TArrDeCompress((uint16_t*)origin_buf, cnt, sizeof(uint16_t) * kMemtableRowNum, compress_data+8,
//                   lens_compress_sz, MyColumnType::MyUInt16);
  
//   return ZSTDDeCompress(compress_data+8+lens_compress_sz, origin_buf+sizeof(uint16_t)*cnt, 
//                     dst_compress_buf_sz, origin_sz-sizeof(uint16_t)*cnt) + sizeof(uint16_t) * cnt;
// }

// int calculateSep(double min, double max) {
//   std::cout << std::abs(ceil(max)) << std::endl;

//   auto exp = log2(std::abs(ceil(max)));

//   std::cout << exp << std::endl;

//   // return exp - ceil(log2(max - min));
//   return log2(std::abs(ceil(max))) -  ceil(log2(max - min)) + 1 + 11;
// }

/**
 * double分成三段，第一段是公共前缀部分，第二段
 */
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
  int diff_cnt = 1;
  buf = reinterpret_cast<char*>(naive_alloc(buf_sz));
  for (int i = 0; i < cnt; i++) {
    // 提取double的高位比特
    uint64_t binaryRepresentation = *reinterpret_cast<uint64_t*>(&double_arr[i]);
    high[i] = (binaryRepresentation >> (64-high_bits)) & high_mask;
    memcpy(buf + low_bytes * i + 8, &double_arr[i], low_bytes);
    if (i == 0) {
      diff_cnt = 1;
      high_min = high[i];
      high_max = high[i];
    } else {
      if (std::abs(high[i] - high[i-1]) >= MAX_DIFF_VAL) diff_cnt++;
      if (high_min > high[i]) high_min = high[i];
      if (high_max < high[i]) high_max = high[i];
    }
  }

  char* buf_high;
  uint64_t high_compress_size;
  TArrCompress(high, cnt, high_min, high_max, diff_cnt, buf_high, high_compress_size, MyColumnType::MyInt32);
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
int TArrCompress(T t_arr[], int cnt, T min, T max, int diff_cnt, OUT char* &compress_buf, OUT uint64_t &size, MyColumnType col_type) {
  if (col_type == MyColumnType::MyInt32 || col_type == MyColumnType::MyUInt16) {
    if (AllEqualCompress(t_arr, cnt, min, max, compress_buf, size) == 0) {
    } else if (DiffCompress(t_arr, cnt, diff_cnt, compress_buf, size) == 0) {
    } else {
      // zstd 兜底
      uint64_t compress_buf_sz = ZSTDMaxDestSize(sizeof(T) * cnt) + 1; // +1是因为头部需要额外存一个字节的type
      auto now = TIME_NOW;
      compress_buf = reinterpret_cast<char*>(naive_alloc(compress_buf_sz));
      auto now2 = TIME_NOW;
      RECORD_FETCH_ADD(alloc_time, TIME_DURATION_US(now, now2));
      compress_buf[0] = (char)CompressType::ZSTD;
      size = ZSTDCompress((const char*)t_arr, sizeof(T) * cnt, compress_buf+1, compress_buf_sz-1) + 1;
    }
  } else if (col_type == MyColumnType::MyDouble) {
    DoubleArrCompress((double*)t_arr, cnt, min, max, compress_buf, size);
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
  if (col_type == MyColumnType::MyInt32 || col_type == MyColumnType::MyUInt16) {
    auto compress_type = compress_buf[0];
    if (compress_type == (char)CompressType::ALL_EQUALS) {
      AllEqualDeCompress(t_arr, cnt, compress_buf, compress_size);
    } else if (compress_type == (char)CompressType::DIFFERENCE) {
      DiffDeCompress(t_arr, cnt, compress_buf, compress_size);
    } else {
      LOG_ASSERT(compress_buf[0] == 2, "no zstd");
      // zstd decompress
      auto ret = ZSTDDeCompress((const char*)compress_buf+1, (char*)t_arr, compress_size-1, origin_sz);
      cnt = ret / sizeof(T);
    }
  } else if (col_type == MyColumnType::MyDouble) {
    DoubleArrDeCompress((double*)t_arr, cnt, origin_sz, compress_buf, compress_size);
  } else if (col_type == MyColumnType::MyInt64) {
    auto compress_type = compress_buf[0];
    if (compress_type == (char)CompressType::DIFFERENCE) {
      TsDiffDeCompress((int64_t*)t_arr, cnt, compress_buf, compress_size);
    } else {
      LOG_ASSERT(compress_buf[0] == 2, "no zstd");
      // zstd decompress
      auto ret = ZSTDDeCompress((const char*)compress_buf+1, (char*)t_arr, compress_size-1, origin_sz);
      cnt = ret / sizeof(T);
    }
  } else {
    LOG_ASSERT(false, "should not run here");
  }

  return 0;
}

} // namespace LindormContest
