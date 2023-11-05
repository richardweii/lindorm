#include "compress.h"
#include <malloc.h>
#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstring>
#include "struct/ColumnValue.h"
#include "util/logging.h"
#include "util/mem_pool.h"
#include "util/stat.h"

namespace LindormContest {

int IntAllEqualCompress(int int_arr[], int cnt, int min, int max, char* &buf, uint64_t &compress_size) {
  if (min == max) {
    RECORD_FETCH_ADD(all_equal_compress_cnt, 1);
    // 说明全是相同的值，只需要存储类型第一个值以及个数即可
    compress_size = 1 + sizeof(int) * 2;
    buf = reinterpret_cast<char*>(naive_alloc(compress_size));
    buf[0] = static_cast<char>(CompressType::INT_ALL_EQUALS);
    *reinterpret_cast<int*>(&buf[1]) = min;
    *reinterpret_cast<int*>(&buf[5]) = cnt;
    return 0;
  }
  return -1;
}

int IntAllEqualDeCompress(int int_arr[], int &cnt, char* buf, uint64_t compress_size) {
  LOG_ASSERT(compress_size == 1 + 4 + 4, "compress size %lu", compress_size);
  // 读取压缩类型
  int type = static_cast<int>(buf[0]);
  LOG_ASSERT(type == (int)CompressType::INT_ALL_EQUALS, "type is %d", (int)CompressType::INT_ALL_EQUALS);
  int number;
  number = *reinterpret_cast<int*>(&buf[1]);
  cnt = *reinterpret_cast<int*>(&buf[5]);
  
  for (int i = 0; i < cnt; i++) {
    int_arr[i] = number;
  }
  return 0;
}

int CalculateBitsRequired(int min, int max) {
  int range = max - min;
  return static_cast<int>(std::ceil(std::log2(range + 1)));
}

int IntDiffCompress(int int_arr[], int cnt, int min, int max, char* &buf, uint64_t &compress_size) {
  // 计算所需比特数
  int bitsRequired = CalculateBitsRequired(min, max);
  if (bitsRequired > 8) return -1;
  RECORD_FETCH_ADD(int_diff_compress_cnt, 1);
  // 计算缓冲区大小
  compress_size = 1 + 4 + 1 + (cnt * bitsRequired + 7) / 8; // 1字节的压缩类型 + 4字节的最小值 + 1字节的比特数 + 压缩数据
  // 分配缓冲区
  buf = reinterpret_cast<char*>(naive_alloc(compress_size));
  memset(buf, 0, compress_size);
  // 写入压缩类型 (1字节)
  buf[0] = static_cast<char>(CompressType::INT_DIFFERENCE);
  // 写入最小值 (4字节)
  *reinterpret_cast<int*>(&buf[1]) = min;
  // 写入比特数 (1字节)
  buf[5] = static_cast<char>(bitsRequired);
  // 差分压缩并写入缓冲区
  int currentByte = 6; // 1字节压缩类型 + 4字节最小值 + 1字节比特数
  int currentBit = 0;
  for (int i = 0; i < cnt; ++i) {
    int diff = int_arr[i] - min;
    for (int j = 0; j < bitsRequired; ++j) {
      if (currentBit == 8) {
        currentBit = 0;
        currentByte++;
      }
      // 设置缓冲区的相应比特位
      buf[currentByte] |= ((diff >> j) & 1) << currentBit;
      currentBit++;
    }
  }
  return 0;
}

int IntDiffDeCompress(int int_arr[], int &cnt, char* buf, uint64_t compress_size) {
  // 读取压缩类型
  int type = static_cast<int>(buf[0]);
  // 读取最小值
  int min = *reinterpret_cast<int*>(&buf[1]);
  // 读取比特数
  int bitsCount = static_cast<int>(buf[5]);
  // 计算元素数量
  cnt = (compress_size - 6) * 8 / bitsCount;
  // 解压数据
  int currentByte = 6;
  int currentBit = 0;
  for (int i = 0; i < cnt; ++i) {
    int diff = 0;
    for (int j = 0; j < bitsCount; ++j) {
      if (currentBit == 8) {
        currentBit = 0;
        currentByte++;
      }
      // 读取缓冲区的相应比特位
      diff |= ((buf[currentByte] >> currentBit) & 1) << j;
      currentBit++;
    }
    int_arr[i] = diff + min;
  }

  return 0;
}

} // namespace LindormContest
