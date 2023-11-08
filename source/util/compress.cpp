#include "compress.h"
#include <malloc.h>
#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstring>
#include "common.h"
#include "struct/ColumnValue.h"
#include "util/logging.h"
#include "util/mem_pool.h"
#include "util/stat.h"

namespace LindormContest {

static int FindCommonPrefix(int num1, int num2) {
  int ret = 1;
  while (ret <= 32) {
    if (((num1 >> (32-ret)) & 1) != ((num2 >>(32-ret))&1)) break;
    ret++;
  }

  return ret - 1;
}

int HighBitCompress(int int_arr[], int cnt, int min, int max, char* &buf, uint64_t &compress_size) {
  if (max > 0 && min < 0) return -1;

  RECORD_FETCH_ADD(high_compress_cnt, 1);
  int high_bits = FindCommonPrefix(min, max);
  high_bits = (high_bits + 7) & ~7;
  high_bits = std::min(32, high_bits);
  uint64_t high_mask = (1ULL << high_bits) - 1ULL;
  int high[cnt];
  int high_min;
  int high_max;

  int low_bits = 32-high_bits;
  int low_bytes = low_bits / 8;
  uint64_t buf_low_sz = low_bytes * cnt;
  int skip = 13;
  uint64_t buf_sz = 4 * cnt + skip; // 一次性分配好，后面压缩的直接追加在后面就好了
  int diff_cnt = 1;
  buf = reinterpret_cast<char*>(naive_alloc(buf_sz));
  for (int i = 0; i < cnt; i++) {
    uint32_t binaryRepresentation = *reinterpret_cast<uint32_t*>(&int_arr[i]);
    high[i] = (binaryRepresentation >> (32-high_bits)) & high_mask;
    memcpy(buf + low_bytes * i + skip, &int_arr[i], low_bytes);
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
  memcpy(buf + buf_low_sz + skip, buf_high, high_compress_size);
  compress_size = buf_low_sz + high_compress_size + skip;

  *reinterpret_cast<char*>(buf) = (char)CompressType::HIGH;
  *reinterpret_cast<int*>(buf+1) = buf_low_sz;
  *reinterpret_cast<int*>(buf+5) = high_compress_size;
  *reinterpret_cast<int*>(buf+9) = high_bits; 

  naive_free(buf_high);

  return 0;
}

int HighBitDeCompress(OUT int int_arr[], int& cnt, int origin_size, char* compress_buf, uint64_t compress_sz) {
  int skip = 13;
  uint64_t buf_low_size = *reinterpret_cast<int*>(compress_buf+1);
  uint64_t high_compress_size = *reinterpret_cast<int*>(compress_buf + 5);
  int high_bits = *reinterpret_cast<int*>(compress_buf + 9);
  uint64_t high_mask = (1ULL << high_bits) - 1ULL;
  int low_bytes = (32-high_bits) / 8;
  cnt = buf_low_size / low_bytes;
  int high[cnt];
  LOG_ASSERT(compress_sz == buf_low_size + high_compress_size + skip, "error");

  int high_cnt;
  TArrDeCompress(high, high_cnt, sizeof(int) * cnt, compress_buf + skip + buf_low_size, high_compress_size, MyColumnType::MyInt32);
  LOG_ASSERT(high_cnt == cnt, "high_cnt %d cnt %d", high_cnt, cnt);

  for (int i = 0; i < cnt; i++) {
    uint32_t hhh = high[i];
    uint32_t res_high = (hhh << (32-high_bits));
    uint32_t res_low = 0;
    uint32_t res = 0;
    memcpy(&res_low, compress_buf + low_bytes * i + skip, low_bytes);
    res = res_high | res_low;
    *reinterpret_cast<uint64_t*>(&int_arr[i]) = res;
  }

  return 0;
}

int TsDiffCompress(int64_t ts_arr[], int cnt, int64_t min, int64_t max, char* &buf, uint64_t &compress_size) {
  int int_ts_arr[cnt];
  int diff_cnt = 1;
  for (int i = 0; i < cnt; i++) {
    int_ts_arr[i] = ts_arr[i] / 1000;
    if (i == 0) {
      diff_cnt = 1;
    } else {
      if (std::abs(int_ts_arr[i] - int_ts_arr[i-1]) > MAX_DIFF_VAL) {
        diff_cnt++;
      }
    }
  }

  return DiffCompress(int_ts_arr, cnt, diff_cnt, buf, compress_size);
}

int TsDiffDeCompress(int64_t ts_arr[], int &cnt, char* buf, uint64_t compress_size) {
  if (buf[0] != 1) return -1;

  int int_ts_arr[kMemtableRowNum];
  DiffDeCompress(int_ts_arr, cnt, buf, compress_size);

  for (int i = 0; i < cnt; i++) {
    ts_arr[i] = int_ts_arr[i];
    ts_arr[i] *= 1000;
  }

  return 0;
}

} // namespace LindormContest
