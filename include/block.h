#ifndef _BLOCK_H_
#define _BLOCK_H_

#include "common.h"
#include "internal.h"
#include "io/writer.h"
#include "status.h"
#include <cstddef>

namespace LindormContest {

// 一块固定大小的内存block
/*
block 编码
/----------------------------------------------------------------------------------
|   header    |  row0_len(2B) | timestamp | col0 | col1 | ... col 60 | row1_len
/----------------------------------------------------------------------------------

col 如果是固定的数值类型就直接紧凑存放
col 如果是字符串就先读一个uint16_t 的长度，然后再读字符串内容


header 编码
/----------------------------
| size | min_ts   | max_ts |
/---------------------------
|  4B  | 8B       | 8B     |
/---------------------------
*/

class Block {
public:
  Block() = default;
  size_t size() const { return *reinterpret_cast<const uint32_t *>(data_); }
  void set_size(size_t sz) { *reinterpret_cast<uint32_t *>(data_) = sz; }

  int64_t min_ts() const { return *reinterpret_cast<const int64_t *>(data_ + 4); }
  void set_min_ts(int64_t ts) { *reinterpret_cast<int64_t *>(data_ + 4) = ts; }

  int64_t max_ts() const { return *reinterpret_cast<const int64_t *>(data_ + 4 + 8); }
  void set_max_ts(int64_t ts) { *reinterpret_cast<int64_t *>(data_ + 4 + 8) = ts; }

  void clear() { *reinterpret_cast<uint32_t *>(data_) = 0; }
  char *data() { return data_; }

private:
  char data_[kBlockSize];
};

// 将row写入block，以及满了之后写入文件
class BlockWriter {
public:
  BlockWriter(InternalSchema *schema, Block *block);

  Status write_row(const Row &row, OUT InternalRow *internal_row);
  Status solidify(Writer &writer);

private:
  void encode_row(char *dst, const Row &row, uint16_t row_sz);
  Block *block_;
  InternalSchema *schema_;
};

// 对block进行读取形式解析
class BlockReader {
public:
  BlockReader(Block *block);

private:
  Block *block_;
};

} // namespace LindormContest
#endif