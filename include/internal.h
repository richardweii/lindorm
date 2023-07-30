#ifndef _INTERNAL_H_
#define _INTERNAL_H_
// Row的紧凑形式，用来下盘和内存存储
#include "Root.h"
#include "common.h"
#include "status.h"
#include "struct/Row.h"
#include "struct/Schema.h"
#include <cstddef>
#include <unordered_map>

namespace LindormContest {

class InternalSchema {
public:
  InternalSchema(Schema &&schema);

  int column_idx(std::string name) const;

  ColumnType column_type(int idx) const;

  Schema &schema() { return schema_; }

  size_t row_size(const Row &row);

private:
  std::vector<std::string> str_cols_; // 字符串列的COL名字
  int numeric_sum_;                   // 非字符串列的空间总和
  std::unordered_map<std::string, int> col_mapping_;
  ColumnType cols_[kColumnNum];
  Schema schema_;
};

class InternalRow {
public:
  InternalRow(const char *base);
  void reset(const char *base) {
    data_ = base;
  }
  void prepare(const InternalSchema &schema);

  template <typename Tp>
  Tp get_col(int idx) {
    auto off = col_off[idx];
    return *reinterpret_cast<Tp *>(&data_[off]);
  }

  uint64_t get_ts() const;

private:
  uint16_t col_off[kColumnNum];
  const char *data_{nullptr};
};

} // namespace LindormContest

#endif