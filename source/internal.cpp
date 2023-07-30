#include "internal.h"
#include "util/logging.h"
#include "util/slice.h"

namespace LindormContest {

InternalSchema::InternalSchema(Schema &&schema) : schema_(std::move(schema)) {
  int idx = 0;
  for (auto &iter : schema.columnTypeMap) {
    switch (iter.second) {
      case COLUMN_TYPE_STRING: {
        this->str_cols_.emplace_back(iter.first);
        break;
      }
      case COLUMN_TYPE_INTEGER: {
        this->numeric_sum_ += sizeof(int32_t);
        break;
      }
      case COLUMN_TYPE_DOUBLE_FLOAT: {
        this->numeric_sum_ += sizeof(double_t);
        break;
      }
      case COLUMN_TYPE_UNINITIALIZED: {
        LOG_FATAL("unexpected col type.");
        break;
      }
    }
    col_mapping_.insert(std::make_pair(iter.first, idx));
    cols_[idx++] = iter.second;
  }
  LOG_ASSERT(idx == kColumnNum, "schema column size is not equal to 60");
}

ColumnType InternalSchema::column_type(int idx) const {
  LOG_ASSERT(0 <= idx && idx < kColumnNum, "invalid idx %d", idx);
  return cols_[idx];
};

int InternalSchema::column_idx(std::string name) const {
  auto iter = col_mapping_.find(name);
  if (UNLIKELY(iter == col_mapping_.end())) {
    return -1;
  }
  return iter->second;
};

size_t InternalSchema::row_size(const Row &row) {
  size_t sz = this->numeric_sum_;
  for (auto &col : this->str_cols_) {
    auto it = row.columns.at(col);
    sz += it.getRawDataSize() - sizeof(int32_t) + sizeof(uint16_t);
  }
  sz += sizeof(uint16_t) + sizeof(int64_t);
  return sz;
};

InternalRow::InternalRow(const char *base) : data_(const_cast<char *>(base)) {}

template <>
inline Slice InternalRow::get_col<Slice>(int idx) {
  auto off = col_off[idx];
  int32_t size = *reinterpret_cast<const int32_t *>(&data_[off]);
  return Slice(&data_[off + 4], size);
}

void InternalRow::prepare(const InternalSchema &schema) {
  uint16_t off = 8;
  for (int idx = 0; idx < kColumnNum; idx++) {
    col_off[idx] = off;
    auto type = schema.column_type(idx);
    switch (type) {
      case COLUMN_TYPE_STRING: {
        uint16_t str_size = *reinterpret_cast<const uint16_t *>(data_ + off);
        off += sizeof(uint16_t) + str_size;
        break;
      }
      case COLUMN_TYPE_INTEGER: {
        off += sizeof(int32_t);
        break;
      }
      case COLUMN_TYPE_DOUBLE_FLOAT: {
        off += sizeof(double_t);
        break;
      }
      case COLUMN_TYPE_UNINITIALIZED: {
        LOG_FATAL("unexpected column type.");
        break;
      };
    }
  }
};

inline uint64_t InternalRow::get_ts() const { return *reinterpret_cast<const int64_t *>(data_); };
} // namespace LindormContest