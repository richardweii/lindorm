#include "block.h"
#include "util/logging.h"
namespace LindormContest {

BlockWriter::BlockWriter(InternalSchema *schema, Block *block) : block_(block), schema_(schema) {}

Status BlockWriter::write_row(const Row &row, OUT InternalRow *internal_row) {
  auto cur_sz = block_->size();
  auto row_sz = schema_->row_size(row);
  if (cur_sz + row_sz > kBlockSize) {
    return Status::ExceedCapacity;
  }

  if (block_->min_ts() > row.timestamp) {
    block_->set_min_ts(row.timestamp);
  } else if (block_->max_ts() < row.timestamp) {
    block_->set_max_ts(row.timestamp);
  }
  auto cur_off = cur_sz;
  auto base = block_->data() + cur_off;
  if (internal_row != nullptr) {
    internal_row->reset(base);
  }
  encode_row(base, row, row_sz);
  block_->set_size(cur_sz + row_sz);
  return Status::OK;
};

void BlockWriter::encode_row(char *dst, const Row &row, uint16_t row_sz) {
  *reinterpret_cast<uint16_t *>(dst) = row_sz;
  dst += sizeof(uint16_t);
  *reinterpret_cast<int64_t *>(dst) = row.timestamp;
  dst += sizeof(int64_t);
  // WARN:这里需要保证遍历时得到的顺序和schema中存放的顺序是一样的
  for (auto &iter : row.columns) {
    auto &val = iter.second;
    switch (iter.second.columnType) {
      case COLUMN_TYPE_STRING: {
        std::pair<int32_t, const char *> sp;
        ENSURE(val.getStringValue(sp) == 0, "unexpected ret");
        *reinterpret_cast<uint16_t *>(dst) = sp.first;
        dst += sizeof(uint16_t);
        ::memcpy(dst, sp.second, sp.first);
        dst += sp.first;
        break;
      }
      case COLUMN_TYPE_INTEGER: {
        int32_t int_val;
        ENSURE(val.getIntegerValue(int_val) == 0, "unexpected ret");
        *reinterpret_cast<int32_t *>(dst) = int_val;
        dst += sizeof(int32_t);
        break;
      }
      case COLUMN_TYPE_DOUBLE_FLOAT: {
        double_t double_val;
        ENSURE(val.getDoubleFloatValue(double_val), "unexpected ret");
        *reinterpret_cast<double_t *>(dst) = double_val;
        dst += sizeof(double_t);
        break;
      }
      default: LOG_FATAL("unexpected column_type");
    }
  }
};
} // namespace LindormContest