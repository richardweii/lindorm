#pragma once

#include <sys/stat.h>

#include <cstdint>
#include <cstring>
#include <new>
#include <string>

#include "BlockMetaManager.h"
#include "common.h"
#include "compress.h"
#include "io/aligned_buffer.h"
#include "io/io_manager.h"
#include "struct/ColumnValue.h"
#include "util/logging.h"
#include "util/stat.h"

namespace LindormContest {

/**
 * 封装了每个列具体的处理过程，对外提供统一的接口，上层调用者不需要考虑列的类型
 */
template <typename T>
class ColumnArr {
public:
  ColumnArr(int col_id, ColumnType type) : col_id_(col_id), type_(type) {}
  virtual ~ColumnArr() {}

  void Add(const ColumnValue& col, int idx) {
    switch (col.getColumnType()) {
      case COLUMN_TYPE_STRING:
        LOG_ASSERT(false, "should not run here");
        break;
      case COLUMN_TYPE_INTEGER: {
        int val;
        col.getIntegerValue(val);
        data_[idx] = (T)val;
        break;
      }
      case COLUMN_TYPE_DOUBLE_FLOAT: {
        double val;
        col.getDoubleFloatValue(val);
        data_[idx] = (T)val;
        break;
      }
      case COLUMN_TYPE_UNINITIALIZED:
        break;
    }
  }

  // 元数据直接写到内存，内存里面的元数据在shutdown的时候会持久化的
  void Flush(AlignedWriteBuffer* buffer, int cnt, BlockMeta* meta) {
    uint64_t offset;
    uint64_t input_sz = cnt * sizeof(T);
    uint64_t compress_buf_sz = max_dest_size_func(input_sz);

    auto compress_buf = reinterpret_cast<char*>(BuddyThreadHeap::get_instance()->alloc(compress_buf_sz));
    uint64_t compress_sz = compress_func((const char*)data_, input_sz, compress_buf, compress_buf_sz);

    buffer->write(compress_buf, compress_sz, offset);
    BuddyThreadHeap::get_instance()->free_local(compress_buf);

    meta->offset[col_id_] = offset;
    meta->origin_sz[col_id_] = input_sz;
    meta->compress_sz[col_id_] = compress_sz;
    RECORD_ARR_FETCH_ADD(origin_szs, col_id_, input_sz);
    RECORD_ARR_FETCH_ADD(compress_szs, col_id_, compress_sz);
  }

  void Read(File* file, AlignedWriteBuffer* buffer, BlockMeta* meta) {
    LOG_ASSERT(meta != nullptr, "error");

    // buf的长度按512字节对齐
    size_t compressed_buf_sz = roundup512(meta->compress_sz[col_id_]);
    auto buf = reinterpret_cast<char*>(BuddyThreadHeap::get_instance()->alloc(compressed_buf_sz));
    char* compressed_data = buf;

    uint64_t offset = meta->offset[col_id_];
    size_t compress_sz = meta->compress_sz[col_id_];

    if (LIKELY(buffer == nullptr) || buffer->empty() || offset + compress_sz <= (uint64_t)buffer->FlushedSz()) {
      // 全部在文件里面
      struct stat st;
      fstat(file->fd(), &st);
      LOG_ASSERT(offset + compress_sz <= (uint64_t)st.st_size, "offset %lu read size %lu filesz %lu", offset,
                 compress_sz, st.st_size);
      auto read_off = rounddown512(offset);
      file->read(compressed_data, compressed_buf_sz, read_off);
      compressed_data += (offset - read_off); // 偏移修正
    } else {
      if (offset >= (uint64_t)buffer->FlushedSz() * (uint64_t)kWriteBufferSize) {
        // 全部在buffer里面
        buffer->read(compressed_data, compress_sz, offset);
      } else {
        LOG_ASSERT(!buffer->empty(), "buffer is empty");
        // 一部分在文件里面，一部分在buffer里面
        struct stat st;
        fstat(file->fd(), &st);
        // 先从文件读取
        auto read_off = rounddown512(offset);
        file->read(compressed_data, roundup512(st.st_size - offset), read_off);
        compressed_data += (read_off - offset); // 偏移修正

        // 再从缓冲区读取
        buffer->read(compressed_data + st.st_size - offset, compress_sz - (st.st_size - offset), st.st_size);
      }
    }

    auto ret = decompress_func(compressed_data, (char*)data_, compress_sz, meta->origin_sz[col_id_]);
    LOG_ASSERT(ret == (int)meta->origin_sz[col_id_], "uncompress error");
    BuddyThreadHeap::get_instance()->free_local(buf);
  }

  void Get(int idx, ColumnValue& value) {
    switch (type_) {
      case COLUMN_TYPE_STRING:
        LOG_ASSERT(false, "should not run here");
      case COLUMN_TYPE_INTEGER: {
        if (value.getColumnType() != COLUMN_TYPE_INTEGER) {
          free(value.columnData);
          value.columnType = COLUMN_TYPE_INTEGER;
          value.columnData = (char*)malloc(sizeof(int32_t));
          *((int32_t*)value.columnData) = (int32_t)data_[idx];
        } else {
          *((int32_t*)value.columnData) = (int32_t)data_[idx];
        }
        return;
      }
      case COLUMN_TYPE_DOUBLE_FLOAT: {
        if (value.getColumnType() != COLUMN_TYPE_DOUBLE_FLOAT) {
          free(value.columnData);
          value.columnType = COLUMN_TYPE_DOUBLE_FLOAT;
          value.columnData = (char*)malloc(sizeof(double));
          *((double*)value.columnData) = (double)data_[idx];
        } else {
          *((double*)value.columnData) = (double)data_[idx];
        }
        return;
      }
      case COLUMN_TYPE_UNINITIALIZED:
        LOG_ASSERT(false, "should not run here");
    }
    LOG_ASSERT(false, "should not run here");
  }

  int64_t GetVal(int idx) { return data_[idx]; }

  void Reset(){};

  const int col_id_;

  T data_[kMemtableRowNum];
  // T max;
  ColumnType type_;
};

template <>
class ColumnArr<std::string> {
public:
  ColumnArr(int col_id) : col_id_(col_id), offset_(0) { offsets_[0] = 0; }
  ~ColumnArr() {}

  void Add(const ColumnValue& col, int idx) {
    LOG_ASSERT(col.getColumnType() == COLUMN_TYPE_STRING, "column type is %d", col.getColumnType());

    std::pair<int32_t, const char*> pair;
    col.getStringValue(pair);
    std::string str(pair.second, pair.first);

    data_.append(str);

    offset_ += pair.first;
    offsets_[idx + 1] = offset_;
    lens_[idx] = pair.first;
  }

  // 元数据直接写到内存，内存里面的元数据在shutdown的时候会持久化的
  void Flush(AlignedWriteBuffer* buffer, int cnt, BlockMeta* meta) {
    uint64_t offset;
    uint64_t writesz1 = cnt * sizeof(lens_[0]);
    uint64_t writesz2 = data_.size();
    uint64_t input_sz = writesz1 + writesz2;
    char* origin = reinterpret_cast<char*>(BuddyThreadHeap::get_instance()->alloc(input_sz));
    memcpy(origin, lens_, writesz1);
    memcpy(origin + writesz1, data_.c_str(), writesz2);
    uint64_t compress_buf_sz = max_dest_size_func(input_sz);
    char* compress_buf = reinterpret_cast<char*>(BuddyThreadHeap::get_instance()->alloc(compress_buf_sz));
    uint64_t compress_sz = compress_func((const char*)origin, input_sz, compress_buf, compress_buf_sz);

    uint64_t off;
    buffer->write(compress_buf, compress_sz, off);

    BuddyThreadHeap::get_instance()->free_local(origin);
    BuddyThreadHeap::get_instance()->free_local(compress_buf);

    meta->offset[col_id_] = off;
    meta->origin_sz[col_id_] = input_sz;
    meta->compress_sz[col_id_] = compress_sz;
    RECORD_ARR_FETCH_ADD(origin_szs, col_id_, input_sz);
    RECORD_ARR_FETCH_ADD(compress_szs, col_id_, compress_sz);
  }

  // 可以考虑减少一次拷贝
  void Read(File* file, AlignedWriteBuffer* buffer, BlockMeta* meta) {
    LOG_ASSERT(meta != nullptr, "error");

    size_t compress_buf_sz = roundup512(meta->compress_sz[col_id_]);
    size_t origin_buf_sz = meta->origin_sz[col_id_];

    char* compress_buf = reinterpret_cast<char*>(BuddyThreadHeap::get_instance()->alloc(compress_buf_sz));
    char* compress_data = compress_buf;
    char* origin_buf = reinterpret_cast<char*>(BuddyThreadHeap::get_instance()->alloc(origin_buf_sz));

    uint64_t offset = meta->offset[col_id_];
    size_t compress_sz = meta->compress_sz[col_id_];

    if (LIKELY(buffer == nullptr) || buffer->empty() || offset + compress_sz <= buffer->FlushedSz()) {
      // 全部在文件里面
      struct stat st;
      fstat(file->fd(), &st);
      LOG_ASSERT(offset + compress_sz <= (uint64_t)st.st_size, "offset %lu read size %lu filesz %lu", offset,
                 compress_sz, st.st_size);

      auto read_off = rounddown512(offset);
      file->read(compress_data, compress_buf_sz, read_off);
      compress_data += (offset - read_off); // 偏移修正
    } else {
      if (offset >= buffer->FlushedSz()) {
        // 全部在buffer里面
        buffer->read(compress_data, compress_sz, offset);
      } else {
        LOG_ASSERT(!buffer->empty(), "buffer is empty");
        // 一部分在文件里面，一部分在buffer里面
        struct stat st;
        fstat(file->fd(), &st);
        // 先从文件读取
        auto read_off = rounddown512(offset);
        file->read(compress_data, roundup512(st.st_size - offset), read_off);
        compress_data += (offset - read_off);
        // 再从缓冲区读取
        buffer->read(compress_data + st.st_size - offset, compress_sz - (st.st_size - offset), st.st_size);
      }
    }

    auto ret = decompress_func(compress_data, origin_buf, compress_sz, origin_buf_sz);
    LOG_ASSERT(ret == (int)origin_buf_sz, "uncompress error");

    memcpy(lens_, origin_buf, sizeof(lens_[0]) * (meta->num));
    data_ = std::string(origin_buf + sizeof(lens_[0]) * (meta->num), origin_buf_sz - sizeof(lens_[0]) * meta->num);

    offset = 0;
    for (int i = 0; i < meta->num; i++) {
      offset += lens_[i];
      offsets_[i + 1] = offset;
    }

    BuddyThreadHeap::get_instance()->free_local(compress_buf);
    BuddyThreadHeap::get_instance()->free_local(origin_buf);
  }

  void Get(int idx, ColumnValue& value) {
    free(value.columnData);
    uint32_t off = offsets_[idx];
    uint32_t len = 0;
    len = offsets_[idx + 1] - offsets_[idx];
    LOG_ASSERT(len != 0, "len should not be equal 0");

    const std::string& res_str = data_.substr(off, len);
    value.columnType = COLUMN_TYPE_STRING;
    value.columnData = (char*)malloc(sizeof(int32_t) + res_str.size());
    *((int32_t*)value.columnData) = (int32_t)res_str.size();
    std::memcpy(value.columnData + sizeof(int32_t), res_str.data(), res_str.size());
  }

  void Reset() {
    offset_ = 0;
    offsets_[0] = 0;
    data_.clear();
  };

  const int col_id_;

private:
  uint32_t offset_;
  uint32_t offsets_[kMemtableRowNum + 1];
  uint16_t lens_[kMemtableRowNum];
  std::string data_;
};

class ColumnArrWrapper {
public:
  virtual ~ColumnArrWrapper() {}

  virtual void Add(const ColumnValue& col, int idx) = 0;

  virtual void Flush(AlignedWriteBuffer* buffer, int cnt, BlockMeta* meta) = 0;

  virtual void Read(File* file, AlignedWriteBuffer* buffer, BlockMeta* meta) = 0;

  virtual void Get(int idx, ColumnValue& value) = 0;

  virtual int64_t GetVal(int idx) = 0;

  virtual void Reset() = 0;

  virtual int GetColid() = 0;

  virtual size_t TotalSize() = 0;
};

class IntArrWrapper : public ColumnArrWrapper {
public:
  IntArrWrapper(int col_id) { arr = new ColumnArr<int>(col_id, COLUMN_TYPE_INTEGER); }

  ~IntArrWrapper() { delete arr; }

  void Add(const ColumnValue& col, int idx) override { arr->Add(col, idx); }

  void Flush(AlignedWriteBuffer* buffer, int cnt, BlockMeta* meta) override { arr->Flush(buffer, cnt, meta); }

  void Read(File* file, AlignedWriteBuffer* buffer, BlockMeta* meta) override { arr->Read(file, buffer, meta); }

  void Get(int idx, ColumnValue& value) override { arr->Get(idx, value); }

  int64_t GetVal(int idx) override { return arr->GetVal(idx); }

  void Reset() override { arr->Reset(); }

  int GetColid() override { return arr->col_id_; }

  size_t TotalSize() override { return 0; }

private:
  ColumnArr<int>* arr;
};

class DoubleArrWrapper : public ColumnArrWrapper {
public:
  DoubleArrWrapper(int col_id) { arr = new ColumnArr<double>(col_id, COLUMN_TYPE_DOUBLE_FLOAT); }

  ~DoubleArrWrapper() { delete arr; }

  void Add(const ColumnValue& col, int idx) override { arr->Add(col, idx); }

  void Flush(AlignedWriteBuffer* buffer, int cnt, BlockMeta* meta) override { arr->Flush(buffer, cnt, meta); }

  void Read(File* file, AlignedWriteBuffer* buffer, BlockMeta* meta) override { arr->Read(file, buffer, meta); }

  void Get(int idx, ColumnValue& value) override { arr->Get(idx, value); }

  int64_t GetVal(int idx) override { return arr->GetVal(idx); }

  void Reset() override { arr->Reset(); }

  int GetColid() override { return arr->col_id_; }

  size_t TotalSize() override { return 0; }

private:
  ColumnArr<double>* arr;
};

class StringArrWrapper : public ColumnArrWrapper {
public:
  StringArrWrapper(int col_id) { arr = new ColumnArr<std::string>(col_id); }

  ~StringArrWrapper() { delete arr; }

  void Add(const ColumnValue& col, int idx) override { arr->Add(col, idx); }

  void Flush(AlignedWriteBuffer* buffer, int cnt, BlockMeta* meta) override { arr->Flush(buffer, cnt, meta); }

  void Read(File* file, AlignedWriteBuffer* buffer, BlockMeta* meta) override { arr->Read(file, buffer, meta); }

  void Get(int idx, ColumnValue& value) override { arr->Get(idx, value); }

  int64_t GetVal(int idx) override {
    LOG_ASSERT(false, "Not implemented");
    return -1;
  }

  void Reset() override { arr->Reset(); }

  int GetColid() override { return arr->col_id_; }

  size_t TotalSize() override { return 0; }

private:
  ColumnArr<std::string>* arr;
};

class VidArrWrapper : public ColumnArrWrapper {
public:
  VidArrWrapper(int col_id) { arr = new ColumnArr<uint16_t>(col_id, COLUMN_TYPE_INTEGER); }

  ~VidArrWrapper() { delete arr; }

  void Add(const ColumnValue& col, int idx) override { arr->Add(col, idx); }

  void Flush(AlignedWriteBuffer* buffer, int cnt, BlockMeta* meta) override { arr->Flush(buffer, cnt, meta); }

  void Read(File* file, AlignedWriteBuffer* buffer, BlockMeta* meta) override { arr->Read(file, buffer, meta); }

  void Get(int idx, ColumnValue& value) override { arr->Get(idx, value); }

  int64_t GetVal(int idx) override { return arr->GetVal(idx); }

  void Reset() override { arr->Reset(); }

  int GetColid() override { return arr->col_id_; }

  uint16_t* GetDataArr() { return arr->data_; }

  size_t TotalSize() override { return 0; }

private:
  ColumnArr<uint16_t>* arr;
};

class TsArrWrapper : public ColumnArrWrapper {
public:
  TsArrWrapper(int col_id) { arr = new ColumnArr<int64_t>(col_id, COLUMN_TYPE_DOUBLE_FLOAT); }

  ~TsArrWrapper() { delete arr; }

  void Add(const ColumnValue& col, int idx) override { arr->Add(col, idx); }

  void Flush(AlignedWriteBuffer* buffer, int cnt, BlockMeta* meta) override { arr->Flush(buffer, cnt, meta); }

  void Read(File* file, AlignedWriteBuffer* buffer, BlockMeta* meta) override { arr->Read(file, buffer, meta); }

  void Get(int idx, ColumnValue& value) override { arr->Get(idx, value); }

  int64_t GetVal(int idx) override { return arr->GetVal(idx); }

  void Reset() override { arr->Reset(); }

  int GetColid() override { return arr->col_id_; }

  int64_t* GetDataArr() { return arr->data_; }

  size_t TotalSize() override { return 0; }

private:
  ColumnArr<int64_t>* arr;
};

class IdxArrWrapper : public ColumnArrWrapper {
public:
  IdxArrWrapper(int col_id) { arr = new ColumnArr<uint16_t>(col_id, COLUMN_TYPE_INTEGER); }

  ~IdxArrWrapper() { delete arr; }

  void Add(const ColumnValue& col, int idx) override { arr->Add(col, idx); }

  void Flush(AlignedWriteBuffer* buffer, int cnt, BlockMeta* meta) override { arr->Flush(buffer, cnt, meta); }

  void Read(File* file, AlignedWriteBuffer* buffer, BlockMeta* meta) override { arr->Read(file, buffer, meta); }

  void Get(int idx, ColumnValue& value) override { arr->Get(idx, value); }

  int64_t GetVal(int idx) override { return arr->GetVal(idx); }

  void Reset() override { arr->Reset(); }

  int GetColid() override { return arr->col_id_; }

  uint16_t* GetDataArr() { return arr->data_; }

  size_t TotalSize() override { return 0; }

private:
  ColumnArr<uint16_t>* arr;
};

} // namespace LindormContest
