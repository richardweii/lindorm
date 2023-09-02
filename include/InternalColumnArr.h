#pragma once

#include <sys/stat.h>

#include <cstdint>
#include <cstring>
#include <new>
#include <string>

#include "ShardBlockMetaManager.h"
#include "common.h"
#include "compress.h"
#include "io/file_manager.h"
#include "struct/ColumnValue.h"
#include "util/aligned_buffer.h"
#include "util/logging.h"
#include "util/stat.h"

namespace LindormContest {

/**
 * 封装了每个列具体的处理过程，对外提供统一的接口，上层调用者不需要考虑列的类型
 */
template <typename T>
class ColumnArr {
public:
  ColumnArr(int col_id, ColumnType type) : col_id(col_id), type(type) {
    // switch (type) {
    //   case COLUMN_TYPE_STRING:
    //     LOG_ASSERT(false, "should not run here");
    //     break;
    //   case COLUMN_TYPE_INTEGER: {
    //     max = (T) INT64_MIN;
    //     break;
    //   }
    //   case COLUMN_TYPE_DOUBLE_FLOAT: {
    //     max = (T) -999999.0;
    //     break;
    //   }
    //   case COLUMN_TYPE_UNINITIALIZED:
    //     break;
    // }
  }
  virtual ~ColumnArr() {}

  void Add(const ColumnValue& col, int idx) {
    switch (col.getColumnType()) {
      case COLUMN_TYPE_STRING:
        LOG_ASSERT(false, "should not run here");
        break;
      case COLUMN_TYPE_INTEGER: {
        int val;
        col.getIntegerValue(val);
        datas[idx] = (T)val;
        // if (val > max) {
        //   max = val;
        // }
        break;
      }
      case COLUMN_TYPE_DOUBLE_FLOAT: {
        double val;
        col.getDoubleFloatValue(val);
        datas[idx] = (T)val;
        // if (val > max) {
        //   max = val;
        // }
        break;
      }
      case COLUMN_TYPE_UNINITIALIZED:
        break;
    }
  }

  // 元数据直接写到内存，内存里面的元数据在shutdown的时候会持久化的
  void Flush(AlignedBuffer* buffer, int cnt, BlockMeta* meta) {
    uint64_t offset;
    uint64_t input_sz = cnt * sizeof(T);
    uint64_t compress_buf_sz = max_dest_size_func(input_sz);
    char compress_buf[compress_buf_sz];
    uint64_t compress_sz = compress_func((const char*)datas, input_sz, compress_buf, compress_buf_sz);

    buffer->Add(compress_buf, compress_sz, offset);

    meta->offset[col_id] = offset;
    meta->origin_sz[col_id] = input_sz;
    meta->compress_sz[col_id] = compress_sz;
    RECORD_ARR_FETCH_ADD(origin_szs, col_id, input_sz);
    RECORD_ARR_FETCH_ADD(compress_szs, col_id, compress_sz);
  }

  void Read(File* file, AlignedBuffer* buffer, BlockMeta* meta) {
    LOG_ASSERT(meta != nullptr, "error");
    char compress_data_buf[meta->compress_sz[col_id]];
    uint64_t offset = meta->offset[col_id];

    if (buffer->empty()) {
      // 全部在文件里面
      struct stat st;
      fstat(file->Fd(), &st);
      LOG_ASSERT(offset + meta->compress_sz[col_id] <= (uint64_t)st.st_size, "offset %lu read size %lu filesz %lu",
                 offset, meta->compress_sz[col_id], st.st_size);

      file->read(compress_data_buf, meta->compress_sz[col_id], offset);
    } else {
      if (offset >= (uint64_t)buffer->GetFlushBlkNum() * (uint64_t)kAlignedBufferSize) {
        // 全部在buffer里面
        buffer->Read(compress_data_buf, meta->compress_sz[col_id], offset);
      } else if (offset + meta->compress_sz[col_id] <=
                 (uint64_t)buffer->GetFlushBlkNum() * (uint64_t)kAlignedBufferSize) {
        // 全部在文件里面
        struct stat st;
        fstat(file->Fd(), &st);
        LOG_ASSERT(offset + meta->compress_sz[col_id] <= (uint64_t)st.st_size, "offset %lu read size %lu filesz %lu",
                   offset, meta->compress_sz[col_id], st.st_size);

        file->read(compress_data_buf, meta->compress_sz[col_id], offset);
      } else {
        LOG_ASSERT(!buffer->empty(), "buffer is empty");
        // 一部分在文件里面，一部分在buffer里面
        struct stat st;
        fstat(file->Fd(), &st);
        file->read(compress_data_buf, st.st_size - offset, offset);
        buffer->Read(compress_data_buf + st.st_size - offset, meta->compress_sz[col_id] - (st.st_size - offset),
                     st.st_size);
      }
    }

    auto ret = decompress_func(compress_data_buf, (char*)datas, meta->compress_sz[col_id], meta->origin_sz[col_id]);
    LOG_ASSERT(ret == (int)meta->origin_sz[col_id], "uncompress error");
  }

  void Get(int idx, ColumnValue& value) {
    switch (type) {
      case COLUMN_TYPE_STRING:
        LOG_ASSERT(false, "should not run here");
      case COLUMN_TYPE_INTEGER: {
        if (value.getColumnType() != COLUMN_TYPE_INTEGER) {
          free(value.columnData);
          value.columnType = COLUMN_TYPE_INTEGER;
          value.columnData = (char*)malloc(sizeof(int32_t));
          *((int32_t*)value.columnData) = (int32_t)datas[idx];
        } else {
          *((int32_t*)value.columnData) = (int32_t)datas[idx];
        }
        return;
      }
      case COLUMN_TYPE_DOUBLE_FLOAT: {
        if (value.getColumnType() != COLUMN_TYPE_DOUBLE_FLOAT) {
          free(value.columnData);
          value.columnType = COLUMN_TYPE_DOUBLE_FLOAT;
          value.columnData = (char*)malloc(sizeof(double));
          *((double*)value.columnData) = (double)datas[idx];
        } else {
          *((double*)value.columnData) = (double)datas[idx];
        }
        return;
      }
      case COLUMN_TYPE_UNINITIALIZED:
        LOG_ASSERT(false, "should not run here");
    }
    LOG_ASSERT(false, "should not run here");
  }

  int64_t GetVal(int idx) { return datas[idx]; }

  void Reset(){};

  const int col_id;

  T datas[kMemtableRowNum];
  // T max;
  ColumnType type;
};

template <>
class ColumnArr<std::string> {
public:
  ColumnArr(int col_id) : col_id(col_id), offset(0) { offsets[0] = 0; }
  ~ColumnArr() {}

  void Add(const ColumnValue& col, int idx) {
    LOG_ASSERT(col.getColumnType() == COLUMN_TYPE_STRING, "column type is %d", col.getColumnType());

    std::pair<int32_t, const char*> pair;
    col.getStringValue(pair);
    std::string str(pair.second, pair.first);

    datas.append(str);

    offset += pair.first;
    offsets[idx + 1] = offset;
    lens[idx] = pair.first;
  }

  // 元数据直接写到内存，内存里面的元数据在shutdown的时候会持久化的
  void Flush(AlignedBuffer* buffer, int cnt, BlockMeta* meta) {
    uint64_t offset;
    uint64_t writesz1 = cnt * sizeof(lens[0]);
    uint64_t writesz2 = datas.size();
    uint64_t input_sz = writesz1 + writesz2;
    char* origin = new char[input_sz];
    memcpy(origin, lens, writesz1);
    memcpy(origin + writesz1, datas.c_str(), writesz2);
    uint64_t compress_buf_sz = max_dest_size_func(input_sz);
    char* compress_buf = new char[compress_buf_sz];
    uint64_t compress_sz = compress_func((const char*)origin, input_sz, compress_buf, compress_buf_sz);

    uint64_t off;
    buffer->Add(compress_buf, compress_sz, off);

    delete[] origin;
    delete[] compress_buf;

    meta->offset[col_id] = off;
    meta->origin_sz[col_id] = input_sz;
    meta->compress_sz[col_id] = compress_sz;
    RECORD_ARR_FETCH_ADD(origin_szs, col_id, input_sz);
    RECORD_ARR_FETCH_ADD(compress_szs, col_id, compress_sz);
  }

  void Read(File* file, AlignedBuffer* buffer, BlockMeta* meta) {
    LOG_ASSERT(meta != nullptr, "error");
    char* compress_data_buf = new char[meta->compress_sz[col_id]];
    char* origin_data_buf = new char[meta->origin_sz[col_id]];
    uint64_t offset = meta->offset[col_id];

    if (buffer->empty()) {
      // 全部在文件里面
      struct stat st;
      fstat(file->Fd(), &st);
      LOG_ASSERT(offset + meta->compress_sz[col_id] <= (uint64_t)st.st_size, "offset %lu read size %lu filesz %lu",
                 offset, meta->compress_sz[col_id], st.st_size);

      file->read(compress_data_buf, meta->compress_sz[col_id], offset);
    } else {
      if (offset >= (uint64_t)buffer->GetFlushBlkNum() * (uint64_t)kAlignedBufferSize) {
        // 全部在buffer里面
        buffer->Read(compress_data_buf, meta->compress_sz[col_id], offset);
      } else if (offset + meta->compress_sz[col_id] <=
                 (uint64_t)buffer->GetFlushBlkNum() * (uint64_t)kAlignedBufferSize) {
        // 全部在文件里面
        struct stat st;
        fstat(file->Fd(), &st);
        LOG_ASSERT(offset + meta->compress_sz[col_id] <= (uint64_t)st.st_size, "offset %lu read size %lu filesz %lu",
                   offset, meta->compress_sz[col_id], st.st_size);

        file->read(compress_data_buf, meta->compress_sz[col_id], offset);
      } else {
        LOG_ASSERT(!buffer->empty(), "buffer is empty");
        // 一部分在文件里面，一部分在buffer里面
        struct stat st;
        fstat(file->Fd(), &st);
        file->read(compress_data_buf, st.st_size - offset, offset);
        buffer->Read(compress_data_buf + st.st_size - offset, meta->compress_sz[col_id] - (st.st_size - offset),
                     st.st_size);
      }
    }

    auto ret = decompress_func(compress_data_buf, origin_data_buf, meta->compress_sz[col_id], meta->origin_sz[col_id]);
    LOG_ASSERT(ret == (int)meta->origin_sz[col_id], "uncompress error");

    memcpy(lens, origin_data_buf, sizeof(lens[0]) * (meta->num));
    datas = std::string(origin_data_buf + sizeof(lens[0]) * (meta->num),
                        meta->origin_sz[col_id] - sizeof(lens[0]) * meta->num);

    offset = 0;
    for (int i = 0; i < meta->num; i++) {
      offset += lens[i];
      offsets[i+1] = offset;
    }

    delete[] compress_data_buf;
    delete[] origin_data_buf;
  }

  void Get(int idx, ColumnValue& value) {
    free(value.columnData);
    uint32_t off = offsets[idx];
    uint32_t len = 0;
    len = offsets[idx + 1] - offsets[idx];
    LOG_ASSERT(len != 0, "len should not be equal 0");

    const std::string& res_str = datas.substr(off, len);
    value.columnType = COLUMN_TYPE_STRING;
    value.columnData = (char*)malloc(sizeof(int32_t) + res_str.size());
    *((int32_t*)value.columnData) = (int32_t)res_str.size();
    std::memcpy(value.columnData + sizeof(int32_t), res_str.data(), res_str.size());
  }

  void Reset() {
    offset = 0;
    offsets[0] = 0;
    datas.clear();
  };

  const int col_id;

private:
  uint32_t offset;
  uint32_t offsets[kMemtableRowNum + 1];
  uint16_t lens[kMemtableRowNum];
  std::string datas;
};

class ColumnArrWrapper {
public:
  virtual ~ColumnArrWrapper() {}

  virtual void Add(const ColumnValue& col, int idx) = 0;

  virtual void Flush(AlignedBuffer* buffer, int cnt, BlockMeta* meta) = 0;

  virtual void Read(File* file, AlignedBuffer* buffer, BlockMeta* meta) = 0;

  // TODO: 减少拷贝
  virtual void Get(int idx, ColumnValue& value) = 0;

  virtual int64_t GetVal(int idx) = 0;

  virtual void Reset() = 0;

  virtual int GetColid() = 0;
};

class IntArrWrapper : public ColumnArrWrapper {
public:
  IntArrWrapper(int col_id) { arr = new ColumnArr<int>(col_id, COLUMN_TYPE_INTEGER); }

  ~IntArrWrapper() { delete arr; }

  void Add(const ColumnValue& col, int idx) override { arr->Add(col, idx); }

  void Flush(AlignedBuffer* buffer, int cnt, BlockMeta* meta) override { arr->Flush(buffer, cnt, meta); }

  void Read(File* file, AlignedBuffer* buffer, BlockMeta* meta) override { arr->Read(file, buffer, meta); }

  void Get(int idx, ColumnValue& value) override { arr->Get(idx, value); }

  int64_t GetVal(int idx) override { return arr->GetVal(idx); }

  void Reset() override { arr->Reset(); }

  int GetColid() override { return arr->col_id; }

private:
  ColumnArr<int>* arr;
};

class DoubleArrWrapper : public ColumnArrWrapper {
public:
  DoubleArrWrapper(int col_id) { arr = new ColumnArr<double>(col_id, COLUMN_TYPE_DOUBLE_FLOAT); }

  ~DoubleArrWrapper() { delete arr; }

  void Add(const ColumnValue& col, int idx) override { arr->Add(col, idx); }

  void Flush(AlignedBuffer* buffer, int cnt, BlockMeta* meta) override { arr->Flush(buffer, cnt, meta); }

  void Read(File* file, AlignedBuffer* buffer, BlockMeta* meta) override { arr->Read(file, buffer, meta); }

  void Get(int idx, ColumnValue& value) override { arr->Get(idx, value); }

  int64_t GetVal(int idx) override { return arr->GetVal(idx); }

  void Reset() override { arr->Reset(); }

  int GetColid() override { return arr->col_id; }

private:
  ColumnArr<double>* arr;
};

class StringArrWrapper : public ColumnArrWrapper {
public:
  StringArrWrapper(int col_id) { arr = new ColumnArr<std::string>(col_id); }

  ~StringArrWrapper() { delete arr; }

  void Add(const ColumnValue& col, int idx) override { arr->Add(col, idx); }

  void Flush(AlignedBuffer* buffer, int cnt, BlockMeta* meta) override { arr->Flush(buffer, cnt, meta); }

  void Read(File* file, AlignedBuffer* buffer, BlockMeta* meta) override { arr->Read(file, buffer, meta); }

  void Get(int idx, ColumnValue& value) override { arr->Get(idx, value); }

  int64_t GetVal(int idx) override {
    LOG_ASSERT(false, "Not implemented");
    return -1;
  }

  void Reset() override { arr->Reset(); }

  int GetColid() override { return arr->col_id; }

private:
  ColumnArr<std::string>* arr;
};

class VidArrWrapper : public ColumnArrWrapper {
public:
  VidArrWrapper(int col_id) { arr = new ColumnArr<uint16_t>(col_id, COLUMN_TYPE_INTEGER); }

  ~VidArrWrapper() { delete arr; }

  void Add(const ColumnValue& col, int idx) override { arr->Add(col, idx); }

  void Flush(AlignedBuffer* buffer, int cnt, BlockMeta* meta) override { arr->Flush(buffer, cnt, meta); }

  void Read(File* file, AlignedBuffer* buffer, BlockMeta* meta) override { arr->Read(file, buffer, meta); }

  void Get(int idx, ColumnValue& value) override { arr->Get(idx, value); }

  int64_t GetVal(int idx) override { return arr->GetVal(idx); }

  void Reset() override { arr->Reset(); }

  int GetColid() override { return arr->col_id; }

  uint16_t* GetDataArr() { return arr->datas; }

private:
  ColumnArr<uint16_t>* arr;
};

class TsArrWrapper : public ColumnArrWrapper {
public:
  TsArrWrapper(int col_id) { arr = new ColumnArr<int64_t>(col_id, COLUMN_TYPE_DOUBLE_FLOAT); }

  ~TsArrWrapper() { delete arr; }

  void Add(const ColumnValue& col, int idx) override { arr->Add(col, idx); }

  void Flush(AlignedBuffer* buffer, int cnt, BlockMeta* meta) override { arr->Flush(buffer, cnt, meta); }

  void Read(File* file, AlignedBuffer* buffer, BlockMeta* meta) override { arr->Read(file, buffer, meta); }

  void Get(int idx, ColumnValue& value) override { arr->Get(idx, value); }

  int64_t GetVal(int idx) override { return arr->GetVal(idx); }

  void Reset() override { arr->Reset(); }

  int GetColid() override { return arr->col_id; }

  int64_t* GetDataArr() { return arr->datas; }

private:
  ColumnArr<int64_t>* arr;
};

class IdxArrWrapper : public ColumnArrWrapper {
public:
  IdxArrWrapper(int col_id) { arr = new ColumnArr<uint16_t>(col_id, COLUMN_TYPE_INTEGER); }

  ~IdxArrWrapper() { delete arr; }

  void Add(const ColumnValue& col, int idx) override { arr->Add(col, idx); }

  void Flush(AlignedBuffer* buffer, int cnt, BlockMeta* meta) override { arr->Flush(buffer, cnt, meta); }

  void Read(File* file, AlignedBuffer* buffer, BlockMeta* meta) override { arr->Read(file, buffer, meta); }

  void Get(int idx, ColumnValue& value) override { arr->Get(idx, value); }

  int64_t GetVal(int idx) override { return arr->GetVal(idx); }

  void Reset() override { arr->Reset(); }

  int GetColid() override { return arr->col_id; }

  uint16_t* GetDataArr() { return arr->datas; }

private:
  ColumnArr<uint16_t>* arr;
};

} // namespace LindormContest
