#pragma once

#include "ShardBlockMetaManager.h"
#include "common.h"
#include "compress.h"
#include "io/file_manager.h"
#include "struct/ColumnValue.h"
#include "util/logging.h"
#include "util/stat.h"
#include <cstdint>
#include <cstring>
#include <new>
#include <string>
#include <sys/stat.h>

namespace LindormContest {

/**
 * 封装了每个列具体的处理过程，对外提供统一的接口，上层调用者不需要考虑列的类型
 */
template <typename T>
class ColumnArr {
public:
  ColumnArr(int col_id, ColumnType type) : col_id(col_id), type(type) {}
  virtual ~ColumnArr() {}

  void Add(const ColumnValue &col, int idx) {
    switch (col.getColumnType()) {
      case COLUMN_TYPE_STRING: LOG_ASSERT(false, "should not run here"); break;
      case COLUMN_TYPE_INTEGER: {
        int val;
        col.getIntegerValue(val);
        datas[idx] = (T) val;
        break;
      }
      case COLUMN_TYPE_DOUBLE_FLOAT: {
        double val;
        col.getDoubleFloatValue(val);
        datas[idx] = (T) val;
        break;
      }
      case COLUMN_TYPE_UNINITIALIZED: break;
    }
  }

  // 元数据直接写到内存，内存里面的元数据在shutdown的时候会持久化的
  void Flush(File *file, int cnt, BlockMeta *meta) {
    uint64_t offset = file->GetFileSz();
    uint64_t input_sz = cnt * sizeof(T);
    uint64_t compress_buf_sz = max_dest_size_func(input_sz);
    char compress_buf[compress_buf_sz];
    uint64_t compress_sz = compress_func((const char *) datas, input_sz, compress_buf, compress_buf_sz);
    auto ret = file->write((const char *) compress_buf, compress_sz);
    LOG_ASSERT(ret == Status::OK, "write failed");

    meta->offset[col_id] = offset;
    meta->origin_sz[col_id] = input_sz;
    meta->compress_sz[col_id] = compress_sz;
    RECORD_ARR_FETCH_ADD(origin_szs, col_id, input_sz);
    RECORD_ARR_FETCH_ADD(compress_szs, col_id, compress_sz);
  }

  void Read(File *file, BlockMeta *meta) {
    LOG_ASSERT(meta != nullptr, "error");
    char compress_data_buf[meta->compress_sz[col_id]];
    uint64_t offset = meta->offset[col_id];

    struct stat st;
    fstat(file->Fd(), &st);
    LOG_ASSERT(offset + meta->compress_sz[col_id] <= (uint64_t) st.st_size, "offset %lu read size %lu filesz %lu", offset, meta->compress_sz[col_id], st.st_size);

    file->read(compress_data_buf, meta->compress_sz[col_id], offset);
    auto ret = decompress_func(compress_data_buf, (char *) datas, meta->compress_sz[col_id], meta->origin_sz[col_id]);
    LOG_ASSERT(ret == (int) meta->origin_sz[col_id], "uncompress error");
  }

  void Get(int idx, ColumnValue &value) {
    switch (type) {
      case COLUMN_TYPE_STRING: LOG_ASSERT(false, "should not run here");
      case COLUMN_TYPE_INTEGER: {
        value.columnType = COLUMN_TYPE_INTEGER;
        value.columnData = (char *) malloc(sizeof(int32_t));
        *((int32_t *) value.columnData) = (int32_t) datas[idx];
        return;
      }
      case COLUMN_TYPE_DOUBLE_FLOAT: {
        value.columnType = COLUMN_TYPE_DOUBLE_FLOAT;
        value.columnData = (char *) malloc(sizeof(double));
        *((double *) value.columnData) = (double) datas[idx];
        return;
      }
      case COLUMN_TYPE_UNINITIALIZED: LOG_ASSERT(false, "should not run here");
    }
    LOG_ASSERT(false, "should not run here");
  }

  int64_t GetVal(int idx) { return datas[idx]; }

  void Reset(){};

  const int col_id;

private:
  T datas[kMemtableRowNum];
  ColumnType type;
};

template <>
class ColumnArr<std::string> {
public:
  ColumnArr(int col_id) : col_id(col_id), offset(0) {}
  ~ColumnArr() {}

  void Add(const ColumnValue &col, int idx) {
    LOG_ASSERT(col.getColumnType() == COLUMN_TYPE_STRING, "column type is %d", col.getColumnType());

    std::pair<int32_t, const char *> pair;
    col.getStringValue(pair);
    std::string str(pair.second, pair.first);

    offsets[idx] = offset;
    datas.append(str);

    offset += str.size();
    offsets[idx + 1] = offset;
  }

  // 元数据直接写到内存，内存里面的元数据在shutdown的时候会持久化的
  void Flush(File *file, int cnt, BlockMeta *meta) {
    uint64_t offset = file->GetFileSz();
    uint64_t writesz1 = cnt * sizeof(offsets[0]);
    uint64_t writesz2 = datas.size();
    uint64_t input_sz = writesz1 + writesz2;
    char *origin = new char[input_sz];
    memcpy(origin, offsets, writesz1);
    memcpy(origin + writesz1, datas.c_str(), writesz2);
    uint64_t compress_buf_sz = max_dest_size_func(input_sz);
    char *compress_buf = new char[compress_buf_sz];
    uint64_t compress_sz = compress_func((const char *) origin, input_sz, compress_buf, compress_buf_sz);

    auto ret = file->write((const char *) compress_buf, compress_sz);
    delete[] origin;
    delete[] compress_buf;
    LOG_ASSERT(ret == Status::OK, "write failed");

    meta->offset[col_id] = offset;
    meta->origin_sz[col_id] = input_sz;
    meta->compress_sz[col_id] = compress_sz;
    RECORD_ARR_FETCH_ADD(origin_szs, col_id, input_sz);
    RECORD_ARR_FETCH_ADD(compress_szs, col_id, compress_sz);
  }

  void Read(File *file, BlockMeta *meta) {
    LOG_ASSERT(meta != nullptr, "error");
    char *compress_data_buf = new char[meta->compress_sz[col_id]];
    char *origin_data_buf = new char[meta->origin_sz[col_id]];
    uint64_t offset = meta->offset[col_id];
    file->read(compress_data_buf, meta->compress_sz[col_id], offset);
    auto ret = decompress_func(compress_data_buf, origin_data_buf, meta->compress_sz[col_id], meta->origin_sz[col_id]);
    LOG_ASSERT(ret == (int) meta->origin_sz[col_id], "uncompress error");
    memcpy(offsets, origin_data_buf, sizeof(offsets[0]) * meta->num);
    datas = std::string(origin_data_buf + sizeof(offsets[0]) * meta->num, meta->origin_sz[col_id] - sizeof(offsets[0]) * meta->num);
    delete[] compress_data_buf;
    delete[] origin_data_buf;
  }

  void Get(int idx, ColumnValue &value) {
    uint32_t off = offsets[idx];
    uint32_t len = 0;
    len = offsets[idx + 1] - offsets[idx];
    LOG_ASSERT(len != 0, "len should not be equal 0");

    const std::string &res_str = datas.substr(off, len);
    value.columnType = COLUMN_TYPE_STRING;
    value.columnData = (char *) malloc(sizeof(int32_t) + res_str.size());
    *((int32_t *) value.columnData) = (int32_t) res_str.size();
    std::memcpy(value.columnData + sizeof(int32_t), res_str.data(), res_str.size());
  }

  void Reset() {
    offset = 0;
    datas.clear();
  };

  const int col_id;

private:
  uint32_t offset;
  uint32_t offsets[kMemtableRowNum + 1];
  std::string datas;
};

class ColumnArrWrapper {
public:
  virtual ~ColumnArrWrapper() {}

  virtual void Add(const ColumnValue &col, int idx) = 0;

  virtual void Flush(File *file, int cnt, BlockMeta *meta) = 0;

  virtual void Read(File *file, BlockMeta *meta) = 0;

  // TODO: 减少拷贝
  virtual void Get(int idx, ColumnValue &value) = 0;

  virtual int64_t GetVal(int idx) = 0;

  virtual void Reset() = 0;

  virtual int GetColid() = 0;
};

class IntArrWrapper : public ColumnArrWrapper {
public:
  IntArrWrapper(int col_id) { arr = new ColumnArr<int>(col_id, COLUMN_TYPE_INTEGER); }

  ~IntArrWrapper() { delete arr; }

  void Add(const ColumnValue &col, int idx) override { arr->Add(col, idx); }

  void Flush(File *file, int cnt, BlockMeta *meta) override { arr->Flush(file, cnt, meta); }

  void Read(File *file, BlockMeta *meta) override { arr->Read(file, meta); }

  void Get(int idx, ColumnValue &value) override { arr->Get(idx, value); }

  int64_t GetVal(int idx) override { return arr->GetVal(idx); }

  void Reset() override { arr->Reset(); }

  int GetColid() override { return arr->col_id; }

private:
  ColumnArr<int> *arr;
};

class DoubleArrWrapper : public ColumnArrWrapper {
public:
  DoubleArrWrapper(int col_id) { arr = new ColumnArr<double>(col_id, COLUMN_TYPE_DOUBLE_FLOAT); }

  ~DoubleArrWrapper() { delete arr; }

  void Add(const ColumnValue &col, int idx) override { arr->Add(col, idx); }

  void Flush(File *file, int cnt, BlockMeta *meta) override { arr->Flush(file, cnt, meta); }

  void Read(File *file, BlockMeta *meta) override { arr->Read(file, meta); }

  void Get(int idx, ColumnValue &value) override { arr->Get(idx, value); }

  int64_t GetVal(int idx) override { return arr->GetVal(idx); }

  void Reset() override { arr->Reset(); }

  int GetColid() override { return arr->col_id; }

private:
  ColumnArr<double> *arr;
};

class StringArrWrapper : public ColumnArrWrapper {
public:
  StringArrWrapper(int col_id) { arr = new ColumnArr<std::string>(col_id); }

  ~StringArrWrapper() { delete arr; }

  void Add(const ColumnValue &col, int idx) override { arr->Add(col, idx); }

  void Flush(File *file, int cnt, BlockMeta *meta) override { arr->Flush(file, cnt, meta); }

  void Read(File *file, BlockMeta *meta) override { arr->Read(file, meta); }

  void Get(int idx, ColumnValue &value) override { arr->Get(idx, value); }

  int64_t GetVal(int idx) override {
    LOG_ASSERT(false, "Not implemented");
    return -1;
  }

  void Reset() override { arr->Reset(); }

  int GetColid() override { return arr->col_id; }

private:
  ColumnArr<std::string> *arr;
};

class VidArrWrapper : public ColumnArrWrapper {
public:
  VidArrWrapper(int col_id) { arr = new ColumnArr<uint16_t>(col_id, COLUMN_TYPE_INTEGER); }

  ~VidArrWrapper() { delete arr; }

  void Add(const ColumnValue &col, int idx) override { arr->Add(col, idx); }

  void Flush(File *file, int cnt, BlockMeta *meta) override { arr->Flush(file, cnt, meta); }

  void Read(File *file, BlockMeta *meta) override { arr->Read(file, meta); }

  void Get(int idx, ColumnValue &value) override { arr->Get(idx, value); }

  int64_t GetVal(int idx) override { return arr->GetVal(idx); }

  void Reset() override { arr->Reset(); }

  int GetColid() override { return arr->col_id; }

private:
  ColumnArr<uint16_t> *arr;
};

class TsArrWrapper : public ColumnArrWrapper {
public:
  TsArrWrapper(int col_id) { arr = new ColumnArr<int64_t>(col_id, COLUMN_TYPE_DOUBLE_FLOAT); }

  ~TsArrWrapper() { delete arr; }

  void Add(const ColumnValue &col, int idx) override { arr->Add(col, idx); }

  void Flush(File *file, int cnt, BlockMeta *meta) override { arr->Flush(file, cnt, meta); }

  void Read(File *file, BlockMeta *meta) override { arr->Read(file, meta); }

  void Get(int idx, ColumnValue &value) override { arr->Get(idx, value); }

  int64_t GetVal(int idx) override { return arr->GetVal(idx); }

  void Reset() override { arr->Reset(); }

  int GetColid() override { return arr->col_id; }

private:
  ColumnArr<int64_t> *arr;
};

} // namespace LindormContest
