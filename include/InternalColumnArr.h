#pragma once

#include "ShardBlockMetaManager.h"
#include "common.h"
#include "compress.h"
#include "io/file_manager.h"
#include "struct/ColumnValue.h"
#include "util/logging.h"
#include <cstdint>
#include <cstring>
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

  void Add(ColumnValue col, int idx) {
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
    uint64_t compress_buf_sz = LZ4MaxDestSize(input_sz);
    char compress_buf[compress_buf_sz];
    uint64_t compress_sz = LZ4Compress((const char *) datas, input_sz, compress_buf, compress_buf_sz);
    auto ret = file->write((const char *) compress_buf, compress_sz);
    LOG_ASSERT(ret == Status::OK, "write failed");

    meta->offset[col_id] = offset;
    meta->origin_sz[col_id] = input_sz;
    meta->compress_sz[col_id] = compress_sz;
  }

  void Read(File *file, BlockMeta *meta) {
    LOG_ASSERT(meta != nullptr, "error");
    char compress_data_buf[meta->compress_sz[col_id]];
    uint64_t offset = meta->offset[col_id];

    struct stat st;
    fstat(file->Fd(), &st);
    LOG_ASSERT(offset + meta->compress_sz[col_id] <= st.st_size, "offset %lu read size %lu filesz %lu", offset, meta->compress_sz[col_id], st.st_size);

    file->read(compress_data_buf, meta->compress_sz[col_id], offset);
    auto ret = LZ4DeCompress(compress_data_buf, (char *) datas, meta->compress_sz[col_id], meta->origin_sz[col_id]);
    LOG_ASSERT(ret == (int) meta->origin_sz[col_id], "uncompress error");
  }

  ColumnValue Get(int idx) {
    switch (type) {
      case COLUMN_TYPE_STRING: LOG_ASSERT(false, "should not run here");
      case COLUMN_TYPE_INTEGER: return ColumnValue((int32_t) datas[idx]);
      case COLUMN_TYPE_DOUBLE_FLOAT: return ColumnValue((double) datas[idx]);
      case COLUMN_TYPE_UNINITIALIZED: LOG_ASSERT(false, "should not run here");
    }
    return ColumnValue(-1);
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

  void Add(ColumnValue col, int idx) {
    LOG_ASSERT(col.getColumnType() == COLUMN_TYPE_STRING, "column type is %d", col.getColumnType());

    if (idx > cnt)
      cnt = idx;

    std::pair<int32_t, const char *> pair;
    col.getStringValue(pair);
    std::string str(pair.second, pair.first);

    offsets[idx] = offset;
    datas.append(str);

    offset += pair.first;
  }

  // 元数据直接写到内存，内存里面的元数据在shutdown的时候会持久化的
  void Flush(File *file, int cnt, BlockMeta *meta) {
    uint64_t offset = file->GetFileSz();
    uint64_t writesz1 = cnt * sizeof(uint16_t);
    uint64_t writesz2 = datas.size();
    uint64_t input_sz = writesz1 + writesz2;
    char origin[input_sz];
    memcpy(origin, offsets, writesz1);
    memcpy(origin + writesz1, datas.c_str(), writesz2);
    uint64_t compress_buf_sz = LZ4MaxDestSize(input_sz);
    char compress_buf[compress_buf_sz];
    uint64_t compress_sz = LZ4Compress((const char *) origin, input_sz, compress_buf, compress_buf_sz);

    auto ret = file->write((const char *) compress_buf, compress_sz);
    LOG_ASSERT(ret == Status::OK, "write failed");

    meta->offset[col_id] = offset;
    meta->origin_sz[col_id] = input_sz;
    meta->compress_sz[col_id] = compress_sz;
  }

  void Read(File *file, BlockMeta *meta) {
    LOG_ASSERT(meta != nullptr, "error");
    char compress_data_buf[meta->compress_sz[col_id]];
    char origin_data_buf[meta->origin_sz[col_id]];
    uint64_t offset = meta->offset[col_id];
    file->read(compress_data_buf, meta->compress_sz[col_id], offset);
    auto ret = LZ4DeCompress(compress_data_buf, origin_data_buf, meta->compress_sz[col_id], meta->origin_sz[col_id]);
    LOG_ASSERT(ret == (int) meta->origin_sz[col_id], "uncompress error");
    memcpy(offsets, origin_data_buf, sizeof(uint16_t) * meta->num);
    datas = std::string(origin_data_buf + sizeof(uint16_t) * meta->num, meta->origin_sz[col_id] - sizeof(uint16_t) * meta->num);
    cnt = meta->num;
  }

  ColumnValue Get(int idx) {
    uint16_t offset = offsets[idx];
    uint16_t len = 0;
    if (idx == cnt - 1) {
      len = datas.size() - offset;
    } else {
      len = offsets[idx + 1] - offsets[idx];
    }

    LOG_ASSERT(len != 0, "len should not be equal 0");

    return ColumnValue(datas.substr(offset, len));
  }

  void Reset() {
    offset = 0;
    datas.clear();
    cnt = 0;
  };

  const int col_id;

private:
  uint16_t offset;
  uint16_t offsets[kMemtableRowNum];
  std::string datas;
  int cnt = 0;
};

class ColumnArrWrapper {
public:
  virtual ~ColumnArrWrapper() {}

  virtual void Add(ColumnValue col, int idx) = 0;

  virtual void Flush(File *file, int cnt, BlockMeta *meta) = 0;

  virtual void Read(File *file, BlockMeta *meta) = 0;

  virtual ColumnValue Get(int idx) = 0;

  virtual int64_t GetVal(int idx) = 0;

  virtual void Reset() = 0;

  virtual int GetColid() = 0;
};

class IntArrWrapper : public ColumnArrWrapper {
public:
  IntArrWrapper(int col_id) { arr = new ColumnArr<int>(col_id, COLUMN_TYPE_INTEGER); }

  ~IntArrWrapper() { delete arr; }

  void Add(ColumnValue col, int idx) override { arr->Add(col, idx); }

  void Flush(File *file, int cnt, BlockMeta *meta) override { arr->Flush(file, cnt, meta); }

  void Read(File *file, BlockMeta *meta) override { arr->Read(file, meta); }

  ColumnValue Get(int idx) override { return arr->Get(idx); }

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

  void Add(ColumnValue col, int idx) override { arr->Add(col, idx); }

  void Flush(File *file, int cnt, BlockMeta *meta) override { arr->Flush(file, cnt, meta); }

  void Read(File *file, BlockMeta *meta) override { arr->Read(file, meta); }

  ColumnValue Get(int idx) override { return arr->Get(idx); }

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

  void Add(ColumnValue col, int idx) override { arr->Add(col, idx); }

  void Flush(File *file, int cnt, BlockMeta *meta) override { arr->Flush(file, cnt, meta); }

  void Read(File *file, BlockMeta *meta) override { arr->Read(file, meta); }

  ColumnValue Get(int idx) override { return arr->Get(idx); }

  int64_t GetVal(int idx) override { LOG_ASSERT(false, "Not implemented"); }

  void Reset() override { arr->Reset(); }

  int GetColid() override { return arr->col_id; }

private:
  ColumnArr<std::string> *arr;
};

class VidArrWrapper : public ColumnArrWrapper {
public:
  VidArrWrapper(int col_id) { arr = new ColumnArr<uint16_t>(col_id, COLUMN_TYPE_INTEGER); }

  ~VidArrWrapper() { delete arr; }

  void Add(ColumnValue col, int idx) override { arr->Add(col, idx); }

  void Flush(File *file, int cnt, BlockMeta *meta) override { arr->Flush(file, cnt, meta); }

  void Read(File *file, BlockMeta *meta) override { arr->Read(file, meta); }

  ColumnValue Get(int idx) override { return arr->Get(idx); }

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

  void Add(ColumnValue col, int idx) override { arr->Add(col, idx); }

  void Flush(File *file, int cnt, BlockMeta *meta) override { arr->Flush(file, cnt, meta); }

  void Read(File *file, BlockMeta *meta) override { arr->Read(file, meta); }

  ColumnValue Get(int idx) override { return arr->Get(idx); }

  int64_t GetVal(int idx) override { return arr->GetVal(idx); }

  void Reset() override { arr->Reset(); }

  int GetColid() override { return arr->col_id; }

private:
  ColumnArr<int64_t> *arr;
};

} // namespace LindormContest
