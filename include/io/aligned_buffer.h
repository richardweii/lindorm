#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#include "common.h"
#include "io/file.h"
#include "util/logging.h"
#include "util/mem_pool.h"

namespace LindormContest {

/**
 * 512字节对齐的文件缓冲区buffer，用于异步写IO
 */
class AlignedWriteBuffer {
public:
  AlignedWriteBuffer(File* file) : file_(file) { posix_memalign(&buffer_, 512, kWriteBufferSize); }

  AlignedWriteBuffer(File* file, size_t buffer_size) : file_(file) { posix_memalign(&buffer_, 512, buffer_size); }

  virtual ~AlignedWriteBuffer() {
    if (buffer_ != nullptr) {
      free(buffer_);
    }
  }

  void write(char* compressed_data, int len, OUT uint64_t& file_offset) {
    LOG_ASSERT(offset_ <= kWriteBufferSize, "offset = %d", offset_);

    file_offset = flush_blk_num_ * kWriteBufferSize + offset_;

    while (len != 0) {
      int remain = kWriteBufferSize - offset_;
      int copy_len = (remain >= len) ? len : remain;

      memcpy((char*)buffer_ + offset_, compressed_data, copy_len);

      offset_ += copy_len;
      LOG_ASSERT(offset_ <= kWriteBufferSize, "offset = %d", offset_);

      // buffer满了，下刷
      if (offset_ == kWriteBufferSize) {
        // 异步写
        auto rc = file_->write((const char*)buffer_, kWriteBufferSize);
        LOG_ASSERT(rc == Status::OK, "async write io buffer failed.");
        flush_blk_num_ += 1;
        offset_ = 0;
      }

      compressed_data += copy_len;
      len -= copy_len;
    }
  }

  // 写阶段需要从未下刷完毕的buffer中读取数据
  void read(char* res_buf, size_t length, off_t off) {
    LOG_ASSERT((uint64_t)(off % kWriteBufferSize) + (uint64_t)length <= (uint64_t)offset_,
               "(pos mod kWriteBufferSize) = %ld, offset = %d", (off % kWriteBufferSize), offset_);
    memcpy(res_buf, (char*)buffer_ + (off % kWriteBufferSize), length);
  }

  void flush() {
    AppendWriteFile f(file_->getFileName(), NORMAL_FLAG);
    f.write((const char*)buffer_, offset_);
    offset_ = 0;
  }

  bool empty() { return offset_ == 0; }

  const size_t FlushedSz() const { return flush_blk_num_ * kWriteBufferSize; }

private:
  void* buffer_ = nullptr;
  int flush_blk_num_ = 0; // 已经下刷了多少个块了
  int offset_ = 0;
  File* file_;
};

} // namespace LindormContest
