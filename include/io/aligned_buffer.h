#pragma once

#include <sys/stat.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#include "common.h"
#include "coroutine/coro_cond.h"
#include "io/file.h"
#include "util/logging.h"
#include "util/mem_pool.h"

namespace LindormContest {

/**
 * 512字节对齐的文件缓冲区buffer，用于异步写IO
 */
class AlignedWriteBuffer {
public:
  AlignedWriteBuffer(File* file) : file_(file) {
    buffer_ = std::aligned_alloc(512, kWriteBufferSize);
    ENSURE(buffer_ != nullptr, "std::aligned_alloc(512, kWriteBufferSize) failed");
  }

  AlignedWriteBuffer(File* file, size_t buffer_size) : file_(file) { posix_memalign(&buffer_, 512, buffer_size); }

  virtual ~AlignedWriteBuffer() {
    if (buffer_ != nullptr) {
      std::free(buffer_);
      buffer_ = nullptr;
    }
  }

  void write(char* compressed_data, int len, OUT uint64_t& file_offset) {
    while (in_flush_) {
      cv_.wait(); // 休眠当前协程，等待另一个协程对buf的flush结束
    }
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
        in_flush_ = true;
        auto rc = file_->write((const char*)buffer_, kWriteBufferSize);
        in_flush_ = false;
        cv_.notify(); // 通知其他协程开始flush数据
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
    auto rc = file_->write((const char*)buffer_, kWriteBufferSize);
    LOG_ASSERT(rc == Status::OK, "async write io buffer failed.");
    offset_ = 0;
    struct stat st;
    fstat(file_->fd(), &st);
    LOG_DEBUG("file name%s, file write size %lu, file sz %lu", file_->getFileName().c_str(), st.st_size,
              file_->getFileSz());
  }

  bool empty() { return offset_ == 0; }

  const size_t FlushedSz() const { return flush_blk_num_ * kWriteBufferSize; }

private:
  void* buffer_ = nullptr;
  int flush_blk_num_ = 0; // 已经下刷了多少个块了
  int offset_ = 0;
  File* file_;
  volatile bool in_flush_{false};
  CoroCV cv_;
};

} // namespace LindormContest
