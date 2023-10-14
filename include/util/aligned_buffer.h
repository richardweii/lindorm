#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#include "common.h"
#include "io/file.h"
#include "util/logging.h"

namespace LindormContest {

/**
 * 512字节对齐的buffer，用于直接IO下刷磁盘
 */
class AlignedBuffer {
public:
  AlignedBuffer(File* file) : file_(file) {
    posix_memalign(&buffer_, 512, kAlignedBufferSize);
    posix_memalign(&flush_buffer_, 512, kAlignedBufferSize);
  }

  virtual ~AlignedBuffer() {
    free(buffer_);
    free(flush_buffer_);
  }

  void Add(char* compressed_data, int len, OUT uint64_t& file_offset) {
    LOG_ASSERT(offset_ <= kAlignedBufferSize, "offset = %d", offset_);

    file_offset = flush_blk_num_ * kAlignedBufferSize + offset_;

    int remain = kAlignedBufferSize - offset_;
    int copy_len = (remain >= len) ? len : remain;

    memcpy((char*)buffer_ + offset_, compressed_data, copy_len);

    offset_ += copy_len;
    LOG_ASSERT(offset_ <= kAlignedBufferSize, "offset = %d", offset_);

    // buffer满了，下刷
    if (offset_ == kAlignedBufferSize) {
      while (flushing_) {
        // 循环，直到上一个buffer flush完成
      }
      flushing_ = true;
      std::swap(flush_buffer_, buffer_);
      flush_blk_num_ += 1;
      offset_ = 0;
      if (len > copy_len) {
        // 数据还有部分没有拷贝完成
        memcpy(buffer_, compressed_data + copy_len, len - copy_len);
        offset_ += (len - copy_len);
        LOG_ASSERT(offset_ <= kAlignedBufferSize, "offset = %d", offset_);
      }

      // 异步写
      file_->asyncWrite((const char*)flush_buffer_, kAlignedBufferSize);
      flushing_ = false;
    }
  }

  void Read(char* res_buf, size_t length, __off_t pos) {
    LOG_ASSERT((uint64_t)(pos % kAlignedBufferSize) + (uint64_t)length <= (uint64_t)offset_,
               "(pos mod kAlignedBufferSize) = %ld, offset = %d", (pos % kAlignedBufferSize), offset_);
    memcpy(res_buf, (char*)buffer_ + (pos % kAlignedBufferSize), length);
  }

  void Flush() {
    while (flushing_) {
      // 循环，直到上一个buffer flush完成
    }
    flushing_ = true;
    AppendWriteFile f(file_->getFileName(), NORMAL_FLAG);
    f.write((const char*)buffer_, offset_);
    flushing_ = false;
    offset_ = 0;
  }

  bool empty() { return offset_ == 0; }

  const int GetFlushBlkNum() const { return flush_blk_num_; }

private:
  volatile bool flushing_ = false;
  void* buffer_ = nullptr;
  void* flush_buffer_ = nullptr;
  int flush_blk_num_ = 0; // 已经下刷了多少个块了
  int offset_ = 0;
  File* file_;
};

} // namespace LindormContest
