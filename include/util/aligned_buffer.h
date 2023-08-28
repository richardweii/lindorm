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
  AlignedBuffer(File* file) : file(file) {
    posix_memalign(&buffer, 512, kAlignedBufferSize);
    posix_memalign(&flush_buffer, 512, kAlignedBufferSize);
  }

  virtual ~AlignedBuffer() {
    free(buffer);
    free(flush_buffer);
  }

  void Add(char* compressed_data, int len, OUT uint64_t& file_offset) {
    LOG_ASSERT(offset <= kAlignedBufferSize, "offset = %d", offset);

    file_offset = flush_blk_num * kAlignedBufferSize + offset;

    int remain = kAlignedBufferSize - offset;
    int copy_len = (remain >= len) ? len : remain;

    memcpy((char*)buffer + offset, compressed_data, copy_len);

    offset += copy_len;
    LOG_ASSERT(offset <= kAlignedBufferSize, "offset = %d", offset);

    // buffer满了，下刷
    if (offset == kAlignedBufferSize) {
      while (flushing) {
        // 循环，直到上一个buffer flush完成
      }
      flushing = true;
      std::swap(flush_buffer, buffer);
      flush_blk_num += 1;
      offset = 0;
      if (len > copy_len) {
        // 数据还有部分没有拷贝完成
        memcpy(buffer, compressed_data + copy_len, len - copy_len);
        offset += (len - copy_len);
        LOG_ASSERT(offset <= kAlignedBufferSize, "offset = %d", offset);
      }

      // 异步写
      file->async_write((const char*)flush_buffer, kAlignedBufferSize);
      flushing = false;
    }
  }

  void Read(char* res_buf, size_t length, __off_t pos) {
    LOG_ASSERT((uint64_t)(pos % kAlignedBufferSize) + (uint64_t)length <= (uint64_t)offset,
               "(pos mod kAlignedBufferSize) = %ld, offset = %d", (pos % kAlignedBufferSize), offset);
    memcpy(res_buf, (char*)buffer + (pos % kAlignedBufferSize), length);
  }

  void Flush() {
    while (flushing) {
      // 循环，直到上一个buffer flush完成
    }
    flushing = true;
    AppendWriteFile f(file->GetFileName(), NORMAL_FLAG);
    f.write((const char*)buffer, offset);
    flushing = false;
    offset = 0;
  }

  bool empty() { return offset == 0; }

  const int GetFlushBlkNum() const { return flush_blk_num; }

private:
  volatile bool flushing = false;
  void* buffer = nullptr;
  void* flush_buffer = nullptr;
  int flush_blk_num = 0; // 已经下刷了多少个块了
  int offset = 0;
  File* file;
};

} // namespace LindormContest
