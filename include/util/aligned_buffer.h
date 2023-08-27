#pragma once

#include <cstddef>
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
  AlignedBuffer(File* file) : file(file) { posix_memalign(&buffer, 512, kAlignedBufferSize); }

  virtual ~AlignedBuffer() { free(buffer); }

  void Add(char* compressed_data, int len, OUT uint64_t& file_offset) {
    LOG_ASSERT(offset <= kAlignedBufferSize, "offset = %d", offset);

    file_offset = flush_blk_num * kAlignedBufferSize + offset;

    int remain = kAlignedBufferSize - offset;
    int copy_len = (remain >= len) ? len : remain;

    memcpy((char*)buffer + offset, compressed_data, copy_len);

    offset += copy_len;

    // buffer满了，下刷
    if (offset == kAlignedBufferSize) {
      while (flush_buffer != nullptr) {
        // 循环，直到上一个buffer flush完成
      }
      flush_buffer = buffer;
      flush_blk_num += 1;
      posix_memalign(&buffer, 512, kAlignedBufferSize);
      offset = 0;
      if (len > copy_len) {
        // 数据还有部分没有拷贝完成
        memcpy(buffer, compressed_data + copy_len, len - copy_len);
        offset += (len - copy_len);
      }

      // 异步写
      file->write((const char*)flush_buffer, kAlignedBufferSize);
      free(flush_buffer);
      flush_buffer = nullptr;
    }
  }

  void Flush() {
    while (flush_buffer != nullptr) {
      // 循环，直到上一个buffer flush完成
    }
    file->write((const char*)buffer, offset);
  }

private:
  void* buffer;
  void* flush_buffer = nullptr;
  int flush_blk_num = 0; // 已经下刷了多少个块了
  int offset = 0;
  File* file;
};

} // namespace LindormContest
