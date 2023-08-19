#pragma once

#include <cstdint>
#include "util/lz4.h"

namespace LindormContest {

typedef int MaxDestSizeFunc(int);
typedef uint64_t CompressFunc(const char*, uint64_t, char*, uint64_t);

/**
 * 各种压缩算法的实现
 */
inline int LZ4MaxDestSize(int inputSize) {
  return LZ4_compressBound(inputSize);
}

inline uint64_t LZ4Compress(const char* data, uint64_t len, char* compress_buf, uint64_t compress_len) {
  return LZ4_compress_default(data, compress_buf, len, compress_len);
}

inline int LZ4DeCompress(const char* src, char* dst, int compressedSize, int dstCapacity) {
  return LZ4_decompress_safe(src, dst, compressedSize, dstCapacity);
}

}
