#pragma once

#include <cstdint>
#include <cstring>

#include "util/lz4.h"
#include "util/zstd.h"

namespace LindormContest {

typedef int MaxDestSizeFunc(int);
typedef uint64_t CompressFunc(const char* raw_data, uint64_t raw_data_len, char* dest, uint64_t dest_len);
typedef int DeCompressFunc(const char* compressed, char* dest, int compressed_sz, int dest_cap);
/**
 * 各种压缩算法的实现
 */
inline int LZ4MaxDestSize(int inputSize) { return LZ4_compressBound(inputSize); }

inline int ZSTDMaxDestSize(int inputSize) { return ZSTD_compressBound(inputSize); }

inline uint64_t LZ4Compress(const char* data, uint64_t len, char* compress_buf, uint64_t compress_len) {
  return LZ4_compress_default(data, compress_buf, len, compress_len);
}

inline uint64_t ZSTDCompress(const char* data, uint64_t len, char* compress_buf, uint64_t compress_len) {
  return ZSTD_compress((void*)compress_buf, compress_len, (const void*)data, len, 3);
  // ::memcpy(compress_buf, data, len);
  // return len;
}

inline int LZ4DeCompress(const char* src, char* dst, int compressedSize, int dstCapacity) {
  return LZ4_decompress_safe(src, dst, compressedSize, dstCapacity);
}

inline int ZSTDDeCompress(const char* src, char* dst, int compressedSize, int dstCapacity) {
  return ZSTD_decompress(dst, dstCapacity, src, compressedSize);
  // ::memcpy(dst, src, compressedSize);
  // return compressedSize;
}

extern MaxDestSizeFunc* max_dest_size_func;
extern CompressFunc* compress_func;
extern DeCompressFunc* decompress_func;

} // namespace LindormContest
