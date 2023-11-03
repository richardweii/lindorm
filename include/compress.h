#pragma once

#include <cstdint>

#include "util/lz4.h"
#include "util/zstd.h"
#include "simdcomp/simdcomp.h"
#include <functional>
#include <cstdio>
namespace LindormContest {

typedef std::function<int(int)> MaxDestSizeFunc;
typedef std::function<uint64_t(const char* raw_data, uint64_t raw_data_len, char* dest, uint64_t dest_len)> CompressFunc;
typedef std::function<int(const char* compressed, char* dest, int compressed_sz, int dest_cap)> DeCompressFunc;
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
}

inline int LZ4DeCompress(const char* src, char* dst, int compressedSize, int dstCapacity) {
  return LZ4_decompress_safe(src, dst, compressedSize, dstCapacity);
}

inline int ZSTDDeCompress(const char* src, char* dst, int compressedSize, int dstCapacity) {
  return ZSTD_decompress(dst, dstCapacity, src, compressedSize);
}

inline int SIMDMaxDestSize(uint32_t *src, int Nofints, uint32_t &b) {
  b = maxbits_length(src, Nofints);
  return simdpack_compressedbytes(Nofints, b);
} 

inline int SIMDCompress(uint32_t *src, int Nofints, uint32_t b, char *out) {
  //return copresssed length
  __m128i *eof_ = simdpack_length(src, Nofints, (__m128i *)out, b);
  return (eof_-(__m128i *)out)*sizeof(__m128i);
}

inline void SIMDDeCompress(char *src, int Nofints, uint32_t *out, uint32_t b) {
  simdunpack_length((const __m128i *)src, Nofints, out, b);
}



// string, int, float
const std::vector<MaxDestSizeFunc> max_dest_size_func = {ZSTDMaxDestSize,ZSTDMaxDestSize,ZSTDMaxDestSize,nullptr};
const std::vector<CompressFunc> compress_func = {ZSTDCompress, ZSTDCompress, ZSTDCompress, nullptr};
const std::vector<DeCompressFunc> decompress_func = {ZSTDDeCompress, ZSTDDeCompress, ZSTDDeCompress, nullptr};

} // namespace LindormContest
