#include "compress.h"

namespace LindormContest {

MaxDestSizeFunc* max_dest_size_func = ZSTDMaxDestSize;
CompressFunc* compress_func = ZSTDCompress;
DeCompressFunc* decompress_func = ZSTDDeCompress;
// MaxDestSizeFunc *max_dest_size_func = LZ4MaxDestSize;
// CompressFunc *compress_func = LZ4Compress;
// DeCompressFunc *decompress_func = LZ4DeCompress;

} // namespace LindormContest
