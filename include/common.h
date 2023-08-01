#ifndef _COMMON_H_
#define _COMMON_H_

#include "util/likely.h"
#include "util/logging.h"
#include "util/slice.h"
#include <cstdint>

#define OUT

#define KB (1024)
#define MB (1024 * 1024)

namespace LindormContest {

constexpr uint32_t kBlockSize = 32 * KB;
constexpr int kColumnNum = 60;
constexpr int kShard = 64;

} // namespace LindormContest

#endif