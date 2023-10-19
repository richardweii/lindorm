#ifndef _COMMON_H_
#define _COMMON_H_

#include <cstdint>

#include "util/likely.h"
#include "util/logging.h"
#include "util/slice.h"

#define OUT

#define KB (1024)
#define K (1024)

#define MB (1024 * 1024)
#define M (1024 * 1024)

namespace LindormContest {

constexpr int kColumnNum = 60;
constexpr int kVinNum = 50000;
constexpr int kShardBits = 7;
// 按照vin进行分片的数量，最好保证和kVinNum是整除的关系，这样每个分片的vin数量是均匀的
constexpr int kShardNum = 1 << kShardBits;
constexpr int kVinNumPerShard = (kVinNum / kShardNum) + 1; // 打到每个memtable里面vin的个数
constexpr int kMemtableRowNum = 16 * K;                    // 一个memtable里面最多存储多少行数据
constexpr int kExtraColNum = 3;
constexpr int kWriteBufferSize = 1024 * KB;
constexpr size_t kReadCacheSize = 16 * MB;
constexpr int kWorkerThread = 8;
constexpr int kCoroutinePerThread = 16;

const std::string kVidColName = "myvid";
const std::string kTsColName = "myts";
const std::string kIdxColName = "myidx";
const std::string kTableName = "only_one"; // 目前就一张表，表名预留给复赛

// 低位决定shard
static inline int sharding(uint16_t vid) {
  LOG_ASSERT(vid < kVinNum, "vid = %d", vid);
  return vid % kShardNum;
}

// vid -->  shard 内序号
static inline int vid2svid(uint16_t vid) {
  LOG_ASSERT((vid >> kShardBits) < kVinNumPerShard, "idx = %d", vid >> kShardBits);
  return vid >> kShardBits;
}

static inline int svid2vid(int shard_id, int idx) {
  LOG_ASSERT((shard_id + (idx << kShardBits)) < kVinNum, "vid = %d", shard_id + (idx << kShardBits));
  return shard_id + (idx << kShardBits);
}

static inline size_t rounddown512(size_t offset) { return offset & ~511; }
static inline size_t roundup512(size_t len) { return (len + 511) & ~511; }
} // namespace LindormContest

#endif