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
constexpr int kVinNum = 30000;
constexpr int kShardBits = 4;
constexpr int kShardNum = 1 << kShardBits; // 按照vin进行分片的数量，最好保证和kVinNum是整除的关系，这样每个分片的vin数量是均匀的
constexpr int kVinNumPerShard = kVinNum / kShardNum; // 打到每个memtable里面vin的个数
constexpr int kMemtableRowNum = 6750;                // 一个memtable里面最多存储多少行数据
constexpr int kExtraColNum = 2;
const std::string kVidColName = "myvid";
const std::string kTsColName = "myts";

// 底四位决定shard
inline int Shard(uint16_t vid) {
  LOG_ASSERT(vid < kVinNum, "vid = %d", vid);
  return vid % kShardNum;
}

inline int vid2idx(uint16_t vid) {
  LOG_ASSERT((vid >> kShardBits) < kVinNumPerShard, "idx = %d", vid >> kShardBits);
  return vid >> kShardBits;
}

inline int idx2vid(int shard_id, int idx) {
  LOG_ASSERT((shard_id + (idx << kShardBits)) < kVinNum, "vid = %d", shard_id + (idx << kShardBits));
  return shard_id + (idx << kShardBits);
}

} // namespace LindormContest

#endif