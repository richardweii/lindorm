#pragma once

#include "common.h"
#include "io/file.h"
#include "struct/Vin.h"
#include "util/defer.h"
#include "util/logging.h"
#include <cstdint>
#include <cstring>
#include <mutex>

namespace LindormContest {

typedef struct BlockMeta {
  BlockMeta *next;
  int num; // 一共写了多少行数据
  int64_t min_ts[kVinNumPerShard];
  int64_t max_ts[kVinNumPerShard];
  // 每一列对应的块的元数据
  uint64_t *compress_sz;
  uint64_t *origin_sz;
  uint64_t *offset;

  BlockMeta(int col_num) {
    compress_sz = new uint64_t[col_num];
    origin_sz = new uint64_t[col_num];
    offset = new uint64_t[col_num];
  }

  ~BlockMeta() {
    delete[] compress_sz;
    delete[] origin_sz;
    delete[] offset;
  }
} BlockMeta;

/**
 * 每一个Shard都有一个元数据管理器，存储了每一个Shard下刷的所有的Block的元数据信息
 */
class ShardBlockMetaManager {
public:
  ShardBlockMetaManager(int col_num) : col_num_(col_num) {
    for (int i = 0; i < kShardNum; i++) {
      head[i] = nullptr;
    }
    memset(block_cnts, 0, sizeof(int) * kShardNum);
  }

  BlockMeta *NewVinBlockMeta(int shard_id, int num, int64_t *min_ts, int64_t *max_ts) {
    mutex.lock();
    defer { mutex.unlock(); };

    BlockMeta *blk_meta = new BlockMeta(col_num_);
    blk_meta->num = num;
    for (int i = 0; i < kVinNumPerShard; i++) {
      blk_meta->min_ts[i] = min_ts[i];
      blk_meta->max_ts[i] = max_ts[i];
    }

    blk_meta->next = nullptr;
    block_cnts[shard_id]++;

    if (head[shard_id] == nullptr) {
      head[shard_id] = blk_meta;
    } else {
      // 头插法
      blk_meta->next = head[shard_id];
      head[shard_id] = blk_meta;
    }

    // 剩下的元数据，返回回去，每个列的Flush函数自己填充，当前的测试流程应该不会出现并发问题
    return blk_meta;
  }

  // 遍历所有meta，只要是时间戳区间有重合的都返回
  void GetVinBlockMetasByTimeRange(uint16_t vid, int64_t min_ts, int64_t max_ts, OUT std::vector<BlockMeta *> &blk_metas) {
    mutex.lock();
    defer { mutex.unlock(); };

    blk_metas.clear();
    BlockMeta *p = head[Shard(vid)];
    int idx = vid2idx(vid);
    while (p != nullptr) {
      if (p->max_ts[idx] < min_ts || p->min_ts[idx] >= max_ts) {
        p = p->next;
        continue;
      }

      // TODO: 目前BlockMeta不会被删除，所以不需要Pin住，如果复赛涉及到BlockMeta的删除，那么可能需要添加Pin的逻辑
      blk_metas.emplace_back(p);
      p = p->next;
    }
  }

  // shutdown的时候，持久化到文件
  // 格式：block_cnt [blk_meta]
  //  blk_meta: row_num min_ts[kVinNumPerShard] max_ts[kVinNumPerShard] compress_sz[col_num] origin_sz[col_num] offset[col_num]
  void Save(File *file) {
    LOG_ASSERT(file != nullptr, "error file");
    mutex.lock();
    defer { mutex.unlock(); };

    for (int shard_id = 0; shard_id < kShardNum; shard_id++) {
      int blk_cnt = block_cnts[shard_id];
      // block_cnt
      file->write((const char *) &blk_cnt, sizeof(blk_cnt));
      BlockMeta *p = head[shard_id];
      for (int i = 0; i < blk_cnt; i++) {
        LOG_ASSERT(p != nullptr, "p == nullptr");
        // row num
        file->write((const char *) &p->num, sizeof(p->num));
        // min_ts[]
        file->write((const char *) p->min_ts, sizeof(p->min_ts[0]) * kVinNumPerShard);
        // max_ts
        file->write((const char *) p->max_ts, sizeof(p->max_ts[0]) * kVinNumPerShard);
        // compress_sz
        file->write((const char *) p->compress_sz, sizeof(p->compress_sz[0]) * col_num_);
        // origin_sz
        file->write((const char *) p->origin_sz, sizeof(p->origin_sz[0]) * col_num_);
        // offset
        file->write((const char *) p->offset, sizeof(p->offset[0]) * col_num_);
        p = p->next;
      }
      LOG_ASSERT(p == nullptr, "p should be equal nullptr");
    }
  }

  // connect的时候，从文件读取，重新构建VinBlockMetaManager
  void Load(File *file) {
    LOG_ASSERT(file != nullptr, "error file");

    for (int shard_id = 0; shard_id < kShardNum; shard_id++) {
      // block_cnt
      int blk_cnt;
      file->read((char *) &blk_cnt, sizeof(blk_cnt));
      block_cnts[shard_id] = blk_cnt;

      LOG_ASSERT(head[shard_id] == nullptr, "head[shard_id] should be nullptr");
      for (int i = 0; i < blk_cnt; i++) {
        int num;
        int64_t min_ts[kVinNumPerShard];
        int64_t max_ts[kVinNumPerShard];
        file->read((char *) &num, sizeof(num));
        file->read((char *) min_ts, sizeof(min_ts[0]) * kVinNumPerShard);
        file->read((char *) max_ts, sizeof(max_ts[0]) * kVinNumPerShard);

        auto *p = NewVinBlockMeta(shard_id, num, min_ts, max_ts);

        file->read((char *) p->compress_sz, sizeof(p->compress_sz[0]) * col_num_);
        file->read((char *) p->origin_sz, sizeof(p->origin_sz[0]) * col_num_);
        file->read((char *) p->offset, sizeof(p->offset[0]) * col_num_);
      }
    }
  }

  virtual ~ShardBlockMetaManager() {
    mutex.lock();
    defer { mutex.unlock(); };

    for (int shard_id = 0; shard_id < kShardNum; shard_id++) {
      int num = 0;
      BlockMeta *next = nullptr;
      while (head[shard_id] != nullptr) {
        next = head[shard_id]->next;
        delete head[shard_id];
        head[shard_id] = next;
        num++;
      }
      LOG_ASSERT(num == block_cnts[shard_id], "num != block_cnt");
    }
  }

private:
  std::mutex mutex;
  int block_cnts[kShardNum]; // 一共下刷了多少个block
  BlockMeta *head[kShardNum];
  int col_num_;
};

} // namespace LindormContest
