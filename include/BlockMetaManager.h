#pragma once

#include <cstdint>
#include <cstring>
#include <mutex>

#include "common.h"
#include "io/file.h"
#include "struct/Vin.h"
#include "util/defer.h"
#include "util/logging.h"

namespace LindormContest {

struct BlockMeta {
  BlockMeta* next;
  int num; // 一共写了多少行数据
  int64_t min_ts[kVinNumPerShard];
  int64_t max_ts[kVinNumPerShard];
  // 每一列对应的块的元数据
  uint64_t* compress_sz;
  uint64_t* origin_sz;
  uint64_t* offset;

  BlockMeta() {
    compress_sz = new uint64_t[kColumnNum + kExtraColNum];
    origin_sz = new uint64_t[kColumnNum + kExtraColNum];
    offset = new uint64_t[kColumnNum + kExtraColNum];
  }

  ~BlockMeta() {
    delete[] compress_sz;
    delete[] origin_sz;
    delete[] offset;
  }
};

/**
 * 每一个Shard都有一个元数据管理器，存储了每一个Shard下刷的所有的Block的元数据信息
 */
// TODO: 重构，把kShardNum 去掉
class BlockMetaManager {
public:
  BlockMeta* NewVinBlockMeta(int num, int64_t* min_ts, int64_t* max_ts) {
    BlockMeta* blk_meta = new BlockMeta();
    LOG_ASSERT(blk_meta != nullptr, "blk_meta == nullptr");
    blk_meta->num = num;
    for (int i = 0; i < kVinNumPerShard; i++) {
      blk_meta->min_ts[i] = min_ts[i];
      blk_meta->max_ts[i] = max_ts[i];
    }

    blk_meta->next = nullptr;
    block_cnts_++;

    if (head_ == nullptr) {
      head_ = blk_meta;
    } else {
      // 头插法
      blk_meta->next = head_;
      head_ = blk_meta;
    }

    // 剩下的元数据，返回回去，每个列的Flush函数自己填充，当前的测试流程应该不会出现并发问题
    return blk_meta;
  }

  // 遍历所有meta，只要是时间戳区间有重合的都返回
  void GetVinBlockMetasByTimeRange(uint16_t vid, int64_t min_ts, int64_t max_ts,
                                   OUT std::vector<BlockMeta*>& blk_metas) {
    blk_metas.clear();
    BlockMeta* p = head_;
    int idx = vid2svid(vid);
    while (p != nullptr) {
      if (p->max_ts[idx] < min_ts || p->min_ts[idx] >= max_ts) {
        p = p->next;
        continue;
      }

      blk_metas.emplace_back(p);
      p = p->next;
    }
  }

  // shutdown的时候，持久化到文件
  // 格式：block_cnt [blk_meta]
  //  blk_meta: row_num min_ts[kVinNumPerShard] max_ts[kVinNumPerShard] compress_sz[col_num] origin_sz[col_num]
  //  offset[col_num]
  void Save(File* file) {
    LOG_ASSERT(file != nullptr, "error file");

    int blk_cnt = block_cnts_;
    // block_cnt
    file->write((const char*)&blk_cnt, sizeof(blk_cnt));
    BlockMeta* p = head_;
    for (int i = 0; i < blk_cnt; i++) {
      LOG_ASSERT(p != nullptr, "p == nullptr");
      // row num
      file->write((const char*)&p->num, sizeof(p->num));
      // min_ts[]
      file->write((const char*)p->min_ts, sizeof(p->min_ts[0]) * kVinNumPerShard);
      // max_ts
      file->write((const char*)p->max_ts, sizeof(p->max_ts[0]) * kVinNumPerShard);
      // compress_sz
      file->write((const char*)p->compress_sz, sizeof(p->compress_sz[0]) * (kColumnNum + kExtraColNum));
      // origin_sz
      file->write((const char*)p->origin_sz, sizeof(p->origin_sz[0]) * (kColumnNum + kExtraColNum));
      // offset
      file->write((const char*)p->offset, sizeof(p->offset[0]) * (kColumnNum + kExtraColNum));
      p = p->next;
    }
    LOG_ASSERT(p == nullptr, "p should be equal nullptr");
  }

  // connect的时候，从文件读取，重新构建VinBlockMetaManager
  void Load(File* file) {
    LOG_ASSERT(file != nullptr, "error file");

    // block_cnt
    int blk_cnt;
    file->read((char*)&blk_cnt, sizeof(blk_cnt));
    block_cnts_ = 0;

    LOG_ASSERT(head_ == nullptr, "head should be nullptr");
    for (int i = 0; i < blk_cnt; i++) {
      int num;
      int64_t min_ts[kVinNumPerShard];
      int64_t max_ts[kVinNumPerShard];
      file->read((char*)&num, sizeof(num));
      file->read((char*)min_ts, sizeof(min_ts[0]) * kVinNumPerShard);
      file->read((char*)max_ts, sizeof(max_ts[0]) * kVinNumPerShard);

      auto* p = NewVinBlockMeta(num, min_ts, max_ts);

      file->read((char*)p->compress_sz, sizeof(p->compress_sz[0]) * (kColumnNum + kExtraColNum));
      file->read((char*)p->origin_sz, sizeof(p->origin_sz[0]) * (kColumnNum + kExtraColNum));
      file->read((char*)p->offset, sizeof(p->offset[0]) * (kColumnNum + kExtraColNum));
    }
  }

  virtual ~BlockMetaManager() {
    int num = 0;
    BlockMeta* next = nullptr;
    while (head_ != nullptr) {
      next = head_->next;
      delete head_;
      head_ = next;
      num++;
    }
    LOG_ASSERT(num == block_cnts_, "num != block_cnt");
  }

private:
  int block_cnts_{0}; // 一共下刷了多少个block
  BlockMeta* head_{nullptr};
};

} // namespace LindormContest