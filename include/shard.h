#pragma once
#include <unordered_map>

#include "memtable.h"
namespace LindormContest {

/**
 * @brief 读过程中的读取缓存
 * 因为存的时候不能按vin分开存，如果一个vin一个压缩块的话，压缩块的大小过于的小了。所以只能将一整个分片里的所有vin混合在一起压缩。
  所以一整个压缩块会有很多的数据，如果每次读都读一整个压缩块上来，但是只用到了其中一个vin的数据，就会有300倍的读放大（一个shard里面平均391个vin）
  这里选择将压缩块进行缓存，免得多次读取重复的压缩块。
 */

class ReadCache {
public:
  struct Col {
    BlockMeta* ptr{nullptr};
    int colid{};
    ColumnArrWrapper* column_arr{nullptr};

    std::size_t operator()(const Col& key) const {
      return std::hash<uint64_t>()(reinterpret_cast<uint64_t>(key.ptr)) ^ std::hash<int>()(key.colid);
    }

    bool operator==(const Col& other) const { return ptr == other.ptr && colid == other.colid; }
  };

  ReadCache(size_t max_sz) : max_sz_(max_sz) {}

  template <typename TColumn>
  TColumn* FetchDataArr(BlockMeta* meta, int colid) {
    TColumn* res = dynamic_cast<TColumn*>(GetColumn(meta, colid));
#ifdef ENABLE_STAT
    RECORD_FETCH_ADD(cache_cnt, 1);
    if (res != nullptr) {
      RECORD_FETCH_ADD(cache_hit, 1);
    }
#endif
    if (res == nullptr) {
      res = new TColumn(colid);
      PutColumn(meta, colid, res);
    }
    return res;
  };

  Status PutColumn(BlockMeta* meta, int colid, ColumnArrWrapper* col_data);

  ColumnArrWrapper* GetColumn(BlockMeta* meta, int colid);

private:
  // LRU cache
  std::unordered_map<Col, std::list<Col>::iterator, Col> cache_;
  std::list<Col> lru_list_;
  void updateList(const Col& key);

  size_t total_sz_{0};     // 内存用量
  const size_t max_sz_{0}; // cache上限量
};

// 存储分片
class ShardImpl {
public:
  ShardImpl(int shard_id, TSDBEngineImpl* engine) : shard_id_(shard_id), engine_(engine) {
    for (int i = 0; i < kVinNumPerShard; i++) {
      latest_ts_cache_[i] = -1;
    }
  }
  ~ShardImpl();
  void Init(bool write_phase, File* data_file);

  void InitMemTable();

  void Write(uint16_t vid, const Row& row);

  void GetLatestRow(uint16_t vid, const std::vector<int>& colids, OUT Row& row);

  void GetRowsFromTimeRange(uint64_t vid, int64_t lowerInclusive, int64_t upperExclusive,
                            const std::vector<int>& colids, std::vector<Row>& results);

  void AggregateQuery(uint64_t vid, int64_t lowerInclusive, int64_t upperExclusive, std::vector<int> colids,
                      Aggregator op);

  void DownSampleQuery(uint64_t vid, int64_t lowerInclusive, int64_t upperExclusive, std::vector<int> colids,
                       CompareExpression cmp);

  void SaveLatestRowCache(File* file);

  void LoadLatestRowCache(File* file);

  void SaveBlockMeta(File* file) { block_mgr_->Save(file); }

  void LoadBlockMeta(File* file) { block_mgr_->Load(file); }

  Status Flush(bool shutdown = false);

private:
  int shard_id_;
  TSDBEngineImpl* engine_{nullptr};
  File* data_file_{nullptr};

  bool write_phase_ = false;

  ReadCache* read_cache_{nullptr}; // for read phase

  MemTable* two_memtable_[2]{nullptr}; // 0 - 1 当一个在flush的时候使用另一个
  uint8_t memtable_id{0};
  MemTable* volatile memtable_{nullptr}; // for write phase
  AlignedWriteBuffer* write_buf_{nullptr};

  BlockMetaManager* block_mgr_{nullptr};

  // LatestQueryCache
  // Row latest_row_cache[kVinNumPerShard];
  ColumnValue latest_ts_cols_[kVinNumPerShard][kColumnNum];
  int64_t latest_ts_cache_[kVinNumPerShard];
};

} // namespace LindormContest