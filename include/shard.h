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
    ColumnArrWrapper* column_arr{nullptr};
    uint8_t colid{};
    uint8_t ref{0};               // 只用做delete 的引用计数
    volatile bool filling{false}; // 表示这个col正在填充数据，还不能被访问
    std::size_t operator()(const Col& key) const {
      return std::hash<uint64_t>()(reinterpret_cast<uint64_t>(key.ptr)) ^ std::hash<int>()(key.colid);
    }

    bool operator==(const Col& other) const { return ptr == other.ptr && colid == other.colid; }
  };

  ReadCache(size_t max_sz) : max_sz_(max_sz) {}

  template <typename TColumn>
  TColumn* FetchDataArr(BlockMeta* meta, uint8_t colid, bool& hit) {
    ColumnArrWrapper* res = GetColumn(meta, colid);
    hit = true;
    if (res == nullptr) {
      hit = false;
      res = new TColumn(colid);
      PutColumn(meta, colid, res);
    }
    return dynamic_cast<TColumn*>(res);
  };

  Status PutColumn(BlockMeta* meta, uint8_t colid, ColumnArrWrapper* col_data);

  ColumnArrWrapper* GetColumn(BlockMeta* meta, uint8_t colid);

  void Release(BlockMeta* meta, uint8_t colid, ColumnArrWrapper* data);

private:
  // LRU cache
  std::unordered_map<Col, std::list<Col>::iterator, Col> cache_;
  std::list<Col> lru_list_;      // 可以evict进行剔除
  std::list<Col> ref_list_;      // 正在被引用
  std::list<Col> prepared_list_; // 刚创建还在被某个协程填充数据

  void moveNode(const Col& node, std::list<Col>& from, std::list<Col>& to) {
    from.erase(cache_[node]);
    to.push_front(node);
    cache_[node] = to.begin();
  }

  void lru2ref(const Col& node) {
    // 从LRU链中首次被ref
    moveNode(node, lru_list_, ref_list_);
  };

  void prepared2ref(const Col& node) {
    // 结束数据填充，且有人在等，放入REF链
    moveNode(node, prepared_list_, ref_list_);
  }

  void ref2lru(const Col& node) {
    // 没有其他人引用了，可以放入LRU链
    moveNode(node, ref_list_, lru_list_);
  }

  CoroCV lru_cv_;          // 等LRU
  CoroCV singleflight_cv_; // 等数据
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

  void AggregateQuery(uint64_t vid, int64_t lowerInclusive, int64_t upperExclusive, int colid, Aggregator op,
                      std::vector<Row>& res);

  void DownSampleQuery(uint64_t vid, int64_t lowerInclusive, int64_t upperExclusive, std::vector<int> colids,
                       CompareExpression cmp);

  void SaveLatestRowCache(File* file);

  void LoadLatestRowCache(File* file);

  void SaveBlockMeta(File* file) { block_mgr_->Save(file); }

  void LoadBlockMeta(File* file) { block_mgr_->Load(file); }

  Status Flush(bool shutdown = false);

private:
  template <typename TRes, typename TCol>
  TRes AggregateAVG(std::vector<TCol>& input);

  template <typename TRes, typename TCol>
  TRes AggregateMAX(std::vector<TCol>& input);

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

// template implementation
template <typename TRes, typename TCol>
TRes ShardImpl::AggregateAVG(std::vector<TCol>& input) {}

template <typename TRes, typename TCol>
TRes ShardImpl::AggregateMAX(std::vector<TCol>& input) {}

} // namespace LindormContest