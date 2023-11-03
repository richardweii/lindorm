#pragma once
#include <unordered_map>

#include "memtable.h"
#include "util/likely.h"
#include "util/util.h"
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
    bool operator==(const Col& other) const { return ptr == other.ptr && colid == other.colid; }
  };

  struct ColHasher {
    std::size_t operator()(const Col& key) const {
      return std::hash<uint64_t>()(reinterpret_cast<uint64_t>(key.ptr)) ^ std::hash<int>()(key.colid);
    }
  };

  struct ColEqual {
    bool operator()(const Col& a, const Col& b) const { return a == b; }
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

  // 因为string类型的Column只能在数据读取之后才知道真正size，需要在再进行一次LRU修正，以免cache被string类型冲爆
  void ReviseCacheSize(StringArrWrapper* col) {
    size_t placeholder_sz = 2 * sizeof(ColumnArr<std::string>);
    ENSURE(total_sz_ >= placeholder_sz, "invalid total_sz %zu", total_sz_);
    total_sz_ -= placeholder_sz;
    total_sz_ += col->TotalSize();
    evictForPut();
  }

private:
  void evictForPut();

  // LRU cache
  std::unordered_map<Col, std::list<Col>::iterator, ColHasher, ColEqual> cache_;
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

  void DownSampleQuery(uint64_t vid, int64_t lowerInclusive, int64_t upperExclusive, int64_t interval, int colid,
                       Aggregator op, const CompareExpression& cmp, std::vector<Row>& res);

  void SaveLatestRowCache(File* file);

  void LoadLatestRowCache(File* file);

  void SaveBlockMeta(File* file) { block_mgr_->Save(file); }

  void LoadBlockMeta(File* file) { block_mgr_->Load(file); }

  Status Flush(bool shutdown = false);

private:
  template <typename TAgg, typename TCol>
  void aggregateImpl2(uint64_t vid, int64_t lowerInclusive, int64_t upperExclusive, int colid, std::vector<Row>& res);

  template <typename TAgg, typename TCol>
  void aggregateImpl(const std::vector<Row>& input, const std::string& col_name, Row& res);

  template <typename TAgg, typename TCol>
  void downsampleImpl(const std::vector<Row>& input, const std::string& col_name, int64_t lowerInclusive,
                      int64_t upperExclusive, int64_t interval, const CompareExpression& cmp, std::vector<Row>& res);

  static int position(int64_t lowerInclusive, int64_t interval, int64_t ts) { return (ts - lowerInclusive) / interval; }

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
template <typename TAgg, typename TCol>
inline void ShardImpl::aggregateImpl(const std::vector<Row>& input, const std::string& col_name, Row& res) {
  TAgg agg;
  for (auto& row : input) {
    const ColumnValue& col_val = row.columns.at(col_name);
    ColumnValueWrapper wrapper(&col_val);
    agg.Add(wrapper.getFixedSizeValue<TCol>());
  }

  ColumnValue res_val(agg.GetResult());

  res.columns.emplace(std::make_pair(col_name, std::move(res_val)));
};

template <typename TAgg, typename TCol>
inline void ShardImpl::downsampleImpl(const std::vector<Row>& input, const std::string& col_name,
                                      int64_t lowerInclusive, int64_t upperExclusive, int64_t interval,
                                      const CompareExpression& cmp, std::vector<Row>& res) {
  int bucket_num = (upperExclusive - lowerInclusive) / interval;
  std::vector<TAgg> buckets;
  buckets.reserve(bucket_num);
  ColumnValueWrapper wrapper(&cmp.value);
  for (int i = 0; i < bucket_num; i++) {
    buckets.emplace_back(cmp.compareOp, wrapper.getFixedSizeValue<TCol>());
  }
  for (auto& row : input) {
    int pos = position(lowerInclusive, interval, row.timestamp);
    const ColumnValue& col_val = row.columns.at(col_name);
    ColumnValueWrapper wrapper(&col_val);
    buckets[pos].Add(wrapper.getFixedSizeValue<TCol>());
  }

  for (size_t i = 0; i < buckets.size(); i++) {
    auto& agg = buckets[i];
    Row row;
    row.columns.emplace(std::make_pair(col_name, ColumnValue(agg.GetResult())));
    ::memcpy(row.vin.vin, input.front().vin.vin, VIN_LENGTH);
    row.timestamp = lowerInclusive + i * interval;
    res.push_back(std::move(row));
  }
}

template <typename TAgg, typename TCol>
inline void ShardImpl::aggregateImpl2(uint64_t vid, int64_t lowerInclusive, int64_t upperExclusive, int colid,
                                      std::vector<Row>& res) {
  TAgg agg;

  std::vector<BlockMeta*> blk_metas;
  block_mgr_->GetVinBlockMetasByTimeRange(vid, lowerInclusive, upperExclusive, blk_metas);

  File* rfile = data_file_;
  if (!blk_metas.empty()) {
    RECORD_FETCH_ADD(disk_blk_access_cnt, blk_metas.size());
    ColumnValue col;
    std::vector<uint16_t> idxs;
    std::vector<int64_t> tss;
    std::vector<ColumnArrWrapper*> need_read_from_file;
    std::vector<char*> tmp_bufs;
    for (auto blk_meta : blk_metas) {
      // 去读对应列的block
      need_read_from_file.clear();
      tmp_bufs.clear();

      bool hit;
      VidArrWrapper* tmp_vid_col = read_cache_->FetchDataArr<VidArrWrapper>(blk_meta, kColumnNum, hit);
      if (!hit) need_read_from_file.push_back(tmp_vid_col);

      TsArrWrapper* tmp_ts_col = read_cache_->FetchDataArr<TsArrWrapper>(blk_meta, kColumnNum + 1, hit);
      if (!hit) need_read_from_file.push_back(tmp_ts_col);

      IdxArrWrapper* tmp_idx_col = read_cache_->FetchDataArr<IdxArrWrapper>(blk_meta, kColumnNum + 2, hit);
      if (!hit) need_read_from_file.push_back(tmp_idx_col);

      ColumnArrWrapper* agg_col = nullptr;

      switch (engine_->columns_type_[colid]) {
        case COLUMN_TYPE_STRING:
          agg_col = read_cache_->FetchDataArr<StringArrWrapper>(blk_meta, colid, hit);
          if (!hit) need_read_from_file.push_back(agg_col);
          break;
        case COLUMN_TYPE_INTEGER:
          agg_col = read_cache_->FetchDataArr<IntArrWrapper>(blk_meta, colid, hit);
          if (!hit) need_read_from_file.push_back(agg_col);
          break;
        case COLUMN_TYPE_DOUBLE_FLOAT:
          agg_col = read_cache_->FetchDataArr<DoubleArrWrapper>(blk_meta, colid, hit);
          if (!hit) need_read_from_file.push_back(agg_col);
          break;
        case COLUMN_TYPE_UNINITIALIZED:
          LOG_ASSERT(false, "error");
          break;
      }

      AsyncFile* async_rf = dynamic_cast<AsyncFile*>(rfile);
      LOG_ASSERT(async_rf != nullptr, "invalid cast");

      // 同时下发多个异步读IO
      while (async_rf->avalibaleIOC() < need_read_from_file.size()) {
        RECORD_FETCH_ADD(wait_aio, 1);
        async_rf->waitIOC();
      }
      for (auto& col : need_read_from_file) {
        // 储存用于异步IO的buf
        tmp_bufs.push_back(col->AsyncReadCompressed(async_rf, blk_meta));
      }

      this_coroutine::co_wait(need_read_from_file.size());

      int i = 0;
      for (auto& col : need_read_from_file) {
        col->Decompressed(tmp_bufs[i], blk_meta);
        i++;
      }
      idxs.clear();
      tss.clear();
      // 现在vid + ts是有序的，可以直接应用二分查找
      findMatchingIndices(tmp_vid_col->GetDataArr(), tmp_ts_col->GetDataArr(), tmp_idx_col->GetDataArr(), blk_meta->num,
                          vid2svid(vid), lowerInclusive, upperExclusive, idxs, tss);

      if (!idxs.empty()) {
        for (int i = 0; i < (int)idxs.size(); i++) {
          // fill aggragate container.
          agg_col->Get(idxs[i], col);
          ColumnValueWrapper wrapper(&col);
          agg.Add(wrapper.getFixedSizeValue<TCol>());
        }
      }

      read_cache_->Release(blk_meta, kColumnNum, tmp_vid_col);
      read_cache_->Release(blk_meta, kColumnNum + 1, tmp_ts_col);
      read_cache_->Release(blk_meta, kColumnNum + 2, tmp_idx_col);
      read_cache_->Release(blk_meta, colid, agg_col);

      for (auto& col : need_read_from_file) {
        auto string_col = dynamic_cast<StringArrWrapper*>(col);
        if (UNLIKELY(string_col != nullptr)) {
          read_cache_->ReviseCacheSize(string_col);
        }
      }
    }
  }

  std::string& col_name = engine_->columns_name_[colid];
  Row r;
  r.timestamp = lowerInclusive;
  ::memcpy(r.vin.vin, engine_->vid2vin_[vid].c_str(), VIN_LENGTH);
  ColumnValue res_val(agg.GetResult());
  r.columns.emplace(std::make_pair(col_name, std::move(res_val)));
  res.push_back(std::move(r));
}

} // namespace LindormContest