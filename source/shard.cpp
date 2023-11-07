#include "shard.h"

#include "agg.h"
#include "util/util.h"

namespace LindormContest {

void ReadCache::evictForPut() {
  while (total_sz_ >= max_sz_) {
    // 缓存已满
    while (lru_list_.empty()) {
      RECORD_FETCH_ADD(lru_wait_cnt, 1);
      lru_cv_.wait(); // 等别人Release了才能进行LRU剔除
    }
    auto victim = lru_list_.back();
    LOG_ASSERT(victim.column_arr != nullptr, "invalid victim");
    auto sz = victim.column_arr->TotalSize();
    LOG_ASSERT(victim.ref == 0, "someone are using this.");
    delete victim.column_arr;
    total_sz_ -= sz;
    lru_list_.pop_back();
    cache_.erase(victim);
  }
}

Status ReadCache::PutColumn(BlockMeta* meta, uint8_t colid, ColumnArrWrapper* col_data) {
  Col key = {.ptr = meta, .colid = colid};
  LOG_ASSERT(col_data->TotalSize() <= max_sz_, "ColumnArrWrapper is too big.");

  total_sz_ += col_data->TotalSize();
  evictForPut();

  // 插入新元素到缓存和链表头部
  prepared_list_.push_front(key);
  cache_[key] = prepared_list_.begin();
  cache_[key]->column_arr = col_data;
  cache_[key]->ref = 1;
  cache_[key]->filling = true;
  return Status::OK;
}

ColumnArrWrapper* ReadCache::GetColumn(BlockMeta* meta, uint8_t colid) {
  RECORD_FETCH_ADD(cache_cnt, 1);
  Col key = {.ptr = meta, .colid = colid};
  auto iter = cache_.find(key);
  if (iter == cache_.end()) {
    return nullptr;
  }

  RECORD_FETCH_ADD(cache_hit, 1);
  if (cache_[key]->ref == 0) {
    auto node = *iter->second;
    lru2ref(node);
  }
  cache_[key]->ref++;
  while (cache_[key]->filling) {
    RECORD_FETCH_ADD(data_wait_cnt, 1);
    singleflight_cv_.wait(); // 要读取的数据在缓存中，但是其他协程正在填充数据
  }

  return cache_[key]->column_arr;
};

void ReadCache::Release(BlockMeta* meta, uint8_t colid, ColumnArrWrapper* data) {
  Col key = {.ptr = meta, .colid = colid};
  auto iter = cache_.find(key);
  LOG_ASSERT(iter != cache_.end(), "referenced data should not be invalid");
  auto node = *iter->second; // 用copy模式，因为move list的时候会移动iterator
  node.ref--;
  if (node.filling) {
    node.filling = false;
    prepared2ref(node);
    if (node.ref > 0) {
      singleflight_cv_.notify(); // 唤醒其他等数据的协程
    }
  }

  if (node.ref == 0) {
    ref2lru(node);
    lru_cv_.notify(); // 唤醒LRU协程
  }
  *cache_[node] = node; // copy模式下修改了之后要重新设置
};

void ShardImpl::Init() {
  for (int i = 0; i < kVinNumPerShard; i++) {
    if (write_phase) {
      data_file_[i] = engine_->io_mgr_->OpenAsyncWriteFile(
        VinFileName(engine_->dataDirPath, kTableName, svid2vid(shard_id_, i)), shard2tid(shard_id_));
    } else {
      data_file_[i] = engine_->io_mgr_->OpenAsyncReadFile(
        VinFileName(engine_->dataDirPath, kTableName, svid2vid(shard_id_, i)), shard2tid(shard_id_));
    }
    block_mgr_[i] = new BlockMetaManager();
    write_buf_[i] = nullptr;
    memtable_[i] = nullptr;
  }

  size_t read_cache_sz = write_phase ? kReadCacheSize / 8 : kReadCacheSize;
  read_cache_ = new ReadCache(read_cache_sz);

  if (write_phase) {
    for (int i = 0; i < kVinNumPerShard; i++) {
      write_buf_[i] = new AlignedWriteBuffer(data_file_[i]);
      memtable_[i] = new MemTable(shard_id_, engine_);
    }
  }
};

void ShardImpl::SaveLatestRowCache(File* file) { // vin ts columns
  // columns就按照colid顺序存，除了string先存一个4字节的长度，再存字符串之后，其他字段都是固定长度，直接根据类型进行解析即可
  for (int i = 0; i < kVinNumPerShard; i++) {
    // 需要存kVinNumPerShard个row
    if (latest_ts_cache_[i] == -1) {
      continue;
    }
    file->write((char*)&i, sizeof(i));
    file->write((char*)(&latest_ts_cache_[i]), sizeof(latest_ts_cache_[i]));
    for (int k = 0; k < kColumnNum; k++) {
      auto& col = latest_ts_cols_[i][k];
      switch (engine_->columns_type_[k]) {
        case COLUMN_TYPE_STRING: {
          std::pair<int32_t, const char*> lengthStrPair;
          col.getStringValue(lengthStrPair);
          file->write((char*)&lengthStrPair.first, sizeof(lengthStrPair.first));
          file->write(lengthStrPair.second, lengthStrPair.first);
        } break;
        case COLUMN_TYPE_INTEGER: {
          int num;
          col.getIntegerValue(num);
          file->write((char*)&num, sizeof(num));
        } break;
        case COLUMN_TYPE_DOUBLE_FLOAT: {
          double num;
          col.getDoubleFloatValue(num);
          file->write((char*)&num, sizeof(num));
        } break;
        case COLUMN_TYPE_UNINITIALIZED:
          LOG_ASSERT(false, "error");
          break;
      }
    }
  }
};

void ShardImpl::LoadLatestRowCache(File* file) {
  for (int i = 0; i < kVinNumPerShard; i++) {
    if (file->read((char*)&i, sizeof(i)) == Status::END) break;
    // 需要存kVinNumPerShard个row
    file->read((char*)(&latest_ts_cache_[i]), sizeof(latest_ts_cache_[i]));
    for (int k = 0; k < kColumnNum; k++) {
      auto& col = latest_ts_cols_[i][k];
      switch (engine_->columns_type_[k]) {
        case COLUMN_TYPE_STRING: {
          int32_t len;
          file->read((char*)&len, sizeof(len));
          char buf[len];
          file->read(buf, len);
          col = std::move(ColumnValue(buf, len));
        } break;
        case COLUMN_TYPE_INTEGER: {
          int num;
          file->read((char*)&num, sizeof(num));
          col = std::move(ColumnValue(num));
        } break;
        case COLUMN_TYPE_DOUBLE_FLOAT: {
          double num;
          file->read((char*)&num, sizeof(num));
          col = std::move(ColumnValue(num));
        } break;
        case COLUMN_TYPE_UNINITIALIZED:
          LOG_ASSERT(false, "error");
          break;
      }
    }
  }
};

void ShardImpl::Write(uint16_t vid, const Row& row) {
  // LOG_DEBUG("write shard %d vid", vid);
  int svid = vid2svid(vid);
  while (memtable_[svid]->in_flush_) { // 有可能不停的有协程在flush,所以要用while保证是可用的memtable
    RECORD_FETCH_ADD(write_wait_cnt, 1);
    memtable_[svid]->cv_.wait();
  }
  if (memtable_[svid]->Write(svid, row)) {
    // flush memtable to file
    auto rc = Flush(svid);
    LOG_ASSERT(rc == Status::OK, "flush memtable failed");
  }
};

Status ShardImpl::Flush(uint16_t svid, bool shutdown) {
  if (memtable_[svid] == nullptr) {
    return Status::OK;
  }
  MemTable* immutable_mmt = memtable_[svid];
  immutable_mmt->in_flush_ = true;

  // 如果memtable中的row是更新的，则用memtable的最新来设置缓存的latest row
  if (LIKELY(immutable_mmt->cnt_ != 0)) {
    if (immutable_mmt->mem_latest_row_ts_ > latest_ts_cache_[svid]) {
      latest_ts_cache_[svid] = immutable_mmt->mem_latest_row_ts_;
      int idx = immutable_mmt->mem_latest_row_idx_;
      for (int colid = 0; colid < kColumnNum; colid++) {
        immutable_mmt->columnArrs_[colid]->Get(idx, latest_ts_cols_[svid][colid]);
      }
    }

    BlockMeta* meta =
      block_mgr_[svid]->NewVinBlockMeta(immutable_mmt->cnt_, immutable_mmt->min_ts_, immutable_mmt->max_ts_,
                                        immutable_mmt->max_val_, immutable_mmt->sum_val_);

    // 刷写数据列
    for (int i = 0; i < kColumnNum; i++) {
      immutable_mmt->columnArrs_[i]->Flush(write_buf_[svid], immutable_mmt->cnt_, meta);
    }
    immutable_mmt->ts_col_->Flush(write_buf_[svid], immutable_mmt->cnt_, meta);

    immutable_mmt->cnt_ = 0;
    immutable_mmt->Reset();
    immutable_mmt->cv_.notify();
  }

  if (shutdown) {
    write_buf_[svid]->flush();
  }

  return Status::OK;
};

void ShardImpl::GetLatestRow(uint16_t vid, const std::vector<int>& colids, OUT Row& row) {
  int svid = vid2svid(vid);

  // in the memtable
  if (UNLIKELY(write_phase && memtable_[svid]->mem_latest_row_ts_ > latest_ts_cache_[svid])) {
    memtable_[svid]->GetLatestRow(svid, colids, row);
    return;
  }

  // in the latest cache
  row.timestamp = latest_ts_cache_[svid];
  memcpy(row.vin.vin, engine_->vid2vin_[vid].c_str(), VIN_LENGTH);
  for (auto col_id : colids) {
    //    ColumnValue col_value;
    auto& col_name = engine_->columns_name_[col_id];
    row.columns.insert(std::make_pair(col_name, latest_ts_cols_[svid][col_id]));
  }

  LOG_ASSERT(row.timestamp != -1, "???");
};

// TODO:
// 如果需要从文件读取的列过多，可以考虑控制小batch读取，以免同时分配了过多cache外的内存，导致出现死锁状态，参考lru_wait_cnt指标
void ShardImpl::GetRowsFromTimeRange(uint64_t vid, int64_t lowerInclusive, int64_t upperExclusive,
                                     const std::vector<int>& colids, std::vector<Row>& results) {
  results.reserve((upperExclusive - lowerInclusive) / 1000);
  uint16_t svid = vid2svid(vid);
  if (UNLIKELY(write_phase)) {
    memtable_[svid]->GetRowsFromTimeRange(vid, lowerInclusive, upperExclusive, colids, results);
  }

  std::vector<BlockMeta*> blk_metas;
  block_mgr_[svid]->GetVinBlockMetasByTimeRange(vid, lowerInclusive, upperExclusive, blk_metas);

  File* rfile = data_file_[svid];
  if (UNLIKELY(write_phase)) {
    std::string file_name = VinFileName(engine_->dataDirPath, kTableName, vid);
    rfile = new RandomAccessFile(file_name);
  }

  if (!blk_metas.empty()) {
    RECORD_FETCH_ADD(disk_blk_access_cnt, blk_metas.size());
    for (auto blk_meta : blk_metas) {
      auto func = [blk_meta, this, &colids, rfile, &results, vid, lowerInclusive, upperExclusive,
                   father = this_coroutine::current(), svid]() {
        // 去读对应列的block
        std::vector<ColumnArrWrapper*> need_read_from_file;

        bool hit;
        TsArrWrapper* tmp_ts_col = read_cache_->FetchDataArr<TsArrWrapper>(blk_meta, kColumnNum, hit);
        if (!hit) need_read_from_file.push_back(tmp_ts_col);

        ColumnArrWrapper* cols[colids.size()];

        int icol_idx = 0;
        for (const auto col_id : colids) {
          switch (engine_->columns_type_[col_id]) {
            case COLUMN_TYPE_STRING:
              cols[icol_idx] = read_cache_->FetchDataArr<StringArrWrapper>(blk_meta, col_id, hit);
              if (!hit) need_read_from_file.push_back(cols[icol_idx]);
              break;
            case COLUMN_TYPE_INTEGER:
              cols[icol_idx] = read_cache_->FetchDataArr<IntArrWrapper>(blk_meta, col_id, hit);
              if (!hit) need_read_from_file.push_back(cols[icol_idx]);
              break;
            case COLUMN_TYPE_DOUBLE_FLOAT:
              cols[icol_idx] = read_cache_->FetchDataArr<DoubleArrWrapper>(blk_meta, col_id, hit);
              if (!hit) need_read_from_file.push_back(cols[icol_idx]);
              break;
            case COLUMN_TYPE_UNINITIALIZED:
              LOG_ASSERT(false, "error");
              break;
          }
          icol_idx++;
        }

        if (UNLIKELY(write_phase)) {
          for (auto& col : need_read_from_file) {
            // 异步非Batch IO
            col->Read(rfile, write_buf_[svid], blk_meta);
          }
        } else {
          // 分成多个文件之后操作系统能创建的IO上下文不够了，这里就没法批量了
          auto async_rfile = dynamic_cast<AsyncFile*>(rfile);
          ENSURE(async_rfile != nullptr, "empty async_file");
          for (auto& col : need_read_from_file) {
            while (async_rfile->avalibaleIOC() <= 0) {
              async_rfile->waitIOC();
            }
            auto buf = col->AsyncReadCompressed(async_rfile, blk_meta);
            async_rfile->burst();
            col->Decompressed(buf, blk_meta);
          }
        }

        auto tss = tmp_ts_col->GetDataArr();
        if (lowerInclusive <= blk_meta->min_ts && blk_meta->max_ts < upperExclusive) {
          // 完全包裹了这个block, 就不需要额外的时间戳判断，减少一个if语句
          for (int i = 0; i < blk_meta->num; i++) {
            // build res row
            Row resultRow;
            resultRow.timestamp = tss[i];
            memcpy(resultRow.vin.vin, engine_->vid2vin_[vid].c_str(), VIN_LENGTH);
            for (size_t k = 0; k < colids.size(); k++) {
              ColumnValue col;
              cols[k]->Get(i, col);
              resultRow.columns.insert(std::make_pair(engine_->columns_name_[cols[k]->GetColid()], std::move(col)));
            }
            results.push_back(std::move(resultRow));
          }
        } else {
          for (int i = 0; i < blk_meta->num; i++) {
            // build res row
            if (lowerInclusive <= tss[i] && tss[i] < upperExclusive) {
              Row resultRow;
              resultRow.timestamp = tss[i];
              memcpy(resultRow.vin.vin, engine_->vid2vin_[vid].c_str(), VIN_LENGTH);
              for (size_t k = 0; k < colids.size(); k++) {
                ColumnValue col;
                cols[k]->Get(i, col);
                resultRow.columns.insert(std::make_pair(engine_->columns_name_[cols[k]->GetColid()], std::move(col)));
              }
              results.push_back(std::move(resultRow));
            }
          }
        }

        read_cache_->Release(blk_meta, kColumnNum, tmp_ts_col);
        for (size_t i = 0; i < colids.size(); i++) {
          read_cache_->Release(blk_meta, colids[i], cols[i]);
        }

        for (auto& col : need_read_from_file) {
          auto string_col = dynamic_cast<StringArrWrapper*>(col);
          if (UNLIKELY(string_col != nullptr)) {
            read_cache_->ReviseCacheSize(string_col);
          }
        }
        father->wakeup_once();
      };
      this_coroutine::coro_scheduler()->addTask(std::move(func));
    }
    this_coroutine::co_wait(blk_metas.size());
  }

  if (UNLIKELY(write_phase)) {
    delete rfile;
  }
};

ShardImpl::~ShardImpl() {
  delete read_cache_;
  for (int i = 0; i < kVinNumPerShard; i++) {
    delete write_buf_[i];
    delete memtable_[i];
    delete block_mgr_[i];
  }
};

void ShardImpl::InitMemTable() {
  for (int i = 0; i < kVinNumPerShard; i++) {
    memtable_[i]->Init();
  }
};

// TODO: 改成time range 过程中就计算，减少对row的构建
void ShardImpl::AggregateQuery(uint64_t vid, int64_t lowerInclusive, int64_t upperExclusive, int colid, Aggregator op,
                               std::vector<Row>& res) {
  if (UNLIKELY(write_phase)) {
    std::vector<Row> tmp_res;
    GetRowsFromTimeRange(vid, lowerInclusive, upperExclusive, {colid}, tmp_res);
    // 空范围
    if (UNLIKELY(tmp_res.empty())) {
      return;
    }

    std::string& col_name = engine_->columns_name_[colid];
    ColumnType t = engine_->columns_type_[colid];
    Row row;
    if (op == AVG) {
      if (t == COLUMN_TYPE_INTEGER) {
        aggregateImpl<AvgAggregate<int>, int>(tmp_res, col_name, row);
      } else if (t == COLUMN_TYPE_DOUBLE_FLOAT) {
        aggregateImpl<AvgAggregate<double>, double>(tmp_res, col_name, row);
      } else {
        LOG_ERROR("should not be STRING TYPE");
      }
    } else if (op == MAX) {
      if (t == COLUMN_TYPE_INTEGER) {
        aggregateImpl<MaxAggreate<int>, int>(tmp_res, col_name, row);
      } else if (t == COLUMN_TYPE_DOUBLE_FLOAT) {
        aggregateImpl<MaxAggreate<double>, double>(tmp_res, col_name, row);
      } else {
        LOG_ERROR("should not be STRING TYPE");
      }
    }
    row.timestamp = lowerInclusive;
    ::memcpy(row.vin.vin, engine_->vid2vin_[vid].c_str(), VIN_LENGTH);
    res.push_back(std::move(row));
  } else {
    std::string& col_name = engine_->columns_name_[colid];
    ColumnType t = engine_->columns_type_[colid];
    if (op == AVG) {
      if (t == COLUMN_TYPE_INTEGER) {
        aggregateImpl2<AvgAggregate<int64_t>, int64_t>(vid, lowerInclusive, upperExclusive, colid, res);
      } else if (t == COLUMN_TYPE_DOUBLE_FLOAT) {
        aggregateImpl2<AvgAggregate<double>, double>(vid, lowerInclusive, upperExclusive, colid, res);
      } else {
        LOG_ERROR("should not be STRING TYPE");
      }
    } else if (op == MAX) {
      if (t == COLUMN_TYPE_INTEGER) {
        aggregateImpl2<MaxAggreate<int>, int>(vid, lowerInclusive, upperExclusive, colid, res);
      } else if (t == COLUMN_TYPE_DOUBLE_FLOAT) {
        aggregateImpl2<MaxAggreate<double>, double>(vid, lowerInclusive, upperExclusive, colid, res);
      } else {
        LOG_ERROR("should not be STRING TYPE");
      }
    }
  }
};

void ShardImpl::DownSampleQuery(uint64_t vid, int64_t lowerInclusive, int64_t upperExclusive, int64_t interval,
                                int colid, Aggregator op, const CompareExpression& cmp, std::vector<Row>& res) {
  std::vector<Row> tmp_res;
  GetRowsFromTimeRange(vid, lowerInclusive, upperExclusive, {colid}, tmp_res);
  // 空范围
  if (UNLIKELY(tmp_res.empty())) {
    return;
  }

  std::string& col_name = engine_->columns_name_[colid];
  ColumnType t = engine_->columns_type_[colid];

  if (op == AVG) {
    if (t == COLUMN_TYPE_INTEGER) {
      downsampleImpl<AvgAggregate<int>, int>(tmp_res, col_name, lowerInclusive, upperExclusive, interval, cmp, res);
    } else if (t == COLUMN_TYPE_DOUBLE_FLOAT) {
      downsampleImpl<AvgAggregate<double>, double>(tmp_res, col_name, lowerInclusive, upperExclusive, interval, cmp,
                                                   res);
    } else {
      LOG_ERROR("should not be STRING TYPE");
    }
  } else if (op == MAX) {
    if (t == COLUMN_TYPE_INTEGER) {
      downsampleImpl<MaxAggreate<int>, int>(tmp_res, col_name, lowerInclusive, upperExclusive, interval, cmp, res);
    } else if (t == COLUMN_TYPE_DOUBLE_FLOAT) {
      downsampleImpl<MaxAggreate<double>, double>(tmp_res, col_name, lowerInclusive, upperExclusive, interval, cmp,
                                                  res);
    } else {
      LOG_ERROR("should not be STRING TYPE");
    }
  }
}

} // namespace LindormContest