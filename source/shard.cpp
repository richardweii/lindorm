#include "shard.h"

#include "util/util.h"

namespace LindormContest {

Status ReadCache::PutColumn(BlockMeta* meta, int colid, ColumnArrWrapper* col_data) {
  Col key = {.ptr = meta, .colid = colid};
  LOG_ASSERT(col_data->TotalSize() <= max_sz_, "ColumnArrWrapper is too big.");

  while (total_sz_ + col_data->TotalSize() >= max_sz_) {
    // 缓存已满
    auto victim = lru_list_.back();
    auto sz = victim.column_arr->TotalSize();
    delete victim.column_arr;
    total_sz_ -= sz;
    lru_list_.pop_back();
    cache_.erase(victim);
  }

  // 插入新元素到缓存和链表头部
  lru_list_.push_front(key);
  cache_[key] = lru_list_.begin();
  cache_[key]->column_arr = col_data;
  return Status::OK;
}

ColumnArrWrapper* ReadCache::GetColumn(BlockMeta* meta, int colid) {
  Col key = {.ptr = meta, .colid = colid};
  auto iter = cache_.find(key);
  if (iter == cache_.end()) {
    return nullptr;
  }

  updateList(*iter->second);

  return cache_[key]->column_arr;
};

void ReadCache::updateList(const Col& key) {
  // 移除原来位置的元素，插入到链表头部
  LOG_ASSERT(key.column_arr != nullptr, "empty node");
  lru_list_.erase(cache_[key]);
  lru_list_.push_front(key);
  cache_[key] = lru_list_.begin();
}

void ShardImpl::Init(bool write_phase, File* data_file) {
  data_file_ = data_file;
  write_phase_ = write_phase;

  size_t read_cache_sz = write_phase_ ? kReadCacheSize / 4 : kReadCacheSize;
  read_cache_ = new ReadCache(read_cache_sz);
  block_mgr_ = new BlockMetaManager();

  if (write_phase_) {
    LOG_INFO("write phase");
    write_buf_ = new AlignedWriteBuffer(data_file);
    two_memtable_[0] = new MemTable(shard_id_, engine_);
    two_memtable_[1] = new MemTable(shard_id_, engine_);
    memtable_ = two_memtable_[memtable_id];
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
  while (memtable_->in_flush_) { // 有可能不停的有协程在flush,所以要用while保证是可用的memtable
    memtable_->cv_.wait();
  }
  if (memtable_->Write(svid, row)) {
    // flush memtable to file
    auto rc = Flush();
    LOG_ASSERT(rc == Status::OK, "flush memtable failed");
  }
};

Status ShardImpl::Flush(bool shutdown) {
  if (memtable_ == nullptr) {
    return Status::OK;
  }
  MemTable* immutable_mmt = memtable_;
  immutable_mmt->in_flush_ = true;

  memtable_id = 1 - memtable_id;
  memtable_ = two_memtable_[memtable_id];

  // 如果memtable中的row是更新的，则用memtable的最新来设置缓存的latest row
  if (LIKELY(immutable_mmt->cnt_ != 0)) {
    for (int svid = 0; svid < kVinNumPerShard; svid++) {
      if (immutable_mmt->mem_latest_row_ts_[svid] > latest_ts_cache_[svid]) {
        latest_ts_cache_[svid] = immutable_mmt->mem_latest_row_ts_[svid];
        int idx = immutable_mmt->mem_latest_row_idx_[svid];
        for (int colid = 0; colid < kColumnNum; colid++) {
          immutable_mmt->columnArrs_[colid]->Get(idx, latest_ts_cols_[svid][colid]);
        }
      }
    }

    BlockMeta* meta = block_mgr_->NewVinBlockMeta(immutable_mmt->cnt_, immutable_mmt->min_ts_, immutable_mmt->max_ts_);

    // 先对 vid + ts idx这三列进行排序
    immutable_mmt->sort();

    // 刷写数据列
    for (int i = 0; i < kColumnNum; i++) {
      immutable_mmt->columnArrs_[i]->Flush(write_buf_, immutable_mmt->cnt_, meta);
    }
    immutable_mmt->svid_col_->Flush(write_buf_, immutable_mmt->cnt_, meta);
    immutable_mmt->ts_col_->Flush(write_buf_, immutable_mmt->cnt_, meta);
    immutable_mmt->idx_col_->Flush(write_buf_, immutable_mmt->cnt_, meta);

    immutable_mmt->cnt_ = 0;
    immutable_mmt->Reset();
    immutable_mmt->cv_.notify();
  }

  if (shutdown) {
    write_buf_->flush();
  }

  return Status::OK;
};

void ShardImpl::GetLatestRow(uint16_t vid, const std::vector<int>& colids, OUT Row& row) {
  int svid = vid2svid(vid);

  // in the memtable
  if (UNLIKELY(write_phase_ && memtable_->mem_latest_row_ts_[svid] > latest_ts_cache_[svid])) {
    memtable_->GetLatestRow(svid, colids, row);
    return;
  }

  // in the latest cache
  row.timestamp = latest_ts_cache_[svid];
  memcpy(row.vin.vin, engine_->vid2vin_[vid].c_str(), VIN_LENGTH);
  for (auto col_id : colids) {
    ColumnValue col_value;
    auto& col_name = engine_->columns_name_[col_id];
    row.columns.insert(std::make_pair(col_name, latest_ts_cols_[svid][col_id]));
  }

  LOG_ASSERT(row.timestamp != -1, "???");
};

void ShardImpl::GetRowsFromTimeRange(uint64_t vid, int64_t lowerInclusive, int64_t upperExclusive,
                                     const std::vector<int>& colids, std::vector<Row>& results) {
  if (UNLIKELY(write_phase_)) {
    memtable_->GetRowsFromTimeRange(vid, lowerInclusive, upperExclusive, colids, results);
  }

  std::vector<BlockMeta*> blk_metas;
  block_mgr_->GetVinBlockMetasByTimeRange(vid, lowerInclusive, upperExclusive, blk_metas);

  File* rfile = data_file_;
  if (UNLIKELY(write_phase_)) {
    std::string file_name = ShardDataFileName(engine_->dataDirPath, kTableName, shard_id_);
    rfile = new RandomAccessFile(file_name);
  }

  if (!blk_metas.empty()) {
    RECORD_FETCH_ADD(tr_disk_blk_query_cnt, blk_metas.size());
    for (auto blk_meta : blk_metas) {
      // 去读对应列的block
      VidArrWrapper* tmp_vid_col = read_cache_->FetchDataArr<VidArrWrapper>(blk_meta, kColumnNum);
      TsArrWrapper* tmp_ts_col = read_cache_->FetchDataArr<TsArrWrapper>(blk_meta, kColumnNum + 1);
      IdxArrWrapper* tmp_idx_col = read_cache_->FetchDataArr<IdxArrWrapper>(blk_meta, kColumnNum + 2);

      ColumnArrWrapper* cols[colids.size()];

      int icol_idx = 0;
      for (const auto col_id : colids) {
        switch (engine_->columns_type_[col_id]) {
          case COLUMN_TYPE_STRING:
            cols[icol_idx] = read_cache_->FetchDataArr<StringArrWrapper>(blk_meta, col_id);
            break;
          case COLUMN_TYPE_INTEGER:
            cols[icol_idx] = read_cache_->FetchDataArr<IntArrWrapper>(blk_meta, col_id);
            break;
          case COLUMN_TYPE_DOUBLE_FLOAT:
            cols[icol_idx] = read_cache_->FetchDataArr<DoubleArrWrapper>(blk_meta, col_id);
            break;
          case COLUMN_TYPE_UNINITIALIZED:
            LOG_ASSERT(false, "error");
            break;
        }
        cols[icol_idx++]->Read(rfile, write_buf_, blk_meta);
      }
      tmp_vid_col->Read(rfile, write_buf_, blk_meta);
      tmp_ts_col->Read(rfile, write_buf_, blk_meta);
      tmp_idx_col->Read(rfile, write_buf_, blk_meta);

      // 现在vid + ts是有序的，可以直接应用二分查找
      std::vector<uint16_t> idxs;
      std::vector<int64_t> tss;
      findMatchingIndices(tmp_vid_col->GetDataArr(), tmp_ts_col->GetDataArr(), tmp_idx_col->GetDataArr(), blk_meta->num,
                          vid2svid(vid), lowerInclusive, upperExclusive, idxs, tss);

      if (!idxs.empty()) {
        for (int i = 0; i < (int)idxs.size(); i++) {
          // build res row
          Row resultRow;
          resultRow.timestamp = tss[i];
          memcpy(resultRow.vin.vin, engine_->vid2vin_[vid].c_str(), VIN_LENGTH);
          for (size_t k = 0; k < colids.size(); k++) {
            ColumnValue col;
            cols[k]->Get(idxs[i], col);
            resultRow.columns.insert(std::make_pair(engine_->columns_name_[cols[k]->GetColid()], std::move(col)));
          }
          results.push_back(std::move(resultRow));
        }
      }
    }
  }

  if (rfile != data_file_) {
    delete rfile;
  }
};

ShardImpl::~ShardImpl() {
  delete memtable_;
  delete read_cache_;
  delete write_buf_;
  delete block_mgr_;
};

void ShardImpl::InitMemTable() {
  LOG_ASSERT(memtable_ != nullptr, "invalid memtable nullptr.");
  two_memtable_[0]->Init();
  two_memtable_[1]->Init();
};
} // namespace LindormContest