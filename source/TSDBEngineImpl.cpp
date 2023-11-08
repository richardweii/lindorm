//
// You should modify this file.
//
// Refer TSDBEngineSample.cpp to ensure that you have understood
// the interface semantics correctly.
//

#include "TSDBEngineImpl.h"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <sstream>
#include <utility>

#include "common.h"
#include "filename.h"
#include "io/file.h"
#include "io/io_manager.h"
#include "memtable.h"
#include "shard.h"
#include "struct/Vin.h"
#include "util/likely.h"
#include "util/logging.h"
#include "util/stat.h"
#include "util/waitgroup.h"

std::once_flag start_coro;

namespace LindormContest {
bool write_phase = false;

/**
 * This constructor's function signature should not be modified.
 * Our evaluation program will call this constructor.
 * The function's body can be modified.
 */
TSDBEngineImpl::TSDBEngineImpl(const std::string& dataDirPath) : TSDBEngine(dataDirPath) {}

void TSDBEngineImpl::loadSchema() {
  LOG_INFO("start Load Schema");
  // Read schema.
  std::ifstream schemaFin;
  schemaFin.open(getDataPath() + "/schema", std::ios::in);
  if (!schemaFin.is_open() || !schemaFin.good()) {
    std::cout << "Connect new database with empty pre-written data" << std::endl;
    write_phase = true;
    schemaFin.close();
    return;
  }
  write_phase = false;
  int magic = 0;
  schemaFin >> magic;
  if (magic != kColumnNum) {
    std::cerr << "Unexpected columns' num: [" << magic << "]" << std::endl;
    schemaFin.close();
    throw std::exception();
  }
  std::cout << "Found pre-written data with columns' num: [" << kColumnNum << "]" << std::endl;

  columns_type_ = new ColumnType[kColumnNum];
  columns_name_ = new std::string[kColumnNum];

  for (int i = 0; i < kColumnNum; ++i) {
    schemaFin >> columns_name_[i];
    int32_t columnTypeInt;
    schemaFin >> columnTypeInt;
    columns_type_[i] = (ColumnType)columnTypeInt;
    column_idx_.emplace(columns_name_[i], i);
  }
  RemoveFile(getDataPath() + "/schema");
  LOG_INFO("Load Schema finished");
}

int TSDBEngineImpl::connect() {
#ifdef ENABLE_STAT
  cache_hit = 0;
  cache_cnt = 0;
  data_wait_cnt = 0;
  lru_wait_cnt = 0;
#endif
  print_memory_usage();
  loadSchema();
  io_mgr_ = new IOManager();
  for (int i = 0; i < kShardNum; i++) {
    shards_[i] = new ShardImpl(i, this);
  }
  coro_pool_ = new CoroutinePool(kWorkerThread, kCoroutinePerThread);
  // 初始化shard
  for (int i = 0; i < kShardNum; i++) {
    shards_[i]->Init();
  }

  // load vin2vid
  {
    vin2vid_lck_.wlock();
    std::string filename = Vin2vidFileName(dataDirPath, kTableName);
    if (io_mgr_->Exist(filename)) {
      LOG_INFO("start load vin2vid");
      SequentialReadFile file(filename);
      int num;
      file.read((char*)&num, sizeof(num));
      for (int i = 0; i < num; i++) {
        char vin[VIN_LENGTH] = {0};
        uint16_t vid;
        file.read(vin, VIN_LENGTH);
        file.read((char*)&vid, sizeof(vid));
        std::string vin_str(vin, VIN_LENGTH);
        vin2vid_.emplace(std::make_pair(vin_str, vid));
        vid2vin_.emplace(std::make_pair(vid, vin_str));
      }
      LOG_INFO("load vin2vid finished");
      RemoveFile(filename);
    }
    vin2vid_lck_.unlock();
  }

  // load block meta
  LOG_INFO("start load block meta");
  for (int i = 0; i < kShardNum; i++) {
    std::string filename = ShardMetaFileName(dataDirPath, kTableName, i);
    if (io_mgr_->Exist(filename)) {
      SequentialReadFile file(filename);
      shards_[i]->LoadBlockMeta(&file);
      // 删除文件
      RemoveFile(filename);
    }
  }
  LOG_INFO("load block meta finished");

  // load latest row cache
  LOG_INFO("start load latest row cache");
  for (int i = 0; i < kShardNum; i++) {
    std::string filename = LatestRowFileName(dataDirPath, kTableName, i);
    if (io_mgr_->Exist(filename)) {
      SequentialReadFile file(filename);
      shards_[i]->LoadLatestRowCache(&file);
      RemoveFile(filename);
    }
  }
  LOG_INFO("load latest row cache finished");

  // mem_pool_addr_ = std::aligned_alloc(512, kMemoryPoolSz);
  // ENSURE(mem_pool_addr_ != nullptr, "invalid mem_pool_addr");
  // InitMemPool(mem_pool_addr_, kMemoryPoolSz, 2 * MB);

  coro_pool_->registerPollingFunc(std::bind(&IOManager::PollingIOEvents, io_mgr_));
  coro_pool_->start();
  LOG_INFO("======== Finish connect!========");
  return 0;
}

int TSDBEngineImpl::createTable(const std::string& tableName, const Schema& schema) {
  LOG_INFO("start create table %s", tableName.c_str());
  LOG_ASSERT(kColumnNum == (int32_t)schema.columnTypeMap.size(), "schema.column.num != 60");
  columns_name_ = new std::string[kColumnNum];
  columns_type_ = new ColumnType[kColumnNum];
  int i = 0;
  for (auto it = schema.columnTypeMap.cbegin(); it != schema.columnTypeMap.cend(); ++it) {
    column_idx_.emplace(it->first, i);
    columns_name_[i] = it->first;
    columns_type_[i++] = it->second;
  }

  for (int i = 0; i < kShardNum; i++) {
    shards_[i]->InitMemTable();
  }

  LOG_INFO("create table %s finished", tableName.c_str());
  return 0;
}

void TSDBEngineImpl::saveSchema() {
  // Persist the schema.
  if (kColumnNum > 0) {
    std::ofstream schemaFout;
    schemaFout.open(getDataPath() + "/schema", std::ios::out);
    schemaFout << kColumnNum;
    schemaFout << " ";
    for (int i = 0; i < kColumnNum; ++i) {
      schemaFout << columns_name_[i] << " ";
      schemaFout << (int32_t)columns_type_[i] << " ";
    }
    schemaFout.close();
  }
}

int TSDBEngineImpl::shutdown() {
  LOG_INFO("start shutdown");
  inflight_write_.Wait();
  // Close all resources, assuming all writing and reading process has finished.
  // No mutex is fetched by assumptions.
  // save schema
  LOG_INFO("Start Save Schema");
  saveSchema();

  // flush memtable
  LOG_INFO("Start flush memtable");
  WaitGroup wg(kShardNum);
  for (int i = 0; i < kShardNum; i++) {
    coro_pool_->enqueue(
      [&wg, this, i]() {
        for (int svid = 0 ; svid < kVinNumPerShard; svid++) {
          shards_[i]->Flush(svid, true);
        }
        wg.Done();
      },
      shard2tid(i));
  }
  wg.Wait();

  if (write_phase) {
    print_file_summary(columns_type_, columns_name_);
  }
  print_performance_statistic();

  // save block meta
  for (int i = 0; i < kShardNum; i++) {
    std::string filename = ShardMetaFileName(dataDirPath, kTableName, i);
    File* file = io_mgr_->Open(filename, NORMAL_FLAG);
    shards_[i]->SaveBlockMeta(file);
  }

  // save latest row cache
  for (int i = 0; i < kShardNum; i++) {
    std::string filename = LatestRowFileName(dataDirPath, kTableName, i);
    File* file = io_mgr_->Open(filename, NORMAL_FLAG);
    shards_[i]->SaveLatestRowCache(file);
  }

  // save vin2vid
  {
    vin2vid_lck_.wlock();
    std::string filename = Vin2vidFileName(dataDirPath, kTableName);
    File* file = io_mgr_->Open(filename, NORMAL_FLAG);
    int num = vin2vid_.size();
    file->write((char*)&num, sizeof(num));
    for (auto& pair : vin2vid_) {
      LOG_ASSERT(pair.first.size() == VIN_LENGTH, "size = %zu", pair.first.size());
      file->write(pair.first.c_str(), VIN_LENGTH);
      file->write((char*)&pair.second, sizeof(pair.second));
    }
    vin2vid_lck_.unlock();
  }

  if (coro_pool_ != nullptr) {
    delete coro_pool_;
    coro_pool_ = nullptr;
  }

  if (columns_type_ != nullptr) {
    delete[] columns_type_;
    columns_type_ = nullptr;
  }

  if (columns_name_ != nullptr) {
    delete[] columns_name_;
    columns_name_ = nullptr;
  }

  if (io_mgr_ != nullptr) {
    delete io_mgr_;
    io_mgr_ = nullptr;
  }

  for (int i = 0; i < kShardNum; i++) {
    if (shards_[i] != nullptr) {
      delete shards_[i];
      shards_[i] = nullptr;
    }
  }

  // DestroyMemPool();
  // std::free(mem_pool_addr_);
  // mem_pool_addr_ = nullptr;

  return 0;
}

std::mutex mm;
int TSDBEngineImpl::write(const WriteRequest& writeRequest) {
  RECORD_FETCH_ADD(write_cnt, writeRequest.rows.size());
  std::vector<std::vector<Row>> rows(kWorkerThread);
  for (auto& row : writeRequest.rows) {
    uint16_t vid = getVidForWrite(row.vin);
    int shard = sharding(vid);
    int tid = shard2tid(shard);
    rows[tid].emplace_back(std::move(row));

    LOG_ASSERT(vid != UINT16_MAX, "error");
  }

  if (UNLIKELY(write_phase)) {
    while (sync_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  for (int tid = 0; tid < kWorkerThread; tid++) {
    if (rows[tid].empty()) {
      continue;
    }
    inflight_write_.Add();
    coro_pool_->enqueue(
      [this, rs = std::move(rows[tid])]() {
        for (auto& r : rs) {
          uint16_t vid = getVidForRead(r.vin);
          int shard = sharding(vid);
          shards_[shard]->Write(vid, r);
        }
        inflight_write_.Done();
      },
      tid);
  }
  return 0;
}

int TSDBEngineImpl::executeLatestQuery(const LatestQueryRequest& pReadReq, std::vector<Row>& pReadRes) {
#ifdef ENABLE_STAT
#endif
  RECORD_FETCH_ADD(latest_query_cnt, pReadReq.vins.size());
  std::vector<int> colids;
  fillColids(pReadReq.requestedColumns, colids);

  WaitGroup wg;
  std::mutex mt;
  std::vector<std::vector<uint16_t>> vids(kWorkerThread);
  for (const auto& vin : pReadReq.vins) {
    uint16_t vid = getVidForRead(vin);
    if (vid == UINT16_MAX) {
      continue;
    }
    int shard = sharding(vid);
    int tid = shard2tid(shard);
    vids[tid].push_back(vid);
  }

  for (int tid = 0; tid < kWorkerThread; tid++) {
    if (vids[tid].empty()) {
      continue;
    }
    wg.Add();
    coro_pool_->enqueue(
      [this, vs = vids[tid], &colids, &wg, &pReadRes, &mt]() {
        for (auto vid : vs) {
          int shard = sharding(vid);
          Row row;
          shards_[shard]->GetLatestRow(vid, colids, row);
          mt.lock();
          pReadRes.push_back(std::move(row));
          mt.unlock();
        }
        wg.Done();
      },
      tid);
  }

  wg.Wait();
  return 0;
}

int TSDBEngineImpl::executeTimeRangeQuery(const TimeRangeQueryRequest& trReadReq, std::vector<Row>& trReadRes) {
  RECORD_FETCH_ADD(time_range_query_cnt, 1);
  std::vector<int> colids;
  fillColids(trReadReq.requestedColumns, colids);

  uint16_t vid = getVidForRead(trReadReq.vin);
  if (UNLIKELY(vid == UINT16_MAX)) {
    return 0;
  }
#ifdef ENABLE_STAT
  int a = time_range_query_cnt.load();
  if (a > 100000 && a <= 100050) {
    printf("TimeRangeQueryRequest :VID %d  [%ld, %ld) ", vid, trReadReq.timeLowerBound, trReadReq.timeUpperBound);
    for (auto& col : trReadReq.requestedColumns) {
      printf("[%s] ", col.c_str());
    }
    printf("\n");
  }
#endif

  if (UNLIKELY(write_phase)) {
    sync_ = true;
    while (inflight_write_.Cnt() != 0) {
      std::this_thread::yield();
    }
    write_phase_sync.fetch_add(1);
  }

  int shard = sharding(vid);
  WaitGroup wg(1);
  coro_pool_->enqueue(
    [this, shard, vid, &trReadReq, &colids, &trReadRes, &wg]() {
      shards_[shard]->GetRowsFromTimeRange(vid, trReadReq.timeLowerBound, trReadReq.timeUpperBound, colids, trReadRes);
      wg.Done();
    },
    shard2tid(shard));
  wg.Wait();

  if (UNLIKELY(write_phase)) {
    if (write_phase_sync.fetch_sub(1) == 1) {
      sync_ = false;
    }
  }
  return 0;
}

int TSDBEngineImpl::executeAggregateQuery(const TimeRangeAggregationRequest& aggregationReq,
                                          std::vector<Row>& aggregationRes) {
  RECORD_FETCH_ADD(agg_query_cnt, 1);

  uint16_t vid = getVidForRead(aggregationReq.vin);
  if (vid == UINT16_MAX) {
    return 0;
  }

#ifdef ENABLE_STAT
  int a = agg_query_cnt.load();
  if (a > 10000 && a <= 10050) {
    printf("TimeRangeAggregationRequest :VID %d [%ld, %ld) ", vid, aggregationReq.timeLowerBound,
           aggregationReq.timeUpperBound);
    printf("[%s] ", aggregationReq.columnName.c_str());
    printf("\n");
  }
#endif
  int colid = column_idx_.at(aggregationReq.columnName);

  int shard = sharding(vid);
  WaitGroup wg(1);
  coro_pool_->enqueue(
    [this, shard, vid, &aggregationReq, colid, &wg, &aggregationRes]() {
      shards_[shard]->AggregateQuery(vid, aggregationReq.timeLowerBound, aggregationReq.timeUpperBound, colid,
                                     aggregationReq.aggregator, aggregationRes);
      wg.Done();
    },
    shard2tid(shard));
  wg.Wait();
#ifdef ENABLE_STAT
  auto tq = agg_query_cnt.fetch_add(1);
  if (tq == 20000) {
    print_performance_statistic();
  }
#endif

  return 0;
}

int TSDBEngineImpl::executeDownsampleQuery(const TimeRangeDownsampleRequest& downsampleReq,
                                           std::vector<Row>& downsampleRes) {
  RECORD_FETCH_ADD(downsample_query_cnt, 1);

  uint16_t vid = getVidForRead(downsampleReq.vin);
  if (vid == UINT16_MAX) {
    return 0;
  }

#ifdef ENABLE_STAT
  int a = downsample_query_cnt.load();
  if (a > 200000 && a <= 200050) {
    printf("TimeRangeDownsampleRequest :VID %d [%ld, %ld), interval %ld ", vid, downsampleReq.timeLowerBound,
           downsampleReq.timeUpperBound, downsampleReq.interval);
    printf("[%s] ", downsampleReq.columnName.c_str());
    printf("\n");
  }
#endif

  int colid = column_idx_.at(downsampleReq.columnName);

  int shard = sharding(vid);
  WaitGroup wg(1);
  coro_pool_->enqueue(
    [this, shard, vid, &downsampleReq, colid, &wg, &downsampleRes]() {
      shards_[shard]->DownSampleQuery(vid, downsampleReq.timeLowerBound, downsampleReq.timeUpperBound,
                                      downsampleReq.interval, colid, downsampleReq.aggregator,
                                      downsampleReq.columnFilter, downsampleRes);
      wg.Done();
    },
    shard2tid(shard));
  wg.Wait();
#ifdef ENABLE_STAT
  auto tq = downsample_query_cnt.fetch_add(1);
  if (tq == 500000) {
    print_performance_statistic();
  }
#endif
  return 0;
}

TSDBEngineImpl::~TSDBEngineImpl() = default;

uint16_t TSDBEngineImpl::getVidForRead(const Vin& vin) {
  vin2vid_lck_.rlock();
  std::string str(vin.vin, VIN_LENGTH);
  auto it = vin2vid_.find(str);
  if (it == vin2vid_.end()) {
    vin2vid_lck_.unlock();
    LOG_ERROR("lookup invalid VIN");
    return UINT16_MAX;
  }
  uint16_t vid = it->second;
  vin2vid_lck_.unlock();

  return vid;
}

uint16_t TSDBEngineImpl::getVidForWrite(const Vin& vin) {
  uint16_t vid = UINT16_MAX;
  std::string str(vin.vin, VIN_LENGTH);
  vin2vid_lck_.rlock();
  auto it = vin2vid_.find(str);
  if (LIKELY(it != vin2vid_.cend())) {
    vid = it->second;
    vin2vid_lck_.unlock();
  } else {
    vin2vid_lck_.unlock();
    vin2vid_lck_.wlock();
    it = vin2vid_.find(str);
    if (LIKELY(it == vin2vid_.cend())) {
      vid = vid_cnt_;
#ifdef ENABLE_STAT
      if (vid % 10000 == 9999) {
        LOG_INFO("vid %d", vid);
      }
#endif
      vid2vin_.emplace(std::make_pair(vid_cnt_, str));
      vin2vid_.emplace(std::make_pair(str, vid_cnt_++));
      vin2vid_lck_.unlock();
    } else {
      vid = it->second;
      vin2vid_lck_.unlock();
    }
    LOG_ASSERT(vid != UINT16_MAX, "vid == UINT16_MAX");
  }

  return vid;
}

void TSDBEngineImpl::fillColids(const std::set<std::string>& requestedColumns, std::vector<int>& colids) {
  for (auto& col_name : requestedColumns) {
    auto iter = column_idx_.find(col_name);
    if (LIKELY(iter != column_idx_.end())) {
      colids.emplace_back(iter->second);
    } else {
      LOG_ERROR("request invalid column name.");
    }
  }

  if (UNLIKELY(colids.empty())) {
    // LOG_DEBUG("request all columns");
    for (int i = 0; i < kColumnNum; i++) {
      colids.emplace_back(i);
    }
  }
};
} // namespace LindormContest