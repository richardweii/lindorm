//
// You should modify this file.
//
// Refer TSDBEngineSample.h to ensure that you have understood
// the interface semantics correctly.
//

#ifndef LINDORMTSDBCONTESTCPP_TSDBENGINEIMPL_H
#define LINDORMTSDBCONTESTCPP_TSDBENGINEIMPL_H
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

#include "Hasher.hpp"
#include "TSDBEngine.hpp"
#include "coroutine/coroutine_pool.h"
#include "io/io_manager.h"
#include "util/rwlock.h"
#include "util/waitgroup.h"

namespace LindormContest {
extern bool write_phase;
class ShardImpl;
class TSDBEngineImpl : public TSDBEngine {
public:
  /**
   * This constructor's function signature should not be modified.
   * Our evaluation program will call this constructor.
   * The function's body can be modified.
   */
  explicit TSDBEngineImpl(const std::string& dataDirPath);

  int connect() override;

  int createTable(const std::string& tableName, const Schema& schema) override;

  int shutdown() override;

  int write(const WriteRequest& writeRequest) override;

  void wait_write() { inflight_write_.Wait(); };

  int executeLatestQuery(const LatestQueryRequest& pReadReq, std::vector<Row>& pReadRes) override;

  int executeTimeRangeQuery(const TimeRangeQueryRequest& trReadReq, std::vector<Row>& trReadRes) override;

  // 遍历的时候在线处理聚合
  int executeAggregateQuery(const TimeRangeAggregationRequest& aggregationReq,
                            std::vector<Row>& aggregationRes) override;

  // 遍历的时候分桶处理
  int executeDownsampleQuery(const TimeRangeDownsampleRequest& downsampleReq, std::vector<Row>& downsampleRes) override;

  ~TSDBEngineImpl() override;

private:
  void fillColids(const std::set<std::string>& requestedColumns, std::vector<int>& colids);

  friend class MemTable;
  friend class ShardImpl;
  void saveSchema();
  void loadSchema();

  uint16_t getVidForWrite(const Vin& vin);

  uint16_t getVidForRead(const Vin& vin);

  // The column's type for each column.
  ColumnType* columns_type_ = nullptr;
  // The column's name for each column.
  std::string* columns_name_ = nullptr;

  // 用于存储 列名到其在schema中下标的映射
  std::unordered_map<std::string, int> column_idx_;

  // 用于存储 17个字节的 vin 到 对应的唯一的一个uint16_t的vid的映射关系
  RWLock vin2vid_lck_;
  uint16_t vid_cnt_ = 0;
  std::unordered_map<std::string, uint16_t> vin2vid_;
  std::unordered_map<uint16_t, std::string> vid2vin_;

  IOManager* io_mgr_{nullptr};
  ShardImpl* shards_[kShardNum];

  CoroutinePool* coro_pool_{nullptr};
  void* mem_pool_addr_{nullptr};

  WaitGroup inflight_write_{0};

  std::thread* stat_thread_{nullptr};
  volatile bool stop_{false};

  volatile bool sync_{false};
}; // End class TSDBEngineImpl.

} // namespace LindormContest

#endif // LINDORMTSDBCONTESTCPP_TSDBENGINEIMPL_H
