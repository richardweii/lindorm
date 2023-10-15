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
#include "io/io_manager.h"
#include "util/rwlock.h"

namespace LindormContest {

class ShardMemtable;
class TSDBEngineImpl : public TSDBEngine {
  // How many columns is defined in schema for the sole table.
  static constexpr int kColumnNum = 60;
public:
    /**
     * This constructor's function signature should not be modified.
     * Our evaluation program will call this constructor.
     * The function's body can be modified.
     */
    explicit TSDBEngineImpl(const std::string &dataDirPath);

    int connect() override;

    int createTable(const std::string &tableName, const Schema &schema) override;

    int shutdown() override;

    int write(const WriteRequest &writeRequest) override;

    int executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) override;

    int executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) override;

    // 遍历的时候在线处理聚合
    int executeAggregateQuery(const TimeRangeAggregationRequest &aggregationReq, std::vector<Row> &aggregationRes) override;

    // 遍历的时候分桶处理    
    int executeDownsampleQuery(const TimeRangeDownsampleRequest &downsampleReq, std::vector<Row> &downsampleRes) override;

    ~TSDBEngineImpl() override;
private:
friend class MemTable;
  void saveSchema();
  void loadSchema();

  uint16_t getVidForWrite(const Vin& vin);

  uint16_t getVidForRead(const Vin& vin);

  std::string table_name_;
  // The column's type for each column.
  ColumnType* columns_type_ = nullptr;
  // The column's name for each column.
  std::string* columns_name_ = nullptr;

  // 用于存储 列名到其在schema中下标的映射
  std::unordered_map<std::string, int> column_idx_;

  // 用于存储 17个字节的 vin 到 对应的唯一的一个uint16_t的vid的映射关系
  RWLock vin2vid_lck_;
  uint16_t vid_cnt_ = 0;
  std::unordered_map<std::string, uint16_t> vin2vid_;   // TODO:改成Vin
  std::unordered_map<uint16_t, std::string> vid2vin_;

  IOManager* io_manager_;
  ShardMemtable* shard_memtable_;
}; // End class TSDBEngineImpl.

}

#endif //LINDORMTSDBCONTESTCPP_TSDBENGINEIMPL_H
