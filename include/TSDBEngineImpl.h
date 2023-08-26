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
#include "io/file_manager.h"
#include "util/rwlock.h"

namespace LindormContest {

class ShardMemtable;
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

  int upsert(const WriteRequest& wReq) override;

  int executeLatestQuery(const LatestQueryRequest& pReadReq, std::vector<Row>& pReadRes) override;

  int executeTimeRangeQuery(const TimeRangeQueryRequest& trReadReq, std::vector<Row>& trReadRes) override;

  ~TSDBEngineImpl() override;

private:
  friend class MemTable;
  void SaveSchema();
  void LoadSchema();

  uint16_t write_get_vid(const Vin& vin) {
    uint16_t vid = UINT16_MAX;
    vin2vid_lck.rlock();
    std::string str(vin.vin, VIN_LENGTH);
    auto it = vin2vid.find(str);
    if (LIKELY(it != vin2vid.cend())) {
      vin2vid_lck.unlock();
      vid = it->second;
    } else {
      vin2vid_lck.unlock();
      vin2vid_lck.wlock();
      it = vin2vid.find(str);
      if (LIKELY(it == vin2vid.cend())) {
        vid = vid_cnt_;
        if (vid % 10000 == 9999) {
          LOG_INFO("vid %d", vid);
        }
        vid2vin.emplace(std::make_pair(vid_cnt_, str));
        vin2vid.emplace(std::make_pair(str, vid_cnt_++));
        vin2vid_lck.unlock();
      } else {
        vid = it->second;
        vin2vid_lck.unlock();
      }
      LOG_ASSERT(vid != UINT16_MAX, "vid == UINT16_MAX");
    }

    return vid;
  }

  uint16_t read_get_vid(const Vin& vin) {
    vin2vid_lck.rlock();
    std::string str(vin.vin, VIN_LENGTH);
    auto it = vin2vid.find(str);
    if (it == vin2vid.end()) {
      vin2vid_lck.unlock();
      LOG_INFO("查找了一个不存在的vin");
      return UINT16_MAX;
    }
    uint16_t vid = it->second;
    vin2vid_lck.unlock();

    return vid;
  }

  std::string table_name_;
  // How many columns is defined in schema for the sole table.
  int columnsNum;
  // The column's type for each column.
  ColumnType* columnsType = nullptr;
  // The column's name for each column.
  std::string* columnsName = nullptr;

  // 用于存储 列名到其在schema中下标的映射
  std::unordered_map<std::string, int> col2colid;

  // 用于存储 17个字节的 vin 到 对应的唯一的一个uint16_t的vid的映射关系
  RWLock vin2vid_lck;
  uint16_t vid_cnt_ = 0;
  std::unordered_map<std::string, uint16_t> vin2vid;
  std::unordered_map<uint16_t, std::string> vid2vin;

  FileManager* file_manager_;
  ShardMemtable* shard_memtable_;
}; // End class TSDBEngineImpl.

} // namespace LindormContest

#endif // LINDORMTSDBCONTESTCPP_TSDBENGINEIMPL_H
