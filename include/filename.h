#pragma once

#include "TSDBEngine.hpp"
#include "util/logging.h"
#include <filesystem>
#include <string>

namespace LindormContest {

template <typename T>
static inline std::string NumToStr(T num) {
  std::ostringstream oss;
  oss << num;
  return oss.str();
}

// 对于每个shard，其中每一列所存的文件名
inline std::string ColFileName(const std::string &kDataDirPath, const std::string &tableName, uint16_t shardid, const std::string &colName) {
  LOG_ASSERT(kDataDirPath != "", "kDataDirPath: %s", kDataDirPath.c_str());
  return kDataDirPath + "/" + tableName + "_" + NumToStr<uint16_t>(shardid) + "_" + colName;
}

// 每个shard的元数据的文件名
inline std::string ShardMetaFileName(const std::string &kDataDirPath, const std::string &tableName, uint16_t shardid) {
  LOG_ASSERT(kDataDirPath != "", "kDataDirPath: %s", kDataDirPath.c_str());
  return kDataDirPath + "/" + tableName + "_" + NumToStr<uint16_t>(shardid) + ".meta";
}

// 存储vin到vid的映射关系的文件名
inline std::string Vin2vidFileName(const std::string &kDataDirPath, const std::string &tableName) {
  LOG_ASSERT(kDataDirPath != "", "kDataDirPath: %s", kDataDirPath.c_str());
  return kDataDirPath + "/" + tableName + ".vin2vid";
}

// 存储每个vin所对应的最新ts的row的文件名
inline std::string LatestRowFileName(const std::string &kDataDirPath, const std::string &tableName, uint16_t shardid) {
  LOG_ASSERT(kDataDirPath != "", "kDataDirPath: %s", kDataDirPath.c_str());
  return kDataDirPath + "/" + tableName + "_" + NumToStr<uint16_t>(shardid) + ".latestrow";
}

} // namespace LindormContest
