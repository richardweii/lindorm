#pragma once

#include "io/file.h"
#include "util/defer.h"
#include <fcntl.h>
#include <fstream>
#include <mutex>
#include <string>
#include <unordered_map>

namespace LindormContest {

/**
 * 缓存打开的AppendWriteFile文件描述符
 */
class FileManager {
public:
  bool Exist(std::string filename) {
    return access(filename.c_str(), F_OK) != -1;
  }

  File *Open(std::string filename) {
    globalMutex.lock();
    defer { globalMutex.unlock(); };

    auto it = outFiles.find(filename);
    if (it != outFiles.cend()) {
      auto pFileOut = it->second;
      return pFileOut;
    }

    File *file = new AppendWriteFile(filename);
    outFiles.insert(std::make_pair(filename, file));
    return file;
  }

  virtual ~FileManager() {
    for (const auto &pair : outFiles) {
      delete pair.second;
    }
  }

private:
  // TODO: 后续自己实现哈希表，细粒度锁
  std::mutex globalMutex;
  std::unordered_map<std::string, File *> outFiles;
};

} // namespace LindormContest
