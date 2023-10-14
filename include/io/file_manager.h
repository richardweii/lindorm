#pragma once

#include <fcntl.h>

#include <fstream>
#include <mutex>
#include <string>
#include <unordered_map>

#include "io/file.h"
#include "util/defer.h"
#include "util/rwlock.h"

namespace LindormContest {

/**
 * 缓存打开的AppendWriteFile文件描述符
 */
class FileManager {
public:
  bool Exist(std::string filename) { return access(filename.c_str(), F_OK) != -1; }

  File* Open(std::string filename, int flag) {
    rwlock.rlock();
    auto it = outFiles.find(filename);
    if (it != outFiles.cend()) {
      auto pFileOut = it->second;
      rwlock.unlock();
      return pFileOut;
    }

    rwlock.unlock();

    rwlock.wlock();
    it = outFiles.find(filename);
    if (it != outFiles.cend()) {
      auto pFileOut = it->second;
      rwlock.unlock();
      return pFileOut;
    }

    File* file = new AppendWriteFile(filename, flag);
    outFiles.insert(std::make_pair(filename, file));
    rwlock.unlock();

    return file;
  }

  virtual ~FileManager() {
    for (const auto& pair : outFiles) {
      delete pair.second;
    }
  }

private:
  RWLock rwlock;
  std::unordered_map<std::string, File*> outFiles;
};

} // namespace LindormContest
