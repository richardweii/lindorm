#pragma once

#include <fcntl.h>

#include <fstream>
#include <mutex>
#include <string>
#include <unordered_map>

#include "io/file.h"
#include "util/libaio.h"
#include "util/rwlock.h"

namespace LindormContest {

/**
 * 缓存打开的AppendWriteFile文件描述符
 */
class IOManager {
public:
  bool Exist(std::string filename) { return access(filename.c_str(), F_OK) != -1; }

  File* Open(std::string filename, int flag) {
    rwlock.rlock();
    auto it = opened_files_.find(filename);
    if (it != opened_files_.cend()) {
      auto pFileOut = it->second;
      rwlock.unlock();
      return pFileOut;
    }

    rwlock.unlock();

    rwlock.wlock();
    it = opened_files_.find(filename);
    if (it != opened_files_.cend()) {
      auto pFileOut = it->second;
      rwlock.unlock();
      return pFileOut;
    }

    File* file = new AppendWriteFile(filename, flag);
    opened_files_.insert(std::make_pair(filename, file));
    rwlock.unlock();

    return file;
  }

  File* OpenAsyncWriteFile(std::string filename, int tid) {
    auto it = opened_files_.find(filename);
    if (it != opened_files_.cend()) {
      auto pFileOut = it->second;
      return pFileOut;
    }

    auto* file = new AsyncWriteFile(filename);
    io_ctxs_[tid].push_back(file->getAIOContext());
    opened_files_.insert(std::make_pair(filename, file));
    return file;
  }

  File* OpenAsyncReadFile(std::string filename, int tid) {
    auto it = opened_files_.find(filename);
    if (it != opened_files_.cend()) {
      auto pFileOut = it->second;
      return pFileOut;
    }

    auto* file = new AsyncRandomAccessFile(filename);
    io_ctxs_[tid].push_back(file->getAIOContext());
    opened_files_.insert(std::make_pair(filename, file));
    return file;
  }

  void PollingIOEvents() {
    timespec timeout = {0, 0};
    int ev_cnt = 0;
    auto sched = this_coroutine::coro_scheduler()->tid();
    for (auto ioctx : io_ctxs_[sched]) {
      if ((ev_cnt = io_getevents(*ioctx, 1, AsyncFile::kMaxIONum, (io_event *)events_[sched], &timeout)) != 0) {
        for (int i = 0; i < ev_cnt; i++) {
          AsyncFile::IOContext* io = reinterpret_cast<AsyncFile::IOContext*>(events_[sched][i].data);
          AsyncFile::done(io);
        }
      }
      ev_cnt = 0;
    }
  }

  virtual ~IOManager() {
    for (const auto& pair : opened_files_) {
      delete pair.second;
    }
  }

private:
  RWLock rwlock;
  volatile io_event events_[AsyncFile::kMaxIONum][kWorkerThread];
  std::vector<io_context_t*> io_ctxs_[kWorkerThread];
  std::unordered_map<std::string, File*> opened_files_;
};

} // namespace LindormContest
