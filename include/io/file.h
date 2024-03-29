#ifndef _FILE_H_
#define _FILE_H_

#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <thread>

#include "common.h"
#include "coroutine/coro_cond.h"
#include "coroutine/scheduler.h"
#include "status.h"
#include "util/libaio.h"
#include "util/likely.h"
#include "util/logging.h"
#include "util/mem_pool.h"
#include "util/slice.h"

#define LIBAIO_FLAG O_WRONLY | O_APPEND | O_CREAT | O_DIRECT
#define NORMAL_FLAG O_WRONLY | O_APPEND | O_CREAT

namespace LindormContest {

inline void RemoveFile(std::string file_name) {
  if (access(file_name.c_str(), F_OK) != -1) {
    if (remove(file_name.c_str()) == 0) {
      // LOG_INFO("删除文件 %s 成功", file_name.c_str());
    } else {
      // LOG_INFO("删除文件 %s 失败", file_name.c_str());
    }
  }
}

class File {
public:
  File(const std::string& filename) : filename_(filename) {}
  virtual Status write(const char* buf, size_t length) {
    LOG_ASSERT(false, "Not implemented");
    return Status::NotSupported;
  };

  virtual Status read(char* res_buf, size_t length, off_t pos) {
    LOG_ASSERT(false, "Not implemented");
    return Status::NotSupported;
  }

  virtual Status read(char* res_buf, size_t length) {
    LOG_ASSERT(false, "Not implemented");
    return Status::NotSupported;
  };

  virtual off_t getFileSz() { return file_sz_; }

  virtual std::string getFileName() { return filename_; }

  virtual ~File() {
    if (fd_ != -1) {
      close(fd_);
      fd_ = -1;
    }
  };

  int fd() const { return fd_; }

protected:
  int fd_{-1};
  std::string filename_;
  off_t file_sz_{0};
};

class AppendWriteFile : public File {
public:
  AppendWriteFile(const std::string& filename, int flag) : File(filename) {
    fd_ = open(filename_.c_str(), flag, S_IRUSR | S_IWUSR);
    if (fd_ < 0) {
      perror("Open File failed.");
    }
    LOG_ASSERT(fd_ >= 0, "fd_ is %d", fd_);
  }

  Status write(const char* buf, size_t length) override {
    file_sz_ += length;

    while (length > 0) {
      ssize_t write_result = ::write(fd_, buf, length);
      if (UNLIKELY(write_result < 0)) {
        if (LIKELY(errno == EINTR)) {
          continue; // Retry
        }
        LOG_ASSERT(0, "write to file %s failed", filename_.c_str());
        return Status::IOError;
      }
      buf += write_result;
      length -= write_result;
    }

    return Status::OK;
  };
};

class SequentialReadFile : public File {
public:
  SequentialReadFile(const std::string& filename) : File(filename) {
    fd_ = open(filename.c_str(), O_RDONLY, 0600);
    if (fd_ < 0) {
      perror("Open File failed.");
    }
  }

  Status read(char* res_buf, size_t length) override {
    while (length > 0) {
      ssize_t read_result = ::read(fd_, res_buf, length);
      if (read_result == 0) {
        return Status::END;
      }
      if (UNLIKELY(read_result < 0)) {
        if (LIKELY(errno == EINTR)) {
          continue; // Retry
        }
        LOG_ASSERT(0, "read from file %s failed, fd_ %d", filename_.c_str(), fd_);
        return Status::IOError;
      }
      res_buf += read_result;
      length -= read_result;
    }

    return Status::OK;
  };
};

class RandomAccessFile : public File {
public:
  RandomAccessFile(const std::string& filename) : File(filename) {
    fd_ = open(filename.c_str(), O_RDONLY, 0600);
    if (fd_ < 0) {
      perror("Open File failed.");
    }
  }

  Status read(char* res_buf, size_t length, off_t pos) override {
    ::lseek(fd_, pos, SEEK_SET);
    while (length > 0) {
      ssize_t read_result = ::read(fd_, res_buf, length);
      if (UNLIKELY(read_result < 0)) {
        if (LIKELY(errno == EINTR)) {
          continue; // Retry
        }
        LOG_ASSERT(0, "read from file %s failed", filename_.c_str());
        return Status::IOError;
      }
      res_buf += read_result;
      length -= read_result;
    }

    return Status::OK;
  };
};

class AsyncFile : public File {
public:
  static constexpr int kMaxIONum = 8;
  struct IOContext {
    Coroutine* coro;
    AsyncFile* file;
    iocb cb;
  };

  AsyncFile(const std::string& filename) : File(filename) {
    memset(&ctx_, 0, sizeof(io_context_t));
    for (int i = 0; i < kMaxIONum; i++) {
      iocb_data_[i].coro = nullptr;
      iocb_data_[i].file = this;
    }
  }

  virtual Status async_write(const char* buf, size_t length) {
    LOG_ASSERT(false, "Not implemented");
    return Status::NotSupported;
  };

  virtual Status async_read(char* res_buf, size_t length, off_t pos) {
    LOG_ASSERT(false, "Not implemented");
    return Status::NotSupported;
  }

  virtual Status burst() {
    LOG_ASSERT(false, "Not implemented");
    return Status::NotSupported;
  }

  virtual ~AsyncFile() { io_destroy(ctx_); }

  static void done(IOContext* ioctx) {
    ioctx->coro->wakeup_once();
    ioctx->coro = nullptr;
    ioctx->file->inflight_--;
    ioctx->file->cv_.notify();
    ioctx->file->ctx_list_.push_back(ioctx);
  }

  io_context_t* getAIOContext() { return &ctx_; }

  // 还有能发起多少个IO
  int avalibaleIOC() const {
    ENSURE(inflight_ >= 0 && inflight_ < kMaxIONum, "invalid inflight %d", inflight_);
    return kMaxIONum - inflight_;
  }

  // 等其他人IO结束释放IOC
  void waitIOC() { cv_.wait(); }

protected:
  struct batch_job {
    const char* buf;
    uint32_t size;
    int32_t offset;
  };
  batch_job batchs_[kMaxIONum];
  int batch_idx_{0};

  io_context_t ctx_;
  IOContext iocb_data_[kMaxIONum];
  std::list<IOContext*> ctx_list_;
  volatile int inflight_{0};
  CoroCV cv_;
};

class AsyncWriteFile : public AsyncFile {
public:
  AsyncWriteFile(const std::string& filename) : AsyncFile(filename) {
    fd_ = open(filename.c_str(), LIBAIO_FLAG, S_IRUSR | S_IWUSR);
    LOG_ASSERT(fd_ >= 0, "fd_ is %d", fd_);

    int ret = io_setup(kMaxIONum, &ctx_);
    LOG_ASSERT(ret == 0, "io_setup error ret = %d", ret);
    for (int i = 0; i < kMaxIONum; i++) {
      ctx_list_.push_back(&iocb_data_[i]);
    }
  }

  // NOT THREAD-SAFE，上层需要保证buf、len、offset都是512对齐的
  Status write(const char* buf, size_t length) override {
    async_write(buf, length);
    burst();
    return Status::OK;
  }

  // NOT THREAD-SAFE，上层需要保证buf、len、offset都是512对齐的
  Status async_write(const char* buf, size_t length) override {
    ENSURE(inflight_ >= 0 && inflight_ < kMaxIONum, "too many parallel write");
    ENSURE((length & 511) == 0, "invalid length.");
    ENSURE(((uint64_t)buf & 511) == 0, "invalid buf address.");

    auto& job = batchs_[batch_idx_++];
    job.size = length;
    job.buf = buf;
    job.offset = file_sz_;
    file_sz_ += length;
    inflight_++;

    // LOG_DEBUG("[coro %d] [file %s] async write", this_coroutine::current()->id(), this->filename_.c_str());
    return Status::OK;
  }

  Status burst() override {
    iocb ios[batch_idx_];
    iocb *iosp[batch_idx_];
    for (int i = 0; i < batch_idx_; i++) {
      iosp[i] = &ios[i];
    }
    for (int i = 0; i < batch_idx_; i++) {
      auto& job = batchs_[i];
      iocb* io = &ios[i];
      io_prep_pwrite(io, fd_, (void*)job.buf, job.size, job.offset);

      IOContext* ioctx = ctx_list_.front();
      ctx_list_.pop_front();
      ioctx->coro = this_coroutine::current();
      io->data = ioctx;
    }
    int ret = io_submit(ctx_, batch_idx_, iosp);
    ENSURE(ret == batch_idx_, "io_submit failed");
    int cnt = batch_idx_;
    batch_idx_ = 0;
    this_coroutine::co_wait(cnt);
    return Status::OK;
  }
};

class AsyncRandomAccessFile : public AsyncFile {
public:
  AsyncRandomAccessFile(const std::string& filename) : AsyncFile(filename) {
    fd_ = open(filename.c_str(), O_RDONLY, S_IRUSR | S_IWUSR);
    LOG_ASSERT(fd_ >= 0, "fd_ is %d", fd_);

    int ret = io_setup(kMaxIONum, &ctx_);
    LOG_ASSERT(ret == 0, "io_setup error ret = %d", ret);
    for (int i = 0; i < kMaxIONum; i++) {
      ctx_list_.push_back(&iocb_data_[i]);
    }
  }

  // NOT THREAD-SAFE，上层需要保证buf、len、offset都是512对齐的
  Status read(char* res_buf, size_t length, off_t pos) override {
    async_read(res_buf, length, pos);
    burst();
    return Status::OK;
  }

  // NOT THREAD-SAFE，上层需要保证buf、len、offset都是512对齐的
  Status async_read(char* res_buf, size_t length, off_t pos) override {
    ENSURE(inflight_ >= 0 && inflight_ < kMaxIONum, "too many parallel read");
    ENSURE((length & 511) == 0, "invalid length.");
    ENSURE(((uint64_t)res_buf & 511) == 0, "invalid res_buf.");
    ENSURE((pos & 511) == 0, "invalid pos.");

    auto& job = batchs_[batch_idx_++];
    job.size = length;
    job.buf = res_buf;
    job.offset = pos;

    inflight_++;
    return Status::OK;
  }

  Status burst() override {
    iocb ios[batch_idx_];
    iocb *iosp[batch_idx_];
    for (int i = 0; i < batch_idx_; i++) {
      iosp[i] = &ios[i];
    }
    for (int i = 0; i < batch_idx_; i++) {
      auto& job = batchs_[i];
      iocb* io = &ios[i];
      io_prep_pread(io, fd_, (void*)job.buf, job.size, job.offset);

      IOContext* ioctx = ctx_list_.front();
      ctx_list_.pop_front();
      ioctx->coro = this_coroutine::current();
      io->data = ioctx;
    }
    int ret = io_submit(ctx_, batch_idx_, iosp);
    ENSURE(ret == batch_idx_, "io_submit failed");
    int cnt = batch_idx_;
    batch_idx_ = 0;
    this_coroutine::co_wait(cnt);
    return Status::OK;
  }
};
} // namespace LindormContest
#endif
