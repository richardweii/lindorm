#ifndef _FILE_H_
#define _FILE_H_

#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <thread>

#include "status.h"
#include "util/libaio.h"
#include "util/likely.h"
#include "util/logging.h"
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
  virtual Status write(const char* buf, size_t length) {
    LOG_ASSERT(false, "Not implemented");
    return Status::NotSupported;
  };

  virtual Status read(char* res_buf, size_t length, __off_t pos) {
    LOG_ASSERT(false, "Not implemented");
    return Status::NotSupported;
  }

  virtual Status read(char* res_buf, size_t length) {
    LOG_ASSERT(false, "Not implemented");
    return Status::NotSupported;
  };

  virtual off_t GetFileSz() {
    LOG_ASSERT(false, "Not implemented");
    return -1;
  }

  virtual Status async_write(const char* buf, size_t length) {
    LOG_ASSERT(false, "Not implemented");
    return Status::NotSupported;
  }

  virtual std::string GetFileName() {
    LOG_ASSERT(false, "Not implemented");
    return "";
  }

  virtual ~File() = default;

  int Fd() const { return fd_; }

protected:
  int fd_;
};

class AppendWriteFile : public File {
public:
  AppendWriteFile(const std::string& filename, int flag) : filename(filename) {
    fd_ = open(filename.c_str(), flag, 0600);

    LOG_ASSERT(fd_ >= 0, "fd_ is %d", fd_);
  }

  Status write(const char* buf, size_t length) override {
    file_sz += length;

    while (length > 0) {
      ssize_t write_result = ::write(fd_, buf, length);
      if (UNLIKELY(write_result < 0)) {
        if (LIKELY(errno == EINTR)) {
          continue; // Retry
        }
        LOG_ASSERT(0, "write to file %s failed", filename.c_str());
        return Status::IOError;
      }
      buf += write_result;
      length -= write_result;
    }

    return Status::OK;
  };

  static void write_done(io_context_t ctx, struct iocb* iocb, long res, long res2) {}

  Status async_write(const char* buf, size_t length) override {
    memset(&ctx, 0, sizeof(io_context_t));
    int ret = io_setup(1, &ctx);
    LOG_ASSERT(ret == 0, "io_setup error ret = %d", ret);
    struct iocb io, *p = &io;
    io_prep_pwrite(&io, fd_, (void*)buf, length, file_sz);
    io_set_callback(&io, write_done);
    file_sz += length;
    ret = io_submit(ctx, 1, &p);
    std::this_thread::yield();

    LOG_ASSERT(ret == 1, "io_submit error");

    struct io_event e;
    while (1) {
      if (io_getevents(ctx, ret, ret, &e, NULL) == ret) {
        io_callback_t cb = (io_callback_t)e.data;
        cb(ctx, e.obj, e.res, e.res2);
        break;
      }
      std::this_thread::yield();
    }

    return Status::OK;
  }

  off_t GetFileSz() override { return file_sz; }

  virtual ~AppendWriteFile() {
    if (fd_ != -1) {
      close(fd_);
    }
    // io_destroy(ctx);
  }

  std::string GetFileName() override { return filename; }

private:
  io_context_t ctx;
  std::string filename;
  off_t file_sz = 0;
};

class SequentialReadFile : public File {
public:
  SequentialReadFile(const std::string& filename) : filename(filename) { fd_ = open(filename.c_str(), O_RDONLY, 0600); }

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
        LOG_ASSERT(0, "read from file %s failed, fd_ %d", filename.c_str(), fd_);
        return Status::IOError;
      }
      res_buf += read_result;
      length -= read_result;
    }

    return Status::OK;
  };

  virtual ~SequentialReadFile() {
    if (fd_ != -1) {
      close(fd_);
    }
  }

private:
  std::string filename;
};

class RandomAccessFile : public File {
public:
  RandomAccessFile(const std::string& filename) : filename(filename) { fd_ = open(filename.c_str(), O_RDONLY, 0600); }

  Status read(char* res_buf, size_t length, __off_t pos) override {
    ::lseek(fd_, pos, SEEK_SET);
    while (length > 0) {
      ssize_t read_result = ::read(fd_, res_buf, length);
      if (UNLIKELY(read_result < 0)) {
        if (LIKELY(errno == EINTR)) {
          continue; // Retry
        }
        LOG_ASSERT(0, "read from file %s failed", filename.c_str());
        return Status::IOError;
      }
      res_buf += read_result;
      length -= read_result;
    }

    return Status::OK;
  };

  virtual ~RandomAccessFile() {
    if (fd_ != -1) {
      close(fd_);
    }
  }

private:
  std::string filename;
};

} // namespace LindormContest

#endif
