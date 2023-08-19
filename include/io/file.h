#ifndef _FILE_H_
#define _FILE_H_

#include "status.h"
#include "util/likely.h"
#include "util/logging.h"
#include "util/slice.h"
#include <fcntl.h>
#include <unistd.h>

namespace LindormContest {

class File {
public:
  virtual Status write(const char *buf, size_t length) { LOG_ASSERT(false, "Not implemented"); };

  virtual Status read(char *res_buf, size_t length, __off_t pos) { LOG_ASSERT(false, "Not implemented"); }

  virtual Status read(char *res_buf, size_t length) { LOG_ASSERT(false, "Not implemented"); };

  virtual off_t GetFileSz() { LOG_ASSERT(false, "Not implemented"); }

  virtual ~File() = default;

  int Fd() const {
    return fd_;
  }

protected:
  int fd_;
};

class AppendWriteFile : public File {
public:
  AppendWriteFile(const std::string &filename) : filename(filename) {
    fd_ = open(filename.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0600);
    LOG_ASSERT(fd_ >= 0, "fd_ is %d", fd_);
  }

  Status write(const char *buf, size_t length) override {
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

  off_t GetFileSz() override {
    return file_sz;
  }

  virtual ~AppendWriteFile() {
    if (fd_ != -1) {
      close(fd_);
    }
  }

private:
  std::string filename;
  off_t file_sz = 0;
};

class SequentialReadFile : public File {
public:
  SequentialReadFile(const std::string &filename) : filename(filename) { fd_ = open(filename.c_str(), O_RDONLY, 0600); }

  Status read(char *res_buf, size_t length) override {
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
  RandomAccessFile(const std::string &filename) : filename(filename) { fd_ = open(filename.c_str(), O_RDONLY, 0600); }

  Status read(char *res_buf, size_t length, __off_t pos) override {
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
