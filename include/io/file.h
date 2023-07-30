#ifndef _FILE_H_
#define _FILE_H_

#include "status.h"
#include "util/slice.h"
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace LindormContest {

class File {
public:
  File(const char *path, int flag) : fd_(::open(path, flag)) {
    auto name_len = ::strlen(path);
    filename_ = new char[name_len + 1];
    ::memcpy(filename_, path, name_len);
    filename_[name_len] = '\0';
  }

  virtual ~File() { ::close(fd_); };

  const char *filename() const { return filename_; }

  virtual Status write(const char *buf, size_t length) { return Status::NotSupported; };

  virtual Status read(char *buf, size_t length) { return Status::NotSupported; };

protected:
  char *filename_;
  const int fd_;
};

} // namespace LindormContest
#endif