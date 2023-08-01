#include "io/writer.h"
#include "common.h"
#include "util/logging.h"

namespace LindormContest {

Writer::Writer(const char *path) : File(path, O_APPEND | O_WRONLY | O_TRUNC | O_CREAT){};

Status Writer::append(const Slice &data, OUT uint32_t &cur_off) {
  std::unique_lock<std::mutex> lk(lock_);
  auto rc = write(data.data(), data.size());
  if (rc == Status::OK) {
    written_size_ += data.size();
    cur_off = written_size_;
  }
  return rc;
};

Status Writer::write(const char *buf, size_t length) {
  while (length > 0) {
    ssize_t write_result = ::write(fd_, buf, length);
    if (UNLIKELY(write_result < 0)) {
      if (LIKELY(errno == EINTR)) {
        continue; // Retry
      }
      char tmp[128];
      ::sprintf(tmp, "write to file %s failed", filename_);
      ::perror(tmp);
      return Status::IOError;
    }
    buf += write_result;
    length -= write_result;
  }
  return Status::OK;
};

} // namespace LindormContest