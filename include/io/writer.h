#ifndef _WRITER_H_
#define _WRITER_H_
#include "common.h"
#include "file.h"
#include <mutex>

namespace LindormContest {
// 只支持追加写
class Writer : public File {
public:
  Writer(const char *path);
  Status append(const Slice &data, OUT uint32_t &cur_off);

private:
  std::mutex lock_;
  Status write(const char *buf, size_t length) override;

  size_t written_size_{0};
};
} // namespace LindormContest
#endif