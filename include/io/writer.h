#ifndef _WRITER_H_
#define _WRITER_H_
#include "file.h"

namespace LindormContest {
// 只支持追加写
class Writer : public File {
public:
  Writer(const char *path);
  Status append(const Slice &data);
  size_t written_size() const { return written_size_; };

private:
  Status write(const char *buf, size_t length) override;

  size_t written_size_{0};
};
} // namespace LindormContest
#endif