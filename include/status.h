#ifndef _STATUS_H
#define _STATUS_H

namespace LindormContest {
enum class Status {
  OK = 0,
  InvalidArgument,
  IOError,
  NotFound,
  NotSupported,
  ExceedCapacity,
  END,
  NotEnough,
};
} // namespace LindormContest

#endif
