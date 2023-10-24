#pragma once

#include <cstdio>
/*
测试的头文件
*/

#define OUTPUT(format, ...) printf(format, ##__VA_ARGS__)
#define ASSERT(condition, format, ...)                                                                      \
  if (!(condition)) {                                                                                       \
    OUTPUT("\033[;31mAssertion ' %s ' Failed!\n%s:%d: " format "\n\033[0m", #condition, __FILE__, __LINE__, \
           ##__VA_ARGS__);                                                                                  \
    abort();                                                                                                \
  }

#define EXPECT(condition, format, ...)                                                                            \
  if (!(condition)) {                                                                                             \
    OUTPUT("\033[;33mExpect ' %s ' \n%s:%d: " format "\n\033[0m", #condition, __FILE__, __LINE__, ##__VA_ARGS__); \
  }

#include <sched.h>

#include "util/logging.h"
#include "util/progress_bar.h"
#include "util/waitgroup.h"

// binding of `progressbar` and `waitgroup`
class Progress {
public:
  Progress(size_t cnt) : wg_(cnt), bar_(cnt), cnt_(cnt) {}
  void Wait() {
    volatile size_t old = cnt_;
    volatile size_t cur = wg_.Cnt();
    while (old > 0) {
      for (size_t i = 0; i < old - cur; i++) {
        bar_.update();
      }
      old = cur;
      cur = wg_.Cnt();
      asm volatile("pause\n" : : : "memory");
    }
    ASSERT(wg_.Cnt() == 0, "counter != 0");
  }
  WaitGroup* wg() { return &wg_; }

  void Finish() { bar_.finish(); }

private:
  WaitGroup wg_;
  progressbar bar_;
  size_t cnt_;
};
