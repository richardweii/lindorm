#pragma once
// 用于协程中的条件变量，eg:一个协程等待其他协程对某个对象的使用结束后才能使用

#include "coroutine/scheduler.h"
class CoroCV {
public:
  void notify() {
    for (auto& coro : waitting_list_) {
      coro->wakeup_once();
    }
    waitting_list_.clear();
  };

  void wait() {
    waitting_list_.push_back(this_coroutine::current());
    this_coroutine::co_wait();
  }

private:
  std::vector<Coroutine*> waitting_list_;
};