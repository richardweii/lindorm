#pragma once

#include <functional>
#include <list>
#include <memory>
#include <queue>
#include <vector>

#include "coroutine/coroutine.h"

using TaskQueue = moodycamel::ConcurrentQueue<CoroutineTask>;
using AdvanceFunc = std::function<void()>;

namespace this_coroutine {
bool is_coro_env();
Coroutine* current();
void yield();
void co_wait(int events = 1);
Scheduler* coro_scheduler();
} // namespace this_coroutine

class Scheduler : public Coroutine {
  static constexpr size_t kTaskBufLen = 128;

public:
  explicit Scheduler(int coroutine_num);
  ~Scheduler();
  void registerPollingFunc(AdvanceFunc func) { polling_ = std::move(func); }
  void scheduling();
  void exit() { stop = true; }
  void addTask(CoroutineTask&& task) {
    queue_.enqueue(std::move(task));
    queue_sz_++;
  }
  void addWakupCoroutine(Coroutine* coro) { wakeup_list_.enqueue(coro); };

  friend Coroutine* this_coroutine::current();

private:
  Coroutine* current_{};
  void dispatch();
  void wakeup();

  volatile bool stop = false;
  int coro_num_;

  TaskQueue queue_;
  int queue_sz_{0};
  CoroutineTask task_buf_[kTaskBufLen];
  int task_cnt_{0};
  int task_pos_{0};
  AdvanceFunc polling_;
  std::vector<std::shared_ptr<Coroutine>> coros_;
  std::list<Coroutine*> idle_list_;
  std::list<Coroutine*> runnable_list_;
  std::list<Coroutine*> waiting_list_;

  moodycamel::ConcurrentQueue<Coroutine*> wakeup_list_;
  Coroutine** wakeup_buf_;
  // ------------------------------
};