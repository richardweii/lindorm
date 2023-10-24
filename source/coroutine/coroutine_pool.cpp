#include "coroutine/coroutine_pool.h"

#include "coroutine/coroutine.h"
#include "coroutine/scheduler.h"

CoroutinePool::CoroutinePool(int thread_num, int coroutine_per_thread) : worker_num_(thread_num) {
  schedulers_.reserve(thread_num);
  for (int i = 0; i < thread_num; i++) {
    schedulers_.emplace_back(new Scheduler(coroutine_per_thread, i));
  }
};

CoroutinePool::~CoroutinePool() {
  for (auto& sched : schedulers_) {
    sched->exit();
  }
  for (auto& thr : workers_) {
    thr.join();
  }
}

// do task in background
void CoroutinePool::start() {
  for (auto& sched : schedulers_) {
    workers_.emplace_back(&Scheduler::scheduling, sched.get());
  }
};
void CoroutinePool::registerPollingFunc(AdvanceFunc func) {
  for (auto& sched : schedulers_) {
    sched->registerPollingFunc(func);
  }
}
