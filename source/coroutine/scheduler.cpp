#include "coroutine/scheduler.h"

#include <atomic>

#include "TSDBEngineImpl.h"
#include "coroutine/coroutine.h"
#include "util/likely.h"
#include "util/logging.h"
#include "util/stat.h"

thread_local Scheduler* scheduler = nullptr;

std::atomic_int id_generator{0};

Scheduler::Scheduler(int coroutine_num, int tid) : Coroutine(-1, this), coro_num_(coroutine_num), tid_(tid) {
  // init coroutine
  coros_.reserve(coroutine_num);
  for (int i = 0; i < coroutine_num; i++) {
    coros_.emplace_back(new Coroutine(id_generator.fetch_add(1), this));
    idle_list_.push_back(coros_.back().get());
  }
  scheduler = this;
  wakeup_buf_ = new Coroutine*[coroutine_num];
};

Scheduler::~Scheduler() { delete[] wakeup_buf_; }

void Scheduler::scheduling() {
  // 绑核
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(tid_, &cpuset);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  
  char tmp[20];
  ::sprintf(tmp, "scheduler_%d", tid_);
  pthread_setname_np(pthread_self(), tmp);

  scheduler = this;
  int idle_cnt = 0;
  int yield_cnt = 0;
  int polling_cnt = 0;
  // auto start = TIME_NOW;
  while (!(stop && runnable_list_.empty() && waiting_list_.empty())) {
    polling_cnt++;
    // 只对写阶段做定期sleep给前台让核
    if (LindormContest::write_phase) {
      yield_cnt--;
      if (yield_cnt == 0) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
        yield_cnt = kYieldCnt;
      }
    }

    // auto cur = TIME_NOW;
    // if (UNLIKELY(TIME_DURATION_US(start, cur) > 10000 * 1000)) {
    //   start = cur;
    //   LOG_DEBUG("[tid : %d ] runnable: %ld, waiting: %ld, idle: %ld. task_num %d", tid_, runnable_list_.size(),
    //             waiting_list_.size(), idle_list_.size(), task_num_.load());
    // }

    if (UNLIKELY(polling_cnt == kPollingCnt)) {
      // do none-block polling work
      polling_cnt = 0;
      if (LIKELY(polling_ != nullptr)) {
        polling_();
      }
    }

    wakeup();

    if (!idle_list_.empty()) {
      dispatch();
    }

    if (UNLIKELY(runnable_list_.empty())) {
      std::this_thread::yield();
      if (LindormContest::write_phase && idle_cnt++ >= kIDLECnt) {
        std::this_thread::sleep_for(std::chrono::microseconds(1000));
        idle_cnt = 0;
      }
      continue;
    }
    idle_cnt = 0;
    current_ = runnable_list_.front();
    runnable_list_.pop_front();
    current_->resume();
    switch (current_->state_) {
      case CoroutineState::IDLE:
        idle_list_.push_back(current_);
        break;
      case CoroutineState::RUNNABLE:
        runnable_list_.push_back(current_);
        break;
      case CoroutineState::WAITING:
        waiting_list_.push_front(current_);
        current_->iter = waiting_list_.begin();
        break;
    }
    current_ = nullptr;
  }
};

void Scheduler::dispatch() {
  bool try_deque = false;
  auto iter = idle_list_.begin();
  while (iter != idle_list_.end()) {
    if (LIKELY(task_pos_ != task_cnt_)) {
      auto coro = *iter;
      coro->task_ = std::move(task_buf_[task_pos_]);
      coro->state_ = CoroutineState::RUNNABLE;
      task_pos_++;
      idle_list_.erase(iter++);
      runnable_list_.push_back(coro);
      task_num_--;
    } else if (!try_deque) {
      try_deque = true;
      task_cnt_ = queue_.try_dequeue_bulk(task_buf_, kTaskBufLen);
      task_pos_ = 0;
    } else {
      break;
    }
  }
};

void Scheduler::wakeup() {
  auto cnt = wakeup_list_.try_dequeue_bulk(wakeup_buf_, coro_num_);
  for (size_t i = 0; i < cnt; i++) {
    auto coro = wakeup_buf_[i];
    LOG_ASSERT(coro->waiting_events_ == 0, "coro->waiting_events %d != 0", coro->waiting_events_.load());
    LOG_ASSERT(coro->state_ == CoroutineState::WAITING, "state %d", (int)coro->state_);
    LOG_ASSERT(coro->iter != waiting_list_.end(), "invalid iter");
    waiting_list_.erase(coro->iter);
    runnable_list_.push_back(coro);
    coro->iter = waiting_list_.end();
    coro->state_ = CoroutineState::RUNNABLE;
  }
};
namespace this_coroutine {

Coroutine* current() {
  if (LIKELY(scheduler != nullptr)) return scheduler->current_;
  return nullptr;
}

void yield() {
  if (LIKELY(scheduler != nullptr)) current()->yield();
}

void co_wait(int events) {
  if (LIKELY(scheduler != nullptr)) current()->co_wait(events);
}

bool is_coro_env() { return scheduler != nullptr; };

Scheduler* coro_scheduler() { return scheduler; };
} // namespace this_coroutine