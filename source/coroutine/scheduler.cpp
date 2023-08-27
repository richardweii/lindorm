#include "coroutine/scheduler.h"

#include <atomic>

#include "coroutine/coroutine.h"
#include "util/likely.h"
#include "util/logging.h"

thread_local Scheduler* scheduler = nullptr;

std::atomic_int id_generator{0};

Scheduler::Scheduler(int coroutine_num) : Coroutine(-1, this), coro_num_(coroutine_num) {
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
  scheduler = this;
  while (!stop) {
    if (UNLIKELY(runnable_list_.empty())) {
      // do none-block polling work
      if (LIKELY(polling_ != nullptr)) {
        polling_();
      }
    }
    wakeup();
    dispatch();
    if (UNLIKELY(runnable_list_.empty())) {
      // do batch flush work
      continue;
    }
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
  if (UNLIKELY(task_pos_ == task_cnt_)) {
    task_cnt_ = queue_.try_dequeue_bulk(task_buf_, kTaskBufLen);
    task_pos_ = 0;
  }
  auto iter = idle_list_.begin();
  while (iter != idle_list_.end() && task_pos_ != task_cnt_) {
    auto coro = *iter;
    coro->task_ = std::move(task_buf_[task_pos_]);
    coro->state_ = CoroutineState::RUNNABLE;
    task_pos_++;
    idle_list_.erase(iter++);
    runnable_list_.push_back(coro);
  }
};

void Scheduler::wakeup() {
  auto cnt = wakeup_list_.try_dequeue_bulk(wakeup_buf_, coro_num_);
  for (int i = 0; i < cnt; i++) {
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