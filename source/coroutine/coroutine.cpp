#include "coroutine/coroutine.h"

#include "coroutine/coroutine_pool.h"
#include "coroutine/scheduler.h"
#include "util/logging.h"

void Coroutine::entrance() {
  Coroutine* coro = this_coroutine::current();
  Coroutine* sched = coro->sched_;
  coro->routine();
}

void Coroutine::routine() {
  while (true) {
    state_ = CoroutineState::RUNNABLE;
    // do some work
    task_();
    // exit
    state_ = CoroutineState::IDLE;
    clear_user_ctx();
    switch_coro(this, sched_);
  }
}

void Coroutine::yield() {
  if (this == sched_) {
    LOG_FATAL("DOT NOT ALLOW YIELD SCHEDULER.");
  }
  state_ = CoroutineState::RUNNABLE;
  switch_coro(this, sched_);
}

// only sheduler can use
void Coroutine::resume() { switch_coro(sched_, this); }

void Coroutine::switch_coro(Coroutine* current, Coroutine* target) {
  if (swapcontext(current->ctx_.fctx(), target->ctx_.fctx()) == -1) {
    perror("switch coroutine failed");
    exit(EXIT_FAILURE);
  }
}

void Coroutine::co_wait(int events) {
  if (events == 0) return;
  if (waiting_events_.fetch_add(events) + events > 0) {
    state_ = CoroutineState::WAITING;
    switch_coro(this, sched_);
  }
}

extern thread_local Scheduler* scheduler;

void Coroutine::wakeup_once() {
  if (waiting_events_ == 0) {
    LOG_DEBUG("ERROR");
  }
  if ((--waiting_events_) == 0) {
    LOG_ASSERT(scheduler != nullptr, "invalid scheduler");
    sched_->addWakupCoroutine(this);
  }
}

Coroutine::Coroutine(coro_id_t id, Scheduler* sched) : coro_id_(id), sched_(sched) {
  if (getcontext(ctx_.fctx()) == -1) {
    perror("get context failed");
  };
  ctx_.fctx()->uc_stack.ss_sp = ctx_.sp();
  ctx_.fctx()->uc_stack.ss_size = ctx_.size();

  if (sched != this) {
    ctx_.fctx()->uc_link = sched->ctx_.fctx();
    makecontext(ctx_.fctx(), &Coroutine::entrance, 0);
  }
}
