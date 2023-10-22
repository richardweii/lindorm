#pragma once
#include <atomic>
#include <functional>
#include <list>

#include "util/concurrent_queue.h"
#include <ucontext.h>

using coro_id_t = int;
using CoroutineTask = std::function<void()>;

// idle -> runnable -> waiting -> runnable -> idle
enum class CoroutineState {
  IDLE,
  RUNNABLE,
  WAITING,
};

class StackContext {
  static constexpr int kCoroutineStackSize = 2 << 20;  // 2MB
 public:
  StackContext() : fctx_(), base_(new char[kCoroutineStackSize]), size_(kCoroutineStackSize) {}
  ~StackContext() { delete[] base_; }
  inline char *sp() { return base_; }  // 栈地址是从高到低的，所以从边界开始
  inline size_t size() { return size_; }

  inline ucontext_t *fctx() { return &fctx_; }
  inline void setFctx(ucontext_t fctx) { fctx_ = fctx; }

 private:
  ucontext_t fctx_;
  char *base_ = nullptr;  // 协程在堆上的位置
  size_t size_;
};

class Scheduler;
class Coroutine {
 public:
  friend class Scheduler;
  Coroutine(coro_id_t id, Scheduler *sched);
  void yield();
  void co_wait(int events = 1);
  void wakeup_once();
  coro_id_t id() const { return coro_id_; }

  void *user_ctx() const { return user_ctx_; }
  void set_user_ctx(void *ctx) { user_ctx_ = ctx; }
  void clear_user_ctx() { user_ctx_ = nullptr; }

 protected:
  virtual void routine();

 private:
  void switch_coro(Coroutine *current, Coroutine *target);
  void resume();
  static void entrance();

  coro_id_t coro_id_;
  Scheduler *sched_;
  StackContext ctx_;
  CoroutineTask task_;
  std::atomic_int waiting_events_{};
  std::list<Coroutine *>::iterator iter{};
  CoroutineState state_{CoroutineState::IDLE};
  //---------- for user ------------
  void *user_ctx_ = nullptr;
};