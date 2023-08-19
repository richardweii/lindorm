/*
 * ngx的spinlock较pthread性能低~20%，但更节省CPU资源
 * ngx的rwlock较pthread性能高~500%
 */

#ifndef _NGX_LOCK_H_
#define _NGX_LOCK_H_

#include <sched.h>
#define NGX_RWLOCK_SPIN 2048
#define NGX_RWLOCK_WLOCK ((unsigned long) -1)

namespace LindormContest {

typedef volatile unsigned long ngx_atomic_t;
typedef unsigned long int ngx_uint_t;

class RWLock {
public:
  void wlock() {
    ngx_uint_t i, n;
    for (;;) {
      if (lock == 0 && __sync_bool_compare_and_swap(&lock, 0, NGX_RWLOCK_WLOCK)) {
        return;
      }
      /*随着等待的次数越来越多，检查lock频率会越来越小。*/
      for (n = 1; n < NGX_RWLOCK_SPIN; n <<= 1) {
        for (i = 0; i < n; i++) {
          __asm__("pause");
        }
        if (lock == 0 && __sync_bool_compare_and_swap(&lock, 0, NGX_RWLOCK_WLOCK)) {
          return;
        }
      }
      /* 让出CPU */
      sched_yield();
    }
  }

  void rlock() {
    ngx_uint_t i, n;
    unsigned long old_readers;
    for (;;) {
      old_readers = lock;
      if (old_readers != NGX_RWLOCK_WLOCK && __sync_bool_compare_and_swap(&lock, old_readers, old_readers + 1)) {
        return;
      }
      for (n = 1; n < NGX_RWLOCK_SPIN; n <<= 1) {
        for (i = 0; i < n; i++) {
          __asm__("pause");
        }
        old_readers = lock;
        if (old_readers != NGX_RWLOCK_WLOCK && __sync_bool_compare_and_swap(&lock, old_readers, old_readers + 1)) {
          return;
        }
      }
      sched_yield();
    }
  }

  void unlock() {
    unsigned long old_readers;
    old_readers = lock;
    if (old_readers == NGX_RWLOCK_WLOCK) {
      lock = 0;
      return;
    }
    for (;;) {
      if (__sync_bool_compare_and_swap(&lock, old_readers, old_readers - 1)) {
        return;
      }
      old_readers = lock;
    }
  }

private:
  ngx_atomic_t lock = 0;
};

inline void ngx_spinlock_lock(ngx_atomic_t *lock, long value, ngx_uint_t spin) {
  ngx_uint_t i, n;
  for (;;) {
    if (*lock == 0 && __sync_bool_compare_and_swap(lock, 0, value)) {
      return;
    }
    /*
    在多处理器下，更好的做法是当前进程不要立刻“让出”正在使用的CPU处理器，而是等待一段时间，看看其他处理器上的
    进程是否会释放锁，这会减少进程间切换的次数
    */
    for (n = 1; n < spin; n <<= 1) {
      /*随着等待的次数越来越多，检查lock频率会越来越小。*/
      for (i = 0; i < n; i++) {
        /*pause是在许多架构体系中专门为了自旋锁而提供的指令，它会告诉CPU现在处于自旋锁等待状悉
        CPU会将自己置于节能状态，降低功耗。*/
        __asm__("pause");
      }
      if (*lock == 0 && __sync_bool_compare_and_swap(lock, 0, value)) {
        return;
      }
    }
    /* 让出CPU */
    sched_yield();
  }
}

#define ngx_spinlock_trylock(lock) (*(lock) == 0 && __sync_bool_compare_and_swap(lock, 0, 1))
#define ngx_spinlock_unlock(lock) *(lock) = 0

} // namespace LindormContest

#endif
