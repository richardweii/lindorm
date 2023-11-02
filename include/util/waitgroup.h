#pragma once
// C++ WaitGroup like golang sync.WaitGroup
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <mutex>
// class WaitGroup {
//    public:
//     WaitGroup(int init = 0) : counter(init), done_flag(false) {}

//     void Add(int incr = 1) {
//         // assert(!done_flag && "'Add' after 'Done' is not allowed");
//         counter += incr;
//     }

//     void Done() {
//         // if (!done_flag) done_flag = true;
//         if (--counter <= 0) cond.notify_all();
//     }

//     void Wait() {
//         std::unique_lock<std::mutex> lock(mutex);
//         cond.wait(lock, [&] { return counter <= 0; });
//     }

//     int Cnt() const { return counter.load(std::memory_order_relaxed); }

//    private:
//     std::mutex mutex;
//     std::condition_variable cond;
//     std::atomic<int> counter;
//     volatile bool done_flag;
// };

class WaitGroup {
public:
    WaitGroup(int init = 0) : count(init) {}

    void Add(int delta = 1) {
        std::unique_lock<std::mutex> lock(mutex);
        count += delta;
    }

    void Done() {
        std::unique_lock<std::mutex> lock(mutex);
        count--;
        if (count == 0) {
            condition.notify_all();
        }
    }

    void Wait() {
        std::unique_lock<std::mutex> lock(mutex);
        while (count > 0) {
            condition.wait(lock);
        }
    }

    int Cnt() {
        return count;
    }

private:
    volatile int count;
    std::mutex mutex;
    std::condition_variable condition;
};