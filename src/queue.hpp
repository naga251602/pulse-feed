#pragma once

#include "common.hpp"
#include <atomic>
#include <mutex>
#include <vector>

namespace pulse {
class Queue {
    const bool lock_free_;
    std::vector<Frame> slots_;
    alignas(64) std::atomic<uint64_t> head_{0};
    alignas(64) std::atomic<uint64_t> tail_{0};
    mutable std::mutex mutex_;

    std::unique_lock<std::mutex> lock() const {
        std::unique_lock guard(mutex_, std::defer_lock);
        if (!lock_free_) guard.lock();
        return guard;
    }

public:
    Queue(size_t capacity, bool lock_free) : lock_free_(lock_free), slots_(capacity) {}
    size_t capacity() const { return slots_.size(); }

    bool push(const Frame& frame, size_t& depth) {
        auto guard = lock();
        auto head = head_.load(std::memory_order_relaxed);
        auto tail = tail_.load(std::memory_order_acquire);
        if (head - tail == slots_.size()) return false;
        slots_[head % slots_.size()] = frame;
        head_.store(head + 1, std::memory_order_release);
        depth = head - tail + 1;
        return true;
    }

    bool pop(Frame& frame) {
        auto guard = lock();
        auto tail = tail_.load(std::memory_order_relaxed);
        auto head = head_.load(std::memory_order_acquire);
        if (tail == head) return false;
        frame = slots_[tail % slots_.size()];
        tail_.store(tail + 1, std::memory_order_release);
        return true;
    }

    // Called only by the producer, whose head cannot change during this read.
    size_t producer_depth() const {
        auto guard = lock();
        return head_.load(std::memory_order_relaxed) - tail_.load(std::memory_order_acquire);
    }
};
}
