#pragma once

#include "infra/mpmc_queue.hpp"
#include "task.hpp"
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace jz
{

enum class task_priority : uint8_t
{
    critical,
    high,
    normal,
    low,
    background,
    priority_num
};

using priority_type = std::underlying_type_t< task_priority >;

constexpr bool operator==(task_priority a, task_priority b) noexcept
{
    return static_cast< priority_type >(a) == static_cast< priority_type >(b);
}
constexpr bool operator!=(task_priority a, task_priority b) noexcept { return !(a == b); }
constexpr bool operator<(task_priority a, task_priority b) noexcept
{
    return static_cast< priority_type >(a) < static_cast< priority_type >(b);
}
constexpr bool operator>(task_priority a, task_priority b) noexcept { return b < a; }
constexpr bool operator<=(task_priority a, task_priority b) noexcept { return !(b < a); }
constexpr bool operator>=(task_priority a, task_priority b) noexcept { return !(a < b); }

constexpr task_priority& operator++(task_priority& p) noexcept
{
    p = static_cast< task_priority >(static_cast< priority_type >(p) + 1);
    return p;
}
constexpr task_priority operator++(task_priority& p, int) noexcept
{
    auto old = p;
    ++p;
    return old;
}
constexpr task_priority& operator--(task_priority& p) noexcept
{
    p = static_cast< task_priority >(static_cast< priority_type >(p) - 1);
    return p;
}
constexpr task_priority operator--(task_priority& p, int) noexcept
{
    auto old = p;
    --p;
    return old;
}

constexpr task_priority operator+(task_priority p, int n) noexcept
{
    using utype = priority_type;
    auto v      = static_cast< int >(static_cast< utype >(p)) + n;
    return static_cast< task_priority >(static_cast< utype >(v));
}
constexpr task_priority operator+(int n, task_priority p) noexcept { return p + n; }
constexpr task_priority operator-(task_priority p, int n) noexcept { return p + (-n); }
constexpr priority_type operator-(task_priority a, task_priority b) noexcept
{
    return static_cast< priority_type >(a) - static_cast< priority_type >(b);
}

constexpr task_priority& operator+=(task_priority& p, int n) noexcept
{
    p = p + n;
    return p;
}
constexpr task_priority& operator-=(task_priority& p, int n) noexcept
{
    p = p - n;
    return p;
}

class priority_queue
{
public:
    using task  = std::shared_ptr< task_base >;
    using clock = std::chrono::steady_clock;

private:
    struct task_entry {
        task              tk;
        clock::time_point enqueue_time;
    };

    struct delayed_task {
        task              tk;
        task_priority     priority;
        clock::time_point enqueue_time;
    };

    static constexpr size_t PRIORITY_COUNT = static_cast< size_t >(task_priority::priority_num);

public:
    static constexpr auto default_aging_interval  = std::chrono::seconds(1);
    static constexpr auto default_aging_threshold = std::chrono::seconds(60);

public:
    priority_queue() = default;

    ~priority_queue() { stop_aging(); }

    priority_queue(const priority_queue&)            = delete;
    priority_queue& operator=(const priority_queue&) = delete;
    priority_queue(priority_queue&&)                 = delete;
    priority_queue& operator=(priority_queue&&)      = delete;

public:
    void start_aging(clock::duration interval  = default_aging_interval,
                     clock::duration threshold = default_aging_threshold)
    {
        std::lock_guard lk(aging_mtx_);
        if (aging_running_)
            return;
        aging_running_ = true;
        aging_thread_  = std::thread([ this, interval, threshold ] {
            aging_loop(interval, threshold);
        });
    }

    void stop_aging() noexcept
    {
        {
            std::lock_guard lk(aging_mtx_);
            aging_running_ = false;
        }
        aging_cv_.notify_one();
        if (aging_thread_.joinable()) {
            aging_thread_.join();
        }
    }

public:
    bool push(task t, task_priority p = task_priority::normal)
    {
        auto       idx = static_cast< size_t >(p);
        task_entry e {std::move(t), clock::now()};
        return queues_[ idx ].push(std::move(e));
    }

    template < typename F,
               std::enable_if_t< !std::is_convertible_v< std::decay_t< F >, task >, int > = 0 >
    bool push(F&& f, task_priority p = task_priority::normal)
    {
        return push(make_task(std::forward< F >(f)), p);
    }

    std::optional< task > pop()
    {
        task_entry e;
        for (size_t i = 0; i < PRIORITY_COUNT; ++i) {
            if (queues_[ i ].pop(e)) {
                return std::move(e.tk);
            }
        }
        return std::nullopt;
    }

    size_t pop_bulk(task* out, size_t count)
    {
        size_t n = 0;
        for (size_t i = 0; i < PRIORITY_COUNT && n < count; ++i) {
            task_entry e;
            while (n < count && queues_[ i ].pop(e)) {
                out[ n++ ] = std::move(e.tk);
            }
        }
        return n;
    }

    void aging(clock::duration threshold) { drain_and_promote(clock::now(), threshold); }

    void cancel_all()
    {
        for (size_t i = 0; i < PRIORITY_COUNT; ++i) {
            task_entry e;
            while (queues_[ i ].pop(e)) {
                if (e.tk)
                    e.tk->cancel();
            }
        }
    }

    bool empty() const
    {
        for (size_t i = 0; i < PRIORITY_COUNT; ++i) {
            if (queues_[ i ].size_hint() > 0)
                return false;
        }
        return true;
    }

    size_t size() const
    {
        size_t n = 0;
        for (size_t i = 0; i < PRIORITY_COUNT; ++i) {
            n += queues_[ i ].size_hint();
        }
        return n;
    }

private:
    void drain_and_promote(clock::time_point now, clock::duration threshold)
    {
        // 先处理上一轮缓存的任务
        flush_aging_buffer();

        for (int p = static_cast< int >(PRIORITY_COUNT) - 1; p > 0; --p) {
            auto& q         = queues_[ p ];
            bool  hit_front = false;
            while (!hit_front) {
                task_entry e;
                if (!q.pop(e))
                    break;
                if (now - e.enqueue_time >= threshold) {
                    if (!queues_[ p - 1 ].push(task_entry {e.tk, e.enqueue_time})) {
                        aging_buffer_.push_back(
                            {std::move(e.tk), static_cast< task_priority >(p - 1), e.enqueue_time});
                    }
                } else {
                    if (!q.push(std::move(e))) {
                        aging_buffer_.push_back(
                            {std::move(e.tk), static_cast< task_priority >(p), e.enqueue_time});
                    }
                    hit_front = true;
                }
            }
        }
    }

    void flush_aging_buffer()
    {
        std::lock_guard lk(aging_buf_mtx_);
        if (aging_buffer_.empty())
            return;
        std::vector< delayed_task > retry;
        std::swap(aging_buffer_, retry);
        for (auto& d : retry) {
            auto idx = static_cast< size_t >(d.priority);
            if (!queues_[ idx ].push(task_entry {std::move(d.tk), d.enqueue_time})) {
                aging_buffer_.push_back(std::move(d));
            }
        }
    }

    void aging_loop(clock::duration interval, clock::duration threshold)
    {
        std::unique_lock lk(aging_mtx_);
        while (aging_running_) {
            aging_cv_.wait_for(lk, interval, [ this ] {
                return !aging_running_;
            });
            if (!aging_running_)
                break;
            lk.unlock();
            drain_and_promote(clock::now(), threshold);
            lk.lock();
        }
    }

private:
    mpmc_queue< task_entry, default_queue_capacity > queues_[ PRIORITY_COUNT ];

    std::vector< delayed_task > aging_buffer_;
    mutable std::mutex          aging_buf_mtx_;
    std::thread                 aging_thread_;
    std::mutex                  aging_mtx_;
    std::condition_variable     aging_cv_;
    bool                        aging_running_ = false;
};

}  // namespace jz
