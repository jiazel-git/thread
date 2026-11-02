#pragma once

#include "priority_queue.hpp"
#include "task.hpp"
#include "work_stealing_queue.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

namespace jz
{

static constexpr size_t DEFAULT_LOCAL_QUEUE_SIZE = 256;

struct thread_manager_config {
    size_t                    core_threads = 0;  // 0 = hardware_concurrency - 1
    size_t                    max_threads  = 0;  // 0 = core_threads * 2
    std::chrono::milliseconds idle_timeout {60000};
    float                     growth_factor        = 0.5f;
    float                     shrink_factor        = 0.7f;
    size_t                    min_tasks_per_thread = 10;
};

class thread_manager
{
public:
    using queue_size_t = work_stealing_queue< DEFAULT_LOCAL_QUEUE_SIZE >;

public:
    thread_manager() noexcept = default;

    ~thread_manager() { shutdown(); }

    thread_manager(const thread_manager&)            = delete;
    thread_manager& operator=(const thread_manager&) = delete;

public:
    void start(thread_manager_config cfg)
    {
        using namespace std::chrono;

        if (cfg.core_threads == 0)
            cfg.core_threads = std::thread::hardware_concurrency() - 1;
        if (cfg.max_threads == 0)
            cfg.max_threads = cfg.core_threads * 2;
        if (cfg.core_threads < 1)
            cfg.core_threads = 1;

        cfg_ = std::move(cfg);

        locals_.clear();
        for (size_t i = 0; i < cfg_.max_threads; ++i)
            locals_.push_back(std::make_unique< queue_size_t >());

        running_.store(true, std::memory_order_release);

        for (size_t i = 0; i < cfg_.core_threads; ++i) {
            cores_.emplace_back();
            auto& w = cores_.back();
            w.alive.store(true, std::memory_order_release);
            w.thread = std::thread(&thread_manager::worker_entry, this, std::ref(w), true);
        }

        manager_thread_ = std::thread(&thread_manager::manager_entry, this);

        global_.start_aging();
    }

    void shutdown() noexcept
    {
        if (!running_.exchange(false))
            return;

        global_.stop_aging();
        cv_.notify_all();

        if (manager_thread_.joinable())
            manager_thread_.join();

        for (auto& w : cores_) {
            if (w.thread.joinable())
                w.thread.join();
        }
        cores_.clear();

        for (auto& w : dynamics_) {
            if (w.thread.joinable())
                w.thread.join();
        }
        dynamics_.clear();
    }

    void notify() noexcept { cv_.notify_all(); }

    template < typename F >
    bool push(F&& f, task_priority p = task_priority::normal)
    {
        submitted_.fetch_add(1, std::memory_order_relaxed);
        return global_.push(std::forward< F >(f), p);
    }

    bool push(std::shared_ptr< task_base > const& t, task_priority p = task_priority::normal)
    {
        submitted_.fetch_add(1, std::memory_order_relaxed);
        return global_.push(t, p);
    }

    size_t thread_count() const noexcept { return cores_.size() + dynamics_.size(); }

    size_t task_count() const noexcept { return global_.size(); }

    size_t completed_count() const noexcept { return completed_.load(std::memory_order_relaxed); }

    size_t submitted_count() const noexcept { return submitted_.load(std::memory_order_relaxed); }

    void enable_scaling(bool flag) noexcept { scaling_enabled_ = flag; }

    void pause() noexcept { paused_.store(true, std::memory_order_release); }

    void resume() noexcept
    {
        paused_.store(false, std::memory_order_release);
        cv_.notify_all();
    }

    bool is_paused() const noexcept { return paused_.load(std::memory_order_acquire); }

    void wait_all()
    {
        std::unique_lock lk(done_mtx_);
        done_cv_.wait(lk, [ this ] {
            return completed_.load(std::memory_order_acquire) >=
                       submitted_.load(std::memory_order_acquire) ||
                   !running_.load(std::memory_order_acquire);
        });
    }

    void cancel_pending() { global_.cancel_all(); }

    void set_core_threads(size_t n)
    {
        if (n < 1)
            n = 1;
        cfg_.core_threads = n;
        if (cfg_.max_threads < n)
            cfg_.max_threads = n;
    }

    void set_max_threads(size_t n)
    {
        cfg_.max_threads = n;
        if (cfg_.core_threads > n)
            cfg_.core_threads = n;
    }

private:
    struct worker_thread {
        std::thread         thread;
        std::atomic< bool > alive {false};
    };

private:
    void worker_entry(worker_thread& self, bool is_core)
    {
        // 分配 worker_id
        size_t idx = next_worker_id_.fetch_add(1, std::memory_order_relaxed);

        while (running_.load(std::memory_order_acquire)) {
            if (try_exec(idx))
                continue;

            std::unique_lock lk(mtx_);
            if (is_core) {
                cv_.wait(lk);
            } else {
                if (cv_.wait_for(lk, cfg_.idle_timeout) == std::cv_status::timeout) {
                    self.alive.store(false, std::memory_order_release);
                    return;
                }
            }
        }

        self.alive.store(false, std::memory_order_release);
    }

    bool try_exec(size_t idx)
    {
        if (paused_.load(std::memory_order_acquire))
            return false;

        auto& local = *locals_[ idx ];

        // 1. 本地队列 LIFO（缓存友好）
        if (auto t = local.pop()) {
            (*t)->execute();
            on_done();
            return true;
        }

        // 2. 从全局偷一批到本地
        std::shared_ptr< task_base > buf[16];
        auto n = global_.pop_bulk(buf, 16);
        if (n > 0) {
            for (size_t i = n; i > 0; --i)
                local.push(std::move(buf[ i - 1 ]));
            if (auto t = local.pop()) {
                (*t)->execute();
                on_done();
                return true;
            }
        }

        // 3. 全局单取（兜底）
        if (auto t = global_.pop()) {
            (*t)->execute();
            on_done();
            return true;
        }

        // 4. 随机窃取
        if (locals_.size() > 1) {
            auto n = locals_.size();
            auto r = static_cast< size_t >(std::rand());
            for (size_t i = 0; i < n; ++i) {
                auto victim = (r + i + 1) % n;
                if (victim == idx)
                    continue;
                if (auto t = locals_[ victim ]->steal()) {
                    (*t)->execute();
                    on_done();
                    return true;
                }
            }
        }

        return false;
    }

    void on_done() noexcept
    {
        completed_.fetch_add(1, std::memory_order_relaxed);
        done_cv_.notify_one();
    }

    void manager_entry()
    {
        while (running_.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(cfg_.idle_timeout / 2);
            if (!running_.load(std::memory_order_acquire))
                break;

            cleanup_exited();

            if (scaling_enabled_)
                scale();
        }
    }

    void scale()
    {
        auto backlog = global_.size();
        auto total   = cores_.size() + dynamics_.size();

        if (backlog > total * cfg_.min_tasks_per_thread && total < cfg_.max_threads) {
            auto to_add = std::max< size_t >(1, static_cast< size_t >(total * cfg_.growth_factor));
            to_add      = std::min(to_add, cfg_.max_threads - total);
            for (size_t i = 0; i < to_add; ++i)
                add_worker();
        }
    }

    void add_worker()
    {
        dynamics_.emplace_back();
        auto& w = dynamics_.back();
        w.alive.store(true, std::memory_order_release);
        w.thread = std::thread(&thread_manager::worker_entry, this, std::ref(w), false);

        notify();
    }

    void cleanup_exited()
    {
        for (auto it = dynamics_.begin(); it != dynamics_.end();) {
            if (!it->alive.load(std::memory_order_acquire)) {
                if (it->thread.joinable())
                    it->thread.join();
                it = dynamics_.erase(it);
            } else {
                ++it;
            }
        }
    }

    size_t active_count()
    {
        size_t n = 0;
        for (auto& w : cores_) {
            (void)w;
            ++n;
        }
        for (auto& w : dynamics_) {
            if (w.alive.load(std::memory_order_acquire))
                ++n;
        }
        return n;
    }

private:
    priority_queue                                 global_;
    std::vector< std::unique_ptr< queue_size_t > > locals_;

    std::list< worker_thread > cores_;
    std::list< worker_thread > dynamics_;

    std::atomic< size_t > next_worker_id_ {0};
    std::atomic< size_t > completed_ {0};
    std::atomic< size_t > submitted_ {0};
    std::atomic< bool >   paused_ {false};
    thread_manager_config cfg_;
    std::atomic< bool >   running_ {false};
    bool                  scaling_enabled_ {true};

    std::mutex              mtx_;
    std::condition_variable cv_;
    std::mutex              done_mtx_;
    std::condition_variable done_cv_;
    std::thread             manager_thread_;
};

}  // namespace jz
