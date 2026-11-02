#pragma once

#include "priority_queue.hpp"
#include "task.hpp"
#include "thread_manager.hpp"
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <future>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace jz
{

struct pool_statistics {
    size_t tasks_submitted = 0;
    size_t tasks_completed = 0;
    size_t tasks_pending   = 0;
    size_t thread_count    = 0;
};

class thread_pool
{
public:
    using clock = std::chrono::steady_clock;

public:
    thread_pool() noexcept = default;

    ~thread_pool() { shutdown(); }

    thread_pool(const thread_pool&)            = delete;
    thread_pool& operator=(const thread_pool&) = delete;
    thread_pool(thread_pool&&)                 = delete;
    thread_pool& operator=(thread_pool&&)      = delete;

public:
    void start(thread_manager_config cfg = {})
    {
        mgr_ = std::make_shared< thread_manager >();
        mgr_->start(std::move(cfg));
    }

    void shutdown()
    {
        if (mgr_)
            mgr_->shutdown();
    }

    // —— 任务提交 ——

    template < typename F, typename... Args >
    auto submit(F&& f, Args&&... args) -> std::future< std::invoke_result_t< F, Args... > >
    {
        return submit_priority(task_priority::normal, std::forward< F >(f),
                               std::forward< Args >(args)...);
    }

    template < typename F, typename... Args >
    auto submit_priority(task_priority p, F&& f, Args&&... args)
        -> std::future< std::invoke_result_t< F, Args... > >
    {
        auto t   = make_task(std::forward< F >(f), std::forward< Args >(args)...);
        auto fut = t->get_future();
        while (!mgr_->push(t, p)) {
            mgr_->notify();
            std::this_thread::yield();
        }
        mgr_->notify();
        return fut;
    }

    template < typename F, typename... Args >
    void detach(F&& f, Args&&... args)
    {
        detach_priority(task_priority::normal, std::forward< F >(f), std::forward< Args >(args)...);
    }

    template < typename F, typename... Args >
    void detach_priority(task_priority p, F&& f, Args&&... args)
    {
        auto t = make_void_task(std::forward< F >(f), std::forward< Args >(args)...);
        while (!mgr_->push(t, p)) {
            mgr_->notify();
            std::this_thread::yield();
        }
        mgr_->notify();
    }

    template < typename F, typename... Args >
    auto submit_delayed(clock::duration delay, F&& f, Args&&... args)
        -> std::future< std::invoke_result_t< F, Args... > >
    {
        auto t   = make_task(std::forward< F >(f), std::forward< Args >(args)...);
        auto fut = t->get_future();

        std::thread([ mgr = mgr_, delay, t = std::move(t) ] {
            std::this_thread::sleep_for(delay);
            while (!mgr->push(t)) {
                mgr->notify();
                std::this_thread::yield();
            }
            mgr->notify();
        }).detach();

        return fut;
    }

    template < typename F, typename... Args >
    auto submit_batch(F&& f, size_t count, Args&&... args)
        -> std::vector< std::future< std::invoke_result_t< F, size_t, Args... > > >
    {
        using R = std::invoke_result_t< F, size_t, Args... >;
        std::vector< std::future< R > > futures;
        futures.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            futures.push_back(submit(f, i, args...));
        }
        return futures;
    }

    // —— 任务控制 ——

    void wait() { mgr_->wait_all(); }

    template < typename Rep, typename Period >
    bool wait_for(const std::chrono::duration< Rep, Period >& timeout)
    {
        auto deadline = clock::now() + timeout;
        while (task_count() > 0 || mgr_->completed_count() < mgr_->submitted_count()) {
            if (clock::now() >= deadline)
                return false;
            std::this_thread::yield();
        }
        return true;
    }

    void pause() { mgr_->pause(); }

    void resume() { mgr_->resume(); }

    bool is_paused() const { return mgr_->is_paused(); }

    void cancel_pending() { mgr_->cancel_pending(); }

    // —— 线程控制 ——

    size_t thread_count() const noexcept { return mgr_->thread_count(); }

    size_t task_count() const noexcept { return mgr_->task_count(); }

    void set_core_threads(size_t n) { mgr_->set_core_threads(n); }

    void set_max_threads(size_t n) { mgr_->set_max_threads(n); }

    void enable_scaling(bool flag) { mgr_->enable_scaling(flag); }

    // —— 统计 ——

    pool_statistics stats() const noexcept
    {
        return {
            .tasks_submitted = mgr_->submitted_count(),
            .tasks_completed = mgr_->completed_count(),
            .tasks_pending   = mgr_->task_count(),
            .thread_count    = mgr_->thread_count(),
        };
    }

private:
    std::shared_ptr< thread_manager > mgr_;
};

}  // namespace jz
