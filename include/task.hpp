#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

namespace jz
{

enum class task_state : uint8_t
{
    pending,
    queued,
    running,
    completed,
    canceled,
    failed
};

class task_canceled_exception : public std::exception
{
public:
    const char* what() const noexcept override { return "task canceled"; }
};

class task_base
{
public:
    using clock = std::chrono::steady_clock;

public:
    task_base() noexcept                   = default;

    virtual ~task_base()                   = default;

    task_base(const task_base&)            = delete;
    task_base& operator=(const task_base&) = delete;

    task_base(task_base&& oth) noexcept
        : state_(oth.state_.load(std::memory_order_relaxed))
        , created_(oth.created_)
        , started_(oth.started_)
        , completed_(oth.completed_)
        , completion_cb_(std::move(oth.completion_cb_))
    {
    }

public:
    virtual void execute() = 0;

    virtual void cancel()
    {
        auto expected = task_state::pending;
        if (state_.compare_exchange_strong(expected, task_state::canceled,
                                           std::memory_order_acq_rel)) {
            return;
        }
        expected = task_state::queued;
        state_.compare_exchange_strong(expected, task_state::canceled, std::memory_order_acq_rel);
    }

    bool try_acquire() noexcept
    {
        auto expected = task_state::pending;
        return state_.compare_exchange_strong(expected, task_state::queued,
                                              std::memory_order_acq_rel);
    }

    bool try_start() noexcept
    {
        auto expected = task_state::queued;
        if (state_.compare_exchange_strong(expected, task_state::running,
                                           std::memory_order_acq_rel)) {
            started_ = clock::now();
            return true;
        }
        return false;
    }

    void complete() noexcept
    {
        state_.store(task_state::completed, std::memory_order_release);
        completed_ = clock::now();
        fire_callback();
    }

    void fail() noexcept
    {
        state_.store(task_state::failed, std::memory_order_release);
        completed_ = clock::now();
        fire_callback();
    }

    task_state state() const noexcept { return state_.load(std::memory_order_acquire); }

    bool is_canceled() const noexcept { return state() == task_state::canceled; }

    bool is_completed() const noexcept
    {
        auto s = state();
        return s == task_state::completed || s == task_state::failed;
    }

    auto created_time() const noexcept { return created_; }

    auto started_time() const noexcept { return started_; }

    auto completed_time() const noexcept { return completed_; }

    void set_completion_callback(std::function< void(task_state) > cb) noexcept
    {
        completion_cb_ = std::move(cb);
    }

private:
    void fire_callback()
    {
        if (completion_cb_) {
            completion_cb_(state_.load(std::memory_order_relaxed));
        }
    }

private:
    std::atomic< task_state >        state_ {task_state::pending};
    clock::time_point                created_ {clock::now()};
    clock::time_point                started_ {};
    clock::time_point                completed_ {};
    std::function< void(task_state) > completion_cb_;
};

template < typename >
class task;

template < typename R, typename... Args >
class task< R( Args... ) > final : public task_base
{
    std::function< R( Args... ) > func_;
    std::tuple< Args... >        args_;
    std::promise< R >            promise_;

public:
    explicit task( std::function< R( Args... ) > f, Args... args )
        : func_( std::move( f ) )
        , args_( std::move( args )... )
    {
    }

    std::future< R > get_future() { return promise_.get_future(); }

    void execute() override
    {
        if ( is_canceled() )
            return;

        try {
            if constexpr ( std::is_void_v< R > ) {
                std::apply( func_, args_ );
                promise_.set_value();
            } else {
                promise_.set_value( std::apply( func_, args_ ) );
            }
            complete();
        } catch ( ... ) {
            promise_.set_exception( std::current_exception() );
            fail();
        }
    }

    void cancel() override
    {
        task_base::cancel();
        if ( state() == task_state::canceled ) {
            try {
                throw task_canceled_exception();
            } catch ( ... ) {
                promise_.set_exception( std::current_exception() );
            }
        }
    }
};

class task_void final : public task_base
{
    std::function< void() > func_;

public:
    template < typename F >
    explicit task_void( F&& f )
        : func_( std::forward< F >( f ) )
    {
    }

    void execute() override
    {
        try {
            func_();
            complete();
        } catch ( ... ) {
            fail();
        }
    }

    void cancel() override
    {
        task_base::cancel();
    }
};

template < typename F, typename... Args >
auto make_void_task( F&& f, Args&&... args )
    -> std::shared_ptr< task_void >
{
    return std::make_shared< task_void >(
        [ f = std::forward< F >( f ),
          tup = std::make_tuple( std::forward< Args >( args )... ) ]() mutable {
            std::apply( std::move( f ), std::move( tup ) );
        } );
}

template < typename F, typename... Args >
auto make_task( F&& f, Args&&... args )
    -> std::shared_ptr< task< std::invoke_result_t< F, Args... >( std::decay_t< Args >... ) > >
{
    using R = std::invoke_result_t< F, Args... >;
    return std::make_shared< task< R( std::decay_t< Args >... ) > >(
        std::function< R( std::decay_t< Args >... ) >( std::forward< F >( f ) ),
        std::decay_t< Args >( std::forward< Args >( args ) )... );
}

}  // namespace jz
