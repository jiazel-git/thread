#include "task.hpp"
#include <cassert>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

static int failures = 0;

#define TEST( name, expr )                                                        \
    do {                                                                          \
        if ( !( expr ) ) {                                                        \
            std::cerr << "  FAIL: " << name << " (" << #expr << ")" << std::endl; \
            ++failures;                                                           \
        } else {                                                                  \
            std::cout << "  PASS: " << name << std::endl;                         \
        }                                                                         \
    } while ( 0 )

using namespace std::chrono_literals;

// ========== 正确性测试 ==========

void test_initial_state() {
    std::cout << "[test_initial_state]" << std::endl;
    auto t = jz::make_task( [] {} );
    TEST( "initial state is pending", t->state() == jz::task_state::pending );
    TEST( "not completed", !t->is_completed() );
    TEST( "not canceled", !t->is_canceled() );
}

void test_state_transitions() {
    std::cout << "[test_state_transitions]" << std::endl;
    auto t = jz::make_task( [] {} );

    // pending → queued
    TEST( "try_acquire succeeds", t->try_acquire() );
    TEST( "state is queued", t->state() == jz::task_state::queued );
    TEST( "try_acquire again fails", !t->try_acquire() );

    // queued → running
    TEST( "try_start succeeds", t->try_start() );
    TEST( "state is running", t->state() == jz::task_state::running );
    TEST( "try_start again fails", !t->try_start() );

    // running → completed
    t->execute();
    TEST( "state is completed after execute", t->state() == jz::task_state::completed );
    TEST( "is_completed", t->is_completed() );
}

void test_cancel_before_acquire() {
    std::cout << "[test_cancel_before_acquire]" << std::endl;
    auto t = jz::make_task( [] {} );
    t->cancel();
    TEST( "state is canceled", t->state() == jz::task_state::canceled );
    TEST( "try_acquire fails after cancel", !t->try_acquire() );
    TEST( "try_start fails after cancel", !t->try_start() );
}

void test_cancel_after_acquire() {
    std::cout << "[test_cancel_after_acquire]" << std::endl;
    auto t = jz::make_task( [] {} );
    t->try_acquire();
    t->cancel();
    TEST( "state is canceled after acquire+cancel", t->state() == jz::task_state::canceled );
    TEST( "try_start fails", !t->try_start() );
}

void test_cannot_cancel_running() {
    std::cout << "[test_cannot_cancel_running]" << std::endl;
    auto t = jz::make_task( [] {} );
    t->try_acquire();
    t->try_start();
    t->cancel();
    TEST( "cancel does not affect running", t->state() == jz::task_state::running );
}

void test_execute_calls_function() {
    std::cout << "[test_execute_calls_function]" << std::endl;
    int  val = 0;
    auto t   = jz::make_task( [ & ] { val = 42; } );
    t->try_acquire();
    t->try_start();
    t->execute();
    TEST( "function was executed", val == 42 );
}

void test_execute_failure() {
    std::cout << "[test_execute_failure]" << std::endl;
    auto t = jz::make_task( [] { throw std::runtime_error( "oops" ); } );
    t->try_acquire();
    t->try_start();
    t->execute();
    TEST( "state is failed after exception", t->state() == jz::task_state::failed );
    TEST( "is_completed includes failed", t->is_completed() );
}

void test_timing() {
    std::cout << "[test_timing]" << std::endl;
    auto t = jz::make_task( [] {} );

    // created time should be set
    auto created = t->created_time();
    TEST( "created time is set", created.time_since_epoch().count() > 0 );

    // started time should be 0 before start
    TEST( "started time is zero before start", t->started_time().time_since_epoch().count() == 0 );

    t->try_acquire();
    std::this_thread::sleep_for( 1ms );
    t->try_start();
    auto started = t->started_time();
    TEST( "started time is after created", started > created );

    t->execute();
    auto completed = t->completed_time();
    TEST( "completed time is after started", completed > started );
}

void test_completion_callback() {
    std::cout << "[test_completion_callback]" << std::endl;
    auto t = jz::make_task( [] {} );

    jz::task_state cb_state = jz::task_state::pending;
    t->set_completion_callback( [ & ]( auto s ) { cb_state = s; } );

    t->try_acquire();
    t->try_start();
    t->execute();

    TEST( "completion callback fired with completed state", cb_state == jz::task_state::completed );
}

void test_completion_callback_failure() {
    std::cout << "[test_completion_callback_failure]" << std::endl;
    auto t = jz::make_task( [] { throw 0; } );

    jz::task_state cb_state = jz::task_state::pending;
    t->set_completion_callback( [ & ]( auto s ) { cb_state = s; } );

    t->try_acquire();
    t->try_start();
    t->execute();

    TEST( "completion callback fired with failed state", cb_state == jz::task_state::failed );
}

void test_make_task() {
    std::cout << "[test_make_task]" << std::endl;
    int  val = 0;
    auto t   = jz::make_task( [ & ] { val = 10; } );
    TEST( "make_task creates pending task", t->state() == jz::task_state::pending );

    t->try_acquire();
    t->try_start();
    t->execute();
    TEST( "make_task function executed", val == 10 );
}

void test_execute_chain() {
    std::cout << "[test_execute_chain]" << std::endl;
    // 完整执行链：pending → queued → running → completed
    int  val = 0;
    auto t   = jz::make_task( [ & ] { val = 99; } );

    TEST( "chain: pending", t->state() == jz::task_state::pending );
    TEST( "chain: try_acquire", t->try_acquire() );
    TEST( "chain: try_start", t->try_start() );
    t->execute();
    TEST( "chain: executed", val == 99 );
    TEST( "chain: completed", t->state() == jz::task_state::completed );
}

void test_skip_if_canceled() {
    std::cout << "[test_skip_if_canceled]" << std::endl;
    int  val = 0;
    auto t   = jz::make_task( [ & ] { val = 1; } );

    t->try_acquire();
    t->cancel();
    // worker 应该检测到 canceled 并跳过执行
    if ( t->try_start() ) {
        t->execute();
    }
    // val 不应该被修改
    TEST( "skipped execution", val == 0 );
}

void test_non_void_return()
{
    std::cout << "[test_non_void_return]" << std::endl;
    auto t   = jz::make_task( [] { return 42; } );
    auto fut = t->get_future();
    t->try_acquire();
    t->try_start();
    t->execute();
    TEST( "future returns value", fut.get() == 42 );
    TEST( "state is completed", t->state() == jz::task_state::completed );
}

void test_void_future()
{
    std::cout << "[test_void_future]" << std::endl;
    int val = 0;
    auto t   = jz::make_task( [ & ] { val = 7; } );
    auto fut = t->get_future();
    t->try_acquire();
    t->try_start();
    t->execute();
    fut.get();
    TEST( "void future completes", val == 7 );
}

void test_future_cancel()
{
    std::cout << "[test_future_cancel]" << std::endl;
    auto t   = jz::make_task( [] { return 99; } );
    auto fut = t->get_future();
    t->try_acquire();
    t->cancel();
    // 尝试执行（应该被 cancel 阻止）
    if ( t->try_start() ) {
        t->execute();
    }
    // future 应该抛出 task_canceled_exception
    bool caught = false;
    try {
        fut.get();
    } catch ( const jz::task_canceled_exception& ) {
        caught = true;
    }
    TEST( "canceled task throws on future get", caught );
}

void test_future_exception()
{
    std::cout << "[test_future_exception]" << std::endl;
    auto t   = jz::make_task( []() -> int { throw std::runtime_error( "fail" ); } );
    auto fut = t->get_future();
    t->try_acquire();
    t->try_start();
    t->execute();
    bool caught = false;
    try {
        fut.get();
    } catch ( const std::runtime_error& e ) {
        caught = ( std::string( e.what() ) == "fail" );
    }
    TEST( "exception propagates through future", caught );
    TEST( "state is failed", t->state() == jz::task_state::failed );
}

void test_future_with_make_task()
{
    std::cout << "[test_future_with_make_task]" << std::endl;
    auto t   = jz::make_task( [] { return std::string( "hello" ); } );
    auto fut = t->get_future();
    TEST( "make_task creates pending task", t->state() == jz::task_state::pending );
    TEST( "future is valid", fut.valid() );

    t->try_acquire();
    t->try_start();
    t->execute();
    TEST( "task result via future", fut.get() == "hello" );
}

int main() {
    std::cout << "=== task 单元测试 ===" << std::endl;

    test_initial_state();
    test_state_transitions();
    test_cancel_before_acquire();
    test_cancel_after_acquire();
    test_cannot_cancel_running();
    test_execute_calls_function();
    test_execute_failure();
    test_timing();
    test_completion_callback();
    test_completion_callback_failure();
    test_make_task();
    test_execute_chain();
    test_skip_if_canceled();
    test_non_void_return();
    test_void_future();
    test_future_cancel();
    test_future_exception();
    test_future_with_make_task();

    std::cout << "\n=== 结果 ===" << std::endl;
    if ( failures == 0 ) {
        std::cout << "全部测试通过!" << std::endl;
        return 0;
    } else {
        std::cerr << failures << " 个测试失败!" << std::endl;
        return 1;
    }
}
