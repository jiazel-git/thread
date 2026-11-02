#include "thread_manager.hpp"
#include <atomic>
#include <chrono>
#include <cassert>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

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

void test_lifecycle()
{
    std::cout << "[test_lifecycle]" << std::endl;
    jz::thread_manager mgr;
    jz::thread_manager_config cfg;
    cfg.core_threads = 1;
    cfg.idle_timeout = 50ms;

    mgr.start( cfg );
    std::this_thread::sleep_for( 50ms );
    mgr.shutdown();

    TEST( "lifecycle: shutdown without crash", true );
}

void test_core_threads_process_tasks()
{
    std::cout << "[test_core_threads_process_tasks]" << std::endl;
    jz::thread_manager mgr;

    jz::thread_manager_config cfg;
    cfg.core_threads = 2;
    cfg.idle_timeout = 50ms;
    cfg.min_tasks_per_thread = 5;

    mgr.start( cfg );

    std::atomic< int > counter{ 0 };
    for ( int i = 0; i < 10; ++i ) {
        mgr.push( [ &counter ] { counter.fetch_add( 1 ); } );
    }
    mgr.notify();

    std::this_thread::sleep_for( 200ms );
    mgr.shutdown();

    TEST( "core threads executed all pushed tasks", counter.load() == 10 );
}

void test_push_with_priority()
{
    std::cout << "[test_push_with_priority]" << std::endl;
    jz::thread_manager mgr;

    jz::thread_manager_config cfg;
    cfg.core_threads = 1;
    cfg.idle_timeout = 50ms;

    mgr.start( cfg );

    std::vector< int > order;
    std::mutex         mtx;

    // high 先 push, critical 后 push, 但 critical 应先执行
    mgr.push( [ & ] {
        std::lock_guard lk( mtx );
        order.push_back( 1 );
    }, jz::task_priority::high );

    mgr.push( [ & ] {
        std::lock_guard lk( mtx );
        order.push_back( 2 );
    }, jz::task_priority::critical );

    mgr.notify();
    std::this_thread::sleep_for( 100ms );
    mgr.shutdown();

    TEST( "priority: critical before high",
          order.size() == 2 && order[ 0 ] == 2 && order[ 1 ] == 1 );
}

void test_double_shutdown()
{
    std::cout << "[test_double_shutdown]" << std::endl;
    jz::thread_manager mgr;

    jz::thread_manager_config cfg;
    cfg.core_threads = 1;
    cfg.idle_timeout = 50ms;

    mgr.start( cfg );
    std::this_thread::sleep_for( 50ms );
    mgr.shutdown();
    mgr.shutdown();

    TEST( "double shutdown does not crash", true );
}

int main()
{
    std::cout << "=== thread_manager 单元测试 ===" << std::endl;

    test_lifecycle();
    test_core_threads_process_tasks();
    test_push_with_priority();
    test_double_shutdown();

    std::cout << "\n=== 结果 ===" << std::endl;
    if ( failures == 0 ) {
        std::cout << "全部测试通过!" << std::endl;
        return 0;
    } else {
        std::cerr << failures << " 个测试失败!" << std::endl;
        return 1;
    }
}
