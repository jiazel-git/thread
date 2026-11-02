#include "priority_queue.hpp"
#include <atomic>
#include <bits/chrono.h>
#include <cassert>
#include <chrono>
#include <iostream>
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

// ========== 正确性测试 ==========

void test_empty_queue() {
    std::cout << "[test_empty_queue]" << std::endl;
    jz::priority_queue q;
    TEST( "empty returns true", q.empty() );
    TEST( "size is 0", q.size() == 0 );
    TEST( "pop returns nullopt", !q.pop().has_value() );
}

void test_push_pop_single() {
    std::cout << "[test_push_pop_single]" << std::endl;
    jz::priority_queue q;
    int                val = 0;
    q.push( [ & ] { val = 1; }, jz::task_priority::normal );
    TEST( "not empty after push", !q.empty() );
    TEST( "size is 1", q.size() == 1 );

    auto t = q.pop();
    TEST( "pop returns task", t.has_value() );
    ( *t )->execute();
    TEST( "task executed correctly", val == 1 );
    TEST( "empty after pop", q.empty() );
}

void test_priority_order() {
    std::cout << "[test_priority_order]" << std::endl;
    jz::priority_queue q;
    std::vector< int > order;

    // 按 low → high → critical 顺序 push
    q.push( [ & ] { order.push_back( 1 ); }, jz::task_priority::low );
    q.push( [ & ] { order.push_back( 2 ); }, jz::task_priority::high );
    q.push( [ & ] { order.push_back( 3 ); }, jz::task_priority::critical );

    // pop 应该按优先级从高到低：critical → high → low
    auto t1 = q.pop();
    assert( t1.has_value() );
    ( *t1 )->execute();

    auto t2 = q.pop();
    assert( t2.has_value() );
    ( *t2 )->execute();

    auto t3 = q.pop();
    assert( t3.has_value() );
    ( *t3 )->execute();

    TEST( "priority order: critical first", order[ 0 ] == 3 );
    TEST( "priority order: high second", order[ 1 ] == 2 );
    TEST( "priority order: low third", order[ 2 ] == 1 );
}

void test_same_priority_fifo() {
    std::cout << "[test_same_priority_fifo]" << std::endl;
    jz::priority_queue q;
    std::vector< int > order;

    // 同优先级，按 1 2 3 顺序 push
    q.push( [ & ] { order.push_back( 1 ); }, jz::task_priority::normal );
    std::this_thread::sleep_for( 1ms );  // 确保时间戳差异
    q.push( [ & ] { order.push_back( 2 ); }, jz::task_priority::normal );
    std::this_thread::sleep_for( 1ms );
    q.push( [ & ] { order.push_back( 3 ); }, jz::task_priority::normal );

    auto t1 = q.pop();
    ( *t1 )->execute();
    auto t2 = q.pop();
    ( *t2 )->execute();
    auto t3 = q.pop();
    ( *t3 )->execute();

    TEST( "FIFO order preserved", order[ 0 ] == 1 && order[ 1 ] == 2 && order[ 2 ] == 3 );
}

void test_mixed_priorities() {
    std::cout << "[test_mixed_priorities]" << std::endl;
    jz::priority_queue q;
    std::vector< int > order;

    // 交叠 push 不同优先级
    q.push( [ & ] { order.push_back( 1 ); }, jz::task_priority::background );
    q.push( [ & ] { order.push_back( 2 ); }, jz::task_priority::critical );
    q.push( [ & ] { order.push_back( 3 ); }, jz::task_priority::low );
    q.push( [ & ] { order.push_back( 4 ); }, jz::task_priority::high );
    q.push( [ & ] { order.push_back( 5 ); }, jz::task_priority::normal );

    // 预期顺序：2(critical) → 4(high) → 5(normal) → 3(low) → 1(background)
    for ( int i = 0; i < 5; ++i ) {
        auto t = q.pop();
        assert( t.has_value() );
        ( *t )->execute();
    }

    bool ok = order[ 0 ] == 2 && order[ 1 ] == 4 && order[ 2 ] == 5 && order[ 3 ] == 3 && order[ 4 ] == 1;
    TEST( "mixed priorities correct order", ok );
}

void test_aging_promotion() {
    std::cout << "[test_aging_promotion]" << std::endl;
    jz::priority_queue q;
    std::vector< int > order;

    // 两个同优先级 background，老的被老化提升后先出
    q.push( [ & ] { order.push_back( 1 ); }, jz::task_priority::background );
    std::this_thread::sleep_for( 2ms );
    q.push( [ & ] { order.push_back( 2 ); }, jz::task_priority::background );

    // aging 阈值 1ms → 第一个 background 提升到 low
    q.aging( 1ms );

    // 现在: task1=low, task2=background → low 先出
    auto t1 = q.pop();
    assert( t1.has_value() );
    ( *t1 )->execute();
    auto t2 = q.pop();
    assert( t2.has_value() );
    ( *t2 )->execute();

    TEST( "aged task promoted from background to low and popped first", order[ 0 ] == 1 && order[ 1 ] == 2 );
}

void test_aging_multiple_steps() {
    std::cout << "[test_aging_multiple_steps]" << std::endl;
    jz::priority_queue q;
    std::vector< int > order;

    // background 任务，两次 aging 后升到 normal
    q.push( [ & ] { order.push_back( 1 ); }, jz::task_priority::background );
    q.push( [ & ] { order.push_back( 2 ); }, jz::task_priority::high );

    // 第一次 aging: background → low
    q.aging( 0ms );
    // 第二次 aging: low → normal
    q.aging( 0ms );

    // 现在: task1=normal, task2=high → high 仍然优先
    auto t1 = q.pop();
    ( *t1 )->execute();
    auto t2 = q.pop();
    ( *t2 )->execute();
    TEST( "multiple aging steps still respect remaining diff", order[ 0 ] == 2 && order[ 1 ] == 1 );
}

void test_critical_never_ages() {
    std::cout << "[test_critical_never_ages]" << std::endl;
    jz::priority_queue q;
    q.push( [] {}, jz::task_priority::critical );

    // critical 不应该被老化降低，实际上 aging 只升不降
    // 验证老化不会影响 critical
    q.aging( 0ms );
    TEST( "critical still present after aging", q.size() == 1 );
    auto t = q.pop();
    TEST( "critical can still be popped", t.has_value() );
}

void test_aging_only_raises() {
    std::cout << "[test_aging_only_raises]" << std::endl;
    jz::priority_queue q;
    std::vector< int > order;

    // push 一个 background，一个 critical
    q.push( [ & ] { order.push_back( 1 ); }, jz::task_priority::background );
    q.push( [ & ] { order.push_back( 2 ); }, jz::task_priority::critical );

    // aging 后 background 最多升到 low → critical 仍然优先
    q.aging( 0ms );

    auto t1 = q.pop();
    ( *t1 )->execute();
    auto t2 = q.pop();
    ( *t2 )->execute();
    TEST( "critical still before aged background", order[ 0 ] == 2 && order[ 1 ] == 1 );
}

// ========== 压力测试 ==========

void stress_concurrent() {
    std::cout << "[stress_concurrent]" << std::endl;
    jz::priority_queue q;
    constexpr int      N_TASKS   = 10000;
    constexpr int      N_THREADS = 4;
    std::atomic< int > executed{ 0 };

    std::vector< std::thread > producers;
    for ( int i = 0; i < N_THREADS; ++i ) {
        producers.emplace_back( [ &, i ] {
            for ( int j = 0; j < N_TASKS; ++j ) {
                auto p = static_cast< jz::task_priority >( std::rand() % static_cast< int >( jz::task_priority::priority_num ) );
                while ( !q.push( [ &executed ] { executed.fetch_add( 1, std::memory_order_relaxed ); }, p ) ) {
                }
            }
        } );
    }

    std::thread consumer( [ & ] {
        int popped = 0;
        int target = N_TASKS * N_THREADS;
        while ( popped < target ) {
            auto t = q.pop();
            if ( t ) {
                ( *t )->execute();
                ++popped;
            }
        }
    } );

    for ( auto& p : producers )
        p.join();
    consumer.join();

    TEST( "concurrent stress: all tasks executed", executed.load() == N_TASKS * N_THREADS );
}

void test_async_aging_lifecycle() {
    std::cout << "[test_async_aging_lifecycle]" << std::endl;
    jz::priority_queue q;

    std::atomic< int > counter{ 0 };
    q.push( [ & ] { counter++; }, jz::task_priority::background );
    q.push( [ & ] { counter++; }, jz::task_priority::background );

    // start_aging → stop_aging 不崩溃即可
    q.start_aging( 10ms, 10ms );
    std::this_thread::sleep_for( 30ms );
    q.stop_aging();

    // 任务仍在
    TEST( "async aging: tasks remain after lifecycle", q.size() == 2 );

    // 任务仍可执行
    auto t1 = q.pop();
    assert( t1.has_value() );
    ( *t1 )->execute();
    auto t2 = q.pop();
    assert( t2.has_value() );
    ( *t2 )->execute();
    TEST( "async aging: tasks still executable", counter == 2 );
}

void test_async_aging_twice() {
    std::cout << "[test_async_aging_twice]" << std::endl;
    jz::priority_queue q;

    q.start_aging( 10ms, 10ms );
    q.start_aging( 10ms, 10ms );  // 第二次调用不应创建新线程
    std::this_thread::sleep_for( 30ms );
    q.stop_aging();

    q.push( [] {}, jz::task_priority::normal );
    auto t = q.pop();
    TEST( "async aging: double start does not break", t.has_value() );
}

void stress_aging() {
    std::cout << "[stress_aging]" << std::endl;
    jz::priority_queue q;
    constexpr int      N_TASKS = 5000;
    std::atomic< int > executed{ 0 };

    // push 一批低优先级任务
    for ( int i = 0; i < N_TASKS; ++i ) {
        while ( !q.push( [ &executed ] { executed.fetch_add( 1, std::memory_order_relaxed ); }, jz::task_priority::background ) ) {
        }
    }

    // push 少量高优先级任务
    for ( int i = 0; i < 100; ++i ) {
        while ( !q.push( [ &executed ] { executed.fetch_add( 1, std::memory_order_relaxed ); }, jz::task_priority::critical ) ) {
        }
    }

    // 老化提升 background
    q.aging( 0ms );

    // pop 所有任务
    int popped = 0;
    while ( q.size() > 0 ) {
        auto t = q.pop();
        if ( t ) {
            ( *t )->execute();
            ++popped;
        }
    }

    TEST( "stress aging: all tasks popped", popped == N_TASKS + 100 );
    TEST( "stress aging: all tasks executed", executed.load() == N_TASKS + 100 );
}

int main() {
    std::cout << "=== jz::priority_queue 正确性测试 ===" << std::endl;
    test_empty_queue();
    test_push_pop_single();
    test_priority_order();
    test_same_priority_fifo();
    test_mixed_priorities();
    test_aging_promotion();
    test_critical_never_ages();
    test_aging_only_raises();
    test_aging_multiple_steps();

    std::cout << "\n=== 异步老化测试 ===" << std::endl;
    test_async_aging_lifecycle();
    test_async_aging_twice();

    std::cout << "\n=== 压力测试 ===" << std::endl;
    stress_concurrent();
    stress_aging();

    std::cout << "\n=== 结果 ===" << std::endl;
    if ( failures == 0 ) {
        std::cout << "全部测试通过!" << std::endl;
        return 0;
    } else {
        std::cerr << failures << " 个测试失败!" << std::endl;
        return 1;
    }
}
