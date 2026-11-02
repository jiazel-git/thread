#include "work_stealing_queue.hpp"
#include <atomic>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

static constexpr size_t Q_SIZE = 256;
using queue_t                  = jz::work_stealing_queue< Q_SIZE >;

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

void test_empty_queue() {
    std::cout << "[test_empty_queue]" << std::endl;
    queue_t q;
    TEST( "empty returns true", q.empty() );
    TEST( "pop returns nullopt", !q.pop().has_value() );
    TEST( "steal returns nullopt", !q.steal().has_value() );
}

void test_push_pop_lifo() {
    std::cout << "[test_push_pop_lifo]" << std::endl;
    queue_t q;
    q.push( jz::make_task( [] {} ) );
    q.push( jz::make_task( [] {} ) );
    q.push( jz::make_task( [] {} ) );
    TEST( "not empty after 3 pushes", !q.empty() );
    TEST( "pop 1", q.pop().has_value() );
    TEST( "pop 2", q.pop().has_value() );
    TEST( "pop 3", q.pop().has_value() );
    TEST( "empty after all popped", q.empty() );
}

void test_push_steal_fifo() {
    std::cout << "[test_push_steal_fifo]" << std::endl;
    queue_t            q;
    std::vector< int > results;
    std::mutex         mtx;

    auto make_task_fn = [ & ]( int v ) {
        return jz::make_task( [ &results, &mtx, v ] {
            std::lock_guard lk( mtx );
            results.push_back( v );
        } );
    };

    q.push( make_task_fn( 10 ) );
    q.push( make_task_fn( 20 ) );
    q.push( make_task_fn( 30 ) );

    auto t1 = q.steal();
    assert( t1.has_value() );
    ( *t1 )->execute();
    auto t2 = q.steal();
    assert( t2.has_value() );
    ( *t2 )->execute();
    auto t3 = q.steal();
    assert( t3.has_value() );
    ( *t3 )->execute();

    TEST( "steal FIFO order", results.size() == 3 && results[ 0 ] == 10 && results[ 1 ] == 20 && results[ 2 ] == 30 );
}

void test_push_pop_lifo_order() {
    std::cout << "[test_push_pop_lifo_order]" << std::endl;
    queue_t            q;
    std::vector< int > results;
    std::mutex         mtx;

    auto make_task_fn = [ & ]( int v ) {
        return jz::make_task( [ &results, &mtx, v ] {
            std::lock_guard lk( mtx );
            results.push_back( v );
        } );
    };

    q.push( make_task_fn( 10 ) );
    q.push( make_task_fn( 20 ) );
    q.push( make_task_fn( 30 ) );

    auto t1 = q.pop();
    assert( t1.has_value() );
    ( *t1 )->execute();
    auto t2 = q.pop();
    assert( t2.has_value() );
    ( *t2 )->execute();
    auto t3 = q.pop();
    assert( t3.has_value() );
    ( *t3 )->execute();

    TEST( "pop LIFO order", results.size() == 3 && results[ 0 ] == 30 && results[ 1 ] == 20 && results[ 2 ] == 10 );
}

void test_overflow() {
    std::cout << "[test_overflow]" << std::endl;
    queue_t q;
    bool    all_pushed = true;
    for ( size_t i = 0; i < Q_SIZE; ++i ) {
        all_pushed &= q.push( jz::make_task( [] {} ) );
    }
    TEST( "all Q_SIZE pushes succeed", all_pushed );
    TEST( "overflow push fails", !q.push( jz::make_task( [] {} ) ) );
}

void test_interleaved_push_pop_steal() {
    std::cout << "[test_interleaved_push_pop_steal]" << std::endl;
    queue_t q;
    q.push( jz::make_task( [] {} ) );
    q.push( jz::make_task( [] {} ) );
    TEST( "pop after 2 pushes", q.pop().has_value() );
    TEST( "steal after pop", q.steal().has_value() );
    TEST( "empty after 2 ops", q.empty() );

    q.push( jz::make_task( [] {} ) );
    TEST( "pop single element", q.pop().has_value() );
    TEST( "empty after pop", q.empty() );
}

void test_single_element_race() {
    std::cout << "[test_single_element_race]" << std::endl;
    for ( int trial = 0; trial < 100; ++trial ) {
        queue_t            q;
        std::atomic< int > counter{ 0 };
        q.push( jz::make_task( [ &counter ] { counter.fetch_add( 1, std::memory_order_relaxed ); } ) );

        std::thread owner( [ & ] {
            auto t = q.pop();
            if ( t )
                ( *t )->execute();
        } );
        std::thread thief( [ & ] {
            auto t = q.steal();
            if ( t )
                ( *t )->execute();
        } );
        owner.join();
        thief.join();

        int c = counter.load();
        if ( c != 1 ) {
            std::cerr << "  FAIL: single element race, counter=" << c << " (expected 1)" << std::endl;
            ++failures;
            return;
        }
    }
    std::cout << "  PASS: single element race (100 trials)" << std::endl;
}

void stress_push_pop() {
    std::cout << "[stress_push_pop]" << std::endl;
    queue_t            q;
    constexpr int      N_TASKS = 100000;
    std::atomic< int > executed{ 0 };

    std::thread owner( [ & ] {
        int pushed = 0, popped = 0;
        while ( pushed < N_TASKS || popped < N_TASKS ) {
            if ( pushed < N_TASKS ) {
                if ( q.push( jz::make_task( [ &executed ] { executed.fetch_add( 1, std::memory_order_relaxed ); } ) ) ) {
                    ++pushed;
                }
            }
            if ( popped < pushed ) {
                auto t = q.pop();
                if ( t ) {
                    ( *t )->execute();
                    ++popped;
                }
            }
        }
    } );
    owner.join();
    TEST( "stress push/pop all executed", executed.load() == N_TASKS );
}

void stress_push_steal() {
    std::cout << "[stress_push_steal]" << std::endl;
    queue_t             q;
    constexpr int       N_TASKS = 100000;
    std::atomic< int >  executed{ 0 };
    std::atomic< int >  consumed{ 0 };
    std::atomic< bool > done_push{ false };

    auto producer = [ & ] {
        for ( int i = 0; i < N_TASKS; ++i ) {
            while ( !q.push( jz::make_task( [ &executed ] { executed.fetch_add( 1, std::memory_order_relaxed ); } ) ) ) {
                std::this_thread::yield();
            }
        }
        done_push.store( true, std::memory_order_release );
    };

    auto stealer = [ & ] {
        while ( !done_push.load( std::memory_order_acquire ) || consumed.load( std::memory_order_acquire ) < N_TASKS ) {
            auto t = q.steal();
            if ( t ) {
                ( *t )->execute();
                consumed.fetch_add( 1, std::memory_order_release );
            }
        }
    };

    std::thread p( producer );
    std::thread s( stealer );
    p.join();
    s.join();
    TEST( "stress push/steal all executed", executed.load() == N_TASKS );
}

void stress_multi_steal() {
    std::cout << "[stress_multi_steal]" << std::endl;
    queue_t             q;
    constexpr int       N_TASKS    = 100000;
    constexpr int       N_STEALERS = 4;
    std::atomic< int >  executed{ 0 };
    std::atomic< int >  consumed{ 0 };
    std::atomic< bool > done_push{ false };

    auto producer = [ & ] {
        for ( int i = 0; i < N_TASKS; ++i ) {
            while ( !q.push( jz::make_task( [ &executed ] { executed.fetch_add( 1, std::memory_order_relaxed ); } ) ) ) {
                std::this_thread::yield();
            }
        }
        done_push.store( true, std::memory_order_release );
    };

    auto stealer = [ & ] {
        while ( !done_push.load( std::memory_order_acquire ) || consumed.load( std::memory_order_acquire ) < N_TASKS ) {
            auto t = q.steal();
            if ( t ) {
                ( *t )->execute();
                consumed.fetch_add( 1, std::memory_order_release );
            }
        }
    };

    std::thread                p( producer );
    std::vector< std::thread > stealers;
    for ( int i = 0; i < N_STEALERS; ++i )
        stealers.emplace_back( stealer );
    p.join();
    for ( auto& s : stealers )
        s.join();
    TEST( "stress multi-steal all executed", executed.load() == N_TASKS );
}

void stress_mixed() {
    std::cout << "[stress_mixed]" << std::endl;
    queue_t             q;
    constexpr int       N_TASKS = 100000;
    std::atomic< int >  executed{ 0 };
    std::atomic< int >  consumed{ 0 };
    std::atomic< bool > done_push{ false };

    std::thread owner( [ & ] {
        for ( int i = 0; i < N_TASKS; ++i ) {
            while ( !q.push( jz::make_task( [ &executed ] { executed.fetch_add( 1, std::memory_order_relaxed ); } ) ) ) {
                auto t = q.pop();
                if ( t ) {
                    ( *t )->execute();
                    consumed.fetch_add( 1, std::memory_order_release );
                }
            }
        }
        done_push.store( true, std::memory_order_release );
        while ( consumed.load( std::memory_order_acquire ) < N_TASKS ) {
            auto t = q.pop();
            if ( t ) {
                ( *t )->execute();
                consumed.fetch_add( 1, std::memory_order_release );
            }
        }
    } );

    auto thief = [ & ] {
        while ( !done_push.load( std::memory_order_acquire ) || consumed.load( std::memory_order_acquire ) < N_TASKS ) {
            auto t = q.steal();
            if ( t ) {
                ( *t )->execute();
                consumed.fetch_add( 1, std::memory_order_release );
            }
        }
    };

    std::thread t1( thief );
    std::thread t2( thief );
    owner.join();
    t1.join();
    t2.join();
    TEST( "stress mixed all executed", executed.load() == N_TASKS );
}

int main() {
    std::cout << "=== 正确性测试 ===" << std::endl;
    test_empty_queue();
    test_push_pop_lifo();
    test_push_steal_fifo();
    test_push_pop_lifo_order();
    test_overflow();
    test_interleaved_push_pop_steal();
    test_single_element_race();

    std::cout << "\n=== 压力测试 ===" << std::endl;
    stress_push_pop();
    stress_push_steal();
    stress_multi_steal();
    stress_mixed();

    std::cout << "\n=== 结果 ===" << std::endl;
    if ( failures == 0 ) {
        std::cout << "全部测试通过!" << std::endl;
    } else {
        std::cerr << failures << " 个测试失败!" << std::endl;
        return 1;
    }
    return 0;
}
