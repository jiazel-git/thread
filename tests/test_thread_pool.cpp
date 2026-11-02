#include "priority_queue.hpp"
#include "thread_manager.hpp"
#include "thread_pool.hpp"
#include <atomic>
#include <bits/chrono.h>
#include <cassert>
#include <chrono>
#include <future>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

static int failures = 0;

#define TEST(name, expr)                                                                           \
    do {                                                                                           \
        if (!(expr)) {                                                                             \
            std::cerr << "  FAIL: " << name << " (" << #expr << ")" << std::endl;                  \
            ++failures;                                                                            \
        } else {                                                                                   \
            std::cout << "  PASS: " << name << std::endl;                                          \
        }                                                                                          \
    } while (0)

using namespace std::chrono_literals;

void test_submit()
{
    std::cout << "[test_submit]" << std::endl;
    jz::thread_pool           pool;
    jz::thread_manager_config cfg;
    cfg.core_threads = 2;
    cfg.idle_timeout = 100ms;
    pool.start(cfg);

    auto fut = pool.submit([] {
        return 42;
    });
    TEST("submit returns correct value", fut.get() == 42);

    pool.shutdown();
}

void test_submit_void()
{
    std::cout << "[test_submit_void]" << std::endl;
    jz::thread_pool           pool;
    jz::thread_manager_config cfg;
    cfg.core_threads = 1;
    cfg.idle_timeout = 100ms;
    pool.start(cfg);

    int val = 0;
    pool.submit([ & ] {
            val = 7;
        })
        .get();

    pool.shutdown();
    TEST("void submit executes", val == 7);
}

void test_submit_priority()
{
    std::cout << "[test_submit_priority]" << std::endl;
    jz::thread_pool           pool;
    jz::thread_manager_config cfg;
    cfg.core_threads = 1;
    cfg.idle_timeout = 100ms;
    pool.start(cfg);

    std::vector< int > order;
    std::mutex         mtx;

    auto fut1 = pool.submit_priority(jz::task_priority::high, [ & ] {
        std::lock_guard lk(mtx);
        order.push_back(1);
    });
    auto fut2 = pool.submit_priority(jz::task_priority::critical, [ & ] {
        std::lock_guard lk(mtx);
        order.push_back(2);
    });

    fut1.get();
    fut2.get();
    pool.shutdown();

    TEST("priority: critical before high", order.size() == 2 && order[ 0 ] == 2 && order[ 1 ] == 1);
}

void test_detach()
{
    std::cout << "[test_detach]" << std::endl;
    jz::thread_pool           pool;
    jz::thread_manager_config cfg;
    cfg.core_threads = 1;
    cfg.idle_timeout = 100ms;
    pool.start(cfg);

    std::atomic< int > val {0};
    pool.detach([ & ] {
        val = 42;
    });

    std::this_thread::sleep_for(100ms);
    pool.shutdown();

    TEST("detached task executed", val.load() == 42);
}

void test_pause_resume()
{
    std::cout << "[test_pause_resume]" << std::endl;
    jz::thread_pool           pool;
    jz::thread_manager_config cfg;
    cfg.core_threads = 1;
    cfg.idle_timeout = 100ms;
    pool.start(cfg);

    std::atomic< int > counter {0};
    pool.pause();

    pool.submit([ & ] {
        counter++;
    });
    std::this_thread::sleep_for(50ms);
    // paused, task should not be executed
    int before = counter.load();

    pool.resume();
    std::this_thread::sleep_for(100ms);
    int after = counter.load();

    pool.shutdown();

    TEST("paused: task not executed", before == 0);
    TEST("resumed: task executed", after == 1);
}

void test_submit_delayed()
{
    std::cout << "[test_submit_delayed]" << std::endl;
    jz::thread_pool           pool;
    jz::thread_manager_config cfg;
    cfg.core_threads = 1;
    cfg.idle_timeout = 100ms;
    pool.start(cfg);

    auto start   = std::chrono::steady_clock::now();
    auto fut     = pool.submit_delayed(50ms, [] {
        return 99;
    });
    auto result  = fut.get();
    auto elapsed = std::chrono::steady_clock::now() - start;

    pool.shutdown();

    TEST("delayed task returns correct value", result == 99);
    TEST("delayed task waited at least 50ms", elapsed >= 50ms);
}

void test_batch()
{
    std::cout << "[test_batch]" << std::endl;
    jz::thread_pool           pool;
    jz::thread_manager_config cfg;
    cfg.core_threads = 4;
    cfg.idle_timeout = 100ms;
    pool.start(cfg);

    auto futures = pool.submit_batch(
        [](int i) {
            return i * i;
        },
        10);

    int sum = 0;
    for (auto& f : futures)
        sum += f.get();

    pool.shutdown();

    int expected = 0;
    for (int i = 0; i < 10; ++i)
        expected += i * i;

    TEST("batch sum correct", sum == expected);
}

void test_multiple_tasks()
{
    std::cout << "[test_multiple_tasks]" << std::endl;
    jz::thread_pool           pool;
    jz::thread_manager_config cfg;
    cfg.core_threads = 4;
    cfg.idle_timeout = 100ms;
    pool.start(cfg);

    constexpr int                     N = 100;
    std::vector< std::future< int > > futures;
    for (int i = 0; i < N; ++i) {
        futures.push_back(pool.submit([ i ] {
            return i * i;
        }));
    }

    int sum = 0;
    for (auto& f : futures)
        sum += f.get();

    pool.shutdown();

    int expected = 0;
    for (int i = 0; i < N; ++i)
        expected += i * i;

    TEST("multiple tasks all complete correctly", sum == expected);
}

void test_statistics()
{
    std::cout << "[test_statistics]" << std::endl;
    jz::thread_pool           pool;
    jz::thread_manager_config cfg;
    cfg.core_threads = 2;
    cfg.idle_timeout = 100ms;
    pool.start(cfg);

    pool.submit([] {
            return 1;
        })
        .get();
    pool.submit([] {
            return 2;
        })
        .get();

    auto s = pool.stats();

    pool.shutdown();

    TEST("stats: tasks_submitted >= 2", s.tasks_submitted >= 2);
    TEST("stats: tasks_completed >= 2", s.tasks_completed >= 2);
    TEST("stats: tasks_pending == 0", s.tasks_pending == 0);
    TEST("stats: thread_count == 2", s.thread_count == 2);
}

int main()
{
    std::cout << "=== thread_pool 单元测试 ===" << std::endl;

    test_submit();
    test_submit_void();
    test_submit_priority();
    test_detach();
    test_pause_resume();
    test_submit_delayed();
    test_batch();
    test_multiple_tasks();
    test_statistics();

    std::cout << "\n=== 结果 ===" << std::endl;
    if (failures == 0) {
        std::cout << "全部测试通过!" << std::endl;
        return 0;
    }
    std::cerr << failures << " 个测试失败!" << std::endl;
    return 1;
}
