#include "thread_manager.hpp"
#include "thread_pool.hpp"
#include <atomic>
#include <bits/chrono.h>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <future>
#include <mutex>
#include <numeric>
#include <vector>

using namespace std::chrono_literals;

struct bench_result {
    const char* label;
    size_t      threads;
    size_t      tasks;
    double      elapsed_ms;
    double      throughput;  // tasks/sec
    double      avg_latency_us;
};

static double now_ms()
{
    return std::chrono::duration_cast< std::chrono::milliseconds >(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

static double now_us()
{
    return std::chrono::duration_cast< std::chrono::microseconds >(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// 直接提交不等待
void bench_yield_latency(bench_result& r, size_t threads, size_t tasks)
{
    std::vector< size_t > latencies;
    latencies.reserve(tasks);

    jz::thread_pool           pool;
    jz::thread_manager_config cfg;
    cfg.core_threads = threads;
    cfg.idle_timeout = 500ms;
    pool.start(cfg);

    std::atomic< size_t >   done {0};
    std::mutex              mtx;
    std::condition_variable cv;

    auto t0 = now_ms();

    for (size_t i = 0; i < tasks; ++i) {
        auto t_start = now_us();
        pool.detach([ &done, &cv, t_start, &latencies, &mtx ] {
            auto elapsed = static_cast< size_t >(now_us() - t_start);
            {
                std::lock_guard lk(mtx);
                latencies.push_back(elapsed);
            }
            done.fetch_add(1, std::memory_order_release);
            cv.notify_one();
        });
    }

    {
        std::unique_lock lk(mtx);
        cv.wait(lk, [ & ] {
            return done.load() >= tasks;
        });
    }

    auto elapsed_ms = now_ms() - t0;
    pool.shutdown();

    auto sum = std::accumulate(latencies.begin(), latencies.end(), size_t {0});
    r        = {
        .label          = "yield",
        .threads        = threads,
        .tasks          = tasks,
        .elapsed_ms     = elapsed_ms,
        .throughput     = tasks / (elapsed_ms / 1000.0),
        .avg_latency_us = static_cast< double >(sum) / latencies.size(),
    };
}

// 提交并等待
void bench_wait_latency(bench_result& r, size_t threads, size_t tasks)
{
    jz::thread_pool           pool;
    jz::thread_manager_config cfg;
    cfg.core_threads = threads;
    cfg.idle_timeout = 500ms;
    pool.start(cfg);

    auto t0 = now_ms();

    std::vector< std::future< void > > futs;
    futs.reserve(tasks);

    for (size_t i = 0; i < tasks; ++i) {
        futs.push_back(pool.submit([] {}));
    }

    for (auto& f : futs)
        f.get();

    auto elapsed_ms = now_ms() - t0;
    pool.shutdown();

    r = {
        .label      = "wait",
        .threads    = threads,
        .tasks      = tasks,
        .elapsed_ms = elapsed_ms,
        .throughput = tasks / (elapsed_ms / 1000.0),
    };
}

// 计算密集型任务
void bench_compute(bench_result& r, size_t threads, size_t tasks)
{
    jz::thread_pool           pool;
    jz::thread_manager_config cfg;
    cfg.core_threads = threads;
    cfg.idle_timeout = 500ms;
    pool.start(cfg);

    auto t0 = now_ms();

    std::vector< std::future< uint64_t > > futs;
    futs.reserve(tasks);

    for (size_t i = 0; i < tasks; ++i) {
        futs.push_back(pool.submit(
            [](uint64_t n) {
                uint64_t s = 0;
                for (uint64_t j = 0; j < n; ++j)
                    s += j;
                return s;
            },
            10000));
    }

    for (auto& f : futs)
        f.get();

    auto elapsed_ms = now_ms() - t0;
    pool.shutdown();

    r = {
        .label      = "compute",
        .threads    = threads,
        .tasks      = tasks,
        .elapsed_ms = elapsed_ms,
        .throughput = tasks / (elapsed_ms / 1000.0),
    };
}

void print_header()
{
    std::printf("%-12s | %7s | %8s | %12s | %12s | %14s\n", "benchmark", "threads", "tasks",
                "elapsed(ms)", "thrpt(t/s)", "avg_lat(us)");
    std::printf("%-12s-+-%7s-+-%8s-+-%12s-+-%12s-+-%14s\n", "------------", "-------", "--------",
                "------------", "------------", "--------------");
}

void print_row(const bench_result& r)
{
    std::printf("%-12s | %7zu | %8zu | %12.0f | %12.0f | %14.1f\n", r.label, r.threads, r.tasks,
                r.elapsed_ms, r.throughput, r.avg_latency_us);
}

int main()
{
    std::printf("=== Thread Pool Benchmark ===\n\n");

    std::vector< size_t > thread_counts = {1, 2, 4, 8};
    size_t                tasks         = 100000;

    print_header();

    // 轻量任务吞吐
    for (auto t : thread_counts) {
        bench_result r;
        bench_wait_latency(r, t, tasks);
        print_row(r);
    }

    std::printf("\n");
    print_header();

    // 计算型任务吞吐
    tasks = 50000;
    for (auto t : thread_counts) {
        bench_result r;
        bench_compute(r, t, 50000);
        print_row(r);
    }

    std::printf("\n--- 延迟测试 (detach, no future) ---\n");
    tasks = 50000;
    print_header();

    for (auto t : {1, 2, 4}) {
        bench_result r;
        bench_yield_latency(r, t, tasks);
        print_row(r);
    }

    std::printf("\nDone.\n");
    return 0;
}
