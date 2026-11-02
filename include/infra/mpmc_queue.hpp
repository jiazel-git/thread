#pragma once

#include "infra/ring_buf.hpp"
#include <atomic>
#include <cstddef>
#include <utility>

namespace jz
{

inline constexpr size_t default_queue_capacity = 1024;

template < typename T, size_t N = default_queue_capacity >
class mpmc_queue
{
    static_assert((N & (N - 1)) == 0, "N must be power of 2");

    struct mpmc_cell {
        std::atomic< bool > filled {false};
        T                   data;
    };

    using cell = mpmc_cell;

public:
    mpmc_queue()                             = default;

    mpmc_queue(const mpmc_queue&)            = delete;
    mpmc_queue& operator=(const mpmc_queue&) = delete;
    mpmc_queue(mpmc_queue&&)                 = delete;
    mpmc_queue& operator=(mpmc_queue&&)      = delete;

    template < typename F >
    bool push(F&& data)
    {
        return push_impl(std::forward< F >(data));
    }

private:
    bool push_copy(const T& data)
    {
        for (;;) {
            auto pos = buf_.tail(std::memory_order_relaxed);
            auto h   = buf_.head(std::memory_order_acquire);
            if (pos - h >= N)
                return false;

            if (buf_.tail_cas(pos, pos + 1, std::memory_order_relaxed)) {
                while (buf_[ pos ].filled.load(std::memory_order_acquire)) {}
                buf_[ pos ].data = data;
                buf_[ pos ].filled.store(true, std::memory_order_release);
                return true;
            }
        }
    }

    bool push_move(T&& data)
    {
        for (;;) {
            auto pos = buf_.tail(std::memory_order_relaxed);
            auto h   = buf_.head(std::memory_order_acquire);
            if (pos - h >= N)
                return false;

            if (buf_.tail_cas(pos, pos + 1, std::memory_order_relaxed)) {
                while (buf_[ pos ].filled.load(std::memory_order_acquire)) {}
                buf_[ pos ].data = std::move(data);
                buf_[ pos ].filled.store(true, std::memory_order_release);
                return true;
            }
        }
    }

    template < typename F >
    bool push_impl(F&& data)
    {
        if constexpr (std::is_lvalue_reference_v< F >) {
            return push_copy(data);
        } else {
            return push_move(std::forward< F >(data));
        }
    }

public:
    bool pop(T& data)
    {
        for (;;) {
            auto pos = buf_.head(std::memory_order_relaxed);
            auto t   = buf_.tail(std::memory_order_acquire);
            if (pos >= t)
                return false;

            if (buf_.head_cas(pos, pos + 1, std::memory_order_relaxed)) {
                while (!buf_[ pos ].filled.load(std::memory_order_acquire)) {}
                data = std::move(buf_[ pos ].data);
                buf_[ pos ].filled.store(false, std::memory_order_release);
                return true;
            }
        }
    }

    size_t size_hint() const noexcept
    {
        auto h = buf_.head(std::memory_order_relaxed);
        auto t = buf_.tail(std::memory_order_relaxed);
        return t - h;
    }

private:
    ring_buf< cell, N > buf_;
};

}  // namespace jz
