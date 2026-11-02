#pragma once

#include "ring_buf.hpp"
#include <atomic>
#include <cstddef>
#include <optional>
#include <utility>
#include <vector>

namespace jz
{

template < typename T, size_t N >
class spsc_queue
{
public:
    template < typename F >
    bool push(F&& data) noexcept
    {
        return push_impl(std::forward< F >(data));
    }

    bool pop(T& data) noexcept
    {
        auto head = buf_.head(std::memory_order_relaxed);
        if (head >= cached_tail_) [[unlikely]] {
            cached_tail_ = buf_.tail(std::memory_order_acquire);
            if (head >= cached_tail_)
                return false;
        }
        data = std::move(buf_[ head ]);
        buf_.head_store(head + 1, std::memory_order_release);
        return true;
    }

    std::optional< T > try_pop() noexcept
    {
        auto head = buf_.head(std::memory_order_relaxed);
        if (head >= cached_tail_) [[unlikely]] {
            cached_tail_ = buf_.tail(std::memory_order_acquire);
            if (head >= cached_tail_)
                return std::nullopt;
        }
        auto data = std::move(buf_[ head ]);
        buf_.head_store(head + 1, std::memory_order_release);
        return data;
    }

    size_t pop_bulk(T* out, size_t count) noexcept
    {
        auto head = buf_.head(std::memory_order_relaxed);
        if (head >= cached_tail_) [[unlikely]] {
            cached_tail_ = buf_.tail(std::memory_order_acquire);
            if (head >= cached_tail_)
                return 0;
        }
        auto avail = cached_tail_ - head;
        auto n     = avail < count ? avail : count;
        for (size_t i = 0; i < n; ++i)
            out[ i ] = std::move(buf_[ head + i ]);
        buf_.head_store(head + n, std::memory_order_release);
        return n;
    }

    size_t pop_all(std::vector< T >& out) noexcept
    {
        auto head = buf_.head(std::memory_order_relaxed);
        if (head >= cached_tail_) [[unlikely]] {
            cached_tail_ = buf_.tail(std::memory_order_acquire);
            if (head >= cached_tail_)
                return 0;
        }
        auto avail = cached_tail_ - head;
        out.reserve(out.size() + avail);
        for (size_t i = 0; i < avail; ++i)
            out.push_back(std::move(buf_[ head + i ]));
        buf_.head_store(head + avail, std::memory_order_release);
        return avail;
    }

    bool empty() const noexcept
    {
        auto head = buf_.head(std::memory_order_acquire);
        auto tail = buf_.tail(std::memory_order_relaxed);
        return head == tail;
    }

    // for work_stealing_queue: steal / LIFO pop
    std::atomic< size_t >& head_ref() noexcept { return buf_.head_atom(); }
    std::atomic< size_t >& tail_ref() noexcept { return buf_.tail_atom(); }

    T&     operator[](size_t i) noexcept { return buf_[ i ]; }
    size_t mask_val() const noexcept { return buf_.mask(); }

private:
    bool push_copy(const T& data) noexcept
    {
        auto tail = buf_.tail(std::memory_order_relaxed);
        if (tail - cached_head_ >= N) [[unlikely]] {
            cached_head_ = buf_.head(std::memory_order_acquire);
            if (tail - cached_head_ >= N)
                return false;
        }
        buf_[ tail ] = data;
        buf_.tail_store(tail + 1, std::memory_order_release);
        return true;
    }

    bool push_move(T&& data) noexcept
    {
        auto tail = buf_.tail(std::memory_order_relaxed);
        if (tail - cached_head_ >= N) [[unlikely]] {
            cached_head_ = buf_.head(std::memory_order_acquire);
            if (tail - cached_head_ >= N)
                return false;
        }
        buf_[ tail ] = std::move(data);
        buf_.tail_store(tail + 1, std::memory_order_release);
        return true;
    }

    template < typename F >
    bool push_impl(F&& data) noexcept
    {
        if constexpr (std::is_lvalue_reference_v< F >) {
            return push_copy(data);
        } else {
            return push_move(std::forward< F >(data));
        }
    }

private:
    ring_buf< T, N > buf_;
    alignas(cache_line_size) size_t cached_head_ {0};
    alignas(cache_line_size) size_t cached_tail_ {0};
};

}  // namespace jz
