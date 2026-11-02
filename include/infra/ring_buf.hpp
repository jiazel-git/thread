#pragma once

#include <atomic>
#include <cstddef>
#include <cstdlib>

namespace jz
{

inline constexpr size_t cache_line_size = 64;

template < typename T, size_t N >
class ring_buf
{
    static_assert(N >= 2 && (N & (N - 1)) == 0, "N must be power of 2, >= 2");

    static constexpr size_t MASK = N - 1;

    static T* do_alloc()
    {
        auto raw = std::aligned_alloc(alignof(T), N * sizeof(T));
        auto buf = static_cast< T* >(raw);
        for (size_t i = 0; i < N; ++i) {
            new (&buf[ i ]) T();
        }
        return buf;
    }

public:
    ring_buf()
        : buf_(do_alloc())
    {
    }

    ~ring_buf()
    {
        if (!buf_)
            return;
        for (size_t i = 0; i < N; ++i) {
            buf_[ i ].~T();
        }
        std::free(buf_);
    }

    ring_buf(const ring_buf&)            = delete;
    ring_buf& operator=(const ring_buf&) = delete;
    ring_buf(ring_buf&&)                 = delete;
    ring_buf& operator=(ring_buf&&)      = delete;

    T& operator[](size_t i) noexcept { return buf_[ i & static_cast< size_t >(MASK) ]; }

    T& slot_at(size_t i) noexcept { return (*this)[ i ]; }

    size_t head(std::memory_order order = std::memory_order_seq_cst) const noexcept
    {
        return head_.load(order);
    }
    void head_store(size_t val, std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        head_.store(val, order);
    }
    bool head_cas(size_t& expected, size_t desired,
                  std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        return head_.compare_exchange_weak(expected, desired, order);
    }
    std::atomic< size_t >& head_atom() noexcept { return head_; }

    size_t tail(std::memory_order order = std::memory_order_seq_cst) const noexcept
    {
        return tail_.load(order);
    }
    void tail_store(size_t val, std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        tail_.store(val, order);
    }
    bool tail_cas(size_t& expected, size_t desired,
                  std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        return tail_.compare_exchange_weak(expected, desired, order);
    }
    std::atomic< size_t >& tail_atom() noexcept { return tail_; }

    static constexpr size_t mask() noexcept { return MASK; }

    static constexpr size_t capacity() noexcept { return N; }

private:
    T* const buf_;
    alignas(cache_line_size) std::atomic< size_t > head_ {0};
    alignas(cache_line_size) std::atomic< size_t > tail_ {0};
};

}  // namespace jz
