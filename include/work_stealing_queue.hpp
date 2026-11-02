#pragma once

#include "task.hpp"
#include "infra/spsc_queue.hpp"
#include <atomic>
#include <cstddef>
#include <memory>
#include <optional>
#include <utility>

namespace jz
{

template < size_t N >
class work_stealing_queue
{
    static_assert( N >= 2 );

public:
    using task = std::shared_ptr< task_base >;

public:
    work_stealing_queue() noexcept = default;

    work_stealing_queue( const work_stealing_queue& )            = delete;
    work_stealing_queue& operator=( const work_stealing_queue& ) = delete;
    work_stealing_queue( work_stealing_queue&& )                 = delete;
    work_stealing_queue& operator=( work_stealing_queue&& )      = delete;

public:
    bool push( task ts ) noexcept {
        return queue_.push( std::move( ts ) );
    }

    std::optional< task > pop() noexcept {
        auto& head = queue_.head_ref();
        auto& tail = queue_.tail_ref();

        auto t = tail.load( std::memory_order_relaxed );
        auto h = head.load( std::memory_order_acquire );
        if ( t == h ) {
            return std::nullopt;
        }

        if ( t - 1 == h ) {
            // 单元素: 与 steal 竞争，需要 CAS
            if ( head.compare_exchange_strong( h, h + 1, std::memory_order_acq_rel ) ) {
                return std::move( queue_[ t - 1 ] );
            }
            return std::nullopt;
        }

        // 多元素: owner LIFO pop，无竞争
        auto ts = queue_[ t - 1 ];
        tail.store( t - 1, std::memory_order_relaxed );
        return std::move( ts );
    }

    std::optional< task > steal() noexcept {
        auto& head = queue_.head_ref();
        auto& tail = queue_.tail_ref();

        auto h = head.load( std::memory_order_acquire );
        auto t = tail.load( std::memory_order_acquire );
        if ( t == h ) {
            return std::nullopt;
        }

        auto ts = queue_[ h ];
        if ( head.compare_exchange_strong( h, h + 1, std::memory_order_acq_rel ) ) {
            return std::move( ts );
        }
        return std::nullopt;
    }

    bool empty() noexcept {
        return queue_.empty();
    }

private:
    spsc_queue< task, N > queue_;
};

}  // namespace jz
