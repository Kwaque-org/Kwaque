#pragma once

#include "src/storage/local_control.h"

namespace kwaque::storage {
namespace detail {
template<typename High>
struct local_id_range final {
    High current{};
    std::uint32_t remaining{0};
};
template<typename High>
auto local_id_value(High high) noexcept {
    if constexpr (std::same_as<High, local_wal_high>)
        return high.incarnation();
    else
        return high.sequence();
}
template<typename High>
using local_id_type = typename decltype(local_id_value(High{}))::value_type;
} // namespace detail

// The control owner outlives this allocator. Reopening starts with no cached
// range: every unused ID in an earlier durable reservation is burned. Only
// one allocator may issue IDs for a control, preserving issuance order as well
// as uniqueness across durable blocks.
// Join close before closing control; destroy both before stopping the workload.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
class local_id_allocator final : public runtime::shard_affine {
public:
    using control_type = local_control_owner<Backend, Owner>;
    [[nodiscard]] static runtime::result<std::unique_ptr<local_id_allocator>>
    make(
      control_type& control,
      workload_budget& budget,
      std::uint32_t block_size = 256) {
        if (block_size == 0 || block_size > 65536)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        if (auto state = control.snapshot(); !state)
            return runtime::failure(state.error());
        if (control.id_allocator_active_)
            return runtime::failure(detail::path_error(errc::already_exists));
        auto instance = budget.allocation_charge(
          byte_count{sizeof(local_id_allocator)});
        if (!instance) return runtime::failure(instance.error());
        auto held = budget.try_reserve(byte_count{instance->value() + 4096});
        if (!held) return runtime::failure(held.error());
        auto allocator = std::unique_ptr<local_id_allocator>{
          new local_id_allocator(control, block_size, std::move(*held))};
        control.id_allocator_active_ = true;
        return allocator;
    }
    local_id_allocator(const local_id_allocator&) = delete;
    local_id_allocator& operator=(const local_id_allocator&) = delete;
    ~local_id_allocator() {
        assert_current();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-ID-ALLOCATOR-DRAINED"},
          closed_,
          "ID allocator destroyed before joined close");
    }
    [[nodiscard]] auto allocate_wal(codec::cooperative_work& work) {
        return allocate(wal_, &local_shard_control::wal_high, work);
    }
    [[nodiscard]] auto allocate_object(codec::cooperative_work& work) {
        return allocate(object_, &local_shard_control::object_high, work);
    }
    [[nodiscard]] auto allocate_decision(codec::cooperative_work& work) {
        return allocate(decision_, &local_shard_control::decision_high, work);
    }
    [[nodiscard]] auto allocate_deletion(codec::cooperative_work& work) {
        return allocate(deletion_, &local_shard_control::deletion_high, work);
    }
    [[nodiscard]] seastar::future<runtime::result<void>> close() {
        assert_current();
        if (closed_) co_return runtime::result<void>{};
        if (closing_)
            co_return runtime::failure(detail::path_error(errc::queue_full));
        closing_ = true;
        co_await operations_.close();
        closed_ = true;
        control_.id_allocator_active_ = false;
        co_return runtime::result<void>{};
    }

private:
    local_id_allocator(
      control_type& control,
      std::uint32_t block_size,
      workload_reservation held)
      : held_(std::move(held))
      , control_(control)
      , block_size_(block_size) {}
    template<typename High>
    static runtime::result<detail::local_id_type<High>>
    serve(detail::local_id_range<High>& range) {
        auto next = range.current.checked_advance(1);
        if (!next)
            return runtime::failure(detail::path_error(errc::out_of_range));
        auto id = detail::local_id_value(*next);
        if (!id)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        range.current = *next;
        --range.remaining;
        return *id;
    }
    template<typename High>
    seastar::future<runtime::result<detail::local_id_type<High>>> allocate(
      detail::local_id_range<High>& range,
      High local_shard_control::* member,
      codec::cooperative_work& work) {
        assert_current();
        using result_type = runtime::result<detail::local_id_type<High>>;
        auto reject = [](runtime::operation_error error) {
            return seastar::make_ready_future<result_type>(
              runtime::failure(error));
        };
        if (closing_ || closed_)
            return reject(detail::path_error(errc::closed));
        if (busy_) return reject(detail::path_error(errc::queue_full));
        // Cached IDs remain reserved, but a fenced/closed control cannot admit
        // further work, including the allocation fast path.
        if (auto state = control_.snapshot(); !state)
            return reject(state.error());
        if (auto ready = work.poll(); !ready)
            return reject(detail::path_error(ready.error().code()));
        if (range.remaining != 0)
            return seastar::make_ready_future<result_type>(serve(range));
        return refill(range, member, work, operations_.hold());
    }
    template<typename High>
    seastar::future<runtime::result<detail::local_id_type<High>>> refill(
      detail::local_id_range<High>& range,
      High local_shard_control::* member,
      codec::cooperative_work& work,
      seastar::gate::holder holder) {
        busy_ = true;
        auto idle = seastar::defer([this] noexcept { busy_ = false; });
        static_cast<void>(holder);
        High start{};
        std::uint32_t count = block_size_;
        auto result = co_await control_.update(
          [member, &start, &count](
            local_shard_control& candidate) -> runtime::result<void> {
              start = candidate.*member;
              auto high = start.checked_advance(count);
              // Do not strand the terminal IDs when fewer than a full block
              // remain. The rare terminal tail uses single-ID reservations.
              if (!high) {
                  count = 1;
                  high = start.checked_advance(count);
              }
              if (!high)
                  return runtime::failure(
                    detail::path_error(errc::out_of_range));
              candidate.*member = *high;
              return {};
          },
          work);
        if (auto done = result.failure.outcome(); !done)
            co_return runtime::failure(done.error());
        if (result.disposition != local_publication_disposition::durable)
            co_return runtime::failure(detail::path_error(errc::io_failure));
        // A failed/ambiguous publication never installs a serveable range.
        range = {start, count};
        co_return serve(range);
    }
    workload_reservation held_;
    control_type& control_;
    std::uint32_t block_size_;
    detail::local_id_range<local_wal_high> wal_;
    detail::local_id_range<local_object_high> object_;
    detail::local_id_range<local_decision_high> decision_;
    detail::local_id_range<local_deletion_high> deletion_;
    seastar::gate operations_;
    bool busy_{false}, closing_{false}, closed_{false};
};
} // namespace kwaque::storage
