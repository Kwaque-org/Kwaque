#include "src/base/units.h"
#include "src/resource/bounded_work_queue.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/testing/perf_tests.hh>

#include <cstddef>
#include <cstdint>
#include <exception>
#include <expected>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace kwaque::resource {

namespace {

constexpr std::size_t admission_iterations{64};
constexpr byte_count admitted_cost{4'096};

enum class admission_case { round_trip, item_limit, byte_limit };

bounded_work_queue_config admission_config(admission_case selected) {
    return bounded_work_queue_config{
      .maximum_items
      = item_count{selected == admission_case::item_limit ? 1U : 2U},
      .maximum_bytes
      = byte_count{selected == admission_case::byte_limit ? 4'096U : 8'192U},
      .maximum_producer_waiters = 0,
    };
}

// This baseline is deliberately limited to serial admission with no waiters.
// Both paths carry a scalar item charged for 4 KiB, retain native byte units
// until pop, and return the same checked outcomes. It does not compare worker
// execution or concurrent producer/consumer fairness.
class native_admission_queue final : public runtime::shard_affine {
public:
    explicit native_admission_queue(bounded_work_queue_config config)
      : config_(config)
      , items_(config.maximum_items.value())
      , memory_(config.maximum_bytes.value()) {
        if (!config_.validate() || config_.maximum_producer_waiters != 0) {
            throw std::invalid_argument(
              "invalid native admission benchmark bounds");
        }
    }

    seastar::future<queue_result<void>>
    push(std::uint64_t item, byte_count cost, seastar::abort_source& abort) {
        assert_current();
        if (cost.value() == 0) {
            return rejected(queue_failure_kind::invalid_cost, cost);
        }
        if (cost > config_.maximum_bytes) {
            return rejected(queue_failure_kind::oversized, cost);
        }
        if (closed_) {
            return rejected(queue_failure_kind::closed, cost);
        }
        if (abort.abort_requested()) {
            return rejected(queue_failure_kind::aborted, cost);
        }
        if (
          items_.full()
          || cost.value() > config_.maximum_bytes.value() - held_bytes_) {
            return rejected(
              queue_failure_kind::producer_waiters_exhausted, cost);
        }
        auto units = seastar::try_get_units(memory_, cost.value());
        if (!units) {
            return rejected(
              queue_failure_kind::producer_waiters_exhausted, cost);
        }
        if (!items_.push(charged_item{std::move(*units), item})) {
            throw std::logic_error(
              "native admission lost its reserved item slot");
        }
        held_bytes_ += cost.value();
        return seastar::make_ready_future<queue_result<void>>(
          queue_result<void>{});
    }

    seastar::future<queue_result<std::uint64_t>>
    pop(seastar::abort_source& abort) {
        assert_current();
        if (items_.empty() || closed_ || abort.abort_requested()) {
            throw std::logic_error(
              "native admission benchmark has no admitted item");
        }
        auto admitted = items_.pop();
        held_bytes_ -= admitted.units.count();
        admitted.units.return_all();
        return seastar::make_ready_future<queue_result<std::uint64_t>>(
          queue_result<std::uint64_t>{admitted.item});
    }

    seastar::future<> close(queue_close_mode) {
        assert_current();
        while (!items_.empty()) {
            auto admitted = items_.pop();
            admitted.units.return_all();
        }
        held_bytes_ = 0;
        closed_ = true;
        return seastar::make_ready_future<>();
    }

    std::size_t size() const {
        assert_current();
        return items_.size();
    }
    byte_count bytes() const {
        assert_current();
        return byte_count{held_bytes_};
    }
    std::size_t waiting_producers() const {
        assert_current();
        return memory_.waiters();
    }

private:
    struct charged_item final {
        seastar::semaphore_units<> units;
        std::uint64_t item;
    };

    seastar::future<queue_result<void>>
    rejected(queue_failure_kind kind, byte_count cost) const {
        return seastar::make_ready_future<queue_result<void>>(std::unexpected(
          queue_failure{
            .kind = kind,
            .requested_bytes = cost,
            .queued_items = item_count{items_.size()},
          }));
    }

    const bounded_work_queue_config config_;
    seastar::queue<charged_item> items_;
    seastar::semaphore memory_;
    std::uint64_t held_bytes_{0};
    bool closed_{false};
};

template<bool Native, admission_case Selected>
class admission_fixture {
public:
    admission_fixture()
      : queue_(admission_config(Selected)) {
        try {
            if (!queue_.push(0, admitted_cost, abort_).get()) {
                throw std::runtime_error(
                  "queue benchmark warmup admission failed");
            }
            if constexpr (Selected == admission_case::round_trip) {
                const auto popped = queue_.pop(abort_).get();
                if (!popped || *popped != 0) {
                    throw std::runtime_error(
                      "queue benchmark warmup pop failed");
                }
            }
        } catch (...) {
            const auto failure = std::current_exception();
            queue_.close(queue_close_mode::abort).get();
            std::rethrow_exception(failure);
        }
    }

    ~admission_fixture() { queue_.close(queue_close_mode::abort).get(); }

    seastar::future<std::size_t> execute() {
        std::exception_ptr failure;
        perf_tests::start_measuring_time();
        try {
            // These serial cases cannot wait. Keep the coroutine chain intact
            // while excluding caller task-quota yields from measured work.
            for (std::size_t index = 0; index < admission_iterations; ++index) {
                if constexpr (Selected == admission_case::round_trip) {
                    const auto admitted
                      = co_await seastar::coroutine::without_preemption_check(
                        queue_.push(index, admitted_cost, abort_));
                    if (
                      !admitted || queue_.size() != 1
                      || queue_.bytes() != admitted_cost) {
                        throw std::runtime_error(
                          "queue benchmark admission mismatch");
                    }
                    const auto popped
                      = co_await seastar::coroutine::without_preemption_check(
                        queue_.pop(abort_));
                    if (
                      !popped || *popped != index || queue_.size() != 0
                      || queue_.bytes().value() != 0) {
                        throw std::runtime_error(
                          "queue benchmark pop mismatch");
                    }
                    perf_tests::do_not_optimize(*popped);
                } else {
                    const auto rejected
                      = co_await seastar::coroutine::without_preemption_check(
                        queue_.push(index, byte_count{1}, abort_));
                    if (
                      rejected
                      || rejected.error().kind
                           != queue_failure_kind::producer_waiters_exhausted
                      || rejected.error().requested_bytes != byte_count{1}
                      || rejected.error().queued_items != item_count{1}
                      || queue_.size() != 1
                      || queue_.bytes() != admitted_cost) {
                        throw std::runtime_error(
                          "queue benchmark saturation mismatch");
                    }
                    perf_tests::do_not_optimize(rejected.error());
                }
                if (queue_.waiting_producers() != 0) {
                    throw std::runtime_error(
                      "queue benchmark retained a producer waiter");
                }
            }
        } catch (...) {
            failure = std::current_exception();
        }
        perf_tests::stop_measuring_time();
        if (failure) {
            std::rethrow_exception(failure);
        }
        co_return admission_iterations;
    }

private:
    using queue_type = std::conditional_t<
      Native,
      native_admission_queue,
      bounded_work_queue<std::uint64_t>>;
    queue_type queue_;
    seastar::abort_source abort_;
};

using native_queue_admission
  = admission_fixture<true, admission_case::round_trip>;
using kwaque_queue_admission
  = admission_fixture<false, admission_case::round_trip>;
using native_queue_item_limit
  = admission_fixture<true, admission_case::item_limit>;
using kwaque_queue_item_limit
  = admission_fixture<false, admission_case::item_limit>;
using native_queue_byte_limit
  = admission_fixture<true, admission_case::byte_limit>;
using kwaque_queue_byte_limit
  = admission_fixture<false, admission_case::byte_limit>;

} // namespace

PERF_TEST_F(native_queue_admission, admit_pop_charge4096) { return execute(); }
PERF_TEST_F(kwaque_queue_admission, admit_pop_charge4096) { return execute(); }
PERF_TEST_F(native_queue_item_limit, reject_without_waiter) {
    return execute();
}
PERF_TEST_F(kwaque_queue_item_limit, reject_without_waiter) {
    return execute();
}
PERF_TEST_F(native_queue_byte_limit, reject_without_waiter) {
    return execute();
}
PERF_TEST_F(kwaque_queue_byte_limit, reject_without_waiter) {
    return execute();
}

} // namespace kwaque::resource
