#include "src/codec/cooperative.h"

#include "src/base/invariant.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/preempt.hh>
#include <seastar/coroutine/maybe_yield.hh>

namespace kwaque::codec {

cooperative_work::cooperative_work(
  limits policy, seastar::abort_source& abort) noexcept
  : policy_(policy)
  , abort_(abort)
  , byte_quantum_(policy.config().max_work_bytes)
  , item_quantum_(policy.config().max_work_items)
  , bytes_remaining_(byte_quantum_)
  , items_remaining_(item_quantum_) {}

result<void> cooperative_work::poll(error anchor) const noexcept {
    if (abort_.abort_requested()) {
        return codec::failure(
          error{
            errc::aborted,
            anchor.family(),
            anchor.field(),
            anchor.byte_offset()});
    }
    return {};
}

bool cooperative_work::fits(byte_count bytes, item_count items) const noexcept {
    return bytes <= bytes_remaining_ && items <= items_remaining_;
}

void cooperative_work::debit(byte_count bytes, item_count items) noexcept {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CODEC-WORK-DEBIT"},
      fits(bytes, items),
      "admitted work exceeds its remaining quantum");
    bytes_remaining_ = *bytes_remaining_.checked_sub(bytes);
    items_remaining_ = *items_remaining_.checked_sub(items);
}

void cooperative_work::reset() noexcept {
    bytes_remaining_ = byte_quantum_;
    items_remaining_ = item_quantum_;
}

seastar::future<result<void>>
cooperative_work::admit(byte_count bytes, item_count items, error anchor) {
    if (auto valid = poll(anchor); !valid) {
        return seastar::make_ready_future<result<void>>(
          codec::failure(valid.error()));
    }
    if (bytes.value() == 0 && items.value() == 0) {
        items = item_count{1};
    }
    if (bytes > byte_quantum_ || items > item_quantum_) {
        return seastar::make_ready_future<result<void>>(codec::failure(
          error{
            errc::resource_exhausted,
            anchor.family(),
            anchor.field(),
            anchor.byte_offset()}));
    }
    if (fits(bytes, items)) {
        debit(bytes, items);
        return seastar::make_ready_future<result<void>>();
    }
    return admit_slow(bytes, items, anchor);
}

seastar::future<result<void>>
cooperative_work::admit_slow(byte_count bytes, item_count items, error anchor) {
    if (auto checked = co_await checkpoint(anchor); !checked) {
        co_return codec::failure(checked.error());
    }
    if (auto valid = poll(anchor); !valid) {
        co_return codec::failure(valid.error());
    }
    debit(bytes, items);
    co_return result<void>{};
}

seastar::future<result<void>> cooperative_work::checkpoint(error anchor) {
    if (auto valid = poll(anchor); !valid) {
        return seastar::make_ready_future<result<void>>(
          codec::failure(valid.error()));
    }
    if (!seastar::need_preempt()) {
        reset();
        return seastar::make_ready_future<result<void>>();
    }
    return checkpoint_slow(anchor);
}

seastar::future<result<void>> cooperative_work::checkpoint_slow(error anchor) {
    co_await seastar::coroutine::maybe_yield();
    reset();
    co_return poll(anchor);
}

bool cooperative_work::try_drain(byte_count bytes, item_count items) noexcept {
    if (bytes.value() == 0 && items.value() == 0) {
        items = item_count{1};
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CODEC-WORK-DRAIN"},
      bytes <= byte_quantum_ && items <= item_quantum_,
      "cleanup must be split to fit the complete work quantum");
    if (fits(bytes, items)) {
        debit(bytes, items);
        return true;
    }
    reset();
    if (seastar::need_preempt()) {
        return false;
    }
    debit(bytes, items);
    return true;
}

seastar::future<> cooperative_work::drain(byte_count bytes, item_count items) {
    if (try_drain(bytes, items)) {
        return seastar::make_ready_future<>();
    }
    return drain_slow(bytes, items);
}

seastar::future<>
cooperative_work::drain_slow(byte_count bytes, item_count items) {
    co_await seastar::coroutine::maybe_yield();
    if (bytes.value() == 0 && items.value() == 0) {
        items = item_count{1};
    }
    debit(bytes, items);
}

seastar::future<> cooperative_work::drain_checkpoint() {
    if (!seastar::need_preempt()) {
        reset();
        return seastar::make_ready_future<>();
    }
    return drain_checkpoint_slow();
}

seastar::future<> cooperative_work::drain_checkpoint_slow() {
    co_await seastar::coroutine::maybe_yield();
    reset();
}

} // namespace kwaque::codec
