#include "src/storage/page_cache.h"

#include <algorithm>
#include <limits>

namespace kwaque::storage {
namespace detail {
runtime::operation_error cache_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::resource};
}

cache_account::cache_account(
  workload_reservation held, std::uint64_t capacity) noexcept
  : held(std::move(held))
  , capacity(capacity)
  , queues(
      {.cache_size = capacity,
       .small_size = std::clamp<std::uint64_t>(
         capacity / 10, 1, capacity - 1)}) {}
cache_account::~cache_account() {
    assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CACHE-ACCOUNT-EMPTY"},
      used == 0,
      "cache bytes still charged when their last owner let go");
}
void cache_account::make_room(std::uint64_t need) noexcept {
    assert_current();
    cache_queues::effort spent;
    queues.forget_expired(spent);
    while (need > capacity - used) {
        const auto before = used;
        const bool evicted = queues.evict(spent);
        // An eviction frees a value's bytes, and of a value that holds none
        // it frees nothing. Then, and when nothing could be evicted, what
        // is only remembered gives way: the oldest first, a bounded number
        // a request.
        if ((!evicted || used == before) && !queues.forget_oldest(spent))
            return;
    }
}
seastar::future<> cache_account::release() {
    assert_current();
    // It restarts from the queues at each scheduling point, so that no
    // position in them lives across one.
    while (queues.any() != nullptr) {
        do {
            queues.any()->retire();
        } while (queues.any() != nullptr && !seastar::need_preempt());
        co_await seastar::coroutine::maybe_yield();
    }
}

cache_entry::cache_entry(
  cache_admission&& admitted, const cache_entry_kind& kind) noexcept
  : hook(admitted.remembered)
  , kind_(&kind)
  , account_(std::move(admitted.account))
  , funding_(std::move(admitted.funding))
  , cost_(admitted.cost)
  , shell_(admitted.shell) {
    // Before anyone can use it: those who join its load are among them.
    hook.expect(admitted.expected);
    ++account_->loads;
    if (funding_) ++account_->uncached;
}
cache_entry::~cache_entry() {
    assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CACHE-ENTRY-RELEASED"},
      holders_ == 0 && !resident_ && !loading_ && waiters_.empty(),
      "cache entry destroyed while held, indexed or loading");
    account_->used -= cost_;
}
void cache_entry::destroy() noexcept { kind_->destroy(*this); }
void cache_entry::retired() noexcept {
    assert_current();
    account_->queues.remove(*this);
    resident_ = false;
    if (holders_ == 0) destroy();
}
void cache_entry::retire() noexcept {
    if (!resident_) return;
    kind_->unlink(*this);
    retired();
}
runtime::result<void> cache_entry::resize(std::uint64_t backing) noexcept {
    assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CACHE-LOAD-ONCE"},
      loading_,
      "a cache entry was resized after its load ended");
    auto& account = *account_;
    const auto refuse = [&account](errc code) {
        ++account.refused;
        return runtime::failure(cache_error(code));
    };
    if (backing > std::numeric_limits<std::uint64_t>::max() - shell_)
        return refuse(errc::out_of_range);
    const auto cost = backing + shell_;
    if (funding_) {
        if (funding_->retained_bytes().value() < cost)
            return refuse(errc::out_of_range);
        return {};
    }
    if (cost <= cost_) {
        account.used -= cost_ - cost;
        cost_ = cost;
        return {};
    }
    if (account.stopped) return runtime::failure(cache_error(errc::closed));
    if (cost >= account.queues.small_capacity())
        return refuse(errc::out_of_range);
    // It is in no queue while it loads, so making room cannot reach it.
    const auto more = cost - cost_;
    if (more > account.capacity - account.used) account.make_room(more);
    if (more > account.capacity - account.used) return refuse(errc::queue_full);
    account.used += more;
    cost_ = cost;
    return {};
}
void cache_entry::enlist(cache_waiter& waiting) noexcept {
    assert_current();
    ++account_->waits;
    waiters_.push_back(waiting);
}
void cache_entry::settle(
  std::optional<runtime::operation_error> failed,
  std::exception_ptr thrown) noexcept {
    assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CACHE-LOAD-ONCE"},
      loading_ && holders_ != 0,
      "a cache load ended that was not in flight");
    loading_ = false;
    if (failed || thrown || funding_ || account_->stopped)
        retire();
    else if (resident_)
        account_->queues.insert(*this);
    // Each waiter resumes later, on its own task: none runs here.
    while (!waiters_.empty()) {
        auto& waiting = waiters_.front();
        waiters_.pop_front();
        if (thrown)
            waiting.told.set_exception(thrown);
        else
            waiting.told.set_value(failed);
    }
}
bool cache_entry::evicted_from(cache_queue from) noexcept {
    assert_current();
    // A value in use is not evicted: that would free nothing, and lose it
    // for the next reader.
    if (holders_ != 0) return false;
    if (from == cache_queue::small) {
        // The value goes and the entry stays where its object finds it, so
        // that the value's return is recognized.
        kind_->discard(*this);
        account_->used -= cost_ - shell_;
        cost_ = shell_;
        ++account_->evicted;
        return true;
    }
    if (from == cache_queue::main) ++account_->evicted;
    retire();
    return true;
}
} // namespace detail

runtime::result<void> page_cache_config::validate() const noexcept {
    // The cache has two queues, and neither is the whole of it.
    if (
      capacity.value() < 2 || load_slots == 0
      || load_slots > maximum_cache_load_slots)
        return runtime::failure(detail::cache_error(errc::invalid_argument));
    return {};
}

runtime::result<std::unique_ptr<page_cache>>
page_cache::make(workload_budget& budget, page_cache_config config) {
    if (auto valid = config.validate(); !valid)
        return runtime::failure(valid.error());
    const auto instance = budget.allocation_charge(
      byte_count{sizeof(page_cache)});
    const auto account = budget.allocation_charge(
      byte_count{sizeof(detail::cache_account) + 64});
    if (!instance) return runtime::failure(instance.error());
    if (!account) return runtime::failure(account.error());
    auto total = config.capacity.checked_add(*instance);
    if (total) total = total->checked_add(*account);
    if (!total)
        return runtime::failure(detail::cache_error(errc::out_of_range));
    auto held = budget.try_reserve(*total);
    if (!held) return runtime::failure(held.error());
    auto shared = seastar::make_lw_shared<detail::cache_account>(
      std::move(*held), config.capacity.value());
    return std::unique_ptr<page_cache>{
      new page_cache(budget, std::move(shared), config.load_slots)};
}
page_cache::page_cache(
  workload_budget& budget,
  seastar::lw_shared_ptr<detail::cache_account> account,
  std::uint32_t slots)
  : budget_(budget)
  , account_(std::move(account))
  , group_(budget.scheduling_group())
  , slots_(slots) {}
page_cache::~page_cache() {
    assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CACHE-STOPPED"},
      stopped_ && loads_.is_closed() && loads_.get_count() == 0,
      "page cache destroyed before joined stop");
}

page_cache_snapshot page_cache::snapshot() const noexcept {
    assert_current();
    const auto& account = *account_;
    const auto queued = account.queues.size();
    return {
      .capacity = account.capacity,
      .bytes = account.used,
      .loads = account.loads,
      .waits = account.waits,
      .uncached = account.uncached,
      .refused = account.refused,
      .small_bytes = queued.small_queue_size,
      .main_bytes = queued.main_queue_size,
      .remembered = queued.ghost_queue_entries,
      .evicted = account.evicted};
}

seastar::future<> page_cache::stop() {
    assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CACHE-STOP-ONCE"},
      !stopped_,
      "page cache stopped twice");
    stopped_ = true;
    account_->stopped = true;
    stop_.request_abort();
    slots_.broken();
    co_await loads_.close();
    co_await account_->release();
}

seastar::future<runtime::result<page_cache::slot>>
page_cache::wait_slot(seastar::abort_source& caller) {
    using output = runtime::result<slot>;
    assert_current();
    if (stopped_)
        co_return output{runtime::failure(detail::cache_error(errc::closed))};
    auto waited = co_await seastar::coroutine::as_future(
      seastar::get_units(slots_, 1, caller));
    if (waited.failed()) {
        // The wait ends three ways without a slot: its caller left, the
        // cache stopped, or the wait itself could not be made.
        waited.ignore_ready_future();
        co_return output{runtime::failure(
          detail::cache_error(
            caller.abort_requested() ? errc::aborted
            : stopped_               ? errc::closed
                                     : errc::resource_exhausted))};
    }
    // A caller released as the slot came gives it back.
    if (caller.abort_requested())
        co_return output{runtime::failure(detail::cache_error(errc::aborted))};
    co_return output{waited.get()};
}

runtime::result<detail::cache_admission> page_cache::admit(
  byte_count backing,
  std::size_t entry_bytes,
  std::optional<workload_reservation> funding,
  std::uint32_t expected,
  const cache_hook* remembered) {
    assert_current();
    auto& account = *account_;
    if (stopped_) return runtime::failure(detail::cache_error(errc::closed));
    const auto refuse = [&account](errc code) {
        ++account.refused;
        return runtime::failure(detail::cache_error(code));
    };
    const auto object = budget_.allocation_charge(byte_count{entry_bytes});
    if (!object) return runtime::failure(object.error());
    const auto cost = backing.checked_add(*object);
    if (!cost) return refuse(errc::out_of_range);
    detail::cache_admission admitted{remembered};
    if (funding) {
        if (funding->retained_bytes() < *cost)
            return refuse(errc::out_of_range);
        admitted.funding = std::move(funding);
    } else {
        // A value as large as the small queue's share would push every
        // other value out of it on its way through.
        if (cost->value() >= account.queues.small_capacity())
            return refuse(errc::out_of_range);
        if (cost->value() > account.capacity - account.used)
            account.make_room(cost->value());
        if (cost->value() > account.capacity - account.used)
            return refuse(errc::queue_full);
        account.used += cost->value();
        admitted.cost = cost->value();
    }
    admitted.account = account_;
    admitted.shell = static_cast<std::uint32_t>(object->value());
    admitted.expected = expected;
    return admitted;
}

} // namespace kwaque::storage
