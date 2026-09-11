#pragma once

#include "src/base/units.h"
#include "src/codec/error.h"
#include "src/codec/limits.h"

#include <seastar/core/future.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <coroutine>
#include <source_location>

namespace seastar {
class abort_source;
}

namespace kwaque::codec {

// One operation and its sequential children share this work account. It and its
// abort source must remain alive and unmoved until all using futures complete;
// concurrent use is not supported. Memory admission remains with the owner.
class cooperative_work final {
public:
    cooperative_work(limits policy, seastar::abort_source& abort) noexcept;
    cooperative_work(const cooperative_work&) = delete;
    cooperative_work& operator=(const cooperative_work&) = delete;
    cooperative_work(cooperative_work&&) = delete;
    cooperative_work& operator=(cooperative_work&&) = delete;

    [[nodiscard]] limits policy() const noexcept { return policy_; }
    [[nodiscard]] byte_count byte_quantum() const noexcept {
        return byte_quantum_;
    }
    [[nodiscard]] item_count item_quantum() const noexcept {
        return item_quantum_;
    }
    [[nodiscard]] byte_count bytes_remaining() const noexcept {
        return bytes_remaining_;
    }
    [[nodiscard]] item_count items_remaining() const noexcept {
        return items_remaining_;
    }

    // Only the anchor's diagnostic coordinates are used. Poll synchronously
    // after EVERY awaited admission/checkpoint and before mutation: awaiting an
    // already-ready future can itself preempt after the helper's last poll.
    // Final poll, commit and publication must form one non-suspending block.
    [[nodiscard]] result<void>
    poll(error anchor = error{errc::success}) const noexcept;

    // Admit a conservative whole-leaf cost before performing it. Oversized
    // leaves reject without charging; split them unless explicitly indivisible.
    // An all-zero request costs one item so empty children cannot evade checks.
    [[nodiscard]] seastar::future<result<void>> admit(
      byte_count bytes, item_count items, error anchor = error{errc::success});

    // Check around an explicitly bounded indivisible leaf. This attempts native
    // preemption and resets the residual quantum; it need not suspend.
    [[nodiscard]] seastar::future<result<void>>
    checkpoint(error anchor = error{errc::success});

    // Embedded in the calling coroutine: no helper future/frame is allocated.
    // The native awaiter schedules that same coroutine when a checkpoint is
    // required; admission resumes before the caller can perform cleanup.
    class [[nodiscard("co_await the cleanup admission")]]
    cleanup_awaiter final {
    public:
        cleanup_awaiter(const cleanup_awaiter&) = delete;
        cleanup_awaiter& operator=(const cleanup_awaiter&) = delete;
        cleanup_awaiter(cleanup_awaiter&&) = delete;
        cleanup_awaiter& operator=(cleanup_awaiter&&) = delete;

        [[nodiscard]] bool await_ready() noexcept {
            admitted_ = work_.try_drain(bytes_, items_);
            return admitted_;
        }

        template<typename Promise>
        bool await_suspend(
          std::coroutine_handle<Promise> handle,
          std::source_location location
          = std::source_location::current()) noexcept {
            auto native = seastar::coroutine::maybe_yield{}.operator co_await();
            if (native.await_ready()) {
                return false;
            }
            if constexpr (requires {
                              native.await_suspend(handle, location);
                          }) {
                native.await_suspend(handle, location);
            } else {
                native.await_suspend(handle);
            }
            return true;
        }

        void await_resume() noexcept {
            if (!admitted_) {
                work_.debit(bytes_, items_);
            }
        }

    private:
        friend class cooperative_work;
        cleanup_awaiter(
          cooperative_work& work, byte_count bytes, item_count items) noexcept
          : work_(work)
          , bytes_(bytes)
          , items_(
              bytes.value() == 0 && items.value() == 0 ? item_count{1}
                                                       : items) {}

        cooperative_work& work_;
        byte_count bytes_;
        item_count items_;
        bool admitted_{false};
    };

    // Cleanup ignores abort, but still admits bounded work and attempts
    // preemption. The owner preserves its original error and joins teardown.
    // The cost must fit the complete quantum; oversized cleanup must be split.
    // Use drain_inline from a coroutine for allocation-free admission. drain
    // retains the future interface for synchronous Seastar-thread callers.
    [[nodiscard]] cleanup_awaiter
    drain_inline(byte_count bytes, item_count items) noexcept {
        return cleanup_awaiter{*this, bytes, items};
    }
    [[nodiscard]] seastar::future<> drain(byte_count bytes, item_count items);
    [[nodiscard]] seastar::future<> drain_checkpoint();

private:
    // A false result has already reset the account; only our awaiter/future
    // adapter may complete that protocol, with no intervening account use.
    [[nodiscard]] bool try_drain(byte_count bytes, item_count items) noexcept;
    [[nodiscard]] bool fits(byte_count bytes, item_count items) const noexcept;
    void debit(byte_count bytes, item_count items) noexcept;
    void reset() noexcept;
    [[nodiscard]] seastar::future<result<void>>
    admit_slow(byte_count bytes, item_count items, error anchor);
    [[nodiscard]] seastar::future<result<void>> checkpoint_slow(error anchor);
    [[nodiscard]] seastar::future<>
    drain_slow(byte_count bytes, item_count items);
    [[nodiscard]] seastar::future<> drain_checkpoint_slow();

    const limits policy_;
    seastar::abort_source& abort_;
    const byte_count byte_quantum_;
    const item_count item_quantum_;
    byte_count bytes_remaining_;
    item_count items_remaining_;
};

} // namespace kwaque::codec
