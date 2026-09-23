#ifndef KWAQUE_SRC_STORAGE_STATISTICS_H_
#define KWAQUE_SRC_STORAGE_STATISTICS_H_

#include "src/base/invariant.h"
#include "src/runtime/operation_statistics.h"

#include <cstdint>
#include <utility>

namespace kwaque::storage {
enum class storage_outcome : std::uint8_t { success, error, uncertain };
struct storage_statistics_snapshot final {
    runtime::operation_statistics_snapshot operations;
    std::uint64_t succeeded{0}, failed{0}, uncertain{0}, durable{0};
};

class storage_statistics final : public runtime::shard_affine {
public:
    class reservation final {
    public:
        reservation(reservation&& other) noexcept
          : owner_(std::exchange(other.owner_, nullptr))
          , terminal_(std::move(other.terminal_))
          , outcome_(other.outcome_)
          , finished_(other.finished_) {}
        reservation(const reservation&) = delete;
        reservation& operator=(const reservation&) = delete;
        ~reservation() {
            if (!owner_) return;
            owner_->assert_current();
            switch (outcome_) {
            case storage_outcome::success:
                ++owner_->succeeded_;
                break;
            case storage_outcome::error:
                ++owner_->failed_;
                break;
            case storage_outcome::uncertain:
                ++owner_->uncertain_;
                break;
            }
        }
        void finish(storage_outcome outcome) noexcept {
            KWAQUE_INVARIANT(
              invariant_id{"KQ-STORAGE-OUTCOME-ONCE"},
              owner_ && !finished_,
              "storage outcome assigned more than once");
            owner_->assert_current();
            outcome_ = outcome;
            finished_ = true;
        }

    private:
        friend class storage_statistics;
        explicit reservation(storage_statistics& owner) noexcept
          : owner_(&owner)
          , terminal_(owner.operations_.accept()) {}
        storage_statistics* owner_;
        runtime::operation_statistics::reservation terminal_;
        storage_outcome outcome_{storage_outcome::uncertain};
        bool finished_{false};
    };
    [[nodiscard]] reservation accept() noexcept {
        assert_current();
        return reservation{*this};
    }
    void reject() noexcept {
        assert_current();
        operations_.reject();
    }
    void observe_durable() noexcept {
        assert_current();
        ++durable_;
    }
    void observe_uncertain() noexcept {
        assert_current();
        ++uncertain_;
    }
    [[nodiscard]] storage_statistics_snapshot snapshot() const noexcept {
        assert_current();
        return {
          operations_.snapshot(), succeeded_, failed_, uncertain_, durable_};
    }

private:
    runtime::operation_statistics operations_;
    std::uint64_t succeeded_{0}, failed_{0}, uncertain_{0}, durable_{0};
};
} // namespace kwaque::storage
#endif // KWAQUE_SRC_STORAGE_STATISTICS_H_
