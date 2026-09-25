#pragma once

#include "src/storage/tests/wal_append_contract.h"

#include <algorithm>
#include <array>
#include <string_view>

namespace kwaque::storage::testing {
// Bounded external observations. No engine cursor or replay decoder supplies
// these expected bytes. A failed/unfinished whole gather can leave complete
// members, partial bytes or zero-filled native extension after the protected
// prefix. None of those candidates is an acknowledgement.
class wal_survival_oracle final {
public:
    explicit wal_survival_oracle(std::string header)
      : bytes_(std::move(header))
      , minimum_(bytes_.size())
      , written_(minimum_) {
        ends_[0] = minimum_;
    }
    void attempted(std::string bytes) {
        store_contract::require(
          count_ < ends_.size() - 1 && bytes_.size() + bytes.size() <= 65536,
          "survival history exceeded its bound");
        bytes_ += bytes;
        ends_[++count_] = bytes_.size();
    }
    void written(std::uint64_t end) {
        store_contract::require(boundary(end), "unknown written boundary");
        written_ = std::max(written_, end);
    }
    void synced(std::uint64_t capture) {
        store_contract::require(
          boundary(capture) && capture <= written_,
          "barrier preceded its observed complete write");
        minimum_ = std::max(minimum_, capture);
        // A captured earlier flush does not discard later attempted bytes.
    }
    [[nodiscard]] bool allows(std::string_view actual) const noexcept {
        if (actual.size() < minimum_ || actual.size() > bytes_.size())
            return false;
        if (
          actual.substr(0, minimum_)
          != std::string_view{bytes_}.substr(0, minimum_))
            return false;
        for (auto i = minimum_; i < actual.size(); ++i)
            if (actual[i] != bytes_[i] && actual[i] != '\0') return false;
        return true;
    }
    [[nodiscard]] std::uint64_t minimum() const noexcept { return minimum_; }
    [[nodiscard]] std::uint64_t possible() const noexcept {
        return bytes_.size();
    }
    [[nodiscard]] std::uint64_t written() const noexcept { return written_; }
    [[nodiscard]] const std::string& bytes() const noexcept { return bytes_; }

    // Group ordinals come from the independent attempted-byte history. A
    // frozen cohort never includes groups attempted after this call.
    [[nodiscard]] std::size_t freeze(std::size_t first, std::size_t last) {
        store_contract::require(
          first <= last && last < count_ && cohorts_used_ < cohorts_.size(),
          "invalid oracle cohort membership");
        const auto index = cohorts_used_++;
        cohorts_[index] = {first, last};
        return index;
    }
    [[nodiscard]] std::size_t interest(std::size_t group) {
        store_contract::require(
          group < count_ && interests_used_ < interests_.size(),
          "invalid oracle observer registration");
        const auto index = interests_used_++;
        interests_[index].group = group;
        return index;
    }
    [[nodiscard]] std::uint64_t cut(std::size_t cohort) const {
        store_contract::require(
          cohort < cohorts_used_, "unknown oracle cohort");
        return ends_[cohorts_[cohort].last + 1];
    }
    // Effect is observed at the filesystem boundary, independently of the
    // caller's result. An applied flush can persist bytes and still fail or
    // lose its notification. Neither outcome is successful certification.
    void flush_effect(std::size_t cohort) {
        synced(cut(cohort));
        cohorts_[cohort].effect = true;
    }
    void returned(std::size_t cohort, bool success) {
        store_contract::require(
          cohort < cohorts_used_, "unknown oracle cohort");
        auto& state = cohorts_[cohort];
        store_contract::require(
          !state.returned
            && (!success || (state.effect && cut(cohort) <= minimum_)),
          "oracle receipt preceded persistence or returned twice");
        state.returned = true;
        state.success = success;
    }
    [[nodiscard]] bool permits_delivery(
      std::size_t observer, std::size_t cohort, bool success) const noexcept {
        if (observer >= interests_used_ || cohort >= cohorts_used_)
            return false;
        const auto& interest = interests_[observer];
        const auto& state = cohorts_[cohort];
        return interest.terminal == observer_terminal::pending
               && interest.group >= state.first && interest.group <= state.last
               && state.returned && state.success == success;
    }
    void delivered(std::size_t observer, std::size_t cohort, bool success) {
        store_contract::require(
          permits_delivery(observer, cohort, success),
          "oracle observed duplicate, detached or uncovered delivery");
        interests_[observer].terminal = success ? observer_terminal::success
                                                : observer_terminal::failure;
    }
    void detached(std::size_t observer) {
        store_contract::require(
          observer < interests_used_
            && interests_[observer].terminal == observer_terminal::pending,
          "oracle detached terminal interest");
        interests_[observer].terminal = observer_terminal::detached;
    }
    void notification_lost(std::size_t observer) {
        store_contract::require(
          observer < interests_used_
            && interests_[observer].terminal == observer_terminal::pending,
          "oracle lost terminal interest");
        interests_[observer].terminal = observer_terminal::lost;
    }
    [[nodiscard]] std::size_t delivered_successes() const noexcept {
        return static_cast<std::size_t>(std::count_if(
          interests_.begin(),
          interests_.begin() + interests_used_,
          [](const auto& item) {
              return item.terminal == observer_terminal::success;
          }));
    }

private:
    [[nodiscard]] bool boundary(std::uint64_t end) const noexcept {
        return std::find(ends_.begin(), ends_.begin() + count_ + 1, end)
               != ends_.begin() + count_ + 1;
    }
    std::string bytes_;
    std::array<std::uint64_t, 9> ends_{};
    std::size_t count_{0};
    std::uint64_t minimum_, written_;
    enum class observer_terminal { pending, success, failure, detached, lost };
    struct cohort_state {
        std::size_t first{0}, last{0};
        bool effect{false}, returned{false}, success{false};
    };
    struct interest_state {
        std::size_t group{0};
        observer_terminal terminal{observer_terminal::pending};
    };
    std::array<cohort_state, 8> cohorts_{};
    std::array<interest_state, 16> interests_{};
    std::size_t cohorts_used_{0}, interests_used_{0};
};
} // namespace kwaque::storage::testing
