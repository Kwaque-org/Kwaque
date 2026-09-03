#ifndef KWAQUE_SRC_RUNTIME_OPERATION_STATISTICS_H_
#define KWAQUE_SRC_RUNTIME_OPERATION_STATISTICS_H_

#include "src/runtime/shard_affinity.h"

#include <seastar/core/shared_ptr.hh>

#include <cstdint>
#include <utility>

namespace kwaque::runtime {

class operation_statistics_test_access;

struct operation_statistics_snapshot final {
    std::uint64_t active{0};
    std::uint64_t accepted{0};
    std::uint64_t completed{0};
    std::uint64_t rejected{0};
    std::uint64_t completed_bytes{0};

    bool operator==(const operation_statistics_snapshot&) const = default;
};

// Admission returns one move-only terminal token. Destroying it completes the
// admitted operation whether its typed result succeeded or failed; rejection
// is reserved for calls that never crossed admission. Callers add bytes only
// after the byte-producing result succeeds.
class operation_statistics final {
public:
    class reservation final {
    public:
        reservation(const reservation&) = delete;
        reservation& operator=(const reservation&) = delete;
        reservation(reservation&& other) noexcept
          : owner_(std::exchange(other.owner_, nullptr)) {}
        reservation& operator=(reservation&&) = delete;

        ~reservation() {
            if (owner_ != nullptr) {
                owner_->complete();
            }
        }

        void add_completed_bytes(std::uint64_t bytes) noexcept {
            owner_->values_.completed_bytes += bytes;
        }

    private:
        friend class operation_statistics;

        explicit reservation(operation_statistics& owner) noexcept
          : owner_(&owner) {}

        operation_statistics* owner_;
    };

    [[nodiscard]] reservation accept() noexcept {
        ++values_.active;
        ++values_.accepted;
        return reservation{*this};
    }

    void reject() noexcept { ++values_.rejected; }

    [[nodiscard]] constexpr std::uint64_t active() const noexcept {
        return values_.active;
    }
    [[nodiscard]] constexpr std::uint64_t accepted() const noexcept {
        return values_.accepted;
    }
    [[nodiscard]] constexpr std::uint64_t completed() const noexcept {
        return values_.completed;
    }
    [[nodiscard]] constexpr std::uint64_t rejected() const noexcept {
        return values_.rejected;
    }
    [[nodiscard]] constexpr std::uint64_t completed_bytes() const noexcept {
        return values_.completed_bytes;
    }
    [[nodiscard]] constexpr operation_statistics_snapshot
    snapshot() const noexcept {
        return values_;
    }

private:
    friend class operation_statistics_test_access;

    void complete() noexcept {
        --values_.active;
        ++values_.completed;
    }

    operation_statistics_snapshot values_;
};

// Copyable ownership for one direct statistics block. Runtime handles retain a
// copy so an accepted operation and its counters cannot outlive the storage.
// Counter updates still target operation_statistics directly and allocate no
// per-operation state.
class operation_statistics_owner final {
public:
    operation_statistics_owner()
      : value_(seastar::make_lw_shared<operation_statistics>()) {}
    ~operation_statistics_owner() { owner_.assert_current(); }

    operation_statistics_owner(const operation_statistics_owner& other) noexcept
      : owner_(other.owner_)
      , value_(current_value(other)) {}
    operation_statistics_owner&
    operator=(const operation_statistics_owner& other) noexcept {
        owner_.assert_current();
        other.owner_.assert_current();
        value_ = other.value_;
        return *this;
    }
    operation_statistics_owner(operation_statistics_owner&& other) noexcept
      : owner_(other.owner_)
      , value_(current_value(other)) {}
    operation_statistics_owner&
    operator=(operation_statistics_owner&& other) noexcept {
        owner_.assert_current();
        other.owner_.assert_current();
        value_ = other.value_;
        return *this;
    }

    [[nodiscard]] operation_statistics* operator->() noexcept {
        owner_.assert_current();
        return value_.get();
    }
    [[nodiscard]] const operation_statistics* operator->() const noexcept {
        owner_.assert_current();
        return value_.get();
    }
    [[nodiscard]] operation_statistics& get() noexcept {
        owner_.assert_current();
        return *value_;
    }
    [[nodiscard]] const operation_statistics& get() const noexcept {
        owner_.assert_current();
        return *value_;
    }

private:
    [[nodiscard]] static const seastar::lw_shared_ptr<operation_statistics>&
    current_value(const operation_statistics_owner& other) noexcept {
        other.owner_.assert_current();
        return other.value_;
    }

    owner_shard owner_;
    seastar::lw_shared_ptr<operation_statistics> value_;
};

} // namespace kwaque::runtime

#endif // KWAQUE_SRC_RUNTIME_OPERATION_STATISTICS_H_
