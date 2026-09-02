#ifndef KWAQUE_SRC_RUNTIME_OPERATION_STATISTICS_H_
#define KWAQUE_SRC_RUNTIME_OPERATION_STATISTICS_H_

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

} // namespace kwaque::runtime

#endif // KWAQUE_SRC_RUNTIME_OPERATION_STATISTICS_H_
