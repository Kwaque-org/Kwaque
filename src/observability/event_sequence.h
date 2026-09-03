#ifndef KWAQUE_SRC_OBSERVABILITY_EVENT_SEQUENCE_H_
#define KWAQUE_SRC_OBSERVABILITY_EVENT_SEQUENCE_H_

#include "src/observability/event.h"
#include "src/observability/event_identity.h"
#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"

#include <cstdint>
#include <utility>

namespace kwaque::simulation {
class event_log_sink;
}

namespace kwaque::observability {

class event_sequence_test_access;
class production_event_sink;
namespace testing {
class capture_event_sink;
}

class event_sequence final : public runtime::shard_affine {
public:
    class reservation final {
    public:
        ~reservation();

        reservation(const reservation&) = delete;
        reservation& operator=(const reservation&) = delete;
        reservation(reservation&& other) noexcept;
        reservation& operator=(reservation&&) = delete;

        [[nodiscard]] const event& value() const noexcept {
            if (owner_ != nullptr) {
                owner_->assert_current();
            }
            return value_;
        }
        [[nodiscard]] bool active() const noexcept { return owner_ != nullptr; }
        void commit() noexcept;

    private:
        friend class event_sequence;

        reservation(event_sequence& owner, event value) noexcept
          : owner_(&owner)
          , value_(std::move(value)) {}

        event_sequence* owner_;
        event value_;
    };

    ~event_sequence();

    event_sequence(const event_sequence&) = delete;
    event_sequence& operator=(const event_sequence&) = delete;
    event_sequence(event_sequence&&) = delete;
    event_sequence& operator=(event_sequence&&) = delete;

    [[nodiscard]] const event_sink_identity& identity() const noexcept {
        assert_current();
        return identity_;
    }
    [[nodiscard]] std::uint64_t last_sequence() const noexcept {
        assert_current();
        return last_sequence_;
    }

private:
    friend class event_sequence_test_access;
    friend class production_event_sink;
    friend class testing::capture_event_sink;
    friend class ::kwaque::simulation::event_log_sink;

    explicit event_sequence(event_sink_identity identity) noexcept
      : identity_(identity) {}
    [[nodiscard]] runtime::result<reservation>
    prepare(const event_request& request) noexcept;

    void release() noexcept {
        assert_current();
        reserved_ = false;
    }

    event_sink_identity identity_;
    std::uint64_t last_sequence_{0};
    bool reserved_{false};
};

} // namespace kwaque::observability

#endif // KWAQUE_SRC_OBSERVABILITY_EVENT_SEQUENCE_H_
