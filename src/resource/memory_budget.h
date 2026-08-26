#pragma once

#include "src/base/result.h"
#include "src/base/units.h"
#include "src/runtime/error.h"
#include "src/runtime/shard_affinity.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>

#include <cstddef>
#include <cstdint>

namespace kwaque::resource {

struct memory_budget_config final {
    byte_count capacity;
    byte_count soft_watermark;
    byte_count high_watermark;
    std::size_t max_waiters{0};

    [[nodiscard]] static result<memory_budget_config>
    with_defaults(byte_count capacity, std::size_t max_waiters) noexcept;
    [[nodiscard]] result<void> validate() const noexcept;

    bool operator==(const memory_budget_config&) const = default;
};

struct memory_budget_counters final {
    std::uint64_t admitted{0};
    std::uint64_t rejected{0};
    std::uint64_t high_transitions{0};
    std::uint64_t relief_transitions{0};

    bool operator==(const memory_budget_counters&) const = default;
};

class memory_budget;

// Optional pressure notification. The trigger must outlive the budget.
// Implementations run on the budget's owner shard and must only schedule
// already prepared reclaim work.
class memory_reclaim_trigger {
public:
    virtual ~memory_reclaim_trigger() = default;
    virtual void request_reclaim(byte_count target) noexcept = 0;
};

class memory_units final {
public:
    memory_units() noexcept = default;
    memory_units(memory_units&& other) noexcept;
    memory_units& operator=(memory_units&& other) noexcept;
    memory_units(const memory_units&) = delete;
    memory_units& operator=(const memory_units&) = delete;
    ~memory_units();

    [[nodiscard]] byte_count count() const noexcept;
    [[nodiscard]] explicit operator bool() const noexcept;

    [[nodiscard]] result<memory_units> split(byte_count bytes) noexcept;
    [[nodiscard]] result<void> merge(memory_units&& other) noexcept;
    [[nodiscard]] byte_count release() noexcept;

private:
    friend class memory_budget;

    memory_units(
      memory_budget& budget, seastar::semaphore_units<> units) noexcept;

    memory_budget* budget_{nullptr};
    seastar::semaphore_units<> units_;
};

// A hard, byte-counted, shard-local admission budget. Watermarks trigger
// reclamation observations only; admission returns when memory_units do.
// Pressure is measured over committed bytes, so handing a release straight to
// a waiting caller frees nothing and does not relieve pressure.
class memory_budget final : public runtime::shard_affine {
public:
    explicit memory_budget(
      memory_budget_config config,
      memory_reclaim_trigger* reclaim_trigger = nullptr);
    ~memory_budget();

    [[nodiscard]] byte_count capacity() const;
    [[nodiscard]] byte_count available() const;
    [[nodiscard]] byte_count used() const;
    // Bytes the admission counter has already handed to a readied waiter that
    // has not resumed and taken ownership of them yet. They belong to no
    // memory_units and cannot be admitted to anyone else.
    [[nodiscard]] byte_count granted_pending() const;
    // used() + granted_pending(): every byte this budget has committed.
    [[nodiscard]] byte_count committed() const;
    [[nodiscard]] byte_count scheduled_release() const;
    [[nodiscard]] byte_count active() const;
    [[nodiscard]] std::size_t waiting() const;
    [[nodiscard]] std::size_t max_waiters() const;
    [[nodiscard]] bool under_pressure() const;
    [[nodiscard]] memory_budget_counters counters() const;

    [[nodiscard]] runtime::result<memory_units> try_acquire(byte_count bytes);
    [[nodiscard]] seastar::future<runtime::result<memory_units>>
    acquire(byte_count bytes, seastar::abort_source& abort_source);

    [[nodiscard]] result<void> schedule_release(byte_count bytes);

private:
    friend class memory_units;

    [[nodiscard]] runtime::operation_error
    admission_error(errc code, byte_count bytes) const noexcept;
    [[nodiscard]] runtime::result<void>
    validate_request(byte_count bytes) const;
    // `from_grant` is true only on the waiting path, where the admission
    // counter deducted these bytes when the waiter was readied rather than
    // when it resumed.
    [[nodiscard]] memory_units
    adopt(seastar::semaphore_units<> units, bool from_grant) noexcept;
    void release_owned(seastar::semaphore_units<>& units) noexcept;
    [[nodiscard]] byte_count committed_bytes() const noexcept;
    void update_pressure() noexcept;
    [[nodiscard]] static std::size_t
    validated_capacity(const memory_budget_config& config);
    static void increment(std::uint64_t& value);

    const memory_budget_config config_;
    seastar::semaphore admission_;
    memory_reclaim_trigger* reclaim_trigger_;
    byte_count used_;
    byte_count granted_pending_;
    byte_count scheduled_release_;
    std::size_t waiting_{0};
    memory_budget_counters counters_;
    bool under_pressure_{false};
};

} // namespace kwaque::resource
