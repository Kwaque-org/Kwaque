#ifndef KWAQUE_SRC_SIMULATION_EVENT_TRACE_H_
#define KWAQUE_SRC_SIMULATION_EVENT_TRACE_H_

#include "src/base/allocation.h"
#include "src/runtime/error.h"
#include "src/runtime/time.h"

#include <seastar/core/chunked_vector.hh>
#include <seastar/core/future.hh>

#include <array>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace kwaque::simulation {

class scheduler;

inline constexpr std::uint32_t event_trace_schema_version{1};
inline constexpr std::uint32_t scheduler_ordering_version{1};
inline constexpr std::size_t trace_context_fields_max{4};
inline constexpr std::size_t canonical_entry_encoded_size{227};
inline constexpr std::size_t canonical_header_encoded_size{294};
inline constexpr std::uint32_t synchronous_trace_entries_max{1'024};

using trace_digest = std::array<std::uint8_t, 32>;

class trace_artifact final {
public:
    trace_artifact() = default;
    explicit trace_artifact(std::uint64_t expected_size) noexcept
      : expected_size_(expected_size) {}
    trace_artifact(trace_artifact&&) noexcept = default;
    trace_artifact& operator=(trace_artifact&&) noexcept = default;
    trace_artifact(const trace_artifact&) = delete;
    trace_artifact& operator=(const trace_artifact&) = delete;

    void append(std::string_view bytes);
    void push_back(char byte);

    [[nodiscard]] bool empty() const noexcept { return size_ == 0; }
    [[nodiscard]] std::uint64_t size() const noexcept { return size_; }
    [[nodiscard]] char back() const noexcept { return chunks_.back().back(); }
    [[nodiscard]] bool contains(char byte) const noexcept;
    [[nodiscard]] bool
    copy_to(std::uint64_t offset, std::span<char> destination) const noexcept;
    [[nodiscard]] runtime::result<std::string> to_string() const;
    [[nodiscard]] const std::deque<std::vector<char>>& chunks() const noexcept {
        return chunks_;
    }

    [[nodiscard]] bool operator==(const trace_artifact& other) const noexcept;

private:
    std::deque<std::vector<char>> chunks_;
    std::uint64_t size_{0};
    std::uint64_t expected_size_{0};
};

struct trace_scheduler_budget final {
    std::uint32_t pending_events;
    std::uint64_t events_per_pump;
    std::uint64_t total_events;
    std::uint64_t maximum_deadline;

    bool operator==(const trace_scheduler_budget&) const = default;
};

struct trace_limit_values final {
    std::uint32_t entries{524'288};
    std::uint64_t encoded_bytes{std::uint64_t{134'217'728}};
    std::uint32_t line_bytes{1'024};

    bool operator==(const trace_limit_values&) const = default;
};

class trace_limits final {
public:
    static constexpr std::uint32_t entries_absolute{4'194'304};
    static constexpr std::uint64_t encoded_bytes_absolute{
      std::uint64_t{1'073'741'824}};
    static constexpr std::uint32_t line_bytes_absolute{4'096};

    [[nodiscard]] static runtime::result<trace_limits>
    make(trace_limit_values values) noexcept;
    [[nodiscard]] static constexpr trace_limits defaults() noexcept {
        return trace_limits{trace_limit_values{}};
    }

    [[nodiscard]] constexpr std::uint32_t entries() const noexcept {
        return values_.entries;
    }
    [[nodiscard]] constexpr std::uint64_t encoded_bytes() const noexcept {
        return values_.encoded_bytes;
    }
    [[nodiscard]] constexpr std::uint32_t line_bytes() const noexcept {
        return values_.line_bytes;
    }

    bool operator==(const trace_limits&) const = default;

private:
    constexpr explicit trace_limits(trace_limit_values values) noexcept
      : values_(values) {}

    trace_limit_values values_;
};

struct trace_header final {
    std::uint32_t schema_version{event_trace_schema_version};
    std::uint64_t master_seed{0};
    std::uint32_t random_algorithm_version{0};
    std::uint32_t coordinate_schema_version{0};
    std::uint32_t ordering_version{scheduler_ordering_version};
    trace_scheduler_budget scheduler_budget{};
    trace_limit_values trace_budget{};
    trace_digest configuration_digest{};
    trace_digest input_digest{};

    [[nodiscard]] static trace_header current(
      std::uint64_t master_seed,
      std::uint32_t random_algorithm_version,
      std::uint32_t coordinate_schema_version,
      trace_scheduler_budget scheduler_budget,
      trace_limits trace_budget,
      trace_digest configuration_digest,
      trace_digest input_digest) noexcept;
    [[nodiscard]] runtime::result<void> validate() const noexcept;

    bool operator==(const trace_header&) const = default;
};

enum class trace_action : std::uint8_t {
    none = 0,
    scheduled = 1,
    canceled = 2,
    selected = 3,
    time_advanced = 4,
    wall_adjusted = 5,
    keyed_decision = 6,
};

enum class trace_event_kind : std::uint8_t {
    generic = 0,
    timer = 1,
    wall_adjustment = 2,
    keyed_random = 3,
};

enum class trace_context_key : std::uint8_t {
    none = 0,
    expected = 1,
    actual = 2,
    limit = 3,
    detail = 4,
};

struct trace_context_field final {
    trace_context_key key{trace_context_key::none};
    std::uint64_t value{0};

    bool operator==(const trace_context_field&) const = default;
};

struct trace_event_descriptor final {
    trace_event_kind kind{trace_event_kind::generic};
    std::uint32_t domain{0};
    std::uint64_t stable_id{0};
    std::uint64_t coordinate_a{0};
    std::uint64_t coordinate_b{0};
    std::uint64_t value{0};
    std::uint32_t result{0};
    trace_action effect{trace_action::none};

    bool operator==(const trace_event_descriptor&) const = default;
};

struct trace_entry final {
    std::uint64_t sequence{0};
    runtime::monotonic_time time{};
    trace_action action{trace_action::none};
    trace_event_kind kind{trace_event_kind::generic};
    std::uint64_t event_id{0};
    std::uint8_t priority{0};
    std::uint32_t domain{0};
    std::uint64_t stable_id{0};
    std::uint64_t coordinate_a{0};
    std::uint64_t coordinate_b{0};
    std::uint64_t value{0};
    std::uint32_t result{0};
    std::array<trace_context_field, trace_context_fields_max> context{};
    std::uint8_t context_size{0};

    bool operator==(const trace_entry&) const = default;
};

struct decoded_event_trace final {
    trace_header header;
    seastar::chunked_vector<trace_entry> entries;
    std::uint64_t encoded_bytes{0};
};

class event_trace final {
public:
    class reservation final {
    public:
        reservation() noexcept = default;
        ~reservation();

        reservation(const reservation&) = delete;
        reservation& operator=(const reservation&) = delete;
        reservation(reservation&& other) noexcept;
        reservation& operator=(reservation&& other) noexcept;

        [[nodiscard]] bool active() const noexcept { return owner_ != nullptr; }
        [[nodiscard]] std::uint32_t entries() const noexcept {
            return entries_;
        }
        void release() noexcept;

    private:
        friend class event_trace;

        reservation(
          event_trace& owner,
          std::uint32_t entries,
          std::uint64_t encoded_bytes) noexcept
          : owner_(&owner)
          , entries_(entries)
          , encoded_bytes_(encoded_bytes) {}

        event_trace* owner_{nullptr};
        std::uint32_t entries_{0};
        std::uint64_t encoded_bytes_{0};
    };

    event_trace(trace_header header, trace_limits limits);
    ~event_trace();

    event_trace(const event_trace&) = delete;
    event_trace& operator=(const event_trace&) = delete;
    event_trace(event_trace&&) = delete;
    event_trace& operator=(event_trace&&) noexcept = delete;

    [[nodiscard]] static runtime::result<std::unique_ptr<event_trace>> replay(
      trace_header actual_header,
      trace_limits limits,
      decoded_event_trace expected);

    [[nodiscard]] runtime::result<reservation>
    reserve(std::uint32_t entries, std::uint64_t encoded_bytes);
    [[nodiscard]] runtime::result<void>
    observe(trace_entry entry, reservation* reserved = nullptr);
    [[nodiscard]] runtime::result<void> finish_replay() noexcept;

    [[nodiscard]] runtime::result<trace_artifact> encode() const;
    [[nodiscard]] seastar::future<runtime::result<trace_artifact>>
    encode_cooperatively(std::uint32_t entries_per_yield = 1'024) const;
    [[nodiscard]] static runtime::result<decoded_event_trace>
    decode(const trace_artifact& encoded, trace_limits parser_limits);
    [[nodiscard]] static seastar::future<runtime::result<decoded_event_trace>>
    decode_cooperatively(
      trace_artifact encoded,
      trace_limits parser_limits,
      std::uint32_t entries_per_yield = 1'024);
    // Convenience for small diagnostics/tests. Larger inputs must use the
    // chunked artifact path so decoding never duplicates a large allocation.
    [[nodiscard]] static runtime::result<decoded_event_trace>
    decode(std::string_view encoded, trace_limits parser_limits);

    [[nodiscard]] const trace_header& header() const noexcept {
        return header_;
    }
    [[nodiscard]] const trace_limits& limits() const noexcept {
        return limits_;
    }
    [[nodiscard]] const seastar::chunked_vector<trace_entry>&
    entries() const noexcept {
        return entries_;
    }
    [[nodiscard]] std::uint64_t encoded_bytes() const noexcept {
        return encoded_bytes_;
    }
    [[nodiscard]] std::uint64_t next_sequence() const noexcept {
        return observed_entries_ + 1U;
    }
    [[nodiscard]] bool replaying() const noexcept { return replaying_; }
    [[nodiscard]] bool failed() const noexcept { return failure_.has_value(); }
    [[nodiscard]] const runtime::operation_error* failure() const noexcept {
        return failure_ ? &*failure_ : nullptr;
    }

private:
    friend class scheduler;

    event_trace(
      trace_header header,
      trace_limits limits,
      seastar::chunked_vector<trace_entry> expected);

    [[nodiscard]] runtime::result<void>
    validate_entry(const trace_entry& entry) const noexcept;
    [[nodiscard]] runtime::operation_error divergence_error(
      const trace_entry* expected, const trace_entry* actual) const noexcept;
    [[nodiscard]] runtime::result<void>
    remember_failure(runtime::operation_error error) noexcept;
    void attach_scheduler() noexcept;
    void detach_scheduler() noexcept;
    void consume(reservation& reserved) noexcept;
    void release(std::uint32_t entries, std::uint64_t encoded_bytes) noexcept;

    trace_header header_;
    trace_limits limits_;
    seastar::chunked_vector<trace_entry> entries_;
    std::uint64_t encoded_bytes_{0};
    std::uint64_t observed_entries_{0};
    std::uint32_t reserved_entries_{0};
    std::uint64_t reserved_bytes_{0};
    std::optional<runtime::operation_error> failure_;
    bool scheduler_attached_{false};
    bool replaying_{false};
};

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_EVENT_TRACE_H_
