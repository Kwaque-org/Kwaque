#include "src/simulation/event_trace.h"

#include "src/base/invariant.h"

#include <seastar/coroutine/maybe_yield.hh>

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <optional>
#include <stdexcept>
#include <utility>

namespace kwaque::simulation {

namespace {

constexpr invariant_id trace_reservation_invariant{"KQ-TRACE-RESERVATION"};
constexpr invariant_id trace_scheduler_invariant{"KQ-TRACE-SCHEDULER"};

[[nodiscard]] runtime::operation_error trace_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::trace};
}

[[nodiscard]] runtime::result<void> invalid_trace(errc code) noexcept {
    return runtime::failure(trace_error(code));
}

[[nodiscard]] constexpr bool header_matches_limits(
  const trace_header& header, trace_limits limits) noexcept {
    return header.trace_budget.entries == limits.entries()
           && header.trace_budget.encoded_bytes == limits.encoded_bytes()
           && header.trace_budget.line_bytes == limits.line_bytes();
}

template<typename Output, typename Integer>
void append_hex(Output& output, Integer value, std::size_t width) {
    constexpr std::string_view digits{"0123456789abcdef"};
    for (std::size_t index = 0; index < width; ++index) {
        const auto shift = (width - index - 1U) * 4U;
        output.push_back(
          digits[(static_cast<std::uint64_t>(value) >> shift) & 0xfU]);
    }
}

template<typename Output>
void append_digest(Output& output, const trace_digest& digest) {
    for (const auto byte : digest) {
        append_hex(output, byte, 2);
    }
}

template<typename Output>
void append_field(Output& output, std::uint64_t value, std::size_t width) {
    output.push_back(' ');
    append_hex(output, value, width);
}

template<typename Output>
void append_header(
  Output& output, const trace_header& header, std::uint64_t entry_count) {
    output.append("KQTR");
    append_field(output, header.schema_version, 2);
    append_field(output, header.master_seed, 16);
    append_field(output, header.random_algorithm_version, 8);
    append_field(output, header.coordinate_schema_version, 8);
    append_field(output, header.ordering_version, 8);
    append_field(output, header.scheduler_budget.pending_events, 8);
    append_field(output, header.scheduler_budget.events_per_pump, 16);
    append_field(output, header.scheduler_budget.total_events, 16);
    append_field(output, header.scheduler_budget.maximum_deadline, 16);
    append_field(output, header.trace_budget.entries, 8);
    append_field(output, header.trace_budget.encoded_bytes, 16);
    append_field(output, header.trace_budget.line_bytes, 8);
    append_field(output, entry_count, 16);
    output.push_back(' ');
    append_digest(output, header.configuration_digest);
    output.push_back(' ');
    append_digest(output, header.input_digest);
    output.push_back('\n');
}

template<typename Output>
void append_entry(Output& output, const trace_entry& entry) {
    output.push_back('E');
    append_field(output, entry.sequence, 16);
    append_field(output, entry.time.nanoseconds(), 16);
    append_field(output, static_cast<std::uint8_t>(entry.action), 2);
    append_field(output, static_cast<std::uint8_t>(entry.kind), 2);
    append_field(output, entry.event_id, 16);
    append_field(output, entry.priority, 2);
    append_field(output, entry.domain, 8);
    append_field(output, entry.stable_id, 16);
    append_field(output, entry.coordinate_a, 16);
    append_field(output, entry.coordinate_b, 16);
    append_field(output, entry.value, 16);
    append_field(output, entry.result, 8);
    append_field(output, entry.context_size, 2);
    for (const auto& field : entry.context) {
        output.push_back(' ');
        append_hex(output, static_cast<std::uint8_t>(field.key), 2);
        append_hex(output, field.value, 16);
    }
    output.push_back('\n');
}

template<std::size_t Size>
[[nodiscard]] std::optional<std::size_t> tokenize(
  std::string_view line, std::array<std::string_view, Size>& tokens) noexcept {
    if (line.empty() || line.front() == ' ' || line.back() == ' ') {
        return std::nullopt;
    }
    std::size_t count = 0;
    std::size_t begin = 0;
    while (begin < line.size()) {
        if (count == tokens.size()) {
            return std::nullopt;
        }
        const auto end = line.find(' ', begin);
        tokens[count++] = line.substr(
          begin,
          end == std::string_view::npos ? line.size() - begin : end - begin);
        if (end == std::string_view::npos) {
            break;
        }
        begin = end + 1;
        if (begin == line.size() || line[begin] == ' ') {
            return std::nullopt;
        }
    }
    return count;
}

template<typename Integer>
[[nodiscard]] bool
parse_hex(std::string_view text, std::size_t width, Integer& output) noexcept {
    if (text.size() != width) {
        return false;
    }
    std::uint64_t value = 0;
    for (const char character : text) {
        std::uint8_t digit = 0;
        if (character >= '0' && character <= '9') {
            digit = static_cast<std::uint8_t>(character - '0');
        } else if (character >= 'a' && character <= 'f') {
            digit = static_cast<std::uint8_t>(character - 'a' + 10);
        } else {
            return false;
        }
        if (value > (std::numeric_limits<std::uint64_t>::max() >> 4U)) {
            return false;
        }
        value = (value << 4U) | digit;
    }
    if (
      value > static_cast<std::uint64_t>(std::numeric_limits<Integer>::max())) {
        return false;
    }
    output = static_cast<Integer>(value);
    return true;
}

[[nodiscard]] bool
parse_digest(std::string_view text, trace_digest& digest) noexcept {
    if (text.size() != digest.size() * 2U) {
        return false;
    }
    for (std::size_t index = 0; index < digest.size(); ++index) {
        if (!parse_hex(text.substr(index * 2U, 2), 2, digest[index])) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] runtime::result<std::pair<trace_header, std::uint64_t>>
parse_header(std::string_view line) noexcept {
    std::array<std::string_view, 16> tokens{};
    const auto count = tokenize(line, tokens);
    if (!count || *count != 16 || tokens[0] != "KQTR") {
        return runtime::failure(trace_error(errc::malformed_data));
    }

    trace_header header;
    std::uint64_t entry_count = 0;
    if (
      !parse_hex(tokens[1], 2, header.schema_version)
      || !parse_hex(tokens[2], 16, header.master_seed)
      || !parse_hex(tokens[3], 8, header.random_algorithm_version)
      || !parse_hex(tokens[4], 8, header.coordinate_schema_version)
      || !parse_hex(tokens[5], 8, header.ordering_version)
      || !parse_hex(tokens[6], 8, header.scheduler_budget.pending_events)
      || !parse_hex(tokens[7], 16, header.scheduler_budget.events_per_pump)
      || !parse_hex(tokens[8], 16, header.scheduler_budget.total_events)
      || !parse_hex(tokens[9], 16, header.scheduler_budget.maximum_deadline)
      || !parse_hex(tokens[10], 8, header.trace_budget.entries)
      || !parse_hex(tokens[11], 16, header.trace_budget.encoded_bytes)
      || !parse_hex(tokens[12], 8, header.trace_budget.line_bytes)
      || !parse_hex(tokens[13], 16, entry_count)
      || !parse_digest(tokens[14], header.configuration_digest)
      || !parse_digest(tokens[15], header.input_digest)) {
        return runtime::failure(trace_error(errc::malformed_data));
    }
    if (auto valid = header.validate(); !valid) {
        return runtime::failure(valid.error());
    }
    return std::pair{header, entry_count};
}

[[nodiscard]] runtime::result<trace_entry>
parse_entry(std::string_view line) noexcept {
    std::array<std::string_view, 18> tokens{};
    const auto count = tokenize(line, tokens);
    if (!count || *count != 18 || tokens[0] != "E") {
        return runtime::failure(trace_error(errc::malformed_data));
    }

    trace_entry entry;
    std::uint64_t time = 0;
    std::uint8_t action = 0;
    std::uint8_t kind = 0;
    if (
      !parse_hex(tokens[1], 16, entry.sequence)
      || !parse_hex(tokens[2], 16, time) || !parse_hex(tokens[3], 2, action)
      || !parse_hex(tokens[4], 2, kind)
      || !parse_hex(tokens[5], 16, entry.event_id)
      || !parse_hex(tokens[6], 2, entry.priority)
      || !parse_hex(tokens[7], 8, entry.domain)
      || !parse_hex(tokens[8], 16, entry.stable_id)
      || !parse_hex(tokens[9], 16, entry.coordinate_a)
      || !parse_hex(tokens[10], 16, entry.coordinate_b)
      || !parse_hex(tokens[11], 16, entry.value)
      || !parse_hex(tokens[12], 8, entry.result)
      || !parse_hex(tokens[13], 2, entry.context_size)) {
        return runtime::failure(trace_error(errc::malformed_data));
    }
    entry.action = static_cast<trace_action>(action);
    entry.kind = static_cast<trace_event_kind>(kind);
    for (std::size_t index = 0; index < entry.context.size(); ++index) {
        const auto token = tokens[14 + index];
        std::uint8_t key = 0;
        if (
          token.size() != 18 || !parse_hex(token.substr(0, 2), 2, key)
          || !parse_hex(token.substr(2), 16, entry.context[index].value)) {
            return runtime::failure(trace_error(errc::malformed_data));
        }
        entry.context[index].key = static_cast<trace_context_key>(key);
    }
    entry.time = runtime::monotonic_time{time};
    return entry;
}

[[nodiscard]] runtime::result<void> validate_entry_shape(
  const trace_entry& entry, const trace_header& header) noexcept {
    const auto action = static_cast<std::uint8_t>(entry.action);
    const auto kind = static_cast<std::uint8_t>(entry.kind);
    if (
      entry.sequence == 0 || action == 0
      || action > static_cast<std::uint8_t>(trace_action::keyed_decision)
      || kind > static_cast<std::uint8_t>(trace_event_kind::keyed_random)
      || entry.context_size > entry.context.size()
      || entry.time.nanoseconds() > header.scheduler_budget.maximum_deadline) {
        return invalid_trace(errc::malformed_data);
    }
    switch (entry.action) {
    case trace_action::scheduled:
    case trace_action::canceled:
    case trace_action::selected:
        if (
          entry.event_id == 0 || entry.kind == trace_event_kind::keyed_random) {
            return invalid_trace(errc::malformed_data);
        }
        break;
    case trace_action::time_advanced:
        if (
          entry.event_id != 0 || entry.kind != trace_event_kind::generic
          || entry.priority != 0 || entry.domain != 0 || entry.stable_id != 0
          || entry.coordinate_a != entry.time.nanoseconds()
          || entry.value <= entry.coordinate_a
          || entry.value > header.scheduler_budget.maximum_deadline
          || entry.result != 0) {
            return invalid_trace(errc::malformed_data);
        }
        break;
    case trace_action::wall_adjusted:
        if (
          entry.event_id == 0
          || entry.kind != trace_event_kind::wall_adjustment) {
            return invalid_trace(errc::malformed_data);
        }
        break;
    case trace_action::keyed_decision:
        if (
          entry.event_id != 0 || entry.kind != trace_event_kind::keyed_random
          || entry.priority != 0 || entry.domain == 0) {
            return invalid_trace(errc::malformed_data);
        }
        break;
    case trace_action::none:
        return invalid_trace(errc::malformed_data);
    }
    for (std::size_t index = 0; index < entry.context.size(); ++index) {
        const auto key = static_cast<std::uint8_t>(entry.context[index].key);
        if (index < entry.context_size) {
            if (
              key == 0
              || key > static_cast<std::uint8_t>(trace_context_key::detail)) {
                return invalid_trace(errc::malformed_data);
            }
            for (std::size_t previous = 0; previous < index; ++previous) {
                if (entry.context[previous].key == entry.context[index].key) {
                    return invalid_trace(errc::malformed_data);
                }
            }
        } else if (
          entry.context[index].key != trace_context_key::none
          || entry.context[index].value != 0) {
            return invalid_trace(errc::malformed_data);
        }
    }
    return {};
}

} // namespace

void trace_artifact::append(std::string_view bytes) {
    while (!bytes.empty()) {
        if (
          chunks_.empty()
          || chunks_.back().size() == chunks_.back().capacity()) {
            chunks_.emplace_back();
            const auto remaining = expected_size_ > size_
                                     ? expected_size_ - size_
                                     : maximum_contiguous_allocation_bytes;
            chunks_.back().reserve(
              static_cast<std::size_t>(std::min<std::uint64_t>(
                remaining, maximum_contiguous_allocation_bytes)));
        }
        auto& tail = chunks_.back();
        const auto count = std::min(
          bytes.size(), maximum_contiguous_allocation_bytes - tail.size());
        tail.insert(tail.end(), bytes.begin(), bytes.begin() + count);
        bytes.remove_prefix(count);
        size_ += count;
    }
}

void trace_artifact::push_back(char byte) {
    append(std::string_view{&byte, 1});
}

bool trace_artifact::contains(char byte) const noexcept {
    return std::ranges::any_of(chunks_, [byte](const std::vector<char>& chunk) {
        return std::ranges::find(chunk, byte) != chunk.end();
    });
}

bool trace_artifact::copy_to(
  std::uint64_t offset, std::span<char> destination) const noexcept {
    if (offset > size_ || destination.size() > size_ - offset) {
        return false;
    }
    if (destination.empty()) {
        return true;
    }
    std::size_t chunk_index = 0;
    while (offset >= chunks_[chunk_index].size()) {
        offset -= chunks_[chunk_index].size();
        ++chunk_index;
    }
    std::size_t chunk_offset = static_cast<std::size_t>(offset);
    std::size_t copied = 0;
    while (copied < destination.size()) {
        const auto& chunk = chunks_[chunk_index];
        const auto count = std::min(
          chunk.size() - chunk_offset, destination.size() - copied);
        std::memcpy(
          destination.data() + copied, chunk.data() + chunk_offset, count);
        copied += count;
        ++chunk_index;
        chunk_offset = 0;
    }
    return true;
}

runtime::result<std::string> trace_artifact::to_string() const {
    if (size_ >= maximum_contiguous_allocation_bytes) {
        return runtime::failure(trace_error(errc::resource_exhausted));
    }
    std::string result;
    result.reserve(static_cast<std::size_t>(size_));
    for (const auto& chunk : chunks_) {
        result.append(chunk.data(), chunk.size());
    }
    return result;
}

bool trace_artifact::operator==(const trace_artifact& other) const noexcept {
    if (size_ != other.size_) {
        return false;
    }
    auto left_chunk = chunks_.begin();
    auto right_chunk = other.chunks_.begin();
    std::size_t left_offset = 0;
    std::size_t right_offset = 0;
    std::uint64_t compared = 0;
    while (compared < size_) {
        const auto count = std::min(
          left_chunk->size() - left_offset, right_chunk->size() - right_offset);
        if (!std::equal(
              left_chunk->begin() + static_cast<std::ptrdiff_t>(left_offset),
              left_chunk->begin()
                + static_cast<std::ptrdiff_t>(left_offset + count),
              right_chunk->begin()
                + static_cast<std::ptrdiff_t>(right_offset))) {
            return false;
        }
        compared += count;
        left_offset += count;
        right_offset += count;
        if (left_offset == left_chunk->size()) {
            ++left_chunk;
            left_offset = 0;
        }
        if (right_offset == right_chunk->size()) {
            ++right_chunk;
            right_offset = 0;
        }
    }
    return true;
}

runtime::result<trace_limits>
trace_limits::make(trace_limit_values values) noexcept {
    if (
      values.entries == 0 || values.encoded_bytes == 0
      || values.line_bytes == 0) {
        return runtime::failure(trace_error(errc::invalid_argument));
    }
    if (
      values.entries > entries_absolute
      || values.encoded_bytes > encoded_bytes_absolute
      || values.line_bytes > line_bytes_absolute
      || values.line_bytes < std::max(
           canonical_header_encoded_size, canonical_entry_encoded_size)
      || values.encoded_bytes < canonical_header_encoded_size) {
        return runtime::failure(trace_error(errc::out_of_range));
    }
    return trace_limits{values};
}

trace_header trace_header::current(
  std::uint64_t master_seed,
  std::uint32_t random_algorithm_version,
  std::uint32_t coordinate_schema_version,
  trace_scheduler_budget scheduler_budget,
  trace_limits trace_budget,
  trace_digest configuration_digest,
  trace_digest input_digest) noexcept {
    return trace_header{
      .schema_version = event_trace_schema_version,
      .master_seed = master_seed,
      .random_algorithm_version = random_algorithm_version,
      .coordinate_schema_version = coordinate_schema_version,
      .ordering_version = scheduler_ordering_version,
      .scheduler_budget = scheduler_budget,
      .trace_budget = trace_limit_values{
        .entries = trace_budget.entries(),
        .encoded_bytes = trace_budget.encoded_bytes(),
        .line_bytes = trace_budget.line_bytes(),
      },
      .configuration_digest = configuration_digest,
      .input_digest = input_digest,
    };
}

runtime::result<void> trace_header::validate() const noexcept {
    if (
      schema_version != event_trace_schema_version
      || random_algorithm_version == 0 || coordinate_schema_version == 0
      || ordering_version != scheduler_ordering_version
      || scheduler_budget.pending_events == 0
      || scheduler_budget.events_per_pump == 0
      || scheduler_budget.total_events == 0
      || scheduler_budget.maximum_deadline == 0
      || scheduler_budget.events_per_pump > scheduler_budget.total_events) {
        return invalid_trace(errc::malformed_data);
    }
    if (
      scheduler_budget.pending_events > 1'048'576
      || scheduler_budget.events_per_pump > 1'000'000
      || scheduler_budget.total_events > 1'000'000
      || scheduler_budget.maximum_deadline > static_cast<std::uint64_t>(
           std::numeric_limits<std::int64_t>::max())) {
        return invalid_trace(errc::out_of_range);
    }
    auto parsed_limits = trace_limits::make(trace_budget);
    if (!parsed_limits) {
        return runtime::failure(parsed_limits.error());
    }
    return {};
}

event_trace::reservation::~reservation() { release(); }

event_trace::reservation::reservation(reservation&& other) noexcept
  : owner_(std::exchange(other.owner_, nullptr))
  , entries_(std::exchange(other.entries_, 0))
  , encoded_bytes_(std::exchange(other.encoded_bytes_, 0)) {}

event_trace::reservation&
event_trace::reservation::operator=(reservation&& other) noexcept {
    if (this != &other) {
        release();
        owner_ = std::exchange(other.owner_, nullptr);
        entries_ = std::exchange(other.entries_, 0);
        encoded_bytes_ = std::exchange(other.encoded_bytes_, 0);
    }
    return *this;
}

void event_trace::reservation::release() noexcept {
    if (owner_ == nullptr) {
        return;
    }
    auto* owner = std::exchange(owner_, nullptr);
    owner->release(entries_, encoded_bytes_);
    entries_ = 0;
    encoded_bytes_ = 0;
}

event_trace::event_trace(trace_header header, trace_limits limits)
  : header_(std::move(header))
  , limits_(limits)
  , encoded_bytes_(canonical_header_encoded_size) {
    if (
      auto valid = header_.validate();
      !valid || !header_matches_limits(header_, limits_)) {
        throw std::invalid_argument("invalid event trace header");
    }
    entries_.reserve(limits_.entries());
}

event_trace::event_trace(
  trace_header header,
  trace_limits limits,
  seastar::chunked_vector<trace_entry> expected)
  : header_(std::move(header))
  , limits_(limits)
  , entries_(std::move(expected))
  , encoded_bytes_(canonical_header_encoded_size)
  , replaying_(true) {}

event_trace::~event_trace() {
    KWAQUE_INVARIANT(
      trace_reservation_invariant,
      reserved_entries_ == 0 && reserved_bytes_ == 0 && !scheduler_attached_,
      "event trace destroyed with reservations");
}

runtime::result<std::unique_ptr<event_trace>> event_trace::replay(
  trace_header actual_header,
  trace_limits limits,
  decoded_event_trace expected) {
    if (auto valid = actual_header.validate(); !valid) {
        return runtime::failure(valid.error());
    }
    if (actual_header != expected.header) {
        return runtime::failure(trace_error(errc::replay_divergence));
    }
    if (!header_matches_limits(actual_header, limits)) {
        return runtime::failure(trace_error(errc::invalid_argument));
    }
    if (
      expected.entries.size() > limits.entries()
      || expected.encoded_bytes > limits.encoded_bytes()
      || expected.encoded_bytes
           != canonical_header_encoded_size
                + expected.entries.size() * canonical_entry_encoded_size) {
        return runtime::failure(trace_error(errc::out_of_range));
    }
    for (std::size_t index = 0; index < expected.entries.size(); ++index) {
        if (
          expected.entries[index].sequence != index + 1U
          || !validate_entry_shape(expected.entries[index], actual_header)) {
            return runtime::failure(trace_error(errc::malformed_data));
        }
    }
    return std::unique_ptr<event_trace>{new event_trace{
      std::move(actual_header), limits, std::move(expected.entries)}};
}

runtime::result<event_trace::reservation>
event_trace::reserve(std::uint32_t entries, std::uint64_t encoded_bytes) {
    if (failure_) [[unlikely]] {
        return runtime::failure(*failure_);
    }
    if (
      entries == 0
      || encoded_bytes
           != static_cast<std::uint64_t>(entries)
                * canonical_entry_encoded_size) {
        return runtime::failure(trace_error(errc::invalid_argument));
    }
    const auto recorded_entries = replaying_ ? observed_entries_
                                             : entries_.size();
    if (
      entries > limits_.entries() - recorded_entries
      || reserved_entries_ > limits_.entries() - recorded_entries - entries
      || encoded_bytes > limits_.encoded_bytes() - encoded_bytes_
      || reserved_bytes_
           > limits_.encoded_bytes() - encoded_bytes_ - encoded_bytes) {
        return runtime::failure(trace_error(errc::resource_exhausted));
    }
    reserved_entries_ += entries;
    reserved_bytes_ += encoded_bytes;
    return reservation{*this, entries, encoded_bytes};
}

runtime::result<void>
event_trace::observe(trace_entry entry, reservation* reserved) {
    if (failure_) [[unlikely]] {
        return runtime::failure(*failure_);
    }
    entry.sequence = next_sequence();
    if (auto valid = validate_entry(entry); !valid) {
        return runtime::failure(valid.error());
    }
    if (reserved != nullptr) {
        if (
          reserved->owner_ != this || reserved->entries_ == 0
          || reserved->encoded_bytes_ < canonical_entry_encoded_size) {
            return runtime::failure(trace_error(errc::invalid_argument));
        }
    } else {
        const auto recorded_entries = replaying_ ? observed_entries_
                                                 : entries_.size();
        if (
          recorded_entries + reserved_entries_ == limits_.entries()
          || encoded_bytes_ + reserved_bytes_ + canonical_entry_encoded_size
               > limits_.encoded_bytes()) {
            return runtime::failure(trace_error(errc::resource_exhausted));
        }
    }

    if (
      replaying_
      && (observed_entries_ == entries_.size() || entries_[observed_entries_] != entry)) {
        const trace_entry* expected = observed_entries_ < entries_.size()
                                        ? &entries_[observed_entries_]
                                        : nullptr;
        return remember_failure(divergence_error(expected, &entry));
    }

    if (reserved != nullptr) {
        consume(*reserved);
    }
    if (!replaying_) {
        entries_.push_back(entry);
    }
    ++observed_entries_;
    encoded_bytes_ += canonical_entry_encoded_size;
    return {};
}

runtime::result<void> event_trace::finish_replay() noexcept {
    if (failure_) [[unlikely]] {
        return runtime::failure(*failure_);
    }
    if (!replaying_ || observed_entries_ == entries_.size()) {
        return {};
    }
    return remember_failure(
      divergence_error(&entries_[observed_entries_], nullptr));
}

runtime::result<trace_artifact> event_trace::encode() const {
    if (entries_.size() > synchronous_trace_entries_max) {
        return runtime::failure(trace_error(errc::resource_exhausted));
    }
    const auto size = canonical_header_encoded_size
                      + entries_.size() * canonical_entry_encoded_size;
    if (size > limits_.encoded_bytes()) {
        return runtime::failure(trace_error(errc::resource_exhausted));
    }
    trace_artifact encoded{size};
    append_header(encoded, header_, entries_.size());
    for (const auto& entry : entries_) {
        append_entry(encoded, entry);
    }
    if (encoded.size() != size) {
        return runtime::failure(trace_error(errc::invariant_violation));
    }
    return runtime::result<trace_artifact>{std::move(encoded)};
}

seastar::future<runtime::result<trace_artifact>>
event_trace::encode_cooperatively(std::uint32_t entries_per_yield) const {
    if (entries_per_yield == 0) {
        co_return runtime::failure(trace_error(errc::invalid_argument));
    }
    const auto size = canonical_header_encoded_size
                      + entries_.size() * canonical_entry_encoded_size;
    if (size > limits_.encoded_bytes()) {
        co_return runtime::failure(trace_error(errc::resource_exhausted));
    }
    trace_artifact encoded{size};
    append_header(encoded, header_, entries_.size());
    std::uint32_t batch_entries = 0;
    for (const auto& entry : entries_) {
        append_entry(encoded, entry);
        if (++batch_entries == entries_per_yield) {
            batch_entries = 0;
            co_await seastar::coroutine::maybe_yield{};
        }
    }
    if (encoded.size() != size) {
        co_return runtime::failure(trace_error(errc::invariant_violation));
    }
    co_return std::move(encoded);
}

runtime::result<decoded_event_trace>
event_trace::decode(const trace_artifact& encoded, trace_limits parser_limits) {
    if (
      encoded.empty() || encoded.size() > parser_limits.encoded_bytes()
      || encoded.back() != '\n' || encoded.contains('\r')) {
        return runtime::failure(trace_error(errc::malformed_data));
    }

    if (canonical_header_encoded_size - 1U > parser_limits.line_bytes()) {
        return runtime::failure(trace_error(errc::malformed_data));
    }
    std::array<char, canonical_header_encoded_size> header_line{};
    if (!encoded.copy_to(0, header_line) || header_line.back() != '\n') {
        return runtime::failure(trace_error(errc::malformed_data));
    }
    auto parsed_header = parse_header(
      std::string_view{header_line.data(), header_line.size() - 1U});
    if (!parsed_header) {
        return runtime::failure(parsed_header.error());
    }
    auto [header, entry_count] = *parsed_header;
    if (
      entry_count > header.trace_budget.entries
      || entry_count > parser_limits.entries()
      || header.trace_budget.encoded_bytes > parser_limits.encoded_bytes()
      || header.trace_budget.line_bytes > parser_limits.line_bytes()) {
        return runtime::failure(trace_error(errc::out_of_range));
    }
    if (entry_count > synchronous_trace_entries_max) {
        return runtime::failure(trace_error(errc::resource_exhausted));
    }
    const auto expected_size = canonical_header_encoded_size
                               + entry_count * canonical_entry_encoded_size;
    if (expected_size != encoded.size()) {
        return runtime::failure(trace_error(errc::malformed_data));
    }

    seastar::chunked_vector<trace_entry> entries;
    entries.reserve(static_cast<std::size_t>(entry_count));
    std::uint64_t offset = canonical_header_encoded_size;
    std::array<char, canonical_entry_encoded_size> entry_line{};
    for (std::uint64_t index = 0; index < entry_count; ++index) {
        if (
          canonical_entry_encoded_size - 1U > parser_limits.line_bytes()
          || !encoded.copy_to(offset, entry_line)
          || entry_line.back() != '\n') {
            return runtime::failure(trace_error(errc::malformed_data));
        }
        auto entry = parse_entry(
          std::string_view{entry_line.data(), entry_line.size() - 1U});
        if (
          !entry || entry->sequence != index + 1U
          || !validate_entry_shape(*entry, header)) {
            return runtime::failure(trace_error(errc::malformed_data));
        }
        entries.push_back(*entry);
        offset += canonical_entry_encoded_size;
    }
    return decoded_event_trace{
      .header = std::move(header),
      .entries = std::move(entries),
      .encoded_bytes = encoded.size(),
    };
}

runtime::result<decoded_event_trace>
event_trace::decode(std::string_view encoded, trace_limits parser_limits) {
    if (encoded.size() > maximum_contiguous_allocation_bytes) {
        return runtime::failure(trace_error(errc::out_of_range));
    }
    trace_artifact chunked{encoded.size()};
    chunked.append(encoded);
    return decode(chunked, parser_limits);
}

seastar::future<runtime::result<decoded_event_trace>>
event_trace::decode_cooperatively(
  trace_artifact encoded,
  trace_limits parser_limits,
  std::uint32_t entries_per_yield) {
    if (entries_per_yield == 0) {
        co_return runtime::failure(trace_error(errc::invalid_argument));
    }
    if (
      encoded.empty() || encoded.size() > parser_limits.encoded_bytes()
      || encoded.back() != '\n' || encoded.contains('\r')
      || canonical_header_encoded_size - 1U > parser_limits.line_bytes()) {
        co_return runtime::failure(trace_error(errc::malformed_data));
    }

    std::array<char, canonical_header_encoded_size> header_line{};
    if (!encoded.copy_to(0, header_line) || header_line.back() != '\n') {
        co_return runtime::failure(trace_error(errc::malformed_data));
    }
    auto parsed_header = parse_header(
      std::string_view{header_line.data(), header_line.size() - 1U});
    if (!parsed_header) {
        co_return runtime::failure(parsed_header.error());
    }
    auto [header, entry_count] = *parsed_header;
    if (
      entry_count > header.trace_budget.entries
      || entry_count > parser_limits.entries()
      || header.trace_budget.encoded_bytes > parser_limits.encoded_bytes()
      || header.trace_budget.line_bytes > parser_limits.line_bytes()) {
        co_return runtime::failure(trace_error(errc::out_of_range));
    }
    const auto expected_size = canonical_header_encoded_size
                               + entry_count * canonical_entry_encoded_size;
    if (expected_size != encoded.size()) {
        co_return runtime::failure(trace_error(errc::malformed_data));
    }

    seastar::chunked_vector<trace_entry> entries;
    entries.reserve(static_cast<std::size_t>(entry_count));
    std::uint64_t offset = canonical_header_encoded_size;
    std::array<char, canonical_entry_encoded_size> entry_line{};
    std::uint32_t batch_entries = 0;
    for (std::uint64_t index = 0; index < entry_count; ++index) {
        if (
          canonical_entry_encoded_size - 1U > parser_limits.line_bytes()
          || !encoded.copy_to(offset, entry_line)
          || entry_line.back() != '\n') {
            co_return runtime::failure(trace_error(errc::malformed_data));
        }
        auto entry = parse_entry(
          std::string_view{entry_line.data(), entry_line.size() - 1U});
        if (
          !entry || entry->sequence != index + 1U
          || !validate_entry_shape(*entry, header)) {
            co_return runtime::failure(trace_error(errc::malformed_data));
        }
        entries.push_back(*entry);
        offset += canonical_entry_encoded_size;
        if (++batch_entries == entries_per_yield) {
            batch_entries = 0;
            co_await seastar::coroutine::maybe_yield{};
        }
    }
    co_return decoded_event_trace{
      .header = std::move(header),
      .entries = std::move(entries),
      .encoded_bytes = encoded.size(),
    };
}

runtime::result<void>
event_trace::validate_entry(const trace_entry& entry) const noexcept {
    return validate_entry_shape(entry, header_);
}

runtime::operation_error event_trace::divergence_error(
  const trace_entry* expected, const trace_entry* actual) const noexcept {
    auto error = trace_error(errc::replay_divergence);
    static_cast<void>(error.add_context(
      runtime::operation_context_key::sequence, next_sequence()));
    static_cast<void>(error.add_context(
      runtime::operation_context_key::expected,
      expected == nullptr ? 0 : static_cast<std::uint8_t>(expected->action)));
    static_cast<void>(error.add_context(
      runtime::operation_context_key::actual,
      actual == nullptr ? 0 : static_cast<std::uint8_t>(actual->action)));
    static_cast<void>(error.add_context(
      runtime::operation_context_key::limit, entries_.size()));
    return error;
}

runtime::result<void>
event_trace::remember_failure(runtime::operation_error error) noexcept {
    if (!failure_) {
        failure_.emplace(error);
    }
    return runtime::failure(std::move(error));
}

void event_trace::attach_scheduler() noexcept {
    KWAQUE_INVARIANT(
      trace_scheduler_invariant,
      !scheduler_attached_,
      "event trace already has a scheduler");
    scheduler_attached_ = true;
}

void event_trace::detach_scheduler() noexcept {
    KWAQUE_INVARIANT(
      trace_scheduler_invariant,
      scheduler_attached_,
      "event trace scheduler attachment is missing");
    scheduler_attached_ = false;
}

void event_trace::consume(reservation& reserved) noexcept {
    KWAQUE_INVARIANT(
      trace_reservation_invariant,
      reserved.owner_ == this && reserved.entries_ != 0
        && reserved.encoded_bytes_ >= canonical_entry_encoded_size,
      "invalid event trace reservation consumption");
    --reserved_entries_;
    reserved_bytes_ -= canonical_entry_encoded_size;
    --reserved.entries_;
    reserved.encoded_bytes_ -= canonical_entry_encoded_size;
    if (reserved.entries_ == 0) {
        KWAQUE_INVARIANT(
          trace_reservation_invariant,
          reserved.encoded_bytes_ == 0,
          "event trace reservation byte remainder");
        reserved.owner_ = nullptr;
    }
}

void event_trace::release(
  std::uint32_t entries, std::uint64_t encoded_bytes) noexcept {
    KWAQUE_INVARIANT(
      trace_reservation_invariant,
      entries <= reserved_entries_ && encoded_bytes <= reserved_bytes_,
      "event trace reservation release underflow");
    reserved_entries_ -= entries;
    reserved_bytes_ -= encoded_bytes;
}

} // namespace kwaque::simulation
