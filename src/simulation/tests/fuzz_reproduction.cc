#include "src/simulation/tests/fuzz_reproduction.h"

#include "src/observability/event.h"
#include "src/simulation/determinism_version.h"
#include "src/simulation/sha256.h"

#include <seastar/coroutine/maybe_yield.hh>

#include <algorithm>
#include <array>
#include <charconv>
#include <iomanip>
#include <istream>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>

namespace kwaque::simulation::testing {

namespace {

constexpr std::string_view reproduction_magic{"KQREPRO 01"};
constexpr std::size_t raw_bytes_per_output_line{2'000};

[[nodiscard]] runtime::operation_error reproduction_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::trace};
}

template<typename T>
[[nodiscard]] runtime::result<T> reproduction_failure(errc code) noexcept {
    return runtime::failure(reproduction_error(code));
}

[[nodiscard]] bool known_harness(fuzz_harness value) noexcept {
    return value >= fuzz_harness::scheduler
           && value <= fuzz_harness::semantic_canary;
}

[[nodiscard]] bool known_error(errc value) noexcept {
    return value >= errc::success && value <= errc::not_a_directory;
}

[[nodiscard]] bool known_operation(runtime::operation_kind value) noexcept {
    return value >= runtime::operation_kind::generic
           && value <= runtime::operation_kind::observability;
}

template<typename Integer>
[[nodiscard]] std::string hex_integer(Integer value) {
    using unsigned_type = std::make_unsigned_t<Integer>;
    std::ostringstream output;
    output << std::hex << std::nouppercase << std::setfill('0')
           << std::setw(static_cast<int>(sizeof(Integer) * 2U))
           << static_cast<std::uint64_t>(static_cast<unsigned_type>(value));
    return output.str();
}

[[nodiscard]] std::string hex_digest(const fuzz_digest& digest) {
    std::string result;
    result.reserve(digest.size() * 2U);
    constexpr std::string_view digits{"0123456789abcdef"};
    for (const auto byte : digest) {
        result.push_back(digits[byte >> 4U]);
        result.push_back(digits[byte & 0x0fU]);
    }
    return result;
}

[[nodiscard]] fuzz_digest envelope_digest(
  fuzz_harness harness,
  std::uint32_t harness_version,
  std::uint64_t master_seed,
  std::uint64_t event_epoch,
  fuzz_case_outcome outcome,
  const fuzz_digest& configuration_digest,
  const fuzz_digest& input_digest,
  const fuzz_digest& terminal_digest,
  const fuzz_digest& trace_digest,
  const fuzz_digest& event_digest) {
    sha256_hasher hasher;
    constexpr std::string_view domain{"KQREPRO"};
    hasher.update(domain.data(), domain.size());
    const auto update_integer = [&hasher]<typename Integer>(Integer value) {
        using unsigned_type = std::make_unsigned_t<Integer>;
        static_assert(sizeof(Integer) <= sizeof(std::uint64_t));
        auto encoded = static_cast<std::uint64_t>(
          static_cast<unsigned_type>(value));
        std::array<std::uint8_t, sizeof(Integer)> bytes{};
        for (auto position = bytes.rbegin(); position != bytes.rend();
             ++position) {
            *position = static_cast<std::uint8_t>(encoded & 0xffU);
            encoded >>= 8U;
        }
        hasher.update(bytes.data(), bytes.size());
    };
    const auto update_digest = [&hasher](const fuzz_digest& digest) {
        hasher.update(digest.data(), digest.size());
    };
    update_integer(fuzz_reproduction_format_version);
    update_integer(static_cast<std::uint8_t>(harness));
    update_integer(harness_version);
    update_integer(master_seed);
    update_integer(deterministic_random_algorithm_version);
    update_integer(deterministic_random_coordinate_version);
    update_integer(scheduler_ordering_version);
    update_integer(event_trace_schema_version);
    update_integer(observability::event_schema_version);
    update_integer(event_epoch);
    update_integer(static_cast<std::uint32_t>(outcome.code));
    update_integer(static_cast<std::uint8_t>(outcome.operation));
    update_digest(configuration_digest);
    update_digest(input_digest);
    update_digest(terminal_digest);
    update_digest(trace_digest);
    update_digest(event_digest);
    return std::move(hasher).final();
}

[[nodiscard]] bool write_line(std::ostream& output, std::string_view line) {
    if (line.size() + 1U > fuzz_output_line_bytes_max) {
        return false;
    }
    output.write(line.data(), static_cast<std::streamsize>(line.size()));
    output.put('\n');
    return output.good();
}

[[nodiscard]] bool write_metadata(
  std::ostream& output, std::string_view key, std::string_view value) {
    std::string line;
    line.reserve(key.size() + 1U + value.size());
    line.append(key);
    line.push_back(' ');
    line.append(value);
    return write_line(output, line);
}

template<typename Byte>
[[nodiscard]] bool
write_data_line(std::ostream& output, std::span<const Byte> bytes) {
    constexpr std::string_view digits{"0123456789abcdef"};
    std::string line;
    line.reserve(2U + bytes.size() * 2U);
    line.append("D ");
    for (const auto value : bytes) {
        const auto byte = static_cast<std::uint8_t>(value);
        line.push_back(digits[byte >> 4U]);
        line.push_back(digits[byte & 0x0fU]);
    }
    return write_line(output, line);
}

template<typename Artifact>
[[nodiscard]] bool
write_artifact(std::ostream& output, const Artifact& artifact) {
    using byte_type
      = std::decay_t<decltype(artifact.chunks().front())>::value_type;
    static_assert(sizeof(byte_type) == 1);
    std::array<byte_type, raw_bytes_per_output_line> bytes{};
    std::size_t used = 0;
    for (const auto& chunk : artifact.chunks()) {
        auto remaining = std::span{chunk};
        while (!remaining.empty()) {
            const auto count = std::min(remaining.size(), bytes.size() - used);
            std::copy_n(remaining.data(), count, bytes.data() + used);
            used += count;
            remaining = remaining.subspan(count);
            // Carry a partial line across physical chunk boundaries.
            if (used == bytes.size()) {
                if (!write_data_line(
                      output, std::span<const byte_type>{bytes})) {
                    return false;
                }
                used = 0;
            }
        }
    }
    return used == 0
           || write_data_line(
             output, std::span<const byte_type>{bytes}.first(used));
}

[[nodiscard]] bool
write_bytes(std::ostream& output, std::span<const std::uint8_t> bytes) {
    while (!bytes.empty()) {
        const auto count = std::min(bytes.size(), raw_bytes_per_output_line);
        if (!write_data_line(output, bytes.first(count))) {
            return false;
        }
        bytes = bytes.subspan(count);
    }
    return true;
}

[[nodiscard]] bool write_section_begin(
  std::ostream& output, std::string_view name, std::uint64_t size) {
    std::string line{"SECTION "};
    line.append(name);
    line.push_back(' ');
    line.append(hex_integer(size));
    return write_line(output, line);
}

[[nodiscard]] bool
write_section_end(std::ostream& output, std::string_view name) {
    std::string line{"ENDSECTION "};
    line.append(name);
    return write_line(output, line);
}

[[nodiscard]] runtime::result<std::string> read_line(std::istream& input) {
    std::string line;
    line.reserve(fuzz_output_line_bytes_max - 1U);
    while (true) {
        const auto next = input.get();
        if (next == std::char_traits<char>::eof()) {
            return reproduction_failure<std::string>(errc::truncated_data);
        }
        if (next == '\n') {
            return line;
        }
        if (line.size() == fuzz_output_line_bytes_max - 1U) {
            return reproduction_failure<std::string>(errc::out_of_range);
        }
        line.push_back(static_cast<char>(next));
    }
}

[[nodiscard]] runtime::result<std::string>
read_metadata(std::istream& input, std::string_view expected_key) {
    auto line = read_line(input);
    if (!line) {
        return runtime::failure(line.error());
    }
    const auto separator = line->find(' ');
    if (
      separator == std::string::npos
      || std::string_view{*line}.substr(0, separator) != expected_key
      || separator + 1U == line->size()) {
        return reproduction_failure<std::string>(errc::malformed_data);
    }
    return line->substr(separator + 1U);
}

template<typename Integer>
[[nodiscard]] runtime::result<Integer>
parse_hex_integer(std::string_view text) noexcept {
    using unsigned_type = std::make_unsigned_t<Integer>;
    if (
      text.size() != sizeof(Integer) * 2U
      || !std::all_of(text.begin(), text.end(), [](char value) {
             return (value >= '0' && value <= '9')
                    || (value >= 'a' && value <= 'f');
         })) {
        return reproduction_failure<Integer>(errc::malformed_data);
    }
    unsigned_type parsed{};
    const auto result = std::from_chars(
      text.data(), text.data() + text.size(), parsed, 16);
    if (result.ec != std::errc{} || result.ptr != text.data() + text.size()) {
        return reproduction_failure<Integer>(errc::malformed_data);
    }
    return static_cast<Integer>(parsed);
}

[[nodiscard]] runtime::result<fuzz_digest>
parse_digest(std::string_view text) noexcept {
    if (text.size() != fuzz_digest{}.size() * 2U) {
        return reproduction_failure<fuzz_digest>(errc::malformed_data);
    }
    fuzz_digest digest{};
    for (std::size_t index = 0; index < digest.size(); ++index) {
        auto byte = parse_hex_integer<std::uint8_t>(
          text.substr(index * 2U, 2U));
        if (!byte) {
            return runtime::failure(byte.error());
        }
        digest[index] = *byte;
    }
    return digest;
}

template<typename Integer>
[[nodiscard]] runtime::result<Integer>
read_hex_metadata(std::istream& input, std::string_view key) {
    auto value = read_metadata(input, key);
    if (!value) {
        return runtime::failure(value.error());
    }
    return parse_hex_integer<Integer>(*value);
}

[[nodiscard]] runtime::result<fuzz_digest>
read_digest_metadata(std::istream& input, std::string_view key) {
    auto value = read_metadata(input, key);
    if (!value) {
        return runtime::failure(value.error());
    }
    return parse_digest(*value);
}

[[nodiscard]] runtime::result<std::vector<std::uint8_t>>
decode_hex_bytes(std::string_view text) {
    if (text.empty() || text.size() % 2U != 0) {
        return reproduction_failure<std::vector<std::uint8_t>>(
          errc::malformed_data);
    }
    std::vector<std::uint8_t> result;
    result.reserve(text.size() / 2U);
    for (std::size_t index = 0; index < text.size(); index += 2U) {
        auto byte = parse_hex_integer<std::uint8_t>(text.substr(index, 2U));
        if (!byte) {
            return runtime::failure(byte.error());
        }
        result.push_back(*byte);
    }
    return result;
}

template<typename Append>
[[nodiscard]] runtime::result<void> read_section(
  std::istream& input,
  std::string_view expected_name,
  std::uint64_t maximum_size,
  Append append) {
    auto header = read_line(input);
    if (!header) {
        return runtime::failure(header.error());
    }
    const std::string prefix = "SECTION " + std::string{expected_name} + " ";
    if (!header->starts_with(prefix)) {
        return reproduction_failure<void>(errc::malformed_data);
    }
    auto announced = parse_hex_integer<std::uint64_t>(
      std::string_view{*header}.substr(prefix.size()));
    if (!announced) {
        return runtime::failure(announced.error());
    }
    if (*announced > maximum_size) {
        return reproduction_failure<void>(errc::out_of_range);
    }

    std::uint64_t consumed = 0;
    const std::string end = "ENDSECTION " + std::string{expected_name};
    while (true) {
        auto line = read_line(input);
        if (!line) {
            return runtime::failure(line.error());
        }
        if (*line == end) {
            break;
        }
        if (!line->starts_with("D ")) {
            return reproduction_failure<void>(errc::malformed_data);
        }
        auto bytes = decode_hex_bytes(std::string_view{*line}.substr(2));
        if (!bytes) {
            return runtime::failure(bytes.error());
        }
        if (consumed > *announced) {
            return reproduction_failure<void>(errc::out_of_range);
        }
        const auto expected = std::min<std::uint64_t>(
          raw_bytes_per_output_line, *announced - consumed);
        if (bytes->size() != expected) {
            return reproduction_failure<void>(errc::malformed_data);
        }
        auto appended = append(std::span<const std::uint8_t>{*bytes});
        if (!appended) {
            return runtime::failure(appended.error());
        }
        consumed += bytes->size();
    }
    if (consumed != *announced) {
        return reproduction_failure<void>(errc::truncated_data);
    }
    return {};
}

[[nodiscard]] trace_limits reproduction_trace_limits(fuzz_harness harness) {
    auto limits = trace_limits::make(fuzz_trace_budget(harness));
    if (!limits) {
        throw std::logic_error("invalid reproduction trace limits");
    }
    return *limits;
}

[[nodiscard]] observability::event_log_limits reproduction_event_limits() {
    auto limits = observability::event_log_limits::make(
      observability::event_log_limit_values{
        .entries = fuzz_event_entries_max,
        .encoded_bytes = fuzz_artifact_bytes_max,
      });
    if (!limits) {
        throw std::logic_error("invalid reproduction event limits");
    }
    return *limits;
}

[[nodiscard]] bool
digest_equal(const trace_digest& left, const fuzz_digest& right) noexcept {
    return std::equal(left.begin(), left.end(), right.begin(), right.end());
}

} // namespace

trace_limit_values fuzz_trace_budget(fuzz_harness harness) noexcept {
    switch (harness) {
    case fuzz_harness::fake_network:
        return {
          .entries = fuzz_network_trace_entries_max,
          .encoded_bytes = fuzz_trace_artifact_bytes_max,
          .line_bytes = 1'024};
    case fuzz_harness::fake_file:
        return {
          .entries = fuzz_file_trace_entries_max,
          .encoded_bytes = 1U * 1'024U * 1'024U,
          .line_bytes = 1'024};
    default:
        return {
          .entries = fuzz_trace_entries_max,
          .encoded_bytes = fuzz_artifact_bytes_max,
          .line_bytes = 1'024};
    }
}

trace_scheduler_budget fuzz_scheduler_budget(fuzz_harness harness) noexcept {
    if (harness == fuzz_harness::fake_network) {
        return {
          .pending_events = 4'096,
          .events_per_pump = fuzz_scheduler_batch_max,
          .total_events = 32'768,
          .maximum_deadline = UINT64_C(1'000'000'000'000)};
    }
    return {
      .pending_events = fuzz_scheduler_pending_max,
      .events_per_pump = fuzz_scheduler_batch_max,
      .total_events = fuzz_scheduler_total_max,
      .maximum_deadline = fuzz_scheduler_deadline_max};
}

seastar::future<runtime::result<decoded_event_trace>>
decode_fuzz_trace(const trace_artifact& artifact, fuzz_harness harness) {
    const auto limits = reproduction_trace_limits(harness);
    if (artifact.size() > limits.encoded_bytes()) {
        co_return reproduction_failure<decoded_event_trace>(errc::out_of_range);
    }
    if (
      artifact.size()
      <= canonical_header_encoded_size
           + synchronous_trace_entries_max * canonical_entry_encoded_size) {
        co_return event_trace::decode(artifact, limits);
    }
    trace_artifact owned{artifact.size()};
    for (const auto& chunk : artifact.chunks()) {
        owned.append(std::string_view{chunk.data(), chunk.size()});
        co_await seastar::coroutine::maybe_yield{};
    }
    co_return co_await event_trace::decode_cooperatively(
      std::move(owned), limits, 64);
}

std::uint64_t fuzz_master_seed(std::span<const std::uint8_t> input) noexcept {
    std::uint64_t seed = 0;
    const auto count = std::min(input.size(), sizeof(seed));
    for (std::size_t index = 0; index < count; ++index) {
        seed |= static_cast<std::uint64_t>(input[index]) << (index * 8U);
    }
    return seed;
}

std::vector<std::uint8_t> fuzz_configuration(fuzz_harness harness) {
    std::string_view text;
    switch (harness) {
    case fuzz_harness::scheduler:
        text = "scheduler:v2;pending=256;batch=64;total=4096;trace=512;"
               "pressure=cancel";
        break;
    case fuzz_harness::fault_schedule:
        text = "fault:v2;rules=128;evaluations=512";
        break;
    case fuzz_harness::fake_file:
        text = "file:v2;objects=16;handles=4;pending=64;bytes=65536;trace=4096;"
               "fault-history=1";
        break;
    case fuzz_harness::fake_network:
        text = "network:v2;flows=1,8,32,96;commands=512;pending=4096;total="
               "32768;deadline=1000000000000;trace=16384;trace-bytes=4194304";
        break;
    case fuzz_harness::semantic_canary:
        text = "canary:v2";
        break;
    }
    return {text.begin(), text.end()};
}

std::uint64_t fuzz_event_epoch(fuzz_harness harness) noexcept {
    return fuzz_event_epoch_base + static_cast<std::uint8_t>(harness);
}

fuzz_reproduction::fuzz_reproduction(
  fuzz_harness harness,
  std::uint32_t harness_version,
  std::uint64_t master_seed,
  std::uint64_t event_epoch,
  std::vector<std::uint8_t> configuration,
  std::vector<std::uint8_t> input,
  fuzz_digest configuration_digest,
  fuzz_digest input_digest,
  fuzz_digest terminal_digest,
  fuzz_case_outcome outcome,
  trace_artifact trace,
  observability::event_log_artifact events) noexcept
  : harness_(harness)
  , harness_version_(harness_version)
  , master_seed_(master_seed)
  , event_epoch_(event_epoch)
  , configuration_(std::move(configuration))
  , input_(std::move(input))
  , configuration_digest_(configuration_digest)
  , input_digest_(input_digest)
  , terminal_digest_(terminal_digest)
  , outcome_(outcome)
  , trace_(std::move(trace))
  , events_(std::move(events)) {}

runtime::result<fuzz_reproduction> fuzz_reproduction::make(
  fuzz_harness harness,
  std::uint32_t harness_version,
  std::uint64_t master_seed,
  std::uint64_t event_epoch,
  std::vector<std::uint8_t> configuration,
  std::vector<std::uint8_t> input,
  fuzz_digest configuration_digest,
  fuzz_digest input_digest,
  fuzz_digest terminal_digest,
  fuzz_case_outcome outcome,
  trace_artifact trace,
  observability::event_log_artifact events) {
    if (
      !known_harness(harness) || harness_version != fuzz_harness_version
      || event_epoch != fuzz_event_epoch(harness)
      || master_seed != fuzz_master_seed(input) || !known_error(outcome.code)
      || !known_operation(outcome.operation)) {
        return reproduction_failure<fuzz_reproduction>(errc::invalid_argument);
    }
    if (
      configuration.size() > fuzz_input_bytes_max
      || input.size() > fuzz_input_bytes_max
      || trace.size() > fuzz_trace_budget(harness).encoded_bytes
      || events.size() > fuzz_artifact_bytes_max) {
        return reproduction_failure<fuzz_reproduction>(errc::out_of_range);
    }
    if (
      digest_bytes(configuration) != configuration_digest
      || digest_bytes(input) != input_digest
      || configuration != fuzz_configuration(harness)) {
        return reproduction_failure<fuzz_reproduction>(errc::malformed_data);
    }

    auto decoded_trace = decode_fuzz_trace(trace, harness).get();
    if (!decoded_trace) {
        return reproduction_failure<fuzz_reproduction>(errc::malformed_data);
    }
    const auto& header = decoded_trace->header;
    if (
      header.schema_version != event_trace_schema_version
      || header.master_seed != master_seed
      || header.random_algorithm_version
           != deterministic_random_algorithm_version
      || header.coordinate_schema_version
           != deterministic_random_coordinate_version
      || header.ordering_version != scheduler_ordering_version
      || header.scheduler_budget != fuzz_scheduler_budget(harness)
      || header.trace_budget != fuzz_trace_budget(harness)
      || !digest_equal(header.configuration_digest, configuration_digest)
      || !digest_equal(header.input_digest, input_digest)) {
        return reproduction_failure<fuzz_reproduction>(errc::malformed_data);
    }

    auto decoded_events = observability::event_log::decode(
      events, reproduction_event_limits());
    if (!decoded_events) {
        return reproduction_failure<fuzz_reproduction>(errc::malformed_data);
    }
    if (
      (*decoded_events)->identity().epoch.value() != event_epoch
      || (*decoded_events)->identity().configuration_digest
           != configuration_digest) {
        return reproduction_failure<fuzz_reproduction>(errc::malformed_data);
    }

    return fuzz_reproduction{
      harness,
      harness_version,
      master_seed,
      event_epoch,
      std::move(configuration),
      std::move(input),
      configuration_digest,
      input_digest,
      terminal_digest,
      outcome,
      std::move(trace),
      std::move(events),
    };
}

fuzz_digest digest_bytes(std::span<const std::uint8_t> bytes) {
    sha256_hasher hasher;
    hasher.update(bytes.data(), bytes.size());
    return std::move(hasher).final();
}

fuzz_digest digest_trace(const trace_artifact& trace) {
    sha256_hasher hasher;
    for (const auto& chunk : trace.chunks()) {
        hasher.update(chunk.data(), chunk.size());
    }
    return std::move(hasher).final();
}

fuzz_digest digest_events(const observability::event_log_artifact& events) {
    sha256_hasher hasher;
    for (const auto& chunk : events.chunks()) {
        hasher.update(chunk.data(), chunk.size());
    }
    return std::move(hasher).final();
}

runtime::result<void>
write_fuzz_reproduction(std::ostream& output, const fuzz_reproduction& value) {
    const auto trace_digest = digest_trace(value.trace());
    const auto event_digest = digest_events(value.events());
    const auto integrity_digest = envelope_digest(
      value.harness(),
      value.harness_version(),
      value.master_seed(),
      value.event_epoch(),
      value.outcome(),
      value.configuration_digest(),
      value.input_digest(),
      value.terminal_digest(),
      trace_digest,
      event_digest);
    if (
      !write_line(output, reproduction_magic)
      || !write_metadata(
        output,
        "HARNESS",
        hex_integer(static_cast<std::uint8_t>(value.harness())))
      || !write_metadata(
        output, "HARNESS_VERSION", hex_integer(value.harness_version()))
      || !write_metadata(
        output, "MASTER_SEED", hex_integer(value.master_seed()))
      || !write_metadata(
        output,
        "RANDOM_ALGORITHM",
        hex_integer(deterministic_random_algorithm_version))
      || !write_metadata(
        output,
        "COORDINATE_SCHEMA",
        hex_integer(deterministic_random_coordinate_version))
      || !write_metadata(
        output, "ORDERING", hex_integer(scheduler_ordering_version))
      || !write_metadata(
        output, "TRACE_SCHEMA", hex_integer(event_trace_schema_version))
      || !write_metadata(
        output,
        "EVENT_SCHEMA",
        hex_integer(observability::event_schema_version))
      || !write_metadata(
        output, "EVENT_EPOCH", hex_integer(value.event_epoch()))
      || !write_metadata(
        output,
        "OUTCOME_CODE",
        hex_integer(static_cast<std::uint32_t>(value.outcome().code)))
      || !write_metadata(
        output,
        "OUTCOME_OPERATION",
        hex_integer(static_cast<std::uint8_t>(value.outcome().operation)))
      || !write_metadata(
        output,
        "CONFIGURATION_DIGEST",
        hex_digest(value.configuration_digest()))
      || !write_metadata(
        output, "INPUT_DIGEST", hex_digest(value.input_digest()))
      || !write_metadata(
        output, "TERMINAL_DIGEST", hex_digest(value.terminal_digest()))
      || !write_metadata(output, "TRACE_DIGEST", hex_digest(trace_digest))
      || !write_metadata(output, "EVENT_DIGEST", hex_digest(event_digest))
      || !write_metadata(
        output, "ENVELOPE_DIGEST", hex_digest(integrity_digest))) {
        return reproduction_failure<void>(errc::io_failure);
    }

    if (
      !write_section_begin(
        output, "CONFIGURATION", value.configuration().size())
      || !write_bytes(output, value.configuration())
      || !write_section_end(output, "CONFIGURATION")
      || !write_section_begin(output, "INPUT", value.input().size())
      || !write_bytes(output, value.input())
      || !write_section_end(output, "INPUT")
      || !write_section_begin(output, "TRACE", value.trace().size())) {
        return reproduction_failure<void>(errc::io_failure);
    }
    if (!write_artifact(output, value.trace())) {
        return reproduction_failure<void>(errc::io_failure);
    }
    if (
      !write_section_end(output, "TRACE")
      || !write_section_begin(output, "EVENTS", value.events().size())) {
        return reproduction_failure<void>(errc::io_failure);
    }
    if (!write_artifact(output, value.events())) {
        return reproduction_failure<void>(errc::io_failure);
    }
    if (
      !write_section_end(output, "EVENTS")
      || !write_line(output, "END KQREPRO")) {
        return reproduction_failure<void>(errc::io_failure);
    }
    return {};
}

runtime::result<fuzz_reproduction> read_fuzz_reproduction(std::istream& input) {
    auto magic = read_line(input);
    if (!magic) {
        return runtime::failure(magic.error());
    }
    if (*magic != reproduction_magic) {
        return reproduction_failure<fuzz_reproduction>(errc::malformed_data);
    }

    auto harness_value = read_hex_metadata<std::uint8_t>(input, "HARNESS");
    auto harness_version = read_hex_metadata<std::uint32_t>(
      input, "HARNESS_VERSION");
    auto master_seed = read_hex_metadata<std::uint64_t>(input, "MASTER_SEED");
    auto random_algorithm = read_hex_metadata<std::uint32_t>(
      input, "RANDOM_ALGORITHM");
    auto coordinate_schema = read_hex_metadata<std::uint32_t>(
      input, "COORDINATE_SCHEMA");
    auto ordering = read_hex_metadata<std::uint32_t>(input, "ORDERING");
    auto trace_schema = read_hex_metadata<std::uint32_t>(input, "TRACE_SCHEMA");
    auto event_schema = read_hex_metadata<std::uint32_t>(input, "EVENT_SCHEMA");
    auto event_epoch = read_hex_metadata<std::uint64_t>(input, "EVENT_EPOCH");
    auto outcome_code = read_hex_metadata<std::uint32_t>(input, "OUTCOME_CODE");
    auto outcome_operation = read_hex_metadata<std::uint8_t>(
      input, "OUTCOME_OPERATION");
    auto configuration_digest = read_digest_metadata(
      input, "CONFIGURATION_DIGEST");
    auto input_digest = read_digest_metadata(input, "INPUT_DIGEST");
    auto terminal_digest = read_digest_metadata(input, "TERMINAL_DIGEST");
    auto expected_trace_digest = read_digest_metadata(input, "TRACE_DIGEST");
    auto expected_event_digest = read_digest_metadata(input, "EVENT_DIGEST");
    auto expected_envelope_digest = read_digest_metadata(
      input, "ENVELOPE_DIGEST");
    if (
      !harness_value || !harness_version || !master_seed || !random_algorithm
      || !coordinate_schema || !ordering || !trace_schema || !event_schema
      || !event_epoch || !outcome_code || !outcome_operation
      || !configuration_digest || !input_digest || !terminal_digest
      || !expected_trace_digest || !expected_event_digest
      || !expected_envelope_digest) {
        return reproduction_failure<fuzz_reproduction>(errc::malformed_data);
    }
    if (
      !known_harness(static_cast<fuzz_harness>(*harness_value))
      || *harness_version != fuzz_harness_version
      || !known_error(static_cast<errc>(*outcome_code))
      || !known_operation(
        static_cast<runtime::operation_kind>(*outcome_operation))
      || *random_algorithm != deterministic_random_algorithm_version
      || *coordinate_schema != deterministic_random_coordinate_version
      || *ordering != scheduler_ordering_version
      || *trace_schema != event_trace_schema_version
      || *event_schema != observability::event_schema_version) {
        return reproduction_failure<fuzz_reproduction>(errc::malformed_data);
    }
    if (
      *expected_envelope_digest
      != envelope_digest(
        static_cast<fuzz_harness>(*harness_value),
        *harness_version,
        *master_seed,
        *event_epoch,
        fuzz_case_outcome{
          .code = static_cast<errc>(*outcome_code),
          .operation = static_cast<runtime::operation_kind>(*outcome_operation),
        },
        *configuration_digest,
        *input_digest,
        *terminal_digest,
        *expected_trace_digest,
        *expected_event_digest)) {
        return reproduction_failure<fuzz_reproduction>(errc::malformed_data);
    }

    std::vector<std::uint8_t> configuration;
    configuration.reserve(fuzz_input_bytes_max);
    if (
      auto read = read_section(
        input,
        "CONFIGURATION",
        fuzz_input_bytes_max,
        [&](std::span<const std::uint8_t> bytes) -> runtime::result<void> {
            configuration.insert(
              configuration.end(), bytes.begin(), bytes.end());
            return {};
        });
      !read) {
        return runtime::failure(read.error());
    }

    std::vector<std::uint8_t> fuzz_input;
    fuzz_input.reserve(fuzz_input_bytes_max);
    if (
      auto read = read_section(
        input,
        "INPUT",
        fuzz_input_bytes_max,
        [&](std::span<const std::uint8_t> bytes) -> runtime::result<void> {
            fuzz_input.insert(fuzz_input.end(), bytes.begin(), bytes.end());
            return {};
        });
      !read) {
        return runtime::failure(read.error());
    }

    trace_artifact trace;
    if (
      auto read = read_section(
        input,
        "TRACE",
        fuzz_trace_budget(static_cast<fuzz_harness>(*harness_value))
          .encoded_bytes,
        [&](std::span<const std::uint8_t> bytes) -> runtime::result<void> {
            trace.append(
              std::string_view{
                reinterpret_cast<const char*>(bytes.data()), bytes.size()});
            return {};
        });
      !read) {
        return runtime::failure(read.error());
    }

    observability::event_log_artifact events;
    if (
      auto read = read_section(
        input,
        "EVENTS",
        fuzz_artifact_bytes_max,
        [&](std::span<const std::uint8_t> bytes) {
            return events.append(bytes);
        });
      !read) {
        return runtime::failure(read.error());
    }

    auto end = read_line(input);
    if (!end || *end != "END KQREPRO") {
        return reproduction_failure<fuzz_reproduction>(errc::malformed_data);
    }
    if (input.peek() != std::char_traits<char>::eof()) {
        return reproduction_failure<fuzz_reproduction>(errc::malformed_data);
    }
    if (
      digest_trace(trace) != *expected_trace_digest
      || digest_events(events) != *expected_event_digest) {
        return reproduction_failure<fuzz_reproduction>(errc::malformed_data);
    }

    return fuzz_reproduction::make(
      static_cast<fuzz_harness>(*harness_value),
      *harness_version,
      *master_seed,
      *event_epoch,
      std::move(configuration),
      std::move(fuzz_input),
      *configuration_digest,
      *input_digest,
      *terminal_digest,
      fuzz_case_outcome{
        .code = static_cast<errc>(*outcome_code),
        .operation = static_cast<runtime::operation_kind>(*outcome_operation),
      },
      std::move(trace),
      std::move(events));
}

} // namespace kwaque::simulation::testing
