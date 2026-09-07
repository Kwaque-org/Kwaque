#ifndef KWAQUE_SRC_SIMULATION_TESTS_FUZZ_REPRODUCTION_H_
#define KWAQUE_SRC_SIMULATION_TESTS_FUZZ_REPRODUCTION_H_

#include "src/observability/event_log.h"
#include "src/runtime/error.h"
#include "src/simulation/event_trace.h"

#include <seastar/core/future.hh>

#include <array>
#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <span>
#include <vector>

namespace kwaque::simulation::testing {

inline constexpr std::uint32_t fuzz_reproduction_format_version{1};
inline constexpr std::uint32_t fuzz_harness_version{2};
inline constexpr std::size_t fuzz_input_bytes_max{16U * 1'024U};
inline constexpr std::uint32_t fuzz_commands_max{512};
inline constexpr std::uint32_t fuzz_trace_entries_max{512};
inline constexpr std::uint64_t fuzz_artifact_bytes_max{128U * 1'024U};
inline constexpr std::uint32_t fuzz_file_trace_entries_max{4'096};
inline constexpr std::uint32_t fuzz_network_trace_entries_max{16'384};
inline constexpr std::uint64_t fuzz_trace_artifact_bytes_max{
  4U * 1'024U * 1'024U};
inline constexpr std::uint32_t fuzz_event_entries_max{128};
inline constexpr std::size_t fuzz_output_line_bytes_max{4U * 1'024U};
inline constexpr std::uint32_t fuzz_scheduler_pending_max{256};
inline constexpr std::uint64_t fuzz_scheduler_total_max{4'096};
inline constexpr std::uint64_t fuzz_scheduler_batch_max{64};
inline constexpr std::uint64_t fuzz_scheduler_deadline_max{65'535};
inline constexpr std::uint32_t fuzz_fault_rules_max{128};
inline constexpr std::uint32_t fuzz_fault_evaluations_max{512};
inline constexpr std::uint64_t fuzz_event_epoch_base{41};

enum class fuzz_harness : std::uint8_t {
    scheduler = 1,
    fault_schedule = 2,
    fake_file = 3,
    fake_network = 4,
    semantic_canary = 5,
};

[[nodiscard]] trace_limit_values
fuzz_trace_budget(fuzz_harness harness) noexcept;
[[nodiscard]] trace_scheduler_budget
fuzz_scheduler_budget(fuzz_harness harness) noexcept;
// Called in a Seastar thread; large histories retain the existing cooperative
// codec and bounded chunks instead of flattening a complete artifact.
[[nodiscard]] seastar::future<runtime::result<decoded_event_trace>>
decode_fuzz_trace(const trace_artifact& artifact, fuzz_harness harness);

struct fuzz_case_outcome final {
    errc code{errc::success};
    runtime::operation_kind operation{runtime::operation_kind::generic};

    bool operator==(const fuzz_case_outcome&) const = default;
};

using fuzz_digest = std::array<std::uint8_t, 32>;

[[nodiscard]] std::uint64_t
fuzz_master_seed(std::span<const std::uint8_t> input) noexcept;
[[nodiscard]] std::vector<std::uint8_t>
fuzz_configuration(fuzz_harness harness);
[[nodiscard]] std::uint64_t fuzz_event_epoch(fuzz_harness harness) noexcept;

class fuzz_reproduction final {
public:
    [[nodiscard]] static runtime::result<fuzz_reproduction> make(
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
      observability::event_log_artifact events);

    fuzz_reproduction(const fuzz_reproduction&) = delete;
    fuzz_reproduction& operator=(const fuzz_reproduction&) = delete;
    fuzz_reproduction(fuzz_reproduction&&) noexcept = default;
    fuzz_reproduction& operator=(fuzz_reproduction&&) noexcept = default;

    [[nodiscard]] fuzz_harness harness() const noexcept { return harness_; }
    [[nodiscard]] std::uint32_t harness_version() const noexcept {
        return harness_version_;
    }
    [[nodiscard]] std::uint64_t master_seed() const noexcept {
        return master_seed_;
    }
    [[nodiscard]] std::uint64_t event_epoch() const noexcept {
        return event_epoch_;
    }
    [[nodiscard]] std::span<const std::uint8_t> configuration() const noexcept {
        return configuration_;
    }
    [[nodiscard]] std::span<const std::uint8_t> input() const noexcept {
        return input_;
    }
    [[nodiscard]] const fuzz_digest& configuration_digest() const noexcept {
        return configuration_digest_;
    }
    [[nodiscard]] const fuzz_digest& input_digest() const noexcept {
        return input_digest_;
    }
    [[nodiscard]] const fuzz_digest& terminal_digest() const noexcept {
        return terminal_digest_;
    }
    [[nodiscard]] fuzz_case_outcome outcome() const noexcept {
        return outcome_;
    }
    [[nodiscard]] const trace_artifact& trace() const noexcept {
        return trace_;
    }
    [[nodiscard]] const observability::event_log_artifact&
    events() const noexcept {
        return events_;
    }

private:
    fuzz_reproduction(
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
      observability::event_log_artifact events) noexcept;

    fuzz_harness harness_;
    std::uint32_t harness_version_;
    std::uint64_t master_seed_;
    std::uint64_t event_epoch_;
    std::vector<std::uint8_t> configuration_;
    std::vector<std::uint8_t> input_;
    fuzz_digest configuration_digest_;
    fuzz_digest input_digest_;
    fuzz_digest terminal_digest_;
    fuzz_case_outcome outcome_;
    trace_artifact trace_;
    observability::event_log_artifact events_;
};

[[nodiscard]] fuzz_digest digest_bytes(std::span<const std::uint8_t> bytes);
[[nodiscard]] fuzz_digest digest_trace(const trace_artifact& trace);
[[nodiscard]] fuzz_digest
digest_events(const observability::event_log_artifact& events);

[[nodiscard]] runtime::result<void>
write_fuzz_reproduction(std::ostream& output, const fuzz_reproduction& value);
[[nodiscard]] runtime::result<fuzz_reproduction>
read_fuzz_reproduction(std::istream& input);

} // namespace kwaque::simulation::testing

#endif // KWAQUE_SRC_SIMULATION_TESTS_FUZZ_REPRODUCTION_H_
