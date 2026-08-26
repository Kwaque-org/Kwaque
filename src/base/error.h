#pragma once

#include <system_error>
#include <type_traits>

namespace kwaque {

enum class errc {
    success = 0,
    invalid_argument = 1,
    out_of_range = 2,
    malformed_data = 3,
    unavailable = 4,
    aborted = 5,
    closed = 6,
    timed_out = 7,
    resource_exhausted = 8,
    queue_full = 9,
    wrong_shard = 10,
    io_failure = 11,
    network_failure = 12,
    dns_failure = 13,
    fault_injected = 14,
    replay_divergence = 15,
    invariant_violation = 16,
    truncated_data = 17,
};

[[nodiscard]] const std::error_category& error_category() noexcept;
[[nodiscard]] std::error_code make_error_code(errc error) noexcept;

} // namespace kwaque

template<>
struct std::is_error_code_enum<kwaque::errc> : std::true_type {};
