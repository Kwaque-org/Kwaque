#pragma once

#include "src/base/error.h"
#include "src/base/result.h"

#include <cstdint>
#include <limits>

namespace kwaque::model {

// These independent axes are categorical values, not numeric transition order.
// Zero is uninitialized staging and cannot be published as a valid state.
enum class append_state : std::uint8_t {
    uninitialized = 0,
    creating = 1,
    active = 2,
    sealing = 3,
    sealed = 4,
};

enum class retention_state : std::uint8_t {
    uninitialized = 0,
    retained = 1,
    expiring = 2,
    deleted = 3,
};

enum class remote_state : std::uint8_t {
    uninitialized = 0,
    not_offloaded = 1,
    offloading = 2,
    offloaded = 3,
    remote_delete_pending = 4,
    remote_deleted = 5,
};

// One replica's state does not describe the entire replica set.
enum class replica_state : std::uint8_t {
    uninitialized = 0,
    assigned = 1,
    copying = 2,
    readable = 3,
    lost = 4,
    removing = 5,
    removed = 6,
};

[[nodiscard]] constexpr bool is_valid(append_state state) noexcept {
    switch (state) {
    case append_state::creating:
    case append_state::active:
    case append_state::sealing:
    case append_state::sealed:
        return true;
    case append_state::uninitialized:
        return false;
    }
    return false;
}

[[nodiscard]] constexpr bool is_valid(retention_state state) noexcept {
    switch (state) {
    case retention_state::retained:
    case retention_state::expiring:
    case retention_state::deleted:
        return true;
    case retention_state::uninitialized:
        return false;
    }
    return false;
}

[[nodiscard]] constexpr bool is_valid(remote_state state) noexcept {
    switch (state) {
    case remote_state::not_offloaded:
    case remote_state::offloading:
    case remote_state::offloaded:
    case remote_state::remote_delete_pending:
    case remote_state::remote_deleted:
        return true;
    case remote_state::uninitialized:
        return false;
    }
    return false;
}

[[nodiscard]] constexpr bool is_valid(replica_state state) noexcept {
    switch (state) {
    case replica_state::assigned:
    case replica_state::copying:
    case replica_state::readable:
    case replica_state::lost:
    case replica_state::removing:
    case replica_state::removed:
        return true;
    case replica_state::uninitialized:
        return false;
    }
    return false;
}

namespace detail {

template<typename State>
[[nodiscard]] inline result<State> state_from_raw(std::uint64_t raw) noexcept {
    if (raw > std::numeric_limits<std::uint8_t>::max()) {
        return failure(errc::invalid_argument);
    }
    const auto state = static_cast<State>(raw);
    if (!is_valid(state)) {
        return failure(errc::invalid_argument);
    }
    return state;
}

} // namespace detail

[[nodiscard]] inline result<append_state>
append_state_from_raw(std::uint64_t raw) noexcept {
    return detail::state_from_raw<append_state>(raw);
}

[[nodiscard]] inline result<retention_state>
retention_state_from_raw(std::uint64_t raw) noexcept {
    return detail::state_from_raw<retention_state>(raw);
}

[[nodiscard]] inline result<remote_state>
remote_state_from_raw(std::uint64_t raw) noexcept {
    return detail::state_from_raw<remote_state>(raw);
}

[[nodiscard]] inline result<replica_state>
replica_state_from_raw(std::uint64_t raw) noexcept {
    return detail::state_from_raw<replica_state>(raw);
}

// An observation supplied by the metadata owner. Unknown is a valid
// observation, but neither unknown nor incomplete establishes completed work.
// These values are validation inputs, not a persisted status code or an
// authority token.
enum class completion_state : std::uint8_t {
    unknown = 0,
    incomplete = 1,
    complete = 2,
};

[[nodiscard]] constexpr bool is_valid(completion_state state) noexcept {
    switch (state) {
    case completion_state::unknown:
    case completion_state::incomplete:
    case completion_state::complete:
        return true;
    }
    return false;
}

// All facts must describe the same segment/generation/object context as the
// supplied axes. Cleanup covers the full tracked obligation, including partial
// uploads; one lost or removed replica is not aggregate cleanup evidence.
struct segment_completion_facts final {
    // Historical verified data/index upload and committed remote manifest.
    completion_state offload_commit{completion_state::unknown};
    completion_state expiration_commit{completion_state::unknown};
    completion_state remote_cleanup{completion_state::unknown};
    completion_state local_cleanup{completion_state::unknown};

    bool operator==(const segment_completion_facts&) const noexcept = default;
};

// Checks current-state assertions and the required supplied completion facts.
// Success does not authenticate evidence, prove transition history, or
// authorize a proposed commit, deletion or read. The owner verifies context and
// authority.
[[nodiscard]] inline result<void> validate_segment_state(
  append_state append,
  retention_state retention,
  remote_state remote,
  replica_state replica,
  segment_completion_facts facts) noexcept {
    if (
      !is_valid(append) || !is_valid(retention) || !is_valid(remote)
      || !is_valid(replica) || !is_valid(facts.offload_commit)
      || !is_valid(facts.expiration_commit) || !is_valid(facts.remote_cleanup)
      || !is_valid(facts.local_cleanup)) {
        return failure(errc::invalid_argument);
    }

    if (
      remote != remote_state::not_offloaded && append != append_state::sealed) {
        return failure(errc::invalid_argument);
    }
    if (
      (remote == remote_state::offloaded
       || remote == remote_state::remote_delete_pending
       || remote == remote_state::remote_deleted)
      && facts.offload_commit != completion_state::complete) {
        return failure(errc::invalid_argument);
    }
    if (
      (retention == retention_state::expiring
       || retention == retention_state::deleted)
      && facts.expiration_commit != completion_state::complete) {
        return failure(errc::invalid_argument);
    }
    if (
      remote == remote_state::remote_deleted
      && facts.remote_cleanup != completion_state::complete) {
        return failure(errc::invalid_argument);
    }
    if (retention == retention_state::deleted) {
        if (
          facts.remote_cleanup != completion_state::complete
          || facts.local_cleanup != completion_state::complete) {
            return failure(errc::invalid_argument);
        }
        if (
          remote == remote_state::offloaded
          || replica == replica_state::readable) {
            return failure(errc::invalid_argument);
        }
    }
    return {};
}

} // namespace kwaque::model
