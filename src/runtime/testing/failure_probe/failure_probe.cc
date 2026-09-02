#include "src/runtime/testing/failure_probe/failure_probe.h"

namespace kwaque::runtime::testing {

operation_error failure_probe::probe_error(errc code) noexcept {
    return operation_error{code, operation_kind::fault};
}

result<failure_probe::occurrence_candidate> failure_probe::prepare(
  builtin_fault_point point, fault_object_key object) const noexcept {
    const auto index = failure_probe_point_index(point);
    const auto* descriptor = descriptor_for(point);
    if (!index || descriptor == nullptr) {
        return failure(probe_error(errc::invalid_argument));
    }
    const auto previous = occurrences_[*index];
    if (previous == std::numeric_limits<std::uint64_t>::max()) {
        return failure(probe_error(errc::out_of_range));
    }
    const auto occurrence = fault_occurrence::make(previous + 1U);
    if (!occurrence) {
        return failure(occurrence.error());
    }
    return occurrence_candidate{
      .request = fault_request{
        .point = descriptor->id,
        .occurrence = *occurrence,
        .object = object,
      },
      .index = *index,
      .previous = previous,
    };
}

result<void>
failure_probe::commit(const occurrence_candidate& candidate) noexcept {
    if (
      candidate.index >= occurrences_.size()
      || occurrences_[candidate.index] != candidate.previous) {
        return failure(probe_error(errc::invariant_violation));
    }
    ++occurrences_[candidate.index];
    return {};
}

result<std::uint64_t>
failure_probe::occurrences(builtin_fault_point point) const noexcept {
    assert_current();
    const auto index = failure_probe_point_index(point);
    if (!index) {
        return failure(probe_error(errc::invalid_argument));
    }
    return occurrences_[*index];
}

} // namespace kwaque::runtime::testing
