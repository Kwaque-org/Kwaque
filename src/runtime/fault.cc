#include "src/runtime/fault.h"

#include <algorithm>
#include <limits>

namespace kwaque::runtime {

namespace {

operation_error fault_error(errc code) noexcept {
    return operation_error{code, operation_kind::fault};
}

} // namespace

result<fault_point_id> fault_point_id::make(std::uint32_t value) noexcept {
    if (value == 0) {
        return failure(fault_error(errc::invalid_argument));
    }
    return fault_point_id{value};
}

result<fault_occurrence> fault_occurrence::make(std::uint64_t value) noexcept {
    if (value == 0) {
        return failure(fault_error(errc::invalid_argument));
    }
    return fault_occurrence{value};
}

result<fault_object_key>
fault_object_key::from_bytes(std::span<const std::byte> bytes) noexcept {
    if (bytes.size() > maximum_fault_object_key_bytes) {
        return failure(fault_error(errc::out_of_range));
    }
    fault_object_key key;
    std::copy(bytes.begin(), bytes.end(), key.storage_.begin());
    key.size_ = static_cast<std::uint8_t>(bytes.size());
    return key;
}

result<void> validate_fault_decision(
  const fault_request& request, fault_decision decision) noexcept {
    const auto descriptor = validate_fault_request(request);
    if (!descriptor) {
        return failure(descriptor.error());
    }
    if (!(**descriptor).permitted_actions.contains(decision.action())) {
        return failure(fault_error(errc::invalid_argument));
    }
    if (
      (decision.action() == fault_action::delay
       && decision.delay()->nanoseconds() == 0)
      || (decision.action() == fault_action::short_operation
          && decision.short_operation_bytes()->value() == 0)) {
        return failure(fault_error(errc::invalid_argument));
    }
    return {};
}

result<const fault_point_descriptor*>
validate_fault_request(const fault_request& request) noexcept {
    const auto* descriptor = find_builtin_fault_point(request.point);
    if (descriptor == nullptr) {
        return failure(fault_error(errc::invalid_argument));
    }
    return descriptor;
}

result<void>
validate_unique_fault_points(std::span<const fault_point_id> points) noexcept {
    if (points.size() > maximum_fault_points) {
        return failure(fault_error(errc::out_of_range));
    }
    for (std::size_t left = 0; left < points.size(); ++left) {
        for (std::size_t right = left + 1; right < points.size(); ++right) {
            if (points[left] == points[right]) {
                return failure(fault_error(errc::invalid_argument));
            }
        }
    }
    return {};
}

} // namespace kwaque::runtime
