#include "src/protocol/control_memory.h"

#include "proto/kwaque/common/v1/build_info.pb.h"
#include "proto/kwaque/common/v1/capability.pb.h"
#include "proto/kwaque/common/v1/error.pb.h"
#include "proto/kwaque/common/v1/identity.pb.h"
#include "proto/kwaque/control/v1/handshake.pb.h"
#include "proto/kwaque/control/v1/redirect.pb.h"
#include "src/base/allocation.h"

#include <google/protobuf/repeated_field.h>
#include <google/protobuf/repeated_ptr_field.h>
#include <google/protobuf/unknown_field_set.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>

namespace kwaque::protocol::detail {
namespace {
namespace common = kwaque::common::v1;
namespace control = kwaque::control::v1;
using google::protobuf::UnknownField;
using google::protobuf::UnknownFieldSet;
using unknown_array = google::protobuf::RepeatedField<UnknownField>;
using scalar_array = google::protobuf::RepeatedField<std::uint32_t>;
using pointer_array
  = google::protobuf::RepeatedPtrField<common::FormatCapability>;

constexpr std::uint64_t string_slack = 2U * alignof(std::max_align_t);
// The native metadata container adds one Arena* to an UnknownFieldSet. Two
// pointer widths cover that pointer and padding on the supported ABI.
static_assert(alignof(UnknownFieldSet) == alignof(void*));
constexpr std::uint64_t unknown_container_bytes = sizeof(UnknownFieldSet)
                                                  + 2U * sizeof(void*);

std::uint64_t message_bytes(control_message message) noexcept {
    switch (message) {
    case control_message::build_info:
        return sizeof(common::BuildInfo);
    case control_message::routing_context:
        return sizeof(common::RoutingContext);
    case control_message::format_capability:
        return sizeof(common::FormatCapability);
    case control_message::capabilities:
        return sizeof(common::Capabilities);
    case control_message::error:
        return sizeof(common::Error);
    case control_message::handshake_request:
        return sizeof(control::HandshakeRequest);
    case control_message::handshake_response:
        return sizeof(control::HandshakeResponse);
    case control_message::endpoint:
        return sizeof(control::Endpoint);
    case control_message::redirect:
        return sizeof(control::Redirect);
    }
    return 0;
}

// A native repeated array doubles bytes including its header. When growing
// past capacity<n, its new request is <=2*n*element+2*header. The complete
// public container sizeof is a conservative header bound for these three
// concrete native array types. AllocateAtLeast returns the requested capacity.
constexpr std::uint64_t array_request(
  std::uint64_t count,
  std::uint64_t element,
  std::uint64_t container) noexcept {
    return count == 0 ? 0 : 2U * count * element + 2U * container;
}

class footprint final {
public:
    footprint(
      const codec::limits& policy,
      bytes::allocation_charge_fn charge,
      codec::field_context context) noexcept
      : policy_(policy)
      , charge_(charge)
      , context_(context) {}

    std::uint64_t measure(std::uint64_t request, bool live = true) noexcept {
        if (request == 0 || failure) return 0;
        const auto served = charge_(byte_count{request});
        if (served.value() < request) {
            fail(errc::invalid_argument);
            return 0;
        }
        // Bucket probes establish a bound, but are not themselves allocations
        // owned by this operation and do not spend its narrower allocation cap.
        const auto ceiling = live
                               ? policy_.config().max_allocation_bytes.value()
                               : maximum_contiguous_allocation_bytes;
        if (served.value() > ceiling) {
            fail(errc::resource_exhausted);
            return 0;
        }
        if (live)
            cost.largest_allocation = std::max(cost.largest_allocation, served);
        return served.value();
    }

    bool add(std::uint64_t bytes, std::uint64_t count = 1) noexcept {
        if (failure) return false;
        const auto left = policy_.config().max_metadata_bytes.value()
                          - cost.generated_peak.value();
        if (count != 0 && bytes > left / count)
            return fail(errc::resource_exhausted);
        cost.generated_peak = byte_count{
          cost.generated_peak.value() + bytes * count};
        return true;
    }

    bool allocation(std::uint64_t request, std::uint64_t count = 1) noexcept {
        if (count == 0) return true;
        const auto served = measure(request);
        return add(served, count);
    }

    bool fail(errc code) noexcept {
        if (!failure)
            failure.emplace(
              code, context_.family, context_.field, context_.origin);
        return false;
    }

    control_parse_cost cost;
    std::optional<codec::error> failure;

private:
    const codec::limits& policy_;
    bytes::allocation_charge_fn charge_;
    codec::field_context context_;
};

bool message_graph(
  const control_schema& schema,
  std::uint64_t copies,
  std::uint64_t depth,
  std::uint64_t units,
  const codec::limits_config& limits,
  footprint& bound,
  std::uint64_t& objects) noexcept {
    objects += copies;
    if (!bound.allocation(message_bytes(schema.message), copies)) return false;
    for (const auto& field : schema.fields) {
        const auto count = field.repeated
                             ? std::min(
                                 {field.maximum_elements.value(),
                                  limits.max_control_repeated.value(),
                                  units})
                             : std::uint64_t{1};
        if (field.repeated) {
            const bool messages = field.kind == control_field_kind::message;
            const auto request = array_request(
              count,
              messages ? sizeof(void*) : sizeof(std::uint32_t),
              messages ? sizeof(pointer_array) : sizeof(scalar_array));
            // Both old/new allocations may coexist during growth. Reserving
            // two maxima per field is conservative over that migration peak.
            if (!bound.allocation(request, 2U * copies)) return false;
        }
        if (
          field.kind == control_field_kind::message && count != 0
          && depth < limits.max_nesting_depth.value()
          && !message_graph(
            *schema_for(field.child),
            copies * count,
            depth + 1U,
            units,
            limits,
            bound,
            objects))
            return false;
    }
    return true;
}
} // namespace

codec::result<control_parse_cost> bound_control_parse(
  control_message message,
  byte_count wire_bytes,
  const codec::limits& policy,
  bytes::allocation_charge_fn charge,
  codec::field_context context) noexcept {
    const auto error = [&](errc code) {
        return codec::failure(
          codec::error{code, context.family, context.field, context.origin});
    };
    const auto* schema = schema_for(message);
    if (
      schema == nullptr || charge == nullptr
      || wire_bytes.value()
           > std::numeric_limits<std::uint64_t>::max() - context.origin)
        return error(errc::invalid_argument);
    const auto limits = policy.config();
    if (
      wire_bytes
      > std::min(schema->maximum_wire_bytes, limits.max_control_bytes))
        return error(errc::resource_exhausted);

    footprint bound{policy, charge, context};
    // Every tag/packed element occupies at least one wire byte. String objects
    // and unknown entries are each associated with a tag, so this bounds both
    // even when all input is empty or tiny unknown fields.
    const auto units = std::min(
      limits.max_control_fields.value(), wire_bytes.value());
    std::uint64_t objects = 0;
    if (!message_graph(*schema, 1, 1, units, limits, bound, objects))
        return codec::failure(*bound.failure);
    if (units != 0) {
        if (
          !bound.allocation(sizeof(std::string), units)
          || !bound.allocation(unknown_container_bytes, objects))
            return codec::failure(*bound.failure);

        const auto text_limit
          = std::min(limits.max_control_field_bytes, wire_bytes).value();
        const auto text_request = 2U * text_limit + string_slack;
        const auto unknown_request = array_request(
          units, sizeof(UnknownField), sizeof(unknown_array));
        const auto maximum_request = std::max(text_request, unknown_request);
        // Monotonicity plus the upper endpoint of each dyadic bucket proves
        // charge(n)<=slope*n+small for every request through maximum_request.
        // The slope is derived from the supplied capacity profile, not assumed
        // from payload length or hard-coded to a particular allocator.
        const auto small = bound.measure(32, false);
        std::uint64_t slope = 0;
        for (std::uint64_t upper = 64;; upper *= 2U) {
            const auto served = bound.measure(upper, false);
            const auto lower = upper / 2U + 1U;
            slope = std::max(
              slope, served / lower + (served % lower != 0 ? 1U : 0U));
            if (upper >= maximum_request || bound.failure) break;
        }
        if (bound.failure) return codec::failure(*bound.failure);
        // Fresh strings grow to at most twice their largest observed length
        // plus terminator/alignment slack. Sum of those lengths cannot exceed
        // all wire bytes, including overwritten BuildInfo fields. Add one old
        // character buffer for the currently executing native mutation.
        const auto character_requests = 2U * wire_bytes.value()
                                        + string_slack * units;
        if (
          !bound.add(character_requests, slope) || !bound.add(small, units)
          || !bound.allocation(text_request))
            return codec::failure(*bound.failure);

        // Unknown entries are distributed over at most one set per generated
        // object. Sum their requests before applying the affine charge bound;
        // charging a maximum-size set per object would multiply the same tags.
        const auto unknown_requests = 2U * units * sizeof(UnknownField)
                                      + 2U * sizeof(unknown_array) * objects;
        if (
          !bound.add(unknown_requests, 2U * slope)
          || !bound.add(small, 2U * objects))
            return codec::failure(*bound.failure);
        static_cast<void>(bound.measure(unknown_request));
    }
    bound.cost.input_copy = byte_count{bound.measure(wire_bytes.value())};
    if (bound.failure) return codec::failure(*bound.failure);
    if (bound.cost.input_copy > limits.max_retained_bytes)
        return error(errc::resource_exhausted);
    const auto total = bound.cost.generated_peak.checked_add(
      bound.cost.input_copy);
    if (
      !total || *total > codec::limits::defaults().config().max_metadata_bytes
      || *total > limits.max_operation_bytes)
        return error(errc::resource_exhausted);
    return bound.cost;
}
} // namespace kwaque::protocol::detail
