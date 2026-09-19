#include "src/protocol/control_codec.h"

#include "proto/kwaque/common/v1/build_info.pb.h"
#include "proto/kwaque/common/v1/capability.pb.h"
#include "proto/kwaque/common/v1/error.pb.h"
#include "proto/kwaque/common/v1/identity.pb.h"
#include "proto/kwaque/control/v1/handshake.pb.h"
#include "proto/kwaque/control/v1/redirect.pb.h"
#include "src/base/invariant.h"
#include "src/protocol/control_internal.h"
#include "src/protocol/control_memory.h"
#include "src/protocol/control_native.h"
#include "src/protocol/control_preflight.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/temporary_buffer.hh>

#include <google/protobuf/io/coded_stream.h>

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <memory>
#include <optional>
#include <span>
#include <type_traits>
#include <utility>

namespace kwaque::protocol {
namespace {
namespace common = kwaque::common::v1;
namespace native = kwaque::control::v1;
// Both conversion directions preserve these ordinals without a lookup table.
// Keep generated declarations out of the owning model while checking the
// correspondence at the native boundary.
static_assert(static_cast<int>(peer_kind::broker) == native::PEER_KIND_BROKER);
static_assert(static_cast<int>(peer_kind::client) == native::PEER_KIND_CLIENT);
static_assert(
  static_cast<int>(control_error_code::invalid_request)
  == common::ERROR_CODE_INVALID_REQUEST);
static_assert(
  static_cast<int>(control_error_code::unsupported)
  == common::ERROR_CODE_UNSUPPORTED);
static_assert(
  static_cast<int>(control_error_code::stale_routing)
  == common::ERROR_CODE_STALE_ROUTING);
static_assert(
  static_cast<int>(control_error_code::stale_segment)
  == common::ERROR_CODE_STALE_SEGMENT);
static_assert(
  static_cast<int>(control_error_code::unavailable)
  == common::ERROR_CODE_UNAVAILABLE);
static_assert(
  static_cast<int>(control_error_code::resource_limit)
  == common::ERROR_CODE_RESOURCE_LIMIT);
static_assert(
  static_cast<int>(control_error_code::timed_out)
  == common::ERROR_CODE_TIMED_OUT);
static_assert(
  static_cast<int>(control_error_code::aborted) == common::ERROR_CODE_ABORTED);
static_assert(
  static_cast<int>(control_error_code::not_leader)
  == common::ERROR_CODE_NOT_LEADER);
static_assert(
  codec::detail::owning_decode_result<codec::result<decoded_control>>::value);

codec::error error(errc code, codec::field_context context) noexcept {
    return codec::error{code, context.family, context.field, context.origin};
}

template<typename Id>
Id identity(const std::string& value) {
    const auto id = Id::make(
      std::span<const std::uint8_t>{
        reinterpret_cast<const std::uint8_t*>(value.data()), value.size()});
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CONTROL-IDENTITY"},
      id.has_value(),
      "preflight accepted an invalid control identity");
    return *id;
}
routing_context routing(const common::RoutingContext& value) {
    const auto epoch = model::range_routing_epoch::make(value.routing_epoch());
    const auto generation = model::segment_generation::make(
      value.segment_generation());
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CONTROL-EPOCH"},
      epoch && generation,
      "preflight accepted an invalid control epoch");
    return {
      identity<model::topic_id>(value.topic_id()),
      identity<model::range_id>(value.range_id()),
      *epoch,
      identity<model::segment_id>(value.segment_id()),
      *generation};
}

// All dynamic storage in the owning result is admitted while the generated
// state and private input copy are still live. Fresh strings reserve once:
// 2*n+alignment slack bounds the native library's capacity including NUL.
// Vectors reserve their final count before insertion; no old array overlaps.
class conversion_cost final {
public:
    conversion_cost(
      codec::decode_budget memory,
      codec::limits limits,
      codec::field_context context)
      : memory_(memory)
      , limits_(limits)
      , context_(context) {}

    void add(std::size_t request) {
        if (request == 0 || failed) return;
        const auto served = memory_.charge(byte_count{request});
        if (served.value() < request) {
            failed = error(errc::invalid_argument, context_);
            return;
        }
        if (!limits_.validate_allocation(served)) {
            failed = error(errc::resource_exhausted, context_);
            return;
        }
        const auto total = bytes.checked_add(served);
        if (!total || *total > limits_.config().max_metadata_bytes) {
            failed = error(errc::resource_exhausted, context_);
            return;
        }
        bytes = *total;
    }
    void text(const std::string& value) {
        if (!value.empty())
            add(2U * value.size() + 2U * alignof(std::max_align_t));
    }
    void build(const common::BuildInfo& value) {
        text(value.version());
        text(value.revision());
        text(value.build_mode());
    }
    void capabilities(const common::Capabilities& value) {
        add(
          static_cast<std::size_t>(value.protocol_versions_size())
          * sizeof(std::uint16_t));
        add(
          static_cast<std::size_t>(value.formats_size())
          * sizeof(format_capability));
        add(
          static_cast<std::size_t>(value.compression_codecs_size())
          * sizeof(std::uint8_t));
    }
    byte_count bytes;
    std::optional<codec::error> failed;

private:
    codec::decode_budget memory_;
    codec::limits limits_;
    codec::field_context context_;
};

template<typename Message>
void measure_conversion(conversion_cost& cost, const Message& value) {
    if constexpr (
      std::same_as<Message, native::HandshakeRequest>
      || std::same_as<Message, native::HandshakeResponse>) {
        cost.build(value.build());
        cost.capabilities(value.capabilities());
    } else if constexpr (std::same_as<Message, native::Redirect>) {
        cost.text(value.endpoint().host());
    } else {
        if (value.has_reason()) cost.text(value.reason());
    }
}

template<typename Message>
codec::result<void> validate_context(
  const Message& value,
  const control_expectation& expected,
  codec::field_context context) {
    const auto wrong = [&] {
        return codec::failure(error(errc::wrong_context, context));
    };
    if constexpr (std::same_as<Message, native::HandshakeRequest>) {
        const bool broker = value.peer_kind() == native::PEER_KIND_BROKER;
        if (broker != value.has_broker_id())
            return codec::failure(error(errc::malformed_data, context));
        if (
          expected.cluster && value.has_expected_cluster_id()
          && *expected.cluster
               != identity<model::cluster_id>(value.expected_cluster_id()))
            return wrong();
        if (expected.broker && (!value.has_broker_id()
            || *expected.broker != identity<model::broker_id>(value.broker_id())))
            return wrong();
    } else if constexpr (std::same_as<Message, native::HandshakeResponse>) {
        if ((expected.cluster && *expected.cluster != identity<model::cluster_id>(value.cluster_id()))
            || (expected.broker && *expected.broker != identity<model::broker_id>(value.broker_id())))
            return wrong();
    } else if constexpr (std::same_as<Message, native::Redirect>) {
        if ((expected.broker && *expected.broker != identity<model::broker_id>(value.broker_id()))
            || (expected.topic && *expected.topic != identity<model::topic_id>(value.destination().topic_id())))
            return wrong();
    } else {
        if (
          (value.code() == common::ERROR_CODE_STALE_ROUTING
           || value.code() == common::ERROR_CODE_STALE_SEGMENT)
          && !value.has_observed())
            return codec::failure(error(errc::malformed_data, context));
        if (expected.topic && (!value.has_observed()
            || *expected.topic != identity<model::topic_id>(value.observed().topic_id())))
            return wrong();
    }
    return {};
}

class converter final {
public:
    converter(codec::cooperative_work& work, codec::error anchor)
      : work_(work)
      , anchor_(anchor) {}

    seastar::future<codec::result<void>>
    text(std::string& output, const std::string& input) {
        // resize initializes the destination. It is a bounded string leaf;
        // copying below remains split into the caller's shared quanta.
        if (
          auto ready = co_await work_.admit(
            byte_count{input.size()}, item_count{1}, anchor_);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work_.poll(anchor_); !ready)
            co_return codec::failure(ready.error());
        output.reserve(input.size());
        output.resize(input.size());
        for (std::size_t offset = 0; offset < input.size();) {
            const auto count = std::min<std::size_t>(
              input.size() - offset, work_.byte_quantum().value());
            if (
              auto ready = co_await work_.admit(
                byte_count{count}, item_count{1}, anchor_);
              !ready)
                co_return codec::failure(ready.error());
            if (auto ready = work_.poll(anchor_); !ready)
                co_return codec::failure(ready.error());
            std::memcpy(output.data() + offset, input.data() + offset, count);
            offset += count;
        }
        co_return work_.poll(anchor_);
    }

    seastar::future<codec::result<void>>
    build(build_information& output, const common::BuildInfo& input) {
        auto ready = co_await text(output.version, input.version());
        if (!ready) co_return ready;
        ready = co_await text(output.revision, input.revision());
        if (!ready) co_return ready;
        co_return co_await text(output.build_mode, input.build_mode());
    }

    seastar::future<codec::result<void>> capabilities(
      control_capabilities& output, const common::Capabilities& input) {
        if (auto ready = work_.poll(anchor_); !ready)
            co_return codec::failure(ready.error());
        output.protocol_versions.reserve(
          static_cast<std::size_t>(input.protocol_versions_size()));
        output.formats.reserve(static_cast<std::size_t>(input.formats_size()));
        output.compression_codecs.reserve(
          static_cast<std::size_t>(input.compression_codecs_size()));
        for (const auto value : input.protocol_versions()) {
            if (
              auto ready = co_await work_.admit(
                byte_count{sizeof(value)}, item_count{1}, anchor_);
              !ready)
                co_return codec::failure(ready.error());
            if (auto ready = work_.poll(anchor_); !ready)
                co_return codec::failure(ready.error());
            output.protocol_versions.push_back(
              static_cast<std::uint16_t>(value));
        }
        for (const auto& value : input.formats()) {
            if (
              auto ready = co_await work_.admit(
                byte_count{sizeof(format_capability)}, item_count{1}, anchor_);
              !ready)
                co_return codec::failure(ready.error());
            if (auto ready = work_.poll(anchor_); !ready)
                co_return codec::failure(ready.error());
            output.formats.push_back(
              {static_cast<std::uint16_t>(value.family()),
               static_cast<std::uint16_t>(value.oldest_readable()),
               static_cast<std::uint16_t>(value.current()),
               value.supported_features()});
        }
        for (const auto value : input.compression_codecs()) {
            if (
              auto ready = co_await work_.admit(
                byte_count{sizeof(value)}, item_count{1}, anchor_);
              !ready)
                co_return codec::failure(ready.error());
            if (auto ready = work_.poll(anchor_); !ready)
                co_return codec::failure(ready.error());
            output.compression_codecs.push_back(
              static_cast<std::uint8_t>(value));
        }
        output.max_frame_body_bytes = byte_count{input.max_frame_body_bytes()};
        output.max_expanded_batch_bytes = byte_count{
          input.max_expanded_batch_bytes()};
        output.max_header_bytes = byte_count{input.max_header_bytes()};
        output.max_record_bytes = byte_count{input.max_record_bytes()};
        output.max_original_records = item_count{input.max_original_records()};
        co_return work_.poll(anchor_);
    }

private:
    codec::cooperative_work& work_;
    codec::error anchor_;
};

template<typename Message>
seastar::future<codec::result<void>> convert(
  const Message& input,
  control_data& data,
  codec::cooperative_work& work,
  codec::error anchor) {
    if (
      auto ready = co_await work.admit(byte_count{128}, item_count{8}, anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    converter copy{work, anchor};
    if constexpr (
      std::same_as<Message, native::HandshakeRequest>
      || std::same_as<Message, native::HandshakeResponse>) {
        using output_type = std::conditional_t<
          std::same_as<Message, native::HandshakeRequest>,
          handshake_request,
          handshake_response>;
        auto& output = data.emplace<output_type>();
        if constexpr (std::same_as<Message, native::HandshakeRequest>) {
            output.peer = static_cast<peer_kind>(input.peer_kind());
            if (input.has_expected_cluster_id())
                output.expected_cluster = identity<model::cluster_id>(
                  input.expected_cluster_id());
            if (input.has_broker_id())
                output.broker = identity<model::broker_id>(input.broker_id());
        } else {
            output.cluster = identity<model::cluster_id>(input.cluster_id());
            output.broker = identity<model::broker_id>(input.broker_id());
        }
        auto ready = co_await copy.build(output.build, input.build());
        if (!ready) co_return ready;
        co_return co_await copy.capabilities(
          output.capabilities, input.capabilities());
    } else if constexpr (std::same_as<Message, native::Redirect>) {
        auto& output = data.emplace<redirect_control>();
        output.destination = routing(input.destination());
        output.broker = identity<model::broker_id>(input.broker_id());
        output.reason = static_cast<control_error_code>(input.reason());
        output.endpoint.port = static_cast<std::uint16_t>(
          input.endpoint().port());
        co_return co_await copy.text(
          output.endpoint.host, input.endpoint().host());
    } else {
        auto& output = data.emplace<error_control>();
        output.code = static_cast<control_error_code>(input.code());
        if (input.has_observed()) output.observed = routing(input.observed());
        if (input.has_reason())
            co_return co_await copy.text(
              output.reason.emplace(), input.reason());
        co_return work.poll(anchor);
    }
}

template<typename Message>
seastar::future<codec::result<decoded_control>> decode(
  bytes::fragmented_buffer_parser& input,
  detail::control_message schema,
  control_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context) {
    const auto start = codec::detail::integer_read_start(
      input, context, codec::input_boundary::complete);
    if (!start) co_return codec::failure(start.error());
    context.origin = *start;
    const auto anchor = error(errc::success, context);
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const auto size = input.bytes_remaining();
    if (size > work.policy().config().max_control_bytes)
        co_return codec::failure(error(errc::resource_exhausted, context));
    const auto normalized = codec::detail::consume_decode_budget(
      work.policy(), memory, byte_count{}, byte_count{}, context, *start);
    if (!normalized) co_return codec::failure(normalized.error());
    memory = *normalized;
    const auto copy_charge = size.value() == 0 ? byte_count{}
                                               : memory.charge(size);
    if (copy_charge < size)
        co_return codec::failure(error(errc::invalid_argument, context));
    if (
      !work.policy().validate_allocation(copy_charge)
      || copy_charge > work.policy().config().max_retained_bytes)
        co_return codec::failure(error(errc::resource_exhausted, context));
    const auto copy_memory = codec::detail::consume_decode_budget(
      work.policy(), memory, copy_charge, byte_count{}, context, *start);
    if (!copy_memory) co_return codec::failure(copy_memory.error());

    const auto entry_depth = input.checkpoint_depth();
    if (auto mark = input.push_checkpoint(); !mark)
        co_return codec::failure(
          codec::detail::allocation_cost_error(mark.error(), context, *start));
    codec::detail::parser_transaction_guard transaction{input, entry_depth};
    std::optional<seastar::temporary_buffer<char>> wire;
    std::unique_ptr<Message> generated;
    std::optional<control_data> output;
    std::optional<codec::error> failed;
    std::exception_ptr exception;
    codec::decode_budget persistent = memory;
    item_count native_units;
    try {
        do {
            if (
              auto ready = co_await work.admit(
                byte_count{}, item_count{1}, anchor);
              !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            if (size.value() == 0)
                wire.emplace();
            else
                wire.emplace(static_cast<std::size_t>(size.value()));
            for (std::size_t offset = 0; offset < wire->size();) {
                const auto count = std::min(
                  {wire->size() - offset,
                   static_cast<std::size_t>(work.byte_quantum().value() / 2U),
                   static_cast<std::size_t>(work.item_quantum().value())});
                if (count == 0) {
                    failed = error(errc::resource_exhausted, context);
                    break;
                }
                if (
                  auto ready = co_await work.admit(
                    byte_count{2U * count}, item_count{count}, anchor);
                  !ready) {
                    failed = ready.error();
                    break;
                }
                if (auto ready = work.poll(anchor); !ready) {
                    failed = ready.error();
                    break;
                }
                const auto copied = input.read_to(
                  std::span<char>{wire->get_write() + offset, count});
                KWAQUE_INVARIANT(
                  invariant_id{"KQ-CONTROL-COPY"},
                  copied.has_value(),
                  "admitted control copy exceeded the exact payload");
                offset += count;
            }
            if (failed) break;
            const auto checked = co_await detail::preflight_control(
              std::span<const char>{wire->get(), wire->size()},
              schema,
              work,
              context);
            if (!checked) {
                failed = checked.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            native_units = *checked;
            const auto cost = detail::bound_control_parse(
              schema, size, work.policy(), memory.charge, context);
            if (!cost) {
                failed = cost.error();
                break;
            }
            const auto native_memory = codec::detail::consume_decode_budget(
              work.policy(),
              memory,
              cost->input_copy,
              cost->generated_peak,
              context,
              *start);
            if (!native_memory) {
                failed = native_memory.error();
                break;
            }
            // Both native parse and its eventual bounded destructor must fit
            // the complete work quantum. Cleanup ignores abort, not this cap.
            if (
              auto ready = co_await work.admit(size, native_units, anchor);
              !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            generated = std::make_unique<Message>();
            bool parsed = false;
            {
                google::protobuf::io::CodedInputStream stream{
                  reinterpret_cast<const std::uint8_t*>(wire->get()),
                  static_cast<int>(wire->size())};
                stream.SetTotalBytesLimit(static_cast<int>(wire->size()));
                stream.SetRecursionLimit(
                  static_cast<int>(
                    work.policy().config().max_nesting_depth.value() - 1U));
                parsed = generated->ParseFromCodedStream(&stream)
                         && stream.ConsumedEntireMessage()
                         && stream.CurrentPosition()
                              == static_cast<int>(wire->size());
            }
            if (!parsed) {
                failed = error(errc::malformed_data, context);
                break;
            }
            if (auto ready = co_await work.checkpoint(anchor); !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            const auto conversion = detail::control_conversion_storage(
              *generated, expected, memory, work.policy(), context);
            if (!conversion) {
                failed = conversion.error();
                break;
            }
            const auto total = cost->generated_peak.value()
                               + cost->input_copy.value() + conversion->value();
            if (
              total
              > codec::limits::defaults().config().max_metadata_bytes.value()) {
                failed = error(errc::resource_exhausted, context);
                break;
            }
            const auto admitted = codec::detail::consume_decode_budget(
              work.policy(),
              *native_memory,
              byte_count{},
              *conversion,
              context,
              *start);
            if (!admitted) {
                failed = admitted.error();
                break;
            }
            const auto retained = codec::detail::consume_decode_budget(
              work.policy(),
              memory,
              byte_count{},
              *conversion,
              context,
              *start);
            if (!retained) {
                failed = retained.error();
                break;
            }
            persistent = *retained;
            output.emplace();
            const auto converted = co_await detail::copy_control_fields(
              *generated, *output, work, anchor);
            if (!converted) {
                failed = converted.error();
                break;
            }
        } while (false);
    } catch (...) {
        exception = std::current_exception();
    }

    if (generated) {
        co_await work.drain_inline(size, native_units);
        generated.reset();
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    }
    if (wire) {
        co_await work.drain_inline(byte_count{}, item_count{1});
        wire.reset();
    }
    if (!failed && !exception) {
        if (auto ready = work.poll(anchor); !ready) failed = ready.error();
    }
    if (failed || exception) {
        if (output) {
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            output.reset();
        }
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(*failed);
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CONTROL-OUTCOME"},
      output.has_value(),
      "control decode completed without an owning value");
    transaction.commit();
    co_return decoded_control{
      detail::control_access::finish(std::move(*output)), persistent};
}
} // namespace

#define KWAQUE_CONTROL_CONVERSION(Type)                                        \
    codec::result<byte_count> detail::control_conversion_storage(              \
      const Type& value,                                                       \
      control_expectation expected,                                            \
      codec::decode_budget memory,                                             \
      codec::limits policy,                                                    \
      codec::field_context context) {                                          \
        const auto valid = validate_context(value, expected, context);         \
        if (!valid) return codec::failure(valid.error());                      \
        conversion_cost cost{memory, policy, context};                         \
        measure_conversion(cost, value);                                       \
        if (cost.failed) return codec::failure(*cost.failed);                  \
        return cost.bytes;                                                     \
    }                                                                          \
    seastar::future<codec::result<void>> detail::copy_control_fields(          \
      const Type& value,                                                       \
      control_data& output,                                                    \
      codec::cooperative_work& work,                                           \
      codec::error anchor) {                                                   \
        return convert(value, output, work, anchor);                           \
    }
KWAQUE_CONTROL_CONVERSION(kwaque::control::v1::HandshakeRequest)
KWAQUE_CONTROL_CONVERSION(kwaque::control::v1::HandshakeResponse)
KWAQUE_CONTROL_CONVERSION(kwaque::control::v1::Redirect)
KWAQUE_CONTROL_CONVERSION(kwaque::common::v1::Error)
#undef KWAQUE_CONTROL_CONVERSION

seastar::future<codec::result<decoded_control>> decode_control(
  bytes::fragmented_buffer_parser& input,
  frame_kind expected_kind,
  control_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context) {
    const auto fail = [&] {
        return seastar::make_ready_future<codec::result<decoded_control>>(
          codec::failure(error(errc::invalid_argument, context)));
    };
    if (
      !memory.charge
      || !detail::valid_control_expectation(expected_kind, expected))
        return fail();
    const auto* schema = detail::schema_for(expected_kind);
    if (!schema) return fail();
    switch (expected_kind) {
    case frame_kind::handshake_request:
        return decode<native::HandshakeRequest>(
          input, schema->message, expected, memory, work, context);
    case frame_kind::handshake_response:
        return decode<native::HandshakeResponse>(
          input, schema->message, expected, memory, work, context);
    case frame_kind::redirect:
        return decode<native::Redirect>(
          input, schema->message, expected, memory, work, context);
    case frame_kind::error:
        return decode<common::Error>(
          input, schema->message, expected, memory, work, context);
    case frame_kind::submitted_batch:
    case frame_kind::assigned_batch:
        return fail();
    }
    return fail();
}
} // namespace kwaque::protocol
