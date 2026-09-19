#include "src/protocol/control_internal.h"
#include "src/protocol/control_schema.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <bit>
#include <cstdint>
#include <exception>
#include <optional>
#include <utf8_range.h>
#include <utility>

namespace kwaque::protocol::detail {
namespace {
using message = control_message;
std::uint64_t varint_bytes(std::uint64_t value) noexcept {
    return (static_cast<unsigned>(std::bit_width(value | 1U)) + 6U) / 7U;
}

class inspector final {
public:
    inspector(
      codec::cooperative_work& work,
      bytes::allocation_charge_fn charge,
      codec::field_context context)
      : work(work)
      , charge_(charge)
      , context_(context)
      , limits_(work.policy().config()) {}

    void fail(errc code) {
        if (!failed)
            failed.emplace(
              code, context_.family, context_.field, context_.origin);
    }
    bool poll() {
        if (!failed) {
            const auto ready = work.poll(
              codec::error{
                errc::success,
                context_.family,
                context_.field,
                context_.origin});
            if (!ready) failed = ready.error();
        }
        return !failed;
    }
    void storage(std::uint64_t capacity, std::uint64_t width = 1) {
        if (failed || capacity == 0) return;
        if (capacity > limits_.max_allocation_bytes.value() / width) {
            fail(errc::resource_exhausted);
            return;
        }
        const byte_count request{capacity * width};
        const auto cost = charge_(request);
        if (cost < request) {
            fail(errc::invalid_argument);
            return;
        }
        if (
          cost > limits_.max_allocation_bytes
          || cost.value()
               > limits_.max_metadata_bytes.value() - storage_bytes.value()) {
            fail(errc::resource_exhausted);
            return;
        }
        storage_bytes = byte_count{storage_bytes.value() + cost.value()};
    }
    void units(std::uint64_t n = 1) {
        count += n;
        if (count > limits_.max_control_fields.value())
            fail(errc::resource_exhausted);
    }
    bool number(message type, std::uint32_t n, std::uint64_t value) {
        const auto& field = *field_for(*schema_for(type), n);
        if (field.kind == control_field_kind::enumeration) {
            if (value == 0)
                fail(errc::invalid_argument);
            else if (
              value >= 64
              || (field.enum_values & (std::uint64_t{1} << value)) == 0)
                fail(errc::unsupported_format);
        } else if (value < field.minimum_value || value > field.maximum_value)
            fail(errc::invalid_argument);
        return !failed;
    }
    std::uint64_t scalar(message type, std::uint32_t n, std::uint64_t value) {
        if (!poll()) return 0;
        if (!number(type, n, value)) return 0;
        units();
        return 1U
               + (field_for(*schema_for(type), n)->kind == control_field_kind::fixed64 ? 8U : varint_bytes(value));
    }
    std::uint64_t id(const auto& value) {
        if (!poll()) return 0;
        if (value.is_nil()) fail(errc::invalid_argument);
        if (limits_.max_control_field_bytes < control_identity_bytes)
            fail(errc::resource_exhausted);
        units();
        return 1U + varint_bytes(control_identity_bytes.value())
               + control_identity_bytes.value();
    }
    std::uint64_t
    nested(message type, std::uint64_t size, std::uint64_t depth) {
        if (!poll()) return 0;
        if (
          depth > limits_.max_nesting_depth.value()
          || size > std::min(
                      schema_for(type)->maximum_wire_bytes,
                      limits_.max_control_bytes)
                      .value())
            fail(errc::resource_exhausted);
        units();
        return 1U + varint_bytes(size) + size;
    }
    bool array(message type, std::uint32_t n, const auto& values) {
        const auto& field = *field_for(*schema_for(type), n);
        if (values.size() < field.minimum_elements.value())
            fail(errc::invalid_argument);
        if (
          values.size()
          > std::min(field.maximum_elements, limits_.max_control_repeated)
              .value())
            fail(errc::resource_exhausted);
        if (!failed) storage(values.capacity(), sizeof(values[0]));
        return !failed;
    }
    seastar::future<bool> admit(byte_count bytes, item_count items) {
        if (failed) co_return false;
        const codec::error anchor{
          errc::success, context_.family, context_.field, context_.origin};
        const auto ready = co_await work.admit(bytes, items, anchor);
        if (!ready) failed = ready.error();
        if (auto poll = work.poll(anchor); !poll && !failed)
            failed = poll.error();
        co_return !failed;
    }
    seastar::future<std::uint64_t> text(
      message type,
      std::uint32_t n,
      const std::string& value,
      bool omit_empty = false) {
        if (!poll()) co_return 0;
        const auto& field = *field_for(*schema_for(type), n);
        if (value.size() < field.minimum_bytes.value())
            fail(errc::invalid_argument);
        if (
          value.size()
            > std::min(field.maximum_bytes, limits_.max_control_field_bytes)
                .value()
          || value.capacity() >= limits_.max_allocation_bytes.value())
            fail(errc::resource_exhausted);
        if (failed) co_return 0;
        // Conservatively include even inline string capacity. Existing heap
        // capacity, not logical length, is what the borrowed owner retains.
        storage(value.capacity() + 1U);
        if (
          !co_await admit(byte_count{2U * value.size()}, item_count{1})
          || !poll())
            co_return 0;
        if (utf8_range_IsValid(value.data(), value.size()) == 0)
            fail(errc::invalid_argument);
        if (type == message::endpoint && std::ranges::any_of(value, [](char c) {
                const auto octet = static_cast<unsigned char>(c);
                return octet <= 0x20 || octet == 0x7f;
            }))
            fail(errc::invalid_argument);
        if (!poll() || (omit_empty && value.empty())) co_return 0;
        units();
        co_return 1U + varint_bytes(value.size()) + value.size();
    }
    seastar::future<std::uint64_t> build(const build_information& value) {
        auto size = co_await text(message::build_info, 1, value.version, true);
        size += co_await text(message::build_info, 2, value.revision, true);
        size += co_await text(message::build_info, 3, value.build_mode, true);
        co_return nested(message::build_info, size, 2);
    }
    std::uint64_t routing(const routing_context& value) {
        auto size = id(value.topic) + id(value.range) + id(value.segment);
        size += scalar(
          message::routing_context, 3, value.routing_epoch.value());
        size += scalar(message::routing_context, 5, value.generation.value());
        return nested(message::routing_context, size, 2);
    }
    seastar::future<std::uint64_t>
    capabilities(const control_capabilities& value) {
        if (!poll()) co_return 0;
        if (
          !array(message::capabilities, 1, value.protocol_versions)
          || !array(message::capabilities, 2, value.formats)
          || !array(message::capabilities, 3, value.compression_codecs))
            co_return 0;
        std::uint64_t size = 0;
        const auto integers = [&](const auto& values, std::uint32_t field)
          -> seastar::future<std::uint64_t> {
            std::uint64_t packed = 0;
            std::optional<std::uint64_t> previous;
            for (const auto item : values) {
                if (!co_await admit(byte_count{10}, item_count{1}) || !poll())
                    co_return 0;
                if (!number(message::capabilities, field, item)) co_return 0;
                if (previous && item <= *previous) {
                    fail(errc::invalid_argument);
                    co_return 0;
                }
                previous = item;
                packed += varint_bytes(item);
                units();
            }
            units();
            co_return 1U + varint_bytes(packed) + packed;
        };
        size += co_await integers(value.protocol_versions, 1);
        size += co_await integers(value.compression_codecs, 3);
        std::uint16_t previous = 0;
        for (const auto& item : value.formats) {
            if (!co_await admit(byte_count{32}, item_count{4}) || !poll())
                co_return 0;
            if (
              item.family <= previous || item.oldest_readable > item.current) {
                fail(errc::invalid_argument);
                co_return 0;
            }
            previous = item.family;
            const auto bytes
              = scalar(message::format_capability, 1, item.family)
                + scalar(message::format_capability, 2, item.oldest_readable)
                + scalar(message::format_capability, 3, item.current)
                + scalar(
                  message::format_capability, 4, item.supported_features);
            size += nested(message::format_capability, bytes, 3);
        }
        if (!co_await admit(byte_count{64}, item_count{8}) || !poll())
            co_return 0;
        size
          += scalar(
               message::capabilities, 4, value.max_frame_body_bytes.value())
             + scalar(
               message::capabilities, 5, value.max_expanded_batch_bytes.value())
             + scalar(message::capabilities, 6, value.max_header_bytes.value())
             + scalar(message::capabilities, 7, value.max_record_bytes.value())
             + scalar(
               message::capabilities, 8, value.max_original_records.value());
        co_return nested(message::capabilities, size, 2);
    }
    codec::cooperative_work& work;
    std::optional<codec::error> failed;
    byte_count storage_bytes;
    std::uint64_t count{0};

private:
    bytes::allocation_charge_fn charge_;
    codec::field_context context_;
    codec::limits_config limits_;
};
} // namespace

seastar::future<codec::result<control_layout>> inspect_control_value(
  const control_data& data,
  codec::cooperative_work& work,
  bytes::allocation_charge_fn charge,
  codec::field_context context) {
    inspector check{work, charge, context};
    if (!charge || data.valueless_by_exception())
        check.fail(errc::invalid_argument);
    if (!co_await check.admit(byte_count{128}, item_count{8}) || !check.poll())
        co_return codec::failure(*check.failed);
    frame_kind kind = frame_kind::error;
    std::uint64_t size = 0;
    if (const auto* request = std::get_if<handshake_request>(&data)) {
        kind = frame_kind::handshake_request;
        size += check.scalar(
          message::handshake_request,
          1,
          static_cast<std::uint8_t>(request->peer));
        if ((request->peer == peer_kind::broker) != request->broker.has_value())
            check.fail(errc::invalid_argument);
        if (request->expected_cluster)
            size += check.id(*request->expected_cluster);
        if (request->broker) size += check.id(*request->broker);
        size += co_await check.build(request->build);
        size += co_await check.capabilities(request->capabilities);
    } else if (const auto* response = std::get_if<handshake_response>(&data)) {
        kind = frame_kind::handshake_response;
        size += check.id(response->cluster) + check.id(response->broker);
        size += co_await check.build(response->build);
        size += co_await check.capabilities(response->capabilities);
    } else if (const auto* redirect = std::get_if<redirect_control>(&data)) {
        kind = frame_kind::redirect;
        size += check.routing(redirect->destination)
                + check.id(redirect->broker);
        auto endpoint = co_await check.text(
          message::endpoint, 1, redirect->endpoint.host);
        endpoint += check.scalar(message::endpoint, 2, redirect->endpoint.port);
        size += check.nested(message::endpoint, endpoint, 2);
        size += check.scalar(
          message::redirect, 4, static_cast<std::uint8_t>(redirect->reason));
    } else {
        const auto& failure = std::get<error_control>(data);
        size += check.scalar(
          message::error, 1, static_cast<std::uint8_t>(failure.code));
        if (
          (failure.code == control_error_code::stale_routing
           || failure.code == control_error_code::stale_segment)
          && !failure.observed)
            check.fail(errc::invalid_argument);
        if (failure.observed) size += check.routing(*failure.observed);
        if (failure.reason)
            size += co_await check.text(message::error, 2, *failure.reason);
    }
    if (size > work.policy().config().max_control_bytes.value())
        check.fail(errc::resource_exhausted);
    if (size > UINT64_MAX - context.origin) check.fail(errc::invalid_argument);
    if (!co_await check.admit(byte_count{}, item_count{1}) || !check.poll())
        co_return codec::failure(*check.failed);
    co_return control_layout{
      kind, byte_count{size}, check.storage_bytes, item_count{check.count}};
}

bool valid_control_expectation(
  frame_kind kind, control_expectation expected) noexcept {
    if (
      (expected.cluster && expected.cluster->is_nil())
      || (expected.broker && expected.broker->is_nil())
      || (expected.topic && expected.topic->is_nil()))
        return false;
    switch (kind) {
    case frame_kind::handshake_request:
    case frame_kind::handshake_response:
        return !expected.topic;
    case frame_kind::redirect:
        return !expected.cluster;
    case frame_kind::error:
        return !expected.cluster && !expected.broker;
    case frame_kind::submitted_batch:
    case frame_kind::assigned_batch:
        return false;
    }
    return false;
}
} // namespace kwaque::protocol::detail

namespace kwaque::protocol {
seastar::future<codec::result<decoded_control>> make_control(
  control_data&& donor,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context) {
    std::optional<control_data> value{std::in_place, std::move(donor)};
    std::optional<codec::error> failed;
    std::exception_ptr exception;
    codec::decode_budget remaining;
    try {
        const auto checked = co_await detail::inspect_control_value(
          *value, work, memory.charge, context);
        if (!checked)
            failed = checked.error();
        else {
            const auto admitted = codec::detail::consume_decode_budget(
              work.policy(),
              memory,
              byte_count{},
              checked->storage,
              context,
              context.origin);
            if (!admitted)
                failed = admitted.error();
            else
                remaining = *admitted;
        }
    } catch (...) {
        exception = std::current_exception();
    }
    const codec::error anchor{
      errc::success, context.family, context.field, context.origin};
    if (!failed && !exception) {
        if (auto ready = work.poll(anchor); !ready) failed = ready.error();
    }
    if (failed || exception) {
        // Invalid caller-owned vectors may exceed the accepted set caps. The
        // native vector destructor walks elements even when they are trivial
        // in debug builds; shrink those rejected owners in shared-work batches.
        control_capabilities* caps = nullptr;
        if (auto* request = std::get_if<handshake_request>(&*value))
            caps = &request->capabilities;
        if (auto* response = std::get_if<handshake_response>(&*value))
            caps = &response->capabilities;
        if (caps) {
            while (!caps->protocol_versions.empty()) {
                const auto n = std::min<std::size_t>(
                  caps->protocol_versions.size(), work.item_quantum().value());
                co_await work.drain_inline(byte_count{}, item_count{n});
                caps->protocol_versions.resize(
                  caps->protocol_versions.size() - n);
            }
            while (!caps->formats.empty()) {
                const auto n = std::min<std::size_t>(
                  caps->formats.size(), work.item_quantum().value());
                co_await work.drain_inline(byte_count{}, item_count{n});
                caps->formats.resize(caps->formats.size() - n);
            }
            while (!caps->compression_codecs.empty()) {
                const auto n = std::min<std::size_t>(
                  caps->compression_codecs.size(), work.item_quantum().value());
                co_await work.drain_inline(byte_count{}, item_count{n});
                caps->compression_codecs.resize(
                  caps->compression_codecs.size() - n);
            }
        }
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        value.reset();
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(*failed);
    }
    co_return decoded_control{
      detail::control_access::finish(std::move(*value)), remaining};
}
} // namespace kwaque::protocol
