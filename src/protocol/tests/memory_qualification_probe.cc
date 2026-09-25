#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/cooperative.h"
#include "src/codec/tests/benchmark_buffer.h"
#include "src/codec/tests/memory_qualification_support.h"
#include "src/codec/tests/prepared_abort_source.h"
#include "src/codec/transaction.h"
#include "src/compression/compression.h"
#include "src/model/batch_codec.h"
#include "src/model/tests/model_bench_fixture.h"
#include "src/protocol/batch_frame_codec.h"
#include "src/protocol/tests/frame_test_support.h"
#include "src/protocol/tests/memory_qualification_control.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>

#include <boost/program_options.hpp>
#include <crc32c/crc32c.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>

namespace {
namespace codec = kwaque::codec;
namespace model = kwaque::model;
namespace protocol = kwaque::protocol;
namespace observation = codec::testing;
namespace fixture = protocol::testing::frame_fixture;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::bytes::fragmented_buffer;
using kwaque::bytes::fragmented_buffer_parser;
using kwaque::bytes::testing::charge;
using observation::measure;
using observation::require;
using observation::retained_cost;

thread_local seastar::abort_source* admission_abort = nullptr;
byte_count observed_charge(byte_count request) noexcept {
    if (auto* source = std::exchange(admission_abort, nullptr))
        source->request_abort();
    return charge(request);
}
class arm_abort final {
public:
    explicit arm_abort(seastar::abort_source* source) noexcept {
        admission_abort = source;
    }
    ~arm_abort() { admission_abort = nullptr; }
    arm_abort(const arm_abort&) = delete;
    arm_abort& operator=(const arm_abort&) = delete;
};
byte_count string_cost(const std::string& value) {
    return charge(byte_count{value.capacity() + 1U});
}
codec::decode_budget memory(byte_count other = {}) {
    return {
      observation::residual.checked_sub(other).value(),
      byte_count{1U << 20U},
      observed_charge};
}
std::string extension(std::size_t header) {
    return header == 48
             ? std::string{}
             : fixture::extension(7, 0, std::string(header - 56, 'x'));
}

void header_operation(std::string_view name) {
    const auto h = name.ends_with("h4096") ? 4096U
                   : name.ends_with("h57") ? 57U
                                           : 48U;
    const auto text = fixture::header(16U << 20U, 0, extension(h));
    auto bytes = fixture::fragmented(text, 7);
    const auto held
      = string_cost(text).checked_add(retained_cost(bytes)).value();
    fragmented_buffer_parser input{std::move(bytes)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto budget = codec::reserve_decode_input(
                    input, work.policy(), memory(string_cost(text)))
                    .value();
    if (name.starts_with("prefix-")) {
        for (unsigned i = 0; i < 8; ++i)
            input.push_checkpoint().value();
        const auto result = measure(name, text.size(), held, [&] {
            return protocol::peek_frame_prefix(
              input, work.policy(), fixture::bounds);
        });
        require(
          result && result->header_bytes == h
            && result->payload_bytes == (16U << 20U),
          "prefix changed scalar fields");
        require(
          input.checkpoint_depth() == 8
            && input.bytes_consumed() == byte_count{},
          "prefix changed parser marks");
    } else {
        input.push_checkpoint().value();
        const auto result = measure(name, text.size(), held, [&] {
            return protocol::inspect_frame_header(
                     input, fixture::bounds, budget, work)
              .get();
        });
        require(
          result && result->header_bytes == byte_count{h}
            && result->payload_bytes == byte_count{16U << 20U},
          "header inspection required absent payload");
        require(
          input.bytes_consumed() == byte_count{}
            && input.checkpoint_depth() == 1,
          "header inspection did not restore input");
    }
}

void frame_operation(std::string_view name) {
    const bool encode = name.starts_with("encode-");
    const bool cancel = name.ends_with("-abort");
    const bool pressure = name.ends_with("-pressure");
    const bool fragmented = name.ends_with("-max-fragmented");
    const bool large = fragmented || name.ends_with("-max");
    const bool fragment_limit = name.ends_with("-fragment-limit");
    const bool control = name.ends_with("-control");
    const std::size_t size = name.ends_with("-empty") ? 0U
                             : large                  ? 16U << 20U
                             : control                ? 65536U
                                                      : 71U;
    const auto h = name.ends_with("-extended") || control ? 4096U : 48U;
    const auto width = fragmented ? 16401U : large ? 65472U : 67U;
    std::optional<observation::prepared_abort_source> abort;
    const auto setup_cost = observation::observe_setup(
      [&] { abort.emplace(); });
    codec::cooperative_work work{codec::limits::defaults(), *abort};
    auto fields = fixture::metadata;
    if (control) {
        fields.kind = protocol::frame_kind::handshake_request;
        fields.stream = model::transport_stream_id{};
    }
    if (encode) {
        auto prepared
          = fragment_limit
              ? fixture::
                  patterned_payload{fixture::fragmented(std::string(1024, 'x'), 1), 0}
              : fixture::make_payload(size, width, work).get();
        const auto expected_crc = prepared.checksum;
        auto source = std::move(prepared.bytes);
        const auto held = setup_cost.checked_add(retained_cost(source)).value();
        auto result = [&] {
            arm_abort armed{cancel ? &*abort : nullptr};
            return measure(name, source.size().value(), held, [&] {
                return protocol::encode_frame(
                         std::move(source),
                         fields,
                         work,
                         fixture::bounds,
                         {},
                         pressure ? byte_count{}
                                  : memory(setup_cost).operation_remaining,
                         observed_charge)
                  .get();
            });
        }();
        if (cancel || pressure || fragment_limit) {
            require(
              !result
                && result.error().code()
                     == (cancel ? errc::aborted : errc::resource_exhausted),
              "frame writer rejection changed");
            require(
              !cancel || abort->abort_requested(),
              "writer abort boundary not reached");
        } else {
            require(
              result && result->size() == byte_count{48U + size},
              "frame writer lost payload extent");
            require(
              !fragmented || result->fragment_count() == 1024,
              "maximum writer layout changed");
            auto prefix = result->share({}, byte_count{48}).value();
            fragmented_buffer_parser header{std::move(prefix)};
            const auto parsed = protocol::peek_frame_prefix(
                                  header, work.policy(), fixture::bounds)
                                  .value();
            require(
              parsed.payload_crc32c == expected_crc,
              "frame writer changed payload checksum");
            const auto expected_header = fixture::header(
              static_cast<std::uint32_t>(size), expected_crc, {}, fields);
            std::array<char, 48> bytes{};
            header.peek_to(bytes).value();
            require(
              std::string_view{bytes.data(), bytes.size()} == expected_header,
              "frame writer changed protected fields");
        }
        // The buffer contract leaves an inspectable empty moved-from owner.
        // NOLINTBEGIN(bugprone-use-after-move)
        require(
          source.empty(), "entered frame writer did not consume its donor");
        // NOLINTEND(bugprone-use-after-move)
        return;
    }
    fragmented_buffer source;
    byte_count other = setup_cost;
    std::string partial;
    std::uint64_t needed = 0;
    if (name == "decode-missing-prefix") {
        partial = fixture::header(0, 0).substr(0, 17);
        needed = 31;
    } else if (name == "decode-missing-header") {
        partial = fixture::header(0, 0, extension(4096)).substr(0, 48);
        needed = 4048;
    } else if (name == "decode-missing-payload") {
        // The complete header has deliberately wrong body CRC. It must not be
        // checked while the declared payload is still incomplete.
        partial = fixture::header(200, 0) + std::string(52, 'x');
        needed = 148;
    }
    if (needed || name == "decode-corrupt") {
        if (!needed) {
            partial = fixture::wire();
            partial.back() ^= 1;
        }
        source = fixture::fragmented(partial, 7);
        other = other.checked_add(string_cost(partial)).value();
    } else {
        source = fixture::wrap(
                   fixture::make_payload(size, width, work).get(),
                   work,
                   extension(h),
                   fields)
                   .get();
    }
    const auto bytes = source.size();
    const auto held = other.checked_add(retained_cost(source)).value();
    fragmented_buffer_parser input{std::move(source)};
    for (unsigned i = 0; i < 7; ++i)
        input.push_checkpoint().value();
    auto budget = codec::reserve_decode_input(
                    input, work.policy(), memory(other))
                    .value();
    if (pressure) budget.operation_remaining = {};
    const auto result = [&] {
        arm_abort armed{cancel ? &*abort : nullptr};
        return measure(name, bytes.value(), held, [&] {
            return protocol::decode_frame(input, fixture::bounds, budget, work)
              .get();
        });
    }();
    require(
      input.checkpoint_depth() == 7, "frame decoder changed caller marks");
    if (cancel || pressure || name == "decode-corrupt") {
        const auto expected = cancel     ? errc::aborted
                              : pressure ? errc::resource_exhausted
                                         : errc::corrupt_data;
        require(
          !result && result.error().code() == expected
            && input.bytes_consumed() == byte_count{},
          "frame decoder rejection changed");
        require(
          !cancel || abort->abort_requested(),
          "decoder abort boundary not reached");
        return;
    }
    require(result.has_value(), "valid frame failed");
    if (needed) {
        const auto* more = std::get_if<protocol::need_more>(&*result);
        require(
          more && more->additional_bytes == byte_count{needed}
            && input.bytes_consumed() == byte_count{},
          "incremental hint changed");
        return;
    }
    const auto* ready = std::get_if<protocol::framed_payload>(&*result);
    require(
      ready && input.at_end() && ready->header.metadata == fields
        && ready->header.header_bytes == byte_count{h}
        && ready->payload.size() == byte_count{size},
      "frame decoder changed metadata");
    const auto retained = ready->payload.allocation_cost(charge).value();
    require(
      budget.operation_remaining.value()
            - ready->remaining.operation_remaining.value()
          == retained.descriptors.value()
        && budget.metadata_remaining.value()
               - ready->remaining.metadata_remaining.value()
             == retained.descriptors.value(),
      "frame decoder refunded payload ownership");
    for (const auto fragment : ready->payload) {
        require(
          std::ranges::all_of(
            fragment.bytes(), [](char c) { return c == 'x'; }),
          "frame decoder changed payload");
        seastar::thread::maybe_yield();
    }
}

template<bool Assigned>
void raw_operation(std::string_view name) {
    namespace bench = model::bench;
    const bool compressed = name.contains("-lz4-");
    const bool large = name.ends_with("-max");
    const bool sparse = name.starts_with("raw-sparse-");
    const bool cancel = name.ends_with("-abort");
    const bool pressure = name.ends_with("-pressure");
    std::optional<observation::prepared_abort_source> abort;
    const auto setup_cost = observation::observe_setup(
      [&] { abort.emplace(); });
    codec::cooperative_work work{codec::limits::defaults(), *abort};
    auto expected = bench::expected_context();
    std::optional<codec::semantic_batch_digest> fingerprint;
    std::uint64_t original_count = 0, retained_count = 0, record_bytes = 0;
    auto inner = [&] {
        bench::model_fixture fixture{
          large ? 8U : 3U, large ? 1048565U : 64U, 65472};
        fixture.initialize().get();
        fingerprint = fixture.digest;
        original_count = fixture.count;
        retained_count = sparse ? fixture.selected.size() : fixture.count;
        if constexpr (!Assigned)
            return std::move(fixture.submitted_wire);
        else
            return sparse ? std::move(fixture.sparse_wire)
                          : std::move(fixture.assigned_wire);
    }();
    expected.fingerprint = *fingerprint;
    {
        // Preserve the independently built exact bytes until optional encoding
        // completes. Temporary model/parser owners end before observation.
        fragmented_buffer_parser input{inner.share()};
        const auto budget
          = codec::reserve_decode_input(
              input,
              work.policy(),
              memory(setup_cost.checked_add(retained_cost(inner)).value()))
              .value();
        if constexpr (Assigned) {
            auto decoded = model::decode_assigned_batch(
                             input, expected, budget, work)
                             .get()
                             .value();
            record_bytes = decoded.value.records().size().value();
            if (compressed)
                inner = model::encode_assigned_batch(
                          std::move(decoded.value),
                          kwaque::compression::codec_id::lz4,
                          work,
                          budget.operation_remaining,
                          charge)
                          .get()
                          .value();
        } else {
            auto decoded = model::decode_submitted_batch(
                             input, expected, budget, work)
                             .get()
                             .value();
            record_bytes = decoded.value.records().size().value();
            if (compressed)
                inner = model::encode_submitted_batch(
                          std::move(decoded.value),
                          kwaque::compression::codec_id::lz4,
                          work,
                          budget.operation_remaining,
                          charge)
                          .get()
                          .value();
        }
    }
    std::string altered;
    if (name.ends_with("-trailing") || name.ends_with("-corrupt-inner")) {
        require(inner.size().value() < 4096, "mutation fixture is not bounded");
        for (const auto fragment : inner)
            altered.append(fragment.data(), fragment.size());
        if (name.ends_with("-trailing"))
            altered.push_back('!');
        else
            altered.back() ^= 1;
        inner = fixture::fragmented(altered, 7);
    }
    std::uint32_t checksum = 0;
    for (const auto fragment : inner) {
        checksum = ::crc32c::Extend(
          checksum,
          reinterpret_cast<const std::uint8_t*>(fragment.data()),
          fragment.size());
        seastar::thread::maybe_yield();
    }
    auto fields = fixture::metadata;
    fields.kind = Assigned ? protocol::frame_kind::assigned_batch
                           : protocol::frame_kind::submitted_batch;
    if (name.ends_with("-wrong-kind"))
        fields.kind = protocol::frame_kind::redirect;
    if (name.ends_with("-wrong-kind")) fields.stream = {};
    if (name.ends_with("-wrong-context")) {
        std::array<std::uint8_t, 16> bytes{};
        std::copy(
          expected.topic.bytes().begin(),
          expected.topic.bytes().end(),
          bytes.begin());
        bytes[0] ^= 1;
        expected.topic = model::topic_id::make(bytes).value();
        expected.original_binding.reset();
    }
    const auto inner_bytes = inner.size();
    auto wire
      = fixture::wrap({std::move(inner), checksum}, work, {}, fields).get();
    const auto bytes = wire.size();
    const auto other = setup_cost.checked_add(string_cost(altered)).value();
    const auto held = other.checked_add(retained_cost(wire)).value();
    fragmented_buffer_parser input{std::move(wire)};
    for (unsigned i = 0; i < 7; ++i)
        input.push_checkpoint().value();
    auto budget = codec::reserve_decode_input(
                    input, work.policy(), memory(other))
                    .value();
    if (pressure) budget.metadata_remaining = {};
    const auto read = [&] {
        if constexpr (Assigned)
            return protocol::decode_assigned_frame(
                     input, expected, fixture::bounds, budget, work)
              .get();
        else
            return protocol::decode_submitted_frame(
                     input, expected, fixture::bounds, budget, work)
              .get();
    };
    const auto result = [&] {
        arm_abort armed{cancel ? &*abort : nullptr};
        return measure(name, bytes.value(), held, read);
    }();
    require(input.checkpoint_depth() == 7, "raw decoder changed caller marks");
    const bool rejected = cancel || pressure || name.ends_with("-trailing")
                          || name.ends_with("-corrupt-inner")
                          || name.ends_with("-wrong-kind")
                          || name.ends_with("-wrong-context");
    if (rejected) {
        const auto code = cancel     ? errc::aborted
                          : pressure ? errc::resource_exhausted
                          : name.ends_with("-trailing") ? errc::malformed_data
                          : name.ends_with("-corrupt-inner")
                            ? errc::corrupt_data
                            : errc::wrong_context;
        require(
          !result && result.error().code() == code
            && input.bytes_consumed() == byte_count{},
          "raw rejection changed category or position");
        require(
          !cancel || abort->abort_requested(),
          "raw abort boundary not reached");
        return;
    }
    using decoded_type = std::conditional_t<
      Assigned,
      protocol::decoded_assigned_frame,
      protocol::decoded_submitted_frame>;
    require(result.has_value(), "raw frame failed");
    const auto* ready = std::get_if<decoded_type>(&*result);
    require(
      ready && input.at_end()
        && ready->batch.records().size().value() == record_bytes
        && ready->batch.fingerprint() == *fingerprint
        && ready->header.metadata == fields,
      "raw frame changed records or identity");
    const auto original = [&] {
        if constexpr (Assigned)
            return ready->batch.context().submitted();
        else
            return ready->batch.context();
    }();
    require(
      original.id() == *expected.id
        && original.binding() == *expected.original_binding
        && original.original_count().value() == original_count,
      "raw frame changed original context");
    if constexpr (Assigned) {
        require(
          ready->batch.context().retained_count().value() == retained_count
            && ready->batch.context().logical_span().begin().value() == 100
            && ready->batch.context().logical_span().end().value()
                 == 100 + original_count
            && ready->fingerprint_verification
                 == (sparse ? model::batch_fingerprint_verification::carried : model::batch_fingerprint_verification::recomputed),
          "assigned frame changed logical coverage or fingerprint status");
    }
    // Equal-work direct decoding of the identical inner fragment layout checks
    // that the frame refunds only its temporary payload-parser descriptors.
    // This verification occurs after the measured owning result was retained.
    input.rollback().value();
    input.skip(byte_count{48}).value();
    const auto alias
      = input.next_buffer_allocation_cost(inner_bytes, charge).value();
    const auto child_memory
      = codec::detail::consume_decode_budget(
          work.policy(), budget, {}, alias.descriptors, {}, 0)
          .value();
    fragmented_buffer_parser child{input.read_buffer(inner_bytes).value()};
    const auto check_refund = [&](const auto& decoded) {
        require(
          decoded.has_value() && child.at_end(),
          "direct raw comparison failed");
        require(
          budget.operation_remaining.value()
                - ready->remaining.operation_remaining.value()
              == child_memory.operation_remaining.value()
                   - decoded->remaining.operation_remaining.value()
            && budget.metadata_remaining.value()
                   - ready->remaining.metadata_remaining.value()
                 == child_memory.metadata_remaining.value()
                      - decoded->remaining.metadata_remaining.value(),
          "raw frame refunded persistent child memory");
    };
    if constexpr (Assigned)
        check_refund(
          model::decode_assigned_batch(child, expected, child_memory, work)
            .get());
    else
        check_refund(
          model::decode_submitted_batch(child, expected, child_memory, work)
            .get());
}

int exercise(std::string_view name) {
    observation::report_profile();
    std::printf("compiler=%s\n", __clang_version__);
    if (name == "capabilities") return 0;
    observation::require_effective_policy();
    if (name.starts_with("control-"))
        protocol::testing::qualify_control_memory(name);
    else if (name.starts_with("prefix-") || name.starts_with("header-"))
        header_operation(name);
    else if (name.starts_with("encode-") || name.starts_with("decode-"))
        frame_operation(name);
    else if (name.starts_with("raw-submitted-"))
        raw_operation<false>(name);
    else if (
      name.starts_with("raw-assigned-") || name.starts_with("raw-sparse-"))
        raw_operation<true>(name);
    else
        throw std::invalid_argument("unknown frame memory scenario");
    std::puts("status=ok");
    return 0;
}
} // namespace

int main(int argc, char** argv) {
    if (!observation::install_crypto_allocation_observation()) {
        std::fputs("crypto allocation hooks require a fresh process\n", stderr);
        return 1;
    }
    seastar::app_template app;
    app.set_configuration_reader([](boost::program_options::variables_map&) {});
    app.add_options()(
      "scenario", boost::program_options::value<std::string>()->required());
    return app.run(argc, argv, [&app] {
        return seastar::async([&app] {
            return exercise(app.configuration()["scenario"].as<std::string>());
        });
    });
}
