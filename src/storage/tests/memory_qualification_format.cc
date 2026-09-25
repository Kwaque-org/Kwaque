#include "src/codec/tests/benchmark_buffer.h"
#include "src/codec/tests/prepared_abort_source.h"
#include "src/storage/tests/memory_qualification_probe.h"
#include "src/storage/tests/storage_format_fixture.h"
#include "src/storage/tests/storage_large_fixture.h"

#include <optional>
#include <stdexcept>
#include <utility>

namespace kwaque::storage::qualification {
namespace {
using namespace testing;

// One reached accounting boundary, after entry. The allocator charge remains
// identical; the callback cannot allocate and is disarmed before diagnostics.
thread_local seastar::abort_source* abort_at_charge = nullptr;
byte_count cancelling_charge(byte_count requested) noexcept {
    if (auto* source = std::exchange(abort_at_charge, nullptr))
        source->request_abort();
    return charge(requested);
}
class arm_abort final {
public:
    explicit arm_abort(seastar::abort_source& source) noexcept {
        abort_at_charge = &source;
    }
    ~arm_abort() { abort_at_charge = nullptr; }
    arm_abort(const arm_abort&) = delete;
    arm_abort& operator=(const arm_abort&) = delete;
};

void scalar_operation(std::string_view name) {
    const bool is_header = name.starts_with("header-");
    const bool encode = name.contains("-encode-");
    const auto a = alignment_bytes(name);
    const auto h = header_bytes(name);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto value = segment_header::make(
                         sc(), model::range_logical_end{100}, alignment(a))
                         .value();
    // Empty extent evidence permits measuring footer codecs without a hidden
    // pre-existing hash owner or a large fixture owner.
    const footer_expectation expected{
      history(0x30, 1, a), runtime::file_position{a}};
    auto verifier = extent_verifier::make(
                      expected.history,
                      scope(100, 100, 0, 0, a, a),
                      work.policy())
                      .value();
    const auto evidence = verifier.finish(work).value();
    const auto wire = is_header ? header_wire(value, h)
                                : footer_wire(evidence.boundary(), expected, h);
    const auto fixture = held_string(wire);
    if (encode) {
        const auto result = measure(name, wire.size(), fixture, [&] {
            return is_header
                     ? encode_segment_header(
                         value, work, available(fixture), charge)
                         .get()
                     : encode_durable_footer(
                         evidence, expected, work, available(fixture), charge)
                         .get();
        });
        require(
          result && result->content_equals(wire),
          "scalar encoding changed bytes");
        return;
    }
    auto source = buffer(wire, 1024);
    const auto held = fixture.checked_add(retained_cost(source)).value();
    fragmented_buffer_parser input{std::move(source)};
    if (is_header) {
        const auto result = measure(name, wire.size(), held, [&] {
            return decode_segment_header(input, value, {}, memory(held), work)
              .get();
        });
        require(
          result && result->value == value,
          "segment header decoding changed fields");
    } else {
        const auto result = measure(name, wire.size(), held, [&] {
            return decode_durable_footer(input, expected, memory(held), work)
              .get();
        });
        require(
          result && validate_durable_footer(*result, evidence),
          "durable footer decoding changed fields");
    }
    require(input.at_end(), "scalar decoder did not consume exact bytes");
}

void child_operation(std::string_view name) {
    const bool compressed = name.contains("lz4");
    const bool narrow = name.contains("narrow") && !name.contains("narrow-max");
    const bool rejected = name.contains("reject");
    seastar::abort_source abort;
    codec::cooperative_work setup{codec::limits::defaults(), abort};
    if (narrow) {
        const auto wire = assigned_wire(compressed, 4096);
        auto source = buffer(wire, 7);
        const auto held
          = held_string(wire).checked_add(retained_cost(source)).value();
        auto child = validate_encoded_assigned_batch(
                       std::move(source), batch_expected(), memory(held), setup)
                       .get()
                       .value();
        auto config = codec::limits_config{};
        config.max_record_bytes = byte_count{rejected ? 6U : 7U};
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        const auto result = measure(name, wire.size(), held, [&] {
            return child.validate(batch_expected(), memory(held), work).get();
        });
        require(result.has_value() != rejected, "narrow policy result changed");
        if (rejected)
            require(
              result.error().code() == errc::resource_exhausted,
              "narrow policy wrong error");
        require(
          child.bytes().content_equals(wire),
          "narrow policy changed child bytes");
        return;
    }
    auto child
      = large_child(compressed, setup, observation::residual).get().value();
    const auto facts = child.info();
    const auto size = child.bytes().size().value();
    const auto held = retained_cost(child.bytes());
    if (name.contains("make")) {
        auto raw = [&] {
            fragmented_buffer_parser input{std::move(child).release_bytes()};
            auto decoded = model::decode_assigned_batch(
                             input, batch_expected(), memory(held), setup)
                             .get();
            require(
              decoded && input.at_end(),
              "raw child fixture did not decode completely");
            return std::move(decoded->value);
        }();
        const auto raw_held = retained_cost(raw.records());
        const auto result = measure(
          name, raw.records().size().value(), raw_held, [&] {
              return make_encoded_assigned_batch(
                       std::move(raw),
                       compressed ? compression::codec_id::lz4
                                  : compression::codec_id::none,
                       setup,
                       observation::residual,
                       charge)
                .get();
          });
        require(
          result && result->info() == facts,
          "encoded child construction changed facts");
    } else if (name.contains("narrow-max")) {
        auto config = codec::limits_config{};
        config.max_record_headers = item_count{1};
        codec::cooperative_work work{
          codec::limits::make(config).value(), abort};
        const auto result = measure(name, size, held, [&] {
            return child.validate(batch_expected(), memory(held), work).get();
        });
        require(
          result.has_value() && child.info() == facts,
          "maximum child revalidation failed");
    } else if (name.contains("share")) {
        const auto result = measure(name, size, held, [&] {
            return child.share(memory(held), setup).get();
        });
        require(
          result && result->info() == facts
            && result->bytes().size() == child.bytes().size()
            && result->bytes().fragment_at(0)->data()
                 == child.bytes().fragment_at(0)->data(),
          "child share changed facts or backing");
    } else if (name.contains("reuse")) {
        const auto result = measure(name, size, held, [&] {
            return child.validate(batch_expected(), memory(held), setup).get();
        });
        require(
          result.has_value() && child.info() == facts, "child reuse failed");
    } else {
        auto source = std::move(child).release_bytes();
        const auto result = measure(name, size, held, [&] {
            return validate_encoded_assigned_batch(
                     std::move(source), batch_expected(), memory(held), setup)
              .get();
        });
        require(
          result && result->info() == facts,
          "exact child validation changed facts");
    }
}

void extended_wrapper_operation(std::string_view name) {
    const bool wal = name.starts_with("wal-");
    const auto a = alignment_bytes(name);
    const auto target_a = wal ? (a == 512 ? 65536U : 512U) : a;
    const storage_fixture fixture{
      wal ? storage_case::wal : storage_case::block,
      name.contains("lz4"),
      false,
      4096,
      4096,
      target_a,
      a};
    auto held = held_vector(fixture.entries);
    for (const auto* text :
         {&fixture.child,
          &fixture.block,
          &fixture.page,
          &fixture.root,
          &fixture.wire})
        held = held.checked_add(held_string(*text)).value();
    auto bytes = buffer(fixture.wire, 1024);
    held = held.checked_add(retained_cost(bytes)).value();
    fragmented_buffer_parser input{std::move(bytes)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    if (wal) {
        const auto result = measure(name, fixture.wire.size(), held, [&] {
            return decode_wal_prepare(
                     input, fixture.wal_context, memory(held), work)
              .get();
        });
        require(
          result && result->value.batch().bytes().content_equals(fixture.child),
          "extended WAL changed exact child");
    } else {
        const auto result = measure(name, fixture.wire.size(), held, [&] {
            return decode_segment_block(
                     input, fixture.block_context, memory(held), work)
              .get();
        });
        require(
          result && result->value.bytes().content_equals(fixture.wire),
          "extended block changed exact bytes");
    }
    require(input.at_end(), "extended wrapper did not consume exact bytes");
}

void wrapper_operation(std::string_view name) {
    const bool wal = name.starts_with("wal-");
    const bool compressed = name.contains("lz4");
    const bool encode = name.contains("-encode-");
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto child
      = large_child(compressed, work, observation::residual).get().value();
    const auto facts = child.info();
    // Exercise independently selected WAL/target alignment endpoints.
    const auto a = alignment_bytes(name);
    const auto wc = wal_expected(a, a == 512 ? 65536 : 512);
    const auto bc = block_expected(0x30, 1, a, 0, a);
    const auto held_child = retained_cost(child.bytes());
    const auto child_size = child.bytes().size();
    fragmented_buffer wire;
    if (encode) {
        if (wal) {
            auto result = measure(name, child_size.value(), held_child, [&] {
                return encode_wal_prepare(
                         std::move(child),
                         wc,
                         work,
                         observation::residual,
                         charge)
                  .get();
            });
            require(result.has_value(), "maximum WAL encode failed");
            wire = std::move(*result);
        } else {
            auto result = measure(name, child_size.value(), held_child, [&] {
                return encode_segment_block(
                         std::move(child),
                         bc,
                         work,
                         observation::residual,
                         charge)
                  .get();
            });
            require(
              result && result->descriptor().batch() == facts,
              "maximum block encode failed");
            wire = std::move(*result).release_bytes();
        }
    } else {
        wire
          = wal ? encode_wal_prepare(
                    std::move(child), wc, work, observation::residual, charge)
                    .get()
                    .value()
                : std::move(
                    encode_segment_block(
                      std::move(child), bc, work, observation::residual, charge)
                      .get()
                      .value())
                    .release_bytes();
        // Large fragmented input reaches the public fragment admission path.
        wire = codec::bench::copy_layout(
                 std::move(wire), 16384, work, observation::residual, 1024)
                 .get();
        require(
          wire.fragment_count() > 512 && wire.fragment_count() <= 1024,
          "maximum wrapper fixture lost fragmentation");
    }
    const auto size = wire.size().value();
    const auto held = retained_cost(wire);
    fragmented_buffer_parser input{std::move(wire)};
    if (wal) {
        auto read = [&] {
            return decode_wal_prepare(input, wc, memory(held), work).get();
        };
        const auto result = encode ? read() : measure(name, size, held, read);
        require(
          result && result->value.batch().info() == facts
            && result->value.batch().bytes().size() == child_size,
          "WAL decode changed child");
    } else {
        auto read = [&] {
            return decode_segment_block(input, bc, memory(held), work).get();
        };
        const auto result = encode ? read() : measure(name, size, held, read);
        require(
          result && result->value.descriptor().batch() == facts,
          "block decode changed child");
    }
    require(input.at_end(), "wrapper decoder did not consume exact bytes");
}

void rejected_operation(std::string_view name) {
    std::optional<observation::prepared_abort_source> abort;
    const auto setup = observation::observe_setup([&] { abort.emplace(); });
    codec::cooperative_work work{codec::limits::defaults(), *abort};
    auto wire = block_wire(assigned_wire(true, 4096));
    const bool cancel = name == "block-abort";
    const bool pressure = name == "block-pressure";
    if (name == "block-padding") {
        wire.back() = 1;
        repair(wire);
    } else if (name == "block-nested") {
        // Repaired outer integrity must still reject the corrupt child.
        wire[32 + 120 + 4096] ^= 1;
        repair(wire);
    }
    auto source = buffer(wire, 7);
    const auto held = held_string(wire)
                        .checked_add(retained_cost(source))
                        .value()
                        .checked_add(setup)
                        .value();
    fragmented_buffer_parser input{std::move(source)};
    input.push_checkpoint().value();
    auto budget = memory(held);
    if (pressure) budget.operation_remaining = {};
    if (cancel) budget.charge = cancelling_charge;
    codec::result<decoded_segment_block> result = codec::failure(
      codec::error{errc::invalid_argument});
    const auto read = [&] {
        return decode_segment_block(input, block_expected(), budget, work)
          .get();
    };
    if (cancel) {
        arm_abort armed{*abort};
        result = measure(name, wire.size(), held, read);
    } else {
        result = measure(name, wire.size(), held, read);
    }
    const auto expected = cancel                    ? errc::aborted
                          : pressure                ? errc::resource_exhausted
                          : name == "block-padding" ? errc::malformed_data
                                                    : errc::corrupt_data;
    require(
      !result && result.error().code() == expected,
      "wrapper rejection changed category");
    require(
      !cancel || abort->abort_requested(), "cancellation boundary not reached");
    require(
      input.bytes_consumed() == byte_count{} && input.checkpoint_depth() == 1,
      "rejection changed parser transaction");
}
} // namespace

void format_operation(std::string_view name) {
    if (name.starts_with("header-") || name.starts_with("durable-"))
        scalar_operation(name);
    else if (name.starts_with("child-"))
        child_operation(name);
    else if (
      name == "block-abort" || name == "block-pressure"
      || name == "block-padding" || name == "block-nested")
        rejected_operation(name);
    else if (name.contains("-extended-"))
        extended_wrapper_operation(name);
    else if (name.starts_with("block-") || name.starts_with("wal-"))
        wrapper_operation(name);
    else
        throw std::invalid_argument("unknown storage memory scenario");
}
} // namespace kwaque::storage::qualification
