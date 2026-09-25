#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/collection.h"
#include "src/codec/cooperative.h"
#include "src/codec/error.h"
#include "src/codec/tests/allocation_observer.h"
#include "src/codec/tests/benchmark_buffer.h"
#include "src/codec/tests/memory_qualification_support.h"
#include "src/codec/tests/prepared_abort_source.h"
#include "src/codec/transaction.h"
#include "src/compression/compression.h"
#include "src/model/batch_builder.h"
#include "src/model/batch_codec.h"
#include "src/model/batch_rewrite.h"
#include "src/model/checkpoint_codec.h"
#include "src/model/fingerprint.h"
#include "src/model/record_codec.h"
#include "src/model/record_scan.h"
#include "src/model/tests/checkpoint_test_support.h"
#include "src/model/tests/model_bench_fixture.h"
#include "src/runtime/time.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>

#include <boost/program_options.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <lz4.h>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {
namespace codec = kwaque::codec;
namespace compression = kwaque::compression;
namespace model = kwaque::model;
namespace bench = model::bench;
namespace observation = codec::testing;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer;
using kwaque::bytes::fragmented_buffer_parser;
using observation::measure;
using observation::require;
using observation::retained_cost;
constexpr byte_count maximum{8U << 20U};

using observation::prepared_abort_source;

// The returned charge is unchanged. Only these isolated cancellation cases
// request abort at a reached accounting boundary, without another task or an
// allocation in the callback. Each process runs one operation on one reactor.
struct abort_point final {
    seastar::abort_source& source;
    std::uint64_t remaining;
    std::uint64_t request{0};
    bool fired{false};
};
thread_local abort_point* active_abort = nullptr;
byte_count observed_charge(byte_count requested) noexcept {
    if (
      active_abort && !active_abort->fired
      && (active_abort->request == 0 || active_abort->request == requested.value())
      && --active_abort->remaining == 0) {
        active_abort->fired = true;
        active_abort->source.request_abort();
    }
    return bench::capacity_bound(requested);
}
class arm_abort final {
public:
    explicit arm_abort(abort_point* point) noexcept { active_abort = point; }
    ~arm_abort() { active_abort = nullptr; }
    arm_abort(const arm_abort&) = delete;
    arm_abort& operator=(const arm_abort&) = delete;
};

codec::decode_budget memory(byte_count remaining = observation::residual) {
    return {remaining, byte_count{1U << 20U}, observed_charge};
}
byte_count held_with(const bench::model_fixture& fixture, byte_count input) {
    return fixture.cache_charge.checked_add(input).value();
}

void record_read(std::string_view scenario) {
    const bool scan = scenario == "record-scan";
    bench::model_fixture fixture{
      scan ? 4096U : 8U,
      scan ? 0U : (scenario == "record-materialize-max" ? 1048565U : 64U),
      observation::fixture_fragment_bytes};
    fixture.initialize().get();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    if (scan) {
        auto source = fixture.assigned(work).get();
        const auto context = source.context().submitted();
        const auto held = held_with(fixture, retained_cost(source.records()));
        const auto scanned = measure(
          scenario, source.records().size().value(), held, [&] {
              auto scanner = model::record_region_scanner::make(
                               std::move(source).release_records(),
                               {context.original_timestamp_base(),
                                context.original_count(),
                                item_count{fixture.count},
                                item_count{fixture.count},
                                model::record_region_kind::dense_original},
                               memory(fixture.remaining),
                               work)
                               .get()
                               .value();
              std::size_t count = 0;
              while (scanner.next(work).get().value())
                  ++count;
              const bool complete = scanner.complete();
              scanner.close(work).get();
              return std::pair{count, complete};
          });
        require(
          scanned.first == fixture.count && scanned.second,
          "record scan did not validate its complete region");
        return;
    }
    auto wire = fixture.record_wire.share();
    const auto held = held_with(fixture, retained_cost(wire));
    fragmented_buffer_parser input{std::move(wire)};
    const auto budget = codec::reserve_decode_input(
                          input, work.policy(), memory(fixture.remaining))
                          .value();
    auto result = measure(
      scenario, fixture.record_wire.size().value(), held, [&] {
          return model::decode_record(
                   input,
                   {kwaque::runtime::wall_time{100},
                    model::range_logical_count{8},
                    item_count{4096}},
                   budget,
                   work)
            .get();
      });
    require(
      result.has_value() && input.at_end(), "record materialization failed");
    require(
      model::records_equal(result->value, *fixture.value, work).get().value(),
      "record materialization changed fields");
}

void batch_operation(std::string_view scenario) {
    const bool headers = scenario.ends_with("headers");
    bench::model_fixture fixture{
      headers ? 4096U : 8U,
      headers ? 0U : 1048565U,
      observation::fixture_fragment_bytes};
    fixture.initialize().get();
    prepared_abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    if (scenario.starts_with("batch-build")) {
        auto result = measure(
          scenario,
          fixture.record_region_bytes.value(),
          fixture.cache_charge,
          [&] { return fixture.build(work).get(); });
        require(
          result.fingerprint() == *fixture.digest
            && result.records().size() == fixture.record_region_bytes,
          "batch construction changed its original projection");
        return;
    }
    if (scenario == "batch-fingerprint-max") {
        auto source = fixture.submitted(work).get();
        const auto held = held_with(fixture, retained_cost(source.records()));
        auto digest = measure(
          scenario, source.records().size().value(), held, [&] {
              return model::compute_submitted_fingerprint(
                       source.context(), source.records(), work)
                .get();
          });
        require(
          digest && *digest == *fixture.digest, "batch fingerprint changed");
        return;
    }
    if (scenario.starts_with("batch-rewrite")) {
        auto source = fixture.assigned(work).get();
        const auto original = source.context();
        const auto held = held_with(fixture, retained_cost(source.records()));
        const bool cancel = scenario.ends_with("abort");
        abort_point point{abort, 256};
        codec::result<model::assigned_batch> result = codec::failure(
          codec::error{errc::invalid_argument});
        {
            arm_abort armed{cancel ? &point : nullptr};
            result = measure(
              scenario, source.records().size().value(), held, [&] {
                  return model::rewrite_assigned_batch(
                           std::move(source),
                           fixture.selected,
                           memory(fixture.remaining),
                           work)
                    .get();
              });
        }
        if (cancel) {
            require(
              point.fired && !result && result.error().code() == errc::aborted,
              "rewrite did not reach cancellation");
        } else {
            require(
              result && result->fingerprint() == *fixture.digest
                && result->context().logical_span() == original.logical_span()
                && result->context().retained_count().value()
                     == fixture.selected.size(),
              "rewrite changed original coverage or digest");
        }
        return;
    }
    if (scenario == "batch-lz4-encode") {
        auto source = fixture.assigned(work).get();
        const auto held = held_with(fixture, retained_cost(source.records()));
        auto encoded = measure(
          scenario, source.records().size().value(), held, [&] {
              return model::encode_assigned_batch(
                       std::move(source),
                       compression::codec_id::lz4,
                       work,
                       fixture.remaining,
                       observed_charge)
                .get();
          });
        require(encoded.has_value(), "compressed batch encoding failed");
        fragmented_buffer_parser verify{std::move(*encoded)};
        auto decoded = model::decode_assigned_batch(
                         verify,
                         bench::expected_context(),
                         codec::reserve_decode_input(
                           verify, work.policy(), memory(fixture.remaining))
                           .value(),
                         work)
                         .get();
        require(
          decoded && verify.at_end()
            && decoded->value.fingerprint() == *fixture.digest
            && decoded->value.records().size() == maximum,
          "compressed batch failed complete verification");
        return;
    }
    auto wire = fixture.assigned_wire.share();
    if (scenario == "batch-lz4-decode") {
        wire = model::encode_assigned_batch(
                 fixture.assigned(work).get(),
                 compression::codec_id::lz4,
                 work,
                 fixture.remaining,
                 observed_charge)
                 .get()
                 .value();
    }
    const auto held = held_with(fixture, retained_cost(wire));
    fragmented_buffer_parser input{std::move(wire)};
    auto budget = codec::reserve_decode_input(
                    input, work.policy(), memory(fixture.remaining))
                    .value();
    const bool cancel = scenario == "batch-decode-abort";
    const bool pressure = scenario == "batch-decode-pressure";
    if (pressure) budget.metadata_remaining = {};
    abort_point point{abort, 32};
    codec::result<model::decoded_assigned_batch> result = codec::failure(
      codec::error{errc::invalid_argument});
    {
        arm_abort armed{cancel ? &point : nullptr};
        result = measure(
          scenario, fixture.record_region_bytes.value(), held, [&] {
              return model::decode_assigned_batch(
                       input, bench::expected_context(), budget, work)
                .get();
          });
    }
    if (cancel || pressure) {
        require(
          !result
            && result.error().code()
                 == (cancel ? errc::aborted : errc::resource_exhausted)
            && input.bytes_consumed() == byte_count{}
            && input.checkpoint_depth() == 0 && (!cancel || point.fired),
          "rejected batch changed its parent or missed cancellation");
    } else {
        require(
          result && input.at_end()
            && result->value.fingerprint() == *fixture.digest
            && result->value.context().retained_count().value() == fixture.count
            && result->value.header_count().value() == (headers ? 4096U : 0U)
            && result->value.records().size() == fixture.record_region_bytes,
          "batch decoding changed validated content");
    }
}

void lz4_operation(std::string_view scenario) {
    seastar::abort_source setup_abort;
    codec::cooperative_work setup{codec::limits::defaults(), setup_abort};
    auto input = codec::bench::patterned_buffer(
                   maximum.value(),
                   65536,
                   codec::bench::payload_pattern::mixed,
                   setup,
                   observation::residual)
                   .get();
    const bool decode = scenario.starts_with("lz4-decompress");
    const bool cancel = scenario.ends_with("abort");
    const bool corrupt = scenario.ends_with("corrupt");
    if (decode) {
        const auto cost = input.allocation_cost(observed_charge).value();
        auto encoded
          = compression::compress_lz4(
              std::move(input),
              byte_count{16U << 20U},
              setup,
              codec::detail::consume_decode_budget(
                setup.policy(),
                memory(),
                cost.backing,
                cost.descriptors.checked_add(cost.share_controls).value(),
                {},
                0)
                .value())
              .get()
              .value();
        input = std::move(encoded.value);
    }
    if (corrupt) {
        // Truncate the footer through the public immutable-owner operation.
        const auto prefix = input.size().checked_sub(byte_count{1}).value();
        auto truncated = input.share(byte_count{}, prefix).value();
        input = std::move(truncated);
    }
    const auto held = retained_cost(input);
    const auto cost = input.allocation_cost(observed_charge).value();
    const auto budget
      = codec::detail::consume_decode_budget(
          setup.policy(),
          memory(),
          cost.backing,
          cost.descriptors.checked_add(cost.share_controls).value(),
          {},
          0)
          .value();
    prepared_abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    // The first matching request is pre-admission; the second is the reached
    // native state/buffer allocation under the pinned independent-block
    // profile.
    abort_point point{
      abort,
      2,
      decode ? 65540U : static_cast<std::uint64_t>(LZ4_sizeofState())};
    codec::result<compression::owned_result> result = codec::failure(
      codec::error{errc::invalid_argument});
    {
        arm_abort armed{cancel ? &point : nullptr};
        result = measure(scenario, maximum.value(), held, [&] {
            return decode
                     ? compression::decompress_lz4(
                         std::move(input), maximum, work, budget)
                         .get()
                     : compression::compress_lz4(
                         std::move(input), byte_count{16U << 20U}, work, budget)
                         .get();
        });
    }
    if (cancel || corrupt) {
        require(
          !result
            && result.error().code()
                 == (cancel ? errc::aborted : errc::malformed_data)
            && (!cancel || point.fired),
          "LZ4 failure did not discard the complete operation");
        return;
    }
    require(result.has_value(), "LZ4 operation failed");
    if (!decode) {
        const auto retained = result->retained;
        result = compression::decompress_lz4(
                   std::move(result->value),
                   maximum,
                   setup,
                   codec::detail::consume_decode_budget(
                     setup.policy(),
                     memory(),
                     retained.backing,
                     retained.descriptors.checked_add(retained.share_controls)
                       .value(),
                     {},
                     0)
                     .value())
                   .get();
    }
    require(
      result && result->value.size() == maximum, "LZ4 changed expanded length");
    auto expected = codec::bench::patterned_buffer(
                      maximum.value(),
                      65536,
                      codec::bench::payload_pattern::mixed,
                      setup,
                      observation::residual)
                      .get();
    require(
      codec::bench::buffers_equal(result->value, expected, setup).get(),
      "LZ4 changed payload bytes");
}

void checkpoint_operation(std::string_view scenario) {
    namespace fixture = model::testing::checkpoint_fixture;
    using source_type = seastar::chunked_fifo<model::range_cursor, 16>;
    const bool unordered = scenario.starts_with("checkpoint-unordered-");
    const bool sorted = scenario.starts_with("checkpoint-sorted-");
    const bool cancel = scenario.ends_with("-abort");
    const bool duplicate = scenario.ends_with("-duplicate");
    const bool over = scenario.ends_with("-4097");
    const bool partial = scenario.ends_with("-partial");
    const bool free_chunks = scenario.ends_with("-free-chunks");
    const std::uint32_t count = over ? 4097U : 4096U;
    auto policy = codec::limits_config{};
    if (scenario.ends_with("-count-limit"))
        policy.max_checkpoint_cursors = item_count{4095};
    if (scenario.ends_with("-object-limit"))
        policy.max_object_entries = item_count{4095};
    if (scenario.ends_with("-allocation-limit"))
        policy.max_allocation_bytes = byte_count{1};
    if (scenario.ends_with("-scratch-limit"))
        policy.max_scratch_bytes = byte_count{1};
    if (scenario.ends_with("-work-limit"))
        policy.max_work_items = item_count{1};
    std::optional<prepared_abort_source> abort;
    const auto abort_cost = observation::observe_setup(
      [&] { abort.emplace(); });
    codec::cooperative_work work{codec::limits::make(policy).value(), *abort};
    abort_point point{*abort, 1};
    const auto charge_of = [](std::uint64_t bytes) {
        return bench::capacity_bound(byte_count{bytes});
    };
    const auto budget_for = [&](
                              byte_count other,
                              byte_count held_metadata = byte_count{}) {
        auto budget = memory(observation::residual.checked_sub(other).value());
        budget.metadata_remaining
          = budget.metadata_remaining.checked_sub(held_metadata).value();
        if (scenario.ends_with("-metadata-pressure"))
            budget.metadata_remaining = {};
        if (scenario.ends_with("-operation-pressure"))
            budget.operation_remaining = {};
        return budget;
    };
    const bool denied = over || scenario.ends_with("-limit")
                        || scenario.ends_with("-pressure");
    const auto check_failure = [&](const auto& result) {
        const auto code = cancel        ? errc::aborted
                          : free_chunks ? errc::invalid_argument
                          : duplicate   ? errc::malformed_data
                                        : errc::resource_exhausted;
        require(
          !result && result.error().code() == code,
          "checkpoint rejection changed category");
        require(
          !cancel || point.fired, "checkpoint abort boundary was not reached");
    };
    if (sorted || unordered) {
        if (unordered) {
            source_type source;
            if (free_chunks) source.reserve(count + 32U);
            // A popped leading slot leaves a partial first native chunk without
            // creating a cached free chunk; reverse order exercises run
            // merging.
            if (partial) source.push_back(fixture::numbered(count + 1U));
            for (std::uint32_t i = count; i != 0; --i)
                source.push_back(
                  fixture::numbered(duplicate && i == 1 ? count : i));
            if (partial) source.pop_front();
            const auto chunks = (source.size() + 15U) / 16U + 1U
                                + source.nfree_chunks();
            const auto held
              = abort_cost
                  .checked_add(
                    byte_count{
                      chunks
                      * charge_of(
                          codec::detail::
                            collection_chunk_bytes<model::range_cursor, 16>())
                          .value()})
                  .value();
            auto budget = budget_for(
              abort_cost); // The owning API includes source chunks.
            codec::result<model::constructed_read_checkpoint> result
              = codec::failure(codec::error{errc::invalid_argument});
            {
                arm_abort armed{cancel ? &point : nullptr};
                result = measure(scenario, count * 24U, held, [&] {
                    return model::make_read_checkpoint_from_unordered(
                             fixture::topic(), std::move(source), budget, work)
                      .get();
                });
            }
            if (denied || cancel || duplicate || free_chunks)
                check_failure(result);
            else {
                require(
                  result && result->value.cursors().size() == count,
                  "unordered checkpoint lost entries");
                for (std::uint32_t i = 0; i < count; ++i)
                    require(
                      result->value.cursors()[i] == fixture::numbered(i + 1U),
                      "unordered checkpoint changed canonical order");
            }
            // Both native moves and pretransfer rejection leave an inspectable
            // FIFO. Only unsupported cleanup/free chunks retain the donor.
            // NOLINTBEGIN(bugprone-use-after-move)
            require(
              source.size()
                == (free_chunks || scenario.ends_with("-work-limit") ? count : 0U),
              "unordered checkpoint donor boundary changed");
            // NOLINTEND(bugprone-use-after-move)
            if (result) {
                const auto retained = charge_of(
                  result->value.cursor_capacity().value()
                  * sizeof(model::range_cursor));
                require(
                  result->remaining.operation_remaining
                      == budget.operation_remaining.checked_sub(retained)
                           .value()
                    && result->remaining.metadata_remaining
                         == budget.metadata_remaining.checked_sub(retained)
                              .value(),
                  "unordered checkpoint refunded persistent metadata");
            }
            return;
        }
        std::vector<model::range_cursor> entries;
        entries.reserve(count);
        for (std::uint32_t i = 1; i <= count; ++i)
            entries.push_back(fixture::numbered(i));
        if (duplicate) entries.back() = entries.front();
        const auto held = abort_cost
                            .checked_add(charge_of(
                              entries.capacity() * sizeof(model::range_cursor)))
                            .value();
        auto budget = budget_for(
          held, charge_of(entries.capacity() * sizeof(model::range_cursor)));
        const auto result = [&] {
            arm_abort armed{cancel ? &point : nullptr};
            return measure(scenario, count * 24U, held, [&] {
                return model::make_read_checkpoint(
                         fixture::topic(), entries, budget, work)
                  .get();
            });
        }();
        if (denied || cancel || duplicate)
            check_failure(result);
        else {
            require(
              result && std::ranges::equal(result->value.cursors(), entries),
              "sorted checkpoint changed entries");
            const auto retained = charge_of(
              result->value.cursor_capacity().value()
              * sizeof(model::range_cursor));
            require(
              result->remaining.operation_remaining
                  == budget.operation_remaining.checked_sub(retained).value()
                && result->remaining.metadata_remaining
                     == budget.metadata_remaining.checked_sub(retained).value(),
              "sorted checkpoint residual lost its vector charge");
        }
        return;
    }
    const std::size_t header = scenario.contains("h4096") ? 4096U
                               : scenario.contains("h41") ? 41U
                                                          : 32U;
    auto wire = fixture::wire(count, header);
    const auto expected = fixture::digest(
      std::string_view{wire}.substr(header));
    auto other
      = abort_cost.checked_add(charge_of(wire.capacity() + 1U)).value();
    if (scenario.starts_with("checkpoint-decode-")) {
        if (duplicate) {
            wire.replace(header + 44U, 16, wire.substr(header + 20U, 16));
            fixture::repair(wire, header);
        }
        if (scenario.ends_with("-corrupt")) wire.back() ^= 1;
        auto topic = scenario.ends_with("-wrong-topic")
                       ? fixture::object<model::topic_id>(2)
                       : fixture::topic();
        if (scenario.ends_with("-byte-limit")) {
            policy.max_checkpoint_bytes = byte_count{wire.size() - 1U};
        }
        codec::cooperative_work decode_work{
          codec::limits::make(policy).value(), *abort};
        auto source = fixture::fragmented(wire, 128);
        const auto held = other.checked_add(retained_cost(source)).value();
        fragmented_buffer_parser input{std::move(source)};
        input.push_checkpoint().value();
        // Input admission uses a valid setup policy before applying the tested
        // narrower codec limit; no source backing is reserved twice.
        auto budget = codec::reserve_decode_input(
                        input,
                        codec::limits::defaults(),
                        memory(
                          observation::residual.checked_sub(other).value()))
                        .value();
        if (scenario.ends_with("-metadata-pressure"))
            budget.metadata_remaining = {};
        if (scenario.ends_with("-operation-pressure"))
            budget.operation_remaining = {};
        auto result = [&] {
            arm_abort armed{cancel ? &point : nullptr};
            return measure(scenario, wire.size(), held, [&] {
                return model::decode_read_checkpoint(
                         input, topic, budget, decode_work)
                  .get();
            });
        }();
        if (
          scenario.ends_with("-corrupt")
          || scenario.ends_with("-wrong-topic")) {
            require(
              !result
                && result.error().code()
                     == (scenario.ends_with("-corrupt") ? errc::corrupt_data : errc::wrong_context),
              "checkpoint decode lost integrity/context checks");
        } else if (denied || cancel || duplicate)
            check_failure(result);
        else
            require(
              result && input.at_end() && result->fingerprint == expected
                && fixture::body_from_value(result->value)
                     == std::string_view{wire}.substr(header),
              "checkpoint decode changed canonical value");
        require(
          input.checkpoint_depth() == 1
            && (result || input.bytes_consumed() == byte_count{}),
          "checkpoint decode changed caller marks");
        if (result) {
            const auto retained = charge_of(
              result->value.cursor_capacity().value()
              * sizeof(model::range_cursor));
            require(
              result->remaining.operation_remaining
                  == budget.operation_remaining.checked_sub(retained).value()
                && result->remaining.metadata_remaining
                     == budget.metadata_remaining.checked_sub(retained).value(),
              "checkpoint decode residual changed");
        }
        return;
    }
    seastar::abort_source setup_abort;
    codec::cooperative_work setup{codec::limits::defaults(), setup_abort};
    auto value = [&] {
        fragmented_buffer_parser input{fixture::fragmented(wire, 4096)};
        auto decoded = model::decode_read_checkpoint(
                         input,
                         fixture::topic(),
                         codec::reserve_decode_input(
                           input,
                           setup.policy(),
                           memory(
                             observation::residual.checked_sub(other).value()))
                           .value(),
                         setup)
                         .get();
        require(
          decoded && input.at_end(),
          "checkpoint fixture did not decode completely");
        return std::move(decoded->value);
    }();
    other = other
              .checked_add(charge_of(
                value.cursor_capacity().value() * sizeof(model::range_cursor)))
              .value();
    if (scenario == "checkpoint-fingerprint-4096") {
        const auto result = measure(scenario, wire.size() - header, other, [&] {
            return model::compute_checkpoint_fingerprint(value, work).get();
        });
        require(
          result && *result == expected, "checkpoint fingerprint changed");
    } else {
        const auto result = [&] {
            arm_abort armed{cancel ? &point : nullptr};
            return measure(scenario, wire.size(), other, [&] {
                return model::encode_read_checkpoint(
                         value,
                         work,
                         budget_for(other).operation_remaining,
                         observed_charge)
                  .get();
            });
        }();
        if (denied || cancel)
            check_failure(result);
        else
            require(
              result && result->fingerprint == expected
                && result->bytes.content_equals(wire),
              "checkpoint encoding changed bytes");
    }
}

int exercise(std::string_view scenario) {
    observation::report_profile();
    std::printf("compiler=%s\n", __clang_version__);
    if (scenario == "capabilities") return 0;
    observation::require_effective_policy();
    if (
      scenario == "record-materialize" || scenario == "record-materialize-max"
      || scenario == "record-scan")
        record_read(scenario);
    else if (scenario.starts_with("batch-"))
        batch_operation(scenario);
    else if (scenario.starts_with("lz4-"))
        lz4_operation(scenario);
    else if (scenario.starts_with("checkpoint-"))
        checkpoint_operation(scenario);
    else
        throw std::invalid_argument("unknown memory qualification scenario");
    std::puts("status=ok");
    return 0;
}
} // namespace

int main(int argc, char** argv) {
    if (!observation::install_crypto_allocation_observation()) {
        std::fputs(
          "crypto allocation hooks require a fresh process before "
          "initialization\n",
          stderr);
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
