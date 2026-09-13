#include "src/model/tests/record_fuzz_cases.h"

#include "src/model/batch_codec.h"
#include "src/model/batch_rewrite.h"
#include "src/model/record_codec.h"
#include "src/model/tests/record_fuzz_oracle.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/later.hh>

#include <algorithm>
#include <array>
#include <bit>
#include <exception>
#include <limits>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace kwaque::model::testing {
namespace {
using bytes::fragmented_buffer;
using bytes::fragmented_buffer_parser;
using namespace std::literals;
constexpr codec::field_context coordinates{.origin = 1024};
void require(bool condition) {
    if (!condition) __builtin_trap();
}
byte_count charge(byte_count request) noexcept {
    if (request.value() == 0) return {};
    if (request.value() > (std::uint64_t{1} << 62U))
        return byte_count{UINT64_MAX};
    return byte_count{
      2U * std::bit_ceil(std::max(request.value(), std::uint64_t{16}))};
}
codec::decode_budget memory() {
    return {byte_count{32U << 20U}, byte_count{1U << 20U}, charge};
}
template<typename Id>
Id object(std::uint8_t first) {
    std::array<std::uint8_t, 16> value{};
    for (std::size_t i = 0; i < value.size(); ++i)
        value[i] = static_cast<std::uint8_t>(first + i);
    return Id::make(value).value();
}
batch_decode_expectation expected(bool wrong) {
    return {object<topic_id>(wrong ? 34 : 33), object<range_id>(65)};
}
std::string flatten(const fragmented_buffer& bytes) {
    std::string result;
    for (const auto fragment : bytes)
        result.append(fragment.data(), fragment.size());
    return result;
}
fragmented_buffer_parser
parser(std::string_view raw, std::uint8_t layout, std::uint8_t depth) {
    const auto full = "p" + std::string{raw};
    std::vector<seastar::temporary_buffer<char>> parts;
    const auto width = layout % 3U == 0 ? full.size()
                                        : std::max<std::size_t>(
                                            layout % 3U == 1 ? 7 : 67,
                                            (full.size() + 127U) / 128U);
    for (std::size_t at = 0; at < full.size();) {
        const auto size = std::min(
          full.size() - at,
          layout % 3U == 1 && at < 16 ? std::size_t{1} : width);
        seastar::temporary_buffer<char> part{size};
        std::copy_n(full.data() + at, size, part.get_write());
        parts.push_back(std::move(part));
        at += size;
    }
    auto result = fragmented_buffer_parser{
      fragmented_buffer::copy_from_fragments(parts).value()};
    result.skip(byte_count{1}).value();
    for (std::uint8_t i = 0; i < depth; ++i)
        result.push_checkpoint().value();
    return result;
}
void cursor_matches(
  fragmented_buffer_parser& input,
  std::string_view raw,
  std::size_t used,
  std::uint8_t depth) {
    require(
      input.bytes_consumed().value() == 1U + used
      && input.checkpoint_depth() == depth);
    std::string remaining(raw.size() - used, '\0');
    require(input.peek_to(std::span<char>{remaining}).has_value());
    require(remaining == raw.substr(used));
}
std::string
record_bytes(std::string_view data, std::uint64_t delta, bool dense_first) {
    const auto byte = [&](std::size_t i) {
        return data.empty() ? std::uint8_t{0}
                            : static_cast<std::uint8_t>(data[i % data.size()]);
    };
    std::string body(1, '\0');
    body += varuint(
      dense_first ? 0U : (delta % 2U == 0 ? 2U * delta : 2U * delta - 1U));
    body += varuint(delta);
    for (const auto control : {byte(0), byte(1)}) {
        if (control % 3U == 0)
            body += varuint(1);
        else {
            const auto size = control % 3U == 1
                                ? std::size_t{0}
                                : std::min<std::size_t>(data.size(), 127);
            body += varuint(2U * size);
            body.append(data.substr(0, size));
        }
    }
    const auto headers = byte(2) % 5U;
    body += varuint(headers);
    for (std::uint8_t i = 0; i < headers; ++i) {
        const auto length = byte(3 + i) % 4U;
        body += varuint(length);
        body.append(length, static_cast<char>(byte(7 + i)));
        body += byte(i) % 2U == 0 ? varuint(1) : varuint(0);
    }
    return varuint(body.size()) + body;
}
std::string batch_bytes(
  std::string_view data, bool assigned, bool extension, std::uint8_t count) {
    const auto original = 1U + count % 8U;
    std::string records;
    std::uint64_t headers = 0;
    for (std::uint64_t i = 0; i < original; ++i) {
        auto record = record_bytes(data, i, i == 0);
        headers += probe_record(record, 100, original, 4096, true).headers;
        records += record;
    }
    auto body = identity_bytes();
    body.resize(assigned ? 184 : 168, '\0');
    put(body, 136, 100, 8);
    put(body, 144, original, 4);
    put(body, 148, original, 4);
    put(body, 152, headers, 4);
    put(body, 158, 1, 2);
    put(body, 160, records.size(), 4);
    put(body, 164, records.size(), 4);
    if (assigned) {
        put(body, 168, 100, 8);
        put(body, 176, 100 + original, 8);
    }
    const auto digest = fingerprint(body, records);
    for (std::size_t i = 0; i < digest.size(); ++i)
        body[104 + i] = static_cast<char>(digest[i]);
    body += records;
    return frame(std::move(body), assigned, extension);
}
void mutate(
  std::string& wire, std::uint8_t mutation, std::uint8_t operand, bool batch) {
    if (mutation % 32U == 0) return;
    if (mutation % 32U == 1 && !wire.empty()) {
        wire[operand % wire.size()] ^= static_cast<char>(0x80);
        return;
    }
    if (mutation % 32U == 2) {
        wire.resize(operand % (wire.size() + 1U));
        return;
    }
    if (!batch) {
        if (!wire.empty()) {
            const auto index = mutation % 32U == 3 ? std::size_t{0}
                               : mutation % 32U == 4
                                 ? std::size_t{1}
                                 : std::min<std::size_t>(
                                     operand, wire.size() - 1U);
            if (index < wire.size())
                wire[index] = static_cast<char>(
                  mutation % 32U == 3   ? 0xffU
                  : mutation % 32U == 4 ? 1U
                                        : operand);
        }
        return;
    }
    if (wire.size() < 32) return;
    const auto header = little(wire, 10, 2);
    if (mutation % 32U == 3) {
        put(wire, 4, operand % 12U, 2);
        return;
    }
    if (mutation % 32U == 4) {
        put(wire, 10, 31, 2);
        return;
    }
    if (mutation % 32U == 5) {
        put(wire, 12, 0xffffffffU, 4);
        return;
    }
    if (mutation % 32U == 6) {
        put(wire, 8, 2, 2);
        return;
    }
    if (mutation % 32U == 7) {
        put(wire, 16, 1, 8);
        return;
    }
    if (header < 32 || header > wire.size()) return;
    const auto body = static_cast<std::size_t>(header);
    constexpr std::array<std::size_t, 19> offsets{
      0,
      16,
      24,
      32,
      40,
      56,
      72,
      80,
      96,
      104,
      136,
      144,
      148,
      152,
      156,
      157,
      158,
      160,
      164};
    const auto which = static_cast<std::size_t>(
      (mutation % 32U - 8U) % offsets.size());
    const auto at = body + offsets[which];
    if (at < wire.size()) wire[at] = static_cast<char>(operand);
    if (mutation % 32U == 30 && !wire.empty())
        wire.back() = static_cast<char>(operand);
    if (mutation % 32U == 31 && wire.size() < record_fuzz_max_input - 2U)
        wire.push_back('x');
}
template<typename F>
auto joined(F&& function, bool queued, seastar::abort_source& abort) {
    using result_type = decltype(function());
    std::optional<seastar::future<>> observer;
    if (queued)
        observer.emplace(
          seastar::yield().then([&abort] { abort.request_abort(); }));
    std::optional<result_type> result;
    std::exception_ptr exception;
    try {
        result.emplace(function());
    } catch (...) {
        exception = std::current_exception();
    }
    if (observer) observer->get();
    if (exception) std::rethrow_exception(exception);
    return std::move(*result);
}
void compare(
  errc actual,
  errc oracle,
  std::uint8_t flags,
  std::uint8_t depth,
  bool queued,
  bool invalid_origin) {
    if (invalid_origin)
        require(actual == errc::invalid_argument);
    else if ((flags & 128U) != 0)
        require(actual == errc::aborted);
    else if (queued && actual == errc::aborted)
        return;
    else if (depth == 8)
        require(actual == errc::resource_exhausted);
    else if ((flags & 96U) != 0)
        require(
          actual != errc::success
          && (actual == oracle || actual == errc::resource_exhausted));
    else
        require(actual == oracle);
}
void exercise_record(
  std::string_view raw,
  std::uint8_t layout,
  std::uint8_t depth,
  std::uint8_t flags,
  bool queued,
  bool narrow,
  std::uint8_t origin_flags) {
    const bool complete = (flags & 8U) != 0;
    auto context = coordinates;
    const bool invalid_origin = (origin_flags & 8U) != 0;
    if ((origin_flags & 12U) != 0)
        context.origin = UINT64_MAX - raw.size() - (invalid_origin ? 0U : 1U);
    const auto oracle
      = invalid_origin
          ? record_probe{.error = errc::invalid_argument}
          : probe_record(
              raw, 100, 4096, 4096, complete, UINT64_MAX - context.origin - 1U);
    auto input = parser(raw, layout, depth);
    codec::limits_config config;
    if (narrow) {
        config.max_work_bytes = byte_count{16384};
        config.max_work_items = item_count{64};
    }
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    auto budget = codec::reserve_decode_input(
                    input,
                    work.policy(),
                    memory(),
                    invalid_origin ? codec::field_context{} : context)
                    .value();
    if ((flags & 32U) != 0) budget.operation_remaining = byte_count{};
    if ((flags & 64U) != 0) budget.metadata_remaining = byte_count{};
    if ((flags & 128U) != 0) abort.request_abort();
    auto actual = joined(
      [&] {
          return decode_record(
                   input,
                   {runtime::wall_time{100},
                    range_logical_count{4096},
                    item_count{4096}},
                   budget,
                   work,
                   context,
                   complete ? codec::input_boundary::complete
                            : codec::input_boundary::open)
            .get();
      },
      queued,
      abort);
    compare(
      actual ? errc::success : actual.error().code(),
      oracle.error,
      flags,
      depth,
      queued,
      invalid_origin);
    cursor_matches(input, raw, actual ? oracle.used : 0, depth);
    if (actual) {
        seastar::abort_source fresh;
        codec::cooperative_work next{codec::limits::defaults(), fresh};
        const auto reserved
          = reserve_record_input(actual->value, next, memory()).get().value();
        auto encoded
          = encode_record(
              actual->value, next, reserved.operation_remaining, charge)
              .get();
        require(encoded.has_value());
        require(flatten(*encoded) == raw.substr(0, oracle.used));
        require(actual->value.logical_delta().value() == oracle.delta);
        next.drain(next.byte_quantum(), next.item_quantum()).get();
    }
}
template<bool Assigned>
void exercise_batch(
  std::string_view raw,
  std::uint8_t layout,
  std::uint8_t depth,
  std::uint8_t flags,
  bool queued,
  bool narrow,
  std::uint8_t origin_flags) {
    const bool complete = (flags & 8U) != 0, wrong = (flags & 16U) != 0;
    auto context = coordinates;
    const bool invalid_origin = (origin_flags & 8U) != 0;
    if ((origin_flags & 12U) != 0)
        context.origin = UINT64_MAX - raw.size() - (invalid_origin ? 0U : 1U);
    const auto oracle
      = invalid_origin
          ? batch_probe{.error = errc::invalid_argument}
          : probe_batch(
              raw, Assigned, complete, wrong, UINT64_MAX - context.origin - 1U);
    auto input = parser(raw, layout, depth);
    codec::limits_config config;
    if (narrow) {
        config.max_work_bytes = byte_count{16384};
        config.max_work_items = item_count{64};
    }
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    auto budget = codec::reserve_decode_input(
                    input,
                    work.policy(),
                    memory(),
                    invalid_origin ? codec::field_context{} : context)
                    .value();
    if ((flags & 32U) != 0) budget.operation_remaining = byte_count{};
    if ((flags & 64U) != 0) budget.metadata_remaining = byte_count{};
    if ((flags & 128U) != 0) abort.request_abort();
    auto actual = joined(
      [&] {
          if constexpr (Assigned)
              return decode_assigned_batch(
                       input,
                       expected(wrong),
                       budget,
                       work,
                       context,
                       complete ? codec::input_boundary::complete
                                : codec::input_boundary::open)
                .get();
          else
              return decode_submitted_batch(
                       input,
                       expected(wrong),
                       budget,
                       work,
                       context,
                       complete ? codec::input_boundary::complete
                                : codec::input_boundary::open)
                .get();
      },
      queued,
      abort);
    compare(
      actual ? errc::success : actual.error().code(),
      oracle.error,
      flags,
      depth,
      queued,
      invalid_origin);
    cursor_matches(input, raw, actual ? oracle.used : 0, depth);
    if (actual) {
        require(actual->value.fingerprint().bytes() == oracle.digest);
        require(actual->value.header_count().value() == oracle.headers);
        require(
          flatten(actual->value.records())
          == raw.substr(oracle.records_at, oracle.record_bytes));
        if constexpr (Assigned) {
            require(
              actual->value.context().logical_span().begin().value()
                == oracle.begin
              && actual->value.context().logical_span().end().value()
                   == oracle.end);
            require(
              actual->fingerprint_verification
              == (oracle.original == oracle.retained ? batch_fingerprint_verification::recomputed : batch_fingerprint_verification::carried));
        }
        seastar::abort_source fresh;
        codec::cooperative_work next{codec::limits::defaults(), fresh};
        auto encoded = [&] {
            if constexpr (Assigned)
                return encode_assigned_batch(
                         std::move(actual->value),
                         next,
                         memory().operation_remaining,
                         charge)
                  .get();
            else
                return encode_submitted_batch(
                         std::move(actual->value),
                         next,
                         memory().operation_remaining,
                         charge)
                  .get();
        }();
        require(encoded.has_value());
        const auto rebuilt = flatten(*encoded);
        require(
          probe_batch(rebuilt, Assigned, true, wrong).error == errc::success);
        if (oracle.canonical)
            require(rebuilt == raw.substr(0, oracle.used));
        else
            require(
              std::string_view{rebuilt}.substr(32)
              == raw.substr(little(raw, 10, 2), little(raw, 12, 4)));
        next.drain(next.byte_quantum(), next.item_quantum()).get();
    }
}
void exercise_rewrite(
  std::string_view raw, std::uint8_t selection_bits, std::uint8_t second_bits) {
    auto oracle = probe_batch(raw, true, true, false);
    require(oracle.error == errc::success);
    auto input = parser(raw, 1, 0);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto decoded = decode_assigned_batch(
                     input,
                     expected(false),
                     codec::reserve_decode_input(
                       input, work.policy(), memory(), coordinates)
                       .value(),
                     work,
                     coordinates)
                     .get()
                     .value();
    std::vector<range_logical_count> selected;
    std::string kept;
    std::size_t at = oracle.records_at;
    std::uint64_t headers = 0;
    for (std::uint64_t i = 0; i < oracle.retained; ++i) {
        const auto record = probe_record(
          raw.substr(at), oracle.timestamp, oracle.original, 4096, true);
        require(record.error == errc::success);
        if ((selection_bits & (1U << i)) != 0) {
            selected.emplace_back(record.delta);
            kept.append(raw.substr(at, record.used));
            headers += record.headers;
        }
        at += record.used;
    }
    const auto context = decoded.value.context();
    const auto digest = decoded.value.fingerprint();
    if (selected.empty()) {
        auto removed = remove_all_records(std::move(decoded.value), work).get();
        require(removed.has_value());
        require(removed->submitted() == context.submitted());
        require(removed->logical_span() == context.logical_span());
        require(removed->fingerprint() == digest);
        return;
    }
    auto result = rewrite_assigned_batch(
                    std::move(decoded.value), selected, memory(), work)
                    .get();
    require(result.has_value());
    require(flatten(result->records()) == kept);
    require(
      result->context().logical_span() == context.logical_span()
      && result->context().submitted() == context.submitted());
    require(
      result->fingerprint() == digest
      && result->header_count().value() == headers);
    std::vector<range_logical_count> second;
    for (std::uint64_t i = 0; i < oracle.original; ++i)
        if ((second_bits & (1U << i)) != 0) second.emplace_back(i);
    if (second.empty()) {
        auto removed = remove_all_records(std::move(*result), work).get();
        require(removed.has_value());
        require(removed->submitted() == context.submitted());
        require(removed->logical_span() == context.logical_span());
        require(removed->fingerprint() == digest);
        return;
    }
    const bool valid = std::all_of(
      second.begin(), second.end(), [&](auto item) {
          return std::find(selected.begin(), selected.end(), item)
                 != selected.end();
      });
    auto rewritten = rewrite_assigned_batch(
                       std::move(*result), second, memory(), work)
                       .get();
    require(rewritten.has_value() == valid);
    if (rewritten)
        require(
          rewritten->fingerprint() == digest
          && rewritten->context().logical_span() == context.logical_span());
    else
        require(rewritten.error().code() == errc::invalid_argument);
}
} // namespace

void exercise_record_case(std::span<const std::uint8_t> input) {
    require(input.size() <= record_fuzz_max_input);
    std::array<std::uint8_t, 8> controls{};
    for (std::size_t i = 0; i < std::min(input.size(), controls.size()); ++i)
        controls[i] = input[i];
    const auto payload = input.subspan(std::min(input.size(), controls.size()));
    const std::string data
      = payload.empty()
          ? std::string{}
          : std::string{
              reinterpret_cast<const char*>(payload.data()), payload.size()};
    const auto mode = controls[0] % 7U;
    if (mode == 6) {
        exercise_rewrite(
          batch_bytes(data, true, false, controls[2]),
          controls[6],
          data.empty() ? controls[6] : static_cast<std::uint8_t>(data[0]));
        return;
    }
    const bool batch = mode >= 2, assigned = mode == 3 || mode == 5;
    std::string wire
      = mode == 1 ? record_bytes(data, controls[2] % 64U, false)
        : mode >= 4
          ? batch_bytes(data, assigned, (controls[4] & 4U) != 0, controls[2])
          : data;
    mutate(wire, controls[1], controls[2], batch);
    if (batch && wire.size() >= 32 && (controls[4] & 2U) != 0) {
        const auto header = little(wire, 10, 2);
        if (header >= 32 && header <= wire.size())
            put(wire, 12, wire.size() - header, 4);
    }
    if (batch && (controls[4] & 1U) != 0) repair_crc(wire);
    const auto depth = static_cast<std::uint8_t>(controls[5] % 9U);
    const bool queued = (controls[7] & 1U) != 0,
               narrow = (controls[7] & 2U) != 0;
    if (!batch)
        exercise_record(
          wire, controls[3], depth, controls[4], queued, narrow, controls[7]);
    else if (assigned)
        exercise_batch<true>(
          wire, controls[3], depth, controls[4], queued, narrow, controls[7]);
    else
        exercise_batch<false>(
          wire, controls[3], depth, controls[4], queued, narrow, controls[7]);
}
void verify_record_oracle() {
    const auto empty = "\x06\x00\x00\x00\x01\x01\x00"sv;
    require(probe_record(empty, 0, 1, 0, true).error == errc::success);
    for (std::size_t cut = 0; cut < empty.size(); ++cut)
        require(
          probe_record(empty.substr(0, cut), 0, 1, 0, true).error
          == errc::malformed_data);
    const auto full = batch_bytes({}, true, false, 4);
    require(probe_batch(full, true, true, false).error == errc::success);
    auto corrupt = full;
    corrupt.back() = 1;
    require(
      probe_batch(corrupt, true, true, false).error == errc::corrupt_data);
    repair_crc(corrupt);
    require(
      probe_batch(corrupt, true, true, false).error == errc::malformed_data);
    require(probe_batch(full, true, true, true).error == errc::wrong_context);
}
} // namespace kwaque::model::testing
