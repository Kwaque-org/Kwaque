#include "src/base/invariant.h"
#include "src/base/units.h"
#include "src/codec/tests/codec_bench_fixture.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <crc32c/crc32c.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <optional>
#include <span>

namespace kwaque::codec::bench {

result<std::uint64_t> native_fixed_read(
  bytes::fragmented_buffer_parser& input,
  field_context context,
  input_boundary boundary) {
    const auto start = detail::integer_read_start(input, context, boundary);
    if (!start) {
        return codec::failure(start.error());
    }
    if (input.bytes_remaining().value() < sizeof(std::uint64_t)) {
        return codec::failure(
          detail::integer_shortage(
            context, boundary, context.origin + input.total_bytes().value()));
    }
    std::array<char, sizeof(std::uint64_t)> encoded{};
    const auto read = input.read_to(encoded);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-BENCH-FIXED-READ"},
      read.has_value(),
      "bounded native scalar read failed");
    std::uint64_t value;
    std::memcpy(&value, encoded.data(), sizeof(value));
    return seastar::le_to_cpu(value);
}

result<std::uint64_t> codec_fixed_read(
  bytes::fragmented_buffer_parser& input,
  field_context context,
  input_boundary boundary) {
    return codec::read_le<std::uint64_t>(input, context, boundary);
}

result<std::uint64_t> baseline_varuint_read(
  bytes::fragmented_buffer_parser& input,
  field_context context,
  input_boundary boundary) {
    const auto start = detail::integer_read_start(input, context, boundary);
    if (!start) {
        return codec::failure(start.error());
    }
    std::array<char, 10> encoded{};
    const auto available = static_cast<std::size_t>(
      std::min<std::uint64_t>(input.bytes_remaining().value(), encoded.size()));
    const auto peeked = input.peek_to(
      std::span<char>{encoded}.first(available));
    KWAQUE_INVARIANT(
      invariant_id{"KQ-BENCH-VARUINT-PEEK"},
      peeked.has_value(),
      "bounded varuint probe failed");
    std::uint64_t value = 0;
    unsigned bits_read = 0;
    for (std::size_t index = 0; index < available; ++index) {
        const auto byte = std::bit_cast<std::uint8_t>(encoded[index]);
        if (byte == 0 && bits_read > 0) {
            return codec::failure(
              error{
                errc::malformed_data,
                context.family,
                context.field,
                *start + index});
        }
        const auto remaining_bits = 64U - bits_read;
        if (
          remaining_bits <= 7U
          && static_cast<unsigned>(std::bit_width(byte)) > remaining_bits) {
            return codec::failure(
              error{
                errc::malformed_data,
                context.family,
                context.field,
                *start + index});
        }
        value |= static_cast<std::uint64_t>(byte & 0x7fU) << bits_read;
        if ((byte & 0x80U) == 0) {
            const auto committed = input.skip(byte_count{index + 1U});
            KWAQUE_INVARIANT(
              invariant_id{"KQ-BENCH-VARUINT-COMMIT"},
              committed.has_value(),
              "validated varuint commit failed");
            return value;
        }
        bits_read += 7U;
    }
    return codec::failure(
      detail::integer_shortage(
        context, boundary, context.origin + input.total_bytes().value()));
}

result<std::uint64_t> codec_varuint_read(
  bytes::fragmented_buffer_parser& input,
  field_context context,
  input_boundary boundary) {
    return codec::read_varuint<std::uint64_t>(input, context, boundary);
}

result<void> native_fixed_write(
  bytes::fragmented_buffer_builder& output,
  std::uint64_t value,
  field_context context) {
    const auto encoded = std::bit_cast<std::array<char, sizeof(value)>>(
      seastar::cpu_to_le(value));
    return detail::append_integer(output, encoded, context);
}

result<void> codec_fixed_write(
  bytes::fragmented_buffer_builder& output,
  std::uint64_t value,
  field_context context) {
    return codec::write_le(output, value, context);
}

result<void> baseline_varuint_write(
  bytes::fragmented_buffer_builder& output,
  std::uint64_t value,
  field_context context) {
    std::array<char, 10> encoded{};
    if (value < 128U) {
        encoded[0] = std::bit_cast<char>(static_cast<std::uint8_t>(value));
        return detail::append_integer(
          output, std::span<const char>{encoded}.first(1), context);
    }
    std::size_t count = 0;
    while (value >= 128U) {
        encoded[count++] = std::bit_cast<char>(
          static_cast<std::uint8_t>(static_cast<std::uint8_t>(value) | 0x80U));
        value >>= 7U;
    }
    encoded[count++] = std::bit_cast<char>(static_cast<std::uint8_t>(value));
    return detail::append_integer(
      output, std::span<const char>{encoded}.first(count), context);
}

result<void> codec_varuint_write(
  bytes::fragmented_buffer_builder& output,
  std::uint64_t value,
  field_context context) {
    return codec::write_varuint(output, value, context);
}

namespace {

seastar::future<result<std::uint32_t>> google_checksum_bytes(
  const bytes::fragmented_buffer& input,
  cooperative_work& work,
  std::uint32_t seed,
  error anchor) {
    if (auto valid = work.poll(anchor); !valid) {
        co_return codec::failure(valid.error());
    }
    const auto policy = work.policy();
    if (
      auto valid = policy.validate_buffer(
        input.size(),
        input.retained_bytes(),
        item_count{input.fragment_count()},
        policy.config().max_retained_bytes);
      !valid) {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-BENCH-CRC-INPUT"},
          valid.error() == errc::invalid_argument
            || valid.error() == errc::resource_exhausted,
          "input bound check returned an unexpected error");
        co_return codec::failure(
          error{
            valid.error() == errc::invalid_argument ? errc::invalid_argument
                                                    : errc::resource_exhausted,
            anchor.family(),
            anchor.field(),
            anchor.byte_offset()});
    }
    if (
      auto admitted = co_await work.admit(byte_count{}, item_count{}, anchor);
      !admitted) {
        co_return codec::failure(admitted.error());
    }
    if (auto valid = work.poll(anchor); !valid) {
        co_return codec::failure(valid.error());
    }
    auto checksum = seed;
    for (const auto fragment : input) {
        for (std::size_t offset = 0; offset < fragment.size();) {
            const auto size = std::min(
              fragment.size() - offset,
              static_cast<std::size_t>(work.byte_quantum().value()));
            if (
              auto admitted = co_await work.admit(
                byte_count{size}, item_count{1}, anchor);
              !admitted) {
                co_return codec::failure(admitted.error());
            }
            if (auto valid = work.poll(anchor); !valid) {
                co_return codec::failure(valid.error());
            }
            checksum = ::crc32c::Extend(
              checksum,
              reinterpret_cast<const std::uint8_t*>(fragment.data() + offset),
              size);
            offset += size;
        }
    }
    if (auto valid = work.poll(anchor); !valid) {
        co_return codec::failure(valid.error());
    }
    co_return checksum;
}

} // namespace

seastar::future<result<std::uint32_t>> google_crc32c_cooperatively(
  bytes::fragmented_buffer input,
  cooperative_work& work,
  std::uint32_t seed,
  error anchor) {
    std::optional<result<std::uint32_t>> outcome;
    std::exception_ptr failure;
    try {
        outcome.emplace(
          co_await google_checksum_bytes(input, work, seed, anchor));
    } catch (...) {
        failure = std::current_exception();
    }
    const auto cleanup_bytes = input.fragment_count() == 0
                                 ? byte_count{}
                                 : work.byte_quantum();
    const auto cleanup_items = input.fragment_count() == 0
                                 ? item_count{1}
                                 : work.item_quantum();
    co_await work.drain_inline(cleanup_bytes, cleanup_items);
    input = bytes::fragmented_buffer{};
    if (failure) {
        std::rethrow_exception(failure);
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-BENCH-CRC-OUTCOME"},
      outcome.has_value(),
      "checksum completed without a value or exception");
    if (!outcome->has_value()) {
        co_return codec::failure(outcome->error());
    }
    if (auto valid = work.poll(anchor); !valid) {
        co_return codec::failure(valid.error());
    }
    co_return **outcome;
}

} // namespace kwaque::codec::bench
