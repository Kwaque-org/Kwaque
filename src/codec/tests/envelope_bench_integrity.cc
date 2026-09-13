#include "src/codec/tests/envelope_bench_native.h"

namespace kwaque::codec::bench::checked {

namespace {

constexpr std::size_t checksum_begin = 28;
constexpr std::size_t checksum_end = 32;
constexpr std::array<char, checksum_end - checksum_begin> zero_checksum{};

error integrity_error(
  errc code,
  field_context context,
  envelope_field field,
  std::uint64_t offset) noexcept {
    return error{
      code, context.family, static_cast<std::uint16_t>(field), offset};
}

} // namespace

seastar::future<result<void>> native_header_crc(
  const bytes::fragmented_buffer& header,
  cooperative_work& work,
  field_context context) {
    const auto initial_anchor = integrity_error(
      errc::success, context, envelope_field::header_crc32c, context.origin);
    if (auto ready = work.poll(initial_anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    const auto header_size = header.size().value();
    if (
      header_size
      > std::numeric_limits<std::uint64_t>::max() - context.origin) {
        co_return codec::failure(integrity_error(
          errc::invalid_argument,
          context,
          envelope_field::header_bytes,
          context.origin));
    }
    if (header_size < envelope_prefix_bytes) {
        co_return codec::failure(integrity_error(
          errc::malformed_data,
          context,
          envelope_field::header_bytes,
          context.origin + header_size));
    }
    const auto policy = work.policy();
    if (
      auto bounded = policy.validate_buffer(
        header.size(),
        header.retained_bytes(),
        item_count{header.fragment_count()},
        policy.config().max_header_bytes);
      !bounded) {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-ENVELOPE-HEADER-BOUNDS"},
          bounded.error() == errc::invalid_argument
            || bounded.error() == errc::resource_exhausted,
          "header bound check returned an unexpected error");
        co_return codec::failure(integrity_error(
          bounded.error() == errc::invalid_argument ? errc::invalid_argument
                                                    : errc::resource_exhausted,
          context,
          envelope_field::header_bytes,
          context.origin + 10));
    }
    const auto anchor = integrity_error(
      errc::success,
      context,
      envelope_field::header_crc32c,
      context.origin + checksum_begin);
    if (
      auto entered = co_await work.admit(byte_count{}, item_count{}, anchor);
      !entered) {
        co_return codec::failure(entered.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }

    comparison_crc checksum;
    std::uint32_t stored_checksum = 0;
    std::size_t position = 0;
    for (const auto fragment : header) {
        for (std::size_t offset = 0; offset < fragment.size();) {
            const bool masked = position >= checksum_begin
                                && position < checksum_end;
            const auto boundary = position < checksum_begin ? checksum_begin
                                  : masked
                                    ? checksum_end
                                    : static_cast<std::size_t>(header_size);
            // The checksum slot is read once for comparison and fed as zeros
            // once to the native checksum; every other byte is fed once.
            const std::uint64_t passes = masked ? 2U : 1U;
            const auto byte_limit = work.byte_quantum().value() / passes;
            const auto item_limit
              = masked ? work.item_quantum().value() - 1U
                       : std::numeric_limits<std::uint64_t>::max();
            const auto size = std::min(
              {fragment.size() - offset,
               boundary - position,
               static_cast<std::size_t>(byte_limit),
               static_cast<std::size_t>(item_limit)});
            if (size == 0) {
                co_return codec::failure(integrity_error(
                  errc::resource_exhausted,
                  context,
                  envelope_field::header_crc32c,
                  context.origin + position));
            }
            if (
              auto admitted = co_await work.admit(
                byte_count{size * passes},
                item_count{masked ? size + 1U : 1U},
                anchor);
              !admitted) {
                co_return codec::failure(admitted.error());
            }
            if (auto ready = work.poll(anchor); !ready) {
                co_return codec::failure(ready.error());
            }
            if (masked) {
                for (std::size_t index = 0; index < size; ++index) {
                    const auto octet = std::bit_cast<std::uint8_t>(
                      fragment.data()[offset + index]);
                    const auto shift = static_cast<unsigned>(
                      8U * (position + index - checksum_begin));
                    stored_checksum |= static_cast<std::uint32_t>(octet)
                                       << shift;
                }
                checksum.extend(
                  std::span<const char>{zero_checksum}.first(size));
            } else {
                checksum.extend(
                  std::span<const char>{fragment.data() + offset, size});
            }
            offset += size;
            position += size;
        }
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-ENVELOPE-HEADER-COVERAGE"},
      position == header_size,
      "header fragments did not cover their declared bytes");
    if (checksum.value() != stored_checksum) {
        co_return codec::failure(integrity_error(
          errc::corrupt_data,
          context,
          envelope_field::header_crc32c,
          context.origin + checksum_begin));
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    co_return result<void>{};
}

} // namespace kwaque::codec::bench::checked
