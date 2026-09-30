#pragma once

#include "src/codec/crc32c.h"
#include "src/codec/envelope_decode.h"
#include "src/codec/xxh3.h"
#include "src/storage/encoded_batch.h"
#include "src/storage/format_size.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <type_traits>

namespace kwaque::storage::detail {
struct padded_prefix_cost final {
    byte_count backing;
    std::uint32_t fragments;
};

// Choose one combined allocation only when it fits and costs no more than
// separate envelope/fixed owners. Admission and encoding use the same choice.
[[nodiscard]] codec::result<padded_prefix_cost> plan_padded_prefix(
  byte_count fixed_bytes,
  const codec::limits&,
  kwaque::bytes::allocation_charge_fn,
  codec::field_context = {});

// Fixed field leaves use explicit byte order, never native struct layout.
template<std::size_t Offset, codec::fixed_width_integer T, std::size_t N>
void store(std::array<char, N>& out, T value) noexcept {
    static_assert(Offset <= N && sizeof(T) <= N - Offset);
    using U = std::make_unsigned_t<T>;
    const auto raw = std::bit_cast<std::array<char, sizeof(T)>>(
      seastar::cpu_to_le(std::bit_cast<U>(value)));
    std::copy(raw.begin(), raw.end(), out.begin() + Offset);
}
template<std::size_t Offset, codec::fixed_width_integer T, std::size_t N>
T load(const std::array<char, N>& input) noexcept {
    static_assert(Offset <= N && sizeof(T) <= N - Offset);
    std::array<char, sizeof(T)> raw{};
    std::copy_n(input.begin() + Offset, sizeof(T), raw.begin());
    using U = std::make_unsigned_t<T>;
    return std::bit_cast<T>(seastar::le_to_cpu(std::bit_cast<U>(raw)));
}

// Fill a bounded block from small fragments; complete large spans go directly
// to the digest. Copy and hash work are both admitted before buffering. Nothing
// is retained after completion, including a final partial block.
[[nodiscard]] inline seastar::future<codec::result<void>> hash_fragmented(
  const bytes::fragmented_buffer& input,
  codec::xxh3_128_hasher& hasher,
  codec::cooperative_work& work,
  codec::error anchor) {
    std::array<char, 1024> buffer;
    const auto capacity = std::min<std::size_t>(
      buffer.size(), work.byte_quantum().value() / 2U);
    if (capacity == 0)
        co_return codec::failure(
          codec::error{
            errc::resource_exhausted,
            anchor.family(),
            anchor.field(),
            anchor.byte_offset()});
    std::size_t buffered = 0;
    for (const auto part : input) {
        for (std::size_t at = 0; at != part.size();) {
            const auto available = part.size() - at;
            const bool direct = buffered == 0 && available >= capacity;
            const auto count = std::min<std::size_t>(
              available,
              direct ? work.byte_quantum().value() : capacity - buffered);
            if (
              auto ready = co_await work.admit(
                byte_count{count * (direct ? 1U : 2U)}, item_count{1}, anchor);
              !ready)
                co_return codec::failure(ready.error());
            if (auto ready = work.poll(anchor); !ready)
                co_return codec::failure(ready.error());
            if (direct) {
                hasher.update(part.data() + at, count);
            } else {
                std::copy_n(part.data() + at, count, buffer.data() + buffered);
                buffered += count;
                if (buffered == capacity) {
                    hasher.update(buffer.data(), buffered);
                    buffered = 0;
                }
            }
            at += count;
        }
    }
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (buffered != 0) hasher.update(buffer.data(), buffered);
    co_return work.poll(anchor);
}

// Called only after complete envelope validation or codec-stamped reuse. The
// stored body CRC is therefore evidence for these immutable bytes. Hash the
// actual header (including its CRC slot and extensions), then concatenate the
// checked body CRC. The digest still visits every stored byte exactly once.
[[nodiscard]] inline seastar::future<codec::result<std::uint32_t>>
extend_validated_envelope(
  const bytes::fragmented_buffer& input,
  std::uint32_t seed,
  codec::xxh3_128_hasher* hasher,
  codec::cooperative_work& work,
  codec::field_context context) {
    const codec::error anchor{
      errc::success, context.family, context.field, context.origin};
    if (
      auto ready = co_await work.admit(
        codec::envelope_prefix_work_bytes,
        codec::envelope_prefix_work_items,
        anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    codec::encoded_envelope_prefix prefix{};
    std::size_t copied = 0;
    for (auto fragment : input) {
        const auto count = std::min(fragment.size(), prefix.size() - copied);
        std::copy_n(fragment.data(), count, prefix.data() + copied);
        copied += count;
        if (copied == prefix.size()) break;
    }
    auto header_remaining = std::size_t{load<10, std::uint16_t>(prefix)};
    const auto body_bytes = load<12, std::uint32_t>(prefix);
    const auto body_crc = load<24, std::uint32_t>(prefix);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-STORAGE-VERIFIED-ENVELOPE"},
      copied == prefix.size() && header_remaining >= prefix.size()
        && header_remaining + std::uint64_t{body_bytes} == input.size().value(),
      "validated object changed its envelope");
    codec::crc32c checksum{seed};
    for (auto fragment : input) {
        for (std::size_t offset = 0; offset < fragment.size();) {
            if (header_remaining == 0) break;
            const auto available = std::min(
              fragment.size() - offset, header_remaining);
            const auto count = std::min(
              available, static_cast<std::size_t>(work.byte_quantum().value()));
            if (
              auto ready = co_await work.admit(
                byte_count{count}, item_count{1}, anchor);
              !ready)
                co_return codec::failure(ready.error());
            if (auto ready = work.poll(anchor); !ready)
                co_return codec::failure(ready.error());
            checksum.extend(
              std::span<const char>{fragment.data() + offset, count});
            header_remaining -= count;
            offset += count;
        }
        if (header_remaining == 0) break;
    }
    checksum.extend_checksum(body_crc, body_bytes);
    if (hasher) {
        if (
          auto hashed = co_await hash_fragmented(input, *hasher, work, anchor);
          !hashed)
            co_return codec::failure(hashed.error());
    }
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    co_return checksum.value();
}

// These concrete shared leaves are private to storage codecs. The caller
// retains borrowed arrays/parsers through completion and owns partial buffers.
[[nodiscard]] seastar::future<codec::result<void>> read_fixed(
  kwaque::bytes::fragmented_buffer_parser& input,
  std::span<char> output,
  codec::cooperative_work& work,
  codec::field_context context);
[[nodiscard]] seastar::future<codec::result<void>> read_padding(
  kwaque::bytes::fragmented_buffer_parser& input,
  byte_count padding,
  codec::cooperative_work& work,
  codec::field_context context);
// A validated assigned envelope can reuse its body CRC. Current policy/context
// validation stays with the segment/WAL entrance before this consuming call.
[[nodiscard]] seastar::future<codec::result<kwaque::bytes::fragmented_buffer>>
encode_padded(
  std::span<const char> fixed,
  std::optional<encoded_assigned_batch> child,
  aligned_envelope_layout layout,
  codec::format_family family,
  codec::cooperative_work& work,
  byte_count remaining,
  kwaque::bytes::allocation_charge_fn charge,
  codec::field_context context);

// Page/root entry bytes have no envelope integrity to reuse. This consuming
// entrance hashes the complete raw tail with the same bounded assembly,
// allocation checks and joined cleanup as the validated-child entrance.
[[nodiscard]] seastar::future<codec::result<kwaque::bytes::fragmented_buffer>>
encode_padded(
  std::span<const char> fixed,
  kwaque::bytes::fragmented_buffer&& entries,
  aligned_envelope_layout layout,
  codec::format_family family,
  codec::cooperative_work& work,
  byte_count remaining,
  kwaque::bytes::allocation_charge_fn charge,
  codec::field_context context);
} // namespace kwaque::storage::detail
