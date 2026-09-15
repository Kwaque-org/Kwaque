#pragma once

#include "src/codec/envelope_decode.h"
#include "src/storage/format_size.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <span>
#include <type_traits>

namespace kwaque::storage::detail {
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
[[nodiscard]] seastar::future<codec::result<kwaque::bytes::fragmented_buffer>>
encode_padded(
  std::span<const char> fixed,
  kwaque::bytes::fragmented_buffer&& child,
  aligned_envelope_layout layout,
  codec::format_family family,
  codec::cooperative_work& work,
  byte_count remaining,
  kwaque::bytes::allocation_charge_fn charge,
  codec::field_context context);
} // namespace kwaque::storage::detail
