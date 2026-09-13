#pragma once

#include "src/codec/integer.h"
#include "src/model/batch_context.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <type_traits>

namespace kwaque::model::detail {

// Fixed inline encoding used by the body and semantic projection. Offsets are
// compile-time extents; no native struct representation reaches the wire.
template<std::size_t Offset, codec::fixed_width_integer T, std::size_t N>
void batch_store(std::array<char, N>& out, T value) noexcept {
    static_assert(Offset <= N && sizeof(T) <= N - Offset);
    using U = std::make_unsigned_t<T>;
    const auto bytes = std::bit_cast<std::array<char, sizeof(T)>>(
      seastar::cpu_to_le(std::bit_cast<U>(value)));
    std::copy(bytes.begin(), bytes.end(), out.begin() + Offset);
}

template<std::size_t Offset, codec::fixed_width_integer T, std::size_t N>
[[nodiscard]] T batch_load(const std::array<char, N>& input) noexcept {
    static_assert(Offset <= N && sizeof(T) <= N - Offset);
    std::array<char, sizeof(T)> bytes{};
    std::copy_n(input.begin() + Offset, sizeof(T), bytes.begin());
    using U = std::make_unsigned_t<T>;
    return std::bit_cast<T>(seastar::le_to_cpu(std::bit_cast<U>(bytes)));
}

inline std::array<char, 104>
encode_original_identity(submitted_batch_context context) noexcept {
    std::array<char, 104> out{};
    const auto id = context.id();
    const auto binding = context.binding();
    const auto producer = id.producer();
    const auto topic = binding.topic();
    const auto range = binding.range();
    const auto segment = binding.segment();
    std::copy(producer.bytes().begin(), producer.bytes().end(), out.begin());
    batch_store<16>(out, id.epoch().value());
    batch_store<24>(out, id.stream().value());
    batch_store<32>(out, id.sequence().value());
    std::copy(topic.bytes().begin(), topic.bytes().end(), out.begin() + 40);
    std::copy(range.bytes().begin(), range.bytes().end(), out.begin() + 56);
    batch_store<72>(out, binding.routing_epoch().value());
    std::copy(segment.bytes().begin(), segment.bytes().end(), out.begin() + 80);
    batch_store<96>(out, binding.generation().value());
    return out;
}

} // namespace kwaque::model::detail
