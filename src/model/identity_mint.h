#pragma once

#include "src/base/invariant.h"
#include "src/model/identity.h"

#include <array>
#include <concepts>
#include <cstdint>
#include <span>
#include <type_traits>

namespace kwaque::model {

// Fresh octets for one identity. The runtime backend supplies the source:
// the kernel's random source in production, a keyed stream in simulation.
template<typename Source>
concept object_id_entropy = requires(
  Source& source, std::span<std::uint8_t, topic_id::width> octets) {
    { source.fill(octets) } noexcept -> std::same_as<void>;
};

namespace detail {

template<typename Id>
struct is_object_id : std::false_type {};

template<typename Tag>
struct is_object_id<object_id<Tag>> : std::true_type {};

} // namespace detail

// A new version-4 UUID (RFC 9562 section 5.4) of the requested kind, such as
// topic_id: 122 drawn bits with octet 6 set to 0100xxxx and octet 8 to
// 10xxxxxx. Those fixed bits hold both a one and a zero, so the value is never
// Nil or Max and needs no retry. A version-7 UUID would spend 48 of the drawn
// bits on a creation time, which commands already carry; nothing orders
// identities.
template<typename Id, object_id_entropy Entropy>
requires detail::is_object_id<Id>::value
[[nodiscard]] Id mint_object_id(Entropy& entropy) noexcept {
    std::array<std::uint8_t, Id::width> octets;
    entropy.fill(octets);
    octets[8] = static_cast<std::uint8_t>((octets[8] & 0xbfU) | 0x80U);
    octets[6] = static_cast<std::uint8_t>((octets[6] & 0x4fU) | 0x40U);
    auto minted = Id::make(octets);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-MODEL-OBJECT-ID-MINT"},
      minted.has_value(),
      "a version-4 identity was refused");
    return *minted;
}

} // namespace kwaque::model
