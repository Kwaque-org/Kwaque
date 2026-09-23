#ifndef KWAQUE_SRC_SIMULATION_STORAGE_FAULT_KEY_H_
#define KWAQUE_SRC_SIMULATION_STORAGE_FAULT_KEY_H_

#include "src/codec/sha256.h"
#include "src/runtime/fault.h"
#include "src/simulation/deterministic_random.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace kwaque::simulation {

// The complete context remains the authority. A digest is only a selector.
struct storage_fault_context final {
    std::uint64_t scope;
    std::uint64_t object;
    std::uint64_t generation;
    std::uint64_t position;
    std::uint64_t epoch;
    bool operator==(const storage_fault_context&) const = default;
};

[[nodiscard]] inline codec::sha256_digest
storage_context_digest(storage_fault_context context) {
    std::array<std::uint8_t, 48> input{'K', 'Q', 'F', 'K', 'E', 'Y', '0', '1'};
    const std::array fields{
      context.scope,
      context.object,
      context.generation,
      context.position,
      context.epoch};
    for (std::size_t field = 0; field < fields.size(); ++field)
        for (std::size_t byte = 0; byte < 8; ++byte)
            input[8 + field * 8 + byte] = static_cast<std::uint8_t>(
              fields[field] >> (byte * 8U));
    codec::sha256_hasher hash;
    hash.update(input.data(), input.size());
    return std::move(hash).final();
}

[[nodiscard]] inline runtime::fault_object_key
storage_fault_key(storage_fault_context context) {
    const auto digest = storage_context_digest(context);
    return *runtime::fault_object_key::from_bytes(
      std::as_bytes(std::span{digest}));
}

[[nodiscard]] inline std::array<trace_context_field, 4>
storage_digest_fields(const codec::sha256_digest& digest) noexcept {
    std::array<trace_context_field, 4> fields{};
    for (std::size_t index = 0; index < fields.size(); ++index) {
        fields[index].key = static_cast<trace_context_key>(
          static_cast<std::uint8_t>(trace_context_key::digest_word_0) + index);
        for (std::size_t byte = 0; byte < 8; ++byte)
            fields[index].value
              |= static_cast<std::uint64_t>(digest[index * 8 + byte])
                 << (byte * 8U);
    }
    return fields;
}

[[nodiscard]] inline bool storage_survives(
  std::uint64_t seed, storage_fault_context context, std::uint8_t percent) {
    if (percent == 0) return false;
    if (percent == 100) return true;
    const auto digest = storage_context_digest(context);
    std::array<std::uint64_t, 4> words{};
    for (std::size_t word = 0; word < words.size(); ++word)
        for (std::size_t byte = 0; byte < 8; ++byte)
            words[word] |= static_cast<std::uint64_t>(digest[word * 8 + byte])
                           << (byte * 8U);
    deterministic_random random{seed ^ words[3]};
    const auto coordinate = random_coordinate::make(
      random_domain::storage_decision,
      words[0] == 0 ? UINT64_MAX : words[0],
      words[1]);
    auto cursor = random.cursor(*coordinate, words[2] & (UINT64_MAX >> 1U));
    return *runtime::uniform_u64(cursor, 100) < percent;
}

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_STORAGE_FAULT_KEY_H_
