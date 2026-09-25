#pragma once

#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/tests/memory_qualification_support.h"
#include "src/codec/transaction.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace kwaque::storage::qualification {
namespace observation = codec::testing;
using bytes::fragmented_buffer;
using bytes::fragmented_buffer_parser;
using bytes::testing::charge;
using observation::measure;
using observation::require;
using observation::retained_cost;

inline byte_count held_string(const std::string& text) {
    return charge(byte_count{text.capacity() + 1U});
}
template<typename T>
byte_count held_vector(const std::vector<T>& values) {
    return charge(byte_count{values.capacity() * sizeof(T)});
}
inline byte_count available(byte_count held) {
    return observation::residual.checked_sub(held).value();
}
// All owners alive on entry, including the input, are accounted by held. Do
// not reserve the same backing twice or refund it when returning an alias.
inline codec::decode_budget memory(byte_count held) {
    return {available(held), byte_count{1U << 20U}, charge};
}
inline std::size_t header_bytes(std::string_view name) {
    return name.contains("h4096") ? 4096U : 32U;
}
inline std::uint64_t alignment_bytes(std::string_view name) {
    return name.ends_with("a65536") ? 65536U : 512U;
}

void format_operation(std::string_view);
void metadata_operation(std::string_view);
void extent_operation(std::string_view);
} // namespace kwaque::storage::qualification
