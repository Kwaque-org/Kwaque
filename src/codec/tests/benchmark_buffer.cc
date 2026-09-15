#include "src/codec/tests/benchmark_buffer.h"

#include "src/base/allocation.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/bytes/test_allocation_profile.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/deleter.hh>
#include <seastar/core/temporary_buffer.hh>

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <cstring>
#include <malloc.h>
#include <span>
#include <stdexcept>

namespace kwaque::codec::bench {
using bytes::fragmented_buffer;
void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}
byte_count capacity_bound(byte_count request) noexcept {
    return bytes::testing::charge(request);
}

namespace {
void native_allocation(void* base, std::size_t requested) {
    const auto served = ::malloc_usable_size(base);
    require(
      served >= requested
        && served <= capacity_bound(byte_count{requested}).value(),
      "fixture allocation exceeds verified profile");
}
seastar::future<> qualify_backing(
  const fragmented_buffer& input, std::size_t width, cooperative_work& work) {
    // These builders only copy: every fragment pointer is an allocation base.
    for (const auto fragment : input) {
        native_allocation(const_cast<char*>(fragment.data()), width);
        (co_await work.admit(byte_count{}, item_count{1})).value();
    }
}
} // namespace
void qualify_allocator() {
    for (const auto size : std::array<std::size_t, 11>{
           16,
           32,
           fragmented_buffer::fragment_descriptor_size(),
           sizeof(seastar::free_deleter_impl),
           1024,
           16384,
           16385,
           32768,
           65536,
           65551,
           65552}) {
        require(
          capacity_bound(byte_count{size}).value()
            <= maximum_contiguous_allocation_bytes,
          "fixture capacity bound exceeds the contiguous allocation limit");
        seastar::temporary_buffer<char> allocation{size};
        native_allocation(allocation.get_write(), size);
    }
}

seastar::future<fragmented_buffer> copy_layout(
  fragmented_buffer input,
  std::size_t requested_width,
  codec::cooperative_work& work,
  byte_count remaining,
  std::size_t fragment_limit) {
    const auto size = input.size().value();
    if (size == 0) co_return fragmented_buffer{};
    require(
      fragment_limit != 0 && fragment_limit <= bytes::max_buffer_fragments,
      "invalid fixture fragment limit");
    const auto width = std::min<std::uint64_t>(
      size,
      std::max<std::uint64_t>(
        requested_width == 0 ? 65536 : requested_width,
        (size + fragment_limit - 1U) / fragment_limit));
    const auto count = 1U + (size - 1U) / width;
    require(
      width <= 65536 && count <= fragment_limit,
      "fixture layout exceeds allocation/fragment bound");
    if (auto ready = co_await work.checkpoint(); !ready)
        throw std::runtime_error("fixture aborted");
    const auto source = input.allocation_cost(capacity_bound).value();
    const auto descriptor = capacity_bound(
      byte_count{count * fragmented_buffer::fragment_descriptor_size()});
    const auto backing = byte_count{
      count * capacity_bound(byte_count{width}).value()};
    const operation_usage usage{
      .retained_input = source.backing,
      .staged_output = backing,
      .payload_bookkeeping = byte_count{
        source.descriptors.value() + source.share_controls.value()
        + descriptor.value()}};
    require(
      work.policy().remaining_operation_bytes(usage, remaining).has_value(),
      "fixture copy exceeds remaining operation budget");
    bytes::fragmented_buffer_builder builder{
      {.initial_fragment_bytes = byte_count{width},
       .max_fragment_bytes = byte_count{width},
       .max_total_bytes = byte_count{size},
       .max_retained_bytes = byte_count{count * width},
       .max_fragments = static_cast<std::size_t>(count)}};
    builder.reserve_fragments(item_count{count}).value();
    for (const auto fragment : input) {
        for (std::size_t offset = 0; offset < fragment.size();) {
            const auto n = std::min<std::size_t>(
              {fragment.size() - offset,
               static_cast<std::size_t>(work.byte_quantum().value() / 2U),
               static_cast<std::size_t>(width)});
            const auto ready = co_await work.admit(
              byte_count{2U * n}, item_count{8});
            require(ready.has_value(), "fixture copy aborted");
            builder.append(std::span<const char>{fragment.data() + offset, n})
              .value();
            offset += n;
        }
    }
    const auto ready = co_await work.checkpoint();
    require(ready.has_value(), "fixture publication aborted");
    auto result = builder.finish().value();
    co_await qualify_backing(result, static_cast<std::size_t>(width), work);
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    input = fragmented_buffer{};
    co_return result;
}

seastar::future<bool> buffers_equal(
  const fragmented_buffer& left,
  const fragmented_buffer& right,
  cooperative_work& work) {
    if (left.size() != right.size()) co_return false;
    auto a = left.begin(), b = right.begin();
    std::size_t x = 0, y = 0;
    while (a != left.end()) {
        const auto count = std::min(
          {(*a).size() - x, (*b).size() - y, std::size_t{32768}});
        (co_await work.admit(byte_count{2U * count}, item_count{2})).value();
        if (std::memcmp((*a).data() + x, (*b).data() + y, count) != 0)
            co_return false;
        x += count;
        y += count;
        if (x == (*a).size()) {
            ++a;
            x = 0;
        }
        if (y == (*b).size()) {
            ++b;
            y = 0;
        }
    }
    co_return true;
}

seastar::future<fragmented_buffer> patterned_buffer(
  std::size_t size,
  std::size_t requested_width,
  payload_pattern pattern,
  cooperative_work& work,
  byte_count remaining) {
    if (size == 0) co_return fragmented_buffer{};
    require(size <= (8U << 20U), "benchmark payload exceeds maximum region");
    const auto width = std::min(
      size, std::max(requested_width, (size + 511U) / 512U));
    require(width != 0 && width <= 65536, "invalid benchmark fragment width");
    const auto count = 1U + (size - 1U) / width;
    const auto backing = count * capacity_bound(byte_count{width}).value();
    const auto descriptors = capacity_bound(
      byte_count{count * fragmented_buffer::fragment_descriptor_size()});
    require(
      backing + descriptors.value() <= remaining.value(),
      "benchmark payload exceeds budget");
    bytes::fragmented_buffer_builder builder{
      {.initial_fragment_bytes = byte_count{width},
       .max_fragment_bytes = byte_count{width},
       .max_total_bytes = byte_count{size},
       .max_retained_bytes = byte_count{count * width},
       .max_fragments = count}};
    builder.reserve_fragments(item_count{count}).value();
    std::array<char, 16384> block;
    std::uint32_t state = 0x12345678;
    const auto stripe = std::max(
      std::size_t{1}, std::min(size / 2U, std::size_t{4096}));
    for (std::size_t at = 0; at < size;) {
        const auto length = std::min({block.size(), width, size - at});
        (co_await work.admit(byte_count{4U * length}, item_count{8})).value();
        for (std::size_t i = 0; i < length; ++i) {
            state ^= state << 13U;
            state ^= state >> 17U;
            state ^= state << 5U;
            const bool noise
              = pattern == payload_pattern::incompressible
                || (pattern == payload_pattern::mixed && ((at + i) / stripe) % 2U != 0);
            block[i] = noise
                         ? std::bit_cast<char>(static_cast<std::uint8_t>(state))
                         : 'x';
        }
        builder.append(std::span<const char>{block}.first(length)).value();
        at += length;
    }
    auto result = builder.finish().value();
    co_await qualify_backing(result, width, work);
    co_return result;
}
} // namespace kwaque::codec::bench
