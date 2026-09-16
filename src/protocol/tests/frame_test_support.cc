#include "src/protocol/tests/frame_test_support.h"

#include "src/bytes/fragmented_buffer_builder.h"
#include "src/codec/staging_cooperative.h"

#include <seastar/core/coroutine.hh>

#include <crc32c/crc32c.h>

#include <algorithm>
#include <array>
#include <span>
#include <utility>

namespace kwaque::protocol::testing::frame_fixture {
using bytes::fragmented_buffer;
namespace {
constexpr std::size_t maximum_payload = 16U << 20U;
constexpr byte_count available{63U << 20U};
} // namespace
seastar::future<patterned_payload> make_payload(
  std::size_t size,
  std::size_t requested_width,
  codec::cooperative_work& work) {
    if (size == 0) co_return patterned_payload{{}, 0};
    if (requested_width == 0)
        throw std::invalid_argument("zero fixture fragment width");
    const auto width = std::min(size, requested_width);
    const auto count = 1U + (size - 1U) / width;
    if (size > maximum_payload || requested_width > 65536 || count > 1023)
        throw std::invalid_argument("fixture exceeds frame layout bounds");
    bytes::fragmented_buffer_builder builder{
      {.initial_fragment_bytes = byte_count{width},
       .max_fragment_bytes = byte_count{width},
       .max_total_bytes = byte_count{size},
       .max_retained_bytes = byte_count{count * width},
       .max_fragments = count}};
    builder.reserve_fragments(item_count{count}).value();
    std::array<char, 16384> block;
    block.fill('x');
    std::uint32_t checksum = 0;
    for (std::size_t at = 0; at < size;) {
        const auto length = std::min({block.size(), width, size - at});
        (co_await work.admit(byte_count{4U * length}, item_count{8})).value();
        checksum = ::crc32c::Extend(
          checksum,
          reinterpret_cast<const std::uint8_t*>(block.data()),
          length);
        builder.append(std::span<const char>{block}.first(length)).value();
        at += length;
    }
    co_return patterned_payload{builder.finish().value(), checksum};
}

seastar::future<fragmented_buffer> wrap(
  patterned_payload&& payload,
  codec::cooperative_work& work,
  std::string_view extensions,
  protocol::frame_metadata fields) {
    const auto header = frame_fixture::header(
      static_cast<std::uint32_t>(payload.bytes.size().value()),
      payload.checksum,
      extensions,
      fields);
    const auto total = byte_count{header.size() + payload.bytes.size().value()};
    auto result = co_await codec::assemble_buffer_cooperatively(
      fragmented_buffer::copy_of(header).value(),
      std::move(payload.bytes),
      work,
      total,
      {},
      available,
      charge);
    co_return std::move(result).value();
}

} // namespace kwaque::protocol::testing::frame_fixture
