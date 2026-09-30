#include "src/storage/zero_fill.h"

#include "src/bytes/fragmented_buffer_builder.h"
#include "src/storage/local_paths.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <array>
#include <span>

namespace kwaque::storage::detail {
bytes::fragmented_buffer make_zero_chunk(byte_count size) {
    static constexpr std::array<char, 4096> zeros{};
    bytes::fragmented_buffer_builder filled{
      {.initial_fragment_bytes = size,
       .max_fragment_bytes = size,
       .max_total_bytes = size,
       .max_retained_bytes = byte_count{2 * size.value()},
       .max_fragments = 1}};
    for (std::uint64_t at = 0; at < size.value(); at += zeros.size())
        filled
          .append(
            std::span<const char>{zeros}.first(
              static_cast<std::size_t>(
                std::min<std::uint64_t>(zeros.size(), size.value() - at))))
          .value();
    return filled.finish().value();
}

seastar::future<runtime::result<void>> zero_fill(
  runtime::file& file,
  bytes::fragmented_buffer& chunk,
  std::uint64_t begin,
  std::uint64_t end,
  codec::cooperative_work& work) {
    const auto size = chunk.size().value();
    if (size == 0)
        co_return runtime::failure(path_error(errc::invalid_argument));
    const auto per_write = zero_write_chunks * size;
    for (auto position = begin; position < end;) {
        if (auto ready = work.poll(); !ready)
            co_return runtime::failure(path_error(ready.error().code()));
        const auto length = std::min(end - position, per_write);
        const auto count = static_cast<std::size_t>((length + size - 1) / size);
        bytes::fragmented_buffer_builder payload{
          {.initial_fragment_bytes = byte_count{1},
           .max_fragment_bytes = byte_count{1},
           .max_total_bytes = byte_count{length},
           .max_retained_bytes = byte_count{count * size},
           .max_fragments = count}};
        payload.reserve_fragments(item_count{count}).value();
        for (std::uint64_t at = 0; at < length; at += size) {
            const auto part = std::min(size, length - at);
            payload
              .append_buffer(
                part == size ? chunk.share()
                             : chunk.share({}, byte_count{part}).value())
              .value();
        }
        auto written = co_await file.write(
          runtime::file_position{position}, payload.finish().value());
        if (!written) co_return runtime::failure(written.error());
        if (written->value() != length)
            co_return runtime::failure(path_error(errc::io_failure));
        position += length;
    }
    co_return runtime::result<void>{};
}
} // namespace kwaque::storage::detail
