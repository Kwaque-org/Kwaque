#include "src/base/invariant.h"
#include "src/compression/compression.h"
#include "src/compression/compression_internal.h"
#include "src/compression/lz4.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <array>
#include <cstring>
#include <exception>
#include <limits>
#include <optional>
#include <span>
#include <utility>

namespace kwaque::compression {
namespace {

using bytes::fragmented_buffer;
using detail::lz4_context;
using detail::lz4_direction;
using detail::lz4_plan;
using detail::lz4_staging;

codec::error at(errc code, codec::field_context context) noexcept {
    return codec::error{code, context.family, context.field, context.origin};
}

// Borrowed fragment/byte cursors. The outer coroutine keeps the immutable owner
// alive and unmoved until these cursors have gone away. Empty offers still
// carry a valid address for native code that forms src + size even when size is
// zero.
class input_cursor final {
public:
    explicit input_cursor(const fragmented_buffer& input) noexcept
      : fragment_(input.begin())
      , end_(input.end()) {}

    [[nodiscard]] bool empty() const noexcept { return fragment_ == end_; }
    [[nodiscard]] std::uint64_t position() const noexcept { return consumed_; }
    [[nodiscard]] std::span<const char>
    window(std::size_t limit = 65536) const noexcept {
        if (empty()) return std::span<const char>{&sentinel_, std::size_t{0}};
        const auto fragment = *fragment_;
        return std::span<const char>{
          fragment.data() + offset_,
          std::min(limit, fragment.size() - offset_)};
    }
    void advance(std::size_t count) noexcept {
        if (count == 0) return;
        KWAQUE_INVARIANT(
          invariant_id{"KQ-LZ4-INPUT-CURSOR"},
          !empty() && count <= (*fragment_).size() - offset_,
          "native input consumption exceeds its fragment");
        offset_ += count;
        consumed_ += count;
        if (offset_ == (*fragment_).size()) {
            offset_ = 0;
            ++fragment_;
        }
    }

private:
    fragmented_buffer::const_iterator fragment_;
    fragmented_buffer::const_iterator end_;
    std::size_t offset_{0};
    std::uint64_t consumed_{0};
    char sentinel_{0};
};

codec::field_context
input_at(codec::field_context context, const input_cursor& input) noexcept {
    // The complete encoded/input extent is checked before cursor construction.
    context.origin += input.position();
    return context;
}

seastar::future<codec::result<void>> gather(
  input_cursor& input,
  std::span<char> destination,
  codec::cooperative_work& work,
  codec::field_context context) {
    while (!destination.empty()) {
        const auto current = input_at(context, input);
        if (input.empty())
            co_return codec::failure(at(errc::malformed_data, current));
        const auto part = input.window(destination.size());
        const auto anchor = at(errc::success, current);
        if (
          auto ready = co_await work.admit(
            byte_count{part.size()}, item_count{2}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        std::memcpy(destination.data(), part.data(), part.size());
        destination = destination.subspan(part.size());
        input.advance(part.size());
    }
    co_return codec::result<void>{};
}

seastar::future<codec::result<void>> preflight(
  input_cursor& input,
  lz4_context& native,
  byte_count expanded,
  codec::cooperative_work& work,
  codec::field_context context) {
    std::array<char, LZ4F_HEADER_SIZE_MAX> header;
    constexpr std::size_t prefix_size = LZ4F_MIN_SIZE_TO_KNOW_HEADER_LENGTH;
    const auto anchor = at(errc::success, context);
    if (
      auto copied = co_await gather(
        input, std::span{header}.first(prefix_size), work, context);
      !copied)
        co_return codec::failure(copied.error());
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const auto size = LZ4F_headerSize(header.data(), prefix_size);
    if (
      auto checked = detail::check_lz4_decode(native.memory(), size, context);
      !checked)
        co_return codec::failure(checked.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    KWAQUE_INVARIANT(
      invariant_id{"KQ-LZ4-HEADER-BOUND"},
      size >= prefix_size && size <= header.size(),
      "native frame header exceeds its fixed bound");
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (
      auto copied = co_await gather(
        input,
        std::span{header}.subspan(prefix_size, size - prefix_size),
        work,
        context);
      !copied)
        co_return codec::failure(copied.error());
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const auto accepted = native.read_header(
      std::span{header}.first(size), expanded, context);
    if (!accepted) co_return codec::failure(accepted.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    co_return work.poll(anchor);
}

seastar::future<codec::result<void>> append_output(
  lz4_staging& staging,
  std::size_t produced,
  const lz4_plan& plan,
  codec::cooperative_work& work,
  codec::field_context context) {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-LZ4-OUTPUT-CURSOR"},
      produced <= staging.bounce.size(),
      "native output exceeds its destination");
    const auto remaining = plan.output_limit.checked_sub(staging.output.size());
    if (!remaining || produced > remaining->value())
        co_return codec::failure(at(errc::resource_exhausted, context));
    const auto anchor = at(errc::success, context);
    for (std::size_t offset = 0; offset != produced;) {
        const auto count = std::min(
          {produced - offset,
           static_cast<std::size_t>(work.byte_quantum().value() / 2U),
           static_cast<std::size_t>(
             plan.output_config.max_fragment_bytes.value())});
        if (
          auto ready = co_await work.admit(
            byte_count{2U * count}, item_count{8}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        const auto copied = staging.output.append(
          std::span<const char>{staging.bounce.get() + offset, count});
        KWAQUE_INVARIANT(
          invariant_id{"KQ-LZ4-OUTPUT-ADMISSION"},
          copied.has_value(),
          "bounded output append exceeded its admitted geometry");
        offset += count;
    }
    co_return work.poll(anchor);
}

seastar::future<codec::result<void>> compress_into(
  input_cursor& input,
  byte_count expanded,
  lz4_context& native,
  lz4_staging& staging,
  const lz4_plan& plan,
  codec::cooperative_work& work,
  codec::field_context context) {
    const auto prefs = detail::writer_preferences(expanded);
    auto anchor = at(errc::success, context);
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    auto produced = LZ4F_compressBegin(
      native.compressor(),
      staging.bounce.get_write(),
      staging.bounce.size(),
      &prefs);
    if (
      auto checked = detail::check_lz4_encode(
        native.memory(), produced, context);
      !checked)
        co_return codec::failure(checked.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (
      auto copied = co_await append_output(
        staging, produced, plan, work, context);
      !copied)
        co_return codec::failure(copied.error());

    while (!input.empty()) {
        const auto current = input_at(context, input);
        anchor = at(errc::success, current);
        const auto offered = input.window();
        if (auto ready = co_await work.checkpoint(anchor); !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        const auto capacity = LZ4F_compressBound(offered.size(), &prefs);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-LZ4-UPDATE-BOUND"},
          !LZ4F_isError(capacity) && capacity <= staging.bounce.size(),
          "compression update does not fit its admitted destination");
        produced = LZ4F_compressUpdate(
          native.compressor(),
          staging.bounce.get_write(),
          staging.bounce.size(),
          offered.data(),
          offered.size(),
          nullptr);
        if (
          auto checked = detail::check_lz4_encode(
            native.memory(), produced, current);
          !checked)
            co_return codec::failure(checked.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        // Successful Update consumes the whole offer, even for zero output.
        input.advance(offered.size());
        if (auto ready = co_await work.checkpoint(anchor); !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        if (
          auto copied = co_await append_output(
            staging, produced, plan, work, current);
          !copied)
            co_return codec::failure(copied.error());
    }

    const auto current = input_at(context, input);
    anchor = at(errc::success, current);
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const auto capacity = LZ4F_compressBound(0, &prefs);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-LZ4-END-BOUND"},
      !LZ4F_isError(capacity) && capacity <= staging.bounce.size(),
      "compression completion does not fit its admitted destination");
    produced = LZ4F_compressEnd(
      native.compressor(),
      staging.bounce.get_write(),
      staging.bounce.size(),
      nullptr);
    if (
      auto checked = detail::check_lz4_encode(
        native.memory(), produced, current);
      !checked)
        co_return codec::failure(checked.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    co_return co_await append_output(staging, produced, plan, work, current);
}

seastar::future<codec::result<void>> decompress_into(
  input_cursor& input,
  byte_count expanded,
  lz4_context& native,
  lz4_staging& staging,
  const lz4_plan& plan,
  codec::cooperative_work& work,
  codec::field_context context) {
    for (;;) {
        const auto current = input_at(context, input);
        const auto anchor = at(errc::success, current);
        const auto offered = input.window();
        const auto remaining = expanded.value() - staging.output.size().value();
        // After the expected output, still let the decoder consume the footer.
        // A one-byte destination detects excess data without publishing it.
        const auto capacity = std::min(
          staging.bounce.size(),
          static_cast<std::size_t>(std::max(remaining, std::uint64_t{1})));
        auto consumed = offered.size();
        auto produced = capacity;
        if (auto ready = co_await work.checkpoint(anchor); !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        const auto code = LZ4F_decompress(
          native.decompressor(),
          staging.bounce.get_write(),
          &produced,
          offered.data(),
          &consumed,
          nullptr);
        if (
          auto checked = detail::check_lz4_decode(
            native.memory(), code, current);
          !checked)
            co_return codec::failure(checked.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        KWAQUE_INVARIANT(
          invariant_id{"KQ-LZ4-DECODE-CURSOR"},
          consumed <= offered.size() && produced <= capacity,
          "native decoder exceeded its input or output offer");
        input.advance(consumed);
        if (produced > remaining)
            co_return codec::failure(at(errc::malformed_data, current));
        if (code == 0 && (!input.empty() || produced != remaining))
            co_return codec::failure(at(errc::malformed_data, current));
        if (code != 0 && consumed == 0 && produced == 0)
            co_return codec::failure(at(errc::malformed_data, current));
        if (auto ready = co_await work.checkpoint(anchor); !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        if (
          auto copied = co_await append_output(
            staging, produced, plan, work, current);
          !copied)
            co_return codec::failure(copied.error());
        if (code == 0) co_return work.poll(anchor);
        // Do not stop on input exhaustion: native output may still be buffered.
    }
}

seastar::future<codec::result<owned_result>> transform_into(
  const fragmented_buffer& input,
  lz4_direction direction,
  byte_count expanded,
  byte_count encoded_limit,
  codec::cooperative_work& work,
  codec::decode_budget memory,
  std::optional<lz4_context>& native,
  std::optional<lz4_staging>& staging,
  std::optional<fragmented_buffer>& published,
  codec::field_context context) {
    const auto anchor = at(errc::success, context);
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (
      memory.charge == nullptr
      || input.size().value()
           > std::numeric_limits<std::uint64_t>::max() - context.origin)
        co_return codec::failure(at(errc::invalid_argument, context));
    const auto config = work.policy().config();
    const auto input_limit = direction == lz4_direction::compress
                               ? config.max_expanded_batch_bytes
                               : config.max_encoded_body_bytes;
    if (input.size() > input_limit)
        co_return codec::failure(at(errc::resource_exhausted, context));
    const auto plan = detail::admit_lz4(
      direction, expanded, encoded_limit, work, memory, context);
    if (!plan) co_return codec::failure(plan.error());
    if (
      auto cost = co_await detail::inspect_buffer(
        input, input_limit, work, memory.charge, context);
      !cost)
        co_return codec::failure(cost.error());
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    native.emplace(*plan);
    if (auto ready = native->memory().status(context); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    input_cursor cursor{input};
    if (direction == lz4_direction::decompress) {
        if (
          auto accepted = co_await preflight(
            cursor, *native, expanded, work, context);
          !accepted)
            co_return codec::failure(accepted.error());
    }
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    staging.emplace(*plan);
    const auto transformed
      = direction == lz4_direction::compress
          ? co_await compress_into(
              cursor, expanded, *native, *staging, *plan, work, context)
          : co_await decompress_into(
              cursor, expanded, *native, *staging, *plan, work, context);
    if (!transformed) co_return codec::failure(transformed.error());
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    auto finished = staging->output.finish();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-LZ4-PUBLISH"},
      finished.has_value(),
      "admitted immutable output could not be published");
    published.emplace(std::move(*finished));
    const auto cost = co_await detail::inspect_buffer(
      *published, plan->output_limit, work, memory.charge, context);
    if (!cost) co_return codec::failure(cost.error());
    const auto metadata = cost->descriptors.checked_add(cost->share_controls);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-LZ4-RETAINED-COST"},
      metadata && *metadata <= plan->output_metadata
        && cost->backing <= plan->output_backing,
      "published output exceeds its reserved allocation geometry");
    const auto remaining = codec::detail::consume_decode_budget(
      work.policy(), memory, cost->backing, *metadata, context, context.origin);
    if (!remaining) co_return codec::failure(remaining.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    co_return owned_result{std::move(*published), *cost, *remaining};
}

seastar::future<codec::result<owned_result>> transform_lz4(
  fragmented_buffer&& source,
  lz4_direction direction,
  byte_count expanded,
  byte_count encoded_limit,
  codec::cooperative_work& work,
  codec::decode_budget memory,
  codec::field_context context) {
    auto input = std::move(source);
    std::optional<lz4_context> native;
    std::optional<lz4_staging> staging;
    std::optional<fragmented_buffer> published;
    std::optional<codec::result<owned_result>> produced;
    std::exception_ptr exception;
    try {
        produced.emplace(
          co_await transform_into(
            input,
            direction,
            expanded,
            encoded_limit,
            work,
            memory,
            native,
            staging,
            published,
            context));
    } catch (...) {
        exception = std::current_exception();
    }
    // Bounded foundation builder release (at most 1024 descriptors) and native
    // context release stay reachable here, even if allocating a helper failed.
    if (staging) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        staging.reset();
    }
    if (native) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        native.reset();
    }
    for (auto* buffer : {&input, published ? &*published : nullptr}) {
        if (buffer == nullptr) continue;
        while (!buffer->empty()) {
            co_await work.drain_inline(byte_count{}, item_count{1});
            const auto count = buffer->fragment_at(0)->size();
            const auto trimmed = buffer->trim_front(byte_count{count});
            KWAQUE_INVARIANT(
              invariant_id{"KQ-LZ4-CLEANUP"},
              trimmed.has_value(),
              "owned compression buffer could not be drained");
        }
        co_await work.drain_inline(byte_count{}, item_count{1});
        *buffer = fragmented_buffer{};
    }
    if (exception) std::rethrow_exception(exception);
    if (!produced->has_value()) co_return codec::failure(produced->error());
    if (auto ready = work.poll(at(errc::success, context)); !ready) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        produced.reset();
        co_return codec::failure(ready.error());
    }
    co_return std::move(**produced);
}

} // namespace

seastar::future<codec::result<owned_result>> compress_lz4(
  bytes::fragmented_buffer&& input,
  byte_count encoded_limit,
  codec::cooperative_work& work,
  codec::decode_budget memory,
  codec::field_context context) {
    const auto expanded = input.size();
    return transform_lz4(
      std::move(input),
      lz4_direction::compress,
      expanded,
      encoded_limit,
      work,
      memory,
      context);
}

seastar::future<codec::result<owned_result>> decompress_lz4(
  bytes::fragmented_buffer&& input,
  byte_count expanded_bytes,
  codec::cooperative_work& work,
  codec::decode_budget memory,
  codec::field_context context) {
    return transform_lz4(
      std::move(input),
      lz4_direction::decompress,
      expanded_bytes,
      work.policy().config().max_encoded_body_bytes,
      work,
      memory,
      context);
}

} // namespace kwaque::compression
