#include "src/model/checkpoint_codec.h"

#include "src/base/invariant.h"
#include "src/codec/collection.h"
#include "src/codec/envelope_decode.h"
#include "src/model/checkpoint_wire.h"
#include "src/model/fingerprint.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <array>
#include <cstdint>
#include <exception>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

namespace kwaque::model {
namespace detail {
// Publication is confined to this translation unit after full validation.
class checkpoint_builder final {
public:
    static read_checkpoint
    publish(topic_id topic, std::vector<range_cursor>&& cursors) noexcept {
        return read_checkpoint{topic, std::move(cursors)};
    }
};
} // namespace detail

namespace {
using detail::checkpoint_error;
using cursor_list = seastar::chunked_fifo<range_cursor, 16>;
constexpr auto family = codec::format_family::read_checkpoint;
static_assert(std::is_trivially_destructible_v<range_cursor>);
static_assert(std::is_nothrow_copy_constructible_v<range_cursor>);

codec::field_context in_family(codec::field_context context) noexcept {
    context.family = static_cast<std::uint16_t>(family);
    return context;
}
codec::field_context
field(codec::field_context context, checkpoint_field id) noexcept {
    context.field = static_cast<std::uint16_t>(id);
    return context;
}

codec::result<byte_count> cursor_metadata(
  std::uint64_t count,
  codec::decode_budget memory,
  const codec::limits& policy,
  codec::field_context context) {
    if (memory.charge == nullptr)
        return codec::failure(
          checkpoint_error(errc::invalid_argument, context));
    const byte_count request{count * sizeof(range_cursor)};
    const auto served = memory.charge(request);
    if (served < request)
        return codec::failure(
          checkpoint_error(errc::invalid_argument, context));
    if (!policy.validate_allocation(served))
        return codec::failure(
          checkpoint_error(errc::resource_exhausted, context));
    return served;
}

// Called only with a checked count and a fresh vector. The selected allocator
// reserves exactly count elements; verify actual capacity before inserting.
codec::result<codec::decode_budget> reserve_cursors(
  std::vector<range_cursor>& output,
  std::uint64_t count,
  codec::decode_budget memory,
  const codec::limits& policy,
  codec::field_context context) {
    const auto metadata = cursor_metadata(count, memory, policy, context);
    if (!metadata) return codec::failure(metadata.error());
    const auto remaining = codec::detail::consume_decode_budget(
      policy, memory, {}, *metadata, context, context.origin);
    if (!remaining) return codec::failure(remaining.error());
    output.reserve(static_cast<std::size_t>(count));
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CHECKPOINT-CAPACITY"},
      output.capacity() == count,
      "cursor vector reserve changed its admitted capacity");
    return *remaining;
}

seastar::future<codec::result<codec::decode_budget>> copy_sorted(
  std::vector<range_cursor>& output,
  std::span<const range_cursor> source,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context) {
    const auto anchor = checkpoint_error(errc::success, context);
    if (
      auto ready = co_await work.admit(byte_count{128}, item_count{8}, anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const auto remaining = reserve_cursors(
      output, source.size(), memory, work.policy(), context);
    if (!remaining) co_return codec::failure(remaining.error());
    for (const auto& cursor : source) {
        if (
          auto ready = co_await work.admit(
            byte_count{128}, item_count{8}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        if (!output.empty() && !range_cursor_less{}(output.back(), cursor))
            co_return codec::failure(checkpoint_error(
              errc::malformed_data, field(context, checkpoint_field::range)));
        output.push_back(cursor);
    }
    co_return *remaining;
}

template<typename Id>
codec::result<Id>
read_id(bytes::fragmented_buffer_parser& input, codec::field_context context) {
    const auto offset = input.bytes_consumed().value();
    std::array<char, Id::width> raw{};
    if (const auto copied = input.read_to(raw); !copied)
        return codec::failure(
          checkpoint_error(errc::malformed_data, context, offset));
    std::array<std::uint8_t, Id::width> octets{};
    for (std::size_t i = 0; i < raw.size(); ++i)
        octets[i] = static_cast<std::uint8_t>(raw[i]);
    auto value = Id::make(octets);
    if (!value)
        return codec::failure(
          checkpoint_error(errc::malformed_data, context, offset));
    return *value;
}

seastar::future<codec::result<void>> read_body(
  bytes::fragmented_buffer_parser& input,
  topic_id expected,
  std::vector<range_cursor>& cursors,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context) {
    const auto anchor = checkpoint_error(errc::success, context);
    if (input.bytes_remaining() < checkpoint_fixed_bytes)
        co_return codec::failure(checkpoint_error(
          errc::malformed_data, context, input.bytes_remaining().value()));
    if (
      auto ready = co_await work.admit(byte_count{256}, item_count{64}, anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const auto topic = read_id<topic_id>(
      input, field(context, checkpoint_field::topic));
    if (!topic) co_return codec::failure(topic.error());
    if (*topic != expected)
        co_return codec::failure(checkpoint_error(
          errc::wrong_context, field(context, checkpoint_field::topic)));
    const auto count_context = field(context, checkpoint_field::cursor_count);
    const auto count = codec::read_le<std::uint32_t>(
      input, count_context, codec::input_boundary::complete);
    if (!count) co_return codec::failure(count.error());
    const auto size = detail::checkpoint_body_size(
      *count, work.policy(), count_context);
    if (!size) {
        const auto code = size.error().code() == errc::invalid_argument
                            ? errc::malformed_data
                            : size.error().code();
        co_return codec::failure(checkpoint_error(code, count_context, 16));
    }
    if (*size != input.total_bytes())
        co_return codec::failure(
          checkpoint_error(errc::malformed_data, count_context, 16));
    const auto remaining = reserve_cursors(
      cursors, *count, memory, work.policy(), count_context);
    if (!remaining) co_return codec::failure(remaining.error());
    for (std::uint32_t i = 0; i < *count; ++i) {
        const auto offset = input.bytes_consumed().value();
        const auto key_context = field(context, checkpoint_field::range);
        if (
          auto ready = co_await work.admit(
            byte_count{256}, item_count{64}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        const auto range = read_id<range_id>(input, key_context);
        if (!range) co_return codec::failure(range.error());
        // Reject duplicate/descending keys before reading their values.
        if (!cursors.empty() && !cursors.back().range().canonical_less(*range))
            co_return codec::failure(
              checkpoint_error(errc::malformed_data, key_context, offset));
        const auto next = codec::read_le<std::uint64_t>(
          input,
          field(context, checkpoint_field::next),
          codec::input_boundary::complete);
        if (!next) co_return codec::failure(next.error());
        cursors.push_back(
          range_cursor::make(*range, range_logical_end{*next}).value());
    }
    co_return work.poll(anchor);
}

struct body_decoder final {
    topic_id expected;
    codec::decode_budget original;

    seastar::future<codec::result<decoded_read_checkpoint>> operator()(
      bytes::fragmented_buffer_parser& input,
      codec::field_context context,
      codec::input_boundary,
      codec::decode_budget memory,
      codec::cooperative_work& work) {
        return decode(input, expected, original, memory, work, context);
    }

    static seastar::future<codec::result<decoded_read_checkpoint>> decode(
      bytes::fragmented_buffer_parser& input,
      topic_id expected,
      codec::decode_budget original,
      codec::decode_budget memory,
      codec::cooperative_work& work,
      codec::field_context context) {
        std::vector<range_cursor> cursors;
        std::optional<read_checkpoint> value;
        std::optional<codec::checkpoint_digest> fingerprint;
        std::optional<codec::error> failed;
        std::exception_ptr exception;
        codec::decode_budget remaining;
        const auto anchor = checkpoint_error(errc::success, context);
        try {
            const auto parsed = co_await read_body(
              input, expected, cursors, memory, work, context);
            if (!parsed) {
                failed = parsed.error();
            } else if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
            } else {
                const auto metadata = cursor_metadata(
                  cursors.capacity(), original, work.policy(), context);
                KWAQUE_INVARIANT(
                  invariant_id{"KQ-CHECKPOINT-METADATA"},
                  metadata.has_value(),
                  "admitted cursor capacity changed");
                const auto residual = codec::detail::consume_decode_budget(
                  work.policy(),
                  original,
                  {},
                  *metadata,
                  context,
                  context.origin);
                KWAQUE_INVARIANT(
                  invariant_id{"KQ-CHECKPOINT-RESIDUAL"},
                  residual.has_value(),
                  "retained cursor storage exceeds its reservation");
                remaining = *residual;
                value.emplace(
                  detail::checkpoint_builder::publish(
                    expected, std::move(cursors)));
                // Make the vacated staging state explicit before common
                // cleanup.
                cursors = {};
                const auto digest = co_await compute_checkpoint_fingerprint(
                  *value, work, anchor);
                if (!digest)
                    failed = digest.error();
                else
                    fingerprint = *digest;
            }
        } catch (...) {
            exception = std::current_exception();
        }
        while (!cursors.empty()) {
            co_await work.drain_inline(byte_count{}, item_count{1});
            cursors.pop_back();
        }
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        std::vector<range_cursor>{}.swap(cursors);
        if (!failed && !exception) {
            if (auto ready = work.poll(anchor); !ready) failed = ready.error();
        }
        if (failed || exception) {
            // A checked result contains only trivial scalar entries and one
            // bounded native allocation; it has no opaque per-entry teardown.
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            value.reset();
            if (exception) std::rethrow_exception(exception);
            co_return codec::failure(*failed);
        }
        // The envelope releases aliases and performs the final parent poll.
        co_return decoded_read_checkpoint{
          std::move(*value), *fingerprint, remaining};
    }
};
} // namespace

seastar::future<codec::result<constructed_read_checkpoint>>
make_read_checkpoint(
  topic_id topic,
  std::span<const range_cursor> sorted,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context) {
    context = in_family(context);
    const auto anchor = checkpoint_error(errc::success, context);
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (topic.is_nil() || memory.charge == nullptr)
        co_return codec::failure(
          checkpoint_error(errc::invalid_argument, context));
    const auto size = detail::checkpoint_body_size(
      sorted.size(), work.policy(), context);
    if (!size) co_return codec::failure(size.error());
    std::vector<range_cursor> output;
    std::optional<codec::result<codec::decode_budget>> outcome;
    std::exception_ptr exception;
    try {
        outcome.emplace(
          co_await copy_sorted(output, sorted, memory, work, context));
    } catch (...) {
        exception = std::current_exception();
    }
    if (!exception && outcome->has_value()) {
        if (auto ready = work.poll(anchor); !ready)
            *outcome = codec::failure(ready.error());
    }
    if (exception || !outcome->has_value()) {
        while (!output.empty()) {
            co_await work.drain_inline(byte_count{}, item_count{1});
            output.pop_back();
        }
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        std::vector<range_cursor>{}.swap(output);
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(outcome->error());
    }
    co_return constructed_read_checkpoint{
      detail::checkpoint_builder::publish(topic, std::move(output)), **outcome};
}

seastar::future<codec::result<constructed_read_checkpoint>>
make_read_checkpoint_from_unordered(
  topic_id topic,
  cursor_list&& source,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context) {
    context = in_family(context);
    const auto anchor = checkpoint_error(errc::success, context);
    if (memory.charge == nullptr || source.nfree_chunks() != 0)
        co_return codec::failure(
          checkpoint_error(errc::invalid_argument, context));
    if (!source.empty() && (work.byte_quantum().value() < 2U * sizeof(range_cursor) || work.item_quantum().value() < 2))
        co_return codec::failure(
          checkpoint_error(errc::resource_exhausted, context));
    cursor_list input{std::move(source)};
    cursor_list canonical;
    std::vector<range_cursor> output;
    std::optional<codec::error> failed;
    std::exception_ptr exception;
    codec::decode_budget remaining;
    try {
        do {
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            if (topic.is_nil()) {
                failed = checkpoint_error(errc::invalid_argument, context);
                break;
            }
            const auto size = detail::checkpoint_body_size(
              input.size(), work.policy(), context);
            if (!size) {
                failed = size.error();
                break;
            }
            auto sorted = co_await codec::canonicalize_unordered(
              std::move(input), memory, work, range_cursor_less{}, context);
            if (!sorted) {
                failed = sorted.error();
                break;
            }
            canonical = std::move(*sorted);
            if (
              auto ready = co_await work.admit(
                byte_count{128}, item_count{8}, anchor);
              !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            // Runs/source/heap have ended. Reserve only the still-live output
            // FIFO alongside its final vector, using the sorter's native shape.
            const byte_count chunk_request{
              codec::detail::collection_chunk_bytes<range_cursor, 16>()};
            const auto chunk_charge = memory.charge(chunk_request);
            const auto chunks = (canonical.size() + 15U) / 16U;
            const byte_count fifo_metadata{chunks * chunk_charge.value()};
            const auto overlap = codec::detail::consume_decode_budget(
              work.policy(),
              memory,
              {},
              fifo_metadata,
              context,
              context.origin);
            if (!overlap) {
                failed = overlap.error();
                break;
            }
            const auto admitted = reserve_cursors(
              output, canonical.size(), *overlap, work.policy(), context);
            if (!admitted) {
                failed = admitted.error();
                break;
            }
            while (!canonical.empty()) {
                if (
                  auto ready = co_await work.admit(
                    byte_count{128}, item_count{8}, anchor);
                  !ready) {
                    failed = ready.error();
                    break;
                }
                if (auto ready = work.poll(anchor); !ready) {
                    failed = ready.error();
                    break;
                }
                output.push_back(canonical.front());
                canonical.pop_front();
            }
            if (failed) break;
            const auto metadata = cursor_metadata(
              output.capacity(), memory, work.policy(), context);
            if (!metadata) {
                failed = metadata.error();
                break;
            }
            const auto residual = codec::detail::consume_decode_budget(
              work.policy(), memory, {}, *metadata, context, context.origin);
            if (!residual) {
                failed = residual.error();
                break;
            }
            remaining = *residual;
        } while (false);
    } catch (...) {
        exception = std::current_exception();
    }
    // Native moves leave a valid empty FIFO; a child frame/preflight failure
    // can instead leave input owned here. Drain both states through one path.
    // NOLINTNEXTLINE(bugprone-use-after-move)
    for (auto* owner : {&input, &canonical}) {
        while (!owner->empty()) {
            co_await work.drain_inline(
              byte_count{2U * sizeof(range_cursor)}, item_count{2});
            owner->pop_front();
        }
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        *owner = cursor_list{};
    }
    if (!failed && !exception) {
        if (auto ready = work.poll(anchor); !ready) failed = ready.error();
    }
    if (failed || exception) {
        while (!output.empty()) {
            co_await work.drain_inline(byte_count{}, item_count{1});
            output.pop_back();
        }
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        std::vector<range_cursor>{}.swap(output);
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(*failed);
    }
    co_return constructed_read_checkpoint{
      detail::checkpoint_builder::publish(topic, std::move(output)), remaining};
}

seastar::future<codec::result<decoded_read_checkpoint>> decode_read_checkpoint(
  bytes::fragmented_buffer_parser& input,
  topic_id expected_topic,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context,
  codec::input_boundary boundary) {
    context = in_family(context);
    if (expected_topic.is_nil())
        return seastar::make_ready_future<
          codec::result<decoded_read_checkpoint>>(
          codec::failure(checkpoint_error(errc::invalid_argument, context)));
    const auto cap = work.policy().config().max_checkpoint_bytes;
    return codec::decode_envelope<decoded_read_checkpoint>(
      input,
      family,
      {cap, cap},
      memory,
      work,
      body_decoder{expected_topic, memory},
      context,
      boundary);
}

} // namespace kwaque::model
