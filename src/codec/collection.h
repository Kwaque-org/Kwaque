#pragma once

#include "src/base/error.h"
#include "src/base/invariant.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/cooperative.h"
#include "src/codec/error.h"
#include "src/codec/integer.h"
#include "src/codec/limits.h"
#include "src/codec/transaction.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <algorithm>
#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <iterator>
#include <limits>
#include <optional>
#include <ranges>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace kwaque::codec {

namespace detail {

template<typename T>
concept collection_value = !borrowed_decode_value<std::remove_cv_t<T>>::value
                           && !seastar::is_future<std::remove_cv_t<T>>::value
                           && std::is_nothrow_move_constructible_v<T>
                           && std::is_nothrow_destructible_v<T>;

[[nodiscard]] inline error collection_error(
  errc code, field_context context, std::uint64_t offset) noexcept {
    return error{code, context.family, context.field, offset};
}

[[nodiscard]] inline error
collection_read_error(error failure, input_boundary boundary) noexcept {
    if (
      boundary == input_boundary::complete
      && failure.code() == errc::truncated_data) {
        return error{
          errc::malformed_data,
          failure.family(),
          failure.field(),
          failure.byte_offset()};
    }
    return failure;
}

template<typename T>
concept collection_entry = collection_value<T>
                           && std::is_nothrow_move_assignable_v<T>
                           && std::is_nothrow_swappable_v<T>
                           && alignof(T) <= alignof(std::max_align_t);

} // namespace detail

// Decode into caller-owned PRIVATE staging. The sink owner drains/discards it
// on failure; this helper rolls back the parser, not arbitrary sink effects.
// Readers and insertion are synchronous result-returning callbacks. They share
// memory/work by reference and must account their retained allocations without
// widening either allowance. entry_* bounds cover the entire synchronous entry
// step, including callbacks, comparison, moves and destruction. Returned keys
// and values own their members; known direct borrows are rejected, but
// arbitrary aggregate ownership still requires review. Parser, budget, work,
// sink and all borrowed callback state remain alive and exclusively accessed
// until joined. The enclosing owner has already reserved the input, private
// sink, instantiated coroutine frame and callback storage outside memory's
// residuals; this helper does not infer their allocation size.
template<
  detail::collection_value Key,
  detail::collection_value Value,
  typename ReadKey,
  typename ReadValue,
  typename Insert,
  typename Less>
requires std::is_nothrow_invocable_r_v<bool, Less&, const Key&, const Key&>
[[nodiscard]] seastar::future<result<item_count>> read_ordered_map(
  bytes::fragmented_buffer_parser& input,
  item_count maximum_count,
  decode_budget& memory,
  cooperative_work& work,
  ReadKey read_key,
  ReadValue read_value,
  Insert insert,
  Less less,
  byte_count entry_byte_bound,
  item_count entry_item_bound,
  field_context context = {},
  input_boundary boundary = input_boundary::open) {
    static_assert(std::same_as<
                  std::invoke_result_t<
                    ReadKey&,
                    bytes::fragmented_buffer_parser&,
                    field_context,
                    input_boundary,
                    decode_budget&,
                    cooperative_work&>,
                  result<Key>>);
    static_assert(std::same_as<
                  std::invoke_result_t<
                    ReadValue&,
                    bytes::fragmented_buffer_parser&,
                    field_context,
                    input_boundary,
                    decode_budget&,
                    cooperative_work&>,
                  result<Value>>);
    static_assert(std::same_as<
                  std::invoke_result_t<
                    Insert&,
                    Key&&,
                    Value&&,
                    decode_budget&,
                    cooperative_work&>,
                  result<void>>);
    const auto start = detail::integer_read_start(input, context, boundary);
    if (!start) {
        co_return codec::failure(start.error());
    }
    const auto anchor = detail::collection_error(
      errc::success, context, *start);
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    if (memory.charge == nullptr) {
        co_return codec::failure(
          detail::collection_error(errc::invalid_argument, context, *start));
    }
    const auto policy = work.policy().config();
    memory.operation_remaining = std::min(
      memory.operation_remaining, policy.max_operation_bytes);
    memory.metadata_remaining = std::min(
      memory.metadata_remaining, policy.max_metadata_bytes);
    const auto prefix = co_await work.admit(
      byte_count{std::min<std::uint64_t>(input.bytes_remaining().value(), 5)},
      item_count{6},
      anchor);
    if (!prefix) {
        co_return codec::failure(prefix.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    const auto depth = input.checkpoint_depth();
    if (const auto marked = input.push_checkpoint(); !marked) {
        co_return codec::failure(
          detail::allocation_cost_error(marked.error(), context, *start));
    }
    detail::parser_transaction_guard transaction{input, depth};
    const auto count = read_varuint<std::uint32_t>(input, context, boundary);
    if (!count) {
        co_return codec::failure(count.error());
    }
    if (
      *count > std::min(
        maximum_count.value(),
        work.policy().config().max_object_entries.value())) {
        co_return codec::failure(
          detail::collection_error(errc::resource_exhausted, context, *start));
    }
    std::optional<std::pair<Key, Value>> pending;
    std::optional<error> failed;
    std::exception_ptr exception;
    try {
        for (std::uint32_t index = 0; index < *count; ++index) {
            const auto key_offset = context.origin
                                    + input.bytes_consumed().value();
            const auto entry_anchor = detail::collection_error(
              errc::success, context, key_offset);
            const auto admitted = co_await work.admit(
              entry_byte_bound, entry_item_bound, entry_anchor);
            if (!admitted) {
                failed = admitted.error();
                break;
            }
            if (auto ready = work.poll(entry_anchor); !ready) {
                failed = ready.error();
                break;
            }
            auto key = std::invoke(
              read_key, input, context, boundary, memory, work);
            if (!key) {
                failed = detail::collection_read_error(key.error(), boundary);
                break;
            }
            if (pending && !std::invoke(less, pending->first, *key)) {
                failed = detail::collection_error(
                  errc::malformed_data, context, key_offset);
                break;
            }
            auto value = std::invoke(
              read_value, input, context, boundary, memory, work);
            if (!value) {
                failed = detail::collection_read_error(value.error(), boundary);
                break;
            }
            if (pending) {
                const auto inserted = std::invoke(
                  insert,
                  std::move(pending->first),
                  std::move(pending->second),
                  memory,
                  work);
                if (!inserted) {
                    failed = inserted.error();
                    break;
                }
            }
            pending.emplace(std::move(*key), std::move(*value));
        }
        if (!failed && pending) {
            const auto admitted = co_await work.admit(
              entry_byte_bound, entry_item_bound, anchor);
            if (!admitted) {
                failed = admitted.error();
            } else if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
            } else {
                const auto inserted = std::invoke(
                  insert,
                  std::move(pending->first),
                  std::move(pending->second),
                  memory,
                  work);
                if (!inserted) {
                    failed = inserted.error();
                }
                pending.reset();
            }
        }
    } catch (...) {
        exception = std::current_exception();
    }
    if (pending) {
        co_await work.drain_inline(entry_byte_bound, entry_item_bound);
        pending.reset();
    }
    if (exception) {
        std::rethrow_exception(exception);
    }
    if (failed) {
        co_return codec::failure(*failed);
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    transaction.commit();
    co_return item_count{*count};
}

// Append count and already-ordered pairs to the owner's PRIVATE builder. This
// is a composition leaf, not a complete encoder: no whole-builder rollback is
// promised. Range elements must be stable lvalues for the operation's lifetime.
// Callbacks have the same bounded synchronous/shared-budget contract as reads.
// The owner pre-admits its builder's capacities and reserves contiguous tail
// space for the count prefix before calling; this helper does not grow it.
// Range, instantiated coroutine frame and callback storage are already reserved
// outside memory's residuals. An lvalue binding is required, and every owner
// remains alive and exclusively accessed until the returned future completes.
template<
  std::ranges::forward_range Range,
  typename WriteKey,
  typename WriteValue,
  typename Less>
requires std::ranges::sized_range<const Range>
         && std::ranges::forward_range<const Range>
         && std::is_lvalue_reference_v<
           std::ranges::range_reference_t<const Range>>
[[nodiscard]] seastar::future<result<void>> write_ordered_map(
  bytes::fragmented_buffer_builder& output,
  Range& entries,
  item_count maximum_count,
  decode_budget& memory,
  cooperative_work& work,
  WriteKey write_key,
  WriteValue write_value,
  Less less,
  byte_count entry_byte_bound,
  item_count entry_item_bound,
  field_context context = {}) {
    const auto& ordered = entries;
    using key_type = std::remove_cvref_t<decltype(std::get<0>(
      *std::ranges::begin(ordered)))>;
    using value_type = std::remove_cvref_t<decltype(std::get<1>(
      *std::ranges::begin(ordered)))>;
    static_assert(std::is_nothrow_invocable_r_v<
                  bool,
                  Less&,
                  const key_type&,
                  const key_type&>);
    static_assert(std::same_as<
                  std::invoke_result_t<
                    WriteKey&,
                    bytes::fragmented_buffer_builder&,
                    const key_type&,
                    field_context,
                    decode_budget&,
                    cooperative_work&>,
                  result<void>>);
    static_assert(std::same_as<
                  std::invoke_result_t<
                    WriteValue&,
                    bytes::fragmented_buffer_builder&,
                    const value_type&,
                    field_context,
                    decode_budget&,
                    cooperative_work&>,
                  result<void>>);
    constexpr auto max = std::numeric_limits<std::uint64_t>::max();
    if (output.size().value() > max - context.origin) {
        co_return codec::failure(
          detail::collection_error(
            errc::invalid_argument, context, context.origin));
    }
    const auto anchor = detail::collection_error(
      errc::success, context, context.origin + output.size().value());
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    if (memory.charge == nullptr) {
        co_return codec::failure(
          detail::collection_error(
            errc::invalid_argument, context, anchor.byte_offset()));
    }
    const auto policy = work.policy().config();
    memory.operation_remaining = std::min(
      memory.operation_remaining, policy.max_operation_bytes);
    memory.metadata_remaining = std::min(
      memory.metadata_remaining, policy.max_metadata_bytes);
    const auto size = std::ranges::size(ordered);
    if (
      size > std::min(
        maximum_count.value(),
        work.policy().config().max_object_entries.value())) {
        co_return codec::failure(
          detail::collection_error(
            errc::resource_exhausted, context, anchor.byte_offset()));
    }
    const auto count_bits = static_cast<unsigned>(
      std::bit_width(static_cast<std::uint32_t>(size)));
    const auto prefix_bytes = std::max(1U, (count_bits + 6U) / 7U);
    if (prefix_bytes > max - anchor.byte_offset()) {
        co_return codec::failure(
          detail::collection_error(
            errc::invalid_argument, context, anchor.byte_offset()));
    }
    if (output.finished()) {
        co_return codec::failure(
          detail::collection_error(
            errc::closed, context, anchor.byte_offset()));
    }
    if (output.tail_capacity().value() < prefix_bytes) {
        co_return codec::failure(
          detail::collection_error(
            errc::resource_exhausted, context, anchor.byte_offset()));
    }
    const auto prefix = co_await work.admit(
      byte_count{prefix_bytes}, item_count{6}, anchor);
    if (!prefix) {
        co_return codec::failure(prefix.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    const auto written = write_varuint(
      output, static_cast<std::uint32_t>(size), context);
    if (!written) {
        co_return codec::failure(written.error());
    }
    const key_type* previous = nullptr;
    for (const auto& entry : ordered) {
        const auto admitted = co_await work.admit(
          entry_byte_bound, entry_item_bound, anchor);
        if (!admitted) {
            co_return codec::failure(admitted.error());
        }
        if (auto ready = work.poll(anchor); !ready) {
            co_return codec::failure(ready.error());
        }
        const auto& key = std::get<0>(entry);
        const auto& value = std::get<1>(entry);
        if (previous && !std::invoke(less, *previous, key)) {
            co_return codec::failure(
              detail::collection_error(
                errc::malformed_data,
                context,
                context.origin + output.size().value()));
        }
        const auto key_written = std::invoke(
          write_key, output, key, context, memory, work);
        if (!key_written) {
            co_return codec::failure(key_written.error());
        }
        const auto value_written = std::invoke(
          write_value, output, value, context, memory, work);
        if (!value_written) {
            co_return codec::failure(value_written.error());
        }
        previous = &key;
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    co_return result<void>{};
}

// A const rvalue can bind to Range& by deducing a const Range. Reject that
// temporary-owner path as well as ordinary rvalues.
template<typename Range, typename... Args>
void write_ordered_map(
  bytes::fragmented_buffer_builder&, const Range&&, Args&&...) = delete;

namespace detail {

template<typename Entry, std::size_t ChunkEntries>
[[nodiscard]] constexpr std::uint64_t collection_chunk_bytes() noexcept {
    constexpr auto alignment = std::max(alignof(Entry), alignof(void*));
    constexpr auto bytes = ChunkEntries * sizeof(Entry) + sizeof(void*)
                           + 2U * sizeof(unsigned);
    return ((bytes + alignment - 1U) / alignment) * alignment;
}

[[nodiscard]] inline result<byte_count> collection_charge(
  byte_count requested,
  bytes::allocation_charge_fn charge,
  error anchor) noexcept {
    if (requested.value() == 0) {
        return byte_count{};
    }
    const auto served = charge(requested);
    if (served < requested) {
        return codec::failure(
          error{
            errc::invalid_argument,
            anchor.family(),
            anchor.field(),
            anchor.byte_offset()});
    }
    return served;
}

[[nodiscard]] inline result<byte_count> collection_multiply(
  byte_count value, std::uint64_t count, error anchor) noexcept {
    if (
      count != 0
      && value.value() > std::numeric_limits<std::uint64_t>::max() / count) {
        return codec::failure(
          error{
            errc::out_of_range,
            anchor.family(),
            anchor.field(),
            anchor.byte_offset()});
    }
    return byte_count{value.value() * count};
}

// Deterministic admission calculation, separate from run/merge ownership.
// Requests and charge order match the allocations in the constructor below.
enum class collection_allocation { retained_input, output, scratch };

template<typename Entry, std::size_t ChunkEntries, typename HeapEntry>
[[nodiscard]] result<void> validate_collection_memory(
  std::uint64_t count,
  std::uint64_t run_size,
  std::uint64_t run_count,
  decode_budget memory,
  const limits& policy,
  field_context context) {
    const auto config = policy.config();
    const auto anchor = collection_error(
      errc::success, context, context.origin);
    byte_count all_metadata;
    byte_count scratch;
    const auto charge_many = [&](
                               std::uint64_t requested,
                               std::uint64_t allocations,
                               collection_allocation kind) -> result<void> {
        const auto charged = collection_charge(
          byte_count{requested}, memory.charge, anchor);
        if (!charged) {
            return codec::failure(charged.error());
        }
        if (
          kind != collection_allocation::retained_input
          && *charged > config.max_allocation_bytes) {
            return codec::failure(collection_error(
              errc::resource_exhausted, context, context.origin));
        }
        const auto total = collection_multiply(*charged, allocations, anchor);
        if (!total) {
            return codec::failure(total.error());
        }
        const auto next = all_metadata.checked_add(*total);
        if (!next) {
            return codec::failure(
              collection_error(errc::out_of_range, context, context.origin));
        }
        all_metadata = *next;
        if (kind == collection_allocation::scratch) {
            const auto next_scratch = scratch.checked_add(*total);
            if (!next_scratch) {
                return codec::failure(collection_error(
                  errc::out_of_range, context, context.origin));
            }
            scratch = *next_scratch;
        }
        return {};
    };
    const auto chunk_bytes = collection_chunk_bytes<Entry, ChunkEntries>();
    const auto chunks = count / ChunkEntries
                        + (count % ChunkEntries != 0 ? 1U : 0U);
    if (
      auto admitted = charge_many(
        chunk_bytes, chunks + 1U, collection_allocation::retained_input);
      !admitted) {
        return admitted;
    }
    if (
      auto admitted = charge_many(
        chunk_bytes, chunks, collection_allocation::output);
      !admitted) {
        return admitted;
    }
    if (
      auto admitted = charge_many(
        run_size * sizeof(Entry),
        count / run_size,
        collection_allocation::scratch);
      !admitted) {
        return admitted;
    }
    if (count % run_size != 0) {
        if (
          auto admitted = charge_many(
            (count % run_size) * sizeof(Entry),
            1,
            collection_allocation::scratch);
          !admitted) {
            return admitted;
        }
    }
    if (
      auto admitted = charge_many(
        run_count * sizeof(std::vector<Entry>),
        1,
        collection_allocation::scratch);
      !admitted) {
        return admitted;
    }
    if (
      auto admitted = charge_many(
        run_count * sizeof(HeapEntry), 1, collection_allocation::scratch);
      !admitted) {
        return admitted;
    }
    if (
      scratch > config.max_scratch_bytes
      || all_metadata
           > std::min(memory.metadata_remaining, config.max_metadata_bytes)) {
        return codec::failure(
          collection_error(errc::resource_exhausted, context, context.origin));
    }
    const auto admitted = policy.remaining_operation_bytes(
      operation_usage{.decoded_metadata = all_metadata},
      memory.operation_remaining);
    if (!admitted) {
        return codec::failure(
          allocation_cost_error(admitted.error(), context, context.origin));
    }
    return {};
}

} // namespace detail

// Construct a canonical owner from fixed-size metadata entries. Entry
// operations must have reviewed bounded work and no allocations hidden in
// members or ADL swap; moves, swaps, destruction and key comparison are
// noexcept. A comparison
// reads at most the two entries. Other resources owned by entries/callbacks are
// already excluded from memory's residual allowances. The profile charges
// actual native chunk headers/alignment, run capacities and overlapping
// heap/output owners. Instantiated coroutine frame and callback storage are
// already reserved by the caller; this component does not infer frame sizes.
//
// Source must have no reserve-created free chunks: the native shrink operation
// cannot release those incrementally. Unsupported cleanup shapes reject before
// moving source. Otherwise the coroutine takes ownership before its first
// await; failure to allocate the coroutine frame leaves the caller's source
// untouched. Every in-body rejection/exception drains owned entries before
// completion. Returned ownership, including eventual cooperative disposal,
// belongs to caller.
template<
  detail::collection_entry Entry,
  std::size_t ChunkEntries = 16,
  typename Less = std::less<>>
requires(ChunkEntries != 0 && ChunkEntries <= 64
         && (ChunkEntries & (ChunkEntries - 1U)) == 0)
        && std::
          is_nothrow_invocable_r_v<bool, Less&, const Entry&, const Entry&>
[[nodiscard]] seastar::future<
  result<seastar::chunked_fifo<Entry, ChunkEntries>>>
canonicalize_unordered(
  seastar::chunked_fifo<Entry, ChunkEntries>&& source,
  decode_budget memory,
  cooperative_work& work,
  Less less = {},
  field_context context = {}) {
    const auto anchor = detail::collection_error(
      errc::success, context, context.origin);
    if (memory.charge == nullptr || source.nfree_chunks() != 0) {
        co_return codec::failure(
          detail::collection_error(
            errc::invalid_argument, context, context.origin));
    }
    if (!source.empty() && (2U * sizeof(Entry) > work.byte_quantum().value() || work.item_quantum().value() < 2)) {
        co_return codec::failure(
          detail::collection_error(
            errc::resource_exhausted, context, context.origin));
    }
    seastar::chunked_fifo<Entry, ChunkEntries> input{std::move(source)};
    seastar::chunked_fifo<Entry, ChunkEntries> output;
    using iterator = typename std::vector<Entry>::iterator;
    struct heap_entry {
        const Entry* value;
        std::size_t list_index;
        iterator at;
    };
    std::vector<std::vector<Entry>> runs;
    std::vector<heap_entry> heap;
    const auto count = static_cast<std::uint64_t>(input.size());
    const auto policy = work.policy();
    const auto config = policy.config();
    std::optional<error> failed;
    std::exception_ptr exception;
    std::uint64_t run_size = 0;
    std::uint64_t run_count = 0;
    byte_count heap_bytes;
    item_count heap_items;
    auto heap_less =
      [&less](const heap_entry& left, const heap_entry& right) noexcept {
          return std::invoke(less, *right.value, *left.value);
      };
    try {
        if (auto ready = work.poll(anchor); !ready) {
            failed = ready.error();
        }
        if (!failed && count > config.max_object_entries.value()) {
            failed = detail::collection_error(
              errc::resource_exhausted, context, context.origin);
        }
        if (!failed && count == 0) {
            const auto empty = co_await work.admit(
              byte_count{}, item_count{}, anchor);
            if (!empty) {
                failed = empty.error();
            } else if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
            }
        }
        if (!failed && count != 0) {
            run_size = std::min(
              {count, std::uint64_t{64}, std::uint64_t{8192 / sizeof(Entry)}});
            while (run_size != 0) {
                const auto probe_work = co_await work.admit(
                  byte_count{}, item_count{1}, anchor);
                if (!probe_work) {
                    failed = probe_work.error();
                    break;
                }
                if (auto ready = work.poll(anchor); !ready) {
                    failed = ready.error();
                    break;
                }
                const auto run_bytes = detail::collection_charge(
                  byte_count{run_size * sizeof(Entry)}, memory.charge, anchor);
                if (!run_bytes) {
                    failed = run_bytes.error();
                    break;
                }
                if (*run_bytes <= config.max_allocation_bytes) {
                    break;
                }
                --run_size;
            }
            if (!failed && run_size == 0) {
                failed = detail::collection_error(
                  errc::resource_exhausted, context, context.origin);
            }
        }
        if (!failed && count != 0) {
            run_count = count / run_size + (count % run_size != 0 ? 1U : 0U);
            const auto height = static_cast<std::uint64_t>(
              std::bit_width(run_count));
            heap_items = item_count{8U * height + 16U};
            heap_bytes = byte_count{
              heap_items.value() * (sizeof(heap_entry) + 2U * sizeof(Entry))};
            const auto cleanup_bytes = std::max(
              {2U * sizeof(heap_entry),
               2U * sizeof(decltype(heap)),
               2U * sizeof(std::vector<Entry>),
               2U * sizeof(decltype(runs))});
            if (
              heap_items > work.item_quantum()
              || heap_bytes > work.byte_quantum()
              || cleanup_bytes > work.byte_quantum().value()) {
                failed = detail::collection_error(
                  errc::resource_exhausted, context, context.origin);
            }
        }
        if (!failed && count != 0) {
            const auto admitted = detail::
              validate_collection_memory<Entry, ChunkEntries, heap_entry>(
                count, run_size, run_count, memory, policy, context);
            if (!admitted) {
                failed = admitted.error();
            }
        }
        if (!failed && count != 0) {
            const auto setup = co_await work.admit(
              byte_count{}, item_count{2}, anchor);
            if (!setup) {
                failed = setup.error();
            } else if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
            } else {
                runs.reserve(static_cast<std::size_t>(run_count));
                heap.reserve(static_cast<std::size_t>(run_count));
                KWAQUE_INVARIANT(
                  invariant_id{"KQ-COLLECTION-RESERVE"},
                  runs.capacity() == run_count && heap.capacity() == run_count,
                  "exact collection reserve changed its allocation size");
            }
        }
        while (!failed && !input.empty()) {
            const auto allocation = co_await work.admit(
              byte_count{}, item_count{1}, anchor);
            if (!allocation) {
                failed = allocation.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            runs.emplace_back();
            auto& run = runs.back();
            const auto size = std::min<std::uint64_t>(run_size, input.size());
            run.reserve(static_cast<std::size_t>(size));
            KWAQUE_INVARIANT(
              invariant_id{"KQ-COLLECTION-RUN"},
              run.capacity() == size,
              "exact run reserve changed its allocation size");
            for (std::uint64_t index = 0; index < size; ++index) {
                const auto admitted = co_await work.admit(
                  byte_count{2U * sizeof(Entry)}, item_count{4}, anchor);
                if (!admitted) {
                    failed = admitted.error();
                    break;
                }
                if (auto ready = work.poll(anchor); !ready) {
                    failed = ready.error();
                    break;
                }
                run.emplace_back(std::move(input.front()));
                input.pop_front();
            }
            if (failed) {
                break;
            }
            // The private run is capped at 64 entries and 8 KiB. Its native
            // sort is one bounded leaf, with checkpoints on both sides.
            const auto sorting = co_await work.checkpoint(anchor);
            if (!sorting) {
                failed = sorting.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            std::sort(run.begin(), run.end(), std::ref(less));
            const auto sorted = co_await work.checkpoint(anchor);
            if (!sorted) {
                failed = sorted.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
        }
        for (std::size_t index = 0; !failed && index < runs.size(); ++index) {
            const auto admitted = co_await work.admit(
              heap_bytes, heap_items, anchor);
            if (!admitted) {
                failed = admitted.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            auto at = runs[index].begin();
            heap.push_back(heap_entry{&*at, index, at});
            std::push_heap(heap.begin(), heap.end(), heap_less);
        }
        while (!failed && !heap.empty()) {
            const auto admitted = co_await work.admit(
              heap_bytes, heap_items, anchor);
            if (!admitted) {
                failed = admitted.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            std::pop_heap(heap.begin(), heap.end(), heap_less);
            auto top = heap.back();
            heap.pop_back();
            if (
              !output.empty()
              && !std::invoke(less, output.back(), *top.value)) {
                failed = detail::collection_error(
                  errc::malformed_data, context, context.origin);
                break;
            }
            output.push_back(std::move(*top.at));
            ++top.at;
            if (top.at != runs[top.list_index].end()) {
                top.value = &*top.at;
                heap.push_back(top);
                std::push_heap(heap.begin(), heap.end(), heap_less);
            }
        }
    } catch (...) {
        exception = std::current_exception();
    }

    if (count == 0) {
        if (exception) {
            std::rethrow_exception(exception);
        }
        if (failed) {
            co_return codec::failure(*failed);
        }
        if (auto ready = work.poll(anchor); !ready) {
            co_return codec::failure(ready.error());
        }
        co_return std::move(output);
    }

    // Teardown reuses this coroutine's promise directly. It cannot allocate a
    // new helper frame after an allocation failure, and never observes abort.
    while (!heap.empty()) {
        co_await work.drain_inline(
          byte_count{2U * sizeof(heap_entry)}, item_count{2});
        heap.pop_back();
    }
    if (heap.capacity() != 0) {
        co_await work.drain_inline(
          byte_count{2U * sizeof(decltype(heap))}, item_count{2});
        std::vector<heap_entry>{}.swap(heap);
    }
    while (!input.empty()) {
        co_await work.drain_inline(
          byte_count{2U * sizeof(Entry)}, item_count{2});
        input.pop_front();
    }
    while (!runs.empty()) {
        auto& run = runs.back();
        while (!run.empty()) {
            co_await work.drain_inline(
              byte_count{2U * sizeof(Entry)}, item_count{2});
            run.pop_back();
        }
        co_await work.drain_inline(
          byte_count{2U * sizeof(std::vector<Entry>)}, item_count{2});
        runs.pop_back();
    }
    if (runs.capacity() != 0) {
        co_await work.drain_inline(
          byte_count{2U * sizeof(decltype(runs))}, item_count{2});
        std::vector<std::vector<Entry>>{}.swap(runs);
    }
    if (!failed && !exception) {
        if (auto ready = work.poll(anchor); !ready) {
            failed = ready.error();
        }
    }
    if (failed || exception) {
        while (!output.empty()) {
            co_await work.drain_inline(
              byte_count{2U * sizeof(Entry)}, item_count{2});
            output.pop_front();
        }
        if (exception) {
            std::rethrow_exception(exception);
        }
        co_return codec::failure(*failed);
    }
    co_return std::move(output);
}

} // namespace kwaque::codec
