#pragma once

#include "src/base/error.h"
#include "src/base/invariant.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/error.h"
#include "src/codec/integer.h"
#include "src/codec/limits.h"

#include <seastar/core/future.hh>

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <functional>
#include <span>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>

namespace kwaque::codec {

// A caller-owned allowance, passed by value. The charge function must be a
// nonallocating, nondecreasing served-capacity bound verified for its
// allocator. There is no default profile and no automatic reservation or
// refund. Other producer-owned resources, including resources hidden behind
// native deleters, are already excluded from these remainders; costs are not
// allocator introspection.
struct decode_budget final {
    byte_count operation_remaining;
    byte_count metadata_remaining;
    bytes::allocation_charge_fn charge{nullptr};

    bool operator==(const decode_budget&) const noexcept = default;
};

namespace detail {

template<typename T>
struct borrowed_decode_value
  : std::bool_constant<
      std::is_pointer_v<T> || std::is_reference_v<T>
      || std::same_as<T, bytes::fragment_view>
      || std::same_as<T, bytes::fragmented_buffer_parser>> {};

template<typename T, std::size_t Extent>
struct borrowed_decode_value<std::span<T, Extent>> : std::true_type {};

template<typename Char, typename Traits>
struct borrowed_decode_value<std::basic_string_view<Char, Traits>>
  : std::true_type {};

template<typename T>
struct borrowed_decode_value<std::reference_wrapper<T>> : std::true_type {};

template<typename T>
struct owning_decode_result : std::false_type {};

template<typename T>
struct owning_decode_result<result<T>>
  : std::bool_constant<
      !borrowed_decode_value<std::remove_cv_t<T>>::value
      && !seastar::is_future<std::remove_cv_t<T>>::value
      && (std::is_void_v<T> || (std::is_nothrow_move_constructible_v<T> && std::is_nothrow_destructible_v<T>))
      && std::is_nothrow_move_constructible_v<result<T>>
      && std::is_nothrow_destructible_v<result<T>>> {};

template<typename Callback, typename... Args>
concept synchronous_decode_callback
  = std::invocable<Callback&, Args...>
    && owning_decode_result<std::invoke_result_t<Callback&, Args...>>::value;

// Constructed only after the helper has acquired one mark. Callback code must
// preserve that mark and balance its own nested marks; depth is not an identity
// token that can detect replacing a mark at the same depth.
class parser_transaction_guard final {
public:
    parser_transaction_guard(
      bytes::fragmented_buffer_parser& input, std::size_t entry_depth) noexcept
      : input_(input)
      , entry_depth_(entry_depth) {}

    parser_transaction_guard(const parser_transaction_guard&) = delete;
    parser_transaction_guard&
    operator=(const parser_transaction_guard&) = delete;
    parser_transaction_guard(parser_transaction_guard&&) = delete;
    parser_transaction_guard& operator=(parser_transaction_guard&&) = delete;

    ~parser_transaction_guard() noexcept {
        if (active_) {
            check_depth();
            const auto restored = input_.rollback();
            KWAQUE_INVARIANT(
              invariant_id{"KQ-CODEC-TXN-ROLLBACK"},
              restored.has_value(),
              "owned parser checkpoint could not be rolled back");
        }
    }

    void commit() noexcept {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-CODEC-TXN-COMMIT"},
          active_,
          "parser transaction was already committed");
        check_depth();
        const auto committed = input_.commit();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-CODEC-TXN-COMMIT"},
          committed.has_value(),
          "owned parser checkpoint could not be committed");
        active_ = false;
    }

private:
    void check_depth() const noexcept {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-CODEC-TXN-DEPTH"},
          input_.checkpoint_depth() == entry_depth_ + 1U,
          "callback changed the transaction's checkpoint stack");
    }

    bytes::fragmented_buffer_parser& input_;
    std::size_t entry_depth_;
    bool active_{true};
};

[[nodiscard]] inline error allocation_cost_error(
  std::error_code source,
  field_context context,
  std::uint64_t offset) noexcept {
    for (const auto code :
         {errc::invalid_argument,
          errc::out_of_range,
          errc::resource_exhausted,
          errc::truncated_data}) {
        if (source == code) {
            return error{code, context.family, context.field, offset};
        }
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CODEC-ALLOCATION-COST"},
      false,
      "buffer cost query returned an unexpected error category");
}

[[nodiscard]] inline result<void> validate_decode_cost(
  byte_count logical_bytes,
  byte_count logical_limit,
  const bytes::buffer_allocation_cost& cost,
  const limits& bounds,
  field_context context,
  std::uint64_t offset) {
    const auto buffer = bounds.validate_buffer(
      logical_bytes, cost.backing, cost.fragments, logical_limit);
    if (!buffer) {
        return codec::failure(
          allocation_cost_error(buffer.error(), context, offset));
    }
    const auto allocation = bounds.validate_allocation(cost.largest_allocation);
    if (!allocation) {
        return codec::failure(
          allocation_cost_error(allocation.error(), context, offset));
    }
    return {};
}

[[nodiscard]] inline result<decode_budget> consume_decode_budget(
  const limits& bounds,
  decode_budget budget,
  byte_count backing,
  byte_count metadata,
  field_context context,
  std::uint64_t offset) {
    const auto operation = bounds.remaining_operation_bytes(
      operation_usage{.retained_input = backing, .decoded_metadata = metadata},
      budget.operation_remaining);
    if (!operation) {
        return codec::failure(
          allocation_cost_error(operation.error(), context, offset));
    }
    const auto available_metadata = std::min(
      budget.metadata_remaining, bounds.config().max_metadata_bytes);
    const auto remaining_metadata = available_metadata.checked_sub(metadata);
    if (!remaining_metadata) {
        return codec::failure(
          error{
            errc::resource_exhausted, context.family, context.field, offset});
    }
    return decode_budget{*operation, *remaining_metadata, budget.charge};
}

} // namespace detail

// Callbacks are synchronous and return reviewed owning values. The checks
// reject known direct borrows/native futures; they do not inspect arbitrary
// aggregates. Every returned member must own its lifetime, and no asynchronous
// work or externally published mutation may escape the callback. The parser
// stays alive, unmoved and exclusively accessed; nested transactions balance
// their own marks. The callback owner bounds its synchronous work and any
// further allocations.
template<typename Callback>
requires detail::
  synchronous_decode_callback<Callback, bytes::fragmented_buffer_parser&>
  [[nodiscard]] auto with_transaction(
    bytes::fragmented_buffer_parser& input,
    field_context context,
    Callback&& callback)
    -> std::invoke_result_t<Callback&, bytes::fragmented_buffer_parser&> {
    const auto start = detail::integer_read_start(
      input, context, input_boundary::open);
    if (!start) {
        return codec::failure(start.error());
    }
    const auto depth = input.checkpoint_depth();
    const auto marked = input.push_checkpoint();
    if (!marked) {
        return codec::failure(
          detail::allocation_cost_error(marked.error(), context, *start));
    }
    detail::parser_transaction_guard transaction{input, depth};
    auto decoded = std::invoke(callback, input);
    if (decoded) {
        transaction.commit();
    }
    return decoded;
}

// Admit this parser's whole backing as one input owner before structural
// shares. Retain the backing, descriptor and possible share-control
// reservations for the parent's lifetime, including after failed children:
// first sharing can promote its native ownership permanently. Do not call this
// again for an aliased child.
[[nodiscard]] inline result<decode_budget> reserve_decode_input(
  const bytes::fragmented_buffer_parser& input,
  const limits& bounds,
  decode_budget budget,
  field_context context = {},
  input_boundary boundary = input_boundary::open) {
    const auto start = detail::integer_read_start(input, context, boundary);
    if (!start) {
        return codec::failure(start.error());
    }
    const auto cost = input.allocation_cost(budget.charge);
    if (!cost) {
        return codec::failure(
          detail::allocation_cost_error(cost.error(), context, *start));
    }
    const auto valid = detail::validate_decode_cost(
      input.total_bytes(), input.total_bytes(), *cost, bounds, context, *start);
    if (!valid) {
        return codec::failure(valid.error());
    }
    const auto metadata = cost->descriptors.checked_add(cost->share_controls);
    if (!metadata) {
        return codec::failure(
          error{errc::out_of_range, context.family, context.field, *start});
    }
    return detail::consume_decode_budget(
      bounds, budget, cost->backing, *metadata, context, *start);
}

// budget has already reserved the parent's backing/descriptors/potential share
// controls. This helper charges only additional child descriptors, and passes
// the reduced allowance by value. The caller accounts further callback
// allocations and keeps reservations for returned owners; no implicit refunds
// are performed. Callback errors use the supplied absolute child context, never
// relative offsets. Neither parent nor child may be moved/replaced by the
// callback, and the parent cannot be accessed through another alias while the
// child is decoded.
template<typename Callback>
requires detail::synchronous_decode_callback<
  Callback,
  bytes::fragmented_buffer_parser&,
  field_context,
  input_boundary,
  decode_budget>
[[nodiscard]] auto decode_exact(
  bytes::fragmented_buffer_parser& input,
  byte_count length,
  byte_count child_limit,
  const limits& bounds,
  decode_budget budget,
  field_context context,
  input_boundary boundary,
  Callback&& callback)
  -> std::invoke_result_t<
    Callback&,
    bytes::fragmented_buffer_parser&,
    field_context,
    input_boundary,
    decode_budget> {
    const auto start = detail::integer_read_start(input, context, boundary);
    if (!start) {
        return codec::failure(start.error());
    }
    if (budget.charge == nullptr) {
        return codec::failure(
          error{errc::invalid_argument, context.family, context.field, *start});
    }
    if (length > child_limit) {
        return codec::failure(
          error{
            errc::resource_exhausted, context.family, context.field, *start});
    }
    if (length > input.bytes_remaining()) {
        return codec::failure(
          detail::integer_shortage(
            context, boundary, context.origin + input.total_bytes().value()));
    }
    const auto cost = input.next_buffer_allocation_cost(length, budget.charge);
    if (!cost) {
        return codec::failure(
          detail::allocation_cost_error(cost.error(), context, *start));
    }
    const auto valid = detail::validate_decode_cost(
      length, child_limit, *cost, bounds, context, *start);
    if (!valid) {
        return codec::failure(valid.error());
    }
    const auto remaining = detail::consume_decode_budget(
      bounds, budget, byte_count{}, cost->descriptors, context, *start);
    if (!remaining) {
        return codec::failure(remaining.error());
    }
    const auto entry_depth = input.checkpoint_depth();
    const auto entry_position = input.bytes_consumed();
    auto shared = input.peek_buffer(length);
    if (!shared) {
        return codec::failure(
          detail::allocation_cost_error(shared.error(), context, *start));
    }
    bytes::fragmented_buffer_parser child{std::move(*shared)};
    const field_context child_context{
      .origin = *start, .family = context.family, .field = context.field};
    auto decoded = std::invoke(
      callback, child, child_context, input_boundary::complete, *remaining);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CODEC-CHILD-EXTENT"},
      child.total_bytes() == length,
      "child callback moved or replaced its bounded parser");
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CODEC-CHILD-DEPTH"},
      child.checkpoint_depth() == 0,
      "child callback left unresolved parser checkpoints");
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CODEC-CHILD-PARENT"},
      input.checkpoint_depth() == entry_depth
        && input.bytes_consumed() == entry_position,
      "child callback changed its parent parser");
    if (!decoded) {
        const auto failure = decoded.error();
        if (failure.code() == errc::truncated_data) {
            return codec::failure(
              error{
                errc::malformed_data,
                failure.family(),
                failure.field(),
                failure.byte_offset()});
        }
        return decoded;
    }
    if (!child.at_end()) {
        return codec::failure(
          error{
            errc::malformed_data,
            context.family,
            context.field,
            *start + child.bytes_consumed().value()});
    }
    const auto committed = input.skip(length);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CODEC-CHILD-COMMIT"},
      committed.has_value(),
      "bounded child could not commit its parent position");
    return decoded;
}

} // namespace kwaque::codec
