#include "src/compression/compression.h"

#include "src/base/invariant.h"
#include "src/compression/compression_internal.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <exception>
#include <limits>
#include <optional>
#include <utility>

namespace kwaque::compression {
namespace {

codec::error at(errc code, codec::field_context context) noexcept {
    return codec::error{code, context.family, context.field, context.origin};
}

seastar::future<codec::result<bytes::buffer_allocation_cost>> inspect_none(
  const bytes::fragmented_buffer& input,
  byte_count expanded_bytes,
  codec::cooperative_work& work,
  bytes::allocation_charge_fn charge,
  codec::field_context context) {
    const auto anchor = at(errc::success, context);
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (charge == nullptr)
        co_return codec::failure(at(errc::invalid_argument, context));
    const auto config = work.policy().config();
    if (
      expanded_bytes > config.max_expanded_batch_bytes
      || input.size() > config.max_encoded_body_bytes)
        co_return codec::failure(at(errc::resource_exhausted, context));
    if (input.size() != expanded_bytes)
        co_return codec::failure(at(errc::malformed_data, context));
    if (
      input.size().value()
      > std::numeric_limits<std::uint64_t>::max() - context.origin)
        co_return codec::failure(at(errc::invalid_argument, context));

    co_return co_await detail::inspect_buffer(
      input, config.max_expanded_batch_bytes, work, charge, context);
}

} // namespace

seastar::future<codec::result<bytes::buffer_allocation_cost>>
detail::inspect_buffer(
  const bytes::fragmented_buffer& input,
  byte_count logical_limit,
  codec::cooperative_work& work,
  bytes::allocation_charge_fn charge,
  codec::field_context context) {
    const auto anchor = at(errc::success, context);
    bytes::buffer_allocation_cost cost;
    std::size_t first = 0;
    do {
        const auto count = input.empty() ? 0U : 1U;
        const auto ready = co_await work.admit(
          byte_count{}, item_count{count == 0 ? 1U : 6U}, anchor);
        if (!ready) co_return codec::failure(ready.error());
        if (auto polled = work.poll(anchor); !polled)
            co_return codec::failure(polled.error());
        const auto part = input.allocation_cost(first, count, charge);
        if (!part)
            co_return codec::failure(
              codec::detail::allocation_cost_error(
                part.error(), context, context.origin));
        for (auto member :
             {&bytes::buffer_allocation_cost::backing,
              &bytes::buffer_allocation_cost::descriptors,
              &bytes::buffer_allocation_cost::share_controls}) {
            const auto sum = (cost.*member).checked_add((*part).*member);
            if (!sum) co_return codec::failure(at(errc::out_of_range, context));
            cost.*member = *sum;
        }
        cost.largest_allocation = std::max(
          cost.largest_allocation, part->largest_allocation);
        first += count;
    } while (first != input.fragment_count());
    cost.fragments = item_count{input.fragment_count()};
    const auto metadata = cost.descriptors.checked_add(cost.share_controls);
    if (!metadata || !cost.backing.checked_add(*metadata))
        co_return codec::failure(at(errc::out_of_range, context));
    const auto valid = work.policy().validate_buffer(
      input.size(), cost.backing, cost.fragments, logical_limit);
    if (!valid)
        co_return codec::failure(
          codec::detail::allocation_cost_error(
            valid.error(), context, context.origin));
    co_return cost;
}

codec::result<codec_id>
parse_codec_id(std::uint8_t value, codec::field_context context) noexcept {
    switch (value) {
    case 0:
        return codec_id::none;
    case 1:
        return codec_id::lz4;
    default:
        return codec::failure(at(errc::unsupported_format, context));
    }
}

seastar::future<codec::result<owned_result>> transfer_none(
  bytes::fragmented_buffer&& source,
  byte_count expanded_bytes,
  codec::cooperative_work& work,
  codec::decode_budget memory,
  codec::field_context context) {
    auto input = std::move(source);
    std::optional<codec::result<bytes::buffer_allocation_cost>> cost;
    std::exception_ptr exception;
    try {
        cost.emplace(
          co_await inspect_none(
            input, expanded_bytes, work, memory.charge, context));
    } catch (...) {
        exception = std::current_exception();
    }
    auto failure = at(errc::success, context);
    if (!exception) {
        if (!cost->has_value())
            failure = cost->error();
        else if (auto ready = work.poll(failure); !ready)
            failure = ready.error();
        else
            co_return owned_result{std::move(input), **cost, memory};
    }
    // Free one backing owner per admitted cleanup step, even after abort/OOM.
    while (!input.empty()) {
        co_await work.drain_inline(byte_count{}, item_count{1});
        const auto size = input.fragment_at(0)->size();
        const auto trimmed = input.trim_front(byte_count{size});
        KWAQUE_INVARIANT(
          invariant_id{"KQ-COMPRESSION-CLEANUP"},
          trimmed.has_value(),
          "owned compression input could not be drained");
    }
    co_await work.drain_inline(byte_count{}, item_count{1});
    input = bytes::fragmented_buffer{};
    if (exception) std::rethrow_exception(exception);
    co_return codec::failure(failure);
}

} // namespace kwaque::compression
