#include "src/model/record_scan.h"

#include "src/base/invariant.h"

#include <seastar/core/coroutine.hh>

#include <exception>
#include <utility>

namespace kwaque::model {
namespace {
codec::error
at(errc code, codec::field_context context, byte_count offset = {}) {
    return codec::error{
      code, context.family, context.field, context.origin + offset.value()};
}
} // namespace

record_region_scanner::record_region_scanner(
  kwaque::bytes::fragmented_buffer&& region,
  kwaque::bytes::fragmented_buffer_parser&& parser,
  record_region_context expected,
  codec::decode_budget memory,
  codec::limits policy,
  codec::field_context context) noexcept
  : region_(std::move(region))
  , parser_(std::move(parser))
  , expected_(expected)
  , memory_(memory)
  , policy_(policy)
  , context_(context) {}

record_region_scanner::record_region_scanner(
  record_region_scanner&& other) noexcept
  : region_(std::move(other.region_))
  , parser_(std::move(other.parser_))
  , expected_(other.expected_)
  , memory_(other.memory_)
  , policy_(other.policy_)
  , context_(other.context_)
  , current_(std::move(other.current_))
  , index_(other.index_)
  , headers_(other.headers_)
  , previous_delta_(other.previous_delta_)
  , terminal_(other.terminal_)
  , complete_(other.complete_) {
    other.current_.reset();
    other.terminal_ = true;
    other.complete_ = false;
}

seastar::future<codec::result<record_region_scanner>>
record_region_scanner::make(
  kwaque::bytes::fragmented_buffer&& region,
  record_region_context expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context) {
    // Transfer before the first await, including every typed rejection.
    kwaque::bytes::fragmented_buffer_parser parser{std::move(region)};
    kwaque::bytes::fragmented_buffer alias;
    std::optional<codec::error> failed;
    std::exception_ptr exception;
    codec::decode_budget remaining;
    const auto anchor = at(errc::success, context);
    try {
        do {
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            const auto start = codec::detail::integer_read_start(
              parser, context, codec::input_boundary::complete);
            if (!start) {
                failed = start.error();
                break;
            }
            const auto policy = work.policy();
            if (
              expected.kind != record_region_kind::dense_original
              && expected.kind != record_region_kind::sparse) {
                failed = at(errc::invalid_argument, context);
                break;
            }
            const auto counts = policy.validate_batch_counts(
              item_count{expected.original_count.value()},
              expected.retained_count,
              expected.header_count);
            if (!counts) {
                failed = codec::detail::allocation_cost_error(
                  counts.error(), context, context.origin);
                break;
            }
            if (
              expected.kind == record_region_kind::dense_original
              && expected.retained_count.value()
                   != expected.original_count.value()) {
                failed = at(errc::invalid_argument, context);
                break;
            }
            if (
              parser.total_bytes() > policy.config().max_expanded_batch_bytes
              || work.byte_quantum().value() < 4U * sizeof(record_layout)
              || work.item_quantum().value() < 64) {
                failed = at(errc::resource_exhausted, context);
                break;
            }
            if (
              expected.retained_count.value()
              > parser.total_bytes().value() / 7U) {
                failed = at(
                  errc::malformed_data, context, parser.total_bytes());
                break;
            }
            if (auto ready = co_await work.checkpoint(anchor); !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            const auto reserved = codec::reserve_decode_input(
              parser, policy, memory, context, codec::input_boundary::complete);
            if (!reserved) {
                failed = reserved.error();
                break;
            }
            if (auto ready = co_await work.checkpoint(anchor); !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            const auto cost = parser.next_buffer_allocation_cost(
              parser.total_bytes(), memory.charge);
            if (!cost) {
                failed = codec::detail::allocation_cost_error(
                  cost.error(), context, context.origin);
                break;
            }
            const auto valid = codec::detail::validate_decode_cost(
              parser.total_bytes(),
              policy.config().max_expanded_batch_bytes,
              *cost,
              policy,
              context,
              context.origin);
            if (!valid) {
                failed = valid.error();
                break;
            }
            const auto reduced = codec::detail::consume_decode_budget(
              policy,
              *reserved,
              byte_count{},
              cost->descriptors,
              context,
              context.origin);
            if (!reduced) {
                failed = reduced.error();
                break;
            }
            remaining = *reduced;
            if (auto ready = co_await work.checkpoint(anchor); !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            auto shared = parser.peek_buffer(parser.total_bytes());
            if (!shared) {
                failed = codec::detail::allocation_cost_error(
                  shared.error(), context, context.origin);
                break;
            }
            alias = std::move(*shared);
            if (auto ready = co_await work.checkpoint(anchor); !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
        } while (false);
    } catch (...) {
        exception = std::current_exception();
    }
    if (failed || exception) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        alias = kwaque::bytes::fragmented_buffer{};
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        parser = kwaque::bytes::fragmented_buffer_parser{};
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(*failed);
    }
    co_return record_region_scanner{
      std::move(alias),
      std::move(parser),
      expected,
      remaining,
      work.policy(),
      context};
}

seastar::future<codec::result<bool>>
record_region_scanner::next(codec::cooperative_work& work) & {
    const auto anchor = at(errc::success, context_, parser_.bytes_consumed());
    if (terminal_)
        co_return codec::failure(
          at(errc::closed, context_, parser_.bytes_consumed()));
    current_.reset();
    // Set terminal before any suspension/native allocation. Successful progress
    // alone reopens the cursor, so exceptions cannot leave a reusable prefix.
    terminal_ = true;
    if (work.policy() != policy_)
        co_return codec::failure(at(errc::invalid_argument, context_));
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (complete_) {
        terminal_ = false;
        co_return false;
    }
    if (
      auto ready = co_await work.admit(
        byte_count{4U * sizeof(record_layout)}, item_count{64}, anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    auto scanned = co_await detail::scan_record(
      parser_,
      {expected_.timestamp_base,
       expected_.original_count,
       item_count{expected_.header_count.value() - headers_}},
      memory_,
      work,
      context_);
    if (!scanned) co_return codec::failure(scanned.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const auto delta = scanned->fields.logical_delta.value();
    const bool dense = expected_.kind == record_region_kind::dense_original;
    if (
      (dense
       && (delta != index_ || (index_ == 0 && scanned->fields.timestamp_delta != 0)))
      || (!dense && index_ != 0 && delta <= previous_delta_)) {
        co_return codec::failure(
          at(errc::malformed_data, context_, scanned->encoded.offset));
    }
    const auto headers = headers_ + scanned->header_count;
    const bool last = index_ + 1U == expected_.retained_count.value();
    if (
      (last && (!parser_.at_end() || headers != expected_.header_count.value()))
      || (!last && parser_.at_end())) {
        co_return codec::failure(
          at(errc::malformed_data, context_, parser_.bytes_consumed()));
    }
    current_.emplace(std::move(*scanned));
    ++index_;
    headers_ = headers;
    previous_delta_ = delta;
    complete_ = last;
    terminal_ = false;
    co_return true;
}

seastar::future<>
record_region_scanner::close(codec::cooperative_work& work) & {
    terminal_ = true;
    complete_ = false;
    current_.reset();
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    parser_ = kwaque::bytes::fragmented_buffer_parser{};
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    region_ = kwaque::bytes::fragmented_buffer{};
}

seastar::future<codec::result<decoded_record>>
record_region_scanner::materialize_current(
  codec::decode_budget memory, codec::cooperative_work& work) & {
    const auto anchor = at(errc::success, context_);
    if (terminal_ || !current_)
        co_return codec::failure(at(errc::invalid_argument, context_));
    if (
      work.policy() != policy_ || memory.charge != memory_.charge
      || memory.operation_remaining > memory_.operation_remaining
      || memory.metadata_remaining > memory_.metadata_remaining)
        co_return codec::failure(at(errc::invalid_argument, context_));
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const auto extent = current_->encoded;
    const auto cost = region_.slice_allocation_cost(
      extent.offset, extent.length, memory.charge);
    if (!cost)
        co_return codec::failure(
          codec::detail::allocation_cost_error(
            cost.error(), context_, context_.origin));
    const auto valid = codec::detail::validate_decode_cost(
      extent.length,
      policy_.config().max_record_bytes,
      *cost,
      policy_,
      context_,
      context_.origin + extent.offset.value());
    if (!valid) co_return codec::failure(valid.error());
    const auto reduced = codec::detail::consume_decode_budget(
      policy_,
      memory,
      byte_count{},
      cost->descriptors,
      context_,
      context_.origin + extent.offset.value());
    if (!reduced) co_return codec::failure(reduced.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    std::optional<kwaque::bytes::fragmented_buffer_parser> parser;
    std::optional<codec::result<decoded_record>> result;
    std::exception_ptr exception;
    try {
        auto shared = region_.share(extent.offset, extent.length);
        if (!shared) {
            result.emplace(
              codec::failure(
                codec::detail::allocation_cost_error(
                  shared.error(), context_, context_.origin)));
        } else {
            parser.emplace(std::move(*shared));
            result.emplace(
              co_await decode_record(
                *parser,
                {expected_.timestamp_base,
                 expected_.original_count,
                 item_count{current_->header_count}},
                *reduced,
                work,
                {.origin = context_.origin + extent.offset.value(),
                 .family = context_.family,
                 .field = context_.field},
                codec::input_boundary::complete));
        }
    } catch (...) {
        exception = std::current_exception();
    }
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    parser.reset();
    if (exception) std::rethrow_exception(exception);
    if (!result->has_value()) co_return codec::failure(result->error());
    if (auto ready = work.poll(anchor); !ready) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        result.reset();
        co_return codec::failure(ready.error());
    }
    // Both sums restore only this now-destroyed parser's exact reservation;
    // the inner decoder has already refunded its own temporary body alias.
    auto& remaining = (**result).remaining;
    const auto operation = remaining.operation_remaining.checked_add(
      cost->descriptors);
    const auto metadata = remaining.metadata_remaining.checked_add(
      cost->descriptors);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-RECORD-SCAN-MATERIALIZE-BUDGET"},
      operation && metadata && *operation <= memory.operation_remaining
        && *metadata <= memory.metadata_remaining,
      "released record parser exceeded its original reservation");
    remaining.operation_remaining = *operation;
    remaining.metadata_remaining = *metadata;
    co_return std::move(**result);
}

} // namespace kwaque::model
