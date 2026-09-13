#include "src/model/batch_builder.h"

#include "src/base/invariant.h"
#include "src/model/batch_staging.h"
#include "src/model/fingerprint.h"
#include "src/model/record_encode.h"

#include <seastar/core/coroutine.hh>

#include <cstdint>
#include <exception>
#include <utility>

namespace kwaque::model {
namespace {
constexpr codec::error at(errc code) noexcept {
    return codec::error{code, 1, 80};
}
constexpr auto anchor = at(errc::success);
} // namespace

batch_builder::batch_builder(
  batch_id id,
  producer_stream_binding binding,
  codec::limits policy,
  bytes::allocation_charge_fn charge,
  bytes::fragmented_buffer_builder_config config,
  byte_count descriptor_charge) noexcept
  : id_(id)
  , binding_(binding)
  , policy_(policy)
  , charge_(charge)
  , config_(config)
  , descriptor_charge_(descriptor_charge) {}

batch_builder::batch_builder(batch_builder&& other) noexcept
  : id_(other.id_)
  , binding_(other.binding_)
  , policy_(other.policy_)
  , charge_(other.charge_)
  , config_(other.config_)
  , descriptor_charge_(other.descriptor_charge_)
  , output_(std::move(other.output_))
  , timestamp_base_(other.timestamp_base_)
  , count_(other.count_)
  , headers_(other.headers_)
  , closed_(other.closed_) {
    other.closed_ = true;
    other.output_.reset();
}

codec::result<batch_builder> batch_builder::make(
  batch_id id,
  producer_stream_binding binding,
  codec::limits policy,
  bytes::allocation_charge_fn charge) {
    const auto shape = detail::make_batch_staging(policy, charge, anchor);
    if (!shape) return codec::failure(shape.error());
    return batch_builder{
      id, binding, policy, charge, shape->config, shape->descriptors};
}

codec::result<void>
batch_builder::admit_size(byte_count total, byte_count remaining) const {
    const auto reduced = detail::admit_batch_staging(
      {config_, descriptor_charge_},
      total,
      policy_,
      remaining,
      charge_,
      anchor);
    if (!reduced) return codec::failure(reduced.error());
    return {};
}

seastar::future<codec::result<void>> batch_builder::append(
  const record& value,
  runtime::wall_time timestamp,
  codec::cooperative_work& work,
  byte_count remaining) {
    if (work.policy() != policy_)
        co_return codec::failure(at(errc::invalid_argument));
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (work.byte_quantum().value() < 128 || work.item_quantum().value() < 64)
        co_return codec::failure(at(errc::resource_exhausted));
    if (count_ >= policy_.config().max_original_records)
        co_return codec::failure(at(errc::resource_exhausted));
    const auto next_headers = headers_.checked_add(
      item_count{value.headers().size()});
    if (!next_headers || *next_headers > policy_.config().max_batch_headers)
        co_return codec::failure(at(errc::resource_exhausted));
    const auto base = timestamp_base_.value_or(timestamp);
    const auto delta = checked_timestamp_delta(base, timestamp);
    if (!delta) co_return codec::failure(at(errc::out_of_range));
    const record_fields fields{
      value.attributes(), *delta, range_logical_count{count_.value()}};
    const auto size = co_await detail::record_size_with_fields(
      fields, value, work, anchor);
    if (!size) co_return codec::failure(size.error());
    const auto before = output_ ? output_->size() : byte_count{};
    const auto total = before.checked_add(size->encoded_bytes);
    if (!total) co_return codec::failure(at(errc::out_of_range));
    const auto admitted = admit_size(*total, remaining);
    if (!admitted) co_return codec::failure(admitted.error());
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (!output_) {
        output_.emplace(config_);
        const auto reserved = output_->reserve_fragments(
          item_count{config_.max_fragments});
        KWAQUE_INVARIANT(
          invariant_id{"KQ-BATCH-BUILDER-RESERVE"},
          reserved.has_value(),
          "admitted batch descriptor reservation failed");
    }
    const auto appended = co_await detail::append_record(
      fields,
      value,
      *size,
      *output_,
      work,
      {.family = 1,
       .field = static_cast<std::uint16_t>(record_field::body_bytes)});
    if (!appended) co_return codec::failure(appended.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    KWAQUE_INVARIANT(
      invariant_id{"KQ-BATCH-BUILDER-SIZE"},
      output_->size() == *total,
      "complete record append differs from checked accounting");
    timestamp_base_ = base;
    count_ = item_count{count_.value() + 1U};
    headers_ = *next_headers;
    co_return codec::result<void>{};
}

seastar::future<codec::result<void>> batch_builder::add(
  const record& value,
  runtime::wall_time timestamp,
  codec::cooperative_work& work,
  byte_count parent_remaining) & {
    if (closed_) co_return codec::failure(at(errc::closed));
    closed_ = true;
    std::optional<codec::result<void>> outcome;
    std::exception_ptr exception;
    try {
        outcome.emplace(
          co_await append(value, timestamp, work, parent_remaining));
    } catch (...) {
        exception = std::current_exception();
    }
    if (!exception && outcome->has_value()) {
        if (auto ready = work.poll(anchor); !ready)
            *outcome = codec::failure(ready.error());
    }
    if (exception || !outcome->has_value()) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        output_.reset();
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(outcome->error());
    }
    closed_ = false;
    co_return codec::result<void>{};
}

seastar::future<codec::result<submitted_batch>> batch_builder::finalize(
  codec::cooperative_work& work, byte_count parent_remaining) & {
    if (closed_) co_return codec::failure(at(errc::closed));
    closed_ = true;
    bytes::fragmented_buffer records;
    std::optional<codec::semantic_batch_digest> digest;
    std::optional<submitted_batch_context> context;
    std::optional<codec::error> failed;
    std::exception_ptr exception;
    try {
        do {
            if (work.policy() != policy_) {
                failed = at(errc::invalid_argument);
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            if (!output_ || count_.value() == 0) {
                failed = at(errc::invalid_argument);
                break;
            }
            if (
              auto admitted = admit_size(output_->size(), parent_remaining);
              !admitted) {
                failed = admitted.error();
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
            const auto made = submitted_batch_context::make(
              id_,
              binding_,
              range_logical_count{count_.value()},
              *timestamp_base_);
            KWAQUE_INVARIANT(
              invariant_id{"KQ-BATCH-BUILDER-CONTEXT"},
              made.has_value(),
              "checked original count could not form its submitted context");
            context = *made;
            auto published = output_->finish();
            if (!published) {
                failed = codec::detail::allocation_cost_error(
                  published.error(), {}, 0);
                break;
            }
            records = std::move(*published);
            // finish transferred private descriptor storage to records. Hash
            // the original bytes before constructing the validated batch owner.
            if (auto ready = co_await work.checkpoint(anchor); !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            const auto computed = co_await compute_submitted_fingerprint(
              *context, records, work, anchor);
            if (!computed) {
                failed = computed.error();
                break;
            }
            digest = *computed;
        } while (false);
    } catch (...) {
        exception = std::current_exception();
    }
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    output_.reset();
    if (!failed && !exception) {
        if (auto ready = work.poll(anchor); !ready) failed = ready.error();
    }
    if (failed || exception) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        records = bytes::fragmented_buffer{};
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(*failed);
    }
    co_return submitted_batch{*context, *digest, headers_, std::move(records)};
}

seastar::future<> batch_builder::close(codec::cooperative_work& work) & {
    closed_ = true;
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    output_.reset();
}

} // namespace kwaque::model
