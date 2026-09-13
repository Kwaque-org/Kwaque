#include "src/base/invariant.h"
#include "src/codec/staging_cooperative.h"
#include "src/codec/tests/envelope_bench_native.h"
#include "src/codec/transaction.h"
#include "src/model/batch_wire.h"
#include "src/model/tests/model_bench_reference.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <array>
#include <cstdint>
#include <exception>
#include <limits>
#include <optional>
#include <span>
#include <type_traits>
#include <utility>

namespace kwaque::model::bench {
namespace {
using bytes::fragmented_buffer;
codec::error at(errc code, codec::field_context context) {
    return codec::error{code, context.family, context.field, context.origin};
}

template<bool Assigned>
seastar::future<codec::result<fragmented_buffer>> encode_body(
  std::conditional_t<Assigned, assigned_batch_context, submitted_batch_context>
    metadata,
  codec::semantic_batch_digest fingerprint,
  item_count headers,
  fragmented_buffer& records,
  fragmented_buffer& prefix,
  fragmented_buffer& body,
  codec::cooperative_work& work,
  byte_count remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context context) {
    const auto submitted = [&] {
        if constexpr (Assigned)
            return metadata.submitted();
        else
            return metadata;
    }();
    const auto retained = [&] {
        if constexpr (Assigned)
            return metadata.retained_count();
        else
            return item_count{submitted.original_count().value()};
    }();
    constexpr auto fixed_bytes = Assigned ? assigned_batch_fixed_bytes
                                          : submitted_batch_fixed_bytes;
    constexpr auto family = Assigned ? codec::format_family::assigned_batch
                                     : codec::format_family::submitted_batch;
    const auto anchor = at(errc::success, context);
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (charge == nullptr)
        co_return codec::failure(at(errc::invalid_argument, context));
    const auto policy = work.policy();
    const auto config = policy.config();
    const auto counts = policy.validate_batch_counts(
      item_count{submitted.original_count().value()}, retained, headers);
    if (!counts)
        co_return codec::failure(
          codec::detail::allocation_cost_error(
            counts.error(), context, context.origin));
    if (records.size() > config.max_expanded_batch_bytes)
        co_return codec::failure(at(errc::resource_exhausted, context));
    if (records.empty() || retained.value() > records.size().value() / 7U)
        co_return codec::failure(at(errc::invalid_argument, context));
    const auto total = records.size().checked_add(fixed_bytes);
    if (
      !total
      || total->value() > std::numeric_limits<std::uint64_t>::max()
                            - codec::envelope_prefix_bytes
      || total->value() + codec::envelope_prefix_bytes
           > std::numeric_limits<std::uint64_t>::max() - context.origin)
        co_return codec::failure(at(errc::invalid_argument, context));
    if (*total > config.max_encoded_body_bytes)
        co_return codec::failure(at(errc::resource_exhausted, context));
    if (auto ready = co_await work.checkpoint(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    // Existing backing is already owned. This preflight gates only the new
    // fixed prefix allocations; cooperative assembly admits all overlap next.
    const auto input = records.allocation_cost(charge);
    if (!input)
        co_return codec::failure(
          codec::detail::allocation_cost_error(
            input.error(), context, context.origin));
    const auto payload = policy.validate_buffer(
      records.size(),
      input->backing,
      input->fragments,
      config.max_expanded_batch_bytes);
    if (!payload)
        co_return codec::failure(
          codec::detail::allocation_cost_error(
            payload.error(), context, context.origin));
    const byte_count descriptor_request{
      fragmented_buffer::fragment_descriptor_size()};
    const auto prefix_backing = charge(fixed_bytes);
    const auto prefix_metadata = charge(descriptor_request);
    if (prefix_backing < fixed_bytes || prefix_metadata < descriptor_request)
        co_return codec::failure(at(errc::invalid_argument, context));
    if (
      !policy.validate_allocation(prefix_backing)
      || !policy.validate_allocation(prefix_metadata))
        co_return codec::failure(at(errc::resource_exhausted, context));
    auto bookkeeping = input->descriptors.checked_add(input->share_controls);
    if (bookkeeping) bookkeeping = bookkeeping->checked_add(prefix_metadata);
    const auto backing = input->backing.checked_add(prefix_backing);
    if (!bookkeeping || !backing)
        co_return codec::failure(at(errc::out_of_range, context));
    if (
      *backing > config.max_retained_bytes
      || !policy.remaining_operation_bytes(
        {.retained_input = input->backing,
         .staged_output = prefix_backing,
         .payload_bookkeeping = *bookkeeping},
        remaining))
        co_return codec::failure(at(errc::resource_exhausted, context));
    if (
      auto ready = co_await work.admit(
        byte_count{1024}, item_count{64}, anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    std::array<char, fixed_bytes.value()> fixed{};
    const auto identity = detail::encode_original_identity(submitted);
    const auto digest = fingerprint.bytes();
    std::copy(identity.begin(), identity.end(), fixed.begin());
    std::copy(digest.begin(), digest.end(), fixed.begin() + 104);
    detail::batch_store<136>(
      fixed, submitted.original_timestamp_base().unix_nanoseconds());
    detail::batch_store<144>(
      fixed, static_cast<std::uint32_t>(submitted.original_count().value()));
    detail::batch_store<148>(
      fixed, static_cast<std::uint32_t>(retained.value()));
    detail::batch_store<152>(
      fixed, static_cast<std::uint32_t>(headers.value()));
    // codec/reserved remain zero; record profile is exactly one.
    detail::batch_store<158>(fixed, std::uint16_t{1});
    detail::batch_store<160>(
      fixed, static_cast<std::uint32_t>(records.size().value()));
    detail::batch_store<164>(
      fixed, static_cast<std::uint32_t>(records.size().value()));
    if constexpr (Assigned) {
        detail::batch_store<168>(
          fixed, metadata.logical_span().begin().value());
        detail::batch_store<176>(fixed, metadata.logical_span().end().value());
    }
    auto copied = fragmented_buffer::copy_of(std::span<const char>{fixed});
    if (!copied)
        co_return codec::failure(
          codec::detail::allocation_cost_error(
            copied.error(), context, context.origin));
    prefix = std::move(*copied);
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const codec::field_context body_context{
      .origin = context.origin + codec::envelope_prefix_bytes,
      .family = context.family,
      .field = context.field};
    auto assembled = co_await codec::assemble_buffer_cooperatively(
      std::move(prefix),
      std::move(records),
      work,
      config.max_encoded_body_bytes,
      {},
      remaining,
      charge,
      body_context);
    if (!assembled) co_return codec::failure(assembled.error());
    body = std::move(*assembled);
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    KWAQUE_INVARIANT(
      invariant_id{"KQ-BATCH-BODY-SIZE"},
      body.size() == *total,
      "batch body differs from its finalized fixed fields and record "
      "extent");
    co_return co_await codec::bench::checked::native_encode(
      std::move(body),
      family,
      work,
      {*total, byte_count{total->value() + codec::envelope_prefix_bytes}},
      {},
      remaining,
      charge,
      context);
}
template<bool Assigned>
seastar::future<codec::result<bytes::fragmented_buffer>> encode_batch(
  std::conditional_t<Assigned, assigned_batch, submitted_batch>&& batch,
  codec::cooperative_work& work,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context context) {
    const auto submitted = batch.context();
    const auto fingerprint = batch.fingerprint();
    const auto headers = batch.header_count();
    auto records = std::move(batch).release_records();
    fragmented_buffer prefix;
    fragmented_buffer body;
    context.family = static_cast<std::uint16_t>(
      Assigned ? codec::format_family::assigned_batch
               : codec::format_family::submitted_batch);
    std::optional<codec::result<fragmented_buffer>> outcome;
    std::exception_ptr exception;
    try {
        outcome.emplace(
          co_await encode_body<Assigned>(
            submitted,
            fingerprint,
            headers,
            records,
            prefix,
            body,
            work,
            parent_remaining,
            charge,
            context));
    } catch (...) {
        exception = std::current_exception();
    }
    for (auto* owner : {&records, &prefix, &body}) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        *owner = fragmented_buffer{};
    }
    if (exception) std::rethrow_exception(exception);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-BATCH-ENCODE-OUTCOME"},
      outcome.has_value(),
      "batch writer completed without an outcome");
    if (!outcome->has_value()) co_return codec::failure(outcome->error());
    if (auto ready = work.poll(at(errc::success, context)); !ready) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        outcome.reset();
        co_return codec::failure(ready.error());
    }
    co_return std::move(**outcome);
}

} // namespace

seastar::future<codec::result<bytes::fragmented_buffer>>
checked_encode_submitted(
  submitted_batch&& batch,
  codec::cooperative_work& work,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context context) {
    return encode_batch<false>(
      std::move(batch), work, parent_remaining, charge, context);
}

seastar::future<codec::result<bytes::fragmented_buffer>>
checked_encode_assigned(
  assigned_batch&& batch,
  codec::cooperative_work& work,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context context) {
    return encode_batch<true>(
      std::move(batch), work, parent_remaining, charge, context);
}

} // namespace kwaque::model::bench
