#include "src/storage/format_internal.h"

#include "src/bytes/fragmented_buffer_builder.h"
#include "src/codec/envelope_encode.h"
#include "src/codec/staging_cooperative.h"

#include <seastar/core/deleter.hh>

#include <algorithm>
#include <exception>
#include <optional>

namespace kwaque::storage::detail {
namespace {
codec::error at(errc code, codec::field_context context) noexcept {
    return codec::error{code, context.family, context.field, context.origin};
}
codec::result<byte_count> charged(
  byte_count request,
  const codec::limits& policy,
  kwaque::bytes::allocation_charge_fn charge,
  codec::field_context context) {
    if (request.value() == 0) return byte_count{};
    const auto cost = charge(request);
    if (cost < request)
        return codec::failure(at(errc::invalid_argument, context));
    if (!policy.validate_allocation(cost))
        return codec::failure(at(errc::resource_exhausted, context));
    return cost;
}
} // namespace

seastar::future<codec::result<void>> read_fixed(
  kwaque::bytes::fragmented_buffer_parser& input,
  std::span<char> output,
  codec::cooperative_work& work,
  codec::field_context context) {
    const auto anchor = at(errc::success, context);
    if (output.size() > input.bytes_remaining().value())
        co_return codec::failure(at(errc::malformed_data, context));
    const auto quantum = std::min(
      {std::uint64_t{32},
       work.byte_quantum().value() / 4U,
       work.item_quantum().value() / 2U});
    if (quantum == 0)
        co_return codec::failure(at(errc::resource_exhausted, context));
    while (!output.empty()) {
        const auto count = std::min<std::size_t>(output.size(), quantum);
        if (
          auto ready = co_await work.admit(
            byte_count{4U * count}, item_count{2U * count}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        const auto read = input.read_to(output.first(count));
        KWAQUE_INVARIANT(
          invariant_id{"KQ-STORAGE-FIXED-READ"},
          read.has_value(),
          "admitted fixed read failed");
        output = output.subspan(count);
    }
    co_return work.poll(anchor);
}

seastar::future<codec::result<void>> read_padding(
  kwaque::bytes::fragmented_buffer_parser& input,
  byte_count padding,
  codec::cooperative_work& work,
  codec::field_context context) {
    if (input.bytes_remaining() != padding)
        co_return codec::failure(at(errc::malformed_data, context));
    while (!input.at_end()) {
        auto current = context;
        current.origin += input.bytes_consumed().value();
        const auto anchor = at(errc::success, current);
        const auto fragment = input.peek_current_fragment();
        const auto count = std::min<std::size_t>(
          fragment.size(), work.byte_quantum().value());
        if (
          auto ready = co_await work.admit(
            byte_count{count}, item_count{1}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        if (
          auto valid = validate_zero_padding(
            {fragment.data(), count}, work.policy());
          !valid)
            co_return codec::failure(at(errc::malformed_data, current));
        const auto skipped = input.skip(byte_count{count});
        KWAQUE_INVARIANT(
          invariant_id{"KQ-STORAGE-PADDING-READ"},
          skipped.has_value(),
          "admitted padding read failed");
    }
    co_return work.poll(at(errc::success, context));
}

seastar::future<codec::result<kwaque::bytes::fragmented_buffer>> encode_padded(
  std::span<const char> fixed,
  kwaque::bytes::fragmented_buffer&& source,
  aligned_envelope_layout layout,
  codec::format_family family,
  codec::cooperative_work& work,
  byte_count remaining,
  kwaque::bytes::allocation_charge_fn charge,
  codec::field_context context) {
    auto child = std::move(source);
    kwaque::bytes::fragmented_buffer prefix, padding, body;
    std::optional<kwaque::bytes::fragmented_buffer_builder> padding_builder;
    std::optional<codec::result<kwaque::bytes::fragmented_buffer>> output;
    std::optional<codec::error> failed;
    std::exception_ptr exception;
    const auto anchor = at(errc::success, context);
    try {
        do {
            if (auto ready = co_await work.checkpoint(anchor); !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            const auto meaningful = child.size().checked_add(
              byte_count{fixed.size()});
            const auto body_bytes = meaningful ? meaningful->checked_add(
                                                   layout.padding_bytes())
                                               : std::nullopt;
            if (
              charge == nullptr || fixed.empty() || fixed.size() > 320
              || layout.header_bytes().value() != codec::envelope_prefix_bytes
              || !body_bytes || layout.body_bytes() != *body_bytes) {
                failed = at(errc::invalid_argument, context);
                break;
            }
            const auto cost = child.allocation_cost(charge);
            if (!cost) {
                failed = codec::detail::allocation_cost_error(
                  cost.error(), context, context.origin);
                break;
            }
            byte_count new_backing, new_metadata, padding_backing,
              padding_metadata;
            for (const auto size :
                 {byte_count{fixed.size()}, layout.padding_bytes()}) {
                if (size.value() == 0) continue;
                const auto backing = charged(
                  size, work.policy(), charge, context);
                const auto descriptors = charged(
                  byte_count{kwaque::bytes::fragmented_buffer::
                               fragment_descriptor_size()},
                  work.policy(),
                  charge,
                  context);
                const auto controls = charged(
                  byte_count{sizeof(seastar::free_deleter_impl)},
                  work.policy(),
                  charge,
                  context);
                if (!backing || !descriptors || !controls) {
                    failed = !backing       ? backing.error()
                             : !descriptors ? descriptors.error()
                                            : controls.error();
                    break;
                }
                // Two owners, each with three charges capped at 128 KiB.
                new_backing = *new_backing.checked_add(*backing);
                new_metadata = *new_metadata.checked_add(
                  byte_count{descriptors->value() + controls->value()});
                padding_backing = *backing;
                padding_metadata = byte_count{
                  descriptors->value() + controls->value()};
            }
            if (failed) break;
            if (layout.padding_bytes().value() == 0) {
                padding_backing = {};
                padding_metadata = {};
            }
            const auto metadata = cost->descriptors.checked_add(
              cost->share_controls);
            const auto bookkeeping = metadata
                                       ? metadata->checked_add(new_metadata)
                                       : std::nullopt;
            const auto backing = cost->backing.checked_add(new_backing);
            if (!bookkeeping || !backing) {
                failed = at(errc::out_of_range, context);
                break;
            }
            if (
              *backing > work.policy().config().max_retained_bytes
              || !work.policy().remaining_operation_bytes(
                {.retained_input = cost->backing,
                 .staged_output = new_backing,
                 .payload_bookkeeping = *bookkeeping},
                remaining)) {
                failed = at(errc::resource_exhausted, context);
                break;
            }
            if (
              auto ready = co_await work.admit(
                std::max(byte_count{512}, byte_count{2U * fixed.size()}),
                item_count{8},
                anchor);
              !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            auto copied = kwaque::bytes::fragmented_buffer::copy_of(fixed);
            if (!copied) {
                failed = codec::detail::allocation_cost_error(
                  copied.error(), context, context.origin);
                break;
            }
            prefix = std::move(*copied);
            const auto pad = layout.padding_bytes();
            if (pad.value() != 0) {
                padding_builder.emplace(
                  kwaque::bytes::fragmented_buffer_builder_config{
                    .initial_fragment_bytes = pad,
                    .max_fragment_bytes = pad,
                    .max_total_bytes = pad,
                    .max_retained_bytes = pad,
                    .max_fragments = 1});
                padding_builder->reserve_fragments(item_count{1}).value();
                std::array<char, 128> zeros{};
                while (padding_builder->size() < pad) {
                    const auto count = std::min<std::uint64_t>(
                      {zeros.size(),
                       pad.value() - padding_builder->size().value(),
                       work.byte_quantum().value() / 2U});
                    if (
                      auto ready = co_await work.admit(
                        byte_count{2U * count}, item_count{8}, anchor);
                      !ready) {
                        failed = ready.error();
                        break;
                    }
                    if (auto ready = work.poll(anchor); !ready) {
                        failed = ready.error();
                        break;
                    }
                    padding_builder
                      ->append(std::span<const char>{zeros}.first(count))
                      .value();
                }
                if (failed) break;
                padding = padding_builder->finish().value();
            }
            auto assembled = co_await codec::assemble_buffer_cooperatively(
              std::move(prefix),
              std::move(child),
              work,
              layout.body_bytes(),
              {.retained_input = padding_backing,
               .payload_bookkeeping = padding_metadata},
              remaining,
              charge,
              context);
            if (!assembled) {
                failed = assembled.error();
                break;
            }
            body = std::move(*assembled);
            if (!padding.empty()) {
                assembled = co_await codec::assemble_buffer_cooperatively(
                  std::move(body),
                  std::move(padding),
                  work,
                  layout.body_bytes(),
                  {},
                  remaining,
                  charge,
                  context);
                if (!assembled) {
                    failed = assembled.error();
                    break;
                }
                body = std::move(*assembled);
            }
            output.emplace(
              co_await codec::encode_envelope(
                std::move(body),
                family,
                work,
                {layout.body_bytes(), layout.encoded_bytes()},
                {},
                remaining,
                charge,
                context));
            if (!output->has_value()) failed = output->error();
        } while (false);
    } catch (...) {
        exception = std::current_exception();
    }
    if (padding_builder) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        padding_builder.reset();
    }
    // Reset remains valid after a move and releases retained buffers if a
    // child coroutine frame could not be allocated before taking ownership.
    // NOLINTNEXTLINE(bugprone-use-after-move)
    for (auto* value : {&child, &prefix, &padding, &body}) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        *value = kwaque::bytes::fragmented_buffer{};
    }
    if (!failed && !exception) {
        if (auto ready = work.poll(anchor); !ready) failed = ready.error();
    }
    if (failed || exception) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        output.reset();
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(*failed);
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-STORAGE-ENCODE-OUTCOME"},
      output && output->has_value(),
      "storage encoding completed without bytes");
    co_return std::move(**output);
}
} // namespace kwaque::storage::detail
