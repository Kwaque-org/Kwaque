#include "src/storage/format_internal.h"

#include "src/bytes/fragmented_buffer_builder.h"
#include "src/codec/crc32c_cooperative.h"
#include "src/codec/envelope.h"
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

codec::result<padded_prefix_cost> plan_padded_prefix(
  byte_count fixed,
  const codec::limits& policy,
  kwaque::bytes::allocation_charge_fn charge,
  codec::field_context context) {
    if (!charge || fixed.value() == 0 || fixed.value() > 320)
        return codec::failure(at(errc::invalid_argument, context));
    const auto combined = charged(
      byte_count{codec::envelope_prefix_bytes + fixed.value()},
      policy,
      charge,
      context);
    const auto header = charged(
      byte_count{codec::envelope_prefix_bytes}, policy, charge, context);
    const auto fields = charged(fixed, policy, charge, context);
    if (!header || !fields) {
        if (combined) return padded_prefix_cost{*combined, 1};
        return codec::failure(!header ? header.error() : fields.error());
    }
    const auto separate = *header->checked_add(*fields);
    if (combined && *combined <= separate)
        return padded_prefix_cost{*combined, 1};
    return padded_prefix_cost{separate, 2};
}

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
    kwaque::bytes::fragmented_buffer prefix, fixed_owner, padding;
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
            const auto prefix_cost = plan_padded_prefix(
              byte_count{fixed.size()}, work.policy(), charge, context);
            if (!prefix_cost) {
                failed = prefix_cost.error();
                break;
            }
            byte_count new_backing = prefix_cost->backing;
            byte_count new_metadata;
            const byte_count prefix_bytes{
              codec::envelope_prefix_bytes + fixed.size()};
            const auto owner_descriptor = charged(
              byte_count{
                kwaque::bytes::fragmented_buffer::fragment_descriptor_size()},
              work.policy(),
              charge,
              context);
            const auto owner_control = charged(
              byte_count{sizeof(seastar::free_deleter_impl)},
              work.policy(),
              charge,
              context);
            if (!owner_descriptor || !owner_control) {
                failed = !owner_descriptor ? owner_descriptor.error()
                                           : owner_control.error();
                break;
            }
            const auto small_owners
              = prefix_cost->fragments
                + (layout.padding_bytes().value() != 0 ? 1U : 0U);
            new_metadata = byte_count{
              small_owners
              * (owner_descriptor->value() + owner_control->value())};
            if (layout.padding_bytes().value() != 0) {
                const auto pad_backing = charged(
                  layout.padding_bytes(), work.policy(), charge, context);
                if (!pad_backing) {
                    failed = pad_backing.error();
                    break;
                }
                new_backing = *new_backing.checked_add(*pad_backing);
            }
            const auto metadata = cost->descriptors.checked_add(
              cost->share_controls);
            const auto nodes = child.fragment_count() + small_owners;
            const auto descriptors = charged(
              byte_count{
                nodes
                * kwaque::bytes::fragmented_buffer::fragment_descriptor_size()},
              work.policy(),
              charge,
              context);
            if (!descriptors) {
                failed = descriptors.error();
                break;
            }
            new_metadata = *new_metadata.checked_add(*descriptors);
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
            // Complete the envelope privately. Hash the exact body in wire
            // order while its owner remains live; no checksum alias is needed.
            // Only the published prefix range is initialized and later read.
            std::array<char, codec::envelope_prefix_bytes + 320> fixed_prefix;
            std::copy(
              fixed.begin(),
              fixed.end(),
              fixed_prefix.begin() + codec::envelope_prefix_bytes);
            codec::crc32c checksum;
            checksum.extend(fixed);
            const auto child_crc = co_await codec::crc32c_borrowed(
              child,
              work,
              checksum.value(),
              codec::error{
                errc::success,
                context.family,
                static_cast<std::uint16_t>(codec::envelope_field::body_crc32c),
                context.origin + 24});
            if (!child_crc) {
                failed = child_crc.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            checksum = codec::crc32c{*child_crc};
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
                if (
                  auto ready = co_await work.admit(
                    byte_count{128}, item_count{1}, anchor);
                  !ready) {
                    failed = ready.error();
                    break;
                }
                if (auto ready = work.poll(anchor); !ready) {
                    failed = ready.error();
                    break;
                }
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
                    const auto bytes = std::span<const char>{zeros}.first(
                      count);
                    padding_builder->append(bytes).value();
                    checksum.extend(bytes);
                }
                if (failed) break;
                padding = padding_builder->finish().value();
            }
            if (
              auto ready = co_await work.admit(
                codec::envelope_prefix_work_bytes,
                codec::envelope_prefix_work_items,
                anchor);
              !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            auto header = codec::encode_envelope_prefix(
              {family, layout.body_bytes(), checksum.value(), 0},
              work.policy(),
              {layout.body_bytes(), layout.encoded_bytes()},
              context);
            if (!header) {
                failed = header.error();
                break;
            }
            if (
              auto ready = co_await work.admit(
                codec::envelope_prefix_work_bytes,
                codec::envelope_prefix_work_items,
                anchor);
              !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            codec::crc32c header_checksum;
            header_checksum.extend(std::span<const char>{*header});
            seastar::write_le(header->data() + 28, header_checksum.value());
            std::copy(header->begin(), header->end(), fixed_prefix.begin());
            if (
              auto ready = co_await work.admit(
                prefix_bytes, item_count{8}, anchor);
              !ready) {
                failed = ready.error();
                break;
            }
            if (auto ready = work.poll(anchor); !ready) {
                failed = ready.error();
                break;
            }
            const auto prefix_view = std::span<const char>{fixed_prefix}.first(
              prefix_bytes.value());
            auto copied = kwaque::bytes::fragmented_buffer::copy_of(
              prefix_cost->fragments == 1
                ? prefix_view
                : prefix_view.first(codec::envelope_prefix_bytes));
            if (!copied) {
                failed = codec::detail::allocation_cost_error(
                  copied.error(), context, context.origin);
                break;
            }
            prefix = std::move(*copied);
            if (prefix_cost->fragments == 2) {
                copied = kwaque::bytes::fragmented_buffer::copy_of(
                  prefix_view.subspan(codec::envelope_prefix_bytes));
                if (!copied) {
                    failed = codec::detail::allocation_cost_error(
                      copied.error(), context, context.origin);
                    break;
                }
                fixed_owner = std::move(*copied);
                output.emplace(
                  co_await codec::assemble_buffer_cooperatively(
                    std::move(prefix),
                    std::move(fixed_owner),
                    std::move(child),
                    std::move(padding),
                    work,
                    layout.encoded_bytes(),
                    {},
                    remaining,
                    charge,
                    context));
            } else {
                output.emplace(
                  co_await codec::assemble_buffer_cooperatively(
                    std::move(prefix),
                    std::move(child),
                    std::move(padding),
                    work,
                    layout.encoded_bytes(),
                    {},
                    remaining,
                    charge,
                    context));
            }
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
    for (auto* value : {&child, &prefix, &fixed_owner, &padding}) {
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
