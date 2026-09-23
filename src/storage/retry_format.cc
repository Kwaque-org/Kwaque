#include "src/storage/retry_format.h"

#include "src/storage/footer_internal.h"
#include "src/storage/page_internal.h"
#include "src/storage/retry_entry_internal.h"

namespace kwaque::storage {
namespace detail {
class retry_codec final {
public:
    static retry_page
    make(page_ref ref, std::vector<completed_retry>&& entries) noexcept {
        return retry_page{ref, std::move(entries)};
    }
};
} // namespace detail
namespace {
using detail::load;
using detail::page_error;
using detail::store;
struct retry_reader final {
    footer_expectation root;
    page_ref reference;
    std::uint64_t start;
    std::optional<model::batch_id> previous;
    codec::decode_budget original;
    std::vector<completed_retry> entries;
    seastar::future<codec::result<decoded_retry_page>> operator()(
      bytes::fragmented_buffer_parser& input,
      codec::field_context c,
      codec::input_boundary,
      codec::decode_budget memory,
      codec::cooperative_work& work) {
        const auto anchor = page_error(errc::success, c);
        std::array<char, 100> fixed{};
        if (
          auto read = co_await detail::read_fixed(input, fixed, work, c); !read)
            co_return codec::failure(read.error());
        if (
          auto ready = co_await work.admit(
            byte_count{1024}, item_count{64}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        if (
          auto kind = detail::check_subkind(
            load<0, std::uint16_t>(fixed), 2, c);
          !kind)
            co_return codec::failure(kind.error());
        if (load<2, std::uint16_t>(fixed) != 0)
            co_return codec::failure(page_error(errc::malformed_data, c, 2));
        if (auto sc = detail::read_sc<4>(fixed, root.history.segment, c); !sc)
            co_return codec::failure(sc.error());
        if (load<76, std::uint64_t>(fixed) != root.position.value())
            co_return codec::failure(page_error(errc::wrong_context, c, 76));
        const auto count = load<92, std::uint32_t>(fixed);
        if (
          load<84, std::uint32_t>(fixed) != reference.ordinal().value()
          || load<88, std::uint32_t>(fixed) != reference.first_entry()
          || count != reference.entry_count())
            co_return codec::failure(page_error(errc::wrong_context, c, 84));
        const byte_count header{c.origin - start};
        const auto capacity = detail::page_wire(
          retry_page_capacity(header, root.history.alignment, work.policy()),
          c,
          92);
        if (!capacity) co_return codec::failure(capacity.error());
        if (count == 0)
            co_return codec::failure(page_error(errc::malformed_data, c, 92));
        if (count > *capacity)
            co_return codec::failure(
              page_error(errc::resource_exhausted, c, 92));
        const auto layout = detail::page_wire(
          aligned_envelope_layout::make(
            {header,
             retry_page_fixed_bytes,
             byte_count{static_cast<std::uint64_t>(count) * 160U}},
            root.history.alignment,
            work.policy(),
            {work.policy().config().max_page_bytes,
             work.policy().config().max_page_bytes}),
          c,
          96);
        if (!layout) co_return codec::failure(layout.error());
        if (
          layout->encoded_bytes() != reference.encoded_bytes()
          || input.total_bytes() != layout->body_bytes()
          || load<96, std::uint32_t>(fixed) != layout->padding_bytes().value())
            co_return codec::failure(page_error(errc::malformed_data, c, 96));
        const auto remaining = detail::reserve_entries(
          entries, count, memory, work.policy(), c);
        if (!remaining) co_return codec::failure(remaining.error());
        const byte_count retained_metadata{
          memory.metadata_remaining.value()
          - remaining->metadata_remaining.value()};
        for (std::uint32_t index = 0; index < count; ++index) {
            std::array<char, 160> raw{};
            auto entry_context = c;
            entry_context.origin += input.bytes_consumed().value();
            if (
              auto read = co_await detail::read_fixed(input, raw, work, c);
              !read)
                co_return codec::failure(read.error());
            if (
              auto ready = co_await work.admit(
                byte_count{1024}, item_count{64}, anchor);
              !ready)
                co_return codec::failure(ready.error());
            if (auto ready = work.poll(anchor); !ready)
                co_return codec::failure(ready.error());
            const auto entry = detail::read_retry_entry(
              raw, root.history.segment, entry_context, previous);
            if (!entry) co_return codec::failure(entry.error());
            if (
              entry->returned_span().count().value()
              > work.policy().config().max_original_records.value())
                co_return codec::failure(
                  page_error(errc::resource_exhausted, entry_context));
            previous = entry->id();
            entries.push_back(*entry);
        }
        if (
          auto read = co_await detail::read_padding(
            input, layout->padding_bytes(), work, c);
          !read)
            co_return codec::failure(read.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        // The body alias is released before publication. Only the returned
        // entry vector remains charged to the caller's original allowance.
        const auto retained = codec::detail::consume_decode_budget(
          work.policy(), original, {}, retained_metadata, c, c.origin);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-RETRY-RESIDUAL"},
          retained.has_value(),
          "admitted page metadata exceeded its enclosing reservation");
        co_return decoded_retry_page{
          detail::retry_codec::make(reference, std::move(entries)), *retained};
    }
};
seastar::future<codec::result<decoded_retry_page>> decode_page(
  bytes::fragmented_buffer_parser& input,
  const sealed_footer& root,
  page_ordinal ordinal,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context c,
  codec::input_boundary boundary,
  std::optional<model::batch_id> previous) {
    c.family = static_cast<std::uint16_t>(detail::sealed_family);
    const auto fail = [](codec::error error) {
        return seastar::make_ready_future<codec::result<decoded_retry_page>>(
          codec::failure(error));
    };
    const auto start = codec::detail::integer_read_start(input, c, boundary);
    if (!start) return fail(start.error());
    if (ordinal.value() >= root.pages().size())
        return fail(page_error(errc::invalid_argument, c));
    const auto ref = root.pages()[ordinal.value()];
    if (
      root.retry_count() > work.policy().config().max_object_entries.value()
      || root.pages().size() > work.policy().config().max_object_pages.value())
        return fail(page_error(errc::resource_exhausted, c));
    if (
      auto valid = detail::check_page_ref(
        ref,
        ordinal.value(),
        ref.first_entry(),
        static_cast<std::uint32_t>(retry_page_fixed_bytes.value()),
        static_cast<std::uint32_t>(completed_retry_wire_bytes.value()),
        root.location().history.alignment,
        work.policy(),
        c);
      !valid)
        return fail(valid.error());
    return detail::decode_pinned<decoded_retry_page>(
      input,
      detail::sealed_family,
      ref.digest(),
      memory,
      work,
      retry_reader{root.location(), ref, *start, previous, memory, {}},
      c,
      boundary,
      ref.encoded_bytes());
}
} // namespace

result<std::uint32_t> retry_page_capacity(
  byte_count header,
  storage_alignment alignment,
  const codec::limits& policy) noexcept {
    const auto tail = max_child_bytes(
      header,
      retry_page_fixed_bytes,
      alignment,
      policy,
      {policy.config().max_page_bytes, policy.config().max_page_bytes});
    if (!tail) return failure(tail.error());
    const auto count = std::min(
      tail->value() / completed_retry_wire_bytes.value(),
      policy.config().max_object_entries.value());
    return static_cast<std::uint32_t>(count);
}
seastar::future<codec::result<decoded_retry_page>> decode_retry_page(
  bytes::fragmented_buffer_parser& input,
  const sealed_footer& root,
  page_ordinal ordinal,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context c,
  codec::input_boundary boundary) {
    return decode_page(
      input, root, ordinal, memory, work, c, boundary, std::nullopt);
}

seastar::future<codec::result<encoded_retry_page>> encode_retry_page(
  std::span<const completed_retry> entries,
  footer_expectation root,
  page_ordinal ordinal,
  std::uint32_t first,
  codec::cooperative_work& work,
  byte_count remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context c) {
    c.family = static_cast<std::uint16_t>(detail::sealed_family);
    const auto anchor = page_error(errc::success, c);
    if (
      auto ready = co_await work.admit(
        byte_count{1024}, item_count{64}, anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (auto valid = detail::validate_footer_location(root, c); !valid)
        co_return codec::failure(valid.error());
    const auto cap = retry_page_capacity(
      byte_count{32}, root.history.alignment, work.policy());
    if (!cap)
        co_return codec::failure(
          codec::detail::allocation_cost_error(cap.error(), c, c.origin));
    if (entries.empty())
        co_return codec::failure(page_error(errc::invalid_argument, c));
    if (
      entries.size() > *cap || first > maximum_object_entries
      || entries.size() > maximum_object_entries - first
      || first + entries.size()
           > work.policy().config().max_object_entries.value()
      || ordinal.value() >= work.policy().config().max_object_pages.value())
        co_return codec::failure(page_error(errc::resource_exhausted, c));
    std::optional<model::batch_id> previous;
    for (const auto& entry : entries) {
        if (
          auto ready = co_await work.admit(
            byte_count{1024}, item_count{64}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        if (
          entry.original_binding().topic() != root.history.segment.topic()
          || entry.original_binding().range() != root.history.segment.range())
            co_return codec::failure(page_error(errc::wrong_context, c));
        if (previous && !previous->canonical_less(entry.id()))
            co_return codec::failure(page_error(errc::invalid_argument, c));
        if (
          entry.returned_span().count().value()
          > work.policy().config().max_original_records.value())
            co_return codec::failure(page_error(errc::resource_exhausted, c));
        previous = entry.id();
    }
    const auto layout = aligned_envelope_layout::make(
                          {byte_count{32},
                           retry_page_fixed_bytes,
                           byte_count{entries.size() * 160U}},
                          root.history.alignment,
                          work.policy(),
                          {work.policy().config().max_page_bytes,
                           work.policy().config().max_page_bytes})
                          .value();
    if (
      layout.encoded_bytes().value()
      > std::numeric_limits<std::uint64_t>::max() - c.origin)
        co_return codec::failure(page_error(errc::out_of_range, c));
    std::array<char, 100> fixed{};
    store<0>(fixed, std::uint16_t{2});
    detail::write_sc<4>(fixed, root.history.segment);
    store<76>(fixed, root.position.value());
    store<84>(fixed, ordinal.value());
    store<88>(fixed, first);
    store<92>(fixed, static_cast<std::uint32_t>(entries.size()));
    store<96>(
      fixed, static_cast<std::uint32_t>(layout.padding_bytes().value()));
    bytes::fragmented_buffer tail, output;
    std::optional<codec::immutable_object_digest> digest;
    std::optional<codec::error> failed;
    std::exception_ptr exception;
    try {
        do {
            auto encoded = co_await detail::encode_entries<160>(
              entries, detail::write_retry_entry, work, remaining, charge, c);
            if (!encoded) {
                failed = encoded.error();
                break;
            }
            tail = std::move(*encoded);
            encoded = co_await detail::encode_padded(
              fixed,
              std::move(tail),
              layout,
              detail::sealed_family,
              work,
              remaining,
              charge,
              c);
            if (!encoded) {
                failed = encoded.error();
                break;
            }
            output = std::move(*encoded);
            const auto hash = co_await detail::hash_exact(output, work, c);
            if (!hash) {
                failed = hash.error();
                break;
            }
            digest = *hash;
        } while (false);
    } catch (...) {
        exception = std::current_exception();
    }
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    tail = bytes::fragmented_buffer{};
    if (!failed && !exception) {
        if (auto ready = work.poll(anchor); !ready) failed = ready.error();
    }
    if (failed || exception) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        output = bytes::fragmented_buffer{};
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(*failed);
    }
    const auto reference = page_ref::make(
                             ordinal,
                             first,
                             static_cast<std::uint32_t>(entries.size()),
                             output.size(),
                             *digest)
                             .value();
    co_return encoded_retry_page{std::move(output), reference};
}

codec::result<void> retry_summary_verifier::ready(
  codec::cooperative_work& work, codec::field_context c) {
    if (state_ != state::open)
        return codec::failure(page_error(errc::closed, c));
    if (
      work.policy() != policy_
      || (root_.retry_count() == 0) != root_.pages().empty()) {
        state_ = state::closed;
        return codec::failure(page_error(errc::invalid_argument, c));
    }
    if (
      root_.retry_count() > policy_.config().max_object_entries.value()
      || root_.pages().size() > policy_.config().max_object_pages.value()) {
        state_ = state::closed;
        return codec::failure(page_error(errc::resource_exhausted, c));
    }
    if (auto valid = work.poll(page_error(errc::success, c)); !valid) {
        state_ = state::closed;
        return valid;
    }
    return {};
}
seastar::future<codec::result<decoded_retry_page>> retry_summary_verifier::next(
  bytes::fragmented_buffer_parser& input,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context c,
  codec::input_boundary boundary) {
    c.family = static_cast<std::uint16_t>(detail::sealed_family);
    if (auto valid = ready(work, c); !valid)
        co_return codec::failure(valid.error());
    if (next_ == root_.pages().size()) {
        state_ = state::closed;
        co_return codec::failure(page_error(errc::malformed_data, c));
    }
    const auto depth = input.checkpoint_depth();
    if (auto mark = input.push_checkpoint(); !mark) {
        state_ = state::closed;
        co_return codec::failure(
          codec::detail::allocation_cost_error(mark.error(), c, c.origin));
    }
    codec::detail::parser_transaction_guard transaction{input, depth};
    state_ = state::active;
    std::optional<codec::result<decoded_retry_page>> output;
    std::optional<codec::error> failed;
    std::exception_ptr exception;
    try {
        output.emplace(
          co_await decode_page(
            input,
            root_,
            page_ordinal::make(next_).value(),
            memory,
            work,
            c,
            boundary,
            last_));
        if (!output->has_value()) failed = output->error();
        if (!failed) {
            if (auto valid = work.poll(page_error(errc::success, c)); !valid)
                failed = valid.error();
        }
    } catch (...) {
        exception = std::current_exception();
    }
    if (failed || exception) {
        state_ = state::closed;
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        output.reset();
        if (exception) std::rethrow_exception(exception);
        co_return codec::failure(*failed);
    }
    last_ = (*output)->value.entries().back().id();
    ++next_;
    state_ = state::open;
    transaction.commit();
    co_return std::move(**output);
}
codec::result<verified_retry_summary> retry_summary_verifier::finish(
  codec::cooperative_work& work, codec::field_context c) {
    c.family = static_cast<std::uint16_t>(detail::sealed_family);
    if (auto valid = ready(work, c); !valid)
        return codec::failure(valid.error());
    state_ = state::closed;
    if (next_ != root_.pages().size())
        return codec::failure(page_error(errc::malformed_data, c));
    return verified_retry_summary{root_.digest(), root_.retry_count()};
}
} // namespace kwaque::storage
