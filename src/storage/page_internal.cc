#include "src/storage/page_internal.h"

namespace kwaque::storage::detail {
codec::result<void> check_subkind(
  std::uint16_t actual, std::uint16_t expected, codec::field_context c) {
    if (actual == 0) return codec::failure(page_error(errc::malformed_data, c));
    if (actual > 2)
        return codec::failure(page_error(errc::unsupported_format, c));
    if (actual != expected)
        return codec::failure(page_error(errc::wrong_context, c));
    return {};
}
codec::result<void> check_page_ref(
  const page_ref& ref,
  std::uint32_t ordinal,
  std::uint32_t first,
  std::uint32_t fixed_bytes,
  std::uint32_t entry_bytes,
  storage_alignment alignment,
  const codec::limits& policy,
  codec::field_context c) {
    if (fixed_bytes == 0 || entry_bytes == 0)
        return codec::failure(page_error(errc::invalid_argument, c));
    if (ref.ordinal().value() != ordinal || ref.first_entry() != first)
        return codec::failure(page_error(errc::malformed_data, c));
    if (
      ref.encoded_bytes() > policy.config().max_page_bytes
      || ref.entry_count() > policy.config().max_object_entries.value()
      || ordinal >= policy.config().max_object_pages.value())
        return codec::failure(page_error(errc::resource_exhausted, c));
    // The actual page may have more header bytes, but never fewer than 32.
    const auto minimum = aligned_envelope_layout::make(
      {byte_count{32},
       byte_count{fixed_bytes},
       byte_count{static_cast<std::uint64_t>(ref.entry_count()) * entry_bytes}},
      alignment,
      policy,
      {policy.config().max_page_bytes, policy.config().max_page_bytes});
    if (!minimum)
        return codec::failure(
          codec::detail::allocation_cost_error(minimum.error(), c, c.origin));
    if (
      ref.encoded_bytes() < minimum->encoded_bytes()
      || ref.encoded_bytes().value() % alignment.bytes().value() != 0)
        return codec::failure(page_error(errc::malformed_data, c));
    return {};
}
codec::result<page_ref>
read_page_ref(const std::array<char, 48>& fixed, codec::field_context c) {
    const auto ordinal = page_wire(
      page_ordinal::make(load<0, std::uint32_t>(fixed)), c);
    if (!ordinal) return codec::failure(ordinal.error());
    return page_wire(
      page_ref::make(
        *ordinal,
        load<4, std::uint32_t>(fixed),
        load<8, std::uint32_t>(fixed),
        byte_count{load<12, std::uint32_t>(fixed)},
        codec::immutable_object_digest{read_digest<16>(fixed)}),
      c);
}
void write_page_ref(std::array<char, 48>& fixed, const page_ref& ref) noexcept {
    store<0>(fixed, ref.ordinal().value());
    store<4>(fixed, ref.first_entry());
    store<8>(fixed, ref.entry_count());
    store<12>(fixed, static_cast<std::uint32_t>(ref.encoded_bytes().value()));
    write_digest<16>(fixed, ref.digest());
}
seastar::future<codec::result<codec::immutable_object_digest>> hash_exact(
  const bytes::fragmented_buffer& input,
  codec::cooperative_work& work,
  codec::field_context c) {
    const auto anchor = page_error(errc::success, c);
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    codec::sha256_hasher hash;
    for (auto fragment : input) {
        for (std::size_t offset = 0; offset < fragment.size();) {
            const auto n = std::min(
              fragment.size() - offset,
              static_cast<std::size_t>(work.byte_quantum().value()));
            if (
              auto ready = co_await work.admit(
                byte_count{n}, item_count{1}, anchor);
              !ready)
                co_return codec::failure(ready.error());
            if (auto ready = work.poll(anchor); !ready)
                co_return codec::failure(ready.error());
            hash.update(fragment.data() + offset, n);
            offset += n;
        }
    }
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    co_return codec::immutable_object_digest{std::move(hash).final()};
}
seastar::future<codec::result<codec::immutable_object_digest>> hash_exact(
  bytes::fragmented_buffer_parser& input,
  byte_count length,
  codec::cooperative_work& work,
  codec::field_context c) {
    const auto anchor = page_error(errc::success, c);
    if (length > input.bytes_remaining())
        co_return codec::failure(page_error(errc::malformed_data, c));
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    codec::sha256_hasher hash;
    auto left = length.value();
    while (left != 0) {
        const auto fragment = input.peek_current_fragment();
        const auto n = std::min(
          {left,
           static_cast<std::uint64_t>(fragment.size()),
           work.byte_quantum().value()});
        if (
          auto ready = co_await work.admit(
            byte_count{n}, item_count{1}, anchor);
          !ready)
            co_return codec::failure(ready.error());
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        hash.update(fragment.data(), static_cast<std::size_t>(n));
        input.skip(byte_count{n}).value();
        left -= n;
    }
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    co_return codec::immutable_object_digest{std::move(hash).final()};
}
} // namespace kwaque::storage::detail
