#include "src/codec/tests/envelope_bench_native.h"

namespace kwaque::codec::bench::checked {

namespace {

error extension_error(
  errc code,
  field_context context,
  header_extension_field field,
  std::uint64_t offset) noexcept {
    return error{
      code, context.family, static_cast<std::uint16_t>(field), offset};
}

} // namespace

seastar::future<result<item_count>> native_extensions(
  bytes::fragmented_buffer_parser& input,
  byte_count extension_bytes,
  byte_count fixed_header_bytes,
  cooperative_work& work,
  field_context context) {
    context.field = static_cast<std::uint16_t>(header_extension_field::region);
    const auto start = detail::integer_read_start(
      input, context, input_boundary::complete);
    if (!start) {
        co_return codec::failure(start.error());
    }
    const auto anchor = extension_error(
      errc::success, context, header_extension_field::region, *start);
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    if (input.checkpoint_depth() == 0 || fixed_header_bytes.value() < 32) {
        co_return codec::failure(extension_error(
          errc::invalid_argument,
          context,
          header_extension_field::region,
          *start));
    }
    const auto header_bytes = fixed_header_bytes.checked_add(extension_bytes);
    if (!header_bytes) {
        co_return codec::failure(extension_error(
          errc::invalid_argument,
          context,
          header_extension_field::region,
          *start));
    }
    const auto config = work.policy().config();
    if (*header_bytes > config.max_header_bytes) {
        co_return codec::failure(extension_error(
          errc::resource_exhausted,
          context,
          header_extension_field::region,
          *start));
    }
    if (extension_bytes > input.bytes_remaining()) {
        co_return codec::failure(extension_error(
          errc::malformed_data,
          context,
          header_extension_field::region,
          context.origin + input.total_bytes().value()));
    }
    const auto region_end = *start + extension_bytes.value();
    if (
      auto entered = co_await work.admit(byte_count{}, item_count{}, anchor);
      !entered) {
        co_return codec::failure(entered.error());
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }

    std::uint64_t remaining = extension_bytes.value();
    std::uint16_t previous_tag = 0;
    std::uint64_t count = 0;
    while (remaining != 0) {
        const auto prefix_start = region_end - remaining;
        if (remaining < header_extension_prefix_bytes) {
            co_return codec::failure(extension_error(
              errc::malformed_data,
              context,
              header_extension_field::tag,
              region_end));
        }
        const auto prefix_anchor = extension_error(
          errc::success, context, header_extension_field::tag, prefix_start);
        if (
          auto admitted = co_await work.admit(
            header_extension_prefix_work_bytes,
            header_extension_prefix_work_items,
            prefix_anchor);
          !admitted) {
            co_return codec::failure(admitted.error());
        }
        if (auto ready = work.poll(prefix_anchor); !ready) {
            co_return codec::failure(ready.error());
        }
        if (count == config.max_extensions.value()) {
            co_return codec::failure(extension_error(
              errc::resource_exhausted,
              context,
              header_extension_field::count,
              prefix_start));
        }
        ++count;
        std::array<char, header_extension_prefix_bytes> encoded{};
        const auto read = input.read_to(encoded);
        KWAQUE_INVARIANT(
          invariant_id{"KQ-HEADER-EXTENSION-PREFIX"},
          read.has_value(),
          "complete extension prefix could not be read");
        remaining -= header_extension_prefix_bytes;
        const auto tag = native_load<std::uint16_t>(encoded.data());
        const auto flags = native_load<std::uint16_t>(encoded.data() + 2);
        const auto value_length = native_load<std::uint32_t>(
          encoded.data() + 4);
        if (tag == 0 || tag <= previous_tag) {
            co_return codec::failure(extension_error(
              errc::malformed_data,
              context,
              header_extension_field::tag,
              prefix_start));
        }
        previous_tag = tag;
        if ((flags & std::uint16_t{0xfffe}) != 0) {
            co_return codec::failure(extension_error(
              errc::unsupported_format,
              context,
              header_extension_field::flags,
              prefix_start + 2));
        }
        if (value_length > remaining) {
            co_return codec::failure(extension_error(
              errc::malformed_data,
              context,
              header_extension_field::value,
              region_end));
        }
        if ((flags & std::uint16_t{1}) != 0) {
            co_return codec::failure(extension_error(
              errc::unsupported_format,
              context,
              header_extension_field::tag,
              prefix_start));
        }
        std::uint64_t value_remaining = value_length;
        while (value_remaining != 0) {
            const auto fragment = input.peek_current_fragment();
            const auto size = std::min(
              {value_remaining,
               static_cast<std::uint64_t>(fragment.size()),
               work.byte_quantum().value()});
            KWAQUE_INVARIANT(
              invariant_id{"KQ-HEADER-EXTENSION-VALUE"},
              size != 0,
              "bounded extension value has no readable fragment");
            const auto value_anchor = extension_error(
              errc::success,
              context,
              header_extension_field::value,
              region_end - remaining);
            if (
              auto admitted = co_await work.admit(
                byte_count{size}, item_count{2}, value_anchor);
              !admitted) {
                co_return codec::failure(admitted.error());
            }
            if (auto ready = work.poll(value_anchor); !ready) {
                co_return codec::failure(ready.error());
            }
            const auto skipped = input.skip(byte_count{size});
            KWAQUE_INVARIANT(
              invariant_id{"KQ-HEADER-EXTENSION-SKIP"},
              skipped.has_value(),
              "complete extension value could not be skipped");
            remaining -= size;
            value_remaining -= size;
        }
    }
    if (auto ready = work.poll(anchor); !ready) {
        co_return codec::failure(ready.error());
    }
    co_return item_count{count};
}

} // namespace kwaque::codec::bench::checked
