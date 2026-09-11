#include "src/codec/crc32c_cooperative.h"

#include "src/base/invariant.h"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <algorithm>
#include <cstddef>
#include <exception>
#include <optional>
#include <span>
#include <utility>

namespace kwaque::codec {
namespace {

seastar::future<result<crc32c::value_type>> checksum_bytes(
  const bytes::fragmented_buffer& input,
  cooperative_work& work,
  crc32c::value_type seed,
  error anchor) {
    if (auto valid = work.poll(anchor); !valid) {
        co_return codec::failure(valid.error());
    }
    const auto policy = work.policy();
    if (
      auto valid = policy.validate_buffer(
        input.size(),
        input.retained_bytes(),
        item_count{input.fragment_count()},
        policy.config().max_retained_bytes);
      !valid) {
        KWAQUE_INVARIANT(
          invariant_id{"KQ-CODEC-CRC-INPUT"},
          valid.error() == errc::invalid_argument
            || valid.error() == errc::resource_exhausted,
          "input bound check returned an unexpected error");
        co_return codec::failure(
          error{
            valid.error() == errc::invalid_argument ? errc::invalid_argument
                                                    : errc::resource_exhausted,
            anchor.family(),
            anchor.field(),
            anchor.byte_offset()});
    }
    if (
      auto admitted = co_await work.admit(byte_count{}, item_count{}, anchor);
      !admitted) {
        co_return codec::failure(admitted.error());
    }
    if (auto valid = work.poll(anchor); !valid) {
        co_return codec::failure(valid.error());
    }
    crc32c checksum{seed};
    for (const auto fragment : input) {
        for (std::size_t offset = 0; offset < fragment.size();) {
            const auto size = std::min(
              fragment.size() - offset,
              static_cast<std::size_t>(work.byte_quantum().value()));
            if (
              auto admitted = co_await work.admit(
                byte_count{size}, item_count{1}, anchor);
              !admitted) {
                co_return codec::failure(admitted.error());
            }
            if (auto valid = work.poll(anchor); !valid) {
                co_return codec::failure(valid.error());
            }
            checksum.extend(
              std::span<const char>{fragment.data() + offset, size});
            offset += size;
        }
    }
    if (auto valid = work.poll(anchor); !valid) {
        co_return codec::failure(valid.error());
    }
    co_return checksum.value();
}

} // namespace

seastar::future<result<crc32c::value_type>> crc32c_cooperatively(
  bytes::fragmented_buffer input,
  cooperative_work& work,
  crc32c::value_type seed,
  error anchor) {
    std::optional<result<crc32c::value_type>> outcome;
    std::exception_ptr failure;
    try {
        outcome.emplace(co_await checksum_bytes(input, work, seed, anchor));
    } catch (...) {
        failure = std::current_exception();
    }
    const auto cleanup_bytes = input.fragment_count() == 0
                                 ? byte_count{}
                                 : work.byte_quantum();
    const auto cleanup_items = input.fragment_count() == 0
                                 ? item_count{1}
                                 : work.item_quantum();
    co_await work.drain_inline(cleanup_bytes, cleanup_items);
    // Release the bounded input before the final poll: a native owner may
    // request abort while being destroyed. Preserve an earlier failure.
    input = bytes::fragmented_buffer{};
    if (failure) {
        std::rethrow_exception(failure);
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-CODEC-CRC-OUTCOME"},
      outcome.has_value(),
      "checksum completed without a value or exception");
    if (!outcome->has_value()) {
        co_return codec::failure(outcome->error());
    }
    if (auto valid = work.poll(anchor); !valid) {
        co_return codec::failure(valid.error());
    }
    co_return **outcome;
}

} // namespace kwaque::codec
