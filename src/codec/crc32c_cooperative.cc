#include "src/codec/crc32c_cooperative.h"

#include "src/base/invariant.h"

#include <seastar/core/coroutine.hh>

#include <exception>
#include <optional>
#include <utility>

namespace kwaque::codec {

seastar::future<result<crc32c::value_type>> crc32c_cooperatively(
  bytes::fragmented_buffer input,
  cooperative_work& work,
  crc32c::value_type seed,
  error anchor) {
    std::optional<result<crc32c::value_type>> outcome;
    std::exception_ptr failure;
    try {
        outcome.emplace(co_await crc32c_borrowed(input, work, seed, anchor));
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
