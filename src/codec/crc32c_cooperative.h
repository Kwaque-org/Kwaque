#pragma once

#include "src/base/invariant.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/codec/cooperative.h"
#include "src/codec/crc32c.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <span>

namespace kwaque::codec {

// Read-only joined borrow. The caller keeps input alive and unmoved until the
// returned future completes, then owns its cleanup and final abort check.
// No alias, payload copy or owner release occurs in this helper.
[[nodiscard]] inline seastar::future<result<crc32c::value_type>>
crc32c_borrowed(
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

seastar::future<result<crc32c::value_type>> crc32c_borrowed(
  bytes::fragmented_buffer&&, cooperative_work&, crc32c::value_type, error)
  = delete;

// The caller has admitted input backing/descriptors and the verified native
// initialization peak against its remaining memory allowance. The checks here
// use recorded buffer bounds, not allocator introspection or new admission.
// Input transfers to the coroutine even on failure. work and its abort source
// stay alive and exclusively used until completion; opaque input cleanup is
// separately bounded by its owner. No input share or payload copy is created.
[[nodiscard]] seastar::future<result<crc32c::value_type>> crc32c_cooperatively(
  bytes::fragmented_buffer input,
  cooperative_work& work,
  crc32c::value_type seed = 0,
  error anchor = error{errc::success});

} // namespace kwaque::codec
