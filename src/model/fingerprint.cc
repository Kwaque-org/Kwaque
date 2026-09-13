#include "src/model/fingerprint.h"

#include "src/codec/sha256.h"
#include "src/model/batch_wire.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>

namespace kwaque::model {

seastar::future<codec::result<codec::semantic_batch_digest>>
compute_submitted_fingerprint(
  submitted_batch_context context,
  const bytes::fragmented_buffer& records,
  codec::cooperative_work& work,
  codec::error anchor) {
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    if (
      context.original_count().value()
      > work.policy().config().max_original_records.value())
        co_return codec::failure(
          codec::error{
            errc::resource_exhausted,
            anchor.family(),
            anchor.field(),
            anchor.byte_offset()});
    const auto valid = work.policy().validate_buffer(
      records.size(),
      records.retained_bytes(),
      item_count{records.fragment_count()},
      work.policy().config().max_expanded_batch_bytes);
    if (!valid)
        co_return codec::failure(
          codec::error{
            errc::resource_exhausted,
            anchor.family(),
            anchor.field(),
            anchor.byte_offset()});
    if (
      records.empty()
      || context.original_count().value() > records.size().value() / 7U)
        co_return codec::failure(
          codec::error{
            errc::invalid_argument,
            anchor.family(),
            anchor.field(),
            anchor.byte_offset()});
    // Bounded fixed projection construction, including initialization/copies.
    if (
      auto ready = co_await work.admit(byte_count{768}, item_count{64}, anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    std::optional<codec::semantic_batch_digest> digest;
    {
        std::array<char, 127> prefix{};
        const auto identity = detail::encode_original_identity(context);
        std::copy(
          codec::semantic_batch_domain.begin(),
          codec::semantic_batch_domain.end(),
          prefix.begin());
        std::copy(identity.begin(), identity.end(), prefix.begin() + 11);
        detail::batch_store<115>(
          prefix, context.original_timestamp_base().unix_nanoseconds());
        detail::batch_store<123>(
          prefix, static_cast<std::uint32_t>(context.original_count().value()));
        codec::sha256_hasher hasher;
        hasher.update(prefix.data(), prefix.size());
        for (const auto fragment : records) {
            for (std::size_t offset = 0; offset < fragment.size();) {
                const auto size = std::min(
                  fragment.size() - offset,
                  static_cast<std::size_t>(work.byte_quantum().value()));
                if (
                  auto ready = co_await work.admit(
                    byte_count{size}, item_count{1}, anchor);
                  !ready)
                    co_return codec::failure(ready.error());
                if (auto ready = work.poll(anchor); !ready)
                    co_return codec::failure(ready.error());
                hasher.update(fragment.data() + offset, size);
                offset += size;
            }
        }
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        digest.emplace(std::move(hasher).final());
    } // Native SHA state is destroyed before the final poll and publication.
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    co_return *digest;
}

} // namespace kwaque::model
