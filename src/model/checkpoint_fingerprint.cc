#include "src/codec/sha256.h"
#include "src/model/checkpoint_wire.h"
#include "src/model/fingerprint.h"

#include <seastar/core/coroutine.hh>

#include <cstdint>
#include <optional>
#include <utility>

namespace kwaque::model {

seastar::future<codec::result<codec::checkpoint_digest>>
compute_checkpoint_fingerprint(
  const read_checkpoint& value,
  codec::cooperative_work& work,
  codec::error anchor) {
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    const codec::field_context context{
      .origin = anchor.byte_offset(),
      .family = anchor.family(),
      .field = anchor.field()};
    const auto size = detail::checkpoint_body_size(
      value.cursors().size(), work.policy(), context);
    if (!size) co_return codec::failure(size.error());
    if (
      auto ready = co_await work.admit(byte_count{256}, item_count{16}, anchor);
      !ready)
        co_return codec::failure(ready.error());
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    std::optional<codec::checkpoint_digest> digest;
    {
        codec::sha256_hasher hasher;
        hasher.update(
          codec::checkpoint_domain.data(), codec::checkpoint_domain.size());
        const auto prefix = detail::encode_checkpoint_prefix(
          value.topic(), static_cast<std::uint32_t>(value.cursors().size()));
        hasher.update(prefix.data(), prefix.size());
        for (const auto& cursor : value.cursors()) {
            if (
              auto ready = co_await work.admit(
                byte_count{256}, item_count{16}, anchor);
              !ready)
                co_return codec::failure(ready.error());
            if (auto ready = work.poll(anchor); !ready)
                co_return codec::failure(ready.error());
            const auto encoded = detail::encode_checkpoint_cursor(cursor);
            hasher.update(encoded.data(), encoded.size());
        }
        if (auto ready = work.poll(anchor); !ready)
            co_return codec::failure(ready.error());
        digest.emplace(std::move(hasher).final());
    }
    if (auto ready = work.poll(anchor); !ready)
        co_return codec::failure(ready.error());
    co_return *digest;
}

} // namespace kwaque::model
