#include "src/runtime/cross_shard.h"

#include <cstdint>

namespace kwaque::runtime {

result<cross_shard_bytes>
cross_shard_bytes::copy(std::span<const std::byte> source) {
    if (source.size() > max_size) {
        operation_error error{
          errc::resource_exhausted, operation_kind::resource};
        static_cast<void>(error.add_context(
          operation_context_key::bytes,
          static_cast<std::uint64_t>(source.size())));
        return failure(std::move(error));
    }
    return cross_shard_bytes{
      std::vector<std::byte>{source.begin(), source.end()}};
}

} // namespace kwaque::runtime
