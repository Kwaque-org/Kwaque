#pragma once

#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"

#include <cstdint>
#include <limits>
#include <utility>

namespace kwaque::bytes {

// Native ownership fixtures for lifecycle tests. This target is test-only;
// production byte construction continues to copy or use its I/O boundary.
class fragmented_buffer_test_access final {
public:
    static void set_last_usable_generation(fragmented_buffer& buffer) noexcept {
        buffer.generation_ = std::numeric_limits<std::uint64_t>::max() - 1;
    }

    [[nodiscard]] static result<fragmented_buffer> adopt_fragment(
      fragmented_buffer::fragment_type storage, byte_count retained_bytes) {
        const byte_count size{storage.size()};
        if (
          retained_bytes < size
          || retained_bytes.value() > maximum_contiguous_allocation_bytes
          || (storage.empty() && retained_bytes != byte_count{})) {
            return failure(errc::invalid_argument);
        }
        if (storage.empty()) {
            return fragmented_buffer{};
        }
        fragmented_buffer::fragment_storage fragments;
        fragments.push_back(
          fragmented_buffer::owned_fragment{
            .storage = std::move(storage), .retained_bytes = retained_bytes});
        return fragmented_buffer{std::move(fragments), size, retained_bytes};
    }
};

} // namespace kwaque::bytes
