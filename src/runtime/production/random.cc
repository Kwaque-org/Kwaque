#include "src/runtime/production/random.h"

#include <cstdint>
#include <limits>
#include <new>
#include <random>

namespace kwaque::runtime::production {

static_assert(std::random_device::min() == 0);
static_assert(
  std::random_device::max() == std::numeric_limits<std::uint32_t>::max());

result<random_source> random_source::make() {
    try {
        std::random_device entropy;
        auto seed = detail::read_entropy_seed(entropy);
        if (!seed) {
            return failure(seed.error());
        }
        return random_source{*seed};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        return failure(
          operation_error{errc::unavailable, operation_kind::random});
    }
}

} // namespace kwaque::runtime::production
