#include "src/runtime/production/random.h"

#include <sys/random.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <random>
#include <system_error>

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
    } catch (const std::system_error&) {
        return failure(
          operation_error{errc::unavailable, operation_kind::random});
    }
}

namespace {

long kernel_random(void* buffer, std::size_t size, unsigned flags) noexcept {
    return static_cast<long>(::getrandom(buffer, size, flags));
}

} // namespace

result<identity_entropy> identity_entropy::make() {
    auto call = kernel_random;
    auto probed = detail::probe_identity_entropy(call);
    if (!probed) {
        return failure(probed.error());
    }
    return identity_entropy{};
}

void identity_entropy::fill(
  std::span<std::uint8_t, identity_entropy_bytes> octets) noexcept {
    auto call = kernel_random;
    detail::fill_identity_octets(call, octets);
}

} // namespace kwaque::runtime::production
