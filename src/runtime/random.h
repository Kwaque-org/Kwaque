#ifndef KWAQUE_SRC_RUNTIME_RANDOM_H_
#define KWAQUE_SRC_RUNTIME_RANDOM_H_

#include "src/runtime/error.h"

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <span>

namespace kwaque::runtime {

// The owning backend enforces shard affinity when it exposes a source. Keeping
// the draw contract static avoids an indirect call and an affinity check on
// every generated word.
template<typename Source>
concept random_source = requires(Source& source) {
    { source.next_u64() } noexcept -> std::same_as<std::uint64_t>;
};

namespace detail {

template<random_source Source>
[[nodiscard]] std::uint64_t
uniform_u64_unchecked(Source& source, std::uint64_t upper_exclusive) noexcept {
    auto word = source.next_u64();
    auto product = static_cast<__uint128_t>(word) * upper_exclusive;
    auto low = static_cast<std::uint64_t>(product);
    if (low < upper_exclusive) {
        const auto threshold = -upper_exclusive % upper_exclusive;
        while (low < threshold) {
            word = source.next_u64();
            product = static_cast<__uint128_t>(word) * upper_exclusive;
            low = static_cast<std::uint64_t>(product);
        }
    }
    return static_cast<std::uint64_t>(product >> 64U);
}

} // namespace detail

class probability_ratio final {
public:
    [[nodiscard]] static result<probability_ratio>
    make(std::uint64_t numerator, std::uint64_t denominator) noexcept {
        if (denominator == 0 || numerator > denominator) {
            return failure(
              operation_error{errc::invalid_argument, operation_kind::random});
        }
        return probability_ratio{numerator, denominator};
    }

    [[nodiscard]] constexpr std::uint64_t numerator() const noexcept {
        return numerator_;
    }
    [[nodiscard]] constexpr std::uint64_t denominator() const noexcept {
        return denominator_;
    }

    bool operator==(const probability_ratio&) const = default;

private:
    constexpr probability_ratio(
      std::uint64_t numerator, std::uint64_t denominator) noexcept
      : numerator_(numerator)
      , denominator_(denominator) {}

    std::uint64_t numerator_;
    std::uint64_t denominator_;
};

// Uniformly selects [0, upper_exclusive) with multiply-high rejection. The
// upper bound of one consumes no random word; zero is a typed caller error.
template<random_source Source>
[[nodiscard]] result<std::uint64_t>
uniform_u64(Source& source, std::uint64_t upper_exclusive) noexcept {
    if (upper_exclusive == 0) {
        return failure(
          operation_error{errc::invalid_argument, operation_kind::random});
    }
    if (upper_exclusive == 1) {
        return std::uint64_t{0};
    }

    return detail::uniform_u64_unchecked(source, upper_exclusive);
}

template<random_source Source>
[[nodiscard]] bool
chance(Source& source, probability_ratio probability) noexcept {
    if (probability.numerator() == 0) {
        return false;
    }
    if (probability.numerator() == probability.denominator()) {
        return true;
    }
    return detail::uniform_u64_unchecked(source, probability.denominator())
           < probability.numerator();
}

// Emits a canonical little-endian word stream and never allocates. The caller
// owns the span and therefore owns the work/size bound.
template<random_source Source>
void fill_bytes(Source& source, std::span<std::byte> destination) noexcept {
    std::size_t offset = 0;
    while (offset < destination.size()) {
        auto word = source.next_u64();
        const auto count = std::min<std::size_t>(
          sizeof(word), destination.size() - offset);
        for (std::size_t index = 0; index < count; ++index) {
            destination[offset + index] = static_cast<std::byte>(
              word & std::uint64_t{0xff});
            word >>= 8U;
        }
        offset += count;
    }
}

} // namespace kwaque::runtime

#endif // KWAQUE_SRC_RUNTIME_RANDOM_H_
