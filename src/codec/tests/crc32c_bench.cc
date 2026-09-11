#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/codec/cooperative.h"
#include "src/codec/crc32c.h"
#include "src/codec/crc32c_cooperative.h"
#include "src/codec/error.h"
#include "src/codec/limits.h"
#include "src/codec/tests/codec_bench_fixture.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/testing/perf_tests.hh>

#include <absl/crc/crc32c.h>
#include <crc32c/crc32c.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

namespace kwaque::codec::bench {
namespace {

constexpr std::size_t bytes_per_run = 4U * 1024U * 1024U;
constexpr std::uint32_t seed = 0;

// Keep the same input-memory barrier on both sides; consuming the result alone
// does not prevent an inline checksum from being hoisted out of the loop.
[[gnu::always_inline]] inline void clobber_memory() {
    // Empty compiler barrier prevents hoisting; it emits no instructions.
    // NOLINTNEXTLINE(portability-no-assembler)
    asm volatile("" : : : "memory");
}

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

std::uint32_t google_extend(std::uint32_t crc, std::span<const char> bytes) {
    return bytes.empty()
             ? crc
             : ::crc32c::Extend(
                 crc,
                 reinterpret_cast<const std::uint8_t*>(bytes.data()),
                 bytes.size());
}

template<bool Google>
std::uint32_t checksum(std::span<const char> bytes) {
    if constexpr (Google) {
        return google_extend(seed, bytes);
    } else {
        codec::crc32c crc{seed};
        crc.extend(bytes);
        return crc.value();
    }
}

template<std::size_t Iterations, typename Function>
std::size_t measure_checksums(Function function, std::uint32_t expected) {
    std::uint32_t last = 0;
    perf_tests::start_measuring_time();
    for (std::size_t iteration = 0; iteration < Iterations; ++iteration) {
        clobber_memory();
        last = function();
        perf_tests::do_not_optimize(last);
    }
    perf_tests::stop_measuring_time();
    require(last == expected, "CRC benchmark result mismatch");
    // One iteration is one complete checksum/copy, never one input byte.
    return Iterations;
}

template<typename T>
auto little_endian_bytes(T value) {
    return std::bit_cast<std::array<char, sizeof(T)>>(
      seastar::cpu_to_le(value));
}

template<std::size_t Size>
auto prefix_fields() {
    static_assert(Size == 32 || Size == 48);
    if constexpr (Size == 32) {
        return std::tuple{
          std::uint32_t{0x4642514b},
          std::uint16_t{1},
          std::uint16_t{1},
          std::uint16_t{1},
          std::uint16_t{32},
          std::uint32_t{4096},
          std::uint64_t{0},
          std::uint32_t{0x9c71fe32},
          std::uint32_t{0}};
    } else {
        return std::tuple{
          std::uint32_t{0x4657514b},
          std::uint16_t{1},
          std::uint16_t{16},
          std::uint16_t{48},
          std::uint16_t{0},
          std::uint32_t{4096},
          std::uint64_t{7},
          std::uint64_t{0x0102030405060708},
          std::uint64_t{3},
          std::uint32_t{0},
          std::uint32_t{0x9c71fe32}};
    }
}

template<typename Fields, typename Function>
void each_field(const Fields& fields, Function function) {
    std::apply(
      [&function](const auto&... field) { (function(field), ...); }, fields);
}

template<std::size_t Size>
std::vector<char> prefix_bytes() {
    std::vector<char> bytes(Size);
    std::size_t offset = 0;
    each_field(prefix_fields<Size>(), [&bytes, &offset](auto value) {
        const auto encoded = little_endian_bytes(value);
        std::ranges::copy(encoded, bytes.data() + offset);
        offset += encoded.size();
    });
    require(offset == Size, "CRC prefix field widths mismatch");
    return bytes;
}

std::vector<char> patterned_bytes(std::size_t size) {
    std::vector<char> bytes(size);
    for (std::size_t index = 0; index < size; ++index) {
        bytes[index] = std::bit_cast<char>(
          static_cast<std::uint8_t>((index * 31U + 7U) & 0xffU));
    }
    return bytes;
}

template<std::size_t Size>
class hot_crc_fixture {
    static_assert(Size > 0 && Size <= maximum_contiguous_allocation_bytes);

public:
    hot_crc_fixture()
      : data_(make_data()) {
        expected_ = checksum<true>(data_);
        require(
          checksum<false>(data_) == expected_, "CRC backend fixture mismatch");
    }

    template<bool Google>
    std::size_t execute() {
        return measure_checksums<std::max<std::size_t>(
          1, bytes_per_run / Size)>(
          [this] { return checksum<Google>(data_); }, expected_);
    }

private:
    static std::vector<char> make_data() {
        if constexpr (Size == 32 || Size == 48) {
            return prefix_bytes<Size>();
        } else {
            return patterned_bytes(Size);
        }
    }
    std::vector<char> data_;
    std::uint32_t expected_;
};

template<std::size_t Size>
class incremental_crc_fixture {
public:
    incremental_crc_fixture() {
        expected_ = checksum<true>(prefix_bytes<Size>());
        require(
          calculate<true>() == expected_ && calculate<false>() == expected_,
          "incremental prefix fixture mismatch");
    }

    template<bool Google>
    std::size_t execute() {
        return measure_checksums<bytes_per_run / Size>(
          [this] { return calculate<Google>(); }, expected_);
    }

private:
    template<bool Google>
    std::uint32_t calculate() const {
        if constexpr (Google) {
            auto crc = seed;
            each_field(fields_, [&crc](auto value) {
                const auto encoded = little_endian_bytes(value);
                crc = google_extend(crc, encoded);
            });
            return crc;
        } else {
            codec::crc32c crc{seed};
            each_field(fields_, [&crc](auto value) {
                const auto encoded = little_endian_bytes(value);
                crc.extend(encoded);
            });
            return crc.value();
        }
    }
    decltype(prefix_fields<Size>()) fields_{prefix_fields<Size>()};
    std::uint32_t expected_;
};

bytes::fragmented_buffer
patterned_buffer(std::size_t size, std::size_t fragment_size) {
    std::vector<seastar::temporary_buffer<char>> fragments;
    fragments.reserve((size + fragment_size - 1U) / fragment_size);
    for (std::size_t offset = 0; offset < size; offset += fragment_size) {
        const auto count = std::min(fragment_size, size - offset);
        seastar::temporary_buffer<char> fragment{count};
        for (std::size_t index = 0; index < count; ++index) {
            fragment.get_write()[index] = std::bit_cast<char>(
              static_cast<std::uint8_t>(((offset + index) * 31U + 7U) & 0xffU));
        }
        fragments.push_back(std::move(fragment));
    }
    return bytes::fragmented_buffer::copy_from_fragments(fragments).value();
}

template<bool Google>
std::uint32_t fragmented_checksum(const bytes::fragmented_buffer& input) {
    if constexpr (Google) {
        auto crc = seed;
        for (const auto fragment : input) {
            crc = google_extend(
              crc, std::span<const char>{fragment.data(), fragment.size()});
        }
        return crc;
    } else {
        codec::crc32c crc{seed};
        for (const auto fragment : input) {
            crc.extend(std::span<const char>{fragment.data(), fragment.size()});
        }
        return crc.value();
    }
}

template<std::size_t Size, std::size_t FragmentSize>
class fragmented_crc_fixture {
    static_assert(Size > 0 && Size <= maximum_contiguous_allocation_bytes);
    static_assert(
      FragmentSize > 0 && FragmentSize <= maximum_contiguous_allocation_bytes);
    static_assert(
      (Size + FragmentSize - 1U) / FragmentSize <= bytes::max_buffer_fragments);

public:
    fragmented_crc_fixture()
      : input_(patterned_buffer(Size, FragmentSize)) {
        expected_ = checksum<true>(patterned_bytes(Size));
        require(
          fragmented_checksum<true>(input_) == expected_
            && fragmented_checksum<false>(input_) == expected_,
          "fragmented CRC fixture mismatch");
    }
    template<bool Google>
    std::size_t execute() {
        return measure_checksums<bytes_per_run / Size>(
          [this] { return fragmented_checksum<Google>(input_); }, expected_);
    }

private:
    bytes::fragmented_buffer input_;
    std::uint32_t expected_;
};

template<std::size_t Size>
class copy_crc_fixture {
    static_assert(Size > 0 && Size <= maximum_contiguous_allocation_bytes);

public:
    copy_crc_fixture()
      : source_(patterned_bytes(Size))
      , destination_(Size) {
        expected_ = checksum<true>(source_);
        require(
          copy<true>() == expected_ && destination_ == source_,
          "copy/CRC fixture mismatch");
        require(
          copy<false>() == expected_ && destination_ == source_,
          "fused copy/CRC fixture mismatch");
    }
    template<bool Google>
    std::size_t execute() {
        const auto count = measure_checksums<bytes_per_run / Size>(
          [this] { return copy<Google>(); }, expected_);
        require(destination_ == source_, "copy/CRC output mismatch");
        return count;
    }

private:
    template<bool Google>
    std::uint32_t copy() {
        if constexpr (Google) {
            std::memcpy(destination_.data(), source_.data(), Size);
            return checksum<true>(destination_);
        } else {
            return static_cast<std::uint32_t>(absl::MemcpyCrc32c(
              destination_.data(), source_.data(), Size, absl::crc32c_t{seed}));
        }
    }
    std::vector<char> source_;
    std::vector<char> destination_;
    std::uint32_t expected_;
};

template<std::size_t Size, std::size_t FragmentSize>
class cooperative_crc_fixture {
    static_assert(Size > 0 && Size <= 32U * 1024U * 1024U);
    static_assert(
      FragmentSize > 0 && FragmentSize <= maximum_contiguous_allocation_bytes);
    static_assert(
      (Size + FragmentSize - 1U) / FragmentSize <= bytes::max_buffer_fragments);

public:
    cooperative_crc_fixture()
      : source_(patterned_buffer(Size, FragmentSize)) {
        expected_ = fragmented_checksum<true>(source_);
        require(
          fragmented_checksum<false>(source_) == expected_,
          "cooperative CRC fixture mismatch");
        // Setup and native initialization are outside every timed invocation.
        seastar::abort_source abort;
        cooperative_work work{limits::defaults(), abort};
        const auto baseline = google_crc32c_cooperatively(
                                source_.share(), work, seed, anchor)
                                .get();
        const auto candidate = codec::crc32c_cooperatively(
                                 source_.share(), work, seed, anchor)
                                 .get();
        require(
          baseline && candidate && *baseline == expected_
            && *candidate == expected_,
          "cooperative CRC owner fixture mismatch");
    }

    template<bool Google>
    seastar::future<std::size_t> execute() {
        // Identical owning shares and work snapshots are prepared before
        // timing.
        auto input = source_.share();
        seastar::abort_source abort;
        cooperative_work work{limits::defaults(), abort};
        std::optional<result<std::uint32_t>> outcome;
        clobber_memory();
        perf_tests::start_measuring_time();
        if constexpr (Google) {
            outcome.emplace(
              co_await google_crc32c_cooperatively(
                std::move(input), work, seed, anchor));
        } else {
            outcome.emplace(
              co_await codec::crc32c_cooperatively(
                std::move(input), work, seed, anchor));
        }
        perf_tests::do_not_optimize(*outcome);
        perf_tests::stop_measuring_time();
        require(
          outcome->has_value() && **outcome == expected_,
          "cooperative CRC result mismatch");
        // The buffer move contract guarantees an empty source.
        // NOLINTNEXTLINE(bugprone-use-after-move)
        require(input.empty(), "cooperative CRC input was not transferred");
        require(
          work.bytes_remaining() == byte_count{}
            && work.items_remaining() == item_count{},
          "cooperative CRC cleanup schedule mismatch");
        co_return std::size_t{1};
    }

private:
    static constexpr error anchor{errc::success, 1, 1, 0};
    bytes::fragmented_buffer source_;
    std::uint32_t expected_;
};

// google_configured uses the exact linked build, including its portable ARM
// path when acceleration is disabled. These labels make no hardware claim.
#define CRC_HOT_BENCH(Size)                                                    \
    using crc_hot_##Size = hot_crc_fixture<Size>;                              \
    PERF_TEST_F(crc_hot_##Size, google_configured) { return execute<true>(); } \
    PERF_TEST_F(crc_hot_##Size, abseil_selected) { return execute<false>(); }

CRC_HOT_BENCH(32)
CRC_HOT_BENCH(48)
CRC_HOT_BENCH(63)
CRC_HOT_BENCH(64)
CRC_HOT_BENCH(65)
CRC_HOT_BENCH(255)
CRC_HOT_BENCH(256)
CRC_HOT_BENCH(257)
CRC_HOT_BENCH(1007)
CRC_HOT_BENCH(1008)
CRC_HOT_BENCH(1009)
CRC_HOT_BENCH(4032)
CRC_HOT_BENCH(4080)
CRC_HOT_BENCH(4096)
CRC_HOT_BENCH(16256)
CRC_HOT_BENCH(16384)
CRC_HOT_BENCH(65536)
#undef CRC_HOT_BENCH

using crc_fields_32 = incremental_crc_fixture<32>;
using crc_fields_48 = incremental_crc_fixture<48>;
PERF_TEST_F(crc_fields_32, google_configured) { return execute<true>(); }
PERF_TEST_F(crc_fields_32, abseil_selected) { return execute<false>(); }
PERF_TEST_F(crc_fields_48, google_configured) { return execute<true>(); }
PERF_TEST_F(crc_fields_48, abseil_selected) { return execute<false>(); }

using crc_frag_65536_64 = fragmented_crc_fixture<65536, 64>;
using crc_frag_65536_4096 = fragmented_crc_fixture<65536, 4096>;
PERF_TEST_F(crc_frag_65536_64, google_configured) { return execute<true>(); }
PERF_TEST_F(crc_frag_65536_64, abseil_selected) { return execute<false>(); }
PERF_TEST_F(crc_frag_65536_4096, google_configured) { return execute<true>(); }
PERF_TEST_F(crc_frag_65536_4096, abseil_selected) { return execute<false>(); }

using crc_copy_4096 = copy_crc_fixture<4096>;
using crc_copy_65536 = copy_crc_fixture<65536>;
PERF_TEST_F(crc_copy_4096, google_copy_then_crc) { return execute<true>(); }
PERF_TEST_F(crc_copy_4096, abseil_fused_copy_crc) { return execute<false>(); }
PERF_TEST_F(crc_copy_65536, google_copy_then_crc) { return execute<true>(); }
PERF_TEST_F(crc_copy_65536, abseil_fused_copy_crc) { return execute<false>(); }

using crc_owner_131072_131072 = cooperative_crc_fixture<131072, 131072>;
using crc_owner_1048576_4096 = cooperative_crc_fixture<1048576, 4096>;
PERF_TEST_F(crc_owner_131072_131072, google_configured) {
    return execute<true>();
}
PERF_TEST_F(crc_owner_131072_131072, abseil_selected) {
    return execute<false>();
}
PERF_TEST_F(crc_owner_1048576_4096, google_configured) {
    return execute<true>();
}
PERF_TEST_F(crc_owner_1048576_4096, abseil_selected) {
    return execute<false>();
}

} // namespace
} // namespace kwaque::codec::bench
