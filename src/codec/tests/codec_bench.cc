#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/collection.h"
#include "src/codec/cooperative.h"
#include "src/codec/error.h"
#include "src/codec/integer.h"
#include "src/codec/limits.h"
#include "src/codec/staging_cooperative.h"
#include "src/codec/tests/codec_bench_fixture.h"
#include "src/codec/transaction.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/testing/perf_tests.hh>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace kwaque::codec::bench {
namespace {

constexpr std::size_t scalar_count = 128;
constexpr field_context context{.origin = 512, .family = 1, .field = 3};

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

bytes::fragmented_buffer
make_fragments(std::span<const char> data, std::size_t width) {
    std::vector<seastar::temporary_buffer<char>> fragments;
    for (std::size_t offset = 0; offset < data.size(); offset += width) {
        const auto part = data.subspan(
          offset, std::min(width, data.size() - offset));
        seastar::temporary_buffer<char> fragment{part.size()};
        std::ranges::copy(part, fragment.get_write());
        fragments.push_back(std::move(fragment));
    }
    return bytes::fragmented_buffer::copy_from_fragments(fragments).value();
}

template<bool Variable, std::size_t FragmentSize>
class scalar_fixture {
public:
    scalar_fixture() {
        constexpr std::array<std::uint64_t, 8> values{
          0,
          1,
          127,
          128,
          16384,
          0xffffffffU,
          0x8000000000000000ULL,
          0xffffffffffffffffULL};
        for (std::size_t index = 0; index < scalar_count; ++index) {
            values_[index] = values[index % values.size()];
            auto value = values_[index];
            if constexpr (Variable) {
                while (value >= 128U) {
                    encoded_.push_back(
                      std::bit_cast<char>(
                        static_cast<std::uint8_t>((value & 0x7fU) | 0x80U)));
                    value >>= 7U;
                }
                encoded_.push_back(
                  std::bit_cast<char>(static_cast<std::uint8_t>(value)));
            } else {
                for (unsigned byte = 0; byte < 8; ++byte) {
                    encoded_.push_back(
                      std::bit_cast<char>(static_cast<std::uint8_t>(
                        (value >> (byte * 8U)) & 0xffU)));
                }
            }
        }
        source_ = make_fragments(encoded_, FragmentSize);
        auto warm_share = source_.share();
        require(
          warm_share.content_equals(view()), "scalar fixture sharing mismatch");
    }

    template<bool Candidate>
    std::size_t read() {
        bytes::fragmented_buffer_parser input{source_.share()};
        perf_tests::start_measuring_time();
        for (std::size_t index = 0; index < scalar_count; ++index) {
            clobber_memory();
            if constexpr (Variable && Candidate) {
                reads_[index] = codec_varuint_read(
                  input, context, input_boundary::complete);
            } else if constexpr (Variable) {
                reads_[index] = baseline_varuint_read(
                  input, context, input_boundary::complete);
            } else if constexpr (Candidate) {
                reads_[index] = codec_fixed_read(
                  input, context, input_boundary::complete);
            } else {
                reads_[index] = native_fixed_read(
                  input, context, input_boundary::complete);
            }
            perf_tests::do_not_optimize(reads_[index]);
        }
        perf_tests::stop_measuring_time();
        for (std::size_t index = 0; index < scalar_count; ++index) {
            require(
              reads_[index] && *reads_[index] == values_[index],
              "scalar decode mismatch");
        }
        require(
          input.at_end() && input.checkpoint_depth() == 0,
          "scalar cursor mismatch");
        return scalar_count;
    }

    template<bool Candidate>
    std::size_t write() {
        bytes::fragmented_buffer_builder_config config;
        config.initial_fragment_bytes = byte_count{64};
        config.max_fragment_bytes = byte_count{64};
        config.max_total_bytes = byte_count{encoded_.size()};
        config.max_retained_bytes = byte_count{4096};
        bytes::fragmented_buffer_builder output{config};
        perf_tests::start_measuring_time();
        for (std::size_t index = 0; index < scalar_count; ++index) {
            clobber_memory();
            if constexpr (Variable && Candidate) {
                writes_[index] = codec_varuint_write(
                  output, values_[index], context);
            } else if constexpr (Variable) {
                writes_[index] = baseline_varuint_write(
                  output, values_[index], context);
            } else if constexpr (Candidate) {
                writes_[index] = codec_fixed_write(
                  output, values_[index], context);
            } else {
                writes_[index] = native_fixed_write(
                  output, values_[index], context);
            }
            perf_tests::do_not_optimize(writes_[index]);
        }
        perf_tests::stop_measuring_time();
        for (const auto& written : writes_) {
            require(written.has_value(), "scalar encode rejected fixture");
        }
        require(
          output.size() == byte_count{encoded_.size()},
          "scalar encoded size mismatch");
        const auto published = output.finish();
        require(
          published && published->content_equals(view()),
          "scalar encoded bytes mismatch");
        // These are scalar-write costs; complete-buffer publication is outside
        // the measurement on both paths, as is eventual owner destruction.
        return scalar_count;
    }

private:
    std::string_view view() const { return {encoded_.data(), encoded_.size()}; }
    std::array<std::uint64_t, scalar_count> values_{};
    std::array<result<std::uint64_t>, scalar_count> reads_{};
    std::array<result<void>, scalar_count> writes_{};
    std::vector<char> encoded_;
    bytes::fragmented_buffer source_;
};

// Conservative capacity reservation for the bounded native fixture profile.
// This is not a measured allocation size. Real allocator/task counts come from
// the perf harness; peak ownership and per-allocation bounds are separate
// gates.
byte_count capacity_bound(byte_count requested) noexcept {
    if (requested.value() == 0) {
        return {};
    }
    const auto size = std::max(requested.value(), std::uint64_t{16});
    if (size > (std::uint64_t{1} << 62U)) {
        return byte_count{std::numeric_limits<std::uint64_t>::max()};
    }
    return byte_count{2U * std::bit_ceil(size)};
}

constexpr byte_count frame_reservation{1024U * 1024U};
constexpr byte_count parent_remainder{
  64U * 1024U * 1024U - frame_reservation.value()};

struct entry {
    std::uint64_t key;
    std::uint64_t value;
};
struct entry_less {
    bool operator()(const entry& left, const entry& right) const noexcept {
        return left.key < right.key;
    }
};

template<std::size_t Count>
struct unordered_fixture {
    seastar::future<std::size_t> execute() {
        seastar::chunked_fifo<entry, 16> source;
        for (std::size_t index = Count; index != 0; --index) {
            if (index % 64U == 0) {
                co_await seastar::coroutine::maybe_yield();
            }
            source.push_back(entry{index, index * 3U});
        }
        seastar::abort_source abort;
        cooperative_work work{limits::defaults(), abort};
        const decode_budget budget{
          parent_remainder, byte_count{1024U * 1024U}, capacity_bound};
        clobber_memory();
        perf_tests::start_measuring_time();
        auto result = co_await canonicalize_unordered(
          std::move(source), budget, work, entry_less{}, context);
        perf_tests::do_not_optimize(result);
        perf_tests::stop_measuring_time();
        require(
          result.has_value() && result->size() == Count,
          "canonical owner size mismatch");
        std::uint64_t expected = 1;
        for (const auto& value : *result) {
            if ((expected - 1U) % 64U == 0) {
                co_await seastar::coroutine::maybe_yield();
            }
            require(
              value.key == expected && value.value == expected * 3U,
              "canonical owner content mismatch");
            ++expected;
        }
        // The returned owner's disposal belongs to its caller and is excluded
        // from construction timing, but still cooperates during fixture reuse.
        while (!result->empty()) {
            co_await work.drain_inline(
              byte_count{2U * sizeof(entry)}, item_count{2});
            result->pop_front();
        }
        co_return std::size_t{1};
    }
};

bytes::fragmented_buffer payload_buffer(std::size_t fragments) {
    const std::array<char, 4096> block{};
    std::vector<seastar::temporary_buffer<char>> source;
    source.reserve(fragments);
    for (std::size_t index = 0; index < fragments; ++index) {
        seastar::temporary_buffer<char> fragment{block.size()};
        std::ranges::copy(block, fragment.get_write());
        source.push_back(std::move(fragment));
    }
    return bytes::fragmented_buffer::copy_from_fragments(source).value();
}

template<std::size_t Fragments>
class staging_fixture {
public:
    staging_fixture()
      : prefix_(
          bytes::fragmented_buffer::copy_of(std::string_view{"encoded-prefix"})
            .value())
      , payload_(payload_buffer(Fragments)) {
        auto warm_prefix = prefix_.share();
        auto warm_payload = payload_.share();
        static_cast<void>(warm_prefix);
        static_cast<void>(warm_payload);
        for (const auto* input : {&prefix_, &payload_}) {
            const auto cost = input->allocation_cost(capacity_bound).value();
            other_live_.payload_bookkeeping
              = *other_live_.payload_bookkeeping.checked_add(cost.descriptors);
            other_live_.payload_bookkeeping
              = *other_live_.payload_bookkeeping.checked_add(
                cost.share_controls);
        }
    }

    seastar::future<std::size_t> execute() {
        auto prefix = prefix_.share();
        auto payload = payload_.share();
        const auto total = *prefix.size().checked_add(payload.size());
        seastar::abort_source abort;
        cooperative_work work{limits::defaults(), abort};
        clobber_memory();
        perf_tests::start_measuring_time();
        auto result = co_await assemble_buffer_cooperatively(
          std::move(prefix),
          std::move(payload),
          work,
          total,
          other_live_,
          parent_remainder,
          capacity_bound,
          context);
        perf_tests::do_not_optimize(result);
        perf_tests::stop_measuring_time();
        require(
          result.has_value() && result->size() == total,
          "staged owner size mismatch");
        bytes::fragmented_buffer_parser verify{std::move(*result)};
        std::array<char, 14> prefix_bytes{};
        require(
          verify.read_to(prefix_bytes).has_value(), "staged prefix missing");
        require(
          std::string_view{prefix_bytes.data(), prefix_bytes.size()}
            == "encoded-prefix",
          "staged prefix mismatch");
        std::array<char, 4096> actual{};
        for (std::size_t fragment = 0; fragment < Fragments; ++fragment) {
            if (fragment % 8U == 0) {
                co_await seastar::coroutine::maybe_yield();
            }
            require(
              verify.read_to(actual).has_value(), "staged payload missing");
            require(
              std::ranges::all_of(actual, [](char byte) { return byte == 0; }),
              "staged payload mismatch");
        }
        require(verify.at_end(), "staged owner trailing bytes");
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        verify = bytes::fragmented_buffer_parser{};
        co_return std::size_t{1};
    }

private:
    bytes::fragmented_buffer prefix_;
    bytes::fragmented_buffer payload_;
    operation_usage other_live_;
};

using codec_fixed64_contiguous = scalar_fixture<false, 1024>;
using codec_fixed64_frag13 = scalar_fixture<false, 13>;
using codec_varuint64_contiguous = scalar_fixture<true, 1024>;
using codec_varuint64_frag7 = scalar_fixture<true, 7>;
PERF_TEST_F(codec_fixed64_contiguous, native_validated_read) {
    return read<false>();
}
PERF_TEST_F(codec_fixed64_contiguous, codec_read) { return read<true>(); }
PERF_TEST_F(codec_fixed64_frag13, native_validated_read) {
    return read<false>();
}
PERF_TEST_F(codec_fixed64_frag13, codec_read) { return read<true>(); }
PERF_TEST_F(codec_varuint64_contiguous, canonical_baseline_read) {
    return read<false>();
}
PERF_TEST_F(codec_varuint64_contiguous, codec_read) { return read<true>(); }
PERF_TEST_F(codec_varuint64_frag7, canonical_baseline_read) {
    return read<false>();
}
PERF_TEST_F(codec_varuint64_frag7, codec_read) { return read<true>(); }
PERF_TEST_F(codec_fixed64_contiguous, native_validated_write) {
    return write<false>();
}
PERF_TEST_F(codec_fixed64_contiguous, codec_write) { return write<true>(); }
PERF_TEST_F(codec_varuint64_contiguous, canonical_baseline_write) {
    return write<false>();
}
PERF_TEST_F(codec_varuint64_contiguous, codec_write) { return write<true>(); }

// These whole-owner operation costs are intentionally unpaired: neither an
// unchecked flat sort nor a synchronous copy satisfies the same contracts.
using codec_unordered_128 = unordered_fixture<128>;
using codec_unordered_4096 = unordered_fixture<4096>;
using codec_staging_4096 = staging_fixture<1>;
using codec_staging_262144 = staging_fixture<64>;
PERF_TEST_F(codec_unordered_128, construct_owner) { return execute(); }
PERF_TEST_F(codec_unordered_4096, construct_owner) { return execute(); }
PERF_TEST_F(codec_staging_4096, assemble_owner) { return execute(); }
PERF_TEST_F(codec_staging_262144, assemble_owner) { return execute(); }

} // namespace
} // namespace kwaque::codec::bench
