#include "src/codec/tests/benchmark_buffer.h"
#include "src/compression/compression.h"
#include "src/compression/lz4.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/testing/perf_tests.hh>

#include <fmt/format.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <malloc.h>
#include <utility>

namespace kwaque::compression::bench {
namespace {
using bytes::fragmented_buffer;
using codec::bench::capacity_bound;
using codec::bench::payload_pattern;
using codec::bench::require;
using detail::lz4_direction;

std::uint64_t retained(const fragmented_buffer& value) {
    const auto cost = value.allocation_cost(capacity_bound).value();
    return cost.backing.value() + cost.descriptors.value()
           + cost.share_controls.value();
}

// A native-kernel benchmark has no immutable output publication or admission
// scope. Its context and destination are already warm on every invocation.
// Native calls, checksums, bounded offers and cooperative checkpoints remain.
// Whole owning operations are measured separately below.
seastar::future<std::size_t> kernel(
  detail::lz4_context& owner,
  lz4_direction direction,
  const fragmented_buffer& input,
  byte_count expanded,
  seastar::temporary_buffer<char>& output,
  codec::cooperative_work& work,
  const fragmented_buffer* verify = nullptr) {
    std::size_t total = 0;
    auto expected = verify ? verify->begin()
                           : fragmented_buffer::const_iterator{};
    std::size_t expected_offset = 0;
    const auto consume = [&](std::size_t produced) {
        require(
          produced <= output.size(), "native kernel exceeded destination");
        perf_tests::do_not_optimize(output);
        if (verify) {
            for (std::size_t at = 0; at < produced;) {
                require(
                  expected != verify->end(),
                  "native kernel produced extra bytes");
                const auto fragment = *expected;
                const auto count = std::min(
                  produced - at, fragment.size() - expected_offset);
                require(
                  std::memcmp(
                    output.get() + at, fragment.data() + expected_offset, count)
                    == 0,
                  "native kernel changed bytes");
                at += count;
                expected_offset += count;
                if (expected_offset == fragment.size()) {
                    ++expected;
                    expected_offset = 0;
                }
            }
        }
        total += produced;
    };
    if (direction == lz4_direction::compress) {
        const auto prefs = detail::writer_preferences(expanded);
        (co_await work.checkpoint()).value();
        auto produced = LZ4F_compressBegin(
          owner.compressor(), output.get_write(), output.size(), &prefs);
        detail::check_lz4_encode(owner.memory(), produced, {}).value();
        work.poll().value();
        consume(produced);
        for (const auto fragment : input) {
            for (std::size_t at = 0; at < fragment.size();) {
                const auto count = std::min(
                  fragment.size() - at, std::size_t{65536});
                (co_await work.admit(byte_count{65536}, item_count{8})).value();
                const auto bound = LZ4F_compressBound(count, &prefs);
                require(
                  !LZ4F_isError(bound) && bound <= output.size(),
                  "native kernel compression bound");
                produced = LZ4F_compressUpdate(
                  owner.compressor(),
                  output.get_write(),
                  output.size(),
                  fragment.data() + at,
                  count,
                  nullptr);
                detail::check_lz4_encode(owner.memory(), produced, {}).value();
                work.poll().value();
                consume(produced);
                at += count;
                (co_await work.checkpoint()).value();
            }
        }
        (co_await work.checkpoint()).value();
        const auto bound = LZ4F_compressBound(0, &prefs);
        require(
          !LZ4F_isError(bound) && bound <= output.size(),
          "native kernel footer bound");
        produced = LZ4F_compressEnd(
          owner.compressor(), output.get_write(), output.size(), nullptr);
        detail::check_lz4_encode(owner.memory(), produced, {}).value();
        work.poll().value();
        consume(produced);
    } else {
        LZ4F_resetDecompressionContext(owner.decompressor());
        auto fragment = input.begin();
        std::size_t offset = 0;
        const char sentinel = 0;
        for (;;) {
            const auto offered
              = fragment == input.end()
                  ? 0U
                  : std::min((*fragment).size() - offset, std::size_t{65536});
            auto consumed = offered;
            auto produced = std::min(output.size(), std::size_t{65536});
            (co_await work.admit(byte_count{65536}, item_count{8})).value();
            const auto code = LZ4F_decompress(
              owner.decompressor(),
              output.get_write(),
              &produced,
              offered == 0 ? &sentinel : (*fragment).data() + offset,
              &consumed,
              nullptr);
            detail::check_lz4_decode(owner.memory(), code, {}).value();
            work.poll().value();
            require(consumed <= offered, "native kernel consumed past input");
            consume(produced);
            if (consumed != 0) {
                offset += consumed;
                if (offset == (*fragment).size()) {
                    ++fragment;
                    offset = 0;
                }
            }
            (co_await work.checkpoint()).value();
            if (code == 0) {
                require(
                  fragment == input.end() && total == expanded.value(),
                  "native kernel incomplete frame");
                break;
            }
            require(
              consumed != 0 || produced != 0, "native kernel made no progress");
        }
    }
    if (verify)
        require(expected == verify->end(), "native kernel omitted output");
    (co_await work.checkpoint()).value();
    co_return total;
}

enum class operation { none, compress, decompress };

class measurements {
public:
    measurements(std::size_t size, std::size_t width, payload_pattern pattern)
      : size_(size)
      , width_(width)
      , pattern_(pattern) {}

    template<operation Operation>
    seastar::future<std::size_t> cold() {
        co_await initialize();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto input = Operation == operation::decompress ? encoded_.share()
                                                        : raw_.share();
        const auto cost = input.allocation_cost(capacity_bound).value();
        const auto memory
          = codec::detail::consume_decode_budget(
              work.policy(),
              budget(),
              cost.backing,
              cost.descriptors.checked_add(cost.share_controls).value(),
              {},
              0)
              .value();
        perf_tests::start_measuring_time();
        auto result = co_await [&] {
            if constexpr (Operation == operation::none)
                return transfer_none(
                  std::move(input), byte_count{size_}, work, memory);
            else if constexpr (Operation == operation::compress)
                return compress_lz4(
                  std::move(input), byte_count{16U << 20U}, work, memory);
            else
                return decompress_lz4(
                  std::move(input), byte_count{size_}, work, memory);
        }();
        perf_tests::do_not_optimize(result);
        perf_tests::stop_measuring_time();
        require(
          result.has_value(), "cold compression operation rejected fixture");
        const auto& expected = Operation == operation::compress ? encoded_
                                                                : raw_;
        require(
          co_await codec::bench::buffers_equal(result->value, expected, work),
          "cold compression changed fixture bytes");
        require(
          result->retained
            == result->value.allocation_cost(capacity_bound).value(),
          "cold compression retention mismatch");
        if (!reported_output_) {
            fmt::print(
              "kwaque-compression-output-v1 operation={} "
              "backing_charge_upper={} descriptor_charge_upper={} "
              "share_control_charge_upper={} fragments={}\n",
              Operation == operation::none       ? "none"
              : Operation == operation::compress ? "compress"
                                                 : "decompress",
              result->retained.backing.value(),
              result->retained.descriptors.value(),
              result->retained.share_controls.value(),
              result->retained.fragments.value());
            reported_output_ = true;
        }
        perf_tests::start_measuring_time();
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        result = codec::failure(codec::error{errc::success});
        perf_tests::stop_measuring_time();
        co_return std::size_t{
          1}; // one complete region; byte normalization is reported
    }

    template<lz4_direction Direction>
    seastar::future<std::size_t> warm() {
        co_await initialize();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto plan = detail::admit_lz4(
                            Direction,
                            byte_count{size_},
                            byte_count{16U << 20U},
                            work,
                            budget())
                            .value();
        // Operation-local warmup and teardown are outside this kernel scope.
        detail::lz4_context native{plan};
        native.memory().status().value();
        seastar::temporary_buffer<char> bounce{
          static_cast<std::size_t>(plan.bounce_bytes.value())};
        const auto& input = Direction == lz4_direction::compress ? raw_
                                                                 : encoded_;
        const auto& expected = Direction == lz4_direction::compress ? encoded_
                                                                    : raw_;
        (void)co_await kernel(
          native, Direction, input, byte_count{size_}, bounce, work, &expected);
        perf_tests::start_measuring_time();
        const auto produced = co_await kernel(
          native, Direction, input, byte_count{size_}, bounce, work);
        perf_tests::do_not_optimize(produced);
        perf_tests::stop_measuring_time();
        require(
          produced == expected.size().value(),
          "warm compression changed output length");
        co_return std::size_t{1};
    }

private:
    codec::decode_budget budget() const {
        return {
          byte_count{(63U << 20U) - cache_},
          byte_count{1U << 20U},
          capacity_bound};
    }
    seastar::future<> initialize() {
        if (initialized_) co_return;
        codec::bench::qualify_allocator();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        raw_ = co_await codec::bench::patterned_buffer(
          size_, width_, pattern_, work, budget().operation_remaining);
        cache_ = retained(raw_);
        const auto input_cost = raw_.allocation_cost(capacity_bound).value();
        auto memory = codec::detail::consume_decode_budget(
                        work.policy(),
                        budget(),
                        input_cost.backing,
                        input_cost.descriptors
                          .checked_add(input_cost.share_controls)
                          .value(),
                        {},
                        0)
                        .value();
        auto compressed
          = (co_await compress_lz4(
               raw_.share(), byte_count{16U << 20U}, work, memory))
              .value();
        encoded_ = co_await codec::bench::copy_layout(
          std::move(compressed.value),
          width_,
          work,
          budget().operation_remaining);
        cache_ += retained(encoded_);
        const auto compressed_retained = retained(encoded_);
        for (const auto direction :
             {lz4_direction::compress, lz4_direction::decompress}) {
            const auto plan = detail::admit_lz4(
                                direction,
                                byte_count{size_},
                                byte_count{16U << 20U},
                                work,
                                budget())
                                .value();
            detail::lz4_context native{plan};
            native.memory().status().value();
            seastar::temporary_buffer<char> bounce{
              static_cast<std::size_t>(plan.bounce_bytes.value())};
            const auto& input = direction == lz4_direction::compress ? raw_
                                                                     : encoded_;
            const auto& expected = direction == lz4_direction::compress
                                     ? encoded_
                                     : raw_;
            (void)co_await kernel(
              native,
              direction,
              input,
              byte_count{size_},
              bounce,
              work,
              &expected);
            std::size_t allocations = 0, served = 0, largest = 0;
            for (const auto& entry : native.memory().allocations()) {
                if (!entry.address) continue;
                ++allocations;
                const auto capacity = ::malloc_usable_size(entry.address);
                require(
                  capacity >= entry.requested.value()
                    && capacity <= entry.charged.value(),
                  "native allocation exceeds profile");
                served += capacity;
                largest = std::max(largest, capacity);
            }
            require(
              largest <= 131072 && served <= plan.native_bytes.value(),
              "native scratch exceeds reservation");
            fmt::print(
              "kwaque-compression-capacity-v1 direction={} pattern={} "
              "input_bytes={} encoded_bytes={} ratio={} raw_fragments={} "
              "encoded_fragments={} raw_retained_upper={} "
              "encoded_retained_upper={} output_reserved_upper={} "
              "native_live_allocations={} native_served_bytes={} "
              "largest_native_served={} bounce_served={} "
              "scratch_charge_upper={} normalization=regions\n",
              direction == lz4_direction::compress ? "compress" : "decompress",
              static_cast<int>(pattern_),
              size_,
              encoded_.size().value(),
              static_cast<double>(encoded_.size().value())
                / static_cast<double>(size_),
              raw_.fragment_count(),
              encoded_.fragment_count(),
              retained(raw_),
              compressed_retained,
              plan.output_backing.value() + plan.output_metadata.value(),
              allocations,
              served,
              largest,
              ::malloc_usable_size(bounce.get_write()),
              plan.scratch_bytes.value());
        }
        initialized_ = true;
    }
    std::size_t size_, width_;
    payload_pattern pattern_;
    fragmented_buffer raw_, encoded_;
    std::uint64_t cache_{0};
    bool initialized_{false};
    bool reported_output_{false};
};

template<std::size_t Size, std::size_t Width, payload_pattern Pattern>
struct fixture : measurements {
    fixture()
      : measurements(Size, Width, Pattern) {}
};
using tiny_repeat = fixture<64, 64, payload_pattern::compressible>;
using tiny_noise = fixture<64, 7, payload_pattern::incompressible>;
using tiny_mixed = fixture<64, 7, payload_pattern::mixed>;
using block_repeat = fixture<65536, 67, payload_pattern::compressible>;
using block_noise = fixture<65536, 4096, payload_pattern::incompressible>;
using block_mixed = fixture<65536, 4096, payload_pattern::mixed>;
using maximum_repeat = fixture<8U << 20U, 65536, payload_pattern::compressible>;
using maximum_noise
  = fixture<8U << 20U, 65536, payload_pattern::incompressible>;
using maximum_mixed = fixture<8U << 20U, 65536, payload_pattern::mixed>;

#define COMPRESSION_CASES(group)                                               \
    PERF_TEST_F(group, cold_none) { return cold<operation::none>(); }          \
    PERF_TEST_F(group, cold_compress) { return cold<operation::compress>(); }  \
    PERF_TEST_F(group, cold_decompress) {                                      \
        return cold<operation::decompress>();                                  \
    }                                                                          \
    PERF_TEST_F(group, warm_compress_kernel) {                                 \
        return warm<lz4_direction::compress>();                                \
    }                                                                          \
    PERF_TEST_F(group, warm_decompress_kernel) {                               \
        return warm<lz4_direction::decompress>();                              \
    }
COMPRESSION_CASES(tiny_repeat)
COMPRESSION_CASES(tiny_noise)
COMPRESSION_CASES(tiny_mixed)
COMPRESSION_CASES(block_repeat)
COMPRESSION_CASES(block_noise)
COMPRESSION_CASES(block_mixed)
COMPRESSION_CASES(maximum_repeat)
COMPRESSION_CASES(maximum_noise)
COMPRESSION_CASES(maximum_mixed)
#undef COMPRESSION_CASES
} // namespace
} // namespace kwaque::compression::bench
