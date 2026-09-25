#include "src/codec/tests/benchmark_buffer.h"
#include "src/codec/tests/qualification_profile.h"
#include "src/model/checkpoint_codec.h"
#include "src/model/fingerprint.h"
#include "src/model/tests/checkpoint_test_support.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/testing/perf_tests.hh>

#include <fmt/format.h>

#include <array>
#include <optional>
#include <utility>
#include <vector>

namespace kwaque::model::bench {
namespace {
namespace fixture = testing::checkpoint_fixture;
using bytes::fragmented_buffer;
using bytes::fragmented_buffer_parser;
using codec::bench::capacity_bound;
using codec::bench::require;
enum class operation { sorted, unordered, fingerprint, encode, decode };

class measurements {
public:
    measurements(std::uint32_t count, std::size_t width, std::size_t header)
      : count_(count)
      , width_(width)
      , header_(header) {}

    template<operation Op>
    seastar::future<std::size_t> measure() {
        co_await initialize<Op>();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        if constexpr (Op == operation::sorted || Op == operation::unordered) {
            seastar::chunked_fifo<range_cursor, 16> source;
            if constexpr (Op == operation::unordered) {
                // Donor preparation is outside timing. Native source chunks
                // remain inside memory_'s residual for the owning driver.
                for (std::size_t i = entries_.size(); i != 0; --i) {
                    (co_await work.admit(byte_count{128}, item_count{8}))
                      .value();
                    work.poll().value();
                    source.push_back(entries_[i - 1U]);
                }
            }
            barrier();
            perf_tests::start_measuring_time();
            auto result = co_await [&] {
                if constexpr (Op == operation::sorted)
                    return make_read_checkpoint(
                      fixture::topic(), entries_, memory_, work);
                else
                    return make_read_checkpoint_from_unordered(
                      fixture::topic(), std::move(source), memory_, work);
            }();
            perf_tests::do_not_optimize(result);
            perf_tests::stop_measuring_time();
            require(
              result && result->value.topic() == fixture::topic()
                && std::ranges::equal(result->value.cursors(), entries_),
              "checkpoint construction changed entries");
            perf_tests::start_measuring_time();
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            result = codec::failure(codec::error{errc::success});
            perf_tests::stop_measuring_time();
        } else if constexpr (Op == operation::fingerprint) {
            barrier();
            perf_tests::start_measuring_time();
            auto result = co_await compute_checkpoint_fingerprint(
              *value_, work);
            perf_tests::do_not_optimize(result);
            perf_tests::stop_measuring_time();
            require(
              result && *result == *digest_,
              "checkpoint fingerprint changed projection");
        } else if constexpr (Op == operation::encode) {
            barrier();
            perf_tests::start_measuring_time();
            auto result = co_await encode_read_checkpoint(
              *value_, work, memory_.operation_remaining, capacity_bound);
            perf_tests::do_not_optimize(result);
            perf_tests::stop_measuring_time();
            require(
              result && result->fingerprint == *digest_,
              "checkpoint encode failed");
            require(
              co_await codec::bench::buffers_equal(result->bytes, wire_, work),
              "checkpoint encoder changed bytes");
            perf_tests::start_measuring_time();
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            result = codec::failure(codec::error{errc::success});
            perf_tests::stop_measuring_time();
        } else {
            fragmented_buffer_parser input{wire_.share()};
            const auto budget = codec::reserve_decode_input(
                                  input, work.policy(), memory_)
                                  .value();
            barrier();
            perf_tests::start_measuring_time();
            auto result = co_await decode_read_checkpoint(
              input, fixture::topic(), budget, work);
            perf_tests::do_not_optimize(result);
            perf_tests::stop_measuring_time();
            require(
              result && input.at_end() && result->value == *value_
                && result->fingerprint == *digest_,
              "checkpoint decode changed value");
            perf_tests::start_measuring_time();
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            result = codec::failure(codec::error{errc::success});
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            input = fragmented_buffer_parser{};
            perf_tests::stop_measuring_time();
        }
        co_return count_; // Every scope reports time/counters per cursor.
    }

private:
    static void barrier() {
        // NOLINTNEXTLINE(portability-no-assembler)
        asm volatile("" : : : "memory");
    }
    template<operation Op>
    seastar::future<> initialize() {
        if (initialized_) co_return;
        codec::bench::qualify_allocator();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        entries_.reserve(count_);
        for (std::uint32_t i = 1; i <= count_; ++i) {
            (co_await work.admit(byte_count{128}, item_count{8})).value();
            work.poll().value();
            entries_.push_back(fixture::numbered(i));
        }
        (co_await work.checkpoint()).value();
        work.poll().value();
        if constexpr (Op != operation::sorted && Op != operation::unordered) {
            // Bounded independent fixture/projection setup, outside samples.
            const auto raw = fixture::wire(count_, header_);
            digest_ = fixture::digest(std::string_view{raw}.substr(header_));
            if constexpr (Op != operation::fingerprint)
                wire_ = co_await codec::bench::copy_layout(
                  fragmented_buffer::copy_of(raw).value(),
                  width_,
                  work,
                  memory_.operation_remaining);
        }
        if constexpr (Op != operation::sorted && Op != operation::unordered) {
            auto built = (co_await make_read_checkpoint(
                            fixture::topic(), entries_, memory_, work))
                           .value();
            value_.emplace(std::move(built.value));
        }
        const auto cost = wire_.allocation_cost(capacity_bound).value();
        const auto arrays = capacity_bound(
                              byte_count{
                                entries_.capacity() * sizeof(range_cursor)})
                              .checked_add(
                                value_ ? capacity_bound(
                                           byte_count{
                                             value_->cursor_capacity().value()
                                             * sizeof(range_cursor)})
                                       : byte_count{})
                              .value();
        const auto metadata = arrays.checked_add(cost.descriptors)
                                .value()
                                .checked_add(cost.share_controls)
                                .value();
        memory_ = codec::detail::consume_decode_budget(
                    work.policy(), memory_, cost.backing, metadata, {}, 0)
                    .value();
        constexpr std::array names{
          "sorted", "unordered", "fingerprint", "encode", "decode"};
        const auto logical_bytes = Op == operation::sorted
                                       || Op == operation::unordered
                                     ? count_ * sizeof(range_cursor)
                                   : Op == operation::fingerprint
                                     ? 36U + 24U * count_
                                     : header_ + 20U + 24U * count_;
        fmt::print(
          "kwaque-checkpoint-fixture-v1 scope={} cursors={} header={} "
          "logical_bytes={} cached_wire_bytes={} "
          "fragments={} cached_metadata={} cached_backing={} "
          "normalization=cursors\n",
          names[static_cast<std::size_t>(Op)],
          count_,
          header_,
          logical_bytes,
          wire_.size().value(),
          wire_.fragment_count(),
          metadata.value(),
          cost.backing.value());
        initialized_ = true;
    }

    std::uint32_t count_;
    std::size_t width_;
    std::size_t header_;
    std::vector<range_cursor> entries_;
    std::optional<read_checkpoint> value_;
    fragmented_buffer wire_;
    std::optional<codec::checkpoint_digest> digest_;
    codec::decode_budget memory_{
      codec::testing::residual, byte_count{1U << 20U}, capacity_bound};
    bool initialized_{false};
};

template<std::uint32_t Count, std::size_t Width, std::size_t Header = 32>
struct fixture_type : measurements {
    fixture_type()
      : measurements(Count, Width, Header) {}
};
using checkpoint_tiny = fixture_type<1, 0>;
using checkpoint_tiny_frag7 = fixture_type<8, 7>;
using checkpoint_medium_frag67 = fixture_type<128, 67>;
using checkpoint_max = fixture_type<4096, 4096>;
using checkpoint_extended = fixture_type<4096, 4096, 4096>;
#define CHECKPOINT_CASES(group)                                                \
    PERF_TEST_F(group, sorted) { return measure<operation::sorted>(); }        \
    PERF_TEST_F(group, unordered) { return measure<operation::unordered>(); }  \
    PERF_TEST_F(group, fingerprint) {                                          \
        return measure<operation::fingerprint>();                              \
    }                                                                          \
    PERF_TEST_F(group, encode) { return measure<operation::encode>(); }        \
    PERF_TEST_F(group, decode) { return measure<operation::decode>(); }
CHECKPOINT_CASES(checkpoint_tiny)
CHECKPOINT_CASES(checkpoint_tiny_frag7)
CHECKPOINT_CASES(checkpoint_medium_frag67)
CHECKPOINT_CASES(checkpoint_max)
PERF_TEST_F(checkpoint_extended, decode) {
    return measure<operation::decode>();
}
#undef CHECKPOINT_CASES
} // namespace
} // namespace kwaque::model::bench
