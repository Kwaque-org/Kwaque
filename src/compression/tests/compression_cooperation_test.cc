#include "src/bytes/fragmented_buffer_builder.h"
#include "src/compression/tests/test_support.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/preempt.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/later.hh>

#include <array>
#include <chrono>
#include <exception>
#include <optional>

namespace kwaque::compression {
namespace {
using namespace testing;
using bytes::fragmented_buffer;
constexpr byte_count maximum{8U << 20U};

fragmented_buffer maximum_input(char value = 'x') {
    bytes::fragmented_buffer_builder builder{
      {.initial_fragment_bytes = byte_count{65536},
       .max_fragment_bytes = byte_count{65536},
       .max_total_bytes = maximum,
       .max_retained_bytes = maximum,
       .max_fragments = 128}};
    builder.reserve_fragments(item_count{128}).value();
    std::array<char, 32768> chunk;
    chunk.fill(value);
    while (builder.size() < maximum) {
        builder.append(std::span<const char>{chunk}).value();
        seastar::thread::maybe_yield();
    }
    return builder.finish().value();
}
bool preempt_requested() {
    const auto deadline = std::chrono::steady_clock::now()
                          + std::chrono::seconds{2};
    while (!seastar::need_preempt()
           && std::chrono::steady_clock::now() < deadline) {
    }
    return seastar::need_preempt();
}
seastar::future<> observe(bool& active, std::uint64_t& ticks) {
    while (active) {
        co_await seastar::yield();
        if (active) ++ticks;
    }
}
void check_plain(const fragmented_buffer& value, char expected = 'x') {
    EXPECT_EQ(value.size(), maximum);
    for (const auto fragment : value) {
        EXPECT_TRUE(
          std::all_of(
            fragment.data(),
            fragment.data() + fragment.size(),
            [expected](char c) { return c == expected; }));
        seastar::thread::maybe_yield();
    }
}

TEST(CompressionCooperationTest, ConcurrentOperationsKeepDistinctPayloads) {
    std::array input{maximum_input('x'), maximum_input('y')};
    for (const bool compress : {true, false}) {
        const auto operation =
          [compress](fragmented_buffer& buffer) -> seastar::future<> {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto result = co_await (
              compress
                ? compress_lz4(
                    std::move(buffer), byte_count{16U << 20U}, work, budget())
                : decompress_lz4(std::move(buffer), maximum, work, budget()));
            buffer = std::move(result.value().value);
        };
        ASSERT_TRUE(preempt_requested());
        auto pending = seastar::parallel_for_each(input, operation);
        const bool suspended = !pending.available();
        // Joins both owners on success and exception, while input and the
        // coroutine closure are still alive.
        pending.get();
        EXPECT_TRUE(suspended);
    }
    check_plain(input[0], 'x');
    check_plain(input[1], 'y');
}

TEST(
  CompressionCooperationTest, MaximumOperationsSuspendAndAllowControlProgress) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto input = maximum_input();
    for (const bool compress : {true, false}) {
        ASSERT_TRUE(preempt_requested());
        bool active = true;
        std::uint64_t ticks = 0;
        auto observer = observe(active, ticks);
        std::optional<codec::result<owned_result>> result;
        std::exception_ptr exception;
        bool suspended = false;
        try {
            auto pending
              = compress
                  ? compress_lz4(
                      std::move(input), byte_count{16U << 20U}, work, budget())
                  : decompress_lz4(std::move(input), maximum, work, budget());
            suspended = !pending.available();
            result.emplace(pending.get());
        } catch (...) {
            exception = std::current_exception();
        }
        // Read by the joined observer after its suspension.
        // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
        active = false;
        observer.get();
        if (exception) std::rethrow_exception(exception);
        EXPECT_TRUE(suspended);
        EXPECT_GT(ticks, 0U);
        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE(result->has_value());
        input = std::move((*result)->value);
    }
    check_plain(input);
}

TEST(
  CompressionCooperationTest,
  CancellationJoinsOwnersAndFreshOperationsStillWork) {
    seastar::abort_source build_abort;
    codec::cooperative_work build_work{codec::limits::defaults(), build_abort};
    auto raw = maximum_input();
    auto encoded = compress_lz4(
                     raw.share(), byte_count{16U << 20U}, build_work, budget())
                     .get()
                     .value();
    for (const bool compress : {true, false}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto input = compress ? raw.share() : encoded.value.share();
        ASSERT_TRUE(preempt_requested());
        auto pending
          = compress ? compress_lz4(
                         std::move(input),
                         byte_count{16U << 20U},
                         work,
                         budget(),
                         context)
                     : decompress_lz4(
                         std::move(input), maximum, work, budget(), context);
        const bool suspended = !pending.available();
        abort.request_abort();
        const auto result = pending.get();
        EXPECT_TRUE(suspended);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), errc::aborted);
        seastar::abort_source fresh;
        codec::cooperative_work next{codec::limits::defaults(), fresh};
        auto decoded = decompress_lz4(
                         encoded.value.share(), maximum, next, budget())
                         .get();
        ASSERT_TRUE(decoded.has_value());
        check_plain(decoded->value);
    }
}

TEST(CompressionCooperationTest, NarrowNativeQuantumRejectsBeforeContextEntry) {
    auto config = codec::limits_config{};
    config.max_work_bytes = byte_count{65535};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    auto input = fragmented_buffer::copy_of(std::string_view{"abc"}).value();
    expect_error(
      compress_lz4(std::move(input), byte_count{100}, work, budget(), context)
        .get(),
      errc::resource_exhausted);
    auto empty = fragmented_buffer{};
    expect_error(
      decompress_lz4(std::move(empty), byte_count{}, work, budget(), context)
        .get(),
      errc::resource_exhausted);
}

} // namespace
} // namespace kwaque::compression
