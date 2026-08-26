#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/bytes/fragmented_buffer_parser.h"

#include <seastar/core/temporary_buffer.hh>
#include <seastar/testing/perf_tests.hh>

#include <sys/uio.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace kwaque::bytes {

namespace {

constexpr std::size_t fragment_bytes = 4096;
constexpr std::size_t fragment_total = 64;
constexpr std::size_t tiny_append_total = 1024;
constexpr std::size_t benchmark_inner_iterations = 1000;
constexpr std::size_t shared_append_bytes = 4;
constexpr std::size_t shared_append_total = 1000;
constexpr std::size_t scatter_batch_vectors = 16;
constexpr std::size_t fixture_bytes = fragment_bytes * fragment_total;
constexpr std::size_t scatter_batch_bytes = fragment_bytes
                                            * scatter_batch_vectors;
constexpr std::size_t scatter_batch_total = fixture_bytes / scatter_batch_bytes;
constexpr std::size_t transfer_fragment_bytes = 10;
constexpr std::size_t transfer_fragment_total = 1000;
constexpr std::size_t transfer_input_total = 64;
static_assert(fixture_bytes % scatter_batch_bytes == 0);
static_assert(scatter_batch_total == 4);

// These measurements exist to inform stride, batch, and growth choices. Nothing
// here asserts a latency threshold; a test case returns the number of logical
// operations it performed so the harness reports per-operation cost.
fragmented_buffer
fixed_fragments(std::size_t fragment_count, std::size_t bytes_per_fragment) {
    std::vector<seastar::temporary_buffer<char>> fragments;
    fragments.reserve(fragment_count);
    for (std::size_t index = 0; index < fragment_count; ++index) {
        seastar::temporary_buffer<char> fragment{bytes_per_fragment};
        std::fill_n(
          fragment.get_write(),
          bytes_per_fragment,
          static_cast<char>('a' + index % 26));
        fragments.push_back(std::move(fragment));
    }
    auto buffer = fragmented_buffer::from_fragments(std::move(fragments));
    return std::move(*buffer);
}

fragmented_buffer many_fragments() {
    return fixed_fragments(fragment_total, fragment_bytes);
}

std::vector<fragmented_buffer> shared_small_buffers() {
    const std::string payload(shared_append_total * shared_append_bytes, 's');
    auto source = fragmented_buffer::copy_of(
      std::span<const char>{payload.data(), payload.size()});

    std::vector<fragmented_buffer> inputs;
    inputs.reserve(shared_append_total);
    for (std::size_t index = 0; index < shared_append_total; ++index) {
        auto shared = source->share(
          byte_count{index * shared_append_bytes},
          byte_count{shared_append_bytes});
        inputs.push_back(std::move(*shared));
    }
    return inputs;
}

struct buffer_fixture {
    fragmented_buffer buffer{many_fragments()};
};

struct bulk_builder_fixture {
    std::string payload = std::string(fragment_bytes, 'z');
    fragmented_buffer_builder_config config;

    bulk_builder_fixture() {
        config.max_total_bytes = byte_count{
          static_cast<std::uint64_t>(fragment_bytes) * fragment_total * 2};
    }
};

} // namespace

PERF_TEST_F(buffer_fixture, iterate_262144_bytes_64_fragments) {
    std::size_t total = 0;
    std::size_t visited = 0;
    for (const auto fragment : buffer) {
        total += fragment.size();
        ++visited;
    }
    perf_tests::do_not_optimize(total);
    return visited;
}

PERF_TEST_F(buffer_fixture, move_262144_bytes_64_fragments) {
    perf_tests::start_measuring_time();
    for (std::size_t index = 0; index < benchmark_inner_iterations; ++index) {
        fragmented_buffer moved = std::move(buffer);
        perf_tests::do_not_optimize(moved.size().value());
        buffer = std::move(moved);
    }
    perf_tests::stop_measuring_time();
    return benchmark_inner_iterations * 2;
}

PERF_TEST_F(buffer_fixture, share_whole) {
    auto shared = buffer.share();
    perf_tests::do_not_optimize(shared->fragment_count());
}

PERF_TEST_F(buffer_fixture, share_interior_slice) {
    auto shared = buffer.share(
      byte_count{fragment_bytes / 2}, byte_count{fragment_bytes * 8});
    perf_tests::do_not_optimize(shared->fragment_count());
}

PERF_TEST_F(
  buffer_fixture, export_scatter_262144_bytes_64_fragments_4_batches) {
    std::array<::iovec, scatter_batch_vectors> vectors{};
    scatter_cursor cursor;
    std::size_t batches = 0;
    while (true) {
        const auto batch = buffer.export_scatter(
          vectors, byte_count{scatter_batch_bytes}, cursor);
        perf_tests::do_not_optimize(batch->bytes.value());
        ++batches;
        if (batch->complete) {
            break;
        }
    }
    return batches;
}

PERF_TEST_F(buffer_fixture, linearize_whole) {
    auto linear = buffer.linearize(buffer.size());
    perf_tests::do_not_optimize(linear->size());
}

PERF_TEST_F(buffer_fixture, cross_fragment_fixed_reads) {
    auto shared = buffer.share();
    fragmented_buffer_parser parser{std::move(*shared)};
    // Starting one byte in makes one read per fragment straddle a boundary.
    static_cast<void>(parser.skip(byte_count{1}));
    std::size_t reads = 0;
    perf_tests::start_measuring_time();
    while (parser.bytes_remaining().value() >= sizeof(std::uint64_t)) {
        const auto value = parser.read_be<std::uint64_t>();
        perf_tests::do_not_optimize(*value);
        ++reads;
    }
    perf_tests::stop_measuring_time();
    return reads;
}

PERF_TEST(builder, tiny_append_packing_1024_bytes) {
    fragmented_buffer_builder builder;
    for (std::size_t index = 0; index < tiny_append_total; ++index) {
        static_cast<void>(builder.append(std::string_view{"x"}));
    }
    auto published = builder.finish();
    perf_tests::do_not_optimize(published->fragment_count());
    return tiny_append_total;
}

PERF_TEST(builder, append_shared_buffers_1000_x_4_bytes) {
    auto inputs = shared_small_buffers();
    fragmented_buffer_builder builder;

    perf_tests::start_measuring_time();
    for (auto& input : inputs) {
        static_cast<void>(builder.append_buffer(std::move(input)));
    }
    perf_tests::stop_measuring_time();

    auto published = builder.finish();
    perf_tests::do_not_optimize(published->fragment_count());
    return shared_append_total;
}

PERF_TEST_F(bulk_builder_fixture, bulk_append_262144_bytes_64_appends) {
    perf_tests::start_measuring_time();
    fragmented_buffer_builder builder{config};
    for (std::size_t index = 0; index < fragment_total; ++index) {
        static_cast<void>(builder.append(payload));
    }
    auto published = builder.finish();
    perf_tests::do_not_optimize(published->fragment_count());
    perf_tests::stop_measuring_time();
    return fragment_total;
}

PERF_TEST(fragmented_buffer, release_to_packet_10000_bytes_1000_fragments) {
    std::vector<fragmented_buffer> inputs;
    inputs.reserve(transfer_input_total);
    for (std::size_t index = 0; index < transfer_input_total; ++index) {
        inputs.push_back(
          fixed_fragments(transfer_fragment_total, transfer_fragment_bytes));
    }

    std::vector<seastar::net::packet> outputs;
    outputs.reserve(transfer_input_total);
    perf_tests::start_measuring_time();
    for (auto& input : inputs) {
        auto transferred = std::move(input).release_to_packet();
        outputs.emplace_back(std::move(*transferred));
    }
    perf_tests::do_not_optimize(outputs);
    // Ownership cleanup is part of the transfer cost rather than deferred
    // until after the timed region.
    outputs.clear();
    inputs.clear();
    perf_tests::stop_measuring_time();
    return transfer_input_total;
}

} // namespace kwaque::bytes
