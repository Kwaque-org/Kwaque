#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/bytes/fragmented_buffer_parser.h"

#include <seastar/core/temporary_buffer.hh>
#include <seastar/testing/perf_tests.hh>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
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
    auto buffer = fragmented_buffer::copy_from_fragments(fragments);
    if (!buffer) {
        throw std::runtime_error("buffer benchmark fixture construction");
    }
    return std::move(*buffer);
}

fragmented_buffer many_fragments() {
    return fixed_fragments(fragment_total, fragment_bytes);
}

std::vector<fragmented_buffer> shared_small_buffers() {
    const std::string payload(shared_append_total * shared_append_bytes, 's');
    auto source = fragmented_buffer::copy_of(
      std::span<const char>{payload.data(), payload.size()});
    if (!source) {
        throw std::runtime_error("buffer benchmark shared source");
    }

    std::vector<fragmented_buffer> inputs;
    inputs.reserve(shared_append_total);
    for (std::size_t index = 0; index < shared_append_total; ++index) {
        auto shared = source->share(
          byte_count{index * shared_append_bytes},
          byte_count{shared_append_bytes});
        if (!shared) {
            throw std::runtime_error("buffer benchmark shared input");
        }
        inputs.push_back(std::move(*shared));
    }
    return inputs;
}

struct buffer_fixture {
    fragmented_buffer buffer{many_fragments()};
};

struct tiny_fragments_fixture {
    fragmented_buffer buffer{
      fixed_fragments(transfer_fragment_total, transfer_fragment_bytes)};
};

struct linear_buffer_fixture {
    fragmented_buffer buffer{
      fixed_fragments(fragment_total / 2, fragment_bytes)};
};

struct native_fragments_fixture {
    std::vector<seastar::temporary_buffer<char>> fragments;

    native_fragments_fixture() {
        fragments.reserve(fragment_total);
        for (std::size_t index = 0; index < fragment_total; ++index) {
            fragments.emplace_back(fragment_bytes);
            std::fill_n(
              fragments.back().get_write(),
              fragment_bytes,
              static_cast<char>('a' + index % 26));
        }
    }
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

PERF_TEST_F(native_fragments_fixture, seastar_share_64_fragments) {
    std::vector<seastar::temporary_buffer<char>> shared;
    shared.reserve(fragments.size());
    for (auto& fragment : fragments) {
        shared.push_back(fragment.share());
    }
    if (shared.size() != fragment_total) [[unlikely]] {
        throw std::runtime_error("native buffer benchmark share count");
    }
    perf_tests::do_not_optimize(shared.size());
}

PERF_TEST_F(buffer_fixture, kwaque_share_64_fragments) {
    auto shared = buffer.share();
    if (shared.fragment_count() != fragment_total) [[unlikely]] {
        throw std::runtime_error("buffer benchmark share count");
    }
    perf_tests::do_not_optimize(shared.fragment_count());
}

PERF_TEST_F(buffer_fixture, deep_copy_262144_bytes_64_to_2_fragments) {
    auto copied = buffer.copy();
    perf_tests::do_not_optimize(copied.fragment_count());
}

PERF_TEST_F(tiny_fragments_fixture, deep_copy_10000_bytes_1000_to_1_fragment) {
    auto copied = buffer.copy();
    perf_tests::do_not_optimize(copied.fragment_count());
}

PERF_TEST_F(buffer_fixture, parser_construct_from_share_64_fragments) {
    auto shared = buffer.share();
    fragmented_buffer_parser parser{std::move(shared)};
    perf_tests::do_not_optimize(parser.bytes_remaining().value());
}

PERF_TEST_F(buffer_fixture, share_interior_slice) {
    auto shared = buffer.share(
      byte_count{fragment_bytes / 2}, byte_count{fragment_bytes * 8});
    if (!shared) [[unlikely]] {
        throw std::runtime_error("buffer benchmark slice");
    }
    perf_tests::do_not_optimize(shared->fragment_count());
}

PERF_TEST_F(buffer_fixture, trim_front_262144_bytes_64_fragments) {
    auto working = buffer.share();
    std::size_t trimmed = 0;
    perf_tests::start_measuring_time();
    while (!working.empty()) {
        const auto bytes = working.fragment_at(0)->size();
        if (!working.trim_front(byte_count{bytes})) [[unlikely]] {
            throw std::runtime_error("buffer benchmark trim");
        }
        ++trimmed;
    }
    perf_tests::stop_measuring_time();
    return trimmed;
}

PERF_TEST_F(
  buffer_fixture, export_scatter_262144_bytes_64_fragments_4_batches) {
    scatter_cursor cursor;
    std::size_t batches = 0;
    while (true) {
        const auto batch = buffer.export_scatter(
          scatter_batch_vectors, byte_count{scatter_batch_bytes}, cursor);
        if (!batch) [[unlikely]] {
            throw std::runtime_error("buffer benchmark scatter");
        }
        perf_tests::do_not_optimize(batch->bytes().value());
        ++batches;
        if (batch->complete()) {
            break;
        }
    }
    return batches;
}

PERF_TEST_F(linear_buffer_fixture, linearize_131072_bytes_32_fragments) {
    auto linear = buffer.linearize(buffer.size());
    if (!linear || linear->size() != fixture_bytes / 2) [[unlikely]] {
        throw std::runtime_error("buffer benchmark linearization");
    }
    perf_tests::do_not_optimize(linear->size());
}

PERF_TEST_F(buffer_fixture, reject_linearize_above_contiguous_limit) {
    auto linear = buffer.linearize(buffer.size());
    if (linear || linear.error() != make_error_code(errc::resource_exhausted))
      [[unlikely]] {
        throw std::runtime_error("buffer benchmark linearization rejection");
    }
    perf_tests::do_not_optimize(linear.error());
}

PERF_TEST_F(buffer_fixture, cross_fragment_fixed_reads) {
    auto shared = buffer.share();
    fragmented_buffer_parser parser{std::move(shared)};
    // Starting one byte in makes one read per fragment straddle a boundary.
    if (!parser.skip(byte_count{1})) {
        throw std::runtime_error("buffer benchmark parser setup");
    }
    std::size_t reads = 0;
    perf_tests::start_measuring_time();
    while (parser.bytes_remaining().value() >= sizeof(std::uint64_t)) {
        const auto value = parser.read_be<std::uint64_t>();
        if (!value) [[unlikely]] {
            throw std::runtime_error("buffer benchmark parser read");
        }
        perf_tests::do_not_optimize(*value);
        ++reads;
    }
    perf_tests::stop_measuring_time();
    return reads;
}

PERF_TEST(builder, tiny_append_packing_1024_bytes) {
    fragmented_buffer_builder builder;
    perf_tests::start_measuring_time();
    for (std::size_t index = 0; index < tiny_append_total; ++index) {
        if (!builder.append(std::string_view{"x"})) [[unlikely]] {
            throw std::runtime_error("buffer benchmark tiny append");
        }
    }
    perf_tests::stop_measuring_time();
    auto published = builder.finish();
    if (!published || published->size().value() != tiny_append_total) {
        throw std::runtime_error("buffer benchmark tiny publication");
    }
    perf_tests::do_not_optimize(published->fragment_count());
    return tiny_append_total;
}

PERF_TEST(builder, append_shared_buffers_1000_x_4_bytes) {
    auto inputs = shared_small_buffers();
    fragmented_buffer_builder builder;

    perf_tests::start_measuring_time();
    for (auto& input : inputs) {
        if (!builder.append_buffer(std::move(input))) [[unlikely]] {
            throw std::runtime_error("buffer benchmark shared append");
        }
    }
    perf_tests::stop_measuring_time();

    auto published = builder.finish();
    if (
      !published
      || published->size().value()
           != shared_append_total * shared_append_bytes) {
        throw std::runtime_error("buffer benchmark shared publication");
    }
    perf_tests::do_not_optimize(published->fragment_count());
    return shared_append_total;
}

PERF_TEST_F(bulk_builder_fixture, bulk_append_262144_bytes_64_appends) {
    fragmented_buffer_builder builder{config};
    perf_tests::start_measuring_time();
    for (std::size_t index = 0; index < fragment_total; ++index) {
        if (!builder.append(payload)) [[unlikely]] {
            throw std::runtime_error("buffer benchmark bulk append");
        }
    }
    perf_tests::stop_measuring_time();
    auto published = builder.finish();
    if (!published || published->size().value() != fixture_bytes) {
        throw std::runtime_error("buffer benchmark bulk publication");
    }
    perf_tests::do_not_optimize(published->fragment_count());
    return fragment_total;
}

PERF_TEST(fragmented_buffer, copy_to_packet_10000_bytes_1000_fragments) {
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
        auto transferred = input.copy_to_packet();
        if (!transferred) [[unlikely]] {
            throw std::runtime_error("buffer benchmark packet transfer");
        }
        outputs.emplace_back(std::move(*transferred));
    }
    perf_tests::do_not_optimize(outputs);
    // Packet ownership cleanup is part of conversion cost rather than deferred
    // until after the timed region. Source destruction is independent because
    // this conversion is deliberately non-destructive.
    outputs.clear();
    perf_tests::stop_measuring_time();
    inputs.clear();
    return transfer_input_total;
}

} // namespace kwaque::bytes
