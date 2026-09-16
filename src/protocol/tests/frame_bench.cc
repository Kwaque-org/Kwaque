#include "src/codec/sha256.h"
#include "src/codec/tests/benchmark_buffer.h"
#include "src/model/tests/model_bench_fixture.h"
#include "src/protocol/batch_frame_codec.h"
#include "src/protocol/tests/frame_test_support.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/testing/perf_tests.hh>

#include <crc32c/crc32c.h>
#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>

namespace kwaque::protocol::bench {
namespace {
namespace fixture = testing::frame_fixture;
using bytes::fragmented_buffer;
using bytes::fragmented_buffer_parser;
using codec::bench::capacity_bound;
using codec::bench::require;
enum class operation { prefix, header, payload, submitted, assigned };
enum class shape {
    generic,
    control,
    missing_prefix,
    missing_header,
    missing_payload,
    submitted,
    assigned,
    sparse
};

seastar::future<codec::sha256_digest>
hash_records(const fragmented_buffer& records, codec::cooperative_work& work) {
    codec::sha256_hasher hash;
    for (auto fragment : records) {
        for (std::size_t offset = 0; offset < fragment.size();) {
            const auto n = std::min<std::size_t>(
              32768, fragment.size() - offset);
            (co_await work.admit(byte_count{n}, item_count{1})).value();
            work.poll().value();
            hash.update(fragment.data() + offset, n);
            offset += n;
        }
    }
    co_return std::move(hash).final();
}

class measurements {
public:
    measurements(
      std::size_t payload,
      std::size_t width,
      std::size_t header,
      shape kind = shape::generic,
      bool compressed = false)
      : size_(payload)
      , width_(width)
      , header_(header)
      , shape_(kind)
      , compressed_(compressed) {}

    template<operation Op>
    seastar::future<std::size_t> measure() {
        co_await initialize();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        if constexpr (Op == operation::prefix) {
            constexpr std::size_t iterations = 256;
            std::optional<codec::result<unverified_frame_prefix>> last;
            perf_tests::start_measuring_time();
            for (std::size_t i = 0; i < iterations; ++i) {
                barrier();
                last.emplace(
                  peek_frame_prefix(*input_, work.policy(), fixture::bounds));
                perf_tests::do_not_optimize(*last);
            }
            perf_tests::stop_measuring_time();
            require(
              last && last->has_value() && (**last).payload_bytes == size_
                && (**last).header_bytes == header_
                && input_->bytes_consumed() == byte_count{},
              "prefix probe changed fields/cursor");
            co_return iterations;
        } else if constexpr (Op == operation::header) {
            barrier();
            perf_tests::start_measuring_time();
            auto result = co_await inspect_frame_header(
              *input_, fixture::bounds, memory_, work);
            perf_tests::do_not_optimize(result);
            perf_tests::stop_measuring_time();
            require(
              result && result->payload_bytes.value() == size_
                && result->header_bytes.value() == header_
                && input_->bytes_consumed() == byte_count{},
              "header inspection changed fields/cursor");
        } else {
            // One caller mark restores the persistent fixture after each
            // sample. Input reservation and reset are setup; output release is
            // timed.
            input_->push_checkpoint().value();
            barrier();
            perf_tests::start_measuring_time();
            auto result = co_await [&] {
                if constexpr (Op == operation::submitted)
                    return decode_submitted_frame(
                      *input_, expected_, fixture::bounds, memory_, work);
                else if constexpr (Op == operation::assigned)
                    return decode_assigned_frame(
                      *input_, expected_, fixture::bounds, memory_, work);
                else
                    return decode_frame(
                      *input_, fixture::bounds, memory_, work);
            }();
            perf_tests::do_not_optimize(result);
            perf_tests::stop_measuring_time();
            require(result.has_value(), "frame decode failed");
            if constexpr (Op == operation::payload) {
                if (needed_ != 0) {
                    const auto* more = std::get_if<need_more>(&*result);
                    require(
                      more && more->additional_bytes.value() == needed_
                        && input_->bytes_consumed() == byte_count{},
                      "incomplete frame changed its next boundary");
                } else {
                    const auto* ready = std::get_if<framed_payload>(&*result);
                    require(
                      ready && input_->at_end()
                        && ready->payload.size().value() == size_,
                      "decoded payload extent differs");
                    for (auto fragment : ready->payload) {
                        require(
                          std::all_of(
                            fragment.bytes().begin(),
                            fragment.bytes().end(),
                            [](char c) { return c == 'x'; }),
                          "decoded payload bytes differ");
                        (co_await work.checkpoint()).value();
                    }
                }
            } else {
                using decoded_type = std::conditional_t<
                  Op == operation::submitted,
                  decoded_submitted_frame,
                  decoded_assigned_frame>;
                const auto* ready = std::get_if<decoded_type>(&*result);
                require(
                  ready && input_->at_end()
                    && ready->batch.records().size().value() == record_bytes_
                    && ready->batch.fingerprint() == *digest_,
                  "typed frame changed batch metadata");
                const auto original = [&] {
                    if constexpr (Op == operation::assigned)
                        return ready->batch.context().submitted();
                    else
                        return ready->batch.context();
                }();
                require(
                  original.id() == *expected_.id
                    && original.binding() == *expected_.original_binding
                    && original.original_count().value() == original_count_,
                  "typed frame changed original context");
                require(
                  co_await hash_records(ready->batch.records(), work)
                    == *record_hash_,
                  "typed frame changed record bytes");
                if constexpr (Op == operation::assigned) {
                    require(
                      ready->batch.context().retained_count().value()
                          == retained_count_
                        && ready->batch.context().logical_span().begin().value()
                             == 100
                        && ready->batch.context().logical_span().end().value()
                             == 100 + original_count_,
                      "typed frame changed logical coverage");
                    require(
                      ready->fingerprint_verification
                        == (shape_ == shape::sparse ? model::batch_fingerprint_verification::carried : model::batch_fingerprint_verification::recomputed),
                      "typed frame changed fingerprint verification");
                }
            }
            perf_tests::start_measuring_time();
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            result = codec::failure(codec::error{errc::success});
            perf_tests::stop_measuring_time();
            input_->rollback().value();
        }
        // Native time/allocation/task samples are per frame operation.
        co_return 1;
    }

private:
    static void barrier() {
        // NOLINTNEXTLINE(portability-no-assembler)
        asm volatile("" : : : "memory");
    }
    seastar::future<> initialize() {
        if (input_) co_return;
        codec::bench::qualify_allocator();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        fragmented_buffer wire;
        const auto ext = header_ == 48
                           ? std::string{}
                           : fixture::extension(
                               0x7ffe, 0, std::string(header_ - 56, 'x'));
        if (shape_ == shape::generic || shape_ == shape::control) {
            auto fields = fixture::metadata;
            if (shape_ == shape::control) {
                fields.kind = frame_kind::handshake_response;
                fields.stream = model::transport_stream_id{};
            }
            wire = co_await fixture::wrap(
              co_await fixture::make_payload(size_, width_, work),
              work,
              ext,
              fields);
        } else if (shape_ == shape::missing_prefix) {
            wire = fixture::fragmented(
              std::string_view{fixture::header(0, 0)}.substr(0, 17), width_);
            needed_ = 31;
        } else if (shape_ == shape::missing_header) {
            wire = fixture::fragmented(
              std::string_view{fixture::header(0, 0, ext)}.substr(0, 48),
              width_);
            needed_ = header_ - 48;
        } else if (shape_ == shape::missing_payload) {
            wire = fixture::fragmented(
              fixture::header(static_cast<std::uint32_t>(size_), 0), width_);
            needed_ = size_;
        } else {
            fragmented_buffer inner;
            {
                model::bench::model_fixture source{
                  size_ == 0 ? 3U : 8U, size_ == 0 ? 64U : 1048565U, width_};
                co_await source.initialize();
                digest_ = source.digest;
                original_count_ = source.count;
                retained_count_ = shape_ == shape::sparse
                                    ? source.selected.size()
                                    : source.count;
                inner = shape_ == shape::submitted
                          ? std::move(source.submitted_wire)
                        : shape_ == shape::sparse
                          ? std::move(source.sparse_wire)
                          : std::move(source.assigned_wire);
            }
            if (compressed_) {
                fragmented_buffer_parser source{std::move(inner)};
                const auto memory = codec::reserve_decode_input(
                                      source, work.policy(), memory_)
                                      .value();
                if (shape_ == shape::submitted) {
                    auto decoded = co_await model::decode_submitted_batch(
                      source, expected_, memory, work);
                    require(
                      decoded.has_value(), "submitted fixture decode failed");
                    inner = (co_await model::encode_submitted_batch(
                               std::move(decoded->value),
                               compression::codec_id::lz4,
                               work,
                               memory.operation_remaining,
                               capacity_bound))
                              .value();
                } else {
                    auto decoded = co_await model::decode_assigned_batch(
                      source, expected_, memory, work);
                    require(
                      decoded.has_value(), "assigned fixture decode failed");
                    inner = (co_await model::encode_assigned_batch(
                               std::move(decoded->value),
                               compression::codec_id::lz4,
                               work,
                               memory.operation_remaining,
                               capacity_bound))
                              .value();
                }
            }
            std::uint32_t checksum = 0;
            for (auto fragment : inner) {
                checksum = ::crc32c::Extend(
                  checksum,
                  reinterpret_cast<const std::uint8_t*>(fragment.data()),
                  fragment.size());
                (co_await work.checkpoint()).value();
            }
            auto fields = fixture::metadata;
            fields.kind = shape_ == shape::submitted
                            ? frame_kind::submitted_batch
                            : frame_kind::assigned_batch;
            size_ = inner.size().value();
            wire = co_await fixture::wrap(
              {std::move(inner), checksum}, work, ext, fields);
        }
        input_.emplace(std::move(wire));
        // The one cached parser is the input owner. Its backing, descriptors
        // and persistent promotion are reserved once for the fixture lifetime;
        // samples do not double-charge an alias of that same cached backing.
        const auto cost = input_->allocation_cost(capacity_bound).value();
        memory_ = codec::reserve_decode_input(*input_, work.policy(), memory_)
                    .value();
        if (
          shape_ == shape::submitted || shape_ == shape::assigned
          || shape_ == shape::sparse) {
            input_->push_checkpoint().value();
            if (shape_ == shape::submitted) {
                auto decoded = co_await decode_submitted_frame(
                  *input_, expected_, fixture::bounds, memory_, work);
                require(
                  decoded
                    && std::holds_alternative<decoded_submitted_frame>(
                      *decoded),
                  "typed fixture failed validation");
                const auto& value = std::get<decoded_submitted_frame>(*decoded);
                record_bytes_ = value.batch.records().size().value();
                record_hash_ = co_await hash_records(
                  value.batch.records(), work);
            } else {
                auto decoded = co_await decode_assigned_frame(
                  *input_, expected_, fixture::bounds, memory_, work);
                require(
                  decoded
                    && std::holds_alternative<decoded_assigned_frame>(*decoded),
                  "typed fixture failed validation");
                const auto& value = std::get<decoded_assigned_frame>(*decoded);
                record_bytes_ = value.batch.records().size().value();
                record_hash_ = co_await hash_records(
                  value.batch.records(), work);
            }
            input_->rollback().value();
        }
        fmt::print(
          "frame_fixture header_bytes={} declared_payload_bytes={} "
          "available_bytes={} fragments={} backing_bound={} metadata_bound={} "
          "persistent_input=true normalization=frame\n",
          header_,
          size_,
          input_->total_bytes().value(),
          cost.fragments.value(),
          cost.backing.value(),
          cost.descriptors.value() + cost.share_controls.value());
    }
    std::size_t size_, width_, header_;
    shape shape_;
    bool compressed_;
    std::size_t needed_{0}, record_bytes_{0};
    std::size_t original_count_{0}, retained_count_{0};
    std::optional<fragmented_buffer_parser> input_;
    const model::batch_decode_expectation expected_
      = model::bench::expected_context();
    codec::decode_budget memory_{
      byte_count{63U << 20U}, byte_count{1U << 20U}, capacity_bound};
    std::optional<codec::semantic_batch_digest> digest_;
    std::optional<codec::sha256_digest> record_hash_;
};

template<
  std::size_t Size,
  std::size_t Width,
  std::size_t Header = 48,
  shape Shape = shape::generic,
  bool Compressed = false>
struct fixture_case : measurements {
    fixture_case()
      : measurements(Size, Width, Header, Shape, Compressed) {}
};
using frame_tiny = fixture_case<3, 64>;
using frame_tiny_frag7 = fixture_case<71, 7>;
using frame_medium = fixture_case<65536, 67, 57>;
using frame_max = fixture_case<16777216, 65472>;
using frame_max_frag = fixture_case<16777216, 16401>;
using frame_extended = fixture_case<71, 7, 4096>;
// Control bytes are framing-only here; schema work has its own boundary.
using frame_control = fixture_case<65536, 32704, 48, shape::control>;
#define FRAME_CASES(group)                                                     \
    PERF_TEST_F(group, prefix) { return measure<operation::prefix>(); }        \
    PERF_TEST_F(group, header) { return measure<operation::header>(); }        \
    PERF_TEST_F(group, decode) { return measure<operation::payload>(); }
FRAME_CASES(frame_tiny)
FRAME_CASES(frame_tiny_frag7)
FRAME_CASES(frame_medium)
FRAME_CASES(frame_max)
FRAME_CASES(frame_max_frag)
FRAME_CASES(frame_extended)
FRAME_CASES(frame_control)
#undef FRAME_CASES
using frame_missing_prefix = fixture_case<0, 7, 48, shape::missing_prefix>;
using frame_missing_header = fixture_case<0, 7, 4096, shape::missing_header>;
using frame_missing_payload
  = fixture_case<16777216, 64, 48, shape::missing_payload>;
PERF_TEST_F(frame_missing_prefix, decode) {
    return measure<operation::payload>();
}
PERF_TEST_F(frame_missing_header, decode) {
    return measure<operation::payload>();
}
PERF_TEST_F(frame_missing_payload, decode) {
    return measure<operation::payload>();
}
using frame_submitted_tiny = fixture_case<0, 7, 48, shape::submitted>;
using frame_submitted_lz4 = fixture_case<0, 7, 48, shape::submitted, true>;
using frame_assigned_tiny = fixture_case<0, 7, 48, shape::assigned>;
using frame_sparse_tiny = fixture_case<0, 7, 48, shape::sparse>;
using frame_assigned_max = fixture_case<1, 65472, 48, shape::assigned>;
using frame_assigned_max_lz4
  = fixture_case<1, 65472, 48, shape::assigned, true>;
using frame_sparse_max = fixture_case<1, 65472, 48, shape::sparse>;
PERF_TEST_F(frame_submitted_tiny, decode) {
    return measure<operation::submitted>();
}
PERF_TEST_F(frame_submitted_lz4, decode) {
    return measure<operation::submitted>();
}
PERF_TEST_F(frame_assigned_tiny, decode) {
    return measure<operation::assigned>();
}
PERF_TEST_F(frame_sparse_tiny, decode) {
    return measure<operation::assigned>();
}
PERF_TEST_F(frame_assigned_max, decode) {
    return measure<operation::assigned>();
}
PERF_TEST_F(frame_assigned_max_lz4, decode) {
    return measure<operation::assigned>();
}
PERF_TEST_F(frame_sparse_max, decode) { return measure<operation::assigned>(); }

} // namespace
} // namespace kwaque::protocol::bench
