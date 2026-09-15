#include "src/codec/sha256.h"
#include "src/model/tests/model_bench_fixture.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/testing/perf_tests.hh>

#include <fmt/format.h>

#include <algorithm>
#include <cstddef>
#include <optional>
#include <utility>

namespace kwaque::model::bench {
namespace {
using bytes::fragmented_buffer;
using codec::bench::payload_pattern;

// The caller keeps its immutable batch alive and unmoved until this completes.
// Verification borrows the bytes without promoting their ownership or copying
// the record region. Native SHA state is gone before the final abort poll.
seastar::future<codec::sha256_digest>
hash_records(const fragmented_buffer& records, codec::cooperative_work& work) {
    work.poll().value();
    work.policy()
      .validate_buffer(
        records.size(),
        records.retained_bytes(),
        item_count{records.fragment_count()},
        work.policy().config().max_expanded_batch_bytes)
      .value();
    (co_await work.checkpoint()).value();
    work.poll().value();
    std::optional<codec::sha256_digest> digest;
    {
        codec::sha256_hasher hasher;
        for (const auto fragment : records) {
            for (std::size_t offset = 0; offset < fragment.size();) {
                const auto size = std::min(
                  fragment.size() - offset,
                  static_cast<std::size_t>(work.byte_quantum().value()));
                (co_await work.admit(byte_count{size}, item_count{1})).value();
                work.poll().value();
                hasher.update(fragment.data() + offset, size);
                offset += size;
            }
        }
        work.poll().value();
        digest.emplace(std::move(hasher).final());
    }
    work.poll().value();
    co_return *digest;
}

class compressed_measurements {
public:
    compressed_measurements(
      std::size_t count,
      std::size_t payload,
      std::size_t width,
      payload_pattern pattern)
      : count_(count)
      , payload_(payload)
      , width_(width)
      , pattern_(pattern) {}

    template<bool Assigned, bool Sparse, bool Encode>
    seastar::future<std::size_t> operation() {
        co_await initialize<Assigned, Sparse>();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        bytes::fragmented_buffer_parser input{wire_.share()};
        const auto budget
          = codec::reserve_decode_input(input, work.policy(), memory()).value();
        if constexpr (Encode) {
            auto source = (co_await [&] {
                              if constexpr (Assigned)
                                  return decode_assigned_batch(
                                    input, expected_context(), budget, work);
                              else
                                  return decode_submitted_batch(
                                    input, expected_context(), budget, work);
                          }())
                            .value();
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            input = bytes::fragmented_buffer_parser{};
            perf_tests::start_measuring_time();
            auto result = co_await [&] {
                if constexpr (Assigned)
                    return encode_assigned_batch(
                      std::move(source.value),
                      compression::codec_id::lz4,
                      work,
                      remaining_,
                      capacity_bound);
                else
                    return encode_submitted_batch(
                      std::move(source.value),
                      compression::codec_id::lz4,
                      work,
                      remaining_,
                      capacity_bound);
            }();
            perf_tests::do_not_optimize(result);
            perf_tests::stop_measuring_time();
            require(result.has_value(), "compressed batch encode failed");
            report_output(*result);
            // Independent full decode checks grammar, both CRCs and dense SHA.
            bytes::fragmented_buffer_parser verify{result->share()};
            const auto verify_budget = codec::reserve_decode_input(
                                         verify, work.policy(), memory())
                                         .value();
            {
                auto checked = co_await [&] {
                    if constexpr (Assigned)
                        return decode_assigned_batch(
                          verify, expected_context(), verify_budget, work);
                    else
                        return decode_submitted_batch(
                          verify, expected_context(), verify_budget, work);
                }();
                require(
                  checked && verify.at_end()
                    && checked->value.fingerprint() == *digest_,
                  "compressed batch verification failed");
                require(
                  (co_await hash_records(checked->value.records(), work))
                    == *retained_digest_,
                  "compressed batch changed retained records");
                co_await work.drain_inline(
                  work.byte_quantum(), work.item_quantum());
            }
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            verify = bytes::fragmented_buffer_parser{};
            perf_tests::start_measuring_time();
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            result = codec::failure(codec::error{errc::success});
        } else {
            perf_tests::start_measuring_time();
            auto result = co_await [&] {
                if constexpr (Assigned)
                    return decode_assigned_batch(
                      input, expected_context(), budget, work);
                else
                    return decode_submitted_batch(
                      input, expected_context(), budget, work);
            }();
            perf_tests::do_not_optimize(result);
            perf_tests::stop_measuring_time();
            require(
              result && input.at_end()
                && result->value.fingerprint() == *digest_,
              "compressed batch decode failed");
            require(
              result->value.records().size() == expanded_,
              "compressed batch changed expanded length");
            report_output(result->value.records());
            require(
              (co_await hash_records(result->value.records(), work))
                == *retained_digest_,
              "compressed batch changed retained records");
            perf_tests::start_measuring_time();
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            result = codec::failure(codec::error{errc::success});
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            input = bytes::fragmented_buffer_parser{};
        }
        perf_tests::stop_measuring_time();
        co_return std::size_t{
          1}; // one complete batch, including output cleanup
    }

private:
    void report_output(const fragmented_buffer& value) {
        if (reported_output_) return;
        const auto cost = value.allocation_cost(capacity_bound).value();
        fmt::print(
          "kwaque-batch-output-v1 backing_charge_upper={} "
          "descriptor_charge_upper={} share_control_charge_upper={} "
          "fragments={}\n",
          cost.backing.value(),
          cost.descriptors.value(),
          cost.share_controls.value(),
          cost.fragments.value());
        reported_output_ = true;
    }
    codec::decode_budget memory() const {
        return {remaining_, byte_count{1U << 20U}, capacity_bound};
    }
    template<bool Assigned, bool Sparse>
    seastar::future<> initialize() {
        if (initialized_) co_return;
        model_fixture fixture{count_, payload_, width_, pattern_};
        co_await fixture.initialize();
        digest_ = fixture.digest;
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto source = co_await [&] {
            if constexpr (Assigned)
                return fixture.assigned(work, Sparse);
            else
                return fixture.submitted(work);
        }();
        expanded_ = source.records().size();
        retained_digest_ = co_await hash_records(source.records(), work);
        auto encoded = (co_await [&] {
                           if constexpr (Assigned)
                               return encode_assigned_batch(
                                 std::move(source),
                                 compression::codec_id::lz4,
                                 work,
                                 fixture.remaining,
                                 capacity_bound);
                           else
                               return encode_submitted_batch(
                                 std::move(source),
                                 compression::codec_id::lz4,
                                 work,
                                 fixture.remaining,
                                 capacity_bound);
                       }())
                         .value();
        wire_ = co_await copy_layout(
          std::move(encoded), width_, work, fixture.remaining);
        const auto cost = wire_.allocation_cost(capacity_bound).value();
        const auto retained = cost.backing.value() + cost.descriptors.value()
                              + cost.share_controls.value();
        remaining_ = byte_count{(63U << 20U) - retained};
        fmt::print(
          "kwaque-compressed-batch-v1 kind={} original_records={} "
          "retained_records={} pattern={} "
          "expanded_bytes={} wire_bytes={} encoded_record_bytes={} ratio={} "
          "wire_fragments={} wire_retained_upper={} normalization=batches "
          "scope=cold_operation\n",
          Assigned ? (Sparse ? "sparse" : "assigned") : "submitted",
          count_,
          Sparse ? fixture.selected.size() : count_,
          static_cast<int>(pattern_),
          expanded_.value(),
          wire_.size().value(),
          wire_.size().value() - (Assigned ? 216U : 200U),
          static_cast<double>(wire_.size().value() - (Assigned ? 216U : 200U))
            / static_cast<double>(expanded_.value()),
          wire_.fragment_count(),
          retained);
        initialized_ = true;
    }
    std::size_t count_, payload_, width_;
    payload_pattern pattern_;
    fragmented_buffer wire_;
    std::optional<codec::semantic_batch_digest> digest_;
    std::optional<codec::sha256_digest> retained_digest_;
    byte_count expanded_;
    byte_count remaining_{63U << 20U};
    bool initialized_{false};
    bool reported_output_{false};
};
template<std::size_t Payload, std::size_t Width, payload_pattern Pattern>
struct fixture : compressed_measurements {
    fixture()
      : compressed_measurements(8, Payload, Width, Pattern) {}
};
using batch_lz4_tiny = fixture<64, 7, payload_pattern::compressible>;
using batch_lz4_block_noise
  = fixture<8180, 4096, payload_pattern::incompressible>;
using batch_lz4_block_mixed = fixture<8180, 4096, payload_pattern::mixed>;
using batch_lz4_max_repeat
  = fixture<1048565, 65536, payload_pattern::compressible>;
using batch_lz4_max_noise
  = fixture<1048565, 65536, payload_pattern::incompressible>;
using batch_lz4_max_mixed = fixture<1048565, 65536, payload_pattern::mixed>;
#define BATCH_COMPRESSION_CASES(group)                                         \
    PERF_TEST_F(group, submitted_encode) {                                     \
        return operation<false, false, true>();                                \
    }                                                                          \
    PERF_TEST_F(group, submitted_decode) {                                     \
        return operation<false, false, false>();                               \
    }                                                                          \
    PERF_TEST_F(group, assigned_encode) {                                      \
        return operation<true, false, true>();                                 \
    }                                                                          \
    PERF_TEST_F(group, assigned_decode) {                                      \
        return operation<true, false, false>();                                \
    }                                                                          \
    PERF_TEST_F(group, sparse_encode) {                                        \
        return operation<true, true, true>();                                  \
    }                                                                          \
    PERF_TEST_F(group, sparse_decode) { return operation<true, true, false>(); }
BATCH_COMPRESSION_CASES(batch_lz4_tiny)
BATCH_COMPRESSION_CASES(batch_lz4_block_noise)
BATCH_COMPRESSION_CASES(batch_lz4_block_mixed)
BATCH_COMPRESSION_CASES(batch_lz4_max_repeat)
BATCH_COMPRESSION_CASES(batch_lz4_max_noise)
BATCH_COMPRESSION_CASES(batch_lz4_max_mixed)
#undef BATCH_COMPRESSION_CASES
} // namespace
} // namespace kwaque::model::bench
