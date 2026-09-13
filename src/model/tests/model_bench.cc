#include "src/model/batch_rewrite.h"
#include "src/model/fingerprint.h"
#include "src/model/record_scan.h"
#include "src/model/tests/model_bench_fixture.h"
#include "src/model/tests/model_bench_reference.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/testing/perf_tests.hh>

#include <array>
#include <optional>
#include <utility>

namespace kwaque::model::bench {
namespace {
[[gnu::always_inline]] inline void barrier() {
    // NOLINTNEXTLINE(portability-no-assembler)
    asm volatile("" : : : "memory");
}
class measurements : public model_fixture {
public:
    using model_fixture::model_fixture;
    template<bool Candidate>
    seastar::future<std::size_t> size() {
        co_await initialize(true);
        std::array<result<record_sizes>, 64> outcomes;
        barrier();
        perf_tests::start_measuring_time();
        for (auto& outcome : outcomes) {
            // Symmetric input opacity prevents constant-field size folding.
            // NOLINTNEXTLINE(portability-no-assembler)
            asm volatile("" : "+m"(*value) : : "memory");
            if constexpr (Candidate)
                outcome = codec_size(*value, codec::limits::defaults());
            else
                outcome = checked_size(*value, codec::limits::defaults());
        }
        perf_tests::do_not_optimize(outcomes);
        perf_tests::stop_measuring_time();
        for (const auto& outcome : outcomes)
            require(
              outcome && outcome->encoded_bytes == record_wire.size(),
              "record size measurement disagrees with wire");
        co_return outcomes.size(); // records, never bytes
    }
    seastar::future<std::size_t> admission() {
        co_await initialize(true);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        barrier();
        perf_tests::start_measuring_time();
        auto result = co_await reserve_record_input(*value, work, memory());
        perf_tests::do_not_optimize(result);
        perf_tests::stop_measuring_time();
        require(result.has_value(), "record admission rejected fixture");
        co_return std::size_t{1};
    }
    template<bool Materialize>
    seastar::future<std::size_t> record_read() {
        co_await initialize(true);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        bytes::fragmented_buffer_parser input{record_wire.share()};
        const auto budget
          = codec::reserve_decode_input(input, work.policy(), memory()).value();
        const record_decode_context expected{
          runtime::wall_time{100},
          range_logical_count{count},
          item_count{4096}};
        if constexpr (Materialize) {
            barrier();
            perf_tests::start_measuring_time();
            auto result = co_await decode_record(input, expected, budget, work);
            perf_tests::do_not_optimize(result);
            perf_tests::stop_measuring_time();
            require(
              result.has_value() && input.at_end(),
              "record materialization failed");
            require(
              (co_await records_equal(result->value, *value, work)).value(),
              "materialized record changed fields");
            barrier();
            perf_tests::start_measuring_time();
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            result = codec::failure(codec::error{errc::success});
        } else {
            (co_await work.admit(
               byte_count{4U * sizeof(record_layout)}, item_count{64}))
              .value();
            barrier();
            perf_tests::start_measuring_time();
            auto result = co_await detail::scan_record(
              input, expected, budget, work, {});
            perf_tests::do_not_optimize(result);
            perf_tests::stop_measuring_time();
            require(
              result.has_value() && input.at_end()
                && result->fields == value->fields(),
              "record scan changed relative fields");
            require(
              result->headers().size() == value->headers().size(),
              "record scan changed header count");
            barrier();
            perf_tests::start_measuring_time();
        }
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        input = bytes::fragmented_buffer_parser{};
        perf_tests::stop_measuring_time();
        co_return std::size_t{1};
    }
    template<bool Candidate, bool Assigned>
    seastar::future<std::size_t> decode(bool sparse = false) {
        co_await initialize(true);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto& wire = Assigned ? (sparse ? sparse_wire : assigned_wire)
                              : submitted_wire;
        bytes::fragmented_buffer_parser input{wire.share()};
        const auto budget
          = codec::reserve_decode_input(input, work.policy(), memory()).value();
        barrier();
        perf_tests::start_measuring_time();
        auto result = co_await [&] {
            if constexpr (Assigned && Candidate)
                return codec_decode_assigned(
                  input, expected_context(), budget, work);
            else if constexpr (Assigned)
                return checked_decode_assigned(
                  input, expected_context(), budget, work);
            else if constexpr (Candidate)
                return codec_decode_submitted(
                  input, expected_context(), budget, work);
            else
                return checked_decode_submitted(
                  input, expected_context(), budget, work);
        }();
        perf_tests::do_not_optimize(result);
        perf_tests::stop_measuring_time();
        require(
          result.has_value() && input.at_end(),
          "batch decoder rejected complete fixture");
        require(
          result->value.fingerprint() == *digest,
          "batch decoder changed fingerprint");
        if constexpr (Assigned)
            require(
              result->value.context().retained_count().value()
                == (sparse ? selected.size() : count),
              "batch decoder changed retained count");
        else
            require(
              result->value.context().original_count().value() == count,
              "batch decoder changed original count");
        barrier();
        perf_tests::start_measuring_time();
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        result = codec::failure(codec::error{errc::success});
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        input = bytes::fragmented_buffer_parser{};
        perf_tests::stop_measuring_time();
        co_return std::size_t{1}; // whole batches
    }
    template<bool Candidate, bool Assigned>
    seastar::future<std::size_t> encode(bool sparse = false) {
        co_await initialize(true);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto input = co_await [&] {
            if constexpr (Assigned)
                return assigned(work, sparse);
            else
                return submitted(work);
        }();
        barrier();
        perf_tests::start_measuring_time();
        auto result = co_await [&] {
            if constexpr (Assigned && Candidate)
                return codec_encode_assigned(
                  std::move(input), work, remaining, capacity_bound);
            else if constexpr (Assigned)
                return checked_encode_assigned(
                  std::move(input), work, remaining, capacity_bound);
            else if constexpr (Candidate)
                return codec_encode_submitted(
                  std::move(input), work, remaining, capacity_bound);
            else
                return checked_encode_submitted(
                  std::move(input), work, remaining, capacity_bound);
        }();
        perf_tests::do_not_optimize(result);
        perf_tests::stop_measuring_time();
        require(result.has_value(), "batch encoder rejected fixture");
        // Verification is a separate complete decode, outside the timed region.
        bytes::fragmented_buffer_parser verify{result->share()};
        const auto budget = codec::reserve_decode_input(
                              verify, work.policy(), memory())
                              .value();
        if constexpr (Assigned) {
            auto checked = co_await decode_assigned_batch(
              verify, expected_context(), budget, work);
            require(
              checked && checked->value.fingerprint() == *digest
                && verify.at_end(),
              "encoded assigned batch failed verification");
        } else {
            auto checked = co_await decode_submitted_batch(
              verify, expected_context(), budget, work);
            require(
              checked && checked->value.fingerprint() == *digest
                && verify.at_end(),
              "encoded submission failed verification");
        }
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        verify = bytes::fragmented_buffer_parser{};
        barrier();
        perf_tests::start_measuring_time();
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        result = codec::failure(codec::error{errc::success});
        perf_tests::stop_measuring_time();
        co_return std::size_t{1};
    }
    seastar::future<std::size_t> submitted_build() {
        co_await initialize(true);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        barrier();
        perf_tests::start_measuring_time();
        auto result = co_await build(work);
        perf_tests::do_not_optimize(result);
        perf_tests::stop_measuring_time();
        require(
          result.fingerprint() == *digest
            && result.records().size() == record_region_bytes,
          "submitted build changed bytes or digest");
        require(
          (co_await compute_submitted_fingerprint(
             result.context(), result.records(), work))
              .value()
            == *digest,
          "built bytes failed independent projection pass");
        auto records = std::move(result).release_records();
        barrier();
        perf_tests::start_measuring_time();
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        records = bytes::fragmented_buffer{};
        perf_tests::stop_measuring_time();
        co_return std::size_t{1};
    }
    seastar::future<std::size_t> fingerprint_only() {
        co_await initialize(true);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto source = co_await submitted(work);
        barrier();
        perf_tests::start_measuring_time();
        auto result = co_await compute_submitted_fingerprint(
          source.context(), source.records(), work);
        perf_tests::do_not_optimize(result);
        perf_tests::stop_measuring_time();
        require(
          result && *result == *digest,
          "fingerprint measurement changed projection");
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        co_return std::size_t{1};
    }
    seastar::future<std::size_t> assignment_only() {
        co_await initialize(true);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        // Pure transfer only: preparation, validation and eventual destruction
        // are outside timing. This is not paired with a serialization
        // benchmark.
        std::array<std::optional<submitted_batch>, 64> inputs;
        std::array<std::optional<assigned_batch>, 64> outputs;
        require(
          count <= 64 && payload_size <= 1024,
          "assignment fixture exceeds bounded sample group");
        const auto saved_budget = remaining;
        for (auto& input : inputs) {
            input.emplace(co_await submitted(work));
            const auto cost
              = input->records().allocation_cost(capacity_bound).value();
            remaining = remaining
                          .checked_sub(
                            byte_count{
                              cost.backing.value() + cost.descriptors.value()
                              + cost.share_controls.value()})
                          .value();
        }
        remaining = saved_budget;
        barrier();
        perf_tests::start_measuring_time();
        for (std::size_t i = 0; i < inputs.size(); ++i)
            outputs[i].emplace(
              assigned_batch::assign(
                std::move(*inputs[i]),
                range_logical_end{100},
                fixture_binding())
                .value());
        perf_tests::do_not_optimize(outputs);
        perf_tests::stop_measuring_time();
        for (auto& output : outputs) {
            require(
              output->context().logical_span().count().value() == count
                && output->fingerprint() == *digest,
              "assignment changed original context");
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            output.reset();
        }
        co_return inputs.size();
    }
    seastar::future<std::size_t> sparse_rewrite() {
        co_await initialize(true);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto source = co_await assigned(work);
        const auto original = source.context();
        barrier();
        perf_tests::start_measuring_time();
        auto result = co_await rewrite_assigned_batch(
          std::move(source), selected, memory(), work);
        perf_tests::do_not_optimize(result);
        perf_tests::stop_measuring_time();
        require(
          result && result->fingerprint() == *digest
            && result->context().logical_span() == original.logical_span(),
          "rewrite changed original facts");
        require(
          result->context().retained_count().value() == selected.size(),
          "rewrite changed survivor count");
        barrier();
        perf_tests::start_measuring_time();
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        result = codec::failure(codec::error{errc::success});
        perf_tests::stop_measuring_time();
        co_return std::size_t{1};
    }
    seastar::future<std::size_t> remove_all() {
        co_await initialize(true);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto source = co_await assigned(work);
        const auto original = source.context();
        barrier();
        perf_tests::start_measuring_time();
        auto result = co_await remove_all_records(std::move(source), work);
        perf_tests::do_not_optimize(result);
        perf_tests::stop_measuring_time();
        require(
          result && result->submitted() == original.submitted()
            && result->logical_span() == original.logical_span()
            && result->fingerprint() == *digest,
          "removal changed original coverage or fingerprint");
        co_return std::size_t{1};
    }
};
template<std::size_t Count, std::size_t Payload, std::size_t Width>
struct fixture : measurements {
    fixture()
      : measurements(Count, Payload, Width) {}
};
using model_tiny = fixture<8, 64, 0>;
using model_tiny_frag7 = fixture<8, 64, 7>;
using model_medium_frag67 = fixture<8, 32768, 67>;
using model_max_region = fixture<8, 1048565, 65536>;
using model_dense_headers = fixture<4096, 0, 65536>;

#define MODEL_CASES(group)                                                     \
    PERF_TEST_F(group, checked_size) { return size<false>(); }                 \
    PERF_TEST_F(group, codec_size) { return size<true>(); }                    \
    PERF_TEST_F(group, admission) { return admission(); }                      \
    PERF_TEST_F(group, record_scan) { return record_read<false>(); }           \
    PERF_TEST_F(group, record_materialize) { return record_read<true>(); }     \
    PERF_TEST_F(group, checked_submitted_decode) {                             \
        return decode<false, false>();                                         \
    }                                                                          \
    PERF_TEST_F(group, codec_submitted_decode) {                               \
        return decode<true, false>();                                          \
    }                                                                          \
    PERF_TEST_F(group, checked_assigned_decode) {                              \
        return decode<false, true>();                                          \
    }                                                                          \
    PERF_TEST_F(group, codec_assigned_decode) { return decode<true, true>(); } \
    PERF_TEST_F(group, checked_sparse_decode) {                                \
        return decode<false, true>(true);                                      \
    }                                                                          \
    PERF_TEST_F(group, codec_sparse_decode) {                                  \
        return decode<true, true>(true);                                       \
    }                                                                          \
    PERF_TEST_F(group, checked_submitted_encode) {                             \
        return encode<false, false>();                                         \
    }                                                                          \
    PERF_TEST_F(group, codec_submitted_encode) {                               \
        return encode<true, false>();                                          \
    }                                                                          \
    PERF_TEST_F(group, checked_assigned_encode) {                              \
        return encode<false, true>();                                          \
    }                                                                          \
    PERF_TEST_F(group, codec_assigned_encode) { return encode<true, true>(); } \
    PERF_TEST_F(group, checked_sparse_encode) {                                \
        return encode<false, true>(true);                                      \
    }                                                                          \
    PERF_TEST_F(group, codec_sparse_encode) {                                  \
        return encode<true, true>(true);                                       \
    }                                                                          \
    PERF_TEST_F(group, submitted_build) { return submitted_build(); }          \
    PERF_TEST_F(group, fingerprint) { return fingerprint_only(); }             \
    PERF_TEST_F(group, sparse_rewrite) { return sparse_rewrite(); }            \
    PERF_TEST_F(group, remove_all) { return remove_all(); }

MODEL_CASES(model_tiny)
MODEL_CASES(model_tiny_frag7)
MODEL_CASES(model_medium_frag67)
MODEL_CASES(model_max_region)
MODEL_CASES(model_dense_headers)
PERF_TEST_F(model_tiny, assignment) { return assignment_only(); }
#undef MODEL_CASES
} // namespace
} // namespace kwaque::model::bench
