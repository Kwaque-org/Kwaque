#include "proto/kwaque/common/v1/error.pb.h"
#include "proto/kwaque/control/v1/handshake.pb.h"
#include "proto/kwaque/control/v1/redirect.pb.h"
#include "src/codec/tests/benchmark_buffer.h"
#include "src/protocol/control_frame_codec.h"
#include "src/protocol/control_memory.h"
#include "src/protocol/control_native.h"
#include "src/protocol/control_preflight.h"
#include "src/protocol/tests/control_fuzz_oracle.h"
#include "src/protocol/tests/control_test_payload.h"
#include "src/protocol/tests/control_test_support.h"
#include "src/protocol/tests/frame_test_support.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/testing/perf_tests.hh>

#include <fmt/format.h>
#include <google/protobuf/io/coded_stream.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

namespace kwaque::protocol::bench {
namespace {
namespace fixture = testing::control_fixture;
namespace frame = testing::frame_fixture;
using codec::bench::capacity_bound;
using codec::bench::require;
enum class operation {
    preflight,
    preflight_parse,
    conversion,
    decode,
    serialize,
    framed_decode
};

template<frame_kind Kind, unsigned Shape, std::size_t Width>
class measurement {
    using Native = std::conditional_t<
      Kind == frame_kind::handshake_request,
      kwaque::control::v1::HandshakeRequest,
      std::conditional_t<
        Kind == frame_kind::handshake_response,
        kwaque::control::v1::HandshakeResponse,
        std::conditional_t<
          Kind == frame_kind::redirect,
          kwaque::control::v1::Redirect,
          kwaque::common::v1::Error>>>;

public:
    template<operation Op>
    seastar::future<std::size_t> measure() {
        co_await initialize();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        if constexpr (Op == operation::preflight) {
            std::optional<codec::result<item_count>> last;
            perf_tests::start_measuring_time();
            for (unsigned n = 0; n < 32; ++n) {
                barrier();
                last.emplace(
                  co_await detail::preflight_control(
                    std::span<const char>{payload_}, schema(), work));
                perf_tests::do_not_optimize(*last);
            }
            perf_tests::stop_measuring_time();
            require(
              last && last->has_value() && **last == units_,
              "control preflight changed its counted work");
            co_return 32;
        } else if constexpr (Op == operation::preflight_parse) {
            input_->push_checkpoint().value();
            barrier();
            perf_tests::start_measuring_time();
            const auto cost = detail::bound_control_parse(
                                schema(),
                                byte_count{payload_.size()},
                                work.policy(),
                                capacity_bound)
                                .value();
            static_cast<void>(codec::detail::consume_decode_budget(
                                work.policy(),
                                memory_,
                                cost.input_copy,
                                cost.generated_peak,
                                {},
                                0)
                                .value());
            seastar::temporary_buffer<char> wire{payload_.size()};
            for (std::size_t at = 0; at < wire.size();) {
                const auto n = std::min<std::size_t>(256, wire.size() - at);
                (co_await work.admit(byte_count{2U * n}, item_count{n}))
                  .value();
                work.poll().value();
                input_->read_to(std::span<char>{wire.get_write() + at, n})
                  .value();
                at += n;
            }
            const auto count = (co_await detail::preflight_control(
                                  std::span<const char>{
                                    wire.get(), wire.size()},
                                  schema(),
                                  work))
                                 .value();
            (co_await work.admit(byte_count{wire.size()}, count)).value();
            work.poll().value();
            auto message = std::make_unique<Native>();
            bool parsed = false;
            {
                google::protobuf::io::CodedInputStream stream{
                  reinterpret_cast<const std::uint8_t*>(wire.get()),
                  static_cast<int>(wire.size())};
                stream.SetTotalBytesLimit(static_cast<int>(wire.size()));
                stream.SetRecursionLimit(7);
                parsed = message->ParseFromCodedStream(&stream)
                         && stream.ConsumedEntireMessage()
                         && stream.CurrentPosition()
                              == static_cast<int>(wire.size());
            }
            perf_tests::do_not_optimize(*message);
            perf_tests::stop_measuring_time();
            require(parsed && count == units_, "bounded native parse failed");
            require(
              testing::matches_control(*message, value_->data()),
              "native parse changed control fields");
            perf_tests::start_measuring_time();
            co_await work.drain_inline(byte_count{wire.size()}, count);
            message.reset();
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            wire = {};
            perf_tests::stop_measuring_time();
            input_->rollback().value();
        } else if constexpr (Op == operation::conversion) {
            std::optional<control_data> result;
            const auto& source = static_cast<const Native&>(*reference_);
            barrier();
            perf_tests::start_measuring_time();
            const auto storage = detail::control_conversion_storage(
                                   source, {}, memory_, work.policy(), {})
                                   .value();
            require(
              native_cost_.generated_peak.value()
                  + native_cost_.input_copy.value() + storage.value()
                <= 1048576,
              "conversion exceeded aggregate bound");
            const auto native_memory = codec::detail::consume_decode_budget(
                                         work.policy(),
                                         memory_,
                                         native_cost_.input_copy,
                                         native_cost_.generated_peak,
                                         {},
                                         0)
                                         .value();
            static_cast<void>(
              codec::detail::consume_decode_budget(
                work.policy(), native_memory, byte_count{}, storage, {}, 0)
                .value());
            result.emplace();
            (co_await detail::copy_control_fields(
               source, *result, work, codec::error{errc::success}))
              .value();
            work.poll().value();
            perf_tests::do_not_optimize(*result);
            perf_tests::stop_measuring_time();
            require(
              testing::matches_control(*reference_, *result),
              "conversion changed values or presence");
            perf_tests::start_measuring_time();
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            result.reset();
            perf_tests::stop_measuring_time();
        } else if constexpr (Op == operation::serialize) {
            barrier();
            perf_tests::start_measuring_time();
            auto result = co_await encode_control(
              *value_, work, {}, frame::parent_budget, capacity_bound);
            perf_tests::do_not_optimize(result);
            perf_tests::stop_measuring_time();
            require(result.has_value(), "control serialization failed");
            auto observed = testing::probe_control(
              fixture::flatten(*result), Kind);
            require(
              observed && testing::control_unknowns_empty(*observed)
                && testing::matches_control(*observed, value_->data()),
              "serialized values changed");
            perf_tests::start_measuring_time();
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            result = codec::failure(codec::error{errc::success});
            perf_tests::stop_measuring_time();
        } else {
            auto& input = Op == operation::decode ? *input_ : *framed_;
            const auto memory = Op == operation::decode ? memory_
                                                        : frame_memory_;
            input.push_checkpoint().value();
            barrier();
            perf_tests::start_measuring_time();
            if constexpr (Op == operation::decode) {
                auto result = co_await decode_control(
                  input, Kind, {}, memory, work);
                perf_tests::do_not_optimize(result);
                perf_tests::stop_measuring_time();
                require(
                  result && input.at_end()
                    && testing::matches_control(
                      *reference_, result->value.data()),
                  "control decode changed fields or extent");
                perf_tests::start_measuring_time();
                co_await work.drain_inline(
                  work.byte_quantum(), work.item_quantum());
                result = codec::failure(codec::error{errc::success});
            } else {
                auto result = co_await decode_control_frame(
                  input, Kind, {}, frame::bounds, memory, work);
                perf_tests::do_not_optimize(result);
                perf_tests::stop_measuring_time();
                require(
                  result && input.at_end()
                    && std::holds_alternative<decoded_control_frame>(*result),
                  "framed control did not consume exactly one frame");
                require(
                  testing::matches_control(
                    *reference_,
                    std::get<decoded_control_frame>(*result).value.data()),
                  "framed control changed fields");
                perf_tests::start_measuring_time();
                co_await work.drain_inline(
                  work.byte_quantum(), work.item_quantum());
                result = codec::failure(codec::error{errc::success});
            }
            perf_tests::stop_measuring_time();
            input.rollback().value();
        }
        co_return 1;
    }

private:
    static auto schema() { return detail::schema_for(Kind)->message; }
    static void barrier() {
        // NOLINTNEXTLINE(portability-no-assembler)
        asm volatile("" : : : "memory");
    }
    seastar::future<> initialize() {
        if (input_) co_return;
        codec::bench::qualify_allocator();
        payload_ = testing::control_payload(Kind, Shape);
        reference_ = testing::probe_control(payload_, Kind);
        require(
          reference_ != nullptr,
          "independent control fixture validation failed");
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        units_ = (co_await detail::preflight_control(
                    std::span<const char>{payload_}, schema(), work))
                   .value();
        input_.emplace(fixture::fragmented(payload_, Width));
        const auto input_cost = input_->allocation_cost(capacity_bound).value();
        memory_ = codec::reserve_decode_input(*input_, work.policy(), memory_)
                    .value();
        native_cost_ = detail::bound_control_parse(
                         schema(),
                         byte_count{payload_.size()},
                         work.policy(),
                         capacity_bound)
                         .value();
        input_->push_checkpoint().value();
        auto decoded = co_await decode_control(
          *input_, Kind, {}, memory_, work);
        require(
          decoded
            && testing::matches_control(*reference_, decoded->value.data()),
          "benchmark fixture differs from native oracle");
        value_.emplace(std::move(decoded->value));
        input_->rollback().value();
        auto metadata = frame::metadata;
        metadata.kind = Kind;
        metadata.stream = model::transport_stream_id{};
        auto wire = frame::header(
          static_cast<std::uint32_t>(payload_.size()),
          frame::crc32c(payload_),
          {},
          metadata);
        wire.reserve(wire.size() + payload_.size());
        wire += payload_;
        framed_.emplace(fixture::fragmented(wire, Width));
        frame_memory_ = codec::reserve_decode_input(
                          *framed_, work.policy(), frame_memory_)
                          .value();
        fmt::print(
          "control_fixture kind={} shape={} width={} payload_bytes={} "
          "fields={} fragments={} backing_bound={} input_metadata_bound={} "
          "native_bound={} normalization=message cleanup=timed "
          "persistent_input=true\n",
          static_cast<unsigned>(Kind),
          Shape,
          Width,
          payload_.size(),
          units_.value(),
          input_cost.fragments.value(),
          input_cost.backing.value(),
          input_cost.descriptors.value() + input_cost.share_controls.value(),
          native_cost_.generated_peak.value());
        fmt::print(
          "control_scopes preflight=borrowed_span "
          "preflight_parse=owned_copy+preflight+fresh_parse "
          "conversion=immutable_native_to_owned decode=complete_payload "
          "serialize=validated_owner_to_frozen_bytes "
          "framed_decode=complete_frame copy_bytes_per_parse={} "
          "native_fixture_not_reparsed_for_conversion=true\n",
          payload_.size());
    }
    std::string payload_;
    std::unique_ptr<google::protobuf::Message> reference_;
    std::optional<control> value_;
    std::optional<bytes::fragmented_buffer_parser> input_, framed_;
    item_count units_;
    detail::control_parse_cost native_cost_;
    codec::decode_budget memory_{
      frame::parent_budget, byte_count{1048576}, capacity_bound};
    codec::decode_budget frame_memory_{
      frame::parent_budget, byte_count{1048576}, capacity_bound};
};

using control_error = measurement<frame_kind::error, 0, 7>;
using control_request = measurement<frame_kind::handshake_request, 0, 65536>;
using control_request_frag7 = measurement<frame_kind::handshake_request, 0, 7>;
using control_response = measurement<frame_kind::handshake_response, 2, 67>;
using control_redirect = measurement<frame_kind::redirect, 1, 67>;
using control_max = measurement<frame_kind::handshake_request, 3, 32704>;
using control_max_frag512 = measurement<frame_kind::handshake_request, 3, 512>;
#define CONTROL_BENCHMARKS(group)                                              \
    PERF_TEST_F(group, preflight) { return measure<operation::preflight>(); }  \
    PERF_TEST_F(group, preflight_parse) {                                      \
        return measure<operation::preflight_parse>();                          \
    }                                                                          \
    PERF_TEST_F(group, conversion) {                                           \
        return measure<operation::conversion>();                               \
    }                                                                          \
    PERF_TEST_F(group, decode) { return measure<operation::decode>(); }        \
    PERF_TEST_F(group, serialize) { return measure<operation::serialize>(); }  \
    PERF_TEST_F(group, framed_decode) {                                        \
        return measure<operation::framed_decode>();                            \
    }
// Compare each exact case against itself in a saved baseline binary using the
// paired driver. Different operation names intentionally measure different
// work.
CONTROL_BENCHMARKS(control_error)
CONTROL_BENCHMARKS(control_request)
CONTROL_BENCHMARKS(control_request_frag7)
CONTROL_BENCHMARKS(control_response)
CONTROL_BENCHMARKS(control_redirect)
CONTROL_BENCHMARKS(control_max)
CONTROL_BENCHMARKS(control_max_frag512)
#undef CONTROL_BENCHMARKS
} // namespace
} // namespace kwaque::protocol::bench
