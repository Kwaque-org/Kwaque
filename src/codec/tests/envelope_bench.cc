#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/cooperative.h"
#include "src/codec/crc32c.h"
#include "src/codec/envelope.h"
#include "src/codec/error.h"
#include "src/codec/format_registry.h"
#include "src/codec/integer.h"
#include "src/codec/limits.h"
#include "src/codec/tests/envelope_bench_fixture.h"
#include "src/codec/tests/qualification_profile.h"
#include "src/codec/transaction.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/deleter.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/testing/perf_tests.hh>

#include <crc32c/crc32c.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <malloc.h>
#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace kwaque::codec::bench {
namespace {

constexpr std::size_t leaf_iterations = 64;
constexpr std::uint64_t object_id = 0x0102030405060708ULL;
constexpr std::uint64_t object_generation = 0x1112131415161718ULL;
constexpr envelope_expected_body expected{object_id, object_generation};
constexpr field_context coordinates{.origin = 512, .family = 1, .field = 3};
constexpr envelope_extent_limits extent_limits{
  byte_count{16U * 1024U * 1024U}, byte_count{32U * 1024U * 1024U}};
constexpr byte_count frame_reservation = testing::execution_reservation;
constexpr byte_count parent_remainder{
  64U * 1024U * 1024U - frame_reservation.value()};

[[gnu::always_inline]] inline void clobber_memory() {
    // Symmetric compiler barrier; no machine instruction is emitted.
    // NOLINTNEXTLINE(portability-no-assembler)
    asm volatile("" : : : "memory");
}

[[gnu::always_inline]] inline void
opaque_fields(envelope_prefix_fields& fields) {
    // An input/output constraint prevents constant scalar arguments from making
    // either writer disappear into a hoisted constant result.
    // NOLINTNEXTLINE(portability-no-assembler)
    asm volatile("" : "+m"(fields) : : "memory");
}

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

// Native pooled cells have a conservative two-times power-of-two bound.
// Above the 16-KiB pool ceiling the native buddy allocator serves a
// power-of-two span. Using that actual distinction leaves room for a
// maximum-size body and its header. Native-only capability checks precede every
// benchmark run, and setup additionally checks the allocation bases used by
// these fixtures.
byte_count native_capacity_bound(byte_count request) noexcept {
    if (request.value() == 0) {
        return {};
    }
    if (request.value() > (std::uint64_t{1} << 62U)) {
        return byte_count{std::numeric_limits<std::uint64_t>::max()};
    }
    const auto rounded = std::bit_ceil(
      std::max(request.value(), std::uint64_t{16}));
    return byte_count{request.value() <= 16384 ? 2U * rounded : rounded};
}

void verify_native_allocation(void* base, std::size_t requested) {
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    static_cast<void>(base);
    static_cast<void>(requested);
    throw std::runtime_error(
      "envelope benchmark requires the native allocator");
#else
    const auto served = ::malloc_usable_size(base);
    require(
      served >= requested,
      "native fixture allocation is shorter than requested");
    require(
      served <= native_capacity_bound(byte_count{requested}).value(),
      "native fixture allocation exceeds its reserved capacity");
#endif
}

void qualify_allocator_and_warm_crc() {
    for (const auto size : std::array<std::size_t, 11>{
           16,
           32,
           bytes::fragmented_buffer::fragment_descriptor_size(),
           sizeof(seastar::free_deleter_impl),
           128,
           1024,
           8192,
           16384,
           16385,
           32768,
           65536}) {
        seastar::temporary_buffer<char> allocation{size};
        verify_native_allocation(allocation.get_write(), size);
    }
    const std::array<char, 65> input{};
    codec::crc32c selected;
    selected.extend(std::span<const char>{input});
    const auto comparison = ::crc32c::Extend(
      0, reinterpret_cast<const std::uint8_t*>(input.data()), input.size());
    require(selected.value() == comparison, "CRC setup disagreement");
}

std::uint32_t fixture_crc(std::span<const char> bytes) noexcept {
    std::uint32_t value = 0xffffffffU;
    for (const char byte : bytes) {
        value ^= static_cast<unsigned char>(byte);
        for (unsigned bit = 0; bit < 8; ++bit) {
            value = (value >> 1U) ^ ((value & 1U) ? 0x82f63b78U : 0U);
        }
    }
    return value ^ 0xffffffffU;
}

void put_integer(
  std::span<char> output,
  std::size_t offset,
  std::uint64_t value,
  std::size_t width) {
    for (std::size_t index = 0; index < width; ++index) {
        output[offset + index] = std::bit_cast<char>(
          static_cast<std::uint8_t>(value & 0xffU));
        value >>= 8U;
    }
}

byte_count sum_cost(const bytes::buffer_allocation_cost& cost) {
    return cost.backing.checked_add(cost.descriptors)
      .value()
      .checked_add(cost.share_controls)
      .value();
}

seastar::future<>
verify_source_storage(const bytes::fragmented_buffer& source) {
    // These newly built, untrimmed-front owners expose allocation bases. Later
    // input/output slices are never passed to malloc_usable_size.
    for (std::size_t index = 0; index < source.fragment_count(); ++index) {
        const auto fragment = source.fragment_at(index).value();
        const auto cost
          = source.allocation_cost(index, 1, native_capacity_bound).value();
        const auto served = ::malloc_usable_size(
          const_cast<char*>(fragment.data()));
        require(
          served >= fragment.size() && served <= cost.backing.value(),
          "source backing exceeds its native reservation");
        if (index % 64U == 0) {
            co_await seastar::coroutine::maybe_yield();
        }
    }
    const auto request = 2U * source.fragment_count()
                         * bytes::fragmented_buffer::fragment_descriptor_size();
    if (request != 0) {
        seastar::temporary_buffer<char> descriptors{request};
        verify_native_allocation(descriptors.get_write(), request);
    }
}

template<
  std::size_t BodyBytes,
  std::size_t Extensions,
  std::size_t FragmentWidth>
class envelope_fixture {
    static_assert(
      BodyBytes == 64 || BodyBytes == 32768 || BodyBytes == 16777216);
    static_assert(Extensions <= 64);
    static constexpr std::size_t header_size = 32U + 16U * Extensions;
    static constexpr std::size_t encoded_size = header_size + BodyBytes;
    static_assert(FragmentWidth != 0 || encoded_size <= 65536);
    static_assert(
      FragmentWidth == 0
      || 1U + (encoded_size - 1U) / FragmentWidth
           <= bytes::max_buffer_fragments);
    static constexpr std::uint32_t body_checksum = BodyBytes == 64 ? 0x72ee9c00U
                                                   : BodyBytes == 32768
                                                     ? 0x209e78afU
                                                     : 0x4d757325U;

    char body_byte(std::size_t offset) const noexcept {
        if (offset < 8) {
            return std::bit_cast<char>(
              static_cast<std::uint8_t>(object_id >> (8U * offset)));
        }
        if (offset < 16) {
            return std::bit_cast<char>(static_cast<std::uint8_t>(
              object_generation >> (8U * (offset - 8U))));
        }
        if (offset < 20) {
            return std::bit_cast<char>(static_cast<std::uint8_t>(
              (BodyBytes - 20U) >> (8U * (offset - 16U))));
        }
        return std::bit_cast<char>(
          static_cast<std::uint8_t>(((offset - 20U) * 17U + 3U) & 0xffU));
    }

    char source_byte(std::size_t offset, bool envelope) const noexcept {
        if (envelope && offset < header_size) {
            return header_[offset];
        }
        return body_byte(envelope ? offset - header_size : offset);
    }

    void prepare_header() {
        std::ranges::copy(std::string_view{"KQBF"}, header_.begin());
        put_integer(header_, 4, 1, 2);
        put_integer(header_, 6, 1, 2);
        put_integer(header_, 8, 1, 2);
        put_integer(header_, 10, header_size, 2);
        put_integer(header_, 12, BodyBytes, 4);
        put_integer(header_, 24, body_checksum, 4);
        for (std::size_t index = 0; index < Extensions; ++index) {
            const auto start = 32U + 16U * index;
            put_integer(header_, start, index + 1U, 2);
            put_integer(header_, start + 4U, 8, 4);
            put_integer(header_, start + 8U, 0x12340000U + index, 8);
        }
        put_integer(header_, 28, fixture_crc(header_), 4);
    }

    std::uint32_t stored_header_checksum() const noexcept {
        std::uint32_t value = 0;
        for (unsigned byte = 0; byte < 4; ++byte) {
            value |= static_cast<std::uint32_t>(
                       static_cast<unsigned char>(header_[28U + byte]))
                     << (byte * 8U);
        }
        return value;
    }

    seastar::future<bytes::fragmented_buffer> make_source(bool envelope) {
        const auto total = envelope ? encoded_size : BodyBytes;
        const auto width = FragmentWidth == 0 ? total : FragmentWidth;
        if (total <= 65536) {
            std::vector<seastar::temporary_buffer<char>> fragments;
            fragments.reserve(1U + (total - 1U) / width);
            for (std::size_t offset = 0; offset < total;) {
                const auto size = std::min(width, total - offset);
                seastar::temporary_buffer<char> fragment{size};
                verify_native_allocation(fragment.get_write(), size);
                for (std::size_t index = 0; index < size; ++index) {
                    fragment.get_write()[index] = source_byte(
                      offset + index, envelope);
                }
                fragments.push_back(std::move(fragment));
                offset += size;
                co_await seastar::coroutine::maybe_yield();
            }
            co_return bytes::fragmented_buffer::copy_from_fragments(fragments)
              .value();
        }
        // Maximum objects use bounded native power-of-two fragments. Neither
        // fixture preparation nor an expected-value string linearizes them.
        const auto chunk = std::min<std::size_t>(width, 65536);
        require(
          std::has_single_bit(chunk),
          "large fixture needs power-of-two chunks");
        bytes::fragmented_buffer_builder_config config;
        config.initial_fragment_bytes = byte_count{chunk};
        config.max_fragment_bytes = byte_count{chunk};
        config.max_total_bytes = byte_count{total};
        config.max_retained_bytes = byte_count{total + chunk};
        bytes::fragmented_buffer_builder builder{config};
        std::array<char, 65536> block{};
        for (std::size_t offset = 0; offset < total;) {
            const auto size = std::min(chunk, total - offset);
            for (std::size_t index = 0; index < size; ++index) {
                block[index] = source_byte(offset + index, envelope);
            }
            co_await seastar::coroutine::maybe_yield();
            require(
              builder.append(std::span<const char>{block}.first(size))
                .has_value(),
              "large fixture append rejected");
            offset += size;
            co_await seastar::coroutine::maybe_yield();
        }
        co_return builder.finish().value();
    }

    decode_budget
    memory_for(const bytes::fragmented_buffer_parser& input) const {
        // This valid input aliases the complete encoded cache. Its backing and
        // share controls are reserved with input; only the original descriptor
        // allocation and the independently owned body cache are additional.
        const auto other
          = sum_cost(body_cost_).checked_add(encoded_cost_.descriptors).value();
        const auto remaining = parent_remainder.checked_sub(other).value();
        return reserve_decode_input(
                 input,
                 limits::defaults(),
                 decode_budget{
                   remaining, byte_count{1024U * 1024U}, native_capacity_bound},
                 coordinates,
                 input_boundary::complete)
          .value();
    }

    operation_usage encode_other_live() const {
        return operation_usage{
          .retained_input = encoded_cost_.backing,
          .payload_bookkeeping = encoded_cost_.descriptors
                                   .checked_add(encoded_cost_.share_controls)
                                   .value()
                                   .checked_add(body_cost_.descriptors)
                                   .value()};
    }

    template<bool Candidate>
    seastar::future<result<envelope_decoded_body>> invoke_decode(
      bytes::fragmented_buffer_parser& input,
      cooperative_work& work,
      decode_budget memory,
      envelope_expected_body target = expected) {
        if constexpr (Candidate) {
            return codec_owned_decode(
              input,
              format_family::submitted_batch,
              extent_limits,
              memory,
              work,
              target,
              coordinates,
              input_boundary::complete);
        } else {
            return checked_owned_decode(
              input,
              format_family::submitted_batch,
              extent_limits,
              memory,
              work,
              target,
              coordinates,
              input_boundary::complete);
        }
    }

    seastar::future<> validate_value(const envelope_decoded_body& value) const {
        require(
          value.object == object_id && value.generation == object_generation,
          "decoded envelope context mismatch");
        require(
          value.payload.size() == byte_count{BodyBytes - 20U},
          "decoded envelope payload size mismatch");
        std::size_t position = 20;
        for (const auto fragment : value.payload) {
            bool equal = true;
            for (const char byte : fragment.bytes()) {
                equal = equal && byte == body_byte(position);
                ++position;
            }
            require(equal, "decoded envelope payload mismatch");
            co_await seastar::coroutine::maybe_yield();
        }
        require(
          position == BodyBytes, "decoded envelope payload extent mismatch");
    }

    seastar::future<>
    validate_encoding(const bytes::fragmented_buffer& value) const {
        require(
          value.size() == byte_count{encoded_size},
          "encoded envelope size mismatch");
        std::size_t position = 0;
        for (const auto fragment : value) {
            bool equal = true;
            for (const char byte : fragment.bytes()) {
                equal = equal && byte == source_byte(position, true);
                ++position;
            }
            require(equal, "encoded envelope bytes mismatch");
            co_await seastar::coroutine::maybe_yield();
        }
        require(position == encoded_size, "encoded envelope extent mismatch");
    }

    template<bool Candidate>
    seastar::future<> qualify_reader() {
        bytes::fragmented_buffer_parser input{encoded_.share()};
        seastar::abort_source abort;
        cooperative_work work{limits::defaults(), abort};
        auto memory = memory_for(input);
        auto good = co_await invoke_decode<Candidate>(input, work, memory);
        require(
          good.has_value() && input.at_end() && input.checkpoint_depth() == 0,
          "complete reader qualification failed");
        co_await validate_value(*good);
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        good = codec::failure(error{errc::closed});
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        input = bytes::fragmented_buffer_parser{encoded_.share()};
        memory = memory_for(input);
        auto wrong = co_await invoke_decode<Candidate>(
          input, work, memory, {object_id, object_generation + 1U});
        require(
          !wrong && wrong.error().code() == errc::wrong_context
            && input.bytes_consumed() == byte_count{}
            && input.checkpoint_depth() == 0,
          "reader context rejection or rollback mismatch");
        auto damaged_header = header_;
        damaged_header[28] ^= 1;
        // These inputs do not retain the entire encoded cache. Reserve both
        // preparation owners independently before admitting their input; this
        // includes a last fragment removed by the truncation case below.
        const auto qualification_remainder
          = parent_remainder
              .checked_sub(sum_cost(body_cost_)
                             .checked_add(sum_cost(encoded_cost_))
                             .value())
              .value();
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        input = bytes::fragmented_buffer_parser{
          bytes::fragmented_buffer::copy_of(
            std::span<const char>{damaged_header})
            .value()};
        memory = reserve_decode_input(
                   input,
                   limits::defaults(),
                   decode_budget{
                     qualification_remainder,
                     byte_count{1024U * 1024U},
                     native_capacity_bound},
                   coordinates,
                   input_boundary::complete)
                   .value();
        auto corrupt = co_await invoke_decode<Candidate>(input, work, memory);
        require(
          !corrupt && corrupt.error().code() == errc::corrupt_data
            && input.bytes_consumed() == byte_count{}
            && input.checkpoint_depth() == 0,
          "header CRC must precede the unavailable body");
        auto shortened = encoded_.share();
        require(
          shortened.trim_back(byte_count{1}).has_value(),
          "fixture trim failed");
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        input = bytes::fragmented_buffer_parser{std::move(shortened)};
        memory = reserve_decode_input(
                   input,
                   limits::defaults(),
                   decode_budget{
                     qualification_remainder,
                     byte_count{1024U * 1024U},
                     native_capacity_bound},
                   coordinates,
                   input_boundary::complete)
                   .value();
        auto short_result = co_await invoke_decode<Candidate>(
          input, work, memory);
        require(
          !short_result && short_result.error().code() == errc::malformed_data
            && input.bytes_consumed() == byte_count{}
            && input.checkpoint_depth() == 0,
          "complete-parent truncation or rollback mismatch");
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        input = bytes::fragmented_buffer_parser{encoded_.share()};
        memory = memory_for(input);
        abort.request_abort();
        auto cancelled = co_await invoke_decode<Candidate>(input, work, memory);
        require(
          !cancelled && cancelled.error().code() == errc::aborted
            && input.bytes_consumed() == byte_count{}
            && input.checkpoint_depth() == 0,
          "reader cancellation or rollback mismatch");
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        input = bytes::fragmented_buffer_parser{};
    }

    template<bool Candidate>
    seastar::future<> qualify_writer() {
        for (const bool cancelled : {false, true}) {
            auto input = body_.share();
            seastar::abort_source abort;
            cooperative_work work{limits::defaults(), abort};
            if (cancelled) {
                abort.request_abort();
            }
            auto written = co_await [&] {
                if constexpr (Candidate) {
                    return codec_owned_encode(
                      std::move(input),
                      format_family::submitted_batch,
                      work,
                      extent_limits,
                      encode_other_live(),
                      parent_remainder,
                      native_capacity_bound,
                      coordinates);
                } else {
                    return checked_owned_encode(
                      std::move(input),
                      format_family::submitted_batch,
                      work,
                      extent_limits,
                      encode_other_live(),
                      parent_remainder,
                      native_capacity_bound,
                      coordinates);
                }
            }();
            // NOLINTNEXTLINE(bugprone-use-after-move)
            require(input.empty(), "writer qualification retained the donor");
            if (cancelled) {
                require(
                  !written && written.error().code() == errc::aborted,
                  "writer qualification failed cancellation");
            } else {
                require(
                  written.has_value(), "writer qualification rejected fixture");
                co_await validate_encoding(*written);
                co_await work.drain_inline(
                  work.byte_quantum(), work.item_quantum());
                written = codec::failure(error{errc::closed});
            }
        }
    }

    seastar::future<> initialize() {
        if (initialized_) {
            co_return;
        }
        qualify_allocator_and_warm_crc();
        prepare_header();
        body_ = co_await make_source(false);
        encoded_ = co_await make_source(true);
        co_await verify_source_storage(body_);
        co_await verify_source_storage(encoded_);
        auto warm_body = body_.share();
        auto warm_encoded = encoded_.share();
        body_cost_ = body_.allocation_cost(native_capacity_bound).value();
        encoded_cost_ = encoded_.allocation_cost(native_capacity_bound).value();
        warm_body = bytes::fragmented_buffer{};
        warm_encoded = bytes::fragmented_buffer{};
        co_await qualify_reader<false>();
        co_await qualify_reader<true>();
        if constexpr (Extensions == 0) {
            co_await qualify_writer<false>();
            co_await qualify_writer<true>();
        }
        initialized_ = true;
    }

public:
    template<bool Candidate>
    seastar::future<std::size_t> prefix_read() {
        co_await initialize();
        bytes::fragmented_buffer_parser input{encoded_.share()};
        const auto policy = limits::defaults();
        perf_tests::start_measuring_time();
        for (std::size_t index = 0; index < leaf_iterations; ++index) {
            clobber_memory();
            if constexpr (Candidate) {
                reads_[index] = codec_prefix_read(
                  input,
                  policy,
                  extent_limits,
                  coordinates,
                  input_boundary::complete);
            } else {
                reads_[index] = checked_prefix_read(
                  input,
                  policy,
                  extent_limits,
                  coordinates,
                  input_boundary::complete);
            }
            perf_tests::do_not_optimize(reads_[index]);
        }
        perf_tests::stop_measuring_time();
        for (const auto& value : reads_) {
            require(
              value && value->header_bytes == header_size
                && value->body_bytes == BodyBytes
                && value->body_crc32c == body_checksum && value->family == 1
                && value->writer_version == 1
                && value->minimum_reader_version == 1
                && value->required_features == 0
                && value->header_crc32c == stored_header_checksum(),
              "prefix read mismatch");
        }
        require(
          input.bytes_consumed() == byte_count{}
            && input.checkpoint_depth() == 0,
          "prefix leaf changed parser state");
        co_return leaf_iterations;
    }

    template<bool Candidate>
    seastar::future<std::size_t> prefix_write() {
        static_assert(Extensions == 0);
        co_await initialize();
        const auto policy = limits::defaults();
        // Use the independently stored finalized header checksum, not a second
        // invocation of either prefix writer, for the fixed-field expectation.
        envelope_prefix_fields actual_fields{
          format_family::submitted_batch,
          byte_count{BodyBytes},
          body_checksum,
          stored_header_checksum()};
        perf_tests::start_measuring_time();
        for (std::size_t index = 0; index < leaf_iterations; ++index) {
            clobber_memory();
            opaque_fields(actual_fields);
            if constexpr (Candidate) {
                writes_[index] = codec_prefix_write(
                  actual_fields, policy, extent_limits, coordinates);
            } else {
                writes_[index] = checked_prefix_write(
                  actual_fields, policy, extent_limits, coordinates);
            }
            perf_tests::do_not_optimize(writes_[index]);
        }
        perf_tests::stop_measuring_time();
        for (const auto& value : writes_) {
            require(
              value && std::ranges::equal(*value, header_),
              "prefix write mismatch");
        }
        co_return leaf_iterations;
    }

    template<bool Candidate>
    seastar::future<std::size_t> extensions() {
        co_await initialize();
        bytes::fragmented_buffer_parser input{encoded_.share()};
        require(
          input.push_checkpoint().has_value(), "extension fixture mark failed");
        require(
          input.skip(byte_count{32}).has_value(),
          "extension fixture prefix missing");
        seastar::abort_source abort;
        cooperative_work work{limits::defaults(), abort};
        clobber_memory();
        perf_tests::start_measuring_time();
        auto result = co_await [&] {
            if constexpr (Candidate) {
                return codec_extensions(
                  input,
                  byte_count{16U * Extensions},
                  byte_count{32},
                  work,
                  coordinates);
            } else {
                return checked_extensions(
                  input,
                  byte_count{16U * Extensions},
                  byte_count{32},
                  work,
                  coordinates);
            }
        }();
        perf_tests::do_not_optimize(result);
        perf_tests::stop_measuring_time();
        require(
          result && result->value() == Extensions
            && input.bytes_consumed() == byte_count{header_size}
            && input.checkpoint_depth() == 1,
          "extension scan mismatch");
        require(
          input.rollback().has_value(), "extension fixture rollback failed");
        co_return std::size_t{1};
    }

    template<bool Candidate>
    seastar::future<std::size_t> decode() {
        co_await initialize();
        bytes::fragmented_buffer_parser input{encoded_.share()};
        seastar::abort_source abort;
        cooperative_work work{limits::defaults(), abort};
        const auto memory = memory_for(input);
        clobber_memory();
        perf_tests::start_measuring_time();
        auto result = co_await invoke_decode<Candidate>(input, work, memory);
        perf_tests::do_not_optimize(result);
        perf_tests::stop_measuring_time();
        require(
          result && input.at_end() && input.checkpoint_depth() == 0,
          "full decode rejected or failed exact consumption");
        co_await validate_value(*result);
        // Both measurements include disposal of the returned payload and input.
        // Value checks are outside both segments; the native harness
        // accumulates time, allocations and tasks across the operation and
        // cleanup segments.
        clobber_memory();
        perf_tests::start_measuring_time();
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        result = codec::failure(error{errc::success});
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        input = bytes::fragmented_buffer_parser{};
        perf_tests::stop_measuring_time();
        co_return std::size_t{1};
    }

    template<bool Candidate>
    seastar::future<std::size_t> encode() {
        static_assert(Extensions == 0);
        co_await initialize();
        auto input = body_.share();
        seastar::abort_source abort;
        cooperative_work work{limits::defaults(), abort};
        const auto other_live = encode_other_live();
        clobber_memory();
        perf_tests::start_measuring_time();
        auto result = co_await [&] {
            if constexpr (Candidate) {
                return codec_owned_encode(
                  std::move(input),
                  format_family::submitted_batch,
                  work,
                  extent_limits,
                  other_live,
                  parent_remainder,
                  native_capacity_bound,
                  coordinates);
            } else {
                return checked_owned_encode(
                  std::move(input),
                  format_family::submitted_batch,
                  work,
                  extent_limits,
                  other_live,
                  parent_remainder,
                  native_capacity_bound,
                  coordinates);
            }
        }();
        perf_tests::do_not_optimize(result);
        perf_tests::stop_measuring_time();
        require(result.has_value(), "full encode rejected fixture");
        // NOLINTNEXTLINE(bugprone-use-after-move)
        require(input.empty(), "full encode did not consume donor");
        co_await validate_encoding(*result);
        clobber_memory();
        perf_tests::start_measuring_time();
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        result = codec::failure(error{errc::success});
        perf_tests::stop_measuring_time();
        co_return std::size_t{1};
    }

private:
    bool initialized_{false};
    std::array<char, header_size> header_{};
    bytes::fragmented_buffer body_;
    bytes::fragmented_buffer encoded_;
    bytes::buffer_allocation_cost body_cost_{};
    bytes::buffer_allocation_cost encoded_cost_{};
    std::array<result<unverified_envelope_prefix>, leaf_iterations> reads_{};
    std::array<result<encoded_envelope_prefix>, leaf_iterations> writes_{};
};

using envelope_prefix_contiguous = envelope_fixture<64, 0, 0>;
PERF_TEST_F(envelope_prefix_contiguous, checked_read) {
    return prefix_read<false>();
}
PERF_TEST_F(envelope_prefix_contiguous, codec_read) {
    return prefix_read<true>();
}
PERF_TEST_F(envelope_prefix_contiguous, checked_write) {
    return prefix_write<false>();
}
PERF_TEST_F(envelope_prefix_contiguous, codec_write) {
    return prefix_write<true>();
}

using envelope_prefix_frag1 = envelope_fixture<64, 0, 1>;
PERF_TEST_F(envelope_prefix_frag1, checked_read) {
    return prefix_read<false>();
}
PERF_TEST_F(envelope_prefix_frag1, codec_read) { return prefix_read<true>(); }

using envelope_tlv0_contiguous = envelope_fixture<64, 0, 0>;
PERF_TEST_F(envelope_tlv0_contiguous, checked_scan) {
    return extensions<false>();
}
PERF_TEST_F(envelope_tlv0_contiguous, codec_scan) { return extensions<true>(); }

using envelope_tlv0_frag7 = envelope_fixture<64, 0, 7>;
PERF_TEST_F(envelope_tlv0_frag7, checked_scan) { return extensions<false>(); }
PERF_TEST_F(envelope_tlv0_frag7, codec_scan) { return extensions<true>(); }

using envelope_tlv1_contiguous = envelope_fixture<64, 1, 0>;
PERF_TEST_F(envelope_tlv1_contiguous, checked_scan) {
    return extensions<false>();
}
PERF_TEST_F(envelope_tlv1_contiguous, codec_scan) { return extensions<true>(); }

using envelope_tlv1_frag7 = envelope_fixture<64, 1, 7>;
PERF_TEST_F(envelope_tlv1_frag7, checked_scan) { return extensions<false>(); }
PERF_TEST_F(envelope_tlv1_frag7, codec_scan) { return extensions<true>(); }

using envelope_tlv64_contiguous = envelope_fixture<64, 64, 0>;
PERF_TEST_F(envelope_tlv64_contiguous, checked_scan) {
    return extensions<false>();
}
PERF_TEST_F(envelope_tlv64_contiguous, codec_scan) {
    return extensions<true>();
}

using envelope_tlv64_frag7 = envelope_fixture<64, 64, 7>;
PERF_TEST_F(envelope_tlv64_frag7, checked_scan) { return extensions<false>(); }
PERF_TEST_F(envelope_tlv64_frag7, codec_scan) { return extensions<true>(); }

using envelope_body64_ext0_contiguous = envelope_fixture<64, 0, 0>;
PERF_TEST_F(envelope_body64_ext0_contiguous, checked_decode) {
    return decode<false>();
}
PERF_TEST_F(envelope_body64_ext0_contiguous, codec_decode) {
    return decode<true>();
}
PERF_TEST_F(envelope_body64_ext0_contiguous, checked_encode) {
    return encode<false>();
}
PERF_TEST_F(envelope_body64_ext0_contiguous, codec_encode) {
    return encode<true>();
}

using envelope_body64_ext0_frag67 = envelope_fixture<64, 0, 67>;
PERF_TEST_F(envelope_body64_ext0_frag67, checked_decode) {
    return decode<false>();
}
PERF_TEST_F(envelope_body64_ext0_frag67, codec_decode) {
    return decode<true>();
}
PERF_TEST_F(envelope_body64_ext0_frag67, checked_encode) {
    return encode<false>();
}
PERF_TEST_F(envelope_body64_ext0_frag67, codec_encode) {
    return encode<true>();
}

using envelope_body64_ext1_contiguous = envelope_fixture<64, 1, 0>;
PERF_TEST_F(envelope_body64_ext1_contiguous, checked_decode) {
    return decode<false>();
}
PERF_TEST_F(envelope_body64_ext1_contiguous, codec_decode) {
    return decode<true>();
}

using envelope_body64_ext1_frag67 = envelope_fixture<64, 1, 67>;
PERF_TEST_F(envelope_body64_ext1_frag67, checked_decode) {
    return decode<false>();
}
PERF_TEST_F(envelope_body64_ext1_frag67, codec_decode) {
    return decode<true>();
}

using envelope_body64_ext64_contiguous = envelope_fixture<64, 64, 0>;
PERF_TEST_F(envelope_body64_ext64_contiguous, checked_decode) {
    return decode<false>();
}
PERF_TEST_F(envelope_body64_ext64_contiguous, codec_decode) {
    return decode<true>();
}

using envelope_body64_ext64_frag67 = envelope_fixture<64, 64, 67>;
PERF_TEST_F(envelope_body64_ext64_frag67, checked_decode) {
    return decode<false>();
}
PERF_TEST_F(envelope_body64_ext64_frag67, codec_decode) {
    return decode<true>();
}

using envelope_body32768_ext0_contiguous = envelope_fixture<32768, 0, 0>;
PERF_TEST_F(envelope_body32768_ext0_contiguous, checked_decode) {
    return decode<false>();
}
PERF_TEST_F(envelope_body32768_ext0_contiguous, codec_decode) {
    return decode<true>();
}
PERF_TEST_F(envelope_body32768_ext0_contiguous, checked_encode) {
    return encode<false>();
}
PERF_TEST_F(envelope_body32768_ext0_contiguous, codec_encode) {
    return encode<true>();
}

using envelope_body32768_ext0_frag67 = envelope_fixture<32768, 0, 67>;
PERF_TEST_F(envelope_body32768_ext0_frag67, checked_decode) {
    return decode<false>();
}
PERF_TEST_F(envelope_body32768_ext0_frag67, codec_decode) {
    return decode<true>();
}
PERF_TEST_F(envelope_body32768_ext0_frag67, checked_encode) {
    return encode<false>();
}
PERF_TEST_F(envelope_body32768_ext0_frag67, codec_encode) {
    return encode<true>();
}

using envelope_body32768_ext1_contiguous = envelope_fixture<32768, 1, 0>;
PERF_TEST_F(envelope_body32768_ext1_contiguous, checked_decode) {
    return decode<false>();
}
PERF_TEST_F(envelope_body32768_ext1_contiguous, codec_decode) {
    return decode<true>();
}

using envelope_body32768_ext1_frag67 = envelope_fixture<32768, 1, 67>;
PERF_TEST_F(envelope_body32768_ext1_frag67, checked_decode) {
    return decode<false>();
}
PERF_TEST_F(envelope_body32768_ext1_frag67, codec_decode) {
    return decode<true>();
}

using envelope_body32768_ext64_contiguous = envelope_fixture<32768, 64, 0>;
PERF_TEST_F(envelope_body32768_ext64_contiguous, checked_decode) {
    return decode<false>();
}
PERF_TEST_F(envelope_body32768_ext64_contiguous, codec_decode) {
    return decode<true>();
}

using envelope_body32768_ext64_frag67 = envelope_fixture<32768, 64, 67>;
PERF_TEST_F(envelope_body32768_ext64_frag67, checked_decode) {
    return decode<false>();
}
PERF_TEST_F(envelope_body32768_ext64_frag67, codec_decode) {
    return decode<true>();
}

using envelope_body16777216_ext0_frag32768
  = envelope_fixture<16777216, 0, 32768>;
PERF_TEST_F(envelope_body16777216_ext0_frag32768, checked_decode) {
    return decode<false>();
}
PERF_TEST_F(envelope_body16777216_ext0_frag32768, codec_decode) {
    return decode<true>();
}

using envelope_body16777216_ext1_frag32768
  = envelope_fixture<16777216, 1, 32768>;
PERF_TEST_F(envelope_body16777216_ext1_frag32768, checked_decode) {
    return decode<false>();
}
PERF_TEST_F(envelope_body16777216_ext1_frag32768, codec_decode) {
    return decode<true>();
}

using envelope_body16777216_ext64_frag32768
  = envelope_fixture<16777216, 64, 32768>;
PERF_TEST_F(envelope_body16777216_ext64_frag32768, checked_decode) {
    return decode<false>();
}
PERF_TEST_F(envelope_body16777216_ext64_frag32768, codec_decode) {
    return decode<true>();
}

using envelope_body16777216_ext0_frag65536
  = envelope_fixture<16777216, 0, 65536>;
PERF_TEST_F(envelope_body16777216_ext0_frag65536, checked_encode) {
    return encode<false>();
}
PERF_TEST_F(envelope_body16777216_ext0_frag65536, codec_encode) {
    return encode<true>();
}

} // namespace
} // namespace kwaque::codec::bench
