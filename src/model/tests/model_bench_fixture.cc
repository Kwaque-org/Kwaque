#include "src/model/tests/model_bench_fixture.h"

#include "src/bytes/fragmented_buffer_builder.h"
#include "src/model/batch_rewrite.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/deleter.hh>
#include <seastar/core/temporary_buffer.hh>

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <malloc.h>
#include <stdexcept>
#include <utility>

namespace kwaque::model::bench {
using bytes::fragmented_buffer;
void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}
byte_count capacity_bound(byte_count request) noexcept {
    if (request.value() == 0) return {};
    if (request.value() > (std::uint64_t{1} << 62U))
        return byte_count{UINT64_MAX};
    const auto rounded = std::bit_ceil(
      std::max(request.value(), std::uint64_t{16}));
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    return byte_count{2U * rounded};
#else
    return byte_count{request.value() <= 16384 ? 2U * rounded : rounded};
#endif
}
namespace {
template<typename Id>
Id object(std::uint8_t first) {
    std::array<std::uint8_t, 16> value{};
    for (std::size_t i = 0; i < value.size(); ++i)
        value[i] = static_cast<std::uint8_t>(first + i);
    return Id::make(value).value();
}
void native_allocation(void* base, std::size_t requested) {
    const auto served = ::malloc_usable_size(base);
    require(
      served >= requested
        && served <= capacity_bound(byte_count{requested}).value(),
      "fixture allocation exceeds verified profile");
}
} // namespace
batch_decode_expectation expected_context() {
    return {
      object<topic_id>(33),
      object<range_id>(65),
      fixture_id(),
      fixture_binding()};
}
batch_id fixture_id() {
    return batch_id::make(
             object<producer_id>(1),
             producer_epoch::make(2).value(),
             producer_stream_id::make(3).value(),
             batch_sequence{4})
      .value();
}
producer_stream_binding fixture_binding() {
    return producer_stream_binding::make(
             object<topic_id>(33),
             object<range_id>(65),
             range_routing_epoch::make(7).value(),
             object<segment_id>(97),
             segment_generation::make(9).value())
      .value();
}
void qualify_allocator() {
    for (const auto size : std::array<std::size_t, 13>{
           16,
           32,
           fragmented_buffer::fragment_descriptor_size(),
           sizeof(seastar::free_deleter_impl),
           sizeof(record_header),
           sizeof(record_header) * 64,
           128,
           1024,
           8192,
           16384,
           16385,
           32768,
           65536}) {
        seastar::temporary_buffer<char> allocation{size};
        native_allocation(allocation.get_write(), size);
    }
}

seastar::future<fragmented_buffer> copy_layout(
  fragmented_buffer input,
  std::size_t requested_width,
  codec::cooperative_work& work,
  byte_count remaining,
  std::size_t fragment_limit) {
    const auto size = input.size().value();
    if (size == 0) co_return fragmented_buffer{};
    require(
      fragment_limit != 0 && fragment_limit <= bytes::max_buffer_fragments,
      "invalid fixture fragment limit");
    const auto width = std::min<std::uint64_t>(
      size,
      std::max<std::uint64_t>(
        requested_width == 0 ? 65536 : requested_width,
        (size + fragment_limit - 1U) / fragment_limit));
    const auto count = 1U + (size - 1U) / width;
    require(
      width <= 65536 && count <= fragment_limit,
      "fixture layout exceeds allocation/fragment bound");
    if (auto ready = co_await work.checkpoint(); !ready)
        throw std::runtime_error("fixture aborted");
    const auto source = input.allocation_cost(capacity_bound).value();
    const auto descriptor = capacity_bound(
      byte_count{count * fragmented_buffer::fragment_descriptor_size()});
    const auto backing = byte_count{
      count * capacity_bound(byte_count{width}).value()};
    require(work.policy().remaining_operation_bytes({.retained_input=source.backing,.staged_output=backing,
      .payload_bookkeeping=byte_count{source.descriptors.value()+source.share_controls.value()+descriptor.value()}},remaining).has_value(),"fixture copy exceeds remaining operation budget");
    bytes::fragmented_buffer_builder builder{
      {.initial_fragment_bytes = byte_count{width},
       .max_fragment_bytes = byte_count{width},
       .max_total_bytes = byte_count{size},
       .max_retained_bytes = byte_count{count * width},
       .max_fragments = static_cast<std::size_t>(count)}};
    builder.reserve_fragments(item_count{count}).value();
    for (const auto fragment : input) {
        for (std::size_t offset = 0; offset < fragment.size();) {
            const auto n = std::min<std::size_t>(
              {fragment.size() - offset,
               static_cast<std::size_t>(work.byte_quantum().value() / 2U),
               static_cast<std::size_t>(width)});
            const auto ready = co_await work.admit(
              byte_count{2U * n}, item_count{8});
            require(ready.has_value(), "fixture copy aborted");
            builder.append(std::span<const char>{fragment.data() + offset, n})
              .value();
            offset += n;
        }
    }
    const auto ready = co_await work.checkpoint();
    require(ready.has_value(), "fixture publication aborted");
    auto result = builder.finish().value();
    // All fragment pointers are allocation bases: this builder only copied,
    // never sliced/spliced or trimmed a prefix. Verify every actual backing.
    for (const auto fragment : result) {
        native_allocation(
          const_cast<char*>(fragment.data()), static_cast<std::size_t>(width));
        const auto ready = co_await work.admit(byte_count{}, item_count{1});
        require(ready.has_value(), "fixture allocation check aborted");
    }
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    input = fragmented_buffer{};
    co_return result;
}

seastar::future<> model_fixture::account(codec::cooperative_work& work) {
    byte_count total;
    if (value) {
        const auto cost = (co_await record_allocation_cost(
                             *value, work, capacity_bound))
                            .value();
        total = cost.backing.checked_add(cost.metadata).value();
    }
    for (const auto* buffer :
         {&record_wire, &submitted_wire, &assigned_wire, &sparse_wire}) {
        const auto ready = co_await work.checkpoint();
        require(ready.has_value(), "fixture accounting aborted");
        const auto cost = buffer->allocation_cost(capacity_bound).value();
        for (auto part : {cost.backing, cost.descriptors, cost.share_controls})
            total = total.checked_add(part).value();
    }
    total = total
              .checked_add(capacity_bound(
                byte_count{selected.capacity() * sizeof(range_logical_count)}))
              .value();
    cache_charge = total;
    // Cache aliases are conservatively counted separately. A distinct 1-MiB
    // allowance covers native engines and fixture/coroutine frames; native
    // qualification remains required for that allowance.
    remaining = byte_count{63U << 20U}.checked_sub(total).value();
}
seastar::future<submitted_batch>
model_fixture::build(codec::cooperative_work& work) {
    auto builder
      = batch_builder::make(
          fixture_id(), fixture_binding(), work.policy(), capacity_bound)
          .value();
    for (std::size_t i = 0; i < count; ++i) {
        auto added = co_await builder.add(
          *value, timestamp(i), work, remaining);
        require(added.has_value(), "fixture batch append failed");
    }
    co_return (co_await builder.finalize(work, remaining)).value();
}
seastar::future<submitted_batch>
model_fixture::submitted(codec::cooperative_work& work) {
    bytes::fragmented_buffer_parser input{submitted_wire.share()};
    const auto budget
      = codec::reserve_decode_input(input, work.policy(), memory()).value();
    auto decoded = co_await decode_submitted_batch(
      input, expected_context(), budget, work);
    require(decoded.has_value(), "fixture submission decode failed");
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    input = bytes::fragmented_buffer_parser{};
    co_return std::move(decoded->value);
}
seastar::future<assigned_batch>
model_fixture::assigned(codec::cooperative_work& work, bool sparse) {
    auto& wire = sparse ? sparse_wire : assigned_wire;
    bytes::fragmented_buffer_parser input{wire.share()};
    const auto budget
      = codec::reserve_decode_input(input, work.policy(), memory()).value();
    auto decoded = co_await decode_assigned_batch(
      input, expected_context(), budget, work);
    require(decoded.has_value(), "fixture assigned decode failed");
    co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
    input = bytes::fragmented_buffer_parser{};
    co_return std::move(decoded->value);
}
seastar::future<> model_fixture::initialize(bool report) {
    if (initialized_) co_return;
    qualify_allocator();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    bytes::fragmented_buffer_builder payload{
      {.initial_fragment_bytes = byte_count{32768},
       .max_fragment_bytes = byte_count{65536},
       .max_total_bytes = byte_count{1048576},
       .max_retained_bytes = byte_count{2U << 20U},
       .max_fragments = 1024}};
    (co_await work.admit(byte_count{65536}, item_count{1})).value();
    std::array<char, 32768> block{};
    std::fill(block.begin(), block.end(), 'x');
    for (std::size_t at = 0; at < payload_size;) {
        const auto size = std::min(block.size(), payload_size - at);
        const auto ready = co_await work.admit(
          byte_count{2U * size}, item_count{8});
        require(ready.has_value(), "fixture payload aborted");
        payload.append(std::span<const char>{block}.first(size)).value();
        at += size;
    }
    auto bytes = co_await copy_layout(
      payload.finish().value(), width, work, remaining);
    std::vector<record_header> headers;
    if (payload_size < 1048565)
        headers.push_back(
          make_record_header(fragmented_buffer{}, std::nullopt).value());
    value.emplace(make_record(
                    {},
                    std::nullopt,
                    payload_size == 0
                      ? std::nullopt
                      : std::optional<fragmented_buffer>{std::move(bytes)},
                    std::move(headers))
                    .value());
    co_await account(work);
    record_wire = (co_await encode_record(
                     *value, work, remaining, capacity_bound))
                    .value();
    record_wire = co_await copy_layout(
      std::move(record_wire), width, work, remaining);
    co_await account(work);
    auto original = co_await build(work);
    digest = original.fingerprint();
    record_region_bytes = original.records().size();
    submitted_wire = (co_await encode_submitted_batch(
                        std::move(original), work, remaining, capacity_bound))
                       .value();
    submitted_wire = co_await copy_layout(
      std::move(submitted_wire), width, work, remaining);
    co_await account(work);
    auto dense = assigned_batch::assign(
                   co_await submitted(work),
                   range_logical_end{100},
                   fixture_binding())
                   .value();
    assigned_wire = (co_await encode_assigned_batch(
                       std::move(dense), work, remaining, capacity_bound))
                      .value();
    assigned_wire = co_await copy_layout(
      std::move(assigned_wire), width, work, remaining);
    co_await account(work);
    const auto selection_capacity = (count + 1U) / 2U;
    const auto selection_charge = capacity_bound(
      byte_count{selection_capacity * sizeof(range_logical_count)});
    require(
      work.policy().validate_allocation(selection_charge).has_value()
        && selection_charge <= remaining,
      "fixture selection capacity exceeds budget");
    selected.reserve(selection_capacity);
    for (std::size_t i = count == 1 ? 0 : 1; i < count; i += 2) {
        (co_await work.admit(byte_count{}, item_count{1})).value();
        selected.emplace_back(i);
    }
    co_await account(work);
    auto sparse = (co_await rewrite_assigned_batch(
                     co_await assigned(work), selected, memory(), work))
                    .value();
    sparse_wire = (co_await encode_assigned_batch(
                     std::move(sparse), work, remaining, capacity_bound))
                    .value();
    sparse_wire = co_await copy_layout(
      std::move(sparse_wire), width, work, remaining);
    co_await account(work);
    initialized_ = true;
    if (report) {
        fmt::print(
          "kwaque-model-retention-v1 records={} payload_bytes={} width={} "
          "record_region_bytes={} record_wire_bytes={} submitted_wire_bytes={} "
          "assigned_wire_bytes={} sparse_wire_bytes={} cache_charge_upper={} "
          "record_descriptor_bytes={} header_value_bytes={}\n",
          count,
          payload_size,
          width,
          record_region_bytes.value(),
          record_wire.size().value(),
          submitted_wire.size().value(),
          assigned_wire.size().value(),
          sparse_wire.size().value(),
          cache_charge.value(),
          fragmented_buffer::fragment_descriptor_size(),
          sizeof(record_header));
    }
}
} // namespace kwaque::model::bench
