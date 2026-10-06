#include "src/storage/tests/segment_bench_fixture.h"

#include "src/base/units.h"
#include "src/codec/tests/benchmark_buffer.h"
#include "src/codec/transaction.h"
#include "src/codec/xxh3.h"
#include "src/codec/xxh3_cooperative.h"
#include "src/model/batch_builder.h"
#include "src/model/batch_codec.h"
#include "src/model/record_codec.h"
#include "src/storage/format_size.h"
#include "src/storage/tests/local_installation_contract.h"

#include <crc32c/crc32c.h>

#include <algorithm>
#include <fstream>
#include <span>

namespace kwaque::storage::testing::segment_bench_support {
namespace {
using bytes::testing::charge;
using store_contract::require;
static_assert(sizeof(group_input) <= 4_KiB);
#if defined(_LIBCPP_ABI_USE_SMALL_DEQUE_BLOCK_SIZE)
constexpr std::size_t group_block_entries = sizeof(group_input) < 128
                                              ? 512 / sizeof(group_input)
                                              : 4;
#else
constexpr std::size_t group_block_entries = sizeof(group_input) < 256
                                              ? 4096 / sizeof(group_input)
                                              : 16;
#endif

codec::decode_budget memory() {
    return {working_bytes, byte_count{1_MiB}, charge};
}
storage::coverage prefix(
  const segment_history_context& history,
  model::range_logical_end logical,
  model::segment_relative_end physical,
  runtime::file_position end) {
    return storage::coverage{
      model::range_logical_span::make(history.logical_origin, logical).value(),
      model::segment_relative_span::make(history.physical_origin, physical)
        .value(),
      model::file_byte_span::make(history.data_start, end).value()};
}
extent_verifier empty_verifier(
  const segment_history_context& history, const codec::limits& limits) {
    return extent_verifier::make(
             history,
             prefix(
               history,
               history.logical_origin,
               history.physical_origin,
               history.data_start),
             limits,
             extent_layout_kind::initial_append,
             {},
             extent_integrity::crc32c_and_digest)
      .value();
}
segment_block_expectation placement(
  const segment_history_context& history,
  model::segment_relative_end physical,
  runtime::file_position position) {
    return {
      segment_write_context::make(
        history.segment, history.alignment, physical, position)
        .value(),
      history.data_start,
      {history.segment.topic(), history.segment.range()},
      history.profile};
}

seastar::future<encoded_assigned_batch> make_child(
  const shape& selected,
  const segment_context& segment,
  model::range_logical_end logical,
  std::uint64_t sequence,
  codec::cooperative_work& work) {
    const auto records = std::max<std::size_t>(
      1, selected.payload_bytes / 1_MiB);
    const auto size = selected.payload_bytes == 8_MiB ? 1048565U
                      : selected.payload_bytes == 0
                        ? 64U
                        : selected.payload_bytes / records - 1024U;
    auto payload = co_await codec::bench::patterned_buffer(
      size,
      65504,
      codec::bench::payload_pattern::incompressible,
      work,
      working_bytes);
    auto record
      = model::make_record(
          {}, {}, std::optional{std::move(payload)}, {}, work.policy())
          .value();
    const auto identity = model::batch_id::make(
                            id<model::producer_id>(0x40),
                            model::producer_epoch::make(1).value(),
                            model::producer_stream_id::make(1).value(),
                            model::batch_sequence{sequence})
                            .value();
    const auto binding = model::producer_stream_binding::make(
                           segment.topic(),
                           segment.range(),
                           model::range_routing_epoch::make(1).value(),
                           segment.segment(),
                           segment.generation())
                           .value();
    auto builder = model::batch_builder::make(
                     identity, binding, work.policy(), charge)
                     .value();
    for (std::size_t i = 0; i < records; ++i)
        (co_await builder.add(
           record,
           runtime::wall_time{
             selected.zero_timestamps ? 0 : static_cast<std::int64_t>(i)},
           work,
           working_bytes))
          .value();
    auto submitted = (co_await builder.finalize(work, working_bytes)).value();
    if (selected.payload_bytes == 8_MiB)
        require(
          submitted.records().size() == byte_count{8_MiB},
          "maximum segment fixture has wrong expanded size");
    auto assigned = model::assigned_batch::assign(
                      std::move(submitted), logical, binding)
                      .value();
    auto child
      = (co_await make_encoded_assigned_batch(
           std::move(assigned), selected.encoding, work, working_bytes, charge))
          .value();
    if (selected.fragmented) {
        auto wire = co_await codec::bench::copy_layout(
          std::move(child).release_bytes(), 257, work, working_bytes, 1000);
        child = (co_await validate_encoded_assigned_batch(
                   std::move(wire),
                   {segment.topic(), segment.range()},
                   memory(),
                   work))
                  .value();
    }
    co_return child;
}

bytes::fragmented_buffer join(
  std::span<bytes::fragmented_buffer> blocks,
  bytes::fragmented_buffer& footer) {
    auto fragments = footer.fragment_count();
    auto size = footer.size();
    auto retained = footer.retained_bytes();
    for (const auto& block : blocks) {
        fragments += block.fragment_count();
        size = size.checked_add(block.size()).value();
        retained = retained.checked_add(block.retained_bytes()).value();
    }
    bytes::fragmented_buffer_builder output{
      {.initial_fragment_bytes = byte_count{1},
       .max_fragment_bytes = byte_count{1},
       .max_total_bytes = size,
       .max_retained_bytes = retained,
       .max_fragments = fragments}};
    output.reserve_fragments(item_count{fragments}).value();
    for (auto& block : blocks)
        output.append_buffer(block.share()).value();
    output.append_buffer(footer.share()).value();
    return output.finish().value();
}
} // namespace

seastar::future<encoded_assigned_batch> make_batch(
  const shape& selected,
  const segment_context& segment,
  model::range_logical_end logical,
  std::uint64_t sequence,
  codec::cooperative_work& work) {
    return make_child(selected, segment, logical, sequence, work);
}

codec::limits policy(const shape& selected) {
    auto limits = codec::limits::defaults().config();
    limits.max_page_bytes = byte_count{selected.page_bytes};
    return codec::limits::make(limits).value();
}

seastar::future<extent_input> make_extent(
  const shape& selected, std::uint32_t index, codec::cooperative_work& work) {
    require(
      selected.batches() <= maximum_batches && index < maximum_extent_segments
        && selected.blocks > 0 && selected.blocks <= maximum_group_blocks
        && selected.window > 0 && selected.window <= 8
        && (selected.payload_bytes == 0 || (selected.payload_bytes >= 1_KiB && selected.payload_bytes <= 8_MiB)),
      "segment benchmark shape exceeds its bound");
    auto descriptor = installation_contract::descriptor();
    descriptor.segment
      = segment_context::make(
          descriptor.segment.cluster(),
          descriptor.segment.topic(),
          selected.replace
            ? descriptor.segment.range()
            : id<model::range_id>(static_cast<std::uint8_t>(0x20U + index)),
          id<model::segment_id>(static_cast<std::uint8_t>(0x30U + index)),
          descriptor.segment.generation())
          .value();
    if (selected.replace)
        descriptor.logical_origin = model::range_logical_end{
          descriptor.logical_origin.value()
          + std::uint64_t{index} * selected.batches()
              * std::max<std::size_t>(1, selected.payload_bytes / 1_MiB)};
    descriptor.alignment
      = storage_alignment::make(byte_count{selected.alignment}).value();
    auto header = (co_await encode_segment_header(
                     segment_header::make(
                       descriptor.segment,
                       descriptor.logical_origin,
                       descriptor.alignment)
                       .value(),
                     work,
                     working_bytes,
                     charge))
                    .value();
    const segment_history_context history{
      descriptor.segment,
      descriptor.alignment,
      runtime::file_position{header.size().value()},
      descriptor.logical_origin,
      descriptor.physical_origin,
      descriptor.profile};
    extent_input result{descriptor, history, std::move(header), {}, {}, {}, {}};
    result.header_digest = (co_await codec::xxh3_128_cooperatively(
                              result.header.share(), work))
                             .value();
    require(
      charge(byte_count{group_block_entries * sizeof(group_input)})
        <= byte_count{maximum_contiguous_allocation_bytes},
      "fixture descriptor block cannot meet the allocation ceiling");
    result.completed.reserve(selected.batches());
    auto verifier = empty_verifier(history, work.policy());
    auto logical = history.logical_origin;
    auto physical = history.physical_origin;
    auto position = history.data_start;
    const auto footer_bytes = aligned_envelope_layout::make(
                                {byte_count{codec::envelope_prefix_bytes},
                                 durable_footer_fixed_bytes,
                                 {}},
                                descriptor.alignment,
                                work.policy(),
                                {byte_count{64_KiB}, byte_count{64_KiB}})
                                .value()
                                .encoded_bytes();
    std::uint32_t batch = 0;
    for (std::uint32_t g = 0; g < selected.groups; ++g) {
        const auto begin = position;
        std::vector<encoded_assigned_batch> children;
        std::vector<segment_block> blocks;
        std::vector<bytes::fragmented_buffer> block_wires;
        children.reserve(selected.blocks);
        blocks.reserve(selected.blocks);
        block_wires.reserve(selected.blocks);
        // Blocks are contiguous inside a group; its one footer follows them.
        for (std::uint32_t b = 0; b < selected.blocks; ++b, ++batch) {
            auto child = co_await make_child(
              selected,
              history.segment,
              logical,
              std::uint64_t{index} * maximum_batches + batch,
              work);
            const auto info = child.info();
            const auto original = info.context.submitted();
            result.completed.push_back(
              completed_retry::make(
                original.id(),
                info.fingerprint,
                original.binding(),
                info.context.logical_span(),
                original.binding().generation())
                .value());
            result.child_bytes += child.bytes().size().value();
            result.child_fragments += child.bytes().fragment_count();
            auto shared = (co_await child.share(memory(), work)).value();
            auto encoded = (co_await encode_segment_block(
                              std::move(shared),
                              placement(history, physical, position),
                              work,
                              working_bytes,
                              charge))
                             .value();
            auto block_wire = std::move(encoded).release_bytes();
            bytes::fragmented_buffer_parser parser{block_wire.share()};
            auto decoded = (co_await decode_segment_block(
                              parser,
                              placement(history, physical, position),
                              memory(),
                              work))
                             .value();
            const auto covered = decoded.value.descriptor().coverage();
            children.push_back(std::move(child));
            blocks.push_back(std::move(decoded.value));
            block_wires.push_back(std::move(block_wire));
            logical = covered.logical().end();
            physical = covered.physical().end();
            position = covered.bytes().end();
        }
        const auto footer_begin = position;
        const auto end = footer_begin.checked_add(footer_bytes).value();
        const auto whole = prefix(history, logical, physical, end);
        verifier.extend_expected(whole, work).value();
        for (const auto& block : blocks)
            (co_await verifier.add_block(
               block,
               {history.segment.topic(), history.segment.range()},
               memory(),
               work))
              .value();
        const auto checkpoint = verifier.checkpoint(work).value();
        auto footer = (co_await encode_durable_footer(
                         checkpoint,
                         {history, footer_begin},
                         work,
                         working_bytes,
                         charge))
                        .value();
        require(
          footer.bytes().size() == footer_bytes, "footer projection changed");
        (co_await verifier.add_footer(footer, memory(), work, checkpoint))
          .value();
        auto footer_wire = std::move(footer).release_bytes();
        // One group is one gathered payload; its fragments share one bound.
        auto joined = footer_wire.fragment_count();
        for (const auto& wire : block_wires)
            joined += wire.fragment_count();
        require(
          joined <= bytes::max_buffer_fragments,
          "group exceeds the per-group fragment bound; reduce its batches");
        auto wire = join(block_wires, footer_wire);
        result.encoded_bytes += wire.size().value();
        const auto wire_fragments = wire.fragment_count();
        result.groups.push_back(
          group_input{
            std::move(children),
            std::move(blocks),
            std::move(block_wires),
            std::move(footer_wire),
            std::move(wire),
            whole,
            begin,
            end,
            wire_fragments});
        position = end;
    }
    result.proof.emplace(verifier.finish(work).value());
    codec::xxh3_128_hasher independent_hash;
    std::uint32_t independent_crc = 0;
    for (auto& group : result.groups)
        for (auto fragment : group.wire)
            for (std::size_t at = 0; at < fragment.size();) {
                const auto count = std::min(
                  fragment.size() - at,
                  static_cast<std::size_t>(work.byte_quantum().value()));
                (co_await work.admit(
                   byte_count{count},
                   item_count{1},
                   codec::error{errc::success}))
                  .value();
                independent_hash.update(fragment.data() + at, count);
                independent_crc = ::crc32c::Extend(
                  independent_crc,
                  reinterpret_cast<const std::uint8_t*>(fragment.data() + at),
                  count);
                at += count;
            }
    require(
      result.proof->digest()->bytes() == std::move(independent_hash).final()
        && result.proof->boundary().data_crc32c == independent_crc,
      "benchmark extent differs from independent byte hashing");
    co_return result;
}

byte_count retained_bound(const extent_input& input) {
    byte_count held = charge(byte_count{sizeof(extent_input)});
    auto add = [&](byte_count bytes) {
        held = held.checked_add(bytes).value();
    };
    auto buffer = [&](const bytes::fragmented_buffer& value) {
        const auto cost = value.allocation_cost(charge).value();
        add(cost.backing);
        add(cost.descriptors);
        add(cost.share_controls);
    };
    buffer(input.header);
    // Push-only deque storage: include its spare end slot and the pointer
    // map's geometric spare capacity under the selected libc++ ABI.
    const auto blocks = (input.groups.size() + group_block_entries)
                        / group_block_entries;
    for (std::size_t i = 0; i < blocks; ++i)
        add(charge(byte_count{group_block_entries * sizeof(group_input)}));
    add(charge(byte_count{2 * blocks * sizeof(group_input*)}));
    add(
      charge(byte_count{input.completed.capacity() * sizeof(completed_retry)}));
    for (const auto& group : input.groups) {
        add(charge(
          byte_count{
            group.children.capacity() * sizeof(encoded_assigned_batch)}));
        add(
          charge(byte_count{group.blocks.capacity() * sizeof(segment_block)}));
        add(charge(
          byte_count{
            group.block_wires.capacity() * sizeof(bytes::fragmented_buffer)}));
        for (const auto& child : group.children)
            buffer(child.bytes());
        for (const auto& block : group.blocks)
            buffer(block.bytes());
        for (const auto& wire : group.block_wires)
            buffer(wire);
        buffer(group.footer);
        buffer(group.wire);
    }
    return held;
}

seastar::future<verified_extent>
verify_extent(extent_input& input, bool raw, codec::cooperative_work& work) {
    auto verifier = empty_verifier(input.history, work.policy());
    for (auto& group : input.groups) {
        verifier.extend_expected(group.whole_prefix, work).value();
        const model::batch_decode_expectation expected{
          input.history.segment.topic(), input.history.segment.range()};
        for (std::size_t i = 0; i < group.blocks.size(); ++i) {
            if (raw)
                (co_await verifier.add_block(
                   group.block_wires[i].share(), expected, memory(), work))
                  .value();
            else
                (co_await verifier.add_block(
                   group.blocks[i], expected, memory(), work))
                  .value();
        }
        const auto checkpoint = verifier.checkpoint(work).value();
        (co_await verifier.add_footer(
           group.footer.share(), memory(), work, checkpoint))
          .value();
    }
    co_return verifier.finish(work).value();
}

seastar::future<verified_extent>
encode_extent(extent_input& input, codec::cooperative_work& work) {
    auto verifier = empty_verifier(input.history, work.policy());
    for (auto& group : input.groups) {
        std::vector<segment_block> blocks;
        blocks.reserve(group.children.size());
        for (std::size_t i = 0; i < group.children.size(); ++i) {
            const auto records = group.blocks[i].descriptor().coverage();
            blocks.push_back((co_await encode_segment_block(
                                std::move(group.children[i]),
                                placement(
                                  input.history,
                                  records.physical().begin(),
                                  records.bytes().begin()),
                                work,
                                working_bytes,
                                charge))
                               .value());
        }
        verifier.extend_expected(group.whole_prefix, work).value();
        for (const auto& block : blocks)
            (co_await verifier.add_block(
               block,
               {input.history.segment.topic(), input.history.segment.range()},
               memory(),
               work))
              .value();
        const auto checkpoint = verifier.checkpoint(work).value();
        auto footer
          = (co_await encode_durable_footer(
               checkpoint,
               {input.history,
                group.blocks.back().descriptor().coverage().bytes().end()},
               work,
               working_bytes,
               charge))
              .value();
        (co_await verifier.add_footer(footer, memory(), work, checkpoint))
          .value();
    }
    co_return verifier.finish(work).value();
}

seastar::future<verified_extent> encode_cut_extent(
  const extent_input& input,
  std::span<encoded_assigned_batch> children,
  std::span<const std::uint32_t> cuts,
  codec::cooperative_work& work) {
    const auto& history = input.history;
    const model::batch_decode_expectation expected{
      history.segment.topic(), history.segment.range()};
    const auto footer_bytes = aligned_envelope_layout::make(
                                {byte_count{codec::envelope_prefix_bytes},
                                 durable_footer_fixed_bytes,
                                 {}},
                                history.alignment,
                                work.policy(),
                                {byte_count{64_KiB}, byte_count{64_KiB}})
                                .value()
                                .encoded_bytes();
    auto verifier = empty_verifier(history, work.policy());
    auto logical = history.logical_origin;
    auto physical = history.physical_origin;
    auto position = history.data_start;
    std::size_t next = 0;
    for (const auto size : cuts) {
        require(
          size != 0 && size <= children.size() - next,
          "group cut exceeds the supplied children");
        std::vector<segment_block> blocks;
        blocks.reserve(size);
        // Blocks are contiguous inside a group; its one footer follows them.
        for (std::uint32_t i = 0; i < size; ++i, ++next) {
            auto block = (co_await encode_segment_block(
                            std::move(children[next]),
                            placement(history, physical, position),
                            work,
                            working_bytes,
                            charge))
                           .value();
            const auto covered = block.descriptor().coverage();
            logical = covered.logical().end();
            physical = covered.physical().end();
            position = covered.bytes().end();
            blocks.push_back(std::move(block));
        }
        const auto end = position.checked_add(footer_bytes).value();
        verifier.extend_expected(prefix(history, logical, physical, end), work)
          .value();
        for (const auto& block : blocks)
            (co_await verifier.add_block(block, expected, memory(), work))
              .value();
        const auto checkpoint = verifier.checkpoint(work).value();
        auto footer
          = (co_await encode_durable_footer(
               checkpoint, {history, position}, work, working_bytes, charge))
              .value();
        (co_await verifier.add_footer(footer, memory(), work, checkpoint))
          .value();
        position = end;
    }
    require(next == children.size(), "group cuts omit supplied children");
    co_return verifier.finish(work).value();
}

void export_extent(
  extent_input& input, const shape& selected, std::string_view path) {
    std::ofstream bytes{std::string{path}, std::ios::binary | std::ios::trunc};
    std::ofstream manifest{std::string{path} + ".groups", std::ios::trunc};
    std::ofstream header{
      std::string{path} + ".header", std::ios::binary | std::ios::trunc};
    require(
      bool(bytes) && bool(manifest) && bool(header),
      "cannot create segment fixture");
    for (auto part : input.header)
        header.write(part.data(), static_cast<std::streamsize>(part.size()));
    for (std::size_t i = 0; i < input.groups.size(); ++i) {
        const auto& group = input.groups[i];
        const auto covering = std::min(
          input.groups.size() - 1,
          ((i / selected.window) + 1) * selected.window - 1);
        manifest << group.wire.size().value() << ' '
                 << input.groups[covering].end.value() << ' '
                 << group.wire.fragment_count();
        for (const auto part : group.wire) {
            bytes.write(part.data(), static_cast<std::streamsize>(part.size()));
            manifest << ' ' << part.size();
        }
        manifest << '\n';
    }
    bytes.close();
    manifest.close();
    header.close();
    require(
      bool(bytes) && bool(manifest) && bool(header),
      "segment fixture export was incomplete");
}

seastar::future<> export_records(
  extent_input& input,
  const shape& selected,
  std::string_view path,
  codec::cooperative_work& work) {
    std::ofstream values{std::string{path} + ".values", std::ios::binary};
    std::ofstream layout{std::string{path} + ".records"};
    require(bool(values) && bool(layout), "cannot create record fixture");
    // Native batches keep the same order and per-barrier membership: each
    // Kwaque group contributes its batches to one covering barrier window.
    layout << "segment-records-v1 " << selected.batches() << ' '
           << selected.blocks * selected.window << ' '
           << (selected.encoding == compression::codec_id::lz4 ? "lz4" : "none")
           << ' ' << (selected.fragmented ? 257 : 65504) << '\n';
    for (auto& group : input.groups) {
        for (auto& batch : group.children) {
            auto alias = (co_await batch.share(memory(), work)).value();
            bytes::fragmented_buffer_parser child{
              std::move(alias).release_bytes()};
            const auto budget = codec::reserve_decode_input(
                                  child, work.policy(), memory())
                                  .value();
            auto decoded = (co_await model::decode_assigned_batch(
                              child,
                              {input.history.segment.topic(),
                               input.history.segment.range()},
                              budget,
                              work,
                              {},
                              codec::input_boundary::complete))
                             .value();
            require(
              child.at_end(), "record fixture contains trailing child bytes");
            const auto context = decoded.value.context().submitted();
            const auto count = context.original_count().value();
            require(
              decoded.value.context().retained_count().value() == count,
              "record fixture requires dense batches");
            layout << count << '\n';
            bytes::fragmented_buffer_parser records{
              std::move(decoded.value).release_records()};
            for (std::uint64_t i = 0; i != count; ++i) {
                auto record = (co_await model::decode_record(
                                 records,
                                 {context.original_timestamp_base(),
                                  context.original_count(),
                                  item_count{}},
                                 decoded.remaining,
                                 work,
                                 {},
                                 codec::input_boundary::complete))
                                .value();
                require(
                  record.value.logical_delta().value() == i
                    && record.value.attributes() == 0 && !record.value.has_key()
                    && record.value.has_value()
                    && record.value.headers().empty()
                    && context.original_timestamp_base().unix_nanoseconds() == 0
                    && record.value.timestamp_delta() == 0,
                  "record fixture has unsupported fields");
                const auto& value = *record.value.value();
                layout << record.value.timestamp_delta() << ' '
                       << value.size().value() << '\n';
                for (const auto part : value) {
                    for (std::size_t at = 0; at != part.size();) {
                        const auto size = std::min(
                          part.size() - at,
                          static_cast<std::size_t>(
                            work.byte_quantum().value()));
                        (co_await work.admit(byte_count{size}, item_count{1}))
                          .value();
                        values.write(
                          part.data() + at, static_cast<std::streamsize>(size));
                        at += size;
                    }
                }
            }
            require(
              records.at_end(), "record fixture contains trailing records");
        }
    }
    values.close();
    layout.close();
    require(
      bool(values) && bool(layout), "record fixture export was incomplete");
}
} // namespace kwaque::storage::testing::segment_bench_support
