#pragma once

#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/model/batch.h"
#include "src/model/batch_builder.h"
#include "src/model/batch_identity.h"
#include "src/model/record_codec.h"
#include "src/runtime/time.h"
#include "src/storage/encoded_batch.h"
#include "src/storage/local_generation.h"
#include "src/storage/page_cache.h"
#include "src/storage/segment_reader.h"
#include "src/storage/segment_run.h"
#include "src/storage/sparse_index.h"
#include "src/storage/sparse_index_owner.h"
#include "src/storage/tests/segment_writer_contract.h"
#include "src/storage/tests/sparse_index_contract.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/util/later.hh>

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace kwaque::storage::testing::segment_reader_contract {
using store_contract::require;
using store_contract::take;
namespace installation = installation_contract;
namespace segments = segment_writer_contract;
namespace indexes = sparse_index_contract;

// One run asked for ahead, at once, by the second run in a row: what the
// cases that count requests, loads and waits were written for. A case of
// what a session does when nothing is said says so.
inline const segment_reader_config one_ahead{
  .read_ahead = 1, .read_ahead_grows = false};

inline model::range_logical_offset offset(std::uint64_t value) {
    return model::range_logical_offset::make(value).value();
}
inline segment_run_limits run_limits() {
    return {
      .charge = bytes::testing::charge,
      .run_bytes = byte_count{1_MiB},
      .working_bytes = byte_count{64_KiB},
      .metadata_bytes = byte_count{16_KiB}};
}
inline bytes::fragmented_buffer buffer(const std::string& bytes) {
    return bytes::fragmented_buffer::copy_of(
             std::span<const char>{bytes.data(), bytes.size()})
      .value();
}
// The encoded batch of the block that begins at `at` in a segment's file,
// read from the file's bytes by its own two length fields: the envelope's
// header length, and the block's length of its child.
inline std::string batch_at(const std::string& data, std::uint64_t at) {
    const auto header = get(data, at + 10, 2);
    const auto child = get(data, at + header + 112, 4);
    return data.substr(at + header + 120, child);
}
// With one small block and its footer to a group, every `blocks`-th block is
// an anchor: a block begins two units after the one before it.
inline sparse_index_stride stride_of(std::uint32_t blocks) {
    return sparse_index_stride{byte_count{
      (2ULL * blocks - 1) * segments::descriptor().alignment.bytes().value()}};
}
// Three units to a stride: every second block is an anchor.
inline sparse_index_stride paired_stride() { return stride_of(2); }

// A batch of `records` records of `payload` bytes each, assigned at `logical`
// in the fixtures' segment with every record kept, as an append is.
inline seastar::future<encoded_assigned_batch> dense_child(
  std::uint64_t logical,
  std::uint32_t records,
  std::size_t payload,
  codec::cooperative_work& work) {
    const auto segment = segments::descriptor().segment;
    // What the model is told its caller still has: far more than a batch
    // here takes.
    const byte_count room{32_MiB};
    const auto record = model::make_record(
                          {},
                          {},
                          std::optional{buffer(std::string(payload, 'k'))},
                          {},
                          work.policy())
                          .value();
    const auto origin = segments::descriptor().logical_origin.value();
    const auto identity = model::batch_id::make(
                            id<model::producer_id>(0x40),
                            model::producer_epoch::make(1).value(),
                            model::producer_stream_id::make(1).value(),
                            model::batch_sequence{logical - origin})
                            .value();
    const auto binding = model::producer_stream_binding::make(
                           segment.topic(),
                           segment.range(),
                           model::range_routing_epoch::make(1).value(),
                           segment.segment(),
                           segment.generation())
                           .value();
    auto builder = model::batch_builder::make(
                     identity, binding, work.policy(), bytes::testing::charge)
                     .value();
    for (std::uint32_t i = 0; i < records; ++i)
        (co_await builder.add(
           record,
           runtime::wall_time{static_cast<std::int64_t>(i)},
           work,
           room))
          .value();
    auto submitted = (co_await builder.finalize(work, room)).value();
    auto assigned = model::assigned_batch::assign(
                      std::move(submitted),
                      model::range_logical_end{logical},
                      binding)
                      .value();
    co_return (co_await make_encoded_assigned_batch(
                 std::move(assigned),
                 compression::codec_id::none,
                 work,
                 room,
                 bytes::testing::charge))
      .value();
}

// Writes each of `batches` as a block in a group of its own and seals them,
// as indexes::seal_segment() does its one-record blocks.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<indexes::sealed_segment<segment_writer<Backend, Owner, Clock>>>
seal_batches(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  codec::cooperative_work& work,
  Driver drive,
  std::vector<encoded_assigned_batch> batches,
  sparse_index_stride stride) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    const auto blocks = static_cast<std::uint32_t>(batches.size());
    auto config = segments::configuration();
    config.maximum_groups = blocks;
    config.admission.working_bytes = byte_count{1_MiB};
    config.admission.maximum_blocks = 16;
    const auto described = segments::descriptor();
    indexes::sealed_segment<writer_type> output;
    output.index = std::make_unique<active_sparse_index>(take(
      active_sparse_index::make(
        stride,
        sparse_index_capacity(
          stride,
          described.maximum_data_bytes,
          config.admission.maximum_blocks),
        resources)));
    output.writer = take(
      writer_type::make_new(
        files, owner, spec, 0, described, resources, config));
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(output.writer->create_new(work)));
        take(output.writer->observe_blocks(output.index->observer()));
        for (auto& batch : batches) {
            auto group = co_await segments::freeze_child(
              *output.writer, std::move(batch), resources, work);
            take(co_await output.writer->encode_group(group, work));
            output.blocks.push_back(group.blocks()[0].descriptor());
            auto submitted = take(
              output.writer->submit(std::move(group), work));
            const auto written = co_await drive.lifecycle(
              std::move(submitted.written));
            take(written.failure.outcome());
        }
        std::uint32_t calls = 0;
        output.sealed = co_await drive.lifecycle(output.writer->seal(
          segments::completed_source{{}, {}, &calls},
          0,
          blocks,
          work,
          sparse_index_seal{
            output.index.get(), local_object_sequence::make(46).value()}));
        take(output.sealed.failure.outcome());
        require(
          output.sealed.boundary && output.sealed.retry && output.sealed.extent
            && output.sealed.index && output.index->frozen()
            && output.index->skipped() == 0,
          "the segment was not sealed as asked");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (failed.failed()) {
        try {
            static_cast<void>(co_await drive.lifecycle(output.writer->close()));
        } catch (...) {
        }
        output.writer.reset();
        take(failed.outcome());
    }
    co_return output;
}

// A run is checked block by block, where each lies, and is the blocks that
// were written: their spans, their places, and their batches byte for byte.
// The first block must be the one the run's start names. Bytes that stop
// inside an object are a whole run of nothing and a part of what precedes
// the object. Nothing damaged, misplaced or foreign makes a run: a flipped
// byte is damage, zeroes are not a block, and a start whose block is not the
// one it names says so.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> run_checks(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    auto segment = co_await indexes::seal_segment<Clock>(
      files,
      owner,
      spec,
      resources,
      work,
      drive,
      4,
      sparse_index_stride{byte_count{0}},
      local_object_sequence::make(46).value());
    auto& writer = *segment.writer;
    const auto& blocks = segment.blocks;
    runtime::first_failure failed;
    try {
        const auto history = segment.history();
        const auto unit = history.alignment.bytes().value();
        const auto data = co_await store_contract::read_all_bytes(
          files, indexes::data_path(spec), drive);
        const auto begin = blocks[0].coverage().bytes().begin().value();
        const auto end = segment.context().coverage().bytes().end().value();
        const auto extent = data.substr(begin, end - begin);
        const auto limits = run_limits();
        const auto start_of = [&](std::size_t block) {
            const auto covered = blocks[block].coverage();
            return segment_run_start{
              covered.bytes().begin(),
              offset(covered.logical().begin().value()),
              covered.physical().begin()};
        };
        const auto from = [&](std::size_t block) {
            return extent.substr(
              blocks[block].coverage().bytes().begin().value() - begin);
        };
        const auto check = [&](
                             std::string bytes,
                             segment_history_context context,
                             segment_run_start start,
                             bool whole) {
            return check_segment_run(
              buffer(bytes), context, start, whole, limits, resources, work);
        };
        require(
          segment.index->size() == 4
            && segment_run_at(history, (*segment.index)[0])
                 == std::optional{start_of(0)}
            && segment_run_at(history, (*segment.index)[3])
                 == std::optional{start_of(3)},
          "an anchor does not name where its run begins");
        const auto idle = resources.snapshot();
        {
            const auto checked = take(
              co_await check(extent, history, start_of(0), true));
            const auto& run = checked.run;
            require(
              !checked.cut && run.blocks().size() == 4
                && run.begin().value() == begin && run.end().value() == end
                && run.charge().value() >= extent.size(),
              "the run of a whole extent is not its blocks");
            for (std::size_t i = 0; i < 4; ++i) {
                const auto covered = blocks[i].coverage();
                const auto& block = run.blocks()[i];
                const auto at = covered.bytes().begin().value();
                require(
                  block.logical == covered.logical()
                    && block.physical == covered.physical()
                    && begin + block.offset == at
                    && block.bytes == covered.bytes().size().value()
                    && data.substr(at + block.batch_offset, block.batch_bytes)
                         == batch_at(data, at)
                    && flat(take(run.batch(i))) == batch_at(data, at)
                    && run.seek(offset(covered.logical().begin().value())) == i
                    && run.seek(offset(covered.logical().end().value() - 1))
                         == i
                    && run.at(i) == start_of(i),
                  "a block of a run is not the block that was written");
            }
            require(
              run.seek(offset(blocks[3].coverage().logical().end().value()))
                  == 4
                && run.after(0) == std::optional{start_of(1)}
                && run.after(3).has_value()
                && run.after(3)->position.value() == end,
              "a run does not say what follows its blocks");
        }
        {
            // One alignment unit short: the last object is cut, or is not
            // there. A part holds what is complete; a whole run is refused
            // when its bytes stop inside an object.
            const auto shorter = extent.substr(0, extent.size() - unit);
            const auto part = take(
              co_await check(shorter, history, start_of(0), false));
            const auto whole = co_await check(
              shorter, history, start_of(0), true);
            require(
              part.run.blocks().size() >= 3
                && part.run.end().value() <= end - unit
                && part.cut.has_value() == (part.run.end().value() < end - unit)
                && whole.has_value() == !part.cut.has_value()
                && (whole || whole.error().code() == errc::malformed_data),
              "bytes that stop inside an object were not a part, or were a "
              "whole run");
        }
        {
            // A byte of the second block's batch is flipped: its body's
            // checksum says so.
            auto flipped = extent;
            const auto second = blocks[1].coverage().bytes().begin().value()
                                - begin;
            flipped[second + 32 + 120 + 40] ^= 1;
            const auto damaged = co_await check(
              flipped, history, start_of(0), true);
            // The third block is zeroes.
            auto zeroed = extent;
            const auto third = blocks[2].coverage().bytes().begin().value()
                               - begin;
            zeroed.replace(
              third,
              blocks[2].coverage().bytes().size().value(),
              blocks[2].coverage().bytes().size().value(),
              '\0');
            const auto empty = co_await check(
              zeroed, history, start_of(0), true);
            require(
              !damaged && damaged.error().code() == errc::corrupt_data && !empty
                && empty.error().code() == errc::malformed_data,
              "a damaged or zeroed block made a run");
        }
        {
            // The second block read where the first is expected, under the
            // first's offset, under the first's place among the records, and
            // under another segment's name.
            auto wrong_base = start_of(1);
            wrong_base.base = start_of(0).base;
            auto wrong_place = start_of(1);
            wrong_place.physical = start_of(0).physical;
            auto wrong_position = start_of(0);
            wrong_position.position = start_of(1).position;
            auto foreign = history;
            foreign.segment = take(
              segment_context::make(
                history.segment.cluster(),
                history.segment.topic(),
                history.segment.range(),
                history.segment.segment(),
                model::segment_generation::make(
                  history.segment.generation().value() + 1)
                  .value()));
            const auto base = co_await check(
              from(1), history, wrong_base, true);
            const auto place = co_await check(
              from(1), history, wrong_place, true);
            const auto position = co_await check(
              extent, history, wrong_position, true);
            const auto other = co_await check(
              extent, foreign, start_of(0), true);
            const auto right = co_await check(
              from(1), history, start_of(1), true);
            require(
              !base && base.error().code() == errc::wrong_context && !place
                && place.error().code() == errc::wrong_context && !position
                && position.error().code() == errc::wrong_context && !other
                && other.error().code() == errc::wrong_context
                && right.has_value() && right->run.blocks().size() == 3,
              "a run began with a block its start does not name");
        }
        {
            // A run whose blocks hold several offsets and leave some out,
            // as an extent that kept fewer records than it was appended
            // would: an offset inside a block finds that block, and one no
            // block holds finds the first block after it.
            const auto spans = [&](
                                 std::uint64_t from,
                                 std::uint64_t until,
                                 std::uint64_t ordinal,
                                 std::uint64_t at) {
                return segment_run_block{
                  .logical = model::range_logical_span::make(
                               model::range_logical_end{from},
                               model::range_logical_end{until})
                               .value(),
                  .physical = model::segment_relative_span::make(
                                model::segment_relative_end{ordinal},
                                model::segment_relative_end{
                                  ordinal + until - from})
                                .value(),
                  .offset = at * unit,
                  .bytes = static_cast<std::uint32_t>(unit),
                  .batch_offset = 0,
                  .batch_bytes = 1,
                  .part = 0};
            };
            segment_run_builder building{
              runtime::file_position{begin}, resources};
            take(building.part(buffer(std::string(3 * unit, 'x'))));
            take(building.block(spans(110, 115, 0, 0)));
            take(building.block(spans(120, 121, 5, 1)));
            take(building.block(spans(121, 130, 6, 2)));
            building.end_at(3 * unit);
            const auto run = std::move(building).finish();
            const auto found = [&](std::uint64_t target) {
                return run.seek(offset(target));
            };
            require(
              found(100) == 0 && found(110) == 0 && found(112) == 0
                && found(114) == 0 && found(115) == 1 && found(119) == 1
                && found(120) == 1 && found(121) == 2 && found(125) == 2
                && found(129) == 2 && found(130) == 3 && found(500) == 3,
              "an offset inside a block, or one no block holds, did not find "
              "the block its reader is given");
            const auto second = run.at(1);
            const auto last = run.after(2);
            require(
              second.position.value() == begin + unit
                && second.base == offset(120) && second.physical.value() == 5
                && run.after(0) == std::optional{second} && last.has_value()
                && last->position.value() == begin + 3 * unit
                && last->base == offset(130) && last->physical.value() == 15,
              "a run with offsets left out does not say where its blocks "
              "and what follows them begin");
        }
        const auto after = resources.snapshot();
        require(
          after.tasks == idle.tasks && after.bytes == idle.bytes
            && !writer.failure().failed(),
          "checking a run took something from the budget, or failed its "
          "segment");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        failed.observe(co_await drive.lifecycle(writer.close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    segment.writer.reset();
    take(failed.outcome());
}

// A sealed segment with its generation and its index open and a cache, and
// what each of its blocks' batches is in the file. One small block and its
// footer to a group and three units to a stride: every second block is an
// anchor, so its runs hold two blocks each and the last may hold one.
template<typename Backend, typename Owner, typename Writer>
struct sealed_world final {
    indexes::sealed_segment<Writer> segment;
    sparse_index_residency residency{1};
    sparse_index_rebuild_limit rebuilds;
    std::unique_ptr<page_cache> cache;
    std::unique_ptr<local_generation_owner> generation;
    std::unique_ptr<sealed_sparse_index<Backend, Owner>> index;
    std::optional<local_generation_pin> pin;
    std::vector<std::string> expected;

    [[nodiscard]] std::uint64_t base(std::size_t block) const {
        return segment.blocks[block].coverage().logical().begin().value();
    }
    [[nodiscard]] std::uint64_t position(std::size_t block) const {
        return segment.blocks[block].coverage().bytes().begin().value();
    }
    [[nodiscard]] std::uint64_t first() const { return base(0); }
    [[nodiscard]] std::uint64_t last() const {
        return segment.blocks.back().coverage().logical().end().value();
    }
    [[nodiscard]] runtime::file_position bound() const {
        return pin->data_end().value();
    }
    [[nodiscard]] std::unique_ptr<segment_runs>
    runs(workload_budget& resources) const {
        return take(
          segment_runs::make(
            *cache, resources, segment.history(), {.limits = run_limits()}));
    }
    // A session that may see up to `visible`.
    [[nodiscard]] segment_reader session(
      segment_runs& through,
      workload_budget& resources,
      std::uint64_t visible,
      segment_reader_config config = one_ahead) const {
        return take(
          segment_reader::open(
            through,
            segment_data_source{take(pin->share())},
            {.durable = bound(),
             .sealed = model::range_logical_end{last()},
             .visible = model::range_logical_end{visible}},
            sealed_read_index(*index),
            resources,
            config));
    }
    // Whether `read` returned `count` blocks from `from`, each whole and as
    // it was written.
    [[nodiscard]] bool matches(
      const segment_read& read, std::size_t from, std::size_t count) const {
        if (read.batches.size() != count) return false;
        std::uint64_t bytes = 0;
        for (std::size_t i = 0; i < count; ++i) {
            const auto& batch = read.batches[i];
            const auto covered = segment.blocks[from + i].coverage();
            if (
              batch.logical != covered.logical()
              || batch.physical != covered.physical()
              || flat(batch.bytes) != expected[from + i])
                return false;
            bytes += expected[from + i].size();
        }
        return read.bytes.value() == bytes;
    }
};

// Closes what a sealed world opened, in the order its parts depend on each
// other. The cache is stopped and stays, for whoever looks at what it is
// still charged.
template<typename World, typename Driver>
seastar::future<>
close_world(World& world, runtime::first_failure& failed, Driver drive) {
    if (world.index) {
        try {
            failed.observe(co_await drive.lifecycle(world.index->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        world.index.reset();
    }
    world.pin.reset();
    if (world.generation) {
        try {
            failed.observe(co_await drive.lifecycle(world.generation->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        world.generation.reset();
    }
    if (world.cache) {
        try {
            co_await drive.lifecycle(world.cache->stop());
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    if (world.segment.writer) {
        try {
            failed.observe(
              co_await drive.lifecycle(world.segment.writer->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        world.segment.writer.reset();
    }
}

// Opens a sealed segment's generation and index over a cache: a world.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<std::unique_ptr<
  sealed_world<Backend, Owner, segment_writer<Backend, Owner, Clock>>>>
open_sealed(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  codec::cooperative_work& work,
  Driver drive,
  indexes::sealed_segment<segment_writer<Backend, Owner, Clock>> sealed_one,
  sparse_index_stride stride) {
    using world
      = sealed_world<Backend, Owner, segment_writer<Backend, Owner, Clock>>;
    auto made = std::make_unique<world>();
    made->segment = std::move(sealed_one);
    runtime::first_failure failed;
    try {
        const auto described = segments::descriptor();
        const auto context = made->segment.context();
        const auto history = made->segment.history();
        const auto& sealed = made->segment.sealed;
        const auto data = co_await store_contract::read_all_bytes(
          files, indexes::data_path(spec), drive);
        for (std::size_t i = 0; i < made->segment.blocks.size(); ++i)
            made->expected.push_back(batch_at(data, made->position(i)));
        require(
          history.logical_origin.value() == made->first(),
          "the segment does not begin where its history says");
        made->cache = take(
          page_cache::make(resources, {.capacity = byte_count{2_MiB}}));
        const std::array<local_bundle_context, 2> contexts{
          context, footer_expectation{history, sealed.boundary->position()}};
        const local_generation_expectation opened{
          *made->segment.writer->publication_generation(),
          {described.segment,
           local_object_state::sealed,
           *sealed.boundary,
           {*sealed.index, *sealed.retry}},
          described,
          contexts};
        made->generation = take(
          co_await drive.lifecycle(
            local_generation_owner::open(
              files,
              owner,
              spec,
              0,
              opened,
              resources,
              store_contract::limits(),
              work)));
        made->pin.emplace(take(made->generation->pin()));
        made->index = take(
          sealed_sparse_index<Backend, Owner>::make(
            files,
            owner,
            spec,
            0,
            history,
            context,
            stride,
            *sealed.index,
            std::nullopt,
            made->residency,
            made->rebuilds,
            resources,
            resources,
            store_contract::limits(),
            indexes::scan_limits(),
            work.policy(),
            made->cache.get()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (failed.failed()) {
        co_await close_world(*made, failed, drive);
        take(failed.outcome());
    }
    co_return made;
}

// A world of `blocks` one-record blocks, `per_run` of them to a run.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<std::unique_ptr<
  sealed_world<Backend, Owner, segment_writer<Backend, Owner, Clock>>>>
open_world(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  codec::cooperative_work& work,
  Driver drive,
  std::uint32_t blocks,
  std::uint32_t per_run = 2) {
    const auto stride = stride_of(per_run);
    auto segment = co_await indexes::seal_segment<Clock>(
      files,
      owner,
      spec,
      resources,
      work,
      drive,
      blocks,
      stride,
      local_object_sequence::make(46).value());
    const auto anchors = segment.index->size();
    auto made = co_await open_sealed<Clock>(
      files, owner, spec, resources, work, drive, std::move(segment), stride);
    if (anchors != (blocks + per_run - 1) / per_run) {
        runtime::first_failure failed;
        co_await close_world(*made, failed, drive);
        require(
          false, "the segment's runs are not the ones these cases are about");
    }
    co_return made;
}

// A session on a sealed segment. A read returns whole batches forward from
// the batch its offset lies in, each a share of the run it was read in, byte
// for byte what was written, and says why it stopped and where the next read
// goes on. A run is read from the device once, as the cache's own work, for
// every reader that wants it, those that arrive together included;
// afterwards a read touches no file. The first batch of a read is returned
// whatever the limit unless its caller says otherwise, and is then told how
// large it is. An offset before the segment's first is told so. Nothing at
// or past the offset a session may see is returned. A session that leaves
// nothing behind reads alone. What a read returned outlives its session, its
// segment's runs and the cache.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> sealed_reads(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    auto world = co_await open_world<Clock>(
      files, owner, spec, resources, work, drive, 5);
    auto& w = *world;
    auto& cache = *w.cache;
    std::unique_ptr<segment_runs> runs, scanned, shared;
    // What a read returned, kept past everything it was read through.
    std::optional<segment_read> kept;
    runtime::first_failure failed;
    try {
        const auto first = w.first();
        const auto last = w.last();
        const auto next_block = [&](std::size_t block) {
            return std::optional{runtime::file_position{w.position(block)}};
        };
        runs = w.runs(resources);
        seastar::abort_source asking;
        const byte_count all{1_MiB};
        {
            auto reader = w.session(*runs, resources, last);
            const auto before = cache.snapshot();
            // An offset before the segment's first is another segment's.
            const auto below = co_await drive.lifecycle(
              reader.read(offset(first - 1), all, asking));
            // Told how large the first batch is, when its caller would
            // rather not be given one that does not fit.
            const auto refused = co_await drive.lifecycle(reader.read(
              {{model::range_logical_end{first}, std::nullopt},
               byte_count{1},
               false},
              asking));
            const auto led = cache.snapshot();
            require(
              below.batches.empty()
                && below.stop == segment_read_stop::below_start
                && refused.batches.empty()
                && refused.stop == segment_read_stop::limit
                && refused.wanted.value() == w.expected[0].size()
                && refused.next.offset.value() == first
                && led.loads == before.loads + 3 && led.uncached == 0
                && w.index->loads() == 1,
              "a read below the segment's start, or one whose first batch "
              "did not fit, was not told so");
            // The first batch is returned whatever the limit, with where
            // the next one begins.
            const auto one = co_await drive.lifecycle(
              reader.read(offset(first), byte_count{1}, asking));
            const auto fitted = co_await drive.lifecycle(reader.read(
              {{model::range_logical_end{first}, std::nullopt},
               refused.wanted,
               false},
              asking));
            require(
              w.matches(one, 0, 1) && one.stop == segment_read_stop::limit
                && one.next
                     == segment_read_position{model::range_logical_end{w.base(1)}, next_block(1)}
                && w.matches(fitted, 0, 1) && fitted.next == one.next
                && cache.snapshot().loads == led.loads,
              "the first batch was not returned whole with where the next "
              "begins, or its run was read again");
            // The rest, from where the first read said: the first run is
            // the cache's already, and the two after it are read once each.
            const auto rest = co_await drive.lifecycle(
              reader.read({one.next, all, true}, asking));
            const auto read = cache.snapshot();
            require(
              w.matches(rest, 1, 4)
                && rest.stop == segment_read_stop::end_of_segment
                && rest.next.offset.value() == last && !rest.error
                && read.loads == led.loads + 2 && read.uncached == 0
                && read.refused == 0,
              "the batches after the first were not returned in order, or "
              "their runs were read more than once");
            // Everything, and no file is touched for it.
            const auto settled = resources.snapshot();
            kept.emplace(
              co_await drive.lifecycle(
                reader.read(offset(first), all, asking)));
            const auto again = cache.snapshot();
            const auto now = resources.snapshot();
            require(
              w.matches(*kept, 0, 5)
                && kept->stop == segment_read_stop::end_of_segment
                && again.loads == read.loads && again.bytes == read.bytes
                && w.index->loads() == 1 && now.handles == settled.handles,
              "a read of what the cache keeps read a file, or returned "
              "something else");
            // At the end of a sealed segment and past it there is nothing,
            // ever; and a caller that left is told nothing else.
            const auto at_end = co_await drive.lifecycle(
              reader.read(offset(last), all, asking));
            const auto past = co_await drive.lifecycle(
              reader.read(offset(last + 7), all, asking));
            seastar::abort_source gone;
            gone.request_abort();
            const auto left = co_await drive.lifecycle(
              reader.read(offset(first), all, gone));
            require(
              at_end.batches.empty()
                && at_end.stop == segment_read_stop::end_of_segment
                && at_end.next.offset.value() == last && past.batches.empty()
                && past.stop == segment_read_stop::end_of_segment
                && past.next.offset.value() == last + 7 && left.batches.empty()
                && left.stop == segment_read_stop::cancelled && !left.error
                && cache.snapshot().loads == read.loads,
              "a read at or past the segment's end, or one whose caller "
              "left, returned something or was told something else");
        }
        {
            // A session that may see up to the third batch's offset is
            // given the two before it, whatever the cache holds; and is
            // told that this copy is short when its bound was cut to what
            // the copy holds.
            auto bounded = w.session(*runs, resources, w.base(2));
            const auto seen = co_await drive.lifecycle(
              bounded.read(offset(first), all, asking));
            auto cut = take(
              segment_reader::open(
                *runs,
                segment_data_source{take(w.pin->share())},
                {.durable = w.bound(),
                 .sealed = model::range_logical_end{last},
                 .visible = model::range_logical_end{w.base(2)},
                 .short_of_visibility = true},
                sealed_read_index(*w.index),
                resources,
                one_ahead));
            const auto shown = co_await drive.lifecycle(
              cut.read(offset(first), all, asking));
            require(
              w.matches(seen, 0, 2)
                && seen.stop == segment_read_stop::not_yet_visible
                && seen.next.offset.value() == w.base(2)
                && w.matches(shown, 0, 2)
                && shown.stop == segment_read_stop::short_of_visibility,
              "a session was given a batch past what it may see, or was not "
              "told why it was given no more");
            // A copy whose bound was cut where its sealed records end says
            // that it is short, not that the segment has ended. So does a
            // sealed session whose file bound lies inside the extent, where
            // its bytes end and its records do not.
            auto ended = take(
              segment_reader::open(
                *runs,
                segment_data_source{take(w.pin->share())},
                {.durable = w.bound(),
                 .sealed = model::range_logical_end{last},
                 .visible = model::range_logical_end{last},
                 .short_of_visibility = true},
                sealed_read_index(*w.index),
                resources,
                one_ahead));
            const auto to_end = co_await drive.lifecycle(
              ended.read(offset(first), all, asking));
            const auto at_end = co_await drive.lifecycle(
              ended.read({to_end.next, all, true}, asking));
            auto lagging = take(
              segment_reader::open(
                *runs,
                segment_data_source{take(w.pin->share())},
                {.durable = runtime::file_position{w.segment.blocks[2]
                                                     .coverage()
                                                     .bytes()
                                                     .end()
                                                     .value()},
                 .sealed = model::range_logical_end{last},
                 .visible = model::range_logical_end{last}},
                sealed_read_index(*w.index),
                resources,
                one_ahead));
            const auto lagged = co_await drive.lifecycle(
              lagging.read(offset(first), all, asking));
            require(
              w.matches(to_end, 0, 5)
                && to_end.stop == segment_read_stop::short_of_visibility
                && at_end.batches.empty()
                && at_end.stop == segment_read_stop::short_of_visibility
                && w.matches(lagged, 0, 3)
                && lagged.stop == segment_read_stop::short_of_visibility
                && lagged.next.offset.value() == w.base(3),
              "a sealed copy that is short of what may be seen said its "
              "segment had ended");
        }
        {
            // What a read returns is charged to its reader: its table, and
            // each run its batches keep in the cache. A session told to
            // keep no more than its first run stops there, as for any
            // limit, and goes on with the next read. The charge lasts as
            // long as any batch does and goes back with the last.
            const auto unit = segments::descriptor().alignment.bytes().value();
            auto reader = w.session(
              *runs,
              resources,
              last,
              {.read_ahead = 0, .held_bytes = byte_count{1}});
            const auto idle = resources.snapshot();
            std::vector<segment_read_batch> outliving;
            {
                auto part = co_await drive.lifecycle(
                  reader.read(offset(first), all, asking));
                const auto next = co_await drive.lifecycle(
                  reader.read({part.next, all, true}, asking));
                const auto holding = resources.snapshot();
                require(
                  w.matches(part, 0, 2) && part.stop == segment_read_stop::limit
                    && part.next.offset.value() == w.base(2)
                    && w.matches(next, 2, 2)
                    && next.stop == segment_read_stop::limit
                    && holding.tasks == idle.tasks + 2
                    && holding.bytes >= idle.bytes + 2 * (64_KiB + 4 * unit),
                  "a read kept more runs than its session may, or its "
                  "reader was not charged for what it keeps");
                outliving = std::move(part.batches);
            }
            const auto still = resources.snapshot();
            require(
              outliving.size() == 2 && flat(outliving[0].bytes) == w.expected[0]
                && still.tasks == idle.tasks + 1 && still.bytes > idle.bytes,
              "a batch outlived what its read's reader was charged for it");
            outliving.clear();
            const auto after = resources.snapshot();
            require(
              after.tasks == idle.tasks && after.bytes == idle.bytes,
              "what a read was charged did not go back with its last batch");
        }
        {
            // A session that leaves nothing behind reads each run alone,
            // under its own budget, and the cache keeps none of them.
            scanned = w.runs(resources);
            auto scan = w.session(*scanned, resources, last, {.fill = false});
            const auto before = cache.snapshot();
            {
                const auto swept = co_await drive.lifecycle(
                  scan.read(offset(first), all, asking));
                const auto during = cache.snapshot();
                require(
                  w.matches(swept, 0, 5) && during.loads == before.loads + 3
                    && during.uncached == before.uncached + 3
                    && during.bytes == before.bytes
                    && !scanned->floor(offset(first), false) && !scanned->fed()
                    && scanned->prefetches() == 0,
                  "a session that leaves nothing behind left a run in the "
                  "cache, or did not read alone");
            }
            require(
              cache.snapshot().bytes == before.bytes,
              "a run read for one reader alone outlived what was read of it");
        }
        {
            // Two sessions that want the same run together: one read.
            shared = w.runs(resources);
            auto left = w.session(*shared, resources, last);
            auto right = w.session(*shared, resources, last);
            const auto before = cache.snapshot();
            auto one = left.read(offset(first), byte_count{1}, asking);
            auto two = right.read(offset(first), byte_count{1}, asking);
            const auto led = co_await drive.lifecycle(std::move(one));
            const auto joined = co_await drive.lifecycle(std::move(two));
            require(
              w.matches(led, 0, 1) && w.matches(joined, 0, 1)
                && cache.snapshot().loads == before.loads + 1,
              "two readers that wanted one run together read it twice");
        }
        // Everything a read came through goes: its session went already,
        // and now its segment's runs.
        for (auto* held : {&runs, &scanned, &shared}) {
            take(co_await drive.lifecycle((*held)->close()));
            held->reset();
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    for (auto* held : {&runs, &scanned, &shared}) {
        if (!*held) continue;
        try {
            failed.observe(co_await drive.lifecycle((*held)->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        held->reset();
    }
    co_await close_world(w, failed, drive);
    try {
        // What was read is still what was written, and still charged: the
        // cache is stopped and holds nothing else.
        if (kept && !failed.failed()) {
            bool same = kept->batches.size() == 5;
            for (std::size_t i = 0; same && i < 5; ++i)
                same = flat(kept->batches[i].bytes) == w.expected[i];
            require(
              same && cache.snapshot().bytes != 0,
              "what a read returned did not outlive what it was read "
              "through");
            kept.reset();
            require(
              cache.snapshot().bytes == 0,
              "a run nothing holds any more is still charged");
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    kept.reset();
    take(failed.outcome());
    require(
      w.residency.resident() == 0 && !w.residency.failure().failed(),
      "a closed index left its root open");
}

// A reader that reads runs in a row is read ahead of. The second run in a
// row asks for the one after it, once: a read nobody waits for, which takes
// what it needs without waiting and whose run is an entry like any other.
// The reader then finds it there. Nothing is asked for past what a session
// may see or past where it may read, by a session that was told not to ask,
// or by one that leaves nothing behind. A reader that leaves owes nothing
// for what it asked for. A session told to look further asks for that many
// runs, each once, or begins with one and looks one further each time it
// had to wait. An index is asked only for what it holds in memory: one that
// would have to read to say where the next run lies is not waited for. A
// cache that has no room for a run drops the request.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> prefetched_runs(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    // Five runs: four of two blocks and one of one.
    auto world = co_await open_world<Clock>(
      files, owner, spec, resources, work, drive, 9);
    auto& w = *world;
    auto& cache = *w.cache;
    std::unique_ptr<segment_runs> runs, narrow, short_of, quiet, left;
    std::unique_ptr<segment_runs> deep, growing, unanswered, crowded;
    // A cache too small to keep a run of this segment.
    std::unique_ptr<page_cache> tight;
    runtime::first_failure failed;
    try {
        const auto first = w.first();
        const auto last = w.last();
        seastar::abort_source asking;
        const byte_count all{1_MiB};
        const byte_count one{1};
        runs = w.runs(resources);
        {
            auto reader = w.session(*runs, resources, last);
            const auto before = cache.snapshot();
            // The first run: the index's routes and page, and the run.
            const auto a = co_await drive.lifecycle(
              reader.read(offset(w.base(0)), one, asking));
            const auto b = co_await drive.lifecycle(
              reader.read({a.next, one, true}, asking));
            const auto single = cache.snapshot();
            require(
              w.matches(a, 0, 1) && w.matches(b, 1, 1)
                && single.loads == before.loads + 3 && runs->prefetches() == 0,
              "a reader that read one run was read ahead of");
            // The second run in a row: it is read, and the third is asked
            // for.
            const auto c = co_await drive.lifecycle(
              reader.read({b.next, one, true}, asking));
            const auto second = cache.snapshot();
            require(
              w.matches(c, 2, 1) && second.loads == single.loads + 2
                && runs->prefetches() == 1 && second.uncached == 0,
              "the second run in a row did not ask for the one after it");
            // More of the second run: nothing is asked for twice.
            const auto d = co_await drive.lifecycle(
              reader.read({c.next, one, true}, asking));
            require(
              w.matches(d, 3, 1) && runs->prefetches() == 1
                && cache.snapshot().loads == second.loads,
              "a run was asked for twice");
            // The third run is there, or its read is joined: the reader
            // starts none of its own, and asks for the fourth.
            const auto e = co_await drive.lifecycle(
              reader.read({d.next, one, true}, asking));
            const auto third = cache.snapshot();
            require(
              w.matches(e, 4, 1) && third.loads == second.loads + 1
                && runs->prefetches() == 2,
              "a reader read for itself the run that was read ahead of it");
            // To the end: the fourth run was asked for, and the last when
            // the fourth was read.
            const auto rest = co_await drive.lifecycle(
              reader.read({e.next, all, true}, asking));
            require(
              w.matches(rest, 5, 4)
                && rest.stop == segment_read_stop::end_of_segment
                && runs->prefetches() == 3
                && cache.snapshot().loads == third.loads + 1
                && cache.snapshot().uncached == 0,
              "a reader in a row was not read ahead of to the end");
        }
        {
            // A session that may see up to the third run's first offset:
            // the third run is not asked for.
            narrow = w.runs(resources);
            auto reader = w.session(*narrow, resources, w.base(4));
            const auto before = cache.snapshot();
            const auto seen = co_await drive.lifecycle(
              reader.read(offset(first), all, asking));
            require(
              w.matches(seen, 0, 4)
                && seen.stop == segment_read_stop::not_yet_visible
                && narrow->prefetches() == 0
                && cache.snapshot().loads == before.loads + 2,
              "a run past what a session may see was read ahead");
        }
        {
            // A session that reads the file up to the end of the seventh
            // block: the third run lies below that and is asked for, the
            // fourth does not and is not. What of the fourth lies below
            // the bound is read for this reader alone.
            short_of = w.runs(resources);
            const runtime::file_position bound{
              w.segment.blocks[6].coverage().bytes().end().value()};
            auto reader = take(
              segment_reader::open(
                *short_of,
                segment_data_source{take(w.pin->share())},
                {.durable = bound,
                 .sealed = std::nullopt,
                 .visible = model::range_logical_end{last}},
                sealed_read_index(*w.index),
                resources,
                one_ahead));
            const auto before = cache.snapshot();
            const auto seen = co_await drive.lifecycle(
              reader.read(offset(first), all, asking));
            const auto after = cache.snapshot();
            require(
              w.matches(seen, 0, 7)
                && seen.stop == segment_read_stop::short_of_visibility
                && short_of->prefetches() == 1
                && after.loads == before.loads + 4
                && after.uncached == before.uncached + 1,
              "a run past where a session may read was read ahead, or one "
              "below it was not");
        }
        {
            // A session that was told not to ask.
            quiet = w.runs(resources);
            auto reader = w.session(*quiet, resources, last, {.read_ahead = 0});
            const auto seen = co_await drive.lifecycle(
              reader.read(offset(first), all, asking));
            require(
              w.matches(seen, 0, 9) && quiet->prefetches() == 0,
              "a session that was told not to ask was read ahead of");
        }
        {
            // A reader that asked and left: nobody waits for what it asked
            // for but the runs' own close, and nothing is owed for it.
            left = w.runs(resources);
            {
                auto reader = w.session(*left, resources, last);
                const auto a = co_await drive.lifecycle(
                  reader.read(offset(w.base(0)), one, asking));
                const auto c = co_await drive.lifecycle(
                  reader.read(offset(w.base(2)), one, asking));
                require(
                  w.matches(a, 0, 1) && w.matches(c, 2, 1)
                    && left->prefetches() == 1,
                  "the reader that leaves did not ask");
            }
            take(co_await drive.lifecycle(left->close()));
            left.reset();
        }
        // The sessions above are gone: what they read through goes, so
        // that what follows counts its own loads in a cache that holds
        // nothing else of the segment's data.
        for (auto* held : {&runs, &narrow, &short_of, &quiet}) {
            take(co_await drive.lifecycle((*held)->close()));
            held->reset();
        }
        {
            // Three runs ahead: the second run in a row asks for the three
            // after it at once, and nothing is asked for twice.
            deep = w.runs(resources);
            {
                auto reader = w.session(
                  *deep,
                  resources,
                  last,
                  {.read_ahead = 3, .read_ahead_grows = false});
                const auto before = cache.snapshot();
                const auto a = co_await drive.lifecycle(
                  reader.read(offset(w.base(0)), one, asking));
                const auto b = co_await drive.lifecycle(
                  reader.read({a.next, one, true}, asking));
                require(
                  w.matches(a, 0, 1) && w.matches(b, 1, 1)
                    && deep->prefetches() == 0
                    && cache.snapshot().loads == before.loads + 1,
                  "a reader that read one run was read ahead of");
                const auto c = co_await drive.lifecycle(
                  reader.read({b.next, one, true}, asking));
                require(
                  w.matches(c, 2, 1) && deep->prefetches() == 3
                    && cache.snapshot().loads == before.loads + 5
                    && deep->holds(offset(w.base(4)))
                    && deep->holds(offset(w.base(6)))
                    && deep->holds(offset(w.base(8))),
                  "a session told to look three runs ahead did not ask for "
                  "them");
                const auto rest = co_await drive.lifecycle(
                  reader.read({c.next, all, true}, asking));
                require(
                  w.matches(rest, 3, 6)
                    && rest.stop == segment_read_stop::end_of_segment
                    && deep->prefetches() == 3
                    && cache.snapshot().loads == before.loads + 5
                    && cache.snapshot().uncached == before.uncached,
                  "a run that was read ahead was asked for again, or read by "
                  "its reader");
                // More than the most a session may be told is refused.
                const auto refused = segment_reader::open(
                  *deep,
                  segment_data_source{take(w.pin->share())},
                  reader.bounds(),
                  sealed_read_index(*w.index),
                  resources,
                  {.read_ahead = maximum_read_ahead + 1});
                require(
                  !refused && refused.error().code() == errc::invalid_argument,
                  "a session was told to look further than any may");
            }
            take(co_await drive.lifecycle(deep->close()));
            deep.reset();
        }
        {
            // One run ahead to begin with, and one further each time the
            // reader had to wait for a run in a row.
            growing = w.runs(resources);
            {
                auto reader = w.session(
                  *growing,
                  resources,
                  last,
                  {.read_ahead = 3, .read_ahead_grows = true});
                const auto before = cache.snapshot();
                const auto a = co_await drive.lifecycle(
                  reader.read(offset(w.base(0)), one, asking));
                const auto b = co_await drive.lifecycle(
                  reader.read({a.next, one, true}, asking));
                // The second run was asked for by nobody: its reader waits
                // for it, and looks two runs ahead from there.
                const auto c = co_await drive.lifecycle(
                  reader.read({b.next, one, true}, asking));
                require(
                  w.matches(a, 0, 1) && w.matches(b, 1, 1) && w.matches(c, 2, 1)
                    && growing->prefetches() == 2
                    && cache.snapshot().loads == before.loads + 4,
                  "a reader that waited for a run in a row did not look "
                  "further");
                const auto rest = co_await drive.lifecycle(
                  reader.read({c.next, all, true}, asking));
                require(
                  w.matches(rest, 3, 6)
                    && rest.stop == segment_read_stop::end_of_segment
                    && growing->prefetches() == 3
                    && cache.snapshot().loads == before.loads + 5,
                  "a session that looks further as it waits did not read "
                  "ahead to the end");
            }
            take(co_await drive.lifecycle(growing->close()));
            growing.reset();
        }
        {
            // An index that holds nothing in memory of where the next run
            // lies: whoever asks ahead of itself is told nothing, is not
            // kept waiting, and asks for nothing.
            unanswered = w.runs(resources);
            {
                auto reader = take(
                  segment_reader::open(
                    *unanswered,
                    segment_data_source{take(w.pin->share())},
                    {.durable = w.bound(),
                     .sealed = model::range_logical_end{last},
                     .visible = model::range_logical_end{last}},
                    segment_read_index{
                      [&w](
                        model::range_logical_offset target,
                        seastar::abort_source& caller,
                        sparse_index_place&) {
                          return w.index->locate(target, caller);
                      },
                      [&w](
                        const sparse_index_answer& given,
                        runtime::operation_error why) noexcept {
                          w.index->report(given, why);
                      },
                      [](
                        model::range_logical_offset,
                        const sparse_index_place&) noexcept {
                          return std::optional<sparse_index_answer>{};
                      }},
                    resources,
                    one_ahead));
                const auto before = cache.snapshot();
                const auto seen = co_await drive.lifecycle(
                  reader.read(offset(first), all, asking));
                require(
                  w.matches(seen, 0, 9)
                    && seen.stop == segment_read_stop::end_of_segment
                    && unanswered->prefetches() == 0
                    && cache.snapshot().loads == before.loads + 5,
                  "a reader was read ahead of where its index could not say "
                  "at once where the next run lies");
            }
            take(co_await drive.lifecycle(unanswered->close()));
            unanswered.reset();
        }
        {
            // A cache with no room for a run: each is read for its reader
            // alone, and what the reader asks for ahead of itself is
            // dropped.
            tight = take(
              page_cache::make(resources, {.capacity = byte_count{128_KiB}}));
            crowded = take(
              segment_runs::make(
                *tight,
                resources,
                w.segment.history(),
                {.limits = run_limits()}));
            {
                auto reader = w.session(*crowded, resources, last);
                const auto seen = co_await drive.lifecycle(
                  reader.read(offset(first), all, asking));
                require(
                  w.matches(seen, 0, 9)
                    && seen.stop == segment_read_stop::end_of_segment
                    && crowded->prefetches() == 0
                    && tight->snapshot().uncached == 5
                    && tight->snapshot().loads == 5
                    && tight->snapshot().refused != 0,
                  "a run the cache has no room for was kept, or was read "
                  "ahead");
            }
            require(
              tight->snapshot().bytes == 0,
              "a run read for one reader alone outlived what was read of it");
            take(co_await drive.lifecycle(crowded->close()));
            crowded.reset();
        }
        require(
          cache.snapshot().refused == 0,
          "a read ahead was refused where there was room");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    for (auto* held :
         {&runs,
          &narrow,
          &short_of,
          &quiet,
          &left,
          &deep,
          &growing,
          &unanswered,
          &crowded}) {
        if (!*held) continue;
        try {
            failed.observe(co_await drive.lifecycle((*held)->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        held->reset();
    }
    if (tight) {
        try {
            co_await drive.lifecycle(tight->stop());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        tight.reset();
    }
    co_await close_world(w, failed, drive);
    take(failed.outcome());
    require(
      cache.snapshot().bytes == 0 && w.residency.resident() == 0,
      "what was read ahead is still charged, or an index left its root open");
}

// What a session does when nothing is said of reading ahead. It begins one
// run ahead and looks one further each time it had to wait for a run in a
// row; what it learned of one sequence it does not carry to the next; it
// asks for no more bytes ahead than its window, the first run whatever its
// length; and two runs that do not follow one another are not a sequence.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> ahead_reads(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    // Nine runs of one block.
    auto world = co_await open_world<Clock>(
      files, owner, spec, resources, work, drive, 9, 1);
    auto& w = *world;
    auto& cache = *w.cache;
    std::unique_ptr<segment_runs> plain, windowed, apart;
    runtime::first_failure failed;
    try {
        const auto last = w.last();
        seastar::abort_source asking;
        const byte_count all{1_MiB};
        const byte_count one{1};
        const auto asked = [&w](const segment_runs& runs, std::size_t block) {
            return runs.holds(offset(w.base(block)));
        };
        // The index is read first, so that what follows reads data alone.
        take(
          co_await drive.lifecycle(w.index->locate(offset(w.first()), asking)));
        {
            plain = w.runs(resources);
            auto reader = w.session(
              *plain, resources, last, segment_reader_config{});
            const auto before = cache.snapshot();
            const auto a = co_await drive.lifecycle(
              reader.read(offset(w.base(0)), one, asking));
            // The second run in a row, which nobody asked for: its reader
            // waits for it, and looks two runs ahead from there.
            const auto b = co_await drive.lifecycle(
              reader.read(offset(w.base(1)), one, asking));
            require(
              w.matches(a, 0, 1) && w.matches(b, 1, 1)
                && plain->prefetches() == 2 && asked(*plain, 2)
                && asked(*plain, 3) && !asked(*plain, 4)
                && cache.snapshot().loads == before.loads + 4,
              "a reader that waited for the second run in a row did not "
              "look two runs ahead");
            // Elsewhere: one run, which is no sequence, and then the run
            // after it. The session waits for both itself and begins
            // again: two runs ahead, not three.
            const auto c = co_await drive.lifecycle(
              reader.read(offset(w.base(4)), one, asking));
            const auto apart_asked = plain->prefetches();
            const auto d = co_await drive.lifecycle(
              reader.read(offset(w.base(5)), one, asking));
            require(
              w.matches(c, 4, 1) && apart_asked == 2 && w.matches(d, 5, 1)
                && plain->prefetches() == 4 && asked(*plain, 6)
                && asked(*plain, 7) && !asked(*plain, 8),
              "a session carried how far it looked ahead from one sequence "
              "to the next");
            // To the end: every run was asked for once, by this reader or
            // ahead of it.
            const auto rest = co_await drive.lifecycle(
              reader.read({d.next, all, true}, asking));
            require(
              w.matches(rest, 6, 3)
                && rest.stop == segment_read_stop::end_of_segment
                && plain->prefetches() == 5
                && cache.snapshot().loads == before.loads + 9
                && cache.snapshot().uncached == before.uncached,
              "a session that looks further as it waits did not read ahead "
              "to the end, or a run was read twice");
        }
        take(co_await drive.lifecycle(plain->close()));
        plain.reset();
        {
            // A window of one run: the second run in a row asks for the one
            // after it and for no more, though the session looks two runs
            // ahead by then.
            windowed = w.runs(resources);
            const auto unit = segments::descriptor().alignment.bytes().value();
            auto reader = w.session(
              *windowed,
              resources,
              last,
              {.read_ahead_bytes = byte_count{2 * unit}});
            const auto a = co_await drive.lifecycle(
              reader.read(offset(w.base(0)), one, asking));
            const auto b = co_await drive.lifecycle(
              reader.read(offset(w.base(1)), one, asking));
            require(
              w.matches(a, 0, 1) && w.matches(b, 1, 1)
                && windowed->prefetches() == 1 && asked(*windowed, 2)
                && !asked(*windowed, 3),
              "a session asked for more bytes ahead than its window");
        }
        take(co_await drive.lifecycle(windowed->close()));
        windowed.reset();
        {
            // Two runs that do not follow one another are not two in a
            // row: nothing is asked for until one follows the other.
            apart = w.runs(resources);
            auto reader = w.session(
              *apart, resources, last, segment_reader_config{});
            const auto a = co_await drive.lifecycle(
              reader.read(offset(w.base(0)), one, asking));
            const auto c = co_await drive.lifecycle(
              reader.read(offset(w.base(2)), one, asking));
            const auto none = apart->prefetches();
            const auto d = co_await drive.lifecycle(
              reader.read(offset(w.base(3)), one, asking));
            require(
              w.matches(a, 0, 1) && w.matches(c, 2, 1) && none == 0
                && w.matches(d, 3, 1) && apart->prefetches() == 2
                && asked(*apart, 4) && asked(*apart, 5) && !asked(*apart, 6),
              "two runs that do not follow one another were read ahead of, "
              "or two that do were not");
        }
        take(co_await drive.lifecycle(apart->close()));
        apart.reset();
    } catch (...) {
        failed.observe(std::current_exception());
    }
    for (auto* held : {&plain, &windowed, &apart}) {
        if (!*held) continue;
        try {
            failed.observe(co_await drive.lifecycle((*held)->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        held->reset();
    }
    co_await close_world(w, failed, drive);
    take(failed.outcome());
    require(
      cache.snapshot().bytes == 0 && w.residency.resident() == 0,
      "what was read ahead is still charged, or an index left its root open");
}

// The tail of a segment that is still appended to. A segment nobody reads is
// not fed. Once it has a reader the feed begins with the next anchor, and
// what lies before that is read from the device for its reader alone. A fed
// run is read from memory as far as the segment is durable, also while it is
// still filling, and is byte for byte what a read from the device returns.
// It ends where the index chooses the next anchor and answers from the cache
// once the durable position has passed it. A segment that stops being read
// stops being fed, and the run that was filling is read from the device.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> fed_tail(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    auto config = segments::configuration();
    config.maximum_groups = 8;
    config.admission.working_bytes = byte_count{1_MiB};
    config.admission.maximum_blocks = 16;
    const auto described = segments::descriptor();
    const auto unit = described.alignment.bytes().value();
    const auto stride = paired_stride();
    auto index = std::make_unique<active_sparse_index>(take(
      active_sparse_index::make(
        stride,
        sparse_index_capacity(
          stride,
          described.maximum_data_bytes,
          config.admission.maximum_blocks),
        resources)));
    auto cache = take(
      page_cache::make(resources, {.capacity = byte_count{2_MiB}}));
    auto writer = take(
      writer_type::make_new(
        files, owner, spec, 0, described, resources, config));
    std::unique_ptr<segment_runs> runs, device;
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        std::optional<segment_history_context> origin;
        runtime::file_position start;
        {
            const auto view = take(
              co_await drive.lifecycle(writer->pin_data(work)));
            origin.emplace(view.history);
            start = view.durable;
        }
        const auto history = *origin;
        const auto make_runs = [&] {
            return take(
              segment_runs::make(
                *cache, resources, history, {.limits = run_limits()}));
        };
        runs = make_runs();
        take(writer->observe_blocks(runs->observer(*index)));
        const auto first = history.logical_origin.value();
        const auto at = [&](std::uint64_t group) {
            return start.value() + group * 2 * unit;
        };
        std::string stored;
        codec::crc32c checksum;
        // One block and its footer to a group, a record to a block.
        const auto write =
          [&](
            std::uint64_t group) -> seastar::future<segment_captured_boundary> {
            std::vector<encoded_assigned_batch> batches;
            if (group == 0)
                batches.push_back(co_await segments::child(work));
            else
                batches.push_back(
                  co_await segments::execution_child(first + group, work));
            auto [cut, bytes] = co_await segments::write_group(
              *writer, std::move(batches), resources, work, drive, checksum);
            require(
              bytes.size() == 2 * unit
                && cut.end().bytes.value() == at(group + 1),
              "a group is not one block and its footer");
            stored += bytes;
            co_return cut;
        };
        const auto sync =
          [&](segment_captured_boundary cut) -> seastar::future<> {
            const auto synced = co_await drive.lifecycle(writer->barrier(cut));
            take(synced.failure.outcome());
            co_await drive.lifecycle(runs->settled());
        };
        const auto session =
          [&](segment_runs& through) -> seastar::future<segment_reader> {
            auto view = take(co_await drive.lifecycle(writer->pin_data(work)));
            co_return take(
              segment_reader::open(
                through,
                segment_data_source{std::move(view.pin)},
                {.durable = view.durable,
                 .sealed = std::nullopt,
                 .visible = model::range_logical_end{first + 64}},
                active_read_index(*index, view.durable),
                resources,
                one_ahead));
        };
        const auto matches =
          [&](const segment_read& read, std::uint64_t from, std::size_t count) {
              if (read.batches.size() != count) return false;
              for (std::size_t i = 0; i < count; ++i) {
                  const auto& batch = read.batches[i];
                  if (
                    batch.logical.begin().value() != first + from + i
                    || flat(batch.bytes)
                         != batch_at(stored, (from + i) * 2 * unit))
                      return false;
              }
              return true;
          };
        seastar::abort_source asking;
        const byte_count all{1_MiB};

        // Nobody reads the segment: its writer hands nothing over.
        co_await sync(co_await write(0));
        require(
          !runs->fed() && runs->pending() == 0 && runs->dropped() == 0
            && cache->snapshot().loads == 0 && index->size() == 1,
          "a segment nobody reads was fed");
        // It has a reader. The block written next is not an anchor: the
        // feed begins with the next one, which is the block after it.
        runs->establish(1);
        const auto second = co_await write(1);
        co_await drive.lifecycle(runs->settled());
        require(
          runs->fed() && runs->pending() == 0 && cache->snapshot().loads == 0,
          "the feed began before an anchor");
        const auto third = co_await write(2);
        co_await drive.lifecycle(runs->settled());
        require(
          runs->pending() == 1 && cache->snapshot().loads == 1
            && cache->snapshot().bytes != 0,
          "a run that begins with an anchor was not fed");
        {
            // Durable up to the first group. The fed run is not: a reader
            // is given the first batch, read from the device for it alone.
            auto reader = co_await session(*runs);
            const auto before = cache->snapshot();
            const auto read = co_await drive.lifecycle(
              reader.read(offset(first), all, asking));
            const auto after = cache->snapshot();
            require(
              reader.bounds().durable.value() == at(1) && matches(read, 0, 1)
                && read.stop == segment_read_stop::short_of_visibility
                && read.next
                     == segment_read_position{model::range_logical_end{first + 1}, runtime::file_position{at(1)}}
                && after.loads == before.loads + 1
                && after.uncached == before.uncached + 1,
              "a reader was given what is not durable, or the unfinished "
              "run was not read for it alone");
        }
        co_await sync(third);
        static_cast<void>(second);
        {
            // Durable up to the third group. The run that is filling holds
            // the third block: it is read from memory.
            auto reader = co_await session(*runs);
            const auto before = cache->snapshot();
            const auto fed = co_await drive.lifecycle(
              reader.read(offset(first + 2), all, asking));
            const auto after = cache->snapshot();
            require(
              reader.bounds().durable.value() == at(3) && runs->pending() == 1
                && matches(fed, 2, 1) && after.loads == before.loads
                && after.uncached == before.uncached,
              "the run a segment is filling was not read from memory below "
              "the durable position");
            // From the first batch: the first run was never fed, and is
            // read from the device once, for the cache.
            const auto whole = co_await drive.lifecycle(
              reader.read(offset(first), all, asking));
            const auto loaded = cache->snapshot();
            require(
              matches(whole, 0, 3) && loaded.loads == after.loads + 1
                && loaded.uncached == after.uncached,
              "a run that was not fed was not read from the device for the "
              "cache");
        }
        // The fourth block goes to the run that is filling. The fifth is an
        // anchor: that run ends, and the next begins.
        static_cast<void>(co_await write(3));
        const auto fifth = co_await write(4);
        co_await drive.lifecycle(runs->settled());
        require(
          runs->pending() == 2 && !runs->floor(offset(first + 2))
            && runs->floor(offset(first)),
          "a run that is not durable answered from the cache, or the next "
          "anchor did not end it");
        co_await sync(fifth);
        require(
          runs->pending() == 1 && runs->dropped() == 0,
          "a fed run that is complete and durable is not the cache's");
        {
            const auto kept = runs->floor(offset(first + 2));
            require(
              kept && kept->blocks().size() == 2
                && kept->begin().value() == at(2)
                && kept->end().value() == at(4),
              "a fed run is not the stretch from its anchor to the next");
            // The same stretch read back from the device, through runs
            // nobody feeds: the same blocks and the same bytes.
            device = make_runs();
            const auto view = take(
              co_await drive.lifecycle(writer->pin_data(work)));
            const segment_data_source source{view.pin.share()};
            const auto read = take(
              co_await drive.lifecycle(device->fetch(
                kept->at(0), kept->end(), source, view.durable, asking, 0)));
            bool same = read->blocks().size() == 2;
            for (std::size_t i = 0; same && i < 2; ++i) {
                const auto& fed = kept->blocks()[i];
                const auto& back = read->blocks()[i];
                same = fed.logical == back.logical
                       && fed.physical == back.physical
                       && fed.offset == back.offset && fed.bytes == back.bytes
                       && fed.batch_offset == back.batch_offset
                       && fed.batch_bytes == back.batch_bytes
                       && flat(take(kept->batch(i)))
                            == flat(take(read->batch(i)))
                       && flat(take(kept->batch(i)))
                            == batch_at(stored, (2 + i) * 2 * unit);
            }
            require(
              same && read->end() == kept->end(),
              "a fed run is not what a read from the device returns");
        }
        {
            // The whole tail from memory: the fed run the cache keeps, and
            // the fifth block from the run that is filling.
            auto reader = co_await session(*runs);
            const auto before = cache->snapshot();
            const auto tail = co_await drive.lifecycle(
              reader.read(offset(first + 2), all, asking));
            const auto after = cache->snapshot();
            require(
              reader.bounds().durable.value() == at(5) && matches(tail, 2, 3)
                && after.loads == before.loads
                && after.uncached == before.uncached,
              "a tail that was fed was read from the device");
        }
        // Nobody reads the segment any more: the run that was filling is
        // given up, and what it held is read from the device.
        runs->establish(0);
        co_await drive.lifecycle(runs->settled());
        require(
          !runs->fed() && runs->pending() == 0,
          "a segment that stopped being read is still fed");
        segment_read_position resumed;
        {
            auto reader = co_await session(*runs);
            const auto before = cache->snapshot();
            const auto read = co_await drive.lifecycle(
              reader.read(offset(first + 4), all, asking));
            const auto after = cache->snapshot();
            require(
              matches(read, 4, 1) && after.loads == before.loads + 1
                && after.uncached == before.uncached + 1
                && read.batches[0].held.value()->begin().value() == at(4),
              "a run the feed gave up was not read from the device");
            resumed = read.next;
        }
        // One more block, which nobody feeds. A read that goes on from
        // where the last one said begins at that block, not at the anchor
        // before it; a place that leads to no block is not believed, and
        // the anchor is read from.
        co_await sync(co_await write(5));
        {
            auto reader = co_await session(*runs);
            const auto from_block = co_await drive.lifecycle(
              reader.read({resumed, all, true}, asking));
            auto misled = resumed;
            misled.block = runtime::file_position{at(5) + unit};
            const auto from_anchor = co_await drive.lifecycle(
              reader.read({misled, all, true}, asking));
            require(
              resumed
                  == segment_read_position{model::range_logical_end{first + 5}, runtime::file_position{at(5)}}
                && matches(from_block, 5, 1)
                && from_block.batches[0].held.value()->begin().value() == at(5)
                && from_block.batches[0].held.value()->blocks().size() == 1
                && matches(from_anchor, 5, 1)
                && from_anchor.batches[0].held.value()->begin().value() == at(4)
                && from_anchor.batches[0].held.value()->blocks().size() == 2,
              "a read did not go on from the block the last one named, or "
              "believed a place that led to no block");
        }
        require(
          !writer->failure().failed(),
          "feeding or reading a segment failed it");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    for (auto* held : {&runs, &device}) {
        if (!*held) continue;
        try {
            failed.observe(co_await drive.lifecycle((*held)->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    try {
        failed.observe(co_await drive.lifecycle(writer->close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    // The writer's observer refers to the runs and the index until here.
    writer.reset();
    runs.reset();
    device.reset();
    try {
        co_await drive.lifecycle(cache->stop());
    } catch (...) {
        failed.observe(std::current_exception());
    }
    cache.reset();
    index.reset();
    take(failed.outcome());
}

// One small block in a group of its own, as a sealed segment's blocks are
// written: the group's boundary once its bytes are written.
template<typename Writer, typename Driver>
seastar::future<segment_captured_boundary> append_block(
  Writer& writer,
  std::uint64_t base,
  bool first,
  workload_budget& resources,
  codec::cooperative_work& work,
  Driver drive) {
    std::optional<encoded_assigned_batch> batch;
    if (first)
        batch.emplace(co_await segments::child(work));
    else
        batch.emplace(co_await segments::execution_child(base, work));
    auto group = co_await segments::freeze_child(
      writer, std::move(*batch), resources, work);
    take(co_await writer.encode_group(group, work));
    auto submitted = take(writer.submit(std::move(group), work));
    const auto written = co_await drive.lifecycle(std::move(submitted.written));
    take(written.failure.outcome());
    co_return submitted.boundary;
}

// A seal, and the drain of a segment's readers. A seal changes nothing a
// reader holds: what it was given stays what it was, its session reads on
// under the bounds it took, its pin still reads the file, and the run the
// segment was filling is whole and answers from the cache like any other, as
// the sealed segment's run. Once a segment's readers are drained no session
// is let in; a session that is in is served what the cache holds and
// refused what is not there; and the drain ends when the last session has
// gone. What was read outlives all of it.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> sealed_tail(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    constexpr std::uint32_t groups = 4;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    auto config = segments::configuration();
    config.maximum_groups = groups;
    config.admission.working_bytes = byte_count{1_MiB};
    config.admission.maximum_blocks = 16;
    const auto described = segments::descriptor();
    const auto unit = described.alignment.bytes().value();
    const auto stride = paired_stride();
    auto index = std::make_unique<active_sparse_index>(take(
      active_sparse_index::make(
        stride,
        sparse_index_capacity(
          stride,
          described.maximum_data_bytes,
          config.admission.maximum_blocks),
        resources)));
    auto cache = take(
      page_cache::make(resources, {.capacity = byte_count{2_MiB}}));
    auto writer = take(
      writer_type::make_new(
        files, owner, spec, 0, described, resources, config));
    std::unique_ptr<segment_runs> runs, fresh;
    // A reader's pin taken before the seal, for the sessions opened after.
    std::optional<segment_data_view> pinned;
    std::optional<segment_reader> tailing, bypassing, late;
    // The drain that waits for a session, kept where the case's end finds
    // it.
    std::optional<seastar::future<runtime::result<void>>> draining;
    std::optional<segment_read> early;
    std::vector<std::string> expected;
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        pinned.emplace(take(co_await drive.lifecycle(writer->pin_data(work))));
        const auto history = pinned->history;
        const auto start = pinned->durable;
        const auto first = history.logical_origin.value();
        const auto at = [&](std::uint64_t group) {
            return start.value() + group * 2 * unit;
        };
        const auto make_runs = [&] {
            return take(
              segment_runs::make(
                *cache, resources, history, {.limits = run_limits()}));
        };
        runs = make_runs();
        fresh = make_runs();
        take(writer->observe_blocks(runs->observer(*index)));
        // It has a reader from its first block on.
        runs->establish(1);
        std::optional<segment_captured_boundary> cut;
        for (std::uint32_t i = 0; i < groups; ++i)
            cut.emplace(
              co_await append_block(
                *writer, first + i, i == 0, resources, work, drive));
        {
            const auto synced = co_await drive.lifecycle(writer->barrier(*cut));
            take(synced.failure.outcome());
        }
        co_await drive.lifecycle(runs->settled());
        require(
          cut->end().bytes.value() == at(groups) && index->size() == 2
            && runs->pending() == 1 && runs->dropped() == 0
            && runs->floor(offset(first)) && !runs->floor(offset(first + 2)),
          "the segment's tail is not the one this case is about");
        const auto session =
          [&](segment_runs& through) -> seastar::future<segment_reader> {
            auto view = take(co_await drive.lifecycle(writer->pin_data(work)));
            co_return take(
              segment_reader::open(
                through,
                segment_data_source{std::move(view.pin)},
                {.durable = view.durable,
                 .sealed = std::nullopt,
                 .visible = model::range_logical_end{first + 64}},
                active_read_index(*index, view.durable),
                resources,
                one_ahead));
        };
        seastar::abort_source asking;
        const byte_count all{1_MiB};
        tailing.emplace(co_await session(*runs));
        bypassing.emplace(co_await session(*fresh));
        early.emplace(
          co_await drive.lifecycle(
            tailing->read(offset(first), byte_count{1}, asking)));
        require(
          early->batches.size() == 1
            && tailing->bounds().durable.value() == at(groups),
          "a reader at the tail was not given its first batch");

        // The seal. The run that was filling is whole.
        std::uint32_t calls = 0;
        const auto sealed = co_await drive.lifecycle(writer->seal(
          segments::completed_source{{}, {}, &calls},
          0,
          groups,
          work,
          sparse_index_seal{
            index.get(), local_object_sequence::make(46).value()}));
        take(sealed.failure.outcome());
        require(
          sealed.extent.has_value() && index->frozen(),
          "the segment was not sealed");
        runs->seal();
        co_await drive.lifecycle(runs->settled());
        const auto data = co_await store_contract::read_all_bytes(
          files, indexes::data_path(spec), drive);
        for (std::uint32_t i = 0; i < groups; ++i)
            expected.push_back(batch_at(data, at(i)));
        const auto matches =
          [&](const segment_read& read, std::uint64_t from, std::size_t count) {
              if (read.batches.size() != count) return false;
              for (std::size_t i = 0; i < count; ++i)
                  if (
                    read.batches[i].logical.begin().value() != first + from + i
                    || flat(read.batches[i].bytes) != expected[from + i])
                      return false;
              return true;
          };
        const auto kept = cache->snapshot();
        require(
          runs->pending() == 0 && !runs->fed()
            && runs->floor(offset(first + 2), false) && matches(*early, 0, 1),
          "the run a segment was filling when it sealed is not the cache's, "
          "or what a reader held changed");
        {
            // The session that was at the tail reads on, from where it
            // was, under the bounds it took: from memory.
            const auto rest = co_await drive.lifecycle(
              tailing->read({early->next, all, true}, asking));
            // A session opened before the seal on runs that hold nothing
            // reads the file through the pin it took then.
            const auto through = co_await drive.lifecycle(
              bypassing->read(offset(first), all, asking));
            const auto after = cache->snapshot();
            require(
              matches(rest, 1, 3)
                && rest.stop == segment_read_stop::short_of_visibility
                && matches(through, 0, 4) && after.loads == kept.loads + 2
                && after.uncached == kept.uncached + 1,
              "a seal changed what a session reads, or closed the file "
              "under a reader's pin");
        }
        {
            // A session with the sealed segment's bounds: its runs are the
            // runs the cache kept of the segment while it was written.
            const auto extent = sealed.extent->coverage;
            late.emplace(take(
              segment_reader::open(
                *runs,
                segment_data_source{pinned->pin.share()},
                {.durable = extent.bytes().end(),
                 .sealed = extent.logical().end(),
                 .visible = extent.logical().end()},
                active_read_index(*index, extent.bytes().end()),
                resources,
                one_ahead)));
            const auto before = cache->snapshot();
            const auto whole = co_await drive.lifecycle(
              late->read(offset(first), all, asking));
            require(
              matches(whole, 0, 4)
                && whole.stop == segment_read_stop::end_of_segment
                && cache->snapshot().loads == before.loads,
              "a sealed segment's reader did not read the runs its tail "
              "left in the cache");
            late.reset();
        }
        {
            // The drain. No session is let in. The one that is in is given
            // what the cache holds and refused what is not there, and the
            // drain waits for it.
            draining.emplace(fresh->close());
            const auto let_in = segment_reader::open(
              *fresh,
              segment_data_source{pinned->pin.share()},
              bypassing->bounds(),
              active_read_index(*index, bypassing->bounds().durable),
              resources,
              one_ahead);
            const auto held = co_await drive.lifecycle(
              bypassing->read(offset(first), byte_count{1}, asking));
            const auto denied = co_await drive.lifecycle(
              bypassing->read(offset(first + 2), all, asking));
            require(
              !let_in && let_in.error().code() == errc::closed
                && matches(held, 0, 1) && denied.batches.empty()
                && denied.stop == segment_read_stop::failed && denied.error
                && denied.error->code() == errc::closed
                && !draining->available(),
              "a drain let a reader in, served what the cache did not "
              "hold, or did not wait for the session that was in");
            bypassing.reset();
            take(co_await drive.lifecycle(std::move(*draining)));
            draining.reset();
            require(
              !fresh->failure().failed(),
              "a drain that failed nothing kept a failure");
        }
        tailing.reset();
        take(co_await drive.lifecycle(runs->close()));
        require(
          matches(*early, 0, 1) && !writer->failure().failed(),
          "what a reader was given did not outlive the drain, or reading "
          "failed the segment");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    late.reset();
    bypassing.reset();
    tailing.reset();
    if (draining) {
        try {
            failed.observe(co_await drive.lifecycle(std::move(*draining)));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        draining.reset();
    }
    for (auto* held : {&runs, &fresh}) {
        if (!*held) continue;
        try {
            failed.observe(co_await drive.lifecycle((*held)->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    early.reset();
    pinned.reset();
    try {
        failed.observe(co_await drive.lifecycle(writer->close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    // The writer's observer refers to the runs and the index until here.
    writer.reset();
    runs.reset();
    fresh.reset();
    try {
        co_await drive.lifecycle(cache->stop());
    } catch (...) {
        failed.observe(std::current_exception());
    }
    cache.reset();
    index.reset();
    take(failed.outcome());
}

// What a segment's owner does to its sessions. At the seal it moves them:
// from then on no session holds the pin its writer gave it or refers to the
// index its writer fed, so the writer closes with the sessions open and that
// index is destroyed, and every session, one whose read was in flight at the
// move included, reads on to the bound it took: from the cache, through the
// generation's pin and by the sealed index. At the drain it revokes them: a
// read in flight ends with whole batches or with closed, every later read
// fails with closed, the drains of the runs, of the index and of the
// generation do not wait for a session that is still held, and what a
// session returned is unchanged.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> moved_sessions(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    constexpr std::uint32_t groups = 4;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    auto config = segments::configuration();
    config.maximum_groups = groups;
    config.admission.working_bytes = byte_count{1_MiB};
    config.admission.maximum_blocks = 16;
    const auto described = segments::descriptor();
    const auto unit = described.alignment.bytes().value();
    const auto stride = paired_stride();
    auto index = std::make_unique<active_sparse_index>(take(
      active_sparse_index::make(
        stride,
        sparse_index_capacity(
          stride,
          described.maximum_data_bytes,
          config.admission.maximum_blocks),
        resources)));
    auto cache = take(
      page_cache::make(resources, {.capacity = byte_count{2_MiB}}));
    auto writer = take(
      writer_type::make_new(
        files, owner, spec, 0, described, resources, config));
    sparse_index_residency residency{1};
    sparse_index_rebuild_limit rebuilds;
    std::unique_ptr<segment_runs> runs, fresh, apart;
    std::unique_ptr<local_generation_owner> generation;
    std::unique_ptr<sealed_sparse_index<Backend, Owner>> sealed_index;
    std::optional<local_generation_pin> pin;
    // Before the sessions, which leave it as they go.
    segment_sessions sessions;
    std::optional<segment_reader> tailing, bypassing, cold;
    std::optional<seastar::future<segment_read>> pending;
    std::optional<segment_read> early, through;
    std::vector<std::string> expected;
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        // Where the segment's data begins. The pin that came with the
        // answer is let go at once: the writer's close waits for every pin,
        // and the sessions' are the only ones this case is about.
        std::optional<segment_data_view> origin{
          take(co_await drive.lifecycle(writer->pin_data(work)))};
        const auto history = origin->history;
        const auto start = origin->durable;
        origin.reset();
        const auto first = history.logical_origin.value();
        const auto at = [&](std::uint64_t group) {
            return start.value() + group * 2 * unit;
        };
        const auto make_runs = [&] {
            return take(
              segment_runs::make(
                *cache, resources, history, {.limits = run_limits()}));
        };
        runs = make_runs();
        fresh = make_runs();
        apart = make_runs();
        take(writer->observe_blocks(runs->observer(*index)));
        runs->establish(1);
        std::optional<segment_captured_boundary> cut;
        for (std::uint32_t i = 0; i < groups; ++i)
            cut.emplace(
              co_await append_block(
                *writer, first + i, i == 0, resources, work, drive));
        {
            const auto synced = co_await drive.lifecycle(writer->barrier(*cut));
            take(synced.failure.outcome());
        }
        co_await drive.lifecycle(runs->settled());
        require(
          cut->end().bytes.value() == at(groups) && index->size() == 2,
          "the segment's tail is not the one this case is about");
        const auto session =
          [&](segment_runs& through_runs) -> seastar::future<segment_reader> {
            auto view = take(co_await drive.lifecycle(writer->pin_data(work)));
            co_return take(
              segment_reader::open(
                through_runs,
                segment_data_source{std::move(view.pin)},
                {.durable = view.durable,
                 .sealed = std::nullopt,
                 .visible = model::range_logical_end{first + 64}},
                active_read_index(*index, view.durable),
                resources,
                one_ahead,
                &sessions));
        };
        seastar::abort_source asking;
        const byte_count all{1_MiB};
        tailing.emplace(co_await session(*runs));
        bypassing.emplace(co_await session(*fresh));
        cold.emplace(co_await session(*apart));
        early.emplace(
          co_await drive.lifecycle(
            tailing->read(offset(first), byte_count{1}, asking)));
        require(
          early->batches.size() == 1 && sessions.size() == 3
            && tailing->bounds().durable.value() == at(groups),
          "a session was not listed, or was not given its first batch");

        std::uint32_t calls = 0;
        const auto sealed = co_await drive.lifecycle(writer->seal(
          segments::completed_source{{}, {}, &calls},
          0,
          groups,
          work,
          sparse_index_seal{
            index.get(), local_object_sequence::make(46).value()}));
        take(sealed.failure.outcome());
        require(
          sealed.extent && sealed.index && sealed.boundary && index->frozen(),
          "the segment was not sealed with its index");
        runs->seal();
        co_await drive.lifecycle(runs->settled());
        const auto data = co_await store_contract::read_all_bytes(
          files, indexes::data_path(spec), drive);
        for (std::uint32_t i = 0; i < groups; ++i)
            expected.push_back(batch_at(data, at(i)));
        const auto matches =
          [&](const segment_read& read, std::uint64_t from, std::size_t count) {
              if (read.batches.size() != count) return false;
              for (std::size_t i = 0; i < count; ++i)
                  if (
                    read.batches[i].logical.begin().value() != first + from + i
                    || flat(read.batches[i].bytes) != expected[from + i])
                      return false;
              return true;
          };
        {
            // The generation, opened against what the writer says it
            // published, and the sealed segment's index.
            const auto published = writer->published();
            require(
              published
                && published->publication.state == local_object_state::sealed
                && published->publication.boundary == sealed.boundary
                && published->publication.roots.size() == 2
                && published->roots.size() == 2
                && published->generation == *writer->publication_generation(),
              "a sealed writer does not say what it published");
            generation = take(
              co_await drive.lifecycle(
                local_generation_owner::open(
                  files,
                  owner,
                  spec,
                  0,
                  *published,
                  resources,
                  store_contract::limits(),
                  work)));
        }
        pin.emplace(take(generation->pin()));
        sealed_index = take(
          sealed_sparse_index<Backend, Owner>::make(
            files,
            owner,
            spec,
            0,
            writer->seal_progress().extent->context(),
            take(
              sparse_index_context::make(
                described.segment,
                sealed.extent->coverage,
                sealed.extent->digest,
                described.alignment)),
            stride,
            *sealed.index,
            std::nullopt,
            residency,
            rebuilds,
            resources,
            resources,
            store_contract::limits(),
            indexes::scan_limits(),
            work.policy(),
            cache.get()));

        // The move, with one read in flight: its runs hold nothing, so it
        // reads the file.
        pending.emplace(bypassing->read(offset(first), all, asking));
        {
            const segment_data_source through_pin{take(pin->share())};
            sessions.move(
              through_pin, [&] { return sealed_read_index(*sealed_index); });
        }
        // The writer closes under three open sessions, and the index it fed
        // goes with it.
        take(co_await drive.lifecycle(writer->close()));
        writer.reset();
        index.reset();
        through.emplace(co_await drive.lifecycle(std::move(*pending)));
        pending.reset();
        {
            const auto rest = co_await drive.lifecycle(
              tailing->read({early->next, all, true}, asking));
            const auto found = co_await drive.lifecycle(
              cold->read(offset(first), all, asking));
            require(
              matches(*through, 0, 4) && matches(rest, 1, 3)
                && rest.stop == segment_read_stop::short_of_visibility
                && matches(found, 0, 4)
                && found.stop == segment_read_stop::short_of_visibility,
              "a moved session did not read on to the bound it took");
            require(
              sessions.size() == 3 && !tailing->revoked()
                && !bypassing->revoked() && !cold->revoked(),
              "a move ended a session it could move");
        }

        // The drain, with one read in flight.
        pending.emplace(cold->read(offset(first), all, asking));
        sessions.revoke();
        {
            const auto ended = co_await drive.lifecycle(std::move(*pending));
            pending.reset();
            const auto closed = [](const segment_read& read) {
                return read.batches.empty()
                       && read.stop == segment_read_stop::failed && read.error
                       && read.error->code() == errc::closed;
            };
            const auto denied = co_await drive.lifecycle(
              tailing->read(offset(first), all, asking));
            const auto after = co_await drive.lifecycle(
              cold->read(offset(first), all, asking));
            require(
              closed(ended) || matches(ended, 0, ended.batches.size()),
              "a read in flight at the drain returned neither whole batches "
              "nor closed");
            require(
              sessions.size() == 0 && tailing->revoked() && bypassing->revoked()
                && cold->revoked() && closed(denied) && closed(after),
              "a revoked session was let read");
        }
        // The sessions are still held, and nothing waits for them.
        for (auto* held : {&runs, &fresh, &apart})
            take(co_await drive.lifecycle((*held)->close()));
        take(co_await drive.lifecycle(sealed_index->close()));
        sealed_index.reset();
        pin.reset();
        take(co_await drive.lifecycle(generation->close()));
        generation.reset();
        require(
          matches(*early, 0, 1) && matches(*through, 0, 4),
          "what a session returned did not outlive its segment's drain");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (pending) {
        try {
            static_cast<void>(co_await drive.lifecycle(std::move(*pending)));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        pending.reset();
    }
    sessions.revoke();
    cold.reset();
    bypassing.reset();
    tailing.reset();
    for (auto* held : {&runs, &fresh, &apart}) {
        if (!*held) continue;
        try {
            failed.observe(co_await drive.lifecycle((*held)->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    if (sealed_index) {
        try {
            failed.observe(co_await drive.lifecycle(sealed_index->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        sealed_index.reset();
    }
    pin.reset();
    if (generation) {
        try {
            failed.observe(co_await drive.lifecycle(generation->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        generation.reset();
    }
    through.reset();
    early.reset();
    if (writer) {
        try {
            failed.observe(co_await drive.lifecycle(writer->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        writer.reset();
    }
    runs.reset();
    fresh.reset();
    apart.reset();
    try {
        co_await drive.lifecycle(cache->stop());
    } catch (...) {
        failed.observe(std::current_exception());
    }
    cache.reset();
    index.reset();
    take(failed.outcome());
}

// The edges of a read on a sealed segment. A session that may see nothing is
// given nothing, whatever it asks for and however. A limit that is exactly
// some batches' bytes returns those batches, and one byte less returns one
// batch fewer. A read returns the batches from its offset up to the first
// one its session may not see. An offset that is a batch's last returns that
// batch. Of the runs the cache keeps, one answers for an offset only when it
// holds it: an offset past a kept run's last, or between two kept runs, is
// read from the device, and that run alone. An anchor that leads to another
// block than it names is reported to the index, once, and the read goes on
// with what the index answers then.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> read_edges(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    auto world = co_await open_world<Clock>(
      files, owner, spec, resources, work, drive, 9);
    auto& w = *world;
    auto& cache = *w.cache;
    std::unique_ptr<segment_runs> runs, gapped, misled;
    runtime::first_failure failed;
    try {
        const auto first = w.first();
        const auto last = w.last();
        const auto place = [&](std::size_t block) {
            return segment_read_position{
              model::range_logical_end{w.base(block)},
              runtime::file_position{w.position(block)}};
        };
        seastar::abort_source asking;
        const byte_count all{1_MiB};
        runs = w.runs(resources);
        {
            // Nothing visible: nothing is returned, with or without the
            // first batch's rule, and nothing is read to find that out.
            auto unseeing = w.session(*runs, resources, first);
            bool nothing = true;
            for (const bool at_least_one : {true, false})
                for (const auto from : {first, last - 1, last, last + 1}) {
                    const auto got = co_await drive.lifecycle(unseeing.read(
                      {{model::range_logical_end{from}, std::nullopt},
                       byte_count{1},
                       at_least_one},
                      asking));
                    nothing
                      = nothing && got.batches.empty() && got.bytes.value() == 0
                        && got.wanted.value() == 0 && !got.error
                        && got.next.offset.value() == from
                        && got.stop
                             == (from < last ? segment_read_stop::not_yet_visible : segment_read_stop::end_of_segment);
                }
            require(
              nothing && cache.snapshot().loads == 0 && w.index->loads() == 0,
              "a session that may see nothing was given something, or "
              "something was read for it");
        }
        {
            auto reader = w.session(*runs, resources, last);
            const auto before = cache.snapshot();
            const auto one_batch = w.expected[0].size();
            const auto two_batches = one_batch + w.expected[1].size();
            const auto within = [&](std::uint64_t limit) {
                return reader.read(offset(first), byte_count{limit}, asking);
            };
            const auto one = co_await drive.lifecycle(within(one_batch));
            const auto two = co_await drive.lifecycle(within(two_batches));
            const auto fewer = co_await drive.lifecycle(
              within(two_batches - 1));
            require(
              w.matches(one, 0, 1) && one.stop == segment_read_stop::limit
                && one.next == place(1) && w.matches(two, 0, 2)
                && two.stop == segment_read_stop::limit && two.next == place(2)
                && w.matches(fewer, 0, 1)
                && fewer.stop == segment_read_stop::limit
                && fewer.next == place(1)
                && cache.snapshot().loads == before.loads + 3
                && w.index->loads() == 1,
              "a limit that is exactly some batches' bytes did not return "
              "those batches");
            // From the third batch up to the eighth's offset.
            auto bounded = w.session(*runs, resources, w.base(7));
            const auto ranged = co_await drive.lifecycle(
              bounded.read(offset(w.base(2)), all, asking));
            // An offset that is a batch's last.
            const auto ending = co_await drive.lifecycle(
              reader.read(offset(last - 1), all, asking));
            require(
              w.matches(ranged, 2, 5)
                && ranged.stop == segment_read_stop::not_yet_visible
                && ranged.next.offset.value() == w.base(7)
                && w.matches(ending, 8, 1)
                && ending.stop == segment_read_stop::end_of_segment
                && ending.next.offset.value() == last,
              "a read did not return the batches from its offset to its "
              "session's bound, or the batch its offset is the last of");
        }
        {
            // Two runs are kept, with one between them that is not.
            gapped = w.runs(resources);
            auto reader = w.session(
              *gapped, resources, last, {.read_ahead = 0});
            const auto before = cache.snapshot();
            const auto lead = co_await drive.lifecycle(
              reader.read(offset(first), byte_count{1}, asking));
            const auto third = co_await drive.lifecycle(
              reader.read(offset(w.base(4)), byte_count{1}, asking));
            // Where the kept run that holds an offset begins.
            const auto kept_at =
              [&](std::uint64_t target) -> std::optional<std::uint64_t> {
                const auto wanted = offset(target);
                const auto below = gapped->floor(wanted, false);
                if (!below || below->seek(wanted) == below->blocks().size())
                    return std::nullopt;
                return below->begin().value();
            };
            require(
              w.matches(lead, 0, 1) && w.matches(third, 4, 1)
                && cache.snapshot().loads == before.loads + 2
                && !kept_at(first - 1)
                && kept_at(first) == std::optional{w.position(0)}
                && kept_at(w.base(1)) == std::optional{w.position(0)}
                && !kept_at(w.base(2)) && !kept_at(w.base(3))
                && kept_at(w.base(4)) == std::optional{w.position(4)}
                && kept_at(w.base(5)) == std::optional{w.position(4)}
                && !kept_at(w.base(6)) && !kept_at(last) && !kept_at(last + 40),
              "a kept run answered for an offset it does not hold, or did "
              "not answer for one it holds");
            const auto between = co_await drive.lifecycle(
              reader.read(offset(w.base(2)), byte_count{1}, asking));
            require(
              w.matches(between, 2, 1)
                && cache.snapshot().loads == before.loads + 3
                && kept_at(w.base(3)) == std::optional{w.position(2)},
              "an offset between two kept runs was not read from the "
              "device, or more than its run was");
        }
        {
            // The index's first answer leads to the second block under the
            // first's offset. The block there is not the one it names: the
            // index is told, once, builds itself again from the data, and
            // the read goes on with what it answers then.
            misled = w.runs(resources);
            std::uint32_t asked = 0, reported = 0;
            auto reader = take(
              segment_reader::open(
                *misled,
                segment_data_source{take(w.pin->share())},
                {.durable = w.bound(),
                 .sealed = model::range_logical_end{last},
                 .visible = model::range_logical_end{last}},
                segment_read_index{
                  [&w, &asked](
                    model::range_logical_offset target,
                    seastar::abort_source& caller,
                    sparse_index_place&) {
                      const bool misleads = asked++ == 0;
                      return w.index->locate(target, caller)
                        .then([&w, misleads](
                                runtime::result<sparse_index_answer> answer) {
                            if (misleads && answer && answer->position)
                                answer->position->anchor = sparse_index_entry{
                                  answer->position->anchor.logical_anchor(),
                                  runtime::file_position{w.position(1)}};
                            return answer;
                        });
                  },
                  [&w, &reported](
                    const sparse_index_answer& given,
                    runtime::operation_error why) noexcept {
                      ++reported;
                      w.index->report(given, why);
                  },
                  [&w](
                    model::range_logical_offset target,
                    const sparse_index_place&) noexcept {
                      return w.index->peek(target);
                  }},
                resources,
                one_ahead));
            const auto seen = co_await drive.lifecycle(
              reader.read(offset(first), all, asking));
            require(
              w.matches(seen, 0, 9)
                && seen.stop == segment_read_stop::end_of_segment
                && reported == 1 && asked >= 2 && w.index->owed().has_value()
                && w.index->source() == sparse_index_source::memory,
              "an anchor that led to another block was believed, was not "
              "reported once, or left its reader without an answer");
        }
        for (auto* held : {&runs, &gapped, &misled}) {
            take(co_await drive.lifecycle((*held)->close()));
            held->reset();
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    for (auto* held : {&runs, &gapped, &misled}) {
        if (!*held) continue;
        try {
            failed.observe(co_await drive.lifecycle((*held)->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        held->reset();
    }
    co_await close_world(w, failed, drive);
    take(failed.outcome());
    require(
      cache.snapshot().bytes == 0 && w.residency.resident() == 0,
      "what was read is still charged, or an index left its root open");
}

// A segment nothing was written to, and one that holds one batch. Nothing is
// returned of the first, and no file is read to find that out: a caller that
// may see more is told this copy is short, one that may see nothing that
// nothing is visible, and an offset before the segment's first that it is
// another segment's. The one batch is returned whole to whoever may see it,
// from its offset and from no other.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> empty_reads(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    auto config = segments::configuration();
    config.maximum_groups = 4;
    config.admission.working_bytes = byte_count{1_MiB};
    config.admission.maximum_blocks = 16;
    const auto described = segments::descriptor();
    const auto unit = described.alignment.bytes().value();
    const auto stride = paired_stride();
    auto index = std::make_unique<active_sparse_index>(take(
      active_sparse_index::make(
        stride,
        sparse_index_capacity(
          stride,
          described.maximum_data_bytes,
          config.admission.maximum_blocks),
        resources)));
    auto cache = take(
      page_cache::make(resources, {.capacity = byte_count{2_MiB}}));
    auto writer = take(
      writer_type::make_new(
        files, owner, spec, 0, described, resources, config));
    std::unique_ptr<segment_runs> runs;
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        std::optional<segment_history_context> origin;
        runtime::file_position start;
        {
            const auto view = take(
              co_await drive.lifecycle(writer->pin_data(work)));
            origin.emplace(view.history);
            start = view.durable;
        }
        const auto history = *origin;
        const auto first = history.logical_origin.value();
        runs = take(
          segment_runs::make(
            *cache, resources, history, {.limits = run_limits()}));
        take(writer->observe_blocks(runs->observer(*index)));
        const auto session =
          [&](std::uint64_t visible) -> seastar::future<segment_reader> {
            auto view = take(co_await drive.lifecycle(writer->pin_data(work)));
            co_return take(
              segment_reader::open(
                *runs,
                segment_data_source{std::move(view.pin)},
                {.durable = view.durable,
                 .sealed = std::nullopt,
                 .visible = model::range_logical_end{visible}},
                active_read_index(*index, view.durable),
                resources,
                one_ahead));
        };
        seastar::abort_source asking;
        const byte_count all{1_MiB};
        {
            auto reader = co_await session(first + 64);
            auto unseeing = co_await session(first);
            const auto nothing = co_await drive.lifecycle(
              reader.read(offset(first), all, asking));
            const auto unseen = co_await drive.lifecycle(
              unseeing.read(offset(first), all, asking));
            const auto below = co_await drive.lifecycle(
              reader.read(offset(first - 1), all, asking));
            require(
              reader.bounds().durable.value() == start.value()
                && nothing.batches.empty()
                && nothing.stop == segment_read_stop::short_of_visibility
                && nothing.next.offset.value() == first && !nothing.error
                && unseen.batches.empty()
                && unseen.stop == segment_read_stop::not_yet_visible
                && below.batches.empty()
                && below.stop == segment_read_stop::below_start
                && cache->snapshot().loads == 0 && index->size() == 0,
              "a segment nothing was written to returned something, read a "
              "file, or did not say why it returned nothing");
        }
        std::string stored;
        {
            codec::crc32c checksum;
            std::vector<encoded_assigned_batch> batches;
            batches.push_back(co_await segments::child(work));
            auto [cut, bytes] = co_await segments::write_group(
              *writer, std::move(batches), resources, work, drive, checksum);
            stored = std::move(bytes);
            const auto synced = co_await drive.lifecycle(writer->barrier(cut));
            take(synced.failure.outcome());
            co_await drive.lifecycle(runs->settled());
        }
        {
            auto reader = co_await session(first + 64);
            auto unseeing = co_await session(first);
            const auto before = cache->snapshot();
            const auto only = co_await drive.lifecycle(
              reader.read(offset(first), all, asking));
            const auto past = co_await drive.lifecycle(
              reader.read(offset(first + 1), all, asking));
            const auto unseen = co_await drive.lifecycle(
              unseeing.read(offset(first), all, asking));
            require(
              reader.bounds().durable.value() == start.value() + 2 * unit
                && !runs->fed() && only.batches.size() == 1
                && only.batches[0].logical.begin().value() == first
                && flat(only.batches[0].bytes) == batch_at(stored, 0)
                && only.stop == segment_read_stop::short_of_visibility
                && only.next.offset.value() == first + 1 && past.batches.empty()
                && past.stop == segment_read_stop::short_of_visibility
                && past.next.offset.value() == first + 1
                && unseen.batches.empty()
                && unseen.stop == segment_read_stop::not_yet_visible
                && cache->snapshot().uncached == before.uncached + 2,
              "a segment of one batch did not return that batch whole, or "
              "returned it to a caller that may not see it");
        }
        require(!writer->failure().failed(), "reading a segment failed it");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (runs) {
        try {
            failed.observe(co_await drive.lifecycle(runs->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    try {
        failed.observe(co_await drive.lifecycle(writer->close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    // The writer's observer refers to the runs and the index until here.
    writer.reset();
    runs.reset();
    try {
        co_await drive.lifecycle(cache->stop());
    } catch (...) {
        failed.observe(std::current_exception());
    }
    cache.reset();
    index.reset();
    take(failed.outcome());
}

// Damage at each place of a run: in its first block, in one in its middle
// and in its last; a block of zeroes, also where the run begins; a length
// with a bit flipped, which the header's own checksum gives away before the
// length is believed; a block whose header says, under a checksum that
// holds, that it is longer than it is; and a file that ends where the run
// begins. A run is checked whole before any of it is handed out, so nothing
// of a damaged run is returned and nothing of it is kept: the read returns
// what it read before, says where it could not go on and why, and the same
// read fails the same way again. What lies behind the damage is read all
// the same by a read that begins at an anchor after it, while the damage
// stands. None of it is the index's to answer for, the run's first block
// included: bytes that do not read as a block say nothing of the anchor
// that led to them, and the index goes on answering.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> damaged_runs(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    // Three runs of three blocks.
    auto world = co_await open_world<Clock>(
      files, owner, spec, resources, work, drive, 9, 3);
    auto& w = *world;
    auto& cache = *w.cache;
    std::unique_ptr<segment_runs> runs;
    runtime::first_failure failed;
    try {
        const auto first = w.first();
        const auto last = w.last();
        const auto unit = segments::descriptor().alignment.bytes().value();
        const auto path = indexes::data_path(spec);
        const auto data = co_await store_contract::read_all_bytes(
          files, path, drive);
        seastar::abort_source asking;
        const byte_count all{1_MiB};
        // Where the read that meets the damage goes on: the second run.
        const segment_read_position stopped{
          model::range_logical_end{w.base(3)},
          runtime::file_position{w.position(3)}};
        // The index is read first, so that what follows reads data alone.
        take(co_await drive.lifecycle(w.index->locate(offset(first), asking)));
        enum class damage : std::uint8_t {
            first_flipped,
            middle_flipped,
            last_flipped,
            first_zeroed,
            middle_zeroed,
            first_length_flipped,
            last_length_flipped,
            last_overlong,
            cut_short,
        };
        struct expectation final {
            damage what;
            errc code;
        };
        for (const auto [what, code] :
             {expectation{damage::first_flipped, errc::corrupt_data},
              expectation{damage::middle_flipped, errc::corrupt_data},
              expectation{damage::last_flipped, errc::corrupt_data},
              expectation{damage::first_zeroed, errc::malformed_data},
              expectation{damage::middle_zeroed, errc::malformed_data},
              expectation{damage::first_length_flipped, errc::corrupt_data},
              expectation{damage::last_length_flipped, errc::corrupt_data},
              expectation{damage::last_overlong, errc::malformed_data},
              expectation{damage::cut_short, errc::truncated_data}}) {
            auto damaged = data;
            switch (what) {
            case damage::first_flipped:
                damaged[w.position(3) + 32 + 120 + 40] ^= 1;
                break;
            case damage::middle_flipped:
                damaged[w.position(4) + 32 + 120 + 40] ^= 1;
                break;
            case damage::last_flipped:
                damaged[w.position(5) + 32 + 120 + 40] ^= 1;
                break;
            case damage::first_zeroed:
                damaged.replace(w.position(3), unit, unit, '\0');
                break;
            case damage::middle_zeroed:
                damaged.replace(w.position(4), unit, unit, '\0');
                break;
            case damage::first_length_flipped:
                // The body's length, a gibibyte more: past every limit.
                damaged[w.position(3) + 15] ^= 0x40;
                break;
            case damage::last_length_flipped:
                // One byte more: not a whole unit.
                damaged[w.position(5) + 12] ^= 1;
                break;
            case damage::last_overlong: {
                // Its header's own checksum holds, and says the block is
                // two units longer than it is.
                const auto length = get(damaged, w.position(5) + 10, 2);
                auto header = damaged.substr(w.position(5), length);
                put(header, 12, get(header, 12, 4) + 2 * unit, 4);
                put(header, 28, 0, 4);
                put(header, 28, crc(std::string_view{header}), 4);
                damaged.replace(w.position(5), length, header);
                break;
            }
            case damage::cut_short:
                damaged.resize(w.position(3));
                break;
            }
            co_await store_contract::write_bytes(files, path, damaged, drive);
            runs = w.runs(resources);
            {
                auto reader = w.session(
                  *runs, resources, last, {.read_ahead = 0});
                const auto got = co_await drive.lifecycle(
                  reader.read(offset(first), all, asking));
                const auto again = co_await drive.lifecycle(
                  reader.read({got.next, all, true}, asking));
                // An offset in the damaged run, at the damage or behind it.
                const auto behind = co_await drive.lifecycle(
                  reader.read(offset(w.base(5)), all, asking));
                // A read that begins at the anchor after it.
                const auto beyond = co_await drive.lifecycle(
                  reader.read(offset(w.base(6)), all, asking));
                const auto refused = [code](const segment_read& read) {
                    return read.batches.empty()
                           && read.stop == segment_read_stop::failed
                           && read.error && read.error->code() == code;
                };
                require(
                  w.matches(got, 0, 3) && got.stop == segment_read_stop::failed
                    && got.error && got.error->code() == code
                    && got.next == stopped && refused(again)
                    && again.next == stopped && refused(behind)
                    && behind.next.offset.value() == w.base(5)
                    && (what == damage::cut_short
                          ? refused(beyond)
                          : (w.matches(beyond, 6, 3)
                             && beyond.stop
                                  == segment_read_stop::end_of_segment))
                    && !runs->holds(offset(w.base(3))) && !w.index->owed(),
                  "a damaged run was handed out or kept, or the read that "
                  "met it did not say where and why it stopped");
            }
            take(co_await drive.lifecycle(runs->close()));
            runs.reset();
        }
        {
            // The data is what was sealed again: the session that met
            // damage reads on, through the index it always had.
            co_await store_contract::write_bytes(files, path, data, drive);
            runs = w.runs(resources);
            {
                auto reader = w.session(
                  *runs, resources, last, {.read_ahead = 0});
                const auto whole = co_await drive.lifecycle(
                  reader.read(offset(first), all, asking));
                require(
                  w.matches(whole, 0, 9)
                    && whole.stop == segment_read_stop::end_of_segment
                    && !w.index->owed()
                    && w.index->source() == sparse_index_source::published,
                  "data that is whole again was not read, or damage to it "
                  "cost its index");
            }
            take(co_await drive.lifecycle(runs->close()));
            runs.reset();
        }
        {
            // A place to go on from that is wrong. A session that leaves
            // nothing behind reads from the place it is handed: one that
            // names the block after the one wanted leads to a whole block
            // that is another, and one no block can begin at is not
            // followed at all. Either way the place is forgotten and the
            // anchor is read from: a wrong place costs a read, never the
            // batches.
            runs = w.runs(resources);
            {
                auto reader = w.session(
                  *runs, resources, last, {.fill = false});
                const auto wanted = model::range_logical_end{w.base(4)};
                const auto misplaced = co_await drive.lifecycle(reader.read(
                  {{wanted, runtime::file_position{w.position(5)}}, all, true},
                  asking));
                const auto unaligned = co_await drive.lifecycle(reader.read(
                  {{wanted, runtime::file_position{w.position(4) + 1}},
                   all,
                   true},
                  asking));
                require(
                  w.matches(misplaced, 4, 5)
                    && misplaced.stop == segment_read_stop::end_of_segment
                    && w.matches(unaligned, 4, 5)
                    && unaligned.stop == segment_read_stop::end_of_segment
                    && !w.index->owed(),
                  "a wrong place to go on from failed its read, or was "
                  "believed");
            }
            take(co_await drive.lifecycle(runs->close()));
            runs.reset();
        }
        require(
          co_await store_contract::read_all_bytes(files, path, drive) == data
            && !w.segment.writer->failure().failed(),
          "a damaged read changed the data or failed its segment");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (runs) {
        try {
            failed.observe(co_await drive.lifecycle(runs->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        runs.reset();
    }
    co_await close_world(w, failed, drive);
    take(failed.outcome());
    require(
      cache.snapshot().bytes == 0 && w.residency.resident() == 0,
      "what was read is still charged, or an index left its root open");
}

// Many sessions that want a segment's runs together. Each run is read from
// the device once, for all of them: counted in the cache and, where the
// backend counts its reads, at the device. Every session is given every
// batch. A reader that leaves while it waits leaves alone, though it was the
// first to ask: its read ends there, and everyone else's goes on. A drain
// that begins once a read has started a run's load lets that load end, gives
// its session what was loaded and refuses it what was not, and leaves
// nothing behind.
//
// `reads` says how many reads the device has had of the segment's data, or
// nothing on a backend that does not count them.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Reads>
seastar::future<> crowded_reads(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive,
  Reads reads) {
    constexpr std::size_t crowd = 8;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    auto world = co_await open_world<Clock>(
      files, owner, spec, resources, work, drive, 9);
    auto& w = *world;
    auto& cache = *w.cache;
    std::unique_ptr<segment_runs> runs, late;
    // The drain that waits for a session, kept where the case's end finds
    // it.
    std::optional<seastar::future<runtime::result<void>>> draining;
    runtime::first_failure failed;
    try {
        const auto first = w.first();
        const auto last = w.last();
        seastar::abort_source asking;
        const byte_count all{1_MiB};
        // The index is read first, so that what follows reads data alone.
        take(co_await drive.lifecycle(w.index->locate(offset(first), asking)));
        runs = w.runs(resources);
        {
            seastar::abort_source leaving;
            std::vector<segment_reader> sessions;
            sessions.reserve(crowd + 1);
            for (std::size_t i = 0; i <= crowd; ++i)
                sessions.push_back(w.session(*runs, resources, last));
            const auto before = cache.snapshot();
            const std::optional<std::uint64_t> device = reads();
            // The first to ask is the one that leaves.
            auto left = sessions[0].read(offset(first), all, leaving);
            std::vector<seastar::future<segment_read>> pending;
            pending.reserve(crowd);
            for (std::size_t i = 1; i <= crowd; ++i)
                pending.push_back(sessions[i].read(offset(first), all, asking));
            leaving.request_abort();
            const auto gone = co_await drive.lifecycle(std::move(left));
            bool served = true;
            for (auto& waiting : pending) {
                const auto got = co_await drive.lifecycle(std::move(waiting));
                served = served && w.matches(got, 0, 9)
                         && got.stop == segment_read_stop::end_of_segment;
            }
            const auto after = cache.snapshot();
            const std::optional<std::uint64_t> counted = reads();
            require(
              gone.batches.empty() && gone.stop == segment_read_stop::cancelled
                && !gone.error && served && after.loads == before.loads + 5
                && after.uncached == before.uncached
                && (!device || !counted || *counted - *device == 5),
              "sessions that wanted the same runs together read one more "
              "than once, or a reader that left took the others with it");
        }
        {
            late = w.runs(resources);
            std::optional<segment_reader> session{
              w.session(*late, resources, last, {.read_ahead = 0})};
            const auto idle = cache.snapshot().loads;
            auto pending = session->read(offset(first), all, asking);
            // The drain begins once the read is in: its first run's load
            // has been started. A read that has not come that far when a
            // drain begins is refused like any other.
            for (std::uint32_t turn = 0;
                 cache.snapshot().loads == idle && turn < 4096;
                 ++turn)
                co_await seastar::yield();
            // The runs the read has asked for, of two batches each: the
            // drain lets it have those and no more. Nothing is required
            // while the read is in flight: a session is not destroyed
            // under its read.
            const auto begun = cache.snapshot().loads - idle;
            draining.emplace(late->close());
            const auto got = co_await drive.lifecycle(std::move(pending));
            require(
              begun >= 1 && begun <= 4,
              "a read did not start the load of its first run");
            const auto given = static_cast<std::size_t>(begun) * 2;
            require(
              w.matches(got, 0, given) && got.stop == segment_read_stop::failed
                && got.error && got.error->code() == errc::closed
                && got.next.offset.value() == w.base(given)
                && !draining->available(),
              "a drain did not let a read at the device end, served what "
              "was not loaded, or did not wait for the session");
            session.reset();
            take(co_await drive.lifecycle(std::move(*draining)));
            draining.reset();
            require(
              !late->failure().failed(),
              "a drain that failed nothing kept a failure");
            late.reset();
            // What the read returned is still what was written.
            require(
              w.matches(got, 0, given),
              "what a read returned did not outlive the drain");
        }
        take(co_await drive.lifecycle(runs->close()));
        runs.reset();
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (draining) {
        try {
            failed.observe(co_await drive.lifecycle(std::move(*draining)));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        draining.reset();
    }
    for (auto* held : {&runs, &late}) {
        if (!*held) continue;
        try {
            failed.observe(co_await drive.lifecycle((*held)->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        held->reset();
    }
    co_await close_world(w, failed, drive);
    take(failed.outcome());
    require(
      cache.snapshot().bytes == 0 && w.residency.resident() == 0,
      "what was read is still charged, or an index left its root open");
}

// Batches of several records and of several sizes. Each is read back whole
// and byte for byte wherever in it the offset asked for lies, and its span
// says where it begins. A block longer than a stride is a run of its own.
// With one alignment unit to a read every stretch is read in parts for its
// reader alone: a part is one block, or the footer the part before it
// stopped at, and a first block that one read does not hold is read again
// with exactly its length. Every run ends with its group's footer, so with
// that unit no run is the cache's. A read unit that is not whole alignment
// units is refused.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> mixed_batches(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    const auto unit = segments::descriptor().alignment.bytes().value();
    // Records to a batch, and what each record holds. The third batch's
    // block is four units long, which is more than a stride.
    struct shape final {
        std::uint32_t records;
        std::size_t payload;
    };
    constexpr std::array<shape, 6> shapes{
      {{3, 64}, {2, 64}, {1, 28000}, {5, 64}, {1, 64}, {4, 64}}};
    std::vector<encoded_assigned_batch> batches;
    batches.reserve(shapes.size());
    auto next = segments::descriptor().logical_origin.value();
    for (const auto& made : shapes) {
        batches.push_back(
          co_await dense_child(next, made.records, made.payload, work));
        next = batches.back().info().context.logical_span().end().value();
    }
    const auto stride = stride_of(2);
    auto sealed = co_await seal_batches<Clock>(
      files, owner, spec, resources, work, drive, std::move(batches), stride);
    auto world = co_await open_sealed<Clock>(
      files, owner, spec, resources, work, drive, std::move(sealed), stride);
    auto& w = *world;
    auto& cache = *w.cache;
    std::unique_ptr<segment_runs> runs, parted;
    runtime::first_failure failed;
    try {
        const auto first = w.first();
        const auto last = w.last();
        seastar::abort_source asking;
        const byte_count all{1_MiB};
        // Four runs: the first two blocks, the long block, the next two,
        // and the last.
        const auto& anchors = *w.segment.index;
        require(
          last == next && last == first + 16 && w.base(1) == first + 3
            && w.base(2) == first + 5 && w.base(3) == first + 6
            && w.base(4) == first + 11 && w.base(5) == first + 12
            && anchors.size() == 4
            && w.segment.blocks[2].coverage().bytes().size().value() == 4 * unit
            && anchors[1].block_position().value() == w.position(2)
            && anchors[2].block_position().value() == w.position(3)
            && anchors[3].block_position().value() == w.position(5),
          "the segment's blocks are not the ones this case is about");
        // The index is read first, so that what follows reads data alone.
        take(co_await drive.lifecycle(w.index->locate(offset(first), asking)));
        runs = w.runs(resources);
        {
            auto reader = w.session(*runs, resources, last);
            const auto holding = [&](std::uint64_t target) {
                return reader.read(offset(target), byte_count{1}, asking);
            };
            const auto inside = co_await drive.lifecycle(holding(first + 1));
            const auto ending = co_await drive.lifecycle(holding(first + 4));
            const auto large = co_await drive.lifecycle(holding(first + 5));
            const auto middle = co_await drive.lifecycle(holding(first + 8));
            const auto closing = co_await drive.lifecycle(holding(last - 1));
            require(
              w.matches(inside, 0, 1)
                && inside.batches[0].logical.begin().value() == first
                && inside.next.offset.value() == first + 3
                && w.matches(ending, 1, 1)
                && ending.next.offset.value() == first + 5
                && w.matches(large, 2, 1)
                && large.batches[0].held.value()->blocks().size() == 1
                && large.batches[0].held.value()->begin().value()
                     == w.position(2)
                && large.batches[0].held.value()->end().value() == w.position(3)
                && w.matches(middle, 3, 1)
                && middle.batches[0].logical.begin().value() == first + 6
                && w.matches(closing, 5, 1)
                && closing.next.offset.value() == last,
              "an offset inside a batch did not return that batch whole, "
              "or a block longer than a stride is not a run of its own");
            const auto whole = co_await drive.lifecycle(
              reader.read(offset(first), all, asking));
            require(
              w.matches(whole, 0, 6)
                && whole.stop == segment_read_stop::end_of_segment
                && cache.snapshot().uncached == 0,
              "batches of several sizes were not read back as written");
        }
        {
            auto limits = run_limits();
            limits.run_bytes = byte_count{unit + 1};
            const auto uneven = segment_runs::make(
              cache, resources, w.segment.history(), {.limits = limits});
            require(
              !uneven && uneven.error().code() == errc::invalid_argument,
              "a read unit that is not whole alignment units was taken");
            limits.run_bytes = byte_count{unit};
            parted = take(
              segment_runs::make(
                cache, resources, w.segment.history(), {.limits = limits}));
            auto reader = w.session(*parted, resources, last);
            const auto before = cache.snapshot();
            {
                const auto read = co_await drive.lifecycle(
                  reader.read(offset(first), all, asking));
                const auto after = cache.snapshot();
                // Twelve reads, each for this reader alone: a block and a
                // footer at a time, the long block's first read and its
                // exact one, and the last block. Every run ends with its
                // group's footer, the last one too, so none is one read
                // long; the last footer is not read, since the read ends
                // with the segment's last offset.
                require(
                  w.matches(read, 0, 6)
                    && read.stop == segment_read_stop::end_of_segment
                    && after.loads == before.loads + 12
                    && after.uncached == before.uncached + 12
                    && parted->prefetches() == 0,
                  "a stretch longer than one read was not read in parts, "
                  "each for its reader alone");
            }
        }
        for (auto* held : {&runs, &parted}) {
            take(co_await drive.lifecycle((*held)->close()));
            held->reset();
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    for (auto* held : {&runs, &parted}) {
        if (!*held) continue;
        try {
            failed.observe(co_await drive.lifecycle((*held)->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        held->reset();
    }
    co_await close_world(w, failed, drive);
    take(failed.outcome());
    require(
      cache.snapshot().bytes == 0 && w.residency.resident() == 0,
      "what was read is still charged, or an index left its root open");
}

// What every case of a segment that is still appended to begins with: its
// writer, the index the writer feeds, a cache, and its runs.
template<typename Writer>
struct tail_world final {
    std::unique_ptr<active_sparse_index> index;
    std::unique_ptr<page_cache> cache;
    std::unique_ptr<Writer> writer;
    std::unique_ptr<segment_runs> runs;
    std::optional<segment_history_context> history;
    runtime::file_position start;
    std::uint64_t unit{0};
    // The exact bytes written so far, and their running checksum.
    std::string stored;
    codec::crc32c checksum;

    [[nodiscard]] std::uint64_t first() const {
        return history->logical_origin.value();
    }
    // Where group `group` begins: one block and its footer to a group.
    [[nodiscard]] std::uint64_t at(std::uint64_t group) const {
        return start.value() + group * 2 * unit;
    }
    // Whether `read` returned `count` batches from the block of `from`.
    [[nodiscard]] bool matches(
      const segment_read& read, std::uint64_t from, std::size_t count) const {
        if (read.batches.size() != count) return false;
        for (std::size_t i = 0; i < count; ++i)
            if (
              read.batches[i].logical.begin().value() != first() + from + i
              || flat(read.batches[i].bytes)
                   != batch_at(stored, (from + i) * 2 * unit))
                return false;
        return true;
    }
};

template<typename World, typename Driver>
seastar::future<>
close_tail(World& world, runtime::first_failure& failed, Driver drive) {
    if (world.runs) {
        try {
            failed.observe(co_await drive.lifecycle(world.runs->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    if (world.writer) {
        try {
            failed.observe(co_await drive.lifecycle(world.writer->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    // The writer's observer refers to the runs and the index until here.
    world.writer.reset();
    world.runs.reset();
    if (world.cache) {
        try {
            co_await drive.lifecycle(world.cache->stop());
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    world.cache.reset();
    world.index.reset();
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<
  std::unique_ptr<tail_world<segment_writer<Backend, Owner, Clock>>>>
open_tail(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  codec::cooperative_work& work,
  Driver drive,
  segment_runs_config runs_config) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    auto config = segments::configuration();
    config.maximum_groups = 8;
    config.admission.working_bytes = byte_count{1_MiB};
    config.admission.maximum_blocks = 16;
    const auto described = segments::descriptor();
    const auto stride = paired_stride();
    auto made = std::make_unique<tail_world<writer_type>>();
    made->unit = described.alignment.bytes().value();
    made->index = std::make_unique<active_sparse_index>(take(
      active_sparse_index::make(
        stride,
        sparse_index_capacity(
          stride,
          described.maximum_data_bytes,
          config.admission.maximum_blocks),
        resources)));
    made->cache = take(
      page_cache::make(resources, {.capacity = byte_count{2_MiB}}));
    made->writer = take(
      writer_type::make_new(
        files, owner, spec, 0, described, resources, config));
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(made->writer->create_new(work)));
        const auto view = take(
          co_await drive.lifecycle(made->writer->pin_data(work)));
        made->history.emplace(view.history);
        made->start = view.durable;
        made->runs = take(
          segment_runs::make(
            *made->cache, resources, view.history, std::move(runs_config)));
        take(made->writer->observe_blocks(made->runs->observer(*made->index)));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (failed.failed()) {
        co_await close_tail(*made, failed, drive);
        take(failed.outcome());
    }
    co_return made;
}

// Writes group `group` of a tail: one block and its footer, a record to a
// block. Its boundary once its bytes are written.
template<typename World, typename Driver>
seastar::future<segment_captured_boundary> write_tail(
  World& world,
  std::uint64_t group,
  workload_budget& resources,
  codec::cooperative_work& work,
  Driver drive) {
    std::vector<encoded_assigned_batch> batches;
    if (group == 0)
        batches.push_back(co_await segments::child(work));
    else
        batches.push_back(
          co_await segments::execution_child(world.first() + group, work));
    auto [cut, bytes] = co_await segments::write_group(
      *world.writer,
      std::move(batches),
      resources,
      work,
      drive,
      world.checksum);
    require(
      bytes.size() == 2 * world.unit
        && cut.end().bytes.value() == world.at(group + 1),
      "a group is not one block and its footer");
    world.stored += bytes;
    co_return cut;
}
// Makes a tail durable up to `cut`, and waits for its feed to settle.
template<typename World, typename Driver>
seastar::future<>
sync_tail(World& world, segment_captured_boundary cut, Driver drive) {
    const auto synced = co_await drive.lifecycle(world.writer->barrier(cut));
    take(synced.failure.outcome());
    co_await drive.lifecycle(world.runs->settled());
}
// A session on a tail as it stands now, which may see everything written.
template<typename World, typename Driver>
seastar::future<segment_reader> read_tail(
  World& world,
  workload_budget& resources,
  codec::cooperative_work& work,
  Driver drive) {
    auto view = take(co_await drive.lifecycle(world.writer->pin_data(work)));
    co_return take(
      segment_reader::open(
        *world.runs,
        segment_data_source{std::move(view.pin)},
        {.durable = view.durable,
         .sealed = std::nullopt,
         .visible = model::range_logical_end{world.first() + 64}},
        active_read_index(*world.index, view.durable),
        resources,
        one_ahead));
}

// A segment that is fed for as long as it was read within a time. A read is
// what turns the feed on, and the feed begins with the next anchor. Once
// nothing has read the segment for longer than the time, the next thing
// written to it is not kept, the run that was filling is given up, and the
// segment costs its writer one branch again; what it held is read from the
// device. A read turns the feed on again. A fed run the cache has no room
// for is given up the same way, and nothing fails for it.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> fed_bounds(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    constexpr std::uint64_t linger = 1000;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    // The time the runs are told, in the case's hands.
    const auto ticks = std::make_unique<std::uint64_t>(0);
    auto world = co_await open_tail<Clock>(
      files,
      owner,
      spec,
      resources,
      work,
      drive,
      {.limits = run_limits(),
       .feed_linger_nanoseconds = linger,
       .now = [now = ticks.get()]() noexcept {
           return runtime::monotonic_time{*now};
       }});
    auto& w = *world;
    auto& cache = *w.cache;
    auto& runs = *w.runs;
    // Values that fill the cache and are held, so that nothing has room.
    std::vector<std::unique_ptr<cache_slot<std::uint64_t>>> ballast;
    std::vector<cache_ref<std::uint64_t>> weights;
    runtime::first_failure failed;
    try {
        const auto first = w.first();
        seastar::abort_source asking;
        const byte_count all{1_MiB};
        // Nobody reads the segment: nothing is handed over.
        co_await sync_tail(
          w, co_await write_tail(w, 0, resources, work, drive), drive);
        require(
          !runs.fed() && runs.pending() == 0 && runs.dropped() == 0
            && cache.snapshot().loads == 0,
          "a segment nobody read was fed");
        {
            // A read is what turns the feed on.
            auto reader = co_await read_tail(w, resources, work, drive);
            const auto read = co_await drive.lifecycle(
              reader.read(offset(first), all, asking));
            require(
              w.matches(read, 0, 1) && runs.fed(),
              "a read did not turn the feed on");
        }
        // Within the time. The block written next is not an anchor; the
        // one after it is, and the feed begins with it.
        *ticks = linger / 2;
        static_cast<void>(co_await write_tail(w, 1, resources, work, drive));
        co_await sync_tail(
          w, co_await write_tail(w, 2, resources, work, drive), drive);
        require(
          runs.fed() && runs.pending() == 1 && runs.dropped() == 0,
          "a segment that was read within the time was not fed");
        {
            auto reader = co_await read_tail(w, resources, work, drive);
            const auto before = cache.snapshot();
            const auto fed = co_await drive.lifecycle(
              reader.read(offset(first + 2), all, asking));
            const auto after = cache.snapshot();
            require(
              w.matches(fed, 2, 1) && after.loads == before.loads
                && after.uncached == before.uncached,
              "what the feed holds was read from the device");
        }
        // Longer than the time since that read: the next thing written is
        // not kept, and the run that was filling is given up.
        *ticks = linger / 2 + linger + 1;
        co_await sync_tail(
          w, co_await write_tail(w, 3, resources, work, drive), drive);
        require(
          !runs.fed() && runs.pending() == 0 && runs.dropped() == 1,
          "a segment nothing read for longer than the time was still fed");
        {
            // What the feed gave up is read from the device, and that read
            // turns the feed on again.
            auto reader = co_await read_tail(w, resources, work, drive);
            const auto before = cache.snapshot();
            const auto read = co_await drive.lifecycle(
              reader.read(offset(first + 2), all, asking));
            require(
              w.matches(read, 2, 2)
                && cache.snapshot().uncached == before.uncached + 1
                && runs.fed(),
              "a run the feed gave up was not read from the device, or its "
              "read did not turn the feed on again");
        }
        // The fifth block is an anchor: a run begins with it.
        co_await sync_tail(
          w, co_await write_tail(w, 4, resources, work, drive), drive);
        require(
          runs.pending() == 1 && runs.dropped() == 1,
          "the feed did not begin again with the next anchor");
        // Every byte of the cache is taken and held. The sixth block goes
        // to the run that is filling, which has no room for it.
        for (const std::uint64_t backing : {64_KiB, 1_KiB})
            for (;;) {
                auto slot = std::make_unique<cache_slot<std::uint64_t>>();
                auto led = slot->lead(cache, byte_count{backing});
                if (!led) break;
                std::move(*led).publish(0);
                weights.push_back(slot->get());
                ballast.push_back(std::move(slot));
            }
        require(!weights.empty(), "the cache took no ballast");
        co_await sync_tail(
          w, co_await write_tail(w, 5, resources, work, drive), drive);
        require(
          runs.fed() && runs.pending() == 0 && runs.dropped() == 1,
          "a fed run the cache had no room for was kept, or was counted "
          "as a group that was not handed over");
        weights.clear();
        ballast.clear();
        {
            auto reader = co_await read_tail(w, resources, work, drive);
            const auto before = cache.snapshot();
            const auto read = co_await drive.lifecycle(
              reader.read(offset(first + 4), all, asking));
            require(
              w.matches(read, 4, 2)
                && cache.snapshot().uncached == before.uncached + 1,
              "a run the cache had no room for was not read from the "
              "device");
        }
        require(
          !w.writer->failure().failed(),
          "feeding or reading a segment failed it");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    weights.clear();
    ballast.clear();
    co_await close_tail(w, failed, drive);
    take(failed.outcome());
}

// The bytes of written groups that wait to be copied are bounded. A group
// that does not fit is not handed over: it is counted, nothing waits for
// it, nothing of it is kept, and it is read from the device like any other.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> fed_limit(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    // Room for half a group.
    auto world = co_await open_tail<Clock>(
      files,
      owner,
      spec,
      resources,
      work,
      drive,
      {.limits = run_limits(),
       .feed_bytes = segments::descriptor().alignment.bytes()});
    auto& w = *world;
    auto& cache = *w.cache;
    auto& runs = *w.runs;
    runtime::first_failure failed;
    try {
        const auto first = w.first();
        seastar::abort_source asking;
        const byte_count all{1_MiB};
        runs.establish(1);
        std::optional<segment_captured_boundary> cut;
        for (std::uint64_t group = 0; group < 3; ++group)
            cut.emplace(co_await write_tail(w, group, resources, work, drive));
        co_await sync_tail(w, *cut, drive);
        require(
          runs.fed() && runs.dropped() == 3 && runs.pending() == 0
            && cache.snapshot().loads == 0 && cache.snapshot().bytes == 0,
          "a group that did not fit the feed's bound was kept");
        {
            auto reader = co_await read_tail(w, resources, work, drive);
            const auto read = co_await drive.lifecycle(
              reader.read(offset(first), all, asking));
            require(
              w.matches(read, 0, 3) && cache.snapshot().loads == 2,
              "what the feed did not keep was not read from the device");
        }
        require(
          !w.writer->failure().failed(),
          "a feed that kept nothing failed its segment");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    co_await close_tail(w, failed, drive);
    take(failed.outcome());
}

// What a backend that counts and fails its reads is asked for: the object a
// file is, and how many reads it has had.
struct device_probe final {
    std::uint64_t object{0};
    std::uint64_t reads{0};
};

// A read of a segment's data that fails at the device, for a backend that
// can be told to fail one. The read that met it returns what it read
// before, where it stopped and why; nothing of the failure is kept, and the
// next read goes on from there with one more read of that run. Every run
// is otherwise read from the device once.
//
// `probe` says what the segment's data file is and how many reads it has
// had, which `learned` is given before anything is read. With `faulted`,
// the second read of that file after that fails.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Probe>
seastar::future<> failed_reads(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive,
  bool faulted,
  device_probe& learned,
  Probe probe) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    auto world = co_await open_world<Clock>(
      files, owner, spec, resources, work, drive, 9);
    auto& w = *world;
    auto& cache = *w.cache;
    std::unique_ptr<segment_runs> runs;
    runtime::first_failure failed;
    try {
        const auto first = w.first();
        const auto last = w.last();
        const auto path = indexes::data_path(spec);
        seastar::abort_source asking;
        const byte_count all{1_MiB};
        learned = probe(path);
        runs = w.runs(resources);
        {
            auto reader = w.session(*runs, resources, last);
            const auto got = co_await drive.lifecycle(
              reader.read(offset(first), all, asking));
            if (!faulted) {
                require(
                  w.matches(got, 0, 9)
                    && got.stop == segment_read_stop::end_of_segment
                    && probe(path).reads == learned.reads + 5,
                  "a segment's runs were not each read from the device "
                  "once");
            } else {
                require(
                  w.matches(got, 0, 2) && got.stop == segment_read_stop::failed
                    && got.error && got.error->code() == errc::io_failure
                    && got.next
                         == segment_read_position{model::range_logical_end{w.base(2)}, runtime::file_position{w.position(2)}}
                    && !runs->holds(offset(w.base(2)))
                    && probe(path).reads == learned.reads + 2,
                  "a read that failed at the device did not return what it "
                  "read before, or kept something of the failure");
                const auto rest = co_await drive.lifecycle(
                  reader.read({got.next, all, true}, asking));
                require(
                  w.matches(rest, 2, 7)
                    && rest.stop == segment_read_stop::end_of_segment
                    && probe(path).reads == learned.reads + 6,
                  "the read after one that failed at the device did not go "
                  "on from there with one read of each run");
            }
        }
        take(co_await drive.lifecycle(runs->close()));
        runs.reset();
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (runs) {
        try {
            failed.observe(co_await drive.lifecycle(runs->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        runs.reset();
    }
    co_await close_world(w, failed, drive);
    take(failed.outcome());
    require(
      cache.snapshot().bytes == 0 && w.residency.resident() == 0,
      "what was read is still charged, or an index left its root open");
}

// An index whose bundle cannot be read, for a backend that can be told to
// fail its reads. Each failed read is that lookup's failure and its
// reader's. The third in a row makes the index owed: it is built again from
// the segment's data within the lookup that met it, and its reader is
// answered.
//
// `probe` says what the index's bundle file is and how many reads it has
// had, which `learned` is given before anything looks into the index. With
// `faulted`, the next three reads of that file fail.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Probe>
seastar::future<> starved_index(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive,
  bool faulted,
  device_probe& learned,
  Probe probe) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    auto world = co_await open_world<Clock>(
      files, owner, spec, resources, work, drive, 9);
    auto& w = *world;
    auto& cache = *w.cache;
    std::unique_ptr<segment_runs> runs;
    runtime::first_failure failed;
    try {
        const auto first = w.first();
        const auto last = w.last();
        seastar::abort_source asking;
        const byte_count all{1_MiB};
        learned = probe(take(local_bundle_path(
          spec,
          0,
          *w.segment.sealed.index,
          local_bundle_context{w.segment.context()})));
        runs = w.runs(resources);
        {
            auto reader = w.session(*runs, resources, last);
            if (faulted)
                for (int attempt = 0; attempt < 2; ++attempt) {
                    const auto got = co_await drive.lifecycle(
                      reader.read(offset(first), all, asking));
                    require(
                      got.batches.empty()
                        && got.stop == segment_read_stop::failed && got.error
                        && got.error->code() == errc::io_failure
                        && !w.index->owed()
                        && w.index->source() == sparse_index_source::published,
                      "an index that could not be read once or twice did "
                      "not fail its reader's read, or was given up");
                }
            const auto got = co_await drive.lifecycle(
              reader.read(offset(first), all, asking));
            require(
              w.matches(got, 0, 9)
                && got.stop == segment_read_stop::end_of_segment
                && w.index->owed().has_value() == faulted
                && w.index->source()
                     == (faulted ? sparse_index_source::memory : sparse_index_source::published),
              "an index that could not be read three times running was "
              "not built again for its reader");
        }
        take(co_await drive.lifecycle(runs->close()));
        runs.reset();
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (runs) {
        try {
            failed.observe(co_await drive.lifecycle(runs->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        runs.reset();
    }
    co_await close_world(w, failed, drive);
    take(failed.outcome());
    require(
      cache.snapshot().bytes == 0 && w.residency.resident() == 0,
      "what was read is still charged, or an index left its root open");
}
} // namespace kwaque::storage::testing::segment_reader_contract
