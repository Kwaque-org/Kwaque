#pragma once

#include "src/base/units.h"
#include "src/codec/digest.h"
#include "src/storage/local_generation.h"
#include "src/storage/local_paths.h"
#include "src/storage/recovery_pins.h"
#include "src/storage/retry_snapshot.h"
#include "src/storage/segment_format.h"
#include "src/storage/sparse_index.h"
#include "src/storage/sparse_index_owner.h"
#include "src/storage/tests/segment_writer_contract.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>

namespace kwaque::storage::testing::sparse_index_contract {
using store_contract::require;
using store_contract::take;
namespace installation = installation_contract;
namespace segments = segment_writer_contract;

inline segment_scan_limits scan_limits() {
    return {
      store_contract::limits(),
      {.window_bytes = byte_count{16_KiB}, .read_ahead = 2},
      byte_count{512_KiB},
      byte_count{64_KiB}};
}

// An index made for a segment and fed by its writer: sized from the
// segment's own limits before any append, empty until a barrier covers a
// block, and afterwards holding anchors that name the blocks written there.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> active_feed(
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
    config.maximum_groups = 2;
    config.admission.working_bytes = byte_count{1_MiB};
    config.admission.maximum_blocks = 16;
    const auto described = segments::descriptor();
    // Every block is an anchor, so every block is checked.
    const sparse_index_stride stride{byte_count{0}};
    const auto capacity = sparse_index_capacity(
      stride,
      described.maximum_data_bytes,
      std::min(
        config.admission.maximum_blocks,
        config.admission.maximum_retry_entries));
    require(capacity == 16, "the index was not sized by the segment's blocks");
    const auto before = resources.snapshot();
    auto index = take(active_sparse_index::make(stride, capacity, resources));
    const auto admitted = resources.snapshot();
    require(
      admitted.bytes > before.bytes,
      "the index was not admitted when its segment was made");
    auto writer = take(
      writer_type::make_new(
        files, owner, spec, 0, described, resources, config));
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        take(writer->observe_blocks(index.observer()));
        auto first = co_await segments::freeze_child(
          *writer, co_await segments::child(work, 4096), resources, work);
        const auto first_cut = first.layout().boundary();
        auto second = co_await segments::freeze_child(
          *writer,
          co_await segments::execution_child(101, work),
          resources,
          work);
        const auto second_cut = second.layout().boundary();
        take(co_await writer->encode_group(first, work));
        take(co_await writer->encode_group(second, work));
        const std::array blocks{
          first.blocks()[0].descriptor(), second.blocks()[0].descriptor()};
        auto a = take(writer->submit(std::move(first), work));
        auto b = take(writer->submit(std::move(second), work));
        const auto first_written = co_await drive.lifecycle(
          std::move(a.written));
        const auto second_written = co_await drive.lifecycle(
          std::move(b.written));
        take(first_written.failure.outcome());
        take(second_written.failure.outcome());
        require(
          index.empty() && writer->progress()->written == second_cut.end(),
          "a written block was an anchor before it was durable");
        const auto first_sync = co_await drive.lifecycle(
          writer->barrier(first_cut));
        take(first_sync.failure.outcome());
        require(
          index.size() == 1,
          "a barrier's cut did not bound the anchors it made");
        const auto second_sync = co_await drive.lifecycle(
          writer->barrier(second_cut));
        take(second_sync.failure.outcome());
        require(
          index.size() == blocks.size() && index.skipped() == 0,
          "a durable block is not an anchor");
        const auto context = take(
          sparse_index_context::make(
            described.segment,
            second_cut.covered(),
            codec::extent_digest{codec::content_digest{}},
            described.alignment));
        for (std::uint32_t i = 0; i < index.size(); ++i)
            require(
              validate_sparse_index_anchor(context, index[i], blocks[i])
                .has_value(),
              "an anchor does not name the block written there");
        require(
          resources.snapshot().bytes >= admitted.bytes
            && !writer->failure().failed(),
          "feeding the index released its admission or failed the segment");
        take(co_await drive.lifecycle(writer->close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        failed.observe(co_await drive.lifecycle(writer->close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    writer.reset();
    take(failed.outcome());
}

// Opens the sealed generation a writer published and checks the index it
// names against the index the writer fed: one root that opens without debt,
// each page beginning where the root says, and page bytes equal to the
// frozen pages encoded again.
template<typename Writer, typename Backend, typename Owner, typename Driver>
seastar::future<> check_published_index(
  Writer& writer,
  const active_sparse_index& index,
  local_root_reference named,
  const segment_seal_outcome& sealed,
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  codec::cooperative_work& work,
  Driver drive) {
    const auto described = segments::descriptor();
    const auto context = take(
      sparse_index_context::make(
        described.segment,
        sealed.extent->coverage,
        sealed.extent->digest,
        described.alignment));
    const std::array<local_bundle_context, 2> contexts{
      context,
      footer_expectation{
        writer.seal_progress().extent->context(), sealed.boundary->position()}};
    const local_generation_expectation expected{
      *writer.publication_generation(),
      {described.segment,
       local_object_state::sealed,
       *sealed.boundary,
       {named, *sealed.retry}},
      described,
      contexts};
    auto generation = take(
      co_await drive.lifecycle(
        local_generation_owner::open(
          files,
          owner,
          spec,
          0,
          expected,
          resources,
          store_contract::limits(),
          work)));
    runtime::first_failure failed;
    try {
        auto pin = take(generation->pin());
        require(
          !pin.index_rebuild_reason(),
          "a published index did not open under its sealed generation");
        auto root = take(pin.root(local_root_kind::index));
        const auto& meta = std::get<sparse_index_root>(root.metadata());
        require(
          root.reference() == named && meta.entry_count() == index.size()
            && meta.pages().size() == index.pages()
            && meta.first_anchors().size() == index.pages()
            && meta.context() == context,
          "the published root is not the frozen index's");
        for (std::uint32_t i = 0; i < index.pages(); ++i) {
            const auto anchors = index.page(i);
            require(
              meta.first_anchors()[i] == anchors.front().logical_anchor()
                && meta.pages()[i].entry_count() == anchors.size()
                && meta.pages()[i].first_entry()
                     == i * sparse_index_page_entries,
              "a page does not begin where its root says");
            const auto ordinal = page_ordinal::make(i).value();
            auto page = take(
              co_await drive.lifecycle(root.read(ordinal, work)));
            auto again = co_await encode_sparse_index_page(
              anchors,
              context,
              ordinal,
              i * sparse_index_page_entries,
              work,
              store_contract::limits().operation_bytes,
              charge);
            require(
              again.has_value() && again->reference == meta.pages()[i]
                && again->bytes.content_equals(flat(page.bytes)),
              "a stored page is not the frozen page");
        }
        // Lookups through the root answer as the index that was frozen does.
        const auto end = context.coverage().bytes().end();
        const auto base = index[0].logical_anchor().value();
        if (base != 0) {
            const auto before = take(
              co_await drive.lifecycle(find_published_anchor(
                root,
                model::range_logical_offset::make(base - 1).value(),
                work)));
            require(!before, "an offset before the first anchor has one");
        }
        for (std::uint32_t i = 0; i <= index.size(); ++i) {
            // Each anchor's own base, and an offset past every block.
            const auto target = i < index.size()
                                  ? index[i].logical_anchor()
                                  : model::range_logical_offset::make(
                                      std::numeric_limits<std::uint64_t>::max()
                                      - 1)
                                      .value();
            const auto found = take(
              co_await drive.lifecycle(
                find_published_anchor(root, target, work)));
            const auto anchor = std::min(i, index.size() - 1);
            require(
              found && found->anchor == index[anchor]
                && found == index.find(target, end)
                && found->end
                     == (anchor + 1 < index.size() ? index[anchor + 1].block_position() : end),
              "a lookup did not return the nearest anchor and its scan end");
        }
        // Every anchor names a block of the extent the index is bound to.
        require(
          !take(
            co_await drive.lifecycle(scrub_published_index(
              files,
              owner,
              spec,
              0,
              writer.seal_progress().extent->context(),
              root,
              resources,
              scan_limits(),
              work))),
          "an index its writer fed did not pass the check of all of it");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        failed.observe(co_await drive.lifecycle(generation->close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    generation.reset();
    take(failed.outcome());
}

// One byte of the index bundle is changed on the device, in three places:
// in the first page's first entry, which the page's own checksum covers; in
// the length the page's envelope begins with, which only the reference its
// root pins it with covers; and in the same length of the root, which only
// the publication's pin covers. The sealed generation opens each time and
// its other root stays usable. A damaged page is not read by the open, and
// the lookup that reads it fails with an error about the index alone. A
// damaged root is owed. Bytes that are not the pinned ones are damage
// whatever their decoder met first: a length no page can have is not a
// refusal of the reader's limits.
template<typename Writer, typename Backend, typename Owner, typename Driver>
seastar::future<> damaged_page(
  Writer& writer,
  local_root_reference named,
  sparse_index_context context,
  const segment_seal_outcome& sealed,
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  codec::cooperative_work& work,
  Driver drive) {
    const auto described = segments::descriptor();
    const auto path = take(
      local_bundle_path(spec, 0, named, local_bundle_context{context}));
    const auto pristine = co_await store_contract::read_bytes(
      files, path, drive);
    const auto page = named.bytes().value();
    require(
      pristine.size() > page + 32 + 172, "the index bundle holds no page");
    const std::array<local_bundle_context, 2> contexts{
      context,
      footer_expectation{
        writer.seal_progress().extent->context(), sealed.boundary->position()}};
    const local_generation_expectation expected{
      *writer.publication_generation(),
      {described.segment,
       local_object_state::sealed,
       *sealed.boundary,
       {named, *sealed.retry}},
      described,
      contexts};
    // The top byte of an envelope's body length is its sixteenth.
    enum class damage : std::uint8_t { entry, page_length, root_length };
    for (const auto where :
         {damage::entry, damage::page_length, damage::root_length}) {
        auto bytes = pristine;
        bytes
          [where == damage::entry         ? page + 32 + 172
           : where == damage::page_length ? page + 15
                                          : 15] ^= 1;
        co_await store_contract::write_bytes(files, path, bytes, drive);
        auto generation = take(
          co_await drive.lifecycle(
            local_generation_owner::open(
              files,
              owner,
              spec,
              0,
              expected,
              resources,
              store_contract::limits(),
              work)));
        runtime::first_failure failed;
        try {
            auto pin = take(generation->pin());
            if (where == damage::root_length) {
                const auto owed = pin.index_rebuild_reason();
                require(
                  owed && owed->code() == errc::corrupt_data
                    && !pin.root(local_root_kind::index),
                  "a root that is not the one its publication pins opened, "
                  "or is not owed as damage");
            } else {
                require(
                  !pin.index_rebuild_reason(),
                  "opening an index read a page it was not asked for");
                auto root = take(pin.root(local_root_kind::index));
                const auto& meta = std::get<sparse_index_root>(root.metadata());
                const auto found = co_await drive.lifecycle(
                  find_published_anchor(root, meta.first_anchors()[0], work));
                require(
                  !found
                    && detail::rebuildable_local_index_error(
                      found.error().code())
                    && (where == damage::entry || found.error().code() == errc::corrupt_data),
                  "a damaged page answered a lookup, or failed as anything "
                  "but an index to build again");
            }
            require(
              bool(pin.root(local_root_kind::sealed_retry)),
              "a damaged index cost the generation another root");
        } catch (...) {
            failed.observe(std::current_exception());
        }
        try {
            failed.observe(co_await drive.lifecycle(generation->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        generation.reset();
        take(failed.outcome());
    }
}

// A seal given the index its writer fed names it in its one sealed
// publication. A seal whose index cannot be published seals all the same,
// without one; the index is then named afterwards, and again when it is
// replaced, while a reader keeps the handle it was using.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> sealed_index(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive,
  bool late = false) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    auto config = segments::configuration();
    config.maximum_groups = 2;
    config.admission.working_bytes = byte_count{1_MiB};
    config.admission.maximum_blocks = 16;
    const auto described = segments::descriptor();
    const sparse_index_stride stride{byte_count{0}};
    const auto capacity = sparse_index_capacity(
      stride, described.maximum_data_bytes, config.admission.maximum_blocks);
    take(validate_sparse_index_pages(
      config.metadata, described.alignment, work.policy(), capacity));
    auto index = take(active_sparse_index::make(stride, capacity, resources));
    const auto object = local_object_sequence::make(46).value();
    auto writer = take(
      writer_type::make_new(
        files, owner, spec, 0, described, resources, config));
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        take(writer->observe_blocks(index.observer()));
        auto first = co_await segments::freeze_child(
          *writer, co_await segments::child(work, 4096), resources, work);
        auto second = co_await segments::freeze_child(
          *writer,
          co_await segments::execution_child(101, work),
          resources,
          work);
        take(co_await writer->encode_group(first, work));
        take(co_await writer->encode_group(second, work));
        const std::array blocks{
          first.blocks()[0].descriptor(), second.blocks()[0].descriptor()};
        const auto between = first.layout().boundary().footer()->begin();
        auto a = take(writer->submit(std::move(first), work));
        auto b = take(writer->submit(std::move(second), work));
        const auto first_written = co_await drive.lifecycle(
          std::move(a.written));
        const auto second_written = co_await drive.lifecycle(
          std::move(b.written));
        take(first_written.failure.outcome());
        take(second_written.failure.outcome());
        // No barrier ran: the seal's data sync makes both blocks durable.
        require(index.empty(), "a written block was an anchor before the seal");
        const auto before = writer->publication_generation()->value();
        std::uint32_t calls = 0;
        // Nothing can publish under an object sequence that is not one.
        const auto sealed = co_await drive.lifecycle(writer->seal(
          segments::completed_source{{}, {}, &calls},
          0,
          2,
          work,
          sparse_index_seal{&index, late ? local_object_sequence{} : object}));
        take(sealed.failure.outcome());
        require(
          sealed.boundary && sealed.retry && sealed.extent
            && sealed.unresolved == 2
            && writer->append_state() == model::append_state::sealed
            && writer->publication_generation()->value() == before + 1
            && index.frozen() && index.size() == 2 && index.skipped() == 0,
          "the seal did not finish, or left its index unfrozen");
        const auto data = sealed.extent->coverage.bytes().begin();
        const auto length = described.alignment.bytes();
        // The check a reader makes on the block it reads where an anchor
        // points, and the same check over every anchor and block.
        const auto context = take(
          sparse_index_context::make(
            described.segment,
            sealed.extent->coverage,
            sealed.extent->digest,
            described.alignment));
        const auto merged = [&](
                              std::span<const sparse_index_entry> anchors,
                              std::uint32_t total) {
            sparse_index_scrub scrub{context};
            scrub.supply(anchors);
            for (const auto& block : blocks)
                if (auto valid = scrub.block(block); !valid) return valid;
            return scrub.finish(total);
        };
        const std::array named{index[0], index[1]};
        const sparse_index_entry rebased{
          model::range_logical_offset::make(
            index[1].logical_anchor().value() + 1)
            .value(),
          index[1].block_position()};
        const sparse_index_entry misplaced{index[1].logical_anchor(), between};
        require(
          validate_sparse_index_anchor(context, index[0], blocks[0])
            && validate_sparse_index_anchor(context, index[1], blocks[1])
            && !validate_sparse_index_anchor(context, rebased, blocks[1])
            && !validate_sparse_index_anchor(context, index[1], blocks[0])
            && !validate_sparse_index_anchor(context, misplaced, blocks[1]),
          "a block read where an anchor points did not decide the anchor");
        // An anchor where no block begins, after every block that does.
        const sparse_index_entry left{
          rebased.logical_anchor(), blocks[1].coverage().bytes().end()};
        require(
          merged(named, 2).has_value()
            && !merged(std::array{index[0], rebased}, 2)
            && !merged(std::array{index[0], misplaced}, 2)
            && !merged(std::span{named}.first(1), 2) && !merged(named, 3)
            && !merged(std::array{index[0], index[1], left}, 2),
          "the merge passed an anchor without its block, or lost one");
        // An index that leaves its first block out answers that nothing
        // precedes that block's records.
        require(
          !merged(std::span{named}.subspan(1), 1),
          "the merge passed an index without its first block");
        if (!late) {
            require(
              sealed.index && sealed.index->kind() == local_root_kind::index
                && sealed.index->sequence() == object
                && !writer->seal_progress().index.failed(),
              "the sealed publication does not name the index");
            co_await check_published_index(
              *writer,
              index,
              *sealed.index,
              sealed,
              files,
              owner,
              spec,
              resources,
              work,
              drive);
            // A reader reopens under the publication that names the index.
            take(
              co_await drive.lifecycle(
                writer->read_immutable(data, length, work)));
            take(co_await drive.lifecycle(writer->evict_read_handle()));
            co_await damaged_page(
              *writer,
              *sealed.index,
              context,
              sealed,
              files,
              owner,
              spec,
              resources,
              work,
              drive);
        } else {
            const auto& reason = writer->seal_progress().index;
            require(
              !sealed.index && reason.error()
                && reason.error()->code() == errc::invalid_argument,
              "an index that could not be published was named, or its "
              "failure was lost");
            take(
              co_await drive.lifecycle(
                writer->read_immutable(data, length, work)));
            require(
              writer->handle_state() == segment_handle_state::open,
              "a read did not open the sealed segment");
            // Sealed without an index, the segment has it built again from
            // its data: one walk that a second request joins, an index that
            // answers from memory at once, and then its name in the pointer.
            sparse_index_rebuild_limit shard;
            sparse_index_rebuilder rebuilder{shard};
            std::optional<active_sparse_index> rebuilt;
            seastar::abort_source asking;
            std::uint32_t walks = 0;
            seastar::promise<> held;
            const auto history = writer->seal_progress().extent->context();
            const auto rebuild = [&](seastar::abort_source& stop)
              -> seastar::future<runtime::result<void>> {
                ++walks;
                co_await held.get_future();
                codec::cooperative_work walking{
                  codec::limits::defaults(), stop};
                auto made = co_await rebuild_sparse_index(
                  files,
                  owner,
                  spec,
                  0,
                  history,
                  context,
                  stride,
                  resources,
                  scan_limits(),
                  walking);
                if (!made) co_return runtime::failure(made.error());
                rebuilt.emplace(std::move(*made));
                co_return runtime::result<void>{};
            };
            auto first_request = rebuilder.run(rebuild, asking);
            auto second_request = rebuilder.run(rebuild, asking);
            require(
              walks == 1 && rebuilder.waiters() == 2 && shard.running() == 1,
              "a second request did not join the rebuild in flight");
            held.set_value();
            take(co_await drive.lifecycle(std::move(first_request)));
            take(co_await drive.lifecycle(std::move(second_request)));
            co_await rebuilder.stop();
            const auto extent_end = context.coverage().bytes().end();
            require(
              walks == 1 && shard.running() == 0 && rebuilt && rebuilt->frozen()
                && rebuilt->size() == index.size() && (*rebuilt)[0] == index[0]
                && (*rebuilt)[1] == index[1]
                && rebuilt->find(index[1].logical_anchor(), extent_end)
                     == index.find(index[1].logical_anchor(), extent_end),
              "a rebuild walked twice, or is not the index the writer fed");
            require(
              (co_await store_contract::read_bytes(
                 files,
                 take(take(local_paths::make(spec.root))
                        .segment_file(
                          0,
                          {described.segment.segment(),
                           described.segment.generation()},
                          local_segment_file::data)),
                 drive))
                  .size()
                >= std::min<std::uint64_t>(extent_end.value(), 65536),
              "a rebuild shortened the data it read");
            const auto named = take(
              co_await drive.lifecycle(writer->publish_index(
                sparse_index_seal{&*rebuilt, object}, work)));
            require(
              named.kind() == local_root_kind::index
                && writer->publication_generation()->value() == before + 2
                && writer->handle_state() == segment_handle_state::open
                && !writer->failure().failed(),
              "naming the index closed a reader's handle or failed the owner");
            take(
              co_await drive.lifecycle(
                writer->read_immutable(data, length, work)));
            // An index published again replaces the root the pointer names.
            const auto renamed = take(
              co_await drive.lifecycle(writer->publish_index(
                sparse_index_seal{
                  &index, local_object_sequence::make(47).value()},
                work)));
            require(
              renamed != named
                && writer->publication_generation()->value() == before + 3
                && writer->handle_state() == segment_handle_state::open,
              "a second index did not replace the first");
            take(co_await drive.lifecycle(writer->evict_read_handle()));
            co_await check_published_index(
              *writer,
              index,
              renamed,
              sealed,
              files,
              owner,
              spec,
              resources,
              work,
              drive);
            take(
              co_await drive.lifecycle(
                writer->read_immutable(data, length, work)));
        }
        take(co_await drive.lifecycle(writer->close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        failed.observe(co_await drive.lifecycle(writer->close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    writer.reset();
    take(failed.outcome());
}

// An extent without a record has no index: its seal asks for none, and none
// can be named or built for it afterwards.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> empty_extent(
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
    config.admission.working_bytes = byte_count{1_MiB};
    const auto described = segments::descriptor();
    const sparse_index_stride stride{byte_count{0}};
    auto index = take(active_sparse_index::make(stride, 16, resources));
    const auto object = local_object_sequence::make(46).value();
    auto writer = take(
      writer_type::make_new(
        files, owner, spec, 0, described, resources, config));
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        std::uint32_t calls = 0;
        const auto sealed = co_await drive.lifecycle(writer->seal(
          segments::completed_source{{}, {}, &calls},
          0,
          0,
          work,
          sparse_index_seal{&index, object}));
        take(sealed.failure.outcome());
        require(
          sealed.extent && sealed.extent->coverage.physical().empty()
            && !sealed.index && !writer->seal_progress().index.failed()
            && !index.frozen() && index.empty(),
          "the seal of an empty extent asked for an index");
        const auto named = co_await drive.lifecycle(
          writer->publish_index(sparse_index_seal{&index, object}, work));
        require(
          !named && named.error().code() == errc::invalid_argument
            && !writer->failure().failed(),
          "an index was named for an extent without a record");
        const auto context = take(
          sparse_index_context::make(
            described.segment,
            sealed.extent->coverage,
            sealed.extent->digest,
            described.alignment));
        const auto rebuilt = co_await drive.lifecycle(rebuild_sparse_index(
          files,
          owner,
          spec,
          0,
          writer->seal_progress().extent->context(),
          context,
          stride,
          resources,
          scan_limits(),
          work));
        require(
          !rebuilt && rebuilt.error().code() == errc::invalid_argument,
          "an index was built for an extent without a record");
        // Its owner owes nothing and finds nothing: there is no index to
        // open, to build or to name.
        {
            sparse_index_residency residency;
            sparse_index_rebuild_limit rebuilds;
            auto unindexed = take(
              sealed_sparse_index<Backend, Owner>::make(
                files,
                owner,
                spec,
                0,
                writer->seal_progress().extent->context(),
                context,
                stride,
                std::nullopt,
                std::nullopt,
                residency,
                rebuilds,
                resources,
                resources,
                store_contract::limits(),
                scan_limits(),
                work.policy()));
            seastar::abort_source asking;
            const bool owed = unindexed->owed().has_value();
            const auto source = unindexed->source();
            const auto found = co_await drive.lifecycle(unindexed->find(
              model::range_logical_offset::make(
                context.coverage().logical().begin().value())
                .value(),
              work));
            const auto built = co_await drive.lifecycle(
              unindexed->rebuild(asking));
            const auto pinned = co_await drive.lifecycle(unindexed->pin(work));
            const auto closed = co_await drive.lifecycle(unindexed->close());
            require(
              !owed && source == sparse_index_source::published && found
                && !*found && built && !pinned
                && pinned.error().code() == errc::not_found && closed
                && rebuilds.running() == 0 && residency.resident() == 0,
              "an extent without a record owes an index, or was looked "
              "into for one");
        }
        take(co_await drive.lifecycle(writer->close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        failed.observe(co_await drive.lifecycle(writer->close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    writer.reset();
    take(failed.outcome());
}

// A sealed segment of one-record blocks, each written as its own group, and
// the index its writer fed.
template<typename Writer>
struct sealed_segment final {
    std::unique_ptr<Writer> writer;
    std::unique_ptr<active_sparse_index> index;
    segment_seal_outcome sealed;
    std::vector<complete_block_descriptor> blocks;

    [[nodiscard]] segment_history_context history() const {
        return writer->seal_progress().extent->context();
    }
    [[nodiscard]] sparse_index_context context() const {
        const auto described = segments::descriptor();
        return take(
          sparse_index_context::make(
            described.segment,
            sealed.extent->coverage,
            sealed.extent->digest,
            described.alignment));
    }
};

// Writes `blocks` blocks and seals them. With `object` the seal publishes the
// index under it; without, it is asked to publish under a sequence that is
// none, so the segment is sealed and its index is not named.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<sealed_segment<segment_writer<Backend, Owner, Clock>>>
seal_segment(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  codec::cooperative_work& work,
  Driver drive,
  std::uint32_t blocks,
  sparse_index_stride stride,
  std::optional<local_object_sequence> object) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    auto config = segments::configuration();
    config.maximum_groups = blocks;
    config.admission.working_bytes = byte_count{1_MiB};
    config.admission.maximum_blocks = 16;
    const auto described = segments::descriptor();
    sealed_segment<writer_type> output;
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
        for (std::uint32_t i = 0; i < blocks; ++i) {
            std::optional<encoded_assigned_batch> batch;
            if (i == 0)
                batch.emplace(co_await segments::child(work));
            else
                batch.emplace(
                  co_await segments::execution_child(100 + i, work));
            auto group = co_await segments::freeze_child(
              *output.writer, std::move(*batch), resources, work);
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
            output.index.get(), object.value_or(local_object_sequence{})}));
        take(output.sealed.failure.outcome());
        require(
          output.sealed.boundary && output.sealed.retry && output.sealed.extent
            && output.index->frozen() && output.index->skipped() == 0
            && output.sealed.index.has_value() == object.has_value(),
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

inline runtime::file_path data_path(const local_device_spec& spec) {
    const auto described = segments::descriptor();
    return take(
      take(local_paths::make(spec.root))
        .segment_file(
          0,
          {described.segment.segment(), described.segment.generation()},
          local_segment_file::data));
}

template<typename Backend, typename Driver>
seastar::future<bool> bundle_exists(
  Backend& files,
  const local_device_spec& spec,
  local_root_reference named,
  const sparse_index_context& context,
  Driver drive) {
    co_return take(
      co_await drive.lifecycle(files.exists(take(
        local_bundle_path(spec, 0, named, local_bundle_context{context})))));
}

template<typename Backend, typename Owner>
std::unique_ptr<sealed_sparse_index<Backend, Owner>> owned_index(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  segment_history_context history,
  sparse_index_context context,
  sparse_index_stride stride,
  std::optional<local_root_reference> named,
  std::optional<runtime::operation_error> owed,
  sparse_index_residency& residency,
  sparse_index_rebuild_limit& rebuilds,
  workload_budget& resources,
  const codec::cooperative_work& work) {
    return take(
      sealed_sparse_index<Backend, Owner>::make(
        files,
        owner,
        spec,
        0,
        history,
        context,
        stride,
        named,
        owed,
        residency,
        rebuilds,
        resources,
        resources,
        store_contract::limits(),
        scan_limits(),
        work.policy()));
}

// Every block's own base finds the anchor of that block, and its scan ends
// where the next block begins.
template<typename Index, typename Driver>
seastar::future<> check_every_block(
  Index& index,
  const sparse_index_context& context,
  std::span<const complete_block_descriptor> blocks,
  codec::cooperative_work& work,
  Driver drive) {
    for (std::size_t i = 0; i < blocks.size(); ++i) {
        const auto found = take(
          co_await drive.lifecycle(index.find(
            model::range_logical_offset::make(
              blocks[i].coverage().logical().begin().value())
              .value(),
            work)));
        require(
          found
            && validate_sparse_index_anchor(context, found->anchor, blocks[i])
                 .has_value()
            && found->end
                 == (i + 1 < blocks.size()
                       ? blocks[i + 1].coverage().bytes().begin()
                       : context.coverage().bytes().end()),
          "a lookup did not lead to the block that holds its offset");
    }
}

// An index whose bytes are exactly what was published, checksums and digests
// included, and whose anchor names no block: it opens and answers, the block
// read where it points refuses it, and so does a check of the whole index.
// From then on it is owed to its one owner, which builds it again from the
// data for everyone who asks, answers from memory, and names it in the
// publication. A page that can no longer be read goes the same way. None of
// it touches the data or the segment's owner, and no bundle that nothing
// names is left behind. Data that is no longer what was sealed is never
// given a new index.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> owed_index(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    const sparse_index_stride stride{byte_count{0}};
    const auto sequence = [](std::uint64_t value) {
        return local_object_sequence::make(value).value();
    };
    auto segment = co_await seal_segment<Clock>(
      files, owner, spec, resources, work, drive, 2, stride, sequence(46));
    auto& writer = *segment.writer;
    const auto& blocks = segment.blocks;
    sparse_index_residency residency{2};
    sparse_index_rebuild_limit rebuilds;
    std::unique_ptr<sealed_sparse_index<Backend, Owner>> index;
    runtime::first_failure failed;
    try {
        const auto context = segment.context();
        const auto history = segment.history();
        const auto end = context.coverage().bytes().end();
        const auto second = model::range_logical_offset::make(
                              blocks[1].coverage().logical().begin().value())
                              .value();
        const auto data = co_await store_contract::read_all_bytes(
          files, data_path(spec), drive);
        // The second block's base, at the footer that lies between the two
        // blocks: inside the extent, aligned and in order, and no block.
        const auto between = blocks[0].coverage().bytes().end();
        auto wrong = take(active_sparse_index::make(stride, 2, resources));
        wrong.written(blocks[0].coverage());
        wrong.written(
          coverage{
            blocks[1].coverage().logical(),
            blocks[1].coverage().physical(),
            model::file_byte_span::from_size(
              between, segments::descriptor().alignment.bytes())
              .value()});
        wrong.durable(end);
        wrong.freeze();
        const auto sealed_root = *segment.sealed.index;
        const auto misnamed = take(
          co_await drive.lifecycle(writer.publish_index(
            sparse_index_seal{&wrong, sequence(47)}, work)));
        require(
          !co_await bundle_exists(files, spec, sealed_root, context, drive)
            && co_await bundle_exists(files, spec, misnamed, context, drive),
          "the index bundle a later one replaced was left, or the new one "
          "is gone");
        index = owned_index(
          files,
          owner,
          spec,
          history,
          context,
          stride,
          misnamed,
          std::nullopt,
          residency,
          rebuilds,
          resources,
          work);
        require(
          index->source() == sparse_index_source::published && !index->owed()
            && !index->resident() && index->loads() == 0
            && residency.resident() == 0,
          "an index was opened before anything looked into it");
        const auto misled = take(
          co_await drive.lifecycle(index->find(second, work)));
        require(
          misled && misled->anchor.logical_anchor() == second
            && misled->anchor.block_position() == between && index->resident()
            && index->loads() == 1 && residency.resident() == 1,
          "an index with authentic bytes did not open and answer");
        require(
          !validate_sparse_index_anchor(context, misled->anchor, blocks[1]),
          "the block read where a wrong anchor points accepted it");
        {
            auto root = take(co_await drive.lifecycle(index->pin(work)));
            const auto scrubbed = co_await drive.lifecycle(
              scrub_published_index(
                files,
                owner,
                spec,
                0,
                history,
                root,
                resources,
                scan_limits(),
                work));
            // The walk ran to its end, and its verdict is about the index.
            const auto verdict = take(scrubbed);
            require(
              verdict && detail::rebuildable_local_index_error(verdict->code()),
              "a wrong anchor passed the check of the whole index");
            // A report about another root is about an index that is gone.
            index->owe(*segment.sealed.index, *verdict);
            require(
              index->source() == sparse_index_source::published
                && index->resident(),
              "a report about another root changed the one that answers");
            index->owe(root.reference(), *verdict);
            require(
              index->source() == sparse_index_source::owed && index->owed()
                && *index->owed() == *verdict && !index->resident(),
              "a wrong index is not owed");
        }
        const auto refused = co_await drive.lifecycle(
          index->find(second, work));
        require(
          !refused && refused.error() == *index->owed(),
          "an owed index answered a lookup");
        // Two ask, one walk builds it, and it answers at once.
        seastar::abort_source asking;
        auto first_request = index->rebuild(asking);
        auto second_request = index->rebuild(asking);
        require(
          rebuilds.running() == 1, "two requests did not share one rebuild");
        take(co_await drive.lifecycle(std::move(first_request)));
        take(co_await drive.lifecycle(std::move(second_request)));
        require(
          index->source() == sparse_index_source::memory
            && rebuilds.running() == 0 && index->loads() == 1,
          "the index built again does not answer from memory");
        take(co_await drive.lifecycle(index->rebuild(asking)));
        co_await check_every_block(*index, context, blocks, work, drive);
        const auto named = take(
          co_await drive.lifecycle(index->publish(writer, sequence(48), work)));
        require(
          index->source() == sparse_index_source::published && !index->owed()
            && index->named() == std::optional{named}
            && !co_await bundle_exists(files, spec, misnamed, context, drive)
            && co_await bundle_exists(files, spec, named, context, drive),
          "the index built again was not named, or the wrong one was left");
        co_await check_every_block(*index, context, blocks, work, drive);
        {
            auto root = take(co_await drive.lifecycle(index->pin(work)));
            require(
              root.reference() == named && index->loads() == 2,
              "lookups did not move to the root that was named");
            require(
              !take(
                co_await drive.lifecycle(scrub_published_index(
                  files,
                  owner,
                  spec,
                  0,
                  history,
                  root,
                  resources,
                  scan_limits(),
                  work))),
              "the index built again did not pass the check of all of it");
        }
        // A page that cannot be read. Its root is closed first, so the open
        // that follows is seen to read the root alone.
        while (co_await drive.lifecycle(residency.evict())) {
        }
        require(
          !index->resident() && residency.resident() == 0,
          "a root nothing reads through was not given up");
        const auto path = take(
          local_bundle_path(spec, 0, named, local_bundle_context{context}));
        auto bytes = co_await store_contract::read_bytes(files, path, drive);
        bytes[named.bytes().value() + 32 + 172] ^= 1;
        co_await store_contract::write_bytes(files, path, bytes, drive);
        const auto unread = co_await drive.lifecycle(index->find(second, work));
        require(
          !unread
            && detail::rebuildable_local_index_error(unread.error().code())
            && index->loads() == 3
            && index->source() == sparse_index_source::owed,
          "a damaged page answered, failed its root's open, or is not owed");
        take(co_await drive.lifecycle(index->rebuild(asking)));
        co_await check_every_block(*index, context, blocks, work, drive);
        const auto renamed = take(
          co_await drive.lifecycle(index->publish(writer, sequence(49), work)));
        require(
          renamed != named
            && !co_await bundle_exists(files, spec, named, context, drive),
          "the damaged bundle was left once another was named");
        co_await check_every_block(*index, context, blocks, work, drive);
        // What a restart finds when the named bundle is gone: its
        // publication opens, and all it says of the index is why it did not.
        // The owner is given that and nothing else.
        take(co_await drive.lifecycle(index->close()));
        index.reset();
        take(
          co_await drive.lifecycle(files.remove_file(take(local_bundle_path(
            spec, 0, renamed, local_bundle_context{context})))));
        {
            // An owner that was told nothing looks for the root its
            // publication names. The open that does not find it gives its
            // place back, the index is owed for that reason, and whoever
            // arrived for the same open is told the same.
            auto unaware = owned_index(
              files,
              owner,
              spec,
              history,
              context,
              stride,
              renamed,
              std::nullopt,
              residency,
              rebuilds,
              resources,
              work);
            codec::cooperative_work other{codec::limits::defaults(), abort};
            auto one = unaware->pin(work);
            auto two = unaware->find(second, other);
            const auto pinned = co_await drive.lifecycle(std::move(one));
            const auto looked = co_await drive.lifecycle(std::move(two));
            const auto source = unaware->source();
            const auto loads = unaware->loads();
            const auto open = residency.resident();
            const auto closed = co_await drive.lifecycle(unaware->close());
            require(
              !pinned && pinned.error().code() == errc::not_found && !looked
                && looked.error().code() == errc::not_found
                && source == sparse_index_source::owed && loads == 0
                && open == 0 && closed,
              "a root that is gone opened, kept its place, or is not owed");
        }
        const auto restart = [&]() -> seastar::future<std::pair<
                                     local_object_publication,
                                     std::optional<runtime::operation_error>>> {
            auto found = take(
              co_await drive.lifecycle(open_recovery_generation(
                files,
                owner,
                spec,
                0,
                segments::descriptor(),
                resources,
                store_contract::limits(),
                work)));
            found.owner->retire();
            take(co_await drive.lifecycle(found.owner->close()));
            found.owner.reset();
            co_return std::pair{found.publication, found.index_rebuild};
        };
        const auto [found, debt] = co_await restart();
        require(
          found.state == local_object_state::sealed && debt
            && debt->code() == errc::not_found,
          "a missing index bundle failed its publication, or left no reason");
        index = owned_index(
          files,
          owner,
          spec,
          history,
          context,
          stride,
          renamed,
          debt,
          residency,
          rebuilds,
          resources,
          work);
        require(
          index->source() == sparse_index_source::owed
            && *index->owed() == *debt,
          "what a restart found owed is not owed");
        take(co_await drive.lifecycle(index->rebuild(asking)));
        const auto restored = take(
          co_await drive.lifecycle(index->publish(writer, sequence(50), work)));
        const auto [settled, cleared] = co_await restart();
        require(
          !cleared && !settled.roots.empty()
            && settled.roots.front() == restored,
          "the index named again is still owed at the next restart");
        co_await check_every_block(*index, context, blocks, work, drive);
        // Whatever happened to the index, the data is as it was sealed.
        require(
          co_await store_contract::read_all_bytes(files, data_path(spec), drive)
              == data
            && !writer.failure().failed(),
          "an index error changed the data or failed its segment");
        take(
          co_await drive.lifecycle(writer.read_immutable(
            context.coverage().bytes().begin(),
            segments::descriptor().alignment.bytes(),
            work)));
        // An index is built only for the extent its publication sealed: the
        // same data under another digest is refused, though every object of
        // it verifies.
        {
            const auto unbound = co_await drive.lifecycle(rebuild_sparse_index(
              files,
              owner,
              spec,
              0,
              history,
              take(
                sparse_index_context::make(
                  segments::descriptor().segment,
                  segment.sealed.extent->coverage,
                  codec::extent_digest{codec::content_digest{}},
                  segments::descriptor().alignment)),
              stride,
              resources,
              scan_limits(),
              work));
            require(
              !unbound && unbound.error().code() == errc::corrupt_data,
              "an index was built for an extent its data does not hash to");
        }
        // The data is not what was sealed: a byte of a block changed, then
        // the file cut short of its second block. No index is built over
        // other bytes. The rebuild fails the same way each time it is asked,
        // and leaves the data, the pointer and the bundle as it found them.
        take(co_await drive.lifecycle(writer.evict_read_handle()));
        const auto pointer_path = take(
          take(local_paths::make(spec.root))
            .segment_file(
              0,
              {segments::descriptor().segment.segment(),
               segments::descriptor().segment.generation()},
              local_segment_file::published));
        const auto pointer = co_await store_contract::read_bytes(
          files, pointer_path, drive);
        for (const bool cut_short : {false, true}) {
            auto damaged = data;
            if (cut_short)
                damaged.resize(blocks[1].coverage().bytes().begin().value());
            else
                damaged[blocks[0].coverage().bytes().begin().value() + 64] ^= 1;
            co_await store_contract::write_bytes(
              files, data_path(spec), damaged, drive);
            index->owe(detail::path_error(errc::malformed_data));
            const auto asked = co_await drive.lifecycle(index->rebuild(asking));
            const auto again = co_await drive.lifecycle(index->rebuild(asking));
            require(
              !asked && !again && asked.error().code() == again.error().code()
                && index->source() == sparse_index_source::owed
                && rebuilds.running() == 0,
              "an index was built over data that is not what was sealed, or "
              "the refusal changed when it was asked again");
            require(
              co_await store_contract::read_all_bytes(
                files, data_path(spec), drive)
                  == damaged
                && co_await store_contract::read_bytes(
                     files, pointer_path, drive)
                     == pointer
                && co_await bundle_exists(
                  files, spec, restored, context, drive),
              "a failed rebuild changed what it could not use");
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (index) {
        try {
            failed.observe(co_await drive.lifecycle(index->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        index.reset();
    }
    try {
        failed.observe(co_await drive.lifecycle(writer.close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    segment.writer.reset();
    take(failed.outcome());
    require(
      residency.resident() == 0 && !residency.failure().failed(),
      "a closed index left its root open");
}

// Many sealed indexes on one shard. Each is opened by its root alone when it
// is first looked into; no more than the shard's bound are open at once, and
// the one used longest ago that nothing reads through makes room for the
// next. A root something reads through is never taken. Lookups that arrive
// together for an index that is not open make one open. An index that is
// owed gives up its own root and costs no other index anything.
//
// The fixture has one segment, so the indexes are bundles of the same extent
// under different object sequences: what is bounded is open roots.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> resident_roots(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    constexpr std::uint32_t indexes = 5;
    constexpr std::uint32_t places = 3;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    const sparse_index_stride stride{byte_count{0}};
    const auto sequence = [](std::uint64_t value) {
        return local_object_sequence::make(value).value();
    };
    auto segment = co_await seal_segment<Clock>(
      files, owner, spec, resources, work, drive, 2, stride, std::nullopt);
    auto& writer = *segment.writer;
    const auto& blocks = segment.blocks;
    sparse_index_residency residency{places};
    sparse_index_rebuild_limit rebuilds;
    std::vector<std::unique_ptr<sealed_sparse_index<Backend, Owner>>> owners;
    runtime::first_failure failed;
    try {
        const auto context = segment.context();
        const auto history = segment.history();
        const auto first = model::range_logical_offset::make(
                             blocks[0].coverage().logical().begin().value())
                             .value();
        const auto open_index =
          [&](
            std::optional<local_root_reference> named,
            std::optional<runtime::operation_error> owed) {
              owners.push_back(owned_index(
                files,
                owner,
                spec,
                history,
                context,
                stride,
                named,
                owed,
                residency,
                rebuilds,
                resources,
                work));
          };
        const auto look = [&](std::uint32_t at) {
            return drive.lifecycle(owners[at]->find(first, work));
        };
        // Sealed without an index: owed from the start, for the reason the
        // seal kept.
        const auto reason = writer.seal_progress().index.error();
        require(
          reason.has_value(), "the seal kept no reason for its missing index");
        open_index(std::nullopt, reason);
        auto& sealed = *owners[0];
        const auto unanswered = co_await look(0);
        require(
          sealed.source() == sparse_index_source::owed && !unanswered
            && unanswered.error() == *reason,
          "a segment sealed without an index does not owe one");
        // The index its writer fed is whole: it answers without a walk of
        // the data and is named without one, and its memory is then given
        // back.
        const auto fed = resources.snapshot().bytes;
        take(sealed.adopt(std::move(*segment.index)));
        segment.index.reset();
        seastar::abort_source asking;
        take(co_await drive.lifecycle(sealed.rebuild(asking)));
        require(
          sealed.source() == sparse_index_source::memory
            && rebuilds.running() == 0,
          "the index a writer fed did not answer from memory");
        co_await check_every_block(sealed, context, blocks, work, drive);
        const auto named = take(
          co_await drive.lifecycle(sealed.publish(writer, sequence(46), work)));
        require(
          sealed.source() == sparse_index_source::published
            && sealed.named() == std::optional{named} && sealed.loads() == 0
            && resources.snapshot().bytes < fed,
          "the index was not named, or kept its memory once it was");
        std::vector<local_root_reference> unnamed;
        {
            const auto again = take(
              co_await drive.lifecycle(rebuild_sparse_index(
                files,
                owner,
                spec,
                0,
                history,
                context,
                stride,
                resources,
                scan_limits(),
                work)));
            for (std::uint32_t i = 1; i < indexes; ++i) {
                unnamed.push_back(take(
                  co_await drive.lifecycle(publish_sparse_index(
                    files,
                    owner,
                    spec,
                    0,
                    again,
                    context,
                    sequence(50 + i),
                    resources,
                    store_contract::limits(),
                    work))));
                open_index(unnamed.back(), std::nullopt);
            }
        }
        const auto idle = resources.snapshot();
        // One open of its root each, and never more open than the bound:
        // what the shard holds is that many handles, admissions and bytes.
        workload_budget_snapshot each;
        for (std::uint32_t i = 0; i < indexes; ++i) {
            co_await check_every_block(
              *owners[i], context, blocks, work, drive);
            const auto now = resources.snapshot();
            if (i == 0)
                each = {
                  now.tasks - idle.tasks,
                  now.bytes - idle.bytes,
                  now.handles - idle.handles};
            const auto open = residency.resident();
            require(
              owners[i]->loads() == 1 && owners[i]->resident()
                && open == std::min(i + 1, places) && each.handles == 1
                && now.handles == idle.handles + open
                && now.tasks == idle.tasks + open * each.tasks
                && now.bytes == idle.bytes + open * each.bytes,
              "an index was opened more than once, or the shard holds more "
              "for its open roots than its bound allows");
        }
        require(
          !owners[0]->resident() && !owners[1]->resident()
            && owners[2]->resident() && owners[3]->resident()
            && owners[4]->resident(),
          "the roots used longest ago did not make room");
        // The root that is used again is the last to go.
        take(co_await look(2));
        take(co_await look(0));
        require(
          owners[0]->resident() && owners[0]->loads() == 2
            && owners[2]->resident() && owners[2]->loads() == 1
            && !owners[3]->resident() && owners[4]->resident(),
          "a root that was just used made room before an older one");
        {
            // Every place read through: nothing is taken, and the index
            // that needs a place is refused for want of one and not owed.
            auto fourth = take(co_await drive.lifecycle(owners[4]->pin(work)));
            auto second = take(co_await drive.lifecycle(owners[2]->pin(work)));
            std::optional<local_root_pin> zeroth{
              take(co_await drive.lifecycle(owners[0]->pin(work)))};
            const auto full = co_await look(1);
            require(
              !full && full.error().code() == errc::queue_full
                && !co_await drive.lifecycle(residency.evict())
                && owners[1]->source() == sparse_index_source::published
                && residency.resident() == places,
              "a root that is read through was taken, or a full shard was "
              "taken for a missing index");
            take(co_await look(4));
            // One reader lets go, and exactly its root makes room.
            zeroth.reset();
            take(co_await look(1));
            require(
              !owners[0]->resident() && owners[1]->resident()
                && owners[2]->resident() && owners[4]->resident()
                && second.reference() == unnamed[1]
                && fourth.reference() == unnamed[3],
              "a root other than the one let go made room");
        }
        // Every root given up, and the next lookup opens its own again.
        while (co_await drive.lifecycle(residency.evict())) {
        }
        const auto emptied = resources.snapshot();
        require(
          residency.resident() == 0 && emptied.handles == idle.handles
            && emptied.tasks == idle.tasks && emptied.bytes == idle.bytes
            && std::ranges::none_of(
              owners, [](const auto& index) { return index->resident(); }),
          "a root nothing reads through stayed open, or kept its admission");
        take(co_await look(4));
        require(
          owners[4]->resident() && owners[4]->loads() == 2
            && residency.resident() == 1,
          "an index did not open its root again");
        {
            // Three arrive for an index that is not open: one open.
            codec::cooperative_work other{codec::limits::defaults(), abort};
            const auto before = owners[3]->loads();
            auto one = owners[3]->pin(work);
            auto two = owners[3]->find(first, other);
            auto three = owners[3]->pin(work);
            const auto pinned = take(co_await drive.lifecycle(std::move(one)));
            const auto found = take(co_await drive.lifecycle(std::move(two)));
            const auto shared = take(
              co_await drive.lifecycle(std::move(three)));
            require(
              owners[3]->loads() == before + 1 && found.has_value()
                && pinned.reference() == unnamed[2]
                && shared.reference() == unnamed[2],
              "lookups that arrived together opened one index twice");
        }
        // One index is owed: its own root is given up, the others stay as
        // they are, and it answers from memory once it is built again.
        const auto opened = owners[3]->loads();
        owners[3]->owe(detail::path_error(errc::malformed_data));
        require(
          owners[3]->source() == sparse_index_source::owed
            && !owners[3]->resident() && owners[4]->resident(),
          "an owed index kept its root, or cost another index its own");
        take(co_await look(4));
        take(co_await drive.lifecycle(owners[3]->rebuild(asking)));
        co_await check_every_block(*owners[3], context, blocks, work, drive);
        require(
          owners[3]->source() == sparse_index_source::memory
            && owners[3]->loads() == opened && owners[4]->loads() == 2,
          "an index built again opened a root, or another index opened its "
          "own again");
        for (auto& index : owners)
            take(co_await drive.lifecycle(index->close()));
        owners.clear();
        take(co_await drive.lifecycle(writer.close()));
        // What no publication names goes when the segment is next opened;
        // what one names stays.
        const local_object_publication publication{
          segments::descriptor().segment,
          local_object_state::sealed,
          *segment.sealed.boundary,
          {named, *segment.sealed.retry}};
        const auto removed = take(
          co_await drive.lifecycle(remove_unreferenced_objects(
            files, owner, spec, 0, publication, work)));
        require(
          removed == unnamed.size()
            && co_await bundle_exists(files, spec, named, context, drive),
          "an unnamed index bundle was left, or the named one was removed");
        for (const auto& bundle : unnamed)
            require(
              !co_await bundle_exists(files, spec, bundle, context, drive),
              "an index bundle that nothing names was left");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    for (auto& index : owners) {
        try {
            failed.observe(co_await drive.lifecycle(index->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    owners.clear();
    try {
        failed.observe(co_await drive.lifecycle(writer.close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    segment.writer.reset();
    take(failed.outcome());
    require(
      residency.resident() == 0 && !residency.failure().failed(),
      "a closed index left its root open");
}

// Lookups checked against the data itself. The extent is walked from its
// start and every block it holds is kept, in file order; nothing an index
// says is used to decide what a lookup should answer. For each stride the
// anchors are chosen again by the stride's own rule over the walked blocks,
// and every offset of the extent, and those around it, must find the chosen
// block at or before it, at a position where the walk found that block, with
// the block that holds the offset before the scan's end.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> scanned_lookups(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    constexpr std::uint32_t count = 6;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    const auto sequence = [](std::uint64_t value) {
        return local_object_sequence::make(value).value();
    };
    const auto offset = [](std::uint64_t value) {
        return model::range_logical_offset::make(value).value();
    };
    auto segment = co_await seal_segment<Clock>(
      files,
      owner,
      spec,
      resources,
      work,
      drive,
      count,
      sparse_index_stride{byte_count{0}},
      sequence(46));
    auto& writer = *segment.writer;
    sparse_index_residency residency{1};
    sparse_index_rebuild_limit rebuilds;
    std::unique_ptr<sealed_sparse_index<Backend, Owner>> index;
    runtime::first_failure failed;
    try {
        const auto context = segment.context();
        const auto history = segment.history();
        const auto end = context.coverage().bytes().end();
        std::vector<complete_block_descriptor> scanned;
        scanned.reserve(count);
        const auto walked = take(
          co_await drive.lifecycle(verify_local_segment_extent(
            files,
            owner,
            spec,
            0,
            history,
            end,
            resources,
            scan_limits(),
            work,
            [&scanned](const segment_scanned_object& object) {
                if (object.block) scanned.push_back(object.block->descriptor());
                return seastar::make_ready_future<runtime::result<bool>>(true);
            })));
        require(
          scanned.size() == count && walked.digest()
            && *walked.digest() == context.digest(),
          "the walk did not find the sealed extent's blocks");
        const auto base = [](const complete_block_descriptor& block) {
            return block.coverage().logical().begin().value();
        };
        const auto place = [](const complete_block_descriptor& block) {
            return block.coverage().bytes().begin();
        };
        // The stride's rule, over the walked blocks: the first block, then
        // each that begins a stride of bytes, or of records, after the last
        // one chosen.
        const auto choose = [&](sparse_index_stride stride) {
            std::map<std::uint64_t, std::uint64_t> chosen;
            std::uint64_t bytes = 0, records = 0;
            for (const auto& block : scanned) {
                const auto at = place(block).value();
                const auto ordinal
                  = block.coverage().physical().begin().value();
                if (
                  !chosen.empty() && at - bytes < stride.bytes.value()
                  && (stride.records == 0 || ordinal - records < stride.records))
                    continue;
                chosen.emplace(base(block), at);
                bytes = at;
                records = ordinal;
            }
            return chosen;
        };
        std::vector<std::uint64_t> targets;
        targets.push_back(base(scanned.front()) - 1);
        for (auto at = base(scanned.front()); at <= base(scanned.back()) + 2;
             ++at)
            targets.push_back(at);
        targets.push_back(std::numeric_limits<std::uint64_t>::max() - 1);
        const byte_count never{1_GiB};
        const std::array strides{
          sparse_index_stride{byte_count{0}},
          sparse_index_stride{byte_count{32_KiB}},
          sparse_index_stride{never},
          sparse_index_stride{never, 2}};
        for (std::size_t variant = 0; variant < strides.size(); ++variant) {
            const auto stride = strides[variant];
            const auto chosen = choose(stride);
            // The seal's own index for the first; for the others the index
            // its data builds under that stride.
            auto root = *segment.sealed.index;
            if (variant != 0) {
                const auto built = take(
                  co_await drive.lifecycle(rebuild_sparse_index(
                    files,
                    owner,
                    spec,
                    0,
                    history,
                    context,
                    stride,
                    resources,
                    scan_limits(),
                    work)));
                require(
                  built.size() == chosen.size(),
                  "a stride chose other blocks than its rule does");
                root = take(
                  co_await drive.lifecycle(publish_sparse_index(
                    files,
                    owner,
                    spec,
                    0,
                    built,
                    context,
                    sequence(50 + variant),
                    resources,
                    store_contract::limits(),
                    work)));
            }
            require(
              variant == 0   ? chosen.size() == count
              : variant == 2 ? chosen.size() == 1
                             : chosen.size() > 1 && chosen.size() < count,
              "the strides do not tell indexes apart on this extent");
            index = owned_index(
              files,
              owner,
              spec,
              history,
              context,
              stride,
              root,
              std::nullopt,
              residency,
              rebuilds,
              resources,
              work);
            for (const auto target : targets) {
                const auto found = take(
                  co_await drive.lifecycle(index->find(offset(target), work)));
                if (target < base(scanned.front())) {
                    require(
                      !found, "an offset before every block found an anchor");
                    continue;
                }
                const auto after = chosen.upper_bound(target);
                const auto floor = std::prev(after);
                // The block that holds the offset, and the block that lies
                // where the lookup points, both from the walk.
                const auto holds = std::prev(
                  std::ranges::upper_bound(scanned, target, {}, base));
                const auto there = found ? std::ranges::find(
                                             scanned,
                                             found->anchor.block_position(),
                                             place)
                                         : scanned.end();
                require(
                  found
                    && found->anchor.logical_anchor().value() == floor->first
                    && found->anchor.block_position().value() == floor->second
                    && there != scanned.end()
                    && validate_sparse_index_anchor(
                         context, found->anchor, *there)
                         .has_value()
                    && place(*there) <= place(*holds)
                    && place(*holds) < found->end
                    && found->end.value()
                         == (after == chosen.end() ? end.value() : after->second),
                  "a lookup did not answer as the data and the stride do");
            }
            require(
              index->loads() == 1 && residency.resident() == 1,
              "the lookups of one index opened its root more than once");
            take(co_await drive.lifecycle(index->close()));
            index.reset();
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (index) {
        try {
            failed.observe(co_await drive.lifecycle(index->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        index.reset();
    }
    try {
        failed.observe(co_await drive.lifecycle(writer.close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    segment.writer.reset();
    take(failed.outcome());
}

// Readers and a replacement at once. A reader keeps the index it began with
// and reads it to its end while that index is found wrong, built again by a
// rebuild that one of its callers walks away from, and replaced on the
// device; a reader that comes later gets the new one beside it. With both
// places read through a further reader waits for one to let go. The owner
// alone ends a rebuild. The data is never touched.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> replaced_under_readers(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    const sparse_index_stride stride{byte_count{0}};
    const auto sequence = [](std::uint64_t value) {
        return local_object_sequence::make(value).value();
    };
    auto segment = co_await seal_segment<Clock>(
      files, owner, spec, resources, work, drive, 2, stride, sequence(46));
    auto& writer = *segment.writer;
    const auto& blocks = segment.blocks;
    sparse_index_residency residency{2};
    sparse_index_rebuild_limit rebuilds;
    std::unique_ptr<sealed_sparse_index<Backend, Owner>> index;
    runtime::first_failure failed;
    try {
        const auto context = segment.context();
        const auto history = segment.history();
        const auto wrong = detail::path_error(errc::malformed_data);
        const auto second = model::range_logical_offset::make(
                              blocks[1].coverage().logical().begin().value())
                              .value();
        const auto data = co_await store_contract::read_all_bytes(
          files, data_path(spec), drive);
        index = owned_index(
          files,
          owner,
          spec,
          history,
          context,
          stride,
          segment.sealed.index,
          std::nullopt,
          residency,
          rebuilds,
          resources,
          work);
        std::optional<local_root_pin> reader{
          take(co_await drive.lifecycle(index->pin(work)))};
        const auto began = reader->reference();
        const auto read = [&] {
            return drive.lifecycle(
              find_published_anchor(*reader, second, work));
        };
        const auto answer = take(co_await read());
        require(
          answer && began == *segment.sealed.index
            && validate_sparse_index_anchor(context, answer->anchor, blocks[1])
                 .has_value(),
          "a reader did not get the index the seal named");
        index->owe(wrong);
        // Two ask for it; one walks away, and the rebuild goes on.
        seastar::abort_source leaving, staying;
        auto left = index->rebuild(leaving);
        auto stayed = index->rebuild(staying);
        leaving.request_abort();
        const auto gone = co_await drive.lifecycle(std::move(left));
        require(
          !gone && gone.error().code() == errc::aborted
            && rebuilds.running() == 1,
          "a caller that walked away took the rebuild with it");
        // Until it is built, a lookup is told what is owed, and the reader
        // that began earlier reads on.
        const auto meanwhile = co_await drive.lifecycle(
          index->find(second, work));
        require(
          !meanwhile && meanwhile.error() == wrong,
          "an owed index answered a lookup");
        require(
          take(co_await read()) == answer,
          "a reader lost the index it began with");
        take(co_await drive.lifecycle(std::move(stayed)));
        require(
          index->source() == sparse_index_source::memory
            && rebuilds.running() == 0,
          "the rebuild did not finish for the caller that stayed");
        const auto named = take(
          co_await drive.lifecycle(index->publish(writer, sequence(47), work)));
        // The bundle it began with is unlinked now. The reader reads it to
        // its end all the same, and a later reader gets the new one.
        std::optional<local_root_pin> later{
          take(co_await drive.lifecycle(index->pin(work)))};
        require(
          named != began
            && !co_await bundle_exists(files, spec, began, context, drive)
            && take(co_await read()) == answer && reader->reference() == began
            && later->reference() == named && residency.resident() == 2,
          "readers did not each keep the one index they began with");
        // Wrong again, with both places read through: it is built and named
        // a third time, and a further reader waits for a place.
        index->owe(wrong);
        take(co_await drive.lifecycle(index->rebuild(staying)));
        const auto third = take(
          co_await drive.lifecycle(index->publish(writer, sequence(48), work)));
        const auto waiting = co_await drive.lifecycle(index->pin(work));
        require(
          !waiting && waiting.error().code() == errc::queue_full
            && index->source() == sparse_index_source::published
            && later->reference() == named && residency.resident() == 2,
          "a reader was given a place that another still reads through");
        later.reset();
        co_await check_every_block(*index, context, blocks, work, drive);
        {
            const auto latest = take(
              co_await drive.lifecycle(index->pin(work)));
            require(
              latest.reference() == third && residency.resident() == 2,
              "the place that was let go did not go to the newest index");
        }
        // The first reader's root is closed only once it lets go.
        require(
          take(co_await read()) == answer
            && co_await drive.lifecycle(residency.evict())
            && !co_await drive.lifecycle(residency.evict())
            && residency.resident() == 1,
          "a root that is read through was closed");
        reader.reset();
        require(
          co_await drive.lifecycle(residency.evict())
            && residency.resident() == 0,
          "a root nothing reads through stayed open");
        // Two roots given up, and only the newer still read through. The
        // place of the older is free, so the next reader is not kept
        // waiting for the reader of the newer.
        {
            std::optional<local_root_pin> older{
              take(co_await drive.lifecycle(index->pin(work)))};
            index->owe(wrong);
            take(co_await drive.lifecycle(index->rebuild(staying)));
            const auto fourth = take(
              co_await drive.lifecycle(
                index->publish(writer, sequence(49), work)));
            std::optional<local_root_pin> newer{
              take(co_await drive.lifecycle(index->pin(work)))};
            index->owe(wrong);
            take(co_await drive.lifecycle(index->rebuild(staying)));
            const auto fifth = take(
              co_await drive.lifecycle(
                index->publish(writer, sequence(50), work)));
            require(
              older->reference() == third && newer->reference() == fourth
                && residency.resident() == 2,
              "two readers did not each keep a root that was given up");
            older.reset();
            {
                const auto next = take(
                  co_await drive.lifecycle(index->pin(work)));
                require(
                  next.reference() == fifth && newer->reference() == fourth
                    && residency.resident() == 2,
                  "a reader was kept waiting for a place nothing reads "
                  "through");
            }
            newer.reset();
            while (co_await drive.lifecycle(residency.evict())) {
            }
            require(
              residency.resident() == 0,
              "a root nothing reads through stayed open");
        }
        // Reported wrong while it is being named. It stops answering at
        // once and is not built again under the publication that is
        // encoding it; it is named all the same, and is still owed.
        {
            index->owe(wrong);
            take(co_await drive.lifecycle(index->rebuild(staying)));
            seastar::abort_source apart;
            codec::cooperative_work looking{codec::limits::defaults(), apart};
            auto naming = index->publish(writer, sequence(51), work);
            index->owe(wrong);
            const auto source = index->source();
            // Both are decided where they are asked, before the publication
            // goes on.
            auto asked = index->rebuild(staying);
            auto looked = index->find(second, looking);
            const auto early = co_await drive.lifecycle(std::move(asked));
            const auto distrusted = co_await drive.lifecycle(std::move(looked));
            require(
              source == sparse_index_source::owed && !distrusted
                && distrusted.error() == wrong && !early
                && early.error().code() == errc::queue_full,
              "an index reported wrong went on answering, or was built "
              "again under its publication");
            const auto sixth = take(
              co_await drive.lifecycle(std::move(naming)));
            require(
              index->named() == std::optional{sixth}
                && index->source() == sparse_index_source::owed,
              "an index reported wrong while it was named is not owed");
            take(co_await drive.lifecycle(index->rebuild(staying)));
            take(
              co_await drive.lifecycle(
                index->publish(writer, sequence(52), work)));
            require(
              index->source() == sparse_index_source::published,
              "the index built again was not named");
            co_await check_every_block(*index, context, blocks, work, drive);
        }
        // Closed while a rebuild is in flight: the rebuild ends with it.
        index->owe(wrong);
        auto unfinished = index->rebuild(staying);
        take(co_await drive.lifecycle(index->close()));
        const auto ended = co_await drive.lifecycle(std::move(unfinished));
        require(
          !ended && rebuilds.running() == 0,
          "a rebuild outlived the close of its index");
        index.reset();
        require(
          co_await store_contract::read_all_bytes(files, data_path(spec), drive)
              == data
            && !writer.failure().failed(),
          "an index error changed the data or failed its segment");
        take(
          co_await drive.lifecycle(writer.read_immutable(
            context.coverage().bytes().begin(),
            segments::descriptor().alignment.bytes(),
            work)));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (index) {
        try {
            failed.observe(co_await drive.lifecycle(index->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        index.reset();
    }
    try {
        failed.observe(co_await drive.lifecycle(writer.close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    segment.writer.reset();
    take(failed.outcome());
    require(
      residency.resident() == 0 && !residency.failure().failed(),
      "a closed index left its root open");
}

// A seeded stream of choices, so one seed replays one index exactly.
class anchor_choices final {
public:
    explicit anchor_choices(std::uint64_t seed) noexcept
      : state_(seed) {}
    // Uniform in [0, bound).
    std::uint64_t uniform(std::uint64_t bound) noexcept {
        state_ += 0x9e3779b97f4a7c15ULL;
        auto z = state_;
        z = (z ^ (z >> 30U)) * 0xbf58476d1ce4e5b9ULL;
        z = (z ^ (z >> 27U)) * 0x94d049bb133111ebULL;
        return (z ^ (z >> 31U)) % bound;
    }

private:
    std::uint64_t state_;
};

// An index of several pages, published and read back. Its anchors are chosen
// at random, a few offsets and a few blocks apart, with bases on both sides
// of 2^32. No data stands behind them: what is checked is the index's own
// bytes and its routing. Every page read through the root holds exactly the
// anchors that were frozen, and any offset finds what a sorted map of those
// anchors finds, the largest at or below it, from the one page the root
// routes to.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> paged_lookups(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    constexpr std::uint32_t anchors = 2 * sparse_index_page_entries + 500;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    const sparse_index_stride stride{byte_count{0}};
    const auto sequence = [](std::uint64_t value) {
        return local_object_sequence::make(value).value();
    };
    const auto offset = [](std::uint64_t value) {
        return model::range_logical_offset::make(value).value();
    };
    // The segment only gives the bundle a place to be published in.
    auto segment = co_await seal_segment<Clock>(
      files, owner, spec, resources, work, drive, 2, stride, sequence(46));
    auto& writer = *segment.writer;
    sparse_index_residency residency{1};
    sparse_index_rebuild_limit rebuilds;
    std::unique_ptr<sealed_sparse_index<Backend, Owner>> index;
    runtime::first_failure failed;
    try {
        const auto described = segments::descriptor();
        const auto unit = described.alignment.bytes().value();
        anchor_choices choices{7};
        std::map<std::uint64_t, std::uint64_t> placed;
        // Every base is at least one past the last, so they end above 2^32.
        std::uint64_t base = (std::uint64_t{1} << 32) - anchors / 2;
        std::uint64_t position = unit;
        while (placed.size() < anchors) {
            placed.emplace(base, position);
            base += choices.uniform(15) + 1;
            position += unit * (choices.uniform(4) + 1);
        }
        const auto first = placed.begin()->first;
        const runtime::file_position end{position};
        const auto spans = [](
                             std::uint64_t from,
                             std::uint64_t until,
                             std::uint64_t ordinal,
                             std::uint64_t records,
                             std::uint64_t begins,
                             std::uint64_t ends) {
            return coverage{
              model::range_logical_span::from_count(
                model::range_logical_end{from},
                model::range_logical_count{until - from})
                .value(),
              model::segment_relative_span::from_count(
                model::segment_relative_end{ordinal},
                model::segment_record_count{records})
                .value(),
              model::file_byte_span::from_size(
                runtime::file_position{begins}, byte_count{ends - begins})
                .value()};
        };
        const auto context = take(
          sparse_index_context::make(
            described.segment,
            spans(first, base, 0, anchors, unit, position),
            codec::extent_digest{codec::content_digest{}},
            described.alignment));
        auto frozen = take(
          active_sparse_index::make(stride, anchors, resources));
        std::uint64_t ordinal = 0;
        for (auto entry = placed.begin(); entry != placed.end(); ++entry) {
            const auto next = std::next(entry);
            frozen.written(spans(
              entry->first,
              next == placed.end() ? base : next->first,
              ordinal++,
              1,
              entry->second,
              next == placed.end() ? position : next->second));
        }
        frozen.durable(end);
        frozen.freeze();
        require(
          frozen.size() == anchors && frozen.pages() == 3
            && first < (std::uint64_t{1} << 32)
            && base > (std::uint64_t{1} << 32),
          "the index to publish is not the one this case is about");
        const auto root = take(
          co_await drive.lifecycle(publish_sparse_index(
            files,
            owner,
            spec,
            0,
            frozen,
            context,
            sequence(60),
            resources,
            store_contract::limits(),
            work)));
        index = owned_index(
          files,
          owner,
          spec,
          segment.history(),
          context,
          stride,
          root,
          std::nullopt,
          residency,
          rebuilds,
          resources,
          work);
        {
            auto pin = take(co_await drive.lifecycle(index->pin(work)));
            const auto& meta = std::get<sparse_index_root>(pin.metadata());
            require(
              meta.entry_count() == anchors
                && meta.pages().size() == frozen.pages()
                && meta.context() == context,
              "the root read back is not the root that was published");
            for (std::uint32_t i = 0; i < frozen.pages(); ++i) {
                const auto page = take(
                  co_await drive.lifecycle(
                    pin.read_index(page_ordinal::make(i).value(), work)));
                const auto kept = frozen.page(i);
                require(
                  meta.first_anchors()[i] == kept.front().logical_anchor()
                    && std::ranges::equal(page.value.entries(), kept),
                  "a page read back is not the page that was frozen");
            }
        }
        // Around every page's edge, around both ends, and anywhere else.
        std::vector<std::uint64_t> targets{
          first - 1, first, base - 1, base, base + 20};
        for (const std::uint32_t edge :
             {sparse_index_page_entries, 2 * sparse_index_page_entries}) {
            const auto begins = frozen[edge].logical_anchor().value();
            const auto before = frozen[edge - 1].logical_anchor().value();
            targets.insert(
              targets.end(),
              {before - 1, before, before + 1, begins - 1, begins, begins + 1});
        }
        for (std::uint32_t i = 0; i < 256; ++i)
            targets.push_back(first + choices.uniform(base - first));
        for (const auto target : targets) {
            const auto found = take(
              co_await drive.lifecycle(index->find(offset(target), work)));
            if (target < first) {
                require(!found, "an offset before every anchor found one");
                continue;
            }
            const auto after = placed.upper_bound(target);
            const auto floor = std::prev(after);
            // The scan ends at the next anchor where the page holds it, and
            // at the end of the extent for the last anchor of a page.
            const auto rank = static_cast<std::uint64_t>(
              std::distance(placed.begin(), after));
            const auto scan = after != placed.end()
                                  && rank % sparse_index_page_entries != 0
                                ? after->second
                                : end.value();
            require(
              found && found->anchor.logical_anchor().value() == floor->first
                && found->anchor.block_position().value() == floor->second
                && found->end.value() == scan,
              "a lookup in a paged index did not answer as a sorted map does");
        }
        require(
          index->loads() == 1 && residency.resident() == 1
            && !writer.failure().failed(),
          "the lookups of one index opened its root more than once");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (index) {
        try {
            failed.observe(co_await drive.lifecycle(index->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        index.reset();
    }
    try {
        failed.observe(co_await drive.lifecycle(writer.close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    segment.writer.reset();
    take(failed.outcome());
}
} // namespace kwaque::storage::testing::sparse_index_contract
