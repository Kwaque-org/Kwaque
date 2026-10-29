#pragma once

#include "src/base/units.h"
#include "src/codec/crc32c.h"
#include "src/storage/segment_writer.h"
#include "src/storage/tests/local_installation_contract.h"
#include "src/storage/tests/retry_test_support.h"

#include <algorithm>
#include <array>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace kwaque::storage::testing::segment_writer_contract {
using store_contract::require;
using store_contract::take;
namespace installation = installation_contract;

// The profile these contracts were written for: a segment's file grows
// through ordinary writes. The defaults are run by cases of their own
// (with_defaults()).
inline segment_writer_config configuration() {
    segment_writer_config config{
      local_object_sequence::make(45).value(), store_contract::limits()};
    config.preallocation_bytes = {};
    config.preallocation_extension_bytes = {};
    return config;
}
inline segment_writer_config read_configuration() {
    segment_writer_config config{std::nullopt, store_contract::limits()};
    config.preallocation_bytes = {};
    config.preallocation_extension_bytes = {};
    return config;
}
// The same writer at the production defaults: a zero-written window and
// its extensions, synchronized writes inside them.
inline segment_writer_config with_defaults() {
    return {local_object_sequence::make(45).value(), store_contract::limits()};
}
inline local_segment_descriptor descriptor() {
    auto value = installation::descriptor();
    value.alignment = alignment(8192);
    return value;
}
// Where descriptor()'s segment starts in its range, as its owner records it.
inline model::range_logical_end logical_origin() {
    return model::range_logical_end{100};
}
// The broker time every child here is admitted at. One value for all keeps
// a segment's append times from decreasing.
inline constexpr runtime::wall_time child_append_time{1};
// What a footer or sealed root records of blocks all appended at
// child_append_time, the latest of whose record times is given; nothing when
// no block was written.
inline boundary_times
appended_times(std::optional<runtime::wall_time> max_record) {
    if (!max_record) return {};
    return {child_append_time, child_append_time, *max_record};
}
// What a test has written to one segment so far: the running CRC of every
// byte and the latest record time of every block, which each footer covers.
struct written_prefix final {
    codec::crc32c checksum;
    std::optional<runtime::wall_time> max_record;
    void add(const encoded_assigned_batch& batch) {
        const auto latest = batch.info().max_record_time;
        max_record = max_record ? std::max(*max_record, latest) : latest;
    }
};
inline seastar::future<encoded_assigned_batch>
child(codec::cooperative_work& work, std::size_t header = 32) {
    auto raw = co_await installation::buffer_async(
      assigned_wire(false, header));
    auto validated = co_await validate_encoded_assigned_batch(
      std::move(raw), batch_expected(), budget(), work);
    require(validated.has_value(), "independent child fixture rejected");
    co_return std::move(*validated);
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> creation(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    static_assert(!std::is_aggregate_v<segment_captured_boundary>);
    static_assert(!std::is_copy_constructible_v<segment_group_layout>);
    static_assert(
      !std::
        is_constructible_v<segment_durable_receipt, segment_captured_boundary>);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    const auto description = descriptor();
    {
        auto wide = description;
        wide.alignment = alignment(65536);
        auto narrow = configuration();
        narrow.metadata.operation_bytes = byte_count{64_KiB};
        auto unsupported = writer_type::make_new(
          files, owner, spec, 0, wide, logical_origin(), resources, narrow);
        const bool rejected = !unsupported
                              && unsupported.error().code()
                                   == errc::resource_exhausted;
        if (unsupported)
            take(co_await drive.lifecycle((*unsupported)->close()));
        require(
          rejected,
          "activation admitted an empty seal that cannot fit its held "
          "workspace");
    }
    const local_generation_expectation expected{
      local_publication_generation::make(1).value(),
      {description.segment, local_object_state::active, {}, {}},
      description,
      logical_origin(),
      {}};
    auto missing = co_await drive.lifecycle(
      writer_type::open_existing(
        files,
        owner,
        spec,
        0,
        expected,
        resources,
        read_configuration(),
        work));
    require(
      !missing && missing.error().code() == errc::not_found,
      "existing open bootstrapped a missing segment");
    {
        auto partial = description;
        partial.segment = segment_context::make(
                            description.segment.cluster(),
                            description.segment.topic(),
                            description.segment.range(),
                            description.segment.segment(),
                            model::segment_generation::make(2).value())
                            .value();
        const auto header
          = segment_header::make(partial.segment, partial.alignment).value();
        auto installed = co_await drive.lifecycle(
          publish_local_segment_descriptor(
            files,
            owner,
            spec,
            0,
            partial,
            header,
            resources,
            store_contract::limits(),
            work));
        take(installed.failure.outcome());
        const local_generation_expectation incomplete{
          expected.generation,
          {partial.segment, local_object_state::active, {}, {}},
          partial,
          expected.logical_origin,
          {}};
        auto reopened = co_await drive.lifecycle(
          writer_type::open_existing(
            files,
            owner,
            spec,
            0,
            incomplete,
            resources,
            read_configuration(),
            work));
        require(
          !reopened && reopened.error().code() == errc::not_found,
          "descriptor-only state was treated as a new empty generation");
    }
    auto writer = take(
      writer_type::make_new(
        files,
        owner,
        spec,
        0,
        description,
        logical_origin(),
        resources,
        configuration()));
    runtime::first_failure failed;
    std::optional<segment_captured_boundary> cut;
    try {
        require(!writer->capture(), "creating owner minted an active cut");
        take(co_await drive.lifecycle(writer->create_new(work)));
        cut.emplace(take(writer->capture()));
        require(
          writer->append_state() == model::append_state::active
            && writer->handle_state() == segment_handle_state::open
            && cut->end().logical.value() == 100
            && cut->end().physical.value() == 0
            && cut->end().bytes.value() == 8192 && !cut->footer()
            && cut->covered().logical().begin().value() == 100
            && cut->covered().physical().begin().value() == 0
            && cut->covered().bytes().begin().value() == 8192
            && writer->publication_generation() == expected.generation,
          "new segment conflated record, byte or publication coordinates");
        auto repeated = co_await drive.lifecycle(writer->create_new(work));
        require(!repeated, "same owner repeated creation");
        auto collision = take(
          writer_type::make_new(
            files,
            owner,
            spec,
            0,
            description,
            logical_origin(),
            resources,
            configuration()));
        runtime::first_failure collision_failure;
        try {
            auto result = co_await drive.lifecycle(collision->create_new(work));
            require(
              !result && result.error().code() == errc::already_exists,
              "new creation reused an existing generation");
        } catch (...) {
            collision_failure.observe(std::current_exception());
        }
        try {
            static_cast<void>(co_await drive.lifecycle(collision->close()));
        } catch (...) {
            collision_failure.observe(std::current_exception());
        }
        collision.reset();
        take(collision_failure.outcome());
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
    auto opened = take(
      co_await drive.lifecycle(
        writer_type::open_existing(
          files,
          owner,
          spec,
          0,
          expected,
          resources,
          read_configuration(),
          work)));
    try {
        take(co_await drive.lifecycle(opened->evict_read_handle()));
        take(co_await drive.lifecycle(opened->reopen_read_handle(work)));
        const std::array input{co_await child(work)};
        auto admission = take(opened->prepare_group(input, work));
        require(
          opened->recovered() && !opened->progress()
            && !opened->validate_capture(*cut)
            && admission.decision == segment_capacity_decision::roll_required
            && !admission.prepared,
          "recovered metadata granted new append or durability authority");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        failed.observe(co_await drive.lifecycle(opened->close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    opened.reset();
    take(failed.outcome());
    cut.reset();
    {
        auto extended = description;
        extended.segment = segment_context::make(
                             description.segment.cluster(),
                             description.segment.topic(),
                             description.segment.range(),
                             description.segment.segment(),
                             model::segment_generation::make(3).value())
                             .value();
        extended.alignment = alignment(512);
        const auto header
          = segment_header::make(extended.segment, extended.alignment).value();
        auto installed = co_await drive.lifecycle(
          publish_local_segment_descriptor(
            files,
            owner,
            spec,
            0,
            extended,
            header,
            resources,
            store_contract::limits(),
            work));
        take(installed.failure.outcome());
        const auto selected = take(local_paths::make(spec.root));
        const local_segment_name identity{
          extended.segment.segment(), extended.segment.generation()};
        co_await store_contract::write_bytes(
          files,
          take(selected.segment_file(0, identity, local_segment_file::data)),
          header_wire(header, 4096),
          drive);
        auto publication = local_fixture::read("publication_empty");
        // Independent fixture: change only SC generation, not publication
        // sequence.
        put(publication, 32 + 72 + 64, 3, 8);
        repair(publication);
        co_await store_contract::write_bytes(
          files,
          take(
            selected.segment_file(0, identity, local_segment_file::published)),
          publication,
          drive);
        const local_generation_expectation expectation{
          expected.generation,
          {extended.segment, local_object_state::active, {}, {}},
          extended,
          expected.logical_origin,
          {}};
        auto view = take(
          co_await drive.lifecycle(
            writer_type::open_existing(
              files,
              owner,
              spec,
              0,
              expectation,
              resources,
              read_configuration(),
              work)));
        try {
            require(
              view->header_end() && view->header_end()->value() == 4608
                && view->publication_generation()->value() == 1
                && !view->capture(),
              "existing open guessed the header boundary or acquired append "
              "authority");
        } catch (...) {
            failed.observe(std::current_exception());
        }
        try {
            failed.observe(co_await drive.lifecycle(view->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        view.reset();
        take(failed.outcome());
    }
    const auto paths = take(local_paths::make(spec.root));
    const auto data = take(paths.segment_file(
      0,
      {description.segment.segment(), description.segment.generation()},
      local_segment_file::data));
    auto bytes = co_await store_contract::read_bytes(files, data, drive);
    bytes.at(40) ^= 1;
    co_await store_contract::write_bytes(files, data, bytes, drive);
    auto damaged = co_await drive.lifecycle(
      writer_type::open_existing(
        files,
        owner,
        spec,
        0,
        expected,
        resources,
        read_configuration(),
        work));
    require(
      !damaged && damaged.error().code() == errc::corrupt_data,
      "damaged existing header became an empty segment");
    require(
      (co_await store_contract::read_bytes(files, data, drive)) == bytes,
      "rejected existing open rewrote damaged data");
    auto future = header_wire(
      segment_header::make(description.segment, description.alignment).value());
    put(future, 32 + 80, 2, 2);
    repair(future);
    co_await store_contract::write_bytes(files, data, future, drive);
    auto unsupported = co_await drive.lifecycle(
      writer_type::open_existing(
        files,
        owner,
        spec,
        0,
        expected,
        resources,
        read_configuration(),
        work));
    require(
      !unsupported && unsupported.error().code() == errc::unsupported_format,
      "unknown stored profile became an empty segment");
}

// What a created segment costs its budget in handle credits, against the
// budget's own limit. With exactly segment_writer_handles() and
// segment_creation_handles free a creation succeeds and afterwards holds the
// first: its seal's publications are a seal slot's, not its own. With only
// what the open segment holds free it is refused for handle pressure: each
// publication that makes its files needs credits beyond them.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> creation_handles(
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
    const auto config = configuration();
    const auto each = segment_writer_handles(config);
    const auto limit = std::uint64_t{resources.limits().handles};
    const auto idle = resources.snapshot().handles;
    require(
      limit > idle + each + segment_creation_handles,
      "the handle budget is too small to measure a creation");
    auto filler = take(resources.try_reserve(byte_count{1}));
    take(filler.try_acquire_handles(
      static_cast<std::uint32_t>(
        limit - idle - each - segment_creation_handles)));
    const auto first = descriptor();
    auto second = first;
    second.segment = segment_context::make(
                       first.segment.cluster(),
                       first.segment.topic(),
                       first.segment.range(),
                       first.segment.segment(),
                       model::segment_generation::make(2).value())
                       .value();
    auto created = take(
      writer_type::make_new(
        files, owner, spec, 0, first, logical_origin(), resources, config));
    std::unique_ptr<writer_type> starved;
    std::optional<workload_reservation> one;
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(created->create_new(work)));
        require(
          resources.snapshot().handles == limit - segment_creation_handles,
          "an open segment does not hold the handle credits it is counted "
          "for");
        // Left free: what an open segment holds, and nothing for the
        // publications that make its files.
        const auto spare = limit - resources.snapshot().handles;
        require(
          spare >= each, "the handle budget cannot hold a second segment");
        if (spare > each) {
            one.emplace(take(resources.try_reserve(byte_count{1})));
            take(one->try_acquire_handles(
              static_cast<std::uint32_t>(spare - each)));
        }
        starved = take(
          writer_type::make_new(
            files,
            owner,
            spec,
            0,
            second,
            logical_origin(),
            resources,
            config));
        const auto refused = co_await drive.lifecycle(
          starved->create_new(work));
        require(
          !refused && refused.error().code() == errc::queue_full,
          "a creation with no credit for its publications was not refused "
          "for it");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (starved) {
        try {
            static_cast<void>(co_await drive.lifecycle(starved->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        starved.reset();
    }
    try {
        failed.observe(co_await drive.lifecycle(created->close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    created.reset();
    take(failed.outcome());
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> admission(
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
    auto writer = take(
      writer_type::make_new(
        files,
        owner,
        spec,
        0,
        descriptor(),
        logical_origin(),
        resources,
        configuration()));
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        const auto before = take(writer->capture());
        const std::array input{co_await child(work)};
        auto backing = take(resources.try_reserve_buffer(input[0].bytes()));
        const auto initial = resources.snapshot();
        {
            // A 399-byte block and the footer after it pack into one unit
            // from the data start at 8192; the root takes the next.
            auto prepared = take(writer->prepare_group(input, work));
            require(
              prepared.decision == segment_capacity_decision::fits
                && prepared.prepared
                && prepared.prepared->plan().end->bytes.value() == 16384
                && prepared.prepared->plan().disk.data.value() == 24576
                && prepared.prepared->plan().end->retry_entries == 1,
              "group omitted its footer, root or complete-request obligation");
            require(
              take(writer->capture()) == before,
              "rejectable preparation froze coordinates");
            // The rest of the budget is held elsewhere: the next group meets
            // pressure that passes, not a group too large to fit.
            std::vector<workload_reservation> elsewhere;
            for (auto bytes = resources.limits().bytes.value(); bytes >= 4_KiB;
                 bytes /= 2)
                for (auto held = resources.try_reserve(byte_count{bytes}); held;
                     held = resources.try_reserve(byte_count{bytes}))
                    elsewhere.push_back(std::move(*held));
            auto pressure = writer->prepare_group(input, work);
            require(
              !pressure && pressure.error().code() == errc::queue_full,
              "temporary workload pressure was mistaken for a roll");
        }
        require(
          resources.snapshot().bytes == initial.bytes
            && resources.snapshot().tasks == initial.tasks,
          "abandoned preparation leaked or consumed coordinates");
        seastar::abort_source cancelled;
        cancelled.request_abort();
        codec::cooperative_work stopped{codec::limits::defaults(), cancelled};
        auto rejected = writer->prepare_group(input, stopped);
        require(
          !rejected && rejected.error().code() == errc::aborted
            && take(writer->capture()) == before,
          "cancelled preparation changed the segment");
        resources.close_admission();
        require(
          !writer->prepare_group(input, work), "closed budget admitted work");
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

template<typename Backend, typename Owner, typename Driver>
seastar::future<> reserved_publication(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    {
        auto installed = co_await drive.lifecycle(
          publish_local_segment_descriptor(
            files,
            owner,
            spec,
            0,
            installation::descriptor(),
            installation::segment_head(),
            resources,
            store_contract::limits(),
            work));
        take(installed.failure.outcome());
    }
    // The A image packs both blocks and the footer into 4096..8192; the
    // sealed root begins there.
    const auto ref = local_root_reference::make(
                       local_root_kind::sealed_retry,
                       local_object_sequence::make(45).value(),
                       runtime::file_position{8192},
                       byte_count{4_KiB},
                       page_count::make(1).value(),
                       installation::literal_digest(
                         "ce3d48e2534348af6d0252a246162909"))
                       .value();
    const footer_expectation context{
      {installation::segment(),
       alignment(4096),
       runtime::file_position{4096},
       model::range_logical_end{100},
       model::segment_relative_end{}},
      runtime::file_position{8192}};
    const auto object = take(local_bundle_path(spec, 0, ref, context));
    const auto paths = take(local_paths::make(spec.root));
    const local_segment_name identity{
      installation::segment().segment(), installation::segment().generation()};
    const auto data_path = take(
      paths.segment_file(0, identity, local_segment_file::data));
    co_await store_contract::write_bytes(
      files,
      data_path,
      local_fixture::read("data_header_a") + local_fixture::read("block_a1")
        + local_fixture::read("block_a2") + local_fixture::read("footer_a"),
      drive);
    const auto pointer_path = take(
      paths.segment_file(0, identity, local_segment_file::published));
    co_await store_contract::write_bytes(
      files, pointer_path, local_fixture::read("publication_empty"), drive);
    const auto split = object.value().rfind('/');
    local_file_publisher<Backend> publisher{
      files,
      resources,
      {spec.shard_owner(0).value(),
       spec.root,
       runtime::file_path::make(object.value().substr(0, split)).value(),
       runtime::file_name::make(object.value().substr(split + 1)).value(),
       runtime::file_rename_policy::no_replace,
       {}}};
    runtime::first_failure failed;
    std::optional<local_file_publisher<Backend>> pointer;
    std::optional<runtime::file> data;
    std::optional<completion_resources> completion;
    // One more than the cursors the native backend lets list at once.
    std::array<std::optional<runtime::directory_page>, 65> cursor_pressure;
    try {
        take(
          co_await drive.lifecycle(
            publisher.prepare(byte_count{256_KiB}, work)));
        const auto slash = pointer_path.value().rfind('/');
        pointer.emplace(
          files,
          resources,
          local_publication_target{
            spec.shard_owner(0).value(),
            spec.root,
            runtime::file_path::make(pointer_path.value().substr(0, slash))
              .value(),
            runtime::file_name::make(pointer_path.value().substr(slash + 1))
              .value(),
            runtime::file_rename_policy::replace,
            local_publication_generation::make(1).value()});
        take(
          co_await drive.lifecycle(
            pointer->prepare(byte_count{256_KiB}, work)));
        data.emplace(take(
          co_await drive.lifecycle(files.open(
            data_path, {.access = runtime::file_access::read_write}))));
        completion.emplace(take(completion_resources::make(resources, *data)));
        auto grant = take(resources.try_reserve(byte_count{1_MiB}));
        auto insufficient = take(resources.try_reserve(byte_count{1}));
        auto seal_grant = take(resources.try_reserve(byte_count{1_MiB}));
        auto root = co_await installation::buffer_async(
          local_fixture::read("sealed_a"));
        auto sealed = co_await installation::buffer_async(
          local_fixture::read("sealed_a"));
        // Retained pages keep the cursor admission slot after their native
        // handles close. Saturate directory admission without consuming the
        // temporary-file handles needed by publication. A backend refuses the
        // cursor itself or, when it bounds only those that list, the
        // cursor's first page.
        bool saturated = false;
        for (auto& retained : cursor_pressure) {
            auto opened = co_await drive.lifecycle(
              files.open_directory(spec.root));
            if (!opened) {
                require(
                  opened.error().code() == errc::queue_full,
                  "unexpected cursor pressure failure");
                saturated = true;
                break;
            }
            runtime::first_failure page_failure;
            try {
                auto page = co_await drive.lifecycle(opened->next(
                  {.maximum_entries = item_count{1},
                   .maximum_name_bytes = byte_count{255}}));
                if (page)
                    retained.emplace(std::move(*page));
                else if (page.error().code() == errc::queue_full)
                    saturated = true;
                else
                    page_failure.observe(page);
            } catch (...) {
                page_failure.observe(std::current_exception());
            }
            try {
                page_failure.observe(co_await drive.lifecycle(opened->close()));
            } catch (...) {
                page_failure.observe(std::current_exception());
            }
            take(page_failure.outcome());
            if (saturated) break;
        }
        require(saturated, "directory cursor admission was not saturated");
        resources.close_admission();
        const auto before = resources.snapshot();
        auto denied = co_await local_bundle::make(
          ref,
          context,
          co_await installation::buffer_async(local_fixture::read("sealed_a")),
          resources,
          store_contract::limits(),
          work,
          std::move(insufficient));
        require(
          !denied && denied.error().code() == errc::resource_exhausted,
          "undersized prepared grant bypassed admission");
        auto shared = grant.share();
        auto aliased = co_await local_bundle::make(
          ref,
          context,
          co_await installation::buffer_async(local_fixture::read("sealed_a")),
          resources,
          store_contract::limits(),
          work,
          std::move(shared));
        require(
          !aliased && aliased.error().code() == errc::wrong_context,
          "shared allowance funded another independent bundle");
        auto bundle = take(
          co_await local_bundle::make(
            ref,
            context,
            std::move(root),
            resources,
            store_contract::limits(),
            work,
            std::move(grant)));
        const std::array<std::string_view, 1> names{"sealed_retry_page"};
        auto published = co_await drive.lifecycle(publish_local_bundle(
          files,
          owner,
          spec,
          0,
          std::move(bundle),
          installation::fixture_pages{names},
          installation::ready_dependencies{},
          resources,
          work,
          &publisher));
        take(published.publication.failure.outcome());
        auto written = take(
          co_await drive.lifecycle(
            data->write(runtime::file_position{8192}, std::move(sealed))));
        require(written == byte_count{4_KiB}, "sealed root was short-written");
        take(co_await drive.lifecycle(data->flush(completion->metadata())));
        const auto generation = local_publication_generation::make(2).value();
        const auto header = local_metadata_header::make(
                              local_metadata_kind::object_publication,
                              spec.shard_owner(0).value(),
                              generation)
                              .value();
        const local_metadata_payload payload{local_object_publication{
          installation::segment(),
          local_object_state::sealed,
          local_footer_reference::make(
            ref.position(), ref.bytes(), 7, ref.digest())
            .value(),
          {ref}}};
        auto encoded = co_await encode_local_metadata(
          {header, spec.identity.metadata_alignment, alignment(4096)},
          payload,
          work,
          byte_count{256_KiB},
          store_contract::limits().charge);
        require(encoded.has_value(), "prepared publication encoding failed");
        auto installed = co_await drive.lifecycle(pointer->publish(
          {spec.shard_owner(0).value(),
           generation,
           local_publication_generation::make(1).value()},
          std::move(encoded->bytes),
          work));
        take(installed.failure.outcome());
        auto next_header = local_metadata_header::make(
                             local_metadata_kind::object_publication,
                             spec.shard_owner(0).value(),
                             local_publication_generation::make(3).value())
                             .value();
        auto next = co_await encode_local_metadata(
          {next_header, spec.identity.metadata_alignment, alignment(4096)},
          payload,
          work,
          byte_count{256_KiB},
          store_contract::limits().charge);
        require(next.has_value(), "second publication fixture encoding failed");
        auto blocked = co_await drive.lifecycle(pointer->publish(
          {spec.shard_owner(0).value(), next_header.generation(), generation},
          std::move(next->bytes),
          work));
        require(
          blocked.admission_rejected && blocked.failure.error()
            && blocked.failure.error()->code() == errc::queue_full
            && blocked.stage == local_publication_stage::none,
          "retained outcome did not keep its prepared allowance occupied");
        require(
          published.reference == ref
            && installed.disposition == local_publication_disposition::durable
            && resources.snapshot().accepted == before.accepted
            && resources.snapshot().rejected == before.rejected,
          "prepared bundle publication reacquired ordinary admission");
        require(
          (co_await store_contract::read_bytes(files, object, drive))
            == local_fixture::read("sealed_retry_page"),
          "prepared stream changed independently supplied bytes");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        failed.observe(co_await drive.lifecycle(publisher.close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (pointer) {
        try {
            failed.observe(co_await drive.lifecycle(pointer->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    if (completion) completion->release_metadata();
    if (data) {
        try {
            failed.observe(co_await drive.lifecycle(data->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    take(failed.outcome());
}

inline seastar::future<encoded_assigned_batch>
execution_child(std::uint64_t logical, codec::cooperative_work& work) {
    // Reuse the independent fixture's full-BID/fingerprint construction.
    auto raw = co_await installation::buffer_async(
      data_child(logical, false, true));
    co_return (co_await validate_encoded_assigned_batch(
                 std::move(raw), batch_expected(), budget(), work))
      .value();
}
// The request at `logical` with a record value of `value` bytes: a block of
// 401 + value bytes for a value of 64 to 8,191 bytes.
inline seastar::future<encoded_assigned_batch> valued_execution_child(
  std::uint64_t logical, std::size_t value, codec::cooperative_work& work) {
    auto raw = co_await installation::buffer_async(
      valued_child(logical, value));
    co_return (co_await validate_encoded_assigned_batch(
                 std::move(raw), batch_expected(), budget(), work))
      .value();
}

// What a writer reported to its block observer. Kept alive past the
// writer's close, and sized so that recording never allocates.
struct block_trace final {
    block_trace() {
        written.reserve(8);
        durable.reserve(8);
    }
    [[nodiscard]] segment_block_observer observer() {
        return {
          [this](std::span<const segment_block_layout> blocks) noexcept {
              for (const auto& block : blocks)
                  written.push_back(block.records);
          },
          [this](runtime::file_position end) noexcept {
              durable.push_back(end);
          }};
    }
    std::vector<storage::coverage> written;
    std::vector<runtime::file_position> durable;
};

template<typename Writer>
seastar::future<segment_frozen_group> freeze_child(
  Writer& writer,
  encoded_assigned_batch batch,
  workload_budget& resources,
  codec::cooperative_work& work) {
    auto prepared = take(writer.prepare_group(std::span{&batch, 1}, work));
    require(prepared.prepared.has_value(), "test group could not be prepared");
    auto held = take(resources.try_reserve_buffer(batch.bytes()));
    std::vector<admitted_wal_batch> children;
    children.push_back(take(
      admitted_wal_batch::make(
        std::move(batch), std::move(held), child_append_time, charge)));
    co_return take(
      co_await writer.freeze_group(
        std::move(*prepared.prepared), std::move(children), work));
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> execution(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive,
  bool synchronize = false) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    auto config = configuration();
    config.maximum_groups = 2;
    config.admission.working_bytes = byte_count{1_MiB};
    block_trace trace;
    auto writer = take(
      writer_type::make_new(
        files,
        owner,
        spec,
        0,
        descriptor(),
        logical_origin(),
        resources,
        config));
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        const auto empty = writer->progress()->durable;
        take(writer->observe_blocks(trace.observer()));
        const auto twice = writer->observe_blocks(trace.observer());
        require(
          !twice && twice.error().code() == errc::wrong_context,
          "a second block observer replaced the first");
        written_prefix records;
        auto first_child = co_await child(work, 4096);
        records.add(first_child);
        const auto first_times = appended_times(records.max_record);
        auto first = co_await freeze_child(
          *writer, std::move(first_child), resources, work);
        const auto first_cut = first.layout().boundary();
        auto second_child = co_await execution_child(101, work);
        records.add(second_child);
        const auto second_times = appended_times(records.max_record);
        auto second = co_await freeze_child(
          *writer, std::move(second_child), resources, work);
        const auto second_cut = second.layout().boundary();
        {
            auto next = co_await execution_child(102, work);
            auto prepared = take(
              writer->prepare_group(std::span{&next, 1}, work));
            require(
              prepared.prepared.has_value(),
              "third group did not reach queue admission");
            auto held = take(resources.try_reserve_buffer(next.bytes()));
            std::vector<admitted_wal_batch> children;
            children.push_back(take(
              admitted_wal_batch::make(
                std::move(next), std::move(held), child_append_time, charge)));
            const auto rejected = co_await writer->freeze_group(
              std::move(*prepared.prepared), std::move(children), work);
            require(
              !rejected && rejected.error().code() == errc::queue_full
                && !writer->failure().failed()
                && writer->progress()->reserved == second_cut.end(),
              "queue rejection changed frozen positions or fenced valid work");
        }
        require(
          first.layout().blocks()[0].records.bytes().begin() == empty.bytes
            && second.layout().blocks()[0].records.bytes().begin()
                 == first_cut.footer()->end()
            && writer->progress()->written == empty
            && writer->progress()->durable == empty,
          "freezing changed completed progress or reused footer space");
        const auto selected = take(local_paths::make(spec.root));
        const auto path = take(selected.segment_file(
          0,
          {descriptor().segment.segment(), descriptor().segment.generation()},
          local_segment_file::data));
        require(
          take(co_await drive.lifecycle(files.stat(path))).size.value()
            == empty.bytes.value(),
          "freezing performed a file write");
        take(co_await writer->encode_group(first, work));
        take(co_await writer->encode_group(second, work));
        require(
          first.layout().boundary() == first_cut,
          "later group mutated a frozen layout");
        const std::array frozen{
          first.layout().blocks()[0].records,
          second.layout().blocks()[0].records};
        const auto first_block = flat(first.blocks()[0].bytes());
        const auto second_block = flat(second.blocks()[0].bytes());
        const auto first_footer = footer_wire(
          {first_cut.covered(),
           first_cut.end().blocks,
           first.layout().blocks().back().records,
           first_times,
           crc(first_block)},
          {first_cut.history(), first_cut.footer()->begin()});
        const auto first_bytes = first_block + first_footer;
        const auto second_footer = footer_wire(
          {second_cut.covered(),
           second_cut.end().blocks,
           second.layout().blocks().back().records,
           second_times,
           crc(first_bytes + second_block)},
          {second_cut.history(), second_cut.footer()->begin()});
        const auto second_bytes = second_block + second_footer;
        const auto premature = co_await drive.lifecycle(
          writer->barrier(first_cut));
        require(
          premature.failure.error()
            && premature.failure.error()->code() == errc::queue_full
            && !premature.receipt,
          "barrier accepted an unauthorized frozen group");
        auto a = take(writer->submit(std::move(first), work));
        auto b = take(writer->submit(std::move(second), work));
        auto first_result = co_await drive.lifecycle(std::move(a.written));
        auto second_result = co_await drive.lifecycle(std::move(b.written));
        take(first_result.failure.outcome());
        take(second_result.failure.outcome());
        require(
          first_result.written.value() == first_bytes.size()
            && second_result.written.value() == second_bytes.size()
            && writer->progress()->written == second_cut.end()
            && writer->progress()->durable == empty,
          "write completion fabricated a durable segment cut");
        require(
          std::ranges::equal(trace.written, frozen) && trace.durable.empty(),
          "written blocks were not reported once, in file order, or a "
          "write was reported durable");
        if (synchronize) {
            const auto admission = resources.snapshot();
            resources.close_admission();
            const auto first_sync = co_await drive.lifecycle(
              writer->barrier(first_cut));
            take(first_sync.failure.outcome());
            require(
              first_sync.receipt && first_sync.receipt->boundary() == first_cut
                && writer->progress()->durable == first_cut.end(),
              "captured flush promoted a later written group");
            const auto again = co_await drive.lifecycle(
              writer->barrier(first_cut));
            take(again.failure.outcome());
            require(
              again.receipt && again.receipt->boundary() == first_cut,
              "repeated barrier changed its captured receipt");
            require(
              trace.durable.size() == 1
                && trace.durable[0] == first_cut.end().bytes,
              "a barrier reported more or less than the cut it made durable");
            const auto second_sync = co_await drive.lifecycle(
              writer->barrier(second_cut));
            take(second_sync.failure.outcome());
            require(
              second_sync.receipt
                && second_sync.receipt->boundary() == second_cut
                && writer->progress()->durable == second_cut.end()
                && resources.snapshot().accepted == admission.accepted
                && resources.snapshot().rejected == admission.rejected,
              "segment barrier reacquired ordinary admission or lost its cut");
            require(
              trace.durable.size() == 2
                && trace.durable[1] == second_cut.end().bytes
                && trace.written.size() == frozen.size(),
              "the later barrier did not report its own cut");
        }
        take(co_await drive.lifecycle(writer->close()));
        const auto stored = co_await store_contract::read_bytes(
          files, path, drive);
        require(
          stored.substr(empty.bytes.value()) == first_bytes + second_bytes,
          "segment execution altered exact child/footer bytes");
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

// One group may freeze several batches: their blocks are contiguous, one
// footer after the last block covers all of them, and one barrier makes the
// whole group durable.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> grouped_execution(
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
    auto config = configuration();
    config.maximum_groups = 1;
    config.admission.working_bytes = byte_count{1_MiB};
    auto writer = take(
      writer_type::make_new(
        files,
        owner,
        spec,
        0,
        descriptor(),
        logical_origin(),
        resources,
        config));
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        const auto empty = writer->progress()->durable;
        std::vector<encoded_assigned_batch> batches;
        batches.reserve(3);
        batches.push_back(co_await child(work, 4096));
        batches.push_back(co_await execution_child(101, work));
        batches.push_back(co_await execution_child(102, work));
        auto prepared = take(writer->prepare_group(std::span{batches}, work));
        require(
          prepared.prepared.has_value(), "grouped batches were not prepared");
        std::vector<admitted_wal_batch> children;
        children.reserve(batches.size());
        written_prefix records;
        for (auto& batch : batches) {
            records.add(batch);
            auto held = take(resources.try_reserve_buffer(batch.bytes()));
            children.push_back(take(
              admitted_wal_batch::make(
                std::move(batch), std::move(held), child_append_time, charge)));
        }
        const auto unset = writer->observe_blocks({});
        require(
          !unset && unset.error().code() == errc::invalid_argument,
          "an observer with no calls was bound");
        auto group = take(
          co_await writer->freeze_group(
            std::move(*prepared.prepared), std::move(children), work));
        block_trace late;
        const auto missed = writer->observe_blocks(late.observer());
        require(
          !missed && missed.error().code() == errc::wrong_context
            && !writer->failure().failed(),
          "an observer was bound after a block it would never see");
        const auto cut = group.layout().boundary();
        const auto layout = group.layout().blocks();
        require(
          layout.size() == 3 && layout[0].records.bytes().begin() == empty.bytes
            && layout[1].records.bytes().begin()
                 == layout[0].records.bytes().end()
            && layout[2].records.bytes().begin()
                 == layout[1].records.bytes().end()
            && cut.footer()
            && cut.footer()->begin() == layout[2].records.bytes().end()
            && cut.end().blocks == empty.blocks + 3U
            && cut.end().footers == empty.footers + 1U,
          "grouped batches were not contiguous under one footer");
        take(co_await writer->encode_group(group, work));
        std::string blocks;
        for (const auto& block : group.blocks())
            blocks += flat(block.bytes());
        const auto footer = footer_wire(
          {cut.covered(),
           cut.end().blocks,
           layout.back().records,
           appended_times(records.max_record),
           crc(blocks)},
          {cut.history(), cut.footer()->begin()});
        auto submitted = take(writer->submit(std::move(group), work));
        auto written = co_await drive.lifecycle(std::move(submitted.written));
        take(written.failure.outcome());
        require(
          written.written.value() == blocks.size() + footer.size()
            && writer->progress()->written == cut.end()
            && writer->progress()->durable == empty,
          "grouped write lost bytes or fabricated a durable cut");
        const auto synced = co_await drive.lifecycle(writer->barrier(cut));
        take(synced.failure.outcome());
        require(
          synced.receipt && synced.receipt->boundary() == cut
            && writer->progress()->durable == cut.end(),
          "one barrier did not make the whole group durable");
        take(co_await drive.lifecycle(writer->close()));
        const auto selected = take(local_paths::make(spec.root));
        const auto path = take(selected.segment_file(
          0,
          {descriptor().segment.segment(), descriptor().segment.generation()},
          local_segment_file::data));
        const auto stored = co_await store_contract::read_bytes(
          files, path, drive);
        require(
          stored.substr(empty.bytes.value()) == blocks + footer,
          "grouped execution altered exact block/footer bytes");
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

// Freezes and encodes one group without submitting it. Returns the group
// and its exact stored bytes; the running prefix covers every earlier byte
// and block.
template<typename Writer>
seastar::future<std::pair<segment_frozen_group, std::string>> encoded_group(
  Writer& writer,
  std::vector<encoded_assigned_batch> batches,
  workload_budget& resources,
  codec::cooperative_work& work,
  written_prefix& prefix) {
    auto prepared = take(writer.prepare_group(std::span{batches}, work));
    require(prepared.prepared.has_value(), "test group could not be prepared");
    std::vector<admitted_wal_batch> children;
    children.reserve(batches.size());
    for (auto& batch : batches) {
        prefix.add(batch);
        auto held = take(resources.try_reserve_buffer(batch.bytes()));
        children.push_back(take(
          admitted_wal_batch::make(
            std::move(batch), std::move(held), child_append_time, charge)));
    }
    auto group = take(
      co_await writer.freeze_group(
        std::move(*prepared.prepared), std::move(children), work));
    take(co_await writer.encode_group(group, work));
    const auto cut = group.layout().boundary();
    std::string bytes;
    for (const auto& block : group.blocks())
        bytes += flat(block.bytes());
    prefix.checksum.extend(std::span<const char>{bytes});
    const auto footer = footer_wire(
      {cut.covered(),
       cut.end().blocks,
       group.layout().blocks().back().records,
       appended_times(prefix.max_record),
       prefix.checksum.value()},
      {cut.history(), cut.footer()->begin()});
    prefix.checksum.extend(std::span<const char>{footer});
    bytes += footer;
    co_return std::pair{std::move(group), std::move(bytes)};
}

// Freezes, encodes and submits one group without awaiting its write.
// Returns the submission and exact stored bytes.
template<typename Writer>
seastar::future<std::pair<segment_submission, std::string>> submit_group(
  Writer& writer,
  std::vector<encoded_assigned_batch> batches,
  workload_budget& resources,
  codec::cooperative_work& work,
  written_prefix& prefix) {
    auto [group, bytes] = co_await encoded_group(
      writer, std::move(batches), resources, work, prefix);
    co_return std::pair{
      take(writer.submit(std::move(group), work)), std::move(bytes)};
}

// Submits one group and awaits only its write. Returns its cut and exact
// stored bytes.
template<typename Writer, typename Driver>
seastar::future<std::pair<segment_captured_boundary, std::string>> write_group(
  Writer& writer,
  std::vector<encoded_assigned_batch> batches,
  workload_budget& resources,
  codec::cooperative_work& work,
  Driver drive,
  written_prefix& prefix) {
    auto [submitted, bytes] = co_await submit_group(
      writer, std::move(batches), resources, work, prefix);
    auto written = co_await drive.lifecycle(std::move(submitted.written));
    take(written.failure.outcome());
    co_return std::pair{submitted.boundary, std::move(bytes)};
}

// A reader's pin of a segment that is still written. Readers share one
// read-only handle, opened when the first asks and apart from the handle the
// writer writes through. Each is told how far it may read, the durable
// position as it stood, and reads the exact stored bytes below it: nothing
// before a barrier, the whole group after it. A bound is a snapshot: a read
// past it is refused whatever was appended since, and the reader that wants
// more asks again. A refused read is that read's outcome and fails nothing.
// A read outlives the pin it was asked through, and close waits for pins.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> data_pins(
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
    auto config = configuration();
    config.maximum_groups = 2;
    config.admission.working_bytes = byte_count{1_MiB};
    auto writer = take(
      writer_type::make_new(
        files,
        owner,
        spec,
        0,
        descriptor(),
        logical_origin(),
        resources,
        config));
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        const auto start = writer->progress()->durable.bytes;
        const auto unit = descriptor().alignment.bytes();
        const auto before = resources.snapshot();
        // The first reader: the handle is opened for it, at the cost of one
        // handle and one admission.
        std::optional<segment_data_view> first{
          take(co_await drive.lifecycle(writer->pin_data(work)))};
        const auto opened = resources.snapshot();
        require(
          first->durable == start
            && first->history.segment == descriptor().segment
            && first->history.data_start == start
            && opened.handles == before.handles + 1
            && opened.tasks == before.tasks + 1,
          "the first reader was not given the durable position, or its "
          "handle is not one handle");
        {
            // Nothing is durable: nothing is readable.
            const auto unread = co_await drive.lifecycle(
              first->pin.read(start, unit, first->durable));
            require(
              !unread && unread.error().code() == errc::out_of_range,
              "a reader read past the durable position");
        }
        written_prefix prefix;
        std::vector<encoded_assigned_batch> batches;
        batches.push_back(co_await child(work, 4096));
        batches.push_back(co_await execution_child(101, work));
        const auto [cut, stored] = co_await write_group(
          *writer, std::move(batches), resources, work, drive, prefix);
        const byte_count length{cut.end().bytes.value() - start.value()};
        require(
          stored.size() == length.value()
            && writer->progress()->written == cut.end()
            && writer->progress()->durable.bytes == start,
          "the group to read was not written, or is durable already");
        {
            // Written and not durable: a reader that asks now is told the
            // same position, and reads nothing of the group.
            const auto early = take(
              co_await drive.lifecycle(writer->pin_data(work)));
            const auto unread = co_await drive.lifecycle(
              early.pin.read(start, length, early.durable));
            require(
              early.durable == start && !unread
                && unread.error().code() == errc::out_of_range
                && resources.snapshot().handles == opened.handles,
              "written bytes that are not durable were readable, or a second "
              "reader took a handle of its own");
        }
        const auto synced = co_await drive.lifecycle(writer->barrier(cut));
        take(synced.failure.outcome());
        std::optional<segment_data_view> second{
          take(co_await drive.lifecycle(writer->pin_data(work)))};
        require(
          second->durable == cut.end().bytes
            && resources.snapshot().handles == opened.handles,
          "a reader that asked after the barrier was not told its position");
        {
            const auto whole = take(
              co_await drive.lifecycle(
                second->pin.read(start, length, second->durable)));
            require(
              flat(whole) == stored,
              "the durable group read through a pin is not what was written");
        }
        {
            // The first reader's bound is where it stood: it reads no
            // further, though the bytes are durable now. The pin is the
            // handle, not the bound: under the newer bound it reads them.
            const auto bounded = co_await drive.lifecycle(
              first->pin.read(start, length, first->durable));
            const auto asked_again = take(
              co_await drive.lifecycle(
                first->pin.read(start, length, second->durable)));
            require(
              !bounded && bounded.error().code() == errc::out_of_range
                && flat(asked_again) == stored && !writer->failure().failed(),
              "a bound moved with later appends, or a refused read failed "
              "its segment");
        }
        {
            // A read outlives the pin it was asked through, and a share of
            // a pin is another pin.
            auto shared = second->pin.share();
            auto reading = second->pin.read(start, length, second->durable);
            const auto bound = second->durable;
            second.reset();
            first.reset();
            const auto whole = take(
              co_await drive.lifecycle(std::move(reading)));
            const auto again = take(
              co_await drive.lifecycle(shared.read(start, length, bound)));
            require(
              flat(whole) == stored && flat(again) == stored,
              "a read did not complete once its pin was let go");
        }
        // Every pin is let go: close has nothing to wait for, closes the
        // handle, and gives its credit back.
        take(co_await drive.lifecycle(writer->close()));
        const auto refused = co_await drive.lifecycle(writer->pin_data(work));
        require(
          !refused && refused.error().code() == errc::closed,
          "a closed segment gave out a reader's pin");
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

// A zero-written segment. Creation extends the file with durable zeros. A
// group above the synchronized limit takes an ordinary write and its
// barrier's flush; a small group inside the zero-written range takes a
// synchronized write, durable at completion without any barrier (a backend
// that can crash proves it); a group crossing the range end takes an
// ordinary write again. Stored bytes stay exact throughout.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> preallocated_execution(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    constexpr bool crashable = requires(Backend& backend) { backend.crash(); };
    constexpr std::uint64_t window = 32_KiB;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    auto config = configuration();
    config.maximum_groups = 1;
    config.admission.working_bytes = byte_count{1_MiB};
    config.preallocation_bytes = byte_count{window};
    config.synchronous_write_bytes = byte_count{16_KiB};
    auto writer = take(
      writer_type::make_new(
        files,
        owner,
        spec,
        0,
        descriptor(),
        logical_origin(),
        resources,
        config));
    const auto selected = take(local_paths::make(spec.root));
    const auto path = take(selected.segment_file(
      0,
      {descriptor().segment.segment(), descriptor().segment.generation()},
      local_segment_file::data));
    runtime::first_failure failed;
    bool crashed = false;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        const auto start = writer->progress()->durable.bytes.value();
        const auto zeroed = co_await store_contract::read_bytes(
          files, path, drive);
        require(
          zeroed.size() == start + window
            && zeroed.find_first_not_of('\0', start) == std::string::npos,
          "creation did not zero-write the preallocated range");
        written_prefix prefix;
        // Two blocks of 8,401 bytes and the footer after them span three
        // units: past the synchronized limit. Each later group is one.
        std::vector<encoded_assigned_batch> first;
        first.push_back(co_await valued_execution_child(100, 8000, work));
        first.push_back(co_await valued_execution_child(101, 8000, work));
        auto [ordinary, expected] = co_await write_group(
          *writer, std::move(first), resources, work, drive, prefix);
        require(
          ordinary.end().bytes.value() - start > 16384
            && ordinary.end().bytes.value() <= start + window,
          "ordinary group geometry changed");
        const auto flushed = co_await drive.lifecycle(
          writer->barrier(ordinary));
        take(flushed.failure.outcome());
        require(
          flushed.receipt && writer->progress()->durable == ordinary.end(),
          "ordinary group barrier did not make it durable");
        std::vector<encoded_assigned_batch> second;
        second.push_back(co_await execution_child(102, work));
        auto [synchronized, bytes] = co_await write_group(
          *writer, std::move(second), resources, work, drive, prefix);
        expected += bytes;
        require(
          synchronized.end().bytes.value() - ordinary.end().bytes.value()
              <= 16384
            && synchronized.end().bytes.value() <= start + window
            && writer->progress()->durable == ordinary.end(),
          "synchronized group geometry changed or fabricated durability");
        if constexpr (crashable) {
            // No barrier: only the synchronized completion makes it durable.
            take(co_await drive.lifecycle(files.crash()));
            crashed = true;
            static_cast<void>(co_await drive.lifecycle(writer->close()));
            const auto stored = co_await store_contract::read_bytes(
              files, path, drive);
            require(
              stored.size() == start + window
                && stored.substr(start, expected.size()) == expected
                && stored.find_first_not_of('\0', start + expected.size())
                     == std::string::npos,
              "a completed synchronized write was lost by a crash");
        } else {
            const auto synced = co_await drive.lifecycle(
              writer->barrier(synchronized));
            take(synced.failure.outcome());
            require(
              synced.receipt
                && writer->progress()->durable == synchronized.end(),
              "synchronized group barrier did not make it durable");
            std::vector<encoded_assigned_batch> third;
            third.push_back(co_await execution_child(103, work));
            auto [crossing, tail] = co_await write_group(
              *writer, std::move(third), resources, work, drive, prefix);
            expected += tail;
            require(
              crossing.end().bytes.value() > start + window,
              "crossing group stayed inside the zero-written range");
            const auto crossed = co_await drive.lifecycle(
              writer->barrier(crossing));
            take(crossed.failure.outcome());
            require(
              crossed.receipt && writer->progress()->durable == crossing.end(),
              "crossing group barrier did not make it durable");
            take(co_await drive.lifecycle(writer->close()));
            const auto stored = co_await store_contract::read_bytes(
              files, path, drive);
            require(
              stored.size() == crossing.end().bytes.value()
                && stored.substr(start) == expected,
              "zero-written execution altered exact block/footer bytes");
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        auto closed = co_await drive.lifecycle(writer->close());
        if (!crashed) failed.observe(closed);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    writer.reset();
    take(failed.outcome());
}

// At the production defaults creation zero-writes an 8 MiB window past the
// header, within the data limit, and the segment's credits count the
// synchronized handle: a group inside the window is a synchronized write,
// durable at completion without a barrier (a backend that can crash proves
// it). The range is kept a whole step ahead of the reservations, so the
// first group extends it by one step, which a crash may lose. Stored bytes
// stay exact, and the rest of the range zeros.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> zero_written_by_default(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    constexpr bool crashable = requires(Backend& backend) { backend.crash(); };
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    auto config = with_defaults();
    config.admission.working_bytes = byte_count{1_MiB};
    const auto window = config.preallocation_bytes.value();
    require(
      window == 8_MiB
        && segment_writer_handles(config)
             == segment_writer_handles(configuration()) + 1,
      "the default window changed, or its synchronized handle is not "
      "counted");
    auto writer = take(
      writer_type::make_new(
        files,
        owner,
        spec,
        0,
        descriptor(),
        logical_origin(),
        resources,
        config));
    const auto selected = take(local_paths::make(spec.root));
    const auto path = take(selected.segment_file(
      0,
      {descriptor().segment.segment(), descriptor().segment.generation()},
      local_segment_file::data));
    runtime::first_failure failed;
    bool crashed = false;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        const auto start = writer->progress()->durable.bytes.value();
        const auto zeroed = co_await store_contract::read_all_bytes(
          files, path, drive);
        require(
          zeroed.size() == start + window
            && zeroed.find_first_not_of('\0', start) == std::string::npos,
          "creation did not zero-write the default window");
        written_prefix prefix;
        std::vector<encoded_assigned_batch> batches;
        batches.push_back(co_await execution_child(100, work));
        auto [cut, expected] = co_await write_group(
          *writer, std::move(batches), resources, work, drive, prefix);
        require(
          cut.end().bytes.value() <= start + window
            && writer->progress()->durable.bytes.value() == start,
          "the group left the window or fabricated durability");
        if constexpr (crashable) {
            // No barrier: only the synchronized completion makes it durable.
            take(co_await drive.lifecycle(files.crash()));
            crashed = true;
            static_cast<void>(co_await drive.lifecycle(writer->close()));
        } else {
            const auto synced = co_await drive.lifecycle(writer->barrier(cut));
            take(synced.failure.outcome());
            require(
              synced.receipt && writer->progress()->durable == cut.end(),
              "the group's barrier did not make it durable");
            take(co_await drive.lifecycle(writer->close()));
        }
        const auto stored = co_await store_contract::read_all_bytes(
          files, path, drive);
        const auto extended = start + window
                              + config.preallocation_extension_bytes.value();
        require(
          (stored.size() == extended
           || (crashed && stored.size() == start + window))
            && stored.substr(start, expected.size()) == expected
            && stored.find_first_not_of('\0', start + expected.size())
                 == std::string::npos,
          "a synchronized write in the default window was lost, or the "
          "range was not kept a step ahead");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        auto closed = co_await drive.lifecycle(writer->close());
        if (!crashed) failed.observe(closed);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    writer.reset();
    take(failed.outcome());
}

// Small groups in the zero-written range are written without waiting for
// one another. Completions still arrive in file order, one barrier covers
// them all, and stored bytes stay exact; a backend that can crash proves the
// completed writes durable without that barrier.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> concurrent_execution(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    constexpr bool crashable = requires(Backend& backend) { backend.crash(); };
    constexpr std::uint32_t groups = 4;
    constexpr std::uint64_t window = 64_KiB;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    auto config = configuration();
    config.maximum_groups = groups;
    config.admission.working_bytes = byte_count{1_MiB};
    config.preallocation_bytes = byte_count{window};
    config.synchronous_write_bytes = byte_count{16_KiB};
    config.concurrent_writes = groups;
    auto writer = take(
      writer_type::make_new(
        files,
        owner,
        spec,
        0,
        descriptor(),
        logical_origin(),
        resources,
        config));
    const auto selected = take(local_paths::make(spec.root));
    const auto path = take(selected.segment_file(
      0,
      {descriptor().segment.segment(), descriptor().segment.generation()},
      local_segment_file::data));
    runtime::first_failure failed;
    bool crashed = false;
    std::vector<std::optional<seastar::future<segment_write_completion>>>
      written;
    written.reserve(groups);
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        const auto start = writer->progress()->durable.bytes.value();
        written_prefix prefix;
        std::string expected;
        std::vector<segment_captured_boundary> cuts;
        cuts.reserve(groups);
        for (std::uint32_t i = 0; i < groups; ++i) {
            std::vector<encoded_assigned_batch> batches;
            if (i == 0)
                batches.push_back(co_await child(work));
            else
                batches.push_back(co_await execution_child(100 + i, work));
            auto [submitted, bytes] = co_await submit_group(
              *writer, std::move(batches), resources, work, prefix);
            const auto begin = cuts.empty() ? start
                                            : cuts.back().end().bytes.value();
            require(
              submitted.boundary.end().bytes.value() - begin <= 16384,
              "concurrent test group exceeds the synchronized size");
            expected += bytes;
            cuts.push_back(submitted.boundary);
            written.emplace_back(std::move(submitted.written));
        }
        require(
          cuts.back().end().bytes.value() <= start + window,
          "concurrent test groups left the zero-written range");
        // The last completion is released only after every earlier one.
        auto last = co_await drive.lifecycle(std::move(*written.back()));
        written.back().reset();
        take(last.failure.outcome());
        for (std::uint32_t i = 0; i + 1 < groups; ++i)
            require(
              written[i]->available(),
              "a later group completed before an earlier one");
        for (std::uint32_t i = 0; i + 1 < groups; ++i) {
            auto done = co_await std::move(*written[i]);
            written[i].reset();
            take(done.failure.outcome());
        }
        written.clear();
        require(
          writer->progress()->written == cuts.back().end(),
          "written position skipped a completed group");
        if constexpr (crashable) {
            take(co_await drive.lifecycle(files.crash()));
            crashed = true;
            static_cast<void>(co_await drive.lifecycle(writer->close()));
            const auto stored = co_await store_contract::read_all_bytes(
              files, path, drive);
            require(
              stored.size() == start + window
                && stored.substr(start, expected.size()) == expected,
              "a completed concurrent synchronized write was lost by a crash");
        } else {
            const auto synced = co_await drive.lifecycle(
              writer->barrier(cuts.back()));
            take(synced.failure.outcome());
            require(
              synced.receipt
                && writer->progress()->durable == cuts.back().end(),
              "one barrier did not cover the concurrent groups");
            take(co_await drive.lifecycle(writer->close()));
            const auto stored = co_await store_contract::read_all_bytes(
              files, path, drive);
            require(
              stored.size() == start + window
                && stored.substr(start, expected.size()) == expected
                && stored.find_first_not_of('\0', start + expected.size())
                     == std::string::npos,
              "concurrent writes altered exact block/footer bytes");
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    // An entered completion is never dropped.
    for (auto& pending : written)
        if (pending) {
            try {
                static_cast<void>(
                  co_await drive.lifecycle(std::move(*pending)));
            } catch (...) {
                failed.observe(std::current_exception());
            }
        }
    try {
        auto closed = co_await drive.lifecycle(writer->close());
        if (!crashed) failed.observe(closed);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    writer.reset();
    take(failed.outcome());
}

// The zero-written range keeps moving ahead of the reservations: groups
// past the creation window wait for the next extension instead of taking an
// ordinary write, so every one of them is durable at completion (a backend
// that can crash proves it) and the stored bytes stay exact.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> extended_execution(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    constexpr bool crashable = requires(Backend& backend) { backend.crash(); };
    // Each group is one unit: nine of them pass two windows.
    constexpr std::uint32_t groups = 9;
    constexpr std::uint64_t window = 32_KiB;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    auto config = configuration();
    config.maximum_groups = 1;
    config.admission.working_bytes = byte_count{1_MiB};
    config.preallocation_bytes = byte_count{window};
    config.preallocation_extension_bytes = byte_count{window};
    config.synchronous_write_bytes = byte_count{16_KiB};
    auto writer = take(
      writer_type::make_new(
        files,
        owner,
        spec,
        0,
        descriptor(),
        logical_origin(),
        resources,
        config));
    const auto selected = take(local_paths::make(spec.root));
    const auto path = take(selected.segment_file(
      0,
      {descriptor().segment.segment(), descriptor().segment.generation()},
      local_segment_file::data));
    runtime::first_failure failed;
    bool crashed = false;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        const auto start = writer->progress()->durable.bytes.value();
        written_prefix prefix;
        std::string expected;
        std::optional<segment_captured_boundary> last;
        for (std::uint32_t i = 0; i < groups; ++i) {
            std::vector<encoded_assigned_batch> batches;
            if (i == 0)
                batches.push_back(co_await child(work));
            else
                batches.push_back(co_await execution_child(100 + i, work));
            auto [cut, bytes] = co_await write_group(
              *writer, std::move(batches), resources, work, drive, prefix);
            expected += bytes;
            last = cut;
        }
        require(
          last->end().bytes.value() > start + 2 * window,
          "extension test groups stayed inside two windows");
        if constexpr (crashable) {
            // No barrier: only synchronized completions make them durable.
            take(co_await drive.lifecycle(files.crash()));
            crashed = true;
            static_cast<void>(co_await drive.lifecycle(writer->close()));
            const auto stored = co_await store_contract::read_all_bytes(
              files, path, drive);
            require(
              stored.size() >= start + expected.size()
                && stored.substr(start, expected.size()) == expected,
              "a group past the creation window took an ordinary write");
        } else {
            const auto synced = co_await drive.lifecycle(
              writer->barrier(*last));
            take(synced.failure.outcome());
            require(
              synced.receipt && writer->progress()->durable == last->end(),
              "extended execution barrier did not make it durable");
            take(co_await drive.lifecycle(writer->close()));
            const auto stored = co_await store_contract::read_all_bytes(
              files, path, drive);
            require(
              stored.size() >= start + expected.size()
                && stored.size() % 8192 == 0
                && stored.substr(start, expected.size()) == expected
                && stored.find_first_not_of('\0', start + expected.size())
                     == std::string::npos,
              "window extension altered exact block/footer bytes");
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        auto closed = co_await drive.lifecycle(writer->close());
        if (!crashed) failed.observe(closed);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    writer.reset();
    take(failed.outcome());
}

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> abandoned_group(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive,
  bool keep_borrow = false) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    // What the owner reports of its blocks: a group that is never written
    // is never reported, and nothing of it becomes durable.
    block_trace trace, late;
    auto writer = take(
      writer_type::make_new(
        files,
        owner,
        spec,
        0,
        descriptor(),
        logical_origin(),
        resources,
        configuration()));
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        take(writer->observe_blocks(trace.observer()));
        const auto before = writer->progress()->reserved;
        auto first = co_await freeze_child(
          *writer, co_await child(work), resources, work);
        const auto frozen = first.layout().boundary();
        if (keep_borrow) {
            take(co_await writer->encode_group(first, work));
            const auto blocks = first.blocks();
            const auto encoded = flat(blocks[0].bytes());
            const auto closed = co_await drive.lifecycle(writer->close());
            require(
              !closed && closed.error().code() == errc::aborted,
              "close did not fence an unsubmitted encoded group");
            require(
              flat(blocks[0].bytes()) == encoded,
              "close invalidated a live handoff's encoded byte borrow");
            auto verifier = extent_verifier::make(
                              frozen.history(), frozen.covered(), work.policy())
                              .value();
            (co_await verifier.add_block(
               blocks[0], batch_expected(), budget(), work))
              .value();
            require(
              verifier.finish(work).value().boundary().data_crc32c
                == crc(encoded),
              "closed writer changed independently borrowed extent evidence");
        } else {
            auto abandoned = std::move(first);
        }
        require(
          writer->failure().failed() && !writer->capture()
            && writer->progress()->reserved == frozen.end()
            && writer->progress()->written == before,
          "abandonment rewound or completed a frozen reservation");
        auto closed = co_await drive.lifecycle(writer->close());
        require(
          !closed && closed.error().code() == errc::aborted,
          "close lost an unexecuted reservation or waited for its missing "
          "caller");
        const auto unbound = writer->observe_blocks(late.observer());
        require(
          trace.written.empty() && trace.durable.empty() && !unbound
            && unbound.error().code() == errc::closed,
          "a group that was never written was reported, or a closed owner "
          "took an observer");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        static_cast<void>(co_await drive.lifecycle(writer->close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    writer.reset();
    take(failed.outcome());
}
// Snapshot ownership is explicit; completed facts are supplied independently
// of the segment's write outcome. The empty case still creates a page-only
// file.
struct completed_source final {
    std::optional<workload_reservation> held;
    std::vector<completed_retry> facts;
    std::uint32_t* calls;
    bool change_on_replay{false};
    seastar::future<runtime::result<std::vector<completed_retry>>>
    read(std::uint32_t first, std::uint32_t count, codec::cooperative_work&) {
        if (first > facts.size() || count > facts.size() - first)
            co_return runtime::failure(detail::path_error(errc::out_of_range));
        ++*calls;
        std::vector<completed_retry> output;
        output.reserve(count);
        for (std::uint32_t i = 0; i < count; ++i)
            output.push_back(facts[first + i]);
        if (change_on_replay && *calls > 1 && !output.empty()) {
            const auto& old = output.front();
            auto digest = old.submitted_digest().bytes();
            digest[0] ^= 1;
            output.front() = completed_retry::make(
                               old.id(),
                               codec::semantic_batch_digest{digest},
                               old.original_binding(),
                               old.returned_span(),
                               old.ack_generation())
                               .value();
        }
        co_return output;
    }
};

// `unused_entry`: the group's batch reserved a seal entry that no fact
// fills, as when it was never published durable; the seal publishes the
// facts it is given and nothing for the entry.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> seal_lifecycle(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive,
  bool empty = false,
  bool saturated = false,
  bool changed_source = false,
  bool unused_entry = false) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    auto config = configuration();
    config.admission.working_bytes = byte_count{1_MiB};
    block_trace trace;
    auto writer = take(
      writer_type::make_new(
        files,
        owner,
        spec,
        0,
        descriptor(),
        logical_origin(),
        resources,
        config));
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        require(
          !(co_await drive.lifecycle(writer->evict_read_handle())),
          "active writable descriptor was evicted");
        take(writer->observe_blocks(trace.observer()));
        std::uint32_t calls = 0;
        std::vector<completed_retry> facts;
        std::string extent_bytes;
        std::optional<storage::coverage> last;
        std::optional<runtime::file_position> data_end;
        std::optional<runtime::wall_time> max_record;
        if (!empty) {
            auto batch = co_await child(work);
            const auto info = batch.info();
            max_record = info.max_record_time;
            const auto original = info.context.submitted();
            facts.push_back(
              completed_retry::make(
                original.id(),
                info.fingerprint,
                original.binding(),
                info.context.logical_span(),
                original.binding().generation())
                .value());
            auto group = co_await freeze_child(
              *writer, std::move(batch), resources, work);
            take(co_await writer->encode_group(group, work));
            last = group.layout().blocks()[0].records;
            const auto cut = group.layout().boundary();
            data_end = cut.end().bytes;
            extent_bytes = flat(group.blocks()[0].bytes());
            extent_bytes += footer_wire(
              {cut.covered(),
               cut.end().blocks,
               last,
               appended_times(max_record),
               crc(extent_bytes)},
              {cut.history(), cut.footer()->begin()});
            auto submitted = take(writer->submit(std::move(group), work));
            const auto result = co_await drive.lifecycle(
              std::move(submitted.written));
            take(result.failure.outcome());
        }
        // One fact more than the groups reserved entries for.
        const auto excess = co_await drive.lifecycle(writer->seal(
          completed_source{{}, {}, &calls}, empty ? 1U : 2U, work));
        require(
          excess.failure.error()
            && excess.failure.error()->code() == errc::invalid_argument
            && !writer->failure().failed()
            && writer->append_state() == model::append_state::active
            && calls == 0,
          "a seal of more facts than its groups reserved changed admission");
        require(
          trace.durable.empty()
            && (empty ? trace.written.empty()
                      : trace.written.size() == 1 && trace.written[0] == *last),
          "a refused seal reported durable blocks, or the written block "
          "was not reported");
        if (unused_entry) facts.clear();
        const auto count = static_cast<std::uint32_t>(facts.size());
        auto source_grant = take(resources.try_reserve(byte_count{4_KiB}));
        // The seal slot's grants, reserved before admission closes.
        segment_seal_grants grants;
        take(grants.fill(resources, writer->config().metadata));
        const auto before = resources.snapshot();
        if (saturated) resources.close_admission();
        auto sealed = co_await drive.lifecycle(writer->seal(
          completed_source{
            std::move(source_grant), std::move(facts), &calls, changed_source},
          count,
          work,
          segment_no_index{},
          &grants));
        auto repeated = co_await drive.lifecycle(
          writer->seal(completed_source{{}, {}, &calls}, 0, work));
        require(
          sealed.failure.failed() == changed_source
            && repeated.failure.error() == sealed.failure.error(),
          "repeated seal reran or changed the retained attempt");
        // No barrier covered the group: the seal's own data sync did.
        require(
          empty ? trace.durable.empty()
                : trace.durable.size() == 1 && trace.durable[0] == *data_end,
          "the seal did not report the blocks its data sync made durable");
        require(
          calls == (empty || unused_entry ? 0U : 2U),
          "seal did not use one deterministic pair of page passes");
        if (saturated)
            require(
              resources.snapshot().accepted == before.accepted
                && resources.snapshot().rejected == before.rejected,
              "seal tried fresh ordinary workload admission");
        if (changed_source) {
            require(
              !sealed.extent && !sealed.boundary
                && !writer->seal_progress().root_written,
              "changed completed facts escaped the immutable bundle check");
        } else {
            take(sealed.failure.outcome());
            require(
              sealed.extent && sealed.boundary && sealed.retry
                && writer->append_state() == model::append_state::sealed
                && writer->seal_progress().data_synced
                && writer->seal_progress().root_synced
                && repeated.extent == sealed.extent && !writer->capture(),
              "sealed result preceded dependencies or revived admission");
            const auto history = writer->seal_progress().extent->context();
            const footer_expectation location{
              history, sealed.boundary->position()};
            std::vector<page_ref> refs;
            std::string page;
            if (!empty && !unused_entry) {
                const std::array entries{retry()};
                page = retry_page_wire(entries, location);
                refs.push_back(reference(page));
            }
            const auto expected_root = sealed_wire(
              {sealed.extent->coverage,
               empty ? 0U : 1U,
               last,
               appended_times(max_record),
               0},
              exact_digest(extent_bytes),
              refs,
              location);
            require(
              sealed.extent->digest.bytes() == exact_digest(extent_bytes),
              "seal rehashed, omitted or repeated a footer");
            const auto paths = local_paths::make(spec.root).value();
            const local_segment_name name{
              descriptor().segment.segment(),
              descriptor().segment.generation()};
            const auto data_path = take(
              paths.segment_file(0, name, local_segment_file::data));
            const auto bytes = co_await store_contract::read_bytes(
              files, data_path, drive);
            require(
              bytes.substr(history.data_start.value())
                == extent_bytes + expected_root,
              "sealed data bytes differ from independent footer/root "
              "construction");
            const auto bundle_path = take(
              paths.object(0, name, sealed.retry->sequence()));
            require(
              (co_await store_contract::read_bytes(files, bundle_path, drive))
                == page,
              "retry dependency missing or has the wrong page-only contents");
            if (!saturated) {
                {
                    const auto read = take(
                      co_await drive.lifecycle(writer->read_immutable(
                        sealed.boundary->position(),
                        sealed.boundary->bytes(),
                        work)));
                    require(
                      flat(read.bytes) == expected_root,
                      "sealed read did not reopen read-only");
                }
                take(co_await drive.lifecycle(writer->evict_read_handle()));
                require(
                  writer->handle_state() == segment_handle_state::evicted,
                  "idle read handle did not join close");
                {
                    const auto read = take(
                      co_await drive.lifecycle(writer->read_immutable(
                        sealed.boundary->position(),
                        sealed.boundary->bytes(),
                        work)));
                    require(
                      flat(read.bytes) == expected_root,
                      "idle reopen changed immutable identity");
                }
            }
        }
        auto first_close = writer->close();
        auto second_close = writer->close();
        const auto closed = co_await drive.lifecycle(std::move(first_close));
        const auto again = co_await drive.lifecycle(std::move(second_close));
        require(
          closed.has_value() == !changed_source
            && again.has_value() == closed.has_value(),
          "concurrent close did not join the same result");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        static_cast<void>(co_await drive.lifecycle(writer->close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    writer.reset();
    take(failed.outcome());
}

enum class immutable_import_kind {
    empty_initial,
    sparse_rewrite,
    dense_relocation,
    removed_terminal,
    empty_terminal
};

template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> immutable_import(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive,
  immutable_import_kind kind) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    const bool initial = kind == immutable_import_kind::empty_initial
                         || kind == immutable_import_kind::empty_terminal;
    const bool terminal = kind == immutable_import_kind::removed_terminal
                          || kind == immutable_import_kind::empty_terminal;
    const bool sparse = kind == immutable_import_kind::sparse_rewrite;
    const bool dense = kind == immutable_import_kind::dense_relocation;
    auto description = descriptor();
    description.segment = segment_context::make(
                            spec.owner.cluster(),
                            description.segment.topic(),
                            description.segment.range(),
                            id<model::segment_id>(0x80),
                            model::segment_generation::make(2).value())
                            .value();
    description.layout = initial ? local_layout_kind::initial
                                 : local_layout_kind::rewrite;
    const model::range_logical_end origin{
      kind == immutable_import_kind::removed_terminal ? UINT64_MAX - 10
      : terminal                                      ? UINT64_MAX
                                                      : 100};
    const auto head = segment_header::make(
                        description.segment, description.alignment)
                        .value();
    {
        auto installed = co_await drive.lifecycle(
          publish_local_segment_descriptor(
            files,
            owner,
            spec,
            0,
            description,
            head,
            resources,
            store_contract::limits(),
            work));
        take(installed.failure.outcome());
    }
    const segment_history_context history{
      description.segment,
      description.alignment,
      runtime::file_position{8192},
      origin,
      description.physical_origin};
    const auto original = sparse || dense
                            ? data_child(sparse ? 102 : 104, sparse)
                            : std::string{};
    const auto physical_end = sparse ? 2U : dense ? 1U : 0U;
    // The extent: its one block, if any, and the footer that closes it,
    // padded to the boundary where the root begins.
    std::string raw;
    std::optional<storage::coverage> last;
    std::optional<runtime::wall_time> max_record;
    if (sparse || dense) {
        // Retain the exact original child; only its current placement
        // belongs to this store's independently supplied segment context.
        const auto block = block_wire(
          original,
          {segment_write_context::make(
             description.segment,
             description.alignment,
             description.physical_origin,
             history.data_start)
             .value(),
           history.data_start,
           batch_expected(),
           description.profile});
        const auto block_end = 8192 + block.size();
        const std::uint64_t last_end = sparse ? 107 : 105;
        last = scope(
          sparse ? 102 : 104, last_end, 0, physical_end, 8192, block_end);
        max_record = runtime::wall_time{max_record_time(original)};
        raw = block
              + footer_wire(
                {scope(
                   origin.value(), last_end, 0, physical_end, 8192, block_end),
                 1,
                 last,
                 appended_times(max_record),
                 crc(block)},
                {history, runtime::file_position{block_end}});
    }
    const auto logical_end = terminal ? UINT64_MAX : initial ? 100U : 110U;
    const auto covered = scope(
      origin.value(), logical_end, 0, physical_end, 8192, 8192 + raw.size());
    const footer_expectation location{history, covered.bytes().end()};
    const auto root_bytes = sealed_wire(
      {covered, last ? 1U : 0U, last, appended_times(max_record), 0},
      exact_digest(raw),
      {},
      location);
    const auto root_digest = codec::immutable_object_digest{
      exact_digest(root_bytes)};
    const auto reference = local_root_reference::make(
                             local_root_kind::sealed_retry,
                             local_object_sequence::make(45).value(),
                             location.position,
                             byte_count{root_bytes.size()},
                             page_count::make(0).value(),
                             root_digest)
                             .value();
    const auto footer = local_footer_reference::make(
                          location.position, reference.bytes(), 7, root_digest)
                          .value();
    const auto paths = local_paths::make(spec.root).value();
    const local_segment_name identity{
      description.segment.segment(), description.segment.generation()};
    const auto data_path = take(
      paths.segment_file(0, identity, local_segment_file::data));
    const auto complete = header_wire(head) + raw + root_bytes;
    co_await store_contract::write_bytes(files, data_path, complete, drive);
    co_await store_contract::write_bytes(
      files, take(paths.object(0, identity, reference.sequence())), {}, drive);
    const auto generation = local_publication_generation::make(1).value();
    const local_object_publication publication{
      description.segment, local_object_state::sealed, footer, {reference}};
    const auto metadata_header = local_metadata_header::make(
                                   local_metadata_kind::object_publication,
                                   spec.shard_owner(0).value(),
                                   generation)
                                   .value();
    auto encoded = co_await encode_local_metadata(
      {metadata_header,
       spec.identity.metadata_alignment,
       description.alignment},
      local_metadata_payload{publication},
      work,
      byte_count{256_KiB},
      charge);
    require(encoded.has_value(), "immutable publication fixture rejected");
    co_await store_contract::write_bytes(
      files,
      take(paths.segment_file(0, identity, local_segment_file::published)),
      flat(encoded->bytes),
      drive);
    const std::array<local_bundle_context, 1> roots{location};
    const local_generation_expectation expected{
      generation, publication, description, origin, roots};
    const segment_immutable_expectation extent{
      covered,
      codec::extent_digest{exact_digest(raw)},
      runtime::file_position{complete.size()}};
    auto config = read_configuration();
    config.admission.working_bytes = byte_count{1_MiB};
    require(
      !(co_await drive.lifecycle(
        writer_type::open_existing(
          files, owner, spec, 0, expected, resources, config, work))),
      "metadata-only open accepted sealed data without independent extent "
      "expectations");
    auto opened = take(
      co_await drive.lifecycle(
        writer_type::open_existing(
          files, owner, spec, 0, expected, resources, config, work, extent)));
    runtime::first_failure failed;
    try {
        require(
          opened->append_state() == model::append_state::sealed
            && !opened->capture(),
          "immutable sparse import acquired append authority");
        auto batch = co_await child(work);
        require(
          !opened->prepare_group(std::span{&batch, 1}, work),
          "relocated or empty immutable generation accepted an ordinary "
          "append");
        {
            const auto read = take(
              co_await drive.lifecycle(opened->read_immutable(
                location.position, reference.bytes(), work)));
            require(
              flat(read.bytes) == root_bytes,
              "immutable reader changed a pinned root");
        }
        take(co_await drive.lifecycle(opened->evict_read_handle()));
        {
            const auto read = take(
              co_await drive.lifecycle(opened->read_immutable(
                location.position, reference.bytes(), work)));
            require(
              flat(read.bytes) == root_bytes,
              "reopened import changed its coverage/pin");
        }
        take(co_await drive.lifecycle(opened->evict_read_handle()));
        // A stale pathname cannot resurrect this owner, even if metadata pins
        // remain valid. This suffix is outside the independently selected EOF.
        co_await store_contract::write_bytes(
          files, data_path, complete + std::string(8192, '\0'), drive);
        auto changed = co_await drive.lifecycle(
          opened->read_immutable(location.position, reference.bytes(), work));
        require(
          !changed && opened->failure().failed() && !opened->capture(),
          "idle reopen accepted an extra unverified extent");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        static_cast<void>(co_await drive.lifecycle(opened->close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    opened.reset();
    take(failed.outcome());
    co_await store_contract::write_bytes(files, data_path, complete, drive);
    auto digest = extent.digest.bytes();
    digest[0] ^= 1;
    auto wrong = extent;
    wrong.digest = codec::extent_digest{digest};
    require(
      !(co_await drive.lifecycle(
        writer_type::open_existing(
          files, owner, spec, 0, expected, resources, config, work, wrong))),
      "immutable open trusted a digest from the bytes instead of the supplied "
      "pin");
    const auto retry_path = take(
      paths.object(0, identity, reference.sequence()));
    take(co_await drive.lifecycle(files.remove_file(retry_path)));
    require(
      !(co_await drive.lifecycle(
        writer_type::open_existing(
          files, owner, spec, 0, expected, resources, config, work, extent))),
      "empty retry summary treated a missing required file as an empty bundle");
    co_await store_contract::write_bytes(files, retry_path, {}, drive);
    if (!raw.empty()) {
        auto damaged = complete;
        // Alter an actual data object, retaining the independently pinned root.
        damaged[8192 + 32 + 144] ^= 1;
        co_await store_contract::write_bytes(files, data_path, damaged, drive);
        require(
          !(co_await drive.lifecycle(
            writer_type::open_existing(
              files,
              owner,
              spec,
              0,
              expected,
              resources,
              config,
              work,
              extent))),
          "metadata pins substituted for complete data verification");
    }
}

// The request at `logical` whose block is exactly `bytes` long.
inline seastar::future<encoded_assigned_batch> block_child(
  std::uint64_t logical, std::size_t bytes, codec::cooperative_work& work) {
    for (std::size_t value = bytes - 420; value <= bytes - 400; ++value)
        if (176 + valued_child(logical, value).size() == bytes)
            co_return co_await valued_execution_child(logical, value, work);
    throw std::runtime_error("no child makes a block of that size");
}

// Appends of every size, where nothing rewrites earlier bytes. Groups of
// one, two and three blocks whose unpadded span ends one byte short of a
// boundary, on it and one byte past it, then single blocks across a sweep
// of sizes. Each group's blocks lie
// back to back from its aligned start, the footer follows the last block and
// pads the span to the next boundary, and the next group begins there: k
// blocks of child size c take ceil((sum of (c + 176) + 248) / A) units. The
// file holds exactly those blocks and independently built footers.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> packed_spans(
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
    auto config = configuration();
    config.admission.working_bytes = byte_count{1_MiB};
    auto writer = take(
      writer_type::make_new(
        files,
        owner,
        spec,
        0,
        descriptor(),
        logical_origin(),
        resources,
        config));
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        const std::uint64_t unit = descriptor().alignment.bytes().value();
        const auto start = writer->progress()->reserved.bytes.value();
        // Each group's record values: blocks of 401 + value bytes.
        std::vector<std::vector<std::size_t>> groups;
        for (std::size_t k = 1; k <= 3; ++k)
            for (const std::uint64_t end : {unit - 1, unit, unit + 1}) {
                const auto values = end - 248 - 401 * k;
                std::vector<std::size_t> group(k, values / k);
                group.back() += values % k;
                groups.push_back(std::move(group));
            }
        for (std::size_t value = 121; value < 8192; value += 509)
            groups.push_back({value});
        written_prefix prefix;
        std::string expected;
        std::uint64_t logical = 100;
        for (const auto& values : groups) {
            std::vector<encoded_assigned_batch> batches;
            std::uint64_t blocks = 0;
            for (const auto value : values) {
                batches.push_back(
                  co_await valued_execution_child(logical++, value, work));
                blocks += 401 + value;
            }
            const auto begin = writer->progress()->reserved.bytes.value();
            auto [cut, bytes] = co_await write_group(
              *writer, std::move(batches), resources, work, drive, prefix);
            require(
              cut.footer() && cut.footer()->begin().value() == begin + blocks
                && cut.end().bytes.value()
                     == begin + (blocks + 248 + unit - 1) / unit * unit
                && bytes.size() == cut.end().bytes.value() - begin,
              "a group's blocks did not pack under one footer padded to the "
              "next boundary");
            expected += bytes;
        }
        take(co_await drive.lifecycle(writer->close()));
        const auto selected = take(local_paths::make(spec.root));
        const auto path = take(selected.segment_file(
          0,
          {descriptor().segment.segment(), descriptor().segment.generation()},
          local_segment_file::data));
        require(
          (co_await store_contract::read_all_bytes(files, path, drive))
              .substr(start)
            == expected,
          "packed groups altered exact block or footer bytes");
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

// Freezes one group of these children at these append times; the times are
// what the range's assigner stamped.
template<typename Writer>
seastar::future<runtime::result<segment_frozen_group>> freeze_at(
  Writer& writer,
  std::vector<encoded_assigned_batch> batches,
  std::span<const std::int64_t> times,
  workload_budget& resources,
  codec::cooperative_work& work) {
    auto prepared = take(writer.prepare_group(std::span{batches}, work));
    require(prepared.prepared.has_value(), "test group could not be prepared");
    std::vector<admitted_wal_batch> children;
    children.reserve(batches.size());
    for (std::size_t i = 0; i < batches.size(); ++i) {
        auto held = take(resources.try_reserve_buffer(batches[i].bytes()));
        children.push_back(take(
          admitted_wal_batch::make(
            std::move(batches[i]),
            std::move(held),
            runtime::wall_time{times[i]},
            charge)));
    }
    co_return co_await writer.freeze_group(
      std::move(*prepared.prepared), std::move(children), work);
}

// Append times never go back along a segment, as a time index never does.
// The writer refuses a batch below the floor its creator gave, below the
// newest of its accepted groups, or below an earlier member of its own
// group; equal times are taken. A refused freeze reserves, writes and fails
// nothing, and the next valid group lies where the refused one would have.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> append_order(
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
    auto config = configuration();
    config.admission.working_bytes = byte_count{1_MiB};
    const auto unfloored = writer_type::make_new(
      files,
      owner,
      spec,
      0,
      descriptor(),
      logical_origin(),
      resources,
      config,
      runtime::wall_time{0});
    require(
      !unfloored && unfloored.error().code() == errc::invalid_argument,
      "a segment was made with an append floor below 1 ns");
    auto writer = take(
      writer_type::make_new(
        files,
        owner,
        spec,
        0,
        descriptor(),
        logical_origin(),
        resources,
        config,
        runtime::wall_time{5}));
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        const auto empty = writer->progress()->reserved;
        std::uint64_t logical = 100;
        written_prefix prefix;
        std::string expected;
        // Refused: below the floor; then a decrease inside one group; then
        // below the newest accepted time. Accepted: at the floor, then an
        // equal time and a later one.
        const auto refuse = [&](
                              std::vector<std::int64_t> times,
                              const char* why) -> seastar::future<> {
            std::vector<encoded_assigned_batch> batches;
            for (std::size_t i = 0; i < times.size(); ++i)
                batches.push_back(co_await execution_child(logical + i, work));
            const auto before = writer->progress()->reserved;
            const auto refused = co_await freeze_at(
              *writer, std::move(batches), times, resources, work);
            require(
              !refused && refused.error().code() == errc::invalid_argument
                && writer->progress()->reserved == before
                && writer->progress()->written == before
                && !writer->failure().failed(),
              why);
        };
        const auto accept =
          [&](std::vector<std::int64_t> times) -> seastar::future<> {
            std::vector<encoded_assigned_batch> batches;
            for (std::size_t i = 0; i < times.size(); ++i) {
                batches.push_back(co_await execution_child(logical++, work));
                prefix.add(batches.back());
            }
            auto group = take(
              co_await freeze_at(
                *writer, std::move(batches), times, resources, work));
            take(co_await writer->encode_group(group, work));
            const auto cut = group.layout().boundary();
            std::string bytes;
            for (const auto& block : group.blocks())
                bytes += flat(block.bytes());
            prefix.checksum.extend(std::span<const char>{bytes});
            const boundary_times stamped{
              runtime::wall_time{5},
              runtime::wall_time{times.back()},
              *prefix.max_record};
            const auto footer = footer_wire(
              {cut.covered(),
               cut.end().blocks,
               group.layout().blocks().back().records,
               stamped,
               prefix.checksum.value()},
              {cut.history(), cut.footer()->begin()});
            prefix.checksum.extend(std::span<const char>{footer});
            bytes += footer;
            auto submitted = take(writer->submit(std::move(group), work));
            take((co_await drive.lifecycle(std::move(submitted.written)))
                   .failure.outcome());
            expected += bytes;
        };
        co_await refuse({4}, "a batch below the segment's floor was frozen");
        require(
          writer->progress()->reserved == empty,
          "a refused freeze reserved coordinates");
        co_await accept({5});
        co_await refuse({7, 6}, "a decrease inside one group was frozen");
        co_await refuse({4}, "a batch below the newest accepted was frozen");
        co_await accept({5, 9});
        co_await refuse({8}, "a batch below a later group's newest was frozen");
        take(co_await drive.lifecycle(writer->close()));
        const auto selected = take(local_paths::make(spec.root));
        const auto path = take(selected.segment_file(
          0,
          {descriptor().segment.segment(), descriptor().segment.generation()},
          local_segment_file::data));
        require(
          (co_await store_contract::read_bytes(files, path, drive))
              .substr(empty.bytes.value())
            == expected,
          "refused freezes left bytes, or accepted groups moved");
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

// Each block states where the segment's durable bytes ended when its group
// froze: the data start before any barrier, then the end of the newest cut a
// barrier completed, never a cut still being made durable. The stamps never
// decrease, and a freeze adds no write or flush.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> certified_stamps(
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
    auto config = configuration();
    config.admission.working_bytes = byte_count{1_MiB};
    auto writer = take(
      writer_type::make_new(
        files,
        owner,
        spec,
        0,
        descriptor(),
        logical_origin(),
        resources,
        config));
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        const auto start = writer->progress()->reserved.bytes;
        std::uint64_t logical = 100;
        // Freezes, encodes and writes one group; returns its cut and the
        // stamp its layout and its block carry.
        const auto group =
          [&]() -> seastar::future<
                  std::pair<segment_captured_boundary, std::uint64_t>> {
            auto frozen = co_await freeze_child(
              *writer,
              co_await execution_child(logical++, work),
              resources,
              work);
            take(co_await writer->encode_group(frozen, work));
            const auto cut = frozen.layout().boundary();
            const auto stamp = frozen.layout().certified_end().value();
            const auto block = flat(frozen.blocks()[0].bytes());
            require(
              get(block, 32 + 128, 8) == stamp,
              "a block did not carry its group's certified end");
            auto submitted = take(writer->submit(std::move(frozen), work));
            take((co_await drive.lifecycle(std::move(submitted.written)))
                   .failure.outcome());
            co_return std::pair{cut, stamp};
        };
        const auto [first, before] = co_await group();
        require(
          before == start.value(), "the first group claimed a certified end");
        take(
          (co_await drive.lifecycle(writer->barrier(first))).failure.outcome());
        const auto [second, after] = co_await group();
        require(
          after == first.end().bytes.value(),
          "a group did not state the barrier that completed before it froze");
        // Frozen while nothing covers the second group: it states the first.
        const auto [third, unchanged] = co_await group();
        require(
          unchanged == first.end().bytes.value(),
          "a group stated bytes no completed barrier covered");
        take(
          (co_await drive.lifecycle(writer->barrier(third))).failure.outcome());
        const auto [fourth, newest] = co_await group();
        require(
          newest == third.end().bytes.value() && newest > unchanged,
          "stamps did not follow the newest completed barrier");
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

// Packed groups at a 4 KiB alignment, a barrier after every other one, then
// a crash that keeps or drops unsynced bytes in 512-byte granules: no write
// after a completed barrier touched a unit it certified, so every byte below
// the last such barrier survives exactly. Needs a backend that can crash.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> certified_spans(
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
    auto description = descriptor();
    description.alignment = alignment(4096);
    auto config = configuration();
    config.admission.working_bytes = byte_count{1_MiB};
    auto writer = take(
      writer_type::make_new(
        files,
        owner,
        spec,
        0,
        description,
        logical_origin(),
        resources,
        config));
    runtime::first_failure failed;
    std::string expected;
    std::uint64_t start = 0, certified = 0;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        start = certified = writer->progress()->reserved.bytes.value();
        written_prefix prefix;
        std::uint64_t logical = 100;
        for (std::size_t value = 121; value < 6000; value += 397) {
            std::vector<encoded_assigned_batch> batches;
            batches.push_back(
              co_await valued_execution_child(logical, value, work));
            auto [cut, bytes] = co_await write_group(
              *writer, std::move(batches), resources, work, drive, prefix);
            expected += bytes;
            if (logical++ % 2 == 0) {
                const auto synced = co_await drive.lifecycle(
                  writer->barrier(cut));
                take(synced.failure.outcome());
                certified = cut.end().bytes.value();
            }
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(co_await drive.lifecycle(files.crash()));
    static_cast<void>(co_await drive.lifecycle(writer->close()));
    writer.reset();
    take(failed.outcome());
    const auto selected = take(local_paths::make(spec.root));
    const auto path = take(selected.segment_file(
      0,
      {description.segment.segment(), description.segment.generation()},
      local_segment_file::data));
    const auto stored = co_await store_contract::read_all_bytes(
      files, path, drive);
    require(
      certified > start && stored.size() >= certified
        && stored.substr(start, certified - start)
             == expected.substr(0, certified - start),
      "a crash damaged bytes a completed barrier certified");
}

// What a segment has left is exact: after a first group, a group of one
// block of exactly that many bytes fits, its footer and the root of its seal
// ending at most at the segment's size, and a block one byte longer must
// roll, for its size. (In an empty segment such a block is impossible: no
// segment of that size could take it.)
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> exact_fit(
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
    auto description = descriptor();
    description.maximum_data_bytes = byte_count{4 * 8192};
    auto config = configuration();
    config.admission.working_bytes = byte_count{1_MiB};
    auto writer = take(
      writer_type::make_new(
        files,
        owner,
        spec,
        0,
        description,
        logical_origin(),
        resources,
        config));
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        written_prefix prefix;
        std::vector<encoded_assigned_batch> first;
        first.push_back(co_await execution_child(100, work));
        static_cast<void>(co_await write_group(
          *writer, std::move(first), resources, work, drive, prefix));
        const auto left = take(writer->left());
        require(
          left.blocks != 0 && left.bytes.value() > 1024,
          "a segment with one group had no room for a block");
        {
            const std::array fits{
              co_await block_child(101, left.bytes.value(), work)};
            const auto taken = take(writer->prepare_group(fits, work));
            require(
              taken.decision == segment_capacity_decision::fits
                && taken.prepared
                && taken.prepared->plan().end->bytes.value() % 8192 == 0
                && taken.prepared->plan().disk.data
                     <= description.maximum_data_bytes,
              "a block of exactly what was left did not fit");
        }
        const std::array over{
          co_await block_child(101, left.bytes.value() + 1, work)};
        const auto refused = take(writer->prepare_group(over, work));
        require(
          refused.decision == segment_capacity_decision::roll_required
            && refused.reasons
                 == segment_roll_reasons{segment_roll_reason::size},
          "a block one byte longer than what was left did not roll for size");
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

// What a preparation says its freeze will take is what the frozen group
// takes: from the reserved end to the end of its footer's padding, for
// groups of 1, 2 and 64 blocks at this alignment.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> prepared_extent(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive,
  std::uint64_t unit) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    auto description = descriptor();
    description.alignment = alignment(unit);
    auto config = configuration();
    config.admission.working_bytes = byte_count{1_MiB};
    auto writer = take(
      writer_type::make_new(
        files,
        owner,
        spec,
        0,
        description,
        logical_origin(),
        resources,
        config));
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        written_prefix prefix;
        std::uint64_t logical = 100;
        for (const std::size_t blocks : {1U, 2U, 64U}) {
            std::vector<encoded_assigned_batch> batches;
            for (std::size_t i = 0; i != blocks; ++i)
                batches.push_back(
                  co_await valued_execution_child(logical++, 200, work));
            const auto before = writer->progress()->reserved.bytes;
            std::optional<model::file_byte_span> planned;
            {
                const auto prepared = take(
                  writer->prepare_group(std::span{batches}, work));
                require(
                  prepared.prepared.has_value(),
                  "a test group could not be prepared");
                planned = prepared.prepared->extent();
            }
            const auto [cut, bytes] = co_await write_group(
              *writer, std::move(batches), resources, work, drive, prefix);
            require(
              planned->begin() == before && planned->end() == cut.end().bytes
                && planned->size().value() == bytes.size()
                && planned->end().value() % unit == 0,
              "a preparation's extent is not what its group took");
        }
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

// One group of one child, frozen to be held up to `hold`, encoded and
// submitted: its cut, its submission, its block's records, and its bytes
// with the footer after them when it has one, whose coverage, CRC and times
// are those of every byte before it.
struct held_submission final {
    segment_captured_boundary cut;
    segment_submission submitted;
    std::string bytes;
    storage::coverage block;
};
template<typename Writer>
seastar::future<held_submission> submit_held(
  Writer& writer,
  encoded_assigned_batch batch,
  byte_count hold,
  workload_budget& resources,
  codec::cooperative_work& work,
  written_prefix& prefix) {
    auto prepared = take(writer.prepare_group(std::span{&batch, 1}, work));
    require(prepared.prepared.has_value(), "test group could not be prepared");
    prefix.add(batch);
    auto held = take(resources.try_reserve_buffer(batch.bytes()));
    std::vector<admitted_wal_batch> children;
    children.push_back(take(
      admitted_wal_batch::make(
        std::move(batch), std::move(held), child_append_time, charge)));
    auto group = take(
      co_await writer.freeze_group(
        std::move(*prepared.prepared), std::move(children), work, hold));
    take(co_await writer.encode_group(group, work));
    const auto cut = group.layout().boundary();
    auto bytes = flat(group.blocks()[0].bytes());
    prefix.checksum.extend(std::span<const char>{bytes});
    if (cut.footer()) {
        const auto footer = footer_wire(
          {cut.covered(),
           cut.end().blocks,
           group.layout().blocks().back().records,
           appended_times(prefix.max_record),
           prefix.checksum.value()},
          {cut.history(), cut.footer()->begin()});
        prefix.checksum.extend(std::span<const char>{footer});
        bytes += footer;
    }
    const auto records = group.layout().blocks().back().records;
    auto submitted = take(writer.submit(std::move(group), work));
    co_return held_submission{
      cut, std::move(submitted), std::move(bytes), records};
}
// The footer that closes a run held open at `held`'s end, whose last block
// is `last`.
inline std::string closing_wire(
  const segment_captured_boundary& held,
  const storage::coverage& last,
  written_prefix& prefix) {
    const auto footer = footer_wire(
      {held.covered(),
       held.end().blocks,
       last,
       appended_times(prefix.max_record),
       prefix.checksum.value()},
      {held.history(), held.end().bytes});
    prefix.checksum.extend(std::span<const char>{footer});
    return footer;
}

// A run of held groups. With a hold of two units, each 3000-byte block joins
// the run held open at the reserved end with no footer of its own: it begins
// where the last one ended, nothing is written, and its write completes with
// nothing written once its bytes are kept. The sixth takes the run past the
// hold and ends with one footer covering all six, and the run is then one
// write from the run's start to that footer's padding: k blocks of b bytes
// take ceil((k b + 248) / A) units. A barrier refuses a held group's cut,
// which has no footer. What the owner reports moves the writer's reported
// position and is told to its observer, never back, and a reader's pin is
// given the written and reported ends apart. Each group's bytes are handed
// to whoever reads behind the writer while they are only in memory, and are
// the bytes the device holds once written: a read before the write and one
// after it see the same bytes. `run`, when given, receives the run's span,
// for a caller that sees the device's writes.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> held_runs(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive,
  std::optional<model::file_byte_span>* run = nullptr) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    auto config = configuration();
    config.admission.working_bytes = byte_count{1_MiB};
    auto writer = take(
      writer_type::make_new(
        files,
        owner,
        spec,
        0,
        descriptor(),
        logical_origin(),
        resources,
        config));
    std::vector<storage::coverage> blocks;
    std::vector<runtime::file_position> written, durable, reported;
    blocks.reserve(8);
    written.reserve(8);
    durable.reserve(8);
    reported.reserve(8);
    // What was fed, from where: kept as strings so nothing of the writer's
    // stays shared.
    std::string fed;
    std::optional<runtime::file_position> fed_from;
    bool wanted = true;
    segment_block_observer observer{
      [&blocks](std::span<const segment_block_layout> told) noexcept {
          for (const auto& block : told)
              blocks.push_back(block.records);
      },
      [&durable](runtime::file_position end) noexcept {
          durable.push_back(end);
      },
      [&fed, &fed_from](
        runtime::file_position at,
        bytes::fragmented_buffer& bytes,
        const workload_reservation&) noexcept {
          if (!fed_from) fed_from = at;
          try {
              for (auto fragment : bytes)
                  fed.append(fragment.data(), fragment.size());
          } catch (...) {
              fed.clear();
          }
      },
      &wanted};
    observer.written = [&written](runtime::file_position end) noexcept {
        written.push_back(end);
    };
    observer.reported = [&reported](runtime::file_position end) noexcept {
        reported.push_back(end);
    };
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        take(writer->observe_blocks(std::move(observer)));
        const std::uint64_t unit = descriptor().alignment.bytes().value();
        const byte_count hold{2 * unit};
        constexpr std::uint64_t block = 3000;
        const auto start = writer->progress()->reserved;
        const auto selected = take(local_paths::make(spec.root));
        const auto path = take(selected.segment_file(
          0,
          {descriptor().segment.segment(), descriptor().segment.generation()},
          local_segment_file::data));
        written_prefix prefix;
        std::string expected;
        std::vector<segment_captured_boundary> cuts;
        for (std::uint64_t i = 0; i != 5; ++i) {
            auto [cut, submitted, bytes, records] = co_await submit_held(
              *writer,
              co_await block_child(100 + i, block, work),
              hold,
              resources,
              work,
              prefix);
            const auto done = co_await drive.lifecycle(
              std::move(submitted.written));
            take(done.failure.outcome());
            require(
              !cut.footer()
                && cut.end().bytes.value()
                     == start.bytes.value() + (i + 1) * block
                && done.written.value() == 0
                && writer->progress()->written == start
                && writer->held().value() == (i + 1) * block,
              "a held group was written, took a footer or left a hole");
            expected += bytes;
            cuts.push_back(cut);
        }
        require(
          take(co_await drive.lifecycle(files.stat(path))).size.value()
            == start.bytes.value(),
          "a held run reached the file before its footer");
        const auto refused = co_await drive.lifecycle(
          writer->barrier(cuts.back()));
        require(
          !refused.receipt && refused.failure.error()
            && refused.failure.error()->code() == errc::invalid_argument
            && !writer->failure().failed(),
          "a barrier took a held group's cut, which has no footer");
        take(writer->report(cuts[2]));
        take(writer->report(cuts[1]));
        require(
          writer->progress()->reported.bytes == cuts[2].end().bytes
            && reported.size() == 1 && reported[0] == cuts[2].end().bytes,
          "the reported position did not follow the owner or moved back");
        {
            const auto view = take(
              co_await drive.lifecycle(writer->pin_data(work)));
            require(
              view.durable == start.bytes && view.written == start.bytes
                && view.reported == cuts[2].end().bytes,
              "a reader was not given the written and reported ends apart");
        }
        auto [cut, submitted, bytes, records] = co_await submit_held(
          *writer,
          co_await block_child(105, block, work),
          hold,
          resources,
          work,
          prefix);
        expected += bytes;
        const auto done = co_await drive.lifecycle(
          std::move(submitted.written));
        take(done.failure.outcome());
        const auto end = start.bytes.value()
                         + (6 * block + 248 + unit - 1) / unit * unit;
        require(
          cut.footer()
            && cut.footer()->begin().value() == start.bytes.value() + 6 * block
            && cut.end().bytes.value() == end
            && done.written.value() == end - start.bytes.value()
            && writer->progress()->written == cut.end()
            && writer->held().value() == 0 && !written.empty()
            && written.back() == cut.end().bytes && blocks.size() == 6,
          "the group that reached the hold did not close the run and write "
          "it whole");
        if (run)
            *run = model::file_byte_span::make(start.bytes, cut.end().bytes)
                     .value();
        const auto synced = co_await drive.lifecycle(writer->barrier(cut));
        take(synced.failure.outcome());
        require(
          synced.receipt && writer->progress()->durable == cut.end()
            && durable.size() == 1 && durable[0] == cut.end().bytes,
          "a barrier on the run's footer did not make the run durable");
        take(co_await drive.lifecycle(writer->close()));
        require(
          (co_await store_contract::read_all_bytes(files, path, drive))
              .substr(start.bytes.value())
            == expected,
          "a held run's bytes are not its blocks back to back under one "
          "footer");
        require(
          fed_from == start.bytes && fed == expected,
          "the bytes fed while held are not the bytes the device holds");
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

// A held run that closes while an extension of the zero-written range is
// still writing its zeros. The run lies inside the range, so it is written
// through the synchronized handle beside the extension, not after the
// extension's zeros and their flush: only writes past the extension's start
// wait for it. The run is written whole, a barrier on its footer makes it
// durable, and the stored bytes are the run's with zeros after them. `run`,
// when given, receives the run's span, for a caller that sees the device's
// writes and the order they were submitted in.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> held_run_beside_extension(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& resources,
  Driver drive,
  std::optional<model::file_byte_span>* run = nullptr) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    // A first window that holds the run, and a step of many writes, so that
    // the extension the writer starts at once is still writing when the run
    // closes.
    constexpr std::uint64_t window = 64_KiB;
    constexpr std::uint64_t step = 4_MiB;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation::bootstrap(
      files, owner, spec, resources, work, drive);
    auto config = configuration();
    config.admission.working_bytes = byte_count{1_MiB};
    config.preallocation_bytes = byte_count{window};
    config.preallocation_extension_bytes = byte_count{step};
    config.synchronous_write_bytes = byte_count{window};
    auto writer = take(
      writer_type::make_new(
        files,
        owner,
        spec,
        0,
        descriptor(),
        logical_origin(),
        resources,
        config));
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        const std::uint64_t unit = descriptor().alignment.bytes().value();
        const byte_count hold{2 * unit};
        constexpr std::uint64_t block = 3000;
        const auto start = writer->progress()->reserved;
        const auto selected = take(local_paths::make(spec.root));
        const auto path = take(selected.segment_file(
          0,
          {descriptor().segment.segment(), descriptor().segment.generation()},
          local_segment_file::data));
        written_prefix prefix;
        std::string expected;
        for (std::uint64_t i = 0; i != 5; ++i) {
            auto [cut, submitted, bytes, records] = co_await submit_held(
              *writer,
              co_await block_child(100 + i, block, work),
              hold,
              resources,
              work,
              prefix);
            const auto done = co_await drive.lifecycle(
              std::move(submitted.written));
            take(done.failure.outcome());
            expected += bytes;
        }
        auto [cut, submitted, bytes, records] = co_await submit_held(
          *writer,
          co_await block_child(105, block, work),
          hold,
          resources,
          work,
          prefix);
        expected += bytes;
        const auto done = co_await drive.lifecycle(
          std::move(submitted.written));
        take(done.failure.outcome());
        const auto end = start.bytes.value()
                         + (6 * block + 248 + unit - 1) / unit * unit;
        require(
          cut.footer() && cut.end().bytes.value() == end
            && end - start.bytes.value() <= window
            && done.written.value() == end - start.bytes.value()
            && writer->progress()->written == cut.end(),
          "the run that closed beside an extension was not written whole");
        if (run)
            *run = model::file_byte_span::make(start.bytes, cut.end().bytes)
                     .value();
        const auto synced = co_await drive.lifecycle(writer->barrier(cut));
        take(synced.failure.outcome());
        require(
          synced.receipt && writer->progress()->durable == cut.end(),
          "a barrier on the run's footer did not make it durable");
        take(co_await drive.lifecycle(writer->close()));
        const auto stored = co_await store_contract::read_all_bytes(
          files, path, drive);
        require(
          stored.size() >= start.bytes.value() + expected.size()
            && stored.substr(start.bytes.value(), expected.size()) == expected
            && stored.find_first_not_of(
                 '\0', start.bytes.value() + expected.size())
                 == std::string::npos,
          "a run written beside an extension is not its blocks under one "
          "footer with zeros after them");
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

// The ways a held run closes before its hold. A group that wants durability
// now, behind a held run, is frozen after a footer of the run's own, which
// its barrier covers with it. close_run() closes the run with a footer a
// barrier may take, and changes nothing with no run open. Held groups leave
// the writer's queue once kept, so a bound of two groups in flight never
// refuses a run of four. The writer's close writes the run it holds, closed
// by its own footer.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> held_closes(
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
    auto config = configuration();
    config.admission.working_bytes = byte_count{1_MiB};
    config.maximum_groups = 2;
    auto writer = take(
      writer_type::make_new(
        files,
        owner,
        spec,
        0,
        descriptor(),
        logical_origin(),
        resources,
        config));
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        const std::uint64_t unit = descriptor().alignment.bytes().value();
        const byte_count hold{4 * unit};
        constexpr std::uint64_t block = 3000;
        const auto start = writer->progress()->reserved;
        const auto selected = take(local_paths::make(spec.root));
        const auto path = take(selected.segment_file(
          0,
          {descriptor().segment.segment(), descriptor().segment.generation()},
          local_segment_file::data));
        written_prefix prefix;
        std::string expected;
        const auto keep = [&](std::uint64_t logical)
          -> seastar::future<
            std::pair<segment_captured_boundary, storage::coverage>> {
            auto [cut, submitted, bytes, records] = co_await submit_held(
              *writer,
              co_await block_child(logical, block, work),
              hold,
              resources,
              work,
              prefix);
            const auto done = co_await drive.lifecycle(
              std::move(submitted.written));
            take(done.failure.outcome());
            require(
              !cut.footer() && done.written.value() == 0,
              "a group below its hold was not held");
            expected += bytes;
            co_return std::pair{cut, records};
        };
        // A held group, then one held for nothing.
        {
            const auto [held, last] = co_await keep(100);
            auto batch = co_await block_child(101, block, work);
            auto prepared = take(
              writer->prepare_group(std::span{&batch, 1}, work));
            require(
              prepared.prepared.has_value()
                && prepared.prepared->extent().begin() == held.end().bytes,
              "a group behind a held run was not planned from its end");
            expected += closing_wire(held, last, prefix);
            prefix.add(batch);
            auto charged = take(resources.try_reserve_buffer(batch.bytes()));
            std::vector<admitted_wal_batch> children;
            children.push_back(take(
              admitted_wal_batch::make(
                std::move(batch),
                std::move(charged),
                child_append_time,
                charge)));
            auto group = take(
              co_await writer->freeze_group(
                std::move(*prepared.prepared), std::move(children), work));
            take(co_await writer->encode_group(group, work));
            const auto cut = group.layout().boundary();
            const auto closed = (held.end().bytes.value() + 248 + unit - 1)
                                / unit * unit;
            auto bytes = flat(group.blocks()[0].bytes());
            prefix.checksum.extend(std::span<const char>{bytes});
            bytes += footer_wire(
              {cut.covered(),
               cut.end().blocks,
               group.layout().blocks().back().records,
               appended_times(prefix.max_record),
               prefix.checksum.value()},
              {cut.history(), cut.footer()->begin()});
            prefix.checksum.extend(std::span<const char>{bytes}.subspan(block));
            expected += bytes;
            require(
              group.layout().blocks()[0].records.bytes().begin().value()
                  == closed
                && cut.footer() && cut.end().footers == 2
                && cut.end().bytes == prepared.prepared->plan().end->bytes,
              "a group wanted now did not follow its run's own footer");
            auto submitted = take(writer->submit(std::move(group), work));
            take((co_await drive.lifecycle(std::move(submitted.written)))
                   .failure.outcome());
            const auto synced = co_await drive.lifecycle(writer->barrier(cut));
            take(synced.failure.outcome());
            require(
              synced.receipt && writer->progress()->durable == cut.end(),
              "the barrier of a group wanted now did not cover the run "
              "before it");
        }
        // close_run() on a held group, and again with nothing held.
        {
            const auto [held, last] = co_await keep(102);
            const auto closed = take(writer->close_run());
            expected += closing_wire(held, last, prefix);
            require(
              closed.footer() && closed.footer()->begin() == held.end().bytes
                && closed.end().footers == held.end().footers + 1
                && writer->held().value() == 0,
              "close_run() did not close the run with its own footer");
            const auto synced = co_await drive.lifecycle(
              writer->barrier(closed));
            take(synced.failure.outcome());
            require(
              synced.receipt && writer->progress()->durable == closed.end()
                && take(writer->close_run()) == closed,
              "a closed run was not made durable by its footer, or a close "
              "with nothing held changed something");
        }
        // Four held groups under a bound of two in flight; the close writes
        // them with their own footer.
        std::optional<std::pair<segment_captured_boundary, storage::coverage>>
          last;
        for (std::uint64_t logical = 103; logical != 107; ++logical)
            last = co_await keep(logical);
        require(
          writer->held().value() == 4 * block,
          "held groups counted against the groups in flight");
        expected += closing_wire(last->first, last->second, prefix);
        take(co_await drive.lifecycle(writer->close()));
        require(
          (co_await store_contract::read_all_bytes(files, path, drive))
              .substr(start.bytes.value())
            == expected,
          "a run held at close was not written with its own footer");
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

} // namespace kwaque::storage::testing::segment_writer_contract
