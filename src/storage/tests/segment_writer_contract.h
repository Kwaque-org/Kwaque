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

inline segment_writer_config configuration() {
    return {local_object_sequence::make(45).value(), store_contract::limits()};
}
inline segment_writer_config read_configuration() {
    return {std::nullopt, store_contract::limits()};
}
inline local_segment_descriptor descriptor() {
    auto value = installation::descriptor();
    value.alignment = alignment(8192);
    return value;
}
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
          files, owner, spec, 0, wide, resources, narrow);
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
        const auto header = segment_header::make(
                              partial.segment,
                              partial.logical_origin,
                              partial.alignment)
                              .value();
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
        files, owner, spec, 0, description, resources, configuration()));
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
            files, owner, spec, 0, description, resources, configuration()));
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
        const auto header = segment_header::make(
                              extended.segment,
                              extended.logical_origin,
                              extended.alignment)
                              .value();
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
      segment_header::make(
        description.segment, description.logical_origin, description.alignment)
        .value());
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
// first; with one credit fewer it is refused for handle pressure.
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
      writer_type::make_new(files, owner, spec, 0, first, resources, config));
    std::unique_ptr<writer_type> starved;
    std::optional<workload_reservation> one;
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(created->create_new(work)));
        require(
          resources.snapshot().handles == limit - segment_creation_handles,
          "an open segment does not hold the handle credits it is counted "
          "for");
        one.emplace(take(resources.try_reserve(byte_count{1})));
        take(one->try_acquire_handles(1));
        starved = take(
          writer_type::make_new(
            files, owner, spec, 0, second, resources, config));
        const auto refused = co_await drive.lifecycle(
          starved->create_new(work));
        require(
          !refused && refused.error().code() == errc::queue_full,
          "a creation one handle credit short was not refused for it");
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
        files, owner, spec, 0, descriptor(), resources, configuration()));
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        const auto before = take(writer->capture());
        const std::array input{co_await child(work)};
        auto backing = take(resources.try_reserve_buffer(input[0].bytes()));
        const auto initial = resources.snapshot();
        {
            auto prepared = take(writer->prepare_group(input, work));
            require(
              prepared.decision == segment_capacity_decision::fits
                && prepared.prepared
                && prepared.prepared->plan().end->bytes.value() == 24576
                && prepared.prepared->plan().disk.data.value() == 32768
                && prepared.prepared->plan().end->retry_entries == 1,
              "group omitted its footer, root or complete-request obligation");
            require(
              take(writer->capture()) == before,
              "rejectable preparation froze coordinates");
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
    const auto ref = local_root_reference::make(
                       local_root_kind::sealed_retry,
                       local_object_sequence::make(45).value(),
                       runtime::file_position{16384},
                       byte_count{4_KiB},
                       page_count::make(1).value(),
                       installation::literal_digest(
                         "23f26dfc1ea13edbe500c410124c229c"))
                       .value();
    const footer_expectation context{
      {installation::segment(),
       alignment(4096),
       runtime::file_position{4096},
       model::range_logical_end{100},
       model::segment_relative_end{}},
      runtime::file_position{16384}};
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
            data_path,
            {.access = runtime::file_access::read_write,
             .close_policy = runtime::file_close_policy::checked}))));
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
            auto opened = co_await drive.lifecycle(files.open_directory(
              spec.root, runtime::file_close_policy::checked));
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
            data->write(runtime::file_position{16384}, std::move(sealed))));
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
    const auto block = data_block(logical, 0, 512, false, 0x30, 1, true);
    auto wire = block.substr(32 + 120, get(block, 32 + 112, 4));
    auto raw = co_await installation::buffer_async(std::move(wire));
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
      admitted_wal_batch::make(std::move(batch), std::move(held), charge)));
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
        files, owner, spec, 0, descriptor(), resources, config));
    runtime::first_failure failed;
    try {
        take(co_await drive.lifecycle(writer->create_new(work)));
        const auto empty = writer->progress()->durable;
        take(writer->observe_blocks(trace.observer()));
        const auto twice = writer->observe_blocks(trace.observer());
        require(
          !twice && twice.error().code() == errc::wrong_context,
          "a second block observer replaced the first");
        auto first = co_await freeze_child(
          *writer, co_await child(work, 4096), resources, work);
        const auto first_cut = first.layout().boundary();
        auto second = co_await freeze_child(
          *writer, co_await execution_child(101, work), resources, work);
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
                std::move(next), std::move(held), charge)));
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
           crc(first_block)},
          {first_cut.history(), first_cut.footer()->begin()});
        const auto first_bytes = first_block + first_footer;
        const auto second_footer = footer_wire(
          {second_cut.covered(),
           second_cut.end().blocks,
           second.layout().blocks().back().records,
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
        files, owner, spec, 0, descriptor(), resources, config));
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
        for (auto& batch : batches) {
            auto held = take(resources.try_reserve_buffer(batch.bytes()));
            children.push_back(take(
              admitted_wal_batch::make(
                std::move(batch), std::move(held), charge)));
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
          {cut.covered(), cut.end().blocks, layout.back().records, crc(blocks)},
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

// Freezes, encodes and submits one group without awaiting its write.
// Returns the submission and exact stored bytes; the running CRC covers every
// earlier byte.
template<typename Writer>
seastar::future<std::pair<segment_submission, std::string>> submit_group(
  Writer& writer,
  std::vector<encoded_assigned_batch> batches,
  workload_budget& resources,
  codec::cooperative_work& work,
  codec::crc32c& checksum) {
    auto prepared = take(writer.prepare_group(std::span{batches}, work));
    require(prepared.prepared.has_value(), "test group could not be prepared");
    std::vector<admitted_wal_batch> children;
    children.reserve(batches.size());
    for (auto& batch : batches) {
        auto held = take(resources.try_reserve_buffer(batch.bytes()));
        children.push_back(take(
          admitted_wal_batch::make(std::move(batch), std::move(held), charge)));
    }
    auto group = take(
      co_await writer.freeze_group(
        std::move(*prepared.prepared), std::move(children), work));
    take(co_await writer.encode_group(group, work));
    const auto cut = group.layout().boundary();
    std::string bytes;
    for (const auto& block : group.blocks())
        bytes += flat(block.bytes());
    checksum.extend(std::span<const char>{bytes});
    const auto footer = footer_wire(
      {cut.covered(),
       cut.end().blocks,
       group.layout().blocks().back().records,
       checksum.value()},
      {cut.history(), cut.footer()->begin()});
    checksum.extend(std::span<const char>{footer});
    bytes += footer;
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
  codec::crc32c& checksum) {
    auto [submitted, bytes] = co_await submit_group(
      writer, std::move(batches), resources, work, checksum);
    auto written = co_await drive.lifecycle(std::move(submitted.written));
    take(written.failure.outcome());
    co_return std::pair{submitted.boundary, std::move(bytes)};
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
    constexpr std::uint64_t window = 48_KiB;
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
        files, owner, spec, 0, descriptor(), resources, config));
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
        codec::crc32c checksum;
        std::vector<encoded_assigned_batch> first;
        first.push_back(co_await child(work));
        first.push_back(co_await execution_child(101, work));
        auto [ordinary, expected] = co_await write_group(
          *writer, std::move(first), resources, work, drive, checksum);
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
          *writer, std::move(second), resources, work, drive, checksum);
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
              *writer, std::move(third), resources, work, drive, checksum);
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
        files, owner, spec, 0, descriptor(), resources, config));
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
        codec::crc32c checksum;
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
              *writer, std::move(batches), resources, work, checksum);
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
    constexpr std::uint32_t groups = 5;
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
        files, owner, spec, 0, descriptor(), resources, config));
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
        codec::crc32c checksum;
        std::string expected;
        std::optional<segment_captured_boundary> last;
        for (std::uint32_t i = 0; i < groups; ++i) {
            std::vector<encoded_assigned_batch> batches;
            if (i == 0)
                batches.push_back(co_await child(work));
            else
                batches.push_back(co_await execution_child(100 + i, work));
            auto [cut, bytes] = co_await write_group(
              *writer, std::move(batches), resources, work, drive, checksum);
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
        files, owner, spec, 0, descriptor(), resources, configuration()));
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
  bool unresolved = false) {
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
        files, owner, spec, 0, descriptor(), resources, config));
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
        if (!empty) {
            auto batch = co_await child(work);
            const auto info = batch.info();
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
              {cut.covered(), cut.end().blocks, last, crc(extent_bytes)},
              {cut.history(), cut.footer()->begin()});
            auto submitted = take(writer->submit(std::move(group), work));
            const auto result = co_await drive.lifecycle(
              std::move(submitted.written));
            take(result.failure.outcome());
        }
        const auto incomplete = co_await drive.lifecycle(writer->seal(
          completed_source{{}, {}, &calls}, empty ? 1U : 0U, 0, work));
        require(
          incomplete.failure.error()
            && incomplete.failure.error()->code() == errc::invalid_argument
            && !writer->failure().failed()
            && writer->append_state() == model::append_state::active
            && calls == 0,
          "incomplete completion snapshot changed admission or discharged "
          "obligations");
        require(
          trace.durable.empty()
            && (empty ? trace.written.empty()
                      : trace.written.size() == 1 && trace.written[0] == *last),
          "a refused seal reported durable blocks, or the written block "
          "was not reported");
        if (unresolved) facts.clear();
        const auto count = static_cast<std::uint32_t>(facts.size());
        auto source_grant = take(resources.try_reserve(byte_count{4_KiB}));
        const auto before = resources.snapshot();
        if (saturated) resources.close_admission();
        auto sealed = co_await drive.lifecycle(writer->seal(
          completed_source{
            std::move(source_grant), std::move(facts), &calls, changed_source},
          count,
          unresolved ? 1U : 0U,
          work));
        auto repeated = co_await drive.lifecycle(
          writer->seal(completed_source{{}, {}, &calls}, 0, 0, work));
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
          calls == (empty || unresolved ? 0U : 2U),
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
            require(
              sealed.unresolved == (unresolved ? 1U : 0U),
              "seal discharged an unresolved completion obligation");
            if (!empty && !unresolved) {
                const std::array entries{retry()};
                page = retry_page_wire(entries, location);
                refs.push_back(reference(page));
            }
            const auto expected_root = sealed_wire(
              {sealed.extent->coverage, empty ? 0U : 1U, last, 0},
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
    description.logical_origin = model::range_logical_end{
      kind == immutable_import_kind::removed_terminal ? UINT64_MAX - 10
      : terminal                                      ? UINT64_MAX
                                                      : 100};
    const auto head = segment_header::make(
                        description.segment,
                        description.logical_origin,
                        description.alignment)
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
      description.logical_origin,
      description.physical_origin};
    const auto raw = [&] {
        if (!sparse && !dense) return std::string{};
        const auto fixture = data_block(
          sparse ? 102 : 104, 0, 8192, sparse, 0x80, 2, false, 8192);
        const auto header_bytes = get(fixture, 10, 2);
        // Retain the exact original child; only its current placement belongs
        // to this store's independently supplied segment context.
        return block_wire(
          fixture.substr(
            header_bytes + segment_block_fixed_bytes.value(),
            get(fixture, header_bytes + 112, 4)),
          {segment_write_context::make(
             description.segment,
             description.alignment,
             description.physical_origin,
             history.data_start)
             .value(),
           history.data_start,
           batch_expected(),
           description.profile});
    }();
    const auto logical_end = terminal ? UINT64_MAX : initial ? 100U : 110U;
    const auto covered = scope(
      description.logical_origin.value(),
      logical_end,
      0,
      sparse  ? 2
      : dense ? 1
              : 0,
      8192,
      8192 + raw.size());
    std::optional<storage::coverage> last;
    if (sparse || dense)
        last = scope(
          sparse ? 102 : 104,
          sparse ? 107 : 105,
          0,
          sparse ? 2 : 1,
          8192,
          8192 + raw.size());
    const footer_expectation location{history, covered.bytes().end()};
    const auto root_bytes = sealed_wire(
      {covered, last ? 1U : 0U, last, 0}, exact_digest(raw), {}, location);
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
      generation, publication, description, roots};
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
        damaged[8192 + 32 + 120] ^= 1;
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

} // namespace kwaque::storage::testing::segment_writer_contract
