#pragma once

#include "src/storage/local_cleanup.h"
#include "src/storage/local_generation.h"
#include "src/storage/tests/local_installation_contract.h"
#include "src/storage/tests/local_metadata_fixture_cases.h"

namespace kwaque::storage::testing::reader_contract {
using installation_contract::buffer_async;
using installation_contract::descriptor;
using installation_contract::limits;
using installation_contract::literal_digest;
using installation_contract::require;
using installation_contract::segment;
using installation_contract::segment_head;
using installation_contract::take;
using store_contract::read_bytes;
using store_contract::write_bytes;

inline const local_fixture_case& fixture_case(std::string_view name) {
    for (const auto& value : local_fixture_cases)
        if (value.name == name) return value;
    throw std::runtime_error("missing fixture context");
}
inline local_metadata_expectation
record_expectation(const local_device_spec& spec, std::string_view name) {
    const auto& test = fixture_case(name);
    local_metadata_expectation expected{
      local_metadata_header::make(
        static_cast<local_metadata_kind>(test.kind),
        test.kind == 1 ? spec.owner : spec.shard_owner(0).value(),
        local_publication_generation::make(test.generation).value())
        .value(),
      alignment(4096)};
    if (!test.segment.empty()) {
        expected.segment = segment();
        expected.segment_alignment = alignment(4096);
    }
    if (
      test.kind == 6 || test.kind == 9 || test.kind == 10 || test.kind == 11) {
        expected.digest = literal_digest(test.digest);
        expected.encoded_bytes = byte_count{test.bytes};
    }
    if (test.data_device) {
        expected.data_device = id<device_store_id>(test.data_device);
        expected.data_metadata_alignment = alignment(4096);
    }
    return expected;
}
inline local_root_reference snapshot_ref() {
    return take(
      local_root_reference::make(
        local_root_kind::completed_retry_snapshot,
        local_object_sequence::make(40).value(),
        runtime::file_position{},
        byte_count{4096},
        page_count::make(1).value(),
        literal_digest(
          "9dbc526ee010ac9e12206badf2cf00cef7880702a3492879040f509f77bba73c")));
}
inline local_root_reference sealed_ref() {
    return take(
      local_root_reference::make(
        local_root_kind::sealed_retry,
        local_object_sequence::make(41).value(),
        runtime::file_position{16384},
        byte_count{4096},
        page_count::make(1).value(),
        literal_digest(
          "96975545e00630e51ef3905c0d3694a44801cafd3f2b4875ab926668a374b7ff")));
}
inline local_root_reference index_ref() {
    return take(
      local_root_reference::make(
        local_root_kind::index,
        local_object_sequence::make(42).value(),
        runtime::file_position{},
        byte_count{4096},
        page_count::make(1).value(),
        literal_digest(
          "c51e48c670a584eae798cac6853d4776806f6839b2858287e100427d8816b3e1")));
}
inline segment_history_context history() {
    return {
      segment(),
      alignment(4096),
      runtime::file_position{4096},
      model::range_logical_end{100},
      model::segment_relative_end{0}};
}
inline local_footer_reference active_boundary() {
    return take(
      local_footer_reference::make(
        runtime::file_position{12288},
        byte_count{4096},
        6,
        literal_digest(
          "1f352213da8c69e284cbc1bfc3355fe31efcb08fa102183422233875318b3e51")));
}
inline local_footer_reference sealed_boundary() {
    return take(
      local_footer_reference::make(
        sealed_ref().position(),
        sealed_ref().bytes(),
        7,
        sealed_ref().digest()));
}
inline local_object_publication active_publication() {
    return {segment(), local_object_state::active, active_boundary(), {}};
}
inline local_object_publication sealed_publication() {
    return {
      segment(),
      local_object_state::sealed,
      sealed_boundary(),
      {index_ref(), sealed_ref(), snapshot_ref()}};
}
inline std::array<local_bundle_context, 3>
contexts(const local_device_spec& spec) {
    auto index = sparse_index_context::make(
                   segment(),
                   scope(100, 102, 0, 2, 4096, 16384),
                   codec::extent_digest{literal_digest(
                                          "dd3a755a09e37d0e6235b7dec4edc53d1b59"
                                          "0a447deee72491a366cf63ce9d9e")
                                          .bytes()},
                   alignment(4096))
                   .value();
    return {
      index,
      footer_expectation{history(), sealed_ref().position()},
      record_expectation(spec, "snapshot_root")};
}
template<typename Backend, typename Owner, typename Driver>
seastar::future<> seed_segment(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  codec::cooperative_work& work,
  Driver drive) {
    co_await installation_contract::bootstrap(
      files, owner, spec, budget, work, drive);
    {
        auto result = co_await drive.lifecycle(publish_local_segment_descriptor(
          files,
          owner,
          spec,
          0,
          descriptor(),
          segment_head(),
          budget,
          limits(),
          work));
        take(result.failure.outcome());
    }
    const auto paths = take(local_paths::make(spec.root));
    const local_segment_name name{segment().segment(), segment().generation()};
    co_await write_bytes(
      files,
      take(paths.segment_file(0, name, local_segment_file::data)),
      local_fixture::read("data_header_a") + local_fixture::read("block_a1")
        + local_fixture::read("block_a2") + local_fixture::read("footer_a")
        + local_fixture::read("sealed_a"),
      drive);
    co_await write_bytes(
      files,
      take(paths.object(0, name, snapshot_ref().sequence())),
      local_fixture::read("snapshot_root")
        + local_fixture::read("snapshot_page"),
      drive);
    co_await write_bytes(
      files,
      take(paths.object(0, name, sealed_ref().sequence())),
      local_fixture::read("sealed_retry_page"),
      drive);
    co_await write_bytes(
      files,
      take(paths.object(0, name, index_ref().sequence())),
      local_fixture::read("index_root") + local_fixture::read("index_page"),
      drive);
    co_await write_bytes(
      files,
      take(paths.segment_file(0, name, local_segment_file::published)),
      local_fixture::read("publication_boundary"),
      drive);
}
template<typename Backend, typename Owner, typename Driver>
seastar::future<> seed_checkpoint(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  codec::cooperative_work& work,
  Driver drive) {
    co_await installation_contract::bootstrap(
      files, owner, spec, budget, work, drive);
    const auto path = take(
      take(local_paths::make(spec.root))
        .sequence_file(0, local_sequence_file::checkpoint, 70));
    co_await write_bytes(
      files,
      path,
      local_fixture::read("checkpoint_root")
        + local_fixture::read("checkpoint_page"),
      drive);
}
template<typename Backend, typename Owner, typename Driver>
seastar::future<> root_lifetime(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await seed_checkpoint(files, owner, spec, budget, work, drive);
    auto root = take(
      co_await drive.lifecycle(
        local_root_owner::open(
          files,
          owner,
          spec,
          0,
          installation_contract::checkpoint_reference(),
          installation_contract::checkpoint_expectation(spec),
          budget,
          limits(),
          work,
          {.maximum_pins = 2})));
    std::optional<local_root_pin> pin;
    pin.emplace(take(root->pin()));
    std::optional<local_root_page> page;
    runtime::first_failure failed;
    root->retire();
    auto closing = root->close();
    try {
        require(!root->pin(), "retired root admitted reader");
        require(!closing.available(), "close ignored parked root pin");
        page.emplace(take(
          co_await drive.lifecycle(
            pin->read(page_ordinal::make(0).value(), work))));
        require(
          flat(page->bytes) == local_fixture::read("checkpoint_page"),
          "pinned page bytes changed");
        require(
          page->position.value() == 4096
            && pin->position(page_ordinal::make(0).value())->value() == 4096,
          "derived page position changed");
        require(!pin->share(), "pin bound ignored");
        require(
          std::get<local_checkpoint_root>(
            std::get<local_metadata_record>(pin->metadata()).payload())
              .entry_count
            == 3,
          "root metadata lifetime was lost");
        pin.reset();
        require(!closing.available(), "returned page lost root lifetime");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    page.reset();
    pin.reset();
    take(co_await drive.lifecycle(std::move(closing)));
    take(co_await drive.lifecycle(root->close()));
    root.reset();
    take(failed.outcome());
}
template<typename Backend, typename Owner, typename Driver>
seastar::future<> root_errors(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await seed_checkpoint(files, owner, spec, budget, work, drive);
    const auto path = take(
      take(local_paths::make(spec.root))
        .sequence_file(0, local_sequence_file::checkpoint, 70));
    const auto root_wire = local_fixture::read("checkpoint_root");
    const auto page_wire = local_fixture::read("checkpoint_page");
    for (unsigned fault = 0; fault < 4; ++fault) {
        auto bytes = root_wire + page_wire;
        if (fault == 0) bytes = root_wire;
        if (fault == 1) bytes += std::string(4096, 'x');
        if (fault == 2) {
            auto page = page_wire;
            page[140] ^= 1;
            repair(page);
            bytes = root_wire + page;
        }
        if (fault == 3) {
            auto changed = root_wire;
            put(changed, 8, 2, 2);
            repair(changed);
            bytes = changed + page_wire;
        }
        co_await write_bytes(files, path, bytes, drive);
        auto rejected = co_await drive.lifecycle(
          local_root_owner::open(
            files,
            owner,
            spec,
            0,
            installation_contract::checkpoint_reference(),
            installation_contract::checkpoint_expectation(spec),
            budget,
            limits(),
            work));
        if (rejected) {
            take(co_await drive.lifecycle((*rejected)->close()));
            rejected->reset();
        }
        require(!rejected, "invalid root/page extent was opened");
        require(
          (co_await read_bytes(files, path, drive)) == bytes,
          "read-only root open changed rejected bytes");
    }
    co_await write_bytes(files, path, root_wire + page_wire, drive);
    auto reference = installation_contract::checkpoint_reference();
    auto wrong = take(
      local_root_reference::make(
        reference.kind(),
        reference.sequence(),
        reference.position(),
        reference.bytes(),
        reference.pages(),
        literal_digest(
          "0000000000000000000000000000000000000000000000000000000000000000")));
    auto expected = installation_contract::checkpoint_expectation(spec);
    expected.digest = wrong.digest();
    auto rejected = co_await drive.lifecycle(
      local_root_owner::open(
        files, owner, spec, 0, wrong, expected, budget, limits(), work));
    if (rejected) {
        take(co_await drive.lifecycle((*rejected)->close()));
        rejected->reset();
    }
    require(!rejected, "root accepted a wrong external SHA");
}
template<typename Backend, typename Owner, typename Driver>
seastar::future<> generation_lifetime(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await seed_segment(files, owner, spec, budget, work, drive);
    local_generation_expectation before{
      local_publication_generation::make(1).value(),
      active_publication(),
      descriptor(),
      {}};
    auto old = take(
      co_await drive.lifecycle(
        local_generation_owner::open(
          files, owner, spec, 0, before, budget, limits(), work)));
    std::optional<local_generation_pin> parked;
    parked.emplace(take(old->pin()));
    std::unique_ptr<local_generation_owner> current;
    runtime::first_failure failed;
    try {
        const auto parent = take(
          take(local_paths::make(spec.root))
            .segment(0, {segment().segment(), segment().generation()}));
        local_file_publisher<Backend> publisher{
          files,
          budget,
          {spec.shard_owner(0).value(),
           spec.root,
           parent,
           runtime::file_name::make("published").value(),
           runtime::file_rename_policy::replace,
           before.generation}};
        local_publication_outcome publication;
        try {
            publication = co_await drive.lifecycle(publisher.publish(
              {spec.shard_owner(0).value(),
               local_publication_generation::make(3).value(),
               before.generation},
              co_await buffer_async(
                local_fixture::read("publication_all_roots"), 4096),
              work));
        } catch (...) {
            publication.failure.observe(std::current_exception());
        }
        take(co_await drive.lifecycle(publisher.close()));
        take(publication.failure.outcome());
        require(
          publication.disposition == local_publication_disposition::durable,
          "replacement not durable");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        if (!failed.failed()) {
            const auto expected_contexts = contexts(spec);
            local_generation_expectation after{
              local_publication_generation::make(3).value(),
              sealed_publication(),
              descriptor(),
              expected_contexts};
            current = take(
              co_await drive.lifecycle(
                local_generation_owner::open(
                  files, owner, spec, 0, after, budget, limits(), work)));
            require(
              parked->generation().value() == 1
                && std::holds_alternative<durable_footer>(*parked->boundary()),
              "new sealed generation changed old family-6 pin");
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    old->retire();
    auto drained = old->close();
    try {
        require(
          !old->pin() && !drained.available(), "old reader was not drained");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    parked.reset();
    take(co_await drive.lifecycle(std::move(drained)));
    old.reset();
    if (current) {
        try {
            auto pin = take(current->pin());
            require(
              std::holds_alternative<sealed_footer>(*pin.boundary())
                && !pin.index_rebuild_reason(),
              "new generation root state");
            for (auto kind :
                 {local_root_kind::index,
                  local_root_kind::sealed_retry,
                  local_root_kind::completed_retry_snapshot}) {
                auto root = take(pin.root(kind));
                auto page = take(
                  co_await drive.lifecycle(
                    root.read(page_ordinal::make(0).value(), work)));
                require(
                  page.position.value()
                    == (kind == local_root_kind::sealed_retry ? 0U : 4096U),
                  "root kind placement lost");
            }
        } catch (...) {
            failed.observe(std::current_exception());
        }
        take(co_await drive.lifecycle(current->close()));
        current.reset();
    }
    take(failed.outcome());
}
template<typename Backend, typename Owner, typename Driver>
seastar::future<> generation_errors(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await seed_segment(files, owner, spec, budget, work, drive);
    const auto paths = take(local_paths::make(spec.root));
    const local_segment_name name{segment().segment(), segment().generation()};
    co_await write_bytes(
      files,
      take(paths.segment_file(0, name, local_segment_file::published)),
      local_fixture::read("publication_all_roots"),
      drive);
    const auto expected_contexts = contexts(spec);
    local_generation_expectation expected{
      local_publication_generation::make(3).value(),
      sealed_publication(),
      descriptor(),
      expected_contexts};
    auto index = take(paths.object(0, name, index_ref().sequence()));
    take(co_await drive.lifecycle(files.remove_file(index)));
    auto generation = take(
      co_await drive.lifecycle(
        local_generation_owner::open(
          files, owner, spec, 0, expected, budget, limits(), work)));
    runtime::first_failure failed;
    try {
        auto pin = take(generation->pin());
        require(
          pin.index_rebuild_reason().has_value()
            && !pin.root(local_root_kind::index),
          "missing derived index not reported as rebuild debt");
        require(
          bool(pin.root(local_root_kind::sealed_retry)),
          "required retry root missing");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(co_await drive.lifecycle(generation->close()));
    generation.reset();
    take(failed.outcome());
    co_await write_bytes(
      files,
      index,
      local_fixture::read("index_root") + local_fixture::read("index_page"),
      drive);
    take(
      co_await drive.lifecycle(files.remove_file(
        take(paths.object(0, name, snapshot_ref().sequence())))));
    auto rejected = co_await drive.lifecycle(
      local_generation_owner::open(
        files, owner, spec, 0, expected, budget, limits(), work));
    if (rejected) {
        take(co_await drive.lifecycle((*rejected)->close()));
        rejected->reset();
    }
    require(
      !rejected && rejected.error().code() == errc::not_found,
      "missing required snapshot treated as derived-index debt");
}

inline model::wal_incarnation_id wal(std::uint64_t n) {
    return local_wal_high{}.checked_advance(n)->incarnation().value();
}
template<typename Backend, typename Driver>
seastar::future<> put_wal(
  Backend& files,
  const local_device_spec& spec,
  model::wal_incarnation_id id,
  std::string bytes,
  Driver drive) {
    const auto paths = take(local_paths::make(spec.root));
    const auto path = take(paths.wal(0, id));
    take(
      co_await drive.lifecycle(files.create_directories(take(
        runtime::file_path::make(
          path.value().substr(0, path.value().rfind('/')))))));
    co_await write_bytes(files, path, std::move(bytes), drive);
}
inline local_shard_control chain_control() {
    return {
      local_wal_high{}.checked_advance(128).value(),
      local_object_high{128},
      local_decision_high{64},
      local_deletion_high{80},
      {},
      local_wal_head{
        wal(9),
        literal_digest(
          "4d24e709a20f78d057176c757005e5f3e3b840d8a09ab48368df164b68ae3d1f")}};
}
template<typename Backend, typename Owner, typename Driver>
seastar::future<> wal_chain(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation_contract::bootstrap(
      files, owner, spec, budget, work, drive);
    {
        const local_shard_control unactivated{
          local_wal_high{},
          local_object_high{},
          local_decision_high{},
          local_deletion_high{},
          {},
          {}};
        unsigned visited = 0;
        auto visit = [&](const local_wal_chain_entry&) {
            ++visited;
            return seastar::make_ready_future<runtime::result<bool>>(true);
        };
        seastar::abort_source stopped;
        codec::cooperative_work cancelled{codec::limits::defaults(), stopped};
        stopped.request_abort();
        auto rejected = co_await drive.lifecycle(walk_local_wal_chain(
          files,
          owner,
          spec,
          0,
          unactivated,
          {},
          true,
          budget,
          limits(),
          cancelled,
          visit));
        require(
          !rejected && rejected.error().code() == errc::aborted && visited == 0,
          "cancelled empty WAL inventory reported success");
        auto empty = take(
          co_await drive.lifecycle(walk_local_wal_chain(
            files,
            owner,
            spec,
            0,
            unactivated,
            {},
            true,
            budget,
            limits(),
            work,
            visit)));
        require(
          empty.complete && empty.visited == 0 && visited == 0,
          "uncancelled unactivated WAL inventory changed");
    }
    auto first = local_fixture::read("wal_header")
                 + local_fixture::read("prepare_a1")
                 + local_fixture::read("prepare_b1")
                 + local_fixture::read("prepare_a2");
    co_await put_wal(files, spec, wal(1), first, drive);
    co_await put_wal(
      files, spec, wal(9), local_fixture::read("wal_successor_gap"), drive);
    auto extra = local_fixture::read("wal_header");
    extra[119] = 99;
    repair(extra);
    co_await put_wal(files, spec, wal(99), extra, drive);
    const auto control = chain_control();
    std::array<unsigned, 2> ids{};
    unsigned seen = 0;
    auto visit = [&](const local_wal_chain_entry& entry) {
        require(seen < 2, "unreferenced WAL selected");
        const auto descriptor = std::get<local_wal_descriptor>(
          entry.record.value.payload());
        ids[seen++] = descriptor.incarnation.bytes()[15];
        require(
          entry.head_digest_pinned == (seen == 1),
          "predecessor SHA pin invented");
        if (seen == 2)
            require(
              entry.sealed_end && entry.sealed_end->value() == 16384,
              "predecessor end lost");
        return seastar::make_ready_future<runtime::result<bool>>(true);
    };
    auto complete = take(
      co_await drive.lifecycle(walk_local_wal_chain(
        files,
        owner,
        spec,
        0,
        control,
        {},
        false,
        budget,
        limits(),
        work,
        visit)));
    require(
      complete.complete && complete.visited == 2 && ids[0] == 9 && ids[1] == 1,
      "WAL allocation gaps rejected or maximum filename used");
    seen = 0;
    auto bounded = co_await drive.lifecycle(walk_local_wal_chain(
      files,
      owner,
      spec,
      0,
      control,
      {},
      false,
      budget,
      limits(),
      work,
      visit,
      {100, 1}));
    require(
      !bounded && bounded.error().code() == errc::resource_exhausted,
      "WAL inventory bound ignored");
    auto stop = [](const local_wal_chain_entry&) {
        return seastar::make_ready_future<runtime::result<bool>>(false);
    };
    auto partial = take(
      co_await drive.lifecycle(walk_local_wal_chain(
        files,
        owner,
        spec,
        0,
        control,
        {},
        false,
        budget,
        limits(),
        work,
        stop)));
    require(
      !partial.complete && partial.visited == 1,
      "partial WAL inventory marked complete");
    const auto cutoff
      = local_wal_cursor::make(wal(1), runtime::file_position{8192}).value();
    seen = 0;
    complete = take(
      co_await drive.lifecycle(walk_local_wal_chain(
        files,
        owner,
        spec,
        0,
        control,
        cutoff,
        false,
        budget,
        limits(),
        work,
        visit)));
    require(complete.complete, "known checkpoint cutoff not found");
    const auto missing = take(
      take(local_paths::make(spec.root)).wal(0, wal(1)));
    take(co_await drive.lifecycle(files.remove_file(missing)));
    seen = 0;
    auto absent = co_await drive.lifecycle(walk_local_wal_chain(
      files,
      owner,
      spec,
      0,
      control,
      {},
      false,
      budget,
      limits(),
      work,
      visit));
    require(
      !absent && absent.error().code() == errc::not_found,
      "missing predecessor hidden by a higher file");
    co_await put_wal(files, spec, wal(1), first, drive);
    auto cycle = local_fixture::read("wal_successor_gap");
    cycle[139] = 9;
    repair(cycle);
    co_await put_wal(files, spec, wal(9), cycle, drive);
    auto broken = control;
    broken.wal_head->header_digest = codec::immutable_object_digest{
      exact_sha(cycle)};
    seen = 0;
    auto rejected = co_await drive.lifecycle(walk_local_wal_chain(
      files, owner, spec, 0, broken, {}, false, budget, limits(), work, visit));
    require(!rejected, "nondecreasing predecessor accepted");
}
struct record_resolver final {
    const local_device_spec& spec;
    seastar::future<runtime::result<std::optional<local_record_expectation>>>
    operator()(const local_namespace_entry& entry) const {
        std::optional<local_record_expectation> result;
        if (entry.kind == local_entry_kind::descriptor)
            result.emplace(
              local_record_expectation{
                record_expectation(spec, "descriptor_a"), false});
        if (entry.kind == local_entry_kind::publication)
            result.emplace(
              local_record_expectation{
                record_expectation(spec, "publication_boundary"), true});
        if (entry.kind == local_entry_kind::deletion)
            result.emplace(
              local_record_expectation{
                record_expectation(spec, "deletion"), false});
        return seastar::make_ready_future<
          runtime::result<std::optional<local_record_expectation>>>(
          std::move(result));
    }
};
struct unresolved_records final {
    seastar::future<runtime::result<std::optional<local_record_expectation>>>
    operator()(const local_namespace_entry&) const {
        return seastar::make_ready_future<
          runtime::result<std::optional<local_record_expectation>>>(
          std::nullopt);
    }
};
// Transform independently specified header fixtures, without the production
// encoder. These small images leave identity and predecessor bytes unchanged.
inline std::string realign_wal_fixture(std::string wire, std::size_t a) {
    const auto payload = static_cast<std::size_t>(get(wire, 96, 4));
    const auto tail = wire.at(120) == 0 ? 124U : 148U;
    const auto bytes = ((32 + 72 + payload + a - 1) / a) * a;
    put(wire, tail, a, 4);
    put(wire, tail + 8, bytes, 8);
    put(wire, 100, bytes - 32 - 72 - payload, 4);
    wire.resize(bytes, '\0');
    put(wire, 12, bytes - 32, 4);
    repair(wire);
    return wire;
}
template<typename Backend, typename Owner, typename Driver>
seastar::future<> wal_alignment(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation_contract::bootstrap(
      files, owner, spec, budget, work, drive);
    const auto paths = take(local_paths::make(spec.root));
    for (const auto a : {512U, 8192U}) {
        auto first = realign_wal_fixture(local_fixture::read("wal_header"), a);
        // Header inventory does not certify this supplied predecessor end.
        first.resize(16384, '\0');
        auto head = realign_wal_fixture(
          local_fixture::read("wal_successor_gap"), a);
        co_await put_wal(files, spec, wal(1), first, drive);
        co_await put_wal(files, spec, wal(9), head, drive);
        auto control = chain_control();
        control.wal_head->header_digest = codec::immutable_object_digest{
          exact_sha(head)};

        // Persist an independent control fixture so startup has to validate
        // both its selected head and the differently aligned older header.
        auto control_wire = local_fixture::read("control_head");
        control_wire[167] = '\x09';
        const auto digest = control.wal_head->header_digest.bytes();
        std::copy(digest.begin(), digest.end(), control_wire.begin() + 168);
        repair(control_wire);
        co_await write_bytes(
          files, take(paths.control(0)), control_wire, drive);
        auto report = take(
          co_await drive.lifecycle(inspect_local_store(
            files,
            owner,
            spec,
            {local_store_intent::must_exist, false},
            budget,
            limits(),
            work)));
        require(
          report.state == local_store_state::existing,
          "startup used metadata alignment for WAL headers");

        unsigned seen = 0;
        auto visit_chain = [&](const local_wal_chain_entry& entry) {
            const auto& value = std::get<local_wal_descriptor>(
              entry.record.value.payload());
            require(
              value.alignment == alignment(a),
              "WAL chain lost stored geometry");
            require(
              entry.head_digest_pinned == (seen == 0), "WAL head pin changed");
            ++seen;
            return seastar::make_ready_future<runtime::result<bool>>(true);
        };
        auto inventory = take(
          co_await drive.lifecycle(walk_local_wal_chain(
            files,
            owner,
            spec,
            0,
            control,
            {},
            false,
            budget,
            limits(),
            work,
            visit_chain)));
        require(
          inventory.complete && seen == 2,
          "differently aligned WAL chain rejected");

        unsigned claimed = 0;
        auto visit_claims = [&](const local_discovered_record& entry) {
            if (entry.entry.kind == local_entry_kind::wal) {
                require(
                  !entry.damage && entry.claims
                    && entry.claims->alignment == alignment(a),
                  "WAL discovery probe used metadata alignment");
                ++claimed;
            }
            return seastar::make_ready_future<runtime::result<bool>>(true);
        };
        inventory = take(
          co_await drive.lifecycle(discover_local_records(
            files,
            owner,
            spec,
            budget,
            limits(),
            work,
            unresolved_records{},
            visit_claims)));
        require(
          inventory.complete && claimed == 2,
          "WAL claims inventory incomplete");

        auto resolve = [&](const local_namespace_entry& entry) {
            std::optional<local_record_expectation> selected;
            if (entry.kind == local_entry_kind::wal && entry.wal == wal(9)) {
                local_metadata_expectation expected{
                  local_metadata_header::make(
                    local_metadata_kind::wal_descriptor,
                    spec.shard_owner(0).value(),
                    local_publication_generation::make(1).value())
                    .value(),
                  alignment(a)};
                expected.wal_incarnation = wal(9);
                expected.digest = control.wal_head->header_digest;
                expected.encoded_bytes = byte_count{head.size()};
                selected.emplace(local_record_expectation{expected});
            }
            return seastar::make_ready_future<
              runtime::result<std::optional<local_record_expectation>>>(
              std::move(selected));
        };
        bool decoded = false;
        auto visit_decoded = [&](const local_discovered_record& entry) {
            if (
              entry.entry.kind == local_entry_kind::wal
              && entry.entry.wal == wal(9)) {
                require(
                  !entry.damage && entry.decoded.has_value(),
                  "explicit WAL geometry rejected by namespace validation");
                decoded = true;
            }
            return seastar::make_ready_future<runtime::result<bool>>(true);
        };
        inventory = take(
          co_await drive.lifecycle(discover_local_records(
            files,
            owner,
            spec,
            budget,
            limits(),
            work,
            resolve,
            visit_decoded)));
        require(
          inventory.complete && decoded, "explicit WAL discovery incomplete");

        auto wrong = *control.wal_head;
        wrong.header_digest = literal_digest(fixture_case("wal_header").digest);
        auto rejected = co_await drive.lifecycle(
          load_local_wal_head(files, spec, 0, wrong, budget, limits(), work));
        require(
          !rejected && rejected.error().code() == errc::corrupt_data,
          "stored geometry bypassed independent head digest");
        require(
          co_await read_bytes(files, take(paths.wal(0, wal(9))), drive) == head,
          "failed WAL inspection mutated the file");
    }
}
template<typename Backend, typename Owner, typename Driver>
seastar::future<> discovery_cleanup(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await seed_segment(files, owner, spec, budget, work, drive);
    const auto paths = take(local_paths::make(spec.root));
    const local_segment_name name{segment().segment(), segment().generation()};
    co_await write_bytes(
      files,
      take(paths.segment_file(0, name, local_segment_file::published)),
      local_fixture::read("publication_all_roots"),
      drive);
    co_await write_bytes(
      files,
      take(paths.sequence_file(0, local_sequence_file::deletion, 80)),
      local_fixture::read("deletion"),
      drive);
    const auto temp = take(local_child_path(
      spec.root,
      take(local_temporary_name(
        runtime::file_name::make("store.meta").value(),
        local_publication_generation::make(1).value(),
        7))));
    // Match this independently configured data identity, retaining valid
    // framing.
    auto marker = local_fixture::read("store");
    marker.replace(68, 16, 16, static_cast<char>(0x44));
    repair(marker);
    co_await write_bytes(files, temp, marker, drive);
    const auto unknown = take(local_child_path(
      spec.root, runtime::file_name::make(".unrecognized").value()));
    co_await write_bytes(files, unknown, "preserve", drive);
    unsigned publications = 0, deletions = 0, temporaries = 0, unrecognized = 0;
    auto refs = [](const local_discovered_record& record) {
        local_cleanup_references facts;
        if (
          record.entry.kind == local_entry_kind::object
          && record.entry.sequence >= 40 && record.entry.sequence <= 42)
            facts.persisted = local_reference_status::referenced;
        // Test-supplied complete absence proof for exactly the known marker
        // attempt; an arbitrary .tmp spelling receives no such assertion.
        if (record.entry.kind == local_entry_kind::temporary)
            facts.persisted = local_reference_status::absent;
        return seastar::make_ready_future<
          runtime::result<local_cleanup_references>>(facts);
    };
    auto visit = [&](
                   const local_discovered_record& record,
                   local_cleanup_observation cleanup) {
        if (record.entry.kind == local_entry_kind::publication) {
            require(
              record.decoded.has_value(),
              "publication was not contextually decoded");
            require(
              record.decoded->value.header().generation().value() == 3,
              "selected publication generation was guessed");
            require(
              std::get<local_object_publication>(
                record.decoded->value.payload())
                  .roots.size()
                == 3,
              "object references not inventoried");
            ++publications;
        }
        if (record.entry.kind == local_entry_kind::deletion) {
            require(
              record.decoded
                && !std::get<local_deletion_intent>(
                      record.decoded->value.payload())
                      .objects.empty(),
              "cleanup intent was not inventoried");
            ++deletions;
            require(
              cleanup.classification == local_cleanup_class::unresolved,
              "unproven intent authorized cleanup");
        }
        if (record.entry.kind == local_entry_kind::temporary) {
            require(
              cleanup.classification == local_cleanup_class::temporary_debt,
              "validated temporary debt lost");
            ++temporaries;
        }
        if (record.entry.kind == local_entry_kind::unknown) {
            require(
              cleanup.classification == local_cleanup_class::unsupported,
              "unknown entry not preserved");
            ++unrecognized;
        }
        if (record.entry.kind == local_entry_kind::object)
            require(
              cleanup.classification == local_cleanup_class::referenced,
              "referenced generation classified orphan");
        return seastar::make_ready_future<runtime::result<bool>>(true);
    };
    auto complete = take(
      co_await drive.lifecycle(enumerate_local_cleanup(
        files,
        owner,
        spec,
        budget,
        limits(),
        work,
        record_resolver{spec},
        refs,
        visit)));
    require(
      complete.complete && publications == 1 && deletions == 1
        && temporaries == 1 && unrecognized == 1,
      "bounded inventory omitted records");
    require(
      (co_await read_bytes(files, temp, drive)) == marker
        && (co_await read_bytes(files, unknown, drive)) == "preserve",
      "cleanup classification changed bytes");
    auto stop = [](const local_namespace_entry&) {
        return seastar::make_ready_future<runtime::result<bool>>(false);
    };
    auto partial = take(
      co_await drive.lifecycle(walk_local_namespace(
        files, owner, spec, budget, limits(), work, stop)));
    require(
      !partial.complete && partial.visited == 1,
      "namespace stop became complete inventory");
    auto keep = [](const local_namespace_entry&) {
        return seastar::make_ready_future<runtime::result<bool>>(true);
    };
    auto limited = co_await drive.lifecycle(walk_local_namespace(
      files, owner, spec, budget, limits(), work, keep, {1, 1}));
    require(
      !limited && limited.error().code() == errc::resource_exhausted,
      "namespace limit silently truncated inventory");
}
template<typename Backend, typename Owner, typename Driver>
seastar::future<> empty_retry(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await installation_contract::segment_bundles(
      files, owner, spec, budget, drive);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto sc = segment_context::make(
                      spec.owner.cluster(),
                      id<model::topic_id>(0x10),
                      id<model::range_id>(0x20),
                      id<model::segment_id>(0x32),
                      model::segment_generation::make(2).value())
                      .value();
    const auto reference = take(
      local_root_reference::make(
        local_root_kind::sealed_retry,
        local_object_sequence::make(42).value(),
        runtime::file_position{4096},
        byte_count{4096},
        page_count::make(0).value(),
        literal_digest(
          "7bb455f0a92c8be1ee1ae58801586ee27433bc9e7ed73c8d50a22d3e4cf4e5a3")));
    footer_expectation expected{
      {sc,
       alignment(4096),
       runtime::file_position{4096},
       model::range_logical_end{300},
       model::segment_relative_end{9}},
      runtime::file_position{4096}};
    auto root = take(
      co_await drive.lifecycle(
        local_root_owner::open(
          files, owner, spec, 0, reference, expected, budget, limits(), work)));
    runtime::first_failure failed;
    try {
        auto pin = take(root->pin());
        require(
          !pin.position(page_ordinal::make(0).value()),
          "zero-page retry invented a page");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(co_await drive.lifecycle(root->close()));
    root.reset();
    take(failed.outcome());
    auto path = take(
      take(local_paths::make(spec.root))
        .object(0, {sc.segment(), sc.generation()}, reference.sequence()));
    take(co_await drive.lifecycle(files.remove_file(path)));
    auto rejected = co_await drive.lifecycle(
      local_root_owner::open(
        files, owner, spec, 0, reference, expected, budget, limits(), work));
    if (rejected) {
        take(co_await drive.lifecycle((*rejected)->close()));
        rejected->reset();
    }
    require(
      !rejected && rejected.error().code() == errc::not_found,
      "absent retry bundle treated as empty");
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<> cold_resolution(
  Backend& files,
  Owner& owner,
  std::span<const local_device_spec> specs,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    {
        auto created = co_await drive.lifecycle(initialize_local_stores(
          files,
          owner,
          specs,
          {local_store_intent::create_or_resume, false},
          budget,
          limits(),
          work));
        take(created.failure.outcome());
    }
    auto absent = co_await drive.lifecycle(resolve_local_segment(
      files,
      owner,
      specs,
      0,
      descriptor(),
      segment_head(),
      budget,
      limits(),
      work));
    require(
      !absent && absent.error().code() == errc::not_found,
      "empty catalog resolved a segment");
    for (unsigned device = 0; device < 2; ++device) {
        auto published = co_await drive.lifecycle(
          publish_local_segment_descriptor(
            files,
            owner,
            specs[device],
            0,
            descriptor(),
            segment_head(),
            budget,
            limits(),
            work));
        take(published.failure.outcome());
        const auto paths = take(local_paths::make(specs[device].root));
        co_await write_bytes(
          files,
          take(paths.segment_file(
            0,
            {segment().segment(), segment().generation()},
            local_segment_file::data)),
          local_fixture::read("data_header_a"),
          drive);
        auto result = co_await drive.lifecycle(resolve_local_segment(
          files,
          owner,
          specs,
          0,
          descriptor(),
          segment_head(),
          budget,
          limits(),
          work));
        if (device == 0)
            require(
              result && result->device == specs[0].owner.device(),
              "cold descriptor not resolved independently");
        else
            require(
              !result && result.error().code() == errc::wrong_context,
              "duplicate writable copies accepted");
    }
    const auto damaged = take(take(local_paths::make(specs[1].root))
                                .segment_file(
                                  0,
                                  {segment().segment(), segment().generation()},
                                  local_segment_file::descriptor));
    take(co_await drive.lifecycle(files.remove_file(damaged)));
    auto incomplete = co_await drive.lifecycle(resolve_local_segment(
      files,
      owner,
      specs,
      0,
      descriptor(),
      segment_head(),
      budget,
      limits(),
      work));
    require(!incomplete, "incomplete second copy silently skipped");
}
} // namespace kwaque::storage::testing::reader_contract
