#pragma once

#include "src/base/units.h"
#include "src/storage/local_metadata_layout.h"
#include "src/storage/storage_scan.h"
#include "src/storage/tests/local_reader_contract.h"
#include "src/storage/tests/wal_test_support.h"
#include "src/storage/wal_scan.h"

#include <algorithm>
#include <array>
#include <bit>
#include <string>
#include <utility>
#include <vector>

namespace kwaque::storage::testing::scan_contract {
using installation_contract::descriptor;
using installation_contract::limits;
using installation_contract::segment;
using installation_contract::segment_head;
using reader_contract::chain_control;
using reader_contract::put_wal;
using reader_contract::wal;
using store_contract::read_bytes;
using store_contract::require;
using store_contract::take;
using store_contract::write_bytes;

inline segment_context segment_b() {
    return segment_context::make(
             id<model::cluster_id>(0x11),
             id<model::topic_id>(0x10),
             id<model::range_id>(0x21),
             id<model::segment_id>(0x31),
             model::segment_generation::make(2).value())
      .value();
}
// Where the segment `descriptor_b()` describes starts in its range.
inline model::range_logical_end origin_b() {
    return model::range_logical_end{1000};
}
inline local_segment_descriptor descriptor_b() {
    auto value = descriptor();
    value.segment = segment_b();
    return value;
}
inline segment_header segment_head_b() {
    return segment_header::make(segment_b(), alignment(4096)).value();
}
// The head alone, pinned by its exact header digest.
inline local_shard_control head_control() {
    return {
      local_wal_high{}.checked_advance(1).value(),
      local_object_high{},
      local_decision_high{},
      local_deletion_high{},
      {},
      local_wal_head{
        wal(1),
        codec::immutable_object_digest{
          exact_digest(local_fixture::read("wal_header"))}}};
}
// Header, then PREPAREs for A1, B1 and A2 at 4096, 8192 and 12288.
inline std::string interleaved() {
    return local_fixture::read("wal_header") + local_fixture::read("prepare_a1")
           + local_fixture::read("prepare_b1")
           + local_fixture::read("prepare_a2");
}
inline wal_scan_limits scan_limits(byte_count window = byte_count{8_KiB}) {
    return {
      limits(),
      {.window_bytes = window, .read_ahead = 2},
      byte_count{512_KiB},
      byte_count{64_KiB}};
}

struct seen_prepare final {
    std::uint64_t position, end;
    wal_target_resolution resolution;
    segment_context target;
    std::string child;
    bool survival{false};
};
struct observed final {
    std::vector<seen_prepare> prepares;
    std::vector<wal_file_scan> files;
};
struct observe_prepares final {
    observed* out;
    seastar::future<runtime::result<bool>>
    operator()(const wal_scanned_prepare& value) const {
        out->prepares.push_back(
          {value.begin.position().value(),
           value.end.position().value(),
           value.resolution,
           value.claims.target,
           value.prepare ? flat(value.prepare->batch().bytes()) : "",
           value.survival});
        return seastar::make_ready_future<runtime::result<bool>>(true);
    }
};
struct observe_files final {
    observed* out;
    seastar::future<runtime::result<void>>
    operator()(const wal_file_scan& value) const {
        out->files.push_back(value);
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::result<void>{});
    }
};
// Counts target lookups and answers from a fixed placement list.
struct fixed_targets final {
    std::vector<wal_target> known;
    unsigned lookups{0};
    std::optional<errc> failure;
    seastar::future<runtime::result<std::optional<wal_target>>>
    operator()(const segment_context& claimed) {
        ++lookups;
        if (failure)
            return seastar::make_ready_future<
              runtime::result<std::optional<wal_target>>>(runtime::failure(
              runtime::operation_error{
                *failure, runtime::operation_kind::file}));
        std::optional<wal_target> found;
        for (const auto& target : known)
            if (target.segment == claimed) found = target;
        return seastar::make_ready_future<
          runtime::result<std::optional<wal_target>>>(found);
    }
};
inline wal_target target(segment_context context, std::uint64_t start = 4096) {
    return {
      context,
      alignment(4096),
      runtime::file_position{start},
      storage_profile::v1,
      id<device_store_id>(0x33)};
}

template<typename Backend, typename Owner, typename Resolver, typename Driver>
seastar::future<runtime::result<wal_scan_result>> scan(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  const local_shard_control& control,
  Resolver& resolve,
  observed& out,
  Driver drive,
  std::optional<local_wal_cursor> cutoff = std::nullopt,
  wal_scan_limits bounds = scan_limits()) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    out = {};
    co_return co_await drive.lifecycle(scan_local_wal(
      files,
      owner,
      spec,
      0,
      control,
      cutoff,
      budget,
      bounds,
      work,
      resolve,
      observe_prepares{&out},
      observe_files{&out}));
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<> seed_targets(
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
    const std::array<std::pair<local_segment_descriptor, segment_header>, 2>
      targets{
        {{descriptor(), segment_head()}, {descriptor_b(), segment_head_b()}}};
    const std::array<std::string_view, 2> headers{
      "data_header_a", "data_header_b"};
    for (std::size_t i = 0; i < targets.size(); ++i) {
        auto published = co_await drive.lifecycle(
          publish_local_segment_descriptor(
            files,
            owner,
            spec,
            0,
            targets[i].first,
            targets[i].second,
            budget,
            limits(),
            work));
        take(published.failure.outcome());
        const local_segment_name name{
          targets[i].first.segment.segment(),
          targets[i].first.segment.generation()};
        co_await write_bytes(
          files,
          take(paths.segment_file(0, name, local_segment_file::data)),
          local_fixture::read(headers[i]),
          drive);
    }
}

// Slot classes, windowing and cursor rules of the shared reader.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> reader_slots(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation_contract::bootstrap(
      files, owner, spec, budget, work, drive);
    const auto path = take(take(local_paths::make(spec.root)).wal(0, wal(1)));
    const auto a1 = local_fixture::read("prepare_a1");
    auto header_damage = a1;
    header_damage[12] ^= 1;
    auto oversized = a1;
    put(oversized, 12, 0x7fffffffU, 4);
    repair(oversized);
    struct case_input final {
        std::string bytes;
        std::vector<scan_slot_kind> slots;
    };
    const std::array cases{
      // An object spanning several 1-KiB windows, then zeroed space.
      case_input{
        a1 + std::string(4096, '\0'),
        {scan_slot_kind::object, scan_slot_kind::zero}},
      case_input{
        a1 + std::string(16, 'x'),
        {scan_slot_kind::object, scan_slot_kind::torn}},
      case_input{a1.substr(0, 2048), {scan_slot_kind::torn}},
      case_input{std::string(64, 'x'), {scan_slot_kind::malformed}},
      case_input{header_damage, {scan_slot_kind::corrupt}},
      case_input{a1, {scan_slot_kind::object, scan_slot_kind::end}}};
    for (const auto& test : cases) {
        co_await put_wal(files, spec, wal(1), test.bytes, drive);
        auto file = take(co_await drive.lifecycle(files.open(path, {})));
        runtime::first_failure failed;
        std::unique_ptr<scan_reader> reader;
        try {
            reader = take(
              scan_reader::make(
                file,
                budget,
                runtime::file_position{},
                runtime::file_position{test.bytes.size()},
                {.window_bytes = byte_count{1_KiB}, .read_ahead = 1}));
            for (const auto expected : test.slots) {
                const auto at = reader->position();
                auto slot = take(co_await drive.lifecycle(reader->next(work)));
                require(slot.kind == expected, "scan slot misclassified");
                require(
                  slot.position == at && reader->position() == at,
                  "classification moved the cursor");
                auto again = take(co_await drive.lifecycle(reader->next(work)));
                require(
                  again.kind == slot.kind && again.position == at,
                  "repeated classification changed");
                if (expected != scan_slot_kind::object) {
                    require(!reader->advance(), "advanced past a non-object");
                    break;
                }
                require(
                  flat(take(reader->object())) == a1,
                  "object bytes changed across windows");
                take(reader->advance());
                require(
                  reader->position().value() == at.value() + a1.size(),
                  "advance did not use the verified length");
            }
        } catch (...) {
            failed.observe(std::current_exception());
        }
        if (reader) co_await drive.lifecycle(reader->close());
        reader.reset();
        failed.observe(co_await drive.lifecycle(file.close()));
        take(failed.outcome());
        require(
          (co_await read_bytes(files, path, drive)) == test.bytes,
          "reading changed scanned bytes");
    }
    // An intact header whose size only the limits refuse is a resource
    // limit, never damage: the reader stops without classifying it.
    co_await put_wal(files, spec, wal(1), oversized, drive);
    auto file = take(co_await drive.lifecycle(files.open(path, {})));
    runtime::first_failure failed;
    std::unique_ptr<scan_reader> reader;
    try {
        reader = take(
          scan_reader::make(
            file,
            budget,
            runtime::file_position{},
            runtime::file_position{oversized.size()},
            {.window_bytes = byte_count{1_KiB}, .read_ahead = 1}));
        auto refused = co_await drive.lifecycle(reader->next(work));
        require(
          !refused && refused.error().code() == errc::resource_exhausted,
          "an intact oversized object was classified as damage");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (reader) co_await drive.lifecycle(reader->close());
    reader.reset();
    failed.observe(co_await drive.lifecycle(file.close()));
    take(failed.outcome());
}

// The head's content in WAL order, resolved against independent targets, and
// every head tail ending the content without changing a byte.
template<
  typename Backend,
  typename Owner,
  typename Driver,
  typename Reads = store_contract::uncounted_reads>
seastar::future<> head_tails(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  Reads reads = {}) {
    co_await seed_targets(files, owner, spec, budget, drive);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array specs{spec};
    using catalog_type = local_wal_target_catalog<Backend, Owner>;
    const std::array<typename catalog_type::entry, 2> entries{
      {{descriptor(), segment_head()}, {descriptor_b(), segment_head_b()}}};
    auto catalog = take(
      catalog_type::make(
        files, owner, specs, 0, entries, budget, limits(), work));
    observed out;
    runtime::first_failure failed;
    try {
        co_await put_wal(files, spec, wal(1), interleaved(), drive);
        const auto head_path = take(
          take(local_paths::make(spec.root)).wal(0, wal(1)));
        const auto read_before = reads(head_path);
        auto result = take(
          co_await scan(
            files, owner, spec, budget, head_control(), *catalog, out, drive));
        // A scan reads the head's first envelope in one bounded read, then
        // what follows its header at most twice: a first pass finds the
        // last whole write and the visiting pass reads up to it. Here both
        // read all of it.
        const auto data_start = local_fixture::read("wal_header").size();
        const auto first_read = [](std::uint64_t file) {
            return std::min(
              {file,
               local_metadata_first_read_bytes.value(),
               limits().operation_bytes.value()});
        };
        if (const auto read_after = reads(head_path); read_after && read_before)
            require(
              *read_after - *read_before
                == first_read(16384) + 2 * (16384 - data_start),
              "the head was read more than its header and two passes");
        require(
          result.complete && result.verdict == wal_scan_verdict::intact
            && result.files == 1 && result.prepares == 3
            && result.unresolved == 0,
          "head content not scanned completely");
        require(
          result.content_end && result.content_end->incarnation() == wal(1)
            && result.content_end->position().value() == 16384,
          "head content end lost");
        require(
          out.prepares.size() == 3 && out.prepares[0].position == 4096
            && out.prepares[1].position == 8192
            && out.prepares[2].position == 12288
            && out.prepares[2].end == 16384,
          "PREPAREs not visited in WAL order");
        require(
          out.prepares[0].target == segment()
            && out.prepares[1].target == segment_b()
            && out.prepares[2].target == segment(),
          "claimed targets changed");
        require(
          out.prepares[0].child == local_fixture::read("child_a1")
            && out.prepares[1].child == local_fixture::read("child_b1")
            && out.prepares[2].child == local_fixture::read("child_a2"),
          "exact children not preserved");
        require(
          out.files.size() == 1 && out.files[0].stop == wal_scan_stop::end
            && out.files[0].file_bytes == 16384 && !out.files[0].sealed_end,
          "head report changed");

        const auto a1 = local_fixture::read("prepare_a1");
        auto body_damage = local_fixture::read("prepare_b1");
        body_damage[400] ^= 1;
        auto header_damage = local_fixture::read("prepare_b1");
        header_damage[13] ^= 1;
        auto unsupported = local_fixture::read("prepare_b1");
        put(unsupported, 32 + 124, 2, 2);
        repair(unsupported);
        const auto head = local_fixture::read("wal_header") + a1;
        struct tail_case final {
            std::string bytes;
            wal_scan_stop stop;
            std::uint64_t content_end;
            std::size_t prepares;
        };
        const std::array tails{
          tail_case{
            interleaved() + std::string(4096, '\0'),
            wal_scan_stop::unwritten,
            16384,
            3},
          tail_case{
            head + local_fixture::read("prepare_b1").substr(0, 2048),
            wal_scan_stop::torn,
            8192,
            1},
          tail_case{
            head + body_damage + local_fixture::read("prepare_a2"),
            wal_scan_stop::corrupt,
            8192,
            1},
          tail_case{head + header_damage, wal_scan_stop::corrupt, 8192, 1},
          tail_case{
            head + std::string(4096, 'x'), wal_scan_stop::malformed, 8192, 1},
          // A PREPARE that names another position is foreign here.
          tail_case{head + a1, wal_scan_stop::foreign, 8192, 1},
          // Another family's integrity-valid envelope is foreign too.
          tail_case{
            head + local_fixture::read("block_a1"),
            wal_scan_stop::foreign,
            8192,
            1}};
        for (const auto& tail : tails) {
            co_await put_wal(files, spec, wal(1), tail.bytes, drive);
            const auto tail_before = reads(head_path);
            auto damaged = take(
              co_await scan(
                files,
                owner,
                spec,
                budget,
                head_control(),
                *catalog,
                out,
                drive));
            require(
              damaged.complete && damaged.verdict == wal_scan_verdict::intact
                && damaged.content_end
                && damaged.content_end->position().value() == tail.content_end
                && out.prepares.size() == tail.prepares && out.files.size() == 1
                && out.files[0].stop == tail.stop,
              "head tail misclassified");
            if (
              const auto tail_after = reads(head_path);
              tail_after && tail_before) {
                const auto read = *tail_after - *tail_before;
                const auto size = tail.bytes.size();
                require(
                  read <= first_read(size) + 2 * (size - data_start)
                    && read >= first_read(size) + tail.content_end - data_start,
                  "a head tail was read more than its header and two "
                  "passes, or its content not read");
            }
            const auto path = take(
              take(local_paths::make(spec.root)).wal(0, wal(1)));
            require(
              (co_await read_bytes(files, path, drive)) == tail.bytes,
              "head scan changed tail bytes");
        }
        // Read on after its content, as restart reads it, the head's intact
        // PREPAREs at their own positions are survivals: never content, and
        // their WAL stamps raise what the file proves certified. Without the
        // probe nothing after the content is visited.
        {
            wal_prepare_expectation later{
              wal_write_context::make(
                wal(1), alignment(4096), runtime::file_position{12288})
                .value(),
              runtime::file_position{4096},
              segment_write_context::make(
                segment(),
                alignment(4096),
                model::segment_relative_end{1},
                runtime::file_position{4495})
                .value(),
              runtime::file_position{4096},
              model::range_routing_epoch::make(1).value(),
              batch_expected()};
            auto stamps = wal_stamps(later);
            stamps.wal_certified_end = runtime::file_position{12288};
            const auto bytes
              = head + body_damage
                + wal_wire(local_fixture::read("child_a2"), later, 32, stamps);
            co_await put_wal(files, spec, wal(1), bytes, drive);
            auto proving = scan_limits();
            proving.prove = true;
            auto proved = take(
              co_await scan(
                files,
                owner,
                spec,
                budget,
                head_control(),
                *catalog,
                out,
                drive,
                std::nullopt,
                proving));
            require(
              proved.complete && proved.content_end
                && proved.content_end->position().value() == 8192
                && out.prepares.size() == 2 && !out.prepares[0].survival
                && out.prepares[1].survival && out.prepares[1].position == 12288
                && out.prepares[1].target == segment()
                && out.prepares[1].child.empty() && out.files.size() == 1
                && out.files[0].stop == wal_scan_stop::corrupt
                && out.files[0].proven_end.value() == 12288,
              "a later intact PREPARE did not prove the head's damage");
            auto unproved = take(
              co_await scan(
                files,
                owner,
                spec,
                budget,
                head_control(),
                *catalog,
                out,
                drive));
            require(
              unproved.content_end
                && unproved.content_end->position().value() == 8192
                && out.prepares.size() == 1 && out.files.size() == 1
                && out.files[0].proven_end.value() == 4096,
              "a scan without the probe read past the head's content");
        }
        co_await put_wal(files, spec, wal(1), head + unsupported, drive);
        auto rejected = co_await scan(
          files, owner, spec, budget, head_control(), *catalog, out, drive);
        require(
          !rejected && rejected.error().code() == errc::unsupported_format,
          "unsupported PREPARE accepted or treated as a tail");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    catalog.reset();
    take(failed.outcome());
}

// A PREPARE of A1's child at `position` in the head, one member of its write
// unless it ends it, whose WAL stamp says the bytes before `certified` were
// durable when its group was formed. Of wal(1) unless `of` names another
// incarnation, whose file a recycled head could hold.
inline std::string stamped_member(
  std::uint64_t position,
  std::uint64_t certified,
  bool ends_write = true,
  model::wal_incarnation_id of = wal(1)) {
    const wal_prepare_expectation context{
      wal_write_context::make(
        of, alignment(4096), runtime::file_position{position})
        .value(),
      runtime::file_position{4096},
      segment_write_context::make(
        segment(),
        alignment(4096),
        model::segment_relative_end{0},
        runtime::file_position{4096})
        .value(),
      runtime::file_position{4096},
      model::range_routing_epoch::make(1).value(),
      batch_expected()};
    auto stamps = wal_stamps(context);
    stamps.ends_write = ends_write;
    stamps.wal_certified_end = runtime::file_position{certified};
    return wal_wire(local_fixture::read("child_a1"), context, 32, stamps);
}
// An object with one byte of its child flipped: its header still verifies
// its length. With `header`, a byte of its header, so nothing verifies it.
inline std::string flipped(std::string object, bool header = false) {
    object[header ? 13 : 32 + 168 + 8] ^= 1;
    return object;
}

// What a head's scan makes of damage, read on past it as restart reads it:
// the cases of a reader that confirms damage by the sync offsets later
// chunks carry, on a head of 4 KiB units. Damage below the end the head's
// intact PREPAREs prove certified is corruption; at or past it the tail is
// uncertified. A damaged object's stamp proves nothing, nor does a PREPARE
// of another incarnation. An object whose header holds is passed over by
// its verified length and anything else at the next unit, so an intact
// PREPARE after damage in its own unit still counts. Stamps lie on unit
// boundaries at or before their own object, so no object proves damage in
// its own unit: only a later unit's can. Nothing is changed.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> proven_heads(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await seed_targets(files, owner, spec, budget, drive);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array specs{spec};
    using catalog_type = local_wal_target_catalog<Backend, Owner>;
    const std::array<typename catalog_type::entry, 2> entries{
      {{descriptor(), segment_head()}, {descriptor_b(), segment_head_b()}}};
    auto catalog = take(
      catalog_type::make(
        files, owner, specs, 0, entries, budget, limits(), work));
    observed out;
    runtime::first_failure failed;
    try {
        const auto at = [](
                          std::uint64_t position,
                          std::uint64_t certified,
                          bool ends_write = true) {
            return stamped_member(position, certified, ends_write);
        };
        const auto stale = [](std::uint64_t position, std::uint64_t certified) {
            return stamped_member(position, certified, true, wal(2));
        };
        // A member that does not end its write takes 423 bytes, so the next
        // member of its write begins there.
        const std::uint64_t next = 4096 + 423;
        const std::uint64_t later = 8192 + 423;
        struct proof_case final {
            const char* name;
            std::string tail;
            wal_scan_stop stop;
            std::uint64_t content_end, proven_end;
            // Content PREPAREs, then intact ones visited after the content.
            std::size_t content, survivals;
        };
        const std::array cases{
          proof_case{
            "one damaged object, nothing after it",
            flipped(at(4096, 4096)),
            wal_scan_stop::corrupt,
            4096,
            4096,
            0,
            0},
          proof_case{
            "damage that hides its own length",
            flipped(at(4096, 4096), true),
            wal_scan_stop::corrupt,
            4096,
            4096,
            0,
            0},
          proof_case{
            "two damaged objects",
            flipped(at(4096, 4096)) + flipped(at(8192, 4096)),
            wal_scan_stop::corrupt,
            4096,
            4096,
            0,
            0},
          proof_case{
            "the stamp that would prove it is damaged too",
            flipped(at(4096, 4096)) + flipped(at(8192, 8192)),
            wal_scan_stop::corrupt,
            4096,
            4096,
            0,
            0},
          proof_case{
            "a later unit proves it",
            flipped(at(4096, 4096)) + at(8192, 8192),
            wal_scan_stop::corrupt,
            4096,
            8192,
            0,
            1},
          proof_case{
            "its write's members, then a later unit that proves it",
            flipped(at(4096, 4096, false)) + at(next, 4096) + at(8192, 8192),
            wal_scan_stop::corrupt,
            4096,
            8192,
            0,
            2},
          proof_case{
            "its write's members cannot prove it, and the next unit is "
            "damaged",
            flipped(at(4096, 4096, false)) + at(next, 4096)
              + flipped(at(8192, 8192)),
            wal_scan_stop::corrupt,
            4096,
            4096,
            0,
            1},
          proof_case{
            "proof after damage in its own unit",
            flipped(at(4096, 4096)) + flipped(at(8192, 8192, false))
              + at(later, 8192),
            wal_scan_stop::corrupt,
            4096,
            8192,
            0,
            1},
          proof_case{
            "proof two units on",
            flipped(at(4096, 4096)) + flipped(at(8192, 8192, false))
              + flipped(at(later, 8192)) + at(12288, 12288),
            wal_scan_stop::corrupt,
            4096,
            12288,
            0,
            1},
          proof_case{
            "a stamp that reaches only where the damage begins",
            at(4096, 4096) + flipped(at(8192, 8192)) + at(12288, 8192),
            wal_scan_stop::corrupt,
            8192,
            8192,
            1,
            1},
          proof_case{
            "another incarnation's PREPARE in its place",
            stale(4096, 4096),
            wal_scan_stop::foreign,
            4096,
            4096,
            0,
            0},
          proof_case{
            "another incarnation's PREPARE, then proof",
            stale(4096, 4096) + at(8192, 8192),
            wal_scan_stop::foreign,
            4096,
            8192,
            0,
            1},
          proof_case{
            "another incarnation's stamp proves nothing",
            stale(4096, 4096) + stale(8192, 8192),
            wal_scan_stop::foreign,
            4096,
            4096,
            0,
            0}};
        auto proving = scan_limits();
        proving.prove = true;
        const auto path = take(
          take(local_paths::make(spec.root)).wal(0, wal(1)));
        for (const auto& one : cases) {
            const auto bytes = local_fixture::read("wal_header") + one.tail;
            co_await put_wal(files, spec, wal(1), bytes, drive);
            auto scanned = take(
              co_await scan(
                files,
                owner,
                spec,
                budget,
                head_control(),
                *catalog,
                out,
                drive,
                std::nullopt,
                proving));
            const auto survivals = static_cast<std::size_t>(
              std::ranges::count_if(out.prepares, [](const seen_prepare& seen) {
                  return seen.survival;
              }));
            require(
              scanned.complete && scanned.content_end
                && scanned.content_end->position().value() == one.content_end
                && out.files.size() == 1 && out.files[0].stop == one.stop
                && out.files[0].proven_end.value() == one.proven_end
                && out.prepares.size() == one.content + one.survivals
                && survivals == one.survivals,
              one.name);
            require(
              (co_await read_bytes(files, path, drive)) == bytes,
              "a head's scan changed its bytes");
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    catalog.reset();
    take(failed.outcome());
}

// A PREPARE of the head wal(1) that ends its write or continues it, framed
// independently as the fixtures are.
inline std::string member_at(
  std::string child,
  std::uint64_t wal_position,
  segment_context target,
  std::uint64_t physical,
  std::uint64_t position,
  bool ends_write) {
    const wal_prepare_expectation context{
      wal_write_context::make(
        wal(1), alignment(4096), runtime::file_position{wal_position})
        .value(),
      runtime::file_position{4096},
      segment_write_context::make(
        target,
        alignment(4096),
        model::segment_relative_end{physical},
        runtime::file_position{position})
        .value(),
      runtime::file_position{4096},
      model::range_routing_epoch::make(1).value(),
      batch_expected()};
    auto stamps = wal_stamps(context);
    stamps.ends_write = ends_write;
    return wal_wire(std::move(child), context, 32, stamps);
}

// Packed writes in the head. A write is content only whole: its members
// before the one that sets ends_write are visited only once that member is
// intact, so a write that lost its last member, to zeros, damage or the
// file's end, ends the content where it began. Zeros where a write would
// begin are unwritten space. A cutoff at a PREPARE boundary inside a write
// scans from there. Nothing is changed.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> packed_writes(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await seed_targets(files, owner, spec, budget, drive);
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array specs{spec};
    using catalog_type = local_wal_target_catalog<Backend, Owner>;
    const std::array<typename catalog_type::entry, 2> entries{
      {{descriptor(), segment_head()}, {descriptor_b(), segment_head_b()}}};
    auto catalog = take(
      catalog_type::make(
        files, owner, specs, 0, entries, budget, limits(), work));
    observed out;
    runtime::first_failure failed;
    try {
        // One write of A1 then B1, which ends it; then A2's write alone.
        const auto a1 = member_at(
          local_fixture::read("child_a1"), 4096, segment(), 0, 4096, false);
        const std::uint64_t b1_at = 4096 + a1.size();
        const auto b1 = member_at(
          local_fixture::read("child_b1"), b1_at, segment_b(), 0, 4096, true);
        const std::uint64_t a2_at = b1_at + b1.size();
        const auto a2 = member_at(
          local_fixture::read("child_a2"), a2_at, segment(), 1, 8192, true);
        const auto a2_open = member_at(
          local_fixture::read("child_a2"), a2_at, segment(), 1, 8192, false);
        const std::uint64_t whole_end = a2_at + a2.size();
        const auto header = local_fixture::read("wal_header");
        require(
          a1.size() % 4096 != 0 && a2_at % 4096 == 0 && whole_end % 4096 == 0,
          "the packed writes do not have the shape the cases need");
        const auto path = take(
          take(local_paths::make(spec.root)).wal(0, wal(1)));

        co_await put_wal(files, spec, wal(1), header + a1 + b1 + a2, drive);
        auto whole = take(
          co_await scan(
            files, owner, spec, budget, head_control(), *catalog, out, drive));
        require(
          whole.complete && whole.verdict == wal_scan_verdict::intact
            && whole.content_end
            && whole.content_end->position().value() == whole_end
            && out.prepares.size() == 3 && out.prepares[0].position == 4096
            && out.prepares[1].position == b1_at
            && out.prepares[1].target == segment_b()
            && out.prepares[2].position == a2_at && out.files.size() == 1
            && out.files[0].stop == wal_scan_stop::end
            && out.files[0].content_end.value() == whole_end,
          "packed writes were not scanned whole and in order");

        auto damaged_b1 = b1;
        damaged_b1[400] ^= 1;
        struct tail_case final {
            std::string bytes;
            wal_scan_stop stop;
            std::uint64_t content_end;
            std::size_t prepares;
        };
        const std::array tails{
          tail_case{
            header + a1 + std::string(8192, '\0'),
            wal_scan_stop::torn,
            4096,
            0},
          tail_case{header + a1, wal_scan_stop::torn, 4096, 0},
          tail_case{header + a1 + damaged_b1, wal_scan_stop::corrupt, 4096, 0},
          tail_case{
            header + a1 + b1 + std::string(4096, '\0'),
            wal_scan_stop::unwritten,
            a2_at,
            2},
          tail_case{
            header + a1 + b1 + a2_open + std::string(4096, '\0'),
            wal_scan_stop::torn,
            a2_at,
            2}};
        for (const auto& tail : tails) {
            co_await put_wal(files, spec, wal(1), tail.bytes, drive);
            auto damaged = take(
              co_await scan(
                files,
                owner,
                spec,
                budget,
                head_control(),
                *catalog,
                out,
                drive));
            require(
              damaged.complete && damaged.verdict == wal_scan_verdict::intact
                && damaged.content_end
                && damaged.content_end->position().value() == tail.content_end
                && out.prepares.size() == tail.prepares && out.files.size() == 1
                && out.files[0].stop == tail.stop,
              "a packed head's tail was misclassified, or a member of an "
              "unfinished write was visited");
            require(
              (co_await read_bytes(files, path, drive)) == tail.bytes,
              "the head scan changed bytes");
        }

        // From a cutoff inside the first write.
        const auto inside = take(
          local_wal_cursor::make(wal(1), runtime::file_position{b1_at}));
        co_await put_wal(files, spec, wal(1), header + a1 + b1 + a2, drive);
        auto resumed = take(
          co_await scan(
            files,
            owner,
            spec,
            budget,
            head_control(),
            *catalog,
            out,
            drive,
            inside));
        require(
          resumed.complete && resumed.content_end
            && resumed.content_end->position().value() == whole_end
            && out.prepares.size() == 2 && out.prepares[0].position == b1_at
            && out.prepares[1].position == a2_at && out.files.size() == 1
            && out.files[0].begin.value() == b1_at,
          "a scan from a cutoff inside a write did not start there");
        co_await put_wal(
          files,
          spec,
          wal(1),
          header + a1 + std::string(b1.size(), '\0'),
          drive);
        auto cut = take(
          co_await scan(
            files,
            owner,
            spec,
            budget,
            head_control(),
            *catalog,
            out,
            drive,
            inside));
        require(
          cut.complete && cut.content_end
            && cut.content_end->position().value() == b1_at
            && out.prepares.empty() && out.files.size() == 1
            && out.files[0].stop == wal_scan_stop::torn,
          "zeros inside a write after a cutoff were not a tear");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    catalog.reset();
    take(failed.outcome());
}

// The PREPARE a recycled file's earlier incarnation left at 8192: A1's,
// placed there under another incarnation, so only the scan's identity
// refuses it, and its child differs from B1's, which belongs there now.
inline std::string earlier_prepare() {
    auto stale = local_fixture::read("prepare_a1");
    const auto earlier = wal(2);
    const auto raw = earlier.bytes();
    for (std::size_t i = 0; i < raw.size(); ++i)
        stale[32 + i] = std::bit_cast<char>(raw[i]);
    put(stale, 32 + 16, 8192, 8);
    repair(stale);
    return stale;
}

// A recycled head's own PREPAREs, then its earlier incarnation's: the
// content ends at its own last PREPARE, the earlier bytes are foreign and
// never damage. A new PREPARE torn over the earlier bytes never completes:
// its checksum fails and it ends the content too. Nothing is changed.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> recycled_head(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await seed_targets(files, owner, spec, budget, drive);
    fixed_targets targets{{target(segment()), target(segment_b())}};
    observed out;
    const auto head = local_fixture::read("wal_header")
                      + local_fixture::read("prepare_a1");
    const auto stale = earlier_prepare();
    // B1's first sectors, inside its child, then the earlier bytes.
    const auto torn = local_fixture::read("prepare_b1").substr(0, 256)
                      + stale.substr(256);
    const auto path = take(take(local_paths::make(spec.root)).wal(0, wal(1)));
    for (const auto& [bytes, stop] :
         {std::pair{head + stale, wal_scan_stop::foreign},
          std::pair{head + torn, wal_scan_stop::corrupt}}) {
        co_await put_wal(files, spec, wal(1), bytes, drive);
        auto result = take(
          co_await scan(
            files, owner, spec, budget, head_control(), targets, out, drive));
        require(
          result.complete && result.verdict == wal_scan_verdict::intact
            && result.files == 1 && result.prepares == 1 && result.content_end
            && result.content_end->incarnation() == wal(1)
            && result.content_end->position().value() == 8192
            && out.files.size() == 1 && out.files[0].stop == stop,
          "a recycled head's earlier bytes became content or damage");
        require(
          (co_await read_bytes(files, path, drive)) == bytes,
          "the scan changed a recycled head");
    }
}

// Rotated files are certified to their sealed end; slack after it is not
// content, and a cutoff starts the scan inside its file.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> rotated_chain(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await seed_targets(files, owner, spec, budget, drive);
    fixed_targets targets{{target(segment()), target(segment_b())}};
    observed out;
    const auto successor = local_fixture::read("wal_successor_gap");
    co_await put_wal(files, spec, wal(9), successor, drive);
    co_await put_wal(files, spec, wal(1), interleaved(), drive);
    auto result = take(
      co_await scan(
        files, owner, spec, budget, chain_control(), targets, out, drive));
    require(
      result.complete && result.verdict == wal_scan_verdict::intact
        && result.files == 2 && result.prepares == 3,
      "rotated chain not scanned completely");
    require(
      out.files.size() == 2 && out.files[0].incarnation == wal(1)
        && out.files[1].incarnation == wal(9) && out.files[0].sealed_end
        && out.files[0].sealed_end->value() == 16384
        && out.files[0].stop == wal_scan_stop::end
        && out.files[0].slack.value() == 0,
      "chain not scanned in WAL order");
    require(
      result.content_end && result.content_end->incarnation() == wal(9)
        && result.content_end->position().value() == 4096
        && out.files[1].stop == wal_scan_stop::end,
      "empty head content end lost");

    co_await put_wal(
      files, spec, wal(1), interleaved() + std::string(4096, '\0'), drive);
    result = take(
      co_await scan(
        files, owner, spec, budget, chain_control(), targets, out, drive));
    require(
      result.verdict == wal_scan_verdict::intact && out.prepares.size() == 3
        && out.files[0].slack.value() == 4096 && !out.files[0].nonzero_slack,
      "zero slack treated as content");
    co_await put_wal(
      files, spec, wal(1), interleaved() + std::string(4096, 'x'), drive);
    result = take(
      co_await scan(
        files, owner, spec, budget, chain_control(), targets, out, drive));
    require(
      result.verdict == wal_scan_verdict::intact && out.prepares.size() == 3
        && out.files[0].nonzero_slack,
      "nonzero slack hidden or parsed");
    // A recycled file's earlier PREPARE past the sealed end is slack too.
    co_await put_wal(
      files, spec, wal(1), interleaved() + earlier_prepare(), drive);
    result = take(
      co_await scan(
        files, owner, spec, budget, chain_control(), targets, out, drive));
    require(
      result.verdict == wal_scan_verdict::intact && out.prepares.size() == 3
        && out.files[0].nonzero_slack
        && out.files[0].slack.value() == earlier_prepare().size(),
      "an earlier incarnation's PREPARE past the sealed end was parsed");

    const auto cutoff = take(
      local_wal_cursor::make(wal(1), runtime::file_position{8192}));
    co_await put_wal(files, spec, wal(1), interleaved(), drive);
    result = take(
      co_await scan(
        files,
        owner,
        spec,
        budget,
        chain_control(),
        targets,
        out,
        drive,
        cutoff));
    require(
      result.prepares == 2 && out.prepares[0].position == 8192
        && out.files[0].begin.value() == 8192,
      "checkpoint cutoff ignored");

    auto damaged = interleaved();
    damaged[8192 + 400] ^= 1;
    co_await put_wal(files, spec, wal(1), damaged, drive);
    result = take(
      co_await scan(
        files, owner, spec, budget, chain_control(), targets, out, drive));
    require(
      result.complete && result.verdict == wal_scan_verdict::corrupt
        && result.files == 1 && out.files.size() == 1
        && out.files[0].stop == wal_scan_stop::corrupt
        && out.files[0].content_end.value() == 8192 && !result.content_end,
      "damage in a sealed prefix not reported as corruption");
    co_await put_wal(
      files,
      spec,
      wal(1),
      interleaved().substr(0, 8192) + std::string(8192, '\0'),
      drive);
    result = take(
      co_await scan(
        files, owner, spec, budget, chain_control(), targets, out, drive));
    require(
      result.verdict == wal_scan_verdict::corrupt
        && out.files[0].stop == wal_scan_stop::unwritten,
      "zeroed certified bytes treated as an end");
    // Missing certified bytes reject before any content is scanned.
    co_await put_wal(
      files, spec, wal(1), interleaved().substr(0, 12288), drive);
    auto rejected = co_await scan(
      files, owner, spec, budget, chain_control(), targets, out, drive);
    require(
      !rejected && out.prepares.empty(),
      "a rotated file shorter than its sealed end was accepted");
}

// Claimed targets select context only through independent placements.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> target_resolution(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await seed_targets(files, owner, spec, budget, drive);
    observed out;
    co_await put_wal(files, spec, wal(1), interleaved(), drive);
    fixed_targets only_a{{target(segment())}};
    auto result = take(
      co_await scan(
        files, owner, spec, budget, head_control(), only_a, out, drive));
    require(
      result.prepares == 3 && result.unresolved == 1
        && result.content_end->position().value() == 16384
        && out.prepares[1].resolution == wal_target_resolution::unknown
        && out.prepares[1].child.empty()
        && out.prepares[0].resolution == wal_target_resolution::resolved,
      "unknown target gained authority or ended the content");
    require(only_a.lookups == 3, "target resolved more than once per PREPARE");

    // A known target whose data starts after the claimed positions: A1 at
    // 4096 and A2, packed after it, at 4495. B's own target resolves.
    fixed_targets late{{target(segment(), 8192), target(segment_b())}};
    result = take(
      co_await scan(
        files, owner, spec, budget, head_control(), late, out, drive));
    require(
      result.unresolved == 2
        && out.prepares[0].resolution == wal_target_resolution::misplaced
        && out.prepares[1].resolution == wal_target_resolution::resolved
        && out.prepares[2].resolution == wal_target_resolution::misplaced,
      "misplaced target accepted");

    // Identity is checked before any lookup.
    fixed_targets counted{{target(segment()), target(segment_b())}};
    co_await put_wal(
      files,
      spec,
      wal(1),
      local_fixture::read("wal_header") + local_fixture::read("prepare_a1")
        + local_fixture::read("prepare_a1"),
      drive);
    result = take(
      co_await scan(
        files, owner, spec, budget, head_control(), counted, out, drive));
    require(
      counted.lookups == 1 && out.files[0].stop == wal_scan_stop::foreign,
      "foreign PREPARE reached target resolution");

    fixed_targets lost{{}, 0, errc::unavailable};
    co_await put_wal(files, spec, wal(1), interleaved(), drive);
    auto unavailable = co_await scan(
      files, owner, spec, budget, head_control(), lost, out, drive);
    require(
      !unavailable && unavailable.error().code() == errc::unavailable
        && out.prepares.empty(),
      "unavailable target treated as content or damage");

    // The catalog verifies each target on its device; a catalog entry with
    // no files is an error, never an unknown target.
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const std::array specs{spec};
    using catalog_type = local_wal_target_catalog<Backend, Owner>;
    auto missing = descriptor_b();
    missing.segment = segment_context::make(
                        segment_b().cluster(),
                        segment_b().topic(),
                        segment_b().range(),
                        segment_b().segment(),
                        model::segment_generation::make(3).value())
                        .value();
    auto missing_head
      = segment_header::make(missing.segment, alignment(4096)).value();
    const std::array<typename catalog_type::entry, 2> entries{
      {{descriptor(), segment_head()}, {missing, missing_head}}};
    auto catalog = take(
      catalog_type::make(
        files, owner, specs, 0, entries, budget, limits(), work));
    runtime::first_failure failed;
    try {
        auto resolved = take(co_await drive.lifecycle((*catalog)(segment())));
        require(
          resolved && resolved->data_start.value() == 4096
            && resolved->segment == segment(),
          "catalog target lost its verified layout");
        auto unknown = take(co_await drive.lifecycle((*catalog)(segment_b())));
        require(!unknown, "uncatalogued target resolved");
        auto absent = co_await drive.lifecycle((*catalog)(missing.segment));
        require(
          !absent && absent.error().code() == errc::not_found,
          "catalogued target with no files resolved");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    catalog.reset();
    take(failed.outcome());
}

} // namespace kwaque::storage::testing::scan_contract
