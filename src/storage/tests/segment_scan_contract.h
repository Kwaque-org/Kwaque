#pragma once

#include "src/base/units.h"
#include "src/storage/recovery_cases.h"
#include "src/storage/recovery_pins.h"
#include "src/storage/segment_scan.h"
#include "src/storage/tests/footer_test_support.h"
#include "src/storage/tests/wal_scan_contract.h"

#include <algorithm>
#include <map>
#include <string>
#include <vector>

namespace kwaque::storage::testing::segment_scan_contract {
using installation_contract::buffer_async;
using installation_contract::descriptor;
using installation_contract::limits;
using installation_contract::segment;
using installation_contract::segment_head;
using reader_contract::active_boundary;
using reader_contract::put_wal;
using reader_contract::wal;
using scan_contract::descriptor_b;
using scan_contract::head_control;
using scan_contract::segment_b;
using scan_contract::segment_head_b;
using store_contract::read_bytes;
using store_contract::require;
using store_contract::take;
using store_contract::write_bytes;

inline segment_scan_limits scan_limits() {
    return {
      limits(),
      {.window_bytes = byte_count{8_KiB}, .read_ahead = 2},
      byte_count{512_KiB},
      byte_count{64_KiB}};
}
inline segment_history_context history_a() {
    return reader_contract::history();
}
inline segment_history_context history_b() {
    return {
      segment_b(),
      alignment(4096),
      runtime::file_position{4096},
      model::range_logical_end{1000},
      model::segment_relative_end{0}};
}
inline std::string active_a() {
    return local_fixture::read("data_header_a")
           + local_fixture::read("block_a1") + local_fixture::read("block_a2")
           + local_fixture::read("footer_a");
}
// One request of segment A, framed for this store's segment exactly as the
// block fixtures are.
inline std::string block_a(
  std::uint64_t logical,
  std::uint64_t physical,
  std::uint64_t position,
  bool compressed = false) {
    return block_wire(
      data_child(logical, false, compressed),
      {segment_write_context::make(
         segment(),
         alignment(4096),
         model::segment_relative_end{physical},
         runtime::file_position{position})
         .value(),
       runtime::file_position{4096},
       batch_expected()});
}
// A third request of segment A after footer_a, at 16384.
inline std::string block_a3() { return block_a(102, 2, 16384); }
// The footer naming the prefix through block_a3, at 20480.
inline std::string footer_a3(std::uint32_t crc_delta = 0) {
    const auto stored = local_fixture::read("block_a1")
                        + local_fixture::read("block_a2")
                        + local_fixture::read("footer_a") + block_a3();
    return footer_wire(
      {scope(100, 103, 0, 3, 4096, 20480),
       3,
       scope(102, 103, 2, 3, 16384, 20480),
       crc(stored) + crc_delta},
      {history_a(), runtime::file_position{20480}});
}
inline local_footer_reference
footer_reference(const std::string& footer, std::uint64_t position) {
    return take(
      local_footer_reference::make(
        runtime::file_position{position},
        byte_count{footer.size()},
        6,
        codec::immutable_object_digest{exact_digest(footer)}));
}
inline runtime::file_path
data_path(const local_device_spec& spec, segment_context context) {
    return take(take(local_paths::make(spec.root))
                  .segment_file(
                    0,
                    {context.segment(), context.generation()},
                    local_segment_file::data));
}

// Objects a walk visited, in file order.
struct scanned final {
    bool block;
    std::uint64_t position;
};
struct observe_objects final {
    std::vector<scanned>* out;
    seastar::future<runtime::result<bool>>
    operator()(const segment_scanned_object& value) const {
        out->push_back(
          value.block
            ? scanned{true, value.block->descriptor().coverage().bytes().begin().value()}
            : scanned{false, value.footer->footer.position().value()});
        return seastar::make_ready_future<runtime::result<bool>>(true);
    }
};

template<typename Backend, typename Owner, typename Driver>
seastar::future<segment_scan_report> walk(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  segment_history_context history,
  std::optional<local_footer_reference> pin,
  std::vector<scanned>& out,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    out.clear();
    co_return take(
      co_await drive.lifecycle(scan_local_segment(
        files,
        owner,
        spec,
        0,
        history,
        pin,
        budget,
        scan_limits(),
        work,
        observe_objects{&out})));
}

// A walk resumes after its pinned footer without reading the prefix it
// covers, checks every later footer in the same pass, and ends at its first
// damage with the last verifying footer as the recovered boundary. No byte
// changes.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> resume_and_tails(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await scan_contract::seed_targets(files, owner, spec, budget, drive);
    const auto path = data_path(spec, segment());
    const auto footer_a = local_fixture::read("footer_a");
    const auto pin = footer_reference(footer_a, 12288);
    require(pin == active_boundary(), "pinned footer reference changed");
    require(
      block_a(100, 0, 4096) == local_fixture::read("block_a1")
        && block_a(101, 1, 8192) == local_fixture::read("block_a2"),
      "segment A blocks are not framed as the block fixtures");
    const auto header = local_fixture::read("data_header_a");
    std::vector<scanned> out;
    const auto expect = [&](
                          const segment_scan_report& report,
                          std::optional<std::uint64_t> boundary,
                          std::uint32_t suffix,
                          std::uint64_t content_end,
                          segment_scan_stop stop,
                          segment_scan_verdict verdict,
                          const char* message) {
        require(
          report.complete && report.suffix_blocks == suffix
            && report.content_end.value() == content_end
            && report.stop == stop && report.verdict == verdict
            && (boundary
                  ? report.boundary
                      && report.boundary->footer.position().value() == *boundary
                  : !report.boundary),
          message);
    };

    // From the data start: two blocks under footer_a.
    co_await write_bytes(files, path, active_a(), drive);
    auto report = co_await walk(
      files, owner, spec, budget, history_a(), std::nullopt, out, drive);
    expect(
      report,
      12288,
      0,
      16384,
      segment_scan_stop::end,
      segment_scan_verdict::clean,
      "unpinned walk lost its footer-covered history");
    require(
      report.boundary->footer == pin
        && report.boundary->fields.coverage
             == scope(100, 102, 0, 2, 4096, 12288)
        && report.boundary->fields.block_count == 2
        && report.boundary->fields.data_crc32c
             == crc(
               local_fixture::read("block_a1")
               + local_fixture::read("block_a2")),
      "recovered boundary differs from the footer's independent fields");
    require(
      out.size() == 3 && out[0].block && out[0].position == 4096 && out[1].block
        && out[1].position == 8192 && !out[2].block && out[2].position == 12288,
      "walk did not visit complete objects in file order");

    // Resumed at the pin: only later objects are visited, and their footer
    // verifies against the seeded prefix.
    const auto full = active_a() + block_a3() + footer_a3();
    co_await write_bytes(files, path, full, drive);
    report = co_await walk(
      files, owner, spec, budget, history_a(), pin, out, drive);
    expect(
      report,
      20480,
      0,
      24576,
      segment_scan_stop::end,
      segment_scan_verdict::clean,
      "resumed walk did not reach the later footer");
    require(
      report.pin == pin && out.size() == 2 && out[0].block
        && out[0].position == 16384 && !out[1].block
        && report.boundary->fields.coverage
             == scope(100, 103, 0, 3, 4096, 20480),
      "resumed walk visited the pinned prefix or lost the later footer");
    // The pinned prefix is never read again: zeroing it changes nothing.
    const auto hollow = header + std::string(8192, '\0') + footer_a + block_a3()
                        + footer_a3();
    co_await write_bytes(files, path, hollow, drive);
    report = co_await walk(
      files, owner, spec, budget, history_a(), pin, out, drive);
    expect(
      report,
      20480,
      0,
      24576,
      segment_scan_stop::end,
      segment_scan_verdict::clean,
      "resumed walk read the pinned prefix");
    // Without the pin the same bytes end at the data start.
    report = co_await walk(
      files, owner, spec, budget, history_a(), std::nullopt, out, drive);
    expect(
      report,
      std::nullopt,
      0,
      4096,
      segment_scan_stop::unwritten,
      segment_scan_verdict::uncertified_tail,
      "an unpinned zero prefix was not damage");
    require(
      (co_await read_bytes(files, path, drive)) == hollow,
      "scanning changed segment bytes");

    // Tails after the pin: each ends the content at its first damage, the
    // pin stays the boundary and a later valid object is never evidence.
    auto torn_block = block_a3();
    torn_block.resize(2048);
    auto corrupt_block = block_a3();
    corrupt_block[40] ^= 1;
    struct tail final {
        std::string bytes;
        std::uint32_t suffix;
        std::uint64_t content_end;
        segment_scan_stop stop;
    };
    const std::array tails{
      tail{std::string(8192, '\0'), 0, 16384, segment_scan_stop::unwritten},
      tail{
        std::string(4096, '\0') + block_a3() + footer_a3(),
        0,
        16384,
        segment_scan_stop::unwritten},
      tail{
        block_a3() + std::string(4096, '\0'),
        1,
        20480,
        segment_scan_stop::unwritten},
      tail{torn_block, 0, 16384, segment_scan_stop::torn},
      tail{corrupt_block + footer_a3(), 0, 16384, segment_scan_stop::corrupt},
      // A footer whose data CRC does not cover the scanned bytes.
      tail{block_a3() + footer_a3(1), 1, 20480, segment_scan_stop::corrupt},
      // A footer naming another prefix than the one scanned.
      tail{
        block_a3()
          + footer_wire(
            {scope(100, 102, 0, 2, 4096, 12288),
             2,
             scope(101, 102, 1, 2, 8192, 12288),
             crc(
               local_fixture::read("block_a1")
               + local_fixture::read("block_a2"))},
            {history_a(), runtime::file_position{20480}}),
        1,
        20480,
        segment_scan_stop::malformed},
      // Another segment's block at the next slot.
      tail{
        local_fixture::read("block_b1"), 0, 16384, segment_scan_stop::foreign},
    };
    for (const auto& test : tails) {
        const auto bytes = active_a() + test.bytes;
        co_await write_bytes(files, path, bytes, drive);
        report = co_await walk(
          files, owner, spec, budget, history_a(), pin, out, drive);
        expect(
          report,
          12288,
          test.suffix,
          test.content_end,
          test.stop,
          segment_scan_verdict::uncertified_tail,
          "a tail after the pin was not classified at its first damage");
        require(
          (co_await read_bytes(files, path, drive)) == bytes,
          "scanning a tail changed segment bytes");
    }

    // Damage to the pinned footer is proven corruption, never a tail.
    auto changed = footer_a;
    changed[1000] ^= 1;
    repair(changed);
    const std::array<std::pair<std::string, segment_scan_stop>, 3> pinned{
      {{header + local_fixture::read("block_a1")
          + local_fixture::read("block_a2") + changed,
        segment_scan_stop::corrupt},
       {active_a().substr(0, 14336), segment_scan_stop::torn},
       {header + local_fixture::read("block_a1")
          + local_fixture::read("block_a2") + std::string(4096, '\0'),
        segment_scan_stop::unwritten}}};
    for (const auto& [bytes, stop] : pinned) {
        co_await write_bytes(files, path, bytes, drive);
        report = co_await walk(
          files, owner, spec, budget, history_a(), pin, out, drive);
        require(
          report.verdict == segment_scan_verdict::corrupt && report.stop == stop
            && !report.boundary && out.empty(),
          "damage to the pinned footer was not proven corruption");
    }
    co_await write_bytes(files, path, active_a().substr(0, 8192), drive);
    report = co_await walk(
      files, owner, spec, budget, history_a(), pin, out, drive);
    require(
      report.verdict == segment_scan_verdict::corrupt
        && report.stop == segment_scan_stop::torn,
      "a file shorter than its pin was not proven corruption");
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<local_recovery_generation> open_generation(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_return take(
      co_await drive.lifecycle(open_recovery_generation(
        files, owner, spec, 0, descriptor(), budget, limits(), work)));
}
template<typename Backend, typename Owner, typename Driver>
seastar::future<runtime::result<void>> try_open_generation(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto opened = co_await drive.lifecycle(open_recovery_generation(
      files, owner, spec, 0, descriptor(), budget, limits(), work));
    if (!opened) co_return runtime::failure(opened.error());
    if (opened->owner) {
        opened->owner->retire();
        take(co_await drive.lifecycle(opened->owner->close()));
    }
    co_return runtime::result<void>{};
}
template<typename Driver>
seastar::future<>
close_generation(local_recovery_generation& generation, Driver drive) {
    if (!generation.owner) co_return;
    generation.owner->retire();
    take(co_await drive.lifecycle(generation.owner->close()));
    generation.owner.reset();
}

// Restart selects each publication by its fixed path and opens every root
// against the pins it carries: a missing index is derived-state loss, while a
// missing or wrong mandatory object, boundary or root rejects even with
// correct checksums.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> publication_pins(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await reader_contract::seed_segment(
      files, owner, spec, budget, work, drive);
    const auto paths = take(local_paths::make(spec.root));
    const local_segment_name name{segment().segment(), segment().generation()};
    const auto published = take(
      paths.segment_file(0, name, local_segment_file::published));
    const auto object = [&](std::uint64_t sequence) {
        return take(
          paths.object(0, name, local_object_sequence::make(sequence).value()));
    };

    // Active, pinning footer_a: the scan resumes there.
    auto generation = co_await open_generation(
      files, owner, spec, budget, drive);
    require(
      generation.publication.state == local_object_state::active
        && generation.resume == active_boundary() && !generation.index_rebuild
        && generation.history == history_a(),
      "active publication did not yield its pinned resume point");
    co_await close_generation(generation, drive);
    co_await write_bytes(
      files, published, local_fixture::read("publication_snapshot"), drive);
    generation = co_await open_generation(files, owner, spec, budget, drive);
    require(
      generation.resume == active_boundary() && generation.owner
        && generation.owner->pin()->root(
          local_root_kind::completed_retry_snapshot),
      "active publication lost its completed-retry snapshot");
    co_await close_generation(generation, drive);

    // Sealed, with every root.
    co_await write_bytes(
      files, published, local_fixture::read("publication_all_roots"), drive);
    generation = co_await open_generation(files, owner, spec, budget, drive);
    require(
      generation.publication.state == local_object_state::sealed
        && generation.generation.value() == 3 && !generation.resume
        && !generation.index_rebuild,
      "sealed publication roots did not all verify");
    co_await close_generation(generation, drive);

    // A missing or damaged index is derived state only.
    const auto index = object(42);
    take(co_await drive.lifecycle(files.remove_file(index)));
    generation = co_await open_generation(files, owner, spec, budget, drive);
    require(
      generation.owner && generation.index_rebuild
        && generation.index_rebuild->code() == errc::not_found,
      "a missing index was not derived-state loss");
    co_await close_generation(generation, drive);
    co_await write_bytes(
      files,
      index,
      local_fixture::read("index_root") + local_fixture::read("index_page"),
      drive);

    // Mandatory evidence: missing, or a valid object other than the pinned
    // one, rejects.
    const auto retry = object(41);
    const auto snapshot = object(40);
    const auto rejects = [&](const char* message) -> seastar::future<> {
        auto result = co_await try_open_generation(
          files, owner, spec, budget, drive);
        require(!result, message);
    };
    take(co_await drive.lifecycle(files.remove_file(retry)));
    co_await rejects("a missing sealed retry root was accepted");
    co_await write_bytes(
      files, retry, local_fixture::read("snapshot_page"), drive);
    co_await rejects("another valid object stood in for sealed retry pages");
    co_await write_bytes(
      files, retry, local_fixture::read("sealed_retry_page"), drive);
    co_await write_bytes(
      files,
      snapshot,
      local_fixture::read("root_page_digest")
        + local_fixture::read("snapshot_page"),
      drive);
    co_await rejects("a snapshot root with another identity was accepted");
    co_await write_bytes(
      files,
      snapshot,
      local_fixture::read("snapshot_root")
        + local_fixture::read("snapshot_page"),
      drive);
    const auto data = data_path(spec, segment());
    auto sealed = local_fixture::read("sealed_a");
    sealed[1000] ^= 1;
    repair(sealed);
    co_await write_bytes(files, data, active_a() + sealed, drive);
    co_await rejects("a sealed root other than the pinned one was accepted");
    co_await write_bytes(
      files, data, active_a() + local_fixture::read("sealed_a"), drive);
    take(co_await try_open_generation(files, owner, spec, budget, drive));
}

// One batch slot's or file region's classification.
struct classified final {
    // Absent for a WAL file's region.
    std::optional<segment_context> segment;
    std::uint64_t position;
    std::string label;
    bool operator==(const classified&) const = default;
};
struct classification final {
    std::vector<classified> slots;
    std::vector<classified> regions;
    bool operator==(const classification&) const = default;
};
struct retained final {
    std::uint64_t position, physical;
    std::string bytes;
    bool matched{false};
};
struct segment_evidence final {
    segment_history_context history;
    segment_scan_report report;
    std::vector<retained> blocks;
    // The last PREPARE target seen for this segment, in WAL order.
    std::uint64_t last_target{0};

    [[nodiscard]] std::uint64_t boundary() const {
        return report.boundary
                 ? report.boundary->fields.coverage.bytes().end().value()
                 : history.data_start.value();
    }
};
struct retain_blocks final {
    std::vector<retained>* out;
    seastar::future<runtime::result<bool>>
    operator()(const segment_scanned_object& value) const {
        if (value.block) {
            const auto coverage = value.block->descriptor().coverage();
            out->push_back(
              {coverage.bytes().begin().value(),
               coverage.physical().begin().value(),
               flat(value.block->bytes())});
        }
        return seastar::make_ready_future<runtime::result<bool>>(true);
    }
};
// The scanned block, decoded again from its retained exact bytes.
inline seastar::future<segment_block>
decode_retained(const retained& block, const segment_history_context& history) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    bytes::fragmented_buffer_parser input{
      co_await buffer_async(block.bytes, 4096)};
    auto decoded = co_await decode_segment_block(
      input,
      {take(
         segment_write_context::make(
           history.segment,
           history.alignment,
           model::segment_relative_end{block.physical},
           runtime::file_position{block.position})),
       history.data_start,
       {history.segment.topic(), history.segment.range()},
       history.profile},
      reserve(input, work),
      work,
      {},
      codec::input_boundary::complete);
    require(decoded.has_value(), "a retained block did not decode again");
    co_return std::move(decoded->value);
}
inline std::string label_of(const recovery_slot& slot) {
    return std::string{classify_recovery(batch_parameters(slot)).label};
}
// Joins each PREPARE with its target segment's scanned evidence. The test
// keeps that evidence resident; the bounded merge is separate work.
struct classify_prepares final {
    std::vector<segment_evidence>* segments;
    std::vector<classified>* out;
    seastar::future<runtime::result<bool>>
    operator()(const wal_scanned_prepare& value) const {
        if (!value.prepare) co_return true;
        const auto target = value.prepare->target();
        auto found = std::find_if(
          segments->begin(), segments->end(), [&](const auto& evidence) {
              return evidence.history.segment == target.segment();
          });
        if (found == segments->end()) co_return true;
        const auto position = target.position().value();
        recovery_slot slot{value.prepare};
        slot.covered = position < found->boundary();
        slot.ordered = position > found->last_target;
        found->last_target = position;
        auto block = std::find_if(
          found->blocks.begin(), found->blocks.end(), [&](const auto& kept) {
              return kept.position == position;
          });
        slot.placed = found->history.alignment.aligned(target.position())
                      && target.position() >= found->history.data_start
                      && (block != found->blocks.end() || !slot.covered);
        std::optional<segment_block> decoded;
        if (block != found->blocks.end()) {
            block->matched = true;
            decoded.emplace(co_await decode_retained(*block, found->history));
            slot.block = &*decoded;
            auto difference = compare_recovery_copies(*value.prepare, *decoded);
            if (!difference) throw std::runtime_error("copies not comparable");
            slot.difference = *difference;
        }
        out->push_back({target.segment(), position, label_of(slot)});
        co_return true;
    }
};
struct collect_files final {
    std::vector<wal_file_scan>* out;
    seastar::future<runtime::result<void>>
    operator()(const wal_file_scan& value) const {
        out->push_back(value);
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::result<void>{});
    }
};

// Scans both copies of every request and classifies each slot and file region
// from those bytes alone.
template<typename Backend, typename Owner, typename Driver>
seastar::future<classification> classify_store(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    std::vector<segment_evidence> segments;
    for (const auto& history : {history_a(), history_b()}) {
        segment_evidence evidence{history, {}, {}};
        evidence.report = take(
          co_await drive.lifecycle(scan_local_segment(
            files,
            owner,
            spec,
            0,
            history,
            std::nullopt,
            budget,
            scan_limits(),
            work,
            retain_blocks{&evidence.blocks})));
        segments.push_back(std::move(evidence));
    }
    const std::array specs{spec};
    using catalog_type = local_wal_target_catalog<Backend, Owner>;
    const std::array<typename catalog_type::entry, 2> entries{
      {{descriptor(), segment_head()}, {descriptor_b(), segment_head_b()}}};
    auto catalog = take(
      catalog_type::make(
        files, owner, specs, 0, entries, budget, limits(), work));
    classification out;
    std::vector<wal_file_scan> wal_files;
    take(
      co_await drive.lifecycle(scan_local_wal(
        files,
        owner,
        spec,
        0,
        head_control(),
        std::nullopt,
        budget,
        scan_contract::scan_limits(),
        work,
        *catalog,
        classify_prepares{&segments, &out.slots},
        collect_files{&wal_files})));
    // Blocks no PREPARE named, then each file's region.
    for (const auto& evidence : segments) {
        for (const auto& block : evidence.blocks) {
            if (block.matched) continue;
            const auto decoded = co_await decode_retained(
              block, evidence.history);
            recovery_slot slot;
            slot.block = &decoded;
            slot.covered = block.position < evidence.boundary();
            out.slots.push_back(
              {evidence.history.segment, block.position, label_of(slot)});
        }
        out.regions.push_back(
          {evidence.history.segment,
           evidence.report.content_end.value(),
           std::string{
             classify_recovery(
               region_parameters(
                 false,
                 evidence.report.stop != segment_scan_stop::end,
                 evidence.report.verdict == segment_scan_verdict::corrupt))
               .label}});
    }
    for (const auto& file : wal_files)
        out.regions.push_back(
          {{},
           file.content_end.value(),
           std::string{
             classify_recovery(
               region_parameters(false, file.stop != wal_scan_stop::end, false))
               .label}});
    co_return out;
}

// Every matrix row reachable from scanned bytes, each derived again from the
// same bytes on the next restart.
template<typename Backend, typename Owner, typename Driver>
seastar::future<> recovery_classification(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await scan_contract::seed_targets(files, owner, spec, budget, drive);
    const auto a = data_path(spec, segment());
    co_await write_bytes(
      files,
      data_path(spec, segment_b()),
      local_fixture::read("data_header_b") + local_fixture::read("block_b1")
        + local_fixture::read("footer_b"),
      drive);
    const auto label = [](
                         const std::vector<classified>& all,
                         segment_context context,
                         std::uint64_t position) {
        for (const auto& item : all)
            if (item.segment == context && item.position == position)
                return item.label;
        return std::string{};
    };
    const auto header_a = local_fixture::read("data_header_a");
    const auto interleaved = scan_contract::interleaved();
    auto damaged_a2 = interleaved;
    damaged_a2[12288 + 12] ^= 1;
    struct shape final {
        std::string segment_a, wal;
        // A1 and A2 in segment A, then B1.
        std::array<std::string_view, 3> slots;
        std::string_view region_a, region_wal;
        const char* message;
    };
    const std::array shapes{
      shape{
        active_a(),
        interleaved,
        {"@satisfied", "@satisfied", "@satisfied"},
        "@content",
        "@content",
        "both copies of every request were not satisfied"},
      shape{
        active_a(),
        interleaved.substr(0, 12288),
        {"@satisfied", "@footer_only", "@satisfied"},
        "@content",
        "@content",
        "a footer-covered block without its PREPARE was not ordinary"},
      shape{
        active_a(),
        damaged_a2,
        {"@satisfied", "@footer_only", "@satisfied"},
        "@content",
        "@uncertified_tail",
        "a PREPARE after the head's first damage became a candidate"},
      shape{
        active_a().substr(0, 12288),
        interleaved,
        {"@suffix_sourced", "@suffix_sourced", "@satisfied"},
        "@content",
        "@content",
        "blocks after the boundary were not an unresolved suffix"},
      shape{
        header_a,
        interleaved,
        {"@candidate", "@candidate", "@satisfied"},
        "@content",
        "@content",
        "PREPAREs without blocks were not candidates"},
      // The same request encoded differently at A2's slot; footer_a no
      // longer covers those bytes.
      shape{
        header_a + local_fixture::read("block_a1") + block_a(101, 1, 8192, true)
          + local_fixture::read("footer_a"),
        interleaved,
        {"@suffix_sourced", "@differing_copies", "@satisfied"},
        "@uncertified_tail",
        "@content",
        "differing copies of one slot were not a conflict"},
    };
    for (const auto& test : shapes) {
        co_await write_bytes(files, a, test.segment_a, drive);
        co_await put_wal(files, spec, wal(1), test.wal, drive);
        const auto first = co_await classify_store(
          files, owner, spec, budget, drive);
        require(
          label(first.slots, segment(), 4096) == test.slots[0]
            && label(first.slots, segment(), 8192) == test.slots[1]
            && label(first.slots, segment_b(), 4096) == test.slots[2],
          test.message);
        require(
          first.regions.size() == 3 && first.regions[0].label == test.region_a
            && first.regions[1].label == "@content"
            && first.regions[2].label == test.region_wal,
          test.message);
        const auto again = co_await classify_store(
          files, owner, spec, budget, drive);
        require(again == first, "classification changed across a restart");
    }
}

} // namespace kwaque::storage::testing::segment_scan_contract
