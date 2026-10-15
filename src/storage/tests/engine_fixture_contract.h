#pragma once

#include "src/base/error.h"
#include "src/base/units.h"
#include "src/codec/cooperative.h"
#include "src/codec/digest.h"
#include "src/codec/limits.h"
#include "src/model/position.h"
#include "src/runtime/file.h"
#include "src/runtime/time.h"
#include "src/storage/local_engine.h"
#include "src/storage/local_paths.h"
#include "src/storage/local_store_config.h"
#include "src/storage/local_types.h"
#include "src/storage/recovery_inventory.h"
#include "src/storage/segment_lifecycle.h"
#include "src/storage/tests/footer_test_support.h"
#include "src/storage/tests/local_engine_contract.h"
#include "src/storage/tests/local_installation_contract.h"
#include "src/storage/tests/local_metadata_fixture.h"
#include "src/storage/tests/local_reader_contract.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/tests/recovery_contract.h"
#include "src/storage/tests/segment_lifecycle_contract.h"
#include "src/storage/tests/segment_scan_contract.h"
#include "src/storage/tests/segment_test_support.h"
#include "src/storage/tests/wal_scan_contract.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace kwaque::storage::testing::engine_fixture_contract {
using store_contract::require;
using store_contract::take;
namespace engines = local_engine_contract;
namespace installation = installation_contract;
namespace lifecycle = segment_lifecycle_contract;
namespace reader = reader_contract;
namespace scan = scan_contract;

// The three devices the checked-in store image is of: one that holds the WAL
// and the shard's control record, and two that hold a segment each, all of
// one shard. A backend supplies each one's root and what its host says the
// directory is.
inline std::array<local_device_spec, 3> devices(
  std::array<runtime::file_path, 3> roots,
  std::array<local_directory_identity, 3> directories) {
    return {
      store_contract::specification(
        std::move(roots[0]),
        directories[0],
        0x33,
        local_device_role::wal_control,
        1),
      store_contract::specification(
        std::move(roots[1]), directories[1], 0x44, local_device_role::data, 1),
      store_contract::specification(
        std::move(roots[2]), directories[2], 0x55, local_device_role::data, 1)};
}

// Segment B's publication. The image holds none: this is segment A's, with
// B's device and identity in their places and its checksums made again; and
// with a boundary, B's footer in the place of A's.
inline std::string publication_b(bool boundary = true) {
    auto wire = local_fixture::read(
      boundary ? "publication_boundary" : "publication_empty");
    wire.replace(68, 16, std::string(16, '\x55'));
    wire.replace(136, 16, std::string(16, '\x21'));
    wire.replace(152, 16, std::string(16, '\x31'));
    put(wire, 168, 2, 8);
    if (boundary) {
        put(wire, 188, 8192, 8);
        const auto footer = installation::literal_digest(
                              "26379b17a9588439bcd93421bbe88ca4")
                              .bytes();
        for (std::size_t i = 0; i != footer.size(); ++i)
            wire[204 + i] = std::bit_cast<char>(footer[i]);
    }
    repair(wire);
    return wire;
}

// One file of the image with neither segment sealed: where it lies, on which
// of the three devices, and what can stand in its place. `foreign` is the
// same file of another object; `stale` is an earlier whole version of
// itself. Either is empty where there is none.
struct image_file final {
    runtime::file_path path;
    std::size_t device;
    std::string foreign;
    std::string stale;
};
inline std::vector<image_file>
image_files(std::span<const local_device_spec, 3> devices) {
    const auto read = [](const char* name) {
        return local_fixture::read(name);
    };
    const auto paths = [&](std::size_t device) {
        return take(local_paths::make(devices[device].root));
    };
    const auto in = [&](
                      std::size_t device,
                      const segment_context& context,
                      local_segment_file file) {
        return take(paths(device).segment_file(
          0, {context.segment(), context.generation()}, file));
    };
    const auto a = installation::segment();
    const auto b = scan::segment_b();
    const auto data_a = segment_scan_contract::active_a();
    const auto data_b = recovery_contract::segment_b_bytes();
    std::vector<image_file> files;
    // The WAL and control device.
    files.push_back(
      {take(paths(0).store()), 0, read("image_store_data_a"), {}});
    files.push_back({take(paths(0).control(0)), 0, {}, read("control_empty")});
    files.push_back(
      {take(paths(0).wal(0, reader::wal(1))),
       0,
       read("wal_successor_gap"),
       read("wal_header") + read("prepare_a1") + read("prepare_b1")});
    // Segment A's device.
    files.push_back(
      {take(paths(1).store()), 1, read("image_store_data_b"), {}});
    files.push_back(
      {in(1, a, local_segment_file::descriptor), 1, read("descriptor_b"), {}});
    files.push_back(
      {in(1, a, local_segment_file::data), 1, data_b, read("data_header_a")});
    files.push_back(
      {in(1, a, local_segment_file::published),
       1,
       publication_b(),
       read("publication_empty")});
    // Segment B's device.
    files.push_back(
      {take(paths(2).store()), 2, read("image_store_data_a"), {}});
    files.push_back(
      {in(2, b, local_segment_file::descriptor), 2, read("descriptor_a"), {}});
    files.push_back(
      {in(2, b, local_segment_file::data), 2, data_a, read("data_header_b")});
    files.push_back(
      {in(2, b, local_segment_file::published),
       2,
       read("publication_boundary"),
       publication_b(false)});
    return files;
}

// The image, file by file, as bytes nothing of the engine wrote: every
// record, the WAL with its three PREPAREs and both segments come from the
// checked-in files. With `sealed`, segment A is the sealed one: its sealed
// root, its three objects and the publication that names them.
template<typename Backend, typename Driver>
seastar::future<> lay_out(
  Backend& files,
  std::span<const local_device_spec, 3> devices,
  bool sealed,
  Driver drive) {
    const auto made = [&](runtime::file_path path) -> seastar::future<> {
        take(co_await drive.lifecycle(files.create_directories(path)));
    };
    const auto child = [](const runtime::file_path& parent, const char* name) {
        return take(
          local_child_path(parent, take(runtime::file_name::make(name))));
    };
    const auto put = [&](
                       const runtime::file_path& path,
                       std::string bytes) -> seastar::future<> {
        co_await store_contract::write_bytes(
          files, path, std::move(bytes), drive);
    };
    {
        const auto& device = devices[0];
        const auto paths = take(local_paths::make(device.root));
        const auto shard = recovery_contract::parent_of(take(paths.control(0)));
        co_await made(child(child(shard, "checkpoints"), "evidence"));
        co_await made(child(shard, "decisions"));
        co_await made(child(shard, "deletions"));
        co_await put(
          take(paths.store()), local_fixture::read("image_store_wal"));
        co_await put(
          take(paths.control(0)), local_fixture::read("control_head"));
        co_await reader::put_wal(
          files, device, reader::wal(1), scan::interleaved(), drive);
    }
    const auto segment = [&](
                           const local_device_spec& device,
                           const segment_context& context,
                           const char* store,
                           const char* descriptor,
                           std::string data,
                           std::string published) -> seastar::future<> {
        const auto paths = take(local_paths::make(device.root));
        const local_segment_name name{context.segment(), context.generation()};
        co_await made(
          recovery_contract::parent_of(
            take(paths.object(0, name, reader::index_ref().sequence()))));
        co_await put(take(paths.store()), local_fixture::read(store));
        co_await put(
          take(paths.segment_file(0, name, local_segment_file::descriptor)),
          local_fixture::read(descriptor));
        co_await put(
          take(paths.segment_file(0, name, local_segment_file::data)),
          std::move(data));
        co_await put(
          take(paths.segment_file(0, name, local_segment_file::published)),
          std::move(published));
    };
    const auto a = installation::segment();
    co_await segment(
      devices[1],
      a,
      "image_store_data_a",
      "descriptor_a",
      segment_scan_contract::active_a()
        + (sealed ? local_fixture::read("sealed_a") : std::string{}),
      local_fixture::read(
        sealed ? "publication_all_roots" : "publication_boundary"));
    if (sealed) {
        const auto paths = take(local_paths::make(devices[1].root));
        const local_segment_name name{a.segment(), a.generation()};
        co_await put(
          take(paths.object(0, name, reader::snapshot_ref().sequence())),
          local_fixture::read("snapshot_root")
            + local_fixture::read("snapshot_page"));
        co_await put(
          take(paths.object(0, name, reader::sealed_ref().sequence())),
          local_fixture::read("sealed_retry_page"));
        co_await put(
          take(paths.object(0, name, reader::index_ref().sequence())),
          local_fixture::read("index_root")
            + local_fixture::read("index_page"));
    }
    co_await segment(
      devices[2],
      scan::segment_b(),
      "image_store_data_b",
      "descriptor_b",
      recovery_contract::segment_b_bytes(),
      publication_b());
}

// A record of a file, at `record` in it, made to say its body is `extra`
// bytes longer than it is, with both checksums right for what it then says.
// The body's length is the four bytes at twelve of the record; its checksum
// the four at twenty-four, and the header's own the four after. The bytes as
// they were when the file holds no such record or has no room for the body.
inline std::string
overlong(std::string bytes, std::size_t record, std::size_t extra) {
    if (record + 32 > bytes.size()) return bytes;
    const std::size_t header = get(
      std::string_view{bytes}.substr(record), 10, 2);
    const std::size_t longer = get(
                                 std::string_view{bytes}.substr(record), 12, 4)
                               + extra;
    if (header < 32 || record + header + longer > bytes.size()) return bytes;
    auto head = bytes.substr(record, header);
    put(head, 12, longer, 4);
    put(
      head,
      24,
      crc(std::string_view{bytes}.substr(record + header, longer)),
      4);
    put(head, 28, 0, 4);
    put(head, 28, crc(head), 4);
    bytes.replace(record, header, head);
    return bytes;
}
// Whether the record at `record` holds both checksums of the body its header
// says it has.
inline bool sealed_at(std::string_view bytes, std::size_t record) {
    if (record + 32 > bytes.size()) return false;
    const std::size_t header = get(bytes.substr(record), 10, 2);
    const std::size_t body = get(bytes.substr(record), 12, 4);
    if (header < 32 || record + header + body > bytes.size()) return false;
    auto head = std::string{bytes.substr(record, header)};
    const auto kept = get(head, 28, 4);
    put(head, 28, 0, 4);
    return get(bytes.substr(record), 24, 4)
             == crc(bytes.substr(record + header, body))
           && kept == crc(head);
}

// What a start on the image answered, and how the table then holds its two
// segments.
struct image_start final {
    // The shard was left ready, and no failure was answered.
    bool ready{false};
    bool failed{false};
    std::optional<errc> failure;
    bool restarted{false};
    std::uint32_t recovered{0};
    std::uint64_t unresolved{0};
    // The table holds the segment as one a restart recovered.
    bool a_recovering{false}, b_recovering{false};
};

// Starts the shard on the image as it lies on its devices, with both
// segments in its owner's catalog, and stops it. Nothing is required of
// what the start answers: the image may have been damaged. `answered` is
// called once the start has returned and before the stop begins.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Answered>
seastar::future<image_start> start_on_the_image(
  Backend& files,
  Owner& owner,
  std::span<const local_device_spec, 3> devices,
  workload_budget& budget,
  workload_budget& reserve,
  Driver drive,
  Timer& timer,
  Answered answered) {
    using engine_type = local_engine<Backend, Owner, Clock>;
    const auto a = installation::segment();
    const auto b = scan::segment_b();
    const std::array catalog{
      recovery_catalog_entry{
        installation::descriptor(), installation::segment_head(), std::nullopt},
      recovery_catalog_entry{
        scan::descriptor_b(), scan::segment_head_b(), std::nullopt}};
    lifecycle::told heard;
    auto engine = take(
      engine_type::make(
        {files,
         owner,
         std::span<const local_device_spec>{devices},
         devices[0],
         0,
         budget,
         budget,
         reserve,
         budget},
        engines::configuration()));
    image_start seen;
    std::exception_ptr thrown;
    try {
        const auto began = co_await drive.lifecycle(
          engine->start(timer, catalog, {heard.sink(), {}, {}}));
        seen.failed = began.failure.failed();
        seen.ready = began.state == engine_state::ready && !seen.failed;
        if (began.failure.error()) seen.failure = began.failure.error()->code();
        seen.restarted = began.restarted;
        seen.recovered = began.recovered;
        seen.unresolved = began.unresolved;
        if (auto* table = engine->table()) {
            seen.a_recovering = table->life(a) == segment_life::recovering;
            seen.b_recovering = table->life(b) == segment_life::recovering;
        }
        answered();
    } catch (...) {
        thrown = std::current_exception();
    }
    // What a stop answers after a start that was refused is not the start's.
    static_cast<void>(co_await drive.lifecycle(engine->stop()));
    if (thrown) std::rethrow_exception(thrown);
    co_return seen;
}

// The engine starts on a store it never wrote. The image is laid out from
// the checked-in files alone, on its three devices, and the shard is started
// with both segments in its owner's catalog. The start is a restart and
// leaves the shard ready with nothing unexplained: each of the WAL's three
// PREPAREs is covered by the footer its segment's publication pins. A
// segment that was not sealed is taken in as recovered, and its blocks are
// still where the image has them once the start has rewritten what it
// rewrites. With `sealed`, segment A is the sealed one: the start does not
// take it in, and a reader that comes with what it was sealed as is given
// its two requests, each the checked-in child byte for byte.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer>
seastar::future<> started_on_the_image(
  Backend& files,
  Owner& owner,
  std::span<const local_device_spec, 3> devices,
  workload_budget& budget,
  workload_budget& reserve,
  Driver drive,
  Timer& timer,
  bool sealed) {
    using engine_type = local_engine<Backend, Owner, Clock>;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await lay_out(files, devices, sealed, drive);
    const auto a = installation::segment();
    const auto b = scan::segment_b();
    const std::array catalog{
      recovery_catalog_entry{
        installation::descriptor(), installation::segment_head(), std::nullopt},
      recovery_catalog_entry{
        scan::descriptor_b(), scan::segment_head_b(), std::nullopt}};
    const auto held =
      [&](const local_device_spec& device, const segment_context& context) {
          const auto paths = take(local_paths::make(device.root));
          return store_contract::read_all_bytes(
            files,
            take(paths.segment_file(
              0,
              {context.segment(), context.generation()},
              local_segment_file::data)),
            drive);
      };

    lifecycle::told heard;
    auto engine = take(
      engine_type::make(
        {files,
         owner,
         std::span<const local_device_spec>{devices},
         devices[0],
         0,
         budget,
         budget,
         reserve,
         budget},
        engines::configuration()));
    runtime::first_failure failed;
    try {
        const auto began = co_await drive.lifecycle(
          engine->start(timer, catalog, {heard.sink(), {}, {}}));
        take(began.failure.outcome());
        require(
          began.state == engine_state::ready && began.restarted
            && began.recovered == (sealed ? 1U : 2U) && began.unresolved == 0
            && began.refused == 0 && began.gone == 0 && began.resumed == 0,
          "a start on the checked-in image did not take its segments in, or "
          "left a PREPARE of its WAL unexplained");
        const auto life_a = engine->table()->life(a);
        require(
          engine->table()->life(b) == segment_life::recovering
            && (sealed ? !life_a : life_a == segment_life::recovering),
          "a segment of the image is not held as what the image has it as");
        // The blocks are where the image has them.
        const auto data_a = co_await held(devices[1], a);
        const auto data_b = co_await held(devices[2], b);
        require(
          data_a.starts_with(segment_scan_contract::active_a())
            && data_b.starts_with(recovery_contract::segment_b_bytes()),
          "a start changed a block of the image");
        if (sealed) {
            const auto roots = reader::contexts(devices[1]);
            const sealed_segment_expectation expected{
              devices[1].owner.device(),
              take(local_publication_generation::make(3)),
              reader::sealed_publication(),
              installation::descriptor(),
              {roots.begin(), roots.end()},
              reader::history(),
              {scope(100, 102, 0, 2, 4096, 16384),
               codec::extent_digest{installation::literal_digest(
                                      "eca717818b53c4ae00458133840519cc")
                                      .bytes()},
               runtime::file_position{20480}}};
            auto opened = co_await drive.lifecycle(
              engine->table()->open_session(
                a, lifecycle::sight(1U << 20U), budget, work, &expected));
            auto session = take(std::move(opened));
            const auto read = co_await lifecycle::read_from(
              session,
              {model::range_logical_end{100}, std::nullopt},
              byte_count{1_MiB},
              drive);
            require(
              read.batches.size() == 2
                && read.batches[0].logical.begin().value() == 100
                && read.batches[1].logical.begin().value() == 101
                && flat(read.batches[0].bytes)
                     == local_fixture::read("child_a1")
                && flat(read.batches[1].bytes)
                     == local_fixture::read("child_a2")
                && read.stop == segment_read_stop::end_of_segment,
              "a reader of the sealed segment was not given the image's two "
              "requests");
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    failed.observe(co_await drive.lifecycle(engine->stop()));
    take(failed.outcome());
}

} // namespace kwaque::storage::testing::engine_fixture_contract
