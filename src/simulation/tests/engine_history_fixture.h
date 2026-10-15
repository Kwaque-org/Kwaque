#pragma once

#include "src/simulation/fake_file.h"
#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/tests/local_store_fixture.h"
#include "src/simulation/virtual_time.h"
#include "src/storage/local_store_config.h"
#include "src/storage/tests/engine_history.h"
#include "src/storage/tests/engine_history_contract.h"
#include "src/storage/tests/local_store_contract.h"

#include <seastar/core/future.hh>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

// One history of a shard's storage over a store of its own on the simulated
// file system, as a test and a fuzz input both run it, and what the device
// holds as one number.
namespace kwaque::simulation::testing::engine_history_fixture {
namespace histories = kwaque::storage::testing::engine_history_contract;

// The crash policy a history's header asks of the device.
inline std::optional<fake_crash_policy>
survival(const storage::testing::history_header& header) {
    using storage::testing::history_survival;
    switch (header.survival) {
    case history_survival::none:
        break;
    case history_survival::all:
        return fake_crash_policy{
          .data_percent = 100, .namespace_percent = 100, .eof_percent = 100};
    case history_survival::data:
        return fake_crash_policy{.data_percent = 50, .eof_percent = 50};
    case history_survival::names:
        return fake_crash_policy{.namespace_survivors = header.names};
    }
    return std::nullopt;
}

// What the device holds, as one number: every name under the root and every
// file's bytes, in name order. Two crashes that leave the same number leave
// a start the same store to read.
inline void digest(std::uint64_t& folded, std::uint8_t byte) {
    folded ^= byte;
    folded *= UINT64_C(0x100000001b3);
}
inline void digest(std::uint64_t& folded, std::uint64_t value) {
    for (unsigned i = 0; i != 8; ++i)
        digest(folded, static_cast<std::uint8_t>(value >> (i * 8U)));
}
// A file the device holds: its name, its size, and one digest for each 4 KiB
// of it. Two stores that differ are told apart by the files and pages that do.
struct stored_file final {
    std::string path;
    std::uint64_t size;
    std::vector<std::uint64_t> page_digests;
};

inline void digest(
  fake_file_system& files,
  const std::string& directory,
  std::uint64_t& folded,
  std::vector<stored_file>* stored = nullptr) {
    using storage::testing::store_contract::take;
    const auto listed = take(
      fake_file_test_access::list(
        files, take(fake_file_test_access::resolve(files, directory))));
    std::vector<std::pair<std::string, fake_file_kind>> entries;
    for (const auto& entry : listed)
        entries.emplace_back(entry.name, entry.kind);
    std::ranges::sort(entries);
    for (const auto& [name, kind] : entries) {
        const auto path = directory + "/" + name;
        for (const char letter : path)
            digest(folded, static_cast<std::uint8_t>(letter));
        if (kind == fake_file_kind::directory) {
            digest(files, path, folded, stored);
            continue;
        }
        const auto resolved = take(fake_file_test_access::resolve(files, path));
        const auto size = take(
          fake_file_test_access::visible_size(files, resolved));
        if (stored) stored->push_back({path, size, {}});
        std::array<std::byte, 4096> page{};
        for (std::uint64_t at = 0; at < size; at += page.size()) {
            const auto read = take(
              fake_file_test_access::read(files, resolved, at, page));
            std::uint64_t own = UINT64_C(0xcbf29ce484222325);
            for (const auto byte : std::span{page}.first(read.value())) {
                digest(folded, static_cast<std::uint8_t>(byte));
                digest(own, static_cast<std::uint8_t>(byte));
            }
            if (stored) stored->back().page_digests.push_back(own);
        }
        digest(folded, size);
    }
}
inline std::uint64_t image(fake_file_system& files) {
    std::uint64_t folded = UINT64_C(0xcbf29ce484222325);
    digest(files, "/kwaque", folded);
    return folded;
}
inline std::vector<stored_file> stored_files(fake_file_system& files) {
    std::vector<stored_file> stored;
    std::uint64_t folded = UINT64_C(0xcbf29ce484222325);
    digest(files, "/kwaque", folded, &stored);
    return stored;
}

// What a caller looks at or does around a run, each with the device as it
// stands: as a crash is about to be taken; before a start that follows a
// crash, once the device is back; and when the run is over.
struct around final {
    std::function<void(fake_file_system&, std::uint32_t)> crashing;
    std::function<void(
      fake_file_system&, const storage::local_device_spec&, std::uint32_t)>
      starting;
    std::function<void(fake_file_system&)> over;
    // Round each thing the run asks of the shard, as the driver's hooks of
    // these names are called.
    std::function<void(const histories::history_step&)> asking;
    std::function<bool(const histories::history_step&, bool)> ended;
};

// One run of a history over a store of its own. The device is down from
// each crash to the start after it: what the crash left running is stopped
// and changes nothing. A run whose length its caller did not choose is a
// `long_run`.
inline seastar::future<histories::history_report> run(
  const storage::testing::engine_history& history,
  histories::history_cuts cuts,
  around looked = {},
  bool long_run = false,
  histories::history_settings settings = {}) {
    using namespace local_store_fixture;
    histories::history_report report;
    co_await with_reserved_store(
      [&](
        auto& files,
        auto& owner,
        auto& spec,
        auto& budget,
        auto& reserve,
        auto drive,
        auto& timer) -> seastar::future<> {
          histories::history_hooks hooks;
          hooks.changes = [&files] {
              return fake_file_test_access::device_changes(files);
          };
          hooks.before_crash = [&files, &looked](std::uint32_t life) {
              if (looked.crashing) looked.crashing(files, life);
              return seastar::make_ready_future<>();
          };
          hooks.after_crash = [&files](std::uint32_t) {
              fake_file_test_access::set_down(files, true);
              return seastar::make_ready_future<>();
          };
          hooks.before_start = [&files, &spec, &looked](std::uint32_t life) {
              fake_file_test_access::set_down(files, false);
              if (looked.starting) looked.starting(files, spec, life);
              return seastar::make_ready_future<>();
          };
          hooks.asking = looked.asking;
          hooks.ended = looked.ended;
          report = co_await histories::run_history<simulation::monotonic_clock>(
            files,
            owner,
            spec,
            budget,
            reserve,
            drive,
            timer,
            one_event{&drive.events},
            history,
            cuts,
            std::move(hooks),
            settings);
          if (looked.over) looked.over(files);
      },
      survival(history.header),
      long_run);
    co_return report;
}

} // namespace kwaque::simulation::testing::engine_history_fixture
