#include "src/simulation/tests/storage_recovery_fuzz_cases.h"

#include "src/base/error.h"
#include "src/runtime/file.h"
#include "src/runtime/testing/fuzz_verdict.h"
#include "src/simulation/fake_file.h"
#include "src/simulation/fake_file_test_support.h"
#include "src/simulation/scheduler_driver.h"
#include "src/simulation/tests/engine_history_fixture.h"
#include "src/simulation/tests/local_store_fixture.h"
#include "src/simulation/virtual_time.h"
#include "src/storage/tests/engine_fixture_contract.h"
#include "src/storage/tests/local_store_contract.h"

#include <seastar/core/future.hh>

#include <algorithm>
#include <array>
#include <cstddef>
#include <exception>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace kwaque::simulation::testing {
namespace {
namespace image = storage::testing::engine_fixture_contract;
// A record of the image begins at each multiple of this in its file.
constexpr std::size_t record_bytes = 4096;
} // namespace

std::string damaged_bytes(
  std::string bytes,
  recovery_control control,
  std::span<const std::uint8_t> script,
  std::string_view foreign,
  std::string_view stale) {
    const auto damage = static_cast<image_damage>(
      control.damage % image_damage_count);
    if (damage == image_damage::foreign)
        return foreign.empty() ? bytes : std::string{foreign};
    if (damage == image_damage::stale)
        return stale.empty() ? bytes : std::string{stale};
    if (bytes.empty() && damage != image_damage::appended) return bytes;
    const auto at = bytes.empty() ? 0
                                  : (std::size_t{control.low}
                                     | (std::size_t{control.high} << 8U))
                                      % bytes.size();
    const auto run = std::min<std::size_t>(
      std::size_t{control.length} * 16 + 1, bytes.size() - at);
    switch (damage) {
    case image_damage::none:
    case image_damage::foreign:
    case image_damage::stale:
    case image_damage::removed:
        break;
    case image_damage::appended:
        for (std::size_t i = 0; i != std::size_t{control.length} * 16 + 1; ++i)
            bytes.push_back(
              static_cast<char>(
                script.empty() ? std::uint8_t{0xa5}
                               : script[i % script.size()]));
        break;
    case image_damage::zeros:
        std::fill_n(bytes.begin() + static_cast<std::ptrdiff_t>(at), run, '\0');
        break;
    case image_damage::junk:
        for (std::size_t i = 0; i != run; ++i)
            bytes[at + i] = static_cast<char>(
              script.empty() ? std::uint8_t{0xa5} : script[i % script.size()]);
        break;
    case image_damage::bit:
        bytes[at] ^= static_cast<char>(1U << (control.bit % 8U));
        break;
    case image_damage::cut:
        bytes.resize(at);
        break;
    case image_damage::torn: {
        const auto sector = at / record_bytes * record_bytes;
        const auto from = sector + std::size_t{control.length % 8U} * 512;
        const auto to = std::min(sector + record_bytes, bytes.size());
        if (from < to)
            std::fill(
              bytes.begin() + static_cast<std::ptrdiff_t>(from),
              bytes.begin() + static_cast<std::ptrdiff_t>(to),
              '\0');
        break;
    }
    case image_damage::overlong:
        return image::overlong(
          std::move(bytes),
          at / record_bytes * record_bytes,
          512 * (std::size_t{control.length % 4U} + 1));
    }
    return bytes;
}

std::string_view recovery_verdict(const recovery_seen& seen) noexcept {
    if (
      !seen.changed
      && !(
        seen.ready && seen.restarted && seen.recovered == 2
        && seen.unresolved == 0 && seen.a_recovering && seen.b_recovering))
        return "baseline";
    if (!seen.ready && !seen.untouched) return "wrote";
    if (seen.changed && seen.ready) {
        if (seen.device == 1 && !seen.b_recovering) return "spared";
        if (seen.device == 2 && !seen.a_recovering) return "spared";
    }
    return {};
}

recovery_case_result exercise_recovery_case(
  recovery_control control, std::span<const std::uint8_t> script) {
    using namespace local_store_fixture;
    recovery_case_result result;
    try {
        with_reserved_store(
          [&](
            auto& files,
            auto&,
            auto&,
            auto& budget,
            auto& reserve,
            auto drive,
            auto& timer) -> seastar::future<> {
              const std::array roots{
                take(runtime::file_path::make("/kwaque/w")),
                take(runtime::file_path::make("/kwaque/a")),
                take(runtime::file_path::make("/kwaque/b"))};
              for (const auto& root : roots)
                  take(
                    co_await drive.lifecycle(files.create_directories(root)));
              const auto devices = image::devices(
                roots, {{{1, 2}, {1, 3}, {1, 4}}});
              ownership_input owner{devices};
              co_await image::lay_out(files, devices, false, drive);

              const auto listed = image::image_files(devices);
              const auto& chosen = listed[control.file % listed.size()];
              const auto original = co_await read_all_bytes(
                files, chosen.path, drive);
              const auto damaged = damaged_bytes(
                original, control, script, chosen.foreign, chosen.stale);
              const bool removed = static_cast<image_damage>(
                                     control.damage % image_damage_count)
                                   == image_damage::removed;
              result.seen.device = chosen.device;
              result.seen.changed = removed || damaged != original;
              // At rest, with nothing of the store told.
              const auto held = take(
                fake_file_test_access::resolve(files, chosen.path.value()));
              if (removed) {
                  take(fake_file_test_access::remove_file(files, held));
              } else if (result.seen.changed) {
                  take(fake_file_test_access::truncate(files, held, 0));
                  // Nothing is written of a file cut to nothing.
                  if (!damaged.empty()) {
                      std::vector<std::byte> raw(damaged.size());
                      std::ranges::transform(
                        damaged, raw.begin(), [](char one) {
                            return static_cast<std::byte>(one);
                        });
                      take(fake_file_test_access::write(files, held, 0, raw));
                  }
                  take(fake_file_test_access::flush(files, held));
              }
              const auto given = engine_history_fixture::image(files);
              const auto began = co_await image::start_on_the_image<
                simulation::monotonic_clock>(
                files, owner, devices, budget, reserve, drive, timer, [&] {
                    result.seen.untouched = engine_history_fixture::image(files)
                                            == given;
                });
              result.seen.ready = began.ready;
              result.seen.restarted = began.restarted;
              result.seen.recovered = began.recovered;
              result.seen.unresolved = began.unresolved;
              result.seen.a_recovering = began.a_recovering;
              result.seen.b_recovering = began.b_recovering;
              if (began.failed)
                  result.seen.failure
                    = began.failure
                        ? std::string{make_error_code(*began.failure).message()}
                        : std::string{"a failure that names no code"};
          })
          .get();
        result.word = recovery_verdict(result.seen);
        if (!result.word.empty() && result.seen.failure)
            result.about = *result.seen.failure;
    } catch (const scheduler_liveness_error& failure) {
        result.word = "hang";
        result.about = failure.what();
    } catch (const std::system_error& failure) {
        result.word = failure.code()
                          == make_error_code(errc::resource_exhausted)
                        ? "exhausted"
                        : "threw";
        result.about = failure.what();
    } catch (const std::exception& failure) {
        result.word = "threw";
        result.about = failure.what();
    }
    return result;
}

void fuzz_recovery_case(std::span<const std::uint8_t> input) {
    if (input.size() > storage_recovery_fuzz_max_input) return;
    std::array<std::uint8_t, 8> head{};
    const auto prefix = std::min(input.size(), head.size());
    std::copy_n(input.begin(), prefix, head.begin());
    const recovery_control control{
      head[0], head[1], head[2], head[3], head[4], head[5], head[6], head[7]};
    const auto result = exercise_recovery_case(control, input.subspan(prefix));
    if (!result.word.empty())
        runtime::testing::fail_fuzz_verdict(
          result.word,
          "file " + std::to_string(control.file) + " damage "
            + std::to_string(control.damage % image_damage_count) + ": "
            + result.about);
}

} // namespace kwaque::simulation::testing
