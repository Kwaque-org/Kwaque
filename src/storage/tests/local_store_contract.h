#pragma once

#include "src/bytes/test_allocation_profile.h"
#include "src/storage/local_control.h"
#include "src/storage/tests/local_metadata_fixture.h"
#include "src/storage/tests/local_publication_contract.h"
#include "src/storage/tests/retry_test_support.h"

#include <algorithm>
#include <array>
#include <span>
#include <string>

namespace kwaque::storage::testing::store_contract {
using publication_contract::identity;
using publication_contract::require;
using publication_contract::take;
inline local_store_io_limits limits() {
    return {.charge = bytes::testing::charge};
}
inline local_device_spec specification(
  runtime::file_path root,
  local_directory_identity directory,
  std::uint8_t device = 0x33,
  local_device_role role = local_device_role::all,
  std::uint32_t shards = 2) {
    return {
      std::move(root),
      local_store_context::make(
        identity<model::cluster_id>(0x11),
        identity<model::broker_id>(0x22),
        identity<device_store_id>(device),
        local_store_shard)
        .value(),
      {1, storage_alignment::make(byte_count{4096}).value(), shards, role},
      directory};
}
// Supplied ownership facts only. No fake host flock or synthetic mount probe.
struct ownership_input final {
    std::span<const local_device_spec> specs;
    bool active{true};
    seastar::future<runtime::result<void>>
    validate(const local_device_spec& spec) {
        if (
          !active || std::find(specs.begin(), specs.end(), spec) == specs.end())
            return seastar::make_ready_future<runtime::result<void>>(
              runtime::failure(detail::path_error(errc::wrong_context)));
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::result<void>{});
    }
};
template<runtime::file_system_backend Backend, typename Driver>
seastar::future<std::string>
read_bytes(Backend& files, const runtime::file_path& path, Driver drive) {
    auto file = take(
      co_await drive.lifecycle(files.open(
        path, {.close_policy = runtime::file_close_policy::checked})));
    runtime::first_failure failed;
    std::string result;
    try {
        auto read = take(
          co_await drive.lifecycle(
            file.read(runtime::file_position{}, byte_count{65536})));
        result = kwaque::storage::testing::flat(read.data());
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        failed.observe(co_await drive.lifecycle(file.close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(failed.outcome());
    co_return result;
}
template<runtime::file_system_backend Backend, typename Driver>
seastar::future<> write_bytes(
  Backend& files,
  const runtime::file_path& path,
  std::string bytes,
  Driver drive) {
    auto file = take(
      co_await drive.lifecycle(files.open(
        path,
        {.access = runtime::file_access::read_write,
         .create = true,
         .truncate = true,
         .close_policy = runtime::file_close_policy::checked})));
    runtime::first_failure failed;
    try {
        auto written = co_await drive.lifecycle(file.write(
          runtime::file_position{},
          kwaque::bytes::fragmented_buffer::copy_of(
            std::span<const char>{bytes})
            .value()));
        failed.observe(written);
        if (written)
            require(written->value() == bytes.size(), "fixture short write");
        if (!failed.failed())
            failed.observe(co_await drive.lifecycle(file.flush()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        failed.observe(co_await drive.lifecycle(file.close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(failed.outcome());
    const auto split = path.value().rfind('/');
    take(
      co_await drive.lifecycle(files.sync_directory(
        take(
          runtime::file_path::make(
            split == 0 ? "/" : path.value().substr(0, split))),
        runtime::file_close_policy::checked)));
}

template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  typename Driver>
seastar::future<> exercise(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    const std::array specs{spec};
    const auto paths = take(local_paths::make(spec.root));
    const auto marker = take(paths.store());
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const local_store_open_options create{
      local_store_intent::create_or_resume, false};
    const local_store_open_options unopened{
      local_store_intent::must_exist, false};
    {
        const auto report = take(
          co_await drive.lifecycle(inspect_local_store(
            files, ownership, spec, create, budget, limits(), work)));
        require(
          report.state == local_store_state::pristine
            && report.bootstrap_compatible,
          "empty owned store was not pristine");
        auto refused = co_await drive.lifecycle(initialize_local_stores(
          files, ownership, specs, unopened, budget, limits(), work));
        require(
          refused.failure.failed() && !refused.mutation_attempted,
          "must-exist initialized an empty store");
        require(
          !take(co_await drive.lifecycle(files.exists(marker))),
          "must-exist wrote identity");
    }
    const auto unexpected = take(local_child_path(
      spec.root, take(runtime::file_name::make(".unrecognized"))));
    co_await write_bytes(files, unexpected, std::string(4096, 'x'), drive);
    {
        auto blocked = co_await drive.lifecycle(initialize_local_stores(
          files, ownership, specs, create, budget, limits(), work));
        require(
          blocked.failure.failed()
            && blocked.reports[0].state == local_store_state::unsupported,
          "polluted directory accepted");
        require(
          !take(co_await drive.lifecycle(files.exists(marker))),
          "pollution was hidden by new identity");
        require(
          (co_await read_bytes(files, unexpected, drive))
            == std::string(4096, 'x'),
          "pollution changed on rejection");
    }
    take(co_await drive.lifecycle(files.remove_file(unexpected)));
    // A known interrupted bootstrap prefix, independently specified bytes.
    co_await write_bytes(files, marker, local_fixture::read("store"), drive);
    {
        auto report = take(
          co_await drive.lifecycle(inspect_local_store(
            files, ownership, spec, create, budget, limits(), work)));
        require(
          report.state == local_store_state::incomplete
            && report.bootstrap_compatible,
          "matching identity prefix was not resumable");
        auto initialized = co_await drive.lifecycle(initialize_local_stores(
          files, ownership, specs, create, budget, limits(), work));
        take(initialized.failure.outcome());
        require(
          initialized.stages[0] == local_bootstrap_stage::complete,
          "bootstrap did not finish");
    }
    require(
      (co_await read_bytes(files, marker, drive))
        == local_fixture::read("store"),
      "resume rewrote immutable identity bytes");
    const auto control_path = take(paths.control(0));
    require(
      (co_await read_bytes(files, control_path, drive))
        == local_fixture::read("control_empty"),
      "initial control differs from independent fixture");
    {
        auto report = take(
          co_await drive.lifecycle(inspect_local_store(
            files, ownership, spec, unopened, budget, limits(), work)));
        require(
          report.state == local_store_state::existing
            && report.controls == spec.identity.shard_count
            && !report.all_heads_present,
          "unactivated store classification lost control state");
        auto active_required = co_await drive.lifecycle(initialize_local_stores(
          files, ownership, specs, {}, budget, limits(), work));
        require(
          active_required.failure.failed()
            && !active_required.mutation_attempted,
          "missing required head initialized an empty WAL");
    }
    using control_type = local_control_owner<Backend, Owner>;
    auto control = take(
      co_await drive.lifecycle(
        control_type::open(
          files, ownership, spec, 0, false, budget, limits(), work)));
    runtime::first_failure failed;
    seastar::promise<runtime::result<void>> proceed;
    bool released = false;
    std::optional<seastar::future<local_publication_outcome>> pending;
    try {
        auto before = take(control->snapshot());
        pending.emplace(control->update(
          [](local_shard_control& next) -> runtime::result<void> {
              next.object_high = local_object_high{16};
              return {};
          },
          [&proceed](const auto&, const auto&, auto&) {
              return proceed.get_future();
          },
          work));
        bool overlapped = false;
        auto second = co_await drive.lifecycle(control->update(
          [&overlapped](local_shard_control&) -> runtime::result<void> {
              overlapped = true;
              return {};
          },
          work));
        require(
          second.failure.error()
            && second.failure.error()->code() == errc::queue_full
            && !overlapped,
          "overlapping control edit was admitted");
        require(
          take(control->snapshot()).generation == before.generation,
          "control installed before publication");
        released = true;
        proceed.set_value(runtime::result<void>{});
        {
            auto waiting = std::move(*pending);
            pending.reset();
            auto done = co_await drive.lifecycle(std::move(waiting));
            take(done.failure.outcome());
        }
        {
            auto done = co_await drive.lifecycle(control->update(
              [](local_shard_control& next) -> runtime::result<void> {
                  next.decision_high = local_decision_high{8};
                  return {};
              },
              work));
            take(done.failure.outcome());
        }
        const auto current = take(control->snapshot());
        require(
          current.fields.object_high.value() == 16
            && current.fields.decision_high.value() == 8,
          "serialized update lost another field");
        require(
          current.generation.value() == before.generation.value() + 2,
          "control generation did not advance exactly");
        auto reset = co_await drive.lifecycle(control->update(
          [](local_shard_control& next) -> runtime::result<void> {
              next.object_high = local_object_high{};
              return {};
          },
          work));
        require(
          reset.failure.failed()
            && take(control->snapshot()).fields.object_high.value() == 16,
          "counter high mark reset");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (!released)
        proceed.set_value(runtime::failure(detail::path_error(errc::aborted)));
    if (pending) {
        try {
            auto done = co_await drive.lifecycle(std::move(*pending));
            failed.observe(done.failure.outcome());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        pending.reset();
    }
    take(co_await drive.lifecycle(control->close()));
    require(
      budget.snapshot().tasks == 1,
      "closed control refunded retained state early");
    control.reset();
    take(failed.outcome());
    control = take(
      co_await drive.lifecycle(
        control_type::open(
          files, ownership, spec, 0, false, budget, limits(), work)));
    const auto reopened = control->snapshot();
    take(co_await drive.lifecycle(control->close()));
    control.reset();
    require(
      reopened && reopened->fields.object_high.value() == 16
        && reopened->fields.decision_high.value() == 8,
      "reopen lost persisted control fields");
    const auto stable = co_await read_bytes(files, marker, drive);
    co_await write_bytes(
      files, marker, local_fixture::read("future_minimum"), drive);
    {
        auto report = take(
          co_await drive.lifecycle(inspect_local_store(
            files, ownership, spec, unopened, budget, limits(), work)));
        require(
          report.state == local_store_state::unsupported,
          "future format was not reported");
        auto refused = co_await drive.lifecycle(initialize_local_stores(
          files, ownership, specs, create, budget, limits(), work));
        require(
          refused.failure.failed() && !refused.mutation_attempted,
          "unsupported store was changed");
        require(
          (co_await read_bytes(files, marker, drive))
            == local_fixture::read("future_minimum"),
          "unsupported bytes were rewritten");
    }
    for (auto name : {"reserved_common", "payload_length"}) {
        const auto corrupt = local_fixture::read(name);
        co_await write_bytes(files, marker, corrupt, drive);
        auto report = take(
          co_await drive.lifecycle(inspect_local_store(
            files, ownership, spec, unopened, budget, limits(), work)));
        require(
          report.state == local_store_state::corrupt,
          "malformed store was not classified corrupt");
        auto refused = co_await drive.lifecycle(initialize_local_stores(
          files, ownership, specs, create, budget, limits(), work));
        require(
          refused.failure.failed() && !refused.mutation_attempted,
          "corrupt identity was rewritten");
        require(
          (co_await read_bytes(files, marker, drive)) == corrupt,
          "corrupt bytes changed on rejection");
    }
    co_await write_bytes(files, marker, stable, drive);
    take(co_await drive.lifecycle(files.remove_file(control_path)));
    {
        auto refused = co_await drive.lifecycle(initialize_local_stores(
          files, ownership, specs, unopened, budget, limits(), work));
        require(
          refused.failure.failed() && !refused.mutation_attempted,
          "missing existing control was initialized empty");
        require(
          !take(co_await drive.lifecycle(files.exists(control_path))),
          "missing control was recreated");
    }
}
} // namespace kwaque::storage::testing::store_contract
