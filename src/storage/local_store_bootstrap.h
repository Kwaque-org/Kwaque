#pragma once

#include "src/storage/local_publication.h"
#include "src/storage/local_store_inspection.h"

#include <array>
#include <optional>
#include <span>

namespace kwaque::storage {
namespace detail {
// Called only after independent metadata validation, under the same exclusive
// namespace owner. A fresh checked file owner renews data/name durability;
// there is no retry of a failed barrier on an existing owner and no
// replacement.
template<runtime::file_system_backend Backend>
seastar::future<local_publication_outcome> confirm_local_metadata(
  Backend& files,
  const local_device_spec& spec,
  const runtime::file_path& path,
  byte_count validated_size,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    local_publication_outcome output;
    auto reservation = budget.try_reserve(limits.execution_bytes);
    if (!reservation) {
        output.failure.observe(reservation);
        co_return output;
    }
    if (auto handles = reservation->try_acquire_handles(2); !handles) {
        output.failure.observe(handles);
        co_return output;
    }
    output = local_publication_outcome{std::move(*reservation)};
    auto inspected = co_await inspect_local_path(
      files, spec.root, path, runtime::file_kind::regular, work);
    if (!inspected) {
        output.failure.observe(inspected);
        co_return output;
    }
    std::optional<typename Backend::directory_cursor_type> parent;
    std::optional<runtime::file> file;
    std::optional<runtime::file::metadata_reservation> barrier;
    try {
        do {
            const auto split = path.value().rfind('/');
            auto directory = runtime::file_path::make(
              split == 0 ? "/" : path.value().substr(0, split));
            if (!directory) {
                output.failure.observe(directory);
                break;
            }
            auto opened = co_await files.open_directory(
              *directory, runtime::file_close_policy::checked);
            if (!opened) {
                output.failure.observe(opened);
                break;
            }
            parent.emplace(std::move(*opened));
            output.stage = local_publication_stage::parent_open;
            auto opened_file = co_await files.open(
              path,
              {.access = runtime::file_access::read_write,
               .close_policy = runtime::file_close_policy::checked});
            if (!opened_file) {
                output.failure.observe(opened_file);
                break;
            }
            file.emplace(std::move(*opened_file));
            auto size = co_await file->size();
            if (!size) {
                output.failure.observe(size);
                break;
            }
            if (*size != validated_size.value()) {
                output.failure.observe(path_error(errc::wrong_context));
                break;
            }
            auto unit = file->try_reserve_metadata();
            if (!unit) {
                output.failure.observe(unit);
                break;
            }
            barrier.emplace(std::move(*unit));
            auto synced = co_await file->flush(*barrier);
            if (!synced) {
                output.failure.observe(synced);
                break;
            }
            output.stage = local_publication_stage::file_synced;
            barrier.reset();
            auto closed = co_await file->close();
            file.reset();
            if (!closed) {
                output.failure.observe(closed);
                break;
            }
            output.stage = local_publication_stage::file_closed;
            synced = co_await parent->sync();
            if (!synced) {
                output.failure.observe(synced);
                break;
            }
            output.stage = local_publication_stage::directory_synced;
            output.disposition = local_publication_disposition::durable;
        } while (false);
    } catch (...) {
        output.failure.observe(std::current_exception());
    }
    barrier.reset();
    if (file) {
        try {
            output.failure.observe(co_await file->close());
        } catch (...) {
            output.failure.observe(std::current_exception());
        }
    }
    if (parent) {
        try {
            output.failure.observe(co_await parent->close());
        } catch (...) {
            output.failure.observe(std::current_exception());
        }
    }
    co_return output;
}

template<runtime::file_system_backend Backend>
seastar::future<runtime::result<void>> ensure_local_directory(
  Backend& files,
  const local_device_spec& spec,
  const runtime::file_path& parent,
  const runtime::file_name& name,
  codec::cooperative_work& work) {
    auto checked = co_await inspect_local_path(
      files, spec.root, parent, runtime::file_kind::directory, work);
    if (!checked) co_return checked;
    auto path = local_child_path(parent, name);
    if (!path) co_return runtime::failure(path.error());
    auto opened = co_await files.open_directory(
      parent, runtime::file_close_policy::checked);
    if (!opened) co_return runtime::failure(opened.error());
    auto directory = std::move(*opened);
    runtime::first_failure failed;
    try {
        auto status = co_await files.stat(*path);
        if (!status) {
            if (status.error().code() != errc::not_found)
                failed.observe(status);
            else {
                auto ready = work.poll();
                if (!ready)
                    failed.observe(path_error(ready.error().code()));
                else
                    failed.observe(co_await files.create_directories(*path));
            }
        } else if (status->kind != runtime::file_kind::directory)
            failed.observe(path_error(errc::wrong_context));
        if (!failed.failed()) failed.observe(co_await directory.sync());
    } catch (...) {
        failed.observe(std::current_exception());
    }
    try {
        failed.observe(co_await directory.close());
    } catch (...) {
        failed.observe(std::current_exception());
    }
    co_return failed.outcome();
}

template<runtime::file_system_backend Backend>
seastar::future<local_publication_outcome> publish_initial_metadata(
  Backend& files,
  const local_device_spec& spec,
  local_metadata_header header,
  const local_metadata_payload& payload,
  const runtime::file_path& path,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    local_publication_outcome output;
    auto reservation = budget.try_reserve(
      byte_count{
        limits.operation_bytes.value() + limits.execution_bytes.value()});
    if (!reservation) {
        output.failure.observe(reservation);
        co_return output;
    }
    auto encoded = co_await encode_local_metadata(
      {header, spec.identity.metadata_alignment},
      payload,
      work,
      limits.operation_bytes,
      limits.charge);
    if (!encoded) {
        output.failure.observe(path_error(encoded.error().code()));
        co_return output;
    }
    const auto split = path.value().rfind('/');
    auto parent = runtime::file_path::make(
      split == 0 ? "/" : path.value().substr(0, split));
    auto name = runtime::file_name::make(path.value().substr(split + 1));
    if (!parent || !name) {
        output.failure.observe(path_error(errc::invalid_argument));
        co_return output;
    }
    local_file_publisher<Backend> publisher{
      files,
      budget,
      {header.owner(),
       spec.root,
       *parent,
       *name,
       runtime::file_rename_policy::no_replace,
       {}}};
    try {
        output = co_await publisher.publish(
          {header.owner(), header.generation(), {}},
          std::move(encoded->bytes),
          work);
    } catch (...) {
        output.failure.observe(std::current_exception());
    }
    try {
        output.failure.observe(co_await publisher.close());
    } catch (...) {
        output.failure.observe(std::current_exception());
    }
    co_return output;
}
} // namespace detail

enum class local_bootstrap_stage : std::uint8_t {
    uninspected,
    classified,
    identity,
    complete
};
struct local_bootstrap_outcome final {
    std::array<local_store_report, maximum_local_devices> reports{};
    std::array<local_bootstrap_stage, maximum_local_devices> stages{};
    std::size_t inspected{0};
    bool mutation_attempted{false};
    runtime::first_failure failure;
    std::optional<local_publication_outcome> last_publication;
};

// All specs and the external ownership provider outlive this joined operation.
// Own configured roots must already exist on their expected mounts. This never
// creates/replaces a root, cleans temps, resets counters, or activates a WAL.
// Creation/resumption is limited to a fully matching zero-state bootstrap
// prefix across the complete device set. Any failure requires a fresh
// classified startup attempt; no data/ID admission follows this outcome's
// failure.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<local_bootstrap_outcome> initialize_local_stores(
  Backend& files,
  Owner& owner,
  std::span<const local_device_spec> specs,
  local_store_open_options options,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    local_bootstrap_outcome output;
    if (auto valid = validate_local_device_set(specs); !valid) {
        output.failure.observe(valid);
        co_return output;
    }
    if (auto valid = limits.validate(); !valid) {
        output.failure.observe(valid);
        co_return output;
    }
    auto reservation = budget.try_reserve(
      byte_count{limits.execution_bytes.value() + 32768});
    if (!reservation) {
        output.failure.observe(reservation);
        co_return output;
    }
    try {
        // No storage mutation until every configured owner and namespace
        // passes.
        for (const auto& spec : specs) {
            auto owned = co_await owner.validate(spec);
            if (!owned) {
                output.failure.observe(owned);
                co_return output;
            }
        }
        bool all_existing = true, compatible = true;
        for (std::size_t i = 0; i < specs.size(); ++i) {
            auto report = co_await inspect_local_store(
              files, owner, specs[i], options, budget, limits, work);
            if (!report) {
                output.failure.observe(report);
                co_return output;
            }
            output.reports[i] = *report;
            ++output.inspected;
            output.stages[i] = local_bootstrap_stage::classified;
            all_existing = all_existing
                           && report->state == local_store_state::existing;
            compatible = compatible && report->bootstrap_compatible;
            if (
              report->state == local_store_state::unsupported
              || report->state == local_store_state::corrupt) {
                output.failure.observe(report->reason.value_or(
                  detail::path_error(errc::malformed_data)));
                co_return output;
            }
        }
        if (
          options.intent == local_store_intent::must_exist
          || options.require_wal_head || !compatible) {
            if (!all_existing)
                output.failure.observe(detail::path_error(errc::not_found));
            else
                for (std::size_t i = 0; i < specs.size(); ++i)
                    output.stages[i] = local_bootstrap_stage::complete;
            co_return output;
        }
        // One startup-held namespace handle covers each joined directory step;
        // record publication/confirmation admits its own two-handle owners.
        if (auto handle = reservation->try_acquire_handles(1); !handle) {
            output.failure.observe(handle);
            co_return output;
        }
        for (std::size_t i = 0; i < specs.size(); ++i) {
            const auto& spec = specs[i];
            auto owned = co_await owner.validate(spec);
            if (!owned) {
                output.failure.observe(owned);
                co_return output;
            }
            auto paths = local_paths::make(spec.root).value();
            auto marker = paths.store();
            if (!marker) {
                output.failure.observe(marker);
                co_return output;
            }
            {
                auto existing = co_await load_local_identity(
                  files, spec, *marker, budget, limits, work);
                if (existing) {
                    if (
                      std::get<local_store_identity>(existing->value.payload())
                      != spec.identity) {
                        output.failure.observe(
                          detail::path_error(errc::wrong_context));
                        co_return output;
                    }
                    output.last_publication.emplace(
                      co_await detail::confirm_local_metadata(
                        files,
                        spec,
                        *marker,
                        existing->value.encoded_bytes(),
                        budget,
                        limits,
                        work));
                } else if (existing.error().code() == errc::not_found) {
                    const auto header
                      = local_metadata_header::make(
                          local_metadata_kind::store_identity,
                          spec.owner,
                          local_publication_generation::make(1).value())
                          .value();
                    const local_metadata_payload payload{spec.identity};
                    output.mutation_attempted = true;
                    output.last_publication.emplace(
                      co_await detail::publish_initial_metadata(
                        files,
                        spec,
                        header,
                        payload,
                        *marker,
                        budget,
                        limits,
                        work));
                } else {
                    output.failure.observe(existing);
                    co_return output;
                }
                if (output.last_publication->failure.failed()) {
                    output.failure = output.last_publication->failure;
                    co_return output;
                }
            }
            output.stages[i] = local_bootstrap_stage::identity;
            auto shards = local_child_path(
                            spec.root,
                            runtime::file_name::make("shards").value())
                            .value();
            output.mutation_attempted = true;
            auto created = co_await detail::ensure_local_directory(
              files,
              spec,
              spec.root,
              runtime::file_name::make("shards").value(),
              work);
            if (!created) {
                output.failure.observe(created);
                co_return output;
            }
            for (std::uint32_t shard = 0; shard < spec.identity.shard_count;
                 ++shard) {
                auto control = paths.control(shard);
                if (!control) {
                    output.failure.observe(control);
                    co_return output;
                }
                const auto split = control->value().rfind('/');
                const auto directory = runtime::file_path::make(
                                         control->value().substr(0, split))
                                         .value();
                const auto name = runtime::file_name::make(
                                    directory.value().substr(
                                      directory.value().rfind('/') + 1))
                                    .value();
                created = co_await detail::ensure_local_directory(
                  files, spec, shards, name, work);
                if (!created) {
                    output.failure.observe(created);
                    co_return output;
                }
                if (spec.stores_data()) {
                    created = co_await detail::ensure_local_directory(
                      files,
                      spec,
                      directory,
                      runtime::file_name::make("segments").value(),
                      work);
                    if (!created) {
                        output.failure.observe(created);
                        co_return output;
                    }
                }
                if (spec.controls()) {
                    for (auto leaf :
                         {"wal", "checkpoints", "decisions", "deletions"}) {
                        created = co_await detail::ensure_local_directory(
                          files,
                          spec,
                          directory,
                          runtime::file_name::make(leaf).value(),
                          work);
                        if (!created) {
                            output.failure.observe(created);
                            co_return output;
                        }
                    }
                    auto checkpoints
                      = local_child_path(
                          directory,
                          runtime::file_name::make("checkpoints").value())
                          .value();
                    created = co_await detail::ensure_local_directory(
                      files,
                      spec,
                      checkpoints,
                      runtime::file_name::make("evidence").value(),
                      work);
                    if (!created) {
                        output.failure.observe(created);
                        co_return output;
                    }
                    auto existing = co_await load_local_control(
                      files, spec, shard, *control, budget, limits, work);
                    if (existing) {
                        if (!detail::initial_control(*existing)) {
                            output.failure.observe(
                              detail::path_error(errc::wrong_context));
                            co_return output;
                        }
                        output.last_publication.emplace(
                          co_await detail::confirm_local_metadata(
                            files,
                            spec,
                            *control,
                            existing->value.encoded_bytes(),
                            budget,
                            limits,
                            work));
                    } else if (existing.error().code() == errc::not_found) {
                        const auto header
                          = local_metadata_header::make(
                              local_metadata_kind::shard_control,
                              spec.shard_owner(shard).value(),
                              local_publication_generation::make(1).value())
                              .value();
                        const local_metadata_payload payload{
                          local_shard_control{
                            local_wal_high{},
                            local_object_high{},
                            local_decision_high{},
                            local_deletion_high{},
                            {},
                            {}}};
                        output.last_publication.emplace(
                          co_await detail::publish_initial_metadata(
                            files,
                            spec,
                            header,
                            payload,
                            *control,
                            budget,
                            limits,
                            work));
                    } else {
                        output.failure.observe(existing);
                        co_return output;
                    }
                    if (output.last_publication->failure.failed()) {
                        output.failure = output.last_publication->failure;
                        co_return output;
                    }
                }
            }
            output.stages[i] = local_bootstrap_stage::complete;
        }
    } catch (...) {
        output.failure.observe(std::current_exception());
    }
    co_return output;
}
} // namespace kwaque::storage
