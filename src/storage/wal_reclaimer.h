#pragma once

#include "src/base/units.h"
#include "src/storage/local_control.h"
#include "src/storage/local_discovery.h"
#include "src/storage/local_paths.h"
#include "src/storage/local_spares.h"
#include "src/storage/retained_wal.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <array>
#include <cstdint>
#include <exception>
#include <optional>
#include <utility>

namespace kwaque::storage {

// What removing the WAL's prefix did.
struct wal_reclaim_outcome final {
    // Files removed by this call, and of them those kept as spares.
    std::uint32_t removed{0}, recycled{0};
    // Files below the cutoff's file that are still there: debt, tried again
    // by the next call.
    std::uint32_t owed{0};
    // Why the first of them could not be removed.
    std::optional<runtime::operation_error> failed;
};

// Removes the WAL files `holder` names below the file `cutoff` lies in,
// oldest first, and stops at the first that cannot be removed: the chain
// above the cutoff needs every file, and below it a hole would only hide
// debt. The names are the WAL writer's own, kept as each file became the
// head; `holder` gives them as retained_wal() and is told of each removal
// through wal_released(). While the WAL's spares, which `holder` gives as
// wal_spares(), have room, a file is moved in among them instead, under one
// turn of theirs for the call, and the spare directory is synced once
// after the moves; one unlinked awaits no directory sync. The durable
// cutoff is the authority: a name that comes back after a crash is below it
// and is taken again at the next open, and incarnations are never used
// twice. A file already gone counts as removed. Nothing here reports space
// as durably released.
template<runtime::file_system_backend Backend, typename Holder>
seastar::future<wal_reclaim_outcome> reclaim_wal_prefix(
  Backend& files,
  const local_device_spec& spec,
  std::uint32_t shard,
  local_wal_cursor cutoff,
  Holder& holder) {
    wal_reclaim_outcome output;
    const auto paths = local_paths::make(spec.root);
    if (!paths) {
        output.owed = holder.retained_wal().below(cutoff);
        if (output.owed != 0) output.failed = paths.error();
        co_return output;
    }
    auto& spares = holder.wal_spares();
    const auto turn = co_await spares.turn();
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    while (holder.retained_wal().below(cutoff) != 0) {
        const auto oldest = *holder.retained_wal().oldest();
        const auto path = paths->wal(shard, oldest);
        if (!path) {
            output.failed = path.error();
            break;
        }
        try {
            const auto kept = co_await spares.move_in(*path, oldest, work);
            if (!kept) {
                output.failed = kept.error();
                break;
            }
            if (*kept) {
                ++output.recycled;
            } else {
                const auto removed = co_await files.remove_file(*path);
                if (!removed && removed.error().code() != errc::not_found) {
                    output.failed = removed.error();
                    break;
                }
            }
        } catch (...) {
            output.failed = detail::recognized_or_rethrown();
            break;
        }
        holder.wal_released();
        ++output.removed;
    }
    try {
        // A spare whose move is not durable comes back under its old name
        // below the cutoff, and is taken again at the next open.
        if (const auto settled = co_await spares.settle(work); !settled)
            if (!output.failed) output.failed = settled.error();
    } catch (...) {
        if (!output.failed) output.failed = detail::recognized_or_rethrown();
    }
    output.owed = holder.retained_wal().below(cutoff);
    co_return output;
}

// What a reopen removed below the durable cutoff.
struct wal_reopen_outcome final {
    // Names below the cutoff's file, removed unread, and of them those kept
    // as spares.
    std::uint32_t removed{0}, recycled{0};
    // Names at or above it, left for the chain walk.
    std::uint32_t kept{0};
    // Temporaries of WAL files: bodies made ahead that no rotation took, and
    // what interrupted publications left. With the WAL's spares each is
    // kept as one or removed; without, each counts among the other names.
    std::uint32_t temporaries{0};
    // Names that are not WAL files and anything unknown. They are left for
    // their own owner.
    std::uint32_t other{0};
    // What the spare directory held.
    local_spares_found spares{};
};

namespace detail {
// Takes in a WAL file nothing reads again: as a spare while `spares` has
// room, else by its removal. True when it was kept.
template<runtime::file_system_backend Backend>
seastar::future<runtime::result<bool>> retire_wal_file(
  Backend& files,
  local_spares<Backend, model::wal_incarnation_id>* spares,
  const runtime::file_path& path,
  model::wal_incarnation_id name,
  codec::cooperative_work& work) {
    if (spares) {
        auto kept = co_await spares->move_in(path, name, work);
        if (!kept || *kept) co_return kept;
    }
    auto removed = co_await files.remove_file(path);
    if (!removed && removed.error().code() != errc::not_found)
        co_return runtime::failure(removed.error());
    co_return false;
}
} // namespace detail

// At reopen: every WAL name of the shard below the file the durable cutoff
// lies in is removed without being opened, whatever it holds. Such a name is
// a file a checkpoint reclaimed whose removal a crash undid, or failed;
// nothing reads below the cutoff, so its content proves nothing. The cutoff
// is read from the open control owner, which confirmed its record durable:
// a cutoff copied from anywhere else could name files an older head still
// needs. A name at or above the cutoff is left for the chain walk, which
// fails when one of them is missing. Names are compared as incarnations,
// never guessed from a neighbour. The first removal that fails is returned.
// With no checkpoint there is no cutoff and nothing below it is removed.
//
// Given the WAL's spares, under one turn of theirs: the spares their
// directory holds are taken in first, those of no more than `capacity`
// bytes up to their bound, and the rest removed unread; then a name below
// the cutoff, and a temporary of a WAL file at any place, which nothing
// names and no rotation took, is moved in among them while there is room
// and removed otherwise. None is ever read or promoted by its name.
//
// A restart calls this only once its plan is ready. A plan that stops
// permits no mutation, and a name that came back below the cutoff is then
// left as the crash left it.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<wal_reopen_outcome>> remove_wal_below(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  std::uint32_t shard,
  const local_control_owner<Backend, Owner>& head,
  codec::cooperative_work& work,
  local_spares<Backend, model::wal_incarnation_id>* spares = nullptr,
  byte_count capacity = {}) {
    if (auto valid = validate_local_device_spec(spec); !valid)
        co_return runtime::failure(valid.error());
    const auto durable = head.checkpoint_end();
    if (!durable && !spares) co_return wal_reopen_outcome{};
    if (!spec.shard_owner(shard) || !spec.controls())
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    if (auto valid = co_await ownership.validate(spec); !valid)
        co_return runtime::failure(valid.error());
    std::optional<seastar::semaphore_units<>> turn;
    wal_reopen_outcome output;
    if (spares) {
        turn.emplace(co_await spares->turn());
        auto found = co_await spares->adopt(
          [capacity](byte_count bytes) { return bytes <= capacity; }, work);
        if (!found) co_return runtime::failure(found.error());
        output.spares = *found;
    }
    const auto paths = local_paths::make(spec.root);
    if (!paths) co_return runtime::failure(paths.error());
    const auto root = paths->buckets(shard, local_bucket_kind::wal);
    if (!root) co_return runtime::failure(root.error());
    using cursor = typename Backend::directory_cursor_type;
    std::optional<cursor> buckets, names;
    runtime::first_failure failed;
    try {
        do {
            auto inspected = co_await inspect_local_path(
              files, spec.root, *root, runtime::file_kind::directory, work);
            if (!inspected) {
                failed.observe(inspected);
                break;
            }
            auto opened = co_await files.open_directory(*root);
            if (!opened) {
                failed.observe(opened);
                break;
            }
            buckets.emplace(std::move(*opened));
            bool end = false;
            while (!end && !failed.failed()) {
                if (
                  auto ready = co_await detail::path_checkpoint(work); !ready) {
                    failed.observe(ready);
                    break;
                }
                auto page = co_await buckets->next(
                  {.maximum_entries = item_count{1},
                   .maximum_name_bytes = byte_count{255}});
                if (!page) {
                    failed.observe(page);
                    break;
                }
                end = page->end();
                for (const auto& bucket : page->entries()) {
                    const auto number = parse_local_bucket(bucket.name.value());
                    if (
                      !number || bucket.kind != runtime::file_kind::directory) {
                        ++output.other;
                        continue;
                    }
                    auto parent = local_child_path(*root, bucket.name);
                    if (!parent) {
                        failed.observe(parent);
                        break;
                    }
                    opened = co_await files.open_directory(*parent);
                    if (!opened) {
                        failed.observe(opened);
                        break;
                    }
                    names.emplace(std::move(*opened));
                    bool last = false;
                    while (!last && !failed.failed()) {
                        if (
                          auto ready = co_await detail::path_checkpoint(work);
                          !ready) {
                            failed.observe(ready);
                            break;
                        }
                        auto entries = co_await names->next(
                          {.maximum_entries = item_count{16},
                           .maximum_name_bytes = byte_count{4_KiB}});
                        if (!entries) {
                            failed.observe(entries);
                            break;
                        }
                        last = entries->end();
                        for (const auto& entry : entries->entries()) {
                            const auto& name = entry.name.value();
                            auto file = parse_local_wal_name(name, *number);
                            bool temporary = false;
                            if (!file && spares && name.size() > 24) {
                                const auto target = runtime::file_name::make(
                                  std::string_view{name}.substr(
                                    0, name.size() - 24));
                                if (
                                  target
                                  && parse_local_temporary_name(
                                    name, *target)) {
                                    file = parse_local_wal_name(
                                      target->value(), *number);
                                    temporary = file.has_value();
                                }
                            }
                            if (
                              !file
                              || entry.kind != runtime::file_kind::regular) {
                                ++output.other;
                                continue;
                            }
                            if (
                              !temporary
                              && (!durable || !file->canonical_less(durable->incarnation()))) {
                                ++output.kept;
                                continue;
                            }
                            auto path = local_child_path(*parent, entry.name);
                            if (!path) {
                                failed.observe(path);
                                break;
                            }
                            auto retired = co_await detail::retire_wal_file(
                              files, spares, *path, *file, work);
                            if (!retired) {
                                failed.observe(retired);
                                break;
                            }
                            if (temporary)
                                ++output.temporaries;
                            else
                                ++output.removed;
                            if (*retired) ++output.recycled;
                        }
                    }
                    failed.observe(co_await names->close());
                    names.reset();
                    if (failed.failed()) break;
                }
            }
        } while (false);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (names) {
        try {
            failed.observe(co_await names->close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    if (buckets) {
        try {
            failed.observe(co_await buckets->close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    if (spares && !failed.failed()) {
        try {
            failed.observe(co_await spares->settle(work));
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    if (failed.failed()) {
        auto error = failed.outcome();
        co_return runtime::failure(error.error());
    }
    co_return output;
}

// Whether the shard holds a WAL file its control record never handed out:
// one whose incarnation is above `high`. The record makes an incarnation
// durable before a file of it is made, so a first start cut short leaves
// files at or below it, and one above it is of a WAL the record is older
// than. Names that are no WAL file, such as an unfinished publication's
// temporary, are passed over. A shard with no WAL directory holds none.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<bool>> holds_unallocated_wal(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  std::uint32_t shard,
  local_wal_high high,
  codec::cooperative_work& work) {
    if (auto valid = validate_local_device_spec(spec); !valid)
        co_return runtime::failure(valid.error());
    if (!spec.shard_owner(shard) || !spec.controls())
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    if (auto valid = co_await ownership.validate(spec); !valid)
        co_return runtime::failure(valid.error());
    const auto paths = local_paths::make(spec.root);
    if (!paths) co_return runtime::failure(paths.error());
    const auto root = paths->buckets(shard, local_bucket_kind::wal);
    if (!root) co_return runtime::failure(root.error());
    using cursor = typename Backend::directory_cursor_type;
    std::optional<cursor> buckets, names;
    bool found = false;
    runtime::first_failure failed;
    try {
        do {
            auto inspected = co_await inspect_local_path(
              files,
              spec.root,
              *root,
              runtime::file_kind::directory,
              work,
              true);
            if (!inspected) {
                failed.observe(inspected);
                break;
            }
            auto opened = co_await files.open_directory(*root);
            if (!opened) {
                if (opened.error().code() != errc::not_found)
                    failed.observe(opened);
                break;
            }
            buckets.emplace(std::move(*opened));
            bool end = false;
            while (!end && !found && !failed.failed()) {
                if (
                  auto ready = co_await detail::path_checkpoint(work); !ready) {
                    failed.observe(ready);
                    break;
                }
                auto page = co_await buckets->next(
                  {.maximum_entries = item_count{1},
                   .maximum_name_bytes = byte_count{255}});
                if (!page) {
                    failed.observe(page);
                    break;
                }
                end = page->end();
                for (const auto& bucket : page->entries()) {
                    const auto number = parse_local_bucket(bucket.name.value());
                    if (!number || bucket.kind != runtime::file_kind::directory)
                        continue;
                    auto parent = local_child_path(*root, bucket.name);
                    if (!parent) {
                        failed.observe(parent);
                        break;
                    }
                    opened = co_await files.open_directory(*parent);
                    if (!opened) {
                        failed.observe(opened);
                        break;
                    }
                    names.emplace(std::move(*opened));
                    bool last = false;
                    while (!last && !found && !failed.failed()) {
                        if (
                          auto ready = co_await detail::path_checkpoint(work);
                          !ready) {
                            failed.observe(ready);
                            break;
                        }
                        auto entries = co_await names->next(
                          {.maximum_entries = item_count{16},
                           .maximum_name_bytes = byte_count{4_KiB}});
                        if (!entries) {
                            failed.observe(entries);
                            break;
                        }
                        last = entries->end();
                        for (const auto& entry : entries->entries()) {
                            const auto file = parse_local_wal_name(
                              entry.name.value(), *number);
                            if (
                              !file
                              || entry.kind != runtime::file_kind::regular)
                                continue;
                            const auto handed
                              = local_wal_high::from_incarnation(*file);
                            if (!handed || *handed > high) {
                                found = true;
                                break;
                            }
                        }
                    }
                    failed.observe(co_await names->close());
                    names.reset();
                    if (failed.failed() || found) break;
                }
            }
        } while (false);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (names) {
        try {
            failed.observe(co_await names->close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    if (buckets) {
        try {
            failed.observe(co_await buckets->close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    if (failed.failed()) {
        auto error = failed.outcome();
        co_return runtime::failure(error.error());
    }
    co_return found;
}

} // namespace kwaque::storage
