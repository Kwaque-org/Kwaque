#pragma once

#include "src/storage/local_store_config.h"
#include "src/storage/workload_budget.h"

#include <array>

namespace kwaque::storage {
enum class local_entry_kind : std::uint8_t {
    directory,
    broker_file,
    store,
    control,
    wal,
    segment_directory,
    descriptor,
    data,
    publication,
    object,
    checkpoint,
    evidence,
    decision,
    deletion,
    temporary,
    unknown
};
struct local_namespace_entry final {
    runtime::file_path path;
    runtime::file_kind file_kind;
    local_entry_kind kind{local_entry_kind::unknown};
    std::uint32_t shard{local_store_shard};
    std::optional<local_segment_name> segment;
    std::optional<model::wal_incarnation_id> wal;
    std::uint64_t sequence{0};
    std::optional<local_temporary_identity> temporary;
    local_entry_kind temporary_target{local_entry_kind::unknown};
    bool unexpected_kind{false};
};
struct local_discovery_limits final {
    std::uint64_t maximum_entries{1000000};
    std::uint32_t maximum_wal_files{65536};
    [[nodiscard]] runtime::result<void> validate() const noexcept {
        if (!maximum_entries || !maximum_wal_files)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        return {};
    }
};
struct local_inventory_progress final {
    std::uint64_t visited{0};
    bool complete{false};
};
namespace detail {
enum class local_namespace_node : std::uint8_t {
    root,
    shards,
    shard,
    wal,
    wal_bucket,
    segments,
    segment_bucket,
    segment,
    objects,
    checkpoints,
    evidence,
    decisions,
    deletions
};
struct local_walk_position final {
    local_namespace_node node{local_namespace_node::root};
    std::uint32_t shard{local_store_shard};
    std::uint8_t bucket{0};
    std::optional<local_segment_name> segment;
};
struct local_classified_entry final {
    local_namespace_entry entry;
    std::optional<local_walk_position> descend;
};
[[nodiscard]] runtime::result<local_classified_entry> classify_local_entry(
  const local_device_spec&,
  const runtime::file_path&,
  local_walk_position,
  const runtime::directory_entry&);
} // namespace detail

// Quiescent owned namespace, one borrowed entry per joined callback. Unknown
// entries are reported without opening/following them. An early consumer stop
// returns complete=false; an admission/I/O/close error never means an empty or
// complete inventory. No sorting, whole-store collection, repair or removal.
template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  typename Visitor>
requires std::same_as<
  std::invoke_result_t<Visitor&, const local_namespace_entry&>,
  seastar::future<runtime::result<bool>>>
seastar::future<runtime::result<local_inventory_progress>> walk_local_namespace(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work,
  Visitor visit,
  local_discovery_limits bounds = {}) {
    static_assert(sizeof(Visitor) <= 4096);
    if (auto valid = validate_local_device_spec(spec); !valid)
        co_return runtime::failure(valid.error());
    if (auto valid = limits.validate(); !valid)
        co_return runtime::failure(valid.error());
    if (auto valid = bounds.validate(); !valid)
        co_return runtime::failure(valid.error());
    auto held = budget.try_reserve(
      byte_count{limits.execution_bytes.value() + 131072});
    if (!held) co_return runtime::failure(held.error());
    if (auto handles = held->try_acquire_handles(8); !handles)
        co_return runtime::failure(handles.error());
    auto valid = co_await ownership.validate(spec);
    if (!valid) co_return runtime::failure(valid.error());
    struct frame final {
        std::optional<runtime::file_path> path;
        std::optional<typename Backend::directory_cursor_type> cursor;
        std::optional<runtime::directory_page> page;
        detail::local_walk_position position;
        std::size_t index{0};
        bool end{false};
    };
    std::array<frame, 8> frames;
    std::size_t depth = 0;
    local_inventory_progress progress;
    runtime::first_failure failed;
    bool stopped = false;
    // Named callback remains alive across its immediately joined awaits.
    auto push = [&](
                  runtime::file_path path, detail::local_walk_position position)
      -> seastar::future<runtime::result<void>> {
        if (depth == frames.size())
            co_return runtime::failure(
              detail::path_error(errc::resource_exhausted));
        auto checked = co_await inspect_local_path(
          files, spec.root, path, runtime::file_kind::directory, work);
        if (!checked) co_return checked;
        auto cursor = co_await files.open_directory(
          path, runtime::file_close_policy::checked);
        if (!cursor) co_return runtime::failure(cursor.error());
        auto& current = frames[depth++];
        current.path.emplace(std::move(path));
        current.cursor.emplace(std::move(*cursor));
        current.position = position;
        current.index = 0;
        current.end = false;
        co_return runtime::result<void>{};
    };
    try {
        failed.observe(co_await push(spec.root, {}));
        while (depth && !failed.failed() && !stopped) {
            auto& current = frames[depth - 1];
            if (
              !current.page
              || current.index == current.page->entries().size()) {
                current.page.reset();
                current.index = 0;
                if (current.end) {
                    failed.observe(co_await current.cursor->close());
                    current.cursor.reset();
                    current.path.reset();
                    --depth;
                    continue;
                }
                valid = co_await detail::path_checkpoint(work);
                if (!valid) {
                    failed.observe(valid);
                    break;
                }
                auto page = co_await current.cursor->next(
                  {.maximum_entries = item_count{16},
                   .maximum_name_bytes = byte_count{4096}});
                if (!page) {
                    failed.observe(page);
                    break;
                }
                current.end = page->end();
                current.page.emplace(std::move(*page));
                continue;
            }
            if (progress.visited == bounds.maximum_entries) {
                failed.observe(detail::path_error(errc::resource_exhausted));
                break;
            }
            valid = co_await detail::path_checkpoint(work);
            if (!valid) {
                failed.observe(valid);
                break;
            }
            auto classified = detail::classify_local_entry(
              spec,
              *current.path,
              current.position,
              current.page->entries()[current.index++]);
            if (!classified) {
                failed.observe(classified);
                break;
            }
            // Recheck the entry itself without following links. Descents also
            // recheck every owned ancestor immediately before cursor open.
            auto status = co_await files.stat(classified->entry.path);
            if (!status) {
                failed.observe(status);
                break;
            }
            if (status->kind != classified->entry.file_kind) {
                classified->entry.unexpected_kind = true;
                classified->entry.file_kind = status->kind;
                classified->descend.reset();
            }
            ++progress.visited;
            auto consumed = co_await visit(classified->entry);
            if (!consumed) {
                failed.observe(consumed);
                break;
            }
            stopped = !*consumed;
            if (!stopped && classified->descend)
                failed.observe(
                  co_await push(classified->entry.path, *classified->descend));
        }
    } catch (...) {
        failed.observe(std::current_exception());
    }
    const bool enumerated = depth == 0 && !stopped && !failed.failed();
    while (depth) {
        auto& current = frames[depth - 1];
        current.page.reset();
        if (current.cursor) {
            try {
                failed.observe(co_await current.cursor->close());
            } catch (...) {
                failed.observe(std::current_exception());
            }
            current.cursor.reset();
        }
        current.path.reset();
        --depth;
    }
    if (!failed.failed()) {
        try {
            failed.observe(co_await ownership.validate(spec));
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    if (auto result = failed.outcome(); !result)
        co_return runtime::failure(result.error());
    progress.complete = enumerated;
    co_return progress;
}
} // namespace kwaque::storage
