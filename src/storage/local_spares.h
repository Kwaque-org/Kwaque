#pragma once

#include "src/base/error.h"
#include "src/base/units.h"
#include "src/codec/cooperative.h"
#include "src/runtime/error.h"
#include "src/runtime/file.h"
#include "src/runtime/first_failure.h"
#include "src/runtime/shard_affinity.h"
#include "src/storage/local_paths.h"
#include "src/storage/local_store_bootstrap.h"
#include "src/storage/local_store_config.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/semaphore.hh>

#include <algorithm>
#include <array>
#include <concepts>
#include <cstdint>
#include <exception>
#include <optional>
#include <utility>

namespace kwaque::storage {

// The most spares a shard keeps of WAL files, and of segments' data files
// on one data device, and how many unless configured.
inline constexpr std::uint32_t maximum_wal_spares = 8;
inline constexpr std::uint32_t default_wal_spares = 2;
inline constexpr std::uint32_t maximum_segment_spares = 64;
inline constexpr std::uint32_t default_segment_spares = 8;

// What a start found in a spare directory.
struct local_spares_found final {
    // Kept to be made into later files.
    std::uint32_t adopted{0};
    // Spares that could not be kept, beyond the bound or of no size a new
    // file can be made from: removed unread.
    std::uint32_t removed{0};
    // Names that are no spare's: left where they are.
    std::uint32_t other{0};
};

// The spares one shard keeps of one kind on one device: files a removal
// gave back instead of unlinking them, each kept to be made into a later
// file, so that the later file's writes overwrite blocks that are already
// written and its syncs need no allocation metadata. A spare is never
// read: a file made of one is written from its start, and what it holds of
// its old life carries another identity, at which every scan stops.
//
// This is the one owner of its directory. A move in renames the file,
// syncs the directory the name left and, once for the moves of one turn,
// the spare directory; a move out renames the spare and syncs the spare
// directory, and the caller syncs its destination after that. Moves and
// syncs run inside one turn at a time, so nothing makes a name in one of
// two directories durable before the other directory has let go of it. A
// crash leaves the file under one of its names, or, where each directory is
// made durable apart, under none: a spare lost, never one file under two
// names, which a later file made of the spare would overwrite.
//
// `Identity` names a spare by the identity its file had last, which is
// never used again: a WAL incarnation or a segment's directory name.
template<runtime::file_system_backend Backend, typename Identity>
class local_spares final : public runtime::shard_affine {
    static_assert(
      std::same_as<Identity, model::wal_incarnation_id>
      || std::same_as<Identity, local_segment_name>);

public:
    static constexpr local_spare_kind kind
      = std::same_as<Identity, model::wal_incarnation_id>
          ? local_spare_kind::wal
          : local_spare_kind::segment;

    // `spec` outlives this owner. A bound of zero keeps no spare.
    local_spares(
      Backend& files,
      const local_device_spec& spec,
      std::uint32_t shard,
      std::uint32_t bound) noexcept
      : files_(files)
      , spec_(&spec)
      , shard_(shard)
      , bound_(std::min(bound, maximum_segment_spares)) {}
    local_spares(const local_spares&) = delete;
    local_spares& operator=(const local_spares&) = delete;

    [[nodiscard]] std::uint32_t size() const noexcept {
        assert_current();
        return size_;
    }
    [[nodiscard]] bool full() const noexcept {
        assert_current();
        return size_ >= bound_;
    }
    // The bytes of the oldest spare, the one take() moves next, and of all
    // of them.
    [[nodiscard]] std::optional<byte_count> oldest() const noexcept {
        assert_current();
        if (size_ == 0) return std::nullopt;
        return entries_[first_].bytes;
    }
    [[nodiscard]] byte_count bytes() const noexcept {
        assert_current();
        return bytes_;
    }

    // Every move and sync of the spare directory runs inside a turn.
    [[nodiscard]] seastar::future<seastar::semaphore_units<>> turn() {
        assert_current();
        return seastar::get_units(turn_, 1);
    }

    // Inside a turn: moves the regular file at `from` in under `name`, and
    // syncs the directory `from` lies in. False, with nothing kept, when the
    // spares are full, when nothing is at `from`, when a spare holds the
    // name already, or when the spares would hold more than `within` bytes
    // with it: the caller then removes the file itself. settle() makes the
    // moves of the turn durable in the spare directory.
    [[nodiscard]] seastar::future<runtime::result<bool>> move_in(
      const runtime::file_path& from,
      Identity name,
      codec::cooperative_work& work,
      std::optional<byte_count> within = std::nullopt) {
        assert_current();
        if (size_ >= bound_) co_return false;
        const auto status = co_await files_.stat(from);
        if (!status) {
            if (status.error().code() == errc::not_found) co_return false;
            co_return runtime::failure(status.error());
        }
        if (status->kind != runtime::file_kind::regular) co_return false;
        if (within) {
            const auto held = bytes_.checked_add(status->size);
            if (!held || *held > *within) co_return false;
        }
        if (auto made = co_await prepare(work); !made)
            co_return runtime::failure(made.error());
        const auto to = path(name);
        if (!to) co_return runtime::failure(to.error());
        const auto renamed = co_await files_.rename(
          from, *to, runtime::file_rename_policy::no_replace);
        if (!renamed) {
            if (
              renamed.error().code() == errc::not_found
              || renamed.error().code() == errc::already_exists)
                co_return false;
            co_return runtime::failure(renamed.error());
        }
        const auto bytes = status->size;
        // The name it left goes first: until its directory is synced the
        // spare directory is not.
        if (auto synced = co_await sync(parent_of(from), work); !synced)
            co_return runtime::failure(synced.error());
        push(name, bytes);
        unsettled_ = true;
        co_return true;
    }
    // Inside a turn, after its moves in: syncs the spare directory, once.
    [[nodiscard]] seastar::future<runtime::result<void>>
    settle(codec::cooperative_work& work) {
        assert_current();
        if (!unsettled_) co_return runtime::result<void>{};
        const auto at = directory();
        if (!at) co_return runtime::failure(at.error());
        if (auto synced = co_await sync(*at, work); !synced) co_return synced;
        unsettled_ = false;
        co_return runtime::result<void>{};
    }
    // Inside a turn: moves the oldest spare to `to`, a name in a directory
    // its caller syncs only after this returns, and syncs the spare
    // directory. Its bytes, or none when there is no spare. A spare that is
    // gone is passed over; one whose move failed is dropped, and stays for
    // the next start to find.
    [[nodiscard]] seastar::future<runtime::result<std::optional<byte_count>>>
    take(const runtime::file_path& to, codec::cooperative_work& work) {
        assert_current();
        using output = runtime::result<std::optional<byte_count>>;
        while (size_ != 0) {
            const auto taken = pop();
            const auto from = path(taken.name);
            if (!from) co_return output{runtime::failure(from.error())};
            const auto renamed = co_await files_.rename(
              *from, to, runtime::file_rename_policy::no_replace);
            if (!renamed) {
                if (renamed.error().code() == errc::not_found) continue;
                co_return output{runtime::failure(renamed.error())};
            }
            const auto at = directory();
            if (!at) co_return output{runtime::failure(at.error())};
            if (auto synced = co_await sync(*at, work); !synced)
                co_return output{runtime::failure(synced.error())};
            unsettled_ = false;
            co_return output{taken.bytes};
        }
        co_return output{std::optional<byte_count>{}};
    }
    // Inside a turn: removes the oldest spares until `keep` are left, with
    // no directory sync: what space they hold goes back to the device. A
    // spare already gone counts as removed.
    [[nodiscard]] seastar::future<runtime::result<void>>
    trim(std::uint32_t keep, codec::cooperative_work& work) {
        assert_current();
        while (size_ > keep) {
            if (auto ready = co_await detail::path_checkpoint(work); !ready)
                co_return ready;
            const auto taken = pop();
            const auto at = path(taken.name);
            if (!at) co_return runtime::failure(at.error());
            const auto gone = co_await files_.remove_file(*at);
            if (!gone && gone.error().code() != errc::not_found) co_return gone;
        }
        co_return runtime::result<void>{};
    }
    // Inside a turn, at a start: keeps the spares the directory holds whose
    // size `fits` accepts, up to the bound, and removes the other spares
    // unread. A name that is no spare's is left and counted, as is anything
    // that is no regular file. A shard with no spare directory has none.
    template<std::predicate<byte_count> Fits>
    [[nodiscard]] seastar::future<runtime::result<local_spares_found>>
    adopt(Fits fits, codec::cooperative_work& work) {
        assert_current();
        using output = runtime::result<local_spares_found>;
        local_spares_found found;
        const auto at = directory();
        if (!at) co_return output{runtime::failure(at.error())};
        if (
          auto inspected = co_await inspect_local_path(
            files_,
            spec_->root,
            *at,
            runtime::file_kind::directory,
            work,
            true);
          !inspected)
            co_return output{runtime::failure(inspected.error())};
        auto opened = co_await files_.open_directory(*at);
        if (!opened) {
            if (opened.error().code() == errc::not_found) co_return found;
            co_return output{runtime::failure(opened.error())};
        }
        auto names = std::move(*opened);
        runtime::first_failure failed;
        try {
            bool end = false;
            while (!end && !failed.failed()) {
                if (
                  auto ready = co_await detail::path_checkpoint(work); !ready) {
                    failed.observe(ready);
                    break;
                }
                auto page = co_await names.next(
                  {.maximum_entries = item_count{16},
                   .maximum_name_bytes = byte_count{4_KiB}});
                if (!page) {
                    failed.observe(page);
                    break;
                }
                end = page->end();
                for (const auto& entry : page->entries()) {
                    const auto name = parse(entry.name.value());
                    if (!name || entry.kind != runtime::file_kind::regular) {
                        ++found.other;
                        continue;
                    }
                    const auto spare = local_child_path(*at, entry.name);
                    if (!spare) {
                        failed.observe(spare);
                        break;
                    }
                    const auto status = co_await files_.stat(*spare);
                    if (!status) {
                        if (status.error().code() == errc::not_found) continue;
                        failed.observe(status);
                        break;
                    }
                    if (size_ < bound_ && fits(status->size)) {
                        push(*name, status->size);
                        ++found.adopted;
                        continue;
                    }
                    const auto gone = co_await files_.remove_file(*spare);
                    if (!gone && gone.error().code() != errc::not_found) {
                        failed.observe(gone);
                        break;
                    }
                    ++found.removed;
                }
            }
        } catch (...) {
            failed.observe(std::current_exception());
        }
        try {
            failed.observe(co_await names.close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        if (const auto done = detail::settled(failed); !done)
            co_return output{runtime::failure(done.error())};
        // The directory was there: nothing need make it again.
        ready_ = true;
        co_return found;
    }

private:
    struct entry final {
        Identity name;
        byte_count bytes;
    };

    [[nodiscard]] runtime::result<runtime::file_path> directory() const {
        const auto paths = local_paths::make(spec_->root);
        if (!paths) return runtime::failure(paths.error());
        return paths->spares(shard_, kind);
    }
    [[nodiscard]] runtime::result<runtime::file_path>
    path(const Identity& name) const {
        const auto paths = local_paths::make(spec_->root);
        if (!paths) return runtime::failure(paths.error());
        return paths->spare(shard_, name);
    }
    [[nodiscard]] static runtime::result<Identity>
    parse(std::string_view name) {
        if constexpr (kind == local_spare_kind::wal)
            return parse_local_wal_spare(name);
        else
            return parse_local_segment_spare(name);
    }
    [[nodiscard]] static runtime::file_path
    parent_of(const runtime::file_path& path) {
        return runtime::file_path::make(
                 path.value().substr(0, path.value().rfind('/')))
          .value();
    }
    // The spare directory, made once in this process.
    seastar::future<runtime::result<void>>
    prepare(codec::cooperative_work& work) {
        if (ready_) co_return runtime::result<void>{};
        const auto at = directory();
        if (!at) co_return runtime::failure(at.error());
        const auto leaf = runtime::file_name::make(
          at->value().substr(at->value().rfind('/') + 1));
        if (!leaf) co_return runtime::failure(leaf.error());
        if (
          auto made = co_await detail::ensure_local_directory(
            files_, *spec_, parent_of(*at), *leaf, work);
          !made)
            co_return made;
        ready_ = true;
        co_return runtime::result<void>{};
    }
    seastar::future<runtime::result<void>>
    sync(const runtime::file_path& at, codec::cooperative_work& work) {
        if (
          auto inspected = co_await inspect_local_path(
            files_, spec_->root, at, runtime::file_kind::directory, work);
          !inspected)
            co_return inspected;
        auto opened = co_await files_.open_directory(at);
        if (!opened) co_return runtime::failure(opened.error());
        auto held = std::move(*opened);
        runtime::first_failure failed;
        try {
            failed.observe(co_await held.sync());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        try {
            failed.observe(co_await held.close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        co_return detail::settled(failed);
    }
    void push(const Identity& name, byte_count bytes) noexcept {
        entries_[(first_ + size_) % entries_.size()] = entry{name, bytes};
        ++size_;
        bytes_ = bytes_.checked_add(bytes).value_or(bytes_);
    }
    entry pop() noexcept {
        const auto taken = entries_[first_];
        first_ = (first_ + 1) % entries_.size();
        --size_;
        bytes_ = bytes_.checked_sub(taken.bytes).value_or(byte_count{});
        return taken;
    }

    Backend& files_;
    const local_device_spec* spec_;
    std::uint32_t shard_;
    std::uint32_t bound_;
    seastar::semaphore turn_{1};
    // Oldest first, in a ring.
    std::array<entry, maximum_segment_spares> entries_{};
    std::uint32_t first_{0}, size_{0};
    byte_count bytes_{};
    bool ready_{false}, unsettled_{false};
};

} // namespace kwaque::storage
