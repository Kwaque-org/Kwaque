#ifndef KWAQUE_SRC_SIMULATION_FAKE_FILE_TEST_SUPPORT_H_
#define KWAQUE_SRC_SIMULATION_FAKE_FILE_TEST_SUPPORT_H_

#include "src/simulation/fake_file.h"

#include <sys/uio.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <deque>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace kwaque::simulation {

struct fake_inode_snapshot final {
    std::uint64_t id{0};
    fake_file_kind kind{fake_file_kind::regular};
    std::uint32_t open_references{0};
    std::uint32_t pending_references{0};
    std::uint32_t history_references{0};
    std::uint32_t visible_links{0};
    std::uint32_t durable_links{0};
    std::uint64_t visible_size{0};
    std::uint64_t durable_size{0};
    std::uint64_t namespace_synced_sequence{0};
    std::vector<std::byte> visible_bytes;
    std::vector<std::byte> durable_bytes;
    std::deque<std::pair<std::string, std::uint64_t>> visible_entries;
    std::deque<std::pair<std::string, std::uint64_t>> durable_entries;
    std::array<std::uint64_t, runtime::builtin_fault_points.size()>
      occurrences{};

    bool operator==(const fake_inode_snapshot&) const = default;
};

struct fake_namespace_change_snapshot final {
    std::uint64_t parent{0};
    std::string name;
    std::optional<std::uint64_t> before;
    std::optional<std::uint64_t> after;
    bool operator==(const fake_namespace_change_snapshot&) const = default;
};

struct fake_namespace_group_snapshot final {
    std::uint64_t sequence{0};
    std::array<std::optional<fake_namespace_change_snapshot>, 2> changes;
    bool committed{false};
    bool operator==(const fake_namespace_group_snapshot&) const = default;
};

struct fake_file_state_snapshot final {
    std::deque<fake_inode_snapshot> objects;
    std::deque<fake_namespace_group_snapshot> namespace_history;
    std::uint64_t retained_capacity{0};
    std::uint64_t retained_path_bytes{0};
    std::uint64_t generation{0};
    std::uint64_t crash_epoch{0};
    std::uint64_t namespace_groups{0};
    std::uint64_t history_name_bytes{0};
    std::uint64_t next_namespace_sequence{0};
    std::uint64_t next_object_id{0};
    std::uint64_t next_operation_id{0};
    std::uint32_t open_handles{0};
    std::uint32_t pending_operations{0};
    std::uint32_t pending_reads{0};
    std::uint32_t pending_writes{0};
    std::uint64_t pending_bytes{0};
    std::uint64_t pending_path_bytes{0};
    std::uint32_t pending_opens{0};
    fake_file_system_state state{fake_file_system_state::open};
    bool object_ids_exhausted{false};
    bool operation_ids_exhausted{false};
    std::array<std::uint64_t, runtime::builtin_fault_points.size()>
      global_occurrences{};

    bool operator==(const fake_file_state_snapshot&) const = default;
};

struct fake_file_snapshot_limits final {
    std::uint32_t maximum_objects{4'096};
    byte_count maximum_dense_bytes{maximum_contiguous_allocation_bytes};
    std::uint32_t maximum_namespace_groups{4096};
};

using fake_file_state_digest = std::array<std::uint64_t, 4>;

enum class fake_submission_kind : std::uint8_t {
    read,
    write,
    flush,
    truncate,
    close,
};

class fake_native_file_probe final {
public:
    fake_native_file_probe(fake_native_file_probe&&) noexcept = default;
    fake_native_file_probe& operator=(fake_native_file_probe&&) = delete;
    fake_native_file_probe(const fake_native_file_probe&) = delete;
    fake_native_file_probe& operator=(const fake_native_file_probe&) = delete;

    [[nodiscard]] seastar::future<>
    allocate(std::uint64_t position, std::uint64_t length) {
        return file_.allocate(position, length);
    }
    [[nodiscard]] seastar::future<>
    discard(std::uint64_t position, std::uint64_t length) {
        return file_.discard(position, length);
    }
    [[nodiscard]] seastar::future<std::size_t> write_iovec() {
        return file_.dma_write(0, std::vector<iovec>{}, nullptr);
    }
    [[nodiscard]] seastar::future<std::size_t> read_iovec() {
        return file_.dma_read(0, std::vector<iovec>{}, nullptr);
    }
    [[nodiscard]] seastar::future<std::size_t> read_scalar() {
        return file_.dma_read(
          0, scalar_buffer_.get_write(), scalar_buffer_.size(), &intent_);
    }
    void cancel_intent() noexcept { intent_.cancel(); }
    [[nodiscard]] seastar::future<> close() { return file_.close(); }

private:
    friend class fake_file_test_access;

    explicit fake_native_file_probe(seastar::file file)
      : file_(std::move(file))
      , scalar_buffer_(seastar::temporary_buffer<char>::aligned(4'096, 4'096)) {
    }

    seastar::file file_;
    seastar::io_intent intent_;
    seastar::temporary_buffer<char> scalar_buffer_;
};

class fake_file_test_access final {
public:
    // Inspect the existing dispatch snapshots before the scheduler completes
    // I/O. No payload copy, completion driver or replacement file owner is
    // introduced.
    [[nodiscard]] static runtime::result<std::uint32_t>
    verify_pending_write_buffers(fake_file_system& filesystem) {
        filesystem.assert_current();
        if (filesystem.state_ != fake_file_system_state::open)
            return runtime::failure(
              runtime::operation_error{
                errc::closed, runtime::operation_kind::file});
        filesystem.pending_.copy_keys(filesystem.pending_ids_);
        std::uint64_t bytes = 0;
        std::uint32_t writes = 0;
        for (const auto id : filesystem.pending_ids_) {
            const auto& operation = filesystem.pending_.at(id);
            if (operation.kind != fake_file_system::pending_kind::write)
                continue;
            const auto& io = std::get<fake_file_system::native_io_operation>(
              operation.payload);
            if (
              io.snapshot.size() > maximum_contiguous_allocation_bytes
              || io.snapshot.size() > runtime::maximum_file_write_concurrency
                                          * maximum_contiguous_allocation_bytes
                                        - bytes)
                return runtime::failure(
                  runtime::operation_error{
                    errc::resource_exhausted, runtime::operation_kind::file});
            bytes += io.snapshot.size();
            if (
              !io.source
              || std::mismatch(
                   io.source, io.source + io.snapshot.size(), io.snapshot.get())
                     .first
                   != io.source + io.snapshot.size())
                return runtime::failure(
                  runtime::operation_error{
                    errc::wrong_context, runtime::operation_kind::file});
            ++writes;
        }
        return writes;
    }
    [[nodiscard]] static std::uint64_t
    submitted(const fake_file_system& filesystem, fake_submission_kind kind) {
        filesystem.assert_current();
        auto pending = fake_file_system::pending_kind::count;
        switch (kind) {
        case fake_submission_kind::read:
            pending = fake_file_system::pending_kind::read;
            break;
        case fake_submission_kind::write:
            pending = fake_file_system::pending_kind::write;
            break;
        case fake_submission_kind::flush:
            pending = fake_file_system::pending_kind::flush;
            break;
        case fake_submission_kind::truncate:
            pending = fake_file_system::pending_kind::truncate;
            break;
        case fake_submission_kind::close:
            pending = fake_file_system::pending_kind::close;
            break;
        }
        const auto index = static_cast<std::size_t>(pending);
        return pending == fake_file_system::pending_kind::read
                 ? filesystem.submitted_by_kind_[index]
                     + filesystem.submitted_by_kind_[static_cast<std::size_t>(
                       fake_file_system::pending_kind::bulk_read)]
                 : filesystem.submitted_by_kind_[index];
    }

    [[nodiscard]] static seastar::future<> wait_submitted(
      fake_file_system& filesystem,
      fake_submission_kind kind,
      std::uint64_t count) {
        auto pending = fake_file_system::pending_kind::count;
        switch (kind) {
        case fake_submission_kind::read:
            pending = fake_file_system::pending_kind::read;
            break;
        case fake_submission_kind::write:
            pending = fake_file_system::pending_kind::write;
            break;
        case fake_submission_kind::flush:
            pending = fake_file_system::pending_kind::flush;
            break;
        case fake_submission_kind::truncate:
            pending = fake_file_system::pending_kind::truncate;
            break;
        case fake_submission_kind::close:
            pending = fake_file_system::pending_kind::close;
            break;
        }
        return filesystem.wait_submitted(pending, count);
    }

    [[nodiscard]] static seastar::future<> wait_parked(
      fake_file_system& filesystem,
      fake_submission_kind kind,
      std::uint32_t count) {
        auto pending = fake_file_system::pending_kind::count;
        switch (kind) {
        case fake_submission_kind::read:
            pending = fake_file_system::pending_kind::read;
            break;
        case fake_submission_kind::write:
            pending = fake_file_system::pending_kind::write;
            break;
        case fake_submission_kind::flush:
            pending = fake_file_system::pending_kind::flush;
            break;
        case fake_submission_kind::truncate:
            pending = fake_file_system::pending_kind::truncate;
            break;
        case fake_submission_kind::close:
            pending = fake_file_system::pending_kind::close;
            break;
        }
        return filesystem.wait_parked(pending, count);
    }

    [[nodiscard]] static runtime::result<fake_file_state_digest> state_digest(
      const fake_file_system& filesystem,
      fake_file_snapshot_limits limits = {}) {
        filesystem.assert_current();
        if (
          limits.maximum_objects == 0
          || filesystem.objects_.size() > limits.maximum_objects
          || filesystem.namespace_history_.size()
               > limits.maximum_namespace_groups
          || limits.maximum_dense_bytes.value() == 0
          || limits.maximum_dense_bytes.value()
               > maximum_contiguous_allocation_bytes) {
            return runtime::failure(
              runtime::operation_error{
                errc::resource_exhausted, runtime::operation_kind::file});
        }
        std::uint64_t page_bytes = 0;
        for (const auto& [id, object] : filesystem.objects_) {
            static_cast<void>(id);
            if (object->kind != fake_file_kind::regular) {
                continue;
            }
            const auto& file = std::get<fake_file_system::regular_file_state>(
              object->state);
            const auto pages = file.visible_pages.size()
                               + file.durable_pages.size();
            if (
              pages > (limits.maximum_dense_bytes.value() - page_bytes)
                        / fake_file_page_bytes) {
                return runtime::failure(
                  runtime::operation_error{
                    errc::resource_exhausted, runtime::operation_kind::file});
            }
            page_bytes += pages * fake_file_page_bytes;
        }

        fake_file_state_digest result{
          UINT64_C(0xcbf29ce484222325),
          UINT64_C(0x9e3779b97f4a7c15),
          UINT64_C(0x6a09e667f3bcc909),
          UINT64_C(0xbb67ae8584caa73b),
        };
        mix(result, filesystem.retained_capacity_.value());
        mix(result, filesystem.retained_path_bytes_);
        mix(result, filesystem.generation_);
        if (filesystem.config_.crash_policy) {
            mix(result, filesystem.crash_epoch_);
            mix(result, filesystem.namespace_history_.size());
            mix(result, filesystem.history_name_bytes_);
            mix(result, filesystem.next_namespace_sequence_);
            for (const auto& [sequence, group] :
                 filesystem.namespace_history_) {
                mix(result, sequence);
                mix(result, group.committed);
                for (const auto& change : group.changes) {
                    mix(result, change.has_value());
                    if (!change) continue;
                    mix(result, change->parent.value());
                    append_name(result, change->name);
                    mix(result, change->before ? change->before->value() : 0U);
                    mix(result, change->after ? change->after->value() : 0U);
                }
            }
        }
        mix(result, filesystem.next_object_id_);
        mix(result, filesystem.next_operation_id_);
        mix(result, filesystem.open_handles_);
        mix(result, filesystem.pending_operations_);
        mix(result, filesystem.pending_reads_);
        mix(result, filesystem.pending_writes_);
        mix(result, filesystem.pending_bytes_.value());
        mix(result, filesystem.pending_path_bytes_);
        mix(result, filesystem.pending_opens_);
        mix(result, static_cast<std::uint8_t>(filesystem.state_));
        mix(result, filesystem.object_ids_exhausted_);
        mix(result, filesystem.operation_ids_exhausted_);
        mix(result, filesystem.dirty_head_);
        for (const auto occurrence : filesystem.global_occurrences_) {
            mix(result, occurrence);
        }
        for (const auto submitted : filesystem.submitted_by_kind_) {
            mix(result, submitted);
        }
        seastar::chunked_vector<std::uint64_t> open_ids;
        open_ids.reserve(filesystem.open_objects_.size());
        for (const auto id : filesystem.open_objects_) {
            open_ids.push_back(id);
        }
        std::ranges::sort(open_ids);
        mix(result, open_ids.size());
        for (const auto id : open_ids) {
            mix(result, id);
        }

        seastar::chunked_vector<std::uint64_t> object_ids;
        object_ids.reserve(filesystem.objects_.size());
        for (const auto& [id, object] : filesystem.objects_) {
            static_cast<void>(object);
            object_ids.push_back(id);
        }
        std::ranges::sort(object_ids);
        for (const auto id : object_ids) {
            const auto& object = *filesystem.objects_.find(id)->second;
            mix(result, id);
            mix(result, static_cast<std::uint8_t>(object.kind));
            mix(result, object.open_references);
            mix(result, object.pending_references);
            if (filesystem.config_.crash_policy)
                mix(result, object.history_references);
            mix(result, object.visible_links);
            mix(result, object.durable_links);
            mix(result, object.crash_dirty);
            mix(result, object.dirty_previous);
            mix(result, object.dirty_next);
            for (const auto occurrence : object.occurrences) {
                mix(result, occurrence);
            }
            if (object.kind == fake_file_kind::regular) {
                const auto& file
                  = std::get<fake_file_system::regular_file_state>(
                    object.state);
                mix(result, file.visible_size);
                mix(result, file.durable_size);
                mix(
                  result,
                  file.cleared_from_page.value_or(
                    std::numeric_limits<std::uint64_t>::max()));
                append_pages(result, file.visible_pages, 1);
                append_pages(result, file.durable_pages, 2);
            } else {
                const auto& directory
                  = std::get<fake_file_system::directory_state>(object.state);
                if (filesystem.config_.crash_policy)
                    mix(result, directory.synced_sequence);
                append_directory(result, directory.durable, 3);
                mix(result, 4);
                for (const auto& [name, child] : directory.unsynced) {
                    append_name(result, name);
                    mix(result, child ? child->value() : 0);
                }
            }
        }
        return result;
    }

    [[nodiscard]] static runtime::result<fake_file_state_snapshot> snapshot(
      const fake_file_system& filesystem,
      fake_file_snapshot_limits limits = {}) {
        filesystem.assert_current();
        if (
          limits.maximum_objects == 0
          || filesystem.objects_.size() > limits.maximum_objects
          || filesystem.namespace_history_.size()
               > limits.maximum_namespace_groups
          || limits.maximum_dense_bytes.value() == 0
          || limits.maximum_dense_bytes.value()
               > maximum_contiguous_allocation_bytes) {
            return runtime::failure(
              runtime::operation_error{
                errc::resource_exhausted, runtime::operation_kind::file});
        }
        fake_file_state_snapshot result{
          .retained_capacity = filesystem.retained_capacity_.value(),
          .retained_path_bytes = filesystem.retained_path_bytes_,
          .generation = filesystem.generation_,
          .crash_epoch = filesystem.crash_epoch_,
          .namespace_groups = filesystem.namespace_history_.size(),
          .history_name_bytes = filesystem.history_name_bytes_,
          .next_namespace_sequence = filesystem.next_namespace_sequence_,
          .next_object_id = filesystem.next_object_id_,
          .next_operation_id = filesystem.next_operation_id_,
          .open_handles = filesystem.open_handles_,
          .pending_operations = filesystem.pending_operations_,
          .pending_reads = filesystem.pending_reads_,
          .pending_writes = filesystem.pending_writes_,
          .pending_bytes = filesystem.pending_bytes_.value(),
          .pending_path_bytes = filesystem.pending_path_bytes_,
          .pending_opens = filesystem.pending_opens_,
          .state = filesystem.state_,
          .object_ids_exhausted = filesystem.object_ids_exhausted_,
          .operation_ids_exhausted = filesystem.operation_ids_exhausted_,
          .global_occurrences = filesystem.global_occurrences_,
        };
        for (const auto& [sequence, group] : filesystem.namespace_history_) {
            fake_namespace_group_snapshot copy{
              .sequence = sequence, .committed = group.committed};
            for (std::size_t index = 0; index < group.changes.size(); ++index) {
                const auto& change = group.changes[index];
                if (!change) continue;
                copy.changes[index] = fake_namespace_change_snapshot{
                  change->parent.value(),
                  change->name,
                  change->before
                    ? std::optional<std::uint64_t>{change->before->value()}
                    : std::nullopt,
                  change->after
                    ? std::optional<std::uint64_t>{change->after->value()}
                    : std::nullopt};
            }
            result.namespace_history.push_back(std::move(copy));
        }
        std::uint64_t dense_bytes = 0;
        for (const auto& [id, object] : filesystem.objects_) {
            fake_inode_snapshot copy{
              .id = id,
              .kind = object->kind,
              .open_references = object->open_references,
              .pending_references = object->pending_references,
              .history_references = object->history_references,
              .visible_links = object->visible_links,
              .durable_links = object->durable_links,
              .occurrences = object->occurrences,
            };
            if (object->kind == fake_file_kind::regular) {
                const auto& file
                  = std::get<fake_file_system::regular_file_state>(
                    object->state);
                copy.visible_size = file.visible_size;
                copy.durable_size = file.durable_size;
                if (
                  file.visible_size
                    > limits.maximum_dense_bytes.value() - dense_bytes
                  || file.durable_size > limits.maximum_dense_bytes.value()
                                           - dense_bytes - file.visible_size) {
                    return runtime::failure(
                      runtime::operation_error{
                        errc::resource_exhausted,
                        runtime::operation_kind::file});
                }
                dense_bytes += file.visible_size + file.durable_size;
                copy.visible_bytes.resize(file.visible_size);
                copy.durable_bytes.resize(file.durable_size);
                const auto visible = filesystem.read(
                  object->id, 0, std::span<std::byte>{copy.visible_bytes});
                if (!visible) {
                    return runtime::failure(visible.error());
                }
                copy_pages(file.durable_pages, copy.durable_bytes);
            } else {
                const auto& directory
                  = std::get<fake_file_system::directory_state>(object->state);
                copy.namespace_synced_sequence = directory.synced_sequence;
                for (const auto& [name, child] : directory.durable) {
                    copy.durable_entries.emplace_back(name, child.value());
                }
                append_visible_entries(directory, copy.visible_entries);
            }
            result.objects.push_back(std::move(copy));
        }
        std::ranges::sort(result.objects, {}, &fake_inode_snapshot::id);
        return result;
    }

    [[nodiscard]] static std::uint64_t
    retained_path_bytes(const fake_file_system& filesystem) noexcept {
        filesystem.assert_current();
        return filesystem.retained_path_bytes_;
    }

    [[nodiscard]] static std::uint32_t
    open_handles(const fake_file_system& filesystem) noexcept {
        filesystem.assert_current();
        return filesystem.open_handles_;
    }

    [[nodiscard]] static runtime::result<canonical_fake_path>
    resolve(fake_file_system& filesystem, std::string_view path) {
        return filesystem.resolve(path);
    }

    [[nodiscard]] static runtime::result<fake_object_id>
    create_file(fake_file_system& filesystem, const canonical_fake_path& path) {
        return filesystem.create(path, fake_file_kind::regular);
    }
    [[nodiscard]] static runtime::result<runtime::file> open_at_completion(
      fake_file_system& filesystem,
      const canonical_fake_path& path,
      runtime::file_open_options options) {
        fake_file_system::metadata_operation metadata;
        metadata.path = path;
        metadata.open_options = options;
        bool open_slot = false;
        auto opened = filesystem.apply_open(metadata, open_slot);
        if (!opened) return runtime::failure(opened.error());
        return std::move(*opened).publish();
    }
    [[nodiscard]] static runtime::result<fake_object_id> create_directory(
      fake_file_system& filesystem, const canonical_fake_path& path) {
        return filesystem.create(path, fake_file_kind::directory);
    }
    [[nodiscard]] static runtime::result<void>
    remove_file(fake_file_system& filesystem, const canonical_fake_path& path) {
        return filesystem.remove(path, fake_file_kind::regular);
    }
    [[nodiscard]] static runtime::result<void> remove_directory(
      fake_file_system& filesystem, const canonical_fake_path& path) {
        return filesystem.remove(path, fake_file_kind::directory);
    }
    [[nodiscard]] static runtime::result<void> rename(
      fake_file_system& filesystem,
      const canonical_fake_path& from,
      const canonical_fake_path& to,
      runtime::file_rename_policy policy
      = runtime::file_rename_policy::replace) {
        return filesystem.rename(from, to, policy);
    }
    [[nodiscard]] static runtime::result<void> sync_directory(
      fake_file_system& filesystem, const canonical_fake_path& path) {
        return filesystem.sync_directory(path);
    }
    [[nodiscard]] static runtime::result<
      seastar::chunked_vector<fake_directory_entry>>
    list(fake_file_system& filesystem, const canonical_fake_path& path) {
        return filesystem.list(path);
    }
    [[nodiscard]] static runtime::result<fake_object_id>
    lookup(fake_file_system& filesystem, const canonical_fake_path& path) {
        return filesystem.lookup(path);
    }
    [[nodiscard]] static runtime::result<fake_native_file_probe>
    make_native_file_probe(
      fake_file_system& filesystem,
      const canonical_fake_path& path,
      runtime::file_access access) {
        filesystem.assert_current();
        auto id = filesystem.lookup(path);
        if (!id) {
            return runtime::failure(id.error());
        }
        auto native = filesystem.make_native_file_for_test(*id, access);
        if (!native) {
            return runtime::failure(native.error());
        }
        return fake_native_file_probe{std::move(*native)};
    }

    [[nodiscard]] static runtime::result<byte_count> write(
      fake_file_system& filesystem,
      const canonical_fake_path& path,
      std::uint64_t position,
      std::span<const std::byte> bytes) {
        return filesystem.write(path, position, bytes);
    }
    [[nodiscard]] static runtime::result<byte_count> read(
      const fake_file_system& filesystem,
      const canonical_fake_path& path,
      std::uint64_t position,
      std::span<std::byte> destination) noexcept {
        return filesystem.read(path, position, destination);
    }
    [[nodiscard]] static runtime::result<std::size_t> volatile_page_count(
      const fake_file_system& filesystem, const canonical_fake_path& path) {
        auto file = filesystem.regular_file(path);
        if (!file) return runtime::failure(file.error());
        return (*file)->visible_pages.size();
    }

    [[nodiscard]] static runtime::result<fake_file_system::prepared_truncate>
    prepare_truncate(
      fake_file_system& filesystem,
      const canonical_fake_path& path,
      std::uint64_t size) {
        auto id = filesystem.lookup(path);
        if (!id) return runtime::failure(id.error());
        return filesystem.prepare_truncate(*id, size);
    }

    static void commit_truncate(
      fake_file_system& filesystem,
      fake_file_system::prepared_truncate prepared) noexcept {
        filesystem.commit_truncate(std::move(prepared));
    }

    [[nodiscard]] static runtime::result<void> truncate(
      fake_file_system& filesystem,
      const canonical_fake_path& path,
      std::uint64_t size) {
        return filesystem.truncate(path, size);
    }
    [[nodiscard]] static runtime::result<void>
    flush(fake_file_system& filesystem, const canonical_fake_path& path) {
        return filesystem.flush(path);
    }
    static void crash(fake_file_system& filesystem) {
        filesystem.restore_durable_state();
    }

    [[nodiscard]] static runtime::result<void>
    retain(fake_file_system& filesystem, fake_object_id id) noexcept {
        return filesystem.retain_open_reference(id);
    }
    static void release(fake_file_system& filesystem, fake_object_id id) {
        filesystem.release_open_reference(id);
    }
    [[nodiscard]] static runtime::result<fake_operation_id>
    issue_operation_id(fake_file_system& filesystem) noexcept {
        return filesystem.issue_operation_id();
    }

    static void set_next_object_id(
      fake_file_system& filesystem, std::uint64_t value) noexcept {
        filesystem.next_object_id_ = value;
        filesystem.object_ids_exhausted_ = false;
    }
    static void set_next_operation_id(
      fake_file_system& filesystem, std::uint64_t value) noexcept {
        filesystem.next_operation_id_ = value;
        filesystem.operation_ids_exhausted_ = false;
    }
    static void
    reserve_object_slots(fake_file_system& filesystem, std::size_t slots) {
        filesystem.objects_.reserve(slots);
    }

    [[nodiscard]] static runtime::result<std::uint64_t> visible_size(
      const fake_file_system& filesystem,
      const canonical_fake_path& path) noexcept {
        auto file = filesystem.regular_file(path);
        if (!file) {
            return runtime::failure(file.error());
        }
        return (*file)->visible_size;
    }
    [[nodiscard]] static runtime::result<std::uint64_t> durable_size(
      const fake_file_system& filesystem,
      const canonical_fake_path& path) noexcept {
        auto file = filesystem.regular_file(path);
        if (!file) {
            return runtime::failure(file.error());
        }
        return (*file)->durable_size;
    }
    [[nodiscard]] static runtime::result<const void*> visible_page(
      const fake_file_system& filesystem,
      const canonical_fake_path& path,
      std::uint64_t page_index) noexcept {
        auto file = filesystem.regular_file(path);
        if (!file) {
            return runtime::failure(file.error());
        }
        const auto found = (*file)->visible_pages.find(page_index);
        if (found != (*file)->visible_pages.end()) {
            return static_cast<const void*>(found->second.bytes.get());
        }
        if (
          page_index >= (*file)->cleared_from_page.value_or(
            std::numeric_limits<std::uint64_t>::max())) {
            return static_cast<const void*>(nullptr);
        }
        const auto durable = (*file)->durable_pages.find(page_index);
        return durable == (*file)->durable_pages.end()
                 ? static_cast<const void*>(nullptr)
                 : static_cast<const void*>(durable->second.bytes.get());
    }
    [[nodiscard]] static runtime::result<const void*> durable_page(
      const fake_file_system& filesystem,
      const canonical_fake_path& path,
      std::uint64_t page_index) noexcept {
        auto file = filesystem.regular_file(path);
        if (!file) {
            return runtime::failure(file.error());
        }
        const auto found = (*file)->durable_pages.find(page_index);
        return found == (*file)->durable_pages.end()
                 ? static_cast<const void*>(nullptr)
                 : static_cast<const void*>(found->second.bytes.get());
    }
    [[nodiscard]] static runtime::result<std::size_t> visible_page_count(
      const fake_file_system& filesystem,
      const canonical_fake_path& path) noexcept {
        auto file = filesystem.regular_file(path);
        if (!file) {
            return runtime::failure(file.error());
        }
        std::size_t count = (*file)->visible_pages.size();
        const auto maximum_page = ((*file)->visible_size / fake_file_page_bytes)
                                  + static_cast<std::uint64_t>(
                                    (*file)->visible_size % fake_file_page_bytes
                                    != 0);
        for (const auto& [index, state] : (*file)->durable_pages) {
            static_cast<void>(state);
            if (
              index < maximum_page
              && index < (*file)->cleared_from_page.value_or(
                   std::numeric_limits<std::uint64_t>::max())
              && !(*file)->visible_pages.contains(index)) {
                ++count;
            }
        }
        return count;
    }

private:
    static void
    mix(fake_file_state_digest& state, std::uint64_t value) noexcept {
        for (std::size_t lane = 0; lane < state.size(); ++lane) {
            state[lane] ^= value + lane * UINT64_C(0x100000001b3);
            state[lane] *= UINT64_C(0x100000001b3);
            state[lane] = (state[lane] << 13U) | (state[lane] >> 51U);
        }
    }

    static void
    append_name(fake_file_state_digest& state, std::string_view name) noexcept {
        mix(state, name.size());
        for (const auto value : name) {
            mix(state, static_cast<unsigned char>(value));
        }
    }

    static void append_directory(
      fake_file_state_digest& state,
      const fake_file_system::directory_map& directory,
      std::uint64_t tag) noexcept {
        mix(state, tag);
        mix(state, directory.size());
        for (const auto& [name, child] : directory) {
            append_name(state, name);
            mix(state, child.value());
        }
    }

    template<typename Pages>
    static void append_pages(
      fake_file_state_digest& state, const Pages& pages, std::uint64_t tag) {
        mix(state, tag);
        mix(state, pages.size());
        seastar::chunked_vector<std::uint64_t> indices;
        indices.reserve(pages.size());
        for (const auto& [index, page] : pages) {
            static_cast<void>(page);
            indices.push_back(index);
        }
        std::ranges::sort(indices);
        for (const auto index : indices) {
            mix(state, index);
            const auto& page = *pages.find(index)->second.bytes;
            for (const auto value : page) {
                mix(state, std::to_integer<std::uint8_t>(value));
            }
        }
    }

    static void append_visible_entries(
      const fake_file_system::directory_state& directory,
      std::deque<std::pair<std::string, std::uint64_t>>& destination) {
        auto durable = directory.durable.begin();
        auto changed = directory.unsynced.begin();
        while (durable != directory.durable.end()
               || changed != directory.unsynced.end()) {
            const bool take_durable
              = changed == directory.unsynced.end()
                || (durable != directory.durable.end()
                    && fake_file_system::unsigned_name_less{}(
                      durable->first, changed->first));
            const bool take_changed
              = durable == directory.durable.end()
                || (changed != directory.unsynced.end()
                    && fake_file_system::unsigned_name_less{}(
                      changed->first, durable->first));
            std::string_view name;
            std::optional<fake_object_id> child;
            if (take_durable) {
                name = durable->first;
                child = durable->second;
                ++durable;
            } else if (take_changed) {
                name = changed->first;
                child = changed->second;
                ++changed;
            } else {
                name = changed->first;
                child = changed->second;
                ++durable;
                ++changed;
            }
            if (child) {
                destination.emplace_back(name, child->value());
            }
        }
    }

    template<typename Pages>
    static void
    copy_pages(const Pages& pages, std::vector<std::byte>& destination) {
        for (const auto& [index, state] : pages) {
            const auto offset = index * fake_file_page_bytes;
            if (offset >= destination.size()) {
                continue;
            }
            const auto count = std::min<std::size_t>(
              fake_file_page_bytes, destination.size() - offset);
            std::memcpy(
              destination.data() + offset, state.bytes->data(), count);
        }
    }
};

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_FAKE_FILE_TEST_SUPPORT_H_
