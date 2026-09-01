#ifndef KWAQUE_SRC_SIMULATION_FAKE_FILE_H_
#define KWAQUE_SRC_SIMULATION_FAKE_FILE_H_

#include "src/base/units.h"
#include "src/runtime/error.h"
#include "src/runtime/file.h"
#include "src/runtime/shard_affinity.h"
#include "src/simulation/fault_schedule.h"
#include "src/simulation/scheduler.h"

#include <seastar/core/chunked_hash_map.hh>
#include <seastar/core/chunked_vector.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/internal/io_intent.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/stream.hh>
#include <seastar/core/temporary_buffer.hh>

#include <absl/container/btree_map.h>

#include <algorithm>
#include <array>
#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace kwaque::simulation {

inline constexpr std::size_t fake_file_page_bytes{4'096};
inline constexpr std::size_t fake_path_component_bytes_max{255};
inline constexpr std::size_t fake_path_bytes_max{4'096};
inline constexpr byte_count default_fake_disk_capacity{
  std::uint64_t{256} * 1024U * 1024U};
inline constexpr byte_count maximum_fake_disk_capacity{
  std::uint64_t{4} * 1024U * 1024U * 1024U};
inline constexpr std::uint32_t default_fake_file_objects{65'536};
inline constexpr std::uint32_t maximum_fake_file_objects{1'048'576};
inline constexpr std::uint32_t maximum_fake_open_handles{4'096};
inline constexpr std::uint32_t maximum_fake_pending_operations{1'024};
inline constexpr byte_count maximum_fake_pending_bytes{
  std::uint64_t{1} * 1024U * 1024U * 1024U};
inline constexpr byte_count maximum_fake_retained_path_bytes{
  std::uint64_t{256} * 1024U * 1024U};

class fake_file_test_access;

class fake_object_id final {
public:
    [[nodiscard]] constexpr std::uint64_t value() const noexcept {
        return value_;
    }

    auto operator<=>(const fake_object_id&) const = default;

private:
    friend class fake_file_system;
    friend class fake_file_test_access;

    constexpr explicit fake_object_id(std::uint64_t value) noexcept
      : value_(value) {}

    std::uint64_t value_;
};

class fake_operation_id final {
public:
    [[nodiscard]] constexpr std::uint64_t value() const noexcept {
        return value_;
    }

    auto operator<=>(const fake_operation_id&) const = default;

private:
    friend class fake_file_system;
    friend class fake_file_test_access;

    constexpr explicit fake_operation_id(std::uint64_t value) noexcept
      : value_(value) {}

    std::uint64_t value_;
};

enum class fake_file_kind : std::uint8_t {
    regular,
    directory,
};

class canonical_fake_path final {
public:
    [[nodiscard]] const std::string& bytes() const noexcept { return bytes_; }
    [[nodiscard]] const std::vector<std::string>& components() const noexcept {
        return components_;
    }

    bool operator==(const canonical_fake_path&) const = default;

private:
    friend class fake_file_system;

    canonical_fake_path(
      std::string bytes, std::vector<std::string> components) noexcept
      : bytes_(std::move(bytes))
      , components_(std::move(components)) {}

    std::string bytes_;
    std::vector<std::string> components_;
};

struct fake_directory_entry final {
    std::string name;
    fake_object_id id;
    fake_file_kind kind;

    bool operator==(const fake_directory_entry&) const = default;
};

struct fake_file_system_config final {
    std::string virtual_root{"/kwaque"};
    byte_count logical_capacity{default_fake_disk_capacity};
    std::uint32_t maximum_objects{default_fake_file_objects};
    byte_count maximum_operation_bytes{runtime::maximum_file_io_bytes};
    byte_count maximum_retained_path_bytes{std::uint64_t{16} * 1024U * 1024U};
    std::uint32_t maximum_open_handles{256};
    std::uint32_t maximum_pending_operations{96};
    byte_count maximum_pending_bytes{std::uint64_t{128} * 1024U * 1024U};
    runtime::monotonic_duration base_latency{1};
    runtime::monotonic_duration read_latency_min{};
    runtime::monotonic_duration read_latency_mean{};
    runtime::monotonic_duration write_latency_min{};
    runtime::monotonic_duration write_latency_mean{};
    std::uint32_t maximum_pending_reads{64};
    std::uint32_t maximum_pending_writes{64};
    std::uint32_t memory_dma_alignment{4'096};
    std::uint32_t disk_read_dma_alignment{4'096};
    std::uint32_t disk_write_dma_alignment{4'096};
    std::uint32_t disk_overwrite_dma_alignment{4'096};
    std::uint32_t native_max_length{
      static_cast<std::uint32_t>(maximum_contiguous_allocation_bytes)};
};

enum class fake_file_system_state : std::uint8_t {
    open,
    crashing,
    stopping,
    stopped,
};

class fake_file_system final : public runtime::shard_affine {
public:
    [[nodiscard]] static runtime::result<std::unique_ptr<fake_file_system>>
    make(fake_file_system_config config);
    [[nodiscard]] static runtime::result<std::unique_ptr<fake_file_system>>
    make(
      fake_file_system_config config,
      scheduler& event_scheduler,
      fault_schedule& faults);

    fake_file_system(const fake_file_system&) = delete;
    fake_file_system& operator=(const fake_file_system&) = delete;
    fake_file_system(fake_file_system&&) = delete;
    fake_file_system& operator=(fake_file_system&&) = delete;
    ~fake_file_system();

    [[nodiscard]] const canonical_fake_path& root() const {
        assert_current();
        return root_;
    }
    [[nodiscard]] byte_count retained_capacity() const {
        assert_current();
        return retained_capacity_;
    }
    [[nodiscard]] std::size_t object_count() const {
        assert_current();
        return objects_.size();
    }
    [[nodiscard]] seastar::future<runtime::result<runtime::file>>
    open(runtime::file_path path, runtime::file_open_options options);
    [[nodiscard]] seastar::future<runtime::result<bool>>
    exists(runtime::file_path path);
    [[nodiscard]] seastar::future<runtime::result<runtime::file_status>>
    stat(runtime::file_path path);
    [[nodiscard]] seastar::future<runtime::result<runtime::directory_listing>>
    list(runtime::file_path path, runtime::directory_listing_limits limits);
    [[nodiscard]] seastar::future<runtime::result<void>>
    create_directories(runtime::file_path path);
    [[nodiscard]] seastar::future<runtime::result<void>>
    remove_file(runtime::file_path path);
    [[nodiscard]] seastar::future<runtime::result<void>>
    remove_directory(runtime::file_path path);
    [[nodiscard]] seastar::future<runtime::result<void>>
    rename(runtime::file_path source, runtime::file_path destination);
    [[nodiscard]] seastar::future<runtime::result<void>>
    sync_directory(runtime::file_path path);
    [[nodiscard]] seastar::future<runtime::result<void>> crash();
    [[nodiscard]] seastar::future<runtime::result<void>> stop();
    [[nodiscard]] fake_file_system_state state() const {
        assert_current();
        return state_;
    }
    [[nodiscard]] std::uint32_t pending_operations() const {
        assert_current();
        return pending_operations_;
    }
    [[nodiscard]] byte_count pending_bytes() const {
        assert_current();
        return pending_bytes_;
    }
    [[nodiscard]] std::uint32_t pending_reads() const {
        assert_current();
        return pending_reads_;
    }
    [[nodiscard]] std::uint32_t pending_writes() const {
        assert_current();
        return pending_writes_;
    }

private:
    friend class fake_file_test_access;
    class native_file_impl;

    struct unsigned_name_less {
        using is_transparent = void;

        [[nodiscard]] bool operator()(
          std::string_view left, std::string_view right) const noexcept;
    };

    using directory_map
      = absl::btree_map<std::string, fake_object_id, unsigned_name_less>;
    using directory_delta = absl::
      btree_map<std::string, std::optional<fake_object_id>, unsigned_name_less>;

    struct directory_state final {
        directory_map durable;
        directory_delta unsynced;
    };

    using page = std::array<std::byte, fake_file_page_bytes>;
    using page_pointer = seastar::lw_shared_ptr<const page>;

    struct page_state final {
        page_pointer bytes;
        bool dirty{false};
    };

    using page_map = seastar::chunked_hash_map<std::uint64_t, page_state>;

    struct regular_file_state final {
        // Visible pages contains only volatile overrides. Reads fall through to
        // durable pages when an override is absent.
        page_map visible_pages;
        page_map durable_pages;
        seastar::chunked_vector<std::uint64_t> dirty_pages;
        std::size_t dirty_page_count{0};
        std::optional<std::uint64_t> cleared_from_page;
        std::uint64_t visible_size{0};
        std::uint64_t durable_size{0};
    };

    struct prepared_truncate final {
        fake_object_id object;
        std::uint64_t size{0};
        std::uint64_t previous_size{0};
        std::uint64_t retained_before{0};
        std::uint64_t retained_after{0};
        std::uint64_t kept_pages{0};
        std::optional<page_pointer> tail;
        bool insert_tail{false};
    };

    struct inode final : runtime::shard_affine {
        inode(
          fake_object_id object_id,
          fake_file_kind object_kind,
          std::variant<regular_file_state, directory_state> object_state,
          std::uint32_t object_visible_links = 0,
          std::uint32_t object_durable_links = 0) noexcept
          : id(object_id)
          , kind(object_kind)
          , state(std::move(object_state))
          , visible_links(object_visible_links)
          , durable_links(object_durable_links) {}

        inode(const inode&) = delete;
        inode& operator=(const inode&) = delete;
        inode(inode&&) = delete;
        inode& operator=(inode&&) = delete;

        fake_object_id id;
        fake_file_kind kind;
        std::variant<regular_file_state, directory_state> state;
        std::uint32_t open_references{0};
        std::uint32_t pending_references{0};
        std::uint32_t visible_links{0};
        std::uint32_t durable_links{0};
        std::array<std::uint64_t, runtime::builtin_fault_points.size()>
          occurrences{};
        bool crash_dirty{false};
        std::uint64_t dirty_previous{0};
        std::uint64_t dirty_next{0};
    };

    using inode_map
      = seastar::chunked_hash_map<std::uint64_t, std::unique_ptr<inode>>;

    enum class pending_kind : std::uint8_t {
        open,
        exists,
        stat,
        list,
        create_directories,
        remove_file,
        remove_directory,
        rename,
        sync_directory,
        read,
        write,
        bulk_read,
        flush,
        truncate,
        size,
        close,
        crash_control,
        count,
    };

    enum class pending_phase : std::uint8_t {
        queued,
        parked,
        terminal_scheduled,
        crash_apply_scheduled,
        partial_resize_apply_scheduled,
        discard_pending,
    };

    enum class handle_lifecycle : std::uint8_t {
        open,
        closing,
        closed,
    };

    struct open_handle_state final : runtime::shard_affine {
        handle_lifecycle lifecycle{handle_lifecycle::open};
        bool reference_owned{false};
    };

    using pending_value = std::variant<
      std::monostate,
      bool,
      runtime::file,
      runtime::file_status,
      runtime::directory_listing,
      byte_count,
      std::uint64_t,
      seastar::temporary_buffer<std::uint8_t>>;

    struct metadata_operation final {
        std::optional<canonical_fake_path> path;
        std::optional<canonical_fake_path> destination_path;
        runtime::file_open_options open_options{};
        runtime::directory_listing_limits listing_limits{};
    };

    struct native_io_operation final {
        std::optional<seastar::internal::intent_reference> intent;
        seastar::lw_shared_ptr<open_handle_state> handle;
        seastar::temporary_buffer<char> snapshot;
        const char* source{nullptr};
        char* destination{nullptr};
        std::uint64_t position{0};
        std::uint64_t requested_bytes{0};
        std::uint64_t generation{0};
    };

    using operation_payload
      = std::variant<std::monostate, metadata_operation, native_io_operation>;

    struct prepared_operation {
        prepared_operation(
          fake_operation_id operation_id,
          pending_kind operation_kind,
          runtime::builtin_fault_point fault_point) noexcept
          : id(operation_id)
          , kind(operation_kind)
          , point(fault_point) {}

        prepared_operation(const prepared_operation&) = delete;
        prepared_operation& operator=(const prepared_operation&) = delete;
        prepared_operation(prepared_operation&&) noexcept = default;
        prepared_operation& operator=(prepared_operation&&) noexcept = default;

        fake_operation_id id;
        pending_kind kind;
        runtime::builtin_fault_point point;
        runtime::fault_decision fault;
        runtime::fault_action configured_action{runtime::fault_action::none};
        seastar::promise<runtime::result<pending_value>> completion;
        scheduler::event_id_reservation terminal_event;
        event_trace::reservation terminal_trace;
        scheduler::event_id_reservation crash_event;
        event_trace::reservation crash_trace;
        scheduler::event_id_reservation partial_resize_event;
        event_trace::reservation partial_resize_trace;
        event_id completion_event;
        std::optional<fake_object_id> object;
        operation_payload payload;
        std::optional<prepared_truncate> truncate_commit;
        std::uint64_t fault_a{0};
        std::uint64_t fault_b{0};
        byte_count accounted_bytes{};
        std::uint64_t accounted_path_bytes{0};
        trace_event_kind trace_kind{trace_event_kind::generic};
        bool open_slot{false};
        pending_phase phase{pending_phase::queued};
    };

    struct pending_operation final
      : runtime::shard_affine
      , prepared_operation {
        explicit pending_operation(prepared_operation&& prepared) noexcept
          : prepared_operation(std::move(prepared)) {}

        pending_operation(const pending_operation&) = delete;
        pending_operation& operator=(const pending_operation&) = delete;
        pending_operation(pending_operation&&) = delete;
        pending_operation& operator=(pending_operation&&) = delete;
    };

    static_assert(!std::is_move_constructible_v<inode>);
    static_assert(!std::is_copy_constructible_v<inode>);
    static_assert(std::derived_from<inode, runtime::shard_affine>);
    static_assert(!std::is_move_constructible_v<pending_operation>);
    static_assert(!std::is_copy_constructible_v<pending_operation>);
    static_assert(std::derived_from<pending_operation, runtime::shard_affine>);

    class pending_table final {
    public:
        explicit pending_table(std::size_t capacity);

        [[nodiscard]] std::pair<pending_operation*, bool>
        try_emplace(std::uint64_t key, prepared_operation operation);
        [[nodiscard]] pending_operation& at(std::uint64_t key) noexcept;
        [[nodiscard]] pending_operation* find(std::uint64_t key) noexcept;
        [[nodiscard]] bool erase(std::uint64_t key) noexcept;
        void copy_keys(std::vector<std::uint64_t>& output) const noexcept;

    private:
        using slot = std::optional<pending_operation>;
        static constexpr std::size_t entries_per_chunk = std::max<std::size_t>(
          1, maximum_contiguous_allocation_bytes / sizeof(slot));

        [[nodiscard]] slot& slot_at(std::size_t index) noexcept;

        std::deque<std::vector<slot>> chunks_;
        seastar::chunked_hash_map<std::uint64_t, std::size_t> indices_;
        std::vector<std::size_t> free_slots_;
    };

    class object_worklist final {
    public:
        explicit object_worklist(std::size_t capacity);

        void reset() noexcept { size_ = 0; }
        void push_back(std::uint64_t id) noexcept;
        [[nodiscard]] std::uint64_t
        operator[](std::size_t index) const noexcept;
        [[nodiscard]] std::size_t size() const noexcept { return size_; }

    private:
        static constexpr std::size_t entries_per_chunk = std::max<std::size_t>(
          1, maximum_contiguous_allocation_bytes / sizeof(std::uint64_t));

        std::deque<std::vector<std::uint64_t>> chunks_;
        std::size_t size_{0};
        std::size_t capacity_{0};
    };

    class prepared_directory_change final {
    public:
        prepared_directory_change(
          fake_file_system& owner,
          fake_object_id directory_id,
          directory_state& directory,
          std::string name,
          std::optional<fake_object_id> replacement);
        ~prepared_directory_change();

        prepared_directory_change(const prepared_directory_change&) = delete;
        prepared_directory_change&
        operator=(const prepared_directory_change&) = delete;
        prepared_directory_change(prepared_directory_change&& other) noexcept;
        prepared_directory_change&
        operator=(prepared_directory_change&& other) noexcept;

        void commit() noexcept;
        void rollback() noexcept;

    private:
        fake_file_system* owner_{nullptr};
        fake_object_id directory_id_;
        directory_state* directory_{nullptr};
        std::string name_;
        std::optional<fake_object_id> replacement_;
        std::optional<fake_object_id> previous_visible_;
        bool had_unsynced_{false};
        bool inserted_unsynced_{false};
        bool committed_{false};
        std::int64_t path_byte_delta_{0};
    };

    fake_file_system(
      fake_file_system_config config,
      canonical_fake_path root,
      scheduler* event_scheduler,
      fault_schedule* faults);

    [[nodiscard]] static runtime::result<canonical_fake_path>
    canonicalize_root(std::string_view root);
    [[nodiscard]] static runtime::result<canonical_fake_path> canonicalize(
      std::string_view input,
      const std::vector<std::string>& base,
      std::size_t minimum_depth,
      bool require_absolute);
    [[nodiscard]] runtime::result<canonical_fake_path>
    resolve(std::string_view input) const;
    [[nodiscard]] runtime::result<fake_object_id>
    lookup(const canonical_fake_path& path) const noexcept;
    [[nodiscard]] runtime::result<fake_object_id>
    lookup_parent(const canonical_fake_path& path) const noexcept;
    [[nodiscard]] runtime::result<fake_object_id>
    create(const canonical_fake_path& path, fake_file_kind kind);
    [[nodiscard]] runtime::result<runtime::file>
    apply_open(metadata_operation& metadata, bool& open_slot);
    [[nodiscard]] runtime::result<void>
    remove(const canonical_fake_path& path, fake_file_kind kind);
    [[nodiscard]] runtime::result<void>
    rename(const canonical_fake_path& from, const canonical_fake_path& to);
    [[nodiscard]] runtime::result<void>
    sync_directory(const canonical_fake_path& path);
    [[nodiscard]] runtime::result<seastar::chunked_vector<fake_directory_entry>>
    list(const canonical_fake_path& path) const;

    [[nodiscard]] runtime::result<byte_count> write(
      const canonical_fake_path& path,
      std::uint64_t position,
      std::span<const std::byte> bytes);
    [[nodiscard]] runtime::result<byte_count> write(
      fake_object_id id,
      std::uint64_t position,
      std::span<const std::byte> bytes);
    [[nodiscard]] runtime::result<byte_count> read(
      const canonical_fake_path& path,
      std::uint64_t position,
      std::span<std::byte> destination) const noexcept;
    [[nodiscard]] runtime::result<byte_count> read(
      fake_object_id id,
      std::uint64_t position,
      std::span<std::byte> destination) const noexcept;
    [[nodiscard]] runtime::result<void>
    truncate(const canonical_fake_path& path, std::uint64_t size);
    [[nodiscard]] runtime::result<void>
    truncate(fake_object_id id, std::uint64_t size);
    [[nodiscard]] runtime::result<prepared_truncate>
    prepare_truncate(fake_object_id id, std::uint64_t size);
    void commit_truncate(prepared_truncate prepared) noexcept;
    [[nodiscard]] runtime::result<void> flush(const canonical_fake_path& path);
    [[nodiscard]] runtime::result<void> flush(fake_object_id id);
    void restore_durable_state() noexcept;
    void invalidate_handles() noexcept;
    [[nodiscard]] runtime::result<void> begin_crash(fake_operation_id active);
    [[nodiscard]] runtime::result<void>
    begin_partial_resize(fake_operation_id active);
    [[nodiscard]] runtime::result<void>
    schedule_terminal(pending_operation& operation) noexcept;
    void complete_terminal(fake_operation_id id) noexcept;
    void apply_crash(fake_operation_id active) noexcept;
    void apply_partial_resize(fake_operation_id active) noexcept;
    void discard_operation(
      pending_operation& operation,
      const runtime::operation_error& failure) noexcept;
    void discard_remaining(const runtime::operation_error& failure) noexcept;
    void finish_stop() noexcept;

    [[nodiscard]] runtime::result<void>
    retain_open_reference(fake_object_id id) noexcept;
    [[nodiscard]] runtime::result<seastar::file>
    make_native_file_for_test(fake_object_id id, runtime::file_access access);
    void release_open_reference(fake_object_id id);
    void release_handle_reference(
      fake_object_id id,
      const seastar::lw_shared_ptr<open_handle_state>& handle);
    [[nodiscard]] runtime::result<fake_operation_id>
    issue_operation_id() noexcept;
    [[nodiscard]] seastar::future<>
    wait_submitted(pending_kind kind, std::uint64_t count);
    [[nodiscard]] seastar::future<>
    wait_parked(pending_kind kind, std::uint32_t count);
    [[nodiscard]] seastar::future<runtime::result<pending_value>> submit(
      prepared_operation operation,
      runtime::fault_object_key object_key,
      byte_count retained_bytes,
      trace_event_descriptor descriptor);
    [[nodiscard]] seastar::future<runtime::result<pending_value>>
    submit_file_operation(
      pending_kind kind,
      runtime::builtin_fault_point point,
      fake_object_id object,
      std::uint64_t position,
      std::uint64_t bytes,
      const char* source,
      char* destination,
      seastar::io_intent* intent,
      std::uint64_t generation,
      seastar::lw_shared_ptr<open_handle_state> handle = {});
    void complete(fake_operation_id id) noexcept;
    [[nodiscard]] runtime::result<pending_value>
    apply(pending_operation& operation);
    void finish(
      pending_operation& operation,
      runtime::result<pending_value> result,
      bool resolve);
    [[nodiscard]] runtime::result<void> validate_submission(
      pending_kind kind,
      byte_count retained_bytes,
      std::uint64_t retained_path_bytes,
      bool open_slot) const noexcept;
    [[nodiscard]] std::uint64_t next_occurrence(
      runtime::builtin_fault_point point,
      std::optional<fake_object_id> object) const noexcept;
    void commit_occurrence(
      runtime::builtin_fault_point point,
      std::optional<fake_object_id> object) noexcept;
    [[nodiscard]] runtime::result<runtime::monotonic_time> completion_deadline(
      runtime::fault_decision fault,
      fake_operation_id operation,
      pending_kind kind) const noexcept;
    [[nodiscard]] runtime::monotonic_duration operation_latency(
      fake_operation_id operation, pending_kind kind) const noexcept;
    [[nodiscard]] static bool is_read_operation(pending_kind kind) noexcept;
    [[nodiscard]] static bool is_write_operation(pending_kind kind) noexcept;

    [[nodiscard]] inode* find_inode(fake_object_id id) noexcept;
    [[nodiscard]] const inode* find_inode(fake_object_id id) const noexcept;
    [[nodiscard]] bool is_overwrite(
      fake_object_id id,
      std::uint64_t position,
      std::uint64_t length) const noexcept;
    [[nodiscard]] runtime::result<regular_file_state*>
    regular_file(const canonical_fake_path& path) noexcept;
    [[nodiscard]] runtime::result<const regular_file_state*>
    regular_file(const canonical_fake_path& path) const noexcept;
    [[nodiscard]] runtime::result<directory_state*>
    directory(const canonical_fake_path& path) noexcept;
    [[nodiscard]] runtime::result<const directory_state*>
    directory(const canonical_fake_path& path) const noexcept;

    [[nodiscard]] runtime::result<void> apply_directory_change(
      fake_object_id directory_id,
      directory_state& directory,
      std::string name,
      std::optional<fake_object_id> id);
    [[nodiscard]] runtime::result<void>
    validate_path_delta(std::int64_t delta) const noexcept;
    void apply_path_delta(std::int64_t delta) noexcept;
    [[nodiscard]] static std::int64_t directory_change_path_delta(
      const directory_state& directory, std::string_view name) noexcept;
    [[nodiscard]] static std::optional<fake_object_id> visible_child(
      const directory_state& directory, std::string_view name) noexcept;
    [[nodiscard]] bool
    directory_visible_empty(const directory_state& directory) const noexcept;
    void mark_dirty(inode& object) noexcept;
    void clear_dirty(inode& object) noexcept;
    [[nodiscard]] static std::uint64_t
    retained_path_size(const canonical_fake_path& path) noexcept;
    [[nodiscard]] static std::uint64_t
    retained_size(const regular_file_state& file) noexcept;
    [[nodiscard]] runtime::result<void> update_retained_capacity(
      std::uint64_t before, std::uint64_t after) noexcept;
    void collect_unreachable(fake_object_id candidate);
    void
    collect_unreachable_from(seastar::chunked_vector<std::uint64_t> pending);

    fake_file_system_config config_;
    canonical_fake_path root_;
    inode_map objects_;
    byte_count retained_capacity_{};
    std::uint64_t next_object_id_{2};
    std::uint64_t next_operation_id_{1};
    bool object_ids_exhausted_{false};
    bool operation_ids_exhausted_{false};
    scheduler* scheduler_{nullptr};
    fault_schedule* faults_{nullptr};
    pending_table pending_;
    std::array<std::uint64_t, static_cast<std::size_t>(pending_kind::count)>
      submitted_by_kind_{};
    std::array<std::uint32_t, static_cast<std::size_t>(pending_kind::count)>
      parked_by_kind_{};
    seastar::condition_variable operation_changed_;
    std::array<std::uint64_t, runtime::builtin_fault_points.size()>
      global_occurrences_{};
    std::uint32_t pending_operations_{0};
    std::uint32_t parked_operations_{0};
    std::uint32_t pending_reads_{0};
    std::uint32_t pending_writes_{0};
    byte_count pending_bytes_{};
    std::uint64_t pending_path_bytes_{0};
    std::uint64_t retained_path_bytes_{0};
    std::uint32_t open_handles_{0};
    std::uint32_t pending_opens_{0};
    std::uint64_t generation_{1};
    object_worklist collection_worklist_;
    seastar::chunked_hash_set<std::uint64_t> open_objects_;
    std::uint64_t dirty_head_{0};
    std::vector<std::uint64_t> pending_ids_;
    std::optional<seastar::shared_promise<runtime::result<void>>> stop_done_;
    std::optional<runtime::operation_error> stop_failure_;
    fake_file_system_state state_{fake_file_system_state::open};
};

static_assert(!std::is_move_constructible_v<fake_file_system>);
static_assert(!std::is_move_assignable_v<fake_file_system>);
static_assert(!std::is_copy_constructible_v<fake_file_system>);
static_assert(!std::is_copy_assignable_v<fake_file_system>);
static_assert(runtime::file_system_backend<fake_file_system>);

} // namespace kwaque::simulation

#endif // KWAQUE_SRC_SIMULATION_FAKE_FILE_H_
