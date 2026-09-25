#pragma once

#include "src/codec/cooperative.h"
#include "src/runtime/file.h"
#include "src/runtime/first_failure.h"
#include "src/storage/local_types.h"
#include "src/storage/wal_group.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>

namespace kwaque::storage {
template<runtime::file_system_backend Backend, typename Owner>
class wal_writer;

// Owner-issued complete boundary. Copying one preserves the lifetime token;
// numeric cursors alone cannot manufacture or retarget one. Retained captures
// retain their small accounting owner, never an open file or the writer itself.
class wal_captured_boundary final {
public:
    [[nodiscard]] local_store_context owner() const noexcept { return owner_; }
    [[nodiscard]] local_wal_cursor cursor() const noexcept { return cursor_; }
    // Includes the lifetime token and complete membership, not only position.
    bool operator==(const wal_captured_boundary&) const noexcept = default;

private:
    template<runtime::file_system_backend Backend, typename Owner>
    friend class wal_writer;
    struct lifetime final {
        explicit lifetime(workload_reservation held)
          : held(std::move(held)) {}
        workload_reservation held;
    };
    wal_captured_boundary(
      seastar::lw_shared_ptr<lifetime> token,
      local_store_context owner,
      local_wal_cursor cursor,
      std::uint64_t members) noexcept
      : token_(std::move(token))
      , owner_(owner)
      , cursor_(cursor)
      , members_(members) {}
    seastar::lw_shared_ptr<lifetime> token_;
    local_store_context owner_;
    local_wal_cursor cursor_;
    std::uint64_t members_;
};

struct wal_writer_positions final {
    local_store_context owner;
    local_wal_cursor reserved;
    local_wal_cursor write_complete;
    local_wal_cursor durable;
};

// WAL-only persistence evidence for an owner-issued complete cut. It is not a
// segment barrier, completed append result, checkpoint or producer ACK.
class wal_durable_receipt final {
public:
    [[nodiscard]] const wal_captured_boundary& boundary() const& noexcept {
        return boundary_;
    }
    const wal_captured_boundary& boundary() const&& = delete;

private:
    template<runtime::file_system_backend Backend, typename Owner>
    friend class wal_writer;
    explicit wal_durable_receipt(wal_captured_boundary boundary) noexcept
      : boundary_(std::move(boundary)) {}
    wal_captured_boundary boundary_;
};
struct wal_barrier_outcome final {
    runtime::first_failure failure;
    std::optional<wal_durable_receipt> receipt;
};

namespace detail {
enum class wal_write_state : std::uint8_t {
    encoding,
    queued,
    dispatched,
    done
};

struct wal_write_completion final {
    // Caller retention remains charged after the queue slot is reusable.
    std::optional<workload_reservation> reservation;
    runtime::first_failure failure;
    byte_count written{};
};

// Stable execution node. The queue and the executing operation own it; the
// caller receives only a future of the completion value. Dropping that future
// neither aborts execution nor frees its payload. Both error channels are
// values until the interested caller explicitly observes failure.outcome().
class wal_write_descriptor final {
public:
    using pointer = seastar::lw_shared_ptr<wal_write_descriptor>;
    static constexpr byte_count minimum_execution_bytes{4096};
    [[nodiscard]] static runtime::result<pointer> make(
      workload_budget&,
      model::file_byte_span,
      codec::limits,
      byte_count execution_bytes);
    wal_write_descriptor(
      workload_reservation, model::file_byte_span, codec::limits);
    ~wal_write_descriptor();
    [[nodiscard]] seastar::future<wal_write_completion> observe();
    [[nodiscard]] wal_write_state state() const noexcept {
        owner_.assert_current();
        return state_;
    }
    [[nodiscard]] model::file_byte_span extent() const noexcept {
        owner_.assert_current();
        return extent_;
    }
    [[nodiscard]] codec::cooperative_work& work() noexcept {
        owner_.assert_current();
        return work_;
    }
    [[nodiscard]] seastar::abort_source& execution_abort() noexcept {
        owner_.assert_current();
        return abort_;
    }
    [[nodiscard]] runtime::result<void> encoded(
      bytes::fragmented_buffer,
      workload_reservation,
      bytes::allocation_charge_fn);
    [[nodiscard]] const bytes::fragmented_buffer& payload() const& noexcept {
        owner_.assert_current();
        return payload_;
    }
    const bytes::fragmented_buffer& payload() const&& = delete;
    [[nodiscard]] const runtime::first_failure& failure() const& noexcept {
        owner_.assert_current();
        return result_.failure;
    }
    const runtime::first_failure& failure() const&& = delete;
    void dispatch() noexcept;
    void complete(runtime::result<byte_count>) noexcept;
    void fail(std::exception_ptr) noexcept;
    void fail(runtime::operation_error) noexcept;

private:
    friend class wal_descriptor_queue;
    template<runtime::file_system_backend Backend, typename Owner>
    friend class ::kwaque::storage::wal_writer;
    void notify() noexcept;
    [[nodiscard]] runtime::result<void>
      encoded_group(bytes::fragmented_buffer, bytes::allocation_charge_fn);
    workload_reservation held_;
    std::optional<workload_reservation> payload_held_;
    // Group backing/encoding charges outlive payload descriptors and bytes,
    // including encoded groups fenced before dispatch.
    std::optional<wal_group> group_;
    bytes::fragmented_buffer payload_;
    runtime::owner_shard owner_;
    model::file_byte_span extent_;
    seastar::abort_source abort_;
    codec::cooperative_work work_;
    std::optional<seastar::future<>> encoding_;
    seastar::promise<wal_write_completion> completion_;
    wal_write_completion result_;
    wal_write_state state_{wal_write_state::encoding};
    bool observed_{false}, notified_{false}, linked_{false};
};

// Internal ordered execution storage. Installation is fallible and does not
// reserve file coordinates or attest to their completeness. The enclosing WAL
// admission path must install a whole group before mutating its reserved end.
// Retirement releases one reusable slot before signaling its caller, retaining
// the node/result through notification. It never advances any durable cursor.
class wal_descriptor_queue final {
public:
    struct ready_gather final {
        std::size_t groups{0}, fragments{0};
        byte_count logical{}, retained{};
    };
    wal_descriptor_queue(std::uint32_t maximum, byte_count maximum_bytes)
      : maximum_(maximum)
      , maximum_bytes_(maximum_bytes) {}
    ~wal_descriptor_queue();
    [[nodiscard]] runtime::result<void> push(wal_write_descriptor::pointer);
    [[nodiscard]] wal_write_descriptor::pointer retire_front() noexcept;
    // Split retirement lets the owner install progress before notification.
    [[nodiscard]] wal_write_descriptor::pointer pop_completed_front() noexcept;
    [[nodiscard]] const wal_write_descriptor::pointer& front() const {
        return inflight_.front();
    }
    auto begin() const { return inflight_.begin(); }
    auto end() const { return inflight_.end(); }
    [[nodiscard]] std::size_t size() const noexcept { return inflight_.size(); }
    [[nodiscard]] bool empty() const noexcept { return inflight_.empty(); }
    [[nodiscard]] byte_count bytes() const noexcept { return bytes_; }
    [[nodiscard]] byte_count retained_bytes() const noexcept {
        return retained_;
    }
    [[nodiscard]] ready_gather ready_prefix(
      byte_count maximum_bytes = runtime::maximum_file_io_bytes,
      std::size_t maximum_fragments
      = bytes::max_buffer_fragments) const noexcept;

private:
    // Each chunk allocation is conservatively admitted with its first node;
    // retained chunks are disabled, so node charges cover every live chunk.
    seastar::chunked_fifo<wal_write_descriptor::pointer, 16> inflight_;
    runtime::owner_shard owner_;
    std::uint32_t maximum_;
    byte_count maximum_bytes_, bytes_{}, retained_{};
};
} // namespace detail

// Optional tighter enclosing-owner limits, checked before reservation.
struct wal_submission_limits final {
    std::uint32_t members{maximum_wal_group_members};
    byte_count encoded_bytes{runtime::maximum_file_io_bytes};
};

struct wal_submission final {
    wal_captured_boundary boundary;
    seastar::future<detail::wal_write_completion> written;
    model::file_byte_span extent;
    std::uint32_t members;
};
} // namespace kwaque::storage
