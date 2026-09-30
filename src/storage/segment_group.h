#pragma once

#include "src/runtime/shard_affinity.h"
#include "src/storage/segment_writer_state.h"
#include "src/storage/wal_child.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/gate.hh>

namespace kwaque::storage {
struct segment_write_completion final {
    std::optional<workload_reservation> reservation;
    runtime::first_failure failure;
    byte_count written{};
};

namespace detail {
enum class segment_write_state : std::uint8_t {
    frozen,
    encoding,
    encoded,
    authorized,
    assembling,
    queued,
    dispatched,
    done
};

// Queue and execution retain one stable owner. Only complete immutable buffers
// enter the runtime; its physical window remains the only DMA scheduler.
class segment_write_descriptor final {
public:
    using pointer = seastar::lw_shared_ptr<segment_write_descriptor>;
    segment_write_descriptor(
      workload_reservation control,
      workload_reservation work,
      seastar::lw_shared_ptr<segment_lifetime> lifetime,
      segment_captured_boundary before,
      segment_group_layout layout,
      std::vector<admitted_wal_batch::contents> children,
      codec::limits policy);
    ~segment_write_descriptor();
    void fail(runtime::operation_error) noexcept;
    void fail(std::exception_ptr) noexcept;
    void abandon() noexcept;
    void notify() noexcept;
    [[nodiscard]] model::file_byte_span extent() const noexcept;

    workload_reservation control;
    runtime::owner_shard owner;
    std::optional<workload_reservation> working;
    seastar::lw_shared_ptr<segment_lifetime> lifetime;
    segment_captured_boundary before;
    segment_group_layout layout;
    std::vector<admitted_wal_batch::contents> children;
    std::vector<segment_block> blocks;
    bytes::fragmented_buffer payload;
    // Shared view of the written bytes, kept until the extent digest hashed
    // them.
    bytes::fragmented_buffer digest;
    // The first group of a gathered write in flight: the groups it covers,
    // its joined completion and its outcome, applied in file order.
    std::optional<seastar::future<>> write;
    runtime::first_failure write_failure;
    std::uint32_t gathered{0};
    codec::limits policy;
    seastar::promise<segment_write_completion> completion;
    seastar::gate execution;
    runtime::first_failure failure;
    byte_count gather_credit{}, retained_bound{};
    segment_write_state state{segment_write_state::frozen};
    bool linked{false}, observed{false}, notified{false};
};
} // namespace detail

// Single-use authorization handoff. Dropping it before submission fences its
// owner; frozen positions are never rewound. Layout/blocks are borrowed only
// until the next operation, move or destruction. Keep it alive and exclusive
// across encode_group. A moved-from handoff has no usable borrows.
// Closing the writer fences an unsubmitted handoff but preserves its existing
// byte borrows until this handoff is moved, submitted or destroyed.
class segment_frozen_group final {
public:
    segment_frozen_group(const segment_frozen_group&) = delete;
    segment_frozen_group& operator=(const segment_frozen_group&) = delete;
    segment_frozen_group(segment_frozen_group&&) noexcept = default;
    segment_frozen_group& operator=(segment_frozen_group&&) = delete;
    ~segment_frozen_group();
    [[nodiscard]] const segment_group_layout& layout() const& noexcept;
    const segment_group_layout& layout() const&& = delete;
    [[nodiscard]] std::span<const segment_block> blocks() const& noexcept;
    std::span<const segment_block> blocks() const&& = delete;

private:
    template<
      runtime::file_system_backend Backend,
      typename Owner,
      runtime::monotonic_clock Clock>
    friend class segment_writer;
    explicit segment_frozen_group(
      detail::segment_write_descriptor::pointer node)
      : node_(std::move(node)) {}
    detail::segment_write_descriptor::pointer node_;
};

struct segment_submission final {
    segment_captured_boundary boundary;
    seastar::future<segment_write_completion> written;
};
} // namespace kwaque::storage
