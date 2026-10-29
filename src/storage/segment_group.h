#pragma once

#include "src/runtime/shard_affinity.h"
#include "src/storage/placement_statistics.h"
#include "src/storage/segment_writer_state.h"
#include "src/storage/wal_child.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/gate.hh>

namespace kwaque::storage {
// A group frozen to be held completes once its run keeps its bytes, with
// nothing written: whatever barrier covers the run tells how its write went.
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
    // The bytes its write covers: its own, after those of the run it
    // closes.
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
    // Where the run it closes begins, once it has taken that run's bytes,
    // and what those copies are charged to until they are hashed.
    std::optional<runtime::file_position> run_begin;
    std::optional<workload_reservation> run_memory;
    // A held group's run: the chunks it reserved for its copies.
    std::uint32_t run_chunks{0};
    // The run held open before it, closed by its freeze, and why.
    std::optional<segment_run_close> closed_run;
    segment_write_state state{segment_write_state::frozen};
    bool linked{false}, observed{false}, notified{false};
    // wanted: its durability is wanted now, so its write may go through the
    // synchronized handle. held: it has no footer, and at assembly its
    // bytes are copied into its run and everything else of it is let go;
    // absorbed: that happened.
    bool wanted{false}, held{false}, absorbed{false};
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
    // Whether its blocks wait unwritten in the run held open.
    [[nodiscard]] bool held() const noexcept;
    // The run held open before it, if its freeze closed that run, and why:
    // with a footer of the run's own before this group, or with this
    // group's footer once the run reached its hold with it.
    [[nodiscard]] std::optional<segment_run_close> closed() const noexcept;

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
