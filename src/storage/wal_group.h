#pragma once

#include "src/runtime/file.h"
#include "src/storage/wal_child.h"

#include <seastar/core/chunked_vector.hh>

namespace kwaque::storage {
template<runtime::file_system_backend Backend, typename Owner>
class wal_writer;
namespace detail {
class wal_write_descriptor;
class wal_descriptor_queue;
struct wal_member_memory final {
    byte_count input, additional, codec_bytes, retained;
    item_count fragments;
};
[[nodiscard]] runtime::result<wal_member_memory> wal_prepare_memory(
  const encoded_assigned_batch&,
  aligned_envelope_layout,
  const codec::limits&,
  bytes::allocation_charge_fn);
struct wal_group_member final {
    admitted_wal_batch::contents input;
    wal_child_expectation expected;
    std::optional<wal_prepare_expectation> reserved;
    std::optional<wal_member_memory> memory;
};
} // namespace detail

inline constexpr std::uint32_t maximum_wal_group_members = 64;

// Bounded offered membership, not a WAL reservation. Reserve its metadata
// before adding exact, already admitted children. The consumer freezes this
// owner by moving it; input aliases and their reservations stay with execution.
// Retain the separately admitted segment/original alias before offering the
// WAL child. The supplied target context/pins belong to that enclosing owner.
class wal_group final {
public:
    [[nodiscard]] static runtime::result<wal_group>
    make(workload_budget&, std::uint32_t capacity);
    wal_group(wal_group&&) noexcept;
    ~wal_group() { owner_.assert_current(); }
    wal_group& operator=(wal_group&&) = delete;
    wal_group(const wal_group&) = delete;
    wal_group& operator=(const wal_group&) = delete;
    [[nodiscard]] runtime::result<void>
      append(admitted_wal_batch, wal_child_expectation);
    [[nodiscard]] std::size_t size() const noexcept {
        owner_.assert_current();
        return members_.size();
    }

private:
    template<runtime::file_system_backend Backend, typename Owner>
    friend class wal_writer;
    friend class detail::wal_write_descriptor;
    friend class detail::wal_descriptor_queue;
    wal_group(workload_reservation, std::uint32_t);
    workload_reservation metadata_;
    runtime::owner_shard owner_;
    std::optional<workload_reservation> encoding_;
    seastar::chunked_vector<detail::wal_group_member> members_;
    std::uint32_t capacity_;
    byte_count offered_bytes_{}, offered_retained_{};
    std::size_t offered_fragments_{0};
    byte_count input_bytes_{}, additional_bytes_{}, retained_bound_{};
    std::size_t fragment_bound_{0};
};
} // namespace kwaque::storage
