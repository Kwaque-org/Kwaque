#pragma once

#include "src/storage/wal_format.h"
#include "src/storage/workload_budget.h"

#include <optional>

namespace kwaque::storage {

struct wal_child_limits final {
    byte_count working_bytes{8U * 1024U * 1024U};
    byte_count metadata_bytes{65536};
    byte_count execution_bytes{65536};
    bytes::allocation_charge_fn charge{nullptr};

    [[nodiscard]] runtime::result<void> validate() const noexcept;
};
struct wal_prepared_children;

// Accounting accompanies exact bytes through both destinations. The supplied
// common reservation already covers the entire backing, original descriptors,
// possible sharing controls and any opaque deleter ownership. It must not also
// be used to fund unrelated allocations. Extraction transfers both owners.
class admitted_wal_batch final {
public:
    struct contents final {
        workload_reservation backing;
        std::optional<workload_reservation> aliases;
        encoded_assigned_batch batch;
    };
    [[nodiscard]] static runtime::result<admitted_wal_batch> make(
      encoded_assigned_batch,
      workload_reservation,
      bytes::allocation_charge_fn);
    admitted_wal_batch(admitted_wal_batch&&) noexcept = default;
    admitted_wal_batch& operator=(admitted_wal_batch&&) = delete;
    admitted_wal_batch(const admitted_wal_batch&) = delete;
    admitted_wal_batch& operator=(const admitted_wal_batch&) = delete;
    [[nodiscard]] const encoded_assigned_batch& batch() const& noexcept {
        return value_.batch;
    }
    const encoded_assigned_batch& batch() const&& = delete;
    [[nodiscard]] contents release() && noexcept { return std::move(value_); }

private:
    friend seastar::future<runtime::result<wal_prepared_children>>
    prepare_wal_children(
      admitted_wal_batch&&,
      wal_child_expectation,
      workload_budget&,
      wal_child_limits,
      codec::cooperative_work&);
    explicit admitted_wal_batch(contents value) noexcept
      : value_(std::move(value)) {}
    contents value_;
};

struct wal_prepared_children final {
    // Retains result/context storage even if one child is extracted first.
    workload_reservation preparation;
    admitted_wal_batch wal;
    admitted_wal_batch segment;
    wal_child_expectation expected;
};

// Consumes before the first await, also on entered rejection. Frame allocation
// failure before entry leaves the source intact. Borrow work only until joined
// completion. The outputs contain no work/abort borrow and carry no assigned
// WAL extent, acceptance, write completion or durability claim.
[[nodiscard]] seastar::future<runtime::result<wal_prepared_children>>
prepare_wal_children(
  admitted_wal_batch&&,
  wal_child_expectation,
  workload_budget&,
  wal_child_limits,
  codec::cooperative_work&);

} // namespace kwaque::storage
