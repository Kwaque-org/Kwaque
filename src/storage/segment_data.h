#pragma once

#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/runtime/error.h"
#include "src/runtime/file.h"
#include "src/runtime/file_position.h"
#include "src/runtime/shard_affinity.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>

#include <optional>

namespace kwaque::storage {

// Reads `length` bytes of a segment's data at `position`, through a read-only
// handle its owner keeps open. `bound` is how far the caller may read: the
// end of a sealed extent, or, for a segment that is still written, a
// position its writer's bytes in the file had reached. The bound is the
// caller's own, and later appends do not move it. Nothing at or past it is
// read: a range that does not lie wholly below it is refused with
// out_of_range before any work.
//
// The bytes come back in the file's own buffers. Whoever asks has admitted
// them already: a read reserves nothing here. A read that fails, falls short
// or is refused by the file is that read's outcome and nobody else's; no
// owner is failed by it, and the next read is tried like the first.
[[nodiscard]] seastar::future<runtime::result<bytes::fragmented_buffer>>
read_segment_data(
  runtime::file& file,
  runtime::file_position position,
  byte_count length,
  runtime::file_position bound);

class segment_data_pin;

namespace detail {
// The read-only handle the readers of one segment's data share. Its writer
// opens it when the first reader asks, apart from the handle it writes
// through, and closes it at its own close, once nothing reads through it.
struct segment_data_handle final
  : seastar::enable_lw_shared_from_this<segment_data_handle>
  , runtime::shard_affine {
    explicit segment_data_handle(workload_reservation credit) noexcept
      : held(std::move(credit)) {}
    segment_data_handle(const segment_data_handle&) = delete;
    segment_data_handle& operator=(const segment_data_handle&) = delete;
    ~segment_data_handle();
    // Another reader. The handle is open.
    [[nodiscard]] segment_data_pin pin();
    // The handle's credit, held for as long as it is open.
    workload_reservation held;
    std::optional<runtime::file> file;
    // Readers and reads in flight: its close waits for both.
    seastar::gate readers;
    bool closed{false};
};
} // namespace detail

// The right to read a segment's data through the handle its writer opened
// for readers. The handle stays open while anything holds one of these, and
// its writer's close waits for them. What a holder reads is bounded by the
// position it was given with its pin; see read_segment_data().
class segment_data_pin final {
public:
    segment_data_pin(segment_data_pin&&) noexcept;
    segment_data_pin& operator=(segment_data_pin&&) noexcept = delete;
    segment_data_pin(const segment_data_pin&) = delete;
    segment_data_pin& operator=(const segment_data_pin&) = delete;
    ~segment_data_pin();
    // Another right to the same handle, for work that outlives its caller.
    // It takes nothing and cannot be refused.
    [[nodiscard]] segment_data_pin share() const noexcept;
    // As read_segment_data(). The read keeps the handle open by itself: it
    // completes whatever becomes of this pin meanwhile.
    [[nodiscard]] seastar::future<runtime::result<bytes::fragmented_buffer>>
    read(
      runtime::file_position position,
      byte_count length,
      runtime::file_position bound) const;

private:
    friend struct detail::segment_data_handle;
    segment_data_pin(
      seastar::lw_shared_ptr<detail::segment_data_handle> state,
      seastar::gate::holder holder) noexcept;
    seastar::lw_shared_ptr<detail::segment_data_handle> state_;
    seastar::gate::holder holder_;
};

} // namespace kwaque::storage
