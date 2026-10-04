#pragma once

#include "src/base/units.h"
#include "src/codec/cooperative.h"
#include "src/codec/envelope.h"
#include "src/runtime/file.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/future.hh>

#include <array>
#include <cstdint>
#include <memory>
#include <optional>

namespace kwaque::storage {

inline constexpr std::uint32_t maximum_scan_read_ahead = 4;

struct scan_reader_limits final {
    // One native read per aligned window of this many bytes.
    byte_count window_bytes{maximum_contiguous_allocation_bytes};
    // Windows requested beyond the one being examined. Four reads in flight
    // keep a large sequential scan near the device's rate; more add little.
    std::uint32_t read_ahead{3};
    // The largest complete object admitted; the codec ceiling also applies.
    byte_count maximum_object_bytes{runtime::maximum_file_io_bytes};
    // Native completion, control and frame cost outside the buffered bytes.
    byte_count execution_bytes{64_KiB};

    [[nodiscard]] runtime::result<void> validate() const noexcept;
};

enum class scan_slot_kind : std::uint8_t {
    // A complete envelope whose header checksum matches, so its declared
    // extent can be trusted. Its family decoder checks the body.
    object,
    // No byte remains before the scan end.
    end,
    // The fixed prefix is all zero: unwritten, zero-written or reverted space.
    zero,
    // The prefix, header or declared object extends past the scan end.
    torn,
    // The header checksum does not match.
    corrupt,
    // Wrong magic, or header extents no writer produces.
    malformed,
};

struct scan_slot final {
    scan_slot_kind kind;
    runtime::file_position position;
    // An object's header-verified fixed prefix.
    std::optional<codec::unverified_envelope_prefix> prefix;
    // The codec finding behind a corrupt or malformed slot.
    std::optional<codec::error> cause;
};

// One sequential pass over [begin, end) of a file the caller keeps open,
// alive and unmoved until close() completes. Aligned windows are read ahead
// in file order and objects are examined in place, never with a read of
// their own. Classifying a slot never moves the cursor; advance() does, by
// the header-verified length, once the caller's decoder has accepted the
// object. A failed or short read is returned as an error, never as an end or
// a torn tail. Buffered and in-flight bytes are admitted from the budget.
// One operation at a time: close() only when no next() is pending.
class scan_reader final {
public:
    [[nodiscard]] static runtime::result<std::unique_ptr<scan_reader>> make(
      runtime::file& file,
      workload_budget& budget,
      runtime::file_position begin,
      runtime::file_position end,
      scan_reader_limits limits = {});
    scan_reader(const scan_reader&) = delete;
    scan_reader& operator=(const scan_reader&) = delete;
    scan_reader(scan_reader&&) = delete;
    scan_reader& operator=(scan_reader&&) = delete;
    ~scan_reader();

    // Classifies the slot at the cursor. Repeating it before advance()
    // returns the same slot.
    [[nodiscard]] seastar::future<runtime::result<scan_slot>>
    next(codec::cooperative_work& work);
    // The current object's exact bytes, sharing the read windows. Valid after
    // next() returned an object and before advance() or close().
    [[nodiscard]] runtime::result<bytes::fragmented_buffer> object();
    [[nodiscard]] runtime::result<void> advance();
    [[nodiscard]] runtime::file_position position() const noexcept {
        return cursor_;
    }
    [[nodiscard]] runtime::file_position end() const noexcept { return end_; }
    // Joins every outstanding read and releases the buffered bytes.
    [[nodiscard]] seastar::future<> close() noexcept;

private:
    struct pending_read final {
        runtime::file_position at;
        byte_count length;
        seastar::future<runtime::result<runtime::file_read_result>> read;
    };
    static constexpr std::size_t read_slots = maximum_scan_read_ahead + 1;

    scan_reader(
      runtime::file& file,
      workload_budget& budget,
      workload_reservation reservation,
      runtime::file_position begin,
      runtime::file_position end,
      scan_reader_limits limits) noexcept;
    void issue();
    seastar::future<runtime::result<void>>
    fill(runtime::file_position through, codec::cooperative_work& work);
    [[nodiscard]] runtime::result<bytes::fragmented_buffer>
    share(runtime::file_position at, byte_count length);
    seastar::future<runtime::result<scan_slot>>
    classify(codec::cooperative_work& work);

    runtime::file& file_;
    workload_budget& budget_;
    workload_reservation reservation_;
    std::optional<workload_reservation> object_reservation_;
    scan_reader_limits limits_;
    runtime::file_position cursor_, end_;
    // Bytes [buffered_begin_, buffered_end_) in file order.
    bytes::fragmented_buffer buffered_;
    runtime::file_position buffered_begin_, buffered_end_;
    // The next window to request; reads cover [buffered_end_, next_read_).
    runtime::file_position next_read_;
    std::array<std::optional<pending_read>, read_slots> reads_;
    std::size_t head_{0}, count_{0};
    std::optional<scan_slot> current_;
    bool closed_{false}, broken_{false};
};

} // namespace kwaque::storage
