#pragma once

#include "src/storage/encoded_batch.h"
#include "src/storage/format_context.h"

namespace kwaque::storage {
namespace detail {
class wal_codec;
}

inline constexpr byte_count wal_prepare_fixed_bytes{136};

enum class wal_field : std::uint16_t {
    fixed_body = 192,
    incarnation,
    wal_position,
    cluster,
    topic,
    range,
    segment,
    generation,
    routing_epoch,
    physical_begin,
    segment_position,
    alignment,
    replay_profile,
    reserved,
    assigned_bytes,
    padding,
    assigned_batch,
};

// Independently pinned file lifetimes and positions. WAL alignment governs its
// own envelope; target alignment/profile/data_start come from the segment
// header. The target layout may differ from the child's original append
// binding. routing_epoch must agree with the original child binding, not a new
// route.
struct wal_prepare_expectation final {
    wal_write_context wal;
    segment_write_context target;
    runtime::file_position target_data_start;
    model::range_routing_epoch routing_epoch;
    model::batch_decode_expectation batch;
    replay_profile profile{replay_profile::v1};
    storage_profile target_profile{storage_profile::v1};
};

// One fully validated PREPARE payload. The exact family-2 child, including its
// codec and optional extensions, remains owned; outer envelope extensions are
// validated but not retained by this value. wal_extent describes the complete
// input envelope. No replay, persistence, completed-result or ACK is implied.
// Moves/extraction empty the child's bytes; scalar context remains readable.
class wal_prepare final {
public:
    wal_prepare(const wal_prepare&) = delete;
    wal_prepare& operator=(const wal_prepare&) = delete;
    wal_prepare(wal_prepare&&) noexcept = default;
    wal_prepare& operator=(wal_prepare&&) noexcept = default;
    [[nodiscard]] wal_write_context wal() const noexcept { return wal_; }
    [[nodiscard]] segment_write_context target() const noexcept {
        return target_;
    }
    [[nodiscard]] model::range_routing_epoch routing_epoch() const noexcept {
        return routing_;
    }
    [[nodiscard]] replay_profile profile() const noexcept { return profile_; }
    [[nodiscard]] storage_profile target_profile() const noexcept {
        return target_profile_;
    }
    [[nodiscard]] model::file_byte_span wal_extent() const noexcept {
        return extent_;
    }
    [[nodiscard]] const encoded_assigned_batch& batch() const& noexcept {
        return batch_;
    }
    const encoded_assigned_batch& batch() const&& = delete;
    [[nodiscard]] encoded_assigned_batch release_batch() && noexcept {
        return std::move(batch_);
    }

private:
    friend class detail::wal_codec;
    wal_prepare(
      const wal_prepare_expectation& expected,
      model::file_byte_span extent,
      encoded_assigned_batch batch) noexcept
      : wal_(expected.wal)
      , target_(expected.target)
      , routing_(expected.routing_epoch)
      , profile_(expected.profile)
      , target_profile_(expected.target_profile)
      , extent_(extent)
      , batch_(std::move(batch)) {}
    wal_write_context wal_;
    segment_write_context target_;
    model::range_routing_epoch routing_;
    replay_profile profile_;
    storage_profile target_profile_;
    model::file_byte_span extent_;
    encoded_assigned_batch batch_;
};

struct decoded_wal_prepare final {
    wal_prepare value;
    codec::decode_budget remaining;
};

// Consumes the child before the first await, including failure/cancellation.
// Outer coroutine allocation failure precedes transfer. parent_remaining
// includes this child's backing/metadata and new fixed/padding/envelope
// staging; other live/native/frame costs are excluded. The exact child is never
// recompressed. Narrower semantic limits require revalidation of the same
// bytes. Work/abort/allocator profile stay stable and exclusive until joined
// completion.
[[nodiscard]] seastar::future<codec::result<bytes::fragmented_buffer>>
encode_wal_prepare(
  encoded_assigned_batch&& child,
  wal_prepare_expectation expected,
  codec::cooperative_work& work,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context context = {});

// Decode one complete PREPARE under one outer envelope transaction. Input,
// work and abort stay alive, unmoved and exclusive. Reserve the parent input
// once before entry; memory excludes that backing/metadata/promotion and other
// live/native/frame costs. remaining reserves the returned child's actual
// descriptor capacity; original backing remains reserved while any alias lives.
// Temporary expanded records and parser aliases are joined before publication.
// Short input, invalid padding after child success, exceptions and cancellation
// leave the caller's cursor/marks unchanged. Diagnostic origin is independent
// of both expected file positions. A fragment boundary never completes PREPARE.
[[nodiscard]] seastar::future<codec::result<decoded_wal_prepare>>
decode_wal_prepare(
  bytes::fragmented_buffer_parser& input,
  wal_prepare_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context = {},
  codec::input_boundary boundary = codec::input_boundary::open);
} // namespace kwaque::storage
