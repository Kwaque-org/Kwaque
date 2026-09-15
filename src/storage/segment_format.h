#pragma once

#include "src/storage/encoded_batch.h"
#include "src/storage/format_context.h"

namespace kwaque::storage {
namespace detail {
class segment_codec;
}

inline constexpr byte_count segment_header_fixed_bytes{96};
inline constexpr byte_count segment_block_fixed_bytes{120};

enum class segment_field : std::uint16_t {
    fixed_body = 160,
    cluster,
    topic,
    range,
    segment,
    generation,
    logical_origin,
    storage_profile,
    record_profile,
    alignment,
    padding,
    reserved,
    block_position,
    physical_begin,
    physical_end,
    logical_begin,
    logical_end,
    assigned_bytes,
    assigned_batch,
};

class segment_header final {
public:
    [[nodiscard]] static result<segment_header> make(
      segment_context context,
      model::range_logical_end logical_origin,
      storage_alignment alignment,
      storage_profile profile = storage_profile::v1) noexcept;
    [[nodiscard]] segment_context context() const noexcept { return context_; }
    [[nodiscard]] model::range_logical_end logical_origin() const noexcept {
        return logical_origin_;
    }
    [[nodiscard]] storage_alignment alignment() const noexcept {
        return alignment_;
    }
    [[nodiscard]] storage_profile profile() const noexcept { return profile_; }
    bool operator==(const segment_header&) const noexcept = default;

private:
    segment_header(
      segment_context context,
      model::range_logical_end origin,
      storage_alignment alignment,
      storage_profile profile) noexcept
      : context_(context)
      , logical_origin_(origin)
      , alignment_(alignment)
      , profile_(profile) {}
    segment_context context_;
    model::range_logical_end logical_origin_;
    storage_alignment alignment_;
    storage_profile profile_;
};

struct decoded_segment_header final {
    segment_header value;
    // Header starts at file position zero. This end is the data-start boundary,
    // including the actual accepted envelope extensions and counted padding.
    model::file_byte_span bytes;
};

// Supplied independently of wire fields. data_start comes from the complete
// segment header. Original expectations never stand in for current placement.
struct segment_block_expectation final {
    segment_write_context location;
    runtime::file_position data_start;
    model::batch_decode_expectation batch;
    storage_profile profile{storage_profile::v1};
};

// Minted only after complete block/child/padding validation. A descriptor alone
// cannot authenticate another buffer or establish fdatasync/ACK completion.
class complete_block_descriptor final {
public:
    [[nodiscard]] segment_context context() const noexcept { return context_; }
    [[nodiscard]] storage::coverage coverage() const noexcept {
        return storage::coverage{
          batch_.context.logical_span(), physical_, bytes_};
    }
    [[nodiscard]] assigned_batch_info batch() const noexcept { return batch_; }
    bool operator==(const complete_block_descriptor&) const noexcept = default;

private:
    friend class detail::segment_codec;
    complete_block_descriptor(
      segment_context context,
      model::segment_relative_span physical,
      model::file_byte_span bytes,
      assigned_batch_info batch) noexcept
      : context_(context)
      , physical_(physical)
      , bytes_(bytes)
      , batch_(batch) {}
    segment_context context_;
    model::segment_relative_span physical_;
    model::file_byte_span bytes_;
    assigned_batch_info batch_;
};

// Immutable owning pair: these exact outer bytes produced this descriptor.
// No constructor can attach a descriptor to caller-selected replacement bytes.
class segment_block final {
public:
    segment_block(const segment_block&) = delete;
    segment_block& operator=(const segment_block&) = delete;
    segment_block(segment_block&&) noexcept = default;
    segment_block& operator=(segment_block&&) noexcept = default;
    [[nodiscard]] complete_block_descriptor descriptor() const noexcept {
        return descriptor_;
    }
    [[nodiscard]] const kwaque::bytes::fragmented_buffer&
    bytes() const& noexcept {
        return bytes_;
    }
    const kwaque::bytes::fragmented_buffer& bytes() const&& = delete;
    [[nodiscard]] kwaque::bytes::fragmented_buffer release_bytes() && noexcept {
        return std::exchange(bytes_, kwaque::bytes::fragmented_buffer{});
    }

private:
    friend class detail::segment_codec;
    segment_block(
      kwaque::bytes::fragmented_buffer bytes,
      complete_block_descriptor descriptor) noexcept
      : bytes_(std::move(bytes))
      , descriptor_(descriptor) {}
    kwaque::bytes::fragmented_buffer bytes_;
    complete_block_descriptor descriptor_;
};

struct decoded_segment_block final {
    segment_block value;
    codec::decode_budget remaining;
};

// Pure initial-append check, separate from relocation/format acceptance.
// Requires dense original data and matching original/current topic, range,
// segment and generation. It neither grants a lease nor performs an append.
[[nodiscard]] result<void> validate_initial_append(
  const encoded_assigned_batch& batch,
  const segment_write_context& current) noexcept;

// Current writer uses an extension-free header. parent_remaining excludes
// other live/native/frame owners and includes all new encoded staging.
[[nodiscard]] seastar::future<codec::result<kwaque::bytes::fragmented_buffer>>
encode_segment_header(
  segment_header header,
  codec::cooperative_work& work,
  byte_count parent_remaining,
  kwaque::bytes::allocation_charge_fn charge,
  codec::field_context context = {});

// expected is independently pinned, including logical origin and profile.
// The caller supplies the actual file position; only zero is valid for a
// header. Reserve the input once before entry. Shared work/abort/input remain
// alive, unmoved and exclusive until joined completion. Failure restores
// cursor/marks.
[[nodiscard]] seastar::future<codec::result<decoded_segment_header>>
decode_segment_header(
  kwaque::bytes::fragmented_buffer_parser& input,
  segment_header expected,
  runtime::file_position position,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context = {},
  codec::input_boundary boundary = codec::input_boundary::open);

// Consumes the child before the first await, including rejection/abort; outer
// frame allocation failure precedes transfer. parent_remaining includes this
// child's backing/metadata and new fixed/padding/envelope staging. Other live
// owners and verified native/frame costs are excluded. Exact child bytes pass
// through unchanged; narrower semantic policy revalidates without re-encoding.
[[nodiscard]] seastar::future<codec::result<segment_block>>
encode_segment_block(
  encoded_assigned_batch&& child,
  segment_block_expectation expected,
  codec::cooperative_work& work,
  byte_count parent_remaining,
  kwaque::bytes::allocation_charge_fn charge,
  codec::field_context context = {});

// Validates one object speculatively through the public envelope/model codecs,
// then admits/retains its exact immutable bytes and advances without
// suspension. Uses two free parent checkpoint slots; temporary inner parsers
// have their own marks. memory excludes the input's already reserved
// backing/metadata and all other live/native/frame costs. remaining charges the
// returned alias metadata; keep the original backing reservation until all
// aliases die. No partial descriptor/bytes escape, including after inner
// success during cleanup.
[[nodiscard]] seastar::future<codec::result<decoded_segment_block>>
decode_segment_block(
  kwaque::bytes::fragmented_buffer_parser& input,
  segment_block_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context = {},
  codec::input_boundary boundary = codec::input_boundary::open);
} // namespace kwaque::storage
