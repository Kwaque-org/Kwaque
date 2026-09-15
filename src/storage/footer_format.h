#pragma once

#include "src/codec/digest.h"
#include "src/storage/segment_format.h"

#include <optional>

namespace kwaque::storage {
class extent_verifier;
namespace detail {
class footer_codec;
}

inline constexpr byte_count durable_footer_fixed_bytes{192};

// Independently supplied file namespace, layout and original empty boundary.
// data_start is the checked end of the segment header, not a diagnostic origin.
struct segment_history_context final {
    segment_context segment;
    storage_alignment alignment;
    runtime::file_position data_start;
    model::range_logical_end logical_origin;
    model::segment_relative_end physical_origin;
    storage_profile profile{storage_profile::v1};
    bool operator==(const segment_history_context&) const noexcept = default;
};
struct footer_expectation final {
    segment_history_context history;
    runtime::file_position position;
};

struct boundary_fields final {
    storage::coverage coverage;
    std::uint32_t block_count;
    std::optional<storage::coverage> last_block;
    std::uint32_t data_crc32c;
    bool operator==(const boundary_fields&) const noexcept = default;
};

// Evidence for supplied complete objects and their exact bytes, minted only by
// the bounded verifier. A copied/mutated boundary_fields or bare CRC cannot
// construct this value. It establishes neither persistence nor producer ACK.
class verified_extent final {
public:
    [[nodiscard]] segment_history_context context() const noexcept {
        return context_;
    }
    [[nodiscard]] boundary_fields boundary() const noexcept {
        return boundary_;
    }
    // Present only after finishing a walk that requested exact-byte SHA.
    // Prefix checkpoints deliberately carry CRC evidence only.
    [[nodiscard]] std::optional<codec::extent_digest> digest() const noexcept {
        return digest_;
    }

private:
    friend class extent_verifier;
    verified_extent(
      segment_history_context context,
      boundary_fields boundary,
      std::optional<codec::extent_digest> digest = std::nullopt) noexcept
      : context_(context)
      , boundary_(boundary)
      , digest_(digest) {}
    segment_history_context context_;
    boundary_fields boundary_;
    std::optional<codec::extent_digest> digest_;
};

// Parsed representation only. Its referenced data history has not been proved
// by decoding this envelope; use validate_durable_footer with supplied
// evidence.
class durable_footer final {
public:
    [[nodiscard]] footer_expectation location() const noexcept {
        return location_;
    }
    [[nodiscard]] boundary_fields boundary() const noexcept {
        return boundary_;
    }
    [[nodiscard]] model::file_byte_span encoded_extent() const noexcept {
        return extent_;
    }

private:
    friend class detail::footer_codec;
    durable_footer(
      footer_expectation location,
      boundary_fields boundary,
      model::file_byte_span extent) noexcept
      : location_(location)
      , boundary_(boundary)
      , extent_(extent) {}
    footer_expectation location_;
    boundary_fields boundary_;
    model::file_byte_span extent_;
};

enum class footer_field : std::uint16_t {
    fixed_body = 224,
    cluster,
    topic,
    range,
    segment,
    generation,
    position,
    coverage,
    block_count,
    last_present,
    reserved,
    last_block,
    data_crc32c,
    padding,
};

// No bytes are read here: evidence already binds exact context, coverage,
// complete block count/last block and a CRC of all supplied stored bytes.
[[nodiscard]] codec::result<void> validate_durable_footer(
  const durable_footer& footer,
  const verified_extent& evidence,
  codec::field_context context = {});

// Encode from accumulated evidence rather than an unchecked count/last/CRC
// tuple. A footer may follow the named byte end; bytes outside that span are
// outside its claim. parent_remaining excludes all other live/native/frame
// reservations and includes new fixed/padding/envelope staging. Work/abort and
// the verified allocator profile stay alive and exclusive until joined return.
// Fixed scalar validation requires a 1024-byte / 64-item work quantum.
[[nodiscard]] seastar::future<codec::result<bytes::fragmented_buffer>>
encode_durable_footer(
  verified_extent evidence,
  footer_expectation expected,
  codec::cooperative_work& work,
  byte_count parent_remaining,
  bytes::allocation_charge_fn charge,
  codec::field_context context = {});

// One envelope transaction; integrity, independent context, structural bounds
// and counted zero padding precede publication. Reserve the input once before
// entry; memory excludes that backing/metadata and other live/native/frame
// costs. No new persistent allocation is returned. Input/work/abort remain
// alive, unmoved and exclusive; failure restores the original cursor/marks.
// Fixed scalar validation uses the same minimum work quantum as the writer.
[[nodiscard]] seastar::future<codec::result<durable_footer>>
decode_durable_footer(
  bytes::fragmented_buffer_parser& input,
  footer_expectation expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context = {},
  codec::input_boundary boundary = codec::input_boundary::open);
} // namespace kwaque::storage
