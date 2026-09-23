#pragma once

#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/cooperative.h"
#include "src/codec/integer.h"
#include "src/codec/transaction.h"
#include "src/runtime/time.h"
#include "src/storage/completed_retry.h"
#include "src/storage/local_metadata_layout.h"
#include "src/storage/local_types.h"

#include <optional>
#include <utility>
#include <variant>
#include <vector>

namespace kwaque::storage {

enum class local_device_role : std::uint16_t {
    data = 2,
    wal_control = 5,
    all = 7
};
enum class local_layout_kind : std::uint8_t { initial = 1, rewrite = 2 };
enum class local_object_state : std::uint8_t {
    active = 1,
    recovering = 2,
    sealed = 3,
    deleting = 4
};
enum class local_recovery_action : std::uint16_t {
    preserve = 1,
    reconstruct = 2,
    discard = 3
};
enum class local_checkpoint_disposition : std::uint16_t {
    segment_boundary = 1,
    preserved_candidate = 2,
    authorized_discard = 3
};
enum class local_deletion_reason : std::uint16_t {
    retention = 1,
    replaced_object = 2,
    explicit_owner_cleanup = 3
};
enum class local_deletion_kind : std::uint16_t {
    data = 1,
    descriptor = 2,
    published = 3,
    immutable_bundle = 4
};

class local_metadata_header final {
public:
    [[nodiscard]] static result<local_metadata_header> make(
      local_metadata_kind,
      local_store_context,
      local_publication_generation) noexcept;
    [[nodiscard]] local_metadata_kind kind() const noexcept { return kind_; }
    [[nodiscard]] local_store_context owner() const noexcept { return owner_; }
    [[nodiscard]] local_publication_generation generation() const noexcept {
        return generation_;
    }
    bool operator==(const local_metadata_header&) const = default;

private:
    local_metadata_header(
      local_metadata_kind k,
      local_store_context o,
      local_publication_generation g) noexcept
      : kind_(k)
      , owner_(o)
      , generation_(g) {}
    local_metadata_kind kind_;
    local_store_context owner_;
    local_publication_generation generation_;
};

// Staging values are not persistence/authority proofs. Encoding validates the
// complete representation before publishing bytes; decoded records are owning.
struct local_store_identity final {
    std::uint16_t layout_version;
    storage_alignment metadata_alignment;
    std::uint32_t shard_count;
    local_device_role role;
    bool operator==(const local_store_identity&) const = default;
};
struct local_wal_head final {
    model::wal_incarnation_id incarnation;
    codec::immutable_object_digest header_digest;
    bool operator==(const local_wal_head&) const = default;
};
struct local_shard_control final {
    local_wal_high wal_high;
    local_object_high object_high;
    local_decision_high decision_high;
    local_deletion_high deletion_high;
    std::optional<local_root_reference> checkpoint;
    std::optional<local_wal_head> wal_head;
    bool operator==(const local_shard_control&) const = default;
};
struct local_wal_descriptor final {
    model::wal_incarnation_id incarnation;
    std::optional<local_wal_cursor> predecessor;
    storage_alignment alignment;
    replay_profile profile;
    runtime::file_position data_start;
    byte_count capacity_bytes;
    bool operator==(const local_wal_descriptor&) const = default;
};
struct local_segment_descriptor final {
    segment_context segment;
    model::range_logical_end logical_origin;
    model::segment_relative_end physical_origin;
    storage_alignment alignment;
    storage_profile profile;
    std::uint16_t record_profile;
    local_layout_kind layout;
    byte_count maximum_data_bytes;
    runtime::monotonic_duration maximum_lifetime;
    bool operator==(const local_segment_descriptor&) const = default;
};
struct local_object_publication final {
    segment_context segment;
    local_object_state state;
    std::optional<local_footer_reference> boundary;
    std::vector<local_root_reference> roots;
    bool operator==(const local_object_publication&) const = default;
};
struct local_checkpoint_root final {
    local_wal_cursor begin, end;
    std::uint32_t entry_count;
    std::vector<page_ref> pages;
    bool operator==(const local_checkpoint_root&) const = default;
};
struct local_checkpoint_entry final {
    local_wal_cursor begin, end;
    segment_context segment;
    local_checkpoint_disposition disposition;
    std::uint64_t evidence_sequence;
    codec::immutable_object_digest evidence_digest;
    bool operator==(const local_checkpoint_entry&) const = default;
};
struct local_checkpoint_page final {
    local_object_sequence sequence;
    page_ordinal ordinal;
    std::uint32_t first_entry;
    std::vector<local_checkpoint_entry> entries;
    bool operator==(const local_checkpoint_page&) const = default;
};
struct local_deletion_object final {
    local_deletion_kind kind;
    std::uint64_t sequence;
    bool operator==(const local_deletion_object&) const = default;
};
struct local_deletion_intent final {
    segment_context segment;
    std::uint64_t owner_decision_id;
    local_publication_generation published_generation;
    local_deletion_reason reason;
    std::vector<local_deletion_object> objects;
    bool operator==(const local_deletion_intent&) const = default;
};
struct local_recovery_decision final {
    local_wal_cursor prepare;
    codec::immutable_object_digest prepare_digest;
    segment_context segment;
    runtime::file_position target_position;
    local_recovery_action action;
    std::uint64_t owner_decision_id;
    bool operator==(const local_recovery_decision&) const = default;
};
struct local_boundary_evidence final {
    segment_context segment;
    device_store_id data_device;
    local_footer_reference footer;
    coverage covered;
    std::optional<local_root_reference> retry;
    bool operator==(const local_boundary_evidence& other) const noexcept {
        return segment == other.segment && data_device == other.data_device
               && footer == other.footer
               && covered.logical() == other.covered.logical()
               && covered.physical() == other.covered.physical()
               && covered.bytes() == other.covered.bytes()
               && retry == other.retry;
    }
};
struct local_completed_retry_root final {
    segment_context segment;
    local_footer_reference footer;
    std::uint32_t retry_count;
    std::vector<page_ref> pages;
    bool operator==(const local_completed_retry_root&) const = default;
};
struct local_completed_retry_page final {
    segment_context segment;
    local_object_sequence sequence;
    page_ordinal ordinal;
    std::uint32_t first_entry;
    std::vector<completed_retry> entries;
    bool operator==(const local_completed_retry_page&) const = default;
};
using local_metadata_payload = std::variant<
  local_store_identity,
  local_shard_control,
  local_wal_descriptor,
  local_segment_descriptor,
  local_object_publication,
  local_checkpoint_root,
  local_checkpoint_page,
  local_deletion_intent,
  local_recovery_decision,
  local_boundary_evidence,
  local_completed_retry_root,
  local_completed_retry_page>;

// Expected context comes from configuration/path/publication, never copied
// from the input being authenticated. Pinned roots/pages/evidence and WAL
// headers require digest+encoded length; pages additionally require PageRef.
// SC-bearing records require segment context and independent data alignment.
struct local_metadata_expectation final {
    local_metadata_header header;
    storage_alignment alignment;
    std::optional<segment_context> segment;
    std::optional<storage_alignment> segment_alignment;
    std::optional<model::wal_incarnation_id> wal_incarnation;
    std::optional<device_store_id> data_device;
    // Required when boundary evidence references another device.
    std::optional<storage_alignment> data_metadata_alignment;
    std::optional<codec::immutable_object_digest> digest;
    std::optional<byte_count> encoded_bytes;
    std::optional<page_ref> page;
    std::optional<model::batch_id> previous_retry;
    std::optional<local_wal_cursor> previous_checkpoint_end;
};
namespace detail {
class local_metadata_codec;
}
class local_metadata_record final {
public:
    local_metadata_record(local_metadata_record&&) noexcept = default;
    local_metadata_record&
    operator=(local_metadata_record&&) noexcept = default;
    local_metadata_record(const local_metadata_record&) = delete;
    local_metadata_record& operator=(const local_metadata_record&) = delete;
    [[nodiscard]] local_metadata_header header() const noexcept {
        return header_;
    }
    [[nodiscard]] const local_metadata_payload& payload() const& noexcept {
        return payload_;
    }
    const local_metadata_payload& payload() const&& = delete;
    [[nodiscard]] byte_count encoded_bytes() const noexcept {
        return encoded_bytes_;
    }

private:
    friend class detail::local_metadata_codec;
    local_metadata_record(
      local_metadata_header h,
      local_metadata_payload&& p,
      byte_count b) noexcept
      : header_(h)
      , payload_(std::move(p))
      , encoded_bytes_(b) {}
    local_metadata_header header_;
    local_metadata_payload payload_;
    byte_count encoded_bytes_;
};
struct decoded_local_metadata final {
    local_metadata_record value;
    codec::decode_budget remaining;
};
struct encoded_local_metadata final {
    bytes::fragmented_buffer bytes;
    codec::immutable_object_digest digest;
};
// Integrity-checked routing claims only. This cannot substitute for an expected
// publication pin or authorize a store, repair, deletion or reader admission.
struct local_metadata_claims final {
    local_metadata_header header;
    std::optional<segment_context> segment;
    std::optional<model::wal_incarnation_id> wal_incarnation;
    byte_count encoded_bytes;
    // WAL geometry comes from its checked descriptor; other kinds use the
    // supplied metadata alignment. This is a layout claim, not device proof.
    storage_alignment alignment;
};

struct local_metadata_encoding final {
    local_metadata_header header;
    storage_alignment alignment;
    // Required for SC-bearing records; independent of metadata alignment.
    std::optional<storage_alignment> segment_alignment;
    // Boundary evidence can reside on a different metadata device.
    std::optional<storage_alignment> data_metadata_alignment;
};

// Inputs/work stay alive, unmoved and exclusive until joined completion.
// remaining excludes their backing and all caller/native/control costs.
// Writers emit version 1 with an
// extension-free 32-byte header; readers retain compatible envelope semantics.
[[nodiscard]] seastar::future<codec::result<encoded_local_metadata>>
encode_local_metadata(
  local_metadata_encoding,
  const local_metadata_payload&,
  codec::cooperative_work&,
  byte_count remaining,
  bytes::allocation_charge_fn,
  codec::field_context = {});
// Reserve parser input once. Failure restores cursor/marks; residuals charge
// only returned owning metadata. Pin verification needs two free parent marks.
// Consumes one envelope. File owners separately require exact EOF or the
// expected following bundle/WAL extents; decoding is not persistence proof.
[[nodiscard]] seastar::future<codec::result<decoded_local_metadata>>
decode_local_metadata(
  bytes::fragmented_buffer_parser&,
  local_metadata_expectation,
  codec::decode_budget,
  codec::cooperative_work&,
  codec::field_context = {},
  codec::input_boundary = codec::input_boundary::open);
// Load the current mutable control selected by its fixed final pathname under
// exclusive namespace ownership. Owner/scope are supplied independently; the
// positive generation is learned from this selected record, not guessed from
// siblings or presented as an independently expected generation. No pin or
// recovery/append authority is minted by this operation.
[[nodiscard]] seastar::future<codec::result<decoded_local_metadata>>
decode_selected_shard_control(
  bytes::fragmented_buffer_parser&,
  local_store_context,
  storage_alignment,
  codec::decode_budget,
  codec::cooperative_work&,
  codec::field_context = {},
  codec::input_boundary = codec::input_boundary::open);
// The fixed published pathname selects its positive mutable generation under
// exclusive namespace ownership. SC/owner/alignment are supplied independently.
[[nodiscard]] seastar::future<codec::result<decoded_local_metadata>>
decode_selected_object_publication(
  bytes::fragmented_buffer_parser&,
  local_store_context,
  segment_context,
  storage_alignment metadata_alignment,
  storage_alignment data_alignment,
  codec::decode_budget,
  codec::cooperative_work&,
  codec::field_context = {},
  codec::input_boundary = codec::input_boundary::open);
// The independently selected head supplies its exact header digest; a
// predecessor cursor supplies identity/position but no SHA. Neither stores a
// separate alignment: learn it from the integrity-checked descriptor and verify
// its complete layout. Owner/incarnation remain independent expectations.
// No PREPARE coverage, device compatibility or checkpoint proof is established.
[[nodiscard]] seastar::future<codec::result<decoded_local_metadata>>
decode_local_wal_descriptor(
  bytes::fragmented_buffer_parser&,
  local_store_context,
  model::wal_incarnation_id,
  std::optional<codec::immutable_object_digest>,
  codec::decode_budget,
  codec::cooperative_work&,
  codec::field_context = {},
  codec::input_boundary = codec::input_boundary::open);
// Uses the same bounded body/scalar/integrity checks, discards all decoded
// arrays, and returns no contextual validation or persistence authority. The
// supplied alignment applies to metadata; WAL descriptors carry their own.
[[nodiscard]] seastar::future<codec::result<local_metadata_claims>>
probe_local_metadata(
  bytes::fragmented_buffer_parser&,
  storage_alignment,
  codec::decode_budget,
  codec::cooperative_work&,
  codec::field_context = {},
  codec::input_boundary = codec::input_boundary::open);
} // namespace kwaque::storage
