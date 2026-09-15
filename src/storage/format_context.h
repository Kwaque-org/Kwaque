#pragma once

#include "src/base/result.h"
#include "src/base/units.h"
#include "src/model/epoch.h"
#include "src/model/identity.h"
#include "src/model/position.h"
#include "src/runtime/file_position.h"

#include <cstdint>

namespace kwaque::storage {

enum class storage_profile : std::uint16_t { v1 = 1 };
enum class replay_profile : std::uint16_t { v1 = 1 };

// Zero is invalid; an unknown nonzero profile is unsupported. These identify
// formats only and do not establish device capabilities or replay authority.
[[nodiscard]] result<storage_profile>
parse_storage_profile(std::uint16_t value) noexcept;
[[nodiscard]] result<replay_profile>
parse_replay_profile(std::uint16_t value) noexcept;

// Encoded storage alignment, independently supplied by the caller. A valid
// value does not attest to the alignment requirements of any open file/device.
class storage_alignment final {
public:
    [[nodiscard]] static result<storage_alignment>
    make(byte_count bytes) noexcept;
    [[nodiscard]] constexpr byte_count bytes() const noexcept { return bytes_; }
    [[nodiscard]] constexpr bool
    aligned(runtime::file_position position) const noexcept {
        return position.value() % bytes_.value() == 0;
    }
    bool operator==(const storage_alignment&) const noexcept = default;

private:
    constexpr explicit storage_alignment(byte_count bytes) noexcept
      : bytes_(bytes) {}
    byte_count bytes_;
};

// Current immutable physical object/layout identity. The original producer
// binding stays on the batch; relocation cannot overwrite that binding with
// this context. IDs/generation are checked values, not allocations or leases.
class segment_context final {
public:
    [[nodiscard]] static result<segment_context> make(
      model::cluster_id cluster,
      model::topic_id topic,
      model::range_id range,
      model::segment_id segment,
      model::segment_generation generation) noexcept;

    [[nodiscard]] model::cluster_id cluster() const noexcept {
        return cluster_;
    }
    [[nodiscard]] model::topic_id topic() const noexcept { return topic_; }
    [[nodiscard]] model::range_id range() const noexcept { return range_; }
    [[nodiscard]] model::segment_id segment() const noexcept {
        return segment_;
    }
    [[nodiscard]] model::segment_generation generation() const noexcept {
        return generation_;
    }

    // expected comes from independently pinned object context. Comparison
    // establishes identity equality only, not content integrity or authority.
    [[nodiscard]] result<void>
    validate_expected(const segment_context& expected) const noexcept;
    bool operator==(const segment_context&) const noexcept = default;

private:
    segment_context(
      model::cluster_id cluster,
      model::topic_id topic,
      model::range_id range,
      model::segment_id segment,
      model::segment_generation generation) noexcept
      : cluster_(cluster)
      , topic_(topic)
      , range_(range)
      , segment_(segment)
      , generation_(generation) {}
    model::cluster_id cluster_;
    model::topic_id topic_;
    model::range_id range_;
    model::segment_id segment_;
    model::segment_generation generation_;
};

// Expected location of a segment object. physical_begin is a retained-record
// boundary, including a representable terminal boundary; a later nonempty
// append must separately check its physical end. No write is performed here.
class segment_write_context final {
public:
    [[nodiscard]] static result<segment_write_context> make(
      segment_context segment,
      storage_alignment alignment,
      model::segment_relative_end physical_begin,
      runtime::file_position position) noexcept;
    [[nodiscard]] segment_context segment() const noexcept { return segment_; }
    [[nodiscard]] storage_alignment alignment() const noexcept {
        return alignment_;
    }
    [[nodiscard]] model::segment_relative_end physical_begin() const noexcept {
        return physical_begin_;
    }
    [[nodiscard]] runtime::file_position position() const noexcept {
        return position_;
    }
    [[nodiscard]] result<void>
    validate_expected(const segment_write_context& expected) const noexcept;
    bool operator==(const segment_write_context&) const noexcept = default;

private:
    segment_write_context(
      segment_context segment,
      storage_alignment alignment,
      model::segment_relative_end physical_begin,
      runtime::file_position position) noexcept
      : segment_(segment)
      , alignment_(alignment)
      , physical_begin_(physical_begin)
      , position_(position) {}
    segment_context segment_;
    storage_alignment alignment_;
    model::segment_relative_end physical_begin_;
    runtime::file_position position_;
};

// WAL file lifetime and expected byte position. Reopening the same file keeps
// its incarnation; recycling needs a new ID supplied by the lifecycle owner.
// WAL alignment/position are independent of the target segment's context.
class wal_write_context final {
public:
    [[nodiscard]] static result<wal_write_context> make(
      model::wal_incarnation_id incarnation,
      storage_alignment alignment,
      runtime::file_position position) noexcept;
    [[nodiscard]] model::wal_incarnation_id incarnation() const noexcept {
        return incarnation_;
    }
    [[nodiscard]] storage_alignment alignment() const noexcept {
        return alignment_;
    }
    [[nodiscard]] runtime::file_position position() const noexcept {
        return position_;
    }
    [[nodiscard]] result<void>
    validate_expected(const wal_write_context& expected) const noexcept;
    bool operator==(const wal_write_context&) const noexcept = default;

private:
    wal_write_context(
      model::wal_incarnation_id incarnation,
      storage_alignment alignment,
      runtime::file_position position) noexcept
      : incarnation_(incarnation)
      , alignment_(alignment)
      , position_(position) {}
    model::wal_incarnation_id incarnation_;
    storage_alignment alignment_;
    runtime::file_position position_;
};

// Three already checked, independent half-open spans. Empty physical/byte
// coverage can coexist with nonempty original logical coverage. This value
// does not prove complete batches, retained content, or a durable boundary;
// enclosing formats validate those relationships against supplied evidence.
class coverage final {
public:
    explicit coverage(
      model::range_logical_span logical,
      model::segment_relative_span physical,
      model::file_byte_span bytes) noexcept
      : logical_(logical)
      , physical_(physical)
      , bytes_(bytes) {}
    [[nodiscard]] model::range_logical_span logical() const noexcept {
        return logical_;
    }
    [[nodiscard]] model::segment_relative_span physical() const noexcept {
        return physical_;
    }
    [[nodiscard]] model::file_byte_span bytes() const noexcept {
        return bytes_;
    }
    bool operator==(const coverage&) const noexcept = default;

private:
    model::range_logical_span logical_;
    model::segment_relative_span physical_;
    model::file_byte_span bytes_;
};

} // namespace kwaque::storage
