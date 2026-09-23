#pragma once

#include "src/codec/digest.h"
#include "src/model/epoch.h"
#include "src/model/identity.h"
#include "src/runtime/file_position.h"
#include "src/storage/format_context.h"
#include "src/storage/page_ref.h"

#include <array>
#include <compare>
#include <cstdint>
#include <span>

namespace kwaque::storage {
namespace detail {
struct device_store_id_tag;
struct local_object_sequence_tag;
struct local_decision_sequence_tag;
struct local_deletion_sequence_tag;
struct local_publication_generation_tag;
} // namespace detail
using device_store_id = model::detail::object_id<detail::device_store_id_tag>;
using local_object_sequence
  = model::detail::positive_counter<detail::local_object_sequence_tag>;
using local_decision_sequence
  = model::detail::positive_counter<detail::local_decision_sequence_tag>;
using local_deletion_sequence
  = model::detail::positive_counter<detail::local_deletion_sequence_tag>;
using local_publication_generation
  = model::detail::positive_counter<detail::local_publication_generation_tag>;

// A high mark is not a usable ID. Zero is the initial unreserved state;
// checked advancement calculates a value, without reserving or persisting it.
template<typename Sequence>
class local_allocation_high final {
public:
    explicit constexpr local_allocation_high(std::uint64_t value = 0) noexcept
      : value_(value) {}
    [[nodiscard]] constexpr std::uint64_t value() const noexcept {
        return value_;
    }
    [[nodiscard]] result<local_allocation_high>
    checked_advance(std::uint64_t count) const noexcept {
        if (count == 0) return failure(errc::invalid_argument);
        if (count > UINT64_MAX - value_) return failure(errc::out_of_range);
        return local_allocation_high{value_ + count};
    }
    [[nodiscard]] result<Sequence> sequence() const noexcept {
        return Sequence::make(value_);
    }
    auto operator<=>(const local_allocation_high&) const = default;

private:
    std::uint64_t value_;
};
using local_object_high = local_allocation_high<local_object_sequence>;
using local_decision_high = local_allocation_high<local_decision_sequence>;
using local_deletion_high = local_allocation_high<local_deletion_sequence>;

class local_wal_high final {
public:
    local_wal_high() noexcept = default;
    [[nodiscard]] static result<local_wal_high>
      make(std::span<const std::uint8_t>) noexcept;
    [[nodiscard]] static result<local_wal_high>
      from_incarnation(model::wal_incarnation_id) noexcept;
    [[nodiscard]] result<local_wal_high>
    checked_advance(std::uint64_t count) const noexcept;
    [[nodiscard]] result<model::wal_incarnation_id>
    incarnation() const noexcept;
    [[nodiscard]] bool empty() const noexcept;
    // The value, including any enclosing result, outlives this view.
    [[nodiscard]] std::span<const std::uint8_t, 16> bytes() const& noexcept {
        return bytes_;
    }
    std::span<const std::uint8_t, 16> bytes() && = delete;
    std::span<const std::uint8_t, 16> bytes() const&& = delete;
    auto operator<=>(const local_wal_high&) const = default;

private:
    std::array<std::uint8_t, 16> bytes_{};
};

inline constexpr std::uint32_t local_store_shard = UINT32_MAX;
// Configuration/path ownership supplies this context. The store sentinel is
// legal only for a store-identity record, not an ordinary shard record.
class local_store_context final {
public:
    [[nodiscard]] static result<local_store_context> make(
      model::cluster_id,
      model::broker_id,
      device_store_id,
      std::uint32_t shard) noexcept;
    [[nodiscard]] model::cluster_id cluster() const noexcept {
        return cluster_;
    }
    [[nodiscard]] model::broker_id broker() const noexcept { return broker_; }
    [[nodiscard]] device_store_id device() const noexcept { return device_; }
    [[nodiscard]] std::uint32_t shard() const noexcept { return shard_; }
    [[nodiscard]] bool store_wide() const noexcept {
        return shard_ == local_store_shard;
    }
    [[nodiscard]] result<void>
    validate_expected(const local_store_context&) const noexcept;
    bool operator==(const local_store_context&) const = default;

private:
    local_store_context(
      model::cluster_id c,
      model::broker_id b,
      device_store_id d,
      std::uint32_t s) noexcept
      : cluster_(c)
      , broker_(b)
      , device_(d)
      , shard_(s) {}
    model::cluster_id cluster_;
    model::broker_id broker_;
    device_store_id device_;
    std::uint32_t shard_;
};

// A wire cursor carries no owner. Its enclosing record supplies the store;
// comparisons require both independently selected owner contexts.
// The minimum supported alignment is checked; the actual file alignment and
// membership in its complete prefix still require the independently opened WAL.
class local_wal_cursor final {
public:
    [[nodiscard]] static result<local_wal_cursor>
      make(model::wal_incarnation_id, runtime::file_position) noexcept;
    [[nodiscard]] model::wal_incarnation_id incarnation() const noexcept {
        return incarnation_;
    }
    [[nodiscard]] runtime::file_position position() const noexcept {
        return position_;
    }
    [[nodiscard]] result<std::strong_ordering> compare(
      const local_store_context& owner,
      const local_wal_cursor& other,
      const local_store_context& other_owner) const noexcept;
    bool operator==(const local_wal_cursor&) const = default;

private:
    local_wal_cursor(
      model::wal_incarnation_id id, runtime::file_position p) noexcept
      : incarnation_(id)
      , position_(p) {}
    model::wal_incarnation_id incarnation_;
    runtime::file_position position_;
};

enum class local_root_kind : std::uint16_t {
    index = 1,
    sealed_retry = 2,
    checkpoint = 3,
    manifest = 4,
    completed_retry_snapshot = 5
};
[[nodiscard]] result<local_root_kind>
parse_local_root_kind(std::uint16_t) noexcept;

class local_root_reference final {
public:
    [[nodiscard]] static result<local_root_reference> make(
      local_root_kind,
      local_object_sequence,
      runtime::file_position,
      byte_count,
      page_count,
      codec::immutable_object_digest) noexcept;
    [[nodiscard]] local_root_kind kind() const noexcept { return kind_; }
    [[nodiscard]] local_object_sequence sequence() const noexcept {
        return sequence_;
    }
    [[nodiscard]] runtime::file_position position() const noexcept {
        return position_;
    }
    [[nodiscard]] byte_count bytes() const noexcept { return bytes_; }
    [[nodiscard]] page_count pages() const noexcept { return pages_; }
    [[nodiscard]] codec::immutable_object_digest digest() const noexcept {
        return digest_;
    }
    [[nodiscard]] result<void>
      validate_alignment(storage_alignment) const noexcept;
    bool operator==(const local_root_reference&) const = default;

private:
    local_root_reference(
      local_root_kind k,
      local_object_sequence s,
      runtime::file_position p,
      byte_count b,
      page_count n,
      codec::immutable_object_digest d) noexcept
      : kind_(k)
      , sequence_(s)
      , position_(p)
      , bytes_(b)
      , pages_(n)
      , digest_(d) {}
    local_root_kind kind_;
    local_object_sequence sequence_;
    runtime::file_position position_;
    byte_count bytes_;
    page_count pages_;
    codec::immutable_object_digest digest_;
};

// The expected family is part of the immutable reference, independent of a
// segment's later mutable state. This does not attest to covered data bytes.
class local_footer_reference final {
public:
    [[nodiscard]] static result<local_footer_reference> make(
      runtime::file_position,
      byte_count,
      std::uint16_t family,
      codec::immutable_object_digest) noexcept;
    [[nodiscard]] runtime::file_position position() const noexcept {
        return position_;
    }
    [[nodiscard]] byte_count bytes() const noexcept { return bytes_; }
    [[nodiscard]] std::uint16_t family() const noexcept { return family_; }
    [[nodiscard]] codec::immutable_object_digest digest() const noexcept {
        return digest_;
    }
    [[nodiscard]] result<void>
      validate_alignment(storage_alignment) const noexcept;
    bool operator==(const local_footer_reference&) const = default;

private:
    local_footer_reference(
      runtime::file_position p,
      byte_count b,
      std::uint16_t f,
      codec::immutable_object_digest d) noexcept
      : position_(p)
      , bytes_(b)
      , family_(f)
      , digest_(d) {}
    runtime::file_position position_;
    byte_count bytes_;
    std::uint16_t family_;
    codec::immutable_object_digest digest_;
};
} // namespace kwaque::storage
