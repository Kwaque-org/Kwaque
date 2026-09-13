#pragma once

#include "src/base/result.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/model/position.h"

#include <cstdint>
#include <optional>
#include <span>
#include <utility>
#include <vector>

namespace kwaque::codec {
class limits;
}

namespace kwaque::model {

namespace detail {
template<bool Scan>
class record_decoder;
}

// Relative record fields. The batch owner supplies the timestamp base and
// original slot count before these can identify a timestamp or logical record.
struct record_fields final {
    std::uint8_t attributes{0};
    std::int64_t timestamp_delta{0};
    range_logical_count logical_delta;

    bool operator==(const record_fields&) const noexcept = default;
};

// Checked construction lives in record_codec.h. Names are arbitrary nonnull
// bytes; an empty name is valid. A disengaged value differs from present-empty.
class record_header final {
public:
    record_header(const record_header&) = delete;
    record_header& operator=(const record_header&) = delete;
    record_header(record_header&&) noexcept = default;
    record_header& operator=(record_header&&) noexcept = default;
    ~record_header() = default;

    [[nodiscard]] const bytes::fragmented_buffer& name() const& noexcept {
        return name_;
    }
    const bytes::fragmented_buffer& name() const&& = delete;
    [[nodiscard]] const std::optional<bytes::fragmented_buffer>&
    value() const& noexcept {
        return value_;
    }
    const std::optional<bytes::fragmented_buffer>& value() const&& = delete;

    [[nodiscard]] bytes::fragmented_buffer release_name() && noexcept {
        return std::exchange(name_, bytes::fragmented_buffer{});
    }
    [[nodiscard]] std::optional<bytes::fragmented_buffer>
    release_value() && noexcept {
        return std::exchange(value_, std::nullopt);
    }

private:
    template<bool Scan>
    friend class detail::record_decoder;
    friend result<record_header> make_record_header(
      bytes::fragmented_buffer&&,
      std::optional<bytes::fragmented_buffer>&&,
      const codec::limits&);

    record_header(
      bytes::fragmented_buffer&& name,
      std::optional<bytes::fragmented_buffer>&& value) noexcept
      : name_(std::move(name))
      , value_(std::move(value)) {}

    bytes::fragmented_buffer name_;
    std::optional<bytes::fragmented_buffer> value_;
};

// An independently owned record, not a batch or an encoded record. Construction
// validates the record grammar's size limits without copying/sharing payloads.
// Nullable presence is stored once; encoded sizes are derived from these
// fields, never supplied independently or cached across moves.
//
// Access is read-only and shard-local. Existing aliases to donated metadata may
// not mutate it. Borrowed references end when their owning value is moved,
// replaced or destroyed. Moved-from values may be destroyed or reassigned;
// their original contents are no longer promised. Owners bound destruction of
// their payloads and any opaque deleters, including on move assignment.
// Rvalue extraction transfers one component without allocation; reservations
// follow the extracted owners until freed. The consumed component becomes
// null/empty and its previous borrowed references are no longer usable.
class record final {
public:
    record(const record&) = delete;
    record& operator=(const record&) = delete;
    record(record&&) noexcept = default;
    record& operator=(record&&) noexcept = default;
    ~record() = default;

    [[nodiscard]] record_fields fields() const noexcept { return fields_; }
    [[nodiscard]] std::uint8_t attributes() const noexcept {
        return fields_.attributes;
    }
    [[nodiscard]] std::int64_t timestamp_delta() const noexcept {
        return fields_.timestamp_delta;
    }
    [[nodiscard]] range_logical_count logical_delta() const noexcept {
        return fields_.logical_delta;
    }
    [[nodiscard]] bool has_key() const noexcept { return key_.has_value(); }
    [[nodiscard]] bool has_value() const noexcept { return value_.has_value(); }
    [[nodiscard]] bool is_tombstone() const noexcept { return !has_value(); }

    [[nodiscard]] const std::optional<bytes::fragmented_buffer>&
    key() const& noexcept {
        return key_;
    }
    const std::optional<bytes::fragmented_buffer>& key() const&& = delete;
    [[nodiscard]] const std::optional<bytes::fragmented_buffer>&
    value() const& noexcept {
        return value_;
    }
    const std::optional<bytes::fragmented_buffer>& value() const&& = delete;
    [[nodiscard]] std::span<const record_header> headers() const& noexcept {
        return headers_;
    }
    std::span<const record_header> headers() const&& = delete;

    [[nodiscard]] std::optional<bytes::fragmented_buffer>
    release_key() && noexcept {
        return std::exchange(key_, std::nullopt);
    }
    [[nodiscard]] std::optional<bytes::fragmented_buffer>
    release_value() && noexcept {
        return std::exchange(value_, std::nullopt);
    }
    [[nodiscard]] std::vector<record_header> release_headers() && noexcept {
        return std::exchange(headers_, std::vector<record_header>{});
    }

    // Allocated element capacity, including unused slots. The enclosing
    // record's own inline storage is accounted by its caller, not this vector.
    [[nodiscard]] item_count header_capacity() const noexcept {
        return item_count{headers_.capacity()};
    }

private:
    template<bool Scan>
    friend class detail::record_decoder;
    friend result<record> make_record(
      record_fields,
      std::optional<bytes::fragmented_buffer>&&,
      std::optional<bytes::fragmented_buffer>&&,
      std::vector<record_header>&&,
      const codec::limits&);

    record(
      record_fields fields,
      std::optional<bytes::fragmented_buffer>&& key,
      std::optional<bytes::fragmented_buffer>&& value,
      std::vector<record_header>&& headers) noexcept
      : fields_(fields)
      , key_(std::move(key))
      , value_(std::move(value))
      , headers_(std::move(headers)) {}

    record_fields fields_;
    std::optional<bytes::fragmented_buffer> key_;
    std::optional<bytes::fragmented_buffer> value_;
    std::vector<record_header> headers_;
};

} // namespace kwaque::model
