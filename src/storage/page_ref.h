#pragma once

#include "src/base/error.h"
#include "src/base/result.h"
#include "src/base/units.h"
#include "src/codec/digest.h"

#include <cstdint>

namespace kwaque::storage {
inline constexpr std::uint32_t maximum_object_entries = 65536;
inline constexpr std::uint32_t maximum_object_pages = 256;
inline constexpr byte_count page_ref_wire_bytes{48};

class page_ordinal final {
public:
    [[nodiscard]] static result<page_ordinal>
    make(std::uint32_t value) noexcept {
        if (value >= maximum_object_pages)
            return failure(errc::resource_exhausted);
        return page_ordinal{value};
    }
    [[nodiscard]] std::uint32_t value() const noexcept { return value_; }
    bool operator==(const page_ordinal&) const noexcept = default;

private:
    explicit page_ordinal(std::uint32_t value) noexcept
      : value_(value) {}
    std::uint32_t value_;
};
class page_count final {
public:
    [[nodiscard]] static result<page_count> make(std::uint32_t value) noexcept {
        if (value > maximum_object_pages)
            return failure(errc::resource_exhausted);
        return page_count{value};
    }
    [[nodiscard]] std::uint32_t value() const noexcept { return value_; }
    bool operator==(const page_count&) const noexcept = default;

private:
    explicit page_count(std::uint32_t value) noexcept
      : value_(value) {}
    std::uint32_t value_;
};

// Context/family come from the containing independently pinned root. This
// value has no page address and does not prove that any page was supplied.
class page_ref final {
public:
    [[nodiscard]] static result<page_ref> make(
      page_ordinal ordinal,
      std::uint32_t first_entry,
      std::uint32_t entry_count,
      byte_count encoded_bytes,
      codec::immutable_object_digest digest) noexcept {
        if (entry_count == 0 || encoded_bytes.value() < 32)
            return failure(errc::invalid_argument);
        if (
          first_entry > maximum_object_entries
          || entry_count > maximum_object_entries - first_entry
          || encoded_bytes.value() > 65536)
            return failure(errc::resource_exhausted);
        return page_ref{
          ordinal, first_entry, entry_count, encoded_bytes, digest};
    }
    [[nodiscard]] page_ordinal ordinal() const noexcept { return ordinal_; }
    [[nodiscard]] std::uint32_t first_entry() const noexcept {
        return first_entry_;
    }
    [[nodiscard]] std::uint32_t entry_count() const noexcept {
        return entry_count_;
    }
    [[nodiscard]] byte_count encoded_bytes() const noexcept {
        return encoded_bytes_;
    }
    [[nodiscard]] codec::immutable_object_digest digest() const noexcept {
        return digest_;
    }
    bool operator==(const page_ref&) const noexcept = default;

private:
    page_ref(
      page_ordinal ordinal,
      std::uint32_t first,
      std::uint32_t count,
      byte_count bytes,
      codec::immutable_object_digest digest) noexcept
      : ordinal_(ordinal)
      , first_entry_(first)
      , entry_count_(count)
      , encoded_bytes_(bytes)
      , digest_(digest) {}
    page_ordinal ordinal_;
    std::uint32_t first_entry_;
    std::uint32_t entry_count_;
    byte_count encoded_bytes_;
    codec::immutable_object_digest digest_;
};
} // namespace kwaque::storage
