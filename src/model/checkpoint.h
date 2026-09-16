#pragma once

#include "src/base/result.h"
#include "src/base/units.h"
#include "src/model/identity.h"
#include "src/model/position.h"

#include <cstddef>
#include <span>
#include <utility>
#include <vector>

namespace kwaque::model {
namespace detail {
class checkpoint_builder;
}

// The next logical boundary in one range. Zero and UINT64_MAX are ordinary
// boundaries, including holes; neither encodes absence or a completion flag.
class range_cursor final {
public:
    [[nodiscard]] static result<range_cursor>
    make(range_id range, range_logical_end next) noexcept {
        if (range.is_nil()) return failure(errc::invalid_argument);
        return range_cursor{range, next};
    }

    [[nodiscard]] range_id range() const noexcept { return range_; }
    [[nodiscard]] range_logical_end next() const noexcept { return next_; }
    bool operator==(const range_cursor&) const noexcept = default;

private:
    range_cursor(range_id range, range_logical_end next) noexcept
      : range_(range)
      , next_(next) {}

    range_id range_;
    range_logical_end next_;
};

// Key order only. Equal range identities are duplicates regardless of offset.
struct range_cursor_less final {
    bool operator()(
      const range_cursor& left, const range_cursor& right) const noexcept {
        return left.range().canonical_less(right.range());
    }
};

// Checked construction lives in checkpoint_codec.h. This immutable owner is
// topic-scoped, nonempty and sorted by unsigned range-identity octets. Its
// structural validity proves neither topic membership nor consumer progress.
// Storage is shard-local. Views end on move, replacement or destruction;
// moved-from owners may only be destroyed or reassigned. Scalar entries have
// no hidden allocations. Equality compares at most maximum_cursors entries;
// it does not order checkpoints by progress.
class read_checkpoint final {
public:
    static constexpr std::size_t maximum_cursors = 4096;

    read_checkpoint(const read_checkpoint&) = delete;
    read_checkpoint& operator=(const read_checkpoint&) = delete;
    read_checkpoint(read_checkpoint&&) noexcept = default;
    read_checkpoint& operator=(read_checkpoint&&) noexcept = default;
    ~read_checkpoint() = default;

    [[nodiscard]] topic_id topic() const noexcept { return topic_; }
    [[nodiscard]] std::span<const range_cursor> cursors() const& noexcept {
        return cursors_;
    }
    std::span<const range_cursor> cursors() const&& = delete;
    [[nodiscard]] item_count cursor_capacity() const noexcept {
        return item_count{cursors_.capacity()};
    }
    bool operator==(const read_checkpoint&) const noexcept = default;

private:
    friend class detail::checkpoint_builder;
    read_checkpoint(
      topic_id topic, std::vector<range_cursor>&& cursors) noexcept
      : topic_(topic)
      , cursors_(std::move(cursors)) {}

    topic_id topic_;
    std::vector<range_cursor> cursors_;
};

} // namespace kwaque::model
