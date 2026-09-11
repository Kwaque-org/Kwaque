#pragma once

#include "src/base/error.h"
#include "src/base/result.h"

#include <boost/uuid/uuid.hpp>

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <utility>

namespace kwaque::model {

namespace detail {

// Default construction supplies nil staging. Checked construction copies an
// existing non-nil identity; it does not allocate a new object identity.
template<typename Tag>
class object_id final {
public:
    static constexpr std::size_t width = 16;
    using bytes_view = std::span<const std::uint8_t, width>;

    object_id() noexcept = default;

    [[nodiscard]] static result<object_id>
    make(std::span<const std::uint8_t> bytes) noexcept {
        if (bytes.size() != width) {
            return failure(errc::invalid_argument);
        }
        object_id value;
        std::copy(bytes.begin(), bytes.end(), value.value_.begin());
        if (value.is_nil()) {
            return failure(errc::invalid_argument);
        }
        return value;
    }

    [[nodiscard]] bool is_nil() const noexcept { return value_.is_nil(); }

    // The identity and any enclosing result/tuple must outlive the view.
    [[nodiscard]] bytes_view bytes() const& noexcept {
        return bytes_view{value_.begin(), width};
    }
    bytes_view bytes() && = delete;
    bytes_view bytes() const&& = delete;

    bool operator==(const object_id&) const noexcept = default;

    // Unsigned octet order for canonical collections, not creation or
    // causality.
    [[nodiscard]] bool canonical_less(const object_id& other) const noexcept {
        return value_ < other.value_;
    }

    template<typename H>
    friend H AbslHashValue(H state, const object_id& value) {
        return H::combine_contiguous(
          std::move(state), value.value_.begin(), width);
    }

private:
    static_assert(boost::uuids::uuid::static_size() == width);

    boost::uuids::uuid value_{};
};

struct cluster_id_tag;
struct broker_id_tag;
struct tenant_id_tag;
struct topic_id_tag;
struct range_id_tag;
struct segment_id_tag;
struct producer_id_tag;
struct control_transaction_id_tag;
struct manifest_id_tag;
struct wal_incarnation_id_tag;

} // namespace detail

// Registries own global uniqueness and non-reuse. An opaque ID never proves
// membership in the caller's cluster, tenant, topic or storage context.
// A cluster identity belongs to one cluster lifetime.
using cluster_id = detail::object_id<detail::cluster_id_tag>;
// Registrations belong to a cluster. Broker restart retains its registration;
// retirement and recreation require a new identity.
using broker_id = detail::object_id<detail::broker_id_tag>;
using tenant_id = detail::object_id<detail::tenant_id_tag>;
// Topic lifetime is tenant-scoped; a reused topic name is a different object.
using topic_id = detail::object_id<detail::topic_id_tag>;
// Ranges identify immutable topic-lineage nodes, not reusable keyspace
// intervals.
using range_id = detail::object_id<detail::range_id_tag>;
// Segments identify physical objects within a topic/range. Replica copies keep
// that identity; replacing an object does not.
using segment_id = detail::object_id<detail::segment_id_tag>;
// A durable producer identity survives its fenced producer incarnations.
using producer_id = detail::object_id<detail::producer_id_tag>;
// One cluster control transition; retries retain the same identity.
using control_transaction_id
  = detail::object_id<detail::control_transaction_id_tag>;
// One immutable range-manifest object, replaced rather than reinterpreted.
using manifest_id = detail::object_id<detail::manifest_id_tag>;
// One broker/shard WAL file lifetime; reopening keeps it, recycling replaces
// it.
using wal_incarnation_id = detail::object_id<detail::wal_incarnation_id_tag>;

// A numeric index into a supplied fixed cluster ring. The caller retains the
// cluster/ring context; scalar equality does not establish shared ownership.
class vnode_index final {
public:
    using rep = std::uint32_t;

    [[nodiscard]] static result<vnode_index> make(
      const cluster_id& cluster,
      std::uint64_t ring_size,
      std::uint64_t index) noexcept {
        if (
          cluster.is_nil() || ring_size == 0
          || ring_size > std::numeric_limits<rep>::max()
          || !std::has_single_bit(ring_size)) {
            return failure(errc::invalid_argument);
        }
        if (index >= ring_size) {
            return failure(errc::out_of_range);
        }
        return vnode_index{static_cast<rep>(index)};
    }

    [[nodiscard]] constexpr rep value() const noexcept { return value_; }

    bool operator==(const vnode_index&) const noexcept = default;

private:
    constexpr explicit vnode_index(rep value) noexcept
      : value_(value) {}

    rep value_;
};

} // namespace kwaque::model
