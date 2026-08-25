#pragma once

#include "src/base/invariant.h"

#include <seastar/core/shard_id.hh>

#include <source_location>

namespace kwaque::runtime {

// Identifies the reactor shard that owns mutable state. The value is safe to
// transport, but access to the owned object must still occur on this shard.
class owner_shard final {
public:
    owner_shard() noexcept;

    [[nodiscard]] seastar::shard_id value() const noexcept { return shard_; }
    [[nodiscard]] bool is_current() const noexcept;
    void assert_current(
      std::source_location location = std::source_location::current()) const;

    bool operator==(const owner_shard&) const = default;

private:
    seastar::shard_id shard_;
};

// Base for mutable objects whose complete lifetime belongs to one reactor
// shard. Derived objects must validate affinity at every public access seam.
class shard_affine {
public:
    shard_affine(const shard_affine&) = delete;
    shard_affine& operator=(const shard_affine&) = delete;
    shard_affine(shard_affine&&) = delete;
    shard_affine& operator=(shard_affine&&) = delete;

    [[nodiscard]] owner_shard owner() const noexcept { return owner_; }
    void assert_current(
      std::source_location location = std::source_location::current()) const {
        owner_.assert_current(location);
    }

protected:
    shard_affine() noexcept = default;
    ~shard_affine() = default;

private:
    owner_shard owner_;
};

} // namespace kwaque::runtime
