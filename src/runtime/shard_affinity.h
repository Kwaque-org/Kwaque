#pragma once

#include "src/base/invariant.h"
#include "src/runtime/shard_identity.h"

#include <seastar/core/shard_id.hh>

#include <source_location>

namespace kwaque::runtime {

namespace detail {

[[noreturn]] void fail_wrong_reactor(
  seastar::shard_id expected,
  seastar::shard_id current,
  std::source_location location);

} // namespace detail

// Identifies the reactor shard that owns mutable state. The value is safe to
// transport, but access to the owned object must still occur on this shard.
class owner_shard final {
public:
    owner_shard() noexcept;

    [[nodiscard]] seastar::shard_id value() const noexcept { return shard_; }
    [[nodiscard]] bool is_current() const noexcept {
        return shard_ == current_shard();
    }
    void assert_current(
      std::source_location location = std::source_location::current()) const {
        const auto current = current_shard();
        if (current != shard_) [[unlikely]] {
            fail_wrong_shard(shard_, current, location);
        }
    }

    bool operator==(const owner_shard&) const = default;

private:
    [[noreturn]] static void fail_wrong_shard(
      seastar::shard_id expected,
      seastar::shard_id current,
      std::source_location location);

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

// Base for objects that belong to the reactor thread as a whole rather than
// to one shard: the simulation's world (its scheduler, clock and fakes),
// which every virtual shard on that thread uses. In production every shard is
// a reactor, so it checks what shard_affine checks.
class reactor_affine {
public:
    reactor_affine(const reactor_affine&) = delete;
    reactor_affine& operator=(const reactor_affine&) = delete;
    reactor_affine(reactor_affine&&) = delete;
    reactor_affine& operator=(reactor_affine&&) = delete;

    void assert_current(
      std::source_location location = std::source_location::current()) const {
        const auto current = seastar::this_shard_id();
        if (current != reactor_) [[unlikely]] {
            detail::fail_wrong_reactor(reactor_, current, location);
        }
    }

    [[nodiscard]] seastar::shard_id reactor() const noexcept {
        return reactor_;
    }

protected:
    reactor_affine() noexcept
      : reactor_(seastar::this_shard_id()) {}
    ~reactor_affine() = default;

private:
    seastar::shard_id reactor_;
};
} // namespace kwaque::runtime
