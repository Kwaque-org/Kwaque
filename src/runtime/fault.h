#ifndef KWAQUE_SRC_RUNTIME_FAULT_H_
#define KWAQUE_SRC_RUNTIME_FAULT_H_

#include "src/base/units.h"
#include "src/runtime/error.h"
#include "src/runtime/time.h"

#include <array>
#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <string_view>

namespace kwaque::runtime {

inline constexpr std::size_t maximum_fault_object_key_bytes = 32;
inline constexpr std::size_t maximum_fault_points = 256;

class fault_point_id final {
public:
    template<std::uint32_t Value>
    [[nodiscard]] static consteval fault_point_id constant() noexcept {
        static_assert(Value != 0, "fault point ID zero is reserved");
        return fault_point_id{Value};
    }

    [[nodiscard]] static result<fault_point_id>
    make(std::uint32_t value) noexcept;

    [[nodiscard]] constexpr std::uint32_t value() const noexcept {
        return value_;
    }

    auto operator<=>(const fault_point_id&) const = default;

private:
    constexpr explicit fault_point_id(std::uint32_t value) noexcept
      : value_(value) {}

    std::uint32_t value_;
};

class fault_occurrence final {
public:
    [[nodiscard]] static result<fault_occurrence>
    make(std::uint64_t value) noexcept;
    [[nodiscard]] static constexpr fault_occurrence first() noexcept {
        return fault_occurrence{1};
    }

    [[nodiscard]] constexpr std::uint64_t value() const noexcept {
        return value_;
    }
    [[nodiscard]] constexpr std::optional<fault_occurrence>
    checked_next() const noexcept {
        if (value_ == std::numeric_limits<std::uint64_t>::max()) {
            return std::nullopt;
        }
        return fault_occurrence{value_ + 1};
    }

    auto operator<=>(const fault_occurrence&) const = default;

private:
    constexpr explicit fault_occurrence(std::uint64_t value) noexcept
      : value_(value) {}

    std::uint64_t value_;
};

class fault_object_key final {
public:
    constexpr fault_object_key() noexcept = default;

    [[nodiscard]] static result<fault_object_key>
    from_bytes(std::span<const std::byte> bytes) noexcept;
    [[nodiscard]] static constexpr fault_object_key
    from_u64(std::uint64_t value) noexcept {
        fault_object_key key;
        key.size_ = static_cast<std::uint8_t>(sizeof(value));
        for (std::size_t index = 0; index < sizeof(value); ++index) {
            key.storage_[index] = static_cast<std::byte>(value & 0xffU);
            value >>= 8U;
        }
        return key;
    }
    [[nodiscard]] static constexpr fault_object_key none() noexcept {
        return fault_object_key{};
    }

    [[nodiscard]] constexpr std::span<const std::byte> bytes() const noexcept {
        return {storage_.data(), size_};
    }
    [[nodiscard]] constexpr bool empty() const noexcept { return size_ == 0; }

    auto operator<=>(const fault_object_key&) const = default;

private:
    std::array<std::byte, maximum_fault_object_key_bytes> storage_{};
    std::uint8_t size_{0};
};

enum class fault_action : std::uint8_t {
    none,
    error,
    delay,
    short_operation,
    drop,
    duplicate,
    reorder,
    disconnect,
    corrupt,
    misdirect,
    torn_write,
    drop_completion,
    crash,
    partial_resize = 13,
};

class fault_action_set final {
public:
    constexpr fault_action_set() noexcept = default;

    template<fault_action... Actions>
    [[nodiscard]] static consteval fault_action_set constant() noexcept {
        static_assert(
          ((static_cast<std::uint8_t>(Actions)
            <= static_cast<std::uint8_t>(fault_action::partial_resize))
           && ...),
          "unknown fault action");
        fault_action_set result;
        (result.add(Actions), ...);
        return result;
    }

    [[nodiscard]] constexpr bool contains(fault_action action) const noexcept {
        if (action == fault_action::none) {
            return true;
        }
        const auto index = static_cast<std::uint8_t>(action);
        return index < 64U && (bits_ & (std::uint64_t{1} << index)) != 0;
    }

    bool operator==(const fault_action_set&) const = default;

private:
    constexpr void add(fault_action action) noexcept {
        if (action != fault_action::none) {
            bits_ |= std::uint64_t{1} << static_cast<std::uint8_t>(action);
        }
    }

    std::uint64_t bits_{0};
};

enum class builtin_fault_point : std::uint8_t {
    timer,
    file,
    dns,
    connect,
    accept,
    file_read,
    file_write,
    network_read,
    network_write,
    close,
    file_open,
    file_exists,
    file_stat,
    file_list,
    directory_create,
    file_remove,
    directory_remove,
    file_rename,
    directory_sync,
    file_flush,
    file_truncate,
    file_size,
    file_close,
};

struct fault_point_descriptor final {
    builtin_fault_point point;
    fault_point_id id;
    std::string_view name;
    fault_action_set permitted_actions;

    bool operator==(const fault_point_descriptor&) const = default;
};

inline constexpr std::array builtin_fault_points{
  fault_point_descriptor{
    .point = builtin_fault_point::timer,
    .id = fault_point_id::constant<1>(),
    .name = "timer",
    .permitted_actions = fault_action_set::constant<
      fault_action::error,
      fault_action::delay,
      fault_action::drop_completion>()},
  fault_point_descriptor{
    .point = builtin_fault_point::file,
    .id = fault_point_id::constant<2>(),
    .name = "file",
    .permitted_actions = fault_action_set::constant<
      fault_action::error,
      fault_action::delay,
      fault_action::crash>()},
  fault_point_descriptor{
    .point = builtin_fault_point::dns,
    .id = fault_point_id::constant<3>(),
    .name = "dns",
    .permitted_actions = fault_action_set::constant<
      fault_action::error,
      fault_action::delay,
      fault_action::drop_completion>()},
  fault_point_descriptor{
    .point = builtin_fault_point::connect,
    .id = fault_point_id::constant<4>(),
    .name = "connect",
    .permitted_actions = fault_action_set::constant<
      fault_action::error,
      fault_action::delay,
      fault_action::disconnect,
      fault_action::drop_completion>()},
  fault_point_descriptor{
    .point = builtin_fault_point::accept,
    .id = fault_point_id::constant<5>(),
    .name = "accept",
    .permitted_actions = fault_action_set::constant<
      fault_action::error,
      fault_action::delay,
      fault_action::disconnect,
      fault_action::drop_completion>()},
  fault_point_descriptor{
    .point = builtin_fault_point::file_read,
    .id = fault_point_id::constant<6>(),
    .name = "file_read",
    .permitted_actions = fault_action_set::constant<
      fault_action::error,
      fault_action::delay,
      fault_action::short_operation,
      fault_action::corrupt,
      fault_action::misdirect,
      fault_action::drop_completion,
      fault_action::crash>()},
  fault_point_descriptor{
    .point = builtin_fault_point::file_write,
    .id = fault_point_id::constant<7>(),
    .name = "file_write",
    .permitted_actions = fault_action_set::constant<
      fault_action::error,
      fault_action::delay,
      fault_action::short_operation,
      fault_action::corrupt,
      fault_action::misdirect,
      fault_action::torn_write,
      fault_action::drop_completion,
      fault_action::crash>()},
  fault_point_descriptor{
    .point = builtin_fault_point::network_read,
    .id = fault_point_id::constant<8>(),
    .name = "network_read",
    .permitted_actions = fault_action_set::constant<
      fault_action::error,
      fault_action::delay,
      fault_action::short_operation,
      fault_action::drop,
      fault_action::disconnect,
      fault_action::corrupt,
      fault_action::drop_completion>()},
  fault_point_descriptor{
    .point = builtin_fault_point::network_write,
    .id = fault_point_id::constant<9>(),
    .name = "network_write",
    .permitted_actions = fault_action_set::constant<
      fault_action::error,
      fault_action::delay,
      fault_action::short_operation,
      fault_action::drop,
      fault_action::duplicate,
      fault_action::reorder,
      fault_action::disconnect,
      fault_action::corrupt,
      fault_action::drop_completion>()},
  fault_point_descriptor{
    .point = builtin_fault_point::close,
    .id = fault_point_id::constant<10>(),
    .name = "close",
    .permitted_actions = fault_action_set::constant<
      fault_action::error,
      fault_action::delay,
      fault_action::drop_completion>()},
  fault_point_descriptor{
    .point = builtin_fault_point::file_open,
    .id = fault_point_id::constant<11>(),
    .name = "file_open",
    .permitted_actions = fault_action_set::constant<
      fault_action::error,
      fault_action::delay,
      fault_action::drop_completion,
      fault_action::crash>()},
  fault_point_descriptor{
    .point = builtin_fault_point::file_exists,
    .id = fault_point_id::constant<12>(),
    .name = "file_exists",
    .permitted_actions = fault_action_set::constant<
      fault_action::error,
      fault_action::delay,
      fault_action::drop_completion,
      fault_action::crash>()},
  fault_point_descriptor{
    .point = builtin_fault_point::file_stat,
    .id = fault_point_id::constant<13>(),
    .name = "file_stat",
    .permitted_actions = fault_action_set::constant<
      fault_action::error,
      fault_action::delay,
      fault_action::drop_completion,
      fault_action::crash>()},
  fault_point_descriptor{
    .point = builtin_fault_point::file_list,
    .id = fault_point_id::constant<14>(),
    .name = "file_list",
    .permitted_actions = fault_action_set::constant<
      fault_action::error,
      fault_action::delay,
      fault_action::drop_completion,
      fault_action::crash>()},
  fault_point_descriptor{
    .point = builtin_fault_point::directory_create,
    .id = fault_point_id::constant<15>(),
    .name = "directory_create",
    .permitted_actions = fault_action_set::constant<
      fault_action::error,
      fault_action::delay,
      fault_action::drop_completion,
      fault_action::crash>()},
  fault_point_descriptor{
    .point = builtin_fault_point::file_remove,
    .id = fault_point_id::constant<16>(),
    .name = "file_remove",
    .permitted_actions = fault_action_set::constant<
      fault_action::error,
      fault_action::delay,
      fault_action::drop_completion,
      fault_action::crash>()},
  fault_point_descriptor{
    .point = builtin_fault_point::directory_remove,
    .id = fault_point_id::constant<17>(),
    .name = "directory_remove",
    .permitted_actions = fault_action_set::constant<
      fault_action::error,
      fault_action::delay,
      fault_action::drop_completion,
      fault_action::crash>()},
  fault_point_descriptor{
    .point = builtin_fault_point::file_rename,
    .id = fault_point_id::constant<18>(),
    .name = "file_rename",
    .permitted_actions = fault_action_set::constant<
      fault_action::error,
      fault_action::delay,
      fault_action::drop_completion,
      fault_action::crash>()},
  fault_point_descriptor{
    .point = builtin_fault_point::directory_sync,
    .id = fault_point_id::constant<19>(),
    .name = "directory_sync",
    .permitted_actions = fault_action_set::constant<
      fault_action::error,
      fault_action::delay,
      fault_action::drop_completion,
      fault_action::crash>()},
  fault_point_descriptor{
    .point = builtin_fault_point::file_flush,
    .id = fault_point_id::constant<20>(),
    .name = "file_flush",
    .permitted_actions = fault_action_set::constant<
      fault_action::error,
      fault_action::delay,
      fault_action::drop_completion,
      fault_action::crash>()},
  fault_point_descriptor{
    .point = builtin_fault_point::file_truncate,
    .id = fault_point_id::constant<21>(),
    .name = "file_truncate",
    .permitted_actions = fault_action_set::constant<
      fault_action::error,
      fault_action::delay,
      fault_action::drop_completion,
      fault_action::crash,
      fault_action::partial_resize>()},
  fault_point_descriptor{
    .point = builtin_fault_point::file_size,
    .id = fault_point_id::constant<22>(),
    .name = "file_size",
    .permitted_actions = fault_action_set::constant<
      fault_action::error,
      fault_action::delay,
      fault_action::drop_completion,
      fault_action::crash>()},
  fault_point_descriptor{
    .point = builtin_fault_point::file_close,
    .id = fault_point_id::constant<23>(),
    .name = "file_close",
    .permitted_actions = fault_action_set::constant<
      fault_action::delay,
      fault_action::drop_completion,
      fault_action::crash>()},
};

[[nodiscard]] consteval bool valid_builtin_fault_points() noexcept {
    for (std::size_t left = 0; left < builtin_fault_points.size(); ++left) {
        if (
          builtin_fault_points[left].point
            != static_cast<builtin_fault_point>(left)
          || builtin_fault_points[left].name.empty()
          || builtin_fault_points[left].name.size() > 32) {
            return false;
        }
        for (std::size_t right = left + 1; right < builtin_fault_points.size();
             ++right) {
            if (
              builtin_fault_points[left].id == builtin_fault_points[right].id
              || builtin_fault_points[left].name
                   == builtin_fault_points[right].name) {
                return false;
            }
        }
    }
    return true;
}

static_assert(valid_builtin_fault_points());

[[nodiscard]] constexpr const fault_point_descriptor*
descriptor_for(builtin_fault_point point) noexcept {
    const auto index = static_cast<std::size_t>(point);
    return index < builtin_fault_points.size() ? &builtin_fault_points[index]
                                               : nullptr;
}

[[nodiscard]] constexpr const fault_point_descriptor*
find_builtin_fault_point(fault_point_id id) noexcept {
    for (const auto& descriptor : builtin_fault_points) {
        if (descriptor.id == id) {
            return &descriptor;
        }
    }
    return nullptr;
}

struct fault_request final {
    // Decisions are bounded data. Injectors cannot retain or invoke component
    // callbacks through this request.
    fault_point_id point;
    fault_occurrence occurrence;
    fault_object_key object;

    bool operator==(const fault_request&) const = default;
};

class fault_decision final {
public:
    constexpr fault_decision() noexcept = default;

    [[nodiscard]] static constexpr fault_decision make_error() noexcept {
        return fault_decision{fault_action::error, 0};
    }
    [[nodiscard]] static constexpr fault_decision
    make_delay(monotonic_duration duration) noexcept {
        return fault_decision{fault_action::delay, duration.nanoseconds()};
    }
    [[nodiscard]] static constexpr fault_decision
    make_short_operation(byte_count bytes) noexcept {
        return fault_decision{fault_action::short_operation, bytes.value()};
    }
    [[nodiscard]] static constexpr fault_decision make_drop() noexcept {
        return fault_decision{fault_action::drop, 0};
    }
    [[nodiscard]] static constexpr fault_decision make_duplicate() noexcept {
        return fault_decision{fault_action::duplicate, 0};
    }
    [[nodiscard]] static constexpr fault_decision make_reorder() noexcept {
        return fault_decision{fault_action::reorder, 0};
    }
    [[nodiscard]] static constexpr fault_decision make_disconnect() noexcept {
        return fault_decision{fault_action::disconnect, 0};
    }
    [[nodiscard]] static constexpr fault_decision make_corrupt() noexcept {
        return fault_decision{fault_action::corrupt, 0};
    }
    [[nodiscard]] static constexpr fault_decision make_misdirect() noexcept {
        return fault_decision{fault_action::misdirect, 0};
    }
    [[nodiscard]] static constexpr fault_decision make_torn_write() noexcept {
        return fault_decision{fault_action::torn_write, 0};
    }
    [[nodiscard]] static constexpr fault_decision
    make_drop_completion() noexcept {
        return fault_decision{fault_action::drop_completion, 0};
    }
    [[nodiscard]] static constexpr fault_decision make_crash() noexcept {
        return fault_decision{fault_action::crash, 0};
    }
    [[nodiscard]] static constexpr fault_decision
    make_partial_resize() noexcept {
        return fault_decision{fault_action::partial_resize, 0};
    }

    [[nodiscard]] constexpr fault_action action() const noexcept {
        return action_;
    }
    [[nodiscard]] constexpr std::optional<monotonic_duration>
    delay() const noexcept {
        return action_ == fault_action::delay
                 ? std::optional<monotonic_duration>{monotonic_duration{
                     payload_}}
                 : std::nullopt;
    }
    [[nodiscard]] constexpr std::optional<byte_count>
    short_operation_bytes() const noexcept {
        return action_ == fault_action::short_operation
                 ? std::optional<byte_count>{byte_count{payload_}}
                 : std::nullopt;
    }

    bool operator==(const fault_decision&) const = default;

private:
    constexpr fault_decision(
      fault_action action, std::uint64_t payload) noexcept
      : payload_(payload)
      , action_(action) {}

    std::uint64_t payload_{0};
    fault_action action_{fault_action::none};
};

[[nodiscard]] result<const fault_point_descriptor*>
validate_fault_request(const fault_request& request) noexcept;
[[nodiscard]] result<void> validate_fault_decision(
  const fault_request& request, fault_decision decision) noexcept;
[[nodiscard]] result<void>
validate_unique_fault_points(std::span<const fault_point_id> points) noexcept;

template<typename Injector>
concept fault_injector = requires(
  Injector& injector, const fault_request& request) {
    {
        injector.evaluate(request)
    } noexcept -> std::same_as<result<fault_decision>>;
};

} // namespace kwaque::runtime

#endif // KWAQUE_SRC_RUNTIME_FAULT_H_
