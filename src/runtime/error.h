#pragma once

#include "src/base/error.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace kwaque::runtime {

enum class operation_kind : std::uint8_t {
    generic = 0,
    timer = 1,
    random = 2,
    file = 3,
    network = 4,
    dns = 5,
    fault = 6,
    resource = 7,
};

enum class operation_context_key : std::uint8_t {
    shard = 0,
    peer = 1,
    attempt = 2,
    bytes = 3,
    items = 4,
    occurrence = 5,
    deadline_ns = 6,
    stable_id = 7,
};

struct operation_context_field final {
    operation_context_key key;
    std::uint64_t value;

    bool operator==(const operation_context_field&) const = default;
};

class operation_error final {
public:
    static constexpr std::size_t max_context_fields = 4;
    static constexpr std::size_t max_rendered_size = 256;

    explicit operation_error(
      std::error_code code,
      operation_kind operation = operation_kind::generic) noexcept;
    explicit operation_error(
      errc code, operation_kind operation = operation_kind::generic) noexcept;

    [[nodiscard]] std::error_code code() const noexcept { return code_; }
    [[nodiscard]] operation_kind operation() const noexcept {
        return operation_;
    }
    [[nodiscard]] std::span<const operation_context_field>
    context() const noexcept {
        return {context_.data(), context_size_};
    }

    [[nodiscard]] bool
    add_context(operation_context_key key, std::uint64_t value) noexcept;
    [[nodiscard]] std::string render() const;

    bool operator==(const operation_error&) const = default;

private:
    std::error_code code_;
    operation_kind operation_;
    std::array<operation_context_field, max_context_fields> context_{};
    std::size_t context_size_{0};
};

[[nodiscard]] std::string_view to_string(operation_kind operation) noexcept;
[[nodiscard]] std::string_view to_string(operation_context_key key) noexcept;

template<typename T>
using result = std::expected<T, operation_error>;

// Runtime operations report expected operational failures through result<T>.
// Exceptions remain available for programmer errors and dependency boundaries.

[[nodiscard]] inline std::unexpected<operation_error>
failure(operation_error error) noexcept {
    return std::unexpected(std::move(error));
}

} // namespace kwaque::runtime
