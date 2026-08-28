#pragma once

#include "src/base/result.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <span>

namespace kwaque::bytes {

// Depth of the speculative-parse stack. Bounded so a malformed input cannot
// make a decoder retain unbounded rollback state.
inline constexpr std::size_t max_parser_checkpoints = 8;

// Parser primitives deliberately support only protocol-sized unsigned values.
// Excluding bool and implementation-specific extended integers keeps the wire
// width explicit and makes every supported operation covered by golden tests.
template<typename T>
concept fixed_width_unsigned_integer = std::same_as<T, std::uint8_t>
                                       || std::same_as<T, std::uint16_t>
                                       || std::same_as<T, std::uint32_t>
                                       || std::same_as<T, std::uint64_t>;

// A read-only bounded cursor over one owned buffer.
//
// The parser owns its fragments, so every sub-buffer it produces stays valid
// after the parser and the buffer it was constructed from are both gone. Pass a
// share() of a buffer to keep reading the original elsewhere.
//
// Failure leaves the cursor exactly where it was: a read either consumes all of
// its bytes or none of them.
class fragmented_buffer_parser final {
public:
    fragmented_buffer_parser() noexcept = default;
    explicit fragmented_buffer_parser(fragmented_buffer buffer) noexcept;
    // Moves reset the source. A memberwise move would leave the source's cached
    // bounds pointing into the fragments the destination now owns, so touching
    // the source after the destination died would read freed memory.
    fragmented_buffer_parser(fragmented_buffer_parser&& other) noexcept;
    fragmented_buffer_parser&
    operator=(fragmented_buffer_parser&& other) noexcept;
    fragmented_buffer_parser(const fragmented_buffer_parser&) = delete;
    fragmented_buffer_parser&
    operator=(const fragmented_buffer_parser&) = delete;
    ~fragmented_buffer_parser() = default;

    [[nodiscard]] byte_count total_bytes() const noexcept;
    [[nodiscard]] byte_count bytes_consumed() const noexcept {
        return at_.consumed;
    }
    [[nodiscard]] byte_count bytes_remaining() const noexcept;
    [[nodiscard]] bool at_end() const noexcept {
        return bytes_remaining().value() == 0;
    }
    [[nodiscard]] std::size_t checkpoint_depth() const noexcept {
        return checkpoint_depth_;
    }

    [[nodiscard]] result<void> skip(byte_count bytes);

    // Copies out without advancing.
    [[nodiscard]] result<void> peek_to(std::span<char> destination) const;
    // Copies out and advances.
    [[nodiscard]] result<void> read_to(std::span<char> destination);

    // Zero-copy owning slice of the next `bytes` bytes. read_buffer advances,
    // peek_buffer does not. Neither is const: taking a share converts fragment
    // ownership to a counted form.
    [[nodiscard]] result<fragmented_buffer> read_buffer(byte_count bytes);
    [[nodiscard]] result<fragmented_buffer> peek_buffer(byte_count bytes);

    // Borrowed contiguous view of the unread bytes of the current fragment.
    // Empty at the end of input. Valid only while this parser is alive.
    [[nodiscard]] fragment_view peek_current_fragment() const noexcept;

    // Fixed-width reads with an explicit byte order. No host-endian assumption
    // and no record-format policy: these are the primitives a later codec
    // composes.
    template<fixed_width_unsigned_integer T>
    [[nodiscard]] result<T> read_le() {
        std::array<char, sizeof(T)> raw{};
        if (auto read = read_to(raw); !read) {
            return failure(read.error());
        }
        T value{0};
        for (std::size_t index = 0; index < sizeof(T); ++index) {
            value = static_cast<T>(
              value
              | (static_cast<T>(static_cast<unsigned char>(raw[index])) << (8U * index)));
        }
        return value;
    }

    template<fixed_width_unsigned_integer T>
    [[nodiscard]] result<T> read_be() {
        std::array<char, sizeof(T)> raw{};
        if (auto read = read_to(raw); !read) {
            return failure(read.error());
        }
        T value{0};
        for (std::size_t index = 0; index < sizeof(T); ++index) {
            value = static_cast<T>(
              value
              | (static_cast<T>(static_cast<unsigned char>(raw[index])) << (8U * (sizeof(T) - 1 - index))));
        }
        return value;
    }

    // Speculative parsing. Every checkpoint must be resolved by exactly one
    // rollback or commit.
    [[nodiscard]] result<void> push_checkpoint();
    [[nodiscard]] result<void> rollback();
    [[nodiscard]] result<void> commit();

private:
    struct position final {
        std::size_t fragment{0};
        std::size_t offset{0};
        byte_count consumed{};

        bool operator==(const position&) const = default;
    };

    [[nodiscard]] result<void> advance(std::uint64_t bytes);
    [[nodiscard]] result<void>
    copy_out(std::span<char> destination, const position& from) const;
    void normalize() noexcept;
    // Reloads the cached bounds after the cursor moves to another fragment.
    void refresh_cache() noexcept;

    fragmented_buffer buffer_;
    position at_;
    // Bounds of the fragment the cursor sits in, reloaded only when it crosses
    // a boundary. A read that stays inside one fragment consults no accessor.
    const char* cache_begin_{nullptr};
    const char* cache_end_{nullptr};
    std::array<position, max_parser_checkpoints> checkpoints_{};
    std::size_t checkpoint_depth_{0};
};

} // namespace kwaque::bytes
