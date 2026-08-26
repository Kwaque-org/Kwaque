#include "src/bytes/fragmented_buffer_parser.h"

#include "src/base/error.h"

#include <algorithm>
#include <cstring>
#include <utility>

namespace kwaque::bytes {

fragmented_buffer_parser::fragmented_buffer_parser(
  fragmented_buffer buffer) noexcept
  : buffer_(std::move(buffer)) {
    normalize();
}

fragmented_buffer_parser::fragmented_buffer_parser(
  fragmented_buffer_parser&& other) noexcept
  : buffer_(std::move(other.buffer_))
  , at_(std::exchange(other.at_, position{}))
  , cache_begin_(std::exchange(other.cache_begin_, nullptr))
  , cache_end_(std::exchange(other.cache_end_, nullptr))
  , checkpoints_(other.checkpoints_)
  , checkpoint_depth_(std::exchange(other.checkpoint_depth_, 0)) {}

fragmented_buffer_parser&
fragmented_buffer_parser::operator=(fragmented_buffer_parser&& other) noexcept {
    if (this != &other) {
        buffer_ = std::move(other.buffer_);
        at_ = std::exchange(other.at_, position{});
        cache_begin_ = std::exchange(other.cache_begin_, nullptr);
        cache_end_ = std::exchange(other.cache_end_, nullptr);
        checkpoints_ = other.checkpoints_;
        checkpoint_depth_ = std::exchange(other.checkpoint_depth_, 0);
    }
    return *this;
}

byte_count fragmented_buffer_parser::total_bytes() const noexcept {
    return buffer_.size();
}

byte_count fragmented_buffer_parser::bytes_remaining() const noexcept {
    const auto remaining = buffer_.size().checked_sub(at_.consumed);
    return remaining ? *remaining : byte_count{};
}

void fragmented_buffer_parser::refresh_cache() noexcept {
    cache_begin_ = nullptr;
    cache_end_ = nullptr;
    if (at_.fragment >= buffer_.fragment_count()) {
        return;
    }
    const auto fragment = buffer_.fragment_at(at_.fragment);
    if (!fragment) {
        return;
    }
    cache_begin_ = fragment->data();
    cache_end_ = fragment->data() + fragment->size();
}

// Keeps the cursor off the end of a fragment so peek_current_fragment() and
// copy_out() never start from an exhausted position, and leaves the cached
// bounds pointing at whichever fragment the cursor ended up in.
void fragmented_buffer_parser::normalize() noexcept {
    cache_begin_ = nullptr;
    cache_end_ = nullptr;
    while (at_.fragment < buffer_.fragment_count()) {
        const auto fragment = buffer_.fragment_at(at_.fragment);
        if (!fragment) {
            return;
        }
        if (at_.offset < fragment->size()) {
            cache_begin_ = fragment->data();
            cache_end_ = fragment->data() + fragment->size();
            return;
        }
        at_.offset = 0;
        ++at_.fragment;
    }
}

result<void> fragmented_buffer_parser::advance(std::uint64_t bytes) {
    const auto consumed = at_.consumed.checked_add(byte_count{bytes});
    if (!consumed) {
        return failure(errc::malformed_data);
    }

    std::uint64_t remaining = bytes;
    while (remaining > 0) {
        if (cache_begin_ == nullptr) {
            return failure(errc::malformed_data);
        }
        const auto fragment_size = static_cast<std::uint64_t>(
          cache_end_ - cache_begin_);
        if (at_.offset >= fragment_size) {
            return failure(errc::malformed_data);
        }
        const auto available = fragment_size - at_.offset;
        const auto span = std::min(available, remaining);
        at_.offset += static_cast<std::size_t>(span);
        remaining -= span;
        if (at_.offset == fragment_size) {
            at_.offset = 0;
            ++at_.fragment;
            refresh_cache();
        }
    }
    at_.consumed = *consumed;
    return {};
}

result<void> fragmented_buffer_parser::copy_out(
  std::span<char> destination, const position& from) const {
    if (destination.size() > bytes_remaining().value()) {
        return failure(errc::truncated_data);
    }
    // Fast path: reading from the cursor, and the whole read lies inside the
    // fragment whose bounds are already cached.
    if (
      from == at_ && cache_begin_ != nullptr
      && static_cast<std::uint64_t>(cache_end_ - cache_begin_) - from.offset
           >= destination.size()) {
        std::memcpy(
          destination.data(), cache_begin_ + from.offset, destination.size());
        return {};
    }
    std::size_t written = 0;
    auto cursor = from;
    while (written < destination.size()) {
        if (cursor.fragment >= buffer_.fragment_count()) {
            return failure(errc::malformed_data);
        }
        const auto fragment = buffer_.fragment_at(cursor.fragment);
        if (!fragment) {
            return failure(errc::malformed_data);
        }
        if (cursor.offset >= fragment->size()) {
            cursor.offset = 0;
            ++cursor.fragment;
            continue;
        }
        const auto available = fragment->size() - cursor.offset;
        const auto span = std::min(available, destination.size() - written);
        std::memcpy(
          destination.data() + written, fragment->data() + cursor.offset, span);
        written += span;
        cursor.offset += span;
    }
    return {};
}

result<void> fragmented_buffer_parser::skip(byte_count bytes) {
    if (bytes > bytes_remaining()) {
        return failure(errc::out_of_range);
    }
    if (bytes.value() == 0) {
        return {};
    }
    return advance(bytes.value());
}

result<void>
fragmented_buffer_parser::peek_to(std::span<char> destination) const {
    if (destination.empty()) {
        return {};
    }
    return copy_out(destination, at_);
}

result<void> fragmented_buffer_parser::read_to(std::span<char> destination) {
    if (destination.empty()) {
        return {};
    }
    // Copy before advancing so a truncated read leaves the cursor untouched.
    if (auto copied = copy_out(destination, at_); !copied) {
        return copied;
    }
    return advance(static_cast<std::uint64_t>(destination.size()));
}

result<fragmented_buffer>
fragmented_buffer_parser::peek_buffer(byte_count bytes) {
    if (bytes > bytes_remaining()) {
        return failure(errc::truncated_data);
    }
    if (bytes.value() == 0) {
        return fragmented_buffer{};
    }
    return buffer_.share(at_.consumed, bytes);
}

result<fragmented_buffer>
fragmented_buffer_parser::read_buffer(byte_count bytes) {
    auto shared = peek_buffer(bytes);
    if (!shared) {
        return shared;
    }
    if (bytes.value() == 0) {
        return shared;
    }
    if (auto advanced = advance(bytes.value()); !advanced) {
        return failure(advanced.error());
    }
    return shared;
}

fragment_view fragmented_buffer_parser::peek_current_fragment() const noexcept {
    if (cache_begin_ == nullptr) {
        return fragment_view{};
    }
    const auto* position = cache_begin_ + at_.offset;
    if (position >= cache_end_) {
        return fragment_view{};
    }
    return fragment_view{
      position, static_cast<std::size_t>(cache_end_ - position)};
}

result<std::uint8_t> fragmented_buffer_parser::read_u8() {
    std::array<char, 1> raw{};
    if (auto read = read_to(raw); !read) {
        return failure(read.error());
    }
    return static_cast<std::uint8_t>(static_cast<unsigned char>(raw[0]));
}

result<void> fragmented_buffer_parser::push_checkpoint() {
    if (checkpoint_depth_ == max_parser_checkpoints) {
        return failure(errc::resource_exhausted);
    }
    checkpoints_[checkpoint_depth_] = at_;
    ++checkpoint_depth_;
    return {};
}

result<void> fragmented_buffer_parser::rollback() {
    if (checkpoint_depth_ == 0) {
        return failure(errc::invalid_argument);
    }
    --checkpoint_depth_;
    at_ = checkpoints_[checkpoint_depth_];
    refresh_cache();
    return {};
}

result<void> fragmented_buffer_parser::commit() {
    if (checkpoint_depth_ == 0) {
        return failure(errc::invalid_argument);
    }
    --checkpoint_depth_;
    return {};
}

} // namespace kwaque::bytes
