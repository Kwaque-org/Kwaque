#ifndef KWAQUE_SRC_OBSERVABILITY_EVENT_LOG_H_
#define KWAQUE_SRC_OBSERVABILITY_EVENT_LOG_H_

#include "src/base/allocation.h"
#include "src/observability/event.h"
#include "src/observability/event_codec.h"
#include "src/observability/event_sequence.h"
#include "src/runtime/error.h"

#include <seastar/core/future.hh>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <iterator>
#include <memory>
#include <span>
#include <vector>

namespace kwaque::observability {

inline constexpr std::size_t canonical_event_log_header_encoded_size{60};
inline constexpr std::size_t canonical_event_log_record_prefix_size{2};
inline constexpr std::uint32_t synchronous_event_log_entries_max{1'024};
inline constexpr std::uint64_t synchronous_event_log_encoded_bytes_max{
  maximum_contiguous_allocation_bytes};
inline constexpr std::uint32_t cooperative_event_log_entries_per_yield_max{128};
inline constexpr std::uint32_t event_log_entries_absolute{4'194'304};
inline constexpr std::uint64_t event_log_encoded_bytes_absolute{
  std::uint64_t{1'073'741'824}};

class event_log_artifact final {
public:
    event_log_artifact() = default;
    explicit event_log_artifact(std::uint64_t expected_size) noexcept
      : expected_size_(expected_size) {}
    event_log_artifact(event_log_artifact&&) noexcept = default;
    event_log_artifact& operator=(event_log_artifact&&) noexcept = default;
    event_log_artifact(const event_log_artifact&) = delete;
    event_log_artifact& operator=(const event_log_artifact&) = delete;

    [[nodiscard]] runtime::result<void>
    append(std::span<const std::uint8_t> bytes);
    [[nodiscard]] runtime::result<void> push_back(std::uint8_t byte);

    [[nodiscard]] bool empty() const noexcept { return size_ == 0; }
    [[nodiscard]] std::uint64_t size() const noexcept { return size_; }
    [[nodiscard]] std::uint8_t back() const noexcept {
        return chunks_.back().back();
    }
    [[nodiscard]] bool copy_to(
      std::uint64_t offset, std::span<std::uint8_t> destination) const noexcept;
    [[nodiscard]] runtime::result<std::vector<std::uint8_t>> to_vector() const;
    [[nodiscard]] const std::deque<std::vector<std::uint8_t>>&
    chunks() const noexcept {
        return chunks_;
    }

private:
    std::deque<std::vector<std::uint8_t>> chunks_;
    std::uint64_t size_{0};
    std::uint64_t expected_size_{0};
};

struct event_log_limit_values final {
    std::uint32_t entries{4'096};
    std::uint64_t encoded_bytes{4U * 1'024U * 1'024U};

    bool operator==(const event_log_limit_values&) const = default;
};

class event_log_limits final {
public:
    static constexpr std::uint32_t entries_absolute{event_log_entries_absolute};
    static constexpr std::uint64_t encoded_bytes_absolute{
      event_log_encoded_bytes_absolute};

    [[nodiscard]] static runtime::result<event_log_limits>
    make(event_log_limit_values values) noexcept;
    [[nodiscard]] static constexpr event_log_limits defaults() noexcept {
        return event_log_limits{event_log_limit_values{}};
    }

    [[nodiscard]] constexpr std::uint32_t entries() const noexcept {
        return values_.entries;
    }
    [[nodiscard]] constexpr std::uint64_t encoded_bytes() const noexcept {
        return values_.encoded_bytes;
    }

    bool operator==(const event_log_limits&) const = default;

private:
    constexpr explicit event_log_limits(event_log_limit_values values) noexcept
      : values_(values) {}

    event_log_limit_values values_;
};

class event_entry_log final {
public:
    class const_iterator final {
    public:
        using difference_type = std::ptrdiff_t;
        using value_type = event;
        using reference = const event&;
        using pointer = const event*;
        using iterator_category = std::forward_iterator_tag;

        const_iterator() noexcept = default;

        [[nodiscard]] reference operator*() const noexcept {
            return (*owner_)[index_];
        }
        [[nodiscard]] pointer operator->() const noexcept {
            return &(*owner_)[index_];
        }
        const_iterator& operator++() noexcept {
            ++index_;
            return *this;
        }
        const_iterator operator++(int) noexcept {
            auto previous = *this;
            ++*this;
            return previous;
        }

        bool operator==(const const_iterator&) const = default;

    private:
        friend class event_entry_log;

        const_iterator(const event_entry_log& owner, std::size_t index) noexcept
          : owner_(&owner)
          , index_(index) {}

        const event_entry_log* owner_{nullptr};
        std::size_t index_{0};
    };

    explicit event_entry_log(std::size_t capacity);
    event_entry_log(const event_entry_log&) = delete;
    event_entry_log& operator=(const event_entry_log&) = delete;
    event_entry_log(event_entry_log&&) = delete;
    event_entry_log& operator=(event_entry_log&&) = delete;

    [[nodiscard]] bool empty() const noexcept { return size_ == 0; }
    [[nodiscard]] std::size_t size() const noexcept { return size_; }
    [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }
    [[nodiscard]] const event& operator[](std::size_t index) const noexcept;
    [[nodiscard]] const_iterator begin() const noexcept {
        return const_iterator{*this, 0};
    }
    [[nodiscard]] const_iterator end() const noexcept {
        return const_iterator{*this, size_};
    }

private:
    friend class event_log;

    static constexpr std::size_t entries_per_chunk = std::max<std::size_t>(
      1, maximum_contiguous_allocation_bytes / sizeof(event));

    void append(const event& value) noexcept;

    std::deque<std::vector<event>> chunks_;
    std::size_t size_{0};
    std::size_t capacity_{0};
};

class event_log final {
public:
    event_log(event_sink_identity identity, event_log_limits limits);
    event_log(const event_log&) = delete;
    event_log& operator=(const event_log&) = delete;
    event_log(event_log&&) = delete;
    event_log& operator=(event_log&&) = delete;

    [[nodiscard]] runtime::result<void> append(const event& value) noexcept;

    [[nodiscard]] runtime::result<event_log_artifact> encode() const;
    [[nodiscard]] seastar::future<runtime::result<event_log_artifact>>
    encode_cooperatively(
      std::uint32_t entries_per_yield
      = cooperative_event_log_entries_per_yield_max) const;
    [[nodiscard]] static runtime::result<std::unique_ptr<event_log>>
    decode(const event_log_artifact& encoded, event_log_limits parser_limits);
    [[nodiscard]] static seastar::future<
      runtime::result<std::unique_ptr<event_log>>>
    decode_cooperatively(
      event_log_artifact encoded,
      event_log_limits parser_limits,
      std::uint32_t entries_per_yield
      = cooperative_event_log_entries_per_yield_max);
    [[nodiscard]] static runtime::result<std::unique_ptr<event_log>> decode(
      std::span<const std::uint8_t> encoded, event_log_limits parser_limits);

    [[nodiscard]] const event_log_limits& limits() const noexcept {
        return limits_;
    }
    [[nodiscard]] const event_sink_identity& identity() const noexcept {
        return identity_;
    }
    [[nodiscard]] const event_entry_log& entries() const noexcept {
        return entries_;
    }
    [[nodiscard]] std::uint64_t encoded_bytes() const noexcept {
        return encoded_bytes_;
    }

private:
    event_sink_identity identity_;
    event_log_limits limits_;
    event_entry_log entries_;
    std::uint64_t encoded_bytes_{canonical_event_log_header_encoded_size};
};

} // namespace kwaque::observability

#endif // KWAQUE_SRC_OBSERVABILITY_EVENT_LOG_H_
