#pragma once

#include <seastar/core/chunked_vector.hh>

#include <cstddef>
#include <iterator>
#include <ranges>
#include <type_traits>

namespace kwaque::storage {

// A borrowed table of `T`: one contiguous run, or the chunked table a large
// one is kept in, whose fragments stay within the contiguous allocation
// ceiling. Read by index or in order; what it borrows outlives it.
template<typename T>
class table_view final {
public:
    table_view() noexcept = default;
    table_view(const T* data, std::size_t size) noexcept
      : data_(data)
      , size_(size) {}
    template<std::ranges::contiguous_range Range>
    requires std::is_same_v<std::ranges::range_value_t<Range>, T>
               && (!std::is_same_v<std::remove_cvref_t<Range>, table_view>)
    // NOLINTNEXTLINE(google-explicit-constructor)
    table_view(const Range& range) noexcept
      : data_(std::ranges::data(range))
      , size_(std::ranges::size(range)) {}
    // NOLINTNEXTLINE(google-explicit-constructor)
    table_view(const seastar::chunked_vector<T>& table) noexcept
      : chunked_(&table)
      , size_(table.size()) {}

    [[nodiscard]] std::size_t size() const noexcept { return size_; }
    [[nodiscard]] bool empty() const noexcept { return size_ == 0; }
    [[nodiscard]] const T& operator[](std::size_t index) const noexcept {
        return chunked_ ? (*chunked_)[index] : data_[index];
    }

    class iterator final {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = T;
        using difference_type = std::ptrdiff_t;
        using pointer = const T*;
        using reference = const T&;

        iterator() noexcept = default;
        iterator(const table_view* table, std::size_t index) noexcept
          : table_(table)
          , index_(index) {}
        reference operator*() const noexcept { return (*table_)[index_]; }
        pointer operator->() const noexcept { return &(*table_)[index_]; }
        iterator& operator++() noexcept {
            ++index_;
            return *this;
        }
        iterator operator++(int) noexcept {
            auto before = *this;
            ++index_;
            return before;
        }
        bool operator==(const iterator& other) const noexcept {
            return index_ == other.index_;
        }

    private:
        const table_view* table_{nullptr};
        std::size_t index_{0};
    };
    [[nodiscard]] iterator begin() const noexcept { return {this, 0}; }
    [[nodiscard]] iterator end() const noexcept { return {this, size_}; }

private:
    const T* data_{nullptr};
    const seastar::chunked_vector<T>* chunked_{nullptr};
    std::size_t size_{0};
};

} // namespace kwaque::storage
