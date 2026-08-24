#include "src/runtime/shard_affinity.h"

#include <array>
#include <charconv>
#include <cstddef>
#include <string_view>
#include <system_error>

namespace kwaque::runtime {

namespace {

constexpr invariant_id wrong_shard_invariant{"KQ-WRONG-SHARD-ACCESS"};

class shard_context final {
public:
    shard_context(
      seastar::shard_id expected, seastar::shard_id current) noexcept {
        append("expected=");
        append_number(expected);
        append(" current=");
        append_number(current);
    }

    [[nodiscard]] std::string_view view() const noexcept {
        return {storage_.data(), size_};
    }

private:
    void append(std::string_view value) noexcept {
        for (const char character : value) {
            if (size_ == storage_.size()) {
                return;
            }
            storage_[size_] = character;
            ++size_;
        }
    }

    void append_number(seastar::shard_id value) noexcept {
        const auto [end, error] = std::to_chars(
          storage_.data() + size_, storage_.data() + storage_.size(), value);
        if (error == std::errc{}) {
            size_ = static_cast<std::size_t>(end - storage_.data());
        }
    }

    std::array<char, 64> storage_{};
    std::size_t size_{0};
};

} // namespace

owner_shard::owner_shard() noexcept
  : shard_(seastar::this_shard_id()) {}

bool owner_shard::is_current() const noexcept {
    return shard_ == seastar::this_shard_id();
}

void owner_shard::assert_current(std::source_location location) const {
    if (!is_current()) [[unlikely]] {
        const shard_context context{shard_, seastar::this_shard_id()};
        invariant_failed(
          wrong_shard_invariant,
          "this_shard_id() == owner_shard",
          context.view(),
          location);
    }
}

} // namespace kwaque::runtime
