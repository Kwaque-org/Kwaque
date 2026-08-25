#include "src/base/invariant.h"

#if defined(KWAQUE_INVARIANT_TEST_OBSERVER) && KWAQUE_INVARIANT_TEST_OBSERVER
#include "src/base/invariant_test_observer.h"
#endif

#include <algorithm>
#include <array>
#include <charconv>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <system_error>

namespace kwaque {

namespace {

class diagnostic_buffer final {
public:
    void append(std::string_view value) noexcept {
        const auto copied = std::min(value.size(), remaining());
        std::copy_n(value.data(), copied, storage_.data() + size_);
        size_ += copied;
    }

    void append(char value) noexcept {
        if (remaining() != 0) {
            storage_[size_] = value;
            ++size_;
        }
    }

    void append_number(std::uint_least32_t value) noexcept {
        std::array<char, 16> digits{};
        const auto [end, error] = std::to_chars(
          digits.data(), digits.data() + digits.size(), value);
        if (error == std::errc{}) {
            append(
              std::string_view{
                digits.data(), static_cast<std::size_t>(end - digits.data())});
        }
    }

    void
    append_escaped(std::string_view value, std::size_t input_limit) noexcept {
        constexpr std::string_view hex_digits = "0123456789abcdef";
        const auto bounded_size = std::min(value.size(), input_limit);
        for (const char raw_character : value.substr(0, bounded_size)) {
            const auto character = static_cast<unsigned char>(raw_character);
            switch (character) {
            case '\\':
                append("\\\\");
                break;
            case '\n':
                append("\\n");
                break;
            case '\r':
                append("\\r");
                break;
            case '\t':
                append("\\t");
                break;
            default:
                if (character < 0x20 || character > 0x7e) {
                    append("\\x");
                    append(hex_digits[character >> 4]);
                    append(hex_digits[character & 0x0f]);
                } else {
                    append(static_cast<char>(character));
                }
                break;
            }
        }
        if (value.size() > input_limit) {
            append("<truncated>");
        }
    }

    [[nodiscard]] std::string_view view() const noexcept {
        return {storage_.data(), size_};
    }

private:
    [[nodiscard]] std::size_t remaining() const noexcept {
        return storage_.size() - size_;
    }

    std::array<char, max_invariant_diagnostic_size> storage_{};
    std::size_t size_{0};
};

std::string_view basename(std::string_view path) noexcept {
    const auto separator = path.find_last_of("/\\");
    return separator == std::string_view::npos ? path
                                               : path.substr(separator + 1);
}

diagnostic_buffer format_violation(
  invariant_id id,
  std::string_view expression,
  std::string_view context,
  const std::source_location& location) noexcept {
    diagnostic_buffer output;
    output.append("kwaque invariant violation id=");
    output.append_escaped(
      id.valid() ? id.value() : std::string_view{"INVALID"},
      invariant_id::max_size);
    output.append(" expression=");
    output.append_escaped(expression, max_invariant_expression_size);
    output.append(" context=");
    output.append_escaped(context, max_invariant_context_size);
    output.append(" source=");
    output.append_escaped(basename(location.file_name()), 96);
    output.append(':');
    output.append_number(location.line());
    return output;
}

#if defined(KWAQUE_INVARIANT_TEST_OBSERVER) && KWAQUE_INVARIANT_TEST_OBSERVER
thread_local testing::invariant_observer current_observer = nullptr;
#endif

} // namespace

[[noreturn]] void invariant_failed(
  invariant_id id,
  std::string_view expression,
  std::string_view context,
  std::source_location location) {
    const auto diagnostic = format_violation(id, expression, context, location);

#if defined(KWAQUE_INVARIANT_TEST_OBSERVER) && KWAQUE_INVARIANT_TEST_OBSERVER
    if (current_observer != nullptr) {
        current_observer(diagnostic.view());
    }
#endif

    static_cast<void>(std::fwrite(
      diagnostic.view().data(), 1, diagnostic.view().size(), stderr));
    static_cast<void>(std::fputc('\n', stderr));
    static_cast<void>(std::fflush(stderr));
    std::abort();
}

#if defined(KWAQUE_INVARIANT_TEST_OBSERVER) && KWAQUE_INVARIANT_TEST_OBSERVER
namespace testing {

scoped_invariant_observer::scoped_invariant_observer(
  invariant_observer observer) noexcept
  : previous_(current_observer) {
    current_observer = observer;
}

scoped_invariant_observer::~scoped_invariant_observer() {
    current_observer = previous_;
}

} // namespace testing
#endif

} // namespace kwaque
