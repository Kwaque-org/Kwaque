#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/bytes/fragmented_buffer_parser.h"

#include <seastar/core/temporary_buffer.hh>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace {

using namespace kwaque;
using namespace kwaque::bytes;

constexpr std::size_t max_input_size = 4096;
constexpr std::size_t max_operations = 192;

void require(bool condition) {
    if (!condition) {
        __builtin_trap();
    }
}

// Pulls bounded operands out of the fuzzer input and reports exhaustion instead
// of wrapping, so a short input ends the operation script rather than repeating
// it.
class script final {
public:
    script(const std::uint8_t* data, std::size_t size) noexcept
      : data_(data)
      , size_(size) {}

    [[nodiscard]] bool exhausted() const noexcept { return cursor_ >= size_; }

    [[nodiscard]] std::uint8_t byte() noexcept {
        return cursor_ < size_ ? data_[cursor_++] : 0;
    }

    [[nodiscard]] std::size_t bounded(std::size_t limit) noexcept {
        return limit == 0 ? 0 : static_cast<std::size_t>(byte()) % (limit + 1);
    }

    [[nodiscard]] std::string bytes(std::size_t length) noexcept {
        std::string out;
        out.reserve(length);
        for (std::size_t index = 0; index < length; ++index) {
            out.push_back(static_cast<char>(byte()));
        }
        return out;
    }

private:
    const std::uint8_t* data_;
    std::size_t size_;
    std::size_t cursor_{0};
};

std::string contents(const fragmented_buffer& buffer) {
    std::string out;
    out.resize(buffer.size().value());
    const auto copied = buffer.copy_to(out);
    require(copied.has_value());
    require(copied->value() == buffer.size().value());
    return out;
}

// Drives the builder and keeps an exact contiguous oracle of what it should
// hold.
fragmented_buffer build(script& input, std::string& oracle) {
    fragmented_buffer_builder_config config;
    config.initial_fragment_bytes = byte_count{
      1 + static_cast<std::uint64_t>(input.bounded(63))};
    config.max_fragment_bytes = byte_count{
      config.initial_fragment_bytes.value()
      + static_cast<std::uint64_t>(input.bounded(255))};
    config.max_total_bytes = byte_count{max_input_size * 4};
    config.max_fragments = 1 + input.bounded(31);
    if (!config.validate()) {
        return fragmented_buffer{};
    }

    fragmented_buffer_builder builder{config};
    for (std::size_t operation = 0;
         operation < max_operations && !input.exhausted();
         ++operation) {
        const auto choice = input.byte() % 3;
        const auto length = input.bounded(48);
        if (choice == 0) {
            const auto payload = input.bytes(length);
            if (builder.append(payload)) {
                oracle.append(payload);
            }
        } else if (choice == 1) {
            const auto payload = input.bytes(length);
            seastar::temporary_buffer<char> fragment{payload.size()};
            if (!payload.empty()) {
                std::memcpy(
                  fragment.get_write(), payload.data(), payload.size());
            }
            if (builder.append_fragment(std::move(fragment))) {
                oracle.append(payload);
            }
        } else {
            static_cast<void>(
              builder.reserve(byte_count{static_cast<std::uint64_t>(length)}));
        }
        require(builder.size().value() == oracle.size());
        require(builder.fragment_count() <= config.max_fragments);
    }

    auto published = builder.finish();
    require(published.has_value());
    require(published->size().value() == oracle.size());
    require(published->fragment_count() <= max_buffer_fragments);
    return std::move(*published);
}

// Applies share and trim operations, checking each against the oracle.
void reshape(script& input, fragmented_buffer& buffer, std::string& oracle) {
    for (std::size_t operation = 0;
         operation < max_operations && !input.exhausted();
         ++operation) {
        const auto choice = input.byte() % 4;
        const auto size = static_cast<std::size_t>(buffer.size().value());
        if (choice == 0) {
            const auto amount = input.bounded(size + 1);
            const auto trimmed = buffer.trim_front(
              byte_count{static_cast<std::uint64_t>(amount)});
            if (amount > size) {
                require(!trimmed.has_value());
            } else {
                require(trimmed.has_value());
                oracle.erase(0, amount);
            }
        } else if (choice == 1) {
            const auto amount = input.bounded(size + 1);
            const auto trimmed = buffer.trim_back(
              byte_count{static_cast<std::uint64_t>(amount)});
            if (amount > size) {
                require(!trimmed.has_value());
            } else {
                require(trimmed.has_value());
                oracle.erase(oracle.size() - amount);
            }
        } else if (choice == 2) {
            const auto offset = input.bounded(size);
            const auto length = input.bounded(size - offset);
            auto shared = buffer.share(
              byte_count{static_cast<std::uint64_t>(offset)},
              byte_count{static_cast<std::uint64_t>(length)});
            require(shared.has_value());
            require(contents(*shared) == oracle.substr(offset, length));
            // The slice must stay correct after the source is reshaped.
            const auto expected = contents(*shared);
            require(buffer.trim_front(byte_count{0}).has_value());
            require(contents(*shared) == expected);
        } else {
            auto shared = buffer.share();
            require(shared.has_value());
            require(contents(*shared) == oracle);
            require(shared->size() == buffer.size());
        }
        require(buffer.size().value() == oracle.size());
        require(contents(buffer) == oracle);
        for (const auto fragment : buffer) {
            require(fragment.size() > 0);
        }
    }
}

// Reads the buffer back through the parser and compares every result with the
// oracle, including failures at and past the end.
void parse(
  script& input, fragmented_buffer& buffer, const std::string& oracle) {
    auto shared = buffer.share();
    require(shared.has_value());
    fragmented_buffer_parser parser{std::move(*shared)};
    std::size_t consumed = 0;

    for (std::size_t operation = 0;
         operation < max_operations && !input.exhausted();
         ++operation) {
        const auto remaining = oracle.size() - consumed;
        require(parser.bytes_remaining().value() == remaining);
        require(parser.bytes_consumed().value() == consumed);

        const auto choice = input.byte() % 5;
        const auto request = input.bounded(remaining + 1);
        if (choice == 0) {
            const auto skipped = parser.skip(
              byte_count{static_cast<std::uint64_t>(request)});
            if (request > remaining) {
                require(!skipped.has_value());
            } else {
                require(skipped.has_value());
                consumed += request;
            }
        } else if (choice == 1) {
            std::vector<char> out(request);
            const auto read = parser.read_to(out);
            if (request > remaining) {
                require(!read.has_value());
            } else {
                require(read.has_value());
                require(
                  std::string(out.begin(), out.end())
                  == oracle.substr(consumed, request));
                consumed += request;
            }
        } else if (choice == 2) {
            auto taken = parser.read_buffer(
              byte_count{static_cast<std::uint64_t>(request)});
            if (request > remaining) {
                require(!taken.has_value());
            } else {
                require(taken.has_value());
                require(contents(*taken) == oracle.substr(consumed, request));
                consumed += request;
            }
        } else if (choice == 3) {
            std::vector<char> out(request);
            const auto peeked = parser.peek_to(out);
            if (request > remaining) {
                require(!peeked.has_value());
            } else {
                require(peeked.has_value());
                require(
                  std::string(out.begin(), out.end())
                  == oracle.substr(consumed, request));
            }
        } else {
            if (parser.push_checkpoint()) {
                const auto before = parser.bytes_consumed();
                static_cast<void>(
                  parser.skip(byte_count{static_cast<std::uint64_t>(request)}));
                require(parser.rollback().has_value());
                require(parser.bytes_consumed() == before);
            }
        }

        const auto fragment = parser.peek_current_fragment();
        if (consumed < oracle.size()) {
            require(fragment.size() > 0);
            require(
              oracle.compare(consumed, fragment.size(), fragment.bytes()) == 0);
        } else {
            require(fragment.empty());
        }
    }
}

} // namespace

extern "C" int
LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    if (size > max_input_size) {
        return 0;
    }

    script input{data, size};
    std::string oracle;
    auto buffer = build(input, oracle);
    require(contents(buffer) == oracle);

    reshape(input, buffer, oracle);
    parse(input, buffer, oracle);

    require(contents(buffer) == oracle);
    return 0;
}
