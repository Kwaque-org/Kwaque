#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/collection.h"
#include "src/codec/cooperative.h"
#include "src/codec/crc32c_cooperative.h"
#include "src/codec/digest.h"
#include "src/codec/error.h"
#include "src/codec/integer.h"
#include "src/codec/limits.h"
#include "src/codec/sha256.h"
#include "src/codec/sha256_cooperative.h"
#include "src/codec/staging_cooperative.h"
#include "src/codec/transaction.h"
#include "src/runtime/testing/seastar_fuzz.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <openssl/crypto.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <optional>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

namespace codec = kwaque::codec;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer;
using kwaque::bytes::fragmented_buffer_parser;

constexpr std::size_t maximum_input = 16U * 1024U;
constexpr codec::field_context context{.origin = 100, .family = 3, .field = 7};
constexpr codec::error anchor{errc::success, 3, 7, 100};

void require(bool condition) noexcept {
    if (!condition) {
        __builtin_trap();
    }
}

byte_count charge(byte_count requested) noexcept {
    if (requested.value() == 0) {
        return {};
    }
    if (requested.value() > (std::uint64_t{1} << 62U)) {
        return byte_count{std::numeric_limits<std::uint64_t>::max()};
    }
    return byte_count{
      2U * std::bit_ceil(std::max(requested.value(), std::uint64_t{16}))};
}

struct script final {
    std::array<std::uint8_t, 6> control{};
    std::span<const char> payload;

    explicit script(const std::vector<char>& bytes) noexcept {
        const auto prefix = std::min(bytes.size(), control.size());
        for (std::size_t index = 0; index < prefix; ++index) {
            control[index] = std::bit_cast<std::uint8_t>(bytes[index]);
        }
        payload = std::span<const char>{bytes}.subspan(prefix);
    }

    [[nodiscard]] std::uint64_t byte_quantum() const noexcept {
        return std::uint64_t{1} << (control[1] % 8U);
    }
    [[nodiscard]] bool initially_aborted() const noexcept {
        return (control[3] & 1U) != 0;
    }
    [[nodiscard]] bool queued_abort() const noexcept {
        return (control[3] & 2U) != 0;
    }
};

codec::limits policy(std::uint64_t bytes, std::uint64_t items) {
    codec::limits_config config;
    config.max_work_bytes = byte_count{bytes};
    config.max_work_items = item_count{items};
    return codec::limits::make(config).value();
}

fragmented_buffer fragmented(std::span<const char> bytes, std::uint8_t layout) {
    if (bytes.empty()) {
        return {};
    }
    const auto maximum_fragments = std::size_t{1} + layout % 32U;
    const auto width = (bytes.size() + maximum_fragments - 1U)
                       / maximum_fragments;
    std::vector<seastar::temporary_buffer<char>> fragments;
    fragments.reserve(maximum_fragments);
    for (std::size_t offset = 0; offset < bytes.size(); offset += width) {
        const auto part = bytes.subspan(
          offset, std::min(width, bytes.size() - offset));
        seastar::temporary_buffer<char> fragment{part.size()};
        std::ranges::copy(part, fragment.get_write());
        fragments.push_back(std::move(fragment));
    }
    return fragmented_buffer::copy_from_fragments(fragments).value();
}

// Bit-at-a-time arithmetic is independent of the production dispatch engine.
std::uint32_t
reference_crc(std::span<const char> bytes, std::uint32_t seed) noexcept {
    auto value = seed ^ 0xffffffffU;
    for (const auto byte : bytes) {
        value ^= std::bit_cast<std::uint8_t>(byte);
        for (unsigned bit = 0; bit < 8; ++bit) {
            value = (value >> 1U) ^ ((value & 1U) != 0 ? 0x82f63b78U : 0U);
        }
    }
    return value ^ 0xffffffffU;
}

template<typename Operation>
auto with_queued_abort(
  seastar::abort_source& abort, bool queued, Operation&& operation) {
    using result_type = std::invoke_result_t<Operation&>;
    std::optional<seastar::future<>> observer;
    if (queued) {
        observer.emplace(
          seastar::yield().then([&abort] { abort.request_abort(); }));
    }
    std::optional<result_type> result;
    std::exception_ptr failure;
    try {
        result.emplace(operation());
    } catch (...) {
        failure = std::current_exception();
    }
    if (observer) {
        observer->get();
    }
    if (failure) {
        std::rethrow_exception(failure);
    }
    return std::move(*result);
}

void require_abort(const auto& result) {
    require(!result.has_value());
    require(result.error() == codec::error{errc::aborted, 3, 7, 100});
}

void hashes(const script& input) {
    const auto seed = 0x9e3779b9U ^ input.control[4];
    const auto crc_expected = reference_crc(input.payload, seed);
    codec::sha256_hasher reference;
    reference.update(input.payload.data(), input.payload.size());
    const auto sha_expected = std::move(reference).final();
    auto crc_input = fragmented(input.payload, input.control[5]);
    auto sha_input = fragmented(
      input.payload, static_cast<std::uint8_t>(input.control[5] ^ 0xffU));
    seastar::abort_source abort;
    codec::cooperative_work work{
      policy(input.byte_quantum(), 1U + input.control[2] % 64U), abort};
    if (input.initially_aborted()) {
        abort.request_abort_ex(std::make_exception_ptr(17));
    }
    // A queued abort may run after publication on a ready path. Either a
    // correct published value or a typed abort is valid; this is not a proof
    // of actual suspension, which has separate deterministic coverage.
    const auto results = with_queued_abort(abort, input.queued_abort(), [&] {
        auto crc = codec::crc32c_cooperatively(
                     std::move(crc_input), work, seed, anchor)
                     .get();
        auto sha = codec::sha256_cooperatively(
                     std::move(sha_input), work, anchor)
                     .get();
        return std::pair{std::move(crc), std::move(sha)};
    });
    require(crc_input.empty() && sha_input.empty());
    if (results.first) {
        require(!input.initially_aborted() && *results.first == crc_expected);
    } else {
        require(abort.abort_requested());
        require_abort(results.first);
    }
    if (results.second) {
        require(!input.initially_aborted() && *results.second == sha_expected);
    } else {
        require(abort.abort_requested());
        require_abort(results.second);
    }
}

void staging(const script& input) {
    const auto split = static_cast<std::size_t>(input.control[4])
                       % (input.payload.size() + 1U);
    auto prefix = fragmented(input.payload.first(split), input.control[5]);
    auto payload = fragmented(
      input.payload.subspan(split),
      static_cast<std::uint8_t>(input.control[5] ^ 0xffU));
    seastar::abort_source abort;
    codec::cooperative_work work{
      policy(
        std::max(std::uint64_t{2}, input.byte_quantum()),
        8U + input.control[2] % 9U),
      abort};
    if (input.initially_aborted()) {
        abort.request_abort();
    }
    const bool short_cap = (input.control[3] & 4U) != 0
                           && !input.payload.empty();
    const bool no_memory = (input.control[3] & 8U) != 0;
    const byte_count cap{input.payload.size() - (short_cap ? 1U : 0U)};
    auto result = with_queued_abort(abort, input.queued_abort(), [&] {
        return codec::assemble_buffer_cooperatively(
                 std::move(prefix),
                 std::move(payload),
                 work,
                 cap,
                 {},
                 byte_count{no_memory ? 0U : 2U * 1024U * 1024U},
                 charge,
                 context)
          .get();
    });
    require(prefix.empty() && payload.empty());
    if (!result) {
        require(
          result.error().code() == errc::aborted
          || result.error().code() == errc::resource_exhausted);
        if (result.error().code() == errc::aborted) {
            require(abort.abort_requested());
        } else {
            require(short_cap || (no_memory && !input.payload.empty()));
        }
        require(result.error().family() == context.family);
        require(result.error().field() == context.field);
        require(result.error().byte_offset() == context.origin);
        return;
    }
    require(!input.initially_aborted() && !short_cap);
    require(result->content_equals(
      std::string_view{input.payload.data(), input.payload.size()}));
    auto checksum
      = codec::crc32c_cooperatively(std::move(*result), work, 0, anchor).get();
    if (checksum) {
        require(*checksum == reference_crc(input.payload, 0));
    } else {
        require(abort.abort_requested());
        require_abort(checksum);
    }
}

struct entry final {
    std::uint32_t key;
    std::uint32_t value;
};

void collections(const script& input) {
    const auto count = std::min<std::size_t>(64, input.payload.size() / 2U);
    std::array<std::optional<std::uint8_t>, 256> expected{};
    bool duplicate = false;
    seastar::chunked_fifo<entry, 16> source;
    for (std::size_t index = 0; index < count; ++index) {
        const auto key = std::bit_cast<std::uint8_t>(input.payload[2U * index]);
        const auto value = std::bit_cast<std::uint8_t>(
          input.payload[2U * index + 1U]);
        duplicate = duplicate || expected[key].has_value();
        expected[key] = value;
        source.push_back(entry{key, value});
    }
    seastar::abort_source abort;
    codec::cooperative_work work{
      policy(4096U << (input.control[1] % 3U), 64U << (input.control[2] % 3U)),
      abort};
    if (input.initially_aborted()) {
        abort.request_abort();
    }
    const bool no_memory = (input.control[3] & 8U) != 0;
    codec::decode_budget memory{
      byte_count{no_memory ? 0U : 2U * 1024U * 1024U},
      byte_count{1024U * 1024U},
      charge};
    std::size_t comparisons = 0;
    const auto stop_after = std::size_t{1} + input.control[4];
    auto result
      = codec::canonicalize_unordered(
          std::move(source),
          memory,
          work,
          [&](const entry& left, const entry& right) noexcept {
              ++comparisons;
              if ((input.control[3] & 2U) != 0 && comparisons == stop_after) {
                  abort.request_abort();
              }
              return left.key < right.key;
          },
          context)
          .get();
    // This fixture admits ownership transfer, which empties the source.
    // NOLINTNEXTLINE(bugprone-use-after-move)
    require(source.empty());
    if (!result) {
        require(
          result.error().code() == errc::aborted
          || result.error().code() == errc::resource_exhausted
          || result.error().code() == errc::malformed_data);
        if (result.error().code() == errc::malformed_data) {
            require(duplicate);
        } else if (result.error().code() == errc::resource_exhausted) {
            require(no_memory && count != 0);
        } else {
            require(abort.abort_requested());
        }
        return;
    }
    require(!duplicate && !abort.abort_requested());
    require(result->size() == count);
    std::array<char, 129> encoded{};
    encoded[0] = std::bit_cast<char>(static_cast<std::uint8_t>(count));
    std::size_t at = 1;
    auto found = result->begin();
    for (std::size_t key = 0; key < expected.size(); ++key) {
        if (!expected[key]) {
            continue;
        }
        require(found != result->end());
        require(found->key == key && found->value == *expected[key]);
        encoded[at++] = std::bit_cast<char>(static_cast<std::uint8_t>(key));
        encoded[at++] = std::bit_cast<char>(*expected[key]);
        ++found;
    }
    require(found == result->end());
    fragmented_buffer_parser parser{
      fragmented(std::span{encoded}.first(at), input.control[5])};
    const auto read_byte = [](
                             fragmented_buffer_parser& value,
                             codec::field_context field,
                             codec::input_boundary boundary,
                             codec::decode_budget&,
                             codec::cooperative_work&) {
        return codec::read_le<std::uint8_t>(value, field, boundary);
    };
    std::size_t inserted = 0;
    auto decoded = codec::read_ordered_map<std::uint8_t, std::uint8_t>(
                     parser,
                     item_count{64},
                     memory,
                     work,
                     read_byte,
                     read_byte,
                     [&](
                       std::uint8_t&& key,
                       std::uint8_t&& value,
                       codec::decode_budget&,
                       codec::cooperative_work&) -> codec::result<void> {
                         require(expected[key] && *expected[key] == value);
                         ++inserted;
                         return {};
                     },
                     [](std::uint8_t left, std::uint8_t right) noexcept {
                         return left < right;
                     },
                     byte_count{64},
                     item_count{16},
                     context,
                     codec::input_boundary::complete)
                     .get();
    require(decoded && *decoded == item_count{count});
    require(
      inserted == count && parser.at_end() && parser.checkpoint_depth() == 0);
    // The successful constructor transfers disposal to this caller. Keep it
    // on the same work account after the verification child has completed.
    while (!result->empty()) {
        work.drain(byte_count{2U * sizeof(entry)}, item_count{2}).get();
        result->pop_front();
    }
}

void empty_children(const script& input) {
    const auto items = std::uint64_t{1} + input.control[2] % 64U;
    seastar::abort_source abort;
    codec::cooperative_work work{policy(input.byte_quantum(), items), abort};
    const auto children = std::uint64_t{1} + input.control[4] % 16U;
    for (std::uint64_t index = 0; index < children; ++index) {
        if (input.initially_aborted() && index == input.control[5] % children) {
            abort.request_abort_ex(std::make_exception_ptr(23));
        }
        const auto result
          = codec::crc32c_cooperatively({}, work, 37, anchor).get();
        if (abort.abort_requested()) {
            require_abort(result);
        } else {
            require(result && *result == 37U);
            const auto consumed = 2U * (index + 1U);
            require(
              work.items_remaining()
              == item_count{(items - consumed % items) % items});
            require(work.bytes_remaining() == work.byte_quantum());
        }
    }
}

void exercise(const std::vector<char>& bytes) {
    const script input{bytes};
    switch (input.control[0] % 4U) {
    case 0:
        hashes(input);
        break;
    case 1:
        staging(input);
        break;
    case 2:
        collections(input);
        break;
    case 3:
        empty_children(input);
        break;
    }
}

} // namespace

extern "C" int
LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    if (size > maximum_input) {
        return 0;
    }
    // No fuzzer-owned pointer crosses the reactor callback boundary.
    std::vector<char> owned(size);
    for (std::size_t index = 0; index < size; ++index) {
        owned[index] = std::bit_cast<char>(data[index]);
    }
    kwaque::runtime::testing::run_fuzz_input([owned = std::move(owned)] {
        // Release this worker's crypto state before process-wide cleanup runs.
        auto cleanup = seastar::defer([] noexcept { OPENSSL_thread_stop(); });
        exercise(owned);
    });
    return 0;
}
