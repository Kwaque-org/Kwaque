// Includes both the inline header and xxh3.h: the unit must build.
#include "src/codec/tests/xxh3_static_kernel.h"
#include "src/codec/xxh3.h"
#include "src/codec/xxh3_64_inline.h"

#include <seastar/core/memory.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <map>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

namespace codec = kwaque::codec;
namespace kernel = kwaque::codec::testing;
using codec::xxh3_64_hasher;

static_assert(!std::is_copy_constructible_v<xxh3_64_hasher>);
static_assert(!std::is_move_constructible_v<xxh3_64_hasher>);
static_assert(std::is_nothrow_default_constructible_v<xxh3_64_hasher>);
// Owners, including coroutine frames, need no extended alignment.
static_assert(alignof(xxh3_64_hasher) == 1);
static_assert(alignof(codec::xxh3_128_hasher) == 1);
static_assert(sizeof(xxh3_64_hasher) == sizeof(codec::xxh3_128_hasher));
static_assert(noexcept(codec::xxh3_64(nullptr, 0)));
static_assert(noexcept(codec::xxh3_64_inline(nullptr, 0)));
static_assert(noexcept(codec::xxh3_64_tagged(0, nullptr, 0)));
static_assert(noexcept(std::declval<xxh3_64_hasher&>().update(nullptr, 0)));
static_assert(noexcept(std::declval<xxh3_64_hasher&&>().final()));

constexpr std::size_t vector_rows = 1260;
constexpr std::size_t byte_piece_limit = 4096;

struct vector_row {
    std::string id;
    std::string kind;
    std::vector<std::byte> key;
    std::uint64_t value{};
};

std::vector<std::byte> sanity(std::size_t length) {
    std::vector<std::byte> out(length);
    std::uint64_t generator = 2654435761U;
    for (auto& octet : out) {
        octet = static_cast<std::byte>(generator >> 56U);
        generator *= 11400714785074694797ULL;
    }
    return out;
}

std::vector<std::byte> regenerate(std::string_view kind, std::size_t length) {
    if (kind == "sanity") return sanity(length);
    std::vector<std::byte> out(length);
    for (std::size_t at = 0; at < length; ++at) {
        if (kind == "zero") {
            out[at] = std::byte{0x00};
        } else if (kind == "ff") {
            out[at] = std::byte{0xff};
        } else if (kind == "iota") {
            out[at] = static_cast<std::byte>(at & 0xffU);
        } else {
            throw std::runtime_error("unknown regenerated class");
        }
    }
    return out;
}

template<typename T>
T parse_number(std::string_view text, int base) {
    T value{};
    const auto parsed = std::from_chars(
      text.data(), text.data() + text.size(), value, base);
    if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size())
        throw std::runtime_error("invalid vector number");
    return value;
}

std::vector<std::byte> parse_hex(std::string_view text) {
    if (text.size() % 2 != 0) throw std::runtime_error("odd vector key hex");
    std::vector<std::byte> out;
    for (std::size_t at = 0; at < text.size(); at += 2)
        out.push_back(
          static_cast<std::byte>(
            parse_number<unsigned>(text.substr(at, 2), 16)));
    return out;
}

std::vector<std::string_view> split_tabs(std::string_view line) {
    std::vector<std::string_view> columns;
    for (;;) {
        const auto tab = line.find('\t');
        columns.push_back(line.substr(0, tab));
        if (tab == std::string_view::npos) return columns;
        line.remove_prefix(tab + 1);
    }
}

std::vector<vector_row> load_vectors() {
    const auto* directory = std::getenv("TEST_SRCDIR");
    const auto* workspace = std::getenv("TEST_WORKSPACE");
    const auto root = directory && workspace
                        ? std::string{directory} + "/" + workspace + "/"
                        : std::string{};
    std::ifstream file{root + "src/codec/tests/testdata/xxh3_64_vectors.tsv"};
    if (!file) throw std::runtime_error("cannot open the XXH3-64 vectors");
    std::vector<vector_row> rows;
    std::string line;
    while (std::getline(file, line)) {
        if (line.empty() || line.front() == '#') continue;
        const auto columns = split_tabs(line);
        if (columns.size() != 5) throw std::runtime_error("bad vector row");
        const auto length = parse_number<std::size_t>(columns[2], 10);
        vector_row row{
          std::string{columns[0]},
          std::string{columns[1]},
          {},
          parse_number<std::uint64_t>(columns[4], 16)};
        if (columns[3] == "-") {
            row.key = row.kind == "utf8" ? std::vector<std::byte>{}
                                         : regenerate(row.kind, length);
        } else {
            row.key = parse_hex(columns[3]);
        }
        if (row.key.size() != length)
            throw std::runtime_error("vector key length mismatch");
        rows.push_back(std::move(row));
    }
    return rows;
}

// A seeded split-mix sequence for piece sizes; the same pieces every run.
class piece_sizes final {
public:
    explicit piece_sizes(std::uint64_t seed) noexcept
      : state_(seed) {}

    std::size_t next(std::size_t limit) noexcept {
        state_ += 0x9e3779b97f4a7c15ULL;
        auto mixed = state_;
        mixed = (mixed ^ (mixed >> 30U)) * 0xbf58476d1ce4e5b9ULL;
        mixed = (mixed ^ (mixed >> 27U)) * 0x94d049bb133111ebULL;
        mixed ^= mixed >> 31U;
        return static_cast<std::size_t>(mixed % limit) + 1;
    }

private:
    std::uint64_t state_;
};

std::vector<std::span<const std::byte>>
pieces_of(std::span<const std::byte> bytes, std::size_t width) {
    std::vector<std::span<const std::byte>> pieces;
    for (std::size_t at = 0; at < bytes.size(); at += width)
        pieces.push_back(bytes.subspan(at, std::min(width, bytes.size() - at)));
    return pieces;
}

std::vector<std::span<const std::byte>>
random_pieces_of(std::span<const std::byte> bytes, piece_sizes& sizes) {
    std::vector<std::span<const std::byte>> pieces;
    for (std::size_t at = 0; at < bytes.size();) {
        const auto width = std::min(sizes.next(300), bytes.size() - at);
        pieces.push_back(bytes.subspan(at, width));
        at += width;
    }
    return pieces;
}

std::uint64_t streamed(std::span<const std::byte> bytes, std::size_t width) {
    xxh3_64_hasher hasher;
    for (const auto piece : pieces_of(bytes, width))
        hasher.update(piece.data(), piece.size());
    return std::move(hasher).final();
}

TEST(CodecXxh3x64Test, VectorsHoldThroughEveryEntryPoint) {
    const auto rows = load_vectors();
    ASSERT_EQ(rows.size(), vector_rows);
    std::set<std::string> ids;
    std::map<std::string, std::size_t> classes;
    piece_sizes sizes{0x6b77617175650001ULL};
    for (const auto& row : rows) {
        SCOPED_TRACE(row.id);
        ASSERT_TRUE(ids.insert(row.id).second);
        ++classes[row.kind];
        const std::span<const std::byte> key{row.key};
        EXPECT_EQ(codec::xxh3_64(key.data(), key.size()), row.value);
        EXPECT_EQ(codec::xxh3_64_inline(key.data(), key.size()), row.value);
        EXPECT_EQ(
          kernel::static_kernel_xxh3_64(key.data(), key.size()), row.value);
        EXPECT_EQ(streamed(key, key.size() + 1), row.value);
        EXPECT_EQ(streamed(key, 7), row.value);
        const std::array whole{key};
        EXPECT_EQ(codec::xxh3_64_pieces(whole), row.value);
        EXPECT_EQ(codec::xxh3_64_pieces(pieces_of(key, 7)), row.value);
        EXPECT_EQ(
          codec::xxh3_64_pieces(random_pieces_of(key, sizes)), row.value);
        if (key.size() <= byte_piece_limit) {
            EXPECT_EQ(streamed(key, 1), row.value);
            EXPECT_EQ(codec::xxh3_64_pieces(pieces_of(key, 1)), row.value);
        }
    }
    const std::map<std::string, std::size_t> expected_classes{
      {"ff", 303},
      {"iota", 303},
      {"sanity", 303},
      {"u64be", 16},
      {"u64le", 16},
      {"utf8", 16},
      {"zero", 303}};
    EXPECT_EQ(classes, expected_classes);
}

TEST(CodecXxh3x64Test, DispatcherSelectsAvx2WhereThePlatformHasIt) {
#if defined(__x86_64__)
    // Every x86-64 processor has SSE2, and the build selects nothing wider
    // than AVX2.
    EXPECT_EQ(
      kernel::dispatched_kernel(),
      kernel::avx2_available() ? kernel::avx2_kernel : kernel::sse2_kernel);
#else
    EXPECT_EQ(kernel::dispatched_kernel(), -1);
#endif
}

TEST(CodecXxh3x64Test, EmptyInputIsAnOrdinaryValue) {
    EXPECT_EQ(codec::xxh3_64(nullptr, 0), codec::xxh3_64_empty);
    EXPECT_EQ(codec::xxh3_64_inline(nullptr, 0), codec::xxh3_64_empty);
    EXPECT_EQ(xxh3_64_hasher{}.final(), codec::xxh3_64_empty);
    EXPECT_EQ(
      codec::xxh3_64_pieces(std::span<const std::span<const std::byte>>{}),
      codec::xxh3_64_empty);
    const std::array<std::span<const std::byte>, 3> empty_pieces{};
    EXPECT_EQ(codec::xxh3_64_pieces(empty_pieces), codec::xxh3_64_empty);
}

TEST(CodecXxh3x64Test, PiecesSkipEmptyPiecesAtEveryPosition) {
    const auto bytes = sanity(600);
    for (const std::size_t size : {10UL, 239UL, 240UL, 241UL, 600UL}) {
        SCOPED_TRACE(size);
        const std::span<const std::byte> key{bytes.data(), size};
        const auto expected = codec::xxh3_64(key.data(), key.size());
        const std::array pieces{
          std::span<const std::byte>{},
          key.first(size / 2),
          std::span<const std::byte>{},
          key.subspan(size / 2),
          std::span<const std::byte>{}};
        EXPECT_EQ(codec::xxh3_64_pieces(pieces), expected);
    }
}

TEST(CodecXxh3x64Test, TaggedHashesTheTagByteBeforeTheInput) {
    const auto bytes = sanity(320);
    for (const std::uint8_t tag :
         std::array<std::uint8_t, 4>{0, 1, 0x7f, 0xff}) {
        for (std::size_t size = 0; size <= bytes.size(); ++size) {
            SCOPED_TRACE(size);
            std::vector<std::byte> joined{std::byte{tag}};
            joined.insert(
              joined.end(),
              bytes.begin(),
              bytes.begin() + static_cast<std::ptrdiff_t>(size));
            EXPECT_EQ(
              codec::xxh3_64_tagged(tag, bytes.data(), size),
              codec::xxh3_64(joined.data(), joined.size()));
        }
    }
    EXPECT_NE(
      codec::xxh3_64_tagged(1, bytes.data(), 8),
      codec::xxh3_64_tagged(2, bytes.data(), 8));
}

TEST(CodecXxh3x64Test, HashingAllocatesNothing) {
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
    const auto bytes = sanity(2048);
    const std::span<const std::byte> key{bytes};
    const auto halves = std::array{key.first(100), key.subspan(100)};
    const auto short_halves = std::array{key.first(50), key.subspan(50, 50)};
    std::uint64_t sink = 0;
    const auto before = seastar::memory::stats().mallocs();
    for (const std::size_t size : {0UL, 16UL, 240UL, 241UL, 2048UL}) {
        sink ^= codec::xxh3_64(key.data(), size);
        sink ^= codec::xxh3_64_inline(key.data(), size);
        sink ^= codec::xxh3_64_tagged(3, key.data(), size);
    }
    sink ^= codec::xxh3_64_pieces(halves);
    sink ^= codec::xxh3_64_pieces(short_halves);
    {
        xxh3_64_hasher hasher;
        hasher.update(key.data(), key.size());
        sink ^= std::move(hasher).final();
    }
    const auto after = seastar::memory::stats().mallocs();
    EXPECT_EQ(after - before, 0U);
    EXPECT_NE(sink, 0U);
#else
    GTEST_SKIP() << "the system allocator keeps no allocation count";
#endif
}

} // namespace
