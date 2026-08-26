#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_builder.h"

#include <seastar/core/deleter.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/packet.hh>

#include <gtest/gtest.h>
#include <sys/uio.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace kwaque::bytes {

namespace {

using fragment_type = seastar::temporary_buffer<char>;

fragment_type fragment_of(std::string_view bytes) {
    fragment_type fragment{bytes.size()};
    std::ranges::copy(bytes, fragment.get_write());
    return fragment;
}

fragment_type borrowed_fragment(std::span<char> bytes) {
    return fragment_type::maybe_unsafe_from_deleter(
      bytes.data(), bytes.size(), seastar::deleter{});
}

// Builds a buffer with an exact fragmentation pattern so tests can pin
// behaviour to fragment boundaries rather than to whatever the builder chose.
fragmented_buffer fragmented(std::initializer_list<std::string_view> parts) {
    std::vector<fragment_type> fragments;
    fragments.reserve(parts.size());
    for (const auto part : parts) {
        fragments.push_back(fragment_of(part));
    }
    auto buffer = fragmented_buffer::from_fragments(std::move(fragments));
    EXPECT_TRUE(buffer.has_value());
    return std::move(*buffer);
}

std::string contents(const fragmented_buffer& buffer) {
    std::string out;
    out.resize(buffer.size().value());
    const auto copied = buffer.copy_to(out);
    EXPECT_TRUE(copied.has_value());
    return out;
}

std::vector<std::size_t> fragment_sizes(const fragmented_buffer& buffer) {
    std::vector<std::size_t> sizes;
    for (const auto fragment : buffer) {
        sizes.push_back(fragment.size());
    }
    return sizes;
}

static_assert(!std::is_copy_constructible_v<fragmented_buffer>);
static_assert(!std::is_copy_assignable_v<fragmented_buffer>);
static_assert(std::is_nothrow_move_constructible_v<fragmented_buffer>);
static_assert(std::is_nothrow_move_assignable_v<fragmented_buffer>);
static_assert(!std::is_copy_constructible_v<fragmented_buffer_builder>);

// A published buffer offers no way to hand it storage it does not own, and no
// way to reach writable bytes.
static_assert(!std::is_constructible_v<fragmented_buffer, char*, std::size_t>);
static_assert(
  !std::is_constructible_v<fragmented_buffer, const char*, std::size_t>);
static_assert(std::is_same_v<
              decltype(std::declval<const fragment_view&>().data()),
              const char*>);

} // namespace

// Pins the deterministic allocator size-class model used by the growth policy,
// so any change to the model is explicit.
TEST(BufferBuilder, ServedAllocationSizeMatchesAllocatorClasses) {
    // Every non-empty cell has room for the allocator's free-list link.
    static_assert(served_allocation_size(0) == 0);
    static_assert(served_allocation_size(1) == sizeof(void*));
    static_assert(served_allocation_size(4) == sizeof(void*));
    static_assert(served_allocation_size(sizeof(void*)) == sizeof(void*));
    // Inside the pooled range exact classes stay put, sizes between classes
    // round up, and wider classes preserve the platform's maximum alignment.
    static_assert(served_allocation_size(16) == 16);
    static_assert(served_allocation_size(24) == 32);
    static_assert(served_allocation_size(48) == 48);
    static_assert(served_allocation_size(64) == 64);
    static_assert(served_allocation_size(512) == 512);
    static_assert(served_allocation_size(513) == 640);
    static_assert(served_allocation_size(768) == 768);
    static_assert(served_allocation_size(1152) == 1280);
    static_assert(served_allocation_size(16384) == 16384);
    // Past the pooled range only power-of-two spans exist.
    static_assert(served_allocation_size(16385) == 32768);
    static_assert(served_allocation_size(32768) == 32768);
    static_assert(served_allocation_size(131072) == 131072);

    // Every served size covers its request and asking again is a fixed point.
    for (std::uint64_t size = 1; size <= 4096; ++size) {
        const auto served = served_allocation_size(size);
        ASSERT_GE(served, size) << "size=" << size;
        EXPECT_EQ(served_allocation_size(served), served) << "size=" << size;
    }
}

TEST(FragmentedBuffer, EmptyAndSingleFragmentShapes) {
    const fragmented_buffer empty;
    EXPECT_TRUE(empty.empty());
    EXPECT_EQ(empty.size().value(), 0U);
    EXPECT_EQ(empty.fragment_count(), 0U);
    EXPECT_EQ(empty.begin(), empty.end());
    EXPECT_FALSE(empty.fragment_at(0).has_value());

    auto single = fragmented({"hello"});
    EXPECT_FALSE(single.empty());
    EXPECT_EQ(single.size().value(), 5U);
    EXPECT_EQ(single.fragment_count(), 1U);
    EXPECT_TRUE(single.content_equals("hello"));

    // Empty donations never become links, so the canonical empty
    // representation has no fragments at all.
    auto sparse = fragmented({"", "ab", "", "c", ""});
    EXPECT_EQ(sparse.fragment_count(), 2U);
    EXPECT_EQ(sparse.size().value(), 3U);
    EXPECT_TRUE(sparse.content_equals("abc"));

    auto all_empty = fragmented({"", "", ""});
    EXPECT_TRUE(all_empty.empty());
    EXPECT_EQ(all_empty.fragment_count(), 0U);
}

TEST(FragmentedBuffer, MoveTransfersOwnershipAndClearsSource) {
    auto source = fragmented({"abc", "de"});
    const auto* first = source.fragment_at(0)->data();

    auto moved = std::move(source);
    EXPECT_EQ(moved.size().value(), 5U);
    EXPECT_EQ(moved.fragment_at(0)->data(), first);
    EXPECT_TRUE(moved.content_equals("abcde"));
    // The buffer guarantees a canonical, usable moved-from state.
    // NOLINTBEGIN(bugprone-use-after-move)
    EXPECT_TRUE(source.empty());
    EXPECT_EQ(source.size().value(), 0U);
    EXPECT_EQ(source.fragment_count(), 0U);
    EXPECT_EQ(source.begin(), source.end());
    // NOLINTEND(bugprone-use-after-move)

    fragmented_buffer assigned;
    assigned = std::move(moved);
    EXPECT_EQ(assigned.size().value(), 5U);
    EXPECT_EQ(assigned.fragment_at(0)->data(), first);
    // Move assignment provides the same canonical source state.
    // NOLINTBEGIN(bugprone-use-after-move)
    EXPECT_TRUE(moved.empty());
    EXPECT_EQ(moved.size().value(), 0U);
    EXPECT_EQ(moved.fragment_count(), 0U);
    EXPECT_EQ(moved.begin(), moved.end());
    // NOLINTEND(bugprone-use-after-move)
}

TEST(FragmentedBuffer, RejectsFragmentCountAndSizeOverflow) {
    std::vector<fragment_type> too_many;
    too_many.reserve(max_buffer_fragments + 1);
    for (std::size_t index = 0; index <= max_buffer_fragments; ++index) {
        too_many.push_back(fragment_of("x"));
    }
    const auto rejected = fragmented_buffer::from_fragments(
      std::move(too_many));
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), make_error_code(errc::resource_exhausted));

    std::vector<fragment_type> exactly_enough;
    exactly_enough.reserve(max_buffer_fragments);
    for (std::size_t index = 0; index < max_buffer_fragments; ++index) {
        exactly_enough.push_back(fragment_of("x"));
    }
    const auto accepted = fragmented_buffer::from_fragments(
      std::move(exactly_enough));
    ASSERT_TRUE(accepted.has_value());
    EXPECT_EQ(accepted->size().value(), max_buffer_fragments);
}

TEST(FragmentedBuffer, RejectsStorageWithoutALifetimeOwner) {
    std::array<char, 4> borrowed{'t', 'e', 's', 't'};

    const auto single = fragmented_buffer::from_fragment(
      borrowed_fragment(borrowed));
    ASSERT_FALSE(single.has_value());
    EXPECT_EQ(single.error(), make_error_code(errc::invalid_argument));

    std::vector<fragment_type> fragments;
    fragments.push_back(fragment_of("owned"));
    fragments.push_back(borrowed_fragment(borrowed));
    const auto multiple = fragmented_buffer::from_fragments(
      std::move(fragments));
    ASSERT_FALSE(multiple.has_value());
    EXPECT_EQ(multiple.error(), make_error_code(errc::invalid_argument));

    fragmented_buffer_builder builder;
    ASSERT_TRUE(builder.append(std::string_view{"head"}).has_value());
    const auto rejected = builder.append_fragment(borrowed_fragment(borrowed));
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), make_error_code(errc::invalid_argument));
    EXPECT_EQ(builder.size().value(), 4U);
    EXPECT_EQ(builder.fragment_count(), 1U);
    auto published = builder.finish();
    ASSERT_TRUE(published.has_value());
    EXPECT_TRUE(published->content_equals("head"));
}

TEST(FragmentedBuffer, ShareIsZeroCopyAndOutlivesItsSource) {
    std::array<const char*, 2> backing{};
    fragmented_buffer derived;
    std::string expected;
    {
        auto source = fragmented({"abcd", "efgh"});
        backing[0] = source.fragment_at(0)->data();
        backing[1] = source.fragment_at(1)->data();
        expected = contents(source);

        auto shared = source.share();
        ASSERT_TRUE(shared.has_value());
        // Identical backing pointers prove no payload was copied.
        EXPECT_EQ(shared->fragment_at(0)->data(), backing[0]);
        EXPECT_EQ(shared->fragment_at(1)->data(), backing[1]);
        derived = std::move(*shared);
    }
    EXPECT_EQ(contents(derived), expected);
    EXPECT_EQ(derived.fragment_at(0)->data(), backing[0]);
}

TEST(FragmentedBuffer, SlicedShareSpansFragmentBoundaries) {
    auto source = fragmented({"abcd", "efgh", "ijkl"});

    auto whole = source.share(byte_count{0}, byte_count{12});
    ASSERT_TRUE(whole.has_value());
    EXPECT_EQ(contents(*whole), "abcdefghijkl");
    EXPECT_EQ(whole->fragment_count(), 3U);

    // Interior slice crossing two boundaries keeps only the fragments it needs.
    auto interior = source.share(byte_count{2}, byte_count{7});
    ASSERT_TRUE(interior.has_value());
    EXPECT_EQ(contents(*interior), "cdefghi");
    EXPECT_EQ(interior->fragment_count(), 3U);
    EXPECT_EQ(
      interior->fragment_at(0)->data(), source.fragment_at(0)->data() + 2);

    auto inside_one = source.share(byte_count{5}, byte_count{2});
    ASSERT_TRUE(inside_one.has_value());
    EXPECT_EQ(contents(*inside_one), "fg");
    EXPECT_EQ(inside_one->fragment_count(), 1U);

    auto zero = source.share(byte_count{4}, byte_count{0});
    ASSERT_TRUE(zero.has_value());
    EXPECT_TRUE(zero->empty());

    EXPECT_FALSE(source.share(byte_count{12}, byte_count{1}).has_value());
    EXPECT_FALSE(source.share(byte_count{0}, byte_count{13}).has_value());
    EXPECT_FALSE(source.share(byte_count{11}, byte_count{2}).has_value());
}

TEST(FragmentedBuffer, CopyIsDeepAndIndependent) {
    auto source = fragmented({"abcd", "efgh"});
    auto copied = source.copy();
    ASSERT_TRUE(copied.has_value());
    EXPECT_EQ(contents(*copied), "abcdefgh");
    EXPECT_NE(copied->fragment_at(0)->data(), source.fragment_at(0)->data());
    EXPECT_TRUE(source.content_equals(*copied));
}

// Walks a four-fragment buffer down to empty one trim at a time, checking the
// fragment count, total size, and boundary fragment after each step, then that
// underflow past an exhausted buffer is a typed failure.
TEST(FragmentedBuffer, TrimSequenceAcrossFourFragments) {
    auto buffer = fragmented({"0123", "456789", "abcdefgh", "ijklmnopqr"});
    EXPECT_EQ(buffer.fragment_count(), 4U);
    EXPECT_EQ(buffer.size().value(), 28U);
    EXPECT_EQ(buffer.fragment_at(0)->size(), 4U);

    ASSERT_TRUE(buffer.trim_front(byte_count{1}).has_value());
    EXPECT_EQ(buffer.fragment_count(), 4U);
    EXPECT_EQ(buffer.size().value(), 27U);
    EXPECT_EQ(buffer.fragment_at(0)->size(), 3U);
    EXPECT_TRUE(buffer.content_equals("123456789abcdefghijklmnopqr"));

    ASSERT_TRUE(buffer.trim_front(byte_count{5}).has_value());
    EXPECT_EQ(buffer.fragment_count(), 3U);
    EXPECT_EQ(buffer.size().value(), 22U);
    EXPECT_EQ(buffer.fragment_at(0)->size(), 4U);
    EXPECT_TRUE(buffer.content_equals("6789abcdefghijklmnopqr"));

    ASSERT_TRUE(buffer.trim_back(byte_count{1}).has_value());
    EXPECT_EQ(buffer.fragment_count(), 3U);
    EXPECT_EQ(buffer.size().value(), 21U);
    EXPECT_EQ(buffer.fragment_at(2)->size(), 9U);

    ASSERT_TRUE(buffer.trim_back(byte_count{20}).has_value());
    EXPECT_EQ(buffer.fragment_count(), 1U);
    EXPECT_EQ(buffer.size().value(), 1U);
    EXPECT_TRUE(buffer.content_equals("6"));

    ASSERT_TRUE(buffer.trim_back(byte_count{1}).has_value());
    EXPECT_TRUE(buffer.empty());
    EXPECT_EQ(buffer.fragment_count(), 0U);

    // Underflow past an exhausted buffer is a typed failure, not a crash.
    const auto front_underflow = buffer.trim_front(byte_count{2});
    ASSERT_FALSE(front_underflow.has_value());
    EXPECT_EQ(front_underflow.error(), make_error_code(errc::out_of_range));
    const auto back_underflow = buffer.trim_back(byte_count{30});
    ASSERT_FALSE(back_underflow.has_value());
    EXPECT_EQ(back_underflow.error(), make_error_code(errc::out_of_range));
    EXPECT_TRUE(buffer.empty());
}

TEST(FragmentedBuffer, TrimIsExactAtEveryBoundary) {
    for (std::uint64_t front = 0; front <= 9; ++front) {
        for (std::uint64_t back = 0; back + front <= 9; ++back) {
            auto buffer = fragmented({"abc", "def", "ghi"});
            ASSERT_TRUE(buffer.trim_front(byte_count{front}).has_value());
            ASSERT_TRUE(buffer.trim_back(byte_count{back}).has_value());
            const std::string whole = "abcdefghi";
            const auto expected = whole.substr(
              static_cast<std::size_t>(front),
              static_cast<std::size_t>(9 - front - back));
            EXPECT_EQ(contents(buffer), expected)
              << "front=" << front << " back=" << back;
            EXPECT_EQ(buffer.size().value(), expected.size());
            for (const auto fragment : buffer) {
                EXPECT_GT(fragment.size(), 0U);
            }
        }
    }
}

TEST(FragmentedBuffer, ScatterExportRespectsCapAndResumes) {
    auto buffer = fragmented({"hello", "goodbye", "hello again"});
    const std::string whole = "hellogoodbyehello again";

    // Exactly at the cap: one batch covers everything.
    {
        std::array<::iovec, 3> vectors{};
        scatter_cursor cursor;
        const auto batch = buffer.export_scatter(
          vectors, buffer.size(), cursor);
        ASSERT_TRUE(batch.has_value());
        EXPECT_EQ(batch->vectors, 3U);
        EXPECT_EQ(batch->bytes.value(), 23U);
        EXPECT_TRUE(batch->complete);
        EXPECT_EQ(
          std::string_view(
            static_cast<const char*>(vectors[0].iov_base), vectors[0].iov_len),
          "hello");
        EXPECT_EQ(
          std::string_view(
            static_cast<const char*>(vectors[1].iov_base), vectors[1].iov_len),
          "goodbye");
        EXPECT_EQ(
          std::string_view(
            static_cast<const char*>(vectors[2].iov_base), vectors[2].iov_len),
          "hello again");
        // Exported vectors borrow the buffer's storage; nothing was copied.
        EXPECT_EQ(vectors[0].iov_base, buffer.fragment_at(0)->data());
    }

    // One below the cap: the batch stops short and the cursor resumes exactly.
    {
        std::array<::iovec, 2> vectors{};
        scatter_cursor cursor;
        const auto first = buffer.export_scatter(
          vectors, buffer.size(), cursor);
        ASSERT_TRUE(first.has_value());
        EXPECT_EQ(first->vectors, 2U);
        EXPECT_EQ(first->bytes.value(), 12U);
        EXPECT_FALSE(first->complete);

        const auto second = buffer.export_scatter(
          vectors, buffer.size(), cursor);
        ASSERT_TRUE(second.has_value());
        EXPECT_EQ(second->vectors, 1U);
        EXPECT_EQ(second->bytes.value(), 11U);
        EXPECT_TRUE(second->complete);

        const auto third = buffer.export_scatter(
          vectors, buffer.size(), cursor);
        ASSERT_TRUE(third.has_value());
        EXPECT_EQ(third->vectors, 0U);
        EXPECT_TRUE(third->complete);
    }

    // One above the cap: the spare entry is left untouched.
    {
        std::array<::iovec, 4> vectors{};
        vectors[3].iov_len = 12345;
        scatter_cursor cursor;
        const auto batch = buffer.export_scatter(
          vectors, buffer.size(), cursor);
        ASSERT_TRUE(batch.has_value());
        EXPECT_EQ(batch->vectors, 3U);
        EXPECT_TRUE(batch->complete);
        EXPECT_EQ(vectors[3].iov_len, 12345U);
    }

    // A single-entry cap forces one batch per fragment and still reassembles.
    {
        std::array<::iovec, 1> vectors{};
        scatter_cursor cursor;
        std::string reassembled;
        std::size_t batches = 0;
        while (true) {
            const auto batch = buffer.export_scatter(
              vectors, buffer.size(), cursor);
            ASSERT_TRUE(batch.has_value());
            for (std::size_t index = 0; index < batch->vectors; ++index) {
                reassembled.append(
                  static_cast<const char*>(vectors[index].iov_base),
                  vectors[index].iov_len);
            }
            ++batches;
            if (batch->complete) {
                break;
            }
        }
        EXPECT_EQ(reassembled, whole);
        EXPECT_EQ(batches, 3U);
    }

    std::array<::iovec, 0> none{};
    scatter_cursor cursor;
    EXPECT_FALSE(
      buffer.export_scatter(none, buffer.size(), cursor).has_value());
}

TEST(FragmentedBuffer, ScatterExportRespectsByteCapInsideFragments) {
    auto buffer = fragmented({"abcde", "fghij", "klmno"});
    const std::string whole = "abcdefghijklmno";

    // Exercise one byte below, exactly at, and one byte above a fragment
    // boundary. Enough vector slots are supplied so only the byte budget binds.
    for (const std::uint64_t byte_cap : {4U, 5U, 6U}) {
        std::array<::iovec, 4> vectors{};
        scatter_cursor cursor;
        std::string reassembled;
        std::size_t batches = 0;
        while (true) {
            const auto batch = buffer.export_scatter(
              vectors, byte_count{byte_cap}, cursor);
            ASSERT_TRUE(batch.has_value()) << "byte_cap=" << byte_cap;
            EXPECT_LE(batch->bytes.value(), byte_cap);
            for (std::size_t index = 0; index < batch->vectors; ++index) {
                EXPECT_LE(vectors[index].iov_len, byte_cap);
                reassembled.append(
                  static_cast<const char*>(vectors[index].iov_base),
                  vectors[index].iov_len);
            }
            ++batches;
            if (batch->complete) {
                break;
            }
        }
        EXPECT_EQ(reassembled, whole) << "byte_cap=" << byte_cap;
        EXPECT_EQ(batches, (whole.size() + byte_cap - 1) / byte_cap);
    }

    std::array<::iovec, 4> vectors{};
    {
        scatter_cursor cursor;
        const auto batch = buffer.export_scatter(
          vectors, byte_count{4}, cursor);
        ASSERT_TRUE(batch.has_value());
        EXPECT_EQ(cursor.fragment(), 0U);
        EXPECT_EQ(cursor.offset().value(), 4U);
    }
    {
        scatter_cursor cursor;
        const auto batch = buffer.export_scatter(
          vectors, byte_count{5}, cursor);
        ASSERT_TRUE(batch.has_value());
        EXPECT_EQ(cursor.fragment(), 1U);
        EXPECT_EQ(cursor.offset().value(), 0U);
    }
    {
        scatter_cursor cursor;
        const auto batch = buffer.export_scatter(
          vectors, byte_count{6}, cursor);
        ASSERT_TRUE(batch.has_value());
        EXPECT_EQ(cursor.fragment(), 1U);
        EXPECT_EQ(cursor.offset().value(), 1U);
    }

    scatter_cursor cursor;
    const auto before = cursor;
    const auto zero_budget = buffer.export_scatter(
      vectors, byte_count{}, cursor);
    ASSERT_FALSE(zero_budget.has_value());
    EXPECT_EQ(zero_budget.error(), make_error_code(errc::invalid_argument));
    EXPECT_EQ(cursor, before);

    scatter_cursor foreign_cursor;
    ASSERT_TRUE(buffer.export_scatter(vectors, byte_count{4}, foreign_cursor)
                  .has_value());
    const auto foreign_before = foreign_cursor;
    const fragmented_buffer empty;
    const auto foreign = empty.export_scatter(
      vectors, byte_count{4}, foreign_cursor);
    ASSERT_FALSE(foreign.has_value());
    EXPECT_EQ(foreign.error(), make_error_code(errc::out_of_range));
    EXPECT_EQ(foreign_cursor, foreign_before);
}

TEST(FragmentedBuffer, LinearizationIsExplicitBoundedAndObserved) {
    reset_counters();
    auto buffer = fragmented({"abcd", "efgh"});

    const auto rejected = buffer.linearize(byte_count{7});
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), make_error_code(errc::resource_exhausted));
    EXPECT_EQ(counters().linearize_rejections, 1U);
    EXPECT_EQ(counters().linearizations, 0U);

    const auto exact = buffer.linearize(byte_count{8});
    ASSERT_TRUE(exact.has_value());
    EXPECT_EQ(std::string_view(exact->get(), exact->size()), "abcdefgh");
    EXPECT_EQ(counters().linearizations, 1U);
    EXPECT_EQ(counters().linearized_bytes, 8U);

    std::array<char, 8> destination{};
    const auto copied = buffer.copy_to(destination);
    ASSERT_TRUE(copied.has_value());
    EXPECT_EQ(copied->value(), 8U);
    EXPECT_EQ(std::string_view(destination.data(), 8), "abcdefgh");

    std::array<char, 7> too_small{};
    EXPECT_FALSE(buffer.copy_to(too_small).has_value());
    reset_counters();
}

TEST(FragmentedBuffer, ReleaseToPacketTransfersOwnership) {
    auto buffer = fragmented({"abcd", "efgh"});
    const auto expected_bytes = buffer.size().value();
    const auto expected_fragments = buffer.fragment_count();

    // A share taken beforehand keeps its own claim, so handing the fragments to
    // the transfer type must not invalidate it.
    auto retained = buffer.share();
    ASSERT_TRUE(retained.has_value());

    auto transferred = std::move(buffer).release_to_packet();
    ASSERT_TRUE(transferred.has_value());
    EXPECT_EQ(transferred->len(), expected_bytes);
    EXPECT_EQ(transferred->nr_frags(), expected_fragments);
    // Successful rvalue transfer explicitly leaves the source canonical empty.
    // NOLINTBEGIN(bugprone-use-after-move)
    EXPECT_TRUE(buffer.empty());
    EXPECT_EQ(buffer.fragment_count(), 0U);
    // NOLINTEND(bugprone-use-after-move)
    EXPECT_EQ(contents(*retained), "abcdefgh");
}

TEST(BufferBuilder, RejectsInvalidConfigurationAndUseAfterFinish) {
    fragmented_buffer_builder_config bad;
    bad.initial_fragment_bytes = byte_count{0};
    EXPECT_FALSE(bad.validate().has_value());

    fragmented_buffer_builder_config inverted;
    inverted.initial_fragment_bytes = byte_count{4096};
    inverted.max_fragment_bytes = byte_count{512};
    EXPECT_FALSE(inverted.validate().has_value());

    fragmented_buffer_builder_config fragment_exceeds_total;
    fragment_exceeds_total.max_fragment_bytes = byte_count{2048};
    fragment_exceeds_total.max_total_bytes = byte_count{1024};
    EXPECT_FALSE(fragment_exceeds_total.validate().has_value());

    fragmented_buffer_builder_config oversized_total;
    oversized_total.max_total_bytes = byte_count{
      max_builder_total_bytes.value() + 1};
    EXPECT_FALSE(oversized_total.validate().has_value());

    fragmented_buffer_builder_config maximum_total;
    maximum_total.max_total_bytes = max_builder_total_bytes;
    EXPECT_TRUE(maximum_total.validate().has_value());

    EXPECT_TRUE(fragmented_buffer_builder_config{}.validate().has_value());

    // Lowering the fragment ceiling is coherent on its own. The packing
    // threshold is not part of the configuration, so there is no second field
    // that has to be kept in step with it.
    fragmented_buffer_builder_config lowered;
    lowered.initial_fragment_bytes = byte_count{16};
    lowered.max_fragment_bytes = byte_count{64};
    EXPECT_TRUE(lowered.validate().has_value());

    fragmented_buffer_builder builder;
    ASSERT_TRUE(builder.append(std::string_view{"abc"}).has_value());
    auto published = builder.finish();
    ASSERT_TRUE(published.has_value());
    EXPECT_TRUE(published->content_equals("abc"));

    // finish() is one-way: the builder accepts nothing more.
    EXPECT_FALSE(builder.append(std::string_view{"d"}).has_value());
    EXPECT_FALSE(builder.reserve(byte_count{4}).has_value());
    EXPECT_FALSE(builder.finish().has_value());
}

TEST(BufferBuilder, MoveTransfersStateAndLeavesSourceFinished) {
    fragmented_buffer_builder_config config;
    config.initial_fragment_bytes = byte_count{8};
    config.max_fragment_bytes = byte_count{16};
    config.max_total_bytes = byte_count{64};
    fragmented_buffer_builder source{config};
    ASSERT_TRUE(source.append(std::string_view{"abc"}).has_value());

    auto moved = std::move(source);
    // The builder guarantees a canonical, finished moved-from state.
    // NOLINTBEGIN(bugprone-use-after-move)
    EXPECT_TRUE(source.empty());
    EXPECT_EQ(source.size().value(), 0U);
    EXPECT_EQ(source.fragment_count(), 0U);
    EXPECT_EQ(source.tail_capacity().value(), 0U);
    EXPECT_TRUE(source.finished());
    const auto source_append = source.append(std::string_view{"x"});
    ASSERT_FALSE(source_append.has_value());
    EXPECT_EQ(source_append.error(), make_error_code(errc::closed));
    // NOLINTEND(bugprone-use-after-move)

    EXPECT_EQ(moved.size().value(), 3U);
    EXPECT_EQ(moved.fragment_count(), 1U);
    EXPECT_EQ(moved.tail_capacity().value(), 5U);
    EXPECT_FALSE(moved.finished());
    ASSERT_TRUE(moved.append(std::string_view{"de"}).has_value());

    fragmented_buffer_builder assigned;
    assigned = std::move(moved);
    // Move assignment provides the same canonical source state.
    // NOLINTBEGIN(bugprone-use-after-move)
    EXPECT_TRUE(moved.empty());
    EXPECT_EQ(moved.size().value(), 0U);
    EXPECT_EQ(moved.fragment_count(), 0U);
    EXPECT_EQ(moved.tail_capacity().value(), 0U);
    EXPECT_TRUE(moved.finished());
    // NOLINTEND(bugprone-use-after-move)

    EXPECT_EQ(assigned.size().value(), 5U);
    EXPECT_EQ(assigned.fragment_count(), 1U);
    EXPECT_EQ(assigned.tail_capacity().value(), 3U);
    EXPECT_FALSE(assigned.finished());
    auto published = assigned.finish();
    ASSERT_TRUE(published.has_value());
    EXPECT_TRUE(published->content_equals("abcde"));
}

TEST(BufferBuilder, TinyAppendsReuseTheTailInsteadOfGrowingLinks) {
    fragmented_buffer_builder_config config;
    config.initial_fragment_bytes = byte_count{16};
    config.max_fragment_bytes = byte_count{64};
    fragmented_buffer_builder builder{config};

    // 200 adversarial one-byte appends must not produce 200 fragments.
    for (std::size_t index = 0; index < 200; ++index) {
        ASSERT_TRUE(builder.append(std::string_view{"x"}).has_value());
    }
    EXPECT_EQ(builder.size().value(), 200U);
    auto published = builder.finish();
    ASSERT_TRUE(published.has_value());
    EXPECT_EQ(published->size().value(), 200U);
    // Half-again growth, each step rounded to what the allocator serves:
    // 16 + 32 + 48 + 64 covers 160, and the last fragment is trimmed to 40.
    EXPECT_EQ(published->fragment_count(), 5U);
    EXPECT_EQ(
      fragment_sizes(*published),
      (std::vector<std::size_t>{16, 32, 48, 64, 40}));
    EXPECT_EQ(contents(*published), std::string(200, 'x'));

    // Tiny owned fragments follow the same bounded packing policy even when
    // the builder starts without a tail allocation.
    fragmented_buffer_builder donated_builder{config};
    for (std::size_t index = 0; index < 200; ++index) {
        ASSERT_TRUE(
          donated_builder.append_fragment(fragment_of("x")).has_value());
    }
    auto donated = donated_builder.finish();
    ASSERT_TRUE(donated.has_value());
    EXPECT_EQ(donated->size().value(), 200U);
    EXPECT_EQ(
      fragment_sizes(*donated), (std::vector<std::size_t>{16, 32, 48, 64, 40}));
    EXPECT_EQ(contents(*donated), std::string(200, 'x'));

    // Published buffers already carry the fragment ownership invariant. Their
    // tiny fragments take the same packing path after one whole-buffer bounds
    // check, and each moved-from source is emptied.
    fragmented_buffer_builder buffered_builder{config};
    for (std::size_t index = 0; index < 200; ++index) {
        auto source = fragmented({"x"});
        ASSERT_TRUE(
          buffered_builder.append_buffer(std::move(source)).has_value());
        // append_buffer consumes and empties every published source.
        // NOLINTNEXTLINE(bugprone-use-after-move)
        EXPECT_TRUE(source.empty());
    }
    auto buffered = buffered_builder.finish();
    ASSERT_TRUE(buffered.has_value());
    EXPECT_EQ(buffered->size().value(), 200U);
    EXPECT_EQ(
      fragment_sizes(*buffered),
      (std::vector<std::size_t>{16, 32, 48, 64, 40}));
    EXPECT_EQ(contents(*buffered), std::string(200, 'x'));
}

TEST(BufferBuilder, LongAppendSplitsAtTheFragmentCeiling) {
    fragmented_buffer_builder_config config;
    config.initial_fragment_bytes = byte_count{8};
    config.max_fragment_bytes = byte_count{16};
    fragmented_buffer_builder builder{config};

    const std::string payload(70, 'q');
    ASSERT_TRUE(builder.append(payload).has_value());
    auto published = builder.finish();
    ASSERT_TRUE(published.has_value());
    EXPECT_EQ(published->size().value(), 70U);
    EXPECT_EQ(contents(*published), payload);
    // No allocation exceeds the ceiling even for one long append. A large
    // request starts at the ceiling rather than the initial size, so it does
    // not pay for the geometric ramp.
    for (const auto fragment : *published) {
        EXPECT_LE(fragment.size(), 16U);
    }
    EXPECT_EQ(
      fragment_sizes(*published),
      (std::vector<std::size_t>{16, 16, 16, 16, 6}));
}

TEST(BufferBuilder, DonatedFragmentsArePackedOrLinked) {
    // Fragments large enough that the packing threshold, not tail capacity, is
    // what decides each case below.
    constexpr std::uint64_t ceiling = 8192;
    fragmented_buffer_builder_config config;
    config.initial_fragment_bytes = byte_count{ceiling};
    config.max_fragment_bytes = byte_count{ceiling};
    fragmented_buffer_builder builder{config};

    ASSERT_TRUE(builder.append(std::string_view{"head"}).has_value());
    EXPECT_EQ(builder.fragment_count(), 1U);

    // At or below the threshold with room in the tail: packed, no new link.
    ASSERT_TRUE(builder.append_fragment(fragment_of("tiny")).has_value());
    EXPECT_EQ(builder.fragment_count(), 1U);

    // Above the threshold: linked without copying, even though it would have
    // fitted the tail. This isolates the threshold from the capacity rule.
    const std::string large(
      fragmented_buffer_builder::pack_copy_threshold.value() + 1, 'L');
    ASSERT_LT(large.size(), ceiling - 8);
    auto donated = fragment_of(large);
    const auto* backing = donated.get();
    ASSERT_TRUE(builder.append_fragment(std::move(donated)).has_value());
    EXPECT_EQ(builder.fragment_count(), 2U);

    // Below the threshold with no spare capacity: open a bounded growth tail
    // and copy instead of creating an exact-size link for every tiny donation.
    auto spill = fragment_of("spill");
    const auto* spill_backing = spill.get();
    ASSERT_TRUE(builder.append_fragment(std::move(spill)).has_value());
    EXPECT_EQ(builder.fragment_count(), 3U);

    auto published = builder.finish();
    ASSERT_TRUE(published.has_value());
    EXPECT_EQ(contents(*published), "headtiny" + large + "spill");
    EXPECT_EQ(published->fragment_count(), 3U);
    // The linked fragment kept its original storage.
    EXPECT_EQ(published->fragment_at(1)->data(), backing);
    EXPECT_NE(published->fragment_at(2)->data(), spill_backing);
    // Sealing the tail exposes only the bytes that were written.
    EXPECT_EQ(published->fragment_at(0)->size(), 8U);
    EXPECT_EQ(published->fragment_at(2)->size(), 5U);
}

TEST(BufferBuilder, AppendBufferSplicesAndEmptiesTheSource) {
    fragmented_buffer_builder builder;
    ASSERT_TRUE(builder.append(std::string_view{"first"}).has_value());

    auto donated = fragmented({"second", "third"});
    const auto donated_size = donated.size().value();
    ASSERT_TRUE(builder.append_buffer(std::move(donated)).has_value());
    // append_buffer consumes and empties the published source.
    // NOLINTBEGIN(bugprone-use-after-move)
    EXPECT_TRUE(donated.empty());
    EXPECT_EQ(donated.fragment_count(), 0U);
    // NOLINTEND(bugprone-use-after-move)

    auto published = builder.finish();
    ASSERT_TRUE(published.has_value());
    EXPECT_EQ(published->size().value(), 5U + donated_size);
    EXPECT_EQ(contents(*published), "firstsecondthird");
}

TEST(BufferBuilder, EnforcesTotalBytesFragmentCountAndReserveBounds) {
    fragmented_buffer_builder_config config;
    config.initial_fragment_bytes = byte_count{4};
    config.max_fragment_bytes = byte_count{4};
    config.max_total_bytes = byte_count{10};
    config.max_fragments = 2;
    fragmented_buffer_builder builder{config};

    ASSERT_TRUE(builder.append(std::string_view{"abcd"}).has_value());
    ASSERT_TRUE(builder.append(std::string_view{"efgh"}).has_value());
    EXPECT_EQ(builder.fragment_count(), 2U);

    // A third fragment would exceed max_fragments; the append is rejected and
    // leaves exactly the content that preceded it.
    const auto capped = builder.append(std::string_view{"i"});
    ASSERT_FALSE(capped.has_value());
    EXPECT_EQ(capped.error(), make_error_code(errc::resource_exhausted));
    EXPECT_EQ(builder.size().value(), 8U);
    EXPECT_EQ(builder.fragment_count(), 2U);

    // Exceeding max_total_bytes is rejected before any allocation.
    const auto oversized = builder.append(std::string_view{"ijk"});
    ASSERT_FALSE(oversized.has_value());
    EXPECT_EQ(oversized.error(), make_error_code(errc::resource_exhausted));

    auto published = builder.finish();
    ASSERT_TRUE(published.has_value());
    EXPECT_EQ(contents(*published), "abcdefgh");

    fragmented_buffer_builder reserving{config};
    EXPECT_FALSE(reserving.reserve(byte_count{5}).has_value());
    ASSERT_TRUE(reserving.reserve(byte_count{4}).has_value());
    EXPECT_EQ(reserving.tail_capacity().value(), 4U);
    ASSERT_TRUE(reserving.append(std::string_view{"ab"}).has_value());
    EXPECT_EQ(reserving.tail_capacity().value(), 2U);
    EXPECT_TRUE(std::move(reserving).finish().has_value());
}

TEST(BufferBuilder, PublishedSlicesSurviveTheBuilderAndOriginal) {
    fragmented_buffer slice;
    std::string expected;
    {
        fragmented_buffer_builder builder;
        ASSERT_TRUE(builder.append(std::string_view{"abcdefghij"}).has_value());
        auto published = builder.finish();
        ASSERT_TRUE(published.has_value());
        auto shared = published->share(byte_count{3}, byte_count{4});
        ASSERT_TRUE(shared.has_value());
        expected = contents(*shared);
        slice = std::move(*shared);
    }
    EXPECT_EQ(contents(slice), expected);
    EXPECT_EQ(contents(slice), "defg");
}

} // namespace kwaque::bytes
