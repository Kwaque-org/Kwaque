#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_builder.h"

#include <seastar/core/deleter.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/packet.hh>

#include <gtest/gtest.h>

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
    auto buffer = fragmented_buffer::copy_from_fragments(fragments);
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

TEST(FragmentedBuffer, EmptyAndSingleFragmentShapes) {
    fragmented_buffer empty;
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

TEST(FragmentedBuffer, EnforcesThePublishedFragmentCeiling) {
    std::vector<fragment_type> too_many;
    too_many.reserve(max_buffer_fragments + 1);
    for (std::size_t index = 0; index <= max_buffer_fragments; ++index) {
        too_many.push_back(fragment_of("x"));
    }
    const auto rejected = fragmented_buffer::copy_from_fragments(too_many);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), make_error_code(errc::resource_exhausted));

    std::vector<fragment_type> exactly_enough;
    exactly_enough.reserve(max_buffer_fragments);
    for (std::size_t index = 0; index < max_buffer_fragments; ++index) {
        exactly_enough.push_back(fragment_of("x"));
    }
    const auto accepted = fragmented_buffer::copy_from_fragments(
      exactly_enough);
    ASSERT_TRUE(accepted.has_value());
    EXPECT_EQ(accepted->size().value(), max_buffer_fragments);
}

TEST(FragmentedBuffer, CopiesExternalStorageIntoImmutableBacking) {
    std::array<char, 4> borrowed{'t', 'e', 's', 't'};

    auto borrowed_single = borrowed_fragment(borrowed);
    const auto single = fragmented_buffer::copy_from_fragment(borrowed_single);
    ASSERT_TRUE(single.has_value());
    EXPECT_TRUE(single->content_equals("test"));

    std::vector<fragment_type> fragments;
    fragments.push_back(fragment_of("owned"));
    fragments.push_back(borrowed_fragment(borrowed));
    const auto multiple = fragmented_buffer::copy_from_fragments(fragments);
    ASSERT_TRUE(multiple.has_value());
    EXPECT_TRUE(multiple->content_equals("ownedtest"));
    EXPECT_EQ(multiple->fragment_count(), 2U);
    borrowed[0] = 'X';
    fragments[0].get_write()[0] = 'Y';
    EXPECT_TRUE(single->content_equals("test"));
    EXPECT_TRUE(multiple->content_equals("ownedtest"));

    fragment_type oversized{maximum_contiguous_allocation_bytes + 1U};
    const auto oversized_freeze = fragmented_buffer::copy_from_fragment(
      oversized);
    ASSERT_FALSE(oversized_freeze.has_value());
    EXPECT_EQ(
      oversized_freeze.error(), make_error_code(errc::resource_exhausted));

    std::array<char, 4> builder_bytes{'t', 'e', 's', 't'};
    fragmented_buffer_builder builder;
    ASSERT_TRUE(builder.append(std::string_view{"head"}).has_value());
    auto borrowed_builder = borrowed_fragment(builder_bytes);
    ASSERT_TRUE(builder.append_fragment_copy(borrowed_builder).has_value());
    builder_bytes[0] = 'X';
    EXPECT_EQ(builder.size().value(), 8U);
    auto published = builder.finish();
    ASSERT_TRUE(published.has_value());
    EXPECT_TRUE(published->content_equals("headtest"));
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
        // Identical backing pointers prove no payload was copied.
        EXPECT_EQ(shared.fragment_at(0)->data(), backing[0]);
        EXPECT_EQ(shared.fragment_at(1)->data(), backing[1]);
        derived = std::move(shared);
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
    EXPECT_EQ(interior->retained_bytes().value(), 12U);
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
    EXPECT_EQ(contents(copied), "abcdefgh");
    EXPECT_EQ(copied.fragment_count(), 1U);
    EXPECT_EQ(copied.retained_bytes(), copied.size());
    EXPECT_NE(copied.fragment_at(0)->data(), source.fragment_at(0)->data());
    EXPECT_TRUE(source.content_equals(copied));

    std::vector<fragment_type> tiny_fragments;
    tiny_fragments.reserve(1000);
    for (std::size_t index = 0; index < 1000; ++index) {
        tiny_fragments.push_back(fragment_of("0123456789"));
    }
    auto fragmented_source = fragmented_buffer::copy_from_fragments(
      tiny_fragments);
    ASSERT_TRUE(fragmented_source.has_value());
    ASSERT_EQ(fragmented_source->fragment_count(), 1000U);
    auto coalesced = fragmented_source->copy();
    EXPECT_EQ(coalesced.fragment_count(), 1U);
    EXPECT_EQ(coalesced.size().value(), 10'000U);
    EXPECT_EQ(coalesced.retained_bytes(), coalesced.size());
    EXPECT_TRUE(fragmented_source->content_equals(coalesced));

    const std::string large(200UL * 1024UL, 'L');
    auto large_source = fragmented_buffer::copy_of(
      std::span<const char>{large.data(), large.size()});
    ASSERT_TRUE(large_source.has_value());
    EXPECT_EQ(
      fragment_sizes(*large_source),
      (std::vector<std::size_t>{128UL * 1024UL, 72UL * 1024UL}));
    auto chunked = large_source->copy();
    EXPECT_EQ(
      fragment_sizes(chunked),
      (std::vector<std::size_t>{128UL * 1024UL, 64UL * 1024UL, 8UL * 1024UL}));
    EXPECT_TRUE(large_source->content_equals(chunked));
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
        std::array<scatter_segment, 3> vectors{};
        scatter_cursor cursor;
        const auto batch = buffer.export_scatter(
          vectors.size(), buffer.size(), cursor);
        ASSERT_TRUE(batch.has_value());
        EXPECT_EQ(batch->vector_count(), 3U);
        EXPECT_EQ(batch->bytes().value(), 23U);
        EXPECT_TRUE(batch->complete());
        EXPECT_EQ(
          std::string_view((*batch)[0].data, (*batch)[0].size), "hello");
        EXPECT_EQ(
          std::string_view((*batch)[1].data, (*batch)[1].size), "goodbye");
        EXPECT_EQ(
          std::string_view((*batch)[2].data, (*batch)[2].size), "hello again");
        // Exported vectors borrow the buffer's storage; nothing was copied.
        EXPECT_EQ((*batch)[0].data, buffer.fragment_at(0)->data());
    }

    // One below the cap: the batch stops short and the cursor resumes exactly.
    {
        std::array<scatter_segment, 2> vectors{};
        scatter_cursor cursor;
        const auto first = buffer.export_scatter(
          vectors.size(), buffer.size(), cursor);
        ASSERT_TRUE(first.has_value());
        EXPECT_EQ(first->vector_count(), 2U);
        EXPECT_EQ(first->bytes().value(), 12U);
        EXPECT_FALSE(first->complete());

        const auto second = buffer.export_scatter(
          vectors.size(), buffer.size(), cursor);
        ASSERT_TRUE(second.has_value());
        EXPECT_EQ(second->vector_count(), 1U);
        EXPECT_EQ(second->bytes().value(), 11U);
        EXPECT_TRUE(second->complete());

        const auto third = buffer.export_scatter(
          vectors.size(), buffer.size(), cursor);
        ASSERT_TRUE(third.has_value());
        EXPECT_EQ(third->vector_count(), 0U);
        EXPECT_TRUE(third->complete());
    }

    // One above the cap: the spare entry is left untouched.
    {
        std::array<scatter_segment, 4> vectors{};
        scatter_cursor cursor;
        const auto batch = buffer.export_scatter(
          vectors.size(), buffer.size(), cursor);
        ASSERT_TRUE(batch.has_value());
        EXPECT_EQ(batch->vector_count(), 3U);
        EXPECT_TRUE(batch->complete());
    }

    // A single-entry cap forces one batch per fragment and still reassembles.
    {
        std::array<scatter_segment, 1> vectors{};
        scatter_cursor cursor;
        std::string reassembled;
        std::size_t batches = 0;
        while (true) {
            const auto batch = buffer.export_scatter(
              vectors.size(), buffer.size(), cursor);
            ASSERT_TRUE(batch.has_value());
            for (std::size_t index = 0; index < batch->vector_count();
                 ++index) {
                reassembled.append((*batch)[index].data, (*batch)[index].size);
            }
            ++batches;
            if (batch->complete()) {
                break;
            }
        }
        EXPECT_EQ(reassembled, whole);
        EXPECT_EQ(batches, 3U);
    }

    std::array<scatter_segment, 0> none{};
    scatter_cursor cursor;
    EXPECT_FALSE(
      buffer.export_scatter(none.size(), buffer.size(), cursor).has_value());

    auto lifetime_source = fragmented({"owned", "batch"});
    scatter_cursor lifetime_cursor;
    auto owning_batch = lifetime_source.export_scatter(
      2, lifetime_source.size(), lifetime_cursor);
    ASSERT_TRUE(owning_batch.has_value());
    ASSERT_TRUE(lifetime_source.trim_front(lifetime_source.size()).has_value());
    std::string retained;
    for (const auto segment : *owning_batch) {
        retained.append(segment.data, segment.size);
    }
    EXPECT_EQ(retained, "ownedbatch");
}

TEST(FragmentedBuffer, ScatterExportRespectsByteCapInsideFragments) {
    auto buffer = fragmented({"abcde", "fghij", "klmno"});
    const std::string whole = "abcdefghijklmno";

    // Exercise one byte below, exactly at, and one byte above a fragment
    // boundary. Enough vector slots are supplied so only the byte budget binds.
    for (const std::uint64_t byte_cap : {4U, 5U, 6U}) {
        std::array<scatter_segment, 4> vectors{};
        scatter_cursor cursor;
        std::string reassembled;
        std::size_t batches = 0;
        while (true) {
            const auto batch = buffer.export_scatter(
              vectors.size(), byte_count{byte_cap}, cursor);
            ASSERT_TRUE(batch.has_value()) << "byte_cap=" << byte_cap;
            EXPECT_LE(batch->bytes().value(), byte_cap);
            for (std::size_t index = 0; index < batch->vector_count();
                 ++index) {
                EXPECT_LE((*batch)[index].size, byte_cap);
                reassembled.append((*batch)[index].data, (*batch)[index].size);
            }
            ++batches;
            if (batch->complete()) {
                break;
            }
        }
        EXPECT_EQ(reassembled, whole) << "byte_cap=" << byte_cap;
        EXPECT_EQ(batches, (whole.size() + byte_cap - 1) / byte_cap);
    }

    std::array<scatter_segment, 4> vectors{};
    {
        scatter_cursor cursor;
        const auto batch = buffer.export_scatter(
          vectors.size(), byte_count{4}, cursor);
        ASSERT_TRUE(batch.has_value());
        EXPECT_EQ(cursor.fragment(), 0U);
        EXPECT_EQ(cursor.offset().value(), 4U);
    }
    {
        scatter_cursor cursor;
        const auto batch = buffer.export_scatter(
          vectors.size(), byte_count{5}, cursor);
        ASSERT_TRUE(batch.has_value());
        EXPECT_EQ(cursor.fragment(), 1U);
        EXPECT_EQ(cursor.offset().value(), 0U);
    }
    {
        scatter_cursor cursor;
        const auto batch = buffer.export_scatter(
          vectors.size(), byte_count{6}, cursor);
        ASSERT_TRUE(batch.has_value());
        EXPECT_EQ(cursor.fragment(), 1U);
        EXPECT_EQ(cursor.offset().value(), 1U);
    }

    scatter_cursor cursor;
    const auto before = cursor;
    const auto zero_budget = buffer.export_scatter(
      vectors.size(), byte_count{}, cursor);
    ASSERT_FALSE(zero_budget.has_value());
    EXPECT_EQ(zero_budget.error(), make_error_code(errc::invalid_argument));
    EXPECT_EQ(cursor, before);

    scatter_cursor foreign_cursor;
    ASSERT_TRUE(
      buffer.export_scatter(vectors.size(), byte_count{4}, foreign_cursor)
        .has_value());
    const auto foreign_before = foreign_cursor;
    fragmented_buffer empty;
    const auto foreign = empty.export_scatter(
      vectors.size(), byte_count{4}, foreign_cursor);
    ASSERT_FALSE(foreign.has_value());
    EXPECT_EQ(foreign.error(), make_error_code(errc::invalid_argument));
    EXPECT_EQ(foreign_cursor, foreign_before);

    auto same_shape = fragmented({"12345", "67890", "abcde"});
    EXPECT_FALSE(
      same_shape.export_scatter(vectors.size(), byte_count{4}, foreign_cursor)
        .has_value());

    scatter_cursor stale;
    ASSERT_TRUE(
      buffer.export_scatter(vectors.size(), byte_count{4}, stale).has_value());
    ASSERT_TRUE(buffer.trim_front(byte_count{1}).has_value());
    EXPECT_FALSE(
      buffer.export_scatter(vectors.size(), byte_count{4}, stale).has_value());
}

TEST(FragmentedBuffer, LinearizationIsExplicitAndBounded) {
    auto buffer = fragmented({"abcd", "efgh"});

    const auto rejected = buffer.linearize(byte_count{7});
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), make_error_code(errc::resource_exhausted));

    const auto exact = buffer.linearize(byte_count{8});
    ASSERT_TRUE(exact.has_value());
    EXPECT_EQ(std::string_view(exact->get(), exact->size()), "abcdefgh");

    std::array<char, 8> destination{};
    const auto copied = buffer.copy_to(destination);
    ASSERT_TRUE(copied.has_value());
    EXPECT_EQ(copied->value(), 8U);
    EXPECT_EQ(std::string_view(destination.data(), 8), "abcdefgh");

    std::array<char, 7> too_small{};
    EXPECT_FALSE(buffer.copy_to(too_small).has_value());

    const std::string large(maximum_contiguous_allocation_bytes + 1U, 'x');
    auto chunked = fragmented_buffer::copy_of(
      std::span<const char>{large.data(), large.size()});
    ASSERT_TRUE(chunked.has_value());
    EXPECT_EQ(chunked->fragment_count(), 2U);
    const auto too_wide = chunked->linearize(chunked->size());
    ASSERT_FALSE(too_wide.has_value());
    EXPECT_EQ(too_wide.error(), make_error_code(errc::resource_exhausted));
}

TEST(FragmentedBuffer, CopyToPacketPreservesPublishedImmutability) {
    auto buffer = fragmented({"abcd", "efgh"});
    const auto expected_bytes = buffer.size().value();
    // A share taken beforehand keeps its own claim, so handing the fragments to
    // the transfer type must not invalidate it.
    auto retained = buffer.share();

    auto transferred = buffer.copy_to_packet();
    ASSERT_TRUE(transferred.has_value());
    EXPECT_EQ(transferred->len(), expected_bytes);
    EXPECT_EQ(transferred->nr_frags(), 1U);
    transferred->frag(0).base[0] = 'X';
    EXPECT_TRUE(buffer.content_equals("abcdefgh"));
    EXPECT_EQ(contents(retained), "abcdefgh");

    std::vector<fragment_type> tiny_fragments;
    tiny_fragments.reserve(1000);
    for (std::size_t index = 0; index < 1000; ++index) {
        tiny_fragments.push_back(fragment_of("0123456789"));
    }
    auto fragmented = fragmented_buffer::copy_from_fragments(tiny_fragments);
    ASSERT_TRUE(fragmented.has_value());
    auto coalesced_packet = fragmented->copy_to_packet();
    ASSERT_TRUE(coalesced_packet.has_value());
    EXPECT_EQ(coalesced_packet->len(), 10'000U);
    EXPECT_EQ(coalesced_packet->nr_frags(), 1U);

    fragmented_buffer empty;
    auto empty_packet = empty.copy_to_packet();
    ASSERT_TRUE(empty_packet.has_value());
    EXPECT_EQ(empty_packet->len(), 0U);
    EXPECT_EQ(empty_packet->nr_frags(), 0U);

    const std::string large(maximum_contiguous_allocation_bytes + 1U, 'p');
    auto large_buffer = fragmented_buffer::copy_of(
      std::span<const char>{large.data(), large.size()});
    ASSERT_TRUE(large_buffer.has_value());
    auto large_packet = large_buffer->copy_to_packet();
    ASSERT_TRUE(large_packet.has_value());
    ASSERT_EQ(large_packet->nr_frags(), 2U);
    for (unsigned index = 0; index < large_packet->nr_frags(); ++index) {
        EXPECT_LE(
          large_packet->frag(index).size, maximum_contiguous_allocation_bytes);
    }
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
    maximum_total.max_retained_bytes = max_builder_total_bytes;
    EXPECT_TRUE(maximum_total.validate().has_value());

    fragmented_buffer_builder_config retained_below_logical;
    retained_below_logical.max_retained_bytes = byte_count{1024};
    EXPECT_FALSE(retained_below_logical.validate().has_value());

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
    EXPECT_EQ(published->retained_bytes().value(), 224U);
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
          donated_builder.append_fragment_copy(fragment_of("x")).has_value());
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

TEST(BufferBuilder, ExternalFragmentsAreFrozenThenPackedOrLinked) {
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
    ASSERT_TRUE(builder.append_fragment_copy(fragment_of("tiny")).has_value());
    EXPECT_EQ(builder.fragment_count(), 1U);

    // Above the threshold: copied once into frozen backing and linked, even
    // though it would have fitted the tail.
    const std::string large(
      fragmented_buffer_builder::pack_copy_threshold.value() + 1, 'L');
    ASSERT_LT(large.size(), ceiling - 8);
    auto donated = fragment_of(large);
    const auto* backing = donated.get();
    ASSERT_TRUE(builder.append_fragment_copy(donated).has_value());
    EXPECT_EQ(builder.fragment_count(), 2U);

    // Below the threshold with no spare capacity: open a bounded growth tail
    // and copy instead of creating an exact-size link for every tiny donation.
    auto spill = fragment_of("spill");
    const auto* spill_backing = spill.get();
    ASSERT_TRUE(builder.append_fragment_copy(spill).has_value());
    EXPECT_EQ(builder.fragment_count(), 3U);

    auto published = builder.finish();
    ASSERT_TRUE(published.has_value());
    EXPECT_EQ(contents(*published), "headtiny" + large + "spill");
    EXPECT_EQ(published->fragment_count(), 3U);
    // Neither linked nor packed external storage remains writable through the
    // caller's original aliases.
    EXPECT_NE(published->fragment_at(1)->data(), backing);
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

    fragmented_buffer_builder_config config;
    config.max_fragment_bytes = byte_count{8192};
    config.max_total_bytes = byte_count{16384};
    fragmented_buffer_builder zero_copy_builder{config};
    const std::string large(
      fragmented_buffer_builder::pack_copy_threshold.value() + 1, 'z');
    auto frozen = fragmented_buffer::copy_of(
      std::span<const char>{large.data(), large.size()});
    ASSERT_TRUE(frozen.has_value());
    const auto* backing = frozen->fragment_at(0)->data();
    ASSERT_TRUE(
      zero_copy_builder.append_buffer(std::move(*frozen)).has_value());
    // The move contract explicitly canonicalizes the donated source.
    // NOLINTBEGIN(bugprone-use-after-move)
    EXPECT_TRUE(frozen->empty());
    EXPECT_EQ(frozen->fragment_count(), 0U);
    // NOLINTEND(bugprone-use-after-move)
    auto zero_copy = zero_copy_builder.finish();
    ASSERT_TRUE(zero_copy.has_value());
    EXPECT_EQ(zero_copy->fragment_at(0)->data(), backing);
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

    fragmented_buffer_builder_config retained_config;
    retained_config.initial_fragment_bytes = byte_count{16};
    retained_config.max_fragment_bytes = byte_count{16};
    // The second logical byte is within max_total_bytes, but satisfying it
    // would retain a second 16-byte allocation and exceed the backing cap.
    retained_config.max_total_bytes = byte_count{17};
    retained_config.max_retained_bytes = byte_count{17};
    fragmented_buffer_builder retained_builder{retained_config};
    ASSERT_TRUE(retained_builder.append(std::string(16, 'r')).has_value());
    const auto retained_rejected = retained_builder.append(
      std::string_view{"x"});
    ASSERT_FALSE(retained_rejected.has_value());
    EXPECT_EQ(
      retained_rejected.error(), make_error_code(errc::resource_exhausted));
    EXPECT_EQ(retained_builder.size().value(), 16U);
    EXPECT_EQ(retained_builder.retained_bytes().value(), 16U);
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
