#include "src/base/error.h"
#include "src/base/units.h"
#include "src/bytes/fragmented_buffer.h"
#include "src/bytes/fragmented_buffer_builder.h"
#include "src/bytes/fragmented_buffer_parser.h"
#include "src/codec/collection.h"
#include "src/codec/cooperative.h"
#include "src/codec/error.h"
#include "src/codec/integer.h"
#include "src/codec/limits.h"
#include "src/codec/transaction.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <new>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace {

namespace codec = kwaque::codec;
using kwaque::byte_count;
using kwaque::errc;
using kwaque::item_count;
using kwaque::bytes::fragmented_buffer;
using kwaque::bytes::fragmented_buffer_parser;
using namespace std::literals;

constexpr codec::field_context context{.origin = 100, .family = 7, .field = 11};
constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();

// A conservative test allocation profile, not a production allocator default.
byte_count test_charge(byte_count requested) noexcept {
    if (requested.value() == 0) {
        return {};
    }
    const auto bytes = std::max(requested.value(), std::uint64_t{16});
    if (bytes > (std::uint64_t{1} << 62U)) {
        return byte_count{maximum};
    }
    return byte_count{2U * std::bit_ceil(bytes)};
}

codec::decode_budget generous_budget() noexcept {
    return {
      byte_count{64U * 1024U * 1024U}, byte_count{1024U * 1024U}, test_charge};
}

struct entry final {
    std::uint32_t key;
    std::uint32_t value;

    bool operator==(const entry&) const noexcept = default;
};

struct entry_less final {
    bool operator()(const auto& left, const auto& right) const noexcept {
        return left.key < right.key;
    }
};

template<typename Entry, std::size_t ChunkEntries>
std::vector<entry>
entries_of(const seastar::chunked_fifo<Entry, ChunkEntries>& input) {
    std::vector<entry> values;
    values.reserve(input.size());
    for (const auto& value : input) {
        values.push_back(entry{value.key, value.value});
    }
    return values;
}

template<std::size_t ChunkEntries = 16>
seastar::chunked_fifo<entry, ChunkEntries> unordered_input() {
    seastar::chunked_fifo<entry, ChunkEntries> source;
    for (const auto value : std::array{
           entry{12, 120},
           entry{5, 50},
           entry{2, 20},
           entry{14, 140},
           entry{9, 90},
           entry{1, 10}}) {
        source.push_back(value);
    }
    return source;
}

constexpr std::array sorted_entries{
  entry{1, 10},
  entry{2, 20},
  entry{5, 50},
  entry{9, 90},
  entry{12, 120},
  entry{14, 140}};

TEST(CollectionTest, UnorderedConstructionReturnsEveryEntryAscending) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto source = unordered_input();
    auto result
      = codec::canonicalize_unordered(
          std::move(source), generous_budget(), work, entry_less{}, context)
          .get();
    ASSERT_TRUE(result.has_value());
    // Successful construction transfers ownership and empties the source.
    // NOLINTNEXTLINE(bugprone-use-after-move)
    EXPECT_TRUE(source.empty());
    EXPECT_EQ(
      entries_of(*result),
      (std::vector<entry>{sorted_entries.begin(), sorted_entries.end()}));
}

TEST(CollectionTest, UnorderedResultOutlivesItsSourceOwner) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto result = [&] {
        auto source = unordered_input();
        return codec::canonicalize_unordered(
                 std::move(source),
                 generous_budget(),
                 work,
                 entry_less{},
                 context)
          .get();
    }();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(
      entries_of(*result),
      (std::vector<entry>{sorted_entries.begin(), sorted_entries.end()}));
}

TEST(CollectionTest, NarrowWorkQuantaStillProduceTheCompleteOrdering) {
    auto config = codec::limits::defaults().config();
    config.max_work_bytes = byte_count{4096};
    config.max_work_items = item_count{32};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    auto source = unordered_input();
    auto result
      = codec::canonicalize_unordered(
          std::move(source), generous_budget(), work, entry_less{}, context)
          .get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(
      entries_of(*result),
      (std::vector<entry>{sorted_entries.begin(), sorted_entries.end()}));
}

TEST(CollectionTest, EmptyConstructionNeedsNoEntryAllocationAllowance) {
    auto config = codec::limits::defaults().config();
    config.max_allocation_bytes = byte_count{1};
    config.max_metadata_bytes = byte_count{1};
    config.max_operation_bytes = byte_count{1};
    const auto policy = codec::limits::make(config).value();
    seastar::abort_source abort;
    codec::cooperative_work work{policy, abort};
    seastar::chunked_fifo<entry, 16> source;
    auto budget = generous_budget();
    budget.operation_remaining = byte_count{};
    budget.metadata_remaining = byte_count{};
    const auto before = work.items_remaining();
    auto result = codec::canonicalize_unordered(
                    std::move(source), budget, work, entry_less{}, context)
                    .get();
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->empty());
    EXPECT_LT(work.items_remaining(), before);
}

struct padded_entry final {
    std::uint32_t key;
    std::uint32_t value;
    std::array<char, 509> payload{};
};

static_assert(sizeof(padded_entry) > 512);
static_assert(sizeof(padded_entry) <= 1024);

seastar::chunked_fifo<padded_entry, 1> one_entry_runs_input(bool duplicate) {
    seastar::chunked_fifo<padded_entry, 1> source;
    for (std::uint32_t index = 0; index < 16; ++index) {
        // Multiplication by an odd number permutes all four-bit keys.
        const auto key = (index * 5U) % 16U;
        source.emplace_back(
          padded_entry{duplicate && index == 15 ? 0U : key, key + 1000U, {}});
    }
    return source;
}

codec::limits one_entry_run_policy() {
    auto config = codec::limits::defaults().config();
    config.max_allocation_bytes = byte_count{2048};
    return codec::limits::make(config).value();
}

TEST(CollectionTest, SixteenSingleEntryRunsMergeWithoutTruncation) {
    // One served entry fits, but two entries cannot share a run allocation.
    EXPECT_LE(test_charge(byte_count{sizeof(padded_entry)}), byte_count{2048});
    EXPECT_GT(
      test_charge(byte_count{2U * sizeof(padded_entry)}), byte_count{2048});
    seastar::abort_source abort;
    codec::cooperative_work work{one_entry_run_policy(), abort};
    auto source = one_entry_runs_input(false);
    auto result
      = codec::canonicalize_unordered(
          std::move(source), generous_budget(), work, entry_less{}, context)
          .get();
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 16);
    std::uint32_t expected = 0;
    for (const auto& value : *result) {
        EXPECT_EQ(value.key, expected);
        EXPECT_EQ(value.value, expected + 1000U);
        ++expected;
    }
    EXPECT_EQ(expected, 16);
}

TEST(CollectionTest, DuplicateKeysInDifferentSingleEntryRunsReject) {
    seastar::abort_source abort;
    codec::cooperative_work work{one_entry_run_policy(), abort};
    auto source = one_entry_runs_input(true);
    auto result
      = codec::canonicalize_unordered(
          std::move(source), generous_budget(), work, entry_less{}, context)
          .get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), errc::malformed_data);
    EXPECT_EQ(result.error().family(), context.family);
    EXPECT_EQ(result.error().field(), context.field);
    // Duplicate rejection follows ownership transfer, which empties source.
    // NOLINTNEXTLINE(bugprone-use-after-move)
    EXPECT_TRUE(source.empty());
}

TEST(CollectionTest, ParentAndMetadataResidualsCannotBeReplacedByDefaults) {
    for (const bool deny_metadata : {false, true}) {
        SCOPED_TRACE(deny_metadata);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto source = unordered_input();
        auto budget = generous_budget();
        if (deny_metadata) {
            budget.metadata_remaining = byte_count{};
        } else {
            budget.operation_remaining = byte_count{};
        }
        auto result = codec::canonicalize_unordered(
                        std::move(source), budget, work, entry_less{}, context)
                        .get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), errc::resource_exhausted);
        // Budget rejection follows ownership transfer, which empties source.
        // NOLINTNEXTLINE(bugprone-use-after-move)
        EXPECT_TRUE(source.empty());
    }
}

TEST(CollectionTest, NarrowCountCeilingRejectsInsteadOfTruncating) {
    auto config = codec::limits::defaults().config();
    config.max_object_entries = item_count{5};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    auto source = unordered_input();
    auto result
      = codec::canonicalize_unordered(
          std::move(source), generous_budget(), work, entry_less{}, context)
          .get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), errc::resource_exhausted);
    // Count rejection follows ownership transfer, which empties source.
    // NOLINTNEXTLINE(bugprone-use-after-move)
    EXPECT_TRUE(source.empty());
}

TEST(CollectionTest, UnsupportedCleanupShapesLeaveSourceWithCaller) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    seastar::chunked_fifo<entry, 16> source;
    source.reserve(17);
    source.push_back(entry{3, 30});
    const auto reserved = source.nfree_chunks();
    ASSERT_GT(reserved, 0);
    auto result
      = codec::canonicalize_unordered(
          std::move(source), generous_budget(), work, entry_less{}, context)
          .get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), errc::invalid_argument);
    // Unsupported cleanup shapes reject before moving from the source.
    // NOLINTBEGIN(bugprone-use-after-move)
    ASSERT_EQ(source.size(), 1);
    EXPECT_EQ(source.front(), (entry{3, 30}));
    EXPECT_EQ(source.nfree_chunks(), reserved);
    // NOLINTEND(bugprone-use-after-move)
}

TEST(CollectionTest, ImpossibleCleanupQuantumRejectsBeforeOwnershipTransfer) {
    auto config = codec::limits::defaults().config();
    config.max_work_items = item_count{1};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    auto source = unordered_input();
    auto result
      = codec::canonicalize_unordered(
          std::move(source), generous_budget(), work, entry_less{}, context)
          .get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), errc::resource_exhausted);
    // An impossible cleanup quantum rejects before ownership transfer.
    // NOLINTBEGIN(bugprone-use-after-move)
    EXPECT_EQ(source.size(), sorted_entries.size());
    EXPECT_EQ(source.front(), (entry{12, 120}));
    // NOLINTEND(bugprone-use-after-move)
}

struct lifetime_counts final {
    std::size_t live{0};
    std::size_t destroyed{0};
    seastar::promise<>* first_destruction{nullptr};
};

struct tracked_entry final {
    static inline lifetime_counts* counts{nullptr};
    std::uint32_t key;
    std::uint32_t value;

    tracked_entry(std::uint32_t key_value, std::uint32_t payload) noexcept
      : key(key_value)
      , value(payload) {
        ++counts->live;
    }
    tracked_entry(const tracked_entry& other) noexcept
      : tracked_entry(other.key, other.value) {}
    tracked_entry(tracked_entry&& other) noexcept
      : tracked_entry(std::exchange(other.key, 0U), other.value) {}
    tracked_entry& operator=(const tracked_entry&) noexcept = default;
    tracked_entry& operator=(tracked_entry&& other) noexcept {
        key = std::exchange(other.key, 0U);
        value = other.value;
        return *this;
    }
    ~tracked_entry() noexcept {
        --counts->live;
        ++counts->destroyed;
        if (auto* ready = std::exchange(counts->first_destruction, nullptr)) {
            ready->set_value();
        }
    }
};

class track_lifetimes final {
public:
    explicit track_lifetimes(lifetime_counts& counts) noexcept {
        tracked_entry::counts = &counts;
    }
    track_lifetimes(const track_lifetimes&) = delete;
    track_lifetimes& operator=(const track_lifetimes&) = delete;
    ~track_lifetimes() { tracked_entry::counts = nullptr; }
};

seastar::chunked_fifo<tracked_entry, 16> tracked_source(std::uint32_t count) {
    seastar::chunked_fifo<tracked_entry, 16> source;
    for (auto key = count; key != 0; --key) {
        source.emplace_back(key, key + 1000U);
    }
    return source;
}

TEST(CollectionTest, MovedKeysCannotCorruptMergeOrdering) {
    lifetime_counts counts;
    const track_lifetimes tracking{counts};
    {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto source = tracked_source(65);
        auto result
          = codec::canonicalize_unordered(
              std::move(source), generous_budget(), work, entry_less{}, context)
              .get();
        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(result->size(), 65);
        std::uint32_t expected = 1;
        for (const auto& value : *result) {
            EXPECT_EQ(value.key, expected);
            EXPECT_EQ(value.value, expected + 1000U);
            ++expected;
        }
    }
    EXPECT_EQ(counts.live, 0);
}

TEST(CollectionTest, AllocationFailuresFireAndDestroyPrivateEntries) {
#if !defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    GTEST_SKIP() << "allocation injection is disabled in this verified profile";
#else
    bool succeeded = false;
    bool observed_owned_failure = false;
    for (std::uint64_t ordinal = 0; ordinal < 512 && !succeeded; ++ordinal) {
        SCOPED_TRACE(ordinal);
        lifetime_counts counts;
        const track_lifetimes tracking{counts};
        {
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto source = tracked_source(65);
            std::optional<
              codec::result<seastar::chunked_fifo<tracked_entry, 16>>>
              result;
            bool threw = false;
            auto& injector = seastar::memory::local_failure_injector();
            injector.fail_after(ordinal);
            try {
                result.emplace(
                  codec::canonicalize_unordered(
                    std::move(source),
                    generous_budget(),
                    work,
                    entry_less{},
                    context)
                    .get());
            } catch (const std::bad_alloc&) {
                threw = true;
            } catch (...) {
                injector.cancel();
                throw;
            }
            const bool injected = injector.failed();
            injector.cancel();
            if (threw) {
                EXPECT_TRUE(injected);
                EXPECT_FALSE(result.has_value());
                // Inspect the transfer contract: source is either emptied or
                // untouched, depending on whether ownership was acquired.
                // NOLINTBEGIN(bugprone-use-after-move)
                if (source.empty()) {
                    observed_owned_failure = true;
                    EXPECT_EQ(counts.live, 0);
                } else {
                    // Coroutine-frame failure has not transferred the input.
                    EXPECT_EQ(source.size(), 65);
                    EXPECT_EQ(counts.live, source.size());
                }
                // NOLINTEND(bugprone-use-after-move)
            } else {
                ASSERT_TRUE(result.has_value());
                ASSERT_TRUE(result->has_value());
                EXPECT_FALSE(injected);
                EXPECT_EQ((*result)->size(), 65);
                succeeded = true;
            }
        }
        EXPECT_EQ(counts.live, 0);
    }
    EXPECT_TRUE(observed_owned_failure);
    EXPECT_TRUE(succeeded);
#endif
}

TEST(
  CollectionTest,
  AbortedConstructionDrainsEntriesBeforeReturningAndAllowsProgress) {
    bool observed_cleanup_progress = false;
    std::size_t attempts = 0;
    const auto deadline = std::chrono::steady_clock::now()
                          + std::chrono::seconds{2};
    while (!observed_cleanup_progress && attempts < 64
           && std::chrono::steady_clock::now() < deadline) {
        ++attempts;
        lifetime_counts counts;
        const track_lifetimes tracking{counts};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        seastar::promise<> cleanup_started;
        bool operation_active = false;
        auto observer = cleanup_started.get_future().then([&] {
            observed_cleanup_progress = operation_active && counts.live != 0
                                        && counts.destroyed != 0
                                        && abort.abort_requested();
        });
        auto source = tracked_source(65'536);
        // The observer cannot become runnable before an entry is destroyed.
        // Its storage is already allocated when the noexcept destructor fires.
        counts.first_destruction = &cleanup_started;
        abort.request_abort_ex(
          std::make_exception_ptr(std::runtime_error("stop")));
        std::optional<codec::result<seastar::chunked_fifo<tracked_entry, 16>>>
          result;
        std::exception_ptr failure;
        operation_active = true;
        try {
            result.emplace(
              codec::canonicalize_unordered(
                std::move(source),
                generous_budget(),
                work,
                entry_less{},
                context)
                .get());
        } catch (...) {
            failure = std::current_exception();
        }
        operation_active = false;
        if (std::exchange(counts.first_destruction, nullptr) != nullptr) {
            // Frame-allocation failure may leave input with the caller.
            cleanup_started.set_value();
        }
        observer.get();
        if (failure) {
            std::rethrow_exception(failure);
        }
        ASSERT_TRUE(result.has_value());
        ASSERT_FALSE(result->has_value());
        EXPECT_EQ(result->error().code(), errc::aborted);
        EXPECT_EQ(result->error().family(), context.family);
        EXPECT_EQ(result->error().field(), context.field);
        // Cancellation follows ownership transfer, which empties source.
        // NOLINTNEXTLINE(bugprone-use-after-move)
        EXPECT_TRUE(source.empty());
        EXPECT_EQ(counts.live, 0);
    }
    EXPECT_TRUE(observed_cleanup_progress);
    EXPECT_GT(attempts, 0);
}

fragmented_buffer fragmented_input(std::string_view input, bool split = false) {
    if (!split || input.empty()) {
        return fragmented_buffer::copy_of(input).value();
    }
    std::vector<seastar::temporary_buffer<char>> fragments;
    fragments.reserve(input.size());
    for (const auto octet : input) {
        seastar::temporary_buffer<char> fragment{1};
        fragment.get_write()[0] = octet;
        fragments.push_back(std::move(fragment));
    }
    return fragmented_buffer::copy_from_fragments(fragments).value();
}

struct map_observation final {
    codec::decode_budget* expected_memory{nullptr};
    codec::cooperative_work* expected_work{nullptr};
    codec::input_boundary expected_boundary{codec::input_boundary::open};
    bool same_memory{true};
    bool same_work{true};
    bool same_context{true};
    bool same_boundary{true};
    std::size_t keys{0};
    std::size_t values{0};
    std::size_t inserts{0};
    std::size_t staged{0};
    std::array<entry, 8> entries{};
    std::size_t reject_insert{0};
    std::size_t throw_value{0};
    bool reserve_on_key{false};
    seastar::abort_source* abort_on_insert{nullptr};
    std::optional<codec::error> value_error;

    void observe(
      codec::decode_budget& memory, codec::cooperative_work& work) noexcept {
        same_memory = same_memory && &memory == expected_memory;
        same_work = same_work && &work == expected_work;
    }

    void observe(
      codec::field_context field,
      codec::input_boundary boundary,
      codec::decode_budget& memory,
      codec::cooperative_work& work) noexcept {
        observe(memory, work);
        same_context = same_context && field.origin == context.origin
                       && field.family == context.family
                       && field.field == context.field;
        same_boundary = same_boundary && boundary == expected_boundary;
    }
};

constexpr codec::error sink_error{errc::resource_exhausted, 19, 23, 987};

struct byte_less final {
    bool operator()(std::uint8_t left, std::uint8_t right) const noexcept {
        return left < right;
    }
};

seastar::future<codec::result<item_count>> read_pairs(
  fragmented_buffer_parser& input,
  codec::decode_budget& memory,
  codec::cooperative_work& work,
  map_observation& seen,
  item_count maximum_count = item_count{8},
  codec::input_boundary boundary = codec::input_boundary::open) {
    seen.expected_memory = &memory;
    seen.expected_work = &work;
    seen.expected_boundary = boundary;
    return codec::read_ordered_map<std::uint8_t, std::uint8_t>(
      input,
      maximum_count,
      memory,
      work,
      [&seen](
        fragmented_buffer_parser& parser,
        codec::field_context field,
        codec::input_boundary end,
        codec::decode_budget& budget,
        codec::cooperative_work& shared_work) -> codec::result<std::uint8_t> {
          ++seen.keys;
          seen.observe(field, end, budget, shared_work);
          if (seen.reserve_on_key) {
              const auto metadata = budget.metadata_remaining.checked_sub(
                byte_count{1});
              const auto operation = budget.operation_remaining.checked_sub(
                byte_count{1});
              if (!metadata || !operation) {
                  return codec::failure(
                    codec::error{
                      errc::resource_exhausted,
                      field.family,
                      field.field,
                      field.origin + parser.bytes_consumed().value()});
              }
              budget.metadata_remaining = *metadata;
              budget.operation_remaining = *operation;
          }
          return codec::read_le<std::uint8_t>(parser, field, end);
      },
      [&seen](
        fragmented_buffer_parser& parser,
        codec::field_context field,
        codec::input_boundary end,
        codec::decode_budget& budget,
        codec::cooperative_work& shared_work) -> codec::result<std::uint8_t> {
          ++seen.values;
          seen.observe(field, end, budget, shared_work);
          if (seen.values == seen.throw_value) {
              throw std::runtime_error("value callback failed");
          }
          if (seen.value_error) {
              return codec::failure(*seen.value_error);
          }
          return codec::read_le<std::uint8_t>(parser, field, end);
      },
      [&seen](
        std::uint8_t&& key,
        std::uint8_t&& value,
        codec::decode_budget& budget,
        codec::cooperative_work& shared_work) -> codec::result<void> {
          ++seen.inserts;
          seen.observe(budget, shared_work);
          if (seen.inserts == seen.reject_insert) {
              return codec::failure(sink_error);
          }
          seen.entries[seen.staged++] = entry{key, value};
          if (seen.abort_on_insert != nullptr) {
              seen.abort_on_insert->request_abort();
          }
          return {};
      },
      byte_less{},
      byte_count{64},
      item_count{16},
      context,
      boundary);
}

TEST(
  CollectionTest, OrderedLiteralPairsConsumeOneCountAndPreserveFollowingBytes) {
    constexpr auto wire = "p\x03\x01\x11\x03\x33\x05\x55\x7f"sv;
    for (const bool split : {false, true}) {
        SCOPED_TRACE(split);
        fragmented_buffer_parser input{fragmented_input(wire, split)};
        ASSERT_TRUE(input.push_checkpoint().has_value());
        ASSERT_TRUE(input.skip(byte_count{1}).has_value());
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto memory = generous_budget();
        map_observation seen;
        const auto result = read_pairs(input, memory, work, seen).get();
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(*result, item_count{3});
        EXPECT_EQ(input.bytes_consumed(), byte_count{8});
        EXPECT_EQ(input.bytes_remaining(), byte_count{1});
        EXPECT_EQ(input.checkpoint_depth(), 1);
        EXPECT_EQ(seen.keys, 3);
        EXPECT_EQ(seen.values, 3);
        ASSERT_EQ(seen.staged, 3);
        EXPECT_EQ(seen.entries[0], (entry{1, 0x11}));
        EXPECT_EQ(seen.entries[1], (entry{3, 0x33}));
        EXPECT_EQ(seen.entries[2], (entry{5, 0x55}));
        EXPECT_TRUE(seen.same_memory);
        EXPECT_TRUE(seen.same_work);
        EXPECT_TRUE(seen.same_context);
        EXPECT_TRUE(seen.same_boundary);
        EXPECT_EQ(codec::read_le<std::uint8_t>(input).value(), 0x7f);
        ASSERT_TRUE(input.rollback().has_value());
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
    }
}

TEST(CollectionTest, EmptyOrderedCountInvokesNoElementOrSinkCallback) {
    fragmented_buffer_parser input{fragmented_input("\0z"sv)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto memory = generous_budget();
    map_observation seen;
    const auto result
      = read_pairs(input, memory, work, seen, item_count{}).get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, item_count{});
    EXPECT_EQ(input.bytes_consumed(), byte_count{1});
    EXPECT_EQ(seen.keys, 0);
    EXPECT_EQ(seen.values, 0);
    EXPECT_EQ(seen.inserts, 0);
}

TEST(
  CollectionTest, InvalidCountRejectsBeforeElementCallbacksAndRestoresCursor) {
    struct count_case final {
        std::string_view wire;
        item_count maximum_count;
        errc expected;
    };
    constexpr std::array cases{
      count_case{"\x04"sv, item_count{3}, errc::resource_exhausted},
      count_case{"\x80\x00"sv, item_count{8}, errc::malformed_data},
      count_case{"\x80"sv, item_count{8}, errc::truncated_data}};
    for (const auto& test : cases) {
        fragmented_buffer_parser input{fragmented_input(test.wire)};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto memory = generous_budget();
        map_observation seen;
        const auto result
          = read_pairs(input, memory, work, seen, test.maximum_count).get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), test.expected);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        EXPECT_EQ(input.checkpoint_depth(), 0);
        EXPECT_EQ(seen.keys, 0);
        EXPECT_EQ(seen.values, 0);
        EXPECT_EQ(seen.inserts, 0);
    }
}

TEST(CollectionTest, DuplicateAndDescendingKeysRejectBeforeMissingValue) {
    for (const auto wire : {"\x02\x04\x44\x04"sv, "\x02\x04\x44\x03"sv}) {
        fragmented_buffer_parser input{fragmented_input(wire, true)};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto memory = generous_budget();
        map_observation seen;
        const auto result = read_pairs(input, memory, work, seen).get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), errc::malformed_data);
        EXPECT_EQ(result.error().family(), context.family);
        EXPECT_EQ(result.error().field(), context.field);
        EXPECT_EQ(seen.keys, 2);
        EXPECT_EQ(seen.values, 1);
        EXPECT_EQ(seen.inserts, 0);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        EXPECT_EQ(input.checkpoint_depth(), 0);
    }
}

TEST(CollectionTest, NearEndInvalidOrderRetainsOnlyPrivateAcceptedPrefix) {
    fragmented_buffer_parser input{
      fragmented_input("\x03\x01\x11\x03\x33\x03"sv)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto memory = generous_budget();
    map_observation seen;
    const auto result = read_pairs(input, memory, work, seen).get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), errc::malformed_data);
    EXPECT_EQ(seen.keys, 3);
    EXPECT_EQ(seen.values, 2);
    EXPECT_EQ(seen.inserts, 1);
    ASSERT_EQ(seen.staged, 1);
    EXPECT_EQ(seen.entries[0], (entry{1, 0x11}));
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
    EXPECT_EQ(input.checkpoint_depth(), 0);
}

TEST(CollectionTest, FallibleSinkErrorAndPrivatePrefixArePreserved) {
    fragmented_buffer_parser input{
      fragmented_input("\x03\x01\x11\x03\x33\x05\x55"sv)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto memory = generous_budget();
    map_observation seen;
    seen.reject_insert = 2;
    const auto result = read_pairs(input, memory, work, seen).get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), sink_error);
    EXPECT_EQ(seen.inserts, 2);
    ASSERT_EQ(seen.staged, 1);
    EXPECT_EQ(seen.entries[0], (entry{1, 0x11}));
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
    EXPECT_EQ(input.checkpoint_depth(), 0);
}

TEST(CollectionTest, CallbackReservationsRemainSharedAcrossEntriesAndFailure) {
    fragmented_buffer_parser input{
      fragmented_input("\x03\x01\x11\x03\x33\x05\x55"sv)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto memory = generous_budget();
    memory.operation_remaining = byte_count{2};
    memory.metadata_remaining = byte_count{2};
    map_observation seen;
    seen.reserve_on_key = true;
    const auto result = read_pairs(input, memory, work, seen).get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), errc::resource_exhausted);
    EXPECT_EQ(memory.operation_remaining, byte_count{});
    EXPECT_EQ(memory.metadata_remaining, byte_count{});
    EXPECT_EQ(seen.keys, 3);
    EXPECT_EQ(seen.values, 2);
    EXPECT_EQ(seen.staged, 1);
    EXPECT_TRUE(seen.same_memory);
    EXPECT_TRUE(seen.same_work);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
}

struct moved_key final {
    std::uint8_t value;

    explicit moved_key(std::uint8_t key) noexcept
      : value(key) {}
    moved_key(const moved_key&) noexcept = default;
    moved_key(moved_key&& other) noexcept
      : value(std::exchange(other.value, std::uint8_t{})) {}
    moved_key& operator=(const moved_key&) noexcept = default;
    moved_key& operator=(moved_key&& other) noexcept {
        value = std::exchange(other.value, std::uint8_t{});
        return *this;
    }
};

TEST(CollectionTest, OrderedDuplicateCheckRetainsPreviousKeyAcrossSinkMoves) {
    fragmented_buffer_parser input{
      fragmented_input("\x03\x01\x11\x03\x33\x03"sv)};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto memory = generous_budget();
    std::size_t values_read = 0;
    std::size_t inserts = 0;
    std::uint8_t inserted_key = 0;
    const auto result
      = codec::read_ordered_map<moved_key, std::uint8_t>(
          input,
          item_count{8},
          memory,
          work,
          [](
            fragmented_buffer_parser& parser,
            codec::field_context field,
            codec::input_boundary boundary,
            codec::decode_budget&,
            codec::cooperative_work&) -> codec::result<moved_key> {
              const auto value = codec::read_le<std::uint8_t>(
                parser, field, boundary);
              if (!value) {
                  return codec::failure(value.error());
              }
              return moved_key{*value};
          },
          [&values_read](
            fragmented_buffer_parser& parser,
            codec::field_context field,
            codec::input_boundary boundary,
            codec::decode_budget&,
            codec::cooperative_work&) {
              ++values_read;
              return codec::read_le<std::uint8_t>(parser, field, boundary);
          },
          [&](
            moved_key&& key,
            std::uint8_t&&,
            codec::decode_budget&,
            codec::cooperative_work&) -> codec::result<void> {
              const auto owned_key = std::move(key);
              inserted_key = owned_key.value;
              ++inserts;
              return {};
          },
          [](const moved_key& left, const moved_key& right) noexcept {
              return left.value < right.value;
          },
          byte_count{64},
          item_count{16},
          context)
          .get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), errc::malformed_data);
    EXPECT_EQ(values_read, 2);
    EXPECT_EQ(inserts, 1);
    EXPECT_EQ(inserted_key, 1);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
}

TEST(CollectionTest, CallbackErrorsAndExceptionsRollBackOnlyOwnedParserMark) {
    for (const bool throw_exception : {false, true}) {
        SCOPED_TRACE(throw_exception);
        fragmented_buffer_parser input{fragmented_input("\x01\x02\x22"sv)};
        ASSERT_TRUE(input.push_checkpoint().has_value());
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto memory = generous_budget();
        map_observation seen;
        if (throw_exception) {
            seen.throw_value = 1;
            EXPECT_THROW(
              read_pairs(input, memory, work, seen).get(), std::runtime_error);
        } else {
            seen.value_error = codec::error{errc::out_of_range, 31, 37, 456};
            const auto result = read_pairs(input, memory, work, seen).get();
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error(), *seen.value_error);
        }
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        EXPECT_EQ(input.checkpoint_depth(), 1);
        EXPECT_EQ(seen.inserts, 0);
        ASSERT_TRUE(input.rollback().has_value());
    }
}

TEST(CollectionTest, TruncatedPairsRespectOpenAndCompleteBoundaries) {
    constexpr auto wire = "\x02\x01\x11\x03\x33"sv;
    for (const auto boundary :
         {codec::input_boundary::open, codec::input_boundary::complete}) {
        for (std::size_t length = 0; length < wire.size(); ++length) {
            SCOPED_TRACE(length);
            fragmented_buffer_parser input{
              fragmented_input(wire.substr(0, length))};
            seastar::abort_source abort;
            codec::cooperative_work work{codec::limits::defaults(), abort};
            auto memory = generous_budget();
            map_observation seen;
            const auto result
              = read_pairs(input, memory, work, seen, item_count{8}, boundary)
                  .get();
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(
              result.error().code(),
              boundary == codec::input_boundary::open ? errc::truncated_data
                                                      : errc::malformed_data);
            EXPECT_EQ(result.error().byte_offset(), context.origin + length);
            EXPECT_EQ(input.bytes_consumed(), byte_count{});
            EXPECT_EQ(input.checkpoint_depth(), 0);
            EXPECT_TRUE(seen.same_boundary);
        }
    }
}

TEST(CollectionTest, EighthOwnedMarkWorksAndNinthFailsBeforeCallbacks) {
    for (const std::size_t depth : {7U, 8U}) {
        fragmented_buffer_parser input{fragmented_input("\0"sv)};
        for (std::size_t index = 0; index < depth; ++index) {
            ASSERT_TRUE(input.push_checkpoint().has_value());
        }
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto memory = generous_budget();
        map_observation seen;
        const auto result = read_pairs(input, memory, work, seen).get();
        EXPECT_EQ(result.has_value(), depth == 7);
        if (!result) {
            EXPECT_EQ(result.error().code(), errc::resource_exhausted);
        }
        EXPECT_EQ(input.bytes_consumed(), byte_count{depth == 7 ? 1U : 0U});
        EXPECT_EQ(input.checkpoint_depth(), depth);
        EXPECT_EQ(seen.keys, 0);
        for (std::size_t index = 0; index < depth; ++index) {
            ASSERT_TRUE(input.rollback().has_value());
        }
    }
}

TEST(CollectionTest, OversizedEntryLeafRejectsBeforeItsCallbacks) {
    auto config = codec::limits::defaults().config();
    config.max_work_bytes = byte_count{63};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::make(config).value(), abort};
    fragmented_buffer_parser input{fragmented_input("\x01\x02\x22"sv)};
    auto memory = generous_budget();
    map_observation seen;
    const auto result = read_pairs(input, memory, work, seen).get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), errc::resource_exhausted);
    EXPECT_EQ(seen.keys, 0);
    EXPECT_EQ(seen.values, 0);
    EXPECT_EQ(input.bytes_consumed(), byte_count{});
}

TEST(CollectionTest, AbortBeforeWorkOrFinalPublicationReturnsTypedFailure) {
    for (const bool late : {false, true}) {
        SCOPED_TRACE(late);
        fragmented_buffer_parser input{fragmented_input("\x01\x02\x22"sv)};
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto memory = generous_budget();
        map_observation seen;
        if (late) {
            seen.abort_on_insert = &abort;
        } else {
            abort.request_abort_ex(
              std::make_exception_ptr(std::runtime_error("stop")));
        }
        const auto result = read_pairs(input, memory, work, seen).get();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code(), errc::aborted);
        EXPECT_EQ(seen.staged, late ? 1 : 0);
        EXPECT_EQ(input.bytes_consumed(), byte_count{});
        EXPECT_EQ(input.checkpoint_depth(), 0);
    }
}

template<typename Range>
seastar::future<codec::result<void>> write_pairs(
  kwaque::bytes::fragmented_buffer_builder& output,
  const Range& entries,
  codec::decode_budget& memory,
  codec::cooperative_work& work,
  std::size_t& values_written) {
    return codec::write_ordered_map(
      output,
      entries,
      item_count{8},
      memory,
      work,
      [](
        kwaque::bytes::fragmented_buffer_builder& builder,
        const std::uint8_t& key,
        codec::field_context field,
        codec::decode_budget&,
        codec::cooperative_work&) {
          return codec::write_le(builder, key, field);
      },
      [&values_written](
        kwaque::bytes::fragmented_buffer_builder& builder,
        const std::uint8_t& value,
        codec::field_context field,
        codec::decode_budget&,
        codec::cooperative_work&) {
          ++values_written;
          return codec::write_le(builder, value, field);
      },
      byte_less{},
      byte_count{64},
      item_count{16},
      context);
}

struct expression_byte_writer final {
    codec::result<void> operator()(
      kwaque::bytes::fragmented_buffer_builder&,
      const std::uint8_t&,
      codec::field_context,
      codec::decode_budget&,
      codec::cooperative_work&) const;
};

template<typename Range>
concept ordered_encoder_argument = requires(
  kwaque::bytes::fragmented_buffer_builder& output,
  Range&& entries,
  codec::decode_budget& memory,
  codec::cooperative_work& work) {
    {
        codec::write_ordered_map(
          output,
          std::forward<Range>(entries),
          item_count{8},
          memory,
          work,
          expression_byte_writer{},
          expression_byte_writer{},
          byte_less{},
          byte_count{64},
          item_count{16},
          context)
    } -> std::same_as<seastar::future<codec::result<void>>>;
};

using encoder_entries = std::array<std::pair<std::uint8_t, std::uint8_t>, 1>;
static_assert(ordered_encoder_argument<encoder_entries&>);
static_assert(ordered_encoder_argument<const encoder_entries&>);
static_assert(!ordered_encoder_argument<encoder_entries>);
static_assert(!ordered_encoder_argument<const encoder_entries>);

TEST(CollectionTest, OrderedEncoderMatchesLiteralCountKeyValueBytes) {
    constexpr std::array entries{
      std::pair<std::uint8_t, std::uint8_t>{1, 0x11},
      std::pair<std::uint8_t, std::uint8_t>{3, 0x33},
      std::pair<std::uint8_t, std::uint8_t>{5, 0x55}};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto memory = generous_budget();
    kwaque::bytes::fragmented_buffer_builder output;
    ASSERT_TRUE(output.reserve(byte_count{1}).has_value());
    std::size_t values_written = 0;
    const auto result
      = write_pairs(output, entries, memory, work, values_written).get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(values_written, 3);
    const auto published = output.finish();
    ASSERT_TRUE(published.has_value());
    EXPECT_TRUE(published->content_equals("\x03\x01\x11\x03\x33\x05\x55"sv));
}

TEST(CollectionTest, OrderedEncoderRejectsInsteadOfSortingItsInput) {
    constexpr std::array entries{
      std::pair<std::uint8_t, std::uint8_t>{5, 0x55},
      std::pair<std::uint8_t, std::uint8_t>{3, 0x33}};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto memory = generous_budget();
    kwaque::bytes::fragmented_buffer_builder output;
    ASSERT_TRUE(output.reserve(byte_count{1}).has_value());
    std::size_t values_written = 0;
    const auto result
      = write_pairs(output, entries, memory, work, values_written).get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), errc::malformed_data);
    EXPECT_EQ(values_written, 1);
}

TEST(CollectionTest, OrderedEncoderRequiresOwnerReservedCountPrefix) {
    constexpr std::array entries{
      std::pair<std::uint8_t, std::uint8_t>{1, 0x11}};
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto memory = generous_budget();
    kwaque::bytes::fragmented_buffer_builder output;
    std::size_t values_written = 0;
    const auto result
      = write_pairs(output, entries, memory, work, values_written).get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), errc::resource_exhausted);
    EXPECT_TRUE(output.empty());
    EXPECT_EQ(output.fragment_count(), 0);
    EXPECT_EQ(values_written, 0);
}

} // namespace
