#include "src/base/units.h"
#include "src/bytes/test_allocation_profile.h"
#include "src/codec/cooperative.h"
#include "src/codec/limits.h"
#include "src/model/batch_context.h"
#include "src/resource/resource_registry.h"
#include "src/storage/page_ref.h"
#include "src/storage/retry_summary.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/alloc_failure_injector.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <cstdint>
#include <exception>
#include <optional>
#include <vector>

namespace {
using namespace kwaque;
using namespace kwaque::storage;

template<typename Func>
seastar::future<> with_budget(Func body) {
    auto config = resource::resource_config::from_total_memory(
      byte_count{seastar::memory::stats().total_memory()});
    BOOST_REQUIRE(config.has_value());
    resource::resource_registry registry;
    co_await registry.start(*config);
    resource::resource_manager manager{registry.handles()};
    std::exception_ptr failure;
    try {
        co_await manager.start();
        workload_budget budget{
          manager.acquire_workload(
            resource::workload_class::foreground_protocol),
          {.tasks = 8, .bytes = byte_count{64_MiB}, .handles = 2},
          bytes::testing::charge};
        co_await seastar::futurize_invoke(body, budget);
        BOOST_CHECK_EQUAL(budget.snapshot().bytes, 0U);
    } catch (...) {
        failure = std::current_exception();
    }
    co_await manager.stop();
    co_await registry.stop();
    if (failure) std::rethrow_exception(failure);
}

template<typename Id>
Id identity(std::uint8_t byte) {
    std::array<std::uint8_t, 16> bytes{};
    bytes.fill(byte);
    return Id::make(bytes).value();
}
segment_context segment() {
    return segment_context::make(
             identity<model::cluster_id>(1),
             identity<model::topic_id>(4),
             identity<model::range_id>(5),
             identity<model::segment_id>(6),
             model::segment_generation::make(1).value())
      .value();
}
model::range_routing_epoch routing(std::uint64_t value = 1) {
    return model::range_routing_epoch::make(value).value();
}
// The producer is told apart by its first two bytes, so that 65,536 of them
// fit.
model::producer_id producer(std::uint32_t value) {
    std::array<std::uint8_t, 16> bytes{};
    bytes.fill(0x40);
    bytes[0] = static_cast<std::uint8_t>(value >> 8U);
    bytes[1] = static_cast<std::uint8_t>(value);
    return model::producer_id::make(bytes).value();
}
model::batch_id batch(
  std::uint32_t source,
  std::uint64_t sequence,
  std::uint64_t epoch = 1,
  std::uint64_t stream = 1) {
    return model::batch_id::make(
             producer(source),
             model::producer_epoch::make(epoch).value(),
             model::producer_stream_id::make(stream).value(),
             model::batch_sequence{sequence})
      .value();
}
codec::semantic_batch_digest digest(std::uint64_t value) {
    codec::content_digest bytes{};
    for (std::size_t i = 0; i != 8; ++i)
        bytes[i] = static_cast<unsigned char>(value >> (8U * i));
    return codec::semantic_batch_digest{bytes};
}
model::range_logical_span span(std::uint64_t base, std::uint64_t count = 1) {
    return model::range_logical_span::from_count(
             model::range_logical_end{base}, model::range_logical_count{count})
      .value();
}
// The fact the builder makes of one batch of segment().
completed_retry
fact(const model::batch_id& id, std::uint64_t base, std::uint64_t routed = 1) {
    return completed_retry::make(
             id,
             digest(id.sequence().value()),
             model::producer_stream_binding::make(
               identity<model::topic_id>(4),
               identity<model::range_id>(5),
               routing(routed),
               identity<model::segment_id>(6),
               model::segment_generation::make(1).value())
               .value(),
             span(base),
             model::segment_generation::make(1).value())
      .value();
}
// One batch published durable: admitted and kept, frozen and added, as the
// append owner does. Its logical base is its sequence plus 1,000.
void publish(
  retry_summary_builder& builder,
  const model::batch_id& id,
  std::uint64_t routed = 1) {
    auto ring = builder.admit(id, routing(routed));
    BOOST_REQUIRE(ring.has_value() && *ring != nullptr);
    builder.keep();
    builder.froze(**ring, id.sequence(), false);
    builder.add(
      **ring,
      id.sequence(),
      digest(id.sequence().value()),
      span(1000 + id.sequence().value()));
}
// A batch of segment() as a verified block would describe it.
assigned_batch_info
described(const model::batch_id& id, model::producer_stream_binding binding) {
    const auto submitted
      = model::submitted_batch_context::make(
          id, binding, model::range_logical_count{1}, runtime::wall_time{1})
          .value();
    return assigned_batch_info{
      model::assigned_batch_context::assign(
        submitted,
        model::range_logical_end{1000 + id.sequence().value()},
        binding)
        .value(),
      digest(id.sequence().value()),
      item_count{1},
      model::batch_fingerprint_verification::carried,
      runtime::wall_time{1}};
}
model::producer_stream_binding bound(std::uint64_t routed = 1) {
    return model::producer_stream_binding::make(
             identity<model::topic_id>(4),
             identity<model::range_id>(5),
             routing(routed),
             identity<model::segment_id>(6),
             model::segment_generation::make(1).value())
      .value();
}
// Every fact of a sealed summary, read in pages of `page` as a seal does.
seastar::future<std::vector<completed_retry>> read_all(
  retry_summary_source& source,
  std::uint32_t page,
  codec::cooperative_work& work) {
    std::vector<completed_retry> all;
    for (std::uint32_t first = 0; first < source.completed(); first += page) {
        const auto count = std::min(page, source.completed() - first);
        auto slice = co_await source.read(first, count, work);
        BOOST_REQUIRE(slice.has_value());
        BOOST_REQUIRE(slice->size() == count && slice->capacity() == count);
        all.insert(all.end(), slice->begin(), slice->end());
    }
    co_return all;
}
bool strictly_ordered(const std::vector<completed_retry>& facts) {
    return std::adjacent_find(
             facts.begin(),
             facts.end(),
             [](const completed_retry& left, const completed_retry& right) {
                 return !left.id().canonical_less(right.id());
             })
           == facts.end();
}
} // namespace

// At R = 5 ten batches of one prefix keep the last five with their spans,
// the sixth from last is absent, and the prefix's last is the tenth.
SEASTAR_TEST_CASE(retry_summary_keeps_each_prefix_last_r) {
    return with_budget([](workload_budget& budget) {
        auto builder
          = retry_summary_builder::make(segment(), 5, budget).value();
        for (std::uint64_t sequence = 0; sequence != 10; ++sequence)
            publish(builder, batch(1, sequence));
        BOOST_CHECK(builder.prefixes() == 1 && builder.facts() == 5);
        for (std::uint64_t sequence = 0; sequence != 5; ++sequence)
            BOOST_CHECK(!builder.find(batch(1, sequence)));
        for (std::uint64_t sequence = 5; sequence != 10; ++sequence) {
            const auto found = builder.find(batch(1, sequence));
            BOOST_REQUIRE(found.has_value());
            BOOST_CHECK(*found == fact(batch(1, sequence), 1000 + sequence));
        }
        const auto last = builder.last(retry_prefix::of(batch(1, 0)));
        BOOST_REQUIRE(last.has_value());
        BOOST_CHECK(*last == fact(batch(1, 9), 1009));
        BOOST_CHECK(!builder.last(retry_prefix::of(batch(2, 0))));
    });
}

// A new epoch of the same producer and stream is a prefix of its own, and
// its batches never trim the old one's.
SEASTAR_TEST_CASE(retry_summary_epoch_change_is_a_new_prefix) {
    return with_budget([](workload_budget& budget) {
        auto builder
          = retry_summary_builder::make(segment(), 2, budget).value();
        publish(builder, batch(1, 7, 1));
        publish(builder, batch(1, 0, 2));
        publish(builder, batch(1, 1, 2));
        publish(builder, batch(1, 2, 2));
        BOOST_CHECK(builder.prefixes() == 2 && builder.facts() == 3);
        BOOST_CHECK(builder.find(batch(1, 7, 1)).has_value());
        BOOST_CHECK(!builder.find(batch(1, 0, 2)));
        BOOST_CHECK(builder.find(batch(1, 2, 2)).has_value());
        // So is another stream of the same producer and epoch.
        publish(builder, batch(1, 0, 1, 2));
        BOOST_CHECK(builder.prefixes() == 3 && builder.facts() == 4);
    });
}

// A prefix takes only sequences above the newest that froze in it, and keeps
// the routing epoch it was made with; a refusal changes nothing. A walked
// block that breaks either, or is bound to another segment, is malformed.
SEASTAR_TEST_CASE(retry_summary_refuses_out_of_order_batches) {
    return with_budget([](workload_budget& budget) {
        auto builder
          = retry_summary_builder::make(segment(), 4, budget).value();
        publish(builder, batch(1, 5));
        for (const std::uint64_t sequence :
             std::array<std::uint64_t, 2>{5, 4}) {
            auto refused = builder.admit(batch(1, sequence), routing());
            BOOST_REQUIRE(!refused.has_value());
            BOOST_CHECK(refused.error().code() == errc::invalid_argument);
        }
        auto other = builder.admit(batch(1, 6), routing(2));
        BOOST_REQUIRE(!other.has_value());
        BOOST_CHECK(other.error().code() == errc::wrong_context);
        BOOST_CHECK(builder.facts() == 1 && builder.prefixes() == 1);

        for (const auto& [id, binding] : std::array{
               std::pair{batch(1, 5), bound()},
               std::pair{batch(1, 4), bound()},
               std::pair{batch(1, 6), bound(2)}}) {
            auto refused = builder.derive(described(id, binding));
            BOOST_REQUIRE(!refused.has_value());
            BOOST_CHECK(refused.error().code() == errc::malformed_data);
        }
        const auto foreign = model::producer_stream_binding::make(
                               identity<model::topic_id>(4),
                               identity<model::range_id>(5),
                               routing(),
                               identity<model::segment_id>(7),
                               model::segment_generation::make(1).value())
                               .value();
        auto refused = builder.derive(described(batch(2, 0), foreign));
        BOOST_REQUIRE(!refused.has_value());
        BOOST_CHECK(refused.error().code() == errc::malformed_data);
        BOOST_CHECK(builder.facts() == 1 && builder.prefixes() == 1);
        BOOST_CHECK(*builder.find(batch(1, 5)) == fact(batch(1, 5), 1005));

        // A walked block in order is a fact like a published one.
        auto derived = builder.derive(described(batch(1, 6), bound()));
        BOOST_REQUIRE(derived.has_value() && *derived);
        BOOST_CHECK(*builder.find(batch(1, 6)) == fact(batch(1, 6), 1006));
        BOOST_CHECK(
          !retry_summary_builder::make(segment(), 0, budget).has_value()
          && !retry_summary_builder::make(segment(), 33, budget).has_value());
    });
}

// At ⌊65,536 / R⌋ prefixes a new one is refused, and nothing is evicted for
// it, by time or by count.
SEASTAR_TEST_CASE(retry_summary_never_evicts_a_prefix) {
    return with_budget([](workload_budget& budget) {
        auto builder
          = retry_summary_builder::make(segment(), 32, budget).value();
        const auto limit = retry_summary_builder::prefix_limit(32);
        BOOST_REQUIRE(limit == maximum_object_entries / 32);
        for (std::uint32_t source = 0; source != limit; ++source)
            publish(builder, batch(source, 0));
        auto refused = builder.admit(batch(limit, 0), routing());
        BOOST_REQUIRE(refused.has_value());
        BOOST_CHECK(*refused == nullptr);
        auto unwalked = builder.derive(described(batch(limit, 0), bound()));
        BOOST_REQUIRE(unwalked.has_value());
        BOOST_CHECK(!*unwalked);
        // An existing prefix still takes batches.
        publish(builder, batch(0, 1));
        BOOST_CHECK(
          builder.prefixes() == limit && builder.facts() == limit + 1);
        for (std::uint32_t source = 0; source != limit; ++source)
            BOOST_CHECK(builder.find(batch(source, 0)).has_value());
    });
}

// A stream's batches take seal entries until it holds the depth of them,
// counting those of the same group still forming; then none, however many
// more it publishes.
SEASTAR_TEST_CASE(retry_summary_counts_seal_entries_to_the_depth) {
    return with_budget([](workload_budget& budget) {
        auto builder
          = retry_summary_builder::make(segment(), 2, budget).value();
        auto ring = builder.admit(batch(1, 0), routing()).value();
        BOOST_REQUIRE(ring != nullptr);
        builder.keep();
        BOOST_CHECK(
          builder.opens_entry(*ring, 0) && builder.opens_entry(*ring, 1)
          && !builder.opens_entry(*ring, 2));
        builder.froze(*ring, model::batch_sequence{0}, true);
        BOOST_CHECK(
          builder.opens_entry(*ring, 0) && !builder.opens_entry(*ring, 1));
        builder.froze(*ring, model::batch_sequence{1}, true);
        builder.froze(*ring, model::batch_sequence{2}, false);
        BOOST_CHECK(!builder.opens_entry(*ring, 0));
        // A batch at or below the newest frozen is refused.
        auto refused = builder.admit(batch(1, 2), routing());
        BOOST_REQUIRE(!refused.has_value());
        BOOST_CHECK(refused.error().code() == errc::invalid_argument);
        BOOST_CHECK(builder.admit(batch(1, 3), routing()).value() == ring);
    });
}

// A prefix made for a request that then fails goes with everything it was
// charged, even the map's own memory when it was the only one; one that is
// kept stays. Neither touches the prefixes kept before.
SEASTAR_TEST_CASE(retry_summary_drops_an_unkept_prefix) {
    return with_budget([](workload_budget& budget) {
        auto builder
          = retry_summary_builder::make(segment(), 4, budget).value();
        const auto empty = budget.snapshot();
        BOOST_REQUIRE(builder.admit(batch(1, 0), routing()).value() != nullptr);
        BOOST_CHECK(builder.prefixes() == 1 && budget.snapshot().bytes > 0);
        builder.drop();
        BOOST_CHECK(
          builder.prefixes() == 0 && budget.snapshot().bytes == empty.bytes
          && budget.snapshot().tasks == empty.tasks);
        // Nothing to drop, and nothing kept, changes nothing.
        builder.drop();
        builder.keep();
        BOOST_CHECK(
          builder.prefixes() == 0 && budget.snapshot().bytes == empty.bytes);

        publish(builder, batch(1, 0));
        const auto kept = budget.snapshot();
        BOOST_REQUIRE(builder.admit(batch(2, 0), routing()).value() != nullptr);
        builder.drop();
        BOOST_CHECK(
          builder.prefixes() == 1 && budget.snapshot().bytes == kept.bytes
          && budget.snapshot().tasks == kept.tasks
          && *builder.find(batch(1, 0)) == fact(batch(1, 0), 1000)
          && !builder.find(batch(2, 0)));
        // Made again, a dropped prefix is new.
        publish(builder, batch(2, 0));
        BOOST_CHECK(builder.prefixes() == 2 && builder.facts() == 2);
    });
}

// A ring that exists is admitted again and takes its batches without
// allocating, full or not.
SEASTAR_TEST_CASE(retry_summary_adds_allocate_nothing) {
    return with_budget([](workload_budget& budget) {
        auto builder
          = retry_summary_builder::make(segment(), 3, budget).value();
        auto ring = builder.admit(batch(1, 0), routing()).value();
        BOOST_REQUIRE(ring != nullptr);
        builder.keep();
        auto& injector = seastar::memory::local_failure_injector();
        for (std::uint64_t sequence = 0; sequence != 7; ++sequence) {
            injector.fail_after(0);
            const auto again = builder.admit(batch(1, sequence), routing());
            builder.froze(*ring, model::batch_sequence{sequence}, false);
            builder.add(
              *ring,
              model::batch_sequence{sequence},
              digest(sequence),
              span(1000 + sequence));
            const bool failed = injector.failed();
            injector.cancel();
            BOOST_CHECK(!failed && again.has_value() && *again == ring);
        }
        BOOST_CHECK(builder.facts() == 3);
        BOOST_CHECK(
          *builder.last(retry_prefix::of(batch(1, 0)))
          == fact(batch(1, 6), 1006));
    });
}

// A seal reads the prefixes in batch identity order, each prefix's facts
// oldest first, and a second pass, or any slice read again, returns the same
// facts.
SEASTAR_TEST_CASE(retry_summary_seals_in_identity_order) {
    return with_budget([](workload_budget& budget) -> seastar::future<> {
        auto builder
          = retry_summary_builder::make(segment(), 3, budget).value();
        std::vector<completed_retry> expected;
        // Prefixes made out of order, some with a ring that wrapped.
        for (const std::uint32_t source :
             std::array<std::uint32_t, 5>{9, 2, 7, 0, 4}) {
            const auto batches = std::uint64_t{source % 4 + 1};
            for (std::uint64_t sequence = 0; sequence != batches; ++sequence)
                publish(builder, batch(source, 10 + sequence));
        }
        // A prefix admitted but never published holds nothing.
        BOOST_REQUIRE(builder.admit(batch(5, 0), routing()).value() != nullptr);
        builder.keep();
        for (const std::uint32_t source :
             std::array<std::uint32_t, 5>{0, 2, 4, 7, 9}) {
            const auto batches = std::uint64_t{source % 4 + 1};
            for (std::uint64_t sequence = batches > 3 ? batches - 3 : 0;
                 sequence != batches;
                 ++sequence)
                expected.push_back(
                  fact(batch(source, 10 + sequence), 1010 + sequence));
        }
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        BOOST_REQUIRE(builder.facts() == expected.size());
        auto sealed = co_await retry_summary_builder::seal(
          std::move(builder), work);
        BOOST_REQUIRE(sealed.has_value());
        BOOST_REQUIRE(sealed->completed() == expected.size());
        const auto first = co_await read_all(*sealed, 2, work);
        BOOST_CHECK(first == expected && strictly_ordered(first));
        const auto again = co_await read_all(*sealed, 4, work);
        BOOST_CHECK(again == expected);
        auto middle = co_await sealed->read(3, 3, work);
        BOOST_REQUIRE(middle.has_value());
        BOOST_CHECK(
          std::equal(middle->begin(), middle->end(), expected.begin() + 3));
        auto past = co_await sealed->read(
          static_cast<std::uint32_t>(expected.size()), 1, work);
        BOOST_CHECK(!past.has_value());
    });
}

// At the prefix limit of the default depth, 8,192 prefixes of eight facts
// each, the seal sorts and emits all 65,536 in order and twice alike. Its
// sort polls the work account between bounded steps, so an abort ends it.
SEASTAR_TEST_CASE(retry_summary_seals_the_prefix_limit) {
    return with_budget([](workload_budget& budget) -> seastar::future<> {
        const std::uint8_t depth = 8;
        const auto limit = retry_summary_builder::prefix_limit(depth);
        BOOST_REQUIRE(limit == 8192);
        const auto fill = [&]() {
            auto builder
              = retry_summary_builder::make(segment(), depth, budget).value();
            // Made in an order unlike the identity order.
            for (std::uint32_t i = 0; i != limit; ++i) {
                const auto source = (i * 5003U) % limit;
                for (std::uint64_t sequence = 0; sequence != depth + 2;
                     ++sequence)
                    publish(builder, batch(source, sequence));
            }
            return builder;
        };
        seastar::abort_source stopped;
        stopped.request_abort();
        codec::cooperative_work aborted{codec::limits::defaults(), stopped};
        auto refused = co_await retry_summary_builder::seal(fill(), aborted);
        BOOST_CHECK(!refused.has_value());

        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto sealed = co_await retry_summary_builder::seal(fill(), work);
        BOOST_REQUIRE(sealed.has_value());
        BOOST_REQUIRE(sealed->completed() == maximum_object_entries);
        const auto first = co_await read_all(*sealed, 408, work);
        BOOST_CHECK(strictly_ordered(first));
        BOOST_CHECK(first.front() == fact(batch(0, 2), 1002));
        BOOST_CHECK(first.back() == fact(batch(limit - 1, 9), 1009));
        const auto again = co_await read_all(*sealed, 408, work);
        BOOST_CHECK(again == first);
    });
}

// Every allocation stays within one contiguous allocation, at the largest
// map a builder makes: 65,536 prefixes at R = 1, and the default depth's
// limit.
SEASTAR_TEST_CASE(retry_summary_allocations_stay_bounded) {
    return with_budget([](workload_budget& budget) -> seastar::future<> {
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
        seastar::memory::scoped_large_allocation_warning_threshold threshold{
          maximum_contiguous_allocation_bytes + 1U};
        const auto before = seastar::memory::stats().large_allocations();
#endif
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        for (const std::uint8_t depth : std::array<std::uint8_t, 2>{1, 8}) {
            auto builder
              = retry_summary_builder::make(segment(), depth, budget).value();
            const auto limit = retry_summary_builder::prefix_limit(depth);
            for (std::uint32_t source = 0; source != limit; ++source)
                publish(builder, batch(source, 0));
            BOOST_REQUIRE(builder.prefixes() == limit);
            auto sealed = co_await retry_summary_builder::seal(
              std::move(builder), work);
            BOOST_REQUIRE(sealed.has_value());
            BOOST_CHECK(sealed->completed() == limit);
        }
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
        BOOST_CHECK_EQUAL(seastar::memory::stats().large_allocations(), before);
#endif
    });
}
