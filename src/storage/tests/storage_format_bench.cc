#include "src/codec/sha256_cooperative.h"
#include "src/codec/tests/benchmark_buffer.h"
#include "src/storage/format_size.h"
#include "src/storage/tests/storage_format_fixture.h"
#include "src/storage/tests/storage_large_fixture.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/perf_tests.hh>

#include <fmt/format.h>

namespace kwaque::storage::bench {
namespace {
using bytes::fragmented_buffer;
using bytes::fragmented_buffer_parser;
using codec::bench::capacity_bound;
using codec::bench::require;
enum class operation {
    layout,
    header_decode,
    header_encode,
    block_decode,
    block_encode,
    wal_decode,
    wal_encode,
    durable_decode,
    durable_encode,
    sealed_decode,
    sealed_encode,
    retry_decode,
    retry_encode,
    extent_verify,
    summary_verify,
    exact_hash
};
testing::storage_case family(operation op) {
    using enum operation;
    switch (op) {
    case layout:
    case header_decode:
    case header_encode:
        return testing::storage_case::header;
    case block_decode:
    case block_encode:
    case extent_verify:
    case exact_hash:
        return testing::storage_case::block;
    case wal_decode:
    case wal_encode:
        return testing::storage_case::wal;
    case durable_decode:
    case durable_encode:
        return testing::storage_case::durable;
    case sealed_decode:
    case sealed_encode:
        return testing::storage_case::sealed;
    case retry_decode:
    case retry_encode:
    case summary_verify:
        return testing::storage_case::retry;
    }
    __builtin_unreachable();
}
const char* scope_name(operation op) noexcept {
    switch (op) {
    case operation::layout:
        return "fixed_layout";
    case operation::header_decode:
        return "header_decode";
    case operation::header_encode:
        return "header_encode";
    case operation::block_decode:
        return "block_decode";
    case operation::block_encode:
        return "block_encode";
    case operation::wal_decode:
        return "wal_decode";
    case operation::wal_encode:
        return "wal_encode";
    case operation::durable_decode:
        return "durable_decode";
    case operation::durable_encode:
        return "durable_encode";
    case operation::sealed_decode:
        return "sealed_decode";
    case operation::sealed_encode:
        return "sealed_encode";
    case operation::retry_decode:
        return "retry_decode";
    case operation::retry_encode:
        return "retry_encode";
    case operation::extent_verify:
        return "extent_verify";
    case operation::summary_verify:
        return "summary_verify";
    case operation::exact_hash:
        return "exact_hash";
    }
    __builtin_unreachable();
}
const fragmented_buffer& output_bytes(const fragmented_buffer& value) {
    return value;
}
const fragmented_buffer& output_bytes(const segment_block& value) {
    return value.bytes();
}
const fragmented_buffer& output_bytes(const encoded_retry_page& value) {
    return value.bytes;
}
const fragmented_buffer& output_bytes(const encoded_sealed_footer& value) {
    return value.bytes;
}

class measurements {
public:
    measurements(
      bool compressed,
      std::size_t width,
      std::uint32_t entries,
      bool large = false)
      : compressed_(compressed)
      , large_(large)
      , width_(width)
      , entries_(entries) {}

    template<operation Op>
    seastar::future<std::size_t> measure() {
        co_await initialize<Op>();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        if constexpr (Op == operation::layout) {
            perf_tests::start_measuring_time();
            for (std::uint64_t tail = 0; tail < 256; ++tail) {
                const auto result = aligned_envelope_layout::make(
                  {byte_count{32},
                   segment_header_fixed_bytes,
                   byte_count{tail}},
                  source_->history.alignment,
                  work.policy(),
                  {byte_count{65536}, byte_count{65536}});
                perf_tests::do_not_optimize(result);
                require(
                  result && result->encoded_bytes() == wire_.size(),
                  "fixed layout changed");
            }
            perf_tests::stop_measuring_time();
        } else if constexpr (Op == operation::exact_hash) {
            auto input = wire_.share();
            perf_tests::start_measuring_time();
            const auto result = co_await codec::sha256_cooperatively(
              std::move(input), work);
            perf_tests::do_not_optimize(result);
            perf_tests::stop_measuring_time();
            require(
              result && *result == wire_digest_, "exact object hash changed");
        } else if constexpr (Op == operation::extent_verify) {
            auto input = block_.share();
            const auto cost = input.allocation_cost(capacity_bound).value();
            const auto memory
              = codec::detail::consume_decode_budget(
                  work.policy(),
                  budget(),
                  cost.backing,
                  cost.descriptors.checked_add(cost.share_controls).value(),
                  {},
                  0)
                  .value();
            perf_tests::start_measuring_time();
            auto verifier = extent_verifier::make(
                              source_->history,
                              source_->coverage,
                              work.policy(),
                              extent_layout_kind::initial_append,
                              {},
                              extent_integrity::crc32c_and_sha256)
                              .value();
            (co_await verifier.add_block(
               std::move(input), source_->block_context.batch, memory, work))
              .value();
            const auto result = verifier.finish(work);
            perf_tests::do_not_optimize(result);
            perf_tests::stop_measuring_time();
            require(
              result && result->boundary() == proof_->boundary()
                && result->digest() == proof_->digest(),
              "extent evidence changed");
        } else if constexpr (Op == operation::summary_verify) {
            perf_tests::start_measuring_time();
            retry_summary_verifier verifier{*root_, work.policy()};
            for (auto& page : pages_) {
                fragmented_buffer_parser input{page.share()};
                const auto memory = codec::reserve_decode_input(
                                      input, work.policy(), budget())
                                      .value();
                auto result = co_await verifier.next(input, memory, work);
                perf_tests::do_not_optimize(result);
                require(result && input.at_end(), "summary page failed");
                co_await work.drain_inline(
                  work.byte_quantum(), work.item_quantum());
                result = codec::failure(codec::error{errc::success});
                input = fragmented_buffer_parser{};
            }
            const auto result = verifier.finish(work);
            perf_tests::do_not_optimize(result);
            perf_tests::stop_measuring_time();
            require(
              result && result->entry_count() == 4U * entries_,
              "summary omitted pages");
        } else if constexpr (
          Op == operation::header_encode || Op == operation::block_encode
          || Op == operation::wal_encode || Op == operation::durable_encode
          || Op == operation::sealed_encode || Op == operation::retry_encode) {
            std::optional<encoded_assigned_batch> source;
            if constexpr (
              Op == operation::block_encode || Op == operation::wal_encode)
                source.emplace(
                  (co_await child_->share(budget(), work)).value());
            perf_tests::start_measuring_time();
            auto result = co_await encode<Op>(source, work);
            perf_tests::do_not_optimize(result);
            perf_tests::stop_measuring_time();
            require(result.has_value(), "storage encode failed");
            require(
              co_await codec::bench::buffers_equal(
                output_bytes(*result), wire_, work),
              "storage encoder changed exact bytes");
            report_output(output_bytes(*result));
            perf_tests::start_measuring_time();
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            result = codec::failure(codec::error{errc::success});
            perf_tests::stop_measuring_time();
        } else {
            fragmented_buffer_parser input{wire_.share()};
            const auto memory = codec::reserve_decode_input(
                                  input, work.policy(), budget())
                                  .value();
            perf_tests::start_measuring_time();
            auto result = co_await decode<Op>(input, memory, work);
            perf_tests::do_not_optimize(result);
            perf_tests::stop_measuring_time();
            require(result && input.at_end(), "storage decode failed");
            if constexpr (Op == operation::header_decode)
                require(
                  result->value == source_->header, "header fields changed");
            if constexpr (Op == operation::block_decode) {
                require(
                  result->value.descriptor().batch() == child_->info(),
                  "block changed original metadata");
                require(
                  co_await codec::bench::buffers_equal(
                    result->value.bytes(), wire_, work),
                  "block changed stored bytes");
            }
            if constexpr (Op == operation::wal_decode) {
                require(
                  result->value.batch().info() == child_->info(),
                  "WAL changed original metadata");
                require(
                  co_await codec::bench::buffers_equal(
                    result->value.batch().bytes(), child_->bytes(), work),
                  "WAL changed assigned bytes");
            }
            if constexpr (Op == operation::durable_decode)
                require(
                  validate_durable_footer(*result, *proof_).has_value(),
                  "durable boundary changed");
            if constexpr (Op == operation::sealed_decode)
                require(
                  validate_sealed_footer(result->value, *proof_).has_value(),
                  "sealed boundary changed");
            if constexpr (Op == operation::retry_decode)
                require(
                  std::ranges::equal(result->value.entries(), source_->entries),
                  "retry results changed");
            perf_tests::start_measuring_time();
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            result = codec::failure(codec::error{errc::success});
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            input = fragmented_buffer_parser{};
            perf_tests::stop_measuring_time();
        }
        co_return std::size_t{Op == operation::layout ? 256U : 1U};
    }

private:
    codec::decode_budget budget() const {
        return {remaining_, byte_count{1U << 20U}, capacity_bound};
    }
    template<operation Op>
    auto decode(
      fragmented_buffer_parser& input,
      codec::decode_budget memory,
      codec::cooperative_work& work) {
        if constexpr (Op == operation::header_decode)
            return decode_segment_header(
              input, source_->header, {}, memory, work);
        else if constexpr (Op == operation::block_decode)
            return decode_segment_block(
              input, source_->block_context, memory, work);
        else if constexpr (Op == operation::wal_decode)
            return decode_wal_prepare(
              input, source_->wal_context, memory, work);
        else if constexpr (Op == operation::durable_decode)
            return decode_durable_footer(
              input, source_->durable_context, memory, work);
        else if constexpr (Op == operation::sealed_decode)
            return decode_sealed_footer(
              input, source_->sealed_context, root_digest_, memory, work);
        else
            return decode_retry_page(
              input, *root_, page_ordinal::make(0).value(), memory, work);
    }
    template<operation Op>
    auto encode(
      std::optional<encoded_assigned_batch>& child,
      codec::cooperative_work& work) {
        if constexpr (Op == operation::header_encode)
            return encode_segment_header(
              source_->header, work, remaining_, capacity_bound);
        else if constexpr (Op == operation::block_encode)
            return encode_segment_block(
              std::move(*child),
              source_->block_context,
              work,
              remaining_,
              capacity_bound);
        else if constexpr (Op == operation::wal_encode)
            return encode_wal_prepare(
              std::move(*child),
              source_->wal_context,
              work,
              remaining_,
              capacity_bound);
        else if constexpr (Op == operation::durable_encode)
            return encode_durable_footer(
              *proof_,
              source_->durable_context,
              work,
              remaining_,
              capacity_bound);
        else if constexpr (Op == operation::sealed_encode)
            return encode_sealed_footer(
              *proof_,
              source_->sealed_context,
              entries_,
              root_->pages(),
              work,
              remaining_,
              capacity_bound);
        else
            return encode_retry_page(
              source_->entries,
              source_->sealed_context,
              page_ordinal::make(0).value(),
              0,
              work,
              remaining_,
              capacity_bound);
    }
    seastar::future<fragmented_buffer>
    layout(std::string_view text, codec::cooperative_work& work) {
        co_return co_await codec::bench::copy_layout(
          fragmented_buffer::copy_of(text).value(), width_, work, remaining_);
    }
    void report_output(const fragmented_buffer& value) {
        if (reported_) return;
        const auto cost = value.allocation_cost(capacity_bound).value();
        fmt::print(
          "kwaque-storage-output-v1 bytes={} fragments={} backing_upper={} "
          "metadata_upper={}\n",
          value.size().value(),
          value.fragment_count(),
          cost.backing.value(),
          cost.descriptors.value() + cost.share_controls.value());
        reported_ = true;
    }
    template<operation Op>
    seastar::future<> initialize() {
        if (source_) co_return;
        codec::bench::qualify_allocator();
        source_.emplace(
          family(Op), compressed_, false, 32, 32, 512, 512, entries_);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        if (large_)
            child_.emplace(
              (co_await testing::large_child(compressed_, work, remaining_))
                .value());
        else {
            auto input = co_await layout(source_->child, work);
            const auto cost = input.allocation_cost(capacity_bound).value();
            const auto memory
              = codec::detail::consume_decode_budget(
                  work.policy(),
                  budget(),
                  cost.backing,
                  cost.descriptors.checked_add(cost.share_controls).value(),
                  {},
                  0)
                  .value();
            child_.emplace(
              (co_await validate_encoded_assigned_batch(
                 std::move(input), testing::batch_expected(), memory, work))
                .value());
        }
        if (large_) {
            auto copy = (co_await child_->share(budget(), work)).value();
            auto encoded = (co_await encode_segment_block(
                              std::move(copy),
                              source_->block_context,
                              work,
                              remaining_,
                              capacity_bound))
                             .value();
            source_->coverage = encoded.descriptor().coverage();
            block_ = co_await codec::bench::copy_layout(
              std::move(encoded).release_bytes(), width_, work, remaining_);
            if (family(Op) == testing::storage_case::wal) {
                auto wal_child
                  = (co_await child_->share(budget(), work)).value();
                auto encoded_wal = (co_await encode_wal_prepare(
                                      std::move(wal_child),
                                      source_->wal_context,
                                      work,
                                      remaining_,
                                      capacity_bound))
                                     .value();
                wire_ = co_await codec::bench::copy_layout(
                  std::move(encoded_wal), width_, work, remaining_);
            } else
                wire_ = block_.share();
        } else {
            wire_ = co_await layout(source_->wire, work);
            block_ = co_await layout(source_->block, work);
        }
        // Hash/history setup is outside every measurement. Exact wire owners
        // are subsequently reused under the same explicit per-call scope.
        auto input = block_.share();
        const auto cost = input.allocation_cost(capacity_bound).value();
        const auto memory
          = codec::detail::consume_decode_budget(
              work.policy(),
              budget(),
              cost.backing,
              cost.descriptors.checked_add(cost.share_controls).value(),
              {},
              0)
              .value();
        auto verifier = extent_verifier::make(
                          source_->history,
                          source_->coverage,
                          work.policy(),
                          extent_layout_kind::initial_append,
                          {},
                          extent_integrity::crc32c_and_sha256)
                          .value();
        (co_await verifier.add_block(
           std::move(input), testing::batch_expected(), memory, work))
          .value();
        proof_ = verifier.finish(work).value();
        if constexpr (Op == operation::summary_verify) {
            std::vector<page_ref> refs;
            refs.reserve(4);
            pages_.reserve(4);
            for (std::uint32_t p = 0; p < 4; ++p) {
                std::vector<completed_retry> entries;
                entries.reserve(entries_);
                for (std::uint32_t i = 0; i < entries_; ++i)
                    entries.push_back(testing::retry(p * entries_ + i));
                const auto page = testing::retry_page_wire(
                  entries, source_->sealed_context, p, p * entries_);
                refs.push_back(
                  testing::reference(page, p, p * entries_, entries_));
                pages_.push_back(co_await layout(page, work));
            }
            source_->root = testing::sealed_wire(
              proof_->boundary(),
              proof_->digest()->bytes(),
              refs,
              source_->sealed_context);
        }
        root_digest_ = codec::immutable_object_digest{
          testing::exact_sha(source_->root)};
        {
            fragmented_buffer_parser root_input{
              co_await layout(source_->root, work)};
            const auto root_memory = codec::reserve_decode_input(
                                       root_input, work.policy(), budget())
                                       .value();
            auto decoded = (co_await decode_sealed_footer(
                              root_input,
                              source_->sealed_context,
                              root_digest_,
                              root_memory,
                              work))
                             .value();
            root_.emplace(std::move(decoded.value));
        }
        wire_digest_
          = (co_await codec::sha256_cooperatively(wire_.share(), work)).value();
        // Drop temporary fixture strings; charge every cached buffer, vector
        // and conservatively counted alias. One MiB remains excluded for
        // native engines, inline owners and coroutine frames.
        std::string{}.swap(source_->child);
        std::string{}.swap(source_->block);
        std::string{}.swap(source_->page);
        std::string{}.swap(source_->root);
        std::string{}.swap(source_->wire);
        byte_count retained;
        const auto account = [&](const fragmented_buffer& value) {
            const auto used = value.allocation_cost(capacity_bound).value();
            retained = retained.checked_add(used.backing)
                         .value()
                         .checked_add(used.descriptors)
                         .value()
                         .checked_add(used.share_controls)
                         .value();
        };
        account(wire_);
        account(block_);
        account(child_->bytes());
        for (const auto& page : pages_)
            account(page);
        retained
          = retained
              .checked_add(capacity_bound(
                byte_count{
                  source_->entries.capacity() * sizeof(completed_retry)}))
              .value()
              .checked_add(capacity_bound(
                byte_count{root_->page_capacity() * sizeof(page_ref)}))
              .value()
              .checked_add(capacity_bound(
                byte_count{pages_.capacity() * sizeof(fragmented_buffer)}))
              .value();
        remaining_ = byte_count{63U << 20U}.checked_sub(retained).value();
        fmt::print(
          "kwaque-storage-fixture-v1 scope={} codec={} bytes={} fragments={} "
          "retries={} pages={} cached_upper={} normalization=operations\n",
          scope_name(Op),
          compressed_ ? "lz4" : "none",
          wire_.size().value(),
          wire_.fragment_count(),
          entries_,
          root_->pages().size(),
          retained.value());
    }
    bool compressed_, large_;
    std::size_t width_;
    std::uint32_t entries_;
    std::optional<testing::storage_fixture> source_;
    fragmented_buffer wire_, block_;
    std::vector<fragmented_buffer> pages_;
    std::optional<encoded_assigned_batch> child_;
    std::optional<verified_extent> proof_;
    std::optional<sealed_footer> root_;
    codec::immutable_object_digest root_digest_{{}};
    codec::sha256_digest wire_digest_{};
    byte_count remaining_{63U << 20U};
    bool reported_{false};
};
template<
  bool Compressed,
  std::size_t Width,
  std::uint32_t Entries,
  bool Large = false>
struct fixture : measurements {
    fixture()
      : measurements(Compressed, Width, Entries, Large) {}
};
using storage_tiny = fixture<false, 512, 1>;
using storage_fragmented = fixture<true, 7, 1>;
using storage_page_max = fixture<false, 4096, 408>;
using storage_batch_max = fixture<false, 65536, 1, true>;
using storage_batch_max_lz4 = fixture<true, 65536, 1, true>;
#define STORAGE_CASE(group, name)                                              \
    PERF_TEST_F(group, name) { return measure<operation::name>(); }
#define STORAGE_CASES(group)                                                   \
    PERF_TEST_F(group, layout) { return measure<operation::layout>(); }        \
    PERF_TEST_F(group, header_decode) {                                        \
        return measure<operation::header_decode>();                            \
    }                                                                          \
    PERF_TEST_F(group, header_encode) {                                        \
        return measure<operation::header_encode>();                            \
    }                                                                          \
    PERF_TEST_F(group, block_decode) {                                         \
        return measure<operation::block_decode>();                             \
    }                                                                          \
    PERF_TEST_F(group, block_encode) {                                         \
        return measure<operation::block_encode>();                             \
    }                                                                          \
    PERF_TEST_F(group, wal_decode) {                                           \
        return measure<operation::wal_decode>();                               \
    }                                                                          \
    PERF_TEST_F(group, wal_encode) {                                           \
        return measure<operation::wal_encode>();                               \
    }                                                                          \
    PERF_TEST_F(group, durable_decode) {                                       \
        return measure<operation::durable_decode>();                           \
    }                                                                          \
    PERF_TEST_F(group, durable_encode) {                                       \
        return measure<operation::durable_encode>();                           \
    }                                                                          \
    PERF_TEST_F(group, sealed_decode) {                                        \
        return measure<operation::sealed_decode>();                            \
    }                                                                          \
    PERF_TEST_F(group, sealed_encode) {                                        \
        return measure<operation::sealed_encode>();                            \
    }                                                                          \
    PERF_TEST_F(group, retry_decode) {                                         \
        return measure<operation::retry_decode>();                             \
    }                                                                          \
    PERF_TEST_F(group, retry_encode) {                                         \
        return measure<operation::retry_encode>();                             \
    }                                                                          \
    PERF_TEST_F(group, extent_verify) {                                        \
        return measure<operation::extent_verify>();                            \
    }                                                                          \
    PERF_TEST_F(group, summary_verify) {                                       \
        return measure<operation::summary_verify>();                           \
    }                                                                          \
    PERF_TEST_F(group, exact_hash) { return measure<operation::exact_hash>(); }
STORAGE_CASES(storage_tiny)
STORAGE_CASES(storage_fragmented)
STORAGE_CASE(storage_page_max, sealed_decode)
STORAGE_CASE(storage_page_max, sealed_encode)
STORAGE_CASE(storage_page_max, retry_decode)
STORAGE_CASE(storage_page_max, retry_encode)
STORAGE_CASE(storage_page_max, summary_verify)
STORAGE_CASE(storage_batch_max, block_decode)
STORAGE_CASE(storage_batch_max, block_encode)
STORAGE_CASE(storage_batch_max, wal_decode)
STORAGE_CASE(storage_batch_max, wal_encode)
STORAGE_CASE(storage_batch_max_lz4, block_decode)
STORAGE_CASE(storage_batch_max_lz4, block_encode)
STORAGE_CASE(storage_batch_max_lz4, wal_decode)
STORAGE_CASE(storage_batch_max_lz4, wal_encode)
#undef STORAGE_CASES
#undef STORAGE_CASE
} // namespace
} // namespace kwaque::storage::bench
