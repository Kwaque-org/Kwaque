#include "src/codec/tests/benchmark_buffer.h"
#include "src/storage/tests/storage_metadata_fixture.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/perf_tests.hh>

#include <fmt/format.h>

#include <type_traits>

namespace kwaque::storage::bench {
namespace {
using bytes::fragmented_buffer;
using bytes::fragmented_buffer_parser;
using codec::bench::capacity_bound;
using codec::bench::require;
enum class metadata_operation {
    root_encode,
    root_decode,
    page_encode,
    page_decode,
    pages_verify,
    anchor_compare,
    extent_compare
};
const char* scope_name(metadata_operation op) noexcept {
    switch (op) {
    case metadata_operation::root_encode:
        return "root_encode";
    case metadata_operation::root_decode:
        return "root_decode";
    case metadata_operation::page_encode:
        return "page_encode";
    case metadata_operation::page_decode:
        return "page_decode";
    case metadata_operation::pages_verify:
        return "pages_verify";
    case metadata_operation::anchor_compare:
        return "anchor_compare";
    case metadata_operation::extent_compare:
        return "extent_compare";
    }
    __builtin_unreachable();
}

template<bool Manifest>
class metadata_measurements {
    using root_type
      = std::conditional_t<Manifest, range_manifest_root, sparse_index_root>;
    using entry_type
      = std::conditional_t<Manifest, range_manifest_entry, sparse_index_entry>;
    using verifier_type = std::
      conditional_t<Manifest, range_manifest_verifier, sparse_index_verifier>;

public:
    metadata_measurements(
      std::size_t width,
      std::uint32_t count,
      std::size_t header,
      std::uint64_t alignment)
      : width_(width)
      , count_(count)
      , header_(header)
      , alignment_(alignment) {}

    template<metadata_operation Op>
    seastar::future<std::size_t> measure() {
        co_await initialize<Op>();
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        if constexpr (
          Op == metadata_operation::anchor_compare
          || Op == metadata_operation::extent_compare) {
            perf_tests::start_measuring_time();
            for (unsigned i = 0; i < 256; ++i) {
                // The fixture is unchanged between iterations; force the
                // comparisons to reload it instead of reusing a prior result.
                // NOLINTNEXTLINE(portability-no-assembler)
                asm volatile("" : : : "memory");
                auto result = [&] {
                    if constexpr (Op == metadata_operation::anchor_compare)
                        return validate_sparse_index_anchor(
                          source_->index_context, entries_[0], *block_);
                    else if constexpr (Manifest)
                        return validate_range_manifest_extent(
                          source_->manifest_header.context(),
                          source_->index_context.segment().cluster(),
                          entries_[0],
                          *evidence_);
                    else
                        return validate_sparse_index_extent(*root_, *evidence_);
                }();
                perf_tests::do_not_optimize(result);
                require(
                  result.has_value(), "supplied metadata evidence mismatch");
            }
            perf_tests::stop_measuring_time();
            co_return std::size_t{256};
        } else if constexpr (Op == metadata_operation::pages_verify) {
            perf_tests::start_measuring_time();
            verifier_type verifier{*root_, work.policy()};
            for (auto& page : pages_) {
                fragmented_buffer_parser input{page.share()};
                const auto memory = codec::reserve_decode_input(
                                      input, work.policy(), memory_)
                                      .value();
                auto result = co_await verifier.next(input, memory, work);
                perf_tests::do_not_optimize(result);
                require(
                  result && input.at_end(), "metadata walk rejected page");
                co_await work.drain_inline(
                  work.byte_quantum(), work.item_quantum());
                result = codec::failure(codec::error{errc::success});
                co_await work.drain_inline(
                  work.byte_quantum(), work.item_quantum());
                input = fragmented_buffer_parser{};
            }
            const auto complete = verifier.finish(work);
            perf_tests::do_not_optimize(complete);
            perf_tests::stop_measuring_time();
            require(
              complete && complete->entry_count() == 4U * count_
                && complete->root_digest() == root_->digest(),
              "incomplete metadata walk");
        } else if constexpr (
          Op == metadata_operation::root_encode
          || Op == metadata_operation::page_encode) {
            perf_tests::start_measuring_time();
            auto result = co_await encode<Op>(work);
            perf_tests::do_not_optimize(result);
            perf_tests::stop_measuring_time();
            require(result.has_value(), "metadata encode failed");
            const auto& expected = Op == metadata_operation::root_encode
                                     ? root_wire_
                                     : pages_[0];
            require(
              co_await codec::bench::buffers_equal(
                result->bytes, expected, work),
              "metadata encode changed bytes");
            if constexpr (Op == metadata_operation::root_encode)
                require(
                  result->digest == root_->digest(), "root digest changed");
            else
                require(
                  result->reference == root_->pages()[0],
                  "page reference changed");
            report_output(result->bytes);
            perf_tests::start_measuring_time();
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            result = codec::failure(codec::error{errc::success});
            perf_tests::stop_measuring_time();
        } else {
            auto& wire = Op == metadata_operation::root_decode ? root_wire_
                                                               : pages_[0];
            fragmented_buffer_parser input{wire.share()};
            const auto memory = codec::reserve_decode_input(
                                  input, work.policy(), memory_)
                                  .value();
            perf_tests::start_measuring_time();
            auto result = co_await decode<Op>(input, memory, work);
            perf_tests::do_not_optimize(result);
            perf_tests::stop_measuring_time();
            require(result && input.at_end(), "metadata decode failed");
            if constexpr (Op == metadata_operation::root_decode)
                require(
                  result->value.digest() == root_->digest()
                    && std::ranges::equal(
                      result->value.pages(), root_->pages()),
                  "root changed references");
            else
                require(
                  std::ranges::equal(result->value.entries(), entries_),
                  "page changed entries");
            perf_tests::start_measuring_time();
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            result = codec::failure(codec::error{errc::success});
            co_await work.drain_inline(
              work.byte_quantum(), work.item_quantum());
            input = fragmented_buffer_parser{};
            perf_tests::stop_measuring_time();
        }
        co_return std::size_t{1};
    }

private:
    template<metadata_operation Op>
    auto decode(
      fragmented_buffer_parser& input,
      codec::decode_budget memory,
      codec::cooperative_work& work) {
        if constexpr (Op == metadata_operation::root_decode) {
            if constexpr (Manifest)
                return decode_range_manifest_root(
                  input,
                  source_->manifest_header.context(),
                  source_->manifest_header.logical_span(),
                  source_->alignment,
                  digest_,
                  memory,
                  work);
            else
                return decode_sparse_index_root(
                  input, source_->index_context, digest_, memory, work);
        } else {
            if constexpr (Manifest)
                return decode_range_manifest_page(
                  input, *root_, page_ordinal::make(0).value(), memory, work);
            else
                return decode_sparse_index_page(
                  input, *root_, page_ordinal::make(0).value(), memory, work);
        }
    }
    template<metadata_operation Op>
    auto encode(codec::cooperative_work& work) {
        if constexpr (Op == metadata_operation::root_encode) {
            if constexpr (Manifest)
                return encode_range_manifest_root(
                  source_->manifest_header,
                  root_->pages(),
                  source_->alignment,
                  work,
                  memory_.operation_remaining,
                  capacity_bound);
            else
                return encode_sparse_index_root(
                  source_->index_context,
                  count_,
                  root_->pages(),
                  work,
                  memory_.operation_remaining,
                  capacity_bound);
        } else {
            if constexpr (Manifest)
                return encode_range_manifest_page(
                  source_->page_header(0),
                  entries_,
                  source_->alignment,
                  work,
                  memory_.operation_remaining,
                  capacity_bound);
            else
                return encode_sparse_index_page(
                  entries_,
                  source_->index_context,
                  page_ordinal::make(0).value(),
                  0,
                  work,
                  memory_.operation_remaining,
                  capacity_bound);
        }
    }
    seastar::future<fragmented_buffer>
    layout(std::string_view wire, codec::cooperative_work& work) {
        co_return co_await codec::bench::copy_layout(
          fragmented_buffer::copy_of(wire).value(),
          width_,
          work,
          memory_.operation_remaining);
    }
    template<metadata_operation Op>
    seastar::future<> initialize() {
        if (source_) co_return;
        codec::bench::qualify_allocator();
        constexpr bool supplied = Op == metadata_operation::extent_compare
                                  || Op == metadata_operation::anchor_compare;
        source_.emplace(
          Manifest,
          supplied ? 1U : count_,
          Op == metadata_operation::pages_verify ? 4U : 1U,
          header_,
          alignment_,
          false);
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        if constexpr (supplied) {
            auto bytes = co_await layout(testing::data_block(), work);
            const auto scope = testing::scope(100, 101, 0, 1, 512, 1024);
            {
                fragmented_buffer_parser input{bytes.share()};
                const auto memory = codec::reserve_decode_input(
                                      input, work.policy(), memory_)
                                      .value();
                auto decoded
                  = (co_await decode_segment_block(
                       input, testing::block_expected(), memory, work))
                      .value();
                block_ = decoded.value.descriptor();
            }
            const auto cost = bytes.allocation_cost(capacity_bound).value();
            const auto memory
              = codec::detail::consume_decode_budget(
                  work.policy(),
                  memory_,
                  cost.backing,
                  cost.descriptors.checked_add(cost.share_controls).value(),
                  {},
                  0)
                  .value();
            auto verifier = extent_verifier::make(
                              testing::history(),
                              scope,
                              work.policy(),
                              extent_layout_kind::initial_append,
                              {},
                              extent_integrity::crc32c_and_sha256)
                              .value();
            (co_await verifier.add_block(
               std::move(bytes), testing::batch_expected(), memory, work))
              .value();
            evidence_ = verifier.finish(work).value();
            source_->index_context = sparse_index_context::make(
                                       testing::sc(),
                                       scope,
                                       *evidence_->digest(),
                                       testing::alignment())
                                       .value();
        }
        if constexpr (Manifest)
            entries_ = source_->manifest_entries(0);
        else
            entries_ = source_->index_entries(0);
        if constexpr (Manifest && supplied)
            entries_[0] = range_manifest_entry::make(
                            testing::sc().segment(),
                            testing::sc().generation(),
                            evidence_->boundary().coverage,
                            *evidence_->digest())
                            .value();
        std::vector<page_ref> refs;
        refs.reserve(source_->pages);
        pages_.reserve(source_->pages);
        for (std::uint32_t ordinal = 0; ordinal < source_->pages; ++ordinal) {
            const auto wire = source_->page_wire(ordinal);
            refs.push_back(source_->reference(wire, ordinal));
            pages_.push_back(co_await layout(wire, work));
        }
        const auto wire = source_->root_wire(refs);
        digest_ = codec::immutable_object_digest{testing::index::sha(wire)};
        root_wire_ = co_await layout(wire, work);
        {
            fragmented_buffer_parser input{root_wire_.share()};
            const auto memory = codec::reserve_decode_input(
                                  input, work.policy(), memory_)
                                  .value();
            auto decoded = (co_await decode<metadata_operation::root_decode>(
                              input, memory, work))
                             .value();
            root_.emplace(std::move(decoded.value));
        }
        // Cached buffers, entry/ref vectors, native engines and frames remain
        // reserved across every sample. Current input shares are additionally
        // admitted conservatively, as in the other storage benchmark cases.
        byte_count retained, metadata;
        const auto account = [&](const fragmented_buffer& bytes) {
            const auto cost = bytes.allocation_cost(capacity_bound).value();
            retained = retained.checked_add(cost.backing)
                         .value()
                         .checked_add(cost.descriptors)
                         .value()
                         .checked_add(cost.share_controls)
                         .value();
            metadata = metadata.checked_add(cost.descriptors)
                         .value()
                         .checked_add(cost.share_controls)
                         .value();
        };
        account(root_wire_);
        for (const auto& page : pages_)
            account(page);
        const auto arrays
          = capacity_bound(byte_count{entries_.capacity() * sizeof(entry_type)})
              .checked_add(capacity_bound(
                byte_count{root_->page_capacity() * sizeof(page_ref)}))
              .value()
              .checked_add(capacity_bound(
                byte_count{pages_.capacity() * sizeof(fragmented_buffer)}))
              .value();
        retained = retained.checked_add(arrays).value();
        metadata = metadata.checked_add(arrays).value();
        // Reserve cached metadata against both ceilings before any sample.
        memory_ = codec::detail::consume_decode_budget(
                    work.policy(),
                    memory_,
                    retained.checked_sub(metadata).value(),
                    metadata,
                    {},
                    0)
                    .value();
        std::uint64_t bytes = Op == metadata_operation::root_decode
                                  || Op == metadata_operation::root_encode
                                ? root_wire_.size().value()
                                : pages_[0].size().value();
        if constexpr (Op == metadata_operation::pages_verify) {
            bytes = 0;
            for (const auto& page : pages_)
                bytes += page.size().value();
        }
        fmt::print(
          "kwaque-storage-metadata-fixture-v1 family={} scope={} entries={} "
          "pages={} bytes={} header={} alignment={} fragments={} "
          "cached_upper={} normalization={}\n",
          Manifest ? "manifest" : "index",
          scope_name(Op),
          source_->entries * source_->pages,
          source_->pages,
          bytes,
          header_,
          alignment_,
          pages_[0].fragment_count(),
          retained.value(),
          supplied                                 ? "comparisons"
          : Op == metadata_operation::pages_verify ? "objects"
                                                   : "operations");
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
    std::size_t width_;
    std::uint32_t count_;
    std::size_t header_;
    std::uint64_t alignment_;
    std::optional<testing::metadata_fixture> source_;
    std::vector<entry_type> entries_;
    fragmented_buffer root_wire_;
    std::vector<fragmented_buffer> pages_;
    std::optional<root_type> root_;
    std::optional<verified_extent> evidence_;
    std::optional<complete_block_descriptor> block_;
    codec::immutable_object_digest digest_{{}};
    codec::decode_budget memory_{
      byte_count{63U << 20U}, byte_count{1U << 20U}, capacity_bound};
    bool reported_{false};
};
template<
  bool Manifest,
  std::size_t Width,
  std::uint32_t Count,
  std::size_t Header = 32,
  std::uint64_t Alignment = 512>
struct metadata_fixture : metadata_measurements<Manifest> {
    metadata_fixture()
      : metadata_measurements<Manifest>(Width, Count, Header, Alignment) {}
};
using storage_index_tiny = metadata_fixture<false, 512, 1>;
using storage_index_frag7 = metadata_fixture<false, 7, 32>;
using storage_index_page_max = metadata_fixture<false, 4096, 4083>;
using storage_index_extended = metadata_fixture<false, 4096, 3829, 4096, 65536>;
using storage_index_walk = metadata_fixture<false, 4096, 64>;
using storage_manifest_tiny = metadata_fixture<true, 512, 1>;
using storage_manifest_frag7 = metadata_fixture<true, 7, 32>;
using storage_manifest_page_max = metadata_fixture<true, 4096, 628>;
using storage_manifest_extended
  = metadata_fixture<true, 4096, 589, 4096, 65536>;
using storage_manifest_walk = metadata_fixture<true, 4096, 64>;
#define METADATA_CASE(group, op)                                               \
    PERF_TEST_F(group, op) { return measure<metadata_operation::op>(); }
#define METADATA_CODECS(group)                                                 \
    METADATA_CASE(group, root_encode)                                          \
    METADATA_CASE(group, root_decode)                                          \
    METADATA_CASE(group, page_encode)                                          \
    METADATA_CASE(group, page_decode)
METADATA_CODECS(storage_index_tiny)
METADATA_CODECS(storage_index_frag7)
METADATA_CODECS(storage_index_page_max)
METADATA_CASE(storage_index_extended, root_decode)
METADATA_CASE(storage_index_extended, page_decode)
METADATA_CASE(storage_index_walk, pages_verify)
METADATA_CASE(storage_index_tiny, anchor_compare)
METADATA_CASE(storage_index_tiny, extent_compare)
METADATA_CODECS(storage_manifest_tiny)
METADATA_CODECS(storage_manifest_frag7)
METADATA_CODECS(storage_manifest_page_max)
METADATA_CASE(storage_manifest_extended, root_decode)
METADATA_CASE(storage_manifest_extended, page_decode)
METADATA_CASE(storage_manifest_walk, pages_verify)
METADATA_CASE(storage_manifest_tiny, extent_compare)
#undef METADATA_CODECS
#undef METADATA_CASE
} // namespace
} // namespace kwaque::storage::bench
