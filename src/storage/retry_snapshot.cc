#include "src/storage/retry_snapshot.h"

#include "src/storage/local_metadata_layout.h"

#include <algorithm>

namespace kwaque::storage {
namespace {
// What a fragmented table of `count` facts is charged: its first fragment is
// reserved for exactly what fits of them, and every later one is allocated
// whole.
runtime::result<byte_count>
facts_charge(const workload_budget& budget, std::size_t count) {
    constexpr auto fragment
      = seastar::chunked_vector<completed_retry>::elements_per_fragment();
    byte_count total;
    for (std::size_t left = count; left != 0;) {
        const auto held = left == count ? std::min(left, fragment) : fragment;
        const auto charged = budget.allocation_charge(
          byte_count{held * sizeof(completed_retry)});
        if (!charged) return runtime::failure(charged.error());
        const auto next = total.checked_add(*charged);
        if (!next)
            return runtime::failure(detail::path_error(errc::out_of_range));
        total = *next;
        left -= std::min(left, held);
    }
    return total;
}
} // namespace

runtime::result<std::uint32_t> retry_snapshot_page_entries(
  const local_store_io_limits& limits,
  storage_alignment alignment,
  const codec::limits& policy) noexcept {
    if (auto valid = limits.validate(); !valid)
        return runtime::failure(valid.error());
    const auto encoded = local_metadata_page_capacity(
      local_metadata_kind::completed_retry_page,
      byte_count{codec::envelope_prefix_bytes},
      alignment,
      policy);
    if (!encoded)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    const auto room = std::min(
      limits.metadata_bytes.value() / 2, limits.operation_bytes.value() / 4);
    // The charge of an array never falls as it grows.
    std::uint32_t low = 0, high = *encoded;
    while (low != high) {
        const auto count = low + (high - low + 1) / 2;
        const byte_count requested{
          std::uint64_t{count} * sizeof(completed_retry)};
        const auto served = limits.charge(requested);
        if (
          served >= requested && served.value() <= room
          && policy.validate_allocation(served))
            low = count;
        else
            high = count - 1;
    }
    if (low == 0)
        return runtime::failure(detail::path_error(errc::resource_exhausted));
    return low;
}

seastar::future<runtime::result<std::vector<completed_retry>>>
merged_retry_source::read(
  std::uint32_t first, std::uint32_t count, codec::cooperative_work& work) {
    if (first > completed() || count > completed() - first)
        co_return runtime::failure(detail::path_error(errc::out_of_range));
    // A second pass starts again; a pass itself only moves forward.
    if (first < position_) {
        position_ = page_ = within_ = taken_ = 0;
        loaded_.reset();
    }
    std::vector<completed_retry> output;
    output.reserve(count);
    const auto end = first + count;
    while (position_ != end) {
        const completed_retry* stored = nullptr;
        if (durable_ && page_ != durable_->pages()) {
            if (!loaded_) {
                auto page = co_await durable_->page(page_, work);
                if (!page) co_return runtime::failure(page.error());
                loaded_.emplace(std::move(*page));
            }
            stored = &loaded_->entries()[within_];
        }
        const completed_retry* waiting = taken_ != pending_->size()
                                           ? &(*pending_)[taken_]
                                           : nullptr;
        // Fewer facts than the two inputs counted, or one identity in both:
        // the inputs are not the disjoint sets this merge was made for.
        if (
          (!stored && !waiting)
          || (stored && waiting && stored->id() == waiting->id()))
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        const bool durable
          = stored && (!waiting || stored->id().canonical_less(waiting->id()));
        if (position_ >= first) output.push_back(durable ? *stored : *waiting);
        ++position_;
        if (!durable) {
            ++taken_;
        } else if (++within_ == loaded_->entries().size()) {
            loaded_.reset();
            within_ = 0;
            ++page_;
        }
    }
    co_return output;
}

namespace detail {
runtime::result<workload_reservation>
admit_retry_facts(workload_budget& budget, std::size_t count) {
    const auto charge = facts_charge(budget, std::max<std::size_t>(count, 1));
    if (!charge) return runtime::failure(charge.error());
    return budget.try_reserve(*charge);
}

const completed_retry* held_retry(
  const seastar::chunked_vector<completed_retry>& facts,
  const model::batch_id& id) noexcept {
    const auto at = std::lower_bound(
      facts.begin(),
      facts.end(),
      id,
      [](const completed_retry& stored, const model::batch_id& wanted) {
          return stored.id().canonical_less(wanted);
      });
    if (at == facts.end() || at->id() != id) return nullptr;
    return &*at;
}

void insert_retry(
  seastar::chunked_vector<completed_retry>& facts,
  const completed_retry& fact) {
    // The place is taken as an index: growing the table moves positions.
    const auto position
      = std::lower_bound(
          facts.begin(),
          facts.end(),
          fact.id(),
          [](const completed_retry& stored, const model::batch_id& wanted) {
              return stored.id().canonical_less(wanted);
          })
        - facts.begin();
    facts.push_back(fact);
    std::rotate(facts.begin() + position, facts.end() - 1, facts.end());
}

void restore_retries(
  seastar::chunked_vector<completed_retry>& into,
  seastar::chunked_vector<completed_retry>& from) {
    for (const auto& fact : from)
        insert_retry(into, fact);
    from = {};
}

runtime::result<retry_cut> copy_retry_facts(
  workload_budget& budget,
  const seastar::chunked_vector<completed_retry>& facts) {
    if (facts.empty())
        return runtime::failure(path_error(errc::invalid_argument));
    const auto charge = facts_charge(budget, facts.size());
    if (!charge) return runtime::failure(charge.error());
    auto held = budget.try_reserve(*charge);
    if (!held) return runtime::failure(held.error());
    retry_cut output{std::move(*held), {}};
    output.facts.reserve(facts.size());
    for (const auto& fact : facts)
        output.facts.push_back(fact);
    return output;
}

seastar::future<runtime::result<std::optional<bytes::fragmented_buffer>>>
retry_snapshot_pages::next(codec::cooperative_work& work) {
    using output = std::optional<bytes::fragmented_buffer>;
    if (first == total()) co_return output{};
    const auto count = std::min(capacity, total() - first);
    if (count == 0)
        co_return runtime::failure(path_error(errc::invalid_argument));
    // The page's facts are copied once for the encoder, within the working
    // budget the page is encoded under.
    const byte_count requested{count * sizeof(completed_retry)};
    const auto served = limits.charge(requested);
    if (
      served < requested || !work.policy().validate_allocation(served)
      || served > limits.operation_bytes)
        co_return runtime::failure(path_error(errc::resource_exhausted));
    std::vector<completed_retry> slice;
    if (merged) {
        auto read = co_await merged->read(first, count, work);
        if (!read) co_return runtime::failure(read.error());
        slice = std::move(*read);
    } else {
        slice.reserve(count);
        for (std::uint32_t i = 0; i != count; ++i)
            slice.push_back((*facts)[first + i]);
    }
    const auto generation = local_publication_generation::make(
      sequence.value());
    const auto place = page_ordinal::make(ordinal);
    if (!generation || !place)
        co_return runtime::failure(path_error(errc::out_of_range));
    const auto header = local_metadata_header::make(
      local_metadata_kind::completed_retry_page, owner, *generation);
    if (!header) co_return runtime::failure(path_error(errc::invalid_argument));
    const local_metadata_payload payload{local_completed_retry_page{
      segment, sequence, *place, first, std::move(slice)}};
    auto page = co_await encode_local_metadata(
      {*header, alignment, segment_alignment, {}},
      payload,
      work,
      limits.operation_bytes.checked_sub(served).value(),
      limits.charge);
    if (!page) co_return runtime::failure(path_error(page.error().code()));
    const auto reference = page_ref::make(
      *place, first, count, page->bytes.size(), page->digest);
    if (!reference)
        co_return runtime::failure(path_error(errc::resource_exhausted));
    last = *reference;
    first += count;
    ++ordinal;
    co_return output{std::move(page->bytes)};
}

seastar::future<runtime::result<encoded_retry_snapshot>>
encode_retry_snapshot_root(
  retry_snapshot_pages& pages,
  local_footer_reference footer,
  codec::cooperative_work& work) {
    const auto total = pages.total();
    std::vector<page_ref> references;
    references.reserve((total + pages.capacity - 1) / pages.capacity);
    for (;;) {
        auto page = co_await pages.next(work);
        if (!page) co_return runtime::failure(page.error());
        if (!*page) break;
        references.push_back(*pages.last);
    }
    pages.first = pages.ordinal = 0;
    pages.last.reset();
    const auto generation = local_publication_generation::make(
      pages.sequence.value());
    const auto count = page_count::make(
      static_cast<std::uint32_t>(references.size()));
    if (!generation || !count)
        co_return runtime::failure(path_error(errc::out_of_range));
    const auto header = local_metadata_header::make(
      local_metadata_kind::completed_retry_root, pages.owner, *generation);
    if (!header) co_return runtime::failure(path_error(errc::invalid_argument));
    const local_metadata_payload payload{local_completed_retry_root{
      pages.segment, footer, total, std::move(references)}};
    auto root = co_await encode_local_metadata(
      {*header, pages.alignment, pages.segment_alignment, {}},
      payload,
      work,
      pages.limits.operation_bytes,
      pages.limits.charge);
    if (!root) co_return runtime::failure(path_error(root.error().code()));
    const auto reference = local_root_reference::make(
      local_root_kind::completed_retry_snapshot,
      pages.sequence,
      runtime::file_position{},
      root->bytes.size(),
      *count,
      root->digest);
    if (!reference)
        co_return runtime::failure(path_error(errc::invalid_argument));
    local_metadata_expectation expected{*header, pages.alignment};
    expected.segment = pages.segment;
    expected.segment_alignment = pages.segment_alignment;
    expected.digest = reference->digest();
    expected.encoded_bytes = reference->bytes();
    co_return encoded_retry_snapshot{
      *reference, std::move(expected), std::move(root->bytes)};
}
} // namespace detail

} // namespace kwaque::storage
