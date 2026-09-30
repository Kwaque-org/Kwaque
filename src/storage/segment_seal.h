#pragma once

#include "src/storage/local_publication.h"
#include "src/storage/local_store_config.h"
#include "src/storage/retry_format.h"
#include "src/storage/segment_immutable.h"

namespace kwaque::storage {
struct segment_seal_outcome final {
    runtime::first_failure failure;
    // Present together only after durable sealed-pointer publication. These
    // are segment installation facts, not a combined local append result.
    std::optional<local_footer_reference> boundary;
    std::optional<local_root_reference> retry;
    std::optional<segment_immutable_expectation> extent;
    // The caller's snapshot/WAL-retention owner continues to own these facts.
    // An empty completed summary never discharges unresolved obligations.
    std::uint32_t unresolved{0};
};
struct segment_seal_progress final {
    bool data_synced{false}, root_written{false}, root_synced{false};
    std::optional<verified_extent> extent;
    // Reconciliation candidates, including after uncertain effects. Merely
    // retaining these identities is not successful installation evidence.
    std::optional<local_footer_reference> boundary_candidate;
    std::optional<local_root_reference> retry_candidate;
    local_publication_outcome retry, pointer;
};

namespace detail {
// The supplied source owns an immutable, already charged finite snapshot.
// read(first,count,work) returns exactly that slice in a bounded vector, whose
// transient allocation is funded by this working budget. Replaying a slice
// must return identical facts. No history is retained by the page adapter;
// The bundle verifier checks the second pass against the exact PageRefs.
template<typename Source>
struct segment_retry_pages final {
    Source* source;
    footer_expectation location;
    std::uint32_t total, capacity;
    local_store_io_limits limits;
    byte_count retained_refs;
    std::uint32_t first{0}, ordinal{0};
    std::optional<model::batch_id> previous;
    std::optional<page_ref> last;

    seastar::future<runtime::result<std::optional<bytes::fragmented_buffer>>>
    next(codec::cooperative_work& work) {
        if (first == total) co_return std::optional<bytes::fragmented_buffer>{};
        const auto count = std::min(capacity, total - first);
        auto entries = co_await source->read(first, count, work);
        if (!entries) co_return runtime::failure(entries.error());
        if (entries->size() != count || entries->capacity() > capacity)
            co_return runtime::failure(path_error(errc::wrong_context));
        const byte_count requested{
          entries->capacity() * sizeof(completed_retry)};
        const auto served = limits.charge(requested);
        const auto used = served.checked_add(retained_refs);
        if (
          served < requested || !work.policy().validate_allocation(served)
          || !used || *used > limits.operation_bytes
          || *used > limits.metadata_bytes)
            co_return runtime::failure(path_error(errc::resource_exhausted));
        if (previous && !previous->canonical_less(entries->front().id()))
            co_return runtime::failure(path_error(errc::invalid_argument));
        auto page = co_await encode_retry_page(
          *entries,
          location,
          page_ordinal::make(ordinal).value(),
          first,
          work,
          limits.operation_bytes.checked_sub(*used).value(),
          limits.charge);
        if (!page) co_return runtime::failure(path_error(page.error().code()));
        previous = entries->back().id();
        first += count;
        ++ordinal;
        last = page->reference;
        co_return std::optional<bytes::fragmented_buffer>{
          std::move(page->bytes)};
    }
};
} // namespace detail
} // namespace kwaque::storage
