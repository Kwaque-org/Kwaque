#include "src/storage/retry_lookup.h"

#include <limits>

namespace kwaque::storage {

runtime::result<retry_page_directory>
retry_page_directory::make(workload_budget& budget, std::uint32_t pages) {
    if (pages > maximum_object_pages)
        return runtime::failure(detail::path_error(errc::invalid_argument));
    // An empty root still holds one span's worth.
    const auto charge = budget.allocation_charge(
      byte_count{std::max<std::size_t>(pages, 1) * sizeof(retry_page_span)});
    if (!charge) return runtime::failure(charge.error());
    auto held = budget.try_reserve(*charge);
    if (!held) return runtime::failure(held.error());
    retry_page_directory output{std::move(*held), pages};
    output.spans_.reserve(pages);
    return output;
}

runtime::result<void>
retry_page_directory::add(std::span<const completed_retry> entries) {
    if (complete() || entries.empty())
        return runtime::failure(detail::path_error(errc::malformed_data));
    const auto first = entries.front().id();
    const auto last = entries.back().id();
    // Pages continue one another: a later page never reaches back.
    if (!spans_.empty() && !spans_.back().last.canonical_less(first))
        return runtime::failure(detail::path_error(errc::malformed_data));
    if (last.canonical_less(first))
        return runtime::failure(detail::path_error(errc::malformed_data));
    spans_.push_back({first, last});
    return {};
}

std::optional<std::uint32_t>
retry_page_directory::find(const model::batch_id& id) const noexcept {
    // The first page that begins after the identity; the one before it is
    // the last that begins at or before it.
    const auto after = std::upper_bound(
      spans_.begin(),
      spans_.end(),
      id,
      [](const model::batch_id& wanted, const retry_page_span& page) {
          return wanted.canonical_less(page.first);
      });
    if (after == spans_.begin()) return std::nullopt;
    const auto page = after - 1;
    if (page->last.canonical_less(id)) return std::nullopt;
    return static_cast<std::uint32_t>(page - spans_.begin());
}

std::optional<std::uint32_t>
retry_page_directory::floor(const model::batch_id& id) const noexcept {
    const auto after = std::upper_bound(
      spans_.begin(),
      spans_.end(),
      id,
      [](const model::batch_id& wanted, const retry_page_span& page) {
          return wanted.canonical_less(page.first);
      });
    if (after == spans_.begin()) return std::nullopt;
    return static_cast<std::uint32_t>(after - 1 - spans_.begin());
}

const completed_retry* find_completed_retry(
  std::span<const completed_retry> entries,
  const model::batch_id& id) noexcept {
    const auto at = std::lower_bound(
      entries.begin(),
      entries.end(),
      id,
      [](const completed_retry& stored, const model::batch_id& wanted) {
          return stored.id().canonical_less(wanted);
      });
    if (at == entries.end() || at->id() != id) return nullptr;
    return &*at;
}

const completed_retry* find_completed_retry_floor(
  std::span<const completed_retry> entries,
  const model::batch_id& id) noexcept {
    const auto after = std::upper_bound(
      entries.begin(),
      entries.end(),
      id,
      [](const model::batch_id& wanted, const completed_retry& stored) {
          return wanted.canonical_less(stored.id());
      });
    if (after == entries.begin()) return nullptr;
    return &*(after - 1);
}

seastar::future<runtime::result<retry_root_reader>> retry_root_reader::open(
  local_root_pin pin, workload_budget& budget, codec::cooperative_work& work) {
    const auto* summary = std::get_if<sealed_footer>(&pin.metadata());
    if (!summary)
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    const auto facts = summary->retry_count();
    const auto pages = pin.reference().pages().value();
    auto directory = retry_page_directory::make(budget, pages);
    if (!directory) co_return runtime::failure(directory.error());
    std::uint64_t counted = 0;
    for (std::uint32_t ordinal = 0; ordinal != pages; ++ordinal) {
        const auto place = page_ordinal::make(ordinal);
        if (!place)
            co_return runtime::failure(detail::path_error(errc::out_of_range));
        auto page = co_await pin.read_retries(*place, work);
        if (!page) co_return runtime::failure(page.error());
        if (auto added = directory->add(page->entries()); !added)
            co_return runtime::failure(added.error());
        counted += page->entries().size();
    }
    if (counted != facts)
        co_return runtime::failure(detail::path_error(errc::malformed_data));
    co_return retry_root_reader{
      std::move(pin), std::move(*directory), facts, pages};
}

seastar::future<runtime::result<std::optional<completed_retry>>>
retry_root_reader::find(model::batch_id id, codec::cooperative_work& work) {
    using output = std::optional<completed_retry>;
    const auto ordinal = directory_.find(id);
    if (!ordinal) co_return output{};
    auto page = co_await this->page(*ordinal, work);
    if (!page) co_return runtime::failure(page.error());
    const auto* found = find_completed_retry(page->entries(), id);
    if (!found) co_return output{};
    co_return output{*found};
}

seastar::future<runtime::result<std::optional<completed_retry>>>
retry_root_reader::find_stream_last(
  retry_prefix prefix, codec::cooperative_work& work) {
    using output = std::optional<completed_retry>;
    // The stream's largest identity: no entry of it lies above.
    const auto probe = model::batch_id::make(
      prefix.producer,
      prefix.epoch,
      prefix.stream,
      model::batch_sequence{std::numeric_limits<std::uint64_t>::max()});
    if (!probe)
        co_return runtime::failure(detail::path_error(errc::invalid_argument));
    const auto ordinal = directory_.floor(*probe);
    if (!ordinal) co_return output{};
    auto page = co_await this->page(*ordinal, work);
    if (!page) co_return runtime::failure(page.error());
    const auto* found = find_completed_retry_floor(page->entries(), *probe);
    // A neighbour's entry is never the answer.
    if (!found || retry_prefix::of(found->id()) != prefix) co_return output{};
    co_return output{*found};
}

seastar::future<runtime::result<local_retry_page>>
retry_root_reader::page(std::uint32_t ordinal, codec::cooperative_work& work) {
    const auto place = page_ordinal::make(ordinal);
    if (!place || ordinal >= directory_.pages())
        co_return runtime::failure(detail::path_error(errc::out_of_range));
    auto page = co_await pin_.read_retries(*place, work);
    if (page) ++read_;
    co_return page;
}

seastar::future<runtime::result<completed_retry_lookup>>
completed_retry_lookup::open(
  const local_generation_pin& generation,
  workload_budget& budget,
  codec::cooperative_work& work) {
    std::optional<retry_root_reader> summary;
    for (const auto& root : generation.publication().roots) {
        if (root.kind() != local_root_kind::sealed_retry) continue;
        auto pin = generation.root(root.kind());
        if (!pin) co_return runtime::failure(pin.error());
        auto reader = co_await retry_root_reader::open(
          std::move(*pin), budget, work);
        if (!reader) co_return runtime::failure(reader.error());
        summary.emplace(std::move(*reader));
    }
    co_return completed_retry_lookup{std::move(summary)};
}

seastar::future<runtime::result<std::optional<completed_retry>>>
completed_retry_lookup::find(
  model::batch_id id, codec::cooperative_work& work) {
    if (!summary_) co_return std::optional<completed_retry>{};
    co_return co_await summary_->find(id, work);
}

seastar::future<runtime::result<std::optional<completed_retry>>>
completed_retry_lookup::find_stream_last(
  retry_prefix prefix, codec::cooperative_work& work) {
    if (!summary_) co_return std::optional<completed_retry>{};
    co_return co_await summary_->find_stream_last(prefix, work);
}

} // namespace kwaque::storage
