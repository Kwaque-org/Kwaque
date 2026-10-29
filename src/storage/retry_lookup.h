#pragma once

#include "src/storage/local_generation.h"
#include "src/storage/local_root.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <cstdint>
#include <optional>
#include <span>
#include <utility>
#include <vector>

namespace kwaque::storage {

// The batch identities one page of a retry summary begins and ends with.
struct retry_page_span final {
    model::batch_id first, last;
};

// A search directory derived from verified pages: each page's first and last
// batch identity, in page order. A root's page references carry no keys, so
// the directory is built by reading every page once; that read is the cold
// cost of the first lookup. It holds one span per page and no entry, so a
// root of the largest size costs a fixed, admitted table.
class retry_page_directory final {
public:
    [[nodiscard]] static runtime::result<retry_page_directory>
    make(workload_budget& budget, std::uint32_t pages);
    retry_page_directory(retry_page_directory&&) noexcept = default;
    retry_page_directory& operator=(retry_page_directory&&) noexcept = default;
    retry_page_directory(const retry_page_directory&) = delete;
    retry_page_directory& operator=(const retry_page_directory&) = delete;

    // The next page, in ordinal order. Its entries are already in order; it
    // must begin after the page before it ended.
    [[nodiscard]] runtime::result<void>
    add(std::span<const completed_retry> entries);
    [[nodiscard]] std::uint32_t pages() const noexcept {
        return static_cast<std::uint32_t>(spans_.size());
    }
    // Every page the root pins has been added.
    [[nodiscard]] bool complete() const noexcept {
        return spans_.size() == expected_;
    }
    // The one page that can hold `id`: the last page that begins at or before
    // it, unless that page ends before it. Nothing before the first page,
    // between two pages or after the last.
    [[nodiscard]] std::optional<std::uint32_t>
    find(const model::batch_id& id) const noexcept;
    // The page holding the greatest entry at or below `id`: the last page that
    // begins at or before it, wherever that page ends. Nothing before the
    // first page.
    [[nodiscard]] std::optional<std::uint32_t>
    floor(const model::batch_id& id) const noexcept;

private:
    retry_page_directory(
      workload_reservation held, std::uint32_t expected) noexcept
      : held_(std::move(held))
      , expected_(expected) {}
    // Declared first: released after the table it admitted.
    workload_reservation held_;
    std::vector<retry_page_span> spans_;
    std::uint32_t expected_;
};

// The entry with this identity among one page's, which are in order.
[[nodiscard]] const completed_retry* find_completed_retry(
  std::span<const completed_retry> entries, const model::batch_id& id) noexcept;
// The greatest entry at or below this identity among one page's.
[[nodiscard]] const completed_retry* find_completed_retry_floor(
  std::span<const completed_retry> entries, const model::batch_id& id) noexcept;

// Exact and floor lookups and page-wise reading of one opened sealed retry
// summary. Opening reads each page once, checks that the pages continue one
// another in batch identity order, and keeps only their directory. A lookup
// then reads at most one page, and none when the directory already excludes
// the identity. No history is kept in memory and nothing here expires an
// entry: how long a result is retained is its owner's decision.
class retry_root_reader final {
public:
    [[nodiscard]] static seastar::future<runtime::result<retry_root_reader>>
    open(
      local_root_pin pin,
      workload_budget& budget,
      codec::cooperative_work& work);
    retry_root_reader(retry_root_reader&&) noexcept = default;
    retry_root_reader& operator=(retry_root_reader&&) noexcept = delete;
    retry_root_reader(const retry_root_reader&) = delete;
    retry_root_reader& operator=(const retry_root_reader&) = delete;

    [[nodiscard]] local_root_reference reference() const {
        return pin_.reference();
    }
    // Entries the root holds, by its own count.
    [[nodiscard]] std::uint32_t facts() const noexcept { return facts_; }
    [[nodiscard]] std::uint32_t pages() const noexcept {
        return directory_.pages();
    }
    // Pages read so far, the directory's included.
    [[nodiscard]] std::uint64_t pages_read() const noexcept { return read_; }

    // The stored original with this identity, whole, or nothing.
    [[nodiscard]] seastar::future<
      runtime::result<std::optional<completed_retry>>>
    find(model::batch_id id, codec::cooperative_work& work);
    // The newest stored entry of one stream: the greatest at or below the
    // stream's largest identity, when it is the stream's. One page read.
    [[nodiscard]] seastar::future<
      runtime::result<std::optional<completed_retry>>>
    find_stream_last(retry_prefix prefix, codec::cooperative_work& work);
    // One page, for a walk in batch identity order.
    [[nodiscard]] seastar::future<runtime::result<local_retry_page>>
    page(std::uint32_t ordinal, codec::cooperative_work& work);

private:
    retry_root_reader(
      local_root_pin pin,
      retry_page_directory directory,
      std::uint32_t facts,
      std::uint64_t read) noexcept
      : pin_(std::move(pin))
      , directory_(std::move(directory))
      , facts_(facts)
      , read_(read) {}
    local_root_pin pin_;
    retry_page_directory directory_;
    std::uint32_t facts_;
    std::uint64_t read_;
};

// The durable completed-retry state of one pinned segment generation: its
// sealed summary, which a generation has once it is sealed. Each original
// result is returned as it was stored: its binding and acknowledgement
// generation are the original ones, whatever generation holds it now.
class completed_retry_lookup final {
public:
    // Opens the summary the generation's publication pins, if it has one.
    [[nodiscard]] static seastar::future<
      runtime::result<completed_retry_lookup>>
    open(
      const local_generation_pin& generation,
      workload_budget& budget,
      codec::cooperative_work& work);
    completed_retry_lookup(completed_retry_lookup&&) noexcept = default;
    completed_retry_lookup&
    operator=(completed_retry_lookup&&) noexcept = delete;
    completed_retry_lookup(const completed_retry_lookup&) = delete;
    completed_retry_lookup& operator=(const completed_retry_lookup&) = delete;

    [[nodiscard]] std::optional<retry_root_reader>& summary() & noexcept {
        return summary_;
    }
    [[nodiscard]] std::uint32_t facts() const noexcept {
        return summary_ ? summary_->facts() : 0U;
    }
    [[nodiscard]] std::uint64_t pages_read() const noexcept {
        return summary_ ? summary_->pages_read() : 0U;
    }

    [[nodiscard]] seastar::future<
      runtime::result<std::optional<completed_retry>>>
    find(model::batch_id id, codec::cooperative_work& work);
    [[nodiscard]] seastar::future<
      runtime::result<std::optional<completed_retry>>>
    find_stream_last(retry_prefix prefix, codec::cooperative_work& work);

    // Every durable fact once, in batch identity order, a page at a time.
    // visit(const completed_retry&) returns false to stop; the result says
    // whether the walk ran to the end.
    template<typename Visit>
    [[nodiscard]] seastar::future<runtime::result<bool>>
    enumerate(Visit visit, codec::cooperative_work& work) {
        if (!summary_) co_return true;
        for (std::uint32_t ordinal = 0; ordinal != summary_->pages();
             ++ordinal) {
            auto page = co_await summary_->page(ordinal, work);
            if (!page) co_return runtime::failure(page.error());
            for (const auto& fact : page->entries())
                if (!visit(fact)) co_return false;
        }
        co_return true;
    }

private:
    explicit completed_retry_lookup(
      std::optional<retry_root_reader> summary) noexcept
      : summary_(std::move(summary)) {}
    std::optional<retry_root_reader> summary_;
};

} // namespace kwaque::storage
