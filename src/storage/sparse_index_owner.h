#pragma once

#include "src/base/invariant.h"
#include "src/codec/cooperative.h"
#include "src/codec/limits.h"
#include "src/model/position.h"
#include "src/runtime/error.h"
#include "src/runtime/file.h"
#include "src/runtime/first_failure.h"
#include "src/runtime/shard_affinity.h"
#include "src/storage/footer_format.h"
#include "src/storage/local_bundle.h"
#include "src/storage/local_generation.h"
#include "src/storage/local_paths.h"
#include "src/storage/local_root.h"
#include "src/storage/local_store_config.h"
#include "src/storage/local_types.h"
#include "src/storage/page_cache.h"
#include "src/storage/segment_scan.h"
#include "src/storage/sparse_index.h"
#include "src/storage/sparse_index_format.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>

#include <boost/intrusive/list.hpp>

#include <algorithm>
#include <array>
#include <cstdint>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <utility>
#include <vector>

namespace kwaque::storage {

template<runtime::file_system_backend Backend, local_directory_owner Owner>
class sealed_sparse_index;

// What a lookup routes by: the part of an index root a lookup reads, copied
// out of the root when it is first read through. Kept by a cache, it lets a
// later lookup route without an open root.
struct sparse_index_routes final {
    // The root these were read through.
    local_root_reference reference;
    // Where the extent it indexes ends: the scan's end after the last anchor
    // of a page.
    runtime::file_position end;
    // The anchor each page begins with, and how many anchors each holds.
    std::vector<model::range_logical_offset> first_anchors;
    std::vector<std::uint32_t> entries;
};

// One page of anchors, verified when it was read, and the routes that lead to
// it. While a page is kept, what routes to it is kept too.
struct sparse_index_anchors final {
    cache_ref<sparse_index_routes> routes;
    sparse_index_page page;
};

// What a reader is told: the nearest anchor at or before its offset, or none
// when the offset lies before the first, and the root that was read through
// to find it. No root for an answer from an index held in memory. A reader
// that follows the anchor to a block with another base or position reports
// the answer it was given, so that its report is about the index it read and
// not about one that replaced it meanwhile.
//
// `next` is the offset the anchor after that one begins with, when there is
// one: where the run after the reader's begins, for a reader that reads
// ahead. Nothing without a position.
struct sparse_index_answer final {
    std::optional<sparse_index_position> position;
    std::optional<local_root_reference> through;
    std::optional<model::range_logical_offset> next{};
    bool operator==(const sparse_index_answer&) const = default;
};

// Where a reader stands in a sealed segment's index: the page of anchors its
// last answer came from, held. A lookup that falls in the same page is
// answered from it with no read and no wait, whatever the cache keeps by
// then; one that falls in another page takes that page and lets this one go.
// A page the cache has no room for lives for as long as a reader stands in
// it, under the budget of the index's readers. Empty before the first
// answer, and for an index that answers from memory.
struct sparse_index_place final {
    cache_ref<sparse_index_anchors> page;
    std::uint32_t ordinal{0};
};

namespace detail {
// One open index root in its shard's list.
struct resident_index_root final {
    boost::intrusive::list_member_hook<> link;
    std::unique_ptr<local_root_owner> root;
    // Given up by its index: nothing new reads through it, and it is closed
    // once nothing does.
    bool retired{false};
};
} // namespace detail

// The index roots one shard holds open at once, at most
// maximum_resident_index_roots whatever is asked. A sealed segment's index is
// opened by its root alone, the first time something looks into it, and the
// root then stays open for the next lookup. An open root holds its file's
// handle and what its root and its bundle were admitted for, two admissions
// of the budget, for as long as it is open. Every sealed segment's index on
// the shard shares one of these, which outlives them. When another root
// needs a place, a root its index gave up goes first, and then the open root
// pinned longest ago, of those nothing reads through. A root something reads
// through is never taken, so with every place read through a request is
// refused.
//
// A root that gives its place up is closed by whoever needed the place: the
// request of another index, or a caller of evict(). The files and the budget
// outlive every such call, and its failure is kept here.
inline constexpr std::uint32_t default_resident_index_roots = 16;
inline constexpr std::uint32_t maximum_resident_index_roots = 256;
class sparse_index_residency final {
public:
    explicit sparse_index_residency(
      std::uint32_t most = default_resident_index_roots) noexcept
      : most_(std::min(most, maximum_resident_index_roots)) {}
    sparse_index_residency(const sparse_index_residency&) = delete;
    sparse_index_residency& operator=(const sparse_index_residency&) = delete;
    ~sparse_index_residency();

    // Open roots, one that is being closed included.
    [[nodiscard]] std::uint32_t resident() const noexcept { return resident_; }
    [[nodiscard]] std::uint32_t most() const noexcept { return most_; }
    // The first failure to close a root that gave its place up.
    [[nodiscard]] const runtime::first_failure& failure() const& noexcept {
        return failed_;
    }
    const runtime::first_failure& failure() const&& = delete;

    // Closes the open root used longest ago that nothing reads through.
    // False when there is none.
    [[nodiscard]] seastar::future<bool> evict();

private:
    template<runtime::file_system_backend Backend, local_directory_owner Owner>
    friend class sealed_sparse_index;
    using root = detail::resident_index_root;
    using list = boost::intrusive::list<
      root,
      boost::intrusive::
        member_hook<root, boost::intrusive::list_member_hook<>, &root::link>>;

    // Takes a place for a root about to be opened, making one when every
    // place is taken. False when none can be made.
    seastar::future<bool> admit();
    // Gives back the place of a root that did not open.
    void abandon() noexcept { --resident_; }
    // A root that just opened, or was just read through: the last to go.
    void install(root& held) noexcept { open_.push_back(held); }
    void touch(root& held) noexcept {
        open_.splice(open_.end(), open_, open_.iterator_to(held));
    }
    // A root its index gave up: the first to go once nothing reads through
    // it.
    void demote(root& held) noexcept {
        open_.splice(open_.begin(), open_, open_.iterator_to(held));
    }
    // Closes an open root once nothing reads through it and frees its place.
    // Nothing of `held` is used after the first suspension: its index may be
    // gone before its root is closed.
    seastar::future<runtime::first_failure> release(root& held);

    list open_;
    std::uint32_t most_;
    std::uint32_t resident_{0};
    runtime::first_failure failed_;
};

// What a sealed segment's lookups are answered from.
enum class sparse_index_source : std::uint8_t {
    // The index root its segment's publication names; for a segment sealed
    // without a record, which has no index, nothing.
    published,
    // An index held in memory that no publication names yet.
    memory,
    // Nothing: the index is owed, and is built again from the segment's data.
    owed,
};

// The index of one sealed segment, and the one place that knows what is owed
// for it.
//
// A segment's publication names an index root, or does not. A root that is
// named is opened alone, when something first looks into the index, and
// shared by everyone who does: lookups that arrive together make one open.
// An index whose root is missing or cannot be read, or that gave an anchor
// its block did not bear out, is owed: the reason is kept here, lookups fail
// with it so that their reader walks the data instead, and rebuild() builds
// the index again from the segment's data, once, for everyone who asks. The
// index built answers from memory at once, and publish() then names it in
// the segment's publication, after which the memory is given back.
//
// Nothing here changes the segment's data or fails its owner. A root being
// replaced stays open for those reading through it, and they read the index
// they began with to its end: an index has two places, the open root and one
// given up, so the next root opens beside a root still read through. With a
// root read through in both, the next reader is refused until one lets go.
//
// A reader asks locate(). Given a cache, the owner keeps there what a lookup
// reads of a root, and each page of anchors a lookup reads, verified once
// when it is read: a lookup that finds both opens no file, holds no handle
// and waits for nothing, so open roots bound the indexes being read and not
// the indexes in use. What the cache holds is of the root named now, and
// goes when that root is replaced or its index is owed. find() is one lookup
// read through the root, with nothing kept.
//
// The files, the directory owner, both budgets, both shard limits and the
// cache outlive joined close(). Whoever holds a pin or a page from here
// releases it before close() returns.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
class sealed_sparse_index final : public runtime::shard_affine {
public:
    // `history` and `context` are what the segment's publication sealed: the
    // extent an index of it is bound to, from the sealed root and never from
    // an index. `named` is the index root the publication names, if any, and
    // `owed` why that root did not open when the segment was found again. A
    // segment sealed without an index root owes one from the start, unless
    // it holds no record: such a segment has no index, and none is owed.
    //
    // `budget` funds what readers use: open roots, pins and pages.
    // `rebuilding` funds a rebuild's walk and the index it builds, and the
    // rebuild runs in its scheduling group. Limits that cannot decode a page
    // or name an index of this extent are refused here, so that no lookup
    // meets them later. `cache`, when there is one, keeps what locate()
    // reads; without one every lookup reads through the root.
    [[nodiscard]] static runtime::result<std::unique_ptr<sealed_sparse_index>>
    make(
      Backend& files,
      Owner& ownership,
      const local_device_spec& device,
      std::uint32_t shard,
      segment_history_context history,
      sparse_index_context context,
      sparse_index_stride stride,
      std::optional<local_root_reference> named,
      std::optional<runtime::operation_error> owed,
      sparse_index_residency& residency,
      sparse_index_rebuild_limit& rebuilds,
      workload_budget& budget,
      workload_budget& rebuilding,
      local_store_io_limits limits,
      segment_scan_limits scan,
      codec::limits policy,
      page_cache* cache = nullptr) {
        if (auto valid = limits.validate(); !valid)
            return runtime::failure(valid.error());
        if (auto valid = scan.validate(); !valid)
            return runtime::failure(valid.error());
        if (
          context.segment() != history.segment
          || context.alignment() != history.alignment
          || context.profile() != history.profile
          || (named
              && (named->kind() != local_root_kind::index
                  || !named->validate_alignment(context.alignment())))
          || residency.most() == 0 || rebuilds.most() == 0)
            return runtime::failure(error(errc::invalid_argument));
        if (
          auto valid = validate_sparse_index_pages(
            limits,
            context.alignment(),
            policy,
            sparse_index_capacity(stride, context));
          !valid)
            return runtime::failure(valid.error());
        return std::unique_ptr<sealed_sparse_index>{new sealed_sparse_index{
          files,
          ownership,
          device,
          shard,
          history,
          context,
          stride,
          named,
          owed,
          residency,
          rebuilds,
          budget,
          rebuilding,
          limits,
          scan,
          policy,
          cache}};
    }
    sealed_sparse_index(const sealed_sparse_index&) = delete;
    sealed_sparse_index& operator=(const sealed_sparse_index&) = delete;
    ~sealed_sparse_index() {
        assert_current();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-INDEX-CLOSED"},
          closed_,
          "sealed index destroyed before joined close");
    }

    [[nodiscard]] sparse_index_source source() const noexcept {
        assert_current();
        return answering() ? sparse_index_source::memory
               : owed_     ? sparse_index_source::owed
                           : sparse_index_source::published;
    }
    // Why the root the publication names cannot answer, or that it names
    // none. Set until an index is published again.
    [[nodiscard]] const std::optional<runtime::operation_error>&
    owed() const& noexcept {
        assert_current();
        return owed_;
    }
    const std::optional<runtime::operation_error>& owed() const&& = delete;
    [[nodiscard]] std::optional<local_root_reference> named() const noexcept {
        assert_current();
        return named_;
    }
    // Whether its root is open now.
    [[nodiscard]] bool resident() const noexcept {
        assert_current();
        return roots_[live_].root != nullptr && !roots_[live_].retired;
    }
    // Times its root was opened.
    [[nodiscard]] std::uint64_t loads() const noexcept {
        assert_current();
        return loads_;
    }

    // The frozen index its segment's writer fed, for a segment whose seal
    // could not publish it: it answers from memory until publish() names it,
    // and no walk of the data is needed. Taken only while an index is owed,
    // and only one that can be of this extent: its anchors lie inside the
    // sealed coverage and are no more than its records. That it is this
    // segment's own is its caller's to know.
    [[nodiscard]] runtime::result<void> adopt(active_sparse_index index) {
        assert_current();
        if (closing_) return runtime::failure(detail::path_error(errc::closed));
        const auto covered = context_.coverage();
        if (
          !owed_ || memory_ || rebuilder_.running() || !index.frozen()
          || index.empty() || index.size() > covered.physical().count().value()
          || !covered.logical().contains(index[0].logical_anchor())
          || index[index.size() - 1].block_position() >= covered.bytes().end())
            return runtime::failure(detail::path_error(errc::invalid_argument));
        memory_.emplace(std::move(index));
        unwalkable_.reset();
        return {};
    }

    // A pin of the root the publication names, opened first if it is not
    // open. Whoever arrives while it is being opened waits for that open and
    // shares its outcome. Everything read through one pin is of one root,
    // whatever is published meanwhile. Refused with what is owed while the
    // root cannot answer, and with queue_full when no place can be made for
    // it.
    [[nodiscard]] seastar::future<runtime::result<local_root_pin>>
    pin(codec::cooperative_work& work) {
        using output = runtime::result<local_root_pin>;
        assert_current();
        if (closing_) co_return output{runtime::failure(error(errc::closed))};
        if (auto ready = work.poll(); !ready)
            co_return output{runtime::failure(error(ready.error().code()))};
        const auto holder = operations_.hold();
        for (;;) {
            if (closing_)
                co_return output{runtime::failure(error(errc::closed))};
            if (owed_) co_return output{runtime::failure(*owed_)};
            // An extent without a record has no index.
            if (!named_)
                co_return output{runtime::failure(error(errc::not_found))};
            if (auto pinned = open_pin()) co_return std::move(*pinned);
            auto& held = roots_[live_];
            if (opening_) {
                const auto opened = co_await opening_->get_shared_future();
                if (!opened) co_return output{runtime::failure(opened.error())};
                continue;
            }
            // Both places can hold a root given up before. The next root
            // takes the place nothing reads through.
            if (held.root && held.root->pins() != 0) {
                const auto& other = roots_[live_ ^ 1U];
                if (!other.root || other.root->pins() == 0) {
                    live_ ^= 1U;
                    continue;
                }
            }
            opening_.emplace();
            const auto reference = *named_;
            runtime::result<std::unique_ptr<local_root_owner>> opened
              = runtime::failure(error(errc::io_failure));
            auto attempt = co_await seastar::coroutine::as_future(
              seastar::futurize_invoke(
                [this, &held, reference] { return open(held, reference); }));
            if (attempt.failed())
                opened = runtime::failure(
                  detail::thrown_index_error(attempt.get_exception()));
            else
                opened = std::move(attempt).get();
            // Nothing suspends from here until this caller holds its pin: a
            // root just opened is read through before it can be taken.
            runtime::result<void> outcome{};
            if (opened) {
                ++loads_;
                held.root = std::move(*opened);
                residency_.install(held);
                // The index moved on while its root was being opened.
                if (owed_ || named_ != std::optional{reference}) give_up();
            } else if (named_ == std::optional{reference}) {
                outcome = runtime::failure(opened.error());
                // Counted here, once, however many wait for this open.
                fault(reference, opened.error());
            }
            // Otherwise the root that did not open was replaced meanwhile,
            // which says nothing of the one named now: everyone looks again.
            auto waiting = std::move(*opening_);
            opening_.reset();
            waiting.set_value(outcome);
            if (!outcome) co_return output{runtime::failure(outcome.error())};
        }
    }

    // The nearest anchor at or before an offset and how far its scan reads at
    // most: from the index in memory when there is one, and otherwise from
    // one page read through the root. An error of the index alone, a page
    // that cannot be read or is not the page its root names, makes the index
    // owed and is returned; the reader then walks the data. With its root
    // open a lookup waits for its page alone, in this one frame.
    [[nodiscard]] seastar::future<
      runtime::result<std::optional<sparse_index_position>>>
    find(model::range_logical_offset target, codec::cooperative_work& work) {
        using output = runtime::result<std::optional<sparse_index_position>>;
        assert_current();
        if (closing_) co_return output{runtime::failure(error(errc::closed))};
        if (answering())
            co_return output{
              memory_->find(target, context_.coverage().bytes().end())};
        // An extent without a record has no index and nothing to find.
        if (!named_ && !owed_)
            co_return output{std::optional<sparse_index_position>{}};
        const auto holder = operations_.hold();
        auto pinned = owed_ ? std::nullopt : open_pin();
        if (!pinned) pinned.emplace(co_await pin(work));
        if (!*pinned) co_return output{runtime::failure(pinned->error())};
        const auto& index = **pinned;
        const auto routed = route_published_anchor(index, target);
        if (!routed) co_return output{runtime::failure(routed.error())};
        if (!*routed) co_return output{std::optional<sparse_index_position>{}};
        auto page = co_await index.read_index(**routed, work);
        output found = page
                         ? search_published_page(index, **routed, *page, target)
                         : output{runtime::failure(page.error())};
        if (
          !found && detail::rebuildable_local_index_error(found.error().code()))
            owe(index.reference(), found.error());
        co_return found;
    }

    // The nearest anchor at or before an offset, for a reader. With a cache
    // it is answered from the page the reader stands in, when the offset
    // falls there, and otherwise from what the cache holds of the root named
    // now; what is missing is read once, as the cache's own work, for
    // everyone who asks meanwhile. What the cache has no room for is read
    // all the same, under the budget of the index's readers, for those who
    // stand in it: `at`, when given, is where this reader stands, and is
    // moved to the page that answered. Only when that cannot be funded
    // either does a lookup read through the root and keep nothing. While the
    // index is owed the caller waits, with everyone else who asks, for the
    // one rebuild, and is then answered from memory: no reader walks the
    // data for itself. `caller` releases this caller alone, with `aborted`.
    //
    // A read that fails is that lookup's outcome. Damage makes the index
    // owed at once, and so does an index object that could not be read
    // sparse_index_read_failures times running. An owed index whose data
    // could not be walked fails every lookup with what the walk found, and
    // is not walked again. A lookup may also be refused, with queue_full,
    // while the rebuilds of the shard or the waiters of this one are at
    // their bound.
    [[nodiscard]] seastar::future<runtime::result<sparse_index_answer>> locate(
      model::range_logical_offset target,
      seastar::abort_source& caller,
      sparse_index_place* at = nullptr) {
        using output = runtime::result<sparse_index_answer>;
        assert_current();
        if (closing_) co_return output{runtime::failure(error(errc::closed))};
        if (caller.abort_requested())
            co_return output{runtime::failure(error(errc::aborted))};
        const auto holder = operations_.hold();
        for (;;) {
            if (closing_)
                co_return output{runtime::failure(error(errc::closed))};
            if (answering()) co_return output{remembered(target)};
            if (owed_) {
                if (unwalkable_)
                    co_return output{runtime::failure(*unwalkable_)};
                if (auto rebuilt = co_await rebuild(caller); !rebuilt)
                    co_return output{runtime::failure(rebuilt.error())};
                continue;
            }
            // An extent without a record has no index and nothing to find.
            if (!named_) co_return output{sparse_index_answer{}};
            const auto reference = *named_;
            // A root without a page has nothing a cache could be charged
            // for.
            if (cache_ == nullptr || reference.pages().value() == 0) {
                auto read = co_await read_through(target, caller);
                if (moved_on(reference)) continue;
                co_return read;
            }
            // Shared with this frame: it is looked into again after each
            // wait, and may no longer be the index's own by then.
            const auto held = cached();
            if (!held)
                co_return output{
                  runtime::failure(error(errc::resource_exhausted))};
            const bool standing = at != nullptr && at->page
                                  && at->page->routes->reference == reference;
            auto routes = standing ? at->page->routes : held->routes.get();
            if (!routes) {
                auto loaded = co_await routes_of(held, reference, caller);
                if (moved_on(reference)) continue;
                if (!loaded) {
                    if (!unkept(loaded.error().code()))
                        co_return output{runtime::failure(loaded.error())};
                    auto read = co_await read_through(target, caller);
                    if (moved_on(reference)) continue;
                    co_return read;
                }
                routes = std::move(*loaded);
            }
            const auto page = find_sparse_index_page(
              routes->first_anchors, target);
            if (!page) co_return output{sparse_index_answer{{}, reference}};
            auto anchors = standing && at->ordinal == *page
                             ? at->page
                             : cache_ref<sparse_index_anchors>{};
            if (!anchors) {
                auto loaded = co_await anchors_of(held, routes, *page, caller);
                if (moved_on(reference)) continue;
                if (!loaded) {
                    if (!unkept(loaded.error().code()))
                        co_return output{runtime::failure(loaded.error())};
                    auto read = co_await read_through(target, caller);
                    if (moved_on(reference)) continue;
                    co_return read;
                }
                anchors = std::move(*loaded);
            }
            auto found = find_in(
              *routes, anchors->page.entries(), *page, target);
            // A page's last anchor is followed by the first of the next
            // page: a reader's run ends where that one's block begins, not
            // where the extent does.
            if (found.edge) {
                const auto following = co_await anchors_of(
                  held, routes, *page + 1, caller);
                if (moved_on(reference)) continue;
                if (!following) {
                    if (!unkept(following.error().code()))
                        co_return output{runtime::failure(following.error())};
                    auto read = co_await read_through(target, caller);
                    if (moved_on(reference)) continue;
                    co_return read;
                }
                found.position->end
                  = (*following)->page.entries().front().block_position();
            }
            if (at != nullptr)
                *at = sparse_index_place{
                  std::move(anchors), static_cast<std::uint32_t>(*page)};
            co_return output{
              sparse_index_answer{found.position, reference, found.next}};
        }
    }

    // What locate() would answer for `target`, when that takes no wait and
    // no read: from the index in memory, or from the routes and the pages
    // the cache holds of the root named now. Nothing when the index is
    // owed or closing, or when any of it would have to be read. For a
    // reader that asks about a run it does not read yet: what is not here
    // is not waited for, and whether a lookup would have answered at once
    // is not a thing its future says, since a wait for the shard looks the
    // same as a wait for the device.
    //
    // `at`, when given, is where the reader that asks stands: what it holds
    // answers as what the cache holds does.
    [[nodiscard]] std::optional<sparse_index_answer> peek(
      model::range_logical_offset target,
      const sparse_index_place* at = nullptr) noexcept {
        assert_current();
        if (closing_) return std::nullopt;
        if (answering()) return remembered(target);
        if (owed_ || !named_) return std::nullopt;
        const bool standing = at != nullptr && at->page
                              && at->page->routes->reference == *named_;
        const bool kept = cached_ && cached_->reference == *named_;
        const auto routes = standing ? at->page->routes
                            : kept   ? cached_->routes.get()
                                     : cache_ref<sparse_index_routes>{};
        if (!routes) return std::nullopt;
        const auto page = find_sparse_index_page(routes->first_anchors, target);
        if (!page) return sparse_index_answer{{}, *named_};
        const auto anchors = standing && at->ordinal == *page ? at->page
                             : kept ? cached_->pages[*page].get()
                                    : cache_ref<sparse_index_anchors>{};
        if (!anchors) return std::nullopt;
        auto found = find_in(*routes, anchors->page.entries(), *page, target);
        // As in locate(): the run of a page's last anchor ends where the
        // next page's first anchor's block begins, which that page says.
        if (found.edge) {
            const auto following = kept ? cached_->pages[*page + 1].get()
                                        : cache_ref<sparse_index_anchors>{};
            if (!following) return std::nullopt;
            found.position->end
              = following->page.entries().front().block_position();
        }
        return sparse_index_answer{found.position, *named_, found.next};
    }

    // What a reader reports of an answer its data did not bear out: the
    // block where the anchor points has another base or position. The index
    // that gave the answer is owed from then on; see owe().
    //
    // A report is a reader's word that a block which is whole is not the one
    // its anchor names. While the segment's data cannot be walked nothing
    // better than the root named now can be had, so a report changes
    // nothing then, and the read that made it is that reader's own failure.
    void report(
      const sparse_index_answer& given, runtime::operation_error why) noexcept {
        if (!given.through) {
            owe(why);
            return;
        }
        if (unwalkable_) return;
        const bool before = owed_.has_value();
        owe(*given.through, why);
        if (!before && owed_) reported_ = true;
    }

    // The root `seen` gave an answer its data did not bear out, or could not
    // be read: a reader that followed an anchor read through it to a block
    // with another base or position reports it here, as does whoever checked
    // it against its extent. Its index is owed from then on. A report about
    // a root that is no longer the one named is about an index that is gone,
    // and one about a root already owed says nothing new: neither changes
    // what answers now.
    void owe(
      const local_root_reference& seen, runtime::operation_error why) noexcept {
        assert_current();
        if (closing_ || owed_ || named_ != std::optional{seen}) return;
        owed_ = why;
        faults_.succeeded();
        give_up();
        forget();
    }
    // The same for an answer find() gave to a caller that holds no root: the
    // index that answers now is owed, whatever it answers from. A caller
    // that read through a pin reports the root it read instead, so that a
    // report arriving late is not taken for the index that replaced it.
    void owe(runtime::operation_error why) noexcept {
        assert_current();
        if (closing_) return;
        if (!owed_) owed_ = why;
        faults_.succeeded();
        give_up();
        forget();
        if (!memory_) return;
        // A publication in flight is encoding it; it goes when that ends.
        if (publishing_)
            distrusted_ = true;
        else
            memory_.reset();
    }

    // Builds the owed index again from the segment's data: one bounded walk
    // that everyone who asks meanwhile waits for, among a bounded number on
    // the shard, in the scheduling group of the budget that funds it.
    // `caller` releases this caller alone. Returns at once when nothing is
    // owed or an index already answers from memory. A walk that finds other
    // data than the publication sealed fails and builds nothing. While an
    // index reported wrong is still being published the request is asked to
    // come back: that index goes when its publication ends.
    [[nodiscard]] seastar::future<runtime::result<void>>
    rebuild(seastar::abort_source& caller) {
        assert_current();
        if (closing_) co_return runtime::failure(error(errc::closed));
        if (!owed_ || answering()) co_return runtime::result<void>{};
        if (memory_) co_return runtime::failure(error(errc::queue_full));
        const auto holder = operations_.hold();
        co_return co_await rebuilder_.run(
          [this](seastar::abort_source& stop) {
              return seastar::with_scheduling_group(
                rebuilding_.scheduling_group(),
                [this, &stop] { return build(stop); });
          },
          caller);
    }

    // Names the index held in memory in the segment's publication, under an
    // object sequence nothing else was given, and gives the memory back. The
    // root it replaces stays open for those reading through it. A refusal
    // changes nothing: the index goes on answering from memory. `segment` is
    // the owner of this index's segment, which is its caller's to know; the
    // sequence of the root being replaced is refused, because the bundle it
    // names is removed once the new one is named.
    template<typename Segment>
    [[nodiscard]] seastar::future<runtime::result<local_root_reference>>
    publish(
      Segment& segment,
      local_object_sequence object,
      codec::cooperative_work& work) {
        using output = runtime::result<local_root_reference>;
        assert_current();
        if (closing_) co_return output{runtime::failure(error(errc::closed))};
        if (!memory_ || (named_ && named_->sequence() == object))
            co_return output{runtime::failure(error(errc::invalid_argument))};
        if (publishing_)
            co_return output{runtime::failure(error(errc::queue_full))};
        const auto holder = operations_.hold();
        publishing_ = true;
        const auto idle = seastar::defer([this] noexcept {
            publishing_ = false;
            if (std::exchange(distrusted_, false)) memory_.reset();
        });
        const auto named = co_await segment.publish_index(
          sparse_index_seal{&*memory_, object}, work);
        if (!named) co_return output{runtime::failure(named.error())};
        // The pointer names it now, whatever was reported of it meanwhile.
        give_up();
        forget();
        named_ = *named;
        faults_.succeeded();
        if (!distrusted_) {
            owed_.reset();
            reported_ = false;
            memory_.reset();
        }
        co_return *named;
    }

    // Ends a rebuild in flight, joins everything in flight and closes the
    // roots, each once nothing reads through it.
    [[nodiscard]] seastar::future<runtime::result<void>> close() {
        assert_current();
        if (closed_) co_return failed_.outcome();
        if (closing_) co_return runtime::failure(error(errc::queue_full));
        closing_ = true;
        abort_.request_abort();
        co_await rebuilder_.stop();
        co_await operations_.close();
        forget();
        for (auto& held : roots_)
            if (held.root) observe(co_await residency_.release(held));
        memory_.reset();
        closed_ = true;
        co_return failed_.outcome();
    }

private:
    // What the cache holds of one index root: its routes, and a place for
    // each of its pages. Lookups in flight share it, so it stays where it is
    // until the last of them has returned, whatever becomes of its index
    // meanwhile.
    struct cached_root final {
        explicit cached_root(local_root_reference root)
          : reference(root)
          , pages(
              std::make_unique<cache_slot<sparse_index_anchors>[]>(
                root.pages().value())) {}
        // Lets every value go. One that is held lives on with whoever holds
        // it.
        void drop() noexcept {
            for (std::uint32_t page = 0; page < reference.pages().value();
                 ++page)
                pages[page].drop();
            routes.drop();
        }
        local_root_reference reference;
        cache_slot<sparse_index_routes> routes;
        std::unique_ptr<cache_slot<sparse_index_anchors>[]> pages;
    };

    sealed_sparse_index(
      Backend& files,
      Owner& ownership,
      const local_device_spec& device,
      std::uint32_t shard,
      segment_history_context history,
      sparse_index_context context,
      sparse_index_stride stride,
      std::optional<local_root_reference> named,
      std::optional<runtime::operation_error> owed,
      sparse_index_residency& residency,
      sparse_index_rebuild_limit& rebuilds,
      workload_budget& budget,
      workload_budget& rebuilding,
      local_store_io_limits limits,
      segment_scan_limits scan,
      codec::limits policy,
      page_cache* cache)
      : files_(files)
      , ownership_(ownership)
      , device_(device)
      , shard_(shard)
      , history_(history)
      , context_(context)
      , stride_(stride)
      , named_(named)
      , owed_(
          owed ? owed
          : named || context.coverage().physical().empty()
            ? std::nullopt
            : std::optional{detail::path_error(errc::not_found)})
      , residency_(residency)
      , rebuilder_(rebuilds)
      , budget_(budget)
      , rebuilding_(rebuilding)
      , limits_(limits)
      , scan_(scan)
      , cache_(cache)
      , execution_(policy, abort_) {}

    static runtime::operation_error error(errc code) noexcept {
        return detail::path_error(code);
    }
    void observe(const runtime::first_failure& closed) noexcept {
        if (closed.exception())
            failed_.observe(closed.exception());
        else if (closed.error())
            failed_.observe(*closed.error());
    }

    // The pin of the open root, when one is open: no wait and no frame.
    [[nodiscard]] std::optional<runtime::result<local_root_pin>> open_pin() {
        auto& held = roots_[live_];
        if (!held.root || held.retired) return std::nullopt;
        residency_.touch(held);
        return held.root->pin();
    }

    // Whether the index in memory answers: not while it is reported wrong
    // and only kept for the publication that is encoding it.
    [[nodiscard]] bool answering() const noexcept {
        return memory_.has_value() && !distrusted_;
    }

    // Whether the index a lookup began with is no longer the one that
    // answers: the lookup then begins again.
    [[nodiscard]] bool
    moved_on(const local_root_reference& began) const noexcept {
        return closing_ || owed_ || named_ != std::optional{began};
    }
    // Whether a load ended without a value because the cache had no room for
    // it: that lookup reads through the root.
    [[nodiscard]] static bool unkept(errc code) noexcept {
        return code == errc::queue_full || code == errc::out_of_range;
    }

    // What the cache holds of the root named now, made when first asked for.
    // Nothing when it cannot be made.
    [[nodiscard]] seastar::lw_shared_ptr<cached_root> cached() {
        if (!cached_ || cached_->reference != *named_) {
            forget();
            try {
                cached_ = seastar::make_lw_shared<cached_root>(*named_);
            } catch (const std::bad_alloc&) {
                return nullptr;
            }
        }
        return cached_;
    }
    // Lets go of what the cache holds of this index: its root was replaced,
    // or gave an answer nobody may be given again.
    void forget() noexcept {
        if (!cached_) return;
        cached_->drop();
        cached_ = nullptr;
    }
    // What the cache is charged for the routes of a root of `pages` pages.
    [[nodiscard]] runtime::result<byte_count>
    routes_charge(std::uint32_t pages) const noexcept {
        const auto anchors = cache_->allocation_charge(
          byte_count{
            std::uint64_t{pages} * sizeof(model::range_logical_offset)});
        const auto counts = cache_->allocation_charge(
          byte_count{std::uint64_t{pages} * sizeof(std::uint32_t)});
        if (!anchors) return runtime::failure(anchors.error());
        if (!counts) return runtime::failure(counts.error());
        const auto both = anchors->checked_add(*counts);
        if (!both) return runtime::failure(error(errc::out_of_range));
        return *both;
    }

    // The anchors of one page of the root `routes` were read through, held:
    // what the cache keeps of it, or read once as the cache's own work for
    // everyone who asks meanwhile. `held` is shared with this frame, which
    // stands on one of its slots while it waits.
    seastar::future<runtime::result<cache_ref<sparse_index_anchors>>>
    anchors_of(
      seastar::lw_shared_ptr<cached_root> held,
      cache_ref<sparse_index_routes> routes,
      std::uint32_t page,
      seastar::abort_source& caller) {
        using output = runtime::result<cache_ref<sparse_index_anchors>>;
        if (auto kept = held->pages[page].get())
            co_return output{std::move(kept)};
        const auto charge = cache_->allocation_charge(
          byte_count{
            std::uint64_t{routes->entries[page]} * sizeof(sparse_index_entry)});
        if (!charge) co_return output{runtime::failure(charge.error())};
        auto loaded = co_await held->pages[page].fetch(
          *cache_, *charge, anchors_loader(routes, page), caller);
        if (loaded || !unkept(loaded.error().code())) co_return loaded;
        // No room for it in the cache: read all the same, for whoever asks
        // meanwhile and for as long as one of them holds it.
        const auto funding = lent(*charge);
        if (!funding) co_return loaded;
        co_return co_await held->pages[page].fetch(
          *cache_, *charge, anchors_loader(routes, page), caller, &*funding);
    }
    // The routes of the root `reference`, held, in the same way.
    seastar::future<runtime::result<cache_ref<sparse_index_routes>>> routes_of(
      seastar::lw_shared_ptr<cached_root> held,
      local_root_reference reference,
      seastar::abort_source& caller) {
        using output = runtime::result<cache_ref<sparse_index_routes>>;
        const auto charge = routes_charge(reference.pages().value());
        if (!charge) co_return output{runtime::failure(charge.error())};
        auto loaded = co_await held->routes.fetch(
          *cache_, *charge, routes_loader(reference), caller);
        if (loaded || !unkept(loaded.error().code())) co_return loaded;
        const auto funding = lent(*charge);
        if (!funding) co_return loaded;
        co_return co_await held->routes.fetch(
          *cache_, *charge, routes_loader(reference), caller, &*funding);
    }
    // What funds a value of `charge` bytes that the cache has no room for,
    // and the entry that holds it: taken from what readers of the index are
    // funded by, afresh for each such value, and held by the value alone.
    // The entry itself is charged less than this allows for it.
    static constexpr std::uint64_t lent_entry_bytes = 4096;
    [[nodiscard]] runtime::result<workload_reservation>
    lent(byte_count charge) noexcept {
        try {
            return budget_.try_reserve(
              byte_count{charge.value() + lent_entry_bytes});
        } catch (const std::bad_alloc&) {
            return runtime::failure(error(errc::resource_exhausted));
        }
    }

    // What a lookup finds in one page of anchors: the anchor at or before
    // the offset, the offset the anchor after it begins with (in the page,
    // or the next page's first, which the routes hold), and whether it is
    // the page's last anchor with another page after it, whose run ends
    // where that page's first block begins.
    struct found_anchor final {
        std::optional<sparse_index_position> position;
        std::optional<model::range_logical_offset> next;
        bool edge{false};
    };
    [[nodiscard]] static found_anchor find_in(
      const sparse_index_routes& routes,
      std::span<const sparse_index_entry> entries,
      std::size_t page,
      model::range_logical_offset target) noexcept {
        found_anchor found{
          find_sparse_index_anchor(entries, target, routes.end), {}, false};
        if (!found.position) return found;
        found.next = next_anchor(entries, target);
        if (!found.next && page + 1 < routes.first_anchors.size())
            found.next = routes.first_anchors[page + 1];
        found.edge = page + 1 < routes.entries.size()
                     && found.position->anchor == entries.back();
        return found;
    }

    // The answer of the index held in memory.
    [[nodiscard]] sparse_index_answer
    remembered(model::range_logical_offset target) const noexcept {
        const auto found = memory_->find(
          target, context_.coverage().bytes().end());
        std::optional<model::range_logical_offset> next;
        if (found)
            if (const auto after = memory_->after(target))
                next = after->logical_anchor();
        return sparse_index_answer{found, std::nullopt, next};
    }

    // The offset the first of `anchors` above `target` begins with.
    [[nodiscard]] static std::optional<model::range_logical_offset> next_anchor(
      std::span<const sparse_index_entry> anchors,
      model::range_logical_offset target) noexcept {
        const auto after = std::ranges::upper_bound(
          anchors, target, {}, &sparse_index_entry::logical_anchor);
        if (after == anchors.end()) return std::nullopt;
        return after->logical_anchor();
    }

    // An index object could not be read through `seen`: see
    // sparse_index_read_faults for when that makes its index owed.
    void fault(
      const local_root_reference& seen, runtime::operation_error why) noexcept {
        if (faults_.failed(why.code()) == sparse_index_fault::owed)
            owe(seen, why);
    }

    // One lookup read through the root, with nothing kept.
    seastar::future<runtime::result<sparse_index_answer>> read_through(
      model::range_logical_offset target, seastar::abort_source& caller) {
        using output = runtime::result<sparse_index_answer>;
        codec::cooperative_work work{execution_.policy(), caller};
        auto pinned = co_await pin(work);
        if (!pinned) co_return output{runtime::failure(pinned.error())};
        const auto reference = pinned->reference();
        const auto routed = route_published_anchor(*pinned, target);
        if (!routed) co_return output{runtime::failure(routed.error())};
        if (!*routed) co_return output{sparse_index_answer{{}, reference}};
        auto page = co_await pinned->read_index(**routed, work);
        auto found = page
                       ? search_published_page(*pinned, **routed, *page, target)
                       : runtime::result<std::optional<sparse_index_position>>{
                           runtime::failure(page.error())};
        if (!found) {
            fault(reference, found.error());
            co_return output{runtime::failure(found.error())};
        }
        // As in locate(): the run of a page's last anchor ends where the
        // next page's first anchor's block begins.
        const auto* root = std::get_if<sparse_index_root>(&pinned->metadata());
        const auto following = (*routed)->value() + 1U;
        std::optional<model::range_logical_offset> after;
        if (*found) {
            after = next_anchor(page->value.entries(), target);
            if (
              !after && root != nullptr
              && following < root->first_anchors().size())
                after = root->first_anchors()[following];
        }
        if (
          *found && root != nullptr && following < root->pages().size()
          && (*found)->anchor == page->value.entries().back()) {
            const auto next = co_await pinned->read_index(
              page_ordinal::make(following).value(), work);
            if (!next) {
                fault(reference, next.error());
                co_return output{runtime::failure(next.error())};
            }
            (*found)->end = next->value.entries().front().block_position();
        }
        faults_.succeeded();
        co_return output{sparse_index_answer{*found, reference, after}};
    }

    // What the cache runs to read the routes of `reference`, and one page of
    // anchors that `routes` lead to. Each owns what it reads through: the
    // index is not closed while one runs, and no caller's leaving ends it.
    [[nodiscard]] auto routes_loader(local_root_reference reference) {
        return [this, reference, joined = operations_.hold()](
                 seastar::abort_source& stop) {
            return load_routes(reference, stop);
        };
    }
    [[nodiscard]] auto
    anchors_loader(cache_ref<sparse_index_routes> routes, std::uint32_t page) {
        return
          [this, routes = std::move(routes), page, joined = operations_.hold()](
            seastar::abort_source& stop) {
              return load_anchors(routes, page, stop);
          };
    }
    seastar::future<runtime::result<sparse_index_routes>>
    load_routes(local_root_reference reference, seastar::abort_source& stop) {
        using output = runtime::result<sparse_index_routes>;
        codec::cooperative_work work{execution_.policy(), stop};
        auto pinned = co_await pin(work);
        if (!pinned) co_return output{runtime::failure(pinned.error())};
        const auto* root = std::get_if<sparse_index_root>(&pinned->metadata());
        // The index moved on while this was asked for, or the root is not
        // the one its reference was admitted for: whoever waits looks again.
        if (
          !root || pinned->reference() != reference
          || root->pages().size() != reference.pages().value()
          || root->first_anchors().size() != root->pages().size())
            co_return output{runtime::failure(error(errc::wrong_context))};
        sparse_index_routes routes{
          reference, root->context().coverage().bytes().end(), {}, {}};
        try {
            routes.first_anchors.assign(
              root->first_anchors().begin(), root->first_anchors().end());
            routes.entries.reserve(root->pages().size());
            for (const auto& page : root->pages())
                routes.entries.push_back(page.entry_count());
        } catch (const std::bad_alloc&) {
            co_return output{runtime::failure(error(errc::resource_exhausted))};
        }
        faults_.succeeded();
        co_return output{std::move(routes)};
    }
    seastar::future<runtime::result<sparse_index_anchors>> load_anchors(
      cache_ref<sparse_index_routes> routes,
      std::uint32_t ordinal,
      seastar::abort_source& stop) {
        using output = runtime::result<sparse_index_anchors>;
        codec::cooperative_work work{execution_.policy(), stop};
        const auto reference = routes->reference;
        auto pinned = co_await pin(work);
        if (!pinned) co_return output{runtime::failure(pinned.error())};
        if (pinned->reference() != reference)
            co_return output{runtime::failure(error(errc::wrong_context))};
        // The read's allowance and its pin of the root end with this frame:
        // what is kept is the anchors alone.
        auto page = co_await pinned->read_index(
          page_ordinal::make(ordinal).value(), work);
        if (!page) {
            fault(reference, page.error());
            co_return output{runtime::failure(page.error())};
        }
        // The root routed here by the anchor this page begins with.
        if (
          page->value.entries().front().logical_anchor()
          != routes->first_anchors[ordinal]) {
            const auto wrong = error(errc::malformed_data);
            owe(reference, wrong);
            co_return output{runtime::failure(wrong)};
        }
        faults_.succeeded();
        co_return output{
          sparse_index_anchors{std::move(routes), std::move(page->value)}};
    }

    // Stops reading through the open root. Those reading through it go on;
    // the next root is opened beside it, in whichever place nothing reads
    // through when it is opened.
    void give_up() noexcept {
        auto& held = roots_[live_];
        if (!held.root || held.retired) return;
        held.root->retire();
        held.retired = true;
        residency_.demote(held);
        if (!roots_[live_ ^ 1U].root) live_ ^= 1U;
    }

    // Opens the root `reference` names into `held`'s place.
    seastar::future<runtime::result<std::unique_ptr<local_root_owner>>>
    open(detail::resident_index_root& held, local_root_reference reference) {
        using output = runtime::result<std::unique_ptr<local_root_owner>>;
        // A root given up before still holds this place, and goes first.
        if (held.root) {
            if (held.root->pins() != 0)
                co_return output{runtime::failure(error(errc::queue_full))};
            observe(co_await residency_.release(held));
        }
        for (;;) {
            if (!co_await residency_.admit())
                co_return output{runtime::failure(error(errc::queue_full))};
            output opened = runtime::failure(error(errc::io_failure));
            {
                // The place is given back unless a root takes it, however
                // the open ends.
                auto place = seastar::defer(
                  [this] noexcept { residency_.abandon(); });
                opened = co_await local_root_owner::open(
                  files_,
                  ownership_,
                  device_,
                  shard_,
                  reference,
                  local_bundle_context{context_},
                  budget_,
                  limits_,
                  execution_);
                if (opened) {
                    place.cancel();
                    co_return opened;
                }
            }
            // Refused for want of room and not for the root: another root
            // gives its place up, while one can.
            if (
              opened.error().code() != errc::queue_full
              || !co_await residency_.evict())
                co_return opened;
        }
    }

    // The rebuild itself; the rebuilder runs one at a time.
    seastar::future<runtime::result<void>> build(seastar::abort_source& stop) {
        codec::cooperative_work walking{execution_.policy(), stop};
        auto built = co_await rebuild_sparse_index(
          files_,
          ownership_,
          device_,
          shard_,
          history_,
          context_,
          stride_,
          rebuilding_,
          scan_,
          walking);
        if (!built) {
            // What the walk found in the data it will find again. An index
            // owed on a reader's word goes back to the root that is named,
            // which answers everything the data does not contradict; any
            // other stays owed, and is not walked for again.
            if (detail::rebuildable_local_index_error(built.error().code())) {
                unwalkable_ = built.error();
                if (std::exchange(reported_, false)) owed_.reset();
            }
            co_return runtime::failure(built.error());
        }
        memory_.emplace(std::move(*built));
        reported_ = false;
        co_return runtime::result<void>{};
    }

    Backend& files_;
    Owner& ownership_;
    local_device_spec device_;
    std::uint32_t shard_;
    segment_history_context history_;
    sparse_index_context context_;
    sparse_index_stride stride_;
    // The index root the segment's publication names now.
    std::optional<local_root_reference> named_;
    std::optional<runtime::operation_error> owed_;
    // An index that answers without a root: fed by the writer or built again.
    std::optional<active_sparse_index> memory_;
    sparse_index_residency& residency_;
    // The open root, and one given up that something still reads through.
    std::array<detail::resident_index_root, 2> roots_;
    std::uint32_t live_{0};
    std::uint64_t loads_{0};
    // The open in flight, shared by everyone who arrived during it.
    std::optional<seastar::shared_promise<runtime::result<void>>> opening_;
    sparse_index_rebuilder rebuilder_;
    workload_budget& budget_;
    workload_budget& rebuilding_;
    local_store_io_limits limits_;
    segment_scan_limits scan_;
    page_cache* cache_;
    // What the cache holds of the root named now.
    seastar::lw_shared_ptr<cached_root> cached_;
    sparse_index_read_faults faults_;
    // Why the segment's data could not be walked for an index, once a walk
    // found that out.
    std::optional<runtime::operation_error> unwalkable_;
    // The index is owed on a reader's word alone: its root still reads.
    bool reported_{false};
    // An open never uses a caller's abort source: others wait for it.
    seastar::abort_source abort_;
    codec::cooperative_work execution_;
    seastar::gate operations_;
    runtime::first_failure failed_;
    bool publishing_{false}, distrusted_{false};
    bool closing_{false}, closed_{false};
};

} // namespace kwaque::storage
