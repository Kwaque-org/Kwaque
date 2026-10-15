#pragma once

#include "src/base/units.h"
#include "src/codec/cooperative.h"
#include "src/codec/digest.h"
#include "src/codec/limits.h"
#include "src/model/position.h"
#include "src/runtime/file.h"
#include "src/runtime/time.h"
#include "src/storage/device_placement.h"
#include "src/storage/format_context.h"
#include "src/storage/local_append.h"
#include "src/storage/local_engine.h"
#include "src/storage/local_paths.h"
#include "src/storage/local_store_config.h"
#include "src/storage/recovery_inventory.h"
#include "src/storage/retention.h"
#include "src/storage/segment_lifecycle.h"
#include "src/storage/segment_reader.h"
#include "src/storage/tests/engine_history.h"
#include "src/storage/tests/engine_ledger.h"
#include "src/storage/tests/local_append_contract.h"
#include "src/storage/tests/local_engine_contract.h"
#include "src/storage/tests/local_installation_contract.h"
#include "src/storage/tests/local_store_contract.h"
#include "src/storage/tests/segment_lifecycle_contract.h"
#include "src/storage/tests/segment_test_support.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/noncopyable_function.hh>

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <initializer_list>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace kwaque::storage::testing::engine_history_contract {
using store_contract::require;
using store_contract::take;
namespace appends = local_append_contract;
namespace engines = local_engine_contract;
namespace lifecycle = segment_lifecycle_contract;

// The lives of a shard that a run may cut, the first being the one that
// begins with the store's first start.
inline constexpr std::size_t history_cut_lives = 3;
// Segments one history creates; a creation past them does nothing.
inline constexpr std::size_t maximum_history_segments = 8;

// Where a run is cut: in each life, the device crashes once it has taken
// this many operations that may have changed it, with whatever the life was
// doing left as it stood. Between two such operations a crash leaves the
// same device, so a sweep that takes every count takes every crash there is.
// They are counted from where what is under test begins: in a life that
// follows another, its start; in the first, the history's operation at
// `from`, so that a store's first start and what a history only sets up are
// not cut.
struct history_cuts final {
    std::size_t from{0};
    std::array<std::optional<std::uint64_t>, history_cut_lives> at{};
};
// What a test looks at or does to the device around a crash, each called
// with the number of the life: as the life's crash is about to be taken,
// once it has returned, and before the start of a life that follows
// another. `changes` is the device's count of operations it has applied that
// may have changed it; a run that is cut needs it. A backend that can be taken
// down is taken down in after_crash and brought back in before_start: the
// owners a crash leaves running are still stopped, and change nothing the next
// start reads. Without after_crash the device crashes once more when they have
// stopped, which undoes what they left unsynced and nothing they synced.
//
// `told` is given every line the run's ledger writes, as it is told. And
// `acknowledged` is called each time the run has been told that something is
// durable: a creation, a request, a seal, a removal, a stop. Nothing new is
// asked of the shard until it returns, so whatever it marks lies after
// everything the lines before it report and before anything asked later.
//
// `asking` is called before the run asks the shard for a start, a stop or
// one of the history's operations, and `ended` once what was asked has
// ended, with whether it threw. A step that throws is the run's failure,
// unless `ended` answers that it ends the life as a cut does: the device
// crashes where it stands, and the lives after it start, read back and stop.
using history_hook
  = seastar::noncopyable_function<seastar::future<>(std::uint32_t)>;
// One thing a run asks of the shard: in which life, and for an operation
// its place in the history; for a start or a stop, the place of the
// operation that follows.
struct history_step final {
    enum class kind : std::uint8_t { start, operation, stop };
    kind what{kind::operation};
    std::uint32_t life{0};
    std::size_t index{0};
    bool operator==(const history_step&) const = default;
};
struct history_hooks final {
    history_hook before_crash;
    history_hook after_crash;
    history_hook before_start;
    seastar::noncopyable_function<std::uint64_t()> changes;
    std::function<void(const std::string&)> told;
    seastar::noncopyable_function<seastar::future<>()> acknowledged;
    std::function<void(const history_step&)> asking;
    std::function<bool(const history_step&, bool)> ended;
};
// What a caller changes of a run: the engine's limits, and how many
// segments its history may create in all.
struct history_settings final {
    local_engine_config engine{engines::configuration()};
    std::size_t segments{maximum_history_segments};
};
struct history_report final {
    engine_ledger::verdict verdict{engine_ledger::verdict::holds};
    std::string about;
    // The shard's lives: each began with a start.
    std::uint32_t lives{0};
    // The most segments that took appends at one time.
    std::uint32_t writable{0};
    // The changing operations the device took in each life from where its
    // cuts are counted, and whether the life's cut came before it ended.
    std::array<std::uint64_t, history_cut_lives> changes{};
    std::array<bool, history_cut_lives> reached{};
};

// One run of a history over one shard's storage, with a ledger of what its
// caller did and was told. The history's operations are run in order, each
// waited for one scheduler event at a time; a cut, the crash operation and
// the restart operation end a life, and the next begins with a start that is
// given the catalog the ledger's own knowledge makes. After every start
// that follows a life, each segment is looked for under its two names and
// read back: through a session when the table lets one in, and always as the
// device holds its data file.
//
// A cut ends the history's operations: the lives after it only start, read
// back and stop. step(pending) advances one scheduler event and says whether
// it did; a cut is looked for between two of them.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Step>
class history_run final {
public:
    history_run(
      Backend& files,
      Owner& owner,
      const local_device_spec& spec,
      workload_budget& budget,
      workload_budget& reserve,
      Driver drive,
      Timer& timer,
      Step step)
      : files_(files)
      , owner_(owner)
      , devices_{spec}
      , budget_(budget)
      , reserve_(reserve)
      , drive_(drive)
      , timer_(timer)
      , step_(step)
      , paths_(take(local_paths::make(spec.root))) {}

    [[nodiscard]] const engine_ledger& ledger() const noexcept {
        return ledger_;
    }

    seastar::future<history_report> run(
      const engine_history& history,
      history_cuts cuts,
      history_hooks hooks,
      history_settings settings = {}) {
        hooks_ = std::move(hooks);
        settings_ = std::move(settings);
        if (hooks_.told) ledger_.record(hooks_.told);
        history_report report;
        std::exception_ptr failed;
        try {
            report = co_await lives(history, cuts);
        } catch (...) {
            failed = std::current_exception();
        }
        if (failed) {
            co_await close_failed();
            std::rethrow_exception(failed);
        }
        co_return report;
    }

    // A start on the store another run of this driver left, wherever that
    // run ended, and everything read back. `told` is what that run's ledger
    // was told, a line a call; the lines from `answered` on are heard
    // without their answers, as their caller stood who had asked and not yet
    // heard. Whatever had no result by then has none: the start is given the
    // catalog that knowledge makes, as after a crash.
    seastar::future<history_report>
    check(std::span<const std::string> told, std::size_t answered) {
        history_report report;
        std::exception_ptr failed;
        try {
            std::vector<std::string_view> notes;
            for (std::size_t i = 0; i != told.size(); ++i)
                if (
                  ledger_.hear(told[i], i >= answered)
                  == engine_ledger::line::note)
                    notes.push_back(told[i]);
            for (const auto generation : ledger_.segments())
                segments_.push_back({generation, appends::context(generation)});
            for (const auto note : notes)
                if (const auto asked = authorized(note))
                    if (auto* segment = find(asked->first))
                        segment->authorization = deletion_authorization{
                          segment->context, asked->second};
            ledger_.crash();
            life_ = 1;
            ++report.lives;
            const bool began = co_await begin();
            require(began, "a start with no cut was cut");
            co_await read_back_all();
            const bool stopped = co_await stop();
            require(stopped, "a stop with no cut was cut");
            report.verdict = ledger_.first();
            report.about = ledger_.about();
        } catch (...) {
            failed = std::current_exception();
        }
        if (failed) {
            co_await close_failed();
            std::rethrow_exception(failed);
        }
        co_return report;
    }

private:
    // The run is over: what it left open is closed before its failure is
    // told, whatever that closing fails with.
    seastar::future<> close_failed() {
        for (auto& request : outstanding_) {
            abandon(std::move(request.accepted));
            abandon(std::move(request.result));
        }
        outstanding_.clear();
        try {
            if (engine_)
                static_cast<void>(co_await drive_.lifecycle(engine_->stop()));
            for (auto& left : abandoned_)
                co_await drive_.lifecycle(std::move(left));
        } catch (...) {
            static_cast<void>(std::current_exception());
        }
        abandoned_.clear();
        close_life();
    }

    seastar::future<history_report>
    lives(const engine_history& history, history_cuts cuts) {
        history_report report;
        co_await installation_contract::bootstrap(
          files_, owner_, spec(), budget_, work_, drive_);
        require(
          static_cast<bool>(hooks_.changes)
            || std::ranges::none_of(
              cuts.at, [](const auto& cut) { return cut.has_value(); }),
          "a run is cut and its device does not count its changes");
        std::size_t next = 0;
        for (;; ++life_) {
            cut_ = life_ < cuts.at.size() ? cuts.at[life_] : std::nullopt;
            // A start that follows a life is under test from its beginning.
            counting_ = false;
            if (life_ != 0) count();
            ++report.lives;
            // What ends this life when it is not a cut.
            auto ended = history_op::settle;
            if (life_ != 0 && hooks_.before_start)
                co_await hooks_.before_start(life_);
            bool whole = co_await asked(
              {history_step::kind::start, life_, next},
              [this] { return begin(); });
            if (whole && life_ != 0) co_await read_back_all();
            while (whole && next != history.operations.size()) {
                if (next >= cuts.from) count();
                const auto index = next;
                const auto operation = history.operations[next++];
                if (
                  operation.op == history_op::crash
                  || operation.op == history_op::restart) {
                    ended = operation.op;
                    break;
                }
                whole = co_await asked(
                  {history_step::kind::operation, life_, index},
                  [this, operation] { return apply(operation); });
            }
            if (whole && ended != history_op::crash) {
                count();
                whole = co_await asked(
                  {history_step::kind::stop, life_, next},
                  [this] { return stop(); });
            }
            if (life_ < history_cut_lives) {
                report.changes[life_] = counting_ ? changed() - base_ : 0;
                report.reached[life_] = !whole;
            }
            if (!whole) {
                // A cut: the history's operations end with it.
                next = history.operations.size();
                co_await crash();
                continue;
            }
            if (ended == history_op::crash) {
                co_await crash();
                continue;
            }
            if (ended != history_op::restart) break;
        }
        report.verdict = ledger_.first();
        report.about = ledger_.about();
        report.writable = most_writable_;
        co_return report;
    }

    using engine_type = local_engine<Backend, Owner, Clock>;
    using name = engine_ledger::name;
    struct held final {
        std::uint64_t generation;
        segment_context context;
        // Where its appends go, in the life it was created in.
        std::optional<local_append_target> target;
        std::uint64_t next_logical{100};
        // It takes appends in this life, as far as its caller was told.
        bool writable{false};
        bool gone{false};
        // What its seal answered with, and what its removal was asked with.
        seastar::lw_shared_ptr<const sealed_segment_expectation> expected;
        std::optional<deletion_authorization> authorization;
    };
    struct offered final {
        engine_ledger::request_id id;
        std::uint64_t generation;
        std::string wire;
        seastar::future<local_append_acceptance> accepted;
        seastar::future<local_append_outcome> result;
    };

    [[nodiscard]] const local_device_spec& spec() const noexcept {
        return devices_[0];
    }
    [[nodiscard]] local_segment_name named(const held& segment) const {
        return {segment.context.segment(), segment.context.generation()};
    }
    // The `ordinal`-th segment `wanted` holds for, in creation order.
    template<typename Wanted>
    [[nodiscard]] held* choose(std::uint8_t ordinal, Wanted wanted) {
        std::vector<held*> among;
        for (auto& segment : segments_)
            if (wanted(segment)) among.push_back(&segment);
        return among.empty() ? nullptr : among[ordinal % among.size()];
    }

    [[nodiscard]] std::uint64_t changed() {
        return hooks_.changes ? hooks_.changes() : 0;
    }
    // Counts the device's changes from here on, unless they are counted.
    void count() {
        if (counting_) return;
        counting_ = true;
        base_ = changed();
    }
    // Whether the device has taken as many changes as this life's cut says.
    [[nodiscard]] bool cut_reached() {
        return cut_ && counting_ && changed() - base_ >= *cut_;
    }
    // Runs scheduler events until `pending` is ready; false when this
    // life's cut comes first.
    template<typename T>
    seastar::future<bool> reach(seastar::future<T>& pending) {
        while (!cut_reached()) {
            if (!co_await step_(pending)) {
                require(
                  pending.available(),
                  "an operation stopped without a result and without a cut");
                co_return true;
            }
        }
        co_return false;
    }
    // Left where it stood by a cut: joined after the crash, and nothing it
    // answers then is an answer.
    template<typename T>
    void abandon(seastar::future<T> pending) {
        abandoned_.push_back(
          std::move(pending).then_wrapped(
            [](seastar::future<T> done) { done.ignore_ready_future(); }));
    }

    // One thing asked of the shard, between the two hooks. False when the
    // life ends with it: its cut came first, or it threw and the hook took
    // that for the life's end.
    template<typename Ask>
    seastar::future<bool> asked(history_step step, Ask ask) {
        if (hooks_.asking) hooks_.asking(step);
        std::exception_ptr thrown;
        bool whole = false;
        try {
            whole = co_await ask();
        } catch (...) {
            thrown = std::current_exception();
        }
        const bool ends = hooks_.ended
                          && hooks_.ended(step, static_cast<bool>(thrown));
        if (!thrown) co_return whole;
        if (!ends) std::rethrow_exception(thrown);
        co_return false;
    }

    // A start with the catalog the ledger's knowledge makes. False when the
    // cut came first.
    seastar::future<bool> begin() {
        if (life_ != 0) ledger_.restart();
        catalog_.clear();
        for (auto& segment : segments_) {
            const auto listed = ledger_.listed(segment.generation);
            if (listed == engine_ledger::listing::none) continue;
            if (listed == engine_ledger::listing::kept) {
                catalog_.push_back(engines::listed(segment.generation));
                continue;
            }
            // What was asked with before, or a segment its owner discards
            // as one that was never sealed.
            if (!segment.authorization)
                segment.authorization = deletion_authorization{
                  segment.context, std::nullopt};
            catalog_.push_back(
              engines::listed(segment.generation, segment.authorization));
        }
        heard_.emplace();
        engine_ = take(
          engine_type::make(
            {files_,
             owner_,
             devices_,
             spec(),
             0,
             budget_,
             budget_,
             reserve_,
             budget_},
            settings_.engine));
        auto starting = engine_->start(
          timer_, catalog_, {heard_->sink(), {}, {}});
        if (!co_await reach(starting)) {
            abandon(std::move(starting));
            co_return false;
        }
        const auto began = co_await std::move(starting);
        take(began.failure.outcome());
        require(
          began.state == engine_state::ready,
          "a start did not leave the shard ready");
        // The sampler's part: the shard may promise this much of its pool.
        const std::array pools{
          device_pool_share{true, false, byte_count{1_GiB}}};
        engine_->placement()->sampled(pools, Clock::now());
        co_return true;
    }

    // A stop with nothing left unanswered. False when the cut came first.
    seastar::future<bool> stop() {
        auto stopping = engine_->stop();
        if (!co_await reach(stopping)) {
            abandon(std::move(stopping));
            co_return false;
        }
        take(co_await std::move(stopping));
        // What was accepted drained through both barriers.
        for (auto& request : outstanding_) {
            const auto decided = co_await drive_.lifecycle(
              std::move(request.accepted));
            static_cast<void>(answered(
              request,
              decided.accepted(),
              co_await drive_.lifecycle(std::move(request.result))));
        }
        outstanding_.clear();
        close_life();
        static_cast<void>(ledger_.progressed());
        if (hooks_.acknowledged) co_await hooks_.acknowledged();
        co_return true;
    }
    // The device crashes with the life wherever it stood. A backend whose
    // device cannot be crashed runs no history that asks for it.
    seastar::future<> crash() {
        if constexpr (requires(Backend& files) { files.crash(); }) {
            ledger_.crash();
            for (auto& request : outstanding_) {
                abandon(std::move(request.accepted));
                abandon(std::move(request.result));
            }
            outstanding_.clear();
            if (hooks_.before_crash) co_await hooks_.before_crash(life_);
            take(co_await drive_.lifecycle(files_.crash()));
            if (hooks_.after_crash) co_await hooks_.after_crash(life_);
            // Joined before anything closes.
            for (auto& left : abandoned_)
                co_await drive_.lifecycle(std::move(left));
            abandoned_.clear();
            // What closes after a crash may fail.
            if (engine_)
                static_cast<void>(co_await drive_.lifecycle(engine_->stop()));
            close_life();
            if (!hooks_.after_crash)
                take(co_await drive_.lifecycle(files_.crash()));
        } else {
            require(false, "a history crashes a device that cannot be crashed");
            co_return;
        }
    }
    void close_life() {
        engine_.reset();
        heard_.reset();
        for (auto& segment : segments_) {
            segment.target.reset();
            segment.writable = false;
        }
    }

    // Tells the ledger one request's answers; whether it was reported
    // durable.
    bool answered(
      const offered& request, bool accepted, const local_append_outcome& told) {
        if (!accepted) {
            ledger_.refused(request.id);
            return false;
        }
        ledger_.accepted(request.id);
        if (told.status == local_append_status::durable && told.receipt) {
            ledger_.durable(
              request.id,
              told.receipt->block.records.bytes().begin().value(),
              appends::block_at(
                request.wire, request.generation, told.receipt->block));
            return true;
        }
        ledger_.failed(request.id);
        if (auto* segment = find(request.generation)) segment->writable = false;
        return false;
    }
    // The sealed extent a removal was asked with, as a line among the
    // ledger's: a start that lists the segment as expiring lists it with
    // the same.
    void note_authorized(
      std::uint64_t generation, const segment_immutable_expectation& sealed) {
        std::string text = "removal " + std::to_string(generation);
        for (const std::uint64_t value : std::initializer_list<std::uint64_t>{
               sealed.coverage.logical().begin().value(),
               sealed.coverage.logical().end().value(),
               sealed.coverage.physical().begin().value(),
               sealed.coverage.physical().end().value(),
               sealed.coverage.bytes().begin().value(),
               sealed.coverage.bytes().end().value(),
               sealed.file_end.value()})
            text += " " + std::to_string(value);
        for (const auto byte : sealed.digest.bytes())
            text += " " + std::to_string(byte);
        ledger_.note(text);
    }
    [[nodiscard]] static std::optional<
      std::pair<std::uint64_t, segment_immutable_expectation>>
    authorized(std::string_view note) {
        constexpr std::string_view prefix = "note removal ";
        if (!note.starts_with(prefix)) return std::nullopt;
        note.remove_prefix(prefix.size());
        std::array<std::uint64_t, 8 + codec::content_digest_bytes> values{};
        for (auto& value : values) {
            const auto end = std::min(note.find(' '), note.size());
            const auto* last = note.data() + end;
            const auto parsed = std::from_chars(note.data(), last, value);
            if (end == 0 || parsed.ec != std::errc{} || parsed.ptr != last)
                return std::nullopt;
            note.remove_prefix(std::min(end + 1, note.size()));
        }
        if (!note.empty()) return std::nullopt;
        const auto logical = model::range_logical_span::make(
          model::range_logical_end{values[1]},
          model::range_logical_end{values[2]});
        const auto physical = model::segment_relative_span::make(
          model::segment_relative_end{values[3]},
          model::segment_relative_end{values[4]});
        const auto bytes = model::file_byte_span::make(
          runtime::file_position{values[5]}, runtime::file_position{values[6]});
        if (!logical || !physical || !bytes) return std::nullopt;
        codec::content_digest digest{};
        for (std::size_t i = 0; i != digest.size(); ++i) {
            if (values[8 + i] > 255) return std::nullopt;
            digest[i] = static_cast<unsigned char>(values[8 + i]);
        }
        return std::pair{
          values[0],
          segment_immutable_expectation{
            storage::coverage{*logical, *physical, *bytes},
            codec::extent_digest{digest},
            runtime::file_position{values[7]}}};
    }
    [[nodiscard]] held* find(std::uint64_t generation) {
        for (auto& segment : segments_)
            if (segment.generation == generation) return &segment;
        return nullptr;
    }

    // One operation. False when the cut came first.
    seastar::future<bool> apply(history_operation operation) {
        auto* table = engine_->table();
        switch (operation.op) {
        case history_op::append: {
            const bool stale = (operation.c & 0x80U) != 0;
            auto* segment = choose(operation.a, [stale](const held& one) {
                return one.target && one.writable != stale;
            });
            if (!segment) co_return true;
            for (unsigned i = 0; i != operation.b % 4U + 1U; ++i) {
                auto wire = appends::child_wire(
                  segment->next_logical, segment->generation);
                // Requests in flight together never share a work account:
                // each has its own, borrowed until its acceptance is ready.
                auto work = std::make_unique<codec::cooperative_work>(
                  codec::limits::defaults(), abort_);
                const auto id = ledger_.offer(
                  segment->generation, segment->next_logical++, wire);
                auto stages = engine_->append()->append(
                  *segment->target,
                  co_await appends::request(wire, budget_, *work),
                  *work);
                auto accepted = std::move(stages.accepted)
                                  .finally(
                                    [kept = std::move(work)] noexcept {});
                outstanding_.push_back(
                  {id,
                   segment->generation,
                   std::move(wire),
                   std::move(accepted),
                   std::move(stages.result)});
            }
            co_return true;
        }
        case history_op::settle: {
            while (!outstanding_.empty()) {
                auto& request = outstanding_.front();
                if (
                  !co_await reach(request.accepted)
                  || !co_await reach(request.result))
                    co_return false;
                const auto decided = co_await std::move(request.accepted);
                const bool durable = answered(
                  request,
                  decided.accepted(),
                  co_await std::move(request.result));
                outstanding_.pop_front();
                if (durable && hooks_.acknowledged)
                    co_await hooks_.acknowledged();
            }
            co_return true;
        }
        case history_op::read: {
            if (auto* segment = choose(operation.a, [](const held& one) {
                    return !one.gone;
                }))
                co_await read_back(*segment);
            co_return true;
        }
        case history_op::create: {
            if (segments_.size() >= settings_.segments) co_return true;
            const std::uint64_t generation = segments_.size() + 1;
            segments_.push_back({generation, appends::context(generation)});
            try {
                ledger_.create_asked(generation);
            } catch (...) {
                // The ledger was not told: there is no such segment.
                segments_.pop_back();
                throw;
            }
            auto creating = table->create(
              lifecycle::creation(spec(), generation), work_);
            if (!co_await reach(creating)) {
                abandon(std::move(creating));
                co_return false;
            }
            const auto made = co_await std::move(creating);
            auto& segment = segments_.back();
            if (made) {
                ledger_.created(generation);
                segment.target = *made;
                segment.writable = true;
                most_writable_ = std::max(
                  most_writable_,
                  static_cast<std::uint32_t>(std::ranges::count_if(
                    segments_, [](const held& one) { return one.writable; })));
                if (hooks_.acknowledged) co_await hooks_.acknowledged();
            } else {
                // A creation that left an owner may have left files.
                ledger_.create_refused(
                  generation, !table->life(segment.context));
            }
            co_return true;
        }
        case history_op::seal: {
            auto* segment = choose(
              operation.a, [](const held& one) { return one.writable; });
            if (!segment) co_return true;
            ledger_.seal_asked(segment->generation);
            segment->writable = false;
            auto sealing = table->seal(segment->context);
            if (!co_await reach(sealing)) {
                abandon(std::move(sealing));
                co_return false;
            }
            const auto sealed = co_await std::move(sealing);
            if (!sealed.failure.failed() && sealed.extent && sealed.expected) {
                ledger_.sealed(segment->generation);
                segment->expected = sealed.expected;
                if (hooks_.acknowledged) co_await hooks_.acknowledged();
            } else {
                ledger_.seal_failed(segment->generation);
            }
            co_return true;
        }
        case history_op::checkpoint: {
            auto cutting = engine_->checkpoint()->request();
            if (!co_await reach(cutting)) {
                abandon(std::move(cutting));
                co_return false;
            }
            static_cast<void>(co_await std::move(cutting));
            co_return true;
        }
        case history_op::cut: {
            auto* segment = choose(
              operation.a, [](const held& one) { return one.writable; });
            if (!segment) co_return true;
            auto cutting = table->cut_retries(segment->context, true);
            if (!co_await reach(cutting)) {
                abandon(std::move(cutting));
                co_return false;
            }
            static_cast<void>(co_await std::move(cutting));
            co_return true;
        }
        case history_op::remove: {
            auto* segment = choose(
              operation.a, [](const held& one) { return !one.gone; });
            if (!segment) co_return true;
            if (segment->writable) {
                // A segment that is still written is not removed.
                const auto refused = co_await drive_.lifecycle(
                  table->remove({segment->context, std::nullopt}));
                require(
                  refused.verdict == authorization_verdict::attached
                    && !refused.gone && refused.removed == 0,
                  "a segment that is still written was removed");
                co_return true;
            }
            segment->authorization = deletion_authorization{
              segment->context, std::nullopt};
            if (segment->expected)
                segment->authorization->sealed = segment->expected->extent;
            ledger_.removal_asked(segment->generation);
            if (segment->authorization->sealed)
                note_authorized(
                  segment->generation, *segment->authorization->sealed);
            auto removing = table->remove(
              *segment->authorization,
              segment->expected ? &*segment->expected : nullptr);
            if (!co_await reach(removing)) {
                abandon(std::move(removing));
                co_return false;
            }
            const auto removed = co_await std::move(removing);
            if (removed.gone) {
                ledger_.removed(segment->generation);
                segment->gone = true;
                if (hooks_.acknowledged) co_await hooks_.acknowledged();
            }
            co_return true;
        }
        case history_op::advance: {
            // Nothing is waited for: whatever runs by itself runs.
            seastar::promise<> never;
            auto waiting = never.get_future();
            bool whole = true;
            for (unsigned i = 0; whole && i != operation.a % 16U + 1U; ++i) {
                if (cut_reached()) {
                    whole = false;
                } else if (!co_await step_(waiting)) {
                    break;
                }
            }
            never.set_value();
            co_await std::move(waiting);
            co_return whole;
        }
        case history_op::restart:
        case history_op::crash:
            break;
        }
        co_return true;
    }

    // Every segment the ledger knows: where its directory is, and what is
    // read of it.
    seastar::future<> read_back_all() {
        for (auto& segment : segments_) {
            const auto own = take(
              co_await drive_.lifecycle(
                files_.exists(take(paths_.segment(0, named(segment))))));
            const auto marked = take(
              co_await drive_.lifecycle(
                files_.exists(take(paths_.deleting(0, named(segment))))));
            require(!(own && marked), "a segment is under both of its names");
            static_cast<void>(ledger_.named(
              segment.generation,
              own      ? name::own
              : marked ? name::deleting
                       : name::absent));
            if (own) {
                co_await read_back(segment);
            } else {
                static_cast<void>(ledger_.image_missing(segment.generation));
            }
        }
    }
    seastar::future<> read_back(held& segment) {
        // Everything a reader may be shown.
        constexpr std::uint64_t everything = 1U << 20U;
        auto* table = engine_->table();
        const auto life = table->life(segment.context);
        if (
          life ? segment_admits_readers(*life)
               : static_cast<bool>(segment.expected)) {
            auto opened = co_await drive_.lifecycle(table->open_session(
              segment.context,
              lifecycle::sight(everything),
              budget_,
              work_,
              segment.expected ? &*segment.expected : nullptr));
            if (opened) {
                std::deque<std::string> kept;
                std::vector<engine_ledger::read_batch> batches;
                segment_read_position from{
                  model::range_logical_end{100}, std::nullopt};
                auto stop = segment_read_stop::limit;
                while (stop == segment_read_stop::limit) {
                    auto read = co_await lifecycle::read_from(
                      *opened, from, byte_count{1_MiB}, drive_);
                    for (const auto& batch : read.batches) {
                        kept.push_back(flat(batch.bytes));
                        batches.push_back(
                          {batch.logical.begin().value(), kept.back()});
                    }
                    // A read that returns nothing and no end went nowhere.
                    stop = read.batches.empty()
                               && read.stop == segment_read_stop::limit
                             ? segment_read_stop::failed
                             : read.stop;
                    from = read.next;
                }
                static_cast<void>(ledger_.read_back(
                  segment.generation,
                  batches,
                  everything,
                  stop == segment_read_stop::end_of_segment
                    || stop == segment_read_stop::short_of_visibility));
            }
        }
        const auto data = take(
          paths_.segment_file(0, named(segment), local_segment_file::data));
        if (!take(co_await drive_.lifecycle(files_.exists(data)))) {
            static_cast<void>(ledger_.image_missing(segment.generation));
            co_return;
        }
        static_cast<void>(ledger_.image_back(
          segment.generation,
          co_await store_contract::read_all_bytes(files_, data, drive_)));
    }

    Backend& files_;
    Owner& owner_;
    std::array<local_device_spec, 1> devices_;
    workload_budget& budget_;
    workload_budget& reserve_;
    Driver drive_;
    Timer& timer_;
    Step step_;
    local_paths paths_;
    seastar::abort_source abort_;
    // The account of the operations the driver awaits one at a time.
    codec::cooperative_work work_{codec::limits::defaults(), abort_};
    engine_ledger ledger_;
    std::uint32_t life_{0};
    // This life's cut, whether the device's changes are counted yet, and
    // its count where the counting began.
    std::optional<std::uint64_t> cut_;
    bool counting_{false};
    std::uint64_t base_{0};
    std::deque<held> segments_;
    std::deque<offered> outstanding_;
    std::vector<seastar::future<>> abandoned_;
    std::vector<recovery_catalog_entry> catalog_;
    std::optional<lifecycle::told> heard_;
    std::unique_ptr<engine_type> engine_;
    history_hooks hooks_;
    history_settings settings_;
    std::uint32_t most_writable_{0};
};

// One run of `history` over a store that was never used.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver,
  typename Timer,
  typename Step>
seastar::future<history_report> run_history(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  workload_budget& reserve,
  Driver drive,
  Timer& timer,
  Step step,
  const engine_history& history,
  history_cuts cuts,
  history_hooks hooks = {},
  history_settings settings = {}) {
    history_run<Clock, Backend, Owner, Driver, Timer, Step> run{
      files, owner, spec, budget, reserve, drive, timer, step};
    co_return co_await run.run(
      history, cuts, std::move(hooks), std::move(settings));
}

} // namespace kwaque::storage::testing::engine_history_contract
