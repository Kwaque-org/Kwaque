#pragma once

#include "src/storage/recovery_decision.h"
#include "src/storage/segment_scan.h"
#include "src/storage/segment_writer.h"

#include <seastar/core/coroutine.hh>

#include <exception>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

namespace kwaque::storage {

// Why the extent below a decided end could not be proved.
enum class recovered_seal_gap : std::uint8_t {
    none,
    // An intact object other than the candidate the plan places there, a
    // candidate inside another object, or an object across the end.
    conflict,
    // Bytes that no surviving object or authorized candidate fills and that
    // are not a proved footer-sized gap between groups.
    unproved,
};

struct recovered_seal_outcome final {
    segment_seal_outcome seal;
    // The publication already pinned a sealed root at the decided end: an
    // earlier attempt completed and nothing changed now.
    bool already_sealed{false};
    // The layout below the decided end could not be proved: nothing was
    // written or sealed, the WAL keeps every candidate and the decision
    // waits for its owner.
    recovered_seal_gap unresolved{recovered_seal_gap::none};
    // Where proof failed.
    std::optional<runtime::file_position> at;
    // Candidate blocks and footers reconstructed below the end.
    std::uint32_t blocks{0}, footers{0};
};

namespace detail {
// The end a decoded decision seals this segment at: a seal_at decision for
// exactly this segment, or wrong_context.
[[nodiscard]] runtime::result<runtime::file_position> recovered_seal_end(
  const local_recovery_decision&, const local_segment_descriptor&) noexcept;

// Requires the pinned footer, when it lies below the end, to be exactly the
// footer the walk verified there.
struct recovered_seal_pin final {
    std::optional<local_footer_reference> pin;
    bool* seen;
    seastar::future<runtime::result<bool>>
    operator()(const segment_scanned_object& value) const {
        if (
          value.footer && pin
          && value.footer->footer.position() == pin->position()) {
            if (value.footer->footer != *pin)
                return seastar::make_ready_future<runtime::result<bool>>(
                  runtime::failure(path_error(errc::corrupt_data)));
            *seen = true;
        }
        return seastar::make_ready_future<runtime::result<bool>>(true);
    }
};

// Failures that say the candidate cannot continue the verified layout, not
// that the walk could not run.
[[nodiscard]] bool recovered_layout_failure(errc) noexcept;

struct recovered_reconstruction_result final {
    codec::immutable_object_digest identity;
    recovered_seal_gap gap{recovered_seal_gap::none};
    std::optional<runtime::file_position> at;
    std::uint32_t blocks{0}, footers{0};
    // Present when the layout was proved and the WAL has a content end.
    std::optional<recovered_seal_proof> proof;
};

// One pass of a recovered seal's reconstruction, one decode loop over the
// WAL: the segment's PREPAREs arrive in WAL order, which is file order within
// a segment, and each slot from the certified end to the decided end is
// filled in turn. A slot holds its verified surviving object;
// where the walk finds damage, the PREPARE that names the slot fills it with
// the block encoded from its exact child. A gap before the next candidate is
// filled only when it is exactly the footer the verified prefix encodes, the
// previous object is a block and the candidate continues the prefix with no
// record missing: the end of a group. A surviving later footer must name the
// prefix with the reconstruction in it. Nothing below the certified end is
// replaced, and an intact object is never overwritten. Without a writer the
// pass only proves the layout (Writer is void); with one, each reconstructed
// object is written in place once the walk has verified it. Every PREPARE of
// the segment feeds the plan identity whether or not it is kept.
template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  typename Writer>
class recovered_reconstruction final {
public:
    recovered_reconstruction(
      Backend& files,
      Owner& ownership,
      const local_device_spec& data,
      std::uint32_t shard,
      segment_history_context history,
      runtime::file_position end,
      std::optional<local_footer_reference> pin,
      Writer* writer,
      workload_budget& budget,
      segment_scan_limits limits,
      codec::cooperative_work& work) noexcept
      : files_(files)
      , ownership_(ownership)
      , data_(data)
      , shard_(shard)
      , history_(history)
      , end_(end)
      , pin_(pin)
      , certified_(
          pin ? runtime::
                  file_position{pin->position().value() + pin->bytes().value()}
              : history.data_start)
      , writer_(writer)
      , budget_(budget)
      , limits_(limits)
      , work_(work)
      , identity_(history.segment, end) {}
    recovered_reconstruction(const recovered_reconstruction&) = delete;
    recovered_reconstruction&
    operator=(const recovered_reconstruction&) = delete;

    // Opens the walk at the certified end, unless the certified prefix
    // already covers the decided end.
    seastar::future<runtime::result<void>> open() {
        if (certified_ >= end_) co_return runtime::result<void>{};
        auto handle = budget_.try_reserve(limits_.metadata.execution_bytes);
        if (!handle) co_return runtime::failure(handle.error());
        handle_.emplace(std::move(*handle));
        auto opened = co_await open_local_segment_data(
          files_, ownership_, data_, shard_, history_, *handle_, work_);
        if (!opened) co_return runtime::failure(opened.error());
        file_.emplace(std::move(*opened));
        // Bounded at the decided end: nothing past it is read or placed.
        auto made = co_await segment_scanner::make(
          *file_, budget_, history_, pin_, limits_, work_, end_);
        if (!made) co_return runtime::failure(made.error());
        scanner_ = std::move(*made);
        // Damage to the pinned footer is proven corruption, never a gap.
        if (scanner_->report().verdict == segment_scan_verdict::corrupt)
            co_return runtime::failure(path_error(errc::corrupt_data));
        active_ = true;
        co_return runtime::result<void>{};
    }

    seastar::future<runtime::result<void>>
    visit(const recovery_suffix_prepare& prepare) {
        identity_.add(prepare);
        const auto at = prepare.target;
        if (
          !active_ || gap_ != recovered_seal_gap::none || at >= end_
          || at < certified_)
            co_return runtime::result<void>{};
        for (;;) {
            const auto& report = scanner_->report();
            const auto x = report.content_end;
            if (x > at) co_return unresolve(recovered_seal_gap::conflict, at);
            if (stopped_) {
                if (rejected(report))
                    co_return unresolve(recovered_seal_gap::conflict, x);
                if (x < at) {
                    auto filled = co_await fill_footer(x, at, prepare);
                    if (!filled || gap_ != recovered_seal_gap::none)
                        co_return filled;
                    continue;
                }
                co_return co_await fill_block(prepare);
            }
            auto next = co_await scanner_->next(work_);
            if (!next) co_return runtime::failure(next.error());
            if (!*next) {
                stopped_ = true;
                continue;
            }
            const auto& object = **next;
            const auto begin = object_begin(object);
            if (object_end(object) > end_)
                co_return unresolve(recovered_seal_gap::conflict, begin);
            if (begin < at) {
                last_block_ = object.block != nullptr;
                continue;
            }
            // Its slot holds an intact object: it must be this candidate.
            if (!object.block)
                co_return unresolve(recovered_seal_gap::conflict, at);
            auto difference = compare_recovery_copies(
              *prepare.prepare, *object.block);
            if (!difference)
                co_return runtime::failure(
                  path_error(to_errc(difference.error())
                               .value_or(errc::invariant_violation)));
            if (*difference != recovery_copy_difference::none)
                co_return unresolve(recovered_seal_gap::conflict, at);
            last_block_ = true;
            co_return runtime::result<void>{};
        }
    }

    // After the last PREPARE: every byte up to the decided end must be a
    // surviving object.
    seastar::future<runtime::result<void>> drain() {
        if (!active_ || gap_ != recovered_seal_gap::none)
            co_return runtime::result<void>{};
        for (;;) {
            const auto& report = scanner_->report();
            const auto x = report.content_end;
            if (x == end_) co_return runtime::result<void>{};
            // The last object placed runs across the end.
            if (x > end_)
                co_return unresolve(recovered_seal_gap::conflict, end_);
            if (stopped_)
                co_return unresolve(
                  rejected(report) ? recovered_seal_gap::conflict
                                   : recovered_seal_gap::unproved,
                  x);
            auto next = co_await scanner_->next(work_);
            if (!next) co_return runtime::failure(next.error());
            if (!*next) {
                stopped_ = true;
                continue;
            }
            const auto& object = **next;
            if (object_end(object) > end_)
                co_return unresolve(
                  recovered_seal_gap::conflict, object_begin(object));
        }
    }

    seastar::future<runtime::result<void>> close() {
        runtime::first_failure failed;
        if (scanner_) {
            co_await scanner_->close();
            scanner_.reset();
        }
        if (file_) {
            try {
                failed.observe(co_await file_->close());
            } catch (...) {
                failed.observe(std::current_exception());
            }
            file_.reset();
        }
        handle_.reset();
        co_return failed.outcome();
    }

    [[nodiscard]] recovered_reconstruction_result
    finish(std::optional<local_wal_cursor> content_end) && noexcept {
        recovered_reconstruction_result output{
          std::move(identity_).finish(), gap_, at_, blocks_, footers_, {}};
        if (gap_ == recovered_seal_gap::none && content_end)
            output.proof = recovered_seal_proof{
              history_.segment,
              end_,
              history_.alignment,
              *content_end,
              output.identity};
        return output;
    }

private:
    static runtime::file_position
    object_begin(const segment_scanned_object& object) noexcept {
        return object.block
                 ? object.block->descriptor().coverage().bytes().begin()
                 : object.footer->footer.position();
    }
    static runtime::file_position
    object_end(const segment_scanned_object& object) noexcept {
        if (object.block)
            return object.block->descriptor().coverage().bytes().end();
        const auto& footer = object.footer->footer;
        return runtime::file_position{
          footer.position().value() + footer.bytes().value()};
    }
    // The walk stopped at bytes someone wrote, not at damage: an intact
    // object the prefix rejects, or one whose verified header runs past the
    // decided end while the file goes on.
    bool rejected(const segment_scan_report& report) const noexcept {
        return report.intact
               || (report.stop == segment_scan_stop::torn
                   && report.file_bytes > end_.value());
    }
    runtime::result<void>
    unresolve(recovered_seal_gap gap, runtime::file_position at) noexcept {
        gap_ = gap;
        at_ = at;
        return {};
    }

    seastar::future<runtime::result<void>>
    fill_block(const recovery_suffix_prepare& prepare) {
        const auto location = prepare.prepare->target();
        auto child = std::move(*prepare.prepare).release_batch();
        const auto info = child.info();
        const auto original = info.context.submitted();
        const auto cost = child.bytes().allocation_cost(
          limits_.metadata.charge);
        if (!cost) co_return runtime::failure(path_error(errc::out_of_range));
        auto encoded = co_await encode_segment_block(
          std::move(child),
          {location,
           history_.data_start,
           {history_.segment.topic(),
            history_.segment.range(),
            original.id(),
            original.binding(),
            info.fingerprint},
           history_.profile},
          work_,
          byte_count{
            cost->backing.value() + cost->descriptors.value()
            + cost->share_controls.value() + limits_.working_bytes.value()},
          limits_.metadata.charge);
        if (!encoded) {
            if (recovered_layout_failure(encoded.error().code()))
                co_return unresolve(
                  recovered_seal_gap::conflict, location.position());
            co_return runtime::failure(path_error(encoded.error().code()));
        }
        // A candidate across the decided end: the end is not a boundary.
        if (encoded->descriptor().coverage().bytes().end() > end_)
            co_return unresolve(
              recovered_seal_gap::conflict, location.position());
        auto placed = co_await scanner_->substitute(std::move(*encoded), work_);
        if (!placed) {
            if (recovered_layout_failure(placed.error().code()))
                co_return unresolve(
                  recovered_seal_gap::unproved, location.position());
            co_return runtime::failure(placed.error());
        }
        if constexpr (!std::is_void_v<Writer>) {
            if (writer_) {
                auto written = co_await writer_->write_recovered(
                  location.position(),
                  std::move(*placed).release_bytes(),
                  work_);
                if (!written) co_return written;
            }
        }
        ++blocks_;
        last_block_ = true;
        stopped_ = false;
        co_return runtime::result<void>{};
    }

    seastar::future<runtime::result<void>> fill_footer(
      runtime::file_position at,
      runtime::file_position next,
      const recovery_suffix_prepare& prepare) {
        if (!last_block_) co_return unresolve(recovered_seal_gap::unproved, at);
        auto prefix = scanner_->prefix(work_);
        if (!prefix) co_return runtime::failure(prefix.error());
        if (
          prepare.prepare->target().physical_begin()
          != prefix->boundary().coverage.physical().end())
            co_return unresolve(recovered_seal_gap::unproved, at);
        auto encoded = co_await encode_durable_footer(
          *prefix,
          {history_, at},
          work_,
          limits_.working_bytes,
          limits_.metadata.charge);
        if (!encoded) {
            if (recovered_layout_failure(encoded.error().code()))
                co_return unresolve(recovered_seal_gap::unproved, at);
            co_return runtime::failure(path_error(encoded.error().code()));
        }
        if (at.value() + encoded->bytes().size().value() != next.value())
            co_return unresolve(recovered_seal_gap::unproved, at);
        auto placed = co_await scanner_->substitute(std::move(*encoded), work_);
        if (!placed) co_return runtime::failure(placed.error());
        if constexpr (!std::is_void_v<Writer>) {
            if (writer_) {
                auto written = co_await writer_->write_recovered(
                  at, std::move(*placed).release_bytes(), work_);
                if (!written) co_return written;
            }
        }
        ++footers_;
        last_block_ = false;
        stopped_ = false;
        co_return runtime::result<void>{};
    }

    Backend& files_;
    Owner& ownership_;
    const local_device_spec& data_;
    std::uint32_t shard_;
    segment_history_context history_;
    runtime::file_position end_;
    std::optional<local_footer_reference> pin_;
    runtime::file_position certified_;
    Writer* writer_;
    workload_budget& budget_;
    segment_scan_limits limits_;
    codec::cooperative_work& work_;
    recovery_suffix_identity identity_;
    std::optional<workload_reservation> handle_;
    std::optional<runtime::file> file_;
    std::unique_ptr<segment_scanner> scanner_;
    std::optional<runtime::file_position> at_;
    std::uint32_t blocks_{0}, footers_{0};
    recovered_seal_gap gap_{recovered_seal_gap::none};
    bool active_{false}, stopped_{false}, last_block_{false};
};

// What a recovered seal starts from, as the segment's descriptor, header and
// current publication name it: its data start and that publication. Only
// their values are kept, not the reservations that read them.
struct recovered_seal_state final {
    runtime::file_position data_start;
    local_object_publication publication;
};
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<recovered_seal_state>>
load_recovered_seal_state(
  Backend& files,
  Owner& ownership,
  const local_device_spec& data,
  std::uint32_t shard,
  const local_segment_descriptor& descriptor,
  const segment_header& header,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    runtime::file_position data_start{};
    std::optional<local_object_publication> publication;
    {
        auto loaded = co_await load_local_segment(
          files,
          ownership,
          data,
          shard,
          descriptor,
          header,
          budget,
          limits,
          work);
        if (!loaded) co_return runtime::failure(loaded.error());
        data_start = loaded->header.bytes.end();
    }
    const auto data_paths = local_paths::make(data.root);
    if (!data_paths) co_return runtime::failure(data_paths.error());
    auto published = data_paths->segment_file(
      shard,
      {descriptor.segment.segment(), descriptor.segment.generation()},
      local_segment_file::published);
    if (!published) co_return runtime::failure(published.error());
    const auto data_owner = data.shard_owner(shard);
    if (!data_owner)
        co_return runtime::failure(path_error(errc::wrong_context));
    {
        auto selected = co_await load_local_metadata_file(
          files,
          data,
          *published,
          budget,
          limits,
          work,
          local_metadata_extent::exact_file,
          [owner = *data_owner,
           segment = descriptor.segment,
           metadata = data.identity.metadata_alignment,
           alignment = descriptor.alignment](
            auto& input, byte_count, codec::decode_budget memory, auto& work) {
              return decode_selected_object_publication(
                input,
                owner,
                segment,
                metadata,
                alignment,
                memory,
                work,
                {},
                codec::input_boundary::complete);
          });
        if (!selected) co_return runtime::failure(selected.error());
        publication.emplace(
          std::get<local_object_publication>(selected->value.payload()));
    }
    co_return recovered_seal_state{data_start, std::move(*publication)};
}

// Runs one reconstruction pass over the segment and the WAL.
template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  typename Writer>
seastar::future<runtime::result<recovered_reconstruction_result>>
reconstruct_recovered_extent(
  Backend& files,
  Owner& ownership,
  const local_device_spec& control,
  const local_device_spec& data,
  std::uint32_t shard,
  const recovery_suffix_source& wal,
  segment_history_context history,
  runtime::file_position end,
  std::optional<local_footer_reference> pin,
  Writer* writer,
  workload_budget& budget,
  segment_scan_limits limits,
  codec::cooperative_work& work) {
    recovered_reconstruction<Backend, Owner, Writer> pass{
      files,
      ownership,
      data,
      shard,
      history,
      end,
      pin,
      writer,
      budget,
      limits,
      work};
    runtime::first_failure failed;
    std::optional<local_wal_cursor> content_end;
    try {
        do {
            if (auto opened = co_await pass.open(); !opened) {
                failed.observe(opened);
                break;
            }
            auto scanned = co_await scan_recovery_suffix(
              files,
              ownership,
              control,
              shard,
              wal,
              budget,
              work,
              [state = &pass](const recovery_suffix_prepare& prepare) {
                  return state->visit(prepare);
              });
            if (!scanned) {
                failed.observe(scanned);
                break;
            }
            content_end = scanned->content_end;
            failed.observe(co_await pass.drain());
        } while (false);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    failed.observe(co_await pass.close());
    if (failed.failed()) {
        auto error = failed.outcome();
        co_return runtime::failure(error.error());
    }
    co_return std::move(pass).finish(content_end);
}
} // namespace detail

// A supplied seal's resolution: the durable decision, or, when the layout
// below its end cannot be proved, where the proof stopped. Then nothing is
// persisted and the owner may decide another end.
struct recovered_seal_decision final {
    std::optional<recovery_decision_outcome> decided;
    recovered_seal_gap gap{recovered_seal_gap::none};
    std::optional<runtime::file_position> at;
};

// Resolves a supplied segment-scope seal before anything it authorizes runs.
// What the durable decisions already settle answers first, so a repeated or
// contradicting seal is answered without reading. Otherwise the recovered
// seal's own pass proves the layout below the end against the WAL, writing
// nothing, and only a proved end becomes a durable decision naming the
// suffix plan it proved. An end inside the certified prefix is walked when
// the seal executes. `wal.target` is the segment as inventoried and
// `wal.devices` include `data`.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<recovered_seal_decision>>
resolve_recovered_seal(
  recovery_decision_log<Backend, Owner>& log,
  Backend& files,
  Owner& ownership,
  const local_device_spec& control,
  const local_device_spec& data,
  std::uint32_t shard,
  const recovery_suffix_source& wal,
  recovery_seal_resolution resolution,
  workload_budget& budget,
  segment_scan_limits limits,
  codec::cooperative_work& work) {
    const auto& descriptor = wal.target.descriptor;
    if (auto valid = limits.validate(); !valid)
        co_return runtime::failure(valid.error());
    if (
      resolution.owner_decision_id == 0
      || resolution.segment != descriptor.segment
      || !descriptor.alignment.aligned(resolution.end)
      || resolution.end.value() > descriptor.maximum_data_bytes.value())
        co_return runtime::failure(detail::path_error(errc::invalid_argument));
    if (auto held = log.settled(resolution))
        co_return recovered_seal_decision{std::move(*held), {}, {}};
    auto state = co_await detail::load_recovered_seal_state(
      files,
      ownership,
      data,
      shard,
      descriptor,
      wal.target.header,
      budget,
      limits.metadata,
      work);
    if (!state) co_return runtime::failure(state.error());
    const auto& publication = state->publication;
    if (
      (publication.state != local_object_state::active
       && publication.state != local_object_state::recovering)
      || wal.target.state != publication.state)
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    if (resolution.end < state->data_start)
        co_return runtime::failure(detail::path_error(errc::invalid_argument));
    const segment_history_context history{
      descriptor.segment,
      descriptor.alignment,
      state->data_start,
      descriptor.logical_origin,
      descriptor.physical_origin,
      descriptor.profile};
    auto proved
      = co_await detail::reconstruct_recovered_extent<Backend, Owner, void>(
        files,
        ownership,
        control,
        data,
        shard,
        wal,
        history,
        resolution.end,
        publication.boundary,
        nullptr,
        budget,
        limits,
        work);
    if (!proved) co_return runtime::failure(proved.error());
    if (proved->gap != recovered_seal_gap::none)
        co_return recovered_seal_decision{
          std::nullopt, proved->gap, proved->at};
    if (!proved->proof)
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    auto decided = co_await log.resolve(resolution, *proved->proof, work);
    if (!decided) co_return runtime::failure(decided.error());
    co_return recovered_seal_decision{std::move(*decided), {}, {}};
}

// Executes a durable segment-scope seal decision on one recovered segment:
// removes its tail only inside the recovered seal, after the decision is
// durable. The decision record is read back first, from its fixed path on the
// control device, under its pinned identity, and must be a seal_at decision
// for this segment whose suffix plan is the one the WAL still classifies.
// Nothing is touched otherwise. A pass that writes nothing then proves the
// layout below the decided end from surviving objects and the WAL's
// candidates; a layout it cannot prove is reported unresolved and nothing
// changes. Only then are the reconstructed objects written, the extent below
// the end walked from its data start with a digest, every object and footer
// verifying, and the writer seals it there: retry bundle, sealed root,
// removal of every later byte, one flush, then the sealed publication. The
// WAL is never touched. A crash after any step resumes from the same
// decision: until the sealed publication only bytes the decision places are
// written, and a publication already sealed at the end is reported as done.
// config.retry_object is reserved afresh for every attempt; an earlier
// attempt's bundle is left for reconciliation. `wal.target` is the segment's
// verified descriptor and header, `wal.devices` include `data`. Source and
// the counts are as for segment_writer::seal().
//
// An index source is given every block of the verifying walk and then the
// seal. An attempt uses it up whatever its outcome: its walk has offered
// blocks, so another attempt is given a source nothing was offered to. An
// attempt that finds the segment already sealed offers it nothing, and
// reports the index root the publication names.
template<
  runtime::monotonic_clock Clock,
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  typename Source,
  typename Index = segment_no_index>
seastar::future<runtime::result<recovered_seal_outcome>> execute_recovered_seal(
  Backend& files,
  Owner& ownership,
  const local_device_spec& control,
  const local_device_spec& data,
  std::uint32_t shard,
  const recovery_suffix_source& wal,
  recovery_decision_pin decision,
  workload_budget& budget,
  segment_writer_config config,
  segment_scan_limits limits,
  Source source,
  std::uint32_t completed,
  std::uint32_t unresolved,
  codec::cooperative_work& work,
  Index index = {}) {
    using writer_type = segment_writer<Backend, Owner, Clock>;
    const auto& descriptor = wal.target.descriptor;
    const auto& header = wal.target.header;
    if (auto valid = limits.validate(); !valid)
        co_return runtime::failure(valid.error());
    // The decision first: nothing below is touched without it.
    auto value = co_await detail::load_recovery_decision(
      files,
      ownership,
      control,
      shard,
      decision,
      descriptor,
      budget,
      limits.metadata,
      work);
    if (!value) co_return runtime::failure(value.error());
    const auto end = detail::recovered_seal_end(*value, descriptor);
    if (!end) co_return runtime::failure(end.error());

    auto state = co_await detail::load_recovered_seal_state(
      files,
      ownership,
      data,
      shard,
      descriptor,
      header,
      budget,
      limits.metadata,
      work);
    if (!state) co_return runtime::failure(state.error());
    const auto data_start = state->data_start;
    const auto& publication = state->publication;
    switch (publication.state) {
    case local_object_state::active:
    case local_object_state::recovering:
        break;
    case local_object_state::sealed:
        // An earlier attempt finished: its root sits at the decided end.
        if (
          publication.boundary
          && publication.boundary->family()
               == static_cast<std::uint16_t>(
                 codec::format_family::sealed_extent)
          && publication.boundary->position() == *end) {
            recovered_seal_outcome done;
            done.already_sealed = true;
            done.seal.boundary = publication.boundary;
            for (const auto& root : publication.roots) {
                if (root.kind() == local_root_kind::sealed_retry)
                    done.seal.retry = root;
                if (root.kind() == local_root_kind::index)
                    done.seal.index = root;
            }
            done.seal.unresolved = unresolved;
            co_return done;
        }
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    case local_object_state::deleting:
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    }
    if (wal.target.state != publication.state)
        co_return runtime::failure(detail::path_error(errc::wrong_context));

    const segment_history_context history{
      descriptor.segment,
      descriptor.alignment,
      data_start,
      descriptor.logical_origin,
      descriptor.physical_origin,
      descriptor.profile};
    // Bytes the publication's pin certifies are never reconstructed; a pin
    // below the end must be exactly the footer the final walk verifies.
    std::optional<local_footer_reference> pin;
    if (publication.boundary && publication.boundary->position() < *end)
        pin = publication.boundary;

    // The plan the decision names, and proof of the layout below its end,
    // before any byte changes.
    auto proved
      = co_await detail::reconstruct_recovered_extent<Backend, Owner, void>(
        files,
        ownership,
        control,
        data,
        shard,
        wal,
        history,
        *end,
        publication.boundary,
        nullptr,
        budget,
        limits,
        work);
    if (!proved) co_return runtime::failure(proved.error());
    if (proved->identity != value->prepare_digest)
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    if (proved->gap != recovered_seal_gap::none) {
        recovered_seal_outcome output;
        output.unresolved = proved->gap;
        output.at = proved->at;
        output.seal.unresolved = unresolved;
        co_return output;
    }

    std::unique_ptr<writer_type> writer;
    runtime::first_failure failed;
    recovered_seal_outcome output;
    output.blocks = proved->blocks;
    output.footers = proved->footers;
    try {
        do {
            auto opened = co_await writer_type::open_recovered_seal(
              files, ownership, data, shard, descriptor, budget, config, work);
            if (!opened) {
                failed.observe(opened);
                break;
            }
            writer = std::move(*opened);
            if (proved->blocks + proved->footers != 0) {
                // Only what the proving pass placed, verified again as it is
                // written.
                auto written = co_await detail::
                  reconstruct_recovered_extent<Backend, Owner, writer_type>(
                    files,
                    ownership,
                    control,
                    data,
                    shard,
                    wal,
                    history,
                    *end,
                    publication.boundary,
                    writer.get(),
                    budget,
                    limits,
                    work);
                if (!written) {
                    failed.observe(written);
                    break;
                }
                if (
                  written->identity != proved->identity
                  || written->gap != recovered_seal_gap::none
                  || written->blocks != proved->blocks
                  || written->footers != proved->footers) {
                    failed.observe(detail::path_error(errc::wrong_context));
                    break;
                }
            }
            // Every byte below the end, from the data start, with its digest.
            // A pinned footer below the end must be exactly the one found
            // there.
            // An index source is given each block as it is verified, so the
            // extent is not read again to index it. One that has a `block`
            // the walk cannot call would be left unfed without a word.
            static_assert(
              !requires { &Index::block; }
                || requires(
                  Index& fed,
                  const complete_block_descriptor&
                    described) { fed.block(described); },
              "an index source's block() takes one verified block descriptor");
            bool seen = false;
            auto extent = co_await verify_local_segment_extent(
              files,
              ownership,
              data,
              shard,
              history,
              *end,
              budget,
              limits,
              work,
              [&index, pinned = detail::recovered_seal_pin{pin, &seen}](
                const segment_scanned_object& object) {
                  if constexpr (requires {
                                    index.block(object.block->descriptor());
                                }) {
                      if (object.block) index.block(object.block->descriptor());
                  } else
                      static_cast<void>(index);
                  return pinned(object);
              });
            if (!extent) {
                failed.observe(extent);
                break;
            }
            if (pin && !seen) {
                failed.observe(detail::path_error(errc::corrupt_data));
                break;
            }
            output.seal = co_await writer->seal_recovered(
              std::move(*extent),
              std::move(source),
              completed,
              unresolved,
              work,
              std::move(index));
            failed = output.seal.failure;
        } while (false);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (writer) {
        try {
            failed.observe(co_await writer->close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
        writer.reset();
    }
    if (failed.failed()) {
        auto error = failed.outcome();
        co_return runtime::failure(error.error());
    }
    co_return output;
}

} // namespace kwaque::storage
