#pragma once

#include "src/base/units.h"
#include "src/storage/extent_verifier.h"
#include "src/storage/local_discovery.h"
#include "src/storage/storage_scan.h"

#include <seastar/core/coroutine.hh>

#include <exception>
#include <memory>
#include <optional>
#include <utility>

namespace kwaque::storage {

// Why a segment's scanned content ends where it does.
enum class segment_scan_stop : std::uint8_t {
    // Content runs exactly to the file end.
    end,
    // An all-zero prefix: unwritten, zero-written or preallocated space.
    unwritten,
    // A prefix, header or object extends past the file end.
    torn,
    // A header, body or footer data checksum does not match.
    corrupt,
    // Garbage, an integrity-valid object with malformed content, or a footer
    // whose covered history is not the scanned prefix.
    malformed,
    // Integrity-valid bytes of another family, segment or position.
    foreign,
};

// The pin is persisted certification of the prefix it covers, so damage to
// the pinned footer is proven corruption. Anywhere after it, the first damage
// ends an uncertified tail, however many valid objects survive later.
enum class segment_scan_verdict : std::uint8_t {
    // Content runs exactly to the file end.
    clean,
    uncertified_tail,
    corrupt,
};

// A footer whose covered history verified: the recovered local boundary it
// names, and the exact reference that pins it.
struct segment_scan_boundary final {
    local_footer_reference footer;
    boundary_fields fields;
};

struct segment_scan_report final {
    // The footer the walk resumed after, when one was pinned.
    std::optional<local_footer_reference> pin;
    // The last verifying footer before the first damage, or the pin itself
    // when none follows it. Absent when neither exists.
    std::optional<segment_scan_boundary> boundary;
    // Complete blocks after that boundary and before the first damage: an
    // unresolved suffix, never readable merely because its checksums pass.
    std::uint32_t suffix_blocks{0};
    // The end of the last complete object accepted.
    runtime::file_position content_end;
    std::uint64_t file_bytes{0};
    segment_scan_stop stop{segment_scan_stop::end};
    std::optional<codec::error> cause;
    // The walk stopped at an object whose integrity checks passed but that
    // names another context, is malformed or names another history: bytes
    // someone wrote, never damage.
    bool intact{false};
    segment_scan_verdict verdict{segment_scan_verdict::clean};
    // False when a visitor stopped the walk early.
    bool complete{false};
};

struct segment_scan_limits final {
    local_store_io_limits metadata{};
    scan_reader_limits reader{};
    // One object decode beyond its input: aliases, validation scratch and
    // decoded metadata. Reserved once for the scan.
    byte_count working_bytes{8_MiB};
    // The codec's own metadata ceiling. An object read across windows is
    // shared fragment by fragment, so a large object's share descriptors
    // outgrow a small allowance; this one covers the largest admitted object.
    byte_count decode_metadata_bytes{1_MiB};

    [[nodiscard]] runtime::result<void> validate() const noexcept;
};

// One complete object after the pin, borrowed until the next call.
struct segment_scanned_object final {
    // A block decoded under the segment's independent placement.
    const segment_block* block{nullptr};
    // A footer whose covered history matched the scanned prefix exactly.
    const segment_scan_boundary* footer{nullptr};
};

// One forward pass over an active or recovering segment's data file, from the
// footer an independent record pinned (or the data start) to the first
// damage. The pinned footer must match its pinned bytes and identity; the
// history it covers is neither read nor hashed again. Every later footer's
// covered history is checked against the scanned prefix in the same pass, so
// the recovered boundary is the last verifying footer before the first
// damage. A zero prefix, a tear or a checksum failure is damage, never an
// end. No byte is changed. History comes from the verified descriptor and
// segment header; pins are durable-footer references only. A bounded walk
// treats `end` as its file end; its verifier admits objects up to `end`, so
// a recovered seal may substitute objects past the current file end. A digest
// walk hashes every byte it accepts and therefore starts at the data start,
// never at a pin. The data file and budget outlive close(); one operation at
// a time.
class segment_scanner final {
public:
    [[nodiscard]] static seastar::future<
      runtime::result<std::unique_ptr<segment_scanner>>>
    make(
      runtime::file& data,
      workload_budget& budget,
      segment_history_context history,
      std::optional<local_footer_reference> pin,
      segment_scan_limits limits,
      codec::cooperative_work& work,
      std::optional<runtime::file_position> end = std::nullopt,
      extent_integrity integrity = extent_integrity::crc32c);
    segment_scanner(const segment_scanner&) = delete;
    segment_scanner& operator=(const segment_scanner&) = delete;
    segment_scanner(segment_scanner&&) = delete;
    segment_scanner& operator=(segment_scanner&&) = delete;
    ~segment_scanner();

    // The next complete object; nullopt once the walk stopped, and report()
    // then says where and why. A read error is unavailability, returned as an
    // error and never as an end or a tail.
    [[nodiscard]] seastar::future<
      runtime::result<std::optional<segment_scanned_object>>>
    next(codec::cooperative_work& work);
    [[nodiscard]] const segment_scan_report& report() const noexcept {
        return report_;
    }
    // Once the walk ended cleanly: the evidence for everything it accepted,
    // with the digest of exactly those bytes when one was requested. Once.
    [[nodiscard]] runtime::result<verified_extent>
    finish(codec::cooperative_work& work);
    // Between objects: the evidence for the prefix verified so far, from which
    // the footer that would name it is encoded.
    [[nodiscard]] runtime::result<verified_extent>
    prefix(codec::cooperative_work& work);
    // A recovered seal only. Once the walk stopped at damage after its pin,
    // accepts an object reconstructed for the slot where it stopped as though
    // it had been read there, then continues reading after it. A block must
    // be placed exactly there and extend the verified prefix; a footer must
    // name exactly that prefix. The object is returned for the caller to
    // write. A verifier failure closes the walk.
    [[nodiscard]] seastar::future<runtime::result<segment_block>>
    substitute(segment_block block, codec::cooperative_work& work);
    [[nodiscard]] seastar::future<runtime::result<encoded_durable_footer>>
    substitute(encoded_durable_footer footer, codec::cooperative_work& work);
    [[nodiscard]] seastar::future<> close() noexcept;

private:
    segment_scanner(
      runtime::file& data,
      workload_budget& budget,
      segment_history_context history,
      segment_scan_limits limits,
      workload_reservation working) noexcept;
    [[nodiscard]] runtime::result<void> substitutable() const noexcept;
    // Reads on from `at`, after a substituted object.
    seastar::future<runtime::result<void>> restart(runtime::file_position at);
    seastar::future<runtime::result<void>> resume(
      local_footer_reference pin,
      storage::coverage expected,
      codec::cooperative_work& work);
    seastar::future<runtime::result<std::optional<segment_scanned_object>>>
    take_block(scan_slot slot, codec::cooperative_work& work);
    seastar::future<runtime::result<std::optional<segment_scanned_object>>>
    take_footer(scan_slot slot, codec::cooperative_work& work);
    [[nodiscard]] runtime::result<codec::decode_budget> budget_for(
      const bytes::fragmented_buffer_parser& input,
      byte_count length,
      const codec::limits& policy) const;
    void stop(
      segment_scan_stop kind,
      std::optional<codec::error> cause,
      bool intact = false);

    runtime::file& data_;
    workload_budget& budget_;
    segment_history_context history_;
    segment_scan_limits limits_;
    workload_reservation working_;
    std::optional<runtime::file_position> end_;
    std::unique_ptr<scan_reader> reader_;
    std::optional<extent_verifier> verifier_;
    // The physical begin of the next block.
    model::segment_relative_end physical_;
    std::optional<segment_block> block_;
    segment_scan_report report_;
    bool pending_advance_{false}, stopped_{false}, closed_{false};
};

namespace detail {
// Decode failures of an integrity-checked object that are damage, or nullopt
// for those that are unsupported state, pressure or a caller error.
[[nodiscard]] std::optional<segment_scan_stop>
segment_decode_stop(const codec::error&) noexcept;
// The error for a walk that had to reach its end but stopped first.
[[nodiscard]] runtime::operation_error
segment_short_walk(const segment_scan_report&) noexcept;

template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<runtime::file>> open_local_segment_data(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  std::uint32_t shard,
  const segment_history_context& history,
  workload_reservation& handle,
  codec::cooperative_work& work) {
    if (auto valid = co_await ownership.validate(spec); !valid)
        co_return runtime::failure(valid.error());
    const auto paths = local_paths::make(spec.root);
    if (!paths) co_return runtime::failure(paths.error());
    auto path = paths->segment_file(
      shard,
      {history.segment.segment(), history.segment.generation()},
      local_segment_file::data);
    if (!path) co_return runtime::failure(path.error());
    if (auto held = handle.try_acquire_handles(1); !held)
        co_return runtime::failure(held.error());
    if (
      auto inspected = co_await inspect_local_path(
        files, spec.root, *path, runtime::file_kind::regular, work);
      !inspected)
        co_return runtime::failure(inspected.error());
    co_return co_await files.open(
      *path, {.close_policy = runtime::file_close_policy::checked});
}

// The walk behind scan_local_segment and verify_local_segment_extent: opens
// the data file read-only on its device and walks it with segment_scanner,
// reporting where and why it stopped. With `evidence`, a walk that ends
// cleanly also yields the scanner's finished evidence.
template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  typename Visitor>
seastar::future<runtime::result<segment_scan_report>> walk_local_segment(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  std::uint32_t shard,
  segment_history_context history,
  std::optional<local_footer_reference> pin,
  std::optional<runtime::file_position> end,
  extent_integrity integrity,
  workload_budget& budget,
  segment_scan_limits limits,
  codec::cooperative_work& work,
  Visitor visit,
  std::optional<verified_extent>* evidence = nullptr) {
    if (auto valid = limits.validate(); !valid)
        co_return runtime::failure(valid.error());
    auto handle = budget.try_reserve(limits.metadata.execution_bytes);
    if (!handle) co_return runtime::failure(handle.error());
    auto opened = co_await open_local_segment_data(
      files, ownership, spec, shard, history, *handle, work);
    if (!opened) co_return runtime::failure(opened.error());
    auto file = std::move(*opened);
    runtime::first_failure failed;
    std::optional<segment_scan_report> output;
    std::unique_ptr<segment_scanner> scanner;
    try {
        do {
            auto made = co_await segment_scanner::make(
              file, budget, history, pin, limits, work, end, integrity);
            if (!made) {
                failed.observe(made);
                break;
            }
            scanner = std::move(*made);
            bool complete = true;
            for (;;) {
                auto next = co_await scanner->next(work);
                if (!next) {
                    failed.observe(next);
                    break;
                }
                if (!*next) break;
                auto proceed = co_await visit(**next);
                if (!proceed) {
                    failed.observe(proceed);
                    break;
                }
                if (!*proceed) {
                    complete = false;
                    break;
                }
            }
            if (failed.failed()) break;
            output = scanner->report();
            output->complete = complete;
            if (
              evidence && complete && output->stop == segment_scan_stop::end) {
                auto finished = scanner->finish(work);
                if (!finished) {
                    failed.observe(finished);
                    break;
                }
                evidence->emplace(std::move(*finished));
            }
        } while (false);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (scanner) {
        co_await scanner->close();
        scanner.reset();
    }
    try {
        failed.observe(co_await file.close());
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (failed.failed()) {
        if (evidence) evidence->reset();
        auto error = failed.outcome();
        co_return runtime::failure(error.error());
    }
    co_return std::move(*output);
}
} // namespace detail

// Opens one segment's data file read-only on the device that holds it and
// walks it to its first damage with segment_scanner. visit(const
// segment_scanned_object&) returns future<result<bool>> (false stops) and
// borrows its argument through a joined call.
template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  typename Visitor>
seastar::future<runtime::result<segment_scan_report>> scan_local_segment(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  std::uint32_t shard,
  segment_history_context history,
  std::optional<local_footer_reference> pin,
  workload_budget& budget,
  segment_scan_limits limits,
  codec::cooperative_work& work,
  Visitor visit) {
    static_assert(sizeof(Visitor) <= 8_KiB);
    co_return co_await detail::walk_local_segment(
      files,
      ownership,
      spec,
      shard,
      history,
      pin,
      std::nullopt,
      extent_integrity::crc32c,
      budget,
      limits,
      work,
      std::move(visit));
}

// Walks one segment's data file from its data start to exactly `end`, a
// complete-object boundary, verifying every object and the covered history of
// every footer, and returns that extent's evidence with the digest of exactly
// those bytes. Damage, a tear or zeros before `end`, or an object across it,
// is an error: the extent is not the complete surviving prefix it claims.
// visit is as for scan_local_segment.
template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  typename Visitor>
seastar::future<runtime::result<verified_extent>> verify_local_segment_extent(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  std::uint32_t shard,
  segment_history_context history,
  runtime::file_position end,
  workload_budget& budget,
  segment_scan_limits limits,
  codec::cooperative_work& work,
  Visitor visit) {
    static_assert(sizeof(Visitor) <= 8_KiB);
    std::optional<verified_extent> output;
    auto walked = co_await detail::walk_local_segment(
      files,
      ownership,
      spec,
      shard,
      history,
      std::nullopt,
      end,
      extent_integrity::crc32c_and_digest,
      budget,
      limits,
      work,
      std::move(visit),
      &output);
    if (!walked) co_return runtime::failure(walked.error());
    // A file that ends cleanly before `end` is short, not an extent to it.
    if (!output || !walked->complete || walked->content_end != end)
        co_return runtime::failure(detail::segment_short_walk(*walked));
    co_return std::move(*output);
}

} // namespace kwaque::storage
