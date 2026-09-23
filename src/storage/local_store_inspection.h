#pragma once

#include "src/storage/local_metadata_file.h"

#include <array>
#include <bitset>
#include <optional>
#include <string_view>

namespace kwaque::storage {
namespace detail {
inline bool initial_control(const local_loaded_metadata& record) {
    const auto& value = std::get<local_shard_control>(record.value.payload());
    return record.value.header().generation().value() == 1
           && value.wal_high.empty() && value.object_high.value() == 0
           && value.decision_high.value() == 0
           && value.deletion_high.value() == 0 && !value.checkpoint
           && !value.wal_head;
}
inline bool classify_store_error(
  local_store_report& report, runtime::operation_error error) {
    switch (error.code()) {
    case errc::unsupported_format:
        report.state = local_store_state::unsupported;
        break;
    case errc::malformed_data:
    case errc::corrupt_data:
    case errc::wrong_context:
        report.state = local_store_state::corrupt;
        break;
    case errc::not_found:
        report.state = local_store_state::incomplete;
        break;
    default:
        return false;
    }
    report.reason = std::move(error);
    report.bootstrap_compatible = false;
    return true;
}

template<runtime::file_system_backend Backend>
class local_namespace_scan final {
    enum class node : std::uint8_t {
        root,
        shards,
        shard,
        wal,
        wal_bucket,
        segments,
        segment_bucket,
        segment,
        objects,
        checkpoints,
        evidence,
        decisions,
        deletions
    };
    struct frame final {
        std::optional<runtime::file_path> path;
        std::optional<typename Backend::directory_cursor_type> cursor;
        std::optional<runtime::directory_page> page;
        std::size_t index{0};
        bool end{false};
        node kind{node::root};
        std::uint32_t shard{0};
        std::uint8_t bucket{0};
        std::optional<local_segment_name> segment;
        std::uint16_t seen{0};
        std::optional<local_wal_head> head;
        local_wal_high wal_high;
    };

public:
    local_namespace_scan(
      Backend& files,
      const local_device_spec& spec,
      workload_budget& budget,
      local_store_io_limits limits,
      codec::cooperative_work& work,
      local_store_open_options options)
      : files_(files)
      , spec_(spec)
      , budget_(budget)
      , limits_(limits)
      , work_(work)
      , options_(options)
      , paths_(local_paths::make(spec.root).value()) {}

    seastar::future<runtime::result<local_store_report>> inspect() {
        auto reservation = budget_.try_reserve(
          byte_count{limits_.execution_bytes.value() + 131072});
        if (!reservation) co_return runtime::failure(reservation.error());
        if (
          auto held = reservation->try_acquire_handles(
            static_cast<std::uint32_t>(frames_.size()));
          !held)
            co_return runtime::failure(held.error());
        runtime::first_failure failed;
        try {
            do {
                auto marker_path = paths_.store();
                if (!marker_path) {
                    failed.observe(marker_path);
                    break;
                }
                {
                    auto marker = co_await load_local_identity(
                      files_, spec_, *marker_path, budget_, limits_, work_);
                    if (!marker) {
                        if (marker.error().code() != errc::not_found) {
                            failed.observe(marker);
                            break;
                        }
                    } else {
                        report_.identity_present = true;
                        if (
                          std::get<local_store_identity>(
                            marker->value.payload())
                          != spec_.identity) {
                            failed.observe(path_error(errc::wrong_context));
                            break;
                        }
                        identity_witness_ = true;
                    }
                }
                auto started = co_await push(spec_.root, node::root, nullptr);
                if (!started) {
                    failed.observe(started);
                    break;
                }
                while (depth_ && !failed.failed()) {
                    auto& current = frames_[depth_ - 1];
                    if (
                      !current.page
                      || current.index == current.page->entries().size()) {
                        current.page.reset();
                        current.index = 0;
                        if (current.end) {
                            finish(current);
                            auto closed = co_await current.cursor->close();
                            if (!closed) {
                                failed.observe(closed);
                                break;
                            }
                            current.cursor.reset();
                            current.path.reset();
                            --depth_;
                            continue;
                        }
                        auto ready = co_await path_checkpoint(work_);
                        if (!ready) {
                            failed.observe(ready);
                            break;
                        }
                        auto page = co_await current.cursor->next(
                          {.maximum_entries = item_count{16},
                           .maximum_name_bytes = byte_count{4096}});
                        if (!page) {
                            failed.observe(page);
                            break;
                        }
                        current.end = page->end();
                        current.page.emplace(std::move(*page));
                        continue;
                    }
                    auto ready = co_await path_checkpoint(work_);
                    if (!ready) {
                        failed.observe(ready);
                        break;
                    }
                    const auto& entry
                      = current.page->entries()[current.index++];
                    auto checked = co_await entry_at(current, entry);
                    if (!checked) {
                        failed.observe(checked);
                        break;
                    }
                }
            } while (false);
        } catch (...) {
            failed.observe(std::current_exception());
        }
        while (depth_) {
            auto& frame = frames_[depth_ - 1];
            frame.page.reset();
            if (frame.cursor) {
                try {
                    failed.observe(co_await frame.cursor->close());
                } catch (...) {
                    failed.observe(std::current_exception());
                }
                frame.cursor.reset();
            }
            frame.path.reset();
            --depth_;
        }
        if (failed.failed()) {
            if (failed.exception()) std::rethrow_exception(failed.exception());
            if (!classify_store_error(report_, *failed.error()))
                co_return runtime::failure(*failed.error());
            co_return report_;
        }
        if (spec_.mount_marker && !mount_marker_seen_)
            co_return runtime::failure(path_error(errc::wrong_context));
        report_.bootstrap_compatible
          = zero_prefix_ && !report_.has_payload
            && (identity_witness_ || !storage_entries_);
        if (!storage_entries_ && !report_.identity_present)
            report_.state = local_store_state::pristine;
        else if (
          !report_.identity_present || missing_
          || shards_seen_.count() != spec_.identity.shard_count
          || (spec_.controls() && report_.controls != spec_.identity.shard_count)
          || (spec_.controls() && options_.require_wal_head && !report_.all_heads_present))
            report_.state = local_store_state::incomplete;
        else
            report_.state = local_store_state::existing;
        co_return report_;
    }

private:
    seastar::future<runtime::result<void>> push(
      runtime::file_path path,
      node kind,
      const frame* parent,
      std::optional<std::uint32_t> shard = {},
      std::optional<std::uint8_t> bucket = {},
      std::optional<local_segment_name> segment = {}) {
        if (depth_ == frames_.size())
            co_return runtime::failure(path_error(errc::resource_exhausted));
        auto checked = co_await inspect_local_path(
          files_, spec_.root, path, runtime::file_kind::directory, work_);
        if (!checked) co_return checked;
        auto opened = co_await files_.open_directory(
          path, runtime::file_close_policy::checked);
        if (!opened) co_return runtime::failure(opened.error());
        auto& next = frames_[depth_++];
        next.path = std::move(path);
        next.cursor.emplace(std::move(*opened));
        next.page.reset();
        next.index = 0;
        next.end = false;
        next.kind = kind;
        next.seen = 0;
        next.shard = shard.value_or(parent ? parent->shard : 0);
        next.bucket = bucket.value_or(parent ? parent->bucket : 0);
        next.segment = segment  ? segment
                       : parent ? parent->segment
                                : std::nullopt;
        next.head = parent ? parent->head : std::nullopt;
        next.wal_high = parent ? parent->wal_high : local_wal_high{};
        if (kind == node::shard && spec_.controls()) {
            auto control_path = paths_.control(next.shard);
            if (!control_path) co_return runtime::failure(control_path.error());
            auto loaded = co_await load_local_control(
              files_,
              spec_,
              next.shard,
              *control_path,
              budget_,
              limits_,
              work_);
            if (!loaded) {
                if (loaded.error().code() != errc::not_found)
                    co_return runtime::failure(loaded.error());
                missing_ = true;
                report_.all_heads_present = false;
            } else {
                ++report_.controls;
                const auto& value = std::get<local_shard_control>(
                  loaded->value.payload());
                zero_prefix_ = zero_prefix_ && initial_control(*loaded);
                report_.has_payload = report_.has_payload
                                      || !initial_control(*loaded);
                next.head = value.wal_head;
                next.wal_high = value.wal_high;
                if (!value.wal_head) {
                    report_.all_heads_present = false;
                    if (value.checkpoint) missing_ = true;
                }
                if (value.wal_head) {
                    auto head = co_await load_local_wal_head(
                      files_,
                      spec_,
                      next.shard,
                      *value.wal_head,
                      budget_,
                      limits_,
                      work_);
                    if (!head) {
                        report_.all_heads_present = false;
                        if (head.error().code() == errc::not_found)
                            missing_ = true;
                        else
                            co_return runtime::failure(head.error());
                    }
                }
                if (value.checkpoint) {
                    auto checkpoint = co_await load_local_checkpoint_root(
                      files_,
                      spec_,
                      next.shard,
                      *value.checkpoint,
                      budget_,
                      limits_,
                      work_);
                    if (!checkpoint) {
                        if (checkpoint.error().code() == errc::not_found)
                            missing_ = true;
                        else
                            co_return runtime::failure(checkpoint.error());
                    }
                }
            }
        }
        co_return runtime::result<void>{};
    }
    void finish(const frame& frame) {
        if (frame.kind == node::root) {
            if (!(frame.seen & 2U)) missing_ = true;
        } else if (frame.kind == node::shard) {
            const std::uint16_t needed = static_cast<std::uint16_t>(
              (spec_.controls() ? 1U | 2U | 8U | 16U | 32U : 0U)
              | (spec_.stores_data() ? 4U : 0U));
            if ((frame.seen & needed) != needed) missing_ = true;
        } else if (frame.kind == node::checkpoints && !(frame.seen & 1U))
            missing_ = true;
        else if (frame.kind == node::segment && (frame.seen & 7U) != 7U)
            missing_ = true;
    }
    seastar::future<runtime::result<void>>
    entry_at(frame& frame, const runtime::directory_entry& entry) {
        const auto& name = entry.name.value();
        auto path = local_child_path(*frame.path, entry.name);
        if (!path) co_return runtime::failure(path.error());
        auto descend = [&](
                         node kind,
                         std::optional<std::uint32_t> shard = {},
                         std::optional<std::uint8_t> bucket = {},
                         std::optional<local_segment_name> segment = {}) {
            if (entry.kind != runtime::file_kind::directory)
                return seastar::make_ready_future<runtime::result<void>>(
                  runtime::failure(path_error(errc::wrong_context)));
            return push(*path, kind, &frame, shard, bucket, segment);
        };
        const auto regular = [&]() -> runtime::result<void> {
            if (entry.kind != runtime::file_kind::regular)
                return runtime::failure(path_error(errc::wrong_context));
            return {};
        };
        const auto unsupported = [] {
            return runtime::failure(path_error(errc::unsupported_format));
        };
        switch (frame.kind) {
        case node::root:
            if (name == "kwaque.pid" && spec_.allow_pid_file)
                co_return regular();
            if (spec_.mount_marker && name == spec_.mount_marker->value()) {
                mount_marker_seen_ = true;
                co_return regular();
            }
            storage_entries_ = true;
            if (name == "store.meta") {
                frame.seen |= 1U;
                co_return regular();
            }
            if (name == "shards") {
                frame.seen |= 2U;
                co_return co_await descend(node::shards);
            }
            if (
              auto temp = parse_local_temporary_name(
                name, runtime::file_name::make("store.meta").value());
              temp && temp->generation.value() == 1) {
                if (auto valid = regular(); !valid) co_return valid;
                auto loaded = co_await load_local_identity(
                  files_, spec_, *path, budget_, limits_, work_);
                if (!loaded) co_return runtime::failure(loaded.error());
                if (
                  std::get<local_store_identity>(loaded->value.payload())
                  != spec_.identity)
                    co_return runtime::failure(path_error(errc::wrong_context));
                identity_witness_ = true;
                co_return runtime::result<void>{};
            }
            co_return unsupported();
        case node::shards: {
            auto shard = parse_local_shard_name(name);
            if (!shard || *shard >= spec_.identity.shard_count)
                co_return unsupported();
            if (shards_seen_.test(*shard))
                co_return runtime::failure(path_error(errc::malformed_data));
            shards_seen_.set(*shard);
            co_return co_await descend(node::shard, *shard);
        }
        case node::shard:
            if (spec_.controls()) {
                if (name == "control") {
                    frame.seen |= 1U;
                    co_return regular();
                }
                if (name == "wal") {
                    frame.seen |= 2U;
                    co_return co_await descend(node::wal);
                }
                if (name == "checkpoints") {
                    frame.seen |= 8U;
                    co_return co_await descend(node::checkpoints);
                }
                if (name == "decisions") {
                    frame.seen |= 16U;
                    co_return co_await descend(node::decisions);
                }
                if (name == "deletions") {
                    frame.seen |= 32U;
                    co_return co_await descend(node::deletions);
                }
                if (
                  auto temp = parse_local_temporary_name(
                    name, runtime::file_name::make("control").value());
                  temp) {
                    if (auto valid = regular(); !valid) co_return valid;
                    if (temp->generation.value() != 1) {
                        report_.has_payload = true;
                        zero_prefix_ = false;
                        co_return runtime::result<void>{};
                    }
                    auto loaded = co_await load_local_control(
                      files_,
                      spec_,
                      frame.shard,
                      *path,
                      budget_,
                      limits_,
                      work_,
                      temp->generation);
                    if (!loaded) co_return runtime::failure(loaded.error());
                    const bool initial = initial_control(*loaded);
                    zero_prefix_ = zero_prefix_ && initial;
                    report_.has_payload = report_.has_payload || !initial;
                    co_return runtime::result<void>{};
                }
            }
            if (spec_.stores_data() && name == "segments") {
                frame.seen |= 4U;
                co_return co_await descend(node::segments);
            }
            co_return unsupported();
        case node::wal:
        case node::segments: {
            auto bucket = parse_local_bucket(name);
            if (!bucket) co_return unsupported();
            co_return co_await descend(
              frame.kind == node::wal ? node::wal_bucket : node::segment_bucket,
              {},
              *bucket);
        }
        case node::segment_bucket: {
            auto segment = parse_local_segment_name(name, frame.bucket);
            if (!segment) co_return unsupported();
            report_.has_payload = true;
            co_return co_await descend(node::segment, {}, {}, *segment);
        }
        case node::wal_bucket: {
            if (auto valid = regular(); !valid) co_return valid;
            report_.has_payload = true;
            auto id = parse_local_wal_name(name, frame.bucket);
            if (!id && name.size() > 24) {
                auto target = runtime::file_name::make(
                  std::string_view{name}.substr(0, name.size() - 24));
                if (
                  target && parse_local_temporary_name(name, *target)
                  && parse_local_wal_name(target->value(), frame.bucket))
                    co_return runtime::result<void>{};
            }
            if (!id) co_return unsupported();
            if (local_wal_high::from_incarnation(*id).value() > frame.wal_high)
                co_return runtime::failure(path_error(errc::wrong_context));
            if (frame.head && frame.head->incarnation == *id)
                co_return runtime::result<void>{};
            auto loaded_file = co_await read_local_metadata_file(
              files_,
              spec_.root,
              *path,
              budget_,
              limits_,
              work_,
              local_metadata_extent::first_envelope);
            if (!loaded_file) co_return runtime::failure(loaded_file.error());
            const auto size = loaded_file->bytes.size();
            bytes::fragmented_buffer_parser input{
              std::move(loaded_file->bytes)};
            auto memory = metadata_file_budget(input, limits_, work_);
            if (!memory)
                co_return runtime::failure(path_error(memory.error().code()));
            auto claims = co_await probe_local_metadata(
              input,
              spec_.identity.metadata_alignment,
              *memory,
              work_,
              {},
              codec::input_boundary::complete);
            if (!claims)
                co_return runtime::failure(path_error(claims.error().code()));
            if (
              claims->header.owner() != spec_.shard_owner(frame.shard).value()
              || claims->header.kind() != local_metadata_kind::wal_descriptor
              || claims->wal_incarnation != *id)
                co_return runtime::failure(path_error(errc::wrong_context));
            if (!frame.head && loaded_file->file_bytes != size.value())
                missing_ = true;
            co_return runtime::result<void>{};
        }
        case node::segment:
            if (name == "descriptor") {
                frame.seen |= 1U;
                co_return regular();
            }
            if (name == "data") {
                frame.seen |= 2U;
                co_return regular();
            }
            if (name == "published") {
                frame.seen |= 4U;
                co_return regular();
            }
            if (name == "objects") co_return co_await descend(node::objects);
            for (auto target : {"descriptor", "published"}) {
                if (
                  parse_local_temporary_name(
                    name, runtime::file_name::make(target).value()))
                    co_return regular();
            }
            co_return unsupported();
        case node::checkpoints:
            if (name == "evidence") {
                frame.seen |= 1U;
                co_return co_await descend(node::evidence);
            }
            [[fallthrough]];
        case node::objects:
        case node::evidence:
        case node::decisions:
        case node::deletions: {
            if (auto valid = regular(); !valid) co_return valid;
            report_.has_payload = true;
            const bool metadata = frame.kind == node::evidence
                                  || frame.kind == node::decisions
                                  || frame.kind == node::deletions;
            if (parse_local_sequence_name(name, metadata))
                co_return runtime::result<void>{};
            if (name.size() > 24) {
                auto target = runtime::file_name::make(
                  std::string_view{name}.substr(0, name.size() - 24));
                if (
                  target && parse_local_temporary_name(name, *target)
                  && parse_local_sequence_name(target->value(), metadata))
                    co_return runtime::result<void>{};
            }
            co_return unsupported();
        }
        }
        co_return unsupported();
    }
    Backend& files_;
    const local_device_spec& spec_;
    workload_budget& budget_;
    local_store_io_limits limits_;
    codec::cooperative_work& work_;
    local_store_open_options options_;
    local_paths paths_;
    std::array<frame, 8> frames_;
    std::size_t depth_{0};
    std::bitset<maximum_local_shards> shards_seen_;
    local_store_report report_;
    bool storage_entries_{false}, identity_witness_{false}, zero_prefix_{true},
      missing_{false}, mount_marker_seen_{false};
};
} // namespace detail

// Quiescent-startup namespace/selected-control classification; the external
// owner excludes concurrent namespace mutation until this operation joins.
// Existing is not a
// recovered-store or checkpoint-proof capability; those owners must still
// resolve data/catalog/dependency histories before admitting append/read work.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<local_store_report>> inspect_local_store(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  local_store_open_options options,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    if (auto valid = validate_local_device_spec(spec); !valid)
        co_return runtime::failure(valid.error());
    if (auto valid = limits.validate(); !valid)
        co_return runtime::failure(valid.error());
    if (
      options.intent != local_store_intent::create_or_resume
      && options.intent != local_store_intent::must_exist)
        co_return runtime::failure(detail::path_error(errc::invalid_argument));
    auto ownership = co_await owner.validate(spec);
    if (!ownership) co_return runtime::failure(ownership.error());
    detail::local_namespace_scan<Backend> scan{
      files, spec, budget, limits, work, options};
    co_return co_await scan.inspect();
}
} // namespace kwaque::storage
