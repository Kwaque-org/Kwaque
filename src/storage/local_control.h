#pragma once

#include "src/storage/local_store_bootstrap.h"

#include <memory>
#include <type_traits>

namespace kwaque::storage {
template<runtime::file_system_backend Backend, local_directory_owner Owner>
class local_id_allocator;
struct local_control_snapshot final {
    local_publication_generation generation;
    local_shard_control fields;
};
namespace detail {
inline runtime::result<void> control_progress(
  const local_shard_control& before, const local_shard_control& after) {
    const auto wrong = [] {
        return runtime::failure(path_error(errc::wrong_context));
    };
    if (
      after.wal_high < before.wal_high || after.object_high < before.object_high
      || after.decision_high < before.decision_high
      || after.deletion_high < before.deletion_high)
        return wrong();
    if (before.wal_head) {
        if (!after.wal_head
            || after.wal_head->incarnation.canonical_less(before.wal_head->incarnation)
            || (after.wal_head->incarnation == before.wal_head->incarnation
                && after.wal_head->header_digest != before.wal_head->header_digest))
            return wrong();
    }
    if (after.wal_head) {
        auto high = local_wal_high::from_incarnation(
          after.wal_head->incarnation);
        if (!high || *high > after.wal_high) return wrong();
    }
    if (before.checkpoint && after.checkpoint != before.checkpoint) {
        if (
          !after.checkpoint
          || after.checkpoint->sequence() <= before.checkpoint->sequence())
            return wrong();
    }
    if (after.checkpoint && (!after.wal_head || after.checkpoint->kind()!=local_root_kind::checkpoint
        || after.checkpoint->position().value()!=0 || after.checkpoint->sequence().value()>after.object_high.value()))
        return wrong();
    return {};
}
struct unchanged_control_dependencies final {
    seastar::future<runtime::result<void>> operator()(
      const local_control_snapshot& before,
      const local_shard_control& after,
      codec::cooperative_work&) const {
        if (
          before.fields.wal_head != after.wal_head
          || before.fields.checkpoint != after.checkpoint)
            return seastar::make_ready_future<runtime::result<void>>(
              runtime::failure(path_error(errc::invalid_argument)));
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::result<void>{});
    }
};
} // namespace detail

// Exactly one instance per selected mutable control target, after successful
// complete-device initialization. The external directory owner and metadata
// workload outlive this instance and all returned outcomes. Join it in the
// enclosing operation_scope, then destroy it and its outcomes before stopping
// either provider. Closing drains I/O; retained state stays admitted until
// freed.
template<runtime::file_system_backend Backend, local_directory_owner Owner>
class local_control_owner final : public runtime::shard_affine {
public:
    [[nodiscard]] static seastar::future<
      runtime::result<std::unique_ptr<local_control_owner>>>
    open(
      Backend& files,
      Owner& owner,
      local_device_spec spec,
      std::uint32_t shard,
      bool require_head,
      workload_budget& budget,
      local_store_io_limits limits,
      codec::cooperative_work& work) {
        if (auto valid = validate_local_device_spec(spec); !valid)
            co_return runtime::failure(valid.error());
        if (auto valid = limits.validate(); !valid)
            co_return runtime::failure(valid.error());
        if (!spec.controls() || shard >= spec.identity.shard_count)
            co_return runtime::failure(
              detail::path_error(errc::invalid_argument));
        auto held = budget.try_reserve(byte_count{32768});
        if (!held) co_return runtime::failure(held.error());
        auto ownership = co_await owner.validate(spec);
        if (!ownership) co_return runtime::failure(ownership.error());
        auto paths = local_paths::make(spec.root).value();
        const auto marker = paths.store();
        const auto path = paths.control(shard);
        if (!marker) co_return runtime::failure(marker.error());
        if (!path) co_return runtime::failure(path.error());
        {
            auto identity = co_await load_local_identity(
              files, spec, *marker, budget, limits, work);
            if (!identity) co_return runtime::failure(identity.error());
            if (
              std::get<local_store_identity>(identity->value.payload())
              != spec.identity)
                co_return runtime::failure(
                  detail::path_error(errc::wrong_context));
        }
        auto loaded = co_await load_local_control(
          files, spec, shard, *path, budget, limits, work);
        if (!loaded) co_return runtime::failure(loaded.error());
        const auto fields = std::get<local_shard_control>(
          loaded->value.payload());
        if (
          (!fields.wal_head && require_head)
          || (!fields.wal_head && fields.checkpoint))
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        if (fields.wal_head) {
            auto head = co_await load_local_wal_head(
              files, spec, shard, *fields.wal_head, budget, limits, work);
            if (!head) co_return runtime::failure(head.error());
        }
        if (fields.checkpoint) {
            auto root = co_await load_local_checkpoint_root(
              files, spec, shard, *fields.checkpoint, budget, limits, work);
            if (!root) co_return runtime::failure(root.error());
        }
        // Reconcile a selected, validated record using fresh joined handles.
        // Generation/counters are preserved; this does not activate append or
        // claim checkpoint proof discharge from an existing file alone.
        auto confirmed = co_await detail::confirm_local_metadata(
          files,
          spec,
          *path,
          loaded->value.encoded_bytes(),
          budget,
          limits,
          work);
        if (confirmed.failure.failed()) {
            auto failure = confirmed.failure.outcome();
            co_return runtime::failure(failure.error());
        }
        auto parent = runtime::file_path::make(
                        path->value().substr(0, path->value().rfind('/')))
                        .value();
        co_return std::unique_ptr<local_control_owner>{new local_control_owner(
          files,
          owner,
          std::move(spec),
          shard,
          budget,
          limits,
          {loaded->value.header().generation(), fields},
          std::move(*held),
          std::move(parent))};
    }
    local_control_owner(const local_control_owner&) = delete;
    local_control_owner& operator=(const local_control_owner&) = delete;
    ~local_control_owner() {
        assert_current();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-CONTROL-DRAINED"},
          closed_ && !id_allocator_active_,
          "control owner destroyed before joined close and allocator release");
    }
    [[nodiscard]] runtime::result<local_control_snapshot> snapshot() const {
        assert_current();
        if (closing_ || closed_ || fenced_)
            return runtime::failure(detail::path_error(errc::closed));
        return current_;
    }
    // The edit is synchronous and changes only a candidate copy. It runs after
    // nonwaiting admission, under serialization through directory sync and
    // in-memory installation. Caller-owned captures/pins are already admitted.
    template<typename Edit>
    requires std::same_as<
               std::invoke_result_t<Edit&, local_shard_control&>,
               runtime::result<void>>
             && std::is_nothrow_move_constructible_v<Edit>
    [[nodiscard]] seastar::future<local_publication_outcome>
    update(Edit edit, codec::cooperative_work& work) {
        return update(
          std::move(edit), detail::unchanged_control_dependencies{}, work);
    }
    // New head/checkpoint references require an explicit readiness validator.
    // Its owning capture retains old/new dependency pins through publication;
    // success asserts independent durable installation/full proof obligations
    // owned by the WAL/checkpoint producer. Read-only header/root checks below
    // corroborate identity and extent; they cannot manufacture that authority.
    template<typename Edit, typename Dependencies>
    requires std::same_as<
               std::invoke_result_t<Edit&, local_shard_control&>,
               runtime::result<void>>
             && std::same_as<
               std::invoke_result_t<
                 Dependencies&,
                 const local_control_snapshot&,
                 const local_shard_control&,
                 codec::cooperative_work&>,
               seastar::future<runtime::result<void>>>
             && std::is_nothrow_move_constructible_v<Edit>
             && std::is_nothrow_move_constructible_v<Dependencies>
    [[nodiscard]] seastar::future<local_publication_outcome> update(
      Edit edit, Dependencies dependencies, codec::cooperative_work& work) {
        static_assert(sizeof(Edit) + sizeof(Dependencies) <= 8192);
        assert_current();
        auto reject = [](runtime::operation_error error) {
            local_publication_outcome result;
            result.failure.observe(error);
            return seastar::make_ready_future<local_publication_outcome>(
              std::move(result));
        };
        if (closing_ || closed_ || fenced_)
            return reject(detail::path_error(errc::closed));
        if (busy_)
            return reject(
              runtime::make_file_error(
                errc::queue_full,
                runtime::file_failure_detail::admission_not_dispatched));
        auto reservation = budget_.try_reserve(
          byte_count{
            limits_.operation_bytes.value() + limits_.execution_bytes.value()});
        if (!reservation) return reject(reservation.error());
        return update_owned(
          std::move(edit),
          std::move(dependencies),
          work,
          std::move(*reservation),
          operations_.hold());
    }
    [[nodiscard]] seastar::future<runtime::result<void>> close() {
        assert_current();
        if (closed_) co_return runtime::result<void>{};
        if (closing_)
            co_return runtime::failure(
              runtime::make_file_error(
                errc::queue_full,
                runtime::file_failure_detail::admission_not_dispatched));
        closing_ = true;
        co_await operations_.close();
        auto result = co_await publisher_.close();
        closed_ = true;
        co_return result;
    }
    [[nodiscard]] bool fenced() const noexcept {
        assert_current();
        return fenced_;
    }

private:
    friend class local_id_allocator<Backend, Owner>;
    local_control_owner(
      Backend& files,
      Owner& owner,
      local_device_spec spec,
      std::uint32_t shard,
      workload_budget& budget,
      local_store_io_limits limits,
      local_control_snapshot current,
      workload_reservation held,
      runtime::file_path parent)
      : state_reservation_(std::move(held))
      , files_(files)
      , owner_(owner)
      , spec_(std::move(spec))
      , shard_(shard)
      , budget_(budget)
      , limits_(limits)
      , current_(current)
      , publisher_(
          files,
          budget,
          {spec_.shard_owner(shard).value(),
           spec_.root,
           std::move(parent),
           runtime::file_name::make("control").value(),
           runtime::file_rename_policy::replace,
           current.generation}) {}
    template<typename Edit, typename Dependencies>
    seastar::future<local_publication_outcome> update_owned(
      Edit edit,
      Dependencies dependencies,
      codec::cooperative_work& work,
      workload_reservation reservation,
      seastar::gate::holder holder) {
        busy_ = true;
        auto idle = seastar::defer([this] noexcept { busy_ = false; });
        static_cast<void>(holder);
        local_publication_outcome output{std::move(reservation)};
        try {
            do {
                auto generation = current_.generation.checked_successor();
                if (!generation) {
                    output.failure.observe(
                      detail::path_error(errc::out_of_range));
                    break;
                }
                auto checked = co_await owner_.validate(spec_);
                if (!checked) {
                    output.failure.observe(checked);
                    break;
                }
                auto candidate = current_.fields;
                checked = edit(candidate);
                if (!checked) {
                    output.failure.observe(checked);
                    break;
                }
                checked = detail::control_progress(current_.fields, candidate);
                if (!checked) {
                    output.failure.observe(checked);
                    break;
                }
                checked = co_await dependencies(current_, candidate, work);
                if (!checked) {
                    output.failure.observe(checked);
                    break;
                }
                if (
                  candidate.wal_head != current_.fields.wal_head
                  && candidate.wal_head) {
                    auto head = co_await load_local_wal_head(
                      files_,
                      spec_,
                      shard_,
                      *candidate.wal_head,
                      budget_,
                      limits_,
                      work);
                    if (!head) {
                        output.failure.observe(head);
                        break;
                    }
                }
                if (
                  candidate.checkpoint != current_.fields.checkpoint
                  && candidate.checkpoint) {
                    auto root = co_await load_local_checkpoint_root(
                      files_,
                      spec_,
                      shard_,
                      *candidate.checkpoint,
                      budget_,
                      limits_,
                      work);
                    if (!root) {
                        output.failure.observe(root);
                        break;
                    }
                }
                const auto header = local_metadata_header::make(
                                      local_metadata_kind::shard_control,
                                      spec_.shard_owner(shard_).value(),
                                      *generation)
                                      .value();
                const local_metadata_payload payload{candidate};
                auto encoded = co_await encode_local_metadata(
                  {header, spec_.identity.metadata_alignment},
                  payload,
                  work,
                  limits_.operation_bytes,
                  limits_.charge);
                if (!encoded) {
                    output.failure.observe(
                      detail::path_error(encoded.error().code()));
                    break;
                }
                checked = co_await owner_.validate(spec_);
                if (!checked) {
                    output.failure.observe(checked);
                    break;
                }
                output = co_await publisher_.publish(
                  {header.owner(), *generation, current_.generation},
                  std::move(encoded->bytes),
                  work);
                if (
                  output.disposition == local_publication_disposition::durable)
                    current_ = {*generation, std::move(candidate)};
                if (
                  publisher_.fenced()
                  || output.disposition
                       == local_publication_disposition::uncertain)
                    fenced_ = true;
            } while (false);
        } catch (...) {
            output.failure.observe(std::current_exception());
            if (publisher_.fenced()) fenced_ = true;
        }
        co_return output;
    }
    // Declared first: release accounting after retained paths/state are freed.
    workload_reservation state_reservation_;
    Backend& files_;
    Owner& owner_;
    local_device_spec spec_;
    std::uint32_t shard_;
    workload_budget& budget_;
    local_store_io_limits limits_;
    local_control_snapshot current_;
    local_file_publisher<Backend> publisher_;
    seastar::gate operations_;
    bool id_allocator_active_{false};
    bool busy_{false}, closing_{false}, closed_{false}, fenced_{false};
};
} // namespace kwaque::storage
