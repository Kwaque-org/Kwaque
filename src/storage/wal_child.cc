#include "src/storage/wal_child.h"

#include "src/codec/transaction.h"
#include "src/runtime/file.h"
#include "src/runtime/first_failure.h"

#include <seastar/core/coroutine.hh>

namespace kwaque::storage {
namespace {
runtime::operation_error child_error(errc code) {
    return runtime::operation_error{code, runtime::operation_kind::file};
}
runtime::operation_error child_error(std::error_code code) {
    return child_error(
      codec::detail::allocation_cost_error(code, {}, 0).code());
}
} // namespace

runtime::result<void> wal_child_limits::validate() const noexcept {
    if (
      !charge || working_bytes.value() == 0
      || working_bytes > runtime::maximum_file_io_bytes
      || metadata_bytes.value() == 0 || metadata_bytes > working_bytes
      || execution_bytes.value() < sizeof(wal_prepared_children)
      || execution_bytes.value() > maximum_contiguous_allocation_bytes)
        return runtime::failure(child_error(errc::invalid_argument));
    return {};
}

runtime::result<admitted_wal_batch> admitted_wal_batch::make(
  encoded_assigned_batch batch,
  workload_reservation backing,
  bytes::allocation_charge_fn charge) {
    if (batch.bytes().empty())
        return runtime::failure(child_error(errc::invalid_argument));
    auto cost = batch.bytes().allocation_cost(charge);
    if (!cost) return runtime::failure(child_error(cost.error()));
    auto total = cost->backing.checked_add(cost->descriptors);
    if (total) total = total->checked_add(cost->share_controls);
    if (!total || *total > backing.retained_bytes())
        return runtime::failure(child_error(errc::resource_exhausted));
    return admitted_wal_batch{{std::move(backing), {}, std::move(batch)}};
}

seastar::future<runtime::result<wal_prepared_children>> prepare_wal_children(
  admitted_wal_batch&& source,
  wal_child_expectation expected,
  workload_budget& budget,
  wal_child_limits limits,
  codec::cooperative_work& work) {
    std::optional<admitted_wal_batch> input{std::in_place, std::move(source)};
    std::optional<encoded_assigned_batch> alias;
    std::optional<workload_reservation> alias_reservation;
    std::optional<workload_reservation> working;
    runtime::first_failure failed;
    try {
        do {
            if (auto valid = limits.validate(); !valid) {
                failed.observe(valid);
                break;
            }
            if (auto ready = work.poll(); !ready) {
                failed.observe(child_error(ready.error().code()));
                break;
            }
            auto valid = validate_wal_child_context(
              input->value_.batch.info(), expected);
            if (!valid) {
                failed.observe(child_error(valid.error().code()));
                break;
            }
            auto held = budget.try_reserve(
              byte_count{
                limits.working_bytes.value() + limits.execution_bytes.value()});
            if (!held) {
                failed.observe(held);
                break;
            }
            working.emplace(std::move(*held));
            const codec::decode_budget memory{
              limits.working_bytes, limits.metadata_bytes, limits.charge};
            valid = co_await input->value_.batch.validate(
              expected.batch, memory, work);
            if (!valid) {
                failed.observe(child_error(valid.error().code()));
                break;
            }
            const auto& bytes = input->value_.batch.bytes();
            auto cost = bytes.slice_allocation_cost(
              {}, bytes.size(), limits.charge);
            if (!cost) {
                failed.observe(child_error(cost.error()));
                break;
            }
            // Reserve the alias independently before constructing it. The
            // original common reservation continues covering backing/controls.
            auto state = budget.allocation_charge(
              byte_count{sizeof(wal_prepared_children) + 64});
            if (!state) {
                failed.observe(state);
                break;
            }
            auto total = cost->descriptors.checked_add(*state);
            if (!total) {
                failed.observe(child_error(errc::out_of_range));
                break;
            }
            held = budget.try_reserve(*total);
            if (!held) {
                failed.observe(held);
                break;
            }
            alias_reservation.emplace(std::move(*held));
            auto shared = co_await input->value_.batch.share(memory, work);
            if (!shared) {
                failed.observe(child_error(shared.error().code()));
                break;
            }
            alias.emplace(std::move(*shared));
            if (auto ready = work.poll(); !ready) {
                failed.observe(child_error(ready.error().code()));
                break;
            }
        } while (false);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (failed.failed()) {
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        alias.reset();
        co_await work.drain_inline(work.byte_quantum(), work.item_quantum());
        input.reset();
        auto outcome = failed.outcome();
        co_return runtime::failure(outcome.error());
    }
    // A re-shared input can itself own an older alias charge. Keep that charge
    // with the original, and give the new alias its own descriptor reservation.
    auto preparation = alias_reservation->share();
    admitted_wal_batch second{
      {input->value_.backing.share(),
       std::move(alias_reservation),
       std::move(*alias)}};
    co_return wal_prepared_children{
      std::move(preparation),
      std::move(*input),
      std::move(second),
      std::move(expected)};
}
} // namespace kwaque::storage
