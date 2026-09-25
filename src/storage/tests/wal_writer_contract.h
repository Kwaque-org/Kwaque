#pragma once

#include "src/storage/tests/local_installation_contract.h"
#include "src/storage/tests/wal_test_support.h"
#include "src/storage/wal_writer.h"

#include <seastar/core/preempt.hh>
#include <seastar/util/alloc_failure_injector.hh>

namespace kwaque::storage::testing::wal_writer_contract {
using store_contract::require;
using store_contract::take;

inline wal_writer_config configuration() {
    wal_writer_config config{alignment(8192), byte_count{1048576}};
    config.children.working_bytes = byte_count{2U * 1024U * 1024U};
    config.children.charge = charge;
    return config;
}

inline wal_child_expectation child_context(model::cluster_id cluster) {
    auto expected = wal_expected(8192, 16384);
    const auto previous = expected.target.segment();
    expected.target = segment_write_context::make(
                        segment_context::make(
                          cluster,
                          previous.topic(),
                          previous.range(),
                          id<model::segment_id>(0x77),
                          previous.generation())
                          .value(),
                        alignment(16384),
                        {},
                        runtime::file_position{16384})
                        .value();
    return {
      expected.target,
      expected.target_data_start,
      expected.routing_epoch,
      expected.batch,
      expected.profile,
      expected.target_profile};
}

inline seastar::future<admitted_wal_batch> offer(
  workload_budget& budget, std::string wire, codec::cooperative_work& work) {
    auto raw = co_await installation_contract::buffer_async(wire, 67);
    auto held = take(budget.try_reserve_buffer(raw));
    auto working = take(budget.try_reserve(byte_count{(2U << 20U) + 65536}));
    const auto cost = raw.allocation_cost(charge).value();
    auto memory = codec::detail::consume_decode_budget(
                    work.policy(),
                    {byte_count{2U << 20U}, byte_count{65536}, charge},
                    cost.backing,
                    *cost.descriptors.checked_add(cost.share_controls),
                    {},
                    0)
                    .value();
    auto checked = co_await validate_encoded_assigned_batch(
      std::move(raw), batch_expected(), memory, work);
    require(checked.has_value(), "independent assigned bytes rejected");
    co_return take(
      admitted_wal_batch::make(std::move(*checked), std::move(held), charge));
}

// Independent offsets and CRCs; no metadata encoder chooses the expected file.
inline std::string header_bytes(
  const local_device_spec& spec,
  std::uint32_t shard,
  model::wal_incarnation_id incarnation,
  std::optional<local_wal_cursor> predecessor = std::nullopt,
  std::uint64_t capacity = 1048576) {
    const auto fixed = predecessor ? 68U : 44U;
    std::string body(72 + fixed, '\0');
    put(body, 0, 3, 2);
    auto put_id = [&](std::size_t offset, auto id) {
        for (std::size_t n = 0; n < 16; ++n)
            body[offset + n] = std::bit_cast<char>(id.bytes()[n]);
    };
    put_id(4, spec.owner.cluster());
    put_id(20, spec.owner.broker());
    put_id(36, spec.owner.device());
    put(body, 52, shard, 4);
    put(body, 56, 1, 8);
    put(body, 64, fixed, 4);
    put(body, 68, 8192 - 32 - 72 - fixed, 4);
    put_id(72, incarnation);
    put(body, 88, predecessor ? 1 : 0, 1);
    if (predecessor) {
        put_id(92, predecessor->incarnation());
        put(body, 108, predecessor->position().value(), 8);
    }
    const auto geometry = predecessor ? 116U : 92U;
    put(body, geometry, 8192, 4);
    put(body, geometry + 4, 1, 2);
    put(body, geometry + 8, 8192, 8);
    put(body, geometry + 16, capacity, 8);
    body.resize(8192 - 32, '\0');
    return frame(std::move(body), 11);
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<> exercise(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive,
  wal_writer_config config = configuration()) {
    using writer_type = wal_writer<Backend, Owner>;
    using control_type = typename writer_type::control_type;
    using allocator_type = typename writer_type::allocator_type;
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    std::unique_ptr<control_type> control;
    std::unique_ptr<allocator_type> ids;
    std::unique_ptr<writer_type> writer;
    runtime::first_failure failed;
    try {
        co_await installation_contract::bootstrap(
          files, owner, spec, budget, work, drive);
        control = take(
          co_await drive.lifecycle(
            control_type::open(
              files,
              owner,
              spec,
              0,
              false,
              budget,
              store_contract::limits(),
              work)));
        ids = take(allocator_type::make(*control, budget, 4));
        const auto check_invalid =
          [&](wal_writer_config invalid) -> seastar::future<> {
            const auto before = budget.snapshot();
            const auto selected = take(control->snapshot());
            auto made = writer_type::make(
              *control,
              *ids,
              budget,
              invalid,
              wal_start_intent::known_unactivated);
            // Join an unexpectedly admitted owner before reporting a failed
            // check.
            if (made) {
                take(co_await drive.lifecycle((*made)->close()));
                made->reset();
            }
            require(!made, "invalid WAL configuration registered an owner");
            require(
              made.error().code() == errc::invalid_argument
                || made.error().code() == errc::out_of_range,
              "invalid WAL configuration returned an unrelated error");
            const auto after = budget.snapshot();
            require(
              after.accepted == before.accepted && after.bytes == before.bytes
                && after.tasks == before.tasks
                && after.handles == before.handles
                && take(control->snapshot()).generation == selected.generation
                && take(control->snapshot()).fields == selected.fields,
              "invalid WAL configuration changed admission or persisted "
              "control");
        };
        const std::array invalid_children{
          wal_child_limits{.working_bytes = byte_count{}, .charge = charge},
          wal_child_limits{
            .working_bytes = byte_count{UINT64_MAX - 65535}, .charge = charge},
          wal_child_limits{.metadata_bytes = byte_count{}, .charge = charge},
          wal_child_limits{
            .metadata_bytes = byte_count{9U << 20U}, .charge = charge},
          wal_child_limits{.execution_bytes = byte_count{}, .charge = charge},
          wal_child_limits{
            .execution_bytes = byte_count{4095}, .charge = charge},
          wal_child_limits{
            .execution_bytes
            = byte_count{maximum_contiguous_allocation_bytes + 1},
            .charge = charge}};
        for (const auto limits : invalid_children) {
            auto invalid = configuration();
            invalid.children = limits;
            co_await check_invalid(invalid);
        }
        const std::array invalid_completion{
          completion_resource_limits{.scratch_bytes = byte_count{}},
          completion_resource_limits{
            .scratch_bytes
            = byte_count{maximum_contiguous_allocation_bytes + 1}},
          completion_resource_limits{.execution_bytes = byte_count{}},
          completion_resource_limits{
            .execution_bytes = byte_count{
              maximum_contiguous_allocation_bytes + 1}}};
        for (const auto limits : invalid_completion) {
            auto invalid = configuration();
            invalid.completion = limits;
            co_await check_invalid(invalid);
        }
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
        const auto baseline = budget.snapshot();
        for (std::size_t at = 0; at != 8; ++at) {
            std::unique_ptr<writer_type> candidate;
            auto& injector = seastar::memory::local_failure_injector();
            injector.fail_after(at);
            try {
                auto made = writer_type::make(
                  *control,
                  *ids,
                  budget,
                  configuration(),
                  wal_start_intent::known_unactivated);
                if (made) candidate = std::move(*made);
            } catch (const std::bad_alloc&) {
            }
            injector.cancel();
            if (candidate) take(co_await drive.lifecycle(candidate->close()));
            candidate.reset();
            require(
              budget.snapshot().bytes == baseline.bytes
                && budget.snapshot().tasks == baseline.tasks,
              "writer construction leaked admission");
        }
#endif
        writer = take(
          writer_type::make(
            *control,
            *ids,
            budget,
            config,
            wal_start_intent::known_unactivated));
        require(
          !writer->positions() && !writer->capture(),
          "unpublished header exposed a boundary");
        require(
          !writer_type::make(
            *control,
            *ids,
            budget,
            configuration(),
            wal_start_intent::known_unactivated),
          "duplicate writer admitted");
        require(
          !(co_await drive.lifecycle(ids->close())),
          "allocator closed under writer");
        require(
          !(co_await drive.lifecycle(control->close())),
          "control closed under writer");
        take(co_await drive.lifecycle(writer->bootstrap(work)));
        const auto positions = take(writer->positions());
        const auto cut = take(writer->capture());
        take(writer->validate_capture(cut));
        {
            std::unique_ptr<control_type> other_control;
            std::unique_ptr<allocator_type> other_ids;
            std::unique_ptr<writer_type> other_writer;
            runtime::first_failure other_failed;
            try {
                other_control = take(
                  co_await drive.lifecycle(
                    control_type::open(
                      files,
                      owner,
                      spec,
                      1,
                      false,
                      budget,
                      store_contract::limits(),
                      work)));
                other_ids = take(
                  allocator_type::make(*other_control, budget, 4));
                other_writer = take(
                  writer_type::make(
                    *other_control,
                    *other_ids,
                    budget,
                    configuration(),
                    wal_start_intent::known_unactivated));
                take(co_await drive.lifecycle(other_writer->bootstrap(work)));
                const auto foreign = take(other_writer->capture());
                require(
                  foreign.cursor() == cut.cursor(),
                  "foreign test must use equal numeric cursors");
                require(
                  !writer->validate_capture(foreign)
                    && !other_writer->validate_capture(cut),
                  "foreign owner authorized an equal numeric cut");
            } catch (...) {
                other_failed.observe(std::current_exception());
            }
            if (other_writer) {
                try {
                    other_failed.observe(
                      co_await drive.lifecycle(other_writer->close()));
                } catch (...) {
                    other_failed.observe(std::current_exception());
                }
                other_writer.reset();
            }
            if (other_ids) {
                other_failed.observe(
                  co_await drive.lifecycle(other_ids->close()));
                other_ids.reset();
            }
            if (other_control) {
                other_failed.observe(
                  co_await drive.lifecycle(other_control->close()));
                other_control.reset();
            }
            take(other_failed.outcome());
        }
        require(
          positions.reserved == positions.write_complete
            && positions.reserved == positions.durable,
          "empty published positions disagree");
        require(
          positions.reserved.position().value() == 8192,
          "WAL inherited metadata alignment");
        const auto head = *take(control->snapshot()).fields.wal_head;
        require(
          head == *writer->prepared_head(), "head did not pin prepared file");
        require(
          head.incarnation
            == local_wal_high{}.checked_advance(1)->incarnation().value(),
          "head did not use allocated incarnation");
        require(
          take(control->snapshot()).fields.wal_high
            == local_wal_high{}.checked_advance(4).value(),
          "head publication lost allocator high mark");
        auto paths = take(local_paths::make(spec.root));
        const auto path = take(paths.wal(0, head.incarnation));
        require(
          (co_await store_contract::read_bytes(files, path, drive))
            == header_bytes(spec, 0, head.incarnation),
          "new WAL differs from independent bytes or extended to capacity");
        const auto context = child_context(spec.owner.cluster());
        {
            auto child = co_await offer(budget, assigned_wire(), work);
            auto input = std::move(child).release();
            const auto cost
              = input.batch.bytes().allocation_cost(charge).value();
            const auto required = cost.backing.value()
                                  + cost.descriptors.value()
                                  + cost.share_controls.value();
            auto short_reservation = take(
              budget.try_reserve(byte_count{required - 1}));
            require(
              short_reservation.bytes().value() >= required,
              "test must distinguish payload allowance from bookkeeping");
            auto rejected = admitted_wal_batch::make(
              std::move(input.batch), std::move(short_reservation), charge);
            require(
              !rejected && rejected.error().code() == errc::resource_exhausted,
              "reservation bookkeeping was counted as payload admission");
        }
        for (const bool compressed : {false, true}) {
            const auto before = budget.snapshot();
            std::optional<admitted_wal_batch::contents> survivor;
            const auto expected_wire = assigned_wire(compressed, 48, true);
            {
                const auto wire = assigned_wire(compressed, 48, true);
                auto child = co_await offer(budget, wire, work);
                const auto info = child.batch().info();
                auto prepared = take(
                  co_await writer->prepare(std::move(child), context, work));
                require(
                  prepared.wal.batch().bytes().content_equals(wire)
                    && prepared.segment.batch().bytes().content_equals(wire),
                  "preparation rewrote exact child bytes");
                require(
                  prepared.wal.batch().info() == info
                    && prepared.segment.batch().info() == info,
                  "preparation changed original binding or fingerprint");
                require(
                  prepared.expected.target.segment().segment()
                    != info.context.submitted().binding().segment(),
                  "test must exercise independent relocated target");
                require(
                  take(writer->positions()).reserved == positions.reserved,
                  "preparation reserved WAL coordinates");
                survivor.emplace(
                  compressed ? std::move(prepared.wal).release()
                             : std::move(prepared.segment).release());
            }
            require(
              survivor->batch.bytes().content_equals(expected_wire)
                && budget.snapshot().bytes > before.bytes,
              "last alias lost exact bytes or backing admission");
            survivor.reset();
            require(
              budget.snapshot().bytes == before.bytes
                && budget.snapshot().tasks == before.tasks,
              "last alias did not release its backing and private charges");
        }
        {
            auto limits = codec::limits::defaults().config();
            limits.max_original_records = item_count{1};
            auto policy = codec::limits::make(limits).value();
            seastar::abort_source cancel;
            codec::cooperative_work narrow{policy, cancel};
            auto child = co_await offer(
              budget, assigned_wire(true, 48, true), work);
            auto rejected = co_await writer->prepare(
              std::move(child), context, narrow);
            require(!rejected, "narrower policy reused broad validation");
            require(
              take(writer->positions()).reserved == positions.reserved,
              "policy rejection changed tail");
        }
        for (unsigned mismatch = 0; mismatch != 5; ++mismatch) {
            auto wrong = context;
            if (mismatch == 0)
                wrong.routing_epoch
                  = model::range_routing_epoch::make(9).value();
            if (mismatch == 1) wrong.batch.topic = id<model::topic_id>(0x99);
            if (mismatch == 2)
                wrong.target_data_start = runtime::file_position{32768};
            if (mismatch == 3)
                wrong.target_profile = static_cast<storage_profile>(2);
            if (mismatch == 4) wrong.profile = static_cast<replay_profile>(2);
            auto child = co_await offer(budget, assigned_wire(), work);
            auto rejected = co_await writer->prepare(
              std::move(child), wrong, work);
            require(!rejected, "independent expectation ignored");
            require(
              take(writer->positions()).reserved == positions.reserved,
              "context rejection changed tail");
        }
        {
            auto child = co_await offer(budget, assigned_wire(), work);
            std::array<std::optional<workload_reservation>, 32> pressure;
            for (auto& slot : pressure) {
                auto held = budget.try_reserve(byte_count{4096});
                if (!held) break;
                slot.emplace(std::move(*held));
            }
            auto rejected = co_await writer->prepare(
              std::move(child), context, work);
            require(
              !rejected && rejected.error().code() == errc::queue_full,
              "preparation bypassed task admission");
            require(
              child.batch().bytes().empty(),
              "entered pressure rejection did not consume offer");
            require(
              take(writer->positions()).reserved == positions.reserved,
              "pressure rejection changed cursor");
        }
        {
            auto child = co_await offer(budget, assigned_wire(), work);
            seastar::abort_source canceled;
            codec::cooperative_work canceled_work{
              codec::limits::defaults(), canceled};
            canceled.request_abort();
            auto rejected = co_await writer->prepare(
              std::move(child), context, canceled_work);
            require(
              !rejected && rejected.error().code() == errc::aborted,
              "canceled preflight admitted a child");
            require(
              child.batch().bytes().empty(),
              "entered cancellation did not consume offer");
        }
        {
            // Force the native preemption seam, without another scheduler or
            // elapsed-time wait. The busy slot stays held through validation.
            auto child = co_await offer(
              budget, assigned_wire(true, 48, true), work);
            auto other = co_await offer(budget, assigned_wire(), work);
            seastar::abort_source other_abort;
            codec::cooperative_work other_work{
              codec::limits::defaults(), other_abort};
            auto pending = [&] {
                seastar::internal::preemption_monitor requested{};
                requested.head.store(1, std::memory_order_relaxed);
                const auto* previous
                  = seastar::internal::get_need_preempt_var();
                auto restore = seastar::defer([previous] noexcept {
                    seastar::internal::set_need_preempt_var(previous);
                });
                seastar::internal::set_need_preempt_var(&requested);
                return writer->prepare(std::move(child), context, work);
            }();
            const bool suspended = !pending.available();
            auto overlap = co_await writer->prepare(
              std::move(other), context, other_work);
            auto prepared = co_await std::move(pending);
            require(
              suspended && !overlap
                && overlap.error().code() == errc::queue_full,
              "overlapping preflight did not reject nonwaiting");
            require(
              prepared.has_value(),
              "pending preflight lost execution ownership");
        }
        require(
          !(co_await writer->bootstrap(work)),
          "bootstrap reused an active lifetime");
        take(co_await drive.lifecycle(writer->close()));
        take(co_await drive.lifecycle(writer->close()));
        require(
          !writer->validate_capture(cut), "closed owner accepted old capture");
        require(
          !writer_type::make(
            *control,
            *ids,
            budget,
            configuration(),
            wal_start_intent::known_unactivated),
          "existing head treated as fresh bootstrap");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (writer) {
        try {
            failed.observe(co_await drive.lifecycle(writer->close()));
        } catch (...) {
            failed.observe(std::current_exception());
        }
        writer.reset();
    }
    if (ids) {
        failed.observe(co_await drive.lifecycle(ids->close()));
        ids.reset();
    }
    if (control) {
        failed.observe(co_await drive.lifecycle(control->close()));
        control.reset();
    }
    take(failed.outcome());
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<> construction_failure_cleanup(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    auto invalid = configuration();
    invalid.maximum_descriptors = 0;
    bool rejected = false;
    try {
        co_await exercise(files, owner, spec, budget, drive, invalid);
    } catch (const std::runtime_error& error) {
        rejected = error.what()
                   == ::kwaque::storage::detail::path_error(
                        errc::invalid_argument)
                        .render();
    }
    require(rejected, "construction failure lost its original error");
    require(
      budget.snapshot().tasks == 0 && budget.snapshot().bytes == 0
        && budget.snapshot().handles == 0,
      "construction failure retained a provider or admission");
}
} // namespace kwaque::storage::testing::wal_writer_contract
