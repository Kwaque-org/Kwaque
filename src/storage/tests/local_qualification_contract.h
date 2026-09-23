#pragma once

#include "src/storage/tests/local_reader_contract.h"

#include <seastar/util/alloc_failure_injector.hh>

#include <array>
#include <cstdint>
#include <memory>
#include <new>
#include <optional>

namespace kwaque::storage::testing::qualification_contract {
using namespace reader_contract;

inline void released(const workload_budget& budget) {
    const auto state = budget.snapshot();
    require(
      state.tasks == 0 && state.bytes == 0 && state.handles == 0,
      "joined operation retained workload admission");
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<> metadata_read_limits(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation_contract::bootstrap(
      files, owner, spec, budget, work, drive);
    const auto path = take(take(local_paths::make(spec.root)).store());
    for (const auto extent :
         {local_metadata_extent::exact_file,
          local_metadata_extent::first_envelope}) {
        for (const std::uint64_t cap : {31U, 32U, 1024U}) {
            auto bounded = limits();
            bounded.operation_bytes = byte_count{cap};
            bounded.metadata_bytes = byte_count{cap};
            auto result = co_await drive.lifecycle(read_local_metadata_file(
              files, spec.root, path, budget, bounded, work, extent));
            require(
              !result && result.error().code() == errc::resource_exhausted,
              "metadata read exceeded its operation allowance");
            released(budget);
        }
    }
    {
        auto result = take(
          co_await drive.lifecycle(read_local_metadata_file(
            files, spec.root, path, budget, limits(), work)));
        require(
          result.bytes.content_equals(local_fixture::read("store")),
          "read rejection changed metadata or poisoned retry");
    }
    released(budget);
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<> publication_limits(
  Backend& files,
  Owner&,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    const auto name = take(runtime::file_name::make("bounded"));
    const auto path = take(local_child_path(spec.root, name));
    for (const std::size_t size : {65537U, 65536U}) {
        local_file_publisher<Backend> publisher{
          files,
          budget,
          {spec.owner,
           spec.root,
           spec.root,
           name,
           runtime::file_rename_policy::no_replace,
           {}}};
        runtime::first_failure failed;
        try {
            auto result = co_await drive.lifecycle(publisher.publish(
              {spec.owner, publication_contract::generation(1), {}},
              take(bytes::fragmented_buffer::copy_of(std::string(size, 'q'))),
              work));
            if (size == 65537) {
                require(
                  result.failure.error()
                    && result.failure.error()->code() == errc::invalid_argument
                    && !result.temporary_may_exist,
                  "oversize metadata reached publication");
                require(
                  !take(co_await drive.lifecycle(files.exists(path))),
                  "rejected metadata created a destination");
            } else {
                take(result.failure.outcome());
            }
        } catch (...) {
            failed.observe(std::current_exception());
        }
        failed.observe(co_await drive.lifecycle(publisher.close()));
        take(failed.outcome());
        released(budget);
    }
    require(
      (co_await read_bytes(files, path, drive)) == std::string(65536, 'q'),
      "maximum metadata publication lost bytes");

    // Existing final names and every colliding temporary remain untouched.
    const auto blocked = take(runtime::file_name::make("blocked"));
    const auto version = publication_contract::generation(1);
    for (std::uint8_t i = 0; i != 64; ++i)
        co_await write_bytes(
          files,
          take(local_child_path(
            spec.root, take(local_temporary_name(blocked, version, i)))),
          std::string(4096, static_cast<char>(i)),
          drive);
    for (const bool final_collision : {false, true}) {
        local_file_publisher<Backend> publisher{
          files,
          budget,
          {spec.owner,
           spec.root,
           spec.root,
           final_collision ? name : blocked,
           runtime::file_rename_policy::no_replace,
           {}}};
        runtime::first_failure failed;
        try {
            auto result = co_await drive.lifecycle(publisher.publish(
              {spec.owner, version, {}},
              publication_contract::payload('x'),
              work));
            require(
              result.failure.error()
                && result.failure.error()->code()
                     == (final_collision ? errc::already_exists : errc::resource_exhausted)
                && result.disposition
                     == local_publication_disposition::untouched,
              "collision changed immutable publication");
        } catch (...) {
            failed.observe(std::current_exception());
        }
        failed.observe(co_await drive.lifecycle(publisher.close()));
        take(failed.outcome());
        released(budget);
    }
    require(
      !take(
        co_await drive.lifecycle(
          files.exists(take(local_child_path(spec.root, blocked))))),
      "temporary retry limit fabricated a final name");
    for (std::uint8_t i = 0; i != 64; ++i)
        require(
          (co_await read_bytes(
            files,
            take(local_child_path(
              spec.root, take(local_temporary_name(blocked, version, i)))),
            drive))
            == std::string(4096, static_cast<char>(i)),
          "temporary collision was truncated or removed");
    require(
      (co_await read_bytes(files, path, drive)) == std::string(65536, 'q'),
      "no-replace changed existing bytes");
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<> root_pressure_and_cancellation(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await seed_checkpoint(files, owner, spec, budget, work, drive);
    const auto reference = installation_contract::checkpoint_reference();
    const auto expected = installation_contract::checkpoint_expectation(spec);
    for (unsigned kind = 0; kind != 3; ++kind) {
        std::array<std::optional<workload_reservation>, 32> held;
        if (kind == 0) {
            unsigned count = 0;
            for (; count != held.size(); ++count) {
                auto next = budget.try_reserve(byte_count{1});
                if (!next) break;
                held[count].emplace(std::move(*next));
            }
            require(count != held.size(), "test task bound too small");
        } else {
            held[0].emplace(take(budget.try_reserve(
              byte_count{kind == 1 ? 8U * 1024U * 1024U - 32768U : 1U})));
            if (kind == 2) take(held[0]->try_acquire_handles(32));
        }
        const auto before = budget.snapshot();
        auto rejected = co_await drive.lifecycle(
          local_root_owner::open(
            files,
            owner,
            spec,
            0,
            reference,
            expected,
            budget,
            limits(),
            work));
        if (rejected) {
            take(co_await drive.lifecycle((*rejected)->close()));
            rejected->reset();
        }
        require(
          !rejected && rejected.error().code() == errc::queue_full,
          "root construction bypassed task/byte/handle pressure");
        const auto after = budget.snapshot();
        require(
          before.tasks == after.tasks && before.bytes == after.bytes
            && before.handles == after.handles,
          "failed root construction leaked a partial reservation");
    }
    released(budget);
    for (const unsigned count : {0U, 4097U}) {
        auto rejected = co_await drive.lifecycle(
          local_root_owner::open(
            files,
            owner,
            spec,
            0,
            reference,
            expected,
            budget,
            limits(),
            work,
            {.maximum_pins = count}));
        if (rejected) {
            take(co_await drive.lifecycle((*rejected)->close()));
            rejected->reset();
        }
        require(
          !rejected && rejected.error().code() == errc::invalid_argument,
          "invalid root pin limit accepted");
    }
    auto root = take(
      co_await drive.lifecycle(
        local_root_owner::open(
          files,
          owner,
          spec,
          0,
          reference,
          expected,
          budget,
          limits(),
          work,
          {.maximum_pins = 4096})));
    runtime::first_failure failed;
    try {
        auto pin = take(root->pin());
        const auto before = budget.snapshot();
        abort.request_abort();
        auto rejected = co_await drive.lifecycle(
          pin.read(page_ordinal::make(0).value(), work));
        require(
          !rejected && rejected.error().code() == errc::aborted,
          "cancelled root read produced a page");
        const auto after = budget.snapshot();
        require(
          before.tasks == after.tasks && before.bytes == after.bytes
            && before.handles == after.handles,
          "cancelled read retained descendant ownership");
        seastar::abort_source retry_abort;
        codec::cooperative_work retry{codec::limits::defaults(), retry_abort};
        auto page = take(
          co_await drive.lifecycle(
            pin.read(page_ordinal::make(0).value(), retry)));
        require(
          page.bytes.content_equals(local_fixture::read("checkpoint_page")),
          "one cancelled reader poisoned the root");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    failed.observe(co_await drive.lifecycle(root->close()));
    root.reset();
    take(failed.outcome());
    released(budget);
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<> failed_generation_open(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await seed_segment(files, owner, spec, budget, work, drive);
    const auto paths = take(local_paths::make(spec.root));
    const local_segment_name name{segment().segment(), segment().generation()};
    const auto published = take(
      paths.segment_file(0, name, local_segment_file::published));
    co_await write_bytes(
      files, published, local_fixture::read("publication_all_roots"), drive);
    const auto expected_contexts = contexts(spec);
    const local_generation_expectation expected{
      publication_contract::generation(3),
      sealed_publication(),
      descriptor(),
      expected_contexts};
    const std::array victims{
      take(paths.segment_file(0, name, local_segment_file::data)),
      take(paths.object(0, name, sealed_ref().sequence())),
      take(paths.object(0, name, snapshot_ref().sequence()))};
    for (const auto& victim : victims) {
        const auto saved = take(
          runtime::file_path::make(victim.value() + ".saved"));
        take(
          co_await drive.lifecycle(files.rename(
            victim, saved, runtime::file_rename_policy::no_replace)));
        auto rejected = co_await drive.lifecycle(
          local_generation_owner::open(
            files, owner, spec, 0, expected, budget, limits(), work));
        if (rejected) {
            take(co_await drive.lifecycle((*rejected)->close()));
            rejected->reset();
        }
        take(
          co_await drive.lifecycle(files.rename(
            saved, victim, runtime::file_rename_policy::no_replace)));
        require(
          !rejected && rejected.error().code() == errc::not_found,
          "partial generation open ignored a required dependency");
        released(budget);
        // A clean retry must work after every partial acquisition boundary.
        auto opened = take(
          co_await drive.lifecycle(
            local_generation_owner::open(
              files, owner, spec, 0, expected, budget, limits(), work)));
        take(co_await drive.lifecycle(opened->close()));
        opened.reset();
        released(budget);
    }
    require(
      (co_await read_bytes(files, published, drive))
        == local_fixture::read("publication_all_roots"),
      "failed open rewrote the selected publication");
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<> failed_replacement_keeps_old_pin(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await seed_segment(files, owner, spec, budget, work, drive);
    const local_generation_expectation expected{
      publication_contract::generation(1),
      active_publication(),
      descriptor(),
      {}};
    auto old = take(
      co_await drive.lifecycle(
        local_generation_owner::open(
          files, owner, spec, 0, expected, budget, limits(), work)));
    std::optional<local_generation_pin> pin;
    runtime::first_failure failed;
    try {
        pin.emplace(take(old->pin()));
        const auto parent = take(
          take(local_paths::make(spec.root))
            .segment(0, {segment().segment(), segment().generation()}));
        local_file_publisher<Backend> publisher{
          files,
          budget,
          {take(spec.shard_owner(0)),
           spec.root,
           parent,
           take(runtime::file_name::make("published")),
           runtime::file_rename_policy::no_replace,
           {}}};
        try {
            auto result = co_await drive.lifecycle(publisher.publish(
              {take(spec.shard_owner(0)),
               publication_contract::generation(3),
               {}},
              co_await buffer_async(
                local_fixture::read("publication_all_roots"), 4096),
              work));
            require(
              result.failure.error()
                && result.failure.error()->code() == errc::already_exists,
              "immutable replacement collision was ignored");
            require(
              pin->generation().value() == 1
                && std::holds_alternative<durable_footer>(*pin->boundary()),
              "failed replacement changed the old pinned generation");
        } catch (...) {
            failed.observe(std::current_exception());
        }
        failed.observe(co_await drive.lifecycle(publisher.close()));
    } catch (...) {
        failed.observe(std::current_exception());
    }
    old->retire();
    auto closing = old->close();
    if (pin) {
        try {
            require(
              !closing.available(), "replacement released an old reader early");
            auto child = take(pin->share());
            require(
              child.generation().value() == 1,
              "old pin lost admitted descendants");
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    pin.reset();
    failed.observe(co_await drive.lifecycle(std::move(closing)));
    old.reset();
    take(failed.outcome());
    const auto path = take(take(local_paths::make(spec.root))
                             .segment_file(
                               0,
                               {segment().segment(), segment().generation()},
                               local_segment_file::published));
    require(
      (co_await read_bytes(files, path, drive))
        == local_fixture::read("publication_boundary"),
      "failed replacement overwrote the old publication");
    released(budget);
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<> discovery_cancel_and_unsupported(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await installation_contract::bootstrap(
      files, owner, spec, budget, work, drive);
    unsigned visited = 0;
    auto visitor = [&](const local_namespace_entry&) {
        ++visited;
        abort.request_abort();
        return seastar::make_ready_future<runtime::result<bool>>(true);
    };
    auto rejected = co_await drive.lifecycle(walk_local_namespace(
      files, owner, spec, budget, limits(), work, visitor));
    require(
      !rejected && rejected.error().code() == errc::aborted && visited == 1,
      "cancelled inventory returned a complete or empty store");
    released(budget);
    seastar::abort_source retry_abort;
    codec::cooperative_work retry{codec::limits::defaults(), retry_abort};
    const auto marker = take(take(local_paths::make(spec.root)).store());
    const auto future = local_fixture::read("future_minimum");
    co_await write_bytes(files, marker, future, drive);
    const std::array specs{spec};
    {
        auto refused = co_await drive.lifecycle(initialize_local_stores(
          files,
          owner,
          specs,
          {local_store_intent::create_or_resume, false},
          budget,
          limits(),
          retry));
        require(
          refused.failure.failed() && !refused.mutation_attempted
            && refused.reports[0].state == local_store_state::unsupported,
          "unsupported store was initialized");
    }
    require(
      (co_await read_bytes(files, marker, drive)) == future,
      "unsupported marker was modified");
    released(budget);
}

template<typename Backend, typename Owner, typename Driver>
seastar::future<> allocation_rollback(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    co_await seed_checkpoint(files, owner, spec, budget, work, drive);
    const auto reference = installation_contract::checkpoint_reference();
    const auto expected = installation_contract::checkpoint_expectation(spec);
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
    auto& injector = seastar::memory::local_failure_injector();
    bool completed = false;
    unsigned failures = 0;
    // Inject only in synchronous admission, not in the scheduler/test driver.
    // Pending I/O is always joined after injection is disabled.
    for (unsigned allocation = 0; allocation != 128 && !completed;
         ++allocation) {
        std::optional<
          seastar::future<runtime::result<std::unique_ptr<local_root_owner>>>>
          pending;
        std::exception_ptr exception;
        injector.fail_after(allocation);
        try {
            pending.emplace(
              local_root_owner::open(
                files,
                owner,
                spec,
                0,
                reference,
                expected,
                budget,
                limits(),
                work));
        } catch (...) {
            exception = std::current_exception();
        }
        const bool injected = injector.failed();
        injector.cancel();
        try {
            if (pending) {
                auto result = co_await drive.lifecycle(std::move(*pending));
                if (result) {
                    take(co_await drive.lifecycle((*result)->close()));
                    result->reset();
                } else {
                    require(injected, "unfaulted root admission failed");
                }
            }
        } catch (...) {
            exception = std::current_exception();
        }
        if (exception) {
            require(injected, "unrelated admission exception");
            try {
                std::rethrow_exception(exception);
            } catch (const std::bad_alloc&) {
            }
        }
        failures += injected;
        completed = !injected;
        released(budget);
    }
    require(
      completed && failures != 0, "allocation admission sweep incomplete");
#endif
    auto root = take(
      co_await drive.lifecycle(
        local_root_owner::open(
          files, owner, spec, 0, reference, expected, budget, limits(), work)));
    runtime::first_failure failed;
    try {
        auto pin = take(root->pin());
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
        bool completed = false;
        unsigned failures = 0;
        for (unsigned allocation = 0; allocation != 32 && !completed;
             ++allocation) {
            const auto before = budget.snapshot();
            std::optional<runtime::result<local_root_pin>> shared;
            std::exception_ptr exception;
            injector.fail_after(allocation);
            try {
                shared.emplace(pin.share());
            } catch (...) {
                exception = std::current_exception();
            }
            const bool injected = injector.failed();
            injector.cancel();
            if (exception) {
                require(injected, "unrelated pin exception");
                try {
                    std::rethrow_exception(exception);
                } catch (const std::bad_alloc&) {
                }
            } else {
                require(
                  shared && *shared, "pin share rejected without pressure");
            }
            shared.reset();
            const auto after = budget.snapshot();
            require(
              before.tasks == after.tasks && before.bytes == after.bytes
                && before.handles == after.handles,
              "pin allocation failure leaked its reservation");
            failures += injected;
            completed = !injected;
        }
        require(completed && failures != 0, "pin allocation sweep incomplete");
#endif
        auto page = take(
          co_await drive.lifecycle(
            pin.read(page_ordinal::make(0).value(), work)));
        require(
          page.bytes.content_equals(local_fixture::read("checkpoint_page")),
          "allocation rollback poisoned root state");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    failed.observe(co_await drive.lifecycle(root->close()));
    root.reset();
    take(failed.outcome());
    released(budget);
}
} // namespace kwaque::storage::testing::qualification_contract
