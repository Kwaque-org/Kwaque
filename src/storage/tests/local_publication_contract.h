#pragma once

#include "src/runtime/testing/contracts/file_system_contract.h"
#include "src/storage/local_publication.h"

#include <array>
#include <exception>
#include <optional>
#include <set>
#include <string>
#include <system_error>
#include <type_traits>

namespace kwaque::storage::testing {
namespace publication_contract {
using runtime::testing::file_system_contract_detail::require;
using runtime::testing::file_system_contract_detail::take;
template<typename T>
T take(kwaque::result<T> value) {
    if (!value) throw std::system_error(value.error());
    if constexpr (!std::is_void_v<T>) return std::move(*value);
}
template<typename Id>
Id identity(std::uint8_t value) {
    std::array<std::uint8_t, 16> bytes{};
    bytes.fill(value);
    return Id::make(bytes).value();
}
inline local_store_context owner() {
    return local_store_context::make(
             identity<model::cluster_id>(1),
             identity<model::broker_id>(2),
             identity<device_store_id>(3),
             0)
      .value();
}
inline local_publication_generation generation(std::uint64_t value) {
    return local_publication_generation::make(value).value();
}
inline bytes::fragmented_buffer payload(char c) {
    return bytes::fragmented_buffer::copy_of(std::string(4096, c)).value();
}
template<runtime::file_system_backend Files, typename Driver>
seastar::future<> check_contents(
  Files& files, runtime::file_path path, Driver drive, char expected) {
    auto file = take(
      co_await drive.lifecycle(files.open(
        path, {.close_policy = runtime::file_close_policy::checked})));
    runtime::first_failure first;
    try {
        auto data = take(
          co_await drive.lifecycle(
            file.read(runtime::file_position{}, byte_count{4096})));
        require(
          data.data().content_equals(std::string(4096, expected)),
          "published bytes differ");
    } catch (...) {
        first.observe(std::current_exception());
    }
    first.observe(co_await drive.lifecycle(file.close()));
    take(first.outcome());
}

template<runtime::file_system_backend Files, typename Driver>
seastar::future<> run(
  Files& files,
  workload_budget& budget,
  runtime::file_path root,
  Driver drive) {
    take(co_await drive.lifecycle(files.create_directories(root)));
    auto parent = root.value().substr(0, root.value().rfind('/'));
    take(
      co_await drive.lifecycle(files.sync_directory(
        take(runtime::file_path::make(parent.empty() ? "/" : parent)),
        runtime::file_close_policy::checked)));
    auto name = take(runtime::file_name::make("control"));
    const auto path = take(local_child_path(root, name));
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    local_file_publisher<Files> create{
      files,
      budget,
      {owner(), root, root, name, runtime::file_rename_policy::no_replace, {}}};
    runtime::first_failure failed;
    try {
        auto made = co_await drive.lifecycle(
          create.publish({owner(), generation(1), {}}, payload('a'), work));
        take(made.failure.outcome());
        require(
          budget.snapshot().tasks == 1 && budget.snapshot().handles == 2,
          "retained outcome lost its admission owner");
        require(
          made.stage == local_publication_stage::directory_synced
            && made.disposition == local_publication_disposition::durable,
          "publication did not establish namespace barrier");
        require(
          !made.temporary_may_exist,
          "durable publication retained a temp name");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(co_await drive.lifecycle(create.close()));
    take(failed.outcome());
    co_await check_contents(files, path, drive, 'a');

    auto collision_path = take(local_child_path(
      root, take(local_temporary_name(name, generation(2), 0))));
    auto collision = take(
      co_await drive.lifecycle(files.open(
        collision_path,
        {.access = runtime::file_access::read_write,
         .create = true,
         .exclusive = true,
         .close_policy = runtime::file_close_policy::checked})));
    take(co_await drive.lifecycle(collision.close()));
    local_file_publisher<Files> replace{
      files,
      budget,
      {owner(),
       root,
       root,
       name,
       runtime::file_rename_policy::replace,
       generation(1)}};
    try {
        auto stale = co_await drive.lifecycle(
          replace.publish({owner(), generation(2), {}}, payload('x'), work));
        require(
          stale.failure.error()
            && stale.failure.error()->code() == errc::wrong_context,
          "stale snapshot accepted");
        auto updated = co_await drive.lifecycle(replace.publish(
          {owner(), generation(2), generation(1)}, payload('b'), work));
        take(updated.failure.outcome());
        require(
          updated.disposition == local_publication_disposition::durable,
          "replacement lost its barrier");
        require(
          updated.temporary && updated.temporary->value().ends_with("-01"),
          "exclusive collision did not advance bounded attempt");
        require(
          take(co_await drive.lifecycle(files.exists(collision_path))),
          "collision was removed");
        abort.request_abort();
        auto canceled = co_await drive.lifecycle(replace.publish(
          {owner(), generation(3), generation(2)}, payload('c'), work));
        require(
          canceled.failure.error()
            && canceled.failure.error()->code() == errc::aborted,
          "pre-effect abort was ignored");
        require(
          canceled.disposition == local_publication_disposition::untouched,
          "pre-effect abort changed destination state");
    } catch (...) {
        failed.observe(std::current_exception());
    }
    take(co_await drive.lifecycle(replace.close()));
    take(failed.outcome());
    co_await check_contents(files, path, drive, 'b');
    require(
      budget.snapshot().tasks == 0 && budget.snapshot().handles == 0
        && budget.snapshot().bytes == 0,
      "publication leaked admission");
}

template<runtime::file_system_backend Files, typename Driver>
seastar::future<> walk(Files& files, runtime::file_path root, Driver drive) {
    auto paths = take(local_paths::make(root));
    const std::array<std::uint64_t, 3> numbers{1, 257, 0xab};
    std::set<std::string> expected, observed;
    for (auto n : numbers) {
        auto id = local_wal_high{}.checked_advance(n)->incarnation().value();
        auto path = take(paths.wal(0, id));
        auto slash = path.value().rfind('/');
        take(
          co_await drive.lifecycle(files.create_directories(
            take(runtime::file_path::make(path.value().substr(0, slash))))));
        auto file = take(
          co_await drive.lifecycle(files.open(
            path,
            {.access = runtime::file_access::read_write,
             .create = true,
             .exclusive = true,
             .close_policy = runtime::file_close_policy::checked})));
        take(co_await drive.lifecycle(file.close()));
        expected.insert(path.value());
    }
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    auto visitor = [&observed](const runtime::file_path& path) {
        require(
          observed.insert(path.value()).second, "walker repeated an entry");
        return seastar::make_ready_future<runtime::result<bool>>(true);
    };
    take(
      co_await drive.lifecycle(walk_local_buckets(
        files,
        paths.root(),
        take(paths.buckets(0, local_bucket_kind::wal)),
        local_bucket_kind::wal,
        work,
        visitor)));
    require(
      observed == expected,
      "walker lost canonical names or inferred contiguous IDs");
    auto unknown = take(local_child_path(
      take(paths.buckets(0, local_bucket_kind::wal)),
      take(runtime::file_name::make("unexpected"))));
    take(co_await drive.lifecycle(files.create_directories(unknown)));
    observed.clear();
    auto rejected = co_await drive.lifecycle(walk_local_buckets(
      files,
      paths.root(),
      take(paths.buckets(0, local_bucket_kind::wal)),
      local_bucket_kind::wal,
      work,
      visitor));
    require(
      !rejected && rejected.error().code() == errc::unsupported_format,
      "unknown bucket silently ignored");
    require(
      take(co_await drive.lifecycle(files.exists(unknown))),
      "unknown bucket was removed");
}
} // namespace publication_contract
} // namespace kwaque::storage::testing
