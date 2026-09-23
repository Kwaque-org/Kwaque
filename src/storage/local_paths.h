#pragma once

#include "src/codec/cooperative.h"
#include "src/runtime/file.h"
#include "src/runtime/first_failure.h"
#include "src/storage/local_types.h"

#include <seastar/core/coroutine.hh>

#include <exception>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace kwaque::storage {

enum class local_segment_file : std::uint8_t { descriptor, data, published };
enum class local_sequence_file : std::uint8_t {
    checkpoint,
    evidence,
    decision,
    deletion
};
enum class local_bucket_kind : std::uint8_t { wal, segment };
struct local_segment_name final {
    model::segment_id segment;
    model::segment_generation generation;
    bool operator==(const local_segment_name&) const = default;
};

// Names are layout only. They do not prove device ownership, allocation,
// publication, or eligibility for recovery/deletion.
[[nodiscard]] runtime::result<std::uint8_t>
  parse_local_bucket(std::string_view);
[[nodiscard]] runtime::result<model::wal_incarnation_id>
parse_local_wal_name(std::string_view, std::uint8_t bucket);
[[nodiscard]] runtime::result<local_segment_name>
parse_local_segment_name(std::string_view, std::uint8_t bucket);
[[nodiscard]] runtime::result<std::uint64_t>
parse_local_sequence_name(std::string_view, bool metadata);
[[nodiscard]] runtime::result<std::uint32_t>
  parse_local_shard_name(std::string_view);
[[nodiscard]] runtime::result<void>
validate_local_path(const runtime::file_path&);
[[nodiscard]] runtime::result<runtime::file_path>
local_child_path(const runtime::file_path&, const runtime::file_name&);
[[nodiscard]] runtime::result<runtime::file_name> local_temporary_name(
  const runtime::file_name&,
  local_publication_generation,
  std::uint8_t attempt);

struct local_temporary_identity final {
    local_publication_generation generation;
    std::uint8_t attempt;
};
[[nodiscard]] runtime::result<local_temporary_identity>
parse_local_temporary_name(std::string_view, const runtime::file_name& target);

class local_paths final {
public:
    [[nodiscard]] static runtime::result<local_paths>
    make(runtime::file_path root);
    [[nodiscard]] const runtime::file_path& root() const& noexcept {
        return root_;
    }
    const runtime::file_path& root() const&& = delete;
    [[nodiscard]] runtime::result<runtime::file_path> store() const;
    [[nodiscard]] runtime::result<runtime::file_path>
    control(std::uint32_t shard) const;
    [[nodiscard]] runtime::result<runtime::file_path>
    buckets(std::uint32_t shard, local_bucket_kind) const;
    [[nodiscard]] runtime::result<runtime::file_path>
    wal(std::uint32_t shard, model::wal_incarnation_id) const;
    [[nodiscard]] runtime::result<runtime::file_path>
    segment(std::uint32_t shard, local_segment_name) const;
    [[nodiscard]] runtime::result<runtime::file_path> segment_file(
      std::uint32_t shard, local_segment_name, local_segment_file) const;
    [[nodiscard]] runtime::result<runtime::file_path> object(
      std::uint32_t shard, local_segment_name, local_object_sequence) const;
    [[nodiscard]] runtime::result<runtime::file_path> sequence_file(
      std::uint32_t shard, local_sequence_file, std::uint64_t sequence) const;

private:
    explicit local_paths(runtime::file_path root) noexcept
      : root_(std::move(root)) {}
    runtime::result<runtime::file_path> shard(std::uint32_t) const;
    runtime::file_path root_;
};

namespace detail {
inline runtime::operation_error path_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::file};
}
inline seastar::future<runtime::result<void>>
path_checkpoint(codec::cooperative_work& work) {
    const auto admitted = co_await work.admit(byte_count{8192}, item_count{16});
    if (!admitted)
        co_return runtime::failure(path_error(admitted.error().code()));
    const auto ready = work.poll();
    if (!ready) co_return runtime::failure(path_error(ready.error().code()));
    co_return runtime::result<void>{};
}
} // namespace detail

// The caller holds exclusive namespace/mount ownership across inspection and
// use. Path-based APIs cannot protect against a hostile concurrent ancestor
// swap. Checks the supplied owned namespace root and every descendant ancestor.
// Host ancestors/mount identity outside that root remain the namespace owner's
// responsibility. No creation or repair.
template<runtime::file_system_backend Backend>
seastar::future<runtime::result<void>> inspect_local_path(
  Backend& files,
  const runtime::file_path& namespace_root,
  const runtime::file_path& path,
  runtime::file_kind required,
  codec::cooperative_work& work,
  bool missing_leaf = false) {
    if (auto valid = validate_local_path(path); !valid)
        co_return runtime::failure(valid.error());
    if (auto valid = validate_local_path(namespace_root); !valid)
        co_return runtime::failure(valid.error());
    if (
      required != runtime::file_kind::regular
      && required != runtime::file_kind::directory)
        co_return runtime::failure(detail::path_error(errc::invalid_argument));
    const auto& value = path.value();
    const auto& root = namespace_root.value();
    if (value == root && required != runtime::file_kind::directory)
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    if (
      !value.starts_with(root)
      || (value.size() != root.size() && root != "/" && value[root.size()] != '/'))
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    std::size_t end = root.size();
    for (;;) {
        const auto ready = co_await detail::path_checkpoint(work);
        if (!ready) co_return runtime::failure(ready.error());
        auto prefix = runtime::file_path::make(
          std::string_view{value}.substr(0, end));
        if (!prefix) co_return runtime::failure(prefix.error());
        const auto status = co_await files.stat(std::move(*prefix));
        const bool final = end == value.size();
        if (!status) {
            if (
              final && end != root.size() && missing_leaf
              && status.error().code() == errc::not_found)
                co_return runtime::result<void>{};
            co_return runtime::failure(status.error());
        }
        if (status->kind != (final ? required : runtime::file_kind::directory))
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        if (final) break;
        const auto next = value.find('/', end + 1);
        end = next == std::string::npos ? value.size() : next;
    }
    co_return runtime::result<void>{};
}

// Bounded two-level traversal: one bucket entry and one child page retained.
// The visitor borrows each path only until its returned future settles; false
// stops successfully. Unknown names/kinds are reported, never skipped/deleted.
template<runtime::file_system_backend Backend, typename Visitor>
seastar::future<runtime::result<void>> walk_local_buckets(
  Backend& files,
  runtime::file_path namespace_root,
  runtime::file_path root,
  local_bucket_kind kind,
  codec::cooperative_work& work,
  Visitor visit) {
    static_assert(
      sizeof(Visitor) <= 4096,
      "large visitors require a separately admitted owner");
    using cursor = typename Backend::directory_cursor_type;
    std::optional<cursor> buckets, children;
    runtime::first_failure failed;
    try {
        do {
            if (
              kind != local_bucket_kind::wal
              && kind != local_bucket_kind::segment) {
                failed.observe(detail::path_error(errc::invalid_argument));
                break;
            }
            auto inspected = co_await inspect_local_path(
              files, namespace_root, root, runtime::file_kind::directory, work);
            if (!inspected) {
                failed.observe(inspected);
                break;
            }
            auto opened = co_await files.open_directory(
              root, runtime::file_close_policy::checked);
            if (!opened) {
                failed.observe(opened);
                break;
            }
            buckets.emplace(std::move(*opened));
            bool end = false, stop = false;
            while (!end && !stop && !failed.failed()) {
                auto ready = co_await detail::path_checkpoint(work);
                if (!ready) {
                    failed.observe(ready);
                    break;
                }
                auto page = co_await buckets->next(
                  {.maximum_entries = item_count{1},
                   .maximum_name_bytes = byte_count{255}});
                if (!page) {
                    failed.observe(page);
                    break;
                }
                end = page->end();
                for (const auto& bucket : page->entries()) {
                    auto number = parse_local_bucket(bucket.name.value());
                    if (
                      !number || bucket.kind != runtime::file_kind::directory) {
                        failed.observe(
                          detail::path_error(errc::unsupported_format));
                        break;
                    }
                    auto parent = local_child_path(root, bucket.name);
                    if (!parent) {
                        failed.observe(parent);
                        break;
                    }
                    inspected = co_await inspect_local_path(
                      files,
                      namespace_root,
                      *parent,
                      runtime::file_kind::directory,
                      work);
                    if (!inspected) {
                        failed.observe(inspected);
                        break;
                    }
                    opened = co_await files.open_directory(
                      *parent, runtime::file_close_policy::checked);
                    if (!opened) {
                        failed.observe(opened);
                        break;
                    }
                    children.emplace(std::move(*opened));
                    bool child_end = false;
                    while (!child_end && !stop && !failed.failed()) {
                        ready = co_await detail::path_checkpoint(work);
                        if (!ready) {
                            failed.observe(ready);
                            break;
                        }
                        auto entries = co_await children->next(
                          {.maximum_entries = item_count{16},
                           .maximum_name_bytes = byte_count{4096}});
                        if (!entries) {
                            failed.observe(entries);
                            break;
                        }
                        child_end = entries->end();
                        for (const auto& entry : entries->entries()) {
                            const bool valid
                              = kind == local_bucket_kind::wal
                                  ? entry.kind == runtime::file_kind::regular
                                      && bool(parse_local_wal_name(
                                        entry.name.value(), *number))
                                  : entry.kind == runtime::file_kind::directory
                                      && bool(parse_local_segment_name(
                                        entry.name.value(), *number));
                            if (!valid) {
                                failed.observe(
                                  detail::path_error(errc::unsupported_format));
                                break;
                            }
                            auto path = local_child_path(*parent, entry.name);
                            if (!path) {
                                failed.observe(path);
                                break;
                            }
                            inspected = co_await inspect_local_path(
                              files, namespace_root, *path, entry.kind, work);
                            if (!inspected) {
                                failed.observe(inspected);
                                break;
                            }
                            auto result = co_await visit(*path);
                            if (!result) {
                                failed.observe(result);
                                break;
                            }
                            if (!*result) {
                                stop = true;
                                break;
                            }
                        }
                    }
                    failed.observe(co_await children->close());
                    children.reset();
                }
            }
        } while (false);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (children) {
        try {
            failed.observe(co_await children->close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    if (buckets) {
        try {
            failed.observe(co_await buckets->close());
        } catch (...) {
            failed.observe(std::current_exception());
        }
    }
    co_return failed.outcome();
}
} // namespace kwaque::storage
