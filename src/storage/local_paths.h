#pragma once

#include "src/base/units.h"
#include "src/codec/cooperative.h"
#include "src/runtime/file.h"
#include "src/runtime/first_failure.h"
#include "src/storage/local_types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/timed_out_error.hh>

#include <exception>
#include <new>
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
// What a shard's spare directory on a device keeps: bodies of WAL files, on
// the control device, or data files of segments, on a data device. Each is
// a file a removal gave back to be made into a later one.
enum class local_spare_kind : std::uint8_t { wal, segment };
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
// The name a segment's directory takes when its removal begins: its own with
// the separator changed, so nothing but a segment's directory is ever taken
// for one. A directory under it is being removed whatever it holds, and is
// told so by its name alone.
[[nodiscard]] runtime::result<local_segment_name>
parse_local_deleting_name(std::string_view, std::uint8_t bucket);
[[nodiscard]] runtime::result<std::uint64_t>
parse_local_sequence_name(std::string_view, bool metadata);
// A spare's name: the identity its file had last, then ".spare". No spare's
// name is a WAL file's or a segment directory's, so nothing that walks the
// buckets ever takes one for either.
[[nodiscard]] runtime::result<model::wal_incarnation_id>
  parse_local_wal_spare(std::string_view);
[[nodiscard]] runtime::result<local_segment_name>
  parse_local_segment_spare(std::string_view);
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
    // The directory that holds a segment's directory under either name.
    [[nodiscard]] runtime::result<runtime::file_path>
    segment_bucket(std::uint32_t shard, model::segment_id) const;
    // Where a segment's directory is while it is being removed.
    [[nodiscard]] runtime::result<runtime::file_path>
    deleting(std::uint32_t shard, local_segment_name) const;
    [[nodiscard]] runtime::result<runtime::file_path> segment_file(
      std::uint32_t shard, local_segment_name, local_segment_file) const;
    [[nodiscard]] runtime::result<runtime::file_path> object(
      std::uint32_t shard, local_segment_name, local_object_sequence) const;
    [[nodiscard]] runtime::result<runtime::file_path> sequence_file(
      std::uint32_t shard, local_sequence_file, std::uint64_t sequence) const;
    // A shard's spare directory of one kind on this device, and a spare in
    // it under the identity its file had last.
    [[nodiscard]] runtime::result<runtime::file_path>
    spares(std::uint32_t shard, local_spare_kind) const;
    [[nodiscard]] runtime::result<runtime::file_path>
    spare(std::uint32_t shard, model::wal_incarnation_id) const;
    [[nodiscard]] runtime::result<runtime::file_path>
    spare(std::uint32_t shard, local_segment_name) const;

private:
    explicit local_paths(runtime::file_path root) noexcept
      : root_(std::move(root)) {}
    runtime::result<runtime::file_path> shard(std::uint32_t) const;
    runtime::result<runtime::file_path>
    segment_directory(std::uint32_t, local_segment_name, char separator) const;
    runtime::file_path root_;
};

namespace detail {
inline runtime::operation_error path_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::file};
}
// The code of an exception from a family an operation expects: an
// allocation that failed, a wait that was aborted or cancelled, work turned
// away because its owner closed, a deadline. Nothing for any other, which
// is no operational failure and leaves as itself wherever it can.
inline std::optional<runtime::operation_error>
recognized(const std::exception_ptr& kept) noexcept {
    try {
        std::rethrow_exception(kept);
    } catch (const std::bad_alloc&) {
        return path_error(errc::resource_exhausted);
    } catch (const seastar::abort_requested_exception&) {
        return path_error(errc::aborted);
    } catch (const seastar::cancelled_error&) {
        return path_error(errc::aborted);
    } catch (const seastar::gate_closed_exception&) {
        return path_error(errc::closed);
    } catch (const seastar::broken_condition_variable&) {
        return path_error(errc::closed);
    } catch (const seastar::broken_semaphore&) {
        return path_error(errc::closed);
    } catch (const seastar::timed_out_error&) {
        return path_error(errc::timed_out);
    } catch (const seastar::condition_variable_timed_out&) {
        return path_error(errc::timed_out);
    } catch (...) {
        return std::nullopt;
    }
}
// Inside a handler: the code of the exception it handles when its family is
// recognized. Any other leaves as itself.
[[nodiscard]] inline runtime::operation_error recognized_or_rethrown() {
    if (auto code = recognized(std::current_exception())) return *code;
    throw;
}
// Inside a handler, where an answer is owed and nothing may be thrown: the
// code of a recognized exception. Any other is kept in `keep` as itself,
// for whoever closes its owner, and answered as an internal failure.
[[nodiscard]] inline runtime::operation_error
answer_thrown(runtime::first_failure& keep) noexcept {
    const auto thrown = std::current_exception();
    if (auto code = recognized(thrown)) return *code;
    keep.observe(thrown);
    return path_error(errc::invariant_violation);
}
// What a kept first failure answers with where an answer is owed and nothing
// may be thrown, and whoever joined the attempt is told: a recognized
// exception as its code, and any other as an internal failure, never as an
// I/O failure its device did not have. The exception itself stays with
// whoever kept it.
inline runtime::result<void>
settled(const runtime::first_failure& kept) noexcept {
    if (const auto& error = kept.error()) return runtime::failure(*error);
    if (!kept.exception()) return {};
    return runtime::failure(recognized(kept.exception())
                              .value_or(path_error(errc::invariant_violation)));
}
inline seastar::future<runtime::result<void>>
path_checkpoint(codec::cooperative_work& work) {
    const auto admitted = co_await work.admit(
      byte_count{8_KiB}, item_count{16});
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
        const auto part = std::string_view{value}.substr(0, end);
        const bool final = end == value.size();
        const auto kind = final ? required : runtime::file_kind::directory;
        // Under the exclusive ownership above, nothing else changes a
        // directory the file system verified and has not removed or renamed.
        bool known = false;
        if constexpr (runtime::verified_directory_memo<Backend>)
            known = kind == runtime::file_kind::directory
                    && files.verified_directory(part);
        if (!known) {
            auto prefix = runtime::file_path::make(part);
            if (!prefix) co_return runtime::failure(prefix.error());
            const auto status = co_await files.stat(std::move(*prefix));
            if (!status) {
                if (
                  final && end != root.size() && missing_leaf
                  && status.error().code() == errc::not_found)
                    co_return runtime::result<void>{};
                co_return runtime::failure(status.error());
            }
            if (status->kind != kind)
                co_return runtime::failure(
                  detail::path_error(errc::wrong_context));
            if constexpr (runtime::verified_directory_memo<Backend>)
                if (kind == runtime::file_kind::directory)
                    files.remember_directory(part);
        }
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
      sizeof(Visitor) <= 4_KiB,
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
            auto opened = co_await files.open_directory(root);
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
                    opened = co_await files.open_directory(*parent);
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
                           .maximum_name_bytes = byte_count{4_KiB}});
                        if (!entries) {
                            failed.observe(entries);
                            break;
                        }
                        child_end = entries->end();
                        for (const auto& entry : entries->entries()) {
                            // A segment being removed is no segment to
                            // visit: what its directory holds may be in any
                            // state.
                            if (
                              kind == local_bucket_kind::segment
                              && entry.kind == runtime::file_kind::directory
                              && parse_local_deleting_name(
                                entry.name.value(), *number))
                                continue;
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
