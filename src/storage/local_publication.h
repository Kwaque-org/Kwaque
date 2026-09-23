#pragma once

#include "src/base/invariant.h"
#include "src/storage/local_paths.h"
#include "src/storage/workload_budget.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/util/defer.hh>

#include <algorithm>
#include <exception>
#include <optional>
#include <type_traits>
#include <utility>

namespace kwaque::storage {
template<runtime::file_system_backend Backend>
class local_file_publisher;
enum class local_publication_stage : std::uint8_t {
    none,
    parent_open,
    temporary_open,
    written,
    file_synced,
    file_closed,
    renamed,
    directory_synced
};
enum class local_publication_disposition : std::uint8_t {
    untouched,
    uncertain,
    durable
};
struct local_publication_outcome final {
    local_publication_outcome() = default;
    explicit local_publication_outcome(
      workload_reservation reservation) noexcept
      : reservation_(std::move(reservation)) {}
    local_publication_stage stage{local_publication_stage::none};
    local_publication_disposition disposition{
      local_publication_disposition::untouched};
    runtime::first_failure failure;
    // Publication may have created a temp that survives/reappears after a
    // crash, including when exclusive open returned an error. A visible unlink
    // alone does not discharge this debt. Collisions are not owned temps.
    // Only externally reconciled ownership authorizes later cleanup.
    std::optional<runtime::file_name> temporary;
    bool temporary_may_exist{false};

private:
    template<runtime::file_system_backend Backend>
    friend class local_file_publisher;
    // Retained diagnostic/path ownership cannot bypass admission by keeping
    // an unbounded collection of completed outcomes.
    std::optional<workload_reservation> reservation_;
};
struct local_publication_target final {
    local_store_context owner;
    // Externally owned configured root; inspection cannot escape this boundary.
    runtime::file_path root;
    runtime::file_path parent;
    runtime::file_name name;
    runtime::file_rename_policy policy;
    // Independently observed current generation, under exclusive namespace
    // ownership. No current value means create, which requires no-replace.
    std::optional<local_publication_generation> current;
};
struct local_publication_request final {
    local_store_context owner;
    local_publication_generation generation;
    std::optional<local_publication_generation> expected_current;
};
struct local_publication_limits final {
    byte_count maximum_bytes{65536};
    // Caller-qualified frame/control allowance in addition to payload,
    // native write backing and the bounded path allocations charged below.
    byte_count execution_bytes{65536};
};

// One externally owned namespace target. The caller supplies already validated
// encoded bytes and dependencies, and holds its mutable-state serialization
// from snapshot selection through publish completion/in-memory installation.
// The parent chain is already durably established under that owner; this
// helper neither creates ancestors nor establishes mount/lock ownership.
// This owner rejects overlap without queuing and fences after uncertain
// effects. Join publish/close in the enclosing operation_scope before stopping
// Backend; release returned outcomes before stopping their workload owner.
template<runtime::file_system_backend Backend>
class local_file_publisher final : public runtime::shard_affine {
public:
    local_file_publisher(
      Backend& files,
      workload_budget& budget,
      local_publication_target target,
      local_publication_limits limits = {})
      : files_(files)
      , budget_(budget)
      , target_(std::move(target))
      , limits_(limits) {}
    local_file_publisher(const local_file_publisher&) = delete;
    local_file_publisher& operator=(const local_file_publisher&) = delete;
    ~local_file_publisher() {
        assert_current();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-PUBLICATION-DRAINED"},
          closed_,
          "publisher destroyed before joined close");
    }

    [[nodiscard]] seastar::future<local_publication_outcome> publish(
      local_publication_request request,
      bytes::fragmented_buffer payload,
      codec::cooperative_work& admission) {
        auto final = validate_request(request);
        if (!final) return reject(final.error());
        if (
          payload.empty() || payload.size() > limits_.maximum_bytes
          || payload.fragment_count() > 1024)
            return reject(detail::path_error(errc::invalid_argument));
        auto write_backing = budget_.allocation_charge(payload.size());
        auto paths = path_charge();
        if (!write_backing) return reject(write_backing.error());
        if (!paths) return reject(paths.error());
        auto reservation = budget_.try_reserve_buffer(
          payload,
          byte_count{
            limits_.execution_bytes.value() + write_backing->value()
            + paths->value()});
        if (!reservation) return reject(reservation.error());
        if (auto handles = reservation->try_acquire_handles(2); !handles)
            return reject(handles.error());
        const auto size = payload.size();
        return publish_owned(
          request,
          buffer_writer{std::move(payload)},
          size,
          std::move(*final),
          admission,
          std::move(*reservation),
          operations_.hold());
    }
    // Stream one bounded page at a time into a new immutable file. The named
    // writer remains alive through the joined call. Its full live buffers,
    // decoder metadata and captures are covered by working_bytes; native
    // write backing, paths, handles and execution are admitted separately.
    // Zero length is intentional for an empty page-only retry bundle.
    template<typename Writer>
    requires std::same_as<
               std::invoke_result_t<
                 Writer&,
                 runtime::file&,
                 codec::cooperative_work&>,
               seastar::future<runtime::result<void>>>
             && std::is_nothrow_move_constructible_v<Writer>
    [[nodiscard]] seastar::future<local_publication_outcome> publish_stream(
      local_publication_request request,
      Writer writer,
      byte_count file_bytes,
      byte_count working_bytes,
      codec::cooperative_work& admission) {
        static_assert(sizeof(Writer) <= 8192);
        auto final = validate_request(request);
        if (!final) return reject(final.error());
        if (
          target_.policy != runtime::file_rename_policy::no_replace
          || target_.current || file_bytes.value() > 257ULL * 65536ULL
          || working_bytes.value() < 65536
          || working_bytes.value() > 4U * 1024U * 1024U)
            return reject(detail::path_error(errc::invalid_argument));
        auto backing = budget_.allocation_charge(byte_count{65536});
        auto paths = path_charge();
        if (!backing) return reject(backing.error());
        if (!paths) return reject(paths.error());
        auto reservation = budget_.try_reserve(
          byte_count{
            working_bytes.value() + limits_.execution_bytes.value()
            + backing->value() + paths->value()});
        if (!reservation) return reject(reservation.error());
        if (auto handles = reservation->try_acquire_handles(2); !handles)
            return reject(handles.error());
        return publish_owned(
          request,
          std::move(writer),
          file_bytes,
          std::move(*final),
          admission,
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
        closed_ = true;
        co_return runtime::result<void>{};
    }
    [[nodiscard]] bool fenced() const noexcept {
        assert_current();
        return fenced_;
    }

private:
    static seastar::future<local_publication_outcome>
    reject(runtime::operation_error error) {
        local_publication_outcome result;
        result.failure.observe(error);
        return seastar::make_ready_future<local_publication_outcome>(
          std::move(result));
    }
    runtime::result<runtime::file_path>
    validate_request(const local_publication_request& request) const {
        assert_current();
        if (closing_ || closed_ || fenced_)
            return runtime::failure(detail::path_error(errc::closed));
        if (busy_)
            return runtime::failure(
              runtime::make_file_error(
                errc::queue_full,
                runtime::file_failure_detail::admission_not_dispatched));
        if (
          request.owner != target_.owner
          || request.expected_current != target_.current)
            return runtime::failure(detail::path_error(errc::wrong_context));
        if (
          !request.generation.is_valid()
          || (target_.current && !target_.current->is_valid())
          || (target_.current && request.generation <= *target_.current)
          || (!target_.current && target_.policy != runtime::file_rename_policy::no_replace)
          || (target_.policy != runtime::file_rename_policy::replace && target_.policy != runtime::file_rename_policy::no_replace)
          || limits_.maximum_bytes.value() == 0
          || limits_.maximum_bytes.value() > 65536
          || limits_.execution_bytes.value() == 0
          || limits_.execution_bytes.value()
               > maximum_contiguous_allocation_bytes)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        if (auto valid = validate_local_path(target_.parent); !valid)
            return runtime::failure(valid.error());
        auto final = local_child_path(target_.parent, target_.name);
        if (!final) return final;
        auto last = local_temporary_name(target_.name, request.generation, 63);
        if (!last) return runtime::failure(last.error());
        auto path = local_child_path(target_.parent, *last);
        if (!path) return runtime::failure(path.error());
        return final;
    }
    runtime::result<byte_count> path_charge() const {
        auto cost = budget_.allocation_charge(
          byte_count{runtime::maximum_file_path_bytes + 1});
        if (!cost) return runtime::failure(cost.error());
        return byte_count{8U * cost->value()};
    }
    struct buffer_writer final {
        bytes::fragmented_buffer payload;
        seastar::future<runtime::result<void>>
        operator()(runtime::file& file, codec::cooperative_work&) {
            const auto size = payload.size();
            auto written = co_await file.write(
              runtime::file_position{}, std::move(payload));
            if (!written) co_return runtime::failure(written.error());
            if (*written != size)
                co_return runtime::failure(
                  detail::path_error(errc::io_failure));
            co_return runtime::result<void>{};
        }
    };
    template<typename Writer>
    seastar::future<local_publication_outcome> publish_owned(
      local_publication_request request,
      Writer writer,
      byte_count size,
      runtime::file_path final,
      codec::cooperative_work& admission,
      workload_reservation reservation,
      seastar::gate::holder holder) {
        busy_ = true;
        auto idle = seastar::defer([this] noexcept { busy_ = false; });
        static_cast<void>(reservation);
        static_cast<void>(holder);
        local_publication_outcome output;
        output.reservation_.emplace(std::move(reservation));
        seastar::abort_source execution_abort;
        codec::cooperative_work execution{admission.policy(), execution_abort};
        std::optional<typename Backend::directory_cursor_type> parent;
        std::optional<runtime::file> temporary;
        std::optional<runtime::file::metadata_reservation> barrier;
        std::optional<runtime::file_path> temp_path;
        bool started = false, owned_temp = false, rename_attempted = false;
        try {
            do {
                auto inspected = co_await inspect_local_path(
                  files_,
                  target_.root,
                  target_.parent,
                  runtime::file_kind::directory,
                  admission);
                if (!inspected) {
                    output.failure.observe(inspected);
                    break;
                }
                inspected = co_await inspect_local_path(
                  files_,
                  target_.root,
                  final,
                  runtime::file_kind::regular,
                  admission,
                  !target_.current);
                if (!inspected) {
                    output.failure.observe(inspected);
                    break;
                }
                auto directory = co_await files_.open_directory(
                  target_.parent, runtime::file_close_policy::checked);
                if (!directory) {
                    output.failure.observe(directory);
                    break;
                }
                parent.emplace(std::move(*directory));
                output.stage = local_publication_stage::parent_open;
                for (std::uint8_t attempt = 0; attempt != 64; ++attempt) {
                    auto room = co_await execution.admit(
                      byte_count{16384}, item_count{16});
                    if (!room) {
                        output.failure.observe(
                          detail::path_error(room.error().code()));
                        break;
                    }
                    if (auto ready = execution.poll(); !ready) {
                        output.failure.observe(
                          detail::path_error(ready.error().code()));
                        break;
                    }
                    if (!started) {
                        auto ready = admission.poll();
                        if (!ready) {
                            output.failure.observe(
                              detail::path_error(ready.error().code()));
                            break;
                        }
                    }
                    output.temporary = local_temporary_name(
                                         target_.name,
                                         request.generation,
                                         attempt)
                                         .value();
                    temp_path = local_child_path(
                                  target_.parent, *output.temporary)
                                  .value();
                    started = true;
                    output.temporary_may_exist = true;
                    auto opened = co_await files_.open(
                      *temp_path,
                      {.access = runtime::file_access::read_write,
                       .create = true,
                       .exclusive = true,
                       .close_policy = runtime::file_close_policy::checked});
                    if (!opened) {
                        if (opened.error().code() == errc::already_exists) {
                            output.temporary_may_exist = false;
                            started = false;
                            continue;
                        }
                        const auto cause = runtime::file_detail(opened.error());
                        if (
                          cause
                          && *cause
                               == runtime::file_failure_detail::
                                 admission_not_dispatched) {
                            output.temporary_may_exist = false;
                            started = false;
                        }
                        output.failure.observe(opened);
                        break;
                    }
                    temporary.emplace(std::move(*opened));
                    owned_temp = true;
                    output.stage = local_publication_stage::temporary_open;
                    break;
                }
                if (!temporary) {
                    if (!output.failure.failed())
                        output.failure.observe(
                          detail::path_error(errc::resource_exhausted));
                    break;
                }
                auto unit = temporary->try_reserve_metadata();
                if (!unit) {
                    output.failure.observe(unit);
                    break;
                }
                barrier.emplace(std::move(*unit));
                auto written = co_await writer(*temporary, execution);
                if (!written) {
                    output.failure.observe(written);
                    break;
                }
                if constexpr (!std::same_as<Writer, buffer_writer>) {
                    auto actual = co_await temporary->size();
                    if (!actual) {
                        output.failure.observe(actual);
                        break;
                    }
                    if (*actual != size.value()) {
                        output.failure.observe(
                          detail::path_error(errc::io_failure));
                        break;
                    }
                }
                output.stage = local_publication_stage::written;
                auto synced = co_await temporary->flush(*barrier);
                if (!synced) {
                    output.failure.observe(synced);
                    break;
                }
                output.stage = local_publication_stage::file_synced;
                barrier.reset();
                auto closed = co_await temporary->close();
                temporary.reset();
                if (!closed) {
                    output.failure.observe(closed);
                    break;
                }
                output.stage = local_publication_stage::file_closed;
                rename_attempted = true;
                output.disposition = local_publication_disposition::uncertain;
                auto renamed = co_await files_.rename(
                  *temp_path, final, target_.policy);
                if (!renamed) {
                    const auto detail = runtime::file_detail(renamed.error());
                    if ((detail && *detail == runtime::file_failure_detail::admission_not_dispatched)
                        || (target_.policy == runtime::file_rename_policy::no_replace && renamed.error().code() == errc::already_exists))
                        output.disposition
                          = local_publication_disposition::untouched;
                    output.failure.observe(renamed);
                    break;
                }
                output.stage = local_publication_stage::renamed;
                synced = co_await parent->sync();
                if (!synced) {
                    output.failure.observe(synced);
                    break;
                }
                output.stage = local_publication_stage::directory_synced;
                output.disposition = local_publication_disposition::durable;
                output.temporary_may_exist = false;
                target_.current = request.generation;
            } while (false);
        } catch (...) {
            output.failure.observe(std::current_exception());
        }
        barrier.reset();
        if (temporary) {
            try {
                output.failure.observe(co_await temporary->close());
            } catch (...) {
                output.failure.observe(std::current_exception());
            }
        }
        if (owned_temp && !rename_attempted) {
            try {
                auto removed = co_await files_.remove_file(*temp_path);
                output.failure.observe(removed);
            } catch (...) {
                output.failure.observe(std::current_exception());
            }
        }
        if (parent) {
            try {
                output.failure.observe(co_await parent->close());
            } catch (...) {
                output.failure.observe(std::current_exception());
            }
        }
        if (started && output.failure.failed()) fenced_ = true;
        co_await admission.drain_inline(
          admission.byte_quantum(), admission.item_quantum());
        // Failures retain their typed error/exception alongside confirmed
        // stages.
        co_return output;
    }
    Backend& files_;
    workload_budget& budget_;
    local_publication_target target_;
    local_publication_limits limits_;
    seastar::gate operations_;
    bool busy_{false}, closing_{false}, closed_{false}, fenced_{false};
};
} // namespace kwaque::storage
