#pragma once

#include "src/storage/local_root.h"
#include "src/storage/local_segment.h"

namespace kwaque::storage {
using local_boundary_metadata = std::variant<durable_footer, sealed_footer>;
struct local_generation_expectation final {
    local_publication_generation generation;
    local_object_publication publication;
    local_segment_descriptor descriptor;
    // Same bounded order as publication.roots; inputs remain stable until open
    // joins.
    std::span<const local_bundle_context> roots;
};
namespace detail {
struct local_generation_state;
[[nodiscard]] seastar::future<runtime::result<local_boundary_metadata>>
read_local_boundary(
  runtime::file&,
  segment_history_context,
  local_footer_reference,
  local_store_io_limits,
  codec::cooperative_work&);
[[nodiscard]] runtime::result<void> validate_generation_root_context(
  const local_bundle_context&, segment_history_context);
[[nodiscard]] bool rebuildable_local_index_error(errc) noexcept;
[[nodiscard]] runtime::result<void> validate_generation_root(
  const local_bundle_root&,
  local_root_reference,
  segment_history_context,
  const local_boundary_metadata&);
} // namespace detail
class local_generation_owner;
class local_generation_pin final {
public:
    local_generation_pin(local_generation_pin&&) noexcept;
    local_generation_pin& operator=(local_generation_pin&&) noexcept = delete;
    local_generation_pin(const local_generation_pin&) = delete;
    local_generation_pin& operator=(const local_generation_pin&) = delete;
    ~local_generation_pin();
    [[nodiscard]] runtime::result<local_generation_pin> share() const;
    [[nodiscard]] local_publication_generation generation() const;
    [[nodiscard]] const local_object_publication& publication() const&;
    const local_object_publication& publication() const&& = delete;
    [[nodiscard]] const std::optional<local_boundary_metadata>&
    boundary() const&;
    const std::optional<local_boundary_metadata>& boundary() const&& = delete;
    [[nodiscard]] runtime::result<local_root_pin> root(local_root_kind) const;
    [[nodiscard]] std::optional<runtime::operation_error>
    index_rebuild_reason() const;

private:
    friend class local_generation_owner;
    local_generation_pin(
      workload_reservation held,
      seastar::lw_shared_ptr<detail::local_generation_state> state,
      seastar::gate::holder holder);
    workload_reservation held_;
    seastar::lw_shared_ptr<detail::local_generation_state> state_;
    seastar::gate::holder holder_;
};

// Each generation consumes a nonwaiting workload slot/bytes/handles; old and
// new generations therefore share a real finite bound. Exactly four root
// slots are available. Replacement first durably publishes the new pointer,
// then retires old admission. Shutdown may retire without a replacement.
// Live-pin drain never proves durable reference discharge or authorizes unlink.
// The workload/directory owners outlive every generation and pin. Open borrows
// its independent expected fields under quiescent publication ownership until
// joined completion; all retained metadata thereafter belongs to the owner.
class local_generation_owner final : public runtime::shard_affine {
public:
    local_generation_owner(const local_generation_owner&) = delete;
    local_generation_owner& operator=(const local_generation_owner&) = delete;
    ~local_generation_owner();
    [[nodiscard]] runtime::result<local_generation_pin> pin();
    void retire() noexcept;
    [[nodiscard]] seastar::future<runtime::result<void>> close();
    template<runtime::file_system_backend Backend, local_directory_owner Owner>
    [[nodiscard]] static seastar::future<
      runtime::result<std::unique_ptr<local_generation_owner>>>
    open(
      Backend&,
      Owner&,
      const local_device_spec&,
      std::uint32_t,
      const local_generation_expectation&,
      workload_budget&,
      local_store_io_limits,
      codec::cooperative_work&,
      local_reader_limits = {});

private:
    local_generation_owner();
    static std::unique_ptr<local_generation_owner> adopt(
      workload_reservation&,
      local_loaded_metadata&,
      std::optional<runtime::file>&,
      std::optional<local_boundary_metadata>&,
      std::array<std::unique_ptr<local_root_owner>, 4>&,
      workload_budget&,
      local_reader_limits,
      std::optional<runtime::operation_error>);
    seastar::lw_shared_ptr<detail::local_generation_state> state_;
};

template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<std::unique_ptr<local_generation_owner>>>
local_generation_owner::open(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  std::uint32_t shard,
  const local_generation_expectation& expected,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work,
  local_reader_limits readers) {
    if (auto valid = limits.validate(); !valid)
        co_return runtime::failure(valid.error());
    if (auto valid = readers.validate(); !valid)
        co_return runtime::failure(valid.error());
    if (
      !expected.generation.is_valid()
      || expected.publication.segment != expected.descriptor.segment
      || expected.publication.roots.size() > 4
      || expected.roots.size() != expected.publication.roots.size())
        co_return runtime::failure(detail::path_error(errc::invalid_argument));
    auto held = budget.try_reserve(
      byte_count{
        limits.operation_bytes.value() + limits.execution_bytes.value()
        + 32768});
    if (!held) co_return runtime::failure(held.error());
    if (auto handles = held->try_acquire_handles(1); !handles)
        co_return runtime::failure(handles.error());
    auto header = segment_header::make(
      expected.descriptor.segment,
      expected.descriptor.logical_origin,
      expected.descriptor.alignment,
      expected.descriptor.profile);
    if (!header)
        co_return runtime::failure(detail::path_error(errc::invalid_argument));
    auto segment = co_await load_local_segment(
      files,
      ownership,
      spec,
      shard,
      expected.descriptor,
      *header,
      budget,
      limits,
      work);
    if (!segment) co_return runtime::failure(segment.error());
    const segment_history_context history{
      expected.descriptor.segment,
      expected.descriptor.alignment,
      segment->header.bytes.end(),
      expected.descriptor.logical_origin,
      expected.descriptor.physical_origin,
      expected.descriptor.profile};
    const auto paths = local_paths::make(spec.root).value();
    const local_segment_name identity{
      history.segment.segment(), history.segment.generation()};
    auto path = paths.segment_file(
      shard, identity, local_segment_file::published);
    if (!path) co_return runtime::failure(path.error());
    auto metadata_header = local_metadata_header::make(
                             local_metadata_kind::object_publication,
                             spec.shard_owner(shard).value(),
                             expected.generation)
                             .value();
    auto publication = co_await load_local_metadata_file(
      files,
      spec,
      *path,
      budget,
      limits,
      work,
      local_metadata_extent::exact_file,
      [metadata_header, alignment = spec.identity.metadata_alignment, history](
        auto& input, byte_count, codec::decode_budget memory, auto& work) {
          return decode_local_metadata(
            input,
            {metadata_header, alignment, history.segment, history.alignment},
            memory,
            work,
            {},
            codec::input_boundary::complete);
      });
    if (!publication) co_return runtime::failure(publication.error());
    if (
      std::get<local_object_publication>(publication->value.payload())
      != expected.publication)
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    std::optional<runtime::file> data;
    std::optional<local_boundary_metadata> boundary;
    std::array<std::unique_ptr<local_root_owner>, 4> roots;
    std::unique_ptr<local_generation_owner> output;
    std::optional<runtime::operation_error> index_rebuild;
    runtime::first_failure failed;
    try {
        do {
            if (expected.publication.boundary) {
                path = paths.segment_file(
                  shard, identity, local_segment_file::data);
                if (!path) {
                    failed.observe(path);
                    break;
                }
                auto valid = co_await inspect_local_path(
                  files, spec.root, *path, runtime::file_kind::regular, work);
                if (!valid) {
                    failed.observe(valid);
                    break;
                }
                auto file = co_await files.open(
                  *path, {.close_policy = runtime::file_close_policy::checked});
                if (!file) {
                    failed.observe(file);
                    break;
                }
                data.emplace(std::move(*file));
                auto decoded = co_await detail::read_local_boundary(
                  *data, history, *expected.publication.boundary, limits, work);
                if (!decoded) {
                    failed.observe(decoded);
                    break;
                }
                boundary.emplace(std::move(*decoded));
            }
            for (std::size_t i = 0; i < expected.roots.size(); ++i) {
                auto context_valid = detail::validate_generation_root_context(
                  expected.roots[i], history);
                if (!context_valid) {
                    failed.observe(context_valid);
                    break;
                }
                auto opened = co_await local_root_owner::open(
                  files,
                  ownership,
                  spec,
                  shard,
                  expected.publication.roots[i],
                  expected.roots[i],
                  budget,
                  limits,
                  work,
                  readers);
                if (!opened) {
                    if (
                      expected.publication.roots[i].kind()
                        == local_root_kind::index
                      && detail::rebuildable_local_index_error(
                        opened.error().code())) {
                        index_rebuild = opened.error();
                        continue;
                    }
                    failed.observe(opened);
                    break;
                }
                roots[i] = std::move(*opened);
                auto pin = roots[i]->pin();
                if (!pin) {
                    failed.observe(pin);
                    break;
                }
                auto valid = detail::validate_generation_root(
                  pin->metadata(), pin->reference(), history, *boundary);
                if (!valid) {
                    failed.observe(valid);
                    break;
                }
                if (
                  pin->reference().kind()
                  == local_root_kind::completed_retry_snapshot) {
                    const auto& root = std::get<local_completed_retry_root>(
                      std::get<local_metadata_record>(pin->metadata())
                        .payload());
                    auto footer = co_await detail::read_local_boundary(
                      *data, history, root.footer, limits, work);
                    if (!footer) {
                        failed.observe(footer);
                        break;
                    }
                }
            }
            if (failed.failed()) break;
            auto valid = co_await ownership.validate(spec);
            if (!valid) {
                failed.observe(valid);
                break;
            }
            output = adopt(
              *held,
              *publication,
              data,
              boundary,
              roots,
              budget,
              readers,
              index_rebuild);
            data.reset();
        } while (false);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    if (!output) {
        for (std::size_t i = roots.size(); i != 0; --i) {
            if (roots[i - 1]) {
                try {
                    failed.observe(co_await roots[i - 1]->close());
                } catch (...) {
                    failed.observe(std::current_exception());
                }
                roots[i - 1].reset();
            }
        }
        if (data) {
            try {
                failed.observe(co_await data->close());
            } catch (...) {
                failed.observe(std::current_exception());
            }
        }
    }
    if (auto result = failed.outcome(); !result)
        co_return runtime::failure(result.error());
    co_return std::move(output);
}
} // namespace kwaque::storage
