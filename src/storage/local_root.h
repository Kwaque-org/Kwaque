#pragma once

#include "src/storage/local_bundle.h"

#include <seastar/core/shared_ptr.hh>

#include <memory>

namespace kwaque::storage {
struct local_reader_limits final {
    // Includes shared descendants retained by in-flight/returned page reads.
    std::uint32_t maximum_pins{64};
    [[nodiscard]] runtime::result<void> validate() const noexcept {
        if (maximum_pins == 0 || maximum_pins > 4096)
            return runtime::failure(detail::path_error(errc::invalid_argument));
        return {};
    }
};
namespace detail {
struct local_root_state;
}
class local_root_owner;
struct local_root_page;

// An owning read lifetime. Metadata views never outlive this pin; sharing is
// explicit, bounded and admitted. Existing pins can finish after retirement.
class local_root_pin final {
public:
    local_root_pin(local_root_pin&&) noexcept;
    local_root_pin& operator=(local_root_pin&&) noexcept = delete;
    ~local_root_pin();
    local_root_pin(const local_root_pin&) = delete;
    local_root_pin& operator=(const local_root_pin&) = delete;
    [[nodiscard]] runtime::result<local_root_pin> share() const;
    [[nodiscard]] local_root_reference reference() const;
    [[nodiscard]] const local_bundle_root& metadata() const&;
    const local_bundle_root& metadata() const&& = delete;
    [[nodiscard]] runtime::result<runtime::file_position>
      position(page_ordinal) const;
    [[nodiscard]] seastar::future<runtime::result<local_root_page>>
    read(page_ordinal, codec::cooperative_work&) const;

private:
    friend class local_root_owner;
    local_root_pin(
      workload_reservation held,
      seastar::lw_shared_ptr<detail::local_root_state> state,
      seastar::gate::holder holder);
    workload_reservation held_;
    seastar::lw_shared_ptr<detail::local_root_state> state_;
    seastar::gate::holder holder_;
};
struct local_root_page final {
    workload_reservation reservation;
    local_root_pin pin;
    page_ref reference;
    runtime::file_position position;
    bytes::fragmented_buffer bytes;
};

// One immutable opened root with its exact page file (and, for sealed retry,
// its separate data-file footer). Retirement stops new pins, close drains all
// pin/operation descendants before checked file close. Close never unlinks.
// Workload and directory ownership outlive this owner, its pins and returned
// pages; each read borrows its work input until joined completion.
class local_root_owner final : public runtime::shard_affine {
public:
    local_root_owner(const local_root_owner&) = delete;
    local_root_owner& operator=(const local_root_owner&) = delete;
    ~local_root_owner();
    [[nodiscard]] runtime::result<local_root_pin> pin();
    void retire() noexcept;
    [[nodiscard]] seastar::future<runtime::result<void>> close();
    [[nodiscard]] local_root_reference reference() const;
    template<runtime::file_system_backend Backend, local_directory_owner Owner>
    [[nodiscard]] static seastar::future<
      runtime::result<std::unique_ptr<local_root_owner>>>
    open(
      Backend&,
      Owner&,
      const local_device_spec&,
      std::uint32_t,
      local_root_reference,
      local_bundle_context,
      workload_budget&,
      local_store_io_limits,
      codec::cooperative_work&,
      local_reader_limits = {});

private:
    [[nodiscard]] static std::unique_ptr<local_root_owner> adopt(
      workload_reservation&,
      runtime::file&,
      std::optional<runtime::file>&,
      local_bundle&,
      workload_budget&,
      local_reader_limits);
    local_root_owner();
    seastar::lw_shared_ptr<detail::local_root_state> state_;
};

namespace detail {
// Exact bounded read against an already retained checked file, including
// size/position arithmetic. No hidden open and no descriptor lifecycle change.
[[nodiscard]] seastar::future<runtime::result<bytes::fragmented_buffer>>
read_local_extent(
  runtime::file&, runtime::file_position, byte_count, codec::cooperative_work&);
[[nodiscard]] seastar::future<runtime::result<void>> validate_local_root_page(
  const local_bundle&,
  page_ordinal,
  bytes::fragmented_buffer&,
  codec::cooperative_work&);
} // namespace detail

template<runtime::file_system_backend Backend, local_directory_owner Owner>
seastar::future<runtime::result<std::unique_ptr<local_root_owner>>>
local_root_owner::open(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  std::uint32_t shard,
  local_root_reference reference,
  local_bundle_context context,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work,
  local_reader_limits readers) {
    if (auto valid = limits.validate(); !valid)
        co_return runtime::failure(valid.error());
    if (auto valid = readers.validate(); !valid)
        co_return runtime::failure(valid.error());
    auto path = local_bundle_path(spec, shard, reference, context);
    if (!path) co_return runtime::failure(path.error());
    if (reference.bytes() > limits.operation_bytes)
        co_return runtime::failure(
          detail::path_error(errc::resource_exhausted));
    auto held = budget.try_reserve(
      byte_count{
        limits.operation_bytes.value() + limits.execution_bytes.value()
        + 32768});
    if (!held) co_return runtime::failure(held.error());
    if (
      auto handles = held->try_acquire_handles(
        reference.kind() == local_root_kind::sealed_retry ? 2 : 1);
      !handles)
        co_return runtime::failure(handles.error());
    auto valid = co_await ownership.validate(spec);
    if (!valid) co_return runtime::failure(valid.error());
    std::optional<runtime::file> page_file, data_file;
    std::optional<local_bundle> bundle;
    runtime::first_failure failed;
    std::unique_ptr<local_root_owner> output;
    try {
        do {
            valid = co_await inspect_local_path(
              files, spec.root, *path, runtime::file_kind::regular, work);
            if (!valid) {
                failed.observe(valid);
                break;
            }
            auto opened = co_await files.open(
              *path, {.close_policy = runtime::file_close_policy::checked});
            if (!opened) {
                failed.observe(opened);
                break;
            }
            page_file.emplace(std::move(*opened));
            runtime::file* root_file = &*page_file;
            if (reference.kind() == local_root_kind::sealed_retry) {
                const auto& expected = std::get<footer_expectation>(context);
                auto data_path = local_paths::make(spec.root)->segment_file(
                  shard,
                  {expected.history.segment.segment(),
                   expected.history.segment.generation()},
                  local_segment_file::data);
                if (!data_path) {
                    failed.observe(data_path);
                    break;
                }
                valid = co_await inspect_local_path(
                  files,
                  spec.root,
                  *data_path,
                  runtime::file_kind::regular,
                  work);
                if (!valid) {
                    failed.observe(valid);
                    break;
                }
                auto data_opened = co_await files.open(
                  *data_path,
                  {.close_policy = runtime::file_close_policy::checked});
                if (!data_opened) {
                    failed.observe(data_opened);
                    break;
                }
                data_file.emplace(std::move(*data_opened));
                root_file = &*data_file;
            }
            auto raw = co_await detail::read_local_extent(
              *root_file, reference.position(), reference.bytes(), work);
            if (!raw) {
                failed.observe(raw);
                break;
            }
            auto frozen = co_await local_bundle::make(
              reference,
              std::move(context),
              std::move(*raw),
              budget,
              limits,
              work);
            if (!frozen) {
                failed.observe(frozen);
                break;
            }
            bundle.emplace(std::move(*frozen));
            auto size = co_await page_file->size();
            if (!size) {
                failed.observe(size);
                break;
            }
            if (*size != bundle->file_bytes().value()) {
                failed.observe(detail::path_error(errc::malformed_data));
                break;
            }
            local_bundle_verifier verifier{*bundle, work.policy()};
            runtime::file_position at{
              reference.kind() == local_root_kind::sealed_retry
                ? 0
                : reference.bytes().value()};
            for (const auto& ref : bundle->pages()) {
                if (ref.encoded_bytes() > limits.operation_bytes) {
                    failed.observe(
                      detail::path_error(errc::resource_exhausted));
                    break;
                }
                auto raw_page = co_await detail::read_local_extent(
                  *page_file, at, ref.encoded_bytes(), work);
                if (!raw_page) {
                    failed.observe(raw_page);
                    break;
                }
                valid = co_await verifier.next(*raw_page, work);
                if (!valid) {
                    failed.observe(valid);
                    break;
                }
                at = at.checked_add(ref.encoded_bytes()).value();
            }
            if (failed.failed()) break;
            valid = verifier.finish(work);
            if (!valid) {
                failed.observe(valid);
                break;
            }
            size = co_await page_file->size();
            if (!size) {
                failed.observe(size);
                break;
            }
            if (*size != at.value()) {
                failed.observe(detail::path_error(errc::wrong_context));
                break;
            }
            valid = co_await ownership.validate(spec);
            if (!valid) {
                failed.observe(valid);
                break;
            }
            output = local_root_owner::adopt(
              *held, *page_file, data_file, *bundle, budget, readers);
            page_file.reset();
            data_file.reset();
        } while (false);
    } catch (...) {
        failed.observe(std::current_exception());
    }
    for (auto* file : {&data_file, &page_file}) {
        if (*file) {
            try {
                failed.observe(co_await (*file)->close());
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
