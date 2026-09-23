#include "src/storage/local_bundle.h"

namespace kwaque::storage {
namespace {
std::span<const page_ref> root_pages(const local_bundle_root& root) {
    return std::visit(
      [](const auto& value) -> std::span<const page_ref> {
          using T = std::remove_cvref_t<decltype(value)>;
          if constexpr (std::same_as<T, local_metadata_record>) {
              if (
                const auto* checkpoint = std::get_if<local_checkpoint_root>(
                  &value.payload()))
                  return checkpoint->pages;
              return std::get<local_completed_retry_root>(value.payload())
                .pages;
          } else
              return value.pages();
      },
      root);
}
} // namespace
std::span<const page_ref> local_bundle::pages() const& noexcept {
    return root_pages(root_);
}
seastar::future<runtime::result<local_bundle>> local_bundle::make(
  local_root_reference reference,
  local_bundle_context context,
  bytes::fragmented_buffer bytes,
  workload_budget& budget,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    if (auto valid = limits.validate(); !valid)
        co_return runtime::failure(valid.error());
    if (bytes.size() != reference.bytes() || bytes.fragment_count() > 1024)
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    auto held = budget.try_reserve_buffer(
      bytes,
      byte_count{
        limits.operation_bytes.value() + limits.execution_bytes.value()
        + 32768});
    if (!held) co_return runtime::failure(held.error());
    bytes::fragmented_buffer_parser input{bytes.share()};
    auto memory = detail::metadata_file_budget(input, limits, work);
    if (!memory)
        co_return runtime::failure(detail::path_error(memory.error().code()));
    std::optional<local_bundle_root> root;
    if (const auto* index = std::get_if<sparse_index_context>(&context)) {
        if (
          reference.kind() != local_root_kind::index
          || reference.position().value() != 0)
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        auto decoded = co_await decode_sparse_index_root(
          input,
          *index,
          reference.digest(),
          *memory,
          work,
          {},
          codec::input_boundary::complete);
        if (!decoded)
            co_return runtime::failure(
              detail::path_error(decoded.error().code()));
        root.emplace(std::move(decoded->value));
    } else if (const auto* retry = std::get_if<footer_expectation>(&context)) {
        if (
          reference.kind() != local_root_kind::sealed_retry
          || reference.position() != retry->position)
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        auto decoded = co_await decode_sealed_footer(
          input,
          *retry,
          reference.digest(),
          *memory,
          work,
          {},
          codec::input_boundary::complete);
        if (!decoded)
            co_return runtime::failure(
              detail::path_error(decoded.error().code()));
        root.emplace(std::move(decoded->value));
    } else {
        const auto& local = std::get<local_metadata_expectation>(context);
        const auto kind = reference.kind();
        if (
          (kind != local_root_kind::checkpoint
           && kind != local_root_kind::completed_retry_snapshot)
          || reference.position().value() != 0
          || local.header.kind()
               != (kind == local_root_kind::checkpoint ? local_metadata_kind::checkpoint_root : local_metadata_kind::completed_retry_root)
          || local.header.generation().value() != reference.sequence().value()
          || local.digest != reference.digest()
          || local.encoded_bytes != reference.bytes())
            co_return runtime::failure(detail::path_error(errc::wrong_context));
        auto decoded = co_await decode_local_metadata(
          input, local, *memory, work, {}, codec::input_boundary::complete);
        if (!decoded)
            co_return runtime::failure(
              detail::path_error(decoded.error().code()));
        root.emplace(std::move(decoded->value));
    }
    if (
      !input.at_end() || root_pages(*root).size() != reference.pages().value())
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    auto length = reference.kind() == local_root_kind::sealed_retry
                    ? byte_count{}
                    : reference.bytes();
    for (const auto& page : root_pages(*root)) {
        if (auto ready = co_await detail::path_checkpoint(work); !ready)
            co_return runtime::failure(ready.error());
        auto next = length.checked_add(page.encoded_bytes());
        if (!next)
            co_return runtime::failure(detail::path_error(errc::out_of_range));
        length = *next;
    }
    if (reference.kind() == local_root_kind::sealed_retry)
        bytes = bytes::fragmented_buffer{};
    co_return local_bundle{
      std::move(*held),
      reference,
      std::move(context),
      std::move(*root),
      std::move(bytes),
      length,
      limits};
}
runtime::result<runtime::file_path> local_bundle_path(
  const local_device_spec& spec,
  std::uint32_t shard,
  local_root_reference reference,
  const local_bundle_context& context) {
    if (auto valid = validate_local_device_spec(spec); !valid)
        return runtime::failure(valid.error());
    auto owner = spec.shard_owner(shard);
    if (!owner)
        return runtime::failure(detail::path_error(errc::wrong_context));
    if (
      const auto* local = std::get_if<local_metadata_expectation>(&context);
      local
      && ((reference.kind() != local_root_kind::checkpoint
           && reference.kind() != local_root_kind::completed_retry_snapshot)
          || local->header.kind()
               != (reference.kind() == local_root_kind::checkpoint ? local_metadata_kind::checkpoint_root : local_metadata_kind::completed_retry_root)
          || local->header.generation().value() != reference.sequence().value()
          || local->digest != reference.digest()
          || local->encoded_bytes != reference.bytes()
          || local->header.owner() != *owner
          || local->alignment != spec.identity.metadata_alignment))
        return runtime::failure(detail::path_error(errc::wrong_context));
    const auto alignment = std::visit(
      [](const auto& expected) -> storage_alignment {
          using T = std::remove_cvref_t<decltype(expected)>;
          if constexpr (std::same_as<T, sparse_index_context>)
              return expected.alignment();
          else if constexpr (std::same_as<T, footer_expectation>)
              return expected.history.alignment;
          else
              return expected.alignment;
      },
      context);
    if (!reference.validate_alignment(alignment))
        return runtime::failure(detail::path_error(errc::wrong_context));
    const auto paths = local_paths::make(spec.root).value();
    if (reference.kind() == local_root_kind::checkpoint) {
        const auto* local = std::get_if<local_metadata_expectation>(&context);
        if (
          !local || local->header.kind() != local_metadata_kind::checkpoint_root
          || local->header.generation().value() != reference.sequence().value())
            return runtime::failure(detail::path_error(errc::wrong_context));
        if (!spec.controls())
            return runtime::failure(detail::path_error(errc::wrong_context));
        return paths.sequence_file(
          shard, local_sequence_file::checkpoint, reference.sequence().value());
    }
    std::optional<segment_context> sc;
    if (const auto* index = std::get_if<sparse_index_context>(&context)) {
        if (reference.kind() != local_root_kind::index)
            return runtime::failure(detail::path_error(errc::wrong_context));
        sc = index->segment();
    } else if (const auto* retry = std::get_if<footer_expectation>(&context)) {
        if (
          reference.kind() != local_root_kind::sealed_retry
          || reference.position() != retry->position)
            return runtime::failure(detail::path_error(errc::wrong_context));
        sc = retry->history.segment;
    } else
        sc = std::get<local_metadata_expectation>(context).segment;
    if (!spec.stores_data() || !sc || sc->cluster() != spec.owner.cluster())
        return runtime::failure(detail::path_error(errc::wrong_context));
    return paths.object(
      shard, {sc->segment(), sc->generation()}, reference.sequence());
}
runtime::result<runtime::file_path>
local_bundle::path(const local_device_spec& spec, std::uint32_t shard) const {
    return local_bundle_path(spec, shard, reference_, context_);
}
seastar::future<runtime::result<void>>
local_bundle::write_root(runtime::file& file) {
    if (reference_.kind() == local_root_kind::sealed_retry)
        co_return runtime::result<void>{};
    auto written = co_await file.write(
      runtime::file_position{}, std::move(bytes_));
    if (!written) co_return runtime::failure(written.error());
    if (*written != reference_.bytes())
        co_return runtime::failure(detail::path_error(errc::io_failure));
    co_return runtime::result<void>{};
}
local_bundle_verifier::local_bundle_verifier(
  const local_bundle& bundle, codec::limits policy)
  : bundle_(bundle) {
    if (const auto* index = std::get_if<sparse_index_root>(&bundle.root_))
        index_.emplace(*index, policy);
    if (const auto* retry = std::get_if<sealed_footer>(&bundle.root_))
        retry_.emplace(*retry, policy);
}
seastar::future<runtime::result<void>> local_bundle_verifier::next(
  bytes::fragmented_buffer& bytes, codec::cooperative_work& work) {
    if (failed_ || next_ >= bundle_.pages().size())
        co_return runtime::failure(detail::path_error(errc::closed));
    failed_ = true;
    const auto reference = bundle_.pages()[next_];
    if (
      bytes.size() != reference.encoded_bytes()
      || bytes.fragment_count() > 1024)
        co_return runtime::failure(detail::path_error(errc::wrong_context));
    // The original page remains alive for write while the parser owns a share.
    // Admit both before creating that alias; conservatively double-charge
    // backing rather than silently omit the source descriptors/control words.
    const auto cost = bytes.allocation_cost(bundle_.limits_.charge);
    if (!cost)
        co_return runtime::failure(
          detail::path_error(errc::resource_exhausted));
    const auto metadata = cost->descriptors.checked_add(cost->share_controls);
    const auto total = metadata ? metadata->checked_add(cost->backing)
                                : std::nullopt;
    if (
      !metadata || !total
      || cost->largest_allocation.value() > maximum_contiguous_allocation_bytes
      || *total > bundle_.limits_.operation_bytes
      || *metadata > bundle_.limits_.metadata_bytes)
        co_return runtime::failure(
          detail::path_error(errc::resource_exhausted));
    auto remaining = bundle_.limits_;
    remaining.operation_bytes
      = remaining.operation_bytes.checked_sub(*total).value();
    remaining.metadata_bytes
      = remaining.metadata_bytes.checked_sub(*metadata).value();
    bytes::fragmented_buffer_parser input{bytes.share()};
    auto memory = detail::metadata_file_budget(input, remaining, work);
    if (!memory)
        co_return runtime::failure(detail::path_error(memory.error().code()));
    if (index_) {
        auto page = co_await index_->next(
          input, *memory, work, {}, codec::input_boundary::complete);
        if (!page)
            co_return runtime::failure(detail::path_error(page.error().code()));
    } else if (retry_) {
        auto page = co_await retry_->next(
          input, *memory, work, {}, codec::input_boundary::complete);
        if (!page)
            co_return runtime::failure(detail::path_error(page.error().code()));
    } else {
        auto expected = std::get<local_metadata_expectation>(bundle_.context_);
        const bool checkpoint = bundle_.reference_.kind()
                                == local_root_kind::checkpoint;
        expected.header = local_metadata_header::make(
                            checkpoint
                              ? local_metadata_kind::checkpoint_page
                              : local_metadata_kind::completed_retry_page,
                            expected.header.owner(),
                            expected.header.generation())
                            .value();
        expected.digest = reference.digest();
        expected.encoded_bytes = reference.encoded_bytes();
        expected.page = reference;
        expected.previous_retry = previous_retry_;
        expected.previous_checkpoint_end = previous_checkpoint_;
        auto page = co_await decode_local_metadata(
          input, expected, *memory, work, {}, codec::input_boundary::complete);
        if (!page)
            co_return runtime::failure(detail::path_error(page.error().code()));
        if (checkpoint) {
            const auto& entries
              = std::get<local_checkpoint_page>(page->value.payload()).entries;
            const auto& root = std::get<local_checkpoint_root>(
              std::get<local_metadata_record>(bundle_.root_).payload());
            if (next_ == 0 && entries.front().begin != root.begin)
                co_return runtime::failure(
                  detail::path_error(errc::wrong_context));
            auto order = entries.back().end.compare(
              expected.header.owner(), root.end, expected.header.owner());
            if (!order || *order > 0)
                co_return runtime::failure(
                  detail::path_error(errc::wrong_context));
            previous_checkpoint_ = entries.back().end;
        } else {
            previous_retry_ = std::get<local_completed_retry_page>(
                                page->value.payload())
                                .entries.back()
                                .id();
        }
    }
    if (!input.at_end())
        co_return runtime::failure(detail::path_error(errc::malformed_data));
    ++next_;
    failed_ = false;
    co_return runtime::result<void>{};
}
runtime::result<void>
local_bundle_verifier::finish(codec::cooperative_work& work) {
    if (failed_ || next_ != bundle_.pages().size())
        return runtime::failure(detail::path_error(errc::malformed_data));
    failed_ = true;
    if (index_) {
        auto done = index_->finish(work);
        if (!done)
            return runtime::failure(detail::path_error(done.error().code()));
    } else if (retry_) {
        auto done = retry_->finish(work);
        if (!done)
            return runtime::failure(detail::path_error(done.error().code()));
    } else if (bundle_.reference_.kind() == local_root_kind::checkpoint) {
        const auto& root = std::get<local_checkpoint_root>(
          std::get<local_metadata_record>(bundle_.root_).payload());
        if (
          previous_checkpoint_ ? *previous_checkpoint_ != root.end
                               : root.begin != root.end)
            return runtime::failure(detail::path_error(errc::wrong_context));
    }
    if (auto ready = work.poll(); !ready)
        return runtime::failure(detail::path_error(ready.error().code()));
    return {};
}
} // namespace kwaque::storage
