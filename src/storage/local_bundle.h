#pragma once

#include "src/storage/local_metadata_file.h"
#include "src/storage/local_publication.h"
#include "src/storage/retry_format.h"
#include "src/storage/sparse_index_format.h"

namespace kwaque::storage {
using local_bundle_context = std::
  variant<sparse_index_context, footer_expectation, local_metadata_expectation>;
using local_bundle_root
  = std::variant<sparse_index_root, sealed_footer, local_metadata_record>;
class local_bundle_verifier;
// Resolve only from independently supplied context and reference, before I/O.
[[nodiscard]] runtime::result<runtime::file_path> local_bundle_path(
  const local_device_spec&,
  std::uint32_t shard,
  local_root_reference,
  const local_bundle_context&);

// One frozen root and at most 256 refs. This is not a reader-generation pin:
// no file remains open and no retirement/deletion authority is represented.
class local_bundle final {
public:
    local_bundle(local_bundle&&) noexcept = default;
    local_bundle& operator=(local_bundle&&) noexcept = delete;
    local_bundle(const local_bundle&) = delete;
    local_bundle& operator=(const local_bundle&) = delete;
    [[nodiscard]] static seastar::future<runtime::result<local_bundle>> make(
      local_root_reference,
      local_bundle_context,
      bytes::fragmented_buffer root,
      workload_budget&,
      local_store_io_limits,
      codec::cooperative_work&);
    [[nodiscard]] const local_bundle_root& root() const& noexcept {
        return root_;
    }
    const local_bundle_root& root() const&& = delete;
    [[nodiscard]] const local_bundle_context& context() const& noexcept {
        return context_;
    }
    const local_bundle_context& context() const&& = delete;
    [[nodiscard]] local_root_reference reference() const noexcept {
        return reference_;
    }
    [[nodiscard]] byte_count file_bytes() const noexcept { return file_bytes_; }
    [[nodiscard]] std::span<const page_ref> pages() const& noexcept;
    std::span<const page_ref> pages() const&& = delete;
    [[nodiscard]] runtime::result<runtime::file_path>
    path(const local_device_spec&, std::uint32_t shard) const;
    [[nodiscard]] local_store_io_limits limits() const noexcept {
        return limits_;
    }
    // Called after all pages have been checked and written. Sealed retry's root
    // is already in data, so its page-only file has no root slot to fill.
    [[nodiscard]] seastar::future<runtime::result<void>>
    write_root(runtime::file&);

private:
    friend class local_bundle_verifier;
    local_bundle(
      workload_reservation held,
      local_root_reference reference,
      local_bundle_context context,
      local_bundle_root root,
      bytes::fragmented_buffer bytes,
      byte_count file_bytes,
      local_store_io_limits limits)
      : held_(std::move(held))
      , reference_(reference)
      , context_(std::move(context))
      , root_(std::move(root))
      , bytes_(std::move(bytes))
      , file_bytes_(file_bytes)
      , limits_(limits) {}
    workload_reservation held_;
    local_root_reference reference_;
    local_bundle_context context_;
    local_bundle_root root_;
    bytes::fragmented_buffer bytes_;
    byte_count file_bytes_;
    local_store_io_limits limits_;
};

// Borrows the frozen root for one joined stream. Existing index/retry walkers
// retain their exact cross-page ordering checks; local pages carry the previous
// full BID/cursor through the same contextual decoder used elsewhere.
class local_bundle_verifier final {
public:
    local_bundle_verifier(const local_bundle&, codec::limits);
    local_bundle_verifier(local_bundle&&, codec::limits) = delete;
    local_bundle_verifier(const local_bundle&&, codec::limits) = delete;
    [[nodiscard]] seastar::future<runtime::result<void>>
    next(bytes::fragmented_buffer&, codec::cooperative_work&);
    [[nodiscard]] runtime::result<void> finish(codec::cooperative_work&);

private:
    const local_bundle& bundle_;
    std::optional<sparse_index_verifier> index_;
    std::optional<retry_summary_verifier> retry_;
    std::optional<model::batch_id> previous_retry_;
    std::optional<local_wal_cursor> previous_checkpoint_;
    std::uint32_t next_{0};
    bool failed_{false};
};

struct local_bundle_publication final {
    local_publication_outcome publication;
    // Present only after every page, exact source/file EOF and both durability
    // barriers succeeded. The pointer owner installs this in its own serialized
    // update, keeping dependency pins until that update is joined.
    std::optional<local_root_reference> reference;
};

// Source.next(work) returns one owning encoded page, then nullopt for exact
// EOF. It produces/adopts at most one <=64-KiB page under the admitted working
// budget; it cannot retain whole-object history or launch unjoined work. All
// encoded bytes are checked by the root's concrete format decoder before being
// written. Dependencies(work) retains independently admitted pins and
// establishes data/ footer or checkpoint-evidence readiness before this
// immutable file is exposed. The configured namespace was initialized already;
// segment/object parent creation belongs to its descriptor/publication owner.
template<
  runtime::file_system_backend Backend,
  local_directory_owner Owner,
  typename Source,
  typename Dependencies>
requires std::same_as<
  std::invoke_result_t<Dependencies&, codec::cooperative_work&>,
  seastar::future<runtime::result<void>>>
seastar::future<local_bundle_publication> publish_local_bundle(
  Backend& files,
  Owner& ownership,
  const local_device_spec& spec,
  std::uint32_t shard,
  local_bundle bundle,
  Source source,
  Dependencies dependencies,
  workload_budget& budget,
  codec::cooperative_work& work) {
    static_assert(sizeof(Source) + sizeof(Dependencies) <= 8192);
    local_bundle_publication output;
    auto path = bundle.path(spec, shard);
    if (!path) {
        output.publication.failure.observe(path);
        co_return output;
    }
    auto valid = co_await ownership.validate(spec);
    if (!valid) {
        output.publication.failure.observe(valid);
        co_return output;
    }
    valid = co_await dependencies(work);
    if (!valid) {
        output.publication.failure.observe(valid);
        co_return output;
    }
    valid = co_await ownership.validate(spec);
    if (!valid) {
        output.publication.failure.observe(valid);
        co_return output;
    }
    const auto split = path->value().rfind('/');
    const auto parent
      = runtime::file_path::make(path->value().substr(0, split)).value();
    const auto name
      = runtime::file_name::make(path->value().substr(split + 1)).value();
    const auto generation = local_publication_generation::make(
                              bundle.reference().sequence().value())
                              .value();
    local_file_publisher<Backend> publisher{
      files,
      budget,
      {spec.shard_owner(shard).value(),
       spec.root,
       parent,
       name,
       runtime::file_rename_policy::no_replace,
       {}}};
    // Named callback/captures remain in this frame across every suspension.
    auto writer = [&bundle, &source](
                    runtime::file& file, codec::cooperative_work& execution)
      -> seastar::future<runtime::result<void>> {
        local_bundle_verifier verifier{bundle, execution.policy()};
        auto position = runtime::file_position{
          bundle.reference().kind() == local_root_kind::sealed_retry
            ? 0
            : bundle.reference().bytes().value()};
        for (const auto& ref : bundle.pages()) {
            auto ready = co_await detail::path_checkpoint(execution);
            if (!ready) co_return ready;
            auto page = co_await source.next(execution);
            if (!page) co_return runtime::failure(page.error());
            if (!*page)
                co_return runtime::failure(
                  detail::path_error(errc::malformed_data));
            if (
              (**page).size() != ref.encoded_bytes()
              || (**page).fragment_count() > 1024)
                co_return runtime::failure(
                  detail::path_error(errc::wrong_context));
            ready = co_await verifier.next(**page, execution);
            if (!ready) co_return ready;
            const auto length = (**page).size();
            auto written = co_await file.write(position, std::move(**page));
            if (!written) co_return runtime::failure(written.error());
            if (*written != length)
                co_return runtime::failure(
                  detail::path_error(errc::io_failure));
            auto next = position.checked_add(length);
            if (!next)
                co_return runtime::failure(
                  detail::path_error(errc::out_of_range));
            position = *next;
        }
        auto extra = co_await source.next(execution);
        if (!extra) co_return runtime::failure(extra.error());
        if (*extra)
            co_return runtime::failure(
              detail::path_error(errc::malformed_data));
        auto complete = verifier.finish(execution);
        if (!complete) co_return complete;
        if (position.value() != bundle.file_bytes().value())
            co_return runtime::failure(
              detail::path_error(errc::malformed_data));
        co_return co_await bundle.write_root(file);
    };
    try {
        output.publication = co_await publisher.publish_stream(
          {spec.shard_owner(shard).value(), generation, {}},
          std::move(writer),
          bundle.file_bytes(),
          bundle.limits().operation_bytes,
          work);
    } catch (...) {
        output.publication.failure.observe(std::current_exception());
    }
    try {
        output.publication.failure.observe(co_await publisher.close());
    } catch (...) {
        output.publication.failure.observe(std::current_exception());
    }
    if (
      !output.publication.failure.failed()
      && output.publication.disposition
           == local_publication_disposition::durable)
        output.reference = bundle.reference();
    co_return output;
}
} // namespace kwaque::storage
