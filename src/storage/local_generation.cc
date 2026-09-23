#include "src/storage/local_generation.h"

#include "src/storage/page_internal.h"

namespace kwaque::storage {
namespace detail {
struct local_generation_state final : runtime::shard_affine {
    local_generation_state(
      workload_reservation& reservation,
      local_loaded_metadata& record,
      std::optional<runtime::file>& file,
      std::optional<local_boundary_metadata>& footer,
      std::array<std::unique_ptr<local_root_owner>, 4>& owners,
      workload_budget& workload,
      local_reader_limits bounds,
      std::optional<runtime::operation_error> rebuild) noexcept
      : held(std::move(reservation))
      , publication(std::move(record))
      , data(std::move(file))
      , boundary(std::move(footer))
      , roots(std::move(owners))
      , budget(workload)
      , limits(bounds)
      , index_rebuild(rebuild) {}
    ~local_generation_state() {
        assert_current();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-GENERATION-DRAINED"},
          closed,
          "generation destroyed before joined close");
    }
    workload_reservation held;
    local_loaded_metadata publication;
    std::optional<runtime::file> data;
    std::optional<local_boundary_metadata> boundary;
    std::array<std::unique_ptr<local_root_owner>, 4> roots;
    workload_budget& budget;
    local_reader_limits limits;
    std::optional<runtime::operation_error> index_rebuild;
    seastar::gate readers;
    runtime::first_failure failure;
    bool retired{false}, closing{false}, closed{false};
};
seastar::future<runtime::result<local_boundary_metadata>> read_local_boundary(
  runtime::file& file,
  segment_history_context history,
  local_footer_reference reference,
  local_store_io_limits limits,
  codec::cooperative_work& work) {
    if (auto valid = reference.validate_alignment(history.alignment); !valid)
        co_return runtime::failure(path_error(errc::wrong_context));
    auto raw = co_await read_local_extent(
      file, reference.position(), reference.bytes(), work);
    if (!raw) co_return runtime::failure(raw.error());
    auto hash = co_await hash_exact(*raw, work, {});
    if (!hash) co_return runtime::failure(path_error(hash.error().code()));
    if (*hash != reference.digest())
        co_return runtime::failure(path_error(errc::corrupt_data));
    bytes::fragmented_buffer_parser input{std::move(*raw)};
    auto memory = metadata_file_budget(input, limits, work);
    if (!memory) co_return runtime::failure(path_error(memory.error().code()));
    const footer_expectation expected{history, reference.position()};
    std::optional<local_boundary_metadata> result;
    if (reference.family() == 6) {
        auto value = co_await decode_durable_footer(
          input, expected, *memory, work, {}, codec::input_boundary::complete);
        if (!value)
            co_return runtime::failure(path_error(value.error().code()));
        result.emplace(*value);
    } else if (reference.family() == 7) {
        auto value = co_await decode_sealed_footer(
          input,
          expected,
          reference.digest(),
          *memory,
          work,
          {},
          codec::input_boundary::complete);
        if (!value)
            co_return runtime::failure(path_error(value.error().code()));
        result.emplace(std::move(value->value));
    } else
        co_return runtime::failure(path_error(errc::unsupported_format));
    if (!input.at_end())
        co_return runtime::failure(path_error(errc::malformed_data));
    co_return std::move(*result);
}
bool rebuildable_local_index_error(errc code) noexcept {
    return code == errc::not_found || code == errc::malformed_data
           || code == errc::truncated_data || code == errc::corrupt_data
           || code == errc::wrong_context;
}
runtime::result<void> validate_generation_root_context(
  const local_bundle_context& context, segment_history_context history) {
    const bool valid = std::visit(
      [&](const auto& expected) {
          using T = std::remove_cvref_t<decltype(expected)>;
          if constexpr (std::same_as<T, sparse_index_context>)
              return expected.segment() == history.segment
                     && expected.alignment() == history.alignment
                     && expected.profile() == history.profile;
          else if constexpr (std::same_as<T, footer_expectation>)
              return expected.history == history;
          else
              return expected.segment == history.segment
                     && expected.segment_alignment == history.alignment;
      },
      context);
    if (!valid) return runtime::failure(path_error(errc::wrong_context));
    return {};
}
runtime::result<void> validate_generation_root(
  const local_bundle_root& root,
  local_root_reference reference,
  segment_history_context history,
  const local_boundary_metadata& boundary) {
    auto wrong = [] {
        return runtime::failure(path_error(errc::wrong_context));
    };
    if (const auto* index = std::get_if<sparse_index_root>(&root)) {
        const auto context = index->context();
        const auto coverage = std::visit(
          [](const auto& b) -> storage::coverage {
              if constexpr (
                std::same_as<std::remove_cvref_t<decltype(b)>, durable_footer>)
                  return b.boundary().coverage;
              else
                  return b.coverage();
          },
          boundary);
        if (
          reference.kind() != local_root_kind::index
          || context.segment() != history.segment
          || context.alignment() != history.alignment
          || context.profile() != history.profile
          || context.coverage().logical() != coverage.logical()
          || context.coverage().physical() != coverage.physical()
          || context.coverage().bytes() != coverage.bytes())
            return wrong();
        if (
          const auto* sealed = std::get_if<sealed_footer>(&boundary);
          sealed && sealed->extent_digest() != context.digest())
            return wrong();
    } else if (const auto* sealed = std::get_if<sealed_footer>(&root)) {
        const auto* expected = std::get_if<sealed_footer>(&boundary);
        if (
          !expected || reference.kind() != local_root_kind::sealed_retry
          || sealed->location().history != history
          || sealed->digest() != expected->digest()
          || sealed->location().position != expected->location().position)
            return wrong();
    } else {
        const auto* snapshot = std::get_if<local_completed_retry_root>(
          &std::get<local_metadata_record>(root).payload());
        if (
          !snapshot
          || reference.kind() != local_root_kind::completed_retry_snapshot
          || snapshot->segment != history.segment)
            return wrong();
    }
    return {};
}
} // namespace detail
local_generation_owner::local_generation_owner() = default;
local_generation_pin::local_generation_pin(local_generation_pin&&) noexcept
  = default;
local_generation_pin::local_generation_pin(
  workload_reservation held,
  seastar::lw_shared_ptr<detail::local_generation_state> state,
  seastar::gate::holder holder)
  : held_(std::move(held))
  , state_(std::move(state))
  , holder_(std::move(holder)) {}
local_generation_owner::~local_generation_owner() {
    assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-GENERATION-OWNER-DRAINED"},
      !state_ || state_->closed,
      "generation owner destroyed before joined close");
}
std::unique_ptr<local_generation_owner> local_generation_owner::adopt(
  workload_reservation& held,
  local_loaded_metadata& publication,
  std::optional<runtime::file>& data,
  std::optional<local_boundary_metadata>& boundary,
  std::array<std::unique_ptr<local_root_owner>, 4>& roots,
  workload_budget& budget,
  local_reader_limits limits,
  std::optional<runtime::operation_error> rebuild) {
    auto owner = std::unique_ptr<local_generation_owner>{
      new local_generation_owner};
    owner->state_ = seastar::make_lw_shared<detail::local_generation_state>(
      held, publication, data, boundary, roots, budget, limits, rebuild);
    return owner;
}
runtime::result<local_generation_pin> local_generation_owner::pin() {
    assert_current();
    if (state_->retired)
        return runtime::failure(detail::path_error(errc::closed));
    if (state_->readers.get_count() >= state_->limits.maximum_pins)
        return runtime::failure(detail::path_error(errc::queue_full));
    auto held = state_->budget.try_reserve(byte_count{512});
    if (!held) return runtime::failure(held.error());
    return local_generation_pin{
      std::move(*held), state_, state_->readers.hold()};
}
void local_generation_owner::retire() noexcept {
    assert_current();
    state_->retired = true;
}
seastar::future<runtime::result<void>> local_generation_owner::close() {
    assert_current();
    if (state_->closed) co_return state_->failure.outcome();
    if (state_->closing)
        co_return runtime::failure(detail::path_error(errc::queue_full));
    retire();
    state_->closing = true;
    co_await state_->readers.close();
    for (std::size_t i = state_->roots.size(); i != 0; --i) {
        if (state_->roots[i - 1]) {
            try {
                state_->failure.observe(co_await state_->roots[i - 1]->close());
            } catch (...) {
                state_->failure.observe(std::current_exception());
            }
        }
    }
    if (state_->data) {
        try {
            state_->failure.observe(co_await state_->data->close());
        } catch (...) {
            state_->failure.observe(std::current_exception());
        }
    }
    state_->closed = true;
    co_return state_->failure.outcome();
}
local_generation_pin::~local_generation_pin() {
    if (state_) state_->assert_current();
}
runtime::result<local_generation_pin> local_generation_pin::share() const {
    state_->assert_current();
    if (state_->readers.get_count() >= state_->limits.maximum_pins)
        return runtime::failure(detail::path_error(errc::queue_full));
    auto held = state_->budget.try_reserve(byte_count{512});
    if (!held) return runtime::failure(held.error());
    return local_generation_pin{std::move(*held), state_, holder_};
}
local_publication_generation local_generation_pin::generation() const {
    state_->assert_current();
    return state_->publication.value.header().generation();
}
const local_object_publication& local_generation_pin::publication() const& {
    state_->assert_current();
    return std::get<local_object_publication>(
      state_->publication.value.payload());
}
const std::optional<local_boundary_metadata>&
local_generation_pin::boundary() const& {
    state_->assert_current();
    return state_->boundary;
}
runtime::result<local_root_pin>
local_generation_pin::root(local_root_kind kind) const {
    state_->assert_current();
    for (const auto& owner : state_->roots)
        if (owner && owner->reference().kind() == kind) return owner->pin();
    return runtime::failure(detail::path_error(errc::not_found));
}
} // namespace kwaque::storage

namespace kwaque::storage {
std::optional<runtime::operation_error>
local_generation_pin::index_rebuild_reason() const {
    state_->assert_current();
    return state_->index_rebuild;
}
} // namespace kwaque::storage
