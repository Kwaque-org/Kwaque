#include "src/storage/local_root.h"

namespace kwaque::storage {
namespace detail {
struct local_root_state final : runtime::shard_affine {
    local_root_state(
      workload_reservation& reservation,
      runtime::file& pages,
      std::optional<runtime::file>& data,
      local_bundle& value,
      workload_budget& workload,
      local_reader_limits bounds) noexcept
      : held(std::move(reservation))
      , bundle(std::move(value))
      , page_file(std::move(pages))
      , data_file(std::move(data))
      , budget(workload)
      , limits(bounds) {
        runtime::file_position at{
          bundle.reference().kind() == local_root_kind::sealed_retry
            ? 0
            : bundle.reference().bytes().value()};
        for (const auto& page : bundle.pages()) {
            offsets[page.ordinal().value()] = at;
            at = at.checked_add(page.encoded_bytes()).value();
        }
    }
    ~local_root_state() {
        assert_current();
        KWAQUE_INVARIANT(
          invariant_id{"KQ-ROOT-DRAINED"},
          closed,
          "root destroyed before joined close");
    }
    workload_reservation held;
    local_bundle bundle;
    runtime::file page_file;
    std::optional<runtime::file> data_file;
    workload_budget& budget;
    local_reader_limits limits;
    std::array<runtime::file_position, maximum_object_pages> offsets;
    seastar::gate readers;
    runtime::first_failure failure;
    bool retired{false}, closing{false}, closed{false};
};
seastar::future<runtime::result<bytes::fragmented_buffer>> read_local_extent(
  runtime::file& file,
  runtime::file_position position,
  byte_count length,
  codec::cooperative_work& work) {
    if (
      length.value() == 0 || length.value() > 65536
      || !position.checked_add(length))
        co_return runtime::failure(path_error(errc::invalid_argument));
    if (auto ready = co_await path_checkpoint(work); !ready)
        co_return runtime::failure(ready.error());
    auto size = co_await file.size();
    if (!size) co_return runtime::failure(size.error());
    if (position.checked_add(length)->value() > *size)
        co_return runtime::failure(path_error(errc::truncated_data));
    auto read = co_await file.read(position, length);
    if (!read) co_return runtime::failure(read.error());
    if (read->data().size() != length)
        co_return runtime::failure(path_error(errc::truncated_data));
    co_return std::move(*read).take_data();
}
seastar::future<runtime::result<void>> validate_local_root_page(
  const local_bundle& bundle,
  page_ordinal ordinal,
  bytes::fragmented_buffer& bytes,
  codec::cooperative_work& work) {
    if (ordinal.value() >= bundle.pages().size())
        co_return runtime::failure(path_error(errc::out_of_range));
    const auto ref = bundle.pages()[ordinal.value()];
    if (bytes.size() != ref.encoded_bytes())
        co_return runtime::failure(path_error(errc::wrong_context));
    // Caller reserves the original input and a full decoder/share budget
    // independently. Do not linearize or derive the expected SHA from input.
    bytes::fragmented_buffer_parser input{bytes.share()};
    auto memory = metadata_file_budget(input, bundle.limits(), work);
    if (!memory) co_return runtime::failure(path_error(memory.error().code()));
    if (const auto* index = std::get_if<sparse_index_root>(&bundle.root())) {
        auto value = co_await decode_sparse_index_page(
          input,
          *index,
          ordinal,
          *memory,
          work,
          {},
          codec::input_boundary::complete);
        if (!value)
            co_return runtime::failure(path_error(value.error().code()));
    } else if (const auto* retry = std::get_if<sealed_footer>(&bundle.root())) {
        auto value = co_await decode_retry_page(
          input,
          *retry,
          ordinal,
          *memory,
          work,
          {},
          codec::input_boundary::complete);
        if (!value)
            co_return runtime::failure(path_error(value.error().code()));
    } else {
        auto expected = std::get<local_metadata_expectation>(bundle.context());
        expected.header = local_metadata_header::make(
                            bundle.reference().kind()
                                == local_root_kind::checkpoint
                              ? local_metadata_kind::checkpoint_page
                              : local_metadata_kind::completed_retry_page,
                            expected.header.owner(),
                            expected.header.generation())
                            .value();
        expected.digest = ref.digest();
        expected.encoded_bytes = ref.encoded_bytes();
        expected.page = ref;
        auto value = co_await decode_local_metadata(
          input, expected, *memory, work, {}, codec::input_boundary::complete);
        if (!value)
            co_return runtime::failure(path_error(value.error().code()));
    }
    if (!input.at_end())
        co_return runtime::failure(path_error(errc::malformed_data));
    co_return runtime::result<void>{};
}
} // namespace detail
local_root_owner::local_root_owner() = default;
local_root_pin::local_root_pin(local_root_pin&&) noexcept = default;
local_root_pin::local_root_pin(
  workload_reservation held,
  seastar::lw_shared_ptr<detail::local_root_state> state,
  seastar::gate::holder holder)
  : held_(std::move(held))
  , state_(std::move(state))
  , holder_(std::move(holder)) {}
local_root_owner::~local_root_owner() {
    assert_current();
    KWAQUE_INVARIANT(
      invariant_id{"KQ-ROOT-OWNER-DRAINED"},
      !state_ || state_->closed,
      "root owner destroyed before joined close");
}
std::unique_ptr<local_root_owner> local_root_owner::adopt(
  workload_reservation& held,
  runtime::file& pages,
  std::optional<runtime::file>& data,
  local_bundle& bundle,
  workload_budget& budget,
  local_reader_limits limits) {
    // Allocate the wrapper before moving any resource into shared state.
    auto owner = std::unique_ptr<local_root_owner>{new local_root_owner};
    owner->state_ = seastar::make_lw_shared<detail::local_root_state>(
      held, pages, data, bundle, budget, limits);
    return owner;
}
runtime::result<local_root_pin> local_root_owner::pin() {
    assert_current();
    if (state_->retired)
        return runtime::failure(detail::path_error(errc::closed));
    if (state_->readers.get_count() >= state_->limits.maximum_pins)
        return runtime::failure(detail::path_error(errc::queue_full));
    auto held = state_->budget.try_reserve(byte_count{512});
    if (!held) return runtime::failure(held.error());
    return local_root_pin{std::move(*held), state_, state_->readers.hold()};
}
void local_root_owner::retire() noexcept {
    assert_current();
    state_->retired = true;
}
local_root_reference local_root_owner::reference() const {
    assert_current();
    return state_->bundle.reference();
}
seastar::future<runtime::result<void>> local_root_owner::close() {
    assert_current();
    if (state_->closed) co_return state_->failure.outcome();
    if (state_->closing)
        co_return runtime::failure(detail::path_error(errc::queue_full));
    retire();
    state_->closing = true;
    co_await state_->readers.close();
    if (state_->data_file) {
        try {
            state_->failure.observe(co_await state_->data_file->close());
        } catch (...) {
            state_->failure.observe(std::current_exception());
        }
    }
    try {
        state_->failure.observe(co_await state_->page_file.close());
    } catch (...) {
        state_->failure.observe(std::current_exception());
    }
    state_->closed = true;
    co_return state_->failure.outcome();
}
local_root_pin::~local_root_pin() {
    if (state_) state_->assert_current();
}
runtime::result<local_root_pin> local_root_pin::share() const {
    state_->assert_current();
    if (state_->readers.get_count() >= state_->limits.maximum_pins)
        return runtime::failure(detail::path_error(errc::queue_full));
    auto held = state_->budget.try_reserve(byte_count{512});
    if (!held) return runtime::failure(held.error());
    return local_root_pin{std::move(*held), state_, holder_};
}
local_root_reference local_root_pin::reference() const {
    state_->assert_current();
    return state_->bundle.reference();
}
const local_bundle_root& local_root_pin::metadata() const& {
    state_->assert_current();
    return state_->bundle.root();
}
runtime::result<runtime::file_position>
local_root_pin::position(page_ordinal ordinal) const {
    state_->assert_current();
    if (ordinal.value() >= state_->bundle.pages().size())
        return runtime::failure(detail::path_error(errc::out_of_range));
    return state_->offsets[ordinal.value()];
}
namespace {
seastar::future<runtime::result<local_root_page>> read_root_page(
  local_root_pin pin,
  seastar::lw_shared_ptr<detail::local_root_state> state,
  page_ordinal ordinal,
  workload_reservation held,
  codec::cooperative_work& work) {
    const auto at = state->offsets[ordinal.value()];
    const auto reference = state->bundle.pages()[ordinal.value()];
    auto raw = co_await detail::read_local_extent(
      state->page_file, at, reference.encoded_bytes(), work);
    if (!raw) co_return runtime::failure(raw.error());
    auto valid = co_await detail::validate_local_root_page(
      state->bundle, ordinal, *raw, work);
    if (!valid) co_return runtime::failure(valid.error());
    co_return local_root_page{
      std::move(held), std::move(pin), reference, at, std::move(*raw)};
}
} // namespace
seastar::future<runtime::result<local_root_page>> local_root_pin::read(
  page_ordinal ordinal, codec::cooperative_work& work) const {
    state_->assert_current();
    auto reject = [](runtime::operation_error error) {
        return seastar::make_ready_future<runtime::result<local_root_page>>(
          runtime::failure(error));
    };
    if (auto at = position(ordinal); !at) return reject(at.error());
    auto limits = state_->bundle.limits();
    // Original read buffer and decoder/share each have a full operation budget.
    auto held = state_->budget.try_reserve(
      byte_count{
        2U * limits.operation_bytes.value() + limits.execution_bytes.value()});
    if (!held) return reject(held.error());
    auto pin = share();
    if (!pin) return reject(pin.error());
    return read_root_page(
      std::move(*pin), state_, ordinal, std::move(*held), work);
}
} // namespace kwaque::storage
