#include "src/runtime/file.h"

#include "src/base/invariant.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <new>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace kwaque::runtime {

namespace {

constexpr invariant_id file_move_invariant{"KQ-FILE-MOVE-IDLE"};
constexpr invariant_id file_stopped_invariant{"KQ-FILE-CLOSED"};
constexpr invariant_id file_gate_invariant{"KQ-FILE-GATE-OPEN"};

operation_error file_error(errc code) noexcept {
    return operation_error{code, operation_kind::file};
}

bool contains_nul(const std::string& value) noexcept {
    return std::find(value.begin(), value.end(), '\0') != value.end();
}

errc map_system_error(const std::error_code& error) noexcept {
    if (error == std::errc::operation_canceled) {
        return errc::aborted;
    }
    if (error == std::errc::timed_out) {
        return errc::timed_out;
    }
    if (
      error == std::errc::no_space_on_device
      || error == std::errc::too_many_files_open
      || error == std::errc::too_many_files_open_in_system) {
        return errc::resource_exhausted;
    }
    if (error == std::errc::file_too_large) {
        return errc::out_of_range;
    }
    if (error == std::errc::invalid_argument) {
        return errc::invalid_argument;
    }
    return errc::io_failure;
}

operation_error file_error_from_exception(std::exception_ptr exception) {
    try {
        std::rethrow_exception(std::move(exception));
    } catch (const std::bad_alloc&) {
        throw;
    } catch (const seastar::cancelled_error&) {
        return file_error(errc::aborted);
    } catch (const std::system_error& error) {
        return file_error(map_system_error(error.code()));
    } catch (...) {
        return file_error(errc::io_failure);
    }
}

} // namespace

result<file_path> file_path::make(std::string value) noexcept {
    if (value.empty() || contains_nul(value)) {
        return failure(file_error(errc::invalid_argument));
    }
    if (value.size() > maximum_file_path_bytes) {
        return failure(file_error(errc::out_of_range));
    }
    return file_path{std::move(value)};
}

result<file_name> file_name::make(std::string value) noexcept {
    if (
      value.empty() || value == "." || value == ".."
      || value.find('/') != std::string::npos || contains_nul(value)) {
        return failure(file_error(errc::invalid_argument));
    }
    if (value.size() > maximum_file_name_bytes) {
        return failure(file_error(errc::out_of_range));
    }
    return file_name{std::move(value)};
}

result<void> directory_listing_limits::validate() const noexcept {
    if (maximum_entries.value() == 0 || maximum_name_bytes.value() == 0) {
        return failure(file_error(errc::invalid_argument));
    }
    if (
      maximum_entries.value() > maximum_directory_entries
      || maximum_name_bytes > kwaque::runtime::maximum_directory_name_bytes) {
        return failure(file_error(errc::out_of_range));
    }
    return {};
}

result<directory_listing> directory_listing::make(
  std::vector<directory_entry> entries,
  directory_listing_limits limits) noexcept {
    if (auto valid = limits.validate(); !valid) {
        return failure(valid.error());
    }
    if (entries.size() > limits.maximum_entries.value()) {
        return failure(file_error(errc::resource_exhausted));
    }
    std::uint64_t name_bytes = 0;
    for (const auto& entry : entries) {
        const auto size = static_cast<std::uint64_t>(entry.name.value().size());
        if (size > limits.maximum_name_bytes.value() - name_bytes) {
            return failure(file_error(errc::resource_exhausted));
        }
        name_bytes += size;
    }
    return directory_listing{std::move(entries)};
}

result<void> file_open_options::validate() const noexcept {
    const auto access_value = static_cast<std::uint8_t>(access);
    if (
      access_value > static_cast<std::uint8_t>(file_access::read_write)
      || (exclusive && !create)
      || (truncate && access == file_access::read_only)
      || permissions > 0777U) {
        return failure(file_error(errc::invalid_argument));
    }
    return {};
}

result<file_read_result> file_read_result::make(
  bytes::fragmented_buffer data, bool eof, byte_count maximum_bytes) noexcept {
    if (maximum_bytes.value() == 0) {
        return failure(file_error(errc::invalid_argument));
    }
    if (maximum_bytes > maximum_file_io_bytes) {
        return failure(file_error(errc::out_of_range));
    }
    if (data.size() > maximum_bytes) {
        return failure(file_error(errc::out_of_range));
    }
    if (data.empty() && !eof) {
        return failure(file_error(errc::invalid_argument));
    }
    return file_read_result{std::move(data), eof};
}

file::file(seastar::file&& native_file)
  : native_file_(std::move(native_file)) {
    if (!native_file_) {
        throw std::invalid_argument("file requires an open native handle");
    }
}

owner_shard file::prepare_move(file& other) noexcept {
    other.owner_.assert_current();
    KWAQUE_INVARIANT(
      file_move_invariant,
      other.operations_.get_count() == 0 && other.state_ != file_state::closing,
      "file moved with an operation in progress");
    return other.owner_;
}

file::file(file&& other) noexcept
  : owner_(prepare_move(other))
  , native_file_(std::move(other.native_file_))
  , io_intent_(std::move(other.io_intent_))
  , operations_(std::move(other.operations_))
  , close_done_(std::move(other.close_done_))
  , state_(other.state_)
  , abort_requested_(other.abort_requested_) {
    other.state_ = file_state::closed;
    other.abort_requested_ = true;
    other.moved_from_ = true;
}

file::~file() {
    owner_.assert_current();
    KWAQUE_INVARIANT(
      file_stopped_invariant,
      state_ == file_state::closed && operations_.get_count() == 0,
      "file destroyed before close completed");
}

void file::request_abort() {
    owner_.assert_current();
    if (moved_from_ || state_ == file_state::closed || abort_requested_) {
        return;
    }
    abort_requested_ = true;
    io_intent_.cancel();
}

std::optional<operation_error> file::operation_rejection() const {
    owner_.assert_current();
    if (moved_from_ || state_ != file_state::open) {
        return file_error(errc::closed);
    }
    if (abort_requested_) {
        return file_error(errc::aborted);
    }
    return std::nullopt;
}

seastar::future<result<void>> file::flush() {
    if (auto rejected = operation_rejection()) {
        co_return failure(std::move(*rejected));
    }
    auto holder = operations_.try_hold();
    KWAQUE_INVARIANT(
      file_gate_invariant,
      holder.has_value(),
      "open file rejected operation gate entry");
    try {
        co_await native_file_.flush();
        co_return result<void>{};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(file_error_from_exception(std::current_exception()));
    }
}

seastar::future<result<void>> file::truncate(std::uint64_t size) {
    if (auto rejected = operation_rejection()) {
        co_return failure(std::move(*rejected));
    }
    auto holder = operations_.try_hold();
    KWAQUE_INVARIANT(
      file_gate_invariant,
      holder.has_value(),
      "open file rejected operation gate entry");
    try {
        co_await native_file_.truncate(size);
        co_return result<void>{};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(file_error_from_exception(std::current_exception()));
    }
}

seastar::future<result<std::uint64_t>> file::size() {
    if (auto rejected = operation_rejection()) {
        co_return failure(std::move(*rejected));
    }
    auto holder = operations_.try_hold();
    KWAQUE_INVARIANT(
      file_gate_invariant,
      holder.has_value(),
      "open file rejected operation gate entry");
    try {
        co_return co_await native_file_.size();
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(file_error_from_exception(std::current_exception()));
    }
}

seastar::future<result<void>> file::close() {
    owner_.assert_current();
    if (moved_from_) {
        return seastar::make_ready_future<result<void>>(result<void>{});
    }
    if (state_ == file_state::closing) {
        return close_done_->get_shared_future();
    }
    if (state_ == file_state::closed) {
        return close_done_ && close_done_->available()
                 ? close_done_->get_shared_future()
                 : seastar::make_ready_future<result<void>>(result<void>{});
    }

    try {
        close_done_.emplace();
    } catch (...) {
        return seastar::current_exception_as_future<result<void>>();
    }
    state_ = file_state::closing;
    abort_requested_ = true;
    io_intent_.cancel();
    auto completion = close_once().then_wrapped(
      [this](seastar::future<result<void>> closed) {
          state_ = file_state::closed;
          try {
              close_done_->set_value(closed.get());
          } catch (...) {
              close_done_->set_exception(std::current_exception());
          }
      });
    static_cast<void>(completion);
    return close_done_->get_shared_future();
}

seastar::future<result<void>> file::close_once() {
    co_await operations_.close();
    try {
        co_await native_file_.close();
        co_return result<void>{};
    } catch (const std::bad_alloc&) {
        throw;
    } catch (...) {
        co_return failure(file_error_from_exception(std::current_exception()));
    }
}

file_state file::state() const {
    owner_.assert_current();
    return state_;
}

bool file::abort_requested() const {
    owner_.assert_current();
    return abort_requested_;
}

} // namespace kwaque::runtime
