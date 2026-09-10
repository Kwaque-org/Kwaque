#include "src/broker/crash_limiter.h"

#include "src/base/logging.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>

#include <crc32c/crc32c.h>
#include <sys/stat.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <fcntl.h>
#include <limits>
#include <string_view>
#include <system_error>

namespace kwaque::broker {

namespace {

constexpr std::string_view tracker_name{".kwaque-crash-loop"};
constexpr std::string_view metadata_header{"kwaque-crash-loop-v1\n"};
constexpr std::size_t metadata_limit = 160;
constexpr std::size_t io_alignment_limit = 128U * 1024U;
constexpr std::uint64_t reset_milliseconds = 60U * 60U * 1000U;
static_assert(
  metadata_header.size() + 20 + 1 + 20 + 1 + 64 + 1 + 8 + 1 <= metadata_limit);

bool is_checksum(std::string_view value) noexcept {
    return value.size() == 64 && std::ranges::all_of(value, [](char character) {
               return (character >= '0' && character <= '9')
                      || (character >= 'a' && character <= 'f');
           });
}

std::optional<std::string_view> take_line(std::string_view& contents) noexcept {
    const auto newline = contents.find('\n');
    if (newline == std::string_view::npos) {
        return std::nullopt;
    }
    const auto line = contents.substr(0, newline);
    contents.remove_prefix(newline + 1);
    return line;
}

template<typename Number>
bool parse_integer(
  std::string_view value, Number& destination, int base = 10) noexcept {
    if (value.empty()) {
        return false;
    }
    const auto parsed = std::from_chars(
      value.data(), value.data() + value.size(), destination, base);
    return parsed.ec == std::errc{}
           && parsed.ptr == value.data() + value.size();
}

std::optional<detail::crash_loop_metadata>
decode_metadata(std::string_view contents) noexcept {
    if (
      contents.size() > metadata_limit
      || !contents.starts_with(metadata_header)) {
        return std::nullopt;
    }
    const auto protected_bytes = contents;
    contents.remove_prefix(metadata_header.size());
    const auto count = take_line(contents);
    const auto timestamp = take_line(contents);
    const auto checksum = take_line(contents);
    const auto integrity = take_line(contents);
    std::uint32_t expected_crc = 0;
    detail::crash_loop_metadata metadata;
    if (
      !count || !timestamp || !checksum || !integrity || !contents.empty()
      || !parse_integer(*count, metadata.crash_count)
      || !parse_integer(*timestamp, metadata.last_start_milliseconds)
      || metadata.last_start_milliseconds < 0 || !is_checksum(*checksum)
      || integrity->size() > 8
      || !parse_integer(*integrity, expected_crc, 16)) {
        return std::nullopt;
    }
    const auto protected_size = protected_bytes.size() - integrity->size() - 1;
    if (
      crc32c::Crc32c(protected_bytes.data(), protected_size) != expected_crc) {
        return std::nullopt;
    }
    std::ranges::copy(*checksum, metadata.configuration_checksum.begin());
    return metadata;
}

struct encoded_metadata final {
    std::array<char, metadata_limit> bytes{};
    std::size_t size{0};
};

encoded_metadata encode_metadata(const detail::crash_loop_metadata& metadata) {
    encoded_metadata encoded;
    auto* cursor
      = std::ranges::copy(metadata_header, encoded.bytes.begin()).out;
    const auto append_integer = [&](auto value, int base = 10) {
        const auto written = std::to_chars(
          cursor, encoded.bytes.data() + encoded.bytes.size(), value, base);
        if (written.ec != std::errc{}) {
            throw std::logic_error("crash-loop metadata exceeded its capacity");
        }
        cursor = written.ptr;
        *cursor++ = '\n';
    };
    append_integer(metadata.crash_count);
    append_integer(metadata.last_start_milliseconds);
    cursor = std::ranges::copy(metadata.configuration_checksum, cursor).out;
    *cursor++ = '\n';
    const auto protected_size = static_cast<std::size_t>(
      cursor - encoded.bytes.data());
    append_integer(crc32c::Crc32c(encoded.bytes.data(), protected_size), 16);
    encoded.size = static_cast<std::size_t>(cursor - encoded.bytes.data());
    return encoded;
}

void require_regular_tracker(bool regular, std::uint64_t links) {
    if (!regular || links != 1) {
        throw std::runtime_error(
          "crash-loop metadata must be a regular file with one link");
    }
}

seastar::future<seastar::file>
open_tracker(const std::filesystem::path& path, bool& created) {
    const auto flags = seastar::open_flags::rw
                       | static_cast<seastar::open_flags>(
                         O_NOFOLLOW | O_NONBLOCK);
    seastar::file_open_options options;
    options.extent_allocation_size_hint = 0;
    options.create_permissions = seastar::file_permissions::user_read
                                 | seastar::file_permissions::user_write;
    try {
        co_return co_await seastar::open_file_dma(
          path.native(), flags, options);
    } catch (const std::system_error& error) {
        if (error.code() != std::errc::no_such_file_or_directory) {
            throw;
        }
    }
    auto file = co_await seastar::open_file_dma(
      path.native(),
      flags | seastar::open_flags::create | seastar::open_flags::exclusive,
      options);
    created = true;
    co_return file;
}

seastar::future<std::optional<detail::crash_loop_metadata>>
read_metadata(seastar::file& file, seastar::abort_source& abort, bool created) {
    const auto status = co_await file.stat();
    abort.check();
    require_regular_tracker(S_ISREG(status.st_mode), status.st_nlink);
    if (status.st_size == 0) {
        if (!created) {
            log::broker().warn("ignoring malformed crash-loop metadata");
        }
        co_return std::nullopt;
    }
    if (
      status.st_size < 0
      || static_cast<std::uint64_t>(status.st_size) > metadata_limit) {
        log::broker().warn("ignoring malformed crash-loop metadata");
        co_return std::nullopt;
    }
    if (
      file.disk_read_dma_alignment() > io_alignment_limit
      || file.memory_dma_alignment() > io_alignment_limit) {
        throw std::runtime_error(
          "crash-loop metadata I/O alignment is too large");
    }
    const auto bytes = co_await file.dma_read_bulk<char>(0, metadata_limit + 1);
    abort.check();
    const auto metadata = decode_metadata({bytes.get(), bytes.size()});
    if (!metadata) {
        log::broker().warn("ignoring malformed crash-loop metadata");
    }
    co_return metadata;
}

seastar::future<> write_metadata(
  seastar::file& file, const detail::crash_loop_metadata& metadata) {
    const auto encoded = encode_metadata(metadata);
    const auto alignment = file.disk_write_dma_alignment();
    if (
      alignment == 0 || alignment > io_alignment_limit
      || file.memory_dma_alignment() > io_alignment_limit) {
        throw std::runtime_error(
          "crash-loop metadata I/O alignment is too large");
    }
    const auto size = ((encoded.size + alignment - 1) / alignment) * alignment;
    auto bytes = seastar::temporary_buffer<char>::aligned(
      file.memory_dma_alignment(), size);
    std::fill_n(bytes.get_write(), bytes.size(), '\0');
    std::copy_n(encoded.bytes.data(), encoded.size, bytes.get_write());

    // Persist an unsuccessful attempt before startup can proceed. Once mutation
    // starts, finish the flush sequence even if startup cancellation arrives.
    co_await file.truncate(0);
    std::size_t written = 0;
    while (written < bytes.size()) {
        const auto progress = co_await file.dma_write(
          written, bytes.get() + written, bytes.size() - written);
        if (progress == 0 || progress > bytes.size() - written) {
            throw std::runtime_error(
              "crash-loop metadata write made no progress");
        }
        written += progress;
    }
    co_await file.truncate(encoded.size);
    co_await file.flush();
}

} // namespace

namespace detail {

std::optional<std::uint64_t> next_crash_count(
  const std::optional<crash_loop_metadata>& previous,
  const configuration_identity& configuration,
  std::int64_t now_milliseconds,
  std::optional<std::uint32_t> limit) noexcept {
    if (!previous) {
        return 1;
    }
    const bool configuration_changed = previous->configuration_checksum
                                       != configuration.checksum;
    // A clock that moved backward cannot make a recent failed start expire.
    // Unsigned subtraction also avoids overflow for malformed extreme times.
    const bool tracking_reset
      = now_milliseconds > previous->last_start_milliseconds
        && static_cast<std::uint64_t>(now_milliseconds)
               - static_cast<std::uint64_t>(previous->last_start_milliseconds)
             > reset_milliseconds;
    const bool crash_limit_ok = !limit || previous->crash_count <= *limit;
    if (!crash_limit_ok && !configuration_changed && !tracking_reset) {
        return std::nullopt;
    }
    if (configuration_changed || tracking_reset) {
        return 1;
    }
    return previous->crash_count == std::numeric_limits<std::uint64_t>::max()
             ? previous->crash_count
             : previous->crash_count + 1;
}

} // namespace detail

crash_loop_limit_reached::crash_loop_limit_reached()
  : std::runtime_error(
      "crash loop detected; restart requires more than one hour since the last "
      "attempt, a changed configuration, or removal of .kwaque-crash-loop") {}

crash_limiter::crash_limiter(clock_function clock)
  : clock_(clock) {
    if (clock_ == nullptr) {
        throw std::invalid_argument("crash limiter requires a clock");
    }
}

std::int64_t crash_limiter::system_time_milliseconds() noexcept {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

seastar::future<> crash_limiter::start(
  const std::filesystem::path& data_directory,
  const detail::configuration_identity& configuration,
  bool developer_mode,
  seastar::abort_source& abort,
  std::optional<std::uint32_t> limit) {
    if (attempted_) {
        throw std::logic_error("crash limiter startup was already attempted");
    }
    attempted_ = true;
    abort.check();
    directory_ = data_directory;
    path_ = data_directory / tracker_name;
    if (developer_mode) {
        recorded_ = true;
        co_return;
    }
    if (!is_checksum(configuration.checksum_view())) {
        throw std::invalid_argument(
          "crash limiter requires a configuration checksum");
    }
    bool created = false;
    co_await seastar::with_file(
      open_tracker(path_, created),
      [this, &configuration, &abort, &created, limit](
        seastar::file& file) -> seastar::future<> {
          const auto previous = co_await read_metadata(file, abort, created);
          const auto now = clock_();
          if (now < 0) {
              throw std::runtime_error(
                "crash limiter clock precedes the epoch");
          }
          const auto count = detail::next_crash_count(
            previous, configuration, now, limit);
          if (!count) {
              throw crash_loop_limit_reached{};
          }
          abort.check();
          co_await write_metadata(
            file,
            {
              .crash_count = *count,
              .configuration_checksum = configuration.checksum,
              .last_start_milliseconds = now,
            });
      });
    co_await seastar::sync_directory(directory_.native());
    recorded_ = true;
    abort.check();
}

seastar::future<>
crash_limiter::sync_directory(const std::filesystem::path& directory) {
    return seastar::sync_directory(directory.native());
}

seastar::future<> crash_limiter::record_clean_shutdown() {
    auto cleanup = co_await seastar::get_units(cleanup_, 1);
    if (!recorded_) {
        co_return;
    }
    // Do not follow a replaced link or remove a substituted directory. The
    // caller keeps exclusive directory ownership throughout this operation.
    if (!directory_sync_pending_) {
        std::optional<seastar::stat_data> status;
        try {
            status = co_await seastar::file_stat(
              path_.native(), seastar::follow_symlink::no);
        } catch (const std::system_error& error) {
            if (error.code() != std::errc::no_such_file_or_directory) {
                throw;
            }
        }
        if (!status) {
            recorded_ = false;
            co_return;
        }
        require_regular_tracker(
          status->type == seastar::directory_entry_type::regular,
          status->number_of_links);
        co_await seastar::remove_file(path_.native());
        directory_sync_pending_ = true;
    }
    co_await sync_directory_(directory_);
    directory_sync_pending_ = false;
    recorded_ = false;
}

} // namespace kwaque::broker
