#include "src/broker/crash_recorder.h"

#include "src/base/build_info.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/backtrace.hh>
#include <seastar/util/spinlock.hh>

#include <sys/stat.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <exception>
#include <fcntl.h>
#include <mutex>
#include <pthread.h>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unistd.h>

namespace kwaque::broker {
namespace {

struct signal_slot final {
    int number;
    struct sigaction previous{};
    bool installed{false};
};

// Serializes fatal handlers with one another and with publication/removal only.
// Ordinary owners block these signals before acquiring this dedicated lock.
seastar::util::spinlock signal_lock;
detail::prepared_crash_writer* signal_writer = nullptr;
std::array signal_slots{
  signal_slot{SIGABRT},
  signal_slot{SIGILL},
#ifndef SEASTAR_ASAN_ENABLED
  signal_slot{SIGSEGV},
#endif
};

sigset_t fatal_mask() noexcept {
    sigset_t result;
    ::sigemptyset(&result);
    for (const auto& slot : signal_slots) {
        ::sigaddset(&result, slot.number);
    }
    return result;
}

class blocked_fatal_signals final {
public:
    blocked_fatal_signals() noexcept {
        const auto mask = fatal_mask();
        const auto result = ::pthread_sigmask(SIG_BLOCK, &mask, &previous_);
        if (result != 0) {
            // Proceeding cannot safely acquire the signal lock or release its
            // writer. Valid masks cannot fail on the supported native runtime.
            ::_exit(128 + SIGABRT);
        }
    }
    ~blocked_fatal_signals() {
        ::pthread_sigmask(SIG_SETMASK, &previous_, nullptr);
    }
    blocked_fatal_signals(const blocked_fatal_signals&) = delete;
    blocked_fatal_signals& operator=(const blocked_fatal_signals&) = delete;

private:
    sigset_t previous_{};
};

void print_outcome(bool success) noexcept {
    const std::string_view text = success ? "Recorded crash report\n"
                                          : "Failed to persist crash report\n";
    std::size_t offset = 0;
    while (offset != text.size()) {
        const auto result = ::write(
          STDERR_FILENO, text.data() + offset, text.size() - offset);
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            break;
        }
        offset += static_cast<std::size_t>(result);
    }
}

void fatal_handler(int signal, siginfo_t*, void*) noexcept {
    const auto saved_errno = errno;
    std::lock_guard guard(signal_lock);
    if (signal_writer != nullptr && signal_writer->ready()) {
        const auto kind = signal == SIGABRT ? detail::crash_kind::abort
                          : signal == SIGILL
                            ? detail::crash_kind::illegal_instruction
                            : detail::crash_kind::segmentation_fault;
        print_outcome(
          signal_writer->record(kind, signal, seastar::this_shard_id()));
    }
    for (auto& slot : signal_slots) {
        if (slot.number != signal) {
            continue;
        }
        if (::sigaction(signal, &slot.previous, nullptr) != 0) {
            ::signal(signal, SIG_DFL);
        }
        slot.installed = false;
        break;
    }
    // The signal is masked until this handler returns. The restored native (or
    // sanitizer) handler then owns diagnostics and the original exit behavior.
    const auto result = ::pthread_kill(::pthread_self(), signal);
    errno = saved_errno;
    if (result != 0) {
        ::_exit(128 + signal);
    }
}

void attach_handlers(detail::prepared_crash_writer& writer) {
    const blocked_fatal_signals blocked;
    std::lock_guard guard(signal_lock);
    if (signal_writer != nullptr) {
        throw std::logic_error("fatal crash recorder already installed");
    }
    if (
      std::any_of(
        signal_slots.begin(), signal_slots.end(), [](const signal_slot& slot) {
            return slot.installed;
        })) {
        throw std::logic_error("previous fatal signal restoration failed");
    }
    signal_writer = &writer;
    for (auto& slot : signal_slots) {
        struct sigaction action{};
        action.sa_sigaction = fatal_handler;
        ::sigfillset(&action.sa_mask);
        action.sa_flags = SA_SIGINFO | SA_RESTART;
        if (slot.number == SIGSEGV) {
            action.sa_flags |= SA_ONSTACK;
        }
        if (::sigaction(slot.number, &action, &slot.previous) != 0) {
            throw std::system_error(
              errno, std::system_category(), "install fatal signal handler");
        }
        slot.installed = true;
    }
}

void detach_handlers(detail::prepared_crash_writer& writer) {
    const blocked_fatal_signals blocked;
    std::lock_guard guard(signal_lock);
    if (signal_writer != &writer) {
        return;
    }
    int failure = 0;
    for (auto& slot : signal_slots) {
        if (
          slot.installed
          && ::sigaction(slot.number, &slot.previous, nullptr) != 0) {
            if (failure == 0) {
                failure = errno;
            }
            continue;
        }
        slot.installed = false;
    }
    // A failed restore leaves a wrapper which safely observes no recorder.
    signal_writer = nullptr;
    if (failure != 0) {
        throw std::system_error(
          failure, std::system_category(), "restore fatal signal handler");
    }
}

struct retained_report final {
    std::string name;
    std::int64_t timestamp;
};

bool older(const retained_report& left, const retained_report& right) noexcept {
    return left.timestamp < right.timestamp
           || (left.timestamp == right.timestamp && left.name < right.name);
}

seastar::future<std::int64_t> report_timestamp(
  const std::filesystem::path& path,
  const seastar::stat_data& status,
  seastar::abort_source& abort) {
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                       status.time_changed.time_since_epoch())
                       .count();
    if (
      status.size < detail::crash_v1_header_size
      || status.size > detail::crash_serialization_capacity) {
        co_return timestamp;
    }
    seastar::file file;
    std::exception_ptr failure;
    try {
        file = co_await seastar::open_file_dma(
          path.native(),
          seastar::open_flags::ro
            | static_cast<seastar::open_flags>(O_NOFOLLOW | O_NONBLOCK));
        abort.check();
        const auto opened = co_await file.stat();
        abort.check();
        if (
          !S_ISREG(opened.st_mode) || opened.st_ino != status.inode_number
          || opened.st_dev != status.device_id) {
            throw std::runtime_error("crash report changed during enumeration");
        }
        constexpr std::size_t alignment_limit = 128U * 1024U;
        if (
          file.disk_read_dma_alignment() > alignment_limit
          || file.memory_dma_alignment() > alignment_limit) {
            throw std::runtime_error("crash report I/O alignment is too large");
        }
        auto bytes = co_await file.dma_read_bulk<char>(
          0, detail::crash_serialization_capacity);
        if (bytes.size() > detail::crash_serialization_capacity) {
            bytes.trim(detail::crash_serialization_capacity);
        }
        abort.check();
        std::int64_t recorded_time = 0;
        if (
          detail::crash_report_timestamp(
            {bytes.get(), bytes.size()}, status.size, recorded_time)) {
            timestamp = recorded_time;
        }
    } catch (...) {
        failure = std::current_exception();
    }
    if (file) {
        try {
            co_await file.close();
        } catch (...) {
            if (!failure) {
                failure = std::current_exception();
            }
        }
    }
    if (failure) {
        std::rethrow_exception(failure);
    }
    co_return timestamp;
}

} // namespace

crash_recorder::~crash_recorder() {
    if (handlers_installed_) {
        try {
            detach_handlers(writer_);
        } catch (...) {
            // Destruction still removes the published pointer under the signal
            // lock. A failed restoration cannot leave a dangling writer.
        }
    }
}

seastar::future<> crash_recorder::prune(seastar::abort_source& abort) {
    seastar::file directory;
    std::exception_ptr failure;
    try {
        directory = co_await seastar::open_directory(directory_.native());
        abort.check();
        std::array<retained_report, existing_reports_to_keep> retained;
        std::size_t count = 0;
        std::size_t visited = 0;
        auto entries = directory.experimental_list_directory();
        while (auto entry = co_await entries()) {
            abort.check();
            if (++visited > directory_entry_limit) {
                throw std::runtime_error(
                  "crash report directory entry limit exceeded");
            }
            // Count every entry before filtering, including unrelated names.
            if (entry->name.size() > 255U) {
                throw std::runtime_error(
                  "crash report filename limit exceeded");
            }
            const std::string_view name{entry->name.data(), entry->name.size()};
            if (!name.ends_with(".crash")) {
                co_await seastar::coroutine::maybe_yield();
                continue;
            }
            const auto status = co_await seastar::file_stat(
              directory, entry->name, seastar::follow_symlink::no);
            abort.check();
            if (status.type != seastar::directory_entry_type::regular) {
                continue;
            }
            const auto path = directory_ / name;
            retained_report candidate{
              std::string{name},
              co_await report_timestamp(path, status, abort)};
            if (count < retained.size()) {
                retained[count++] = std::move(candidate);
                continue;
            }
            const auto oldest = std::min_element(
              retained.begin(), retained.end(), older);
            if (older(*oldest, candidate)) {
                co_await seastar::remove_file(
                  (directory_ / oldest->name).native());
                *oldest = std::move(candidate);
            } else {
                co_await seastar::remove_file(path.native());
            }
            abort.check();
            co_await seastar::coroutine::maybe_yield();
        }
    } catch (...) {
        failure = std::current_exception();
    }
    if (directory) {
        try {
            co_await directory.close();
        } catch (...) {
            if (!failure) {
                failure = std::current_exception();
            }
        }
    }
    if (failure) {
        std::rethrow_exception(failure);
    }
}

seastar::future<> crash_recorder::start(
  const std::filesystem::path& data_directory,
  seastar::abort_source& abort,
  bool install_signal_handlers) {
    abort.check();
    if (!directory_.empty()) {
        throw std::logic_error("crash recorder already started");
    }
    directory_ = data_directory / "crash_reports";
    co_await seastar::recursive_touch_directory(directory_.native());
    abort.check();
    co_await seastar::sync_directory(data_directory.native());
    abort.check();
    co_await prune(abort);
    abort.check();
    const auto now
      = std::chrono::system_clock::now().time_since_epoch().count();
    bool created = false;
    struct stat prepared_status{};
    for (std::size_t attempt = 0; attempt < 10; ++attempt) {
        report_path_
          = directory_
            / (std::to_string(now) + "_" + std::to_string(::getpid()) + "_" + std::to_string(attempt) + ".crash");
        seastar::file file;
        try {
            seastar::file_open_options options;
            options.create_permissions = static_cast<seastar::file_permissions>(
              0600);
            file = co_await seastar::open_file_dma(
              report_path_.native(),
              seastar::open_flags::create | seastar::open_flags::wo
                | seastar::open_flags::exclusive,
              options);
            created = true;
            placeholder_owned_ = true;
        } catch (const std::system_error& error) {
            if (error.code().value() != EEXIST) {
                throw;
            }
        }
        if (created) {
            std::exception_ptr failure;
            try {
                prepared_status = co_await file.stat();
            } catch (...) {
                failure = std::current_exception();
            }
            co_await file.close();
            if (failure) {
                std::rethrow_exception(failure);
            }
            break;
        }
        abort.check();
    }
    if (!created) {
        report_path_.clear();
        throw std::runtime_error("unable to prepare a unique crash report");
    }
    abort.check();
    co_await seastar::sync_directory(directory_.native());
    abort.check();
    // Fatal handlers need a plain descriptor, without DMA alignment
    // constraints.
    const int descriptor = ::open(
      report_path_.c_str(), O_WRONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (descriptor < 0) {
        throw std::system_error(
          errno, std::system_category(), "open prepared crash report");
    }
    try {
        struct stat reopened_status{};
        if (::fstat(descriptor, &reopened_status) != 0) {
            throw std::system_error(
              errno, std::system_category(), "stat prepared crash report");
        }
        if (
          !S_ISREG(reopened_status.st_mode) || reopened_status.st_nlink != 1
          || reopened_status.st_ino != prepared_status.st_ino
          || reopened_status.st_dev != prepared_status.st_dev) {
            throw std::runtime_error(
              "prepared crash report changed while opening");
        }
        writer_.initialize(descriptor, build_info::version());
    } catch (...) {
        ::close(descriptor);
        throw;
    }
    if (install_signal_handlers) {
        // Resolve the native unwinder on every reactor before fatal recording;
        // it must not perform lazy initialization in the allocation-free path.
        co_await seastar::smp::invoke_on_all(
          [] { seastar::backtrace([](const seastar::frame&) noexcept {}); });
        abort.check();
        handlers_installed_ = true;
        attach_handlers(writer_);
        co_await seastar::smp::invoke_on_all([] {
            const auto mask = fatal_mask();
            const auto result = ::pthread_sigmask(SIG_UNBLOCK, &mask, nullptr);
            if (result != 0) {
                throw std::system_error(
                  result, std::system_category(), "unblock fatal signals");
            }
        });
        abort.check();
    }
}

void crash_recorder::record_startup_failure() noexcept {
    if (!writer_.initialized()) {
        return;
    }
    // Avoid interrupting a partially serialized startup report with a fatal
    // signal on this thread, and serialize with fatal handlers on other shards.
    const blocked_fatal_signals blocked;
    std::lock_guard guard(signal_lock);
    if (writer_.ready()) {
        print_outcome(writer_.record(
          detail::crash_kind::startup_failure, 0, seastar::this_shard_id()));
    }
}

seastar::future<>
crash_recorder::sync_directory(const std::filesystem::path& directory) {
    return seastar::sync_directory(directory.native());
}

seastar::future<> crash_recorder::stop() {
    auto cleanup = co_await seastar::get_units(cleanup_, 1);
    std::exception_ptr failure;
    if (handlers_installed_) {
        try {
            detach_handlers(writer_);
        } catch (...) {
            failure = std::current_exception();
        }
        handlers_installed_ = false;
    }
    const bool unused = !writer_.initialized() || writer_.release();
    writer_.close();
    if (unused && (placeholder_owned_ || directory_sync_pending_)) {
        try {
            if (placeholder_owned_) {
                co_await seastar::remove_file(report_path_.native());
                placeholder_owned_ = false;
                report_path_.clear();
                directory_sync_pending_ = true;
            }
            co_await sync_directory_(directory_);
            directory_sync_pending_ = false;
        } catch (...) {
            if (!failure) {
                failure = std::current_exception();
            }
        }
    }
    if (failure) {
        std::rethrow_exception(failure);
    }
}

} // namespace kwaque::broker
