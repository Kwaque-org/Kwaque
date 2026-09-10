#include "src/broker/crash_recorder_writer.h"

#include <seastar/util/backtrace.hh>

#include <crc32c/crc32c.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <ctime>
#include <stdexcept>
#include <unistd.h>

namespace kwaque::broker::detail {
namespace {

constexpr std::string_view magic = "KQCRSH2";
static_assert(
  crash_header_size + 2U * crash_field_capacity + crash_version_capacity
    + crash_architecture_capacity + 4U
  <= crash_serialization_capacity);
static_assert(crash_architecture().size() <= crash_architecture_capacity);

ssize_t native_write(int fd, const void* data, std::size_t size) noexcept {
    return ::write(fd, data, size);
}

int native_fsync(int fd) noexcept { return ::fsync(fd); }

void store_number(
  char* output, std::uint64_t value, std::size_t width) noexcept {
    for (std::size_t index = 0; index < width; ++index) {
        output[index] = static_cast<char>(value & 0xffU);
        value >>= 8U;
    }
}

std::uint64_t load_number(const char* input, std::size_t width) noexcept {
    std::uint64_t result = 0;
    for (std::size_t index = 0; index < width; ++index) {
        result |= static_cast<std::uint64_t>(
                    static_cast<unsigned char>(input[index]))
                  << (8U * index);
    }
    return result;
}

template<std::size_t Capacity>
class prepared_text final {
public:
    void append(std::string_view text) noexcept {
        const auto length = std::min(Capacity - size_, text.size());
        std::copy_n(text.data(), length, bytes_.data() + size_);
        size_ += length;
    }

    void append_hex(std::uintptr_t value) noexcept {
        constexpr std::string_view digits = "0123456789abcdef";
        std::array<char, sizeof(value) * 2> text{};
        auto cursor = text.size();
        do {
            text[--cursor] = digits[value & 0xfU];
            value >>= 4U;
        } while (value != 0);
        append("0x");
        append({text.data() + cursor, text.size() - cursor});
    }

    [[nodiscard]] std::string_view view() const noexcept {
        return {bytes_.data(), size_};
    }

private:
    std::array<char, Capacity> bytes_{};
    std::size_t size_{0};
};

std::string_view message_for(crash_kind kind) noexcept {
    switch (kind) {
    case crash_kind::startup_failure:
        return "Failure during startup";
    case crash_kind::abort:
        return "Aborting";
    case crash_kind::illegal_instruction:
        return "Illegal instruction";
    case crash_kind::segmentation_fault:
        return "Segmentation fault";
    }
    return "Unknown fatal failure";
}

} // namespace

struct prepared_crash_writer::storage final {
    prepared_text<crash_field_capacity> message;
    prepared_text<crash_field_capacity> stack;
    prepared_text<crash_version_capacity> version;
    prepared_text<crash_architecture_capacity> architecture;
    std::array<char, crash_serialization_capacity> serialized{};
    std::size_t serialized_size{0};
};

prepared_crash_writer::prepared_crash_writer()
  : prepared_crash_writer({native_write, native_fsync}) {}

prepared_crash_writer::prepared_crash_writer(crash_write_operations operations)
  : operations_(operations) {}

prepared_crash_writer::~prepared_crash_writer() { close(); }

void prepared_crash_writer::initialize(
  int descriptor, std::string_view version) {
    if (state_.load() != state::uninitialized || descriptor < 0) {
        throw std::logic_error("crash writer initialization state");
    }
    auto prepared = std::make_unique<storage>();
    prepared->version.append(version);
    prepared->architecture.append(crash_architecture());
    // Resolve the checksum implementation before publishing to fatal handlers.
    static_cast<void>(crc32c::Crc32c("prepare", 7));
    seastar::backtrace([](const seastar::frame&) noexcept {});
    storage_ = std::move(prepared);
    descriptor_ = descriptor;
    // Publish the descriptor and all prepared storage to other threads last.
    state_.store(state::initialized);
}

bool prepared_crash_writer::initialized() const noexcept {
    return state_.load() != state::uninitialized;
}

bool prepared_crash_writer::ready() const noexcept {
    return state_.load() == state::initialized;
}

bool prepared_crash_writer::record(
  crash_kind kind,
  int signal,
  std::uint32_t shard,
  bool capture_backtrace) noexcept {
    const auto saved_errno = errno;
    auto before = state::initialized;
    if (!state_.compare_exchange_strong(before, state::filled)) {
        return false;
    }
    auto& prepared = *storage_;
    prepared.message.append(message_for(kind));
    if (capture_backtrace) {
        seastar::backtrace([&prepared](const seastar::frame& frame) noexcept {
            if (!prepared.stack.view().empty()) {
                prepared.stack.append(" ");
            }
            // Module basenames identify relative addresses without recording
            // arbitrary paths or the contents of the failing operation.
            const std::string_view name{
              frame.so->name.data(), frame.so->name.size()};
            const auto slash = name.find_last_of('/');
            const auto basename = slash == std::string_view::npos
                                    ? name
                                    : name.substr(slash + 1U);
            if (!basename.empty()) {
                prepared.stack.append(basename.substr(0, 128));
                prepared.stack.append("+");
            }
            prepared.stack.append_hex(frame.addr);
        });
    }
    timespec now{};
    const bool have_time = ::clock_gettime(CLOCK_REALTIME, &now) == 0;
    const auto milliseconds = have_time
                                ? static_cast<std::uint64_t>(now.tv_sec) * 1000U
                                    + static_cast<std::uint64_t>(now.tv_nsec)
                                        / 1000000U
                                : 0U;
    auto* output = prepared.serialized.data();
    std::copy(magic.begin(), magic.end(), output);
    output[7] = '\0';
    store_number(output + 8, milliseconds, 8);
    store_number(output + 16, static_cast<std::uint32_t>(signal), 4);
    store_number(output + 20, shard, 4);
    store_number(output + 24, static_cast<std::uint32_t>(kind), 4);
    store_number(output + 28, prepared.message.view().size(), 4);
    store_number(output + 32, prepared.stack.view().size(), 4);
    store_number(output + 36, prepared.version.view().size(), 4);
    store_number(output + 40, prepared.architecture.view().size(), 4);
    auto offset = crash_header_size;
    for (const auto field :
         {prepared.message.view(),
          prepared.stack.view(),
          prepared.version.view(),
          prepared.architecture.view()}) {
        std::copy(field.begin(), field.end(), output + offset);
        offset += field.size();
    }
    store_number(output + offset, crc32c::Crc32c(output, offset), 4);
    prepared.serialized_size = offset + 4U;
    const bool success = write_record();
    state_.store(state::written);
    errno = saved_errno;
    return success;
}

bool prepared_crash_writer::write_record() noexcept {
    auto& prepared = *storage_;
    std::size_t written = 0;
    bool complete = true;
    while (written < prepared.serialized_size) {
        const auto result = operations_.write(
          descriptor_,
          prepared.serialized.data() + written,
          prepared.serialized_size - written);
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (
          result <= 0
          || static_cast<std::size_t>(result)
               > prepared.serialized_size - written) {
            complete = false;
            break;
        }
        written += static_cast<std::size_t>(result);
    }
    int result = 0;
    do {
        result = operations_.fsync(descriptor_);
    } while (result < 0 && errno == EINTR);
    return complete && result == 0;
}

bool prepared_crash_writer::release() noexcept {
    auto before = state::initialized;
    return state_.compare_exchange_strong(before, state::released)
           || before == state::released;
}

void prepared_crash_writer::close() noexcept {
    if (descriptor_ >= 0) {
        // Linux closes the descriptor even if close reports EINTR. Retrying
        // could close a descriptor subsequently reused by another owner.
        const auto saved_errno = errno;
        ::close(descriptor_);
        descriptor_ = -1;
        errno = saved_errno;
    }
}

bool crash_report_timestamp(
  std::span<const char> record,
  std::uint64_t file_size,
  std::int64_t& timestamp) noexcept {
    if (
      record.size() < crash_v1_header_size + 4U
      || record.size() > crash_serialization_capacity
      || record.size() != file_size
      || !std::equal(magic.begin(), magic.begin() + 6, record.begin())
      || (record[6] != '1' && record[6] != '2') || record[7] != '\0') {
        return false;
    }
    const auto header_size = record[6] == '1' ? crash_v1_header_size
                                              : crash_header_size;
    if (record.size() < header_size + 4U) {
        return false;
    }
    const auto message = load_number(record.data() + 28, 4);
    const auto stack = load_number(record.data() + 32, 4);
    const auto version = load_number(record.data() + 36, 4);
    const auto architecture = record[6] == '1'
                                ? 0U
                                : load_number(record.data() + 40, 4);
    const auto kind = load_number(record.data() + 24, 4);
    const auto time = load_number(record.data() + 8, 8);
    if (
      message > crash_field_capacity || stack > crash_field_capacity
      || version > crash_version_capacity || kind < 1 || kind > 4
      || architecture > crash_architecture_capacity
      || (record[6] == '2' && architecture == 0)
      || file_size
           != header_size + message + stack + version + architecture + 4U
      || time == 0 || time > INT64_MAX) {
        return false;
    }
    const auto protected_size = record.size() - 4U;
    if (
      load_number(record.data() + protected_size, 4)
      != crc32c::Crc32c(record.data(), protected_size)) {
        return false;
    }
    timestamp = static_cast<std::int64_t>(time);
    return true;
}

} // namespace kwaque::broker::detail
