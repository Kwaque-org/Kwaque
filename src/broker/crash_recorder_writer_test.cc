#include "src/broker/crash_recorder_writer.h"

#include <crc32c/crc32c.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <cerrno>
#include <csignal>
#include <fcntl.h>
#include <limits>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

using kwaque::broker::detail::crash_kind;
using kwaque::broker::detail::prepared_crash_writer;

struct io_script final {
    std::array<char, kwaque::broker::detail::crash_serialization_capacity>
      output{};
    std::size_t size{0};
    std::size_t chunk{std::numeric_limits<std::size_t>::max()};
    unsigned write_interruptions{0};
    unsigned sync_interruptions{0};
    unsigned writes{0};
    unsigned syncs{0};
    int write_error{0};
    std::size_t fail_after_bytes{0};
    int sync_error{0};
    bool zero_progress{false};
};

io_script* active_script = nullptr;

ssize_t scripted_write(int, const void* data, std::size_t size) noexcept {
    auto& script = *active_script;
    ++script.writes;
    if (script.write_interruptions != 0) {
        --script.write_interruptions;
        errno = EINTR;
        return -1;
    }
    if (script.write_error != 0 && script.size >= script.fail_after_bytes) {
        errno = script.write_error;
        return -1;
    }
    if (script.zero_progress) {
        return 0;
    }
    const auto copied = std::min(
      {size, script.chunk, script.output.size() - script.size});
    std::copy_n(
      static_cast<const char*>(data),
      copied,
      script.output.data() + script.size);
    script.size += copied;
    return static_cast<ssize_t>(copied);
}

int scripted_fsync(int) noexcept {
    auto& script = *active_script;
    ++script.syncs;
    if (script.sync_interruptions != 0) {
        --script.sync_interruptions;
        errno = EINTR;
        return -1;
    }
    if (script.sync_error != 0) {
        errno = script.sync_error;
        return -1;
    }
    return 0;
}

class CrashWriterTest : public testing::Test {
protected:
    void SetUp() override {
        active_script = &script;
        const int descriptor = ::open("/dev/null", O_WRONLY | O_CLOEXEC);
        ASSERT_GE(descriptor, 0);
        writer.initialize(descriptor, "test-version");
    }

    io_script script;
    prepared_crash_writer writer{{scripted_write, scripted_fsync}};
};

TEST_F(
  CrashWriterTest, RetriesInterruptedShortWritesAndFsyncAndPreservesErrno) {
    script.chunk = 3;
    script.write_interruptions = 1;
    script.sync_interruptions = 1;
    errno = ERANGE;
    EXPECT_TRUE(writer.record(crash_kind::abort, SIGABRT, 7, false));
    EXPECT_EQ(errno, ERANGE);
    EXPECT_GT(script.writes, 2U);
    EXPECT_EQ(script.syncs, 2U);
    const std::string_view encoded{script.output.data(), script.size};
    EXPECT_EQ(encoded.substr(0, 8), std::string_view("KQCRSH2\0", 8));
    constexpr auto header_size = kwaque::broker::detail::crash_header_size;
    constexpr auto architecture = kwaque::broker::detail::crash_architecture();
    EXPECT_EQ(
      encoded.substr(
        header_size, encoded.size() - header_size - 4U - architecture.size()),
      "Abortingtest-version");
    EXPECT_EQ(
      encoded.substr(
        encoded.size() - 4U - architecture.size(), architecture.size()),
      architecture);
    EXPECT_EQ(static_cast<unsigned char>(encoded[40]), architecture.size());
    EXPECT_EQ(static_cast<unsigned char>(encoded[16]), SIGABRT);
    EXPECT_EQ(static_cast<unsigned char>(encoded[20]), 7);
    std::int64_t timestamp = 0;
    EXPECT_TRUE(
      kwaque::broker::detail::crash_report_timestamp(
        {script.output.data(), script.size}, script.size, timestamp));
    EXPECT_GT(timestamp, 0);
    EXPECT_FALSE(writer.release());
    EXPECT_FALSE(writer.record(crash_kind::abort, SIGABRT, 7, false));
}

TEST_F(CrashWriterTest, ZeroProgressTerminatesWithoutClaimingPersistence) {
    script.zero_progress = true;
    EXPECT_FALSE(writer.record(crash_kind::abort, SIGABRT, 0, false));
    EXPECT_EQ(script.writes, 1U);
    EXPECT_EQ(script.syncs, 1U);
    EXPECT_FALSE(writer.release());
}

TEST_F(CrashWriterTest, FullDiskConsumesWriterWithoutReportingSuccess) {
    script.write_error = ENOSPC;
    EXPECT_FALSE(writer.record(crash_kind::abort, SIGABRT, 0, false));
    EXPECT_EQ(script.writes, 1U);
    EXPECT_EQ(script.syncs, 1U);
    EXPECT_FALSE(writer.record(crash_kind::abort, SIGABRT, 0, false));
}

TEST_F(CrashWriterTest, FailedInterruptedFsyncIsNotPersistedSuccess) {
    script.sync_interruptions = 1;
    script.sync_error = EIO;
    EXPECT_FALSE(writer.record(crash_kind::abort, SIGABRT, 0, false));
    EXPECT_GT(script.size, 40U);
    EXPECT_EQ(script.syncs, 2U);
    EXPECT_FALSE(writer.release());
}

TEST_F(CrashWriterTest, ShortWriteThenFailureLeavesAnUntrustedTornRecord) {
    script.chunk = 7;
    script.fail_after_bytes = 7;
    script.write_error = EIO;
    EXPECT_FALSE(writer.record(crash_kind::abort, SIGABRT, 0, false));
    EXPECT_EQ(script.size, 7U);
    EXPECT_EQ(script.writes, 2U);
    EXPECT_EQ(script.syncs, 1U);
    std::int64_t timestamp = 0;
    EXPECT_FALSE(
      kwaque::broker::detail::crash_report_timestamp(
        {script.output.data(), script.size}, script.size, timestamp));
}

TEST_F(CrashWriterTest, ReleasePreventsRecordingAndIsIdempotent) {
    EXPECT_TRUE(writer.release());
    EXPECT_TRUE(writer.release());
    EXPECT_FALSE(writer.record(crash_kind::abort, SIGABRT, 0, false));
    EXPECT_EQ(script.writes, 0U);
}

TEST_F(CrashWriterTest, RacingRecordersHaveExactlyOneWriter) {
    std::barrier ready(3);
    std::atomic<unsigned> successes{0};
    auto record = [&] {
        ready.arrive_and_wait();
        if (writer.record(crash_kind::abort, SIGABRT, 0, false)) {
            successes.fetch_add(1);
        }
    };
    std::thread first(record);
    std::thread second(record);
    ready.arrive_and_wait();
    first.join();
    second.join();
    EXPECT_EQ(successes.load(), 1U);
    EXPECT_EQ(script.writes, 1U);
    EXPECT_EQ(script.syncs, 1U);
}

TEST_F(CrashWriterTest, RacingReleaseAndRecordChooseOneOwner) {
    std::barrier ready(3);
    bool released = false;
    bool recorded = false;
    std::thread first([&] {
        ready.arrive_and_wait();
        released = writer.release();
    });
    std::thread second([&] {
        ready.arrive_and_wait();
        recorded = writer.record(crash_kind::abort, SIGABRT, 0, false);
    });
    ready.arrive_and_wait();
    first.join();
    second.join();
    EXPECT_NE(released, recorded);
    EXPECT_EQ(script.writes, recorded ? 1U : 0U);
}

TEST_F(CrashWriterTest, TornAndTruncatedRecordsAreRejected) {
    ASSERT_TRUE(writer.record(crash_kind::startup_failure, 0, 0, false));
    std::int64_t timestamp = 0;
    const auto header = [&] {
        return std::span<const char>{script.output.data(), script.size};
    };
    EXPECT_FALSE(
      kwaque::broker::detail::crash_report_timestamp(
        header(), script.size - 1U, timestamp));
    EXPECT_FALSE(
      kwaque::broker::detail::crash_report_timestamp(
        {script.output.data(), 39}, script.size, timestamp));
}

void store_u32(char* output, std::uint32_t value) {
    for (std::size_t index = 0; index < 4; ++index) {
        output[index] = static_cast<char>(value & 0xffU);
        value >>= 8U;
    }
}

void repair_checksum(std::vector<char>& bytes) {
    const auto covered = bytes.size() - 4U;
    store_u32(bytes.data() + covered, crc32c::Crc32c(bytes.data(), covered));
}

TEST_F(CrashWriterTest, ChecksummedStructuralErrorsAreRejectedIndependently) {
    ASSERT_TRUE(writer.record(crash_kind::startup_failure, 0, 0, false));
    const std::vector<char> original(
      script.output.data(), script.output.data() + script.size);
    struct oversized_field final {
        std::size_t offset;
        std::uint32_t length;
    };
    const std::array oversized{
      oversized_field{28, 4097},
      oversized_field{32, 4097},
      oversized_field{36, 129},
      oversized_field{40, 17},
    };
    std::int64_t timestamp = 0;
    for (const auto& field : oversized) {
        auto bytes = original;
        for (const auto offset : {28U, 32U, 36U, 40U}) {
            store_u32(bytes.data() + offset, 0);
        }
        store_u32(bytes.data() + 40, 1);
        store_u32(bytes.data() + field.offset, field.length);
        bytes.resize(
          kwaque::broker::detail::crash_header_size + 4U + field.length
            + (field.offset == 40 ? 0U : 1U),
          'x');
        repair_checksum(bytes);
        EXPECT_FALSE(
          kwaque::broker::detail::crash_report_timestamp(
            bytes, bytes.size(), timestamp))
          << field.offset;
    }
    auto bytes = original;
    bytes[6] = '3';
    repair_checksum(bytes);
    EXPECT_FALSE(
      kwaque::broker::detail::crash_report_timestamp(
        bytes, bytes.size(), timestamp));
    for (const auto kind : {0U, 5U}) {
        bytes = original;
        store_u32(bytes.data() + 24, kind);
        repair_checksum(bytes);
        EXPECT_FALSE(
          kwaque::broker::detail::crash_report_timestamp(
            bytes, bytes.size(), timestamp));
    }
    bytes = original;
    std::fill(bytes.begin() + 8, bytes.begin() + 15, static_cast<char>(0xff));
    bytes[15] = 0x7f;
    repair_checksum(bytes);
    EXPECT_TRUE(
      kwaque::broker::detail::crash_report_timestamp(
        bytes, bytes.size(), timestamp));
    EXPECT_EQ(timestamp, std::numeric_limits<std::int64_t>::max());
    bytes[15] = static_cast<char>(0x80);
    repair_checksum(bytes);
    EXPECT_FALSE(
      kwaque::broker::detail::crash_report_timestamp(
        bytes, bytes.size(), timestamp));
}

TEST_F(CrashWriterTest, VersionTwoRequiresBoundedArchitectureMetadata) {
    ASSERT_TRUE(writer.record(crash_kind::startup_failure, 0, 0, false));
    std::vector<char> bytes(
      script.output.data(), script.output.data() + script.size);
    const auto architecture_size
      = kwaque::broker::detail::crash_architecture().size();
    bytes.erase(bytes.end() - 4U - architecture_size, bytes.end() - 4U);
    store_u32(bytes.data() + 40, 0);
    repair_checksum(bytes);
    std::int64_t timestamp = 0;
    EXPECT_FALSE(
      kwaque::broker::detail::crash_report_timestamp(
        bytes, bytes.size(), timestamp));
}

TEST(
  CrashWriterUninitializedTest, RecordingAndReleaseAreSafeBeforePreparation) {
    prepared_crash_writer writer;
    EXPECT_FALSE(writer.record(crash_kind::abort, SIGABRT, 0, false));
    EXPECT_FALSE(writer.release());
}

TEST(CrashReportFormatTest, KnownRecordAndEverySingleByteCorruption) {
    constexpr std::string_view bytes{
      "\x4b\x51\x43\x52\x53\x48\x31\x00\x00\x84\xd9\x7b\x89\x01\x00\x00"
      "\x00\x00\x00\x00\x03\x00\x00\x00\x01\x00\x00\x00\x16\x00\x00\x00"
      "\x00\x00\x00\x00\x02\x00\x00\x00\x46\x61\x69\x6c\x75\x72\x65\x20"
      "\x64\x75\x72\x69\x6e\x67\x20\x73\x74\x61\x72\x74\x75\x70\x76\x31"
      "\xf2\x65\xdc\x5b",
      68};
    std::int64_t timestamp = 0;
    ASSERT_TRUE(
      kwaque::broker::detail::crash_report_timestamp(
        {bytes.data(), bytes.size()}, bytes.size(), timestamp));
    EXPECT_EQ(timestamp, 1690000000000LL);
    std::array<char, bytes.size()> corrupt{};
    for (std::size_t index = 0; index < corrupt.size(); ++index) {
        std::copy(bytes.begin(), bytes.end(), corrupt.begin());
        corrupt[index] ^= 1;
        EXPECT_FALSE(
          kwaque::broker::detail::crash_report_timestamp(
            corrupt, corrupt.size(), timestamp))
          << index;
    }
    // Persisted milliseconds need not fit a nanosecond clock duration. Keeping
    // retention keys in milliseconds avoids overflowing that conversion.
    std::copy(bytes.begin(), bytes.end(), corrupt.begin());
    std::fill(
      corrupt.begin() + 8, corrupt.begin() + 15, static_cast<char>(0xff));
    corrupt[15] = 0x7f;
    corrupt[64] = static_cast<char>(0xa9);
    corrupt[65] = 0x02;
    corrupt[66] = static_cast<char>(0xe9);
    corrupt[67] = 0x02;
    EXPECT_TRUE(
      kwaque::broker::detail::crash_report_timestamp(
        corrupt, corrupt.size(), timestamp));
    EXPECT_EQ(timestamp, std::numeric_limits<std::int64_t>::max());
}

} // namespace
