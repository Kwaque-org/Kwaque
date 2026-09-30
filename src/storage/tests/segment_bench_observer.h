#pragma once

#include <cstdint>

namespace kwaque::storage::testing::segment_bench_support {
// Reactor-local diagnostic intervals. Bulk CRC excludes the dependency's
// inlined small-input path; it must never be reported as total CRC traffic.
struct work_sample final {
    std::uint64_t digest_calls{0}, digest_bytes{0};
    std::uint64_t crc_bulk_calls{0}, crc_bulk_bytes{0};
    std::uint64_t lz4_calls{0}, lz4_frames{0}, lz4_input_bytes{0},
      lz4_output_bytes{0};
};
void begin_work_observation(work_sample&) noexcept;
void end_work_observation() noexcept;
} // namespace kwaque::storage::testing::segment_bench_support
