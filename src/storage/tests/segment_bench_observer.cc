#include "src/storage/tests/segment_bench_observer.h"

#include <absl/crc/crc32c.h>
#include <xxhash/xxhash.h>

#include <cstddef>
#include <cstdlib>
#include <lz4frame.h>

namespace kwaque::storage::testing::segment_bench_support {
namespace {
thread_local work_sample* current = nullptr;
}
void begin_work_observation(work_sample& sample) noexcept {
    if (current) std::abort();
    sample = {};
    current = &sample;
}
void end_work_observation() noexcept {
    if (!current) std::abort();
    current = nullptr;
}
} // namespace kwaque::storage::testing::segment_bench_support

// Content identities stream through this entry; x86-64 routes it to the
// run-time selected kernel.
#if defined(__x86_64__)
#define KWAQUE_XXH3_UPDATE(prefix) prefix##XXH3_128bits_update_dispatch
#else
#define KWAQUE_XXH3_UPDATE(prefix) prefix##XXH3_128bits_update
#endif
extern "C" XXH_errorcode
  KWAQUE_XXH3_UPDATE(__real_)(XXH3_state_t*, const void*, std::size_t);
extern "C" XXH_errorcode KWAQUE_XXH3_UPDATE(__wrap_)(
  XXH3_state_t* state, const void* bytes, std::size_t size) {
    if (
      auto* sample = kwaque::storage::testing::segment_bench_support::current) {
        ++sample->digest_calls;
        sample->digest_bytes += size;
    }
    return KWAQUE_XXH3_UPDATE(__real_)(state, bytes, size);
}
#undef KWAQUE_XXH3_UPDATE
extern "C" std::size_t __real_LZ4F_decompress(
  LZ4F_dctx*,
  void*,
  std::size_t*,
  const void*,
  std::size_t*,
  const LZ4F_decompressOptions_t*);
extern "C" std::size_t __wrap_LZ4F_decompress(
  LZ4F_dctx* context,
  void* destination,
  std::size_t* produced,
  const void* source,
  std::size_t* consumed,
  const LZ4F_decompressOptions_t* options) {
    const auto result = __real_LZ4F_decompress(
      context, destination, produced, source, consumed, options);
    if (
      auto* sample = kwaque::storage::testing::segment_bench_support::current) {
        ++sample->lz4_calls;
        sample->lz4_input_bytes += *consumed;
        sample->lz4_output_bytes += *produced;
        if (!LZ4F_isError(result) && result == 0) ++sample->lz4_frames;
    }
    return result;
}

// Keep the dependency's pinned C++ ABI and call its original implementation.
// Only this diagnostic binary wraps the non-inline bulk entry.
absl::crc32c_t original_crc_bulk(absl::crc32c_t, absl::string_view) asm(
  "__real__ZN4absl12lts_2026052612crc_internal20ExtendCrc32cInternalENS0_"
  "8crc32c_tENSt3__117basic_string_viewIcNS3_11char_traitsIcEEEE");
absl::crc32c_t observed_crc_bulk(absl::crc32c_t, absl::string_view) asm(
  "__wrap__ZN4absl12lts_2026052612crc_internal20ExtendCrc32cInternalENS0_"
  "8crc32c_tENSt3__117basic_string_viewIcNS3_11char_traitsIcEEEE");
absl::crc32c_t observed_crc_bulk(absl::crc32c_t seed, absl::string_view bytes) {
    if (
      auto* sample = kwaque::storage::testing::segment_bench_support::current) {
        ++sample->crc_bulk_calls;
        sample->crc_bulk_bytes += bytes.size();
    }
    return original_crc_bulk(seed, bytes);
}
