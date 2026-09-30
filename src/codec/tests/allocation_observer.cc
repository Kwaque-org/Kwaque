#include "src/codec/tests/allocation_observer.h"

#include "src/bytes/test_allocation_profile.h"

#include <seastar/core/memory.hh>
#include <seastar/util/critical_alloc_section.hh>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <malloc.h>
#include <new>
#include <optional>

#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
extern "C" void* __wrap_malloc(std::size_t) noexcept;
extern "C" void* __wrap_realloc(void*, std::size_t) noexcept;
extern "C" void __wrap_free(void*) noexcept;
#endif

namespace kwaque::codec::testing {
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
namespace {
struct slot {
    void* pointer{};
    std::size_t bytes{};
    bool critical{};
};
// Loader-initialized test workspace, outside every sampled operation. It is
// never allocated in a codec frame or charged as production working memory.
std::array<slot, 16384> slots;
thread_local bool active = false;
thread_local unsigned nesting = 0;
allocation_observation observation;
std::uint64_t critical_live = 0;
bool overflow = false;
std::uint64_t inferred_frees = 0;
std::optional<seastar::memory::statistics> before;

void* tombstone() noexcept {
    return reinterpret_cast<void*>(std::uintptr_t{1});
}
slot* find(void* pointer, bool inserting) noexcept {
    const auto start = (reinterpret_cast<std::uintptr_t>(pointer) >> 4U)
                       & (slots.size() - 1U);
    slot* available = nullptr;
    for (std::size_t count = 0; count < slots.size(); ++count) {
        auto& cell = slots[(start + count) & (slots.size() - 1U)];
        if (cell.pointer == pointer) return &cell;
        if (!cell.pointer)
            return inserting ? (available ? available : &cell) : nullptr;
        if (cell.pointer == tombstone() && !available) available = &cell;
    }
    return inserting ? available : nullptr;
}
void add(std::uint64_t& value, std::uint64_t bytes) noexcept {
    if (bytes > std::numeric_limits<std::uint64_t>::max() - value) {
        overflow = true;
        return;
    }
    value += bytes;
}
void erase(void* pointer) noexcept;
void retain(
  std::uint64_t bytes, bool critical, std::uint64_t largest) noexcept {
    add(observation.live_upper_bound, bytes);
    if (critical) add(critical_live, bytes);
    observation.peak_upper_bound = std::max(
      observation.peak_upper_bound, observation.live_upper_bound);
    observation.critical_peak_upper_bound = std::max(
      observation.critical_peak_upper_bound, critical_live);
    observation.largest_allocation = std::max(
      observation.largest_allocation, largest);
}
void insert(void* pointer, bool critical) noexcept {
    auto* cell = find(pointer, true);
    if (!cell) {
        overflow = true;
        return;
    }
    if (cell->pointer == pointer) {
        // A native allocation cannot reuse a still-live allocation address.
        // An internal free can bypass link wrapping; conservatively retain its
        // charge until reuse proves release. All allocations must still agree
        // with the native counters before this bound can qualify.
        ++inferred_frees;
        erase(pointer);
    }
    const auto size = ::malloc_usable_size(pointer);
    *cell = slot{pointer, size, critical};
    retain(size, critical, size);
}
void erase(void* pointer) noexcept {
    if (auto* cell = find(pointer, false)) {
        if (
          cell->bytes > observation.live_upper_bound
          || (cell->critical && cell->bytes > critical_live)) {
            overflow = true;
        } else {
            observation.live_upper_bound -= cell->bytes;
            if (cell->critical) critical_live -= cell->bytes;
        }
        *cell = slot{tombstone(), 0, false};
    }
}
} // namespace

// The guard also covers indirect allocator calls made by a wrapped entry
// point. Only the outer entry records its native operation, preventing double
// counting without replacing allocator behavior or touching failure injection.
class allocation_call final {
public:
    allocation_call() noexcept
      : outer_(active && nesting == 0)
      , critical_(seastar::memory::is_critical_alloc_section()) {
        ++nesting;
    }
    ~allocation_call() { --nesting; }
    allocation_call(const allocation_call&) = delete;
    allocation_call& operator=(const allocation_call&) = delete;
    void allocated(void* pointer) noexcept {
        if (!outer_ || !pointer) return;
        ++observation.allocations;
        insert(pointer, critical_);
    }
    void releasing(void* pointer) noexcept {
        // Pre-existing owners are outside this observation. In particular,
        // reactor startup can release system-allocated owners during a yield;
        // those releases do not increment the native allocator's free count.
        if (!outer_ || !pointer || !find(pointer, false)) return;
        ++observation.frees;
        erase(pointer);
    }
    void resized(
      void* old,
      std::size_t old_size,
      void* pointer,
      std::size_t requested) noexcept {
        if (!outer_) return;
        if (!old) {
            allocated(pointer);
            return;
        }
        if (!pointer) {
            if (requested == 0) releasing(old);
            return;
        }
        if (pointer == old) {
            // Native shrink records one balanced malloc/free event, including
            // when the size class itself cannot shrink. Preserve the original
            // allocation's classification; no new owner was created.
            if (requested < old_size) {
                ++observation.allocations;
                if (auto* cell = find(old, false)) {
                    ++observation.frees;
                    const auto critical = cell->critical;
                    erase(old);
                    insert(pointer, critical);
                }
            }
            return;
        }
        // The native implementation allocates/copies before freeing old. Its
        // return has already freed old, but the recorded overlap must include
        // both blocks. This is conservative for other realloc implementations.
        allocated(pointer);
        releasing(old);
    }

private:
    bool outer_;
    bool critical_;
};

// Error-message translation allocates inside libc, beyond executable link
// wrappers. This synchronous call cannot yield. Count its native allocations
// and bound every one by the charge for the entire requested-byte delta; retain
// all charges until end(). This overestimates both overlap and largest size,
// without excluding translation or tolerating unobserved allocations elsewhere.
class strerror_observation final {
public:
    strerror_observation() noexcept {
        if (active && nesting == 0) before_.emplace(seastar::memory::stats());
        ++nesting;
    }
    ~strerror_observation() {
        --nesting;
        if (!before_) return;
        const auto after = seastar::memory::stats();
        if (
          after.mallocs() < before_->mallocs()
          || after.total_bytes_allocated() < before_->total_bytes_allocated()
          || after.failed_allocations() != before_->failed_allocations()
          || after.foreign_mallocs() != before_->foreign_mallocs()
          || after.fallback_allocations() != before_->fallback_allocations()) {
            overflow = true;
            return;
        }
        const auto count = after.mallocs() - before_->mallocs();
        const auto requested = after.total_bytes_allocated()
                               - before_->total_bytes_allocated();
        if (count == 0 && requested == 0) return;
        if (count == 0 || requested == 0) {
            overflow = true;
            return;
        }
        const auto largest
          = bytes::testing::charge(byte_count{requested}).value();
        if (largest > std::numeric_limits<std::uint64_t>::max() / count) {
            overflow = true;
            return;
        }
        add(observation.allocations, count);
        // The opaque call cannot classify individual owners. Conservatively
        // charge all of them to the critical subset when it is supported.
#if defined(SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION)
        constexpr bool critical = true;
#else
        constexpr bool critical = false;
#endif
        retain(count * largest, critical, largest);
    }
    strerror_observation(const strerror_observation&) = delete;
    strerror_observation& operator=(const strerror_observation&) = delete;

private:
    std::optional<seastar::memory::statistics> before_;
};
#endif

void begin_allocation_observation() noexcept {
#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
    if (active || nesting != 0) std::abort();
    slots.fill({});
    observation = {};
    critical_live = 0;
    overflow = false;
    inferred_frees = 0;
    before.emplace(seastar::memory::stats());
    active = true;
#endif
}
allocation_observation end_allocation_observation() noexcept {
#if defined(SEASTAR_DEFAULT_ALLOCATOR)
    return {};
#else
    if (!active || nesting != 0) std::abort();
    active = false;
    const auto after = seastar::memory::stats();
    observation.observed = true;
    observation.native_allocations = after.mallocs() - before->mallocs();
    observation.native_frees = after.frees() - before->frees();
    observation.complete
      = !overflow && after.mallocs() >= before->mallocs()
        && after.frees() >= before->frees()
        && observation.allocations == observation.native_allocations
        && observation.frees <= observation.native_frees
        && inferred_frees <= observation.native_frees - observation.frees
        && after.foreign_mallocs() == before->foreign_mallocs()
        && after.fallback_allocations() == before->fallback_allocations();
    return observation;
#endif
}
} // namespace kwaque::codec::testing

#if !defined(SEASTAR_DEFAULT_ALLOCATOR)
using kwaque::codec::testing::allocation_call;

extern "C" char*
__real_strerror_r(int error, char* buffer, std::size_t length) noexcept;
extern "C" char*
__wrap_strerror_r(int error, char* buffer, std::size_t length) noexcept {
    kwaque::codec::testing::strerror_observation scope;
    return __real_strerror_r(error, buffer, length);
}

extern "C" void* __real_malloc(std::size_t size) noexcept;
extern "C" void* __wrap_malloc(std::size_t size) noexcept {
    allocation_call call;
    auto* pointer = __real_malloc(size);
    call.allocated(pointer);
    return pointer;
}

extern "C" void* __real_calloc(std::size_t count, std::size_t size) noexcept;
extern "C" void* __wrap_calloc(std::size_t count, std::size_t size) noexcept {
    allocation_call call;
    auto* pointer = __real_calloc(count, size);
    call.allocated(pointer);
    return pointer;
}

extern "C" void* __real_realloc(void* old, std::size_t size) noexcept;
extern "C" void* __wrap_realloc(void* old, std::size_t size) noexcept {
    allocation_call call;
    const auto old_size = old ? ::malloc_usable_size(old) : 0;
    auto* pointer = __real_realloc(old, size);
    call.resized(old, old_size, pointer, size);
    return pointer;
}

extern "C" void*
__real_memalign(std::size_t alignment, std::size_t size) noexcept;
extern "C" void*
__wrap_memalign(std::size_t alignment, std::size_t size) noexcept {
    allocation_call call;
    auto* pointer = __real_memalign(alignment, size);
    call.allocated(pointer);
    return pointer;
}

extern "C" void*
__real_aligned_alloc(std::size_t alignment, std::size_t size) noexcept;
extern "C" void*
__wrap_aligned_alloc(std::size_t alignment, std::size_t size) noexcept {
    allocation_call call;
    auto* pointer = __real_aligned_alloc(alignment, size);
    call.allocated(pointer);
    return pointer;
}

extern "C" int __real_posix_memalign(
  void** output, std::size_t alignment, std::size_t size) noexcept;
extern "C" int __wrap_posix_memalign(
  void** output, std::size_t alignment, std::size_t size) noexcept {
    allocation_call call;
    const auto status = __real_posix_memalign(output, alignment, size);
    if (status == 0) call.allocated(*output);
    return status;
}

extern "C" void __real_free(void* pointer) noexcept;
extern "C" void __wrap_free(void* pointer) noexcept {
    allocation_call call;
    call.releasing(pointer);
    __real_free(pointer);
}

extern "C" void __real_cfree(void* pointer) noexcept;
extern "C" void __wrap_cfree(void* pointer) noexcept {
    allocation_call call;
    call.releasing(pointer);
    __real_cfree(pointer);
}

extern "C" void __real_free_sized(void* pointer, std::size_t size) noexcept;
extern "C" void __wrap_free_sized(void* pointer, std::size_t size) noexcept {
    allocation_call call;
    call.releasing(pointer);
    __real_free_sized(pointer, size);
}

extern "C" void __real_free_aligned_sized(
  void* pointer, std::size_t alignment, std::size_t size) noexcept;
extern "C" void __wrap_free_aligned_sized(
  void* pointer, std::size_t alignment, std::size_t size) noexcept {
    allocation_call call;
    call.releasing(pointer);
    __real_free_aligned_sized(pointer, alignment, size);
}

extern "C" void* __real__Znwm(std::size_t size);
extern "C" void* __wrap__Znwm(std::size_t size) {
    allocation_call call;
    auto* pointer = __real__Znwm(size);
    call.allocated(pointer);
    return pointer;
}

extern "C" void* __real__ZnwmRKSt9nothrow_t(
  std::size_t size, const std::nothrow_t& tag) noexcept;
extern "C" void* __wrap__ZnwmRKSt9nothrow_t(
  std::size_t size, const std::nothrow_t& tag) noexcept {
    allocation_call call;
    auto* pointer = __real__ZnwmRKSt9nothrow_t(size, tag);
    call.allocated(pointer);
    return pointer;
}

extern "C" void*
__real__ZnwmSt11align_val_t(std::size_t size, std::align_val_t alignment);
extern "C" void*
__wrap__ZnwmSt11align_val_t(std::size_t size, std::align_val_t alignment) {
    allocation_call call;
    auto* pointer = __real__ZnwmSt11align_val_t(size, alignment);
    call.allocated(pointer);
    return pointer;
}

extern "C" void* __real__ZnwmSt11align_val_tRKSt9nothrow_t(
  std::size_t size,
  std::align_val_t alignment,
  const std::nothrow_t& tag) noexcept;
extern "C" void* __wrap__ZnwmSt11align_val_tRKSt9nothrow_t(
  std::size_t size,
  std::align_val_t alignment,
  const std::nothrow_t& tag) noexcept {
    allocation_call call;
    auto* pointer = __real__ZnwmSt11align_val_tRKSt9nothrow_t(
      size, alignment, tag);
    call.allocated(pointer);
    return pointer;
}

extern "C" void* __real__Znam(std::size_t size);
extern "C" void* __wrap__Znam(std::size_t size) {
    allocation_call call;
    auto* pointer = __real__Znam(size);
    call.allocated(pointer);
    return pointer;
}

extern "C" void* __real__ZnamRKSt9nothrow_t(
  std::size_t size, const std::nothrow_t& tag) noexcept;
extern "C" void* __wrap__ZnamRKSt9nothrow_t(
  std::size_t size, const std::nothrow_t& tag) noexcept {
    allocation_call call;
    auto* pointer = __real__ZnamRKSt9nothrow_t(size, tag);
    call.allocated(pointer);
    return pointer;
}

extern "C" void*
__real__ZnamSt11align_val_t(std::size_t size, std::align_val_t alignment);
extern "C" void*
__wrap__ZnamSt11align_val_t(std::size_t size, std::align_val_t alignment) {
    allocation_call call;
    auto* pointer = __real__ZnamSt11align_val_t(size, alignment);
    call.allocated(pointer);
    return pointer;
}

extern "C" void* __real__ZnamSt11align_val_tRKSt9nothrow_t(
  std::size_t size,
  std::align_val_t alignment,
  const std::nothrow_t& tag) noexcept;
extern "C" void* __wrap__ZnamSt11align_val_tRKSt9nothrow_t(
  std::size_t size,
  std::align_val_t alignment,
  const std::nothrow_t& tag) noexcept {
    allocation_call call;
    auto* pointer = __real__ZnamSt11align_val_tRKSt9nothrow_t(
      size, alignment, tag);
    call.allocated(pointer);
    return pointer;
}

extern "C" void __real__ZdlPv(void* pointer) noexcept;
extern "C" void __wrap__ZdlPv(void* pointer) noexcept {
    allocation_call call;
    call.releasing(pointer);
    __real__ZdlPv(pointer);
}

extern "C" void __real__ZdlPvm(void* pointer, std::size_t size) noexcept;
extern "C" void __wrap__ZdlPvm(void* pointer, std::size_t size) noexcept {
    allocation_call call;
    call.releasing(pointer);
    __real__ZdlPvm(pointer, size);
}

extern "C" void
__real__ZdlPvRKSt9nothrow_t(void* pointer, const std::nothrow_t& tag) noexcept;
extern "C" void
__wrap__ZdlPvRKSt9nothrow_t(void* pointer, const std::nothrow_t& tag) noexcept {
    allocation_call call;
    call.releasing(pointer);
    __real__ZdlPvRKSt9nothrow_t(pointer, tag);
}

extern "C" void __real__ZdlPvSt11align_val_t(
  void* pointer, std::align_val_t alignment) noexcept;
extern "C" void __wrap__ZdlPvSt11align_val_t(
  void* pointer, std::align_val_t alignment) noexcept {
    allocation_call call;
    call.releasing(pointer);
    __real__ZdlPvSt11align_val_t(pointer, alignment);
}

extern "C" void __real__ZdlPvmSt11align_val_t(
  void* pointer, std::size_t size, std::align_val_t alignment) noexcept;
extern "C" void __wrap__ZdlPvmSt11align_val_t(
  void* pointer, std::size_t size, std::align_val_t alignment) noexcept {
    allocation_call call;
    call.releasing(pointer);
    __real__ZdlPvmSt11align_val_t(pointer, size, alignment);
}

extern "C" void __real__ZdlPvSt11align_val_tRKSt9nothrow_t(
  void* pointer,
  std::align_val_t alignment,
  const std::nothrow_t& tag) noexcept;
extern "C" void __wrap__ZdlPvSt11align_val_tRKSt9nothrow_t(
  void* pointer,
  std::align_val_t alignment,
  const std::nothrow_t& tag) noexcept {
    allocation_call call;
    call.releasing(pointer);
    __real__ZdlPvSt11align_val_tRKSt9nothrow_t(pointer, alignment, tag);
}

extern "C" void __real__ZdaPv(void* pointer) noexcept;
extern "C" void __wrap__ZdaPv(void* pointer) noexcept {
    allocation_call call;
    call.releasing(pointer);
    __real__ZdaPv(pointer);
}

extern "C" void __real__ZdaPvm(void* pointer, std::size_t size) noexcept;
extern "C" void __wrap__ZdaPvm(void* pointer, std::size_t size) noexcept {
    allocation_call call;
    call.releasing(pointer);
    __real__ZdaPvm(pointer, size);
}

extern "C" void
__real__ZdaPvRKSt9nothrow_t(void* pointer, const std::nothrow_t& tag) noexcept;
extern "C" void
__wrap__ZdaPvRKSt9nothrow_t(void* pointer, const std::nothrow_t& tag) noexcept {
    allocation_call call;
    call.releasing(pointer);
    __real__ZdaPvRKSt9nothrow_t(pointer, tag);
}

extern "C" void __real__ZdaPvSt11align_val_t(
  void* pointer, std::align_val_t alignment) noexcept;
extern "C" void __wrap__ZdaPvSt11align_val_t(
  void* pointer, std::align_val_t alignment) noexcept {
    allocation_call call;
    call.releasing(pointer);
    __real__ZdaPvSt11align_val_t(pointer, alignment);
}

extern "C" void __real__ZdaPvmSt11align_val_t(
  void* pointer, std::size_t size, std::align_val_t alignment) noexcept;
extern "C" void __wrap__ZdaPvmSt11align_val_t(
  void* pointer, std::size_t size, std::align_val_t alignment) noexcept {
    allocation_call call;
    call.releasing(pointer);
    __real__ZdaPvmSt11align_val_t(pointer, size, alignment);
}

extern "C" void __real__ZdaPvSt11align_val_tRKSt9nothrow_t(
  void* pointer,
  std::align_val_t alignment,
  const std::nothrow_t& tag) noexcept;
extern "C" void __wrap__ZdaPvSt11align_val_tRKSt9nothrow_t(
  void* pointer,
  std::align_val_t alignment,
  const std::nothrow_t& tag) noexcept {
    allocation_call call;
    call.releasing(pointer);
    __real__ZdaPvSt11align_val_tRKSt9nothrow_t(pointer, alignment, tag);
}

#endif
