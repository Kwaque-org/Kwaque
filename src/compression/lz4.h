#pragma once

#include "src/bytes/fragmented_buffer_builder.h"
#include "src/codec/cooperative.h"
#include "src/codec/transaction.h"

#include <array>
#include <cstddef>
#include <memory>
#include <span>

// Private implementation declarations, visible only to compression and tests.
#define LZ4F_STATIC_LINKING_ONLY
#include <lz4frame.h>

namespace kwaque::compression::detail {

inline constexpr byte_count lz4_block_bytes{65'536};

enum class lz4_direction { compress, decompress };

[[nodiscard]] LZ4F_preferences_t
writer_preferences(byte_count expanded) noexcept;

// Admission precedes native entry. Input and enclosing frame/opaque owners are
// already reserved in memory. Output descriptors are decoded metadata, so a
// child cannot bypass its parent's metadata remainder. Scratch is temporary;
// returned output must be reconciled using its actual allocation_cost(), never
// by subtracting unused descriptor slots or unfilled tail bytes.
struct lz4_plan final {
    lz4_direction direction;
    codec::limits policy;
    // Residual after reserving the complete scratch/output geometry below.
    codec::decode_budget remaining;
    byte_count native_bytes;
    byte_count bounce_bytes;
    // Additional heap scratch; fixed C++ owners are in the caller's
    // reservation.
    byte_count scratch_bytes;
    byte_count output_limit;
    byte_count output_backing;
    byte_count output_metadata;
    item_count output_fragments;
    bytes::fragmented_buffer_builder_config output_config;
};

[[nodiscard]] codec::result<lz4_plan> admit_lz4(
  lz4_direction direction,
  byte_count expanded,
  byte_count encoded_limit,
  codec::cooperative_work& work,
  codec::decode_budget memory,
  codec::field_context context = {});

// Allocation/free callbacks never suspend, throw, or grow bookkeeping. The
// state is immovable once its address is handed to the C API. Only the
// requested native allocation uses malloc; failure is translated after
// returning from C.
class lz4_memory final {
public:
    struct allocation final {
        void* address{nullptr};
        byte_count requested;
        byte_count charged;
    };

    lz4_memory(
      codec::limits policy,
      byte_count reserved,
      bytes::allocation_charge_fn charge) noexcept;
    lz4_memory(const lz4_memory&) = delete;
    lz4_memory& operator=(const lz4_memory&) = delete;
    lz4_memory(lz4_memory&&) = delete;
    lz4_memory& operator=(lz4_memory&&) = delete;
    ~lz4_memory() noexcept;

    [[nodiscard]] LZ4F_CustomMem callbacks() noexcept;
    [[nodiscard]] codec::result<void>
    status(codec::field_context context = {}) const;
    [[nodiscard]] byte_count live_bytes() const noexcept { return live_; }
    [[nodiscard]] const std::array<allocation, 3>&
    allocations() const noexcept {
        return allocations_;
    }

private:
    enum class failure { none, invalid_charge, denied, allocation_failed };
    static void* allocate(void* opaque, std::size_t size) noexcept;
    static void* allocate_zeroed(void* opaque, std::size_t size) noexcept;
    static void release(void* opaque, void* pointer) noexcept;

    const codec::limits policy_;
    const byte_count reserved_;
    const bytes::allocation_charge_fn charge_;
    std::array<allocation, 3> allocations_{};
    byte_count live_;
    failure failure_{failure::none};
};

template<typename Context, auto Free>
struct context_deleter final {
    void operator()(Context* pointer) const noexcept {
        // An unfinished decoder returns its stage; it still frees everything.
        (void)Free(pointer);
    }
};

class lz4_context final {
public:
    // Check memory().status() before using a handle and after every native
    // call. A denied allocation leaves a null/unfinished context; discard it.
    // status reports policy errors or throws bad_alloc after C has returned.
    explicit lz4_context(const lz4_plan& plan);
    lz4_context(const lz4_context&) = delete;
    lz4_context& operator=(const lz4_context&) = delete;
    lz4_context(lz4_context&&) = delete;
    lz4_context& operator=(lz4_context&&) = delete;

    [[nodiscard]] LZ4F_cctx* compressor() const noexcept {
        return compressor_.get();
    }
    [[nodiscard]] LZ4F_dctx* decompressor() const noexcept {
        return decompressor_.get();
    }
    [[nodiscard]] const lz4_memory& memory() const noexcept { return memory_; }

    // One complete header on a fresh decoder, before any payload call. All
    // later attempts return closed, including after rejection. This bounded
    // synchronous leaf consumes exactly header.size() bytes on success and
    // creates no payload scratch. The caller brackets it with work checkpoints.
    [[nodiscard]] codec::result<void> read_header(
      std::span<const char> header,
      byte_count expanded,
      codec::field_context context = {});

private:
    // Reverse destruction frees the context before its callback state.
    lz4_memory memory_;
    std::unique_ptr<
      LZ4F_cctx,
      context_deleter<LZ4F_cctx, LZ4F_freeCompressionContext>>
      compressor_;
    std::unique_ptr<
      LZ4F_dctx,
      context_deleter<LZ4F_dctx, LZ4F_freeDecompressionContext>>
      decompressor_;
    bool header_attempted_{false};
};

// Inspect allocator status before native errors: admission denial stays typed,
// while a real allocation failure throws only after returning from C.
[[nodiscard]] codec::result<void> check_lz4_decode(
  const lz4_memory& memory, std::size_t code, codec::field_context context);
[[nodiscard]] codec::result<void> check_lz4_encode(
  const lz4_memory& memory, std::size_t code, codec::field_context context);

// Construct only after admission, and after header acceptance when decoding.
// Keep this and the context reachable in the outer operation until joined
// cleanup, including when construction of a subsequent owner fails. Mutable
// bounce storage is copied into output, never donated. No input bounce is
// needed: bounded fragment windows can be offered directly to the native API.
// Enforce plan.output_limit before each append, including the zero-output case:
// the empty builder's structural configuration still has a one-byte minimum.
struct lz4_staging final {
    explicit lz4_staging(const lz4_plan& plan);
    seastar::temporary_buffer<char> bounce;
    bytes::fragmented_buffer_builder output;
};

} // namespace kwaque::compression::detail
