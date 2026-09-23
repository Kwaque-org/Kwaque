#include "src/compression/lz4.h"

#include "src/base/invariant.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <lz4.h>
#include <new>

namespace kwaque::compression::detail {
namespace {

codec::error at(errc code, codec::field_context context) noexcept {
    return codec::error{code, context.family, context.field, context.origin};
}

codec::result<byte_count> allocation_charge(
  byte_count request,
  const codec::limits& policy,
  bytes::allocation_charge_fn charge,
  codec::field_context context) {
    if (request.value() == 0) return byte_count{};
    const auto served = charge(request);
    if (served < request)
        return codec::failure(at(errc::invalid_argument, context));
    if (!policy.validate_allocation(served))
        return codec::failure(at(errc::resource_exhausted, context));
    return served;
}

} // namespace

LZ4F_preferences_t writer_preferences(byte_count expanded) noexcept {
    LZ4F_preferences_t prefs{};
    prefs.frameInfo.blockSizeID = LZ4F_max64KB;
    prefs.frameInfo.blockMode = LZ4F_blockIndependent;
    prefs.frameInfo.contentChecksumFlag = LZ4F_contentChecksumEnabled;
    prefs.frameInfo.blockChecksumFlag = LZ4F_blockChecksumEnabled;
    prefs.frameInfo.contentSize = expanded.value();
    return prefs;
}

codec::result<lz4_plan> admit_lz4(
  lz4_direction direction,
  byte_count expanded,
  byte_count encoded_limit,
  codec::cooperative_work& work,
  codec::decode_budget memory,
  codec::field_context context) {
    constexpr auto block_size = lz4_block_bytes.value();
    if (auto ready = work.poll(at(errc::success, context)); !ready)
        return codec::failure(ready.error());
    if (memory.charge == nullptr)
        return codec::failure(at(errc::invalid_argument, context));
    const auto policy = work.policy();
    const auto config = policy.config();
    if (
      work.byte_quantum() < lz4_block_bytes || work.item_quantum().value() < 8
      || expanded > config.max_expanded_batch_bytes)
        return codec::failure(at(errc::resource_exhausted, context));

    const bool compress = direction == lz4_direction::compress;
    const auto prefs = writer_preferences(expanded);
    const auto update_bound = LZ4F_compressBound(block_size, &prefs);
    const auto end_bound = LZ4F_compressBound(0, &prefs);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-LZ4-BOUND"},
      !LZ4F_isError(update_bound) && !LZ4F_isError(end_bound),
      "fixed compression profile has no valid native bound");
    const byte_count bounce{
      compress
        ? std::max({update_bound, end_bound, std::size_t{LZ4F_HEADER_SIZE_MAX}})
        : block_size};

    // Contexts are opaque. Reserve a bounded 4-KiB envelope; native callbacks
    // enforce the total even if a library's actual context layout changes.
    byte_count native;
    for (const auto request :
         {byte_count{4096},
          byte_count{
            compress ? static_cast<std::uint64_t>(LZ4_sizeofState())
                     : block_size + 4U},
          byte_count{block_size}}) {
        const auto charged = allocation_charge(
          request, policy, memory.charge, context);
        if (!charged) return codec::failure(charged.error());
        native = *native.checked_add(*charged); // Three bounded allocations.
    }
    const auto bounce_charge = allocation_charge(
      bounce, policy, memory.charge, context);
    if (!bounce_charge) return codec::failure(bounce_charge.error());
    // Fixed owners already reside in the caller's reserved frame/owner storage.
    // Include them in the scratch ceiling without charging that storage twice.
    const byte_count owners{
      sizeof(lz4_context) + sizeof(lz4_staging) + sizeof(lz4_plan)
      + LZ4F_HEADER_SIZE_MAX + sizeof(LZ4F_frameInfo_t)};
    const byte_count scratch{native.value() + bounce_charge->value()};
    if (scratch.value() + owners.value() > config.max_scratch_bytes.value())
        return codec::failure(at(errc::resource_exhausted, context));

    // This packing bound belongs only to our writer, not reader acceptance.
    const auto blocks = (expanded.value() + block_size - 1U) / block_size;
    const byte_count frame_bound{
      expanded.value() + 8U * blocks + LZ4F_HEADER_SIZE_MAX + 8U};
    const auto output_limit = compress ? std::min(
                                           {frame_bound,
                                            encoded_limit,
                                            config.max_encoded_body_bytes})
                                       : expanded;
    if (compress && output_limit.value() == 0)
        return codec::failure(at(errc::resource_exhausted, context));

    // Fixed-width tails bound both payload capacity and fragment count. Smaller
    // tails may fit a tighter served-capacity or retained-memory allowance.
    auto width = std::min(
      block_size, std::max(output_limit.value(), std::uint64_t{1}));
    while (width != 0) {
        const auto count = output_limit.value() == 0
                             ? 0U
                             : 1U + (output_limit.value() - 1U) / width;
        if (count > config.max_buffer_fragments.value()) break;
        const auto tail = allocation_charge(
          byte_count{width}, policy, memory.charge, context);
        const auto descriptors = allocation_charge(
          byte_count{
            count * bytes::fragmented_buffer::fragment_descriptor_size()},
          policy,
          memory.charge,
          context);
        const auto promotion = allocation_charge(
          byte_count{sizeof(seastar::free_deleter_impl)},
          policy,
          memory.charge,
          context);
        if (!tail || !descriptors || !promotion) {
            for (const auto* result : {&tail, &descriptors, &promotion}) {
                if (
                  !*result
                  && result->error().code() != errc::resource_exhausted)
                    return codec::failure(result->error());
            }
            width /= 2U;
            continue;
        }
        const byte_count backing{count * tail->value()};
        const byte_count metadata{
          descriptors->value() + count * promotion->value()};
        const auto remaining = policy.remaining_operation_bytes(
          {.staged_output = backing,
           .decoded_metadata = metadata,
           .scratch = scratch},
          memory.operation_remaining);
        if (
          remaining && backing <= config.max_retained_bytes
          && metadata <= memory.metadata_remaining) {
            if (auto ready = work.poll(at(errc::success, context)); !ready)
                return codec::failure(ready.error());
            const byte_count logical_capacity{
              std::max(output_limit.value(), width)};
            return lz4_plan{
              direction,
              policy,
              memory.charge,
              native,
              bounce,
              scratch,
              output_limit,
              backing,
              metadata,
              item_count{count},
              {.initial_fragment_bytes = byte_count{width},
               .max_fragment_bytes = byte_count{width},
               .max_total_bytes = logical_capacity,
               .max_retained_bytes
               = byte_count{std::max(count, std::uint64_t{1}) * width},
               .max_fragments = static_cast<std::size_t>(
                 std::max(count, std::uint64_t{1}))}};
        }
        width /= 2U;
    }
    return codec::failure(at(errc::resource_exhausted, context));
}

lz4_memory::lz4_memory(
  codec::limits policy,
  byte_count reserved,
  bytes::allocation_charge_fn charge) noexcept
  : policy_(policy)
  , reserved_(reserved)
  , charge_(charge) {}

lz4_memory::~lz4_memory() noexcept {
    KWAQUE_INVARIANT(
      invariant_id{"KQ-LZ4-ALLOCATION-LIFETIME"},
      live_.value() == 0
        && std::ranges::all_of(
          allocations_,
          [](const allocation& entry) { return entry.address == nullptr; }),
      "native allocations outlived their callback state");
}

LZ4F_CustomMem lz4_memory::callbacks() noexcept {
    return {allocate, allocate_zeroed, release, this};
}

void* lz4_memory::allocate(void* opaque, std::size_t size) noexcept {
    auto& state = *static_cast<lz4_memory*>(opaque);
    if (state.failure_ != failure::none) return nullptr;
    if (state.charge_ == nullptr) {
        state.failure_ = failure::invalid_charge;
        return nullptr;
    }
    const byte_count request{size};
    const auto charged = state.charge_(request);
    if (charged < request) {
        state.failure_ = failure::invalid_charge;
        return nullptr;
    }
    const auto next = state.live_.checked_add(charged);
    auto slot = std::ranges::find_if(
      state.allocations_,
      [](const allocation& entry) { return entry.address == nullptr; });
    if (
      size == 0 || !state.policy_.validate_allocation(charged) || !next
      || *next > state.reserved_
      || *next > state.policy_.config().max_scratch_bytes
      || slot == state.allocations_.end()) {
        state.failure_ = failure::denied;
        return nullptr;
    }
    void* pointer = std::malloc(size);
    if (pointer == nullptr) {
        state.failure_ = failure::allocation_failed;
        return nullptr;
    }
    *slot = allocation{pointer, request, charged};
    state.live_ = *next;
    return pointer;
}

void* lz4_memory::allocate_zeroed(void* opaque, std::size_t size) noexcept {
    void* pointer = allocate(opaque, size);
    if (pointer != nullptr) std::memset(pointer, 0, size);
    return pointer;
}

void lz4_memory::release(void* opaque, void* pointer) noexcept {
    if (pointer == nullptr) return;
    auto& state = *static_cast<lz4_memory*>(opaque);
    const auto slot = std::ranges::find_if(
      state.allocations_,
      [pointer](const allocation& entry) { return entry.address == pointer; });
    KWAQUE_INVARIANT(
      invariant_id{"KQ-LZ4-ALLOCATION-OWNER"},
      slot != state.allocations_.end(),
      "free of an unowned native allocation");
    state.live_ = *state.live_.checked_sub(slot->charged);
    *slot = allocation{};
    std::free(pointer);
}

codec::result<void> lz4_memory::status(codec::field_context context) const {
    switch (failure_) {
    case failure::none:
        return {};
    case failure::invalid_charge:
        return codec::failure(at(errc::invalid_argument, context));
    case failure::denied:
        return codec::failure(at(errc::resource_exhausted, context));
    case failure::allocation_failed:
        throw std::bad_alloc{};
    }
    KWAQUE_INVARIANT(
      invariant_id{"KQ-LZ4-ALLOCATION-STATUS"},
      false,
      "invalid native allocation status");
}

lz4_context::lz4_context(const lz4_plan& plan)
  : memory_(plan.policy, plan.native_bytes, plan.charge) {
    if (plan.direction == lz4_direction::compress)
        compressor_.reset(LZ4F_createCompressionContext_advanced(
          memory_.callbacks(), LZ4F_VERSION));
    else
        decompressor_.reset(LZ4F_createDecompressionContext_advanced(
          memory_.callbacks(), LZ4F_VERSION));
}

codec::result<void> check_lz4_decode(
  const lz4_memory& memory, std::size_t code, codec::field_context context) {
    if (auto ready = memory.status(context); !ready) return ready;
    if (!LZ4F_isError(code)) return {};
    switch (LZ4F_getErrorCode(code)) {
    case LZ4F_ERROR_headerChecksum_invalid:
    case LZ4F_ERROR_blockChecksum_invalid:
    case LZ4F_ERROR_contentChecksum_invalid:
        return codec::failure(at(errc::corrupt_data, context));
    case LZ4F_ERROR_allocation_failed:
        throw std::bad_alloc{};
    default:
        return codec::failure(at(errc::malformed_data, context));
    }
}

codec::result<void> check_lz4_encode(
  const lz4_memory& memory, std::size_t code, codec::field_context context) {
    if (auto ready = memory.status(context); !ready) return ready;
    KWAQUE_INVARIANT(
      invariant_id{"KQ-LZ4-ENCODE-STATE"},
      !LZ4F_isError(code),
      "bounded compression call failed after admission");
    return {};
}

codec::result<void> lz4_context::read_header(
  std::span<const char> header,
  byte_count expanded,
  codec::field_context context) {
    if (auto ready = memory_.status(context); !ready) return ready;
    if (!decompressor_ || header_attempted_)
        return codec::failure(at(errc::closed, context));
    header_attempted_ = true;
    if (
      header.size() < LZ4F_HEADER_SIZE_MIN
      || header.size() > LZ4F_HEADER_SIZE_MAX)
        return codec::failure(at(errc::malformed_data, context));
    LZ4F_frameInfo_t info{};
    auto consumed = header.size();
    const auto code = LZ4F_getFrameInfo(
      decompressor_.get(), &info, header.data(), &consumed);
    if (auto ready = check_lz4_decode(memory_, code, context); !ready)
        return ready;
    // Skippable headers legitimately consume only their four-byte magic here.
    // Classify the frame before requiring full ordinary-header consumption.
    if (
      info.frameType != LZ4F_frame || info.blockSizeID != LZ4F_max64KB
      || info.blockMode != LZ4F_blockIndependent
      || info.blockChecksumFlag != LZ4F_blockChecksumEnabled
      || info.contentChecksumFlag != LZ4F_contentChecksumEnabled
      || (static_cast<unsigned char>(header[4]) & 1U) != 0 || info.dictID != 0)
        return codec::failure(at(errc::unsupported_format, context));
    const bool has_content_size = (static_cast<unsigned char>(header[4]) & 8U)
                                  != 0;
    if (!has_content_size && expanded.value() != 0)
        return codec::failure(at(errc::unsupported_format, context));
    if (consumed != header.size() || info.contentSize != expanded.value())
        return codec::failure(at(errc::malformed_data, context));
    return {};
}

lz4_staging::lz4_staging(const lz4_plan& plan)
  : bounce(plan.bounce_bytes.value())
  , output(plan.output_config) {
    const auto reserved = output.reserve_fragments(plan.output_fragments);
    KWAQUE_INVARIANT(
      invariant_id{"KQ-LZ4-OUTPUT-RESERVE"},
      reserved.has_value(),
      "admitted compression descriptor reservation failed");
}

} // namespace kwaque::compression::detail
