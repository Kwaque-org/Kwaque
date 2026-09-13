#pragma once

#include "src/model/record_codec.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>

namespace kwaque::model {

// Coordinates within the scanner's immutable record region, not owning slices.
// Nullness is represented by optional<record_byte_range>; zero length is empty.
struct record_byte_range final {
    byte_count offset;
    byte_count length;
    [[nodiscard]] byte_count size() const noexcept { return length; }
    bool operator==(const record_byte_range&) const noexcept = default;
};

struct record_header_range final {
    record_byte_range name;
    std::optional<record_byte_range> value;
};

struct record_layout final {
    record_fields fields;
    record_byte_range encoded;
    std::optional<record_byte_range> key;
    std::optional<record_byte_range> value;
    [[nodiscard]] std::span<const record_header_range>
    headers() const& noexcept {
        return {header_storage.data(), header_count};
    }
    std::span<const record_header_range> headers() const&& = delete;

private:
    template<bool Scan>
    friend class detail::record_decoder;
    friend class record_region_scanner;
    std::array<record_header_range, 64> header_storage;
    std::size_t header_count{0};
};

enum class record_region_kind : std::uint8_t { dense_original, sparse };

struct record_region_context final {
    runtime::wall_time timestamp_base;
    range_logical_count original_count;
    item_count retained_count;
    item_count header_count;
    record_region_kind kind{record_region_kind::dense_original};
};

// Owns the immutable bytes and an admitted parser alias. next() fully validates
// each record, skipping payloads without field shares or a materialized tree.
// complete() becomes true only after exact count/header/order/byte exhaustion;
// an early stop proves nothing about the unseen suffix. Any next() failure or
// exception makes the scanner terminal and clears current().
//
// The only record metadata storage is the inline current layout. Account
// sizeof(record_region_scanner) (including its 64 header slots) in the caller's
// frame/object reservation. No allocation scales with the number of records.
// make() charges input backing, descriptor capacity, possible promotion and the
// parser alias against memory. This reservation lives until the scanner and
// any explicitly created aliases are destroyed; remaining() excludes it.
// The region transfers before the first await, including rejection/exception;
// failure to allocate the outer coroutine frame precedes transfer.
//
// current()/bytes() borrow this owner. Layout coordinates can be copied but do
// not retain bytes. materialize_current() explicitly admits independent field
// owners through decode_record(); it does not advance or invalidate the scan.
// No method permits mutation of encoded contents. Moves/destruction invalidate
// borrows; do not move/destroy/access the scanner while an operation is
// pending. The caller bounds native/opaque destructor work; close() joins byte
// cleanup.
class record_region_scanner final {
public:
    record_region_scanner(const record_region_scanner&) = delete;
    record_region_scanner& operator=(const record_region_scanner&) = delete;
    record_region_scanner(record_region_scanner&&) noexcept;
    record_region_scanner& operator=(record_region_scanner&&) = delete;
    ~record_region_scanner() = default;

    [[nodiscard]] static seastar::future<codec::result<record_region_scanner>>
    make(
      kwaque::bytes::fragmented_buffer&& region,
      record_region_context expected,
      codec::decode_budget memory,
      codec::cooperative_work& work,
      codec::field_context context = {});

    // Same immutable policy and live, exclusive work/abort owner as make().
    // Fixed layout initialization/transfer needs 4*sizeof(record_layout) work
    // bytes and 64 items. Growing payload/header walks share residual work.
    [[nodiscard]] seastar::future<codec::result<bool>>
    next(codec::cooperative_work& work) &;
    // memory narrows remaining(), excludes other live materializations and
    // native/frame costs, and uses the same charge function. The returned
    // residual reserves field/header metadata; use it for subsequent owners.
    // Temporary parser metadata is refunded only after joined cleanup. The
    // scanner's original backing reservation follows all returned aliases.
    [[nodiscard]] seastar::future<codec::result<decoded_record>>
    materialize_current(
      codec::decode_budget memory, codec::cooperative_work& work) &;
    [[nodiscard]] seastar::future<> close(codec::cooperative_work& work) &;

    [[nodiscard]] bool complete() const noexcept { return complete_; }
    [[nodiscard]] codec::decode_budget remaining() const noexcept {
        return memory_;
    }
    [[nodiscard]] const record_layout* current() const& noexcept {
        return current_ ? &*current_ : nullptr;
    }
    const record_layout* current() const&& = delete;
    [[nodiscard]] const kwaque::bytes::fragmented_buffer&
    bytes() const& noexcept {
        return region_;
    }
    const kwaque::bytes::fragmented_buffer& bytes() const&& = delete;

private:
    record_region_scanner(
      kwaque::bytes::fragmented_buffer&& region,
      kwaque::bytes::fragmented_buffer_parser&& parser,
      record_region_context expected,
      codec::decode_budget memory,
      codec::limits policy,
      codec::field_context context) noexcept;

    kwaque::bytes::fragmented_buffer region_;
    kwaque::bytes::fragmented_buffer_parser parser_;
    record_region_context expected_;
    codec::decode_budget memory_;
    codec::limits policy_;
    codec::field_context context_;
    std::optional<record_layout> current_;
    std::uint64_t index_{0};
    std::uint64_t headers_{0};
    std::uint64_t previous_delta_{0};
    bool terminal_{false};
    bool complete_{false};
};

namespace detail {
// The caller has already admitted layout storage/work and parent backing.
// Shares the exact-record transaction and grammar with materializing decode.
[[nodiscard]] seastar::future<codec::result<record_layout>> scan_record(
  bytes::fragmented_buffer_parser& input,
  record_decode_context expected,
  codec::decode_budget memory,
  codec::cooperative_work& work,
  codec::field_context context);
} // namespace detail

} // namespace kwaque::model
