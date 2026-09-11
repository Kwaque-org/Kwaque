#pragma once

#include "src/base/error.h"
#include "src/base/result.h"
#include "src/base/units.h"
#include "src/runtime/time.h"

#include <algorithm>

namespace kwaque::model {

// Positive topic bounds. The storage owner separately accounts for complete
// appends and their framing when enforcing segment rolling and admission.
class topic_policy final {
public:
    [[nodiscard]] static constexpr topic_policy defaults() noexcept {
        return topic_policy{
          byte_count{1'048'576},
          byte_count{1'000'000'000},
          runtime::monotonic_duration{3'600'000'000'000}};
    }

    [[nodiscard]] static result<topic_policy> make(
      byte_count maximum_record_bytes,
      byte_count segment_bytes,
      runtime::monotonic_duration segment_lifetime) noexcept {
        if (
          maximum_record_bytes.value() == 0 || segment_bytes.value() == 0
          || segment_lifetime.nanoseconds() == 0) {
            return failure(errc::invalid_argument);
        }
        return topic_policy{
          maximum_record_bytes, segment_bytes, segment_lifetime};
    }

    // Complete encoded record bytes, including record framing and headers,
    // but excluding the enclosing batch envelope.
    [[nodiscard]] constexpr byte_count maximum_record_bytes() const noexcept {
        return maximum_record_bytes_;
    }

    // Data-file-size rolling threshold, not an allocation request.
    [[nodiscard]] constexpr byte_count segment_bytes() const noexcept {
        return segment_bytes_;
    }

    // Elapsed age; the owner supplies the clock and performs actual rolling.
    [[nodiscard]] constexpr runtime::monotonic_duration
    segment_lifetime() const noexcept {
        return segment_lifetime_;
    }

    // The caller supplies the active codec's cap. This intersection does not
    // size a record or admit a complete storage append.
    [[nodiscard]] result<byte_count>
    effective_record_limit(byte_count codec_limit) const noexcept {
        if (codec_limit.value() == 0) {
            return failure(errc::invalid_argument);
        }
        return std::min(maximum_record_bytes_, codec_limit);
    }

    bool operator==(const topic_policy&) const noexcept = default;

private:
    constexpr topic_policy(
      byte_count maximum_record_bytes,
      byte_count segment_bytes,
      runtime::monotonic_duration segment_lifetime) noexcept
      : maximum_record_bytes_(maximum_record_bytes)
      , segment_bytes_(segment_bytes)
      , segment_lifetime_(segment_lifetime) {}

    byte_count maximum_record_bytes_;
    byte_count segment_bytes_;
    runtime::monotonic_duration segment_lifetime_;
};

} // namespace kwaque::model
