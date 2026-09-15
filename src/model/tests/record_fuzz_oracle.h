#pragma once

#include "src/base/error.h"
#include "src/codec/digest.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace kwaque::model::testing {

struct record_probe final {
    errc error{errc::success};
    std::size_t used{0};
    std::uint64_t delta{0};
    std::int64_t timestamp{0};
    std::uint64_t headers{0};
};
struct batch_probe final {
    errc error{errc::success};
    std::size_t used{0};
    // Encoded start and normalized raw length. For LZ4, compare raw bytes via
    // record_digest; record_bytes is not a contiguous encoded-wire extent.
    std::size_t records_at{0};
    std::size_t record_bytes{0};
    std::uint64_t original{0}, retained{0}, headers{0};
    std::int64_t timestamp{0};
    std::uint64_t begin{0}, end{0};
    codec::sha256_digest digest{};
    codec::sha256_digest record_digest{};
    bool compressed{false};
    bool canonical{false};
};

// Independent bounded arithmetic grammar. Encoded fuzz bytes are contiguous;
// native LZ4 expansion uses fixed-size slabs. No model or compression-wrapper
// encoder/decoder/factory is used; the caller runs inside the joined reactor.
record_probe probe_record(
  std::string_view wire,
  std::int64_t base,
  std::uint64_t original,
  std::uint64_t headers,
  bool complete,
  std::uint64_t extent_limit = UINT64_MAX);
batch_probe probe_batch(
  std::string_view wire,
  bool assigned,
  bool complete,
  bool wrong_context,
  std::uint64_t extent_limit = UINT64_MAX,
  bool narrow_compression_work = false);
std::uint64_t little(std::string_view wire, std::size_t at, std::size_t width);
void put(
  std::string& wire, std::size_t at, std::uint64_t value, std::size_t width);
std::string varuint(std::uint64_t value);
// Small independent native frame fixture; bounded to one native input quantum.
std::string lz4_records(std::string_view records);
codec::sha256_digest
fingerprint(std::string_view fixed, std::string_view records);
void repair_crc(std::string& wire);
std::string frame(std::string body, bool assigned, bool extension);
std::string identity_bytes();

} // namespace kwaque::model::testing
