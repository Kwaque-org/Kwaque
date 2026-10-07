#pragma once

#include "src/base/units.h"
#include "src/storage/extent_verifier.h"
#include "src/storage/local_metadata.h"
#include "src/storage/retry_format.h"
#include "src/storage/workload_budget.h"

#include <cstdint>
#include <deque>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace kwaque::storage::testing::segment_bench_support {
// A group is `blocks` child batches sealed by one durable footer, as one
// group commit freezes them; `window` groups share one segment barrier. The
// baseline alignment is the smallest supported device write alignment.
struct shape final {
    const char* name;
    std::size_t payload_bytes{0};
    compression::codec_id encoding{compression::codec_id::none};
    bool fragmented{false};
    std::uint64_t alignment{4096};
    std::uint32_t groups{2}, blocks{4}, window{1}, segments{1};
    bool replace{false}, retain{false}, pressure{false}, reopen{false};
    std::uint64_t page_bytes{65536};
    bool zero_timestamps{false};
    [[nodiscard]] std::uint32_t batches() const noexcept {
        return groups * blocks;
    }
};
inline constexpr std::uint32_t maximum_batches = 128;
// The writer's own per-group block bound.
inline constexpr std::uint32_t maximum_group_blocks = 64;
// Concurrent segment owners of this benchmark. The fixture can give up to
// maximum_extent_segments segments distinct identities.
inline constexpr std::uint32_t maximum_segments = 4;
inline constexpr std::uint32_t maximum_extent_segments = 32;
inline constexpr byte_count working_bytes{32_MiB};

struct group_input final {
    std::vector<encoded_assigned_batch> children;
    std::vector<segment_block> blocks;
    std::vector<bytes::fragmented_buffer> block_wires;
    bytes::fragmented_buffer footer, wire;
    storage::coverage whole_prefix;
    runtime::file_position begin, end;
    std::size_t wire_fragments{0};
};
struct extent_input final {
    local_segment_descriptor descriptor;
    segment_history_context history;
    bytes::fragmented_buffer header;
    std::deque<group_input> groups;
    std::vector<completed_retry> completed;
    std::optional<verified_extent> proof;
    std::optional<runtime::file_position> sealed_end;
    std::uint64_t child_bytes{0}, child_fragments{0}, encoded_bytes{0};
    codec::content_digest header_digest{};
};

[[nodiscard]] codec::limits policy(const shape&);
// One child batch of the shape, assigned at `logical` in `segment`: what
// every extent here is made of. `sequence` is its place in its producer's
// stream and repeats nowhere in one store. The inputs are borrowed until the
// batch is returned.
[[nodiscard]] seastar::future<encoded_assigned_batch> make_batch(
  const shape&,
  const segment_context&,
  model::range_logical_end logical,
  std::uint64_t sequence,
  codec::cooperative_work&);
[[nodiscard]] seastar::future<extent_input>
make_extent(const shape&, std::uint32_t segment, codec::cooperative_work&);
[[nodiscard]] byte_count retained_bound(const extent_input&);
[[nodiscard]] seastar::future<verified_extent>
verify_extent(extent_input&, bool raw, codec::cooperative_work&);
[[nodiscard]] seastar::future<verified_extent>
encode_extent(extent_input&, codec::cooperative_work&);
// The exact extent of these children, the input's batches in order, stored
// from its data start in groups of the given sizes, each sealed by one
// footer. Consumes the children.
[[nodiscard]] seastar::future<verified_extent> encode_cut_extent(
  const extent_input&,
  std::span<encoded_assigned_batch> children,
  std::span<const std::uint32_t> cuts,
  codec::cooperative_work&);
void export_extent(extent_input&, const shape&, std::string_view path);
[[nodiscard]] seastar::future<> export_records(
  extent_input&, const shape&, std::string_view path, codec::cooperative_work&);
} // namespace kwaque::storage::testing::segment_bench_support
