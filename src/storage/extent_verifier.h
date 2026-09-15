#pragma once

#include "src/codec/sha256.h"
#include "src/storage/footer_format.h"

#include <memory>

namespace kwaque::storage {
enum class extent_layout_kind { initial_append, rewrite };
enum class extent_integrity { crc32c, crc32c_and_sha256 };

// Bounded in-memory evidence over one independently specified extent. No file
// reads, descriptor vector or retained historical buffers. Callers supply each
// complete object exactly once, in file order. Only family-4 data blocks count
// as blocks/physical records; earlier family-6 envelopes contribute bytes/CRC.
// Rewrite mode allows removed logical spans only inside the supplied coverage;
// it does not establish removal authority or earlier survivor membership.
class extent_verifier final {
public:
    [[nodiscard]] static codec::result<extent_verifier> make(
      segment_history_context history,
      storage::coverage expected,
      const codec::limits& policy,
      extent_layout_kind kind = extent_layout_kind::initial_append,
      codec::field_context context = {},
      extent_integrity integrity = extent_integrity::crc32c);
    extent_verifier(const extent_verifier&) = delete;
    extent_verifier& operator=(const extent_verifier&) = delete;
    extent_verifier(extent_verifier&&) noexcept;
    extent_verifier& operator=(extent_verifier&&) = delete;

    // Consumes source before the first await (outer frame allocation failure
    // precedes transfer). Source is already reserved, including descriptors and
    // promotion. memory excludes source, this verifier and all other
    // live/native/ frame costs. The same policy and one exclusive work/abort
    // account apply throughout a call. All temporary owners are joined before
    // state advances. Keep this verifier alive/unmoved and do not call another
    // method while an operation is pending. After entry, every
    // error/exception/abort closes it; no partial prefix can be used afterward.
    // Successful calls retain no source bytes.
    // SHA mode additionally requires the caller's reservation for the native
    // SHA context and its small owning allocation for this verifier's lifetime.
    // Native state is created lazily after admission and never moved across an
    // await. SHA updates cannot roll back: any later failure closes the walk.
    [[nodiscard]] seastar::future<codec::result<void>> add_block(
      bytes::fragmented_buffer&& source,
      model::batch_decode_expectation expected,
      codec::decode_budget memory,
      codec::cooperative_work& work,
      codec::field_context context = {});

    // Verify this earlier footer as a complete context-bound envelope. Its
    // referenced history is checked when it names the current full prefix or
    // when referenced evidence is supplied. Other references are not followed
    // and never enlarge this verifier's coverage. No recursive rehash occurs.
    [[nodiscard]] seastar::future<codec::result<void>> add_footer(
      bytes::fragmented_buffer&& source,
      codec::decode_budget memory,
      codec::cooperative_work& work,
      std::optional<verified_extent> referenced = std::nullopt,
      codec::field_context context = {});

    // Initial-append prefix evidence permits writing a group footer and then
    // continuing the same walk. Rewrite coverage is published only by finish,
    // because trailing removed spans cannot be inferred from a prefix.
    [[nodiscard]] codec::result<verified_extent> checkpoint(
      codec::cooperative_work& work, codec::field_context context = {});
    // Requires exactly the supplied byte/physical coverage and (for initial
    // append) contiguous dense original logical coverage. Success and failure
    // both close the verifier. Empty rewritten coverage preserves the supplied
    // original logical span, but cannot manufacture an empty durable footer.
    [[nodiscard]] codec::result<verified_extent>
    finish(codec::cooperative_work& work, codec::field_context context = {});
    void close() noexcept;
    [[nodiscard]] bool closed() const noexcept {
        return state_ == state::closed;
    }

private:
    enum class state { open, active, closed };
    extent_verifier(
      segment_history_context history,
      storage::coverage expected,
      codec::limits policy,
      extent_layout_kind kind,
      extent_integrity integrity) noexcept;
    [[nodiscard]] boundary_fields prefix() const noexcept;
    [[nodiscard]] codec::result<void>
    ready(codec::cooperative_work& work, codec::field_context context);
    [[nodiscard]] seastar::future<codec::result<void>> add(
      bytes::fragmented_buffer&& source,
      codec::format_family family,
      model::batch_decode_expectation expected,
      codec::decode_budget memory,
      codec::cooperative_work& work,
      std::optional<verified_extent> referenced,
      codec::field_context context);
    segment_history_context history_;
    storage::coverage expected_;
    codec::limits policy_;
    extent_layout_kind kind_;
    extent_integrity integrity_;
    std::unique_ptr<codec::sha256_hasher> sha_;
    model::range_logical_end logical_;
    model::segment_relative_end physical_;
    runtime::file_position position_;
    std::optional<storage::coverage> last_;
    std::uint32_t count_{0};
    std::uint32_t crc_{0};
    state state_{state::open};
};
} // namespace kwaque::storage
