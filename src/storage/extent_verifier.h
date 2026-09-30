#pragma once

#include "src/codec/xxh3.h"
#include "src/storage/footer_format.h"

#include <memory>

namespace kwaque::storage {
enum class extent_layout_kind { initial_append, rewrite };
// A deferred digest leaves hashing to a separate extent_digest_walk, so it
// can trail the CRC walk (and overlap I/O) while finish still requires both
// to cover the same complete byte stream.
enum class extent_integrity {
    crc32c,
    crc32c_and_digest,
    crc32c_and_deferred_digest
};

class extent_verifier;

// The digest half of a deferred-digest walk. Callers supply the same stored
// bytes, in file order, after the owning verifier accepted them. It also
// recomputes CRC32C over exactly the bytes it hashed; finish compares that
// with the verifier's CRC, so the digest cannot describe other bytes.
// Borrow each input until joined completion; one operation at a time. Any
// failure or exception closes the walk. It is independent of the verifier's
// own operations, so the two may run concurrently.
class extent_digest_walk final {
public:
    extent_digest_walk(extent_digest_walk&&) noexcept;
    extent_digest_walk& operator=(extent_digest_walk&&) = delete;
    extent_digest_walk(const extent_digest_walk&) = delete;
    extent_digest_walk& operator=(const extent_digest_walk&) = delete;
    ~extent_digest_walk();

    [[nodiscard]] seastar::future<codec::result<void>> add(
      const bytes::fragmented_buffer& stored,
      codec::cooperative_work& work,
      codec::field_context context = {});
    [[nodiscard]] runtime::file_position end() const noexcept { return end_; }
    [[nodiscard]] bool closed() const noexcept { return closed_; }
    void close() noexcept;

private:
    friend class extent_verifier;
    extent_digest_walk(
      runtime::file_position begin, codec::limits policy) noexcept;
    std::unique_ptr<codec::xxh3_128_hasher> hasher_;
    codec::limits policy_;
    runtime::file_position begin_, end_;
    std::uint32_t crc_{0};
    bool active_{false}, closed_{false};
};

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

    // Between completed initial-append groups only. Origins stay fixed; the
    // previous expectation must be fully supplied, including its footer bytes.
    // Equal coverage is a checked no-op. Byte-only growth admits a footer.
    // Entered failures close the walk without resetting accumulated integrity.
    [[nodiscard]] codec::result<void> extend_expected(
      storage::coverage, codec::cooperative_work&, codec::field_context = {});

    // Consumes source before the first await (outer frame allocation failure
    // precedes transfer). Source is already reserved, including descriptors and
    // promotion. memory excludes source, this verifier and all other
    // live/native/ frame costs. The same policy and one exclusive work/abort
    // account apply throughout a call. All temporary owners are joined before
    // state advances. Keep this verifier alive/unmoved and do not call another
    // method while an operation is pending. After entry, every
    // error/exception/abort closes it; no partial prefix can be used afterward.
    // Successful calls retain no source bytes.
    // Digest mode additionally requires the caller's reservation for the native
    // hash state and its small owning allocation for this verifier's lifetime.
    // Native state is created lazily after admission and never moved across an
    // await. Digest updates cannot roll back: any later failure closes the
    // walk.
    [[nodiscard]] seastar::future<codec::result<void>> add_block(
      bytes::fragmented_buffer&& source,
      model::batch_decode_expectation expected,
      codec::decode_budget memory,
      codec::cooperative_work& work,
      codec::field_context context = {});

    // Borrow the codec-minted owner alive and unmoved through completion.
    // Equal policy and placement reuse validation; other policies decode the
    // exact bytes with admitted temporaries. Hashing never creates an alias.
    // memory excludes the source and this verifier, as for the raw entrance.
    [[nodiscard]] seastar::future<codec::result<void>> add_block(
      const segment_block& source,
      model::batch_decode_expectation expected,
      codec::decode_budget memory,
      codec::cooperative_work& work,
      codec::field_context context = {});
    seastar::future<codec::result<void>> add_block(
      const segment_block&&,
      model::batch_decode_expectation,
      codec::decode_budget,
      codec::cooperative_work&,
      codec::field_context = {}) = delete;

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

    // Borrow a codec-owned footer. Matching policy/placement reuse its checked
    // descriptor; narrower/different policy takes the complete decoder path.
    // The same input reservation, history evidence and joined lifetime apply.
    [[nodiscard]] seastar::future<codec::result<void>> add_footer(
      const encoded_durable_footer& source,
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
    // Deferred-digest walks only: once, before any bytes are supplied, start
    // the separate digest owner for this extent.
    [[nodiscard]] codec::result<extent_digest_walk>
    deferred_digest(codec::field_context context = {});
    // Deferred-digest walks: finish, after `digest` hashed exactly the bytes
    // this walk accepted (same end and CRC). Both close on every outcome.
    [[nodiscard]] codec::result<verified_extent> finish(
      codec::cooperative_work& work,
      extent_digest_walk&& digest,
      codec::field_context context = {});
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
      codec::field_context context,
      const segment_block* typed = nullptr,
      const encoded_durable_footer* typed_footer = nullptr);
    segment_history_context history_;
    storage::coverage expected_;
    codec::limits policy_;
    extent_layout_kind kind_;
    extent_integrity integrity_;
    std::unique_ptr<codec::xxh3_128_hasher> hasher_;
    model::range_logical_end logical_;
    model::segment_relative_end physical_;
    runtime::file_position position_;
    std::optional<storage::coverage> last_;
    std::uint32_t count_{0};
    std::uint32_t crc_{0};
    state state_{state::open};
    bool digest_started_{false};
};
} // namespace kwaque::storage
