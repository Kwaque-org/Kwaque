#include "src/storage/tests/storage_metadata_fuzz_cases.h"

#include "src/storage/tests/storage_metadata_fixture.h"

namespace kwaque::storage::testing {
namespace {
void require(bool valid) {
    if (!valid) __builtin_trap();
}
void repair_if_framed(std::string& wire) {
    if (wire.size() < 32) return;
    const auto h = get(wire, 10, 2);
    if (h >= 32 && h <= 4096 && h <= wire.size()) repair(wire);
}
std::string_view envelope_bytes(std::string_view wire) {
    if (wire.size() < 32) return wire;
    const auto size = get(wire, 10, 2) + get(wire, 12, 4);
    return size <= wire.size() ? wire.substr(0, size) : wire;
}
void compare(const storage_observation& a, const storage_observation& b) {
    if (a.error.has_value() != b.error.has_value()) {
        require(
          (a.error ? a.error : b.error)->code() == errc::resource_exhausted);
        return;
    }
    require(
      a.consumed == b.consumed && a.facts == b.facts && a.digest == b.digest);
    if (
      a.error && a.error->code() != errc::resource_exhausted
      && b.error->code() != errc::resource_exhausted) {
        require(
          a.error->code() == b.error->code()
          && a.error->family() == b.error->family()
          && a.error->field() == b.error->field());
        // Malformed padding reports the start of the checked fragment, so
        // its location can depend on layout. Each observer bounds that
        // diagnostic independently; other point diagnostics still agree.
        if (a.error->code() != errc::malformed_data)
            require(a.error->byte_offset() == b.error->byte_offset());
    }
}

void object_case(
  const std::array<std::uint8_t, 8>& control,
  std::span<const std::uint8_t> payload,
  unsigned selected) {
    const bool is_manifest = selected >= 9, root = (selected % 2U) != 0;
    const auto variant = control[7];
    const std::size_t h = (variant & 8U) != 0   ? 4096U
                          : (variant & 4U) != 0 ? 40U
                                                : 32U;
    const bool empty = root && (variant & 128U) != 0;
    const metadata_fixture fixture{
      is_manifest,
      empty ? 0U : 2U + (control[6] % 7U),
      empty ? 0U : 1U,
      h,
      (variant & 64U) != 0 ? 65536U : 512U,
      (variant & 2U) != 0,
      (variant & 16U) != 0};
    std::vector<page_ref> refs;
    std::string page;
    if (!empty) {
        page = fixture.page_wire(0);
        refs.push_back(fixture.reference(page, 0));
    }
    const auto root_wire = fixture.root_wire(refs);
    const auto original = root ? root_wire : page;
    auto wire = original;
    const bool raw = (control[2] & 1U) != 0;
    const auto mutation = control[3] % 20U;
    const auto byte_offset = std::size_t{control[4]}
                             | (std::size_t{control[5]} << 8U);
    bool repin = (control[2] & 64U) != 0;
    bool known_reject = false;
    if (raw) {
        wire.clear();
        if (!payload.empty())
            wire.assign(
              reinterpret_cast<const char*>(payload.data()), payload.size());
    } else
        switch (mutation) {
        case 0:
            break;
        case 1:
            wire.resize(byte_offset % wire.size());
            known_reject = true;
            break;
        case 2:
            wire[byte_offset % wire.size()] ^= static_cast<char>(
              1U << (control[6] % 8U));
            known_reject = true;
            break;
        case 3:
            wire[byte_offset % wire.size()] ^= static_cast<char>(
              1U << (control[6] % 8U));
            repair_if_framed(wire);
            break;
        case 4:
            wire[h + 4] ^= 1;
            repair(wire);
            known_reject = true;
            repin = true;
            break;
        case 5:
            put(wire, 4, 7, 2);
            repair(wire);
            known_reject = true;
            repin = true;
            break;
        case 6:
            put(wire, h, root ? 2U : 1U, 2);
            repair(wire);
            known_reject = true;
            repin = true;
            break;
        case 7:
            put(wire, 8, 3, 2);
            put(wire, 6, 3, 2);
            repair(wire);
            known_reject = true;
            repin = true;
            break;
        case 8:
            put(wire, h + 2, 1, 2);
            repair(wire);
            known_reject = true;
            repin = true;
            break;
        case 9: {
            // Counts cannot exceed the whole-object limit, before any
            // allocation.
            const std::size_t at = is_manifest ? (root ? 76U : 84U)
                                               : (root ? 156U : 164U);
            put(wire, h + at, 65537, 4);
            repair(wire);
            known_reject = true;
            repin = true;
            break;
        }
        case 10: {
            if (root && !refs.empty())
                put(wire, h + (is_manifest ? 88U : 168U) + 4, 1, 4);
            else
                put(wire, h + (is_manifest ? 80U : 160U), 1, 4);
            repair(wire);
            known_reject = true;
            repin = true;
            break;
        }
        case 11:
            wire.back() ^= 1;
            repair(wire);
            known_reject = true;
            repin = true;
            break;
        case 12: {
            // Append a complete next object at ordinary sizes, or its bounded
            // prefix at maximum alignment. Reserve explicitly so string growth
            // and the terminator keep each allocation below 128 KiB.
            const auto tail = original.size() == 65536 ? std::size_t{512}
                                                       : original.size();
            wire.reserve(original.size() + tail);
            wire.append(original.data(), tail);
            break;
        }
        case 13: {
            // Duplicate/descending keys are checked with a repaired independent
            // pin.
            if (!root) {
                const std::size_t fixed = is_manifest ? 92U : 172U,
                                  width = is_manifest ? 104U : 16U;
                put(wire, h + fixed + width, get(wire, h + fixed, 8), 8);
            } else
                put(wire, h + (is_manifest ? 80U : 160U), 257, 4);
            repair(wire);
            known_reject = true;
            repin = true;
            break;
        }
        case 14: {
            if (!root) {
                const std::size_t fixed = is_manifest ? 92U : 172U,
                                  width = is_manifest ? 104U : 16U;
                const auto at = h + fixed + width + (is_manifest ? 0U : 8U);
                put(
                  wire,
                  at,
                  is_manifest ? get(wire, at, 8) + 1U
                              : get(wire, h + fixed + 8U, 8),
                  8);
            } else
                put(wire, h + (is_manifest ? 76U : 156U), 0, 4);
            repair(wire);
            known_reject = !empty;
            repin = true;
            break;
        }
        case 15: // Valid CRCs with an independently incorrect SHA pin.
            break;
        case 16: {
            // Unknown mandatory extension; h=32 has the mandatory feature word.
            put(wire, h > 32 ? 34U : 20U, 1, h > 32 ? 2U : 4U);
            repair(wire);
            known_reject = true;
            repin = true;
            break;
        }
        case 17: {
            if (!root && is_manifest)
                put(wire, h + 92 + 8, get(wire, h + 92, 8), 8);
            else
                put(wire, h + (is_manifest ? 76U : 156U), 256, 4);
            repair(wire);
            known_reject = true;
            repin = true;
            break;
        }
        case 18: // Exact PageRef length differs despite a matching byte digest.
            break;
        case 19: // A narrower complete-boundary interpretation of the same
                 // input.
            wire.pop_back();
            known_reject = true;
            break;
        }
    auto digest = codec::immutable_object_digest{index::sha(root_wire)};
    if (repin) {
        const auto exact = envelope_bytes(wire);
        if (root)
            digest = codec::immutable_object_digest{index::sha(exact)};
        else
            refs[0] = page_ref::make(
                        refs[0].ordinal(),
                        0,
                        fixture.entries,
                        refs[0].encoded_bytes(),
                        codec::immutable_object_digest{index::sha(exact)})
                        .value();
    }
    if (!raw && mutation == 15) {
        auto bytes = root ? digest.bytes() : refs[0].digest().bytes();
        bytes[0] ^= 1U;
        if (root)
            digest = codec::immutable_object_digest{bytes};
        else
            refs[0] = page_ref::make(
                        refs[0].ordinal(),
                        0,
                        fixture.entries,
                        refs[0].encoded_bytes(),
                        codec::immutable_object_digest{bytes})
                        .value();
        known_reject = true;
    }
    if (!raw && mutation == 18) {
        if (!root) {
            const auto length = refs[0].encoded_bytes().value();
            const auto changed = length == 65536
                                   ? length - fixture.alignment.bytes().value()
                                   : length + fixture.alignment.bytes().value();
            // At the largest alignment there is no second legal length.
            if (changed >= 32 && changed <= 65536) {
                refs[0] = page_ref::make(
                            refs[0].ordinal(),
                            0,
                            fixture.entries,
                            byte_count{changed},
                            refs[0].digest())
                            .value();
                known_reject = true;
            }
        } else {
            digest = codec::immutable_object_digest{index::sha("wrong root")};
            known_reject = true;
        }
    }
    constexpr std::array<std::size_t, 4> widths{1, 7, 67, 4096};
    const auto restrictions = static_cast<std::uint8_t>(
      (control[2] >> 1U) & 31U);
    const auto boundary = (control[2] & 128U) != 0
                            ? codec::input_boundary::complete
                            : codec::input_boundary::open;
    const auto a = observe_metadata(
      fixture,
      root,
      wire,
      refs,
      digest,
      widths[control[1] % 4U],
      restrictions,
      boundary);
    const auto b = observe_metadata(
      fixture, root, wire, refs, digest, 4096, restrictions, boundary);
    compare(a, b);
    if ((restrictions & 23U) != 0 || (!raw && known_reject))
        require(a.error.has_value());
    else if (!raw && (mutation == 0 || mutation == 12))
        require(!a.error && a.consumed.value() == original.size());
}

void walk_case(const std::array<std::uint8_t, 8>& control, bool is_manifest) {
    const auto mode = control[3] % 12U;
    const auto variant = control[7];
    const metadata_fixture fixture{
      is_manifest,
      2,
      2,
      (variant & 8U) != 0 ? 4096U : 32U,
      (variant & 64U) != 0 ? 65536U : 512U,
      (variant & 2U) != 0,
      (variant & 16U) != 0};
    const auto h = fixture.header;
    std::array<std::string, 2> pages{
      fixture.page_wire(0), fixture.page_wire(1)};
    if (mode == 4) {
        pages[1][h + 4] ^= 1;
        repair(pages[1]);
    }
    if (mode == 5 || mode == 6) {
        const auto fixed = is_manifest ? 92U : 172U;
        const auto width = is_manifest ? 104U : 16U;
        // Every page remains independently valid. Only the cross-page edge is
        // bad.
        if (is_manifest) {
            for (const auto at :
                 {std::size_t{60},
                  std::size_t{68},
                  std::size_t{92},
                  std::size_t{100},
                  std::size_t{196},
                  std::size_t{204}})
                put(
                  pages[1],
                  h + at,
                  mode == 5 ? get(pages[1], h + at, 8) + 1U
                            : get(pages[1], h + at, 8) - 1U,
                  8);
        } else {
            const auto field = mode == 5 ? 0U : 8U;
            for (std::size_t i = 0; i < 2; ++i)
                put(
                  pages[1],
                  h + fixed + i * width + field,
                  get(pages[0], h + fixed + i * width + field, 8),
                  8);
        }
        repair(pages[1]);
    }
    if (mode == 8) {
        put(pages[1], h + (is_manifest ? 80U : 160U), 3, 4);
        repair(pages[1]);
    }
    if (mode == 10) {
        pages[1].back() ^= 1;
        repair(pages[1]);
    }
    if (mode == 11) {
        put(pages[1], h + (is_manifest ? 84U : 164U), 3, 4);
        repair(pages[1]);
    }
    std::array refs{
      fixture.reference(pages[0], 0), fixture.reference(pages[1], 1)};
    if (mode == 7) {
        auto hash = refs[1].digest().bytes();
        hash[0] ^= 1U;
        refs[1] = page_ref::make(
                    refs[1].ordinal(),
                    refs[1].first_entry(),
                    refs[1].entry_count(),
                    refs[1].encoded_bytes(),
                    codec::immutable_object_digest{hash})
                    .value();
    }
    auto root_header = fixture.manifest_header;
    if (is_manifest && mode == 5)
        root_header = manifest::root_header(
          4,
          2,
          root_header.context(),
          root_header.logical_span().begin().value(),
          root_header.logical_span().end().value() + 1U);
    const auto root_wire
      = is_manifest ? manifest::expected_root(
                        root_header, refs, fixture.alignment.bytes().value(), h)
                    : fixture.root_wire(refs);
    if (mode == 2) pages[1] = pages[0];
    if (mode == 3) std::swap(pages[0], pages[1]);
    for (const std::size_t width : {67U, 4096U}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto run = [&](const auto& root, auto& walker) {
            bool failed = false;
            for (std::size_t i = 0; i < (mode == 1 ? 1U : 2U); ++i) {
                bytes::fragmented_buffer_parser input{
                  buffer("p" + pages[i], width)};
                input.skip(byte_count{1}).value();
                input.push_checkpoint().value();
                auto memory = reserve(input, work);
                if (mode == 9 && i == 1) abort.request_abort();
                auto result = walker.next(input, memory, work).get();
                if (!result) {
                    require(mode != 0 && mode != 1 && walker.closed());
                    require(
                      input.bytes_consumed().value() == 1
                      && input.checkpoint_depth() == 1);
                    require(!walker.finish(work));
                    require(!walker.next(input, memory, work).get());
                    failed = true;
                    break;
                }
                require(
                  input.bytes_consumed().value() == pages[i].size() + 1U
                  && input.checkpoint_depth() == 1);
            }
            if (!failed) {
                const auto result = walker.finish(work);
                require(mode == 0 ? result.has_value() : !result.has_value());
                if (result)
                    require(
                      result->root_digest() == root.digest()
                      && result->entry_count() == 4);
                require(walker.closed());
            }
        };
        if (is_manifest) {
            const auto root = manifest::pin(
              root_wire, root_header, work, fixture.alignment);
            range_manifest_verifier walker{root, work.policy()};
            run(root, walker);
        } else {
            const auto root = index::pin(
              root_wire, fixture.index_context, work);
            sparse_index_verifier walker{root, work.policy()};
            run(root, walker);
        }
    }
}
void supplied_extent_case(const std::array<std::uint8_t, 8>& control) {
    const auto mode = control[3] % 8U;
    for (const std::size_t width : {7U, 512U}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        const auto data = data_block();
        const auto actual = scope(100, 101, 0, 1, 512, 1024);
        auto verifier = extent_verifier::make(
                          history(),
                          actual,
                          work.policy(),
                          extent_layout_kind::initial_append,
                          {},
                          mode == 7 ? extent_integrity::crc32c
                                    : extent_integrity::crc32c_and_sha256)
                          .value();
        require(feed_block(verifier, data, work, width).has_value());
        const auto proof = verifier.finish(work).value();
        auto digest = index::sha(data);
        if (mode == 1) digest[0] ^= 1U;
        const auto declared = scope(
          100,
          mode == 4 ? 102U : 101U,
          mode == 5 ? 1U : 0U,
          mode == 5 ? 2U : 1U,
          512,
          mode == 6 ? 1536U : 1024U);
        const std::array entries{
          range_manifest_entry::make(
            sc().segment(),
            model::segment_generation::make(mode == 2 ? 2U : 1U).value(),
            declared,
            codec::extent_digest{digest})
            .value()};
        const auto ph = manifest::page_header(
          1,
          manifest::mc(),
          declared.logical().begin().value(),
          declared.logical().end().value());
        const auto wire = manifest::expected_page(ph, entries);
        const std::array refs{manifest::manifest_page_reference(wire, ph)};
        const auto rh = manifest::root_header(
          1,
          1,
          manifest::mc(),
          declared.logical().begin().value(),
          declared.logical().end().value());
        const auto root = manifest::pin(
          manifest::expected_root(rh, refs), rh, work);
        range_manifest_verifier metadata{root, work.policy()};
        bytes::fragmented_buffer_parser input{buffer(wire, width)};
        const auto decoded
          = metadata.next(input, manifest::page_memory(input, root, work), work)
              .get();
        require(decoded.has_value() && metadata.finish(work).has_value());
        // Recomputed metadata hashes certify representation only. Missing or
        // mismatched supplied bytes/namespace must remain a separate failure.
        const auto checked = validate_range_manifest_extent(
          root.context(),
          id<model::cluster_id>(
            mode == 3 ? std::uint8_t{0x51} : std::uint8_t{0x50}),
          decoded->value.entries()[0],
          proof);
        const auto expected = mode == 0                ? errc::success
                              : mode == 1              ? errc::corrupt_data
                              : mode == 2 || mode == 3 ? errc::wrong_context
                              : mode == 7              ? errc::invalid_argument
                                                       : errc::malformed_data;
        require((checked ? errc::success : checked.error().code()) == expected);
    }
}
} // namespace
void exercise_metadata_case(
  const std::array<std::uint8_t, 8>& control,
  std::span<const std::uint8_t> payload,
  unsigned selected) {
    if (selected == 13)
        supplied_extent_case(control);
    else if (selected >= 11)
        walk_case(control, selected == 12);
    else
        object_case(control, payload, selected);
}
} // namespace kwaque::storage::testing
