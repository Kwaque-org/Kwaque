#include "src/storage/tests/storage_format_fuzz_cases.h"

#include "src/storage/tests/storage_format_fixture.h"

namespace kwaque::storage::testing {
namespace {
void require(bool valid) {
    if (!valid) __builtin_trap();
}

void repaired(std::string& wire) {
    if (wire.size() < 32) return;
    const auto h = get(wire, 10, 2);
    if (h >= 32 && h <= 4096 && h <= wire.size()) repair(wire);
}

void extent_case(const std::array<std::uint8_t, 8>& control) {
    const bool compressed = (control[7] & 1U) != 0;
    const auto mode = control[3] % 8U;
    for (const std::size_t width : {7U, 512U}) {
        seastar::abort_source abort;
        codec::cooperative_work work{codec::limits::defaults(), abort};
        auto verifier = extent_verifier::make(
                          history(),
                          scope(100, 102, 0, 2, 512, 2048),
                          work.policy(),
                          extent_layout_kind::initial_append,
                          {},
                          extent_integrity::crc32c_and_sha256)
                          .value();
        const auto first = data_block(100, 0, 512, false, 0x30, 1, compressed);
        const auto second = data_block(
          101, 1, 1536, false, 0x30, 1, compressed);
        const auto span = scope(100, 101, 0, 1, 512, 1024);
        const auto middle = footer_wire(
          {span, 1, span, crc(first)},
          {history(), runtime::file_position{1024}});
        require(feed_block(verifier, first, work, width).has_value());
        auto footer = middle;
        if (mode == 1) {
            footer.back() ^= 1;
            repair(footer);
        }
        if (mode == 2) {
            put(footer, 32 + 184, crc(first) ^ 1U, 4);
            repair(footer);
        }
        if (mode == 3) {
            put(footer, 32 + 72, 1536, 8);
            repair(footer);
        }
        if (mode == 4) footer.pop_back();
        if (mode == 5) footer = first;
        if (mode == 6) abort.request_abort();
        const auto accepted = feed_footer(verifier, footer, work);
        if (mode != 0 && mode != 7) {
            require(!accepted.has_value() && verifier.closed());
            require(!verifier.finish(work));
            continue;
        }
        require(accepted.has_value());
        const auto last = feed_block(
          verifier, mode == 7 ? first : second, work, width);
        if (mode == 7) {
            require(!last.has_value() && verifier.closed());
            continue;
        }
        require(last.has_value());
        const auto evidence = verifier.finish(work).value();
        require(
          evidence.boundary().data_crc32c == crc(first + middle + second));
        require(
          evidence.digest()->bytes() == exact_sha(first + middle + second));
        require(evidence.boundary().block_count == 2 && verifier.closed());
    }
}
} // namespace

void exercise_storage_case(std::span<const std::uint8_t> script) {
    if (script.size() > storage_fuzz_max_input) return;
    std::array<std::uint8_t, 8> control{};
    const auto prefix = std::min(script.size(), control.size());
    std::copy_n(script.begin(), prefix, control.begin());
    script = script.subspan(prefix);
    const auto selected = control[0] % 7U;
    if (selected == 6) {
        extent_case(control);
        return;
    }
    const auto kind = static_cast<storage_case>(selected);
    const auto variant = control[7];
    const std::size_t outer = (variant & 8U) != 0   ? 4096U
                              : (variant & 4U) != 0 ? 40U
                                                    : 32U;
    const std::size_t inner = (variant & 32U) != 0   ? 4096U
                              : (variant & 16U) != 0 ? 40U
                                                     : 32U;
    const storage_fixture fixture{
      kind,
      (variant & 1U) != 0,
      (variant & 2U) != 0,
      outer,
      inner,
      (variant & 64U) != 0 ? 4096U : 512U,
      (variant & 128U) != 0 ? 4096U : 512U};
    const auto flags = control[2];
    const auto restrictions = static_cast<std::uint8_t>((flags >> 1U) & 31U);
    const auto mutation = control[3] % 9U;
    const auto index = std::size_t{control[4]}
                       | (std::size_t{control[5]} << 8U);
    const bool raw = (flags & 1U) != 0;
    std::string wire = fixture.wire;
    if (raw) {
        wire.clear();
        if (!script.empty())
            wire.assign(
              reinterpret_cast<const char*>(script.data()), script.size());
    } else {
        switch (mutation) {
        case 0:
            break;
        case 1:
            wire.resize(index % wire.size());
            break;
        case 2:
        case 3:
            wire[index % wire.size()] ^= static_cast<char>(
              1U << (control[6] % 8U));
            if (mutation == 3) repaired(wire);
            break;
        case 4: {
            const std::size_t sc = kind == storage_case::wal ? 24U
                                   : selected >= 4           ? 4U
                                                             : 0U;
            wire[outer + sc] ^= 1;
            repair(wire);
            break;
        }
        case 5: {
            const std::size_t position = kind == storage_case::wal ? 16U
                                         : selected >= 4           ? 76U
                                                                   : 72U;
            put(
              wire, outer + position, get(wire, outer + position, 8) + 512U, 8);
            repair(wire);
            break;
        }
        case 6:
            wire.back() ^= 1;
            repair(wire);
            break;
        case 7:
            wire += fixture.wire;
            break;
        case 8:
            if (kind == storage_case::block)
                wire = block_wire(
                  fixture.child + fixture.child, fixture.block_context, outer);
            else if (kind == storage_case::wal)
                wire = wal_wire(
                  fixture.child + fixture.child, fixture.wal_context, outer);
            else {
                put(wire, 4, 0, 2);
                repair(wire);
            }
            break;
        }
    }
    constexpr std::array<std::size_t, 4> widths{1, 7, 67, 4096};
    const auto observed = observe_storage(
      fixture,
      wire,
      widths[control[1] % widths.size()],
      codec::limits::defaults(),
      restrictions);
    const auto other = observe_storage(
      fixture, wire, 4096, codec::limits::defaults(), restrictions);
    // All semantic facts and cursor publication are independent of layout.
    // Resource errors may stop at different admission leaves with different
    // physical descriptor counts, so their diagnostic field is not compared.
    if (observed.error.has_value() != other.error.has_value()) {
        const auto& rejected = observed.error ? observed.error : other.error;
        require(rejected->code() == errc::resource_exhausted);
        return;
    }
    require(
      observed.consumed == other.consumed && observed.facts == other.facts
      && observed.digest == other.digest && observed.blocks == other.blocks
      && observed.checksum == other.checksum);
    if ((restrictions & 23U) != 0)
        require(observed.error.has_value());
    else if (!raw) {
        if (mutation == 0 || mutation == 7) {
            require(
              !observed.error
              && observed.consumed.value() == fixture.wire.size());
        } else if (mutation != 3) {
            require(observed.error.has_value());
            if (mutation == 4 || mutation == 5)
                require(observed.error->code() == errc::wrong_context);
        }
    }
}
} // namespace kwaque::storage::testing
