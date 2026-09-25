#include "src/storage/tests/memory_qualification_probe.h"
#include "src/storage/tests/retry_test_support.h"

#include <array>
#include <utility>

namespace kwaque::storage::qualification {
namespace {
using namespace testing;
}

void extent_operation(std::string_view name) {
    seastar::abort_source abort;
    codec::cooperative_work work{codec::limits::defaults(), abort};
    if (name == "extent-empty") {
        auto verifier = extent_verifier::make(
                          history(),
                          scope(100, 100, 0, 0, 512, 512),
                          work.policy(),
                          extent_layout_kind::initial_append,
                          {},
                          extent_integrity::crc32c_and_sha256)
                          .value();
        const auto proof = measure(
          name, 0, {}, [&] { return verifier.finish(work); });
        require(
          proof && proof->digest() && proof->digest()->bytes() == exact_sha("")
            && proof->boundary().block_count == 0 && verifier.closed(),
          "empty extent changed evidence");
        return;
    }
    const bool failure = name == "extent-failure";
    const bool close = name == "extent-close";
    require(
      name == "extent-reuse" || failure || close,
      "unknown extent memory scenario");
    // Two earlier durable boundaries contribute stored bytes, but not data
    // block counts. All fixtures and expected hashes precede observation.
    std::array<std::string, 5> wires;
    wires[0] = data_block(100, 0, 512);
    wires[1] = footer_wire(
      {scope(100, 101, 0, 1, 512, 1024),
       1,
       scope(100, 101, 0, 1, 512, 1024),
       crc(wires[0])},
      {history(), runtime::file_position{1024}});
    wires[2] = data_block(101, 1, 1536);
    const auto prefix = wires[0] + wires[1] + wires[2];
    wires[3] = footer_wire(
      {scope(100, 102, 0, 2, 512, 2048),
       2,
       scope(101, 102, 1, 2, 1536, 2048),
       crc(prefix)},
      {history(), runtime::file_position{2048}});
    wires[4] = data_block(102, 2, 2560);
    const auto all = prefix + wires[3] + wires[4];
    const auto expected = exact_sha(all);
    const auto expected_crc = crc(all);
    if (failure) wires[2].back() ^= 1;
    std::array<fragmented_buffer, 5> inputs;
    byte_count held = held_string(prefix).checked_add(held_string(all)).value();
    for (std::size_t i = 0; i < wires.size(); ++i) {
        inputs[i] = buffer(wires[i], 7);
        held = held.checked_add(held_string(wires[i]))
                 .value()
                 .checked_add(retained_cost(inputs[i]))
                 .value();
    }
    auto verifier = extent_verifier::make(
                      history(),
                      scope(100, 103, 0, 3, 512, 3072),
                      work.policy(),
                      extent_layout_kind::initial_append,
                      {},
                      extent_integrity::crc32c_and_sha256)
                      .value();
    const auto outcome = measure(name, all.size(), held, [&] {
        // One continuous interval includes initial SHA allocation, its retained
        // lifetime across calls, finalization and failure/explicit-close
        // cleanup. Resetting observation between add() calls would lose that
        // owner.
        for (std::size_t i = 0; i < inputs.size(); ++i) {
            const auto result
              = i % 2 == 0
                  ? verifier
                      .add_block(
                        std::move(inputs[i]),
                        batch_expected(),
                        memory(held),
                        work)
                      .get()
                  : verifier
                      .add_footer(std::move(inputs[i]), memory(held), work)
                      .get();
            if (failure && i == 2) {
                require(
                  !result && result.error().code() == errc::corrupt_data
                    && verifier.closed(),
                  "failed extent did not close");
                const auto closed = verifier.finish(work);
                require(
                  !closed && closed.error().code() == errc::closed,
                  "failed extent published evidence");
                verifier.close();
                return true;
            }
            require(
              result.has_value(), "extent rejected valid supplied object");
            if (close && i == 0) {
                verifier.close();
                const auto closed = verifier.finish(work);
                require(
                  !closed && closed.error().code() == errc::closed,
                  "explicit close retained evidence");
                return verifier.closed();
            }
        }
        const auto proof = verifier.finish(work);
        require(
          proof && proof->digest() && proof->digest()->bytes() == expected
            && proof->boundary().data_crc32c == expected_crc
            && proof->boundary().block_count == 3
            && proof->boundary().last_block == scope(102, 103, 2, 3, 2560, 3072)
            && verifier.closed(),
          "extent changed exact history evidence");
        return true;
    });
    require(outcome, "extent qualification did not complete");
}
} // namespace kwaque::storage::qualification
