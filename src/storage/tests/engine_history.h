#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace kwaque::storage::testing {

// One history of a shard's storage, as bytes: what a test writes by hand,
// what a corpus keeps and what a fuzzer finds are the same thing. Eight
// bytes of header say what the store is and how a crash treats what was not
// synced; every four bytes after them are one operation. Nothing says how
// many operations follow, so taking bytes away takes operations away and
// leaves the meaning of the rest as it was. Every byte string is a history:
// bytes that are missing are zero, an operation's code is taken modulo the
// number of codes, and what is past the cap or short of a whole operation
// is not read.
inline constexpr std::size_t history_header_bytes = 8;
inline constexpr std::size_t history_operation_bytes = 4;
inline constexpr std::size_t maximum_history_operations = 256;

// What a crash keeps of what was not synced.
enum class history_survival : std::uint8_t {
    // Nothing.
    none,
    // Everything.
    all,
    // About half of the data, a granule at a time, the size apart from the
    // content; no name.
    data,
    // The names the header's mask says, of those pending when the crash
    // comes, the oldest at its lowest bit; no data.
    names,
};
// Its first byte is the survival; the seven after it are the mask of names,
// the lowest byte first.
struct history_header final {
    history_survival survival{history_survival::none};
    std::uint64_t names{0};
    bool operator==(const history_header&) const = default;
};

// The simplest operation has the lowest code. An operation names a segment
// by its place among those it applies to, counted in the order they were
// created, so that taking an operation away does not rename the segments of
// those after it.
enum class history_op : std::uint8_t {
    // Offers `b` % 4 + 1 requests to the `a`-th segment that takes
    // appends, without waiting for them. With the high bit of `c`, to the
    // `a`-th segment that takes none any more: each is to be refused.
    append,
    // Waits for the result of every request offered.
    settle,
    // Reads the `a`-th segment that is under its own name back.
    read,
    // Creates the next segment.
    create,
    // Seals the `a`-th segment that takes appends.
    seal,
    // Takes a checkpoint.
    checkpoint,
    // Cuts the completed-retry snapshot of the `a`-th segment that takes
    // appends.
    cut,
    // Removes the `a`-th segment that is not gone. One that still takes
    // appends is to be refused.
    remove,
    // Lets `a` % 16 + 1 scheduler events run with nothing waited for.
    advance,
    // Stops the shard and starts it again.
    restart,
    // Crashes the device, stops the shard and starts it again.
    crash,
};
inline constexpr std::uint8_t history_op_count = 11;

struct history_operation final {
    history_op op{history_op::append};
    std::uint8_t a{0}, b{0}, c{0};
    bool operator==(const history_operation&) const = default;
};
struct engine_history final {
    history_header header;
    std::vector<history_operation> operations;
    bool operator==(const engine_history&) const = default;
};

[[nodiscard]] inline engine_history
decode_history(std::span<const std::uint8_t> bytes) {
    engine_history decoded;
    const auto at = [&bytes](std::size_t index) {
        return index < bytes.size() ? bytes[index] : std::uint8_t{0};
    };
    decoded.header.survival = static_cast<history_survival>(at(0) % 4U);
    for (std::size_t i = 1; i != history_header_bytes; ++i)
        decoded.header.names |= std::uint64_t{at(i)} << ((i - 1) * 8U);
    const auto whole = bytes.size() > history_header_bytes
                         ? (bytes.size() - history_header_bytes)
                             / history_operation_bytes
                         : 0;
    const auto count = whole < maximum_history_operations
                         ? whole
                         : maximum_history_operations;
    decoded.operations.reserve(count);
    for (std::size_t i = 0; i != count; ++i) {
        const auto from = history_header_bytes + i * history_operation_bytes;
        decoded.operations.push_back(
          {static_cast<history_op>(at(from) % history_op_count),
           at(from + 1),
           at(from + 2),
           at(from + 3)});
    }
    return decoded;
}

[[nodiscard]] inline std::vector<std::uint8_t>
encode_history(const engine_history& history) {
    std::vector<std::uint8_t> bytes(
      history_header_bytes
        + history.operations.size() * history_operation_bytes,
      std::uint8_t{0});
    bytes[0] = static_cast<std::uint8_t>(history.header.survival);
    for (std::size_t i = 1; i != history_header_bytes; ++i)
        bytes[i] = static_cast<std::uint8_t>(
          history.header.names >> ((i - 1) * 8U));
    auto out = history_header_bytes;
    for (const auto& operation : history.operations) {
        bytes[out++] = static_cast<std::uint8_t>(operation.op);
        bytes[out++] = operation.a;
        bytes[out++] = operation.b;
        bytes[out++] = operation.c;
    }
    return bytes;
}

// Two segments, then a group of requests over both.
[[nodiscard]] inline engine_history appended_history() {
    return {
      {},
      {{history_op::create},
       {history_op::create},
       {history_op::append, 0, 1},
       {history_op::append, 1, 1},
       {history_op::append, 0, 0},
       {history_op::settle}}};
}
// A segment with requests, then another that follows it while it seals,
// and a checkpoint.
[[nodiscard]] inline engine_history rolled_history() {
    return {
      {},
      {{history_op::create},
       {history_op::append, 0, 1},
       {history_op::settle},
       {history_op::create},
       {history_op::append, 1, 0},
       {history_op::seal, 0},
       {history_op::settle},
       {history_op::checkpoint}}};
}
// A segment with requests, then its seal, a checkpoint and its removal.
[[nodiscard]] inline engine_history removed_history() {
    return {
      {},
      {{history_op::create},
       {history_op::append, 0, 1},
       {history_op::settle},
       {history_op::seal, 0},
       {history_op::checkpoint},
       {history_op::remove, 0}}};
}

// One life with everything a history can ask of a store, a clean restart,
// and a second life on what the first left. Nothing in it waits on a race:
// every request is settled before what follows depends on it.
[[nodiscard]] inline engine_history whole_life_history() {
    return {
      {},
      {{history_op::create},
       {history_op::create},
       {history_op::append, 0, 1},
       {history_op::append, 1, 1},
       {history_op::settle},
       {history_op::read, 0},
       {history_op::read, 1},
       {history_op::seal, 0},
       {history_op::append, 0, 0, 0x80},
       {history_op::append, 0, 0},
       {history_op::settle},
       {history_op::read, 0},
       {history_op::cut, 0},
       {history_op::checkpoint},
       {history_op::advance, 3},
       {history_op::remove, 1},
       {history_op::restart},
       {history_op::remove, 1},
       {history_op::read, 0}}};
}

// As many segments as take appends at one time, each with requests, and then
// a cycle: the oldest is sealed, one is created in its place and takes
// requests, a checkpoint is taken and the sealed one is removed. It ends in
// a restart, after which everything is read back.
[[nodiscard]] inline engine_history
churned_history(std::uint8_t writable, unsigned cycles) {
    engine_history history;
    for (std::uint8_t i = 0; i != writable; ++i)
        history.operations.push_back({history_op::create});
    for (std::uint8_t i = 0; i != writable; ++i)
        history.operations.push_back({history_op::append, i, 3});
    history.operations.push_back({history_op::settle});
    const auto newest = static_cast<std::uint8_t>(writable - 1U);
    for (unsigned cycle = 0; cycle != cycles; ++cycle)
        history.operations.insert(
          history.operations.end(),
          {{history_op::seal, 0},
           {history_op::create},
           {history_op::append, newest, 3},
           {history_op::settle},
           {history_op::checkpoint},
           {history_op::remove, 0}});
    history.operations.push_back({history_op::restart});
    return history;
}

} // namespace kwaque::storage::testing
