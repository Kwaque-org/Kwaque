"""Lexical determinism tripwires; goldens and noise tests remain authoritative.

This does not resolve aliases, expand macros, or prove data flow. Reviewed
exceptions match exact code spans and counts, never whole files. New byte-copy,
clock, and hash-container seams require review rather than silent exemption.
Run the determinism_goldens suite plus scheduler_test, fake_file_test, and
structured_event_sink_test for allocator, hash-capacity, and task-noise coverage.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path

try:
    from tools.check_cross_shard_usage import SOURCE_SUFFIXES, code_view, line_number, splice_lines
except ModuleNotFoundError:
    from check_cross_shard_usage import SOURCE_SUFFIXES, code_view, line_number, splice_lines


@dataclass(frozen=True)
class Rule:
    name: str
    pattern: re.Pattern[str]
    remediation: str


RULES = (
    Rule(
        "unordered-state",
        re.compile(
            r"\b(?:unordered_(?:multi)?(?:map|set)|chunked_hash_(?:map|set)|"
            r"(?:flat|node)_hash_(?:map|set))\s*<|\bunordered_dense\s*::"
        ),
        "use ordered state or review an exact lookup-only declaration and its iteration",
    ),
    Rule(
        "host-clock",
        re.compile(
            r"\b(?:steady_clock|system_clock|high_resolution_clock|lowres_clock|"
            r"lowres_system_clock|highres_clock|manual_clock)\b|"
            r"(?<![\w.:>])(?:(?:std\s*)?::\s*)?"
            r"(?:clock_gettime|gettimeofday|clock|times)\s*\(|"
            r"\btime\s*\(\s*(?:nullptr|NULL|0|&)"
        ),
        "use environment time; host clocks belong only in reviewed external watchdogs or benchmarks",
    ),
    Rule(
        "random-source",
        re.compile(
            r"\bstd\s*::\s*(?:\w+_distribution|random_device|seed_seq|"
            r"(?:mt19937|ranlux|knuth_b|minstd_rand)\w*|default_random_engine|"
            r"(?:linear_congruential|mersenne_twister|subtract_with_carry|"
            r"discard_block|independent_bits|shuffle_order)_engine)\b|"
            r"(?<![\w.:>])(?:(?:std\s*)?::\s*)?"
            r"(?:rand|srand|srandom|rand_r|drand48|lrand48|getrandom|getentropy)\s*\(|"
            r"(?:(?<!\w)(?:std\s*)?::\s*|[=,({]\s*|\breturn\s+)random\s*\(|"
            r"\(\s*(?:(?:std\s*)?::\s*)?(?:rand|srand|random|srandom)\s*\)\s*\(|"
            r"\b(?:static|thread_local)\s+[^;={}()]{0,80}\b"
            r"(?:deterministic_random|sequential_random_source|keyed_random_source|random_engine)"
            r"\s+\w+\s*(?:[={;])"
        ),
        "use explicit fixture-owned integer random streams with stable coordinates",
    ),
    Rule(
        "pointer-identity",
        re.compile(
            r"\b(?:uintptr_t|intptr_t)\b|"
            r"\b(?:reinterpret_cast|bit_cast)\s*<\s*(?:(?:std\s*::\s*)?(?:u?int\d+_t|size_t|ptrdiff_t)|"
            r"(?:(?:unsigned|signed)\s+)?(?:long\s+long|long|int))\s*>|"
            r"\bhash\s*<[^;{}>]*\*\s*>|"
            r"(?<!\w)\(\s*(?:(?:std\s*::\s*)?u?int\d+_t|unsigned\s+long)\s*\)\s*&(?!&)"
        ),
        "assign stable numeric IDs; address arithmetic needs an exact non-identity exception",
    ),
    Rule(
        "native-byte-layout",
        re.compile(
            r"\b(?:memcpy|memmove|as_bytes|as_writable_bytes)\s*\(|"
            r"\breinterpret_cast\s*<\s*(?:const\s+)?"
            r"(?:(?:std\s*::\s*)?(?:byte|u?int8_t)|(?:unsigned\s+)?char)\s*\*\s*>|"
            r"\bbit_cast\s*<\s*(?:std\s*::\s*)?(?:array|span)\s*<|"
            r"\b(?:update|write|append)\s*\(\s*(?:std\s*::\s*addressof\s*\(|&)|"
            r"\b(?:EVP_DigestUpdate|SHA\d*_Update)\s*\([^,;]*,\s*(?:std\s*::\s*addressof\s*\(|&)|"
            r"\bfwrite\s*\(\s*(?:std\s*::\s*addressof\s*\(|&)|"
            r"\bendian\s*::\s*native\b|\b(?:htons|htonl|ntohs|ntohl|"
            r"htobe\d+|htole\d+|be\d+toh|le\d+toh)\s*\("
        ),
        "encode integers explicitly in fixed byte order; review exact copies of already-byte data",
    ),
)

RANDOM_TYPES = r"(?:deterministic_random|sequential_random_source|keyed_random_source|random_engine)"
RANDOM_DECLARATION = re.compile(
    rf"\b{RANDOM_TYPES}\s+\w+\s*(?:[={{;(])|"
    rf"\b(?:(?:static|thread_local)\s+)?(?:const\s+)?auto\s+\w+\s*=\s*"
    rf"[^;{{}}]{{0,250}}?(?:\b{RANDOM_TYPES}\b|(?:\.|->)\s*(?:stream|cursor|random)\s*\()"
)
PRIMITIVE_PARAMETER = re.compile(
    r"\s*(?:const\s+)?(?:(?:std\s*::\s*)?(?:u?int\d+_t|size_t|ptrdiff_t)|"
    r"(?:(?:unsigned|signed)\s+)?(?:long\s+long|long|short|int|char)|"
    r"bool|float|double|void)\b\s*[*&]*\s*(?:[A-Za-z_]\w*)?\s*"
    r"(?:=[^,]+)?"
)


def declaration_context(code: str, offset: int) -> tuple[bool, bool]:
    """Conservative brace context; does not resolve C++ types or macros."""
    contexts: list[str] = []
    boundary = 0
    for token in re.finditer(r"[{};]", code[:offset]):
        if token.group() == "{":
            prefix = code[boundary:token.start()]
            if re.search(r"\bnamespace\b[^;{}]*$", prefix):
                contexts.append("namespace")
            elif re.search(r"\b(?:class|struct|union)\s+\w+[^;{}]*$", prefix):
                contexts.append("class")
            else:
                contexts.append("body")
        elif token.group() == "}" and contexts:
            contexts.pop()
        boundary = token.end()
    return "body" in contexts, "class" in contexts


def global_random_matches(code: str):
    for match in RANDOM_DECLARATION.finditer(code):
        if re.search(r"\b(?:class|struct|enum)\s*$", code[:match.start()]):
            continue
        if re.fullmatch(rf"{RANDOM_TYPES}\s+\w+\s*\(", match.group()):
            depth = 1
            end = match.end()
            while end < len(code) and depth:
                depth += (code[end] == "(") - (code[end] == ")")
                end += 1
            parameters = code[match.end():end - 1].strip()
            # Empty parentheses declare a function, never a default-initialized
            # object. Recognize primitive-type parameter lists too, without
            # guessing whether arbitrary identifiers name types or values.
            if not parameters or all(
                PRIMITIVE_PARAMETER.fullmatch(parameter)
                for parameter in parameters.split(",")
            ):
                continue
            if re.match(r"\s*(?:noexcept\b|const\b|->|\{)", code[end:]):
                continue
        in_body, in_class = declaration_context(code, match.start())
        prefix = code[max(0, code.rfind(";", 0, match.start()) + 1):match.start()]
        persistent = re.search(r"\b(?:static|thread_local)\b[^;{}]*$", prefix)
        persistent = persistent or re.match(r"(?:static|thread_local)\b", match.group())
        if persistent or not in_body and not in_class:
            yield match


def hash_names(codes: list[str]) -> set[str]:
    aliases: set[str] = set()
    # Preserve reviewed cross-file member/parameter names even when a caller
    # scans a minimal fixture; infer newly declared container names as well.
    names = {"objects_", "indices_", "open_objects_", "visible_pages", "durable_pages", "pages"}
    for code in codes:
        for match in RULES[0].pattern.finditer(code):
            opening = code.find("<", match.start())
            if opening < 0:
                continue
            depth = 1
            end = opening + 1
            while end < len(code) and depth:
                depth += (code[end] == "<") - (code[end] == ">")
                end += 1
            if depth:
                continue
            alias = re.search(r"\busing\s+(\w+)\s*=\s*(?:\w+\s*::\s*)*$", code[:match.start()])
            if alias:
                aliases.add(alias.group(1))
            variable = re.match(r"\s*[*&]*\s*(\w+)\s*(?:[;={,)])", code[end:])
            if variable:
                names.add(variable.group(1))
    if aliases:
        pattern = re.compile(r"\b(?:" + "|".join(map(re.escape, sorted(aliases))) + r")\b\s*[*&]*\s*(\w+)\s*(?:[;={,)])")
        for code in codes:
            names.update(match.group(1) for match in pattern.finditer(code))
    return names


def hash_iteration_rule(names: set[str]) -> Rule:
    name = r"\b(?:" + "|".join(map(re.escape, sorted(names))) + r")\b" if names else r"(?!)"
    return Rule(
        "unordered-iteration",
        re.compile(
            rf"{name}\s*(?:\.|->)\s*(?:begin|cbegin|rbegin|crbegin)\s*\(|"
            rf"\bfor\s*\([^;)]*:\s*[^;)]*{name}[^;)]*\)|"
            rf"\b(?:begin|cbegin|rbegin|for_each|transform|copy)\s*\(\s*(?:\w+\s*\.\s*)?{name}\b"
        ),
        "sort stable keys before observable traversal; review exact commutative cleanup or indexed export",
    )


@dataclass(frozen=True)
class Allowance:
    path: str
    rule: str
    code: str
    count: int
    reason: str
    matches_per_span: int = 1


@dataclass(frozen=True)
class Writer:
    path: str
    name: str
    code: str


# These declarations index state by stable IDs. Export sorts keys; page cleanup
# and handle invalidation are commutative. No hash traversal assigns event IDs.
ALLOWANCES = (
    Allowance("src/simulation/fake_file.h", "unordered-state",
              "using page_map = seastar::chunked_hash_map<std::uint64_t, page_state>;", 1,
              "Page lookup; dirty-page lists drive writes and exports sort page indices."),
    Allowance("src/simulation/fake_file.h", "unordered-state",
              "using inode_map = seastar::chunked_hash_map<std::uint64_t, std::unique_ptr<inode>>;", 1,
              "Stable-ID lookup; snapshots sort IDs and collection has an explicit worklist."),
    Allowance("src/simulation/fake_file.h", "unordered-state",
              "seastar::chunked_hash_map<std::uint64_t, std::size_t> indices_;", 1,
              "Pending slot lookup; copy_keys scans owned slots, then callers sort IDs."),
    Allowance("src/simulation/fake_file.h", "unordered-state",
              "seastar::chunked_hash_set<std::uint64_t> open_objects_;", 1,
              "Membership and commutative invalidation; diagnostic IDs are sorted."),
    Allowance("src/simulation/scheduler_driver.h", "host-clock",
              "const auto deadline = seastar::lowres_clock::now() + watchdog;", 2,
              "External lifecycle and registered-operation watchdog deadlines."),
    Allowance("src/simulation/scheduler_driver.h", "host-clock",
              "if (seastar::lowres_clock::now() >= deadline) { throw scheduler_watchdog_error{}; }", 2,
              "External timeout failure, never simulated time or artifact data."),
    Allowance("src/runtime/testing/contracts/real_backend_conformance.h", "host-clock",
              "return seastar::with_timeout(seastar::lowres_clock::now() + real_backend_operation_timeout, std::move(operation));", 1,
              "External real-adapter watchdog only."),
    Allowance("src/runtime/testing/contracts/network_contract.h", "host-clock",
              "void arm(seastar::lowres_clock::time_point deadline) { timer_.rearm(deadline); }", 1,
              "External network-test watchdog deadline; expiration cancels and joins the owning scenario."),
    Allowance("src/runtime/testing/contracts/network_contract.h", "host-clock",
              "seastar::timer<seastar::lowres_clock> timer_;", 1,
              "One external watchdog timer; it never orders simulated network effects."),
    Allowance("src/simulation/tests/fake_network_test.cc", "host-clock",
              "watchdog.arm(seastar::lowres_clock::time_point{});", 1,
              "Expired external deadline after the parked-read handshake validates cancellation and drain."),
    Allowance("src/simulation/tests/simulation_bench.cc", "host-clock", """
              template<typename Function>
              auto measure(Function&& function) {
                  const auto started = std::chrono::steady_clock::now();
                  auto result = std::forward<Function>(function)();
                  const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - started);
                  worst_nanoseconds = std::max(
                    worst_nanoseconds, static_cast<std::uint64_t>(elapsed.count()));
                  ++calls;
                  return result;
              }
              """, 1,
              "Benchmark-only dispatch duration observation; the callback result is returned unchanged.",
              matches_per_span=2),
    Allowance("src/simulation/tests/simulation_bench.cc", "host-clock", """
              template<typename T>
              seastar::future<T> wait_asynchronously(seastar::future<T> pending) {
                  if (!measure_dispatch_) {
                      co_await testing::pump_until(*events_, pending);
                      co_return co_await std::move(pending);
                  }
                  constexpr auto watchdog = std::chrono::seconds{10};
                  const auto deadline = seastar::lowres_clock::now() + watchdog;
                  while (!pending.available()) {
                      co_await seastar::yield();
                      if (pending.available()) { break; }
                      if (seastar::lowres_clock::now() >= deadline) {
                          throw testing::scheduler_watchdog_error{};
                      }
                      if (events_->pending_events() == 0U) { continue; }
                      pump_scheduler();
                  }
                  co_return co_await std::move(pending);
              }
              """, 1,
              "Instrumented benchmark wait retains the shared driver's external watchdog; host time never orders events.",
              matches_per_span=2),
    Allowance("src/simulation/fake_file.cc", "pointer-identity",
              "std::bit_cast<std::uintptr_t>(buffer) % _memory_dma_alignment == 0", 1,
              "Native DMA alignment validation; address never becomes an identity."),
)


# The copy patterns below are byte-buffer transfers, not object serialization.
# Every occurrence is counted in its exact file; another copy must be reviewed.
BYTE_COPIES = {
    "src/simulation/event_trace.cc": (
        "std::memcpy(destination.data() + copied, chunk.data() + chunk_offset_, count);",
        "std::memcpy(destination.data() + copied, chunk.data() + chunk_offset, count);",
    ),
    "src/observability/event_log.cc": (
        "std::memcpy(destination.data() + copied, chunk.data() + chunk_offset_, count);",
        "std::memcpy(destination.data() + copied, chunk.data() + chunk_offset, count);",
    ),
    "src/simulation/fake_file.cc": (
        "std::memcpy(replacement->data() + begin, bytes.data() + copied, count);",
        "std::memcpy(destination.data() + copied, visible->second.bytes->data() + page_offset, span);",
        "std::memcpy(destination.data() + copied, durable->second.bytes->data() + page_offset, span);",
        "std::as_writable_bytes(std::span{io->destination, static_cast<std::size_t>(length)})",
        "std::as_bytes(std::span{source, static_cast<std::size_t>(length)})",
        "reinterpret_cast<std::uint8_t*>(io->snapshot.get_write())",
    ),
    "src/simulation/fake_network.cc": (
        "std::memcpy(mutated.get_write(), fragment.data(), fragment.size());",
    ),
    "src/simulation/fake_file_test_support.h": (
        "std::memcpy(destination.data() + offset, state.bytes->data(), count);",
    ),
    "src/simulation/tests/fake_file_test.cc": (
        "std::as_bytes(std::span{value.data(), value.size()})",
        "std::as_writable_bytes(std::span{output.data(), output.size()})",
        "std::memcpy(visible.data() + position, value.data(), value.size());",
        "reinterpret_cast<const char*>(oracle.visible.data())",
    ),
    "src/simulation/tests/fuzz_cases.cc": (
        "reinterpret_cast<const std::uint8_t*>(object.visible_bytes.data())",
        "reinterpret_cast<const std::uint8_t*>(object.durable_bytes.data())",
        "reinterpret_cast<const std::uint8_t*>(fixture.expected[index].data())",
    ),
    "src/simulation/tests/fuzz_reproduction.cc": (
        "reinterpret_cast<const char*>(bytes.data())",
    ),
    "src/observability/event_codec.cc": (
        "reinterpret_cast<const char*>(input_.data() + offset_)",
    ),
    "src/observability/event_codec_test.cc": (
        "reinterpret_cast<const char*>(encoded->bytes().data() + 7U)",
    ),
    "src/runtime/testing/contracts/network_contract.h": (
        "std::memcpy(fragments[index].get_write(), contents[index].data(), contents[index].size());",
        # One already-shifted byte, independent of host integer layout.
        "std::memcpy(result.data() + byte, &encoded, sizeof(encoded));",
    ),
}
ALLOWANCES += tuple(
    Allowance(path, "native-byte-layout", snippet, 1,
              "Copies/views existing character or byte storage; integer encoding is separate.")
    for path, snippets in BYTE_COPIES.items() for snippet in snippets
) + (
    Allowance("src/simulation/tests/fuzz_cases.cc", "native-byte-layout",
              "reinterpret_cast<const std::uint8_t*>(name.data())", 2,
              "Visible/durable directory names are already character bytes."),
)

ALLOWANCES += (
    Allowance("src/simulation/fake_file.cc", "unordered-iteration",
              """for (auto current = file.visible_pages.begin(); current != file.visible_pages.end();) {
                  if (current->first >= prepared.kept_pages) { current = file.visible_pages.erase(current); }
                  else { ++current; }
              }""", 1,
              "Truncate erases pages above a stable numeric boundary; no ordered output."),
    Allowance("src/simulation/fake_file.cc", "unordered-iteration",
              """for (auto current = file.durable_pages.begin(); current != file.durable_pages.end();) {
                  const auto visible = file.visible_pages.find(current->first);
                  if (current->first >= *file.cleared_from_page
                      && (visible == file.visible_pages.end() || !visible->second.dirty)) {
                      current = file.durable_pages.erase(current);
                  } else { ++current; }
              }""", 1,
              "Flush deletes cleared pages; dirty-page IDs determine publishing order."),
    Allowance("src/simulation/fake_file.cc", "unordered-iteration",
              """for (const auto value : open_objects_) {
                  const fake_object_id id{value};
                  auto* object = find_inode(id);
                  if (object == nullptr) { continue; }
                  object->open_references = 0;
                  collect_unreachable(id);
              }""", 1,
              "Commutative handle invalidation with no per-object event publication."),
    Allowance("src/simulation/fake_file_test_support.h", "unordered-iteration",
              """for (const auto& [id, object] : filesystem.objects_) {
                  static_cast<void>(id);
                  if (object->kind != fake_file_kind::regular) { continue; }
                  const auto& file = std::get<fake_file_system::regular_file_state>(object->state);
                  const auto pages = file.visible_pages.size() + file.durable_pages.size();
                  if (pages > (limits.maximum_dense_bytes.value() - page_bytes) / fake_file_page_bytes) {
                      return runtime::failure(runtime::operation_error{errc::resource_exhausted, runtime::operation_kind::file});
                  }
                  page_bytes += pages * fake_file_page_bytes;
              }""", 1,
              "Commutative capacity summation, with no ordered output."),
    Allowance("src/simulation/fake_file_test_support.h", "unordered-iteration",
              """for (const auto& [id, object] : filesystem.objects_) {
                  static_cast<void>(object);
                  object_ids.push_back(id);
              }
              std::ranges::sort(object_ids);""", 1,
              "ID collection and mandatory sorting before digesting."),
    Allowance("src/simulation/fake_file_test_support.h", "unordered-iteration",
              """for (const auto& [id, object] : filesystem.objects_) {
                  fake_inode_snapshot copy{
                    .id = id, .kind = object->kind,
                    .open_references = object->open_references,
                    .pending_references = object->pending_references,
                    .visible_links = object->visible_links,
                    .durable_links = object->durable_links,
                    .occurrences = object->occurrences,
                  };
                  if (object->kind == fake_file_kind::regular) {
                      const auto& file = std::get<fake_file_system::regular_file_state>(object->state);
                      copy.visible_size = file.visible_size;
                      copy.durable_size = file.durable_size;
                      if (file.visible_size > limits.maximum_dense_bytes.value() - dense_bytes
                          || file.durable_size > limits.maximum_dense_bytes.value() - dense_bytes - file.visible_size) {
                          return runtime::failure(runtime::operation_error{errc::resource_exhausted, runtime::operation_kind::file});
                      }
                      dense_bytes += file.visible_size + file.durable_size;
                      copy.visible_bytes.resize(file.visible_size);
                      copy.durable_bytes.resize(file.durable_size);
                      const auto visible = filesystem.read(object->id, 0, std::span<std::byte>{copy.visible_bytes});
                      if (!visible) { return runtime::failure(visible.error()); }
                      copy_pages(file.durable_pages, copy.durable_bytes);
                  } else {
                      const auto& directory = std::get<fake_file_system::directory_state>(object->state);
                      for (const auto& [name, child] : directory.durable) {
                          copy.durable_entries.emplace_back(name, child.value());
                      }
                      append_visible_entries(directory, copy.visible_entries);
                  }
                  result.objects.push_back(std::move(copy));
              }
              std::ranges::sort(result.objects, {}, &fake_inode_snapshot::id);""", 1,
              "Complete indexed snapshots and mandatory ID sort before return."),
    Allowance("src/simulation/fake_file_test_support.h", "unordered-iteration",
              """for (const auto id : filesystem.open_objects_) { open_ids.push_back(id); }
              std::ranges::sort(open_ids);""", 1,
              "Collects IDs that are sorted before digesting."),
    Allowance("src/simulation/fake_file_test_support.h", "unordered-iteration",
              """for (const auto& [index, page] : pages) {
                  static_cast<void>(page); indices.push_back(index);
              }
              std::ranges::sort(indices);""", 1,
              "Collects numeric page indices that are sorted before digesting."),
    Allowance("src/simulation/fake_file_test_support.h", "unordered-iteration",
              """for (const auto& [index, state] : pages) {
                  const auto offset = index * fake_file_page_bytes;
                  if (offset >= destination.size()) { continue; }
                  const auto count = std::min<std::size_t>(fake_file_page_bytes, destination.size() - offset);
                  std::memcpy(destination.data() + offset, state.bytes->data(), count);
              }""", 1,
              "Copies pages into disjoint index-addressed destination ranges."),
)
for _path, _size, _version in (
    ("src/simulation/bandwidth.cc", "sizeof(version)", "0x01"),
    ("src/simulation/tests/network_oracle.cc", "1", "1"),
):
    ALLOWANCES += (
        Allowance(_path, "native-byte-layout",
                  f"const unsigned char version{{{_version}}}; update(&version, {_size});", 1,
                  "Single-byte format version; no integer object-layout dependency."),
    )
    for _tag in (0, 1, 2):
        _tag_size = "sizeof(tag)" if _size != "1" else "1"
        ALLOWANCES += (
            Allowance(_path, "native-byte-layout",
                      f"const unsigned char tag{{{_tag}}}; update(&tag, {_tag_size});", 1,
                      "Single-byte rate tag; multibyte fields use fixed-endian writers."),
        )

# Signed integer bit patterns are independent of pointer identities. Keep each
# conversion explicit so a new bit_cast from an address cannot slip through.
INTEGER_BIT_CASTS = {
    "src/observability/event.h": (
        "from_signed(std::int64_t value) noexcept { return event_field_value{event_field_type::signed_integer, std::bit_cast<std::uint64_t>(value)}; }",
        "std::bit_cast<std::int64_t>(numeric_)",
    ),
    "src/observability/event_codec.cc": (
        "std::bit_cast<std::uint64_t>(*value.as_signed())",
        "std::bit_cast<std::int64_t>(value)",
        "std::bit_cast<std::uint64_t>(value.wall().unix_nanoseconds())",
        "std::bit_cast<std::int64_t>(wall)",
    ),
    "src/observability/event_codec_test.cc": (
        "std::bit_cast<std::uint64_t>(source.wall().unix_nanoseconds())",
    ),
    "src/simulation/virtual_time.cc": (
        "std::bit_cast<std::uint64_t>(offset.nanoseconds())",
    ),
}
ALLOWANCES += tuple(
    Allowance(path, "pointer-identity", snippet, 1,
              "Preserves a signed integer bit pattern; the source is not an address.")
    for path, snippets in INTEGER_BIT_CASTS.items() for snippet in snippets
)


# Pin the arithmetic in named canonical writers. This checks presence and
# shape, not reachability; executable goldens establish the actual wire bytes.
WRITERS = (
    Writer("src/simulation/event_trace.cc", "fixed-width hexadecimal fields", """
        void append_hex(Output& output, Integer value, std::size_t width) {
            constexpr std::string_view digits{"0123456789abcdef"};
            for (std::size_t index = 0; index < width; ++index) {
                const auto shift = (width - index - 1U) * 4U;
                output.push_back(digits[(static_cast<std::uint64_t>(value) >> shift) & 0xfU]);
            }
        }
    """),
    Writer("src/observability/event_codec.cc", "little-endian event integers", """
        bool append_integer(Integer value) noexcept {
            using unsigned_type = std::make_unsigned_t<Integer>;
            auto encoded = static_cast<unsigned_type>(value);
            for (std::size_t index = 0; index < sizeof(Integer); ++index) {
                if (!append_byte(static_cast<std::uint8_t>(encoded & 0xffU))) { return false; }
                encoded >>= 8U;
            }
            return true;
        }
    """),
    Writer("src/observability/event_log.cc", "little-endian event-log integers", """
        append_integer(event_log_artifact& output, Integer value) {
            using unsigned_type = std::make_unsigned_t<Integer>;
            auto encoded = static_cast<unsigned_type>(value);
            for (std::size_t index = 0; index < sizeof(Integer); ++index) {
                if (auto appended = output.push_back(static_cast<std::uint8_t>(encoded & 0xffU)); !appended) {
                    return runtime::failure(appended.error());
                }
                encoded >>= 8U;
            }
            return {};
        }
    """),
    Writer("src/simulation/tests/fuzz_cases.cc", "little-endian terminal digest", """
        void integer(Integer value) {
            using unsigned_type = std::make_unsigned_t<Integer>;
            static_assert(sizeof(Integer) <= sizeof(std::uint64_t));
            auto encoded = static_cast<std::uint64_t>(static_cast<unsigned_type>(value));
            std::array<std::uint8_t, sizeof(Integer)> bytes{};
            for (auto& byte : bytes) { byte = static_cast<std::uint8_t>(encoded & 0xffU); encoded >>= 8U; }
            hasher_.update(bytes.data(), bytes.size());
        }
    """),
    Writer("src/simulation/tests/fuzz_reproduction.cc", "big-endian envelope digest", """
        const auto update_integer = [&hasher]<typename Integer>(Integer value) {
            using unsigned_type = std::make_unsigned_t<Integer>;
            static_assert(sizeof(Integer) <= sizeof(std::uint64_t));
            auto encoded = static_cast<std::uint64_t>(static_cast<unsigned_type>(value));
            std::array<std::uint8_t, sizeof(Integer)> bytes{};
            for (auto position = bytes.rbegin(); position != bytes.rend(); ++position) {
                *position = static_cast<std::uint8_t>(encoded & 0xffU); encoded >>= 8U;
            }
            hasher.update(bytes.data(), bytes.size());
        };
    """),
    Writer("src/simulation/bandwidth.cc", "big-endian multiprecision digest",
           "boost::multiprecision::export_bits(value, bytes.begin(), 8U, true);"),
)


def is_deterministic_source(path: Path) -> bool:
    if path.suffix not in SOURCE_SUFFIXES:
        return False
    name = path.as_posix()
    return (
        name.startswith("src/simulation/")
        or name.startswith("src/runtime/testing/contracts/")
        or name.startswith("src/observability/")
        and path.name.startswith(("event", "capture_event"))
        or any("determin" in part for part in path.parts)
        or name.startswith("src/runtime/tests/")
        and path.name in {"time_random_contract_test.cc", "timer_contract_test.cc"}
    )


def masked_code(source: str) -> tuple[str, tuple[int, ...]]:
    # Preserve numeric separators as code. The shared lexer otherwise sees the
    # apostrophe as a character-literal opener and can hide following tokens.
    logical, offsets = splice_lines(source)
    separated = re.sub(
        r"(?<![\w'])\d(?:[eEpP][+-]|[0-9A-Za-z_.]|'[0-9A-Za-z_])*",
        lambda match: match.group().replace("'", " "), logical,
    )
    code, _ = code_view(separated)
    code = re.sub(r"^[ \t]*#[ \t]*include[^\r\n]*",
                  lambda match: " " * len(match.group()), code, flags=re.MULTILINE)
    return code, offsets


def compact(code: str) -> tuple[str, tuple[int, ...]]:
    positions = tuple(index for index, character in enumerate(code) if not character.isspace())
    return "".join(code[index] for index in positions), positions


def occurrences(code: str, snippet: str) -> list[tuple[int, int]]:
    condensed, positions = compact(code)
    needle, _ = compact(masked_code(snippet)[0])
    if not needle:
        raise ValueError("reviewed code span must contain code tokens")
    spans = []
    start = 0
    while (found := condensed.find(needle, start)) >= 0:
        spans.append((positions[found], positions[found + len(needle) - 1] + 1))
        start = found + len(needle)
    return spans


def scan(root: Path, allowances: tuple[Allowance, ...] = ALLOWANCES,
         writers: tuple[Writer, ...] = WRITERS) -> list[str]:
    violations: list[str] = []
    sources = {
        path.relative_to(root).as_posix(): path.read_text(encoding="utf-8")
        for path in sorted((root / "src").rglob("*"))
        if path.is_file() and is_deterministic_source(path.relative_to(root))
    }
    prepared = {name: masked_code(source) for name, source in sources.items()}
    rules = RULES + (hash_iteration_rule(hash_names([code for code, _ in prepared.values()])),)
    approved: dict[tuple[str, str], list[tuple[int, int]]] = {}
    rule_names = {rule.name for rule in rules}
    for allowance in allowances:
        code, offsets = prepared.get(allowance.path, ("", ()))
        spans = occurrences(code, allowance.code)
        if (allowance.rule not in rule_names or not allowance.reason
                or allowance.count <= 0 or allowance.matches_per_span <= 0):
            violations.append(f"{allowance.path}:1: invalid-allowance: specify a known rule, reason, and positive span/match counts")
            continue
        rule = next(rule for rule in rules if rule.name == allowance.rule)
        relevant = [span for span in spans
                    if sum(1 for _ in rule.pattern.finditer(code, *span)) == allowance.matches_per_span]
        if len(spans) != allowance.count or len(relevant) != len(spans):
            line = line_number(sources[allowance.path], offsets[spans[0][0]]) if spans else 1
            violations.append(
                f"{allowance.path}:{line}: stale-allowance: expected {allowance.count} exact "
                f"{allowance.rule} span(s) with {allowance.matches_per_span} match(es) each, "
                f"found {len(relevant)}; review or remove the allowance"
            )
            continue
        approved.setdefault((allowance.path, allowance.rule), []).extend(spans)
    for name, (code, offsets) in prepared.items():
        for rule in rules:
            spans = approved.get((name, rule.name), ())
            for match in rule.pattern.finditer(code):
                if any(start <= match.start() and match.end() <= end for start, end in spans):
                    continue
                line = line_number(sources[name], offsets[match.start()])
                violations.append(f"{name}:{line}: {rule.name}: {rule.remediation}")
        for match in global_random_matches(code):
            line = line_number(sources[name], offsets[match.start()])
            violations.append(
                f"{name}:{line}: global-random: keep random sources in explicit per-fixture owners"
            )
    for writer in writers:
        code = prepared.get(writer.path, ("", ()))[0]
        if len(occurrences(code, writer.code)) != 1:
            violations.append(
                f"{writer.path}:1: fixed-endian-writer: restore or review the named "
                f"{writer.name} writer and run the canonical goldens"
            )
    return sorted(violations)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path)
    arguments = parser.parse_args()
    root = (arguments.workspace or Path(os.environ.get("BUILD_WORKSPACE_DIRECTORY", Path.cwd()))).resolve()
    if not (root / "src").is_dir():
        parser.error("workspace must contain a src directory")
    violations = scan(root)
    if violations:
        print("\n".join(violations), file=sys.stderr)
        return 1
    print("Determinism source tripwires passed; executable goldens and noise tests are still required")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
