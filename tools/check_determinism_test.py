from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

try:
    from tools.check_determinism import (
        ALLOWANCES,
        WRITERS,
        Allowance,
        Writer,
        is_deterministic_source,
        is_pure_source,
        is_test_source,
        masked_code,
        occurrences,
        scan,
    )
except ModuleNotFoundError:
    from check_determinism import (
        ALLOWANCES,
        WRITERS,
        Allowance,
        Writer,
        is_deterministic_source,
        is_pure_source,
        is_test_source,
        masked_code,
        occurrences,
        scan,
    )


class DeterminismSourceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.path = "src/simulation/sample.cc"

    def write(self, text: str, path: str | None = None) -> None:
        target = self.root / (path or self.path)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8", newline="") as output:
            output.write(text)

    def scan(self, allowances=(), writers=()) -> list[str]:
        return scan(self.root, allowances, writers)

    def test_all_rule_families_reject_synthetic_violations(self) -> None:
        cases = {
            "unordered-state": (
                "std::unordered_map<int, int> ready;",
                "using events = absl::flat_hash_map<int, int>;",
                "seastar::chunked_hash_set<unsigned> ordering;",
                "ankerl::unordered_dense::map<int, int> events;",
            ),
            "host-clock": (
                "auto now = std::chrono::steady_clock::now();",
                "using clock = seastar::lowres_clock;",
                "auto now = time(nullptr);",
                "clock_gettime(CLOCK_MONOTONIC, &result);",
                "seastar::manual_clock::advance(duration);",
            ),
            "random-source": (
                "std::uniform_int_distribution<int> choice;",
                "std::normal_distribution<double> choice;",
                "std::mt19937_64 engine{42};",
                "auto seed = std::random_device{}();",
                "auto value = rand();",
                "auto value = random();",
                "auto value = (::rand)();",
                "getrandom(bytes, size, 0);",
                "static deterministic_random engine{42};",
                "thread_local sequential_random_source stream{};",
            ),
            "pointer-identity": (
                "auto id = reinterpret_cast<std::uint64_t>(pointer);",
                "auto id = std::bit_cast<std::uint64_t>(pointer);",
                "auto id = std::hash<object*>{}(pointer);",
                "auto id = (std::uint64_t)&object;",
                "auto id = reinterpret_cast<std::size_t>(pointer);",
                "auto id = reinterpret_cast<unsigned long long>(pointer);",
                "std::uintptr_t id;",
            ),
            "native-byte-layout": (
                "memcpy(output, &integer, sizeof(integer));",
                "memmove(output, &header, sizeof(header));",
                "auto raw = std::as_bytes(std::span{&value, 1});",
                "auto raw = reinterpret_cast<const char*>(&value);",
                "auto raw = std::bit_cast<std::array<std::byte, 8>>(value);",
                "if constexpr (std::endian::native == std::endian::big) {}",
                "output.append(htonl(value));",
                "hasher.update(&integer, sizeof(integer));",
                "stream.write(std::addressof(header), sizeof(header));",
                "EVP_DigestUpdate(context, &value, sizeof(value));",
                "SHA256_Update(context, &value, sizeof(value));",
                "fwrite(&value, sizeof(value), 1, output);",
            ),
        }
        for rule, snippets in cases.items():
            for snippet in snippets:
                with self.subTest(rule=rule, snippet=snippet):
                    self.write("// heading\n" + snippet)
                    failures = self.scan()
                    self.assertTrue(
                        any(f":2: {rule}: " in failure for failure in failures),
                        failures,
                    )
                    self.assertTrue(
                        all(len(failure.split(": ")[-1]) > 20 for failure in failures)
                    )

    def test_ordered_integer_and_bytewise_operations_are_permitted(self) -> None:
        self.write("""
          void exercise() {
            std::map<unsigned, event> ordered;
            std::vector<unsigned> stable_order;
            auto draws = fixture.random().stream(domain, id);
            random_type& random() { return random_; }
            virtual_time& time();
            auto now = environment.time().monotonic_now();
            if (input.size() > sizeof(std::uint64_t) && input[0]) {}
            for (unsigned byte = 0; byte < 8; ++byte) {
                output.push_back(static_cast<std::uint8_t>(value >> (8U * byte)));
            }
          }
        """)
        self.assertEqual(self.scan(), [])

    def test_comments_literals_and_includes_do_not_trigger(self) -> None:
        self.write(r"""
            #include <seastar/core/lowres_clock.hh>
            // std::unordered_map<int,int> and std::chrono::steady_clock
            /* memcpy(output, &header, sizeof(header)); */
            const char* text = "std::mt19937 engine; rand();";
            auto raw = R"tag(std::bit_cast<std::uint64_t>(pointer))tag";
            auto prefixed = u8R"x(std::as_bytes(value))x";
            auto character = 'a';
        """)
        self.assertEqual(self.scan(), [])

    def test_comments_and_line_splices_cannot_hide_tokens(self) -> None:
        self.write(
            "std::unor\\\ndered_map /* separator */ <int,int> values;\r\n"
            "auto now = std::chrono::steady_clock /* separator */ ::now();\r"
            "std::uni\\\nform_int_distribution<int> random;\n"
        )
        failures = self.scan()
        self.assertTrue(any(":1: unordered-state:" in item for item in failures))
        self.assertTrue(any(":3: host-clock:" in item for item in failures))
        self.assertTrue(any(":4: random-source:" in item for item in failures))

    def test_digit_separators_do_not_mask_later_code(self) -> None:
        for number in ("1'024", "0xAB'CD", "1\\\n'024", "0b1'001", "1.0e1'0"):
            with self.subTest(number=number):
                self.write(f"auto count = {number}; auto choice = rand();\n")
                failures = self.scan()
                self.assertEqual(len(failures), 1)
                self.assertIn("random-source:", failures[0])
        self.write("auto count = 1'024; auto text = \"1'024 rand()\"; auto ch = 'a';")
        self.assertEqual(self.scan(), [])
        self.write("auto ch = '1'_suffix; auto value = rand();")
        self.assertEqual(len(self.scan()), 1)

    def test_scope_includes_deterministic_tests_and_canonical_code(self) -> None:
        for path in (
            "src/simulation/new_file.cc",
            "src/simulation/tests/new_test.cc",
            "src/runtime/testing/contracts/new_contract.h",
            "src/runtime/tests/new_determinism_test.cc",
            "src/runtime/tests/time_random_contract_test.cc",
            "src/observability/event_codec.cc",
            "src/observability/event_bench.cc",
            "src/metadata/proposer.cc",
            "src/raft/journal.h",
            "src/range/engine.cc",
        ):
            self.assertTrue(is_deterministic_source(Path(path)), path)
        for path in (
            "src/broker/application.cc",
            "src/runtime/production/random.cc",
            "src/simulation/README.md",
            "src/simulation/tests/testdata/input.script",
        ):
            self.assertFalse(is_deterministic_source(Path(path)), path)

    def test_pure_sources_are_the_metadata_objects_apply_and_commands(self) -> None:
        for path in (
            "src/metadata/objects.h",
            "src/metadata/objects_topic.cc",
            "src/metadata/apply.cc",
            "src/metadata/apply_range_roll.cc",
            "src/metadata/commands.h",
        ):
            self.assertTrue(is_pure_source(Path(path)), path)
        for path in (
            "src/metadata/proposer.cc",
            "src/metadata/applied.cc",
            "src/metadata/tests/apply_test.cc",
            "src/range/apply.cc",
            "src/metadata/apply.py",
        ):
            self.assertFalse(is_pure_source(Path(path)), path)

    def test_iteration_through_getters_aliases_and_algorithms_is_rejected(
        self,
    ) -> None:
        path = "src/range/streams.h"
        declaration = (
            "absl::flat_hash_map<std::uint64_t, entry> streams_;\n"
            "const absl::flat_hash_map<std::uint64_t, entry>& streams() const;\n"
        )
        allowances = (
            Allowance(
                path,
                "unordered-state",
                "absl::flat_hash_map<std::uint64_t, entry> streams_;",
                1,
                "Lookup by stream; the ordered stream list is iterated instead.",
            ),
            Allowance(
                path,
                "unordered-state",
                "const absl::flat_hash_map<std::uint64_t, entry>& streams() const;",
                1,
                "Lookup by stream; the ordered stream list is iterated instead.",
            ),
        )
        for body in (
            "void f() { for (const auto& [id, e] : streams_) { use(e); } }",
            "void f(owner& s) { for (const auto& [id, e] : s.streams()) { use(e); } }",
            "void f(owner& s) { auto it = s.streams().begin(); }",
            "void f() { auto& alias = streams_; }",
            "void f(owner& s) { const auto& alias = s.streams(); }",
            "void f() { absl::c_for_each(streams_, use); }",
            "void f(owner& s) { std::ranges::for_each(s.streams_, use); }",
            "void f() { std::erase_if(streams_, stale); }",
            "void f() { auto copy = &streams_; }",
            "void f() { const auto& alias{streams_}; }",
            "void f() { auto first = absl::c_find_if(streams_, live); }",
            "void f() { auto low = std::ranges::min_element(streams_); }",
        ):
            with self.subTest(body=body):
                self.write(declaration + body, path)
                failures = self.scan(allowances)
                self.assertEqual(len(failures), 1, failures)
                self.assertIn("unordered-iteration:", failures[0])
        for body in (
            "void f(std::uint64_t id) { auto found = streams_.find(id); }",
            "void f(std::uint64_t id) { auto& e = streams_[id]; }",
            "void f() { auto count = streams_.size(); }",
        ):
            with self.subTest(body=body):
                self.write(declaration + body, path)
                self.assertEqual(self.scan(allowances), [])

    def test_pure_sources_refuse_time_draws_suspension_configuration_and_state(
        self,
    ) -> None:
        path = "src/metadata/apply.cc"
        for code, rule in (
            ("void f() { auto at = env.clock().now(); }", "pure-time"),
            ("void f() { auto at = wall_clock::now(); }", "pure-time"),
            (
                "void f() { auto pick = runtime::uniform_u64(source, 10); }",
                "pure-random",
            ),
            (
                "void f() { auto id = model::mint_object_id<topic_id>(entropy); }",
                "pure-random",
            ),
            ("seastar::future<> f();", "pure-async"),
            ("void f() { co_await yield(); }", "pure-async"),
            ("void f() { auto shard = runtime::current_shard(); }", "pure-async"),
            ("void f() { auto limit = config::default_limit; }", "pure-config"),
            ("static std::uint64_t applied = 0;", "pure-mutable-static"),
            ("thread_local int scratch;", "pure-mutable-static"),
            ("static std::uint64_t applied(0);", "pure-mutable-static"),
            ("static std::uint64_t counts[4];", "pure-mutable-static"),
            ("static std::uint64_t counts[4] = {};", "pure-mutable-static"),
            ("static std::function<void()> hook;", "pure-mutable-static"),
            ("int counter = 0;", "pure-mutable-static"),
            ("inline std::uint64_t counter{0};", "pure-mutable-static"),
            ("namespace m {\ntopic_id last;\n}", "pure-mutable-static"),
            ("auto f() { return seastar::make_ready_future<>(); }", "pure-async"),
            ("void f(future_type f) { f.then(use); }", "pure-async"),
            ("void f() { seastar::when_all(a(), b()); }", "pure-async"),
            ("#include <seastar/core/future.hh>", "pure-include"),
            ("#include <seastar/coroutine/maybe_yield.hh>", "pure-include"),
            ("struct s { std::unordered_map<int, int> topics; };", "unordered-state"),
            ('#include "src/runtime/random.h"', "pure-include"),
            ('#include "src/storage/local_engine.h"', "pure-include"),
            ('#include "src/config/bootstrap_config.h"', "pure-include"),
            ("#include <src/simulation/environment.h>", "pure-include"),
        ):
            with self.subTest(code=code):
                self.write(code + "\n", path)
                failures = self.scan()
                self.assertTrue(failures, code)
                self.assertTrue(
                    all(f"{rule}:" in failure for failure in failures), failures
                )

    def test_pure_sources_accept_value_types_and_constants(self) -> None:
        self.write(
            '#include "src/runtime/time.h"\n'
            '#include "src/runtime/error.h"\n'
            '#include "src/model/identity.h"\n'
            "#include <seastar/core/chunked_vector.hh>\n"
            "struct state {\n"
            "    static result<state> make(limits values);\n"
            "    static constexpr std::size_t window = 4096;\n"
            "    static const int table[3];\n"
            "    absl::btree_map<topic_id, topic> topics;\n"
            "};\n"
            "inline constexpr int version = 1;\n"
            "static_assert(sizeof(state) > 0);\n"
            "auto created(const command& c) { return c.proposed_at; }\n"
            "static int helper(int value);\n"
            "static auto lookup(const state& s) -> int;\n"
            "using topics_type = absl::btree_map<topic_id, topic>;\n"
            "enum class kind : std::uint8_t { a, b };\n"
            "int apply_one(state& s) { int applied = 0; return applied; }\n",
            "src/metadata/objects.h",
        )
        self.assertEqual(self.scan(), [])

    def test_an_allowance_on_a_pure_source_is_itself_a_violation(self) -> None:
        code = "absl::flat_hash_map<topic_id, topic> topics;"
        self.write(code + "\n", "src/metadata/objects.h")
        failures = self.scan(
            (
                Allowance(
                    "src/metadata/objects.h",
                    "unordered-state",
                    code,
                    1,
                    "Lookup only.",
                ),
            )
        )
        self.assertTrue(any("pure-allowance:" in failure for failure in failures))
        self.assertTrue(any("unordered-state:" in failure for failure in failures))

    def test_every_test_source_rejects_host_randomness_but_not_clocks(self) -> None:
        for path in (
            "src/broker/tests/new_test.cc",
            "src/broker/pid_file_test.cc",
            "src/runtime/testing/fixture.h",
            "bazel/tests/probe.cc",
        ):
            self.assertTrue(is_test_source(Path(path)), path)
        for path in ("src/broker/application.cc", "tools/check_determinism.py"):
            self.assertFalse(is_test_source(Path(path)), path)
        for path in ("src/broker/pid_file_test.cc", "bazel/tests/probe.cc"):
            for violation in (
                "std::mt19937 engine{std::random_device{}()};",
                "auto value = seastar::testing::local_random_engine();",
            ):
                with self.subTest(path=path, violation=violation):
                    self.write(violation, path)
                    self.assertTrue(
                        any(": random-source: " in item for item in self.scan()),
                        violation,
                    )
            # Only randomness is in scope outside deterministic code.
            self.write("auto now = std::chrono::steady_clock::now();", path)
            self.assertEqual(self.scan(), [])

    def test_namespace_and_static_inferred_random_sources_are_rejected(self) -> None:
        for source in (
            "sequential_random_source draws;",
            "namespace testing { deterministic_random engine(42); }",
            "void run() { static deterministic_random engine(42); }",
            "namespace testing { deterministic_random engine{42}; }",
            "namespace a::b { auto draws = fixture.random().stream(domain, id); }",
            "void run() { static auto draws = fixture.random().stream(domain, id); }",
            "void run() { thread_local auto rng = deterministic_random{42}; }",
            "struct holder { static sequential_random_source draws; };",
        ):
            with self.subTest(source=source):
                self.write(source)
                self.assertTrue(any("global-random:" in item for item in self.scan()))
        self.write("""
            class sequential_random_source final {};
            class deterministic_random final {};
            struct fixture { sequential_random_source draws; };
            namespace helpers {
                void run() { sequential_random_source local; }
                auto run = [] { auto rng = deterministic_random{42}; };
            }
        """)
        self.assertEqual(self.scan(), [])

    def test_lookup_declaration_does_not_permit_ordering_traversal(self) -> None:
        declaration = "seastar::chunked_hash_map<int, int> ready;"
        allowance = Allowance(
            self.path, "unordered-state", declaration, 1, "Lookup only"
        )
        for traversal in (
            "for (auto& item : ready) { emit(item); }",
            "auto it = ready.begin();",
            "std::ranges::for_each(ready, emit);",
        ):
            self.write(declaration + "\n" + traversal)
            failures = self.scan((allowance,))
            self.assertEqual(len(failures), 1, failures)
            self.assertIn("unordered-iteration:", failures[0])
        self.write(declaration + "\nauto found = ready.find(42);")
        self.assertEqual(self.scan((allowance,)), [])

    def test_random_factory_prototypes_are_not_global_objects(self) -> None:
        self.write("""
            deterministic_random make_random();
            sequential_random_source make_stream(void);
            deterministic_random make_seeded(std::uint64_t seed);
            deterministic_random combine(unsigned long long first, std::uint32_t second = 1);
            struct owner {
                static deterministic_random make_random();
                static sequential_random_source make_seeded(std::uint64_t);
            };
        """)
        self.assertEqual(self.scan(), [])
        for source in (
            "deterministic_random engine(42);",
            "deterministic_random engine(seed);",
            "deterministic_random engine(std::uint64_t{42});",
            "static deterministic_random engine(seed);",
            "static auto rng = random_engine();",
            "auto rng = fixture.random();",
            "auto rng = fixture.random().stream(domain, id);",
        ):
            with self.subTest(source=source):
                self.write(source)
                self.assertTrue(any("global-random:" in item for item in self.scan()))

    def test_hash_alias_member_and_parameter_traversals_are_checked(self) -> None:
        declaration = "using index_type = seastar::chunked_hash_map<int, int>;"
        allowance = Allowance(
            self.path, "unordered-state", declaration, 1, "Lookup only"
        )
        self.write(
            declaration
            + """
            index_type queue;
            void export_values(index_type& values, index_type *ready) {
                for (auto& item : values) { emit(item); }
                auto it = queue.begin();
                auto it2 = ready->begin();
            }
        """
        )
        failures = self.scan((allowance,))
        self.assertEqual(len(failures), 3, failures)
        self.assertTrue(all("unordered-iteration:" in item for item in failures))

    def test_each_reviewed_allowance_is_necessary_and_removable(self) -> None:
        for allowance in ALLOWANCES:
            with self.subTest(path=allowance.path, code=allowance.code):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    target = root / allowance.path
                    target.parent.mkdir(parents=True)
                    target.write_text((allowance.code + "\n") * allowance.count)
                    code = masked_code(target.read_text())[0]
                    companions = tuple(
                        other
                        for other in ALLOWANCES
                        if other != allowance
                        and other.path == allowance.path
                        and len(occurrences(code, other.code)) == other.count
                    )
                    self.assertEqual(scan(root, (allowance, *companions), ()), [])
                    failures = scan(root, companions, ())
                    self.assertEqual(
                        len(failures), allowance.count * allowance.matches_per_span
                    )
                    self.assertTrue(
                        all(f": {allowance.rule}: " in item for item in failures)
                    )

    def test_benchmark_clock_allowances_pin_complete_bodies_and_both_reads(
        self,
    ) -> None:
        allowances = tuple(
            allowance
            for allowance in ALLOWANCES
            if allowance.path == "src/simulation/tests/simulation_bench.cc"
            and allowance.rule == "host-clock"
        )
        self.assertEqual(len(allowances), 2)
        for allowance in allowances:
            with self.subTest(code=allowance.code):
                self.assertEqual(allowance.count, 1)
                self.assertEqual(allowance.matches_per_span, 2)
                self.write(allowance.code, allowance.path)
                self.assertEqual(self.scan((allowance,)), [])
                failures = self.scan((replace(allowance, matches_per_span=1),))
                self.assertTrue(any("stale-allowance:" in item for item in failures))
                self.assertEqual(sum(": host-clock:" in item for item in failures), 2)

                # Retaining both clocks does not permit an unreviewed change
                # to measurement data flow or the watchdog's control flow.
                self.write(
                    allowance.code.replace("{", "{ alter_state();", 1), allowance.path
                )
                failures = self.scan((allowance,))
                self.assertTrue(any("stale-allowance:" in item for item in failures))
                self.assertEqual(sum(": host-clock:" in item for item in failures), 2)

                extra_clock = "auto unreviewed = std::chrono::steady_clock::now();"
                self.write(
                    allowance.code.replace("{", "{" + extra_clock, 1), allowance.path
                )
                failures = self.scan((allowance,))
                self.assertTrue(any("stale-allowance:" in item for item in failures))
                self.assertEqual(sum(": host-clock:" in item for item in failures), 3)

                self.write(allowance.code + "\n" + extra_clock, allowance.path)
                failures = self.scan((allowance,))
                self.assertEqual(len(failures), 1)
                self.assertIn(": host-clock:", failures[0])

    def test_reviewed_iteration_bodies_and_required_sorts_cannot_change(self) -> None:
        for allowance in ALLOWANCES:
            if allowance.rule != "unordered-iteration":
                continue
            with self.subTest(code=allowance.code):
                self.write(allowance.code, allowance.path)
                modified = allowance.code.replace("{", "{ schedule(next_id++);", 1)
                self.write(modified, allowance.path)
                failures = self.scan((allowance,))
                self.assertTrue(any("stale-allowance:" in item for item in failures))
                self.assertTrue(
                    any(": unordered-iteration:" in item for item in failures)
                )
                # A surrounding loop is not allowed to lose its canonical sort.
                if "std::ranges::sort" in allowance.code:
                    self.write(
                        allowance.code.replace("std::ranges::sort", "unsorted_export"),
                        allowance.path,
                    )
                    self.assertTrue(
                        any(
                            "stale-allowance:" in item
                            for item in self.scan((allowance,))
                        )
                    )
                (self.root / allowance.path).unlink()

    def test_allowance_cannot_cover_another_expression_file_or_occurrence(self) -> None:
        snippet = "auto deadline = seastar::lowres_clock::now() + watchdog;"
        allowance = Allowance(self.path, "host-clock", snippet, 1, "External watchdog")
        self.write(snippet + "\nauto timestamp = seastar::lowres_clock::now();")
        self.assertEqual(len(self.scan((allowance,))), 1)
        self.write(snippet, "src/simulation/another.cc")
        self.assertEqual(len(self.scan((allowance,))), 2)
        (self.root / "src/simulation/another.cc").unlink()
        self.write(snippet + "\n" + snippet)
        failures = self.scan((allowance,))
        self.assertTrue(any("stale-allowance:" in item for item in failures))
        self.assertEqual(sum(": host-clock:" in item for item in failures), 2)

    def test_deleted_modified_or_no_longer_relevant_allowances_fail(self) -> None:
        allowance = Allowance(
            self.path, "host-clock", "steady_clock::now()", 1, "External watchdog"
        )
        for source in ("", "system_clock::now()", "// steady_clock::now()"):
            self.write(source)
            self.assertTrue(
                any("stale-allowance:" in item for item in self.scan((allowance,)))
            )
        harmless = Allowance(
            self.path, "host-clock", "return 1;", 1, "Invalid stale seam"
        )
        self.write("return 1;")
        self.assertIn("stale-allowance:", self.scan((harmless,))[0])
        (self.root / self.path).unlink()
        self.assertIn("stale-allowance:", self.scan((allowance,))[0])

    def test_allowance_requires_a_rule_count_and_reason(self) -> None:
        self.write("steady_clock::now();")
        valid = Allowance(self.path, "host-clock", "steady_clock::now();", 1, "reason")
        invalid = (
            replace(valid, rule="unknown"),
            replace(valid, count=0),
            replace(valid, count=-1),
            replace(valid, reason=""),
            replace(valid, matches_per_span=0),
            replace(valid, matches_per_span=-1),
        )
        for allowance in invalid:
            for code in (valid.code, "", "// no reviewed code"):
                with self.subTest(allowance=allowance, code=code):
                    failures = self.scan((replace(allowance, code=code),))
                    self.assertTrue(
                        any(
                            "invalid-allowance: specify a known rule" in item
                            for item in failures
                        )
                    )
                    self.assertEqual(
                        sum(": host-clock:" in item for item in failures), 1
                    )

    def test_allowance_requires_code_tokens_and_does_not_stop_scanning(self) -> None:
        clock = "auto time = steady_clock::now();"
        self.write(clock + "\nstd::mt19937 generator;")
        valid = Allowance(self.path, "host-clock", clock, 1, "Benchmark duration")
        for code in ("", " \t\r\n", "// no code", "/* no code */", "#include <random>"):
            with self.subTest(code=code):
                empty = Allowance(
                    self.path, "random-source", code, 1, "Invalid empty span"
                )
                failures = self.scan((empty, valid))
                self.assertIn(
                    f"{self.path}:1: invalid-allowance: reviewed code span must contain code tokens",
                    failures,
                )
                self.assertEqual(
                    sum(": random-source:" in item for item in failures), 1
                )
                self.assertFalse(any(": host-clock:" in item for item in failures))
                self.assertEqual(len(failures), 2)

    def test_every_canonical_writer_must_remain_present(self) -> None:
        for writer in WRITERS:
            with self.subTest(writer=writer.name):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    target = root / writer.path
                    target.parent.mkdir(parents=True)
                    target.write_text(writer.code)
                    self.assertEqual(scan(root, (), (writer,)), [])
                    target.write_text("void replacement() {}")
                    self.assertIn("fixed-endian-writer:", scan(root, (), (writer,))[0])
                    target.unlink()
                    self.assertIn("fixed-endian-writer:", scan(root, (), (writer,))[0])

    def test_a_blessed_writer_does_not_exempt_other_native_serialization(self) -> None:
        body = "void fixed(unsigned value) { output.push_back(value & 255U); value >>= 8U; }"
        writer = Writer(self.path, "fixed byte order", body)
        self.write(body + "\nmemcpy(output, &value, sizeof(value));")
        failures = self.scan(writers=(writer,))
        self.assertEqual(len(failures), 1)
        self.assertIn(":2: native-byte-layout:", failures[0])
        self.write(body.replace(">>= 8U", ">>= 16U"))
        self.assertIn("fixed-endian-writer:", self.scan(writers=(writer,))[0])


if __name__ == "__main__":
    unittest.main()
