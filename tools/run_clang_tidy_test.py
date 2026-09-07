from __future__ import annotations

import unittest

from tools.run_clang_tidy import production_sources_from_query, select_files


def rule(name: str, source: str, testonly: bool = False) -> str:
    return (f'<rule class="cc_library" name="//src/model:{name}">'
            f'<boolean name="testonly" value="{str(testonly).lower()}"/>'
            f'<list name="srcs"><label value="{source}"/></list></rule>')


class ClangTidySelectionTest(unittest.TestCase):
    def test_strict_scope_uses_testonly_and_architectural_boundaries(self) -> None:
        xml = '<query>' + ''.join((
            rule('production', '//src/model:record.cc'),
            rule('helper', '//src/model:fixture.cc', True),
            rule('simulator', '//src/simulation:scheduler.cc'),
            rule('testing', '//src/runtime/testing:main.cc'),
            rule('test', '//src/model:record_test.cc'),
            rule('bench', '//src/model:record_bench.cc'),
            rule('fuzzer', '//src/model:record_fuzz.cc'),
        )) + '</query>'
        self.assertEqual(production_sources_from_query(xml), {'src/model/record.cc'})

    def test_ordinary_scope_keeps_test_simulation_fuzz_and_benchmark_sources(self) -> None:
        files = ['src/model/record.cc', 'src/model/record_test.cc',
                 'src/simulation/scheduler.cc', 'src/model/record_fuzz.cc',
                 'src/model/record_bench.cc']
        entries = [{'file': name} for name in files]
        self.assertEqual(select_files(entries, [], None), sorted(files))
        self.assertEqual(select_files(entries, [], {files[0]}), [files[0]])

    def test_missing_or_empty_selection_fails_instead_of_passing_silently(self) -> None:
        for entries, requested, production in (([], [], None), ([{}], [], None),
                ([{'file': 'src/a.cc'}], ['src/missing.cc'], None),
                ([{'file': 'src/a.cc'}], [], set()),
                ([{'file': 'src/a.cc'}], [], {'src/a.cc', 'src/missing.cc'})):
            with self.subTest(entries=entries), self.assertRaises(ValueError):
                select_files(entries, requested, production)


if __name__ == '__main__':
    unittest.main()
