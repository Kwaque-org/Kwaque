import io
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch

from tools.check_bazel_package_cycles import find_cycle, graph_from_query_xml, main


class PackageCycleTest(unittest.TestCase):
    def test_accepts_acyclic_graph(self) -> None:
        graph = {
            "//src/broker": {"//src/cluster", "//src/runtime"},
            "//src/cluster": {"//src/model"},
            "//src/model": set(),
            "//src/runtime": set(),
        }
        self.assertIsNone(find_cycle(graph))

    def test_rejects_synthetic_cycle_from_query_xml(self) -> None:
        query_xml = """\
<query version="2">
  <rule class="cc_library" name="//src/model:model">
    <rule-input name="//src/storage:storage"/>
  </rule>
  <rule class="cc_library" name="//src/storage:storage">
    <rule-input name="//src/protocol:protocol"/>
  </rule>
  <rule class="cc_library" name="//src/protocol:protocol">
    <rule-input name="//src/model:model"/>
  </rule>
</query>
"""
        self.assertEqual(
            find_cycle(graph_from_query_xml(query_xml)),
            ["//src/model", "//src/storage", "//src/protocol", "//src/model"],
        )

    def test_rejects_cycle_across_first_party_roots(self) -> None:
        query_xml = """\
<query version="2">
  <rule class="cc_library" name="//src/model:model">
    <rule-input name="//proto/kwaque/common/v1:build_info_cc_proto"/>
  </rule>
  <rule class="cc_library" name="//proto/kwaque/common/v1:codec">
    <rule-input name="//src/model:model"/>
  </rule>
</query>
"""
        self.assertEqual(
            find_cycle(graph_from_query_xml(query_xml)),
            [
                "//proto/kwaque/common/v1",
                "//src/model",
                "//proto/kwaque/common/v1",
            ],
        )

    def test_rejects_cycle_from_root_package_to_descendant(self) -> None:
        query_xml = """\
<query version="2">
  <rule class="package_group" name="//src:root">
    <rule-input name="//src/model:model"/>
  </rule>
  <rule class="cc_library" name="//src/model:model">
    <rule-input name="//src:root"/>
  </rule>
</query>
"""
        self.assertEqual(
            find_cycle(graph_from_query_xml(query_xml)),
            ["//src", "//src/model", "//src"],
        )

    def test_rejects_rule_without_name(self) -> None:
        with self.assertRaisesRegex(ValueError, "rule element.*name attribute"):
            graph_from_query_xml('<query version="2"><rule/></query>')

    def test_rejects_rule_input_without_name(self) -> None:
        query_xml = """\
<query version="2">
  <rule class="cc_library" name="//src/model:model">
    <rule-input/>
  </rule>
</query>
"""
        with self.assertRaisesRegex(ValueError, "rule-input element.*name attribute"):
            graph_from_query_xml(query_xml)

    def test_schema_invalid_file_returns_input_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            query_xml = Path(directory) / "query.xml"
            query_xml.write_text(
                '<query version="2"><rule/></query>', encoding="utf-8"
            )
            stderr = io.StringIO()
            with (
                patch.object(
                    sys,
                    "argv",
                    ["check_bazel_package_cycles", "--query-xml", str(query_xml)],
                ),
                redirect_stderr(stderr),
            ):
                self.assertEqual(main(), 2)

        self.assertIn("missing required name attribute", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
