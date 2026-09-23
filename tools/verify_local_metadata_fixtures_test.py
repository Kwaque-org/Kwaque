from pathlib import Path
import unittest

try:
    from tools.verify_format_fixtures import verify
except ModuleNotFoundError:
    from verify_format_fixtures import verify


class LocalMetadataFixtureTest(unittest.TestCase):
    def test_independent_fields_checksums_and_maximum_encodings(self):
        root = Path(__file__).resolve().parents[1] / "src/storage/tests/testdata/local_metadata"
        for number, count in enumerate((24, 24, 24, 19), 1):
            with self.subTest(manifest=number):
                self.assertEqual(verify(root / f"manifest-{number:02}.json"), count)


if __name__ == "__main__":
    unittest.main()
