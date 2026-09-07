from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tools.cpp_format import main, selected_files


class CppFormatTest(unittest.TestCase):
    def test_all_scope_covers_tracked_and_new_sources_but_not_ignored_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(['git', 'init', '-q', str(root)], check=True)
            (root / '.gitignore').write_text('ignored.cc\n')
            for name in ('tracked.cc', 'new.h', 'ignored.cc', 'notes.txt'):
                (root / name).write_text('')
            subprocess.run(['git', 'add', 'tracked.cc', '.gitignore'], cwd=root, check=True)
            self.assertEqual(selected_files(root, 'all', []), [Path('new.h'), Path('tracked.cc')])

    def test_formatter_failure_is_returned_with_warning_as_error_check_mode(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'example.cc').write_text('int main( ){return 0;}')
            with mock.patch('tools.cpp_format.workspace_root', return_value=root), \
                 mock.patch('sys.argv', ['format', '--tool=/bin/false', '--check', 'example.cc']), \
                 mock.patch('tools.cpp_format.subprocess.run') as run:
                run.return_value.returncode = 1
                self.assertEqual(main(), 1)
                self.assertIn('--dry-run', run.call_args.args[0])
                self.assertIn('--Werror', run.call_args.args[0])


if __name__ == '__main__':
    unittest.main()
