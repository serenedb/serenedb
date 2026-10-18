import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[4]
SPEC = importlib.util.spec_from_file_location("pdf_fixture", ROOT / "scripts/gen_pdf_fixture.py")
fixture = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fixture)


class FixtureGenerationTest(unittest.TestCase):
    def setUp(self):
        self.work = tempfile.TemporaryDirectory()
        self.addCleanup(self.work.cleanup)
        directory = Path(self.work.name)
        self.source = directory / "source"
        self.source.mkdir()
        self.output = directory / "output"
        generator = (
            "import pathlib, sys\n"
            "directory = pathlib.Path(sys.argv[1])\n"
            "directory.mkdir()\n"
            "(directory / 'sample.pdf').write_bytes(b'fixture')\n"
        )
        self.scripts = [self.source / name for name in ("generate.py", "generate_rendered.py")]
        for path in self.scripts:
            path.write_text(generator)
        for name, value in (("SOURCE", self.source), ("OUT", self.output), ("INPUTS", self.scripts)):
            self.enterContext(patch.object(fixture, name, value))
        self.enterContext(patch.object(fixture.importlib.util, "find_spec", return_value=True))

    def test_cache_and_invalidation(self):
        previous_umask = os.umask(0o077)
        try:
            fixture.ensure_fixture()
        finally:
            os.umask(previous_umask)
        for path in [self.output, *self.output.rglob("*")]:
            self.assertEqual(path.stat().st_mode & 0o777, 0o755 if path.is_dir() else 0o644)
        stamp = self.output / ".generated"
        first = stamp.stat().st_mtime_ns
        with patch.object(fixture.subprocess, "run", side_effect=AssertionError("cache miss")):
            fixture.ensure_fixture()
        self.assertEqual(first, stamp.stat().st_mtime_ns)
        (self.output / "rendered/sample.pdf").unlink()
        fixture.ensure_fixture()
        self.assertEqual((self.output / "rendered/sample.pdf").read_bytes(), b"fixture")
        with self.scripts[0].open("a") as script:
            script.write("(directory / 'extra.pdf').write_bytes(b'extra')\n")
        fixture.ensure_fixture()
        self.assertEqual((self.output / "basic/extra.pdf").read_bytes(), b"extra")

    def test_failure_does_not_publish_partial_fixtures(self):
        self.scripts[1].write_text("raise SystemExit(1)\n")
        with self.assertRaises(subprocess.CalledProcessError):
            fixture.ensure_fixture()
        self.assertFalse(self.output.exists())
        self.assertEqual(list(self.output.parent.glob("pdf-fixture-*")), [])


if __name__ == "__main__":
    unittest.main()
