import csv
import io
import os
from pathlib import Path
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[3]
SERENED = os.environ.get("SDB_DRV_SERENED_BIN", str(ROOT / "build/bin/serened"))


@pytest.fixture(scope="module")
def pdf():
    subprocess.run([str(ROOT / "scripts/ensure_pdf_fixture.sh")], check=True, timeout=180)
    return (ROOT / "resources/tests/pdf/basic/text.pdf").as_posix().replace("'", "''")


@pytest.mark.parametrize("function", ["read_pdf", "read_pdf_blocks"])
def test_pdf_extension_in_shell(pdf, function):
    result = subprocess.run(
        [SERENED, "shell", ":memory:", "-csv", "-noheader", "-c",
         "SELECT extension_name, loaded FROM duckdb_extensions() WHERE extension_name = 'pdf'; "
         f"SELECT page, text FROM {function}('{pdf}') ORDER BY page;"],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert list(csv.reader(io.StringIO(result.stdout))) == [
        ["pdf", "true"], ["1", "Hello PDF"], ["2", "Second page"],
    ]
