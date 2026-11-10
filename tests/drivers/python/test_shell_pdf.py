import csv
import io
import os
from pathlib import Path
import subprocess
import random

import psycopg
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
         "SELECT extension_name, loaded, extension_version <> '' "
         "FROM duckdb_extensions() WHERE extension_name = 'pdf'; "
         f"SELECT page, text FROM {function}('{pdf}') ORDER BY page;"],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert list(csv.reader(io.StringIO(result.stdout))) == [
        ["pdf", "true", "true"], ["1", "Hello PDF"], ["2", "Second page"],
    ]


def test_pdf_tables_in_shell(pdf):
    path = ROOT / "resources/tests/pdf/rendered/business_report.pdf"
    escaped = str(path).replace("'", "''")
    result = subprocess.run(
        [SERENED, "shell", ":memory:", "-csv", "-noheader", "-c",
         f"SELECT row_index, cells[1], cells[4] FROM read_pdf_tables('{escaped}') "
         "ORDER BY row_index;"], capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert list(csv.reader(io.StringIO(result.stdout))) == [
        ["1", "Item", "Amount"], ["2", "Document review", "100.00"],
        ["3", "Archive storage", "37.50"], ["4", "Total EUR", "137.50"],
    ]


def test_malformed_pdf_corpus_keeps_server_alive(pdf, own_server, tmp_path):
    source = (ROOT / "resources/tests/pdf/basic/text.pdf").read_bytes()
    samples = [b"", b"not a pdf", b"%PDF-1.4\n1 0 obj\n<<",
               source[:len(source) // 2], source[:-32],
               source.replace(b"/Count 2", b"/Count -1"),
               source.replace(b"/Parent 2 0 R", b"/Parent 4 0 R"),
               source.replace(b"/Length 40", b"/Length 999999999999999999999")]
    rng = random.Random(1129)
    for _ in range(24):
        damaged = bytearray(source)
        for _ in range(8):
            damaged[rng.randrange(len(damaged))] = rng.randrange(256)
        samples.append(bytes(damaged))
    with psycopg.connect(own_server.dsn(), autocommit=True) as connection:
        connection.execute("SET statement_timeout = '3s'")
        for index, contents in enumerate(samples):
            path = tmp_path / f"malformed-{index}.pdf"
            path.write_bytes(contents)
            for function in ("read_pdf", "read_pdf_blocks", "read_pdf_tables"):
                try:
                    connection.execute(f"SELECT count(*) FROM {function}(%s)", (str(path),)).fetchone()
                except psycopg.Error as error:
                    assert not isinstance(error, psycopg.OperationalError), (index, function, error)
                assert connection.execute("SELECT 42").fetchone() == (42,)
        assert connection.execute("SELECT count(*) FROM read_pdf(%s)", (pdf,)).fetchone() == (2,)
    assert not own_server.log.scan()
