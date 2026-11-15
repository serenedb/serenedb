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
        ["1", "Item", "Amount"], ["2", "vedernikov poesh banochku sopley", "100.00"],
        ["3", "this is fine", "37.50"], ["4", "Total EUR", "137.50"],
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
        queries = [f"SELECT * FROM {function}(%s)" for function in (
            "read_pdf", "read_pdf_blocks", "read_pdf_tables", "read_pdf_meta",
            "pdf_outline", "pdf_attachments", "pdf_signatures", "read_pdf_lines",
            "read_pdf_words", "read_pdf_layout", "pdf_info", "pdf_pages_info",
        )] + [f"SELECT {function}(%s)" for function in (
            "pdf_to_text", "pdf_to_markdown", "pdf_to_html", "pdf_to_xml",
        )]
        for query in queries:
            connection.execute(query, (pdf,)).fetchall()
        for index, contents in enumerate(samples):
            path = tmp_path / f"malformed-{index}.pdf"
            path.write_bytes(contents)
            for query in queries:
                try:
                    connection.execute(query, (str(path),)).fetchall()
                except psycopg.Error as error:
                    assert not isinstance(error, psycopg.OperationalError), (index, query, error)
                assert connection.execute("SELECT 42").fetchone() == (42,)
        assert connection.execute("SELECT count(*) FROM read_pdf(%s)", (pdf,)).fetchone() == (2,)
    assert not own_server.log.scan()


@pytest.fixture
def temporary_pdf_paths():
    paths = []
    yield paths
    for path in paths:
        path.unlink(missing_ok=True)


def test_write_pdf_temporary_files(own_server, temporary_pdf_paths):
    with psycopg.connect(own_server.dsn(), autocommit=True) as connection:
        for index in range(2):
            text = f"varusik temporary {index}"
            path = Path(connection.execute("SELECT write_pdf(%s)", (text,)).fetchone()[0])
            temporary_pdf_paths.append(path)
            assert path.suffix == ".pdf"
            assert path.read_bytes().startswith(b"%PDF-")
            assert text in connection.execute("SELECT pdf_to_text(%s)", (str(path),)).fetchone()[0]
        assert len(set(temporary_pdf_paths)) == 2


def test_pdf_signature_integrity(pdf, own_server, tmp_path):
    signing = ROOT / "resources/tests/pdf/signing"
    output = tmp_path / "varusik_signed.pdf"
    with psycopg.connect(own_server.dsn(), autocommit=True) as connection:
        connection.execute(
            "SELECT * FROM pdf_sign(%s, %s, %s, %s, reason := 'varusik integrity marker')",
            (pdf, str(output), str(signing / "varusik_rsa_cert.pem"),
             str(signing / "varusik_rsa_key.pem")),
        ).fetchall()
        query = "SELECT verified, covers_whole_file FROM pdf_signatures(%s)"
        assert connection.execute(query, (str(output),)).fetchall() == [(True, True)]
        original = output.read_bytes()
        assert b"varusik integrity marker" in original
        tampered = tmp_path / "varusik_tampered.pdf"
        tampered.write_bytes(original.replace(b"varusik integrity marker", b"varusuk integrity marker", 1))
        assert connection.execute(query, (str(tampered),)).fetchall() == [(False, True)]
        extended = tmp_path / "varusik_extended.pdf"
        extended.write_bytes(original + b"\n")
        assert connection.execute(query, (str(extended),)).fetchall() == [(True, False)]


@pytest.mark.parametrize("alias", ["symlink", "hardlink"])
def test_pdf_sign_refuses_input_alias(pdf, own_server, tmp_path, alias):
    signing = ROOT / "resources/tests/pdf/signing"
    source = tmp_path / "varusik_source.pdf"
    original = Path(pdf).read_bytes()
    source.write_bytes(original)
    output = tmp_path / "varusik_alias.pdf"
    if alias == "symlink":
        output.symlink_to(source)
    else:
        output.hardlink_to(source)
    with psycopg.connect(own_server.dsn(), autocommit=True) as connection:
        with pytest.raises(psycopg.Error, match="must differ from every input file"):
            connection.execute(
                "SELECT * FROM pdf_sign(%s, %s, %s, %s)",
                (str(source), str(output), str(signing / "varusik_rsa_cert.pem"),
                 str(signing / "varusik_rsa_key.pem")),
            ).fetchall()
        assert connection.execute("SELECT 42").fetchone() == (42,)
    assert source.read_bytes() == original
