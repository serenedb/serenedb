"""pg-wire COPY ... FROM STDIN / TO STDOUT against SereneDB."""

from __future__ import annotations

import gzip
import io
import json
import os
import shutil
import struct
import threading
import uuid
from pathlib import Path

import psycopg
import pytest
from spec_loader import conn_kwargs


@pytest.fixture()
def conn() -> psycopg.Connection:
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    yield c
    c.close()


@pytest.fixture()
def table_name() -> str:
    return f"copy_probe_{uuid.uuid4().hex[:12]}"


@pytest.fixture()
def shared_path(tmp_path: Path) -> Path:
    shared = os.environ.get("SDB_DRV_SHARED_DIR")
    if not shared:
        yield tmp_path
        return
    path = Path(shared) / f"{tmp_path.name}-{uuid.uuid4().hex[:8]}"
    path.mkdir(parents=True)
    yield path
    shutil.rmtree(path, ignore_errors=True)


def _create_int_varchar(conn: psycopg.Connection, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table}"(x INT, label VARCHAR)')


def _drop(conn: psycopg.Connection, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS public."{table}"')


@pytest.mark.parametrize(
    "fmt,payload",
    [
        ("TEXT", b"1\tone\n2\ttwo\n3\tthree\n"),
        ("CSV",  b"1,one\n2,two\n3,three\n"),
    ],
    ids=["text", "csv"],
)
def test_copy_from_stdin(conn: psycopg.Connection, table_name: str,
                          fmt: str, payload: bytes) -> None:
    _create_int_varchar(conn, table_name)
    try:
        with conn.cursor() as cur:
            with cur.copy(
                f'COPY public."{table_name}" FROM STDIN (FORMAT {fmt})'
            ) as cp:
                cp.write(payload)
            cur.execute(
                f'SELECT count(*), sum(x) FROM public."{table_name}"')
            n, s = cur.fetchone()
            assert n == 3
            assert s == 6
    finally:
        _drop(conn, table_name)


_STREAM_ROWS = 400_000


def _stream_payload(fmt: str, bad_line: int | None = None) -> bytes:
    rows = []
    for i in range(_STREAM_ROWS):
        label = f"row {i} " + "x" * (i % 37)
        if i + 1 == bad_line:
            rows.append('{"x": oops}')
        elif fmt == "csv":
            rows.append(f'{i},"{label}"')
        else:
            rows.append(json.dumps({"x": i, "label": label}))
    if fmt == "json_array":
        return ("[\n" + ",\n".join(rows) + "\n]\n").encode()
    return ("\n".join(rows) + "\n").encode()


@pytest.mark.parametrize(
    "fmt,options",
    [
        ("json", "FORMAT json"),
        ("json_array", "FORMAT json"),
        ("csv", "FORMAT csv, SAMPLE_SIZE 1000"),
    ],
    ids=["json_lines", "json_array", "csv_duckdb_option"],
)
def test_copy_from_stdin_reads_the_stream_in_ranges(conn: psycopg.Connection,
                                                    table_name: str, fmt: str,
                                                    options: str) -> None:
    _create_int_varchar(conn, table_name)
    payload = _stream_payload(fmt)
    try:
        with conn.cursor() as cur:
            with cur.copy(
                f'COPY public."{table_name}" FROM STDIN ({options})'
            ) as cp:
                for begin in range(0, len(payload), 1 << 20):
                    cp.write(payload[begin:begin + (1 << 20)])
            cur.execute(f'SELECT count(*), sum(x), sum(length(label)) '
                        f'FROM public."{table_name}"')
            assert cur.fetchone() == (
                _STREAM_ROWS, _STREAM_ROWS * (_STREAM_ROWS - 1) // 2,
                sum(len(f"row {i} ") + i % 37 for i in range(_STREAM_ROWS)))
    finally:
        _drop(conn, table_name)


@pytest.mark.usefixtures("own_server")
@pytest.mark.parametrize("fmt", ["text", "csv", "json"])
def test_copy_from_stdin_takes_rows_longer_than_its_window_of_batches(
        conn: psycopg.Connection, table_name: str, fmt: str) -> None:
    conn.execute("SET threads = 1")
    _create_int_varchar(conn, table_name)
    size = 5 << 20
    rows = []
    for i in range(3):
        label = chr(ord("a") + i) * size
        if fmt == "json":
            rows.append(json.dumps({"x": i, "label": label}))
        elif fmt == "csv":
            rows.append(f'{i},"{label}"')
        else:
            rows.append(f"{i}\t{label}")
    payload = ("\n".join(rows) + "\n").encode()
    try:
        with conn.cursor() as cur:
            with cur.copy(
                f'COPY public."{table_name}" FROM STDIN (FORMAT {fmt})'
            ) as cp:
                for begin in range(0, len(payload), 1 << 20):
                    cp.write(payload[begin:begin + (1 << 20)])
            cur.execute(f'SELECT count(*), sum(x), sum(length(label)) '
                        f'FROM public."{table_name}"')
            assert cur.fetchone() == (3, 3, 3 * size)
    finally:
        _drop(conn, table_name)


def test_copy_from_stdin_json_error_names_its_line(conn: psycopg.Connection,
                                                   table_name: str) -> None:
    _create_int_varchar(conn, table_name)
    try:
        with conn.cursor() as cur:
            with pytest.raises(psycopg.errors.Error, match="line 300001"):
                with cur.copy(f'COPY public."{table_name}" FROM STDIN '
                              "(FORMAT json)") as cp:
                    cp.write(_stream_payload("json", bad_line=300_001))
    finally:
        _drop(conn, table_name)


@pytest.mark.parametrize(
    "compression", ["none", "uncompressed", "auto", "auto_detect", "gzip"])
def test_copy_from_stdin_json_compression(conn: psycopg.Connection,
                                          table_name: str,
                                          compression: str) -> None:
    payload = b'{"x": 1, "label": "one"}\n{"x": 2, "label": "two"}\n'
    if compression == "gzip":
        payload = gzip.compress(payload)
    _create_int_varchar(conn, table_name)
    try:
        with conn.cursor() as cur:
            with cur.copy(f'COPY public."{table_name}" FROM STDIN '
                          f"(FORMAT json, COMPRESSION {compression})") as cp:
                cp.write(payload)
            cur.execute(f'SELECT x, label FROM public."{table_name}" ORDER BY x')
            assert cur.fetchall() == [(1, "one"), (2, "two")]
    finally:
        _drop(conn, table_name)


@pytest.mark.parametrize(
    "fmt, payload, expected",
    [
        ("text", b"1\tone\r2\ttw\\\ro\r\\.\r",
         [(1, "one"), (2, "tw\ro")]),
        ("text", b"1\tone\r\n2\ttwo\r\n\\.\r\n", [(1, "one"), (2, "two")]),
        ("csv", b'1,"o\rne"\r2,"tw\no"\r3,\r',
         [(1, "o\rne"), (2, "tw\no"), (3, None)]),
    ],
)
def test_copy_from_stdin_reads_carriage_return_lines(
        conn: psycopg.Connection, table_name: str, fmt: str, payload: bytes,
        expected: list) -> None:
    _create_int_varchar(conn, table_name)
    try:
        with conn.cursor() as cur:
            with cur.copy(f'COPY public."{table_name}" FROM STDIN '
                          f"(FORMAT {fmt})") as cp:
                for piece in range(0, len(payload), 3):
                    cp.write(payload[piece:piece + 3])
            cur.execute(f'SELECT x, label FROM public."{table_name}" ORDER BY x')
            assert cur.fetchall() == expected
    finally:
        _drop(conn, table_name)


@pytest.mark.parametrize("encoding,codec", [("utf-8", "utf-8"),
                                            ("latin-1", "latin-1")])
def test_copy_from_stdin_csv_reader_option_and_encoding(
        conn: psycopg.Connection, table_name: str, encoding: str,
        codec: str) -> None:
    _create_int_varchar(conn, table_name)
    try:
        with conn.cursor() as cur:
            with cur.copy(f'COPY public."{table_name}" FROM STDIN '
                          f"(FORMAT csv, HEADER, SAMPLE_SIZE 10, "
                          f"ENCODING '{encoding}')") as cp:
                cp.write("x,label\n1,café\n".encode(codec))
            cur.execute(f'SELECT x, label FROM public."{table_name}"')
            assert cur.fetchall() == [(1, "café")]
    finally:
        _drop(conn, table_name)


def test_copy_from_stdin_format_without_a_reader(conn: psycopg.Connection,
                                                 table_name: str) -> None:
    _create_int_varchar(conn, table_name)
    try:
        with conn.cursor() as cur:
            with pytest.raises(psycopg.Error,
                               match='COPY FROM is not supported for FORMAT "blob"'):
                with cur.copy(f'COPY public."{table_name}" FROM STDIN '
                              "(FORMAT blob)") as cp:
                    cp.write(b"1\tone\n")
            cur.execute(f'SELECT count(*) FROM public."{table_name}"')
            assert cur.fetchone() == (0,)
    finally:
        _drop(conn, table_name)


def test_copy_from_stdin_csv_record_across_messages(conn: psycopg.Connection,
                                                    table_name: str) -> None:
    label = "y" * 200
    _create_int_varchar(conn, table_name)
    try:
        with conn.cursor() as cur:
            with cur.copy(f'COPY public."{table_name}" FROM STDIN '
                          "(FORMAT csv)") as cp:
                cp.write(f'0,"zero"\n1,"{label[:10]}'.encode())
                cp.write(f'{label[10:]}"\n2,"two"\n'.encode())
            cur.execute(f'SELECT x, label FROM public."{table_name}" ORDER BY x')
            assert cur.fetchall() == [(0, "zero"), (1, label), (2, "two")]
    finally:
        _drop(conn, table_name)


def test_copy_from_stdin_end_marker_inside_a_field(conn: psycopg.Connection,
                                                   table_name: str) -> None:
    _create_int_varchar(conn, table_name)
    try:
        with conn.cursor() as cur:
            with pytest.raises(psycopg.errors.BadCopyFileFormat,
                               match="end-of-copy marker is not alone on its line"):
                with cur.copy(f'COPY public."{table_name}" FROM STDIN') as cp:
                    cp.write(b"1\tsi\\.x\n")
    finally:
        _drop(conn, table_name)


_ENCODED_LABELS = {
    "LATIN1": ("latin-1", ["café", "ÿ a\\b"]),
    "UTF16LE": ("utf-16-le", ["é€😀", "a\\b 😀z"]),
    "UTF16BE": ("utf-16-be", ["é€😀", "a\\b 😀z"]),
    "UTF32LE": ("utf-32-le", ["é€😀", "a\\b 😀z"]),
    "UTF32BE": ("utf-32-be", ["é€😀", "a\\b 😀z"]),
}


@pytest.mark.parametrize("chunk", [1, 3, 64 * 1024])
@pytest.mark.parametrize("fmt", ["text", "csv"])
@pytest.mark.parametrize("encoding", list(_ENCODED_LABELS))
def test_copy_from_stdin_decodes_its_encoding(conn: psycopg.Connection,
                                              table_name: str, encoding: str,
                                              fmt: str, chunk: int) -> None:
    codec, labels = _ENCODED_LABELS[encoding]
    if fmt == "text":
        lines = [f"{i}\t{label.replace(chr(92), chr(92) * 2)}\n"
                 for i, label in enumerate(labels)] + [f"{len(labels)}\t\\N\n"]
    else:
        lines = [f'{i},"{label}"\n' for i, label in enumerate(labels)]
        lines.append(f"{len(labels)},\n")
    payload = "".join(lines).encode(codec)
    _create_int_varchar(conn, table_name)
    try:
        with conn.cursor() as cur:
            with cur.copy(f'COPY public."{table_name}" FROM STDIN '
                          f"(FORMAT {fmt}, ENCODING '{encoding}')") as cp:
                for start in range(0, len(payload), chunk):
                    cp.write(payload[start:start + chunk])
            cur.execute(f'SELECT x, label FROM public."{table_name}" ORDER BY x')
            assert cur.fetchall() == [*enumerate(labels), (len(labels), None)]
    finally:
        _drop(conn, table_name)


@pytest.mark.parametrize(
    "encoding,payload,message",
    [
        ("UTF16LE", "1\tone\n".encode("utf-16-le") + b"\x32",
         'invalid byte sequence for encoding "UTF16LE": incomplete character '
         "at the end of the input"),
        ("UTF16BE", "1\tone\n".encode("utf-16-be") + b"\xdc\x00\x00\x0a",
         'invalid byte sequence for encoding "UTF16BE": invalid UTF-16 code unit'),
        ("UTF32LE", "1\tone\n".encode("utf-32-le") + b"\x00\x00\x11\x00",
         'invalid byte sequence for encoding "UTF32LE": invalid UTF-32 code point'),
    ],
    ids=["incomplete", "lone_low_surrogate", "beyond_unicode"])
def test_copy_from_stdin_refuses_bytes_outside_its_encoding(
        conn: psycopg.Connection, table_name: str, encoding: str,
        payload: bytes, message: str) -> None:
    _create_int_varchar(conn, table_name)
    try:
        with pytest.raises(psycopg.Error) as info:
            with conn.cursor() as cur:
                with cur.copy(f'COPY public."{table_name}" FROM STDIN '
                              f"(FORMAT text, ENCODING '{encoding}')") as cp:
                    cp.write(payload)
        assert info.value.diag.message_primary == message
        with conn.cursor() as cur:
            cur.execute(f'SELECT count(*) FROM public."{table_name}"')
            assert cur.fetchone() == (0,)
    finally:
        _drop(conn, table_name)


@pytest.mark.parametrize(
    "fmt,encoding,codec,text",
    [("text", "UTF16BE", "utf-16-be", "é€😀"), ("csv", "LATIN1", "latin-1", "café"),
     ("text", "UTF32LE", "utf-32-le", "a\tb")],
    ids=["text_utf16be", "csv_latin1", "text_utf32le"])
def test_copy_to_stdout_encodes_its_encoding(conn: psycopg.Connection, fmt: str,
                                             encoding: str, codec: str,
                                             text: str) -> None:
    with conn.cursor() as cur:
        with cur.copy(f"COPY (SELECT %s::text AS t, 1 AS n) TO STDOUT "
                      f"(FORMAT {fmt}, ENCODING '{encoding}')", (text,)) as cp:
            data = b"".join(bytes(chunk) for chunk in cp)
    field = text.replace("\t", "\\t") if fmt == "text" else text
    delim = "\t" if fmt == "text" else ","
    assert data == f"{field}{delim}1\n".encode(codec)


def test_copy_stdio_force_column_lists(conn: psycopg.Connection,
                                       table_name: str) -> None:
    with conn.cursor() as cur:
        with cur.copy("COPY (SELECT 1 AS a, 'x' AS b, NULL::text AS c) TO STDOUT "
                      "(FORMAT csv, FORCE_QUOTE (b, c))") as cp:
            assert b"".join(bytes(chunk) for chunk in cp) == b'1,"x",\n'
        with cur.copy("COPY (SELECT 1 AS a, 'x' AS b) TO STDOUT "
                      "(FORMAT csv, FORCE_QUOTE *)") as cp:
            assert b"".join(bytes(chunk) for chunk in cp) == b'"1","x"\n'
    _create_int_varchar(conn, table_name)
    try:
        with conn.cursor() as cur:
            with cur.copy(f'COPY public."{table_name}" FROM STDIN '
                          "(FORMAT csv, FORCE_NOT_NULL (label), FORCE_NULL (x))") as cp:
                cp.write(b'1,\n"",two\n')
            cur.execute(f'SELECT x, label FROM public."{table_name}" ORDER BY label')
            assert cur.fetchall() == [(1, ""), (None, "two")]
    finally:
        _drop(conn, table_name)


def test_copy_to_stdout_takes_an_expression_option(conn: psycopg.Connection,
                                                   shared_path: Path) -> None:
    path = shared_path / "delimited.csv"
    with conn.cursor() as cur:
        cur.execute(f"COPY (SELECT 1 AS a, 2 AS b) TO '{path}' "
                    "(FORMAT csv, DELIMITER chr(124))")
        assert path.read_bytes() == b"1|2\n"
        with cur.copy("COPY (SELECT 1 AS a, 2 AS b) TO STDOUT "
                      "(FORMAT csv, DELIMITER chr(124))") as cp:
            assert b"".join(bytes(chunk) for chunk in cp) == b"1|2\n"


@pytest.mark.parametrize(
    "fmt,duckdb,postgres",
    [
        ("text", b"1\t[1, 2]\ttrue\n", b"1\t{1,2}\tt\n"),
        ("csv", b'1,"[1, 2]",true\n', b'1,"{1,2}",t\n'),
    ],
    ids=["text", "csv"],
)
def test_copy_stdio_rendering(conn: psycopg.Connection, table_name: str,
                              fmt: str, duckdb: bytes, postgres: bytes) -> None:
    query = "SELECT 1 AS id, ARRAY[1, 2] AS l, true AS b"
    with conn.cursor() as cur:
        with cur.copy(f"COPY ({query}) TO STDOUT "
                      f"(FORMAT {fmt}, RENDERING duckdb)") as cp:
            assert b"".join(bytes(chunk) for chunk in cp) == duckdb
        with cur.copy(f"COPY ({query}) TO STDOUT (FORMAT {fmt})") as cp:
            assert b"".join(bytes(chunk) for chunk in cp) == postgres
        cur.execute(f'CREATE TABLE public."{table_name}"'
                    "(id INT, l INT[], b BOOLEAN)")
        try:
            with cur.copy(f'COPY public."{table_name}" FROM STDIN '
                          f"(FORMAT {fmt}, RENDERING duckdb)") as cp:
                cp.write(duckdb)
            cur.execute(f'SELECT id, l, b FROM public."{table_name}"')
            assert cur.fetchall() == [(1, [1, 2], True)]
            with pytest.raises(psycopg.errors.DataError):
                with cur.copy(f'COPY public."{table_name}" FROM STDIN '
                              f"(FORMAT {fmt})") as cp:
                    cp.write(duckdb)
        finally:
            _drop(conn, table_name)


def test_copy_from_stdin_crlf_end_marker(conn: psycopg.Connection,
                                         table_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table_name}"(label VARCHAR)')
    try:
        with conn.cursor() as cur:
            with cur.copy(f'COPY public."{table_name}" FROM STDIN') as cp:
                cp.write(b"\\\\.\r\ntwo\r\n\\.\r\n")
            cur.execute(f'SELECT label FROM public."{table_name}" ORDER BY label')
            assert cur.fetchall() == [("\\.",), ("two",)]
    finally:
        _drop(conn, table_name)


def test_copy_from_stdin_bare_keyword(conn: psycopg.Connection,
                                       table_name: str) -> None:
    _create_int_varchar(conn, table_name)
    try:
        with conn.cursor() as cur:
            with cur.copy(f'COPY public."{table_name}" FROM STDIN') as cp:
                cp.write(b"7\tseven\n8\teight\n")
            cur.execute(f'SELECT count(*), sum(x) FROM public."{table_name}"')
            n, s = cur.fetchone()
            assert n == 2
            assert s == 15
    finally:
        _drop(conn, table_name)


def test_copy_from_stdin_reg_names(conn: psycopg.Connection,
                                   table_name: str) -> None:
    rows = 100000
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table_name}"(r regclass, t regtype)')
    try:
        with conn.cursor() as cur:
            with cur.copy(f'COPY public."{table_name}" FROM STDIN') as cp:
                cp.write(b"pg_class\tinteger\n" * rows)
            cur.execute(
                f'SELECT count(*), min(r::text), max(t::text) '
                f'FROM public."{table_name}"')
            assert cur.fetchone() == (rows, "pg_class", "integer")
    finally:
        _drop(conn, table_name)


def test_copy_binary_round_trip(conn: psycopg.Connection,
                                table_name: str) -> None:
    # COPY ... TO STDOUT (FORMAT BINARY) produces the PostgreSQL binary (PGCOPY)
    # stream, and COPY ... FROM STDIN (FORMAT BINARY) loads it back unchanged --
    # including NULLs. Also exercises the unquoted (FORMAT BINARY) spelling.
    dest = f"{table_name}_dst"
    _create_int_varchar(conn, table_name)
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{dest}"(x INT, label VARCHAR)')
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'INSERT INTO public."{table_name}" VALUES '
                f"(1,'one'),(2,NULL),(3,'three')")
            chunks: list[bytes] = []
            with cur.copy(
                f'COPY public."{table_name}" TO STDOUT (FORMAT BINARY)'
            ) as cp:
                for chunk in cp:
                    chunks.append(bytes(chunk))
            blob = b"".join(chunks)
            assert blob.startswith(b"PGCOPY\n\xff\r\n\x00"), \
                f"expected PGCOPY signature, got {blob[:11]!r}"
            assert blob.endswith(b"\xff\xff"), "expected PGCOPY -1 trailer"

            with cur.copy(
                f'COPY public."{dest}" FROM STDIN (FORMAT BINARY)'
            ) as cp:
                cp.write(blob)
            cur.execute(
                f'SELECT count(*), sum(x), count(label) FROM public."{dest}"')
            n, s, labels = cur.fetchone()
            assert n == 3
            assert s == 6
            assert labels == 2  # the NULL label is preserved
    finally:
        _drop(conn, table_name)
        _drop(conn, dest)


@pytest.mark.parametrize("fmt", ["TEXT", "BINARY"])
def test_copy_to_stdout_rows(conn: psycopg.Connection, table_name: str,
                             fmt: str) -> None:
    width = 600_000
    with conn.cursor() as cur:
        with cur.copy(
            f"COPY (SELECT i, repeat(chr((97 + i)::INTEGER), {width}) AS s "
            f"FROM range(8) t(i) ORDER BY i) TO STDOUT (FORMAT {fmt})"
        ) as cp:
            cp.set_types(["int8", "text"])
            assert list(cp.rows()) == [(i, chr(97 + i) * width)
                                       for i in range(8)]
        with cur.copy(
            f"COPY (SELECT 1::BIGINT AS i WHERE false) TO STDOUT (FORMAT {fmt})"
        ) as cp:
            cp.set_types(["int8"])
            assert list(cp.rows()) == []
        cur.execute(f'CREATE TABLE public."{table_name}" AS '
                    f"SELECT i::BIGINT AS i FROM range(500000) t(i)")
        try:
            for preserve in ("true", "false"):
                cur.execute(
                    f"SET SESSION preserve_insertion_order = {preserve}")
                with cur.copy(
                    f'COPY public."{table_name}" TO STDOUT (FORMAT {fmt})'
                ) as cp:
                    cp.set_types(["int8"])
                    assert sorted(row[0] for row in cp.rows()) == list(
                        range(500000))
        finally:
            _drop(conn, table_name)


# Matches PostgreSQL (verified vs PG 16): text never emits a header; csv emits
# one only with the HEADER option (PG defaults csv HEADER off).
@pytest.mark.parametrize(
    "fmt,header_row,data_rows",
    [
        ("TEXT", None,
         [b"1\tone", b"2\ttwo", b"3\tthree"]),
        ("CSV", None,
         [b"1,one", b"2,two", b"3,three"]),
        ("CSV, HEADER", b"x,label",
         [b"1,one", b"2,two", b"3,three"]),
    ],
    ids=["text", "csv", "csv_header"],
)
def test_copy_to_stdout(conn: psycopg.Connection, table_name: str,
                        fmt: str, header_row: bytes | None,
                        data_rows: list[bytes]) -> None:
    _create_int_varchar(conn, table_name)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'INSERT INTO public."{table_name}" VALUES '
                f"(1,'one'),(2,'two'),(3,'three')")
            chunks: list[bytes] = []
            with cur.copy(
                f'COPY public."{table_name}" TO STDOUT (FORMAT {fmt})'
            ) as cp:
                for chunk in cp:
                    chunks.append(bytes(chunk))
        rows = [r for r in b"".join(chunks).splitlines() if r]
        if header_row is None:
            assert sorted(rows) == sorted(data_rows), \
                f"text format should have no header: {rows!r}"
        else:
            assert rows[0] == header_row, \
                f"expected header {header_row!r}, got {rows[0]!r}"
            assert sorted(rows[1:]) == sorted(data_rows)
    finally:
        _drop(conn, table_name)


@pytest.mark.parametrize(
    "delim,expected",
    [
        (":", rb'12\:30\:45:{"a b","c\\"d"}:-5:x\:y,"z":\N' + b"\n"),
        ('"', rb'12:30:45"{\"a b\",\"c\\\"d\"}"-5"x:y,\"z\""\N' + b"\n"),
        (",", rb'12:30:45,{"a b"\,"c\\"d"},-5,x:y\,"z",\N' + b"\n"),
    ],
    ids=["colon", "quote", "comma"],
)
def test_copy_to_stdout_text_delimiter(conn: psycopg.Connection,
                                       table_name: str, delim: str,
                                       expected: bytes) -> None:
    with conn.cursor() as cur:
        with cur.copy(
            "COPY (SELECT TIME '12:30:45' AS t, ARRAY['a b', 'c\"d'] AS a, "
            "-5 AS i, 'x:y,\"z\"' AS s, NULL::INT AS n) "
            f"TO STDOUT (FORMAT text, DELIMITER '{delim}')"
        ) as cp:
            data = b"".join(bytes(chunk) for chunk in cp)
        assert data == expected
        cur.execute(f'CREATE TABLE public."{table_name}"'
                    "(t TIME, a TEXT[], i INT, s TEXT, n INT)")
        try:
            with cur.copy(f'COPY public."{table_name}" FROM STDIN '
                          f"(FORMAT text, DELIMITER '{delim}')") as cp:
                cp.write(data)
            cur.execute(f'SELECT t::TEXT, a, i, s, n FROM public."{table_name}"')
            assert cur.fetchall() == [("12:30:45", ["a b", 'c"d'], -5,
                                       'x:y,"z"', None)]
        finally:
            _drop(conn, table_name)


def test_copy_to_stdout_parquet_is_binary_blob(conn: psycopg.Connection,
                                                 table_name: str) -> None:
    _create_int_varchar(conn, table_name)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'INSERT INTO public."{table_name}" VALUES (1,\'one\'),(2,\'two\')')
            buf = io.BytesIO()
            with cur.copy(
                f'COPY public."{table_name}" TO STDOUT (FORMAT PARQUET)'
            ) as cp:
                for chunk in cp:
                    buf.write(chunk)
        data = buf.getvalue()
        assert data.startswith(b"PAR1"), \
            f"expected Parquet magic, got first 8 bytes={data[:8]!r}"
        assert data.endswith(b"PAR1"), \
            f"expected Parquet magic at EOF, got last 8 bytes={data[-8:]!r}"
    finally:
        _drop(conn, table_name)


def test_copy_arrows_round_trip(conn: psycopg.Connection,
                                table_name: str) -> None:
    dest = f"{table_name}_dst"
    _create_int_varchar(conn, table_name)
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{dest}"(x INT, label VARCHAR)')
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'INSERT INTO public."{table_name}" SELECT i, CASE WHEN i % 7 = 0 '
                f"THEN NULL ELSE 'label ' || i END FROM generate_series(1, 5000) i")
            blob = b""
            with cur.copy(
                f'COPY public."{table_name}" TO STDOUT (FORMAT arrows)'
            ) as cp:
                for chunk in cp:
                    blob += bytes(chunk)
            assert blob.startswith(b"\xff\xff\xff\xff"), blob[:8]
            with cur.copy(
                f'COPY public."{dest}" FROM STDIN (FORMAT arrows)'
            ) as cp:
                for i in range(0, len(blob), 4096):
                    cp.write(blob[i:i + 4096])
            cur.execute(
                f'SELECT count(*) FROM (SELECT * FROM public."{table_name}" '
                f'EXCEPT ALL SELECT * FROM public."{dest}")')
            assert cur.fetchone() == (0,)
            cur.execute(f'SELECT count(*), count(label) FROM public."{dest}"')
            assert cur.fetchone() == (5000, 5000 - 5000 // 7)
    finally:
        _drop(conn, table_name)
        _drop(conn, dest)


@pytest.mark.parametrize("codec", ["null", "deflate", "zstandard"])
def test_copy_avro_round_trip(conn: psycopg.Connection, table_name: str,
                              codec: str) -> None:
    dest = f"{table_name}_dst"
    _create_int_varchar(conn, table_name)
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{dest}"(x INT, label VARCHAR)')
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'INSERT INTO public."{table_name}" SELECT i, CASE WHEN i % 7 = 0 '
                f"THEN NULL ELSE 'label ' || i END FROM generate_series(1, 20000) i")
            blob = b""
            with cur.copy(
                f'COPY public."{table_name}" TO STDOUT '
                f"(FORMAT avro, CODEC '{codec}')"
            ) as cp:
                assert cur.pgresult.binary_tuples == psycopg.pq.Format.BINARY
                for chunk in cp:
                    blob += bytes(chunk)
            assert blob.startswith(b"Obj\x01"), blob[:8]
            with cur.copy(
                f'COPY public."{dest}" FROM STDIN (FORMAT avro)'
            ) as cp:
                assert cur.pgresult.binary_tuples == psycopg.pq.Format.BINARY
                for i in range(0, len(blob), 4096):
                    cp.write(blob[i:i + 4096])
            cur.execute(
                f'SELECT count(*) FROM (SELECT * FROM public."{table_name}" '
                f'EXCEPT ALL SELECT * FROM public."{dest}")')
            assert cur.fetchone() == (0,)
            cur.execute(f'SELECT count(*), count(label) FROM public."{dest}"')
            assert cur.fetchone() == (20000, 20000 - 20000 // 7)
    finally:
        _drop(conn, table_name)
        _drop(conn, dest)


def test_copy_to_stdout_arrow_file_is_binary_blob(conn: psycopg.Connection,
                                                   shared_path: Path) -> None:
    with conn.cursor() as cur:
        blob = b""
        with cur.copy(
            "COPY (SELECT i AS x, 'v' || i AS label FROM generate_series(1, 100) i) "
            "TO STDOUT (FORMAT arrow)"
        ) as cp:
            for chunk in cp:
                blob += bytes(chunk)
        assert blob.startswith(b"ARROW1"), blob[:8]
        assert blob.endswith(b"ARROW1"), blob[-8:]
        path = shared_path / "stdout.arrow"
        path.write_bytes(blob)
        cur.execute(f"SELECT count(*), sum(x) FROM read_arrow('{path}')")
        assert cur.fetchone() == (100, 5050)


def test_copy_from_stdin_arrow_file_is_refused(conn: psycopg.Connection,
                                               table_name: str) -> None:
    _create_int_varchar(conn, table_name)
    try:
        with pytest.raises(psycopg.errors.FeatureNotSupported,
                           match="use FORMAT arrows"):
            with conn.cursor() as cur:
                with cur.copy(
                    f'COPY public."{table_name}" FROM STDIN (FORMAT arrow)'
                ) as cp:
                    cp.write(b"ARROW1")
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)
    finally:
        _drop(conn, table_name)


def test_copy_to_stdout_json_lines(conn: psycopg.Connection,
                                   table_name: str) -> None:
    # COPY ... TO STDOUT (FORMAT JSON) emits one JSON object per row (newline
    # delimited); NULL becomes JSON null.
    _create_int_varchar(conn, table_name)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'INSERT INTO public."{table_name}" VALUES (1,\'one\'),(2,NULL)')
            out = b""
            with cur.copy(
                f'COPY public."{table_name}" TO STDOUT (FORMAT JSON)'
            ) as cp:
                for chunk in cp:
                    out += bytes(chunk)
        rows = [json.loads(r) for r in out.splitlines() if r]
        assert rows == [{"x": 1, "label": "one"}, {"x": 2, "label": None}], rows
    finally:
        _drop(conn, table_name)


def test_copy_json_round_trip(conn: psycopg.Connection,
                              table_name: str) -> None:
    # JSON TO STDOUT -> FROM STDIN preserves rows incl. NULL and a value with a
    # double-quote (JSON string-escaped).
    dest = f"{table_name}_dst"
    _create_int_varchar(conn, table_name)
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{dest}"(x INT, label VARCHAR)')
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'INSERT INTO public."{table_name}" VALUES '
                f"(1,'one'),(2,NULL),(3,'a\"b')")
            blob = b""
            with cur.copy(
                f'COPY public."{table_name}" TO STDOUT (FORMAT JSON)'
            ) as cp:
                for chunk in cp:
                    blob += bytes(chunk)
            with cur.copy(
                f'COPY public."{dest}" FROM STDIN (FORMAT JSON)'
            ) as cp:
                cp.write(blob)
            cur.execute(f'SELECT count(*), count(label) FROM public."{dest}"')
            n, labels = cur.fetchone()
            assert n == 3 and labels == 2  # NULL preserved
    finally:
        _drop(conn, table_name)
        _drop(conn, dest)


@pytest.mark.parametrize("header_opt,expect_header", [
    ("HEADER true",  True),
    ("HEADER false", False),
])
def test_copy_to_stdout_csv_header_flag(conn: psycopg.Connection,
                                          table_name: str,
                                          header_opt: str,
                                          expect_header: bool) -> None:
    _create_int_varchar(conn, table_name)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'INSERT INTO public."{table_name}" VALUES '
                f"(1,'one'),(2,'two')")
            out = b""
            with cur.copy(
                f'COPY public."{table_name}" '
                f'TO STDOUT (FORMAT CSV, {header_opt})'
            ) as cp:
                for chunk in cp:
                    out += bytes(chunk)
        rows = [r for r in out.splitlines() if r]
        data_rows = sorted([b"1,one", b"2,two"])
        if expect_header:
            assert rows[0] == b"x,label", \
                f"expected header row, got {rows[0]!r}"
            assert sorted(rows[1:]) == data_rows
        else:
            assert sorted(rows) == data_rows, \
                f"unexpected rows: {rows!r}"
    finally:
        _drop(conn, table_name)


def test_copy_to_stdout_csv_quotes_special_fields(conn: psycopg.Connection,
                                                   table_name: str) -> None:
    _create_int_varchar(conn, table_name)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'INSERT INTO public."{table_name}" VALUES '
                "(1, 'a,b'), (2, 'x\"y'), (3, E'line\\nbreak'), (4, ''), "
                "(5, NULL), (6, 'plain')")
            out = b""
            with cur.copy(
                f'COPY (SELECT * FROM public."{table_name}" ORDER BY x) '
                'TO STDOUT (FORMAT CSV)'
            ) as cp:
                for chunk in cp:
                    out += bytes(chunk)
        assert out == (b'1,"a,b"\n2,"x""y"\n3,"line\nbreak"\n4,""\n5,\n'
                       b'6,plain\n'), f"unexpected output: {out!r}"
    finally:
        _drop(conn, table_name)


@pytest.mark.parametrize("header_opt", ["HEADER true", "HEADER false"])
def test_copy_from_stdin_csv_header_flag(conn: psycopg.Connection,
                                           table_name: str,
                                           header_opt: str) -> None:
    _create_int_varchar(conn, table_name)
    try:
        with conn.cursor() as cur:
            payload = b"x,label\n1,one\n2,two\n" if "true" in header_opt \
                else b"1,one\n2,two\n"
            with cur.copy(
                f'COPY public."{table_name}" '
                f'FROM STDIN (FORMAT CSV, {header_opt})'
            ) as cp:
                cp.write(payload)
            cur.execute(
                f'SELECT count(*), sum(x) FROM public."{table_name}"')
            n, s = cur.fetchone()
            assert n == 2 and s == 3, \
                f"expected 2 rows sum 3, got n={n} s={s}"
    finally:
        _drop(conn, table_name)


def test_copy_to_stdout_bare_keyword(conn: psycopg.Connection,
                                      table_name: str) -> None:
    _create_int_varchar(conn, table_name)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'INSERT INTO public."{table_name}" VALUES (10,\'ten\')')
            out = b""
            with cur.copy(f'COPY public."{table_name}" TO STDOUT') as cp:
                for chunk in cp:
                    out += bytes(chunk)
        rows = [r for r in out.splitlines() if r]
        # default format is PG text: one tab-delimited row, no header
        assert rows == [b"10\tten"], f"expected text row, got {out!r}"
    finally:
        _drop(conn, table_name)


def test_copy_does_not_corrupt_session(conn: psycopg.Connection,
                                        table_name: str) -> None:
    _create_int_varchar(conn, table_name)
    try:
        with conn.cursor() as cur:
            with cur.copy(
                f'COPY public."{table_name}" FROM STDIN (FORMAT CSV)'
            ) as cp:
                cp.write(b"42,answer\n")
            # Issue a regular query right after; the protocol state
            # must be back to ReadyForQuery.
            cur.execute(f'SELECT label FROM public."{table_name}"')
            assert cur.fetchone() == ("answer",)
            cur.execute("SELECT 1+1")
            assert cur.fetchone() == (2,)
    finally:
        _drop(conn, table_name)


def test_copy_from_stdin_parquet_does_not_crash(conn: psycopg.Connection,
                                                table_name: str) -> None:
    # COPY FROM STDIN (FORMAT parquet) cannot work (parquet needs a seekable
    # file, not a pipe). It must error cleanly, NOT crash the server.
    _create_int_varchar(conn, table_name)
    try:
        with conn.cursor() as cur:
            blob = b""
            with cur.copy("COPY (SELECT 1 x, 'a' label) TO STDOUT (FORMAT parquet)") as cp:
                for chunk in cp:
                    blob += bytes(chunk)
        with pytest.raises(psycopg.Error):
            with conn.cursor() as cur:
                with cur.copy(
                    f'COPY public."{table_name}" FROM STDIN (FORMAT parquet)'
                ) as cp:
                    cp.write(blob)
        # the server must still be serving (fresh connection)
        with psycopg.connect(**conn_kwargs(), autocommit=True) as c2:
            with c2.cursor() as cur:
                cur.execute("SELECT 1")
                assert cur.fetchone() == (1,)
    finally:
        _drop(conn, table_name)


_ROWS = 300_000
_STORAGES = [""]
_SOURCES = ["stdin", "file"]


def _text_rows(count: int, first: int = 0) -> bytes:
    return b"".join(
        b"%d\tname\\t%d\n" % (i, i) for i in range(first, first + count))


def _copy_from(cur, table: str, payload: bytes, options: str, source: str,
               shared_path: Path) -> None:
    if source == "file":
        path = shared_path / "copy.dat"
        path.write_bytes(payload)
        cur.execute(f"COPY public.\"{table}\" FROM '{path}' ({options})")
        return
    with cur.copy(f'COPY public."{table}" FROM STDIN ({options})') as cp:
        for start in range(0, len(payload), 64 * 1024):
            cp.write(payload[start:start + 64 * 1024])


def _count(cur, table: str, storage: str, where: str = "") -> int:
    cur.execute(f'SELECT count(*) FROM public."{table}" {where}')
    return cur.fetchone()[0]


@pytest.mark.parametrize("source", _SOURCES)
@pytest.mark.parametrize("storage", _STORAGES, ids=["table"])
def test_large_text_copy_keeps_every_row(conn: psycopg.Connection,
                                         table_name: str, storage: str,
                                         source: str, shared_path: Path) -> None:
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table_name}"(x INT, label VARCHAR)'
                    f" {storage}")
        try:
            _copy_from(cur, table_name, _text_rows(_ROWS), "FORMAT TEXT",
                       source, shared_path)
            assert _count(cur, table_name, storage) == _ROWS
            cur.execute(f'SELECT sum(x), count(DISTINCT x) FROM '
                        f'public."{table_name}"')
            assert cur.fetchone() == (_ROWS * (_ROWS - 1) // 2, _ROWS)
            assert _count(cur, table_name, storage,
                          "WHERE label = 'name' || chr(9) || x::VARCHAR"
                          ) == _ROWS
        finally:
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}"')


@pytest.mark.parametrize("source", _SOURCES)
@pytest.mark.parametrize("storage", _STORAGES, ids=["table"])
def test_large_text_copy_header_and_last_row_without_newline(
        conn: psycopg.Connection, table_name: str, storage: str, source: str,
        shared_path: Path) -> None:
    payload = b"x\tlabel\n" + _text_rows(_ROWS) + b"%d\tlast" % _ROWS
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table_name}"(x INT, label VARCHAR)'
                    f" {storage}")
        try:
            _copy_from(cur, table_name, payload, "FORMAT TEXT, HEADER", source,
                       shared_path)
            assert _count(cur, table_name, storage) == _ROWS + 1
            cur.execute(f'SELECT label FROM public."{table_name}" '
                        f"WHERE x = {_ROWS}")
            assert cur.fetchone()[0] == "last"
        finally:
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}"')


@pytest.mark.parametrize("source", _SOURCES)
def test_text_copy_crlf_line_endings(conn: psycopg.Connection,
                                     table_name: str, source: str,
                                     shared_path: Path) -> None:
    payload = b"x\tlabel\r\n0\tzero\r\n1\tone\\r\r\n2\ttwo\\\\\r\n3\tthree"
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table_name}"(x INT, label VARCHAR)')
        try:
            _copy_from(cur, table_name, payload, "FORMAT TEXT, HEADER", source,
                       shared_path)
            cur.execute(f'SELECT x, label FROM public."{table_name}" ORDER BY x')
            assert cur.fetchall() == [(0, "zero"), (1, "one\r"), (2, "two\\"),
                                      (3, "three")]
        finally:
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}"')


@pytest.mark.parametrize("source", _SOURCES)
def test_text_copy_escaped_newlines(conn: psycopg.Connection,
                                    table_name: str, source: str,
                                    shared_path: Path) -> None:
    payload = b"0\tline\\\nbreak\n1\tslash\\\\\n2\t\\\\\\\nx\n3\tlast"
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table_name}"(x INT, label VARCHAR)')
        try:
            _copy_from(cur, table_name, payload, "FORMAT TEXT", source,
                       shared_path)
            cur.execute(f'SELECT x, label FROM public."{table_name}" ORDER BY x')
            assert cur.fetchall() == [(0, "line\nbreak"), (1, "slash\\"),
                                      (2, "\\\nx"), (3, "last")]
        finally:
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}"')


def _escaped_boundary_payload() -> tuple[bytes, list[str]]:
    rows = [b"0\t" + b"y" * (_RANGE - 4) + b"\\\ntail\n",
            b"1\t" + b"y" * (_RANGE - 10) + b"\\\\\n",
            b"2\t" + b"y" * (_RANGE - 6) + b"\\\\\\\nz\n"]
    labels = ["y" * (_RANGE - 4) + "\ntail", "y" * (_RANGE - 10) + "\\",
              "y" * (_RANGE - 6) + "\\\nz"]
    for x in range(3, 8):
        rows.append(b"%d\tz\n" % x)
        labels.append("z")
    return b"".join(rows), labels


@pytest.mark.parametrize("source", _SOURCES)
def test_text_copy_escaped_newlines_on_range_boundaries(
        conn: psycopg.Connection, table_name: str, source: str,
        shared_path: Path) -> None:
    payload, labels = _escaped_boundary_payload()
    assert payload[_RANGE - 2:_RANGE] == b"\\\n"
    assert payload[2 * _RANGE - 3:2 * _RANGE] == b"\\\\\n"
    assert payload[3 * _RANGE - 4:3 * _RANGE] == b"\\\\\\\n"
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table_name}"(x INT, label VARCHAR)')
        try:
            _copy_from(cur, table_name, payload, "FORMAT TEXT", source,
                       shared_path)
            cur.execute(f'SELECT x, length(label), right(label, 6) FROM '
                        f'public."{table_name}" ORDER BY x')
            assert cur.fetchall() == [(x, len(label), label[-6:])
                                      for x, label in enumerate(labels)]
        finally:
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}"')


def _label_length(i: int) -> int:
    if i == 1:
        return 17_000_000
    if i == 1_500:
        return 9_000_000
    return 100_000 if i % 7 == 0 else i % 50


@pytest.mark.parametrize("source", _SOURCES)
@pytest.mark.parametrize("storage", _STORAGES, ids=["table"])
def test_large_text_copy_rows_longer_than_a_read(conn: psycopg.Connection,
                                                 table_name: str,
                                                 storage: str, source: str,
                                                 shared_path: Path) -> None:
    rows = 3_000
    payload = b"".join(
        b"%d\t%s\n" % (i, b"y" * _label_length(i)) for i in range(rows))
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table_name}"(x INT, label VARCHAR)'
                    f" {storage}")
        try:
            _copy_from(cur, table_name, payload, "FORMAT TEXT", source,
                       shared_path)
            assert _count(cur, table_name, storage) == rows
            cur.execute(f'SELECT sum(x), sum(length(label)) FROM '
                        f'public."{table_name}"')
            assert cur.fetchone() == (rows * (rows - 1) // 2,
                                      sum(map(_label_length, range(rows))))
        finally:
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}"')


@pytest.mark.parametrize("source", _SOURCES)
@pytest.mark.parametrize("storage", _STORAGES, ids=["table"])
def test_large_text_copy_bad_row_inserts_nothing(conn: psycopg.Connection,
                                                 table_name: str, storage: str,
                                                 source: str,
                                                 shared_path: Path) -> None:
    payload = (_text_rows(200_000) + b"oops\n" +
               _text_rows(_ROWS - 200_000, 200_000))
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table_name}"(x INT, label VARCHAR)'
                    f" {storage}")
        try:
            with pytest.raises(psycopg.Error):
                _copy_from(cur, table_name, payload, "FORMAT TEXT", source,
                           shared_path)
            assert _count(cur, table_name, storage) == 0
        finally:
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}"')


def _binary_rows(cur, count: int) -> bytes:
    with cur.copy(f"COPY (SELECT i::INT AS x, 'name' || i::VARCHAR AS label "
                  f"FROM range({count}) t(i)) TO STDOUT (FORMAT BINARY)") as cp:
        return b"".join(bytes(part) for part in cp)


@pytest.mark.parametrize("source", _SOURCES)
@pytest.mark.parametrize("storage", _STORAGES, ids=["table"])
def test_large_binary_copy_keeps_every_row(conn: psycopg.Connection,
                                           table_name: str, storage: str,
                                           source: str,
                                           shared_path: Path) -> None:
    with conn.cursor() as cur:
        payload = _binary_rows(cur, _ROWS)
        cur.execute(f'CREATE TABLE public."{table_name}"(x INT, label VARCHAR)'
                    f" {storage}")
        try:
            _copy_from(cur, table_name, payload, "FORMAT BINARY", source,
                       shared_path)
            assert _count(cur, table_name, storage) == _ROWS
            cur.execute(f'SELECT sum(x), count(DISTINCT label) FROM '
                        f'public."{table_name}"')
            assert cur.fetchone() == (_ROWS * (_ROWS - 1) // 2, _ROWS)
            assert _count(cur, table_name, storage,
                          "WHERE label = 'name' || x::VARCHAR") == _ROWS
        finally:
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}"')


def _bytea_value(i: int) -> bytes:
    length = i % 100 + 1
    if i % 3 == 1:
        return bytes(97 + (i + k) % 26 for k in range(length))
    return bytes((i + k) % 256 for k in range(length))


def _bytea_text_field(i: int, value: bytes) -> bytes:
    if i % 3 == 0:
        return b"\\\\x" + value.hex().encode()
    if i % 3 == 1:
        return value
    return b"".join(b"\\\\%03o" % byte for byte in value)


def _bytea_payload(fmt: str, values: list[bytes]) -> bytes:
    if fmt == "TEXT":
        return b"".join(b"%d\t%s\n" % (i, _bytea_text_field(i, value))
                        for i, value in enumerate(values))
    rows = b"".join(struct.pack("!hiii", 2, 4, i, len(value)) + value
                    for i, value in enumerate(values))
    return (b"PGCOPY\n\xff\r\n\x00" + struct.pack("!ii", 0, 0) + rows +
            struct.pack("!h", -1))


@pytest.mark.parametrize("fmt", ["TEXT", "BINARY"])
@pytest.mark.parametrize("source", _SOURCES)
@pytest.mark.parametrize("storage", _STORAGES, ids=["table"])
def test_large_bytea_copy_keeps_every_value(conn: psycopg.Connection,
                                            table_name: str, storage: str,
                                            source: str, fmt: str,
                                            shared_path: Path) -> None:
    values = [_bytea_value(i) for i in range(100_000)]
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table_name}"(x INT, b BYTEA)'
                    f" {storage}")
        try:
            _copy_from(cur, table_name, _bytea_payload(fmt, values),
                       f"FORMAT {fmt}", source, shared_path)
            assert _count(cur, table_name, storage) == len(values)
            cur.execute(f'SELECT x, b FROM public."{table_name}" ORDER BY x')
            wrong = [x for x, b in cur if b != values[x]]
            assert not wrong, wrong[:5]
        finally:
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}"')


@pytest.mark.parametrize("fmt", ["TEXT", "BINARY"])
@pytest.mark.parametrize("source", _SOURCES)
def test_bytea_copy_values_longer_than_a_batch(conn: psycopg.Connection,
                                              table_name: str, source: str,
                                              fmt: str,
                                              shared_path: Path) -> None:
    values = [bytes(range(256)) * 1024, b"y" * 200_000,
              bytes(range(256)) * 512]
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table_name}"(x INT, b BYTEA)')
        try:
            _copy_from(cur, table_name, _bytea_payload(fmt, values),
                       f"FORMAT {fmt}", source, shared_path)
            cur.execute(f'SELECT x, b FROM public."{table_name}" ORDER BY x')
            assert [(x, len(b), b == values[x]) for x, b in cur] == [
                (x, len(value), True) for x, value in enumerate(values)]
        finally:
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}"')


def _cut_inside_row(payload: bytes, at: int) -> bytes:
    pos = 19
    while True:
        end = pos + 2
        for _ in range(struct.unpack_from("!h", payload, pos)[0]):
            length = struct.unpack_from("!i", payload, end)[0]
            end += 4 + max(length, 0)
        if end > at:
            return payload[:pos + 1]
        pos = end


@pytest.mark.parametrize("source", _SOURCES)
@pytest.mark.parametrize("storage", _STORAGES, ids=["table"])
@pytest.mark.parametrize("damage", ["after_trailer", "truncated"])
def test_large_binary_copy_damaged_stream_inserts_nothing(
        conn: psycopg.Connection, table_name: str, storage: str,
        damage: str, source: str, shared_path: Path) -> None:
    with conn.cursor() as cur:
        payload = _binary_rows(cur, _ROWS)
        payload = (payload + b"\x00\x01" if damage == "after_trailer"
                   else _cut_inside_row(payload, len(payload) * 2 // 3))
        cur.execute(f'CREATE TABLE public."{table_name}"(x INT, label VARCHAR)'
                    f" {storage}")
        try:
            with pytest.raises(psycopg.Error):
                _copy_from(cur, table_name, payload, "FORMAT BINARY", source,
                           shared_path)
            assert _count(cur, table_name, storage) == 0
        finally:
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}"')


_PGCOPY_HEADER = b"PGCOPY\n\xff\r\n\x00" + struct.pack("!ii", 0, 0)
_PGCOPY_ROW = struct.pack("!hii", 1, 4, 7)
_PGCOPY_EXTENDED = (b"PGCOPY\n\xff\r\n\x00" + struct.pack("!ii", 0, 8) +
                    b"abcdefgh" + _PGCOPY_ROW + b"\xff\xff")


@pytest.mark.parametrize("chunks, loaded", [
    ([_PGCOPY_HEADER], 0),
    ([_PGCOPY_HEADER, b"\xff\xff"], 0),
    ([_PGCOPY_HEADER, _PGCOPY_ROW, b"\xff\xff"], 1),
    ([bytes([byte]) for byte in _PGCOPY_EXTENDED], 1),
], ids=["header_only", "header_and_trailer", "rows_apart",
        "extension_byte_by_byte"])
def test_binary_copy_from_stdin_takes_any_split_of_the_stream(
        conn: psycopg.Connection, table_name: str, chunks: list[bytes],
        loaded: int) -> None:
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table_name}"(a INT)')
        try:
            with cur.copy(
                    f'COPY public."{table_name}" FROM STDIN (FORMAT BINARY)'
            ) as cp:
                for chunk in chunks:
                    cp.write(chunk)
            cur.execute(f'SELECT count(*) FROM public."{table_name}"')
            assert cur.fetchone()[0] == loaded
        finally:
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}"')


@pytest.mark.parametrize("source", _SOURCES)
@pytest.mark.parametrize("payload", [b"", _PGCOPY_HEADER[:5]],
                         ids=["no_bytes", "cut_signature"])
def test_binary_copy_without_a_whole_signature_is_an_error(
        conn: psycopg.Connection, table_name: str, payload: bytes,
        source: str, shared_path: Path) -> None:
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table_name}"(a INT)')
        try:
            with pytest.raises(psycopg.errors.BadCopyFileFormat):
                if source == "file":
                    path = shared_path / "copy.dat"
                    path.write_bytes(payload)
                    cur.execute(f"COPY public.\"{table_name}\" FROM '{path}' "
                                f"(FORMAT BINARY)")
                else:
                    with cur.copy(f'COPY public."{table_name}" FROM STDIN '
                                  f'(FORMAT BINARY)') as cp:
                        cp.write(payload)
        finally:
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}"')


@pytest.mark.parametrize("source", _SOURCES)
@pytest.mark.parametrize("payload", [b"1\t\xc3\\\xa9\n", b"1\xc3\\\xa9\tx\n"],
                         ids=["text_column", "integer_column"])
def test_text_copy_refuses_bytes_unescaping_would_join_into_utf8(
        conn: psycopg.Connection, table_name: str, payload: bytes,
        source: str, shared_path: Path) -> None:
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table_name}"(a INT, b TEXT)')
        try:
            with pytest.raises(psycopg.errors.CharacterNotInRepertoire,
                               match="0xc3 0x5c"):
                _copy_from(cur, table_name, payload, "FORMAT TEXT", source,
                           shared_path)
            cur.execute(f'SELECT count(*) FROM public."{table_name}"')
            assert cur.fetchone()[0] == 0
        finally:
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}"')


_RANGE = 8 << 20


def _boundary_payload(header: bool) -> tuple[bytes, list[int]]:
    head = b"x\tlabel\n" if header else b""
    rows = []
    lengths = []
    pos = len(head)
    for x, row_sep in enumerate((_RANGE - 1, 2 * _RANGE, 3 * _RANGE + 1)):
        prefix = b"%d\t" % x
        length = row_sep - pos - len(prefix)
        rows.append(prefix + b"y" * length + b"\n")
        lengths.append(length)
        pos = row_sep + 1
    for x in range(3, 8):
        rows.append(b"%d\tz\n" % x)
        lengths.append(1)
    return head + b"".join(rows), lengths


def _assert_lengths(cur, table: str, storage: str,
                    lengths: list[int]) -> None:
    assert _count(cur, table, storage) == len(lengths)
    cur.execute(f'SELECT x, length(label) FROM public."{table}" ORDER BY x')
    assert cur.fetchall() == list(enumerate(lengths))


@pytest.mark.parametrize("header", [False, True], ids=["plain", "header"])
@pytest.mark.parametrize("storage", _STORAGES, ids=["table"])
def test_text_file_rows_on_range_boundaries(conn: psycopg.Connection,
                                            table_name: str, storage: str,
                                            header: bool,
                                            shared_path: Path) -> None:
    payload, lengths = _boundary_payload(header)
    options = "FORMAT TEXT, HEADER" if header else "FORMAT TEXT"
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table_name}"(x INT, label VARCHAR)'
                    f" {storage}")
        try:
            _copy_from(cur, table_name, payload, options, "file", shared_path)
            _assert_lengths(cur, table_name, storage, lengths)
        finally:
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}"')


@pytest.mark.parametrize("storage", _STORAGES, ids=["table"])
def test_text_copy_from_named_pipe(conn: psycopg.Connection, table_name: str,
                                   storage: str, shared_path: Path) -> None:
    pipe = shared_path / "rows.pipe"
    os.mkfifo(pipe)
    writer = threading.Thread(target=pipe.write_bytes,
                              args=(b"x\tlabel\n" + _text_rows(_ROWS),),
                              daemon=True)
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table_name}"(x INT, label VARCHAR)'
                    f" {storage}")
        try:
            writer.start()
            cur.execute(f"COPY public.\"{table_name}\" FROM '{pipe}' "
                        "(FORMAT TEXT, HEADER)")
            writer.join(timeout=60)
            assert _count(cur, table_name, storage) == _ROWS
        finally:
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}"')


@pytest.mark.parametrize("setting", ["async_threads", "read_ahead_depth"])
@pytest.mark.parametrize("fmt", ["text", "binary"])
def test_file_copy_without_read_ahead(conn: psycopg.Connection,
                                      table_name: str, setting: str, fmt: str,
                                      shared_path: Path) -> None:
    with conn.cursor() as cur:
        cur.execute(f"SET {setting} = 0")
        cur.execute(f'CREATE TABLE public."{table_name}"(x INT, label VARCHAR)')
        try:
            if fmt == "text":
                payload, lengths = _boundary_payload(False)
                _copy_from(cur, table_name, payload, "FORMAT TEXT", "file",
                           shared_path)
                _assert_lengths(cur, table_name, "", lengths)
            else:
                _copy_from(cur, table_name, _binary_rows(cur, _ROWS),
                           "FORMAT BINARY", "file", shared_path)
                assert _count(cur, table_name, "") == _ROWS
        finally:
            cur.execute(f"RESET {setting}")
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}"')


@pytest.mark.parametrize(
    "payload,options",
    [(b"", "FORMAT TEXT"), (b"x\tlabel\n", "FORMAT TEXT, HEADER"),
     (b"x\tlabel", "FORMAT TEXT, HEADER")],
    ids=["empty", "header_only", "header_without_newline"])
def test_text_file_copy_without_rows(conn: psycopg.Connection,
                                     table_name: str, payload: bytes,
                                     options: str, shared_path: Path) -> None:
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table_name}"(x INT, label VARCHAR)')
        try:
            _copy_from(cur, table_name, payload, options, "file", shared_path)
            assert _count(cur, table_name, "") == 0
        finally:
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}"')
