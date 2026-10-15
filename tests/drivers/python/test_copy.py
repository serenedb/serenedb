"""pg-wire COPY ... FROM STDIN / TO STDOUT against SereneDB."""

from __future__ import annotations

import io
import json
import os
import shutil
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
                f'SELECT count(*), sum(x) FROM public."{table_name}"'
            )
            n, s = cur.fetchone()
            assert n == 3
            assert s == 6
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
                f"(1,'one'),(2,NULL),(3,'three')"
            )
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
                f'SELECT count(*), sum(x), count(label) FROM public."{dest}"'
            )
            n, s, labels = cur.fetchone()
            assert n == 3
            assert s == 6
            assert labels == 2  # the NULL label is preserved
    finally:
        _drop(conn, table_name)
        _drop(conn, dest)


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
                f"(1,'one'),(2,'two'),(3,'three')"
            )
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


def test_copy_to_stdout_parquet_is_binary_blob(conn: psycopg.Connection,
                                                 table_name: str) -> None:
    _create_int_varchar(conn, table_name)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'INSERT INTO public."{table_name}" VALUES (1,\'one\'),(2,\'two\')'
            )
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


def test_copy_to_stdout_json_lines(conn: psycopg.Connection,
                                   table_name: str) -> None:
    # COPY ... TO STDOUT (FORMAT JSON) emits one JSON object per row (newline
    # delimited); NULL becomes JSON null.
    _create_int_varchar(conn, table_name)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'INSERT INTO public."{table_name}" VALUES (1,\'one\'),(2,NULL)'
            )
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
                f"(1,'one'),(2,NULL),(3,'a\"b')"
            )
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
                f"(1,'one'),(2,'two')"
            )
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
                f'SELECT count(*), sum(x) FROM public."{table_name}"'
            )
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
                f'INSERT INTO public."{table_name}" VALUES (10,\'ten\')'
            )
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
_STORAGES = ["", "WITH (storage = 'search')"]
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
    if storage:
        cur.execute(f'VACUUM (REFRESH_TABLE) public."{table}"')
    cur.execute(f'SELECT count(*) FROM public."{table}" {where}')
    return cur.fetchone()[0]


def _assert_insertion_order(cur, table: str, storage: str, rows: int) -> None:
    if not storage:
        cur.execute(f'SELECT x FROM public."{table}"')
        assert [row[0] for row in cur] == list(range(rows))


@pytest.mark.parametrize("source", _SOURCES)
@pytest.mark.parametrize("storage", _STORAGES, ids=["duckdb", "search"])
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
            _assert_insertion_order(cur, table_name, storage, _ROWS)
        finally:
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}"')


@pytest.mark.parametrize("source", _SOURCES)
@pytest.mark.parametrize("storage", _STORAGES, ids=["duckdb", "search"])
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
            _assert_insertion_order(cur, table_name, "", len(labels))
        finally:
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}"')


def _label_length(i: int) -> int:
    if i == 1:
        return 17_000_000
    if i == 1_500:
        return 9_000_000
    return 100_000 if i % 7 == 0 else i % 50


@pytest.mark.parametrize("source", _SOURCES)
@pytest.mark.parametrize("storage", _STORAGES, ids=["duckdb", "search"])
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
            _assert_insertion_order(cur, table_name, storage, rows)
        finally:
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}"')


@pytest.mark.parametrize("source", _SOURCES)
@pytest.mark.parametrize("storage", _STORAGES, ids=["duckdb", "search"])
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
@pytest.mark.parametrize("storage", _STORAGES, ids=["duckdb", "search"])
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
            _assert_insertion_order(cur, table_name, storage, _ROWS)
        finally:
            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}"')


@pytest.mark.parametrize("source", _SOURCES)
@pytest.mark.parametrize("storage", _STORAGES, ids=["duckdb", "search"])
@pytest.mark.parametrize("damage", ["after_trailer", "truncated"])
def test_large_binary_copy_damaged_stream_inserts_nothing(
        conn: psycopg.Connection, table_name: str, storage: str,
        damage: str, source: str, shared_path: Path) -> None:
    with conn.cursor() as cur:
        payload = _binary_rows(cur, _ROWS)
        payload = (payload + b"\x00\x01" if damage == "after_trailer"
                   else payload[:len(payload) * 2 // 3])
        cur.execute(f'CREATE TABLE public."{table_name}"(x INT, label VARCHAR)'
                    f" {storage}")
        try:
            with pytest.raises(psycopg.Error):
                _copy_from(cur, table_name, payload, "FORMAT BINARY", source,
                           shared_path)
            assert _count(cur, table_name, storage) == 0
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
    _assert_insertion_order(cur, table, storage, len(lengths))


@pytest.mark.parametrize("header", [False, True], ids=["plain", "header"])
@pytest.mark.parametrize("storage", _STORAGES, ids=["duckdb", "search"])
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


@pytest.mark.parametrize("storage", _STORAGES, ids=["duckdb", "search"])
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
            _assert_insertion_order(cur, table_name, storage, _ROWS)
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
                _assert_insertion_order(cur, table_name, "", _ROWS)
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
