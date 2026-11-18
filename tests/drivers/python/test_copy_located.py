"""COPY FROM in the PostgreSQL text formats: CSV over STDIN in any chunking,
errors that name their line, column and value, and ON_ERROR's notices."""

from __future__ import annotations

import os
import shutil
import uuid
from pathlib import Path

import psycopg
import pytest
from spec_loader import conn_kwargs

_UNIT_BYTES = 1 << 20


@pytest.fixture()
def conn() -> psycopg.Connection:
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    c.notices = []
    c.add_notice_handler(lambda d: c.notices.append(d.message_primary))
    yield c
    c.close()


@pytest.fixture()
def table(conn: psycopg.Connection) -> str:
    name = f"copy_located_{uuid.uuid4().hex[:12]}"
    yield name
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS public."{name}"')


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


def _create(conn: psycopg.Connection, table: str, columns: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table}"({columns})')


def _copy_in(conn: psycopg.Connection, sql: str, data: bytes,
             chunk: int = _UNIT_BYTES) -> None:
    with conn.cursor() as cur:
        with cur.copy(sql) as cp:
            for off in range(0, len(data), chunk):
                cp.write(data[off:off + chunk])


def _copy_error(conn: psycopg.Connection, sql: str, data: bytes,
                chunk: int = _UNIT_BYTES) -> psycopg.Error:
    with pytest.raises(psycopg.Error) as info:
        _copy_in(conn, sql, data, chunk)
    return info.value


def _good_rows(count: int, start: int = 0) -> bytes:
    return b"".join(b'%d,"q, %d"\n' % (i, i) for i in range(start, start + count))


def _quoted_rows(count: int) -> tuple[bytes, list[tuple[int, str]]]:
    rows = []
    out = []
    for i in range(count):
        value = f'line "{i}",\nnext\r\n{"x" * (i % 17)}'
        rows.append((i, value))
        out.append(f'{i},"{value.replace(chr(34), chr(34) * 2)}"\n')
    return "".join(out).encode(), rows


@pytest.mark.parametrize("chunk", [5, 4096, _UNIT_BYTES])
def test_csv_stdin_quoted_newlines_in_any_chunking(conn: psycopg.Connection,
                                                   table: str,
                                                   chunk: int) -> None:
    _create(conn, table, "a INT, b TEXT")
    data, rows = _quoted_rows(20000 if chunk > 5 else 2000)
    _copy_in(conn, f'COPY public."{table}" FROM STDIN (FORMAT csv)', data,
             chunk)
    with conn.cursor() as cur:
        cur.execute(f'SELECT a, b FROM public."{table}" ORDER BY a')
        assert cur.fetchall() == rows


@pytest.mark.parametrize("chunk", [3, _UNIT_BYTES])
def test_csv_stdin_escape_differs_from_quote(conn: psycopg.Connection,
                                             table: str, chunk: int) -> None:
    _create(conn, table, "a INT, b TEXT")
    data = b"".join(b"%d;'it\\'s %d\\\\;\nend'\n" % (i, i) for i in range(3000))
    _copy_in(conn,
             f"COPY public.\"{table}\" FROM STDIN "
             f"(FORMAT csv, DELIMITER ';', QUOTE '''', ESCAPE '\\')",
             data, chunk)
    with conn.cursor() as cur:
        cur.execute(f'SELECT count(*), min(b), max(a) FROM public."{table}"')
        count, first, last = cur.fetchone()
    assert (count, last) == (3000, 2999)
    assert first == "it's 0\\;\nend"


def test_value_error_names_line_column_and_value(conn: psycopg.Connection,
                                                 table: str) -> None:
    _create(conn, table, "a INT, b TEXT")
    rows = 200000
    data = _good_rows(rows) + b'zz,"bad"\n' + _good_rows(10, rows)
    error = _copy_error(conn, f'COPY public."{table}" FROM STDIN (FORMAT csv)',
                        data)
    assert len(data) > 3 * _UNIT_BYTES
    assert error.diag.sqlstate == "22P02"
    assert error.diag.message_primary == 'invalid input syntax for type integer: "zz"'
    assert error.diag.context == f'COPY {table}, line {rows + 1}, column a: "zz"'


def test_first_error_in_file_order_wins(conn: psycopg.Connection,
                                        table: str) -> None:
    _create(conn, table, "a INT, b TEXT")
    data = (_good_rows(50000) + b"x1,y\n" + _good_rows(250000, 50000) +
            b"x2,y\n" + _good_rows(10))
    for _ in range(3):
        error = _copy_error(conn,
                            f'COPY public."{table}" FROM STDIN (FORMAT csv)',
                            data)
        assert error.diag.context == f'COPY {table}, line 50001, column a: "x1"'


def test_line_counts_newlines_inside_quotes(conn: psycopg.Connection,
                                            table: str) -> None:
    _create(conn, table, "a INT, b TEXT")
    data = b'1,"two\nlines"\n2,"three\nmore\nlines"\nbad,x\n'
    error = _copy_error(conn, f'COPY public."{table}" FROM STDIN (FORMAT csv)',
                        data)
    assert error.diag.context == f'COPY {table}, line 6, column a: "bad"'
    error = _copy_error(conn, f'COPY public."{table}" FROM STDIN (FORMAT csv)',
                        b'1,"a\nb"\n2,"open\n')
    assert error.diag.message_primary == "unterminated CSV quoted field"
    assert error.diag.context == f'COPY {table}, line 4: "2,"open\n"'


def test_value_error_shows_at_most_100_bytes(conn: psycopg.Connection,
                                             table: str) -> None:
    _create(conn, table, "a INT, b TEXT")
    value = "x" * 99 + "é" + "y" * 50
    shown = "x" * 99 + "..."
    error = _copy_error(conn, f'COPY public."{table}" FROM STDIN (FORMAT csv)',
                        f"{value},b\n".encode())
    assert error.diag.message_primary == (
        f'invalid input syntax for type integer: "{value}"')
    assert error.diag.context == f'COPY {table}, line 1, column a: "{shown}"'
    conn.notices.clear()
    _copy_in(conn, f'COPY public."{table}" FROM STDIN '
             "(FORMAT csv, ON_ERROR ignore, LOG_VERBOSITY verbose)",
             f"{value},b\n1,a\n".encode())
    assert conn.notices == [
        f'skipping row due to data type incompatibility at line 1 for column "a": "{shown}"',
        "1 row was skipped due to data type incompatibility",
    ]


@pytest.mark.parametrize("fmt,data,line,text", [
    ("csv", b"1,a\n2\n", 2, "2"),
    ("csv", b"1,a\n2,b,c\n", 2, "2,b,c"),
    ("text", b"1\ta\n2\n", 2, "2"),
    ("text", b"1\ta\n2\tb\tc\n", 2, "2\tb\tc"),
    ("csv", b"1,a\r\n2,b,c\r\n", 2, "2,b,c"),
    ("text", b"1\ta\r\n2\tb\tc\r\n", 2, "2\tb\tc"),
    ("csv", b"1,a\n2,b,c", 2, "2,b,c"),
], ids=["csv-missing", "csv-extra", "text-missing", "text-extra", "csv-crlf",
        "text-crlf", "csv-last-line"])
def test_structure_errors_name_the_line(conn: psycopg.Connection, table: str,
                                        fmt: str, data: bytes, line: int,
                                        text: str) -> None:
    _create(conn, table, "a INT, b TEXT")
    error = _copy_error(conn,
                        f'COPY public."{table}" FROM STDIN (FORMAT {fmt})',
                        data)
    assert error.diag.sqlstate == "22P04"
    assert error.diag.context == f'COPY {table}, line {line}: "{text}"'


def test_binary_errors_name_the_tuple(conn: psycopg.Connection,
                                      table: str) -> None:
    _create(conn, table, "a INT, b TEXT")
    header = b"PGCOPY\n\xff\r\n\x00" + b"\x00" * 8
    good = b"\x00\x02\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x01x"
    bad_count = b"\x00\x01\x00\x00\x00\x04\x00\x00\x00\x02"
    error = _copy_error(conn,
                        f'COPY public."{table}" FROM STDIN (FORMAT binary)',
                        header + good + bad_count + b"\xff\xff")
    assert error.diag.message_primary == "row field count is 1, expected 2"
    assert error.diag.context == f"COPY {table}, line 2"
    short_int = b"\x00\x02\x00\x00\x00\x02\x00\x01\x00\x00\x00\x01x"
    error = _copy_error(conn,
                        f'COPY public."{table}" FROM STDIN (FORMAT binary)',
                        header + good + short_int + b"\xff\xff")
    assert error.diag.context == f"COPY {table}, line 2, column a"


def test_errors_in_a_later_file_range(conn: psycopg.Connection, table: str,
                                      shared_path: Path) -> None:
    _create(conn, table, "a INT, b TEXT")
    rows = 1200000
    path = shared_path / "late_error.csv"
    path.write_bytes(_good_rows(rows) + b'oops,"x"\n' + _good_rows(5, rows))
    assert path.stat().st_size > 2 * (8 << 20)
    with pytest.raises(psycopg.Error) as info, conn.cursor() as cur:
        cur.execute(f"COPY public.\"{table}\" FROM '{path}' (FORMAT csv)")
    assert info.value.diag.context == (
        f'COPY {table}, line {rows + 1}, column a: "oops"')


def test_on_error_notices(conn: psycopg.Connection, table: str) -> None:
    _create(conn, table, "a INT, b TEXT")
    data = (_good_rows(100000) + b"x,y\n" + _good_rows(250000, 100000) +
            b"z,w\n")
    sql = f'COPY public."{table}" FROM STDIN (FORMAT csv, ON_ERROR ignore'
    conn.notices.clear()
    _copy_in(conn, sql + ")", data)
    assert conn.notices == ["2 rows were skipped due to data type incompatibility"]
    conn.notices.clear()
    _copy_in(conn, sql + ", LOG_VERBOSITY verbose)", data)
    assert conn.notices == [
        'skipping row due to data type incompatibility at line 100001 for column "a": "x"',
        'skipping row due to data type incompatibility at line 350002 for column "a": "z"',
        "2 rows were skipped due to data type incompatibility",
    ]
    conn.notices.clear()
    _copy_in(conn, sql + ", LOG_VERBOSITY silent)", data)
    assert conn.notices == []
    conn.notices.clear()
    _copy_in(conn, sql + ")", b"1,a\nbad,b\n")
    assert conn.notices == ["1 row was skipped due to data type incompatibility"]
    with conn.cursor() as cur:
        cur.execute(f'SELECT count(*) FROM public."{table}"')
        assert cur.fetchone()[0] == 3 * 350000 + 1


def test_header_match_names_line_one(conn: psycopg.Connection,
                                     table: str) -> None:
    _create(conn, table, "a INT, b TEXT")
    error = _copy_error(conn,
                        f'COPY public."{table}" FROM STDIN (FORMAT csv, HEADER MATCH)',
                        b"a,c\n1,x\n")
    assert error.diag.message_primary == (
        'column name mismatch in header line field 2: got "c", expected "b"')
    assert error.diag.context == f'COPY {table}, line 1: "a,c"'
