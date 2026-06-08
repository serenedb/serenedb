"""pg-wire COPY ... FROM STDIN / TO STDOUT against SereneDB."""

from __future__ import annotations

import io
import uuid

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


@pytest.mark.parametrize(
    "fmt,header_row,data_rows",
    [
        ("TEXT", b"x\tlabel",
         [b"1\tone", b"2\ttwo", b"3\tthree"]),
        ("CSV",  b"x,label",
         [b"1,one", b"2,two", b"3,three"]),
    ],
    ids=["text", "csv"],
)
def test_copy_to_stdout(conn: psycopg.Connection, table_name: str,
                        fmt: str, header_row: bytes,
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
        assert len(rows) >= 2, f"expected header + data, got {out!r}"
        data = b"\n".join(rows[1:])
        assert b"10" in data and b"ten" in data, \
            f"unexpected stdout payload: {out!r}"
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
