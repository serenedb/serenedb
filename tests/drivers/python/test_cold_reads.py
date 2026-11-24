"""Reads of index files on an object store: how many requests a query that finds
nothing cached asks for, and how many rounds of requests it waits for.
"""

from __future__ import annotations

import collections
import time
from pathlib import Path

import pytest
from fake_s3 import FakeS3
from test_tablespaces import _create_secret, _Server, _Store, pytestmark  # noqa: F401

ONE_ROUND_LATENCY = 0.5


def _one_round(log: list[dict], extension: str) -> list[dict]:
    reads = [entry for entry in log if entry["key"].endswith(extension)]
    assert reads, log
    assert max(entry["began"] for entry in reads) < min(
        entry["ended"] for entry in reads), reads
    return reads


def _rounds(reads: list[dict]) -> int:
    """How many requests in a row a query waited for: a request is one round later than the latest of the requests
    that had ended when it began."""
    rounds: list[int] = []
    for i, entry in enumerate(reads):
        rounds.append(1 + max((rounds[j] for j in range(i)
                               if reads[j]["ended"] <= entry["began"]), default=0))
    return max(rounds)


def test_cold_top_k_reads_postings_and_columns_in_one_round(
        tmp_path: Path) -> None:
    location = f"s3://bucket/rounds-{tmp_path.name}"
    segments = 4
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret",
                                          "bucket"), location)
            with server.connect() as conn:
                conn.execute(
                    "CREATE TEXT SEARCH DICTIONARY words AS "
                    "split_by_non_alpha(case := 'lower') "
                    "WITH (frequency, position, norm)")
                conn.execute("CREATE TABLE docs(id INTEGER, body TEXT) "
                             "WITH (compaction_interval = 0) TABLESPACE idx")
                conn.execute(
                    "CREATE INDEX docs_body ON docs USING inverted(id, body words)")
                for segment in range(segments):
                    conn.execute(
                        "INSERT INTO docs SELECT i, CASE i % 10 WHEN 0 THEN 'alpha' "
                        "WHEN 1 THEN 'beta' ELSE 'gamma' END "
                        f"FROM range({segment * 1000}, {(segment + 1) * 1000}) t(i)")
                    conn.execute("VACUUM (FLUSH_TABLE) docs")
                conn.execute("SET GLOBAL threads = 2")
                conn.execute("SET GLOBAL enable_external_file_cache = false")
                conn.execute("SET GLOBAL enable_external_file_cache = true")
                fake.latency = ONE_ROUND_LATENCY
                fake.log = []
                rows = conn.execute(
                    "SELECT id FROM docs_body WHERE body @@ to_tsquery('alpha beta') "
                    "ORDER BY BM25(docs_body.tableoid) DESC LIMIT 1000").fetchall()
                log, fake.log = fake.log, None
        finally:
            server.stop()
    assert sorted(row[0] for row in rows) == [
        i for i in range(segments * 1000) if i % 10 < 2]
    assert len(_one_round(log, ".doc")) <= segments
    assert len(_one_round(log, ".col")) == segments


def test_cold_late_materialization_reads_columns_in_one_round(
        tmp_path: Path) -> None:
    location = f"s3://bucket/lookup-{tmp_path.name}"
    query = ("SELECT * FROM wide WHERE a LIKE '%ab%' "
             "ORDER BY b LIMIT 5")
    segments, segment_rows = 8, 25000
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret",
                                          "bucket"), location)
            with server.connect() as conn:
                conn.execute(
                    "CREATE TABLE wide(id BIGINT, a TEXT, b TEXT, c TEXT, d TEXT, e TEXT, "
                    "f TEXT) WITH (compaction_interval = 0) TABLESPACE idx")
                for segment in range(segments):
                    first = f"i % {segment_rows} = 0"
                    conn.execute(
                        f"INSERT INTO wide SELECT i, CASE WHEN {first} THEN 'ab' ELSE '' END "
                        f"|| md5(i::TEXT), CASE WHEN {first} THEN ' ' || i::TEXT "
                        "ELSE md5((-i)::TEXT) END, "
                        + ", ".join(f"md5((i + {k})::TEXT) || md5((i * {k})::TEXT)"
                                    for k in (2, 3, 4, 5))
                        + f" FROM range({segment * segment_rows}, "
                        f"{(segment + 1) * segment_rows}) t(i)")
                    conn.execute("VACUUM (FLUSH_TABLE) wide")
                plan = "\n".join(row[-1] for row in conn.execute(
                    "EXPLAIN " + query).fetchall())
                expected = conn.execute(query).fetchall()
                conn.execute("SET GLOBAL enable_external_file_cache = false")
                conn.execute("SET GLOBAL enable_external_file_cache = true")
                conn.execute("SELECT a, b FROM wide").fetchall()
                fake.latency = ONE_ROUND_LATENCY
                fake.log = []
                rows = conn.execute(query).fetchall()
                log, fake.log = fake.log, None
        finally:
            server.stop()
    assert "LOOKUP_JOIN" in plan, plan
    assert rows == expected
    assert len(_one_round(log, ".col")) >= 4, log


@pytest.mark.parametrize("query", [
    "SELECT count(*) FROM narrow WHERE v <> 0",
    "SELECT v % 7, count(*) FROM narrow WHERE v <> 0 GROUP BY ALL ORDER BY ALL",
    "SELECT sum(v % 7) FROM narrow",
])
def test_cold_column_scan_reads_its_columns_in_one_round(
        tmp_path: Path, query: str) -> None:
    location = f"s3://bucket/scan-{tmp_path.name}"
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret",
                                          "bucket"), location)
            with server.connect() as conn:
                conn.execute(
                    "CREATE TABLE narrow WITH (compaction_interval = 0) TABLESPACE idx AS "
                    "SELECT i AS id, hash(i) AS v FROM range(2000000) t(i)")
                conn.execute("VACUUM (FLUSH_TABLE) narrow")
                expected = conn.execute(query).fetchall()
                conn.execute("SET GLOBAL enable_external_file_cache = false")
                conn.execute("SET GLOBAL enable_external_file_cache = true")
                fake.latency = ONE_ROUND_LATENCY
                fake.log = []
                rows = conn.execute(query).fetchall()
                log, fake.log = fake.log, None
        finally:
            server.stop()
    assert rows == expected
    _one_round(log, ".col")


def test_a_cold_column_scan_keeps_reading_ahead_of_its_workers(tmp_path: Path) -> None:
    location = f"s3://bucket/ahead-{tmp_path.name}"
    query = "SELECT sum(v % 7) FROM wide"
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret",
                                          "bucket"), location)
            with server.connect() as conn:
                conn.execute(
                    "CREATE TABLE wide WITH (compaction_interval = 0) "
                    "TABLESPACE idx AS SELECT hash(i) AS v FROM range(12000000) t(i)")
                conn.execute("VACUUM (FLUSH_TABLE) wide")
                conn.execute("SET threads = 2")
                expected = conn.execute(query).fetchall()
                conn.execute("SET GLOBAL enable_external_file_cache = false")
                conn.execute("SET GLOBAL enable_external_file_cache = true")
                fake.latency = 0.05
                fake.log = []
                rows = conn.execute(query).fetchall()
                log, fake.log = fake.log, None
        finally:
            server.stop()
    assert rows == expected
    reads = sorted((entry for entry in log if entry["key"].endswith(".col")),
                   key=lambda entry: entry["began"])
    assert len({entry["key"] for entry in reads}) >= 16, reads
    assert _rounds(reads) <= 10, _rounds(reads)


def _fetched_and_stored(fake: FakeS3, log: list[dict]) -> tuple[int, int]:
    with fake.lock:
        stored = [len(obj.data) for (_, key), obj in fake.objects.items()
                  if key.endswith(".col")]
    assert len(stored) == 1, stored
    fetched = 0
    for entry in log:
        if entry["key"].endswith(".col"):
            first, _, last = entry["range"].removeprefix("bytes=").partition("-")
            fetched += int(last) - int(first) + 1
    return fetched, stored[0]


def test_a_cold_column_scan_skips_the_row_groups_a_filter_over_two_columns_rules_out(
        tmp_path: Path) -> None:
    location = f"s3://bucket/pruned-pairs-{tmp_path.name}"
    query = "SELECT sum(a % 7) FROM pairs WHERE b > a"
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret",
                                          "bucket"), location)
            with server.connect() as conn:
                conn.execute("SET threads = 1")
                conn.execute(
                    "CREATE TABLE pairs WITH (compaction_interval = 0) "
                    "TABLESPACE idx AS SELECT a, CASE WHEN a < 153722867280912930 "
                    "THEN 4611686018427387903 ELSE 0 END AS b "
                    "FROM (SELECT hash(i) // 4 AS a FROM range(6000000) t(i)) ORDER BY a")
                conn.execute("VACUUM (FLUSH_TABLE) pairs")
                expected = conn.execute(query).fetchall()
                conn.execute("SET GLOBAL enable_external_file_cache = false")
                conn.execute("SET GLOBAL enable_external_file_cache = true")
                fake.log = []
                rows = conn.execute(query).fetchall()
                log, fake.log = fake.log, None
        finally:
            server.stop()
        fetched, stored = _fetched_and_stored(fake, log)
    assert rows == expected
    assert 0 < fetched * 4 < stored, (fetched, stored)


def test_a_cold_column_scan_skips_the_row_groups_its_filter_rules_out(tmp_path: Path) -> None:
    location = f"s3://bucket/pruned-{tmp_path.name}"
    query = "SELECT sum(k % 7) FROM sorted WHERE k < 184467440737095516"
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret",
                                          "bucket"), location)
            with server.connect() as conn:
                conn.execute("SET threads = 1")
                conn.execute(
                    "CREATE TABLE sorted WITH (compaction_interval = 0) "
                    "TABLESPACE idx AS SELECT hash(i) AS k FROM range(6000000) t(i) ORDER BY k")
                conn.execute("VACUUM (FLUSH_TABLE) sorted")
                conn.execute("RESET threads")
                expected = conn.execute(query).fetchall()
                conn.execute("SET GLOBAL enable_external_file_cache = false")
                conn.execute("SET GLOBAL enable_external_file_cache = true")
                fake.log = []
                rows = conn.execute(query).fetchall()
                log, fake.log = fake.log, None
        finally:
            server.stop()
        fetched, stored = _fetched_and_stored(fake, log)
    assert rows == expected
    assert 0 < fetched * 4 < stored, (fetched, stored)


def test_a_cold_filtered_scan_asks_for_the_projected_columns_of_a_row_group_together(
        tmp_path: Path) -> None:
    location = f"s3://bucket/projected-{tmp_path.name}"
    query = ("SELECT sum(a % 7), sum(b % 7), sum(c % 7), sum(d % 7) FROM cols "
             "WHERE k > 9223372036854775807")
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret",
                                          "bucket"), location)
            with server.connect() as conn:
                conn.execute("SET threads = 1")
                conn.execute(
                    "CREATE TABLE cols WITH (compaction_interval = 0) "
                    "TABLESPACE idx AS SELECT hash(i) AS k, hash(i + 1) AS a, hash(i + 2) AS b, "
                    "hash(i + 3) AS c, hash(i + 4) AS d FROM range(1500000) t(i)")
                conn.execute("VACUUM (FLUSH_TABLE) cols")
                conn.execute("SET threads = 2")
                expected = conn.execute(query).fetchall()
                conn.execute("SET GLOBAL enable_external_file_cache = false")
                conn.execute("SET GLOBAL enable_external_file_cache = true")
                fake.latency = 0.05
                fake.log = []
                rows = conn.execute(query).fetchall()
                log, fake.log = fake.log, None
        finally:
            server.stop()
    assert rows == expected
    reads = sorted((entry for entry in log if entry["key"].endswith(".col")),
                   key=lambda entry: entry["began"])
    assert len({entry["key"] for entry in reads}) == 1, reads
    assert 0 < len(reads) <= 140, len(reads)
    assert _rounds(reads) <= 5, _rounds(reads)


def test_a_cold_filtered_scan_asks_ahead_only_for_the_rows_its_filter_leaves(
        tmp_path: Path) -> None:
    location = f"s3://bucket/ahead-pruned-{tmp_path.name}"
    query = "SELECT sum(v % 7) FROM sorted WHERE k < 8301034833169298227"
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret",
                                          "bucket"), location)
            with server.connect() as conn:
                conn.execute("SET threads = 1")
                conn.execute(
                    "CREATE TABLE sorted WITH (compaction_interval = 0) "
                    "TABLESPACE idx AS SELECT hash(i) AS k, hash(i + 1) AS v "
                    "FROM range(1500000) t(i) ORDER BY k")
                conn.execute("VACUUM (FLUSH_TABLE) sorted")
                expected = conn.execute(query).fetchall()
                conn.execute("SET GLOBAL enable_external_file_cache = false")
                conn.execute("SET GLOBAL enable_external_file_cache = true")
                fake.latency = 0.05
                fake.log = []
                rows = conn.execute(query).fetchall()
                log, fake.log = fake.log, None
        finally:
            server.stop()
        with fake.lock:
            stored = [len(obj.data) for (_, key), obj in fake.objects.items()
                      if key.endswith(".col")]
    assert rows == expected
    assert len(stored) == 1, stored
    reads = sorted((entry for entry in log if entry["key"].endswith(".col")),
                   key=lambda entry: entry["began"])
    fetched = 0
    for entry in reads:
        first, _, last = entry["range"].removeprefix("bytes=").partition("-")
        fetched += int(last) - int(first) + 1
    assert stored[0] * 4 < fetched * 10 < stored[0] * 6, (fetched, stored)
    assert _rounds(reads) <= 4, _rounds(reads)


@pytest.mark.parametrize("query", [
    "SELECT count(*) FROM logs WHERE body @@ ts_regexp('c.che')",
    "SELECT id FROM logs WHERE body @@ 'cache' "
    "ORDER BY BM25(logs.tableoid) DESC, id LIMIT 10",
    "SELECT body, count(*) FROM logs WHERE body @@ 'cache' GROUP BY ALL",
])
def test_a_repeated_query_reads_nothing_from_the_store(
        tmp_path: Path, query: str) -> None:
    location = f"s3://bucket/repeat-{tmp_path.name}"
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret", "bucket"),
                           location)
            with server.connect() as conn:
                conn.execute(
                    "CREATE TEXT SEARCH DICTIONARY words AS "
                    "split_by_non_alpha(case := 'lower') "
                    "WITH (frequency, position, norm)")
                conn.execute(
                    "CREATE TABLE logs WITH (compaction_interval = 0) "
                    "TABLESPACE idx AS SELECT 0 AS id, '' AS body WHERE false")
                conn.execute("CREATE INDEX logs_body ON logs USING inverted(body words)")
                conn.execute("SET threads = 1")
                for segment in range(4):
                    conn.execute(
                        "INSERT INTO logs SELECT i, concat_ws(' ', "
                        "'svc' || (hash(i) % 50), "
                        "CASE WHEN hash(i * 5) % 100 < 6 THEN 'cache' END, "
                        "'w' || (hash(i * 3) % 3000)) "
                        f"FROM range({segment * 2000000}, {(segment + 1) * 2000000}) t(i)")
                    conn.execute("VACUUM (FLUSH_TABLE) logs")
                conn.execute("RESET threads")
                conn.execute("SET GLOBAL threads = 8")
                conn.execute("SET GLOBAL enable_external_file_cache = false")
                conn.execute("SET GLOBAL enable_external_file_cache = true")
                fake.latency = 0.02
                first = sorted(conn.execute(query).fetchall())
                fake.log = []
                repeated = time.monotonic()
                repeats = [sorted(conn.execute(query).fetchall()) for _ in range(12)]
                reads = [entry["key"] for entry in fake.log
                         if entry["began"] >= repeated]
                fake.log = None
        finally:
            server.stop()
    assert repeats == [first] * 12
    assert reads == []


def test_cold_count_reads_its_postings_in_one_round(tmp_path: Path) -> None:
    location = f"s3://bucket/counts-{tmp_path.name}"
    segments = 4
    with FakeS3() as fake:
        server = _Server(tmp_path / "data")
        try:
            assert server.wait_ready(), server.output()
            _create_secret(server, _Store(fake.endpoint, "key", "secret",
                                          "bucket"), location)
            with server.connect() as conn:
                conn.execute(
                    "CREATE TEXT SEARCH DICTIONARY words AS "
                    "split_by_non_alpha(case := 'lower') "
                    "WITH (frequency, position, norm)")
                conn.execute("CREATE TABLE docs(id INTEGER, body TEXT) "
                             "WITH (compaction_interval = 0) TABLESPACE idx")
                conn.execute(
                    "CREATE INDEX docs_body ON docs USING inverted(id, body words)")
                for segment in range(segments):
                    conn.execute(
                        "INSERT INTO docs SELECT i, CASE i % 10 WHEN 0 THEN 'alpha beta' "
                        "WHEN 1 THEN 'beta alpha' ELSE 'gamma' END "
                        f"FROM range({segment * 1000}, {(segment + 1) * 1000}) t(i)")
                    conn.execute("VACUUM (FLUSH_TABLE) docs")
                conn.execute("SET GLOBAL threads = 2")
                fake.latency = ONE_ROUND_LATENCY

                def cold_count(query: str) -> tuple[int, list[dict]]:
                    conn.execute("SET GLOBAL enable_external_file_cache = false")
                    conn.execute("SET GLOBAL enable_external_file_cache = true")
                    fake.log = []
                    count = conn.execute(
                        "SELECT count(*) FROM docs_body WHERE body @@ to_tsquery(%s)",
                        (query,)).fetchone()[0]
                    log, fake.log = fake.log, None
                    return count, log

                term, term_log = cold_count("alpha")
                phrase, phrase_log = cold_count('"alpha beta"')
        finally:
            server.stop()
    assert term == segments * 200
    assert not [entry for entry in term_log
                if entry["key"].endswith((".doc", ".pos"))], term_log
    assert phrase == segments * 100
    by_segment = collections.defaultdict(list)
    for entry in phrase_log:
        name = entry["key"].rsplit("/", 1)[-1]
        if name.endswith((".doc", ".pos")):
            by_segment[name.split(".")[0]].append(entry)
    assert len(by_segment) == segments, phrase_log
    for reads in by_segment.values():
        assert {entry["key"].rsplit(".", 1)[-1] for entry in reads} == {"doc", "pos"}, reads
        assert max(entry["began"] for entry in reads) < min(
            entry["ended"] for entry in reads), reads
