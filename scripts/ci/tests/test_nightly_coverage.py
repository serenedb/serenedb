import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "scripts" / "ci"), str(ROOT / "scripts" / "coverage")]

import nightly  # noqa: E402
import report  # noqa: E402


@pytest.mark.parametrize("path, where", [
    ("server/connector/functions/string.cpp", ("server", "server", "server/connector", "functions")),
    ("server/auth/auth.cpp", ("server", "server", "server/auth", "")),
    ("server/serened_static_extension_loader.c", ("server", "server", "server", "")),
    ("iresearch/search/top/top_scorer.cpp", ("iresearch", "iresearch", "iresearch/search", "top")),
    ("third_party/duckdb/src/common/types/vector.cpp", ("duckdb", "duckdb", "duckdb/common", "types")),
    ("third_party/duckdb/src/include/duckdb/common/types/vector.hpp",
     ("duckdb", "duckdb", "duckdb/common", "types")),
    ("third_party/duckdb/src/include/duckdb/common/helper.hpp", ("duckdb", "duckdb", "duckdb/common", "")),
    ("third_party/duckdb/src/include/duckdb.h", ("duckdb", "duckdb", "duckdb/main", "")),
    ("third_party/duckdb/tools/shell/shell.cpp", ("duckdb", "duckdb", "duckdb/tools", "shell")),
    ("third_party/duckdb/extension/parquet/include/writer/primitive_column_writer.hpp",
     ("duckdb", "duckdb/extension/parquet", "duckdb/extension/parquet", "writer")),
    ("third_party/duckdb/extension/parquet/parquet_reader.cpp",
     ("duckdb", "duckdb/extension/parquet", "duckdb/extension/parquet", "")),
    ("third_party/duckdb_iceberg/src/include/rest_catalog/objects/table.hpp",
     ("duckdb", "duckdb_iceberg", "duckdb_iceberg", "rest_catalog")),
    ("third_party/duckdb_clickhouse/src/clickhouse_connection.cpp",
     ("duckdb", "duckdb_clickhouse", "duckdb_clickhouse", "")),
    ("third_party/database-connector/src/connector.cpp",
     ("duckdb", "database-connector", "database-connector", "")),
    ("third_party/re2/re2/re2.cc", ("re2", "re2", "re2", "re2")),
    ("third_party/croaring/src/roaring.c", ("other", "croaring", "croaring", "")),
    ("third_party/duckdb/third_party/fmt/format.cc", None),
    ("third_party/duckdb_spatial/src/third_party/shapelib/shpopen.cpp", None),
    ("generated/otel/schema_sql.h", None),
])
def test_components_follow_each_projects_layout(path, where):
    assert nightly.locate(path) == where


def record(path, hit, found):
    return (f"SF:{path}\nFNF:2\nFNH:1\nDA:1,{int(hit > 0)}\nDA:2,0\nBRF:2\nBRH:1\nLF:{found}\nLH:{hit}\n"
            "end_of_record\n")


def test_the_report_counts_the_projects_built_with_coverage_without_what_they_vendor():
    text = "".join([
        record("server/connector/scan/scan.cpp", 3, 4),
        record("third_party/duckdb/src/common/types/vector.cpp", 1, 2),
        record("third_party/duckdb/src/include/duckdb/common/types/vector.hpp", 2, 2),
        record("third_party/duckdb/third_party/fmt/format.cc", 0, 9),
        record("third_party/curl/include/curl/curl.h", 0, 5),
        record("third_party/re2/re2/re2.cc", 5, 10),
    ])
    written = []
    components = nightly.collect(text, written.append)
    assert sorted(components) == ["duckdb/common", "re2", "server/connector"]
    common = components["duckdb/common"]
    assert common["category"] == "duckdb"
    assert common["lines"] == [3, 4]
    assert common["subdirs"]["types"]["lines"] == [3, 4]
    assert common["functions"] == [2, 4]
    assert common["branches"] == [2, 4]
    files = [line[3:].strip() for line in written if line.startswith("SF:")]
    assert files == ["server/connector/scan/scan.cpp", "third_party/duckdb/src/common/types/vector.cpp",
                     "third_party/duckdb/src/include/duckdb/common/types/vector.hpp",
                     "third_party/re2/re2/re2.cc"]
    assert written.count("end_of_record\n") == 4


def counts(hit, found):
    return {"lines": [hit, found], "functions": [1, 1], "branches": [1, 2]}


def meta(components, date="2026-10-30", sha="0123456789abcdef"):
    return {"run": "1-1", "sha": sha, "label": "dev/x86_64, 4 shards", "date": date,
            "components": components}


def test_a_large_component_lists_its_large_subdirectories_and_the_rest_together():
    big = nightly.SPLIT_LINES
    part = nightly.SUBDIR_LINES
    components = {
        "duckdb/common": {"category": "duckdb", **counts(big - 100, big), "subdirs": {
            "types": counts(part - 50, part), "adbc": counts(0, big - part - 10), "enums": counts(5, 9),
            "": counts(1, 1)}},
        "duckdb/storage": {"category": "duckdb", **counts(big - 1, big), "subdirs": {
            "": counts(big - 1, big)}},
        "re2": {"category": "re2", **counts(5, 10), "subdirs": {"re2": counts(5, 10)}},
    }
    keys = [key for key, _, _, _ in nightly.rows(meta(components))]
    assert keys == ["*", "[duckdb]", "duckdb/common", "duckdb/common/adbc", "duckdb/common/types",
                    "duckdb/common (rest)", "duckdb/storage", "[re2]"]
    rest = {key: c for key, _, c, _ in nightly.rows(meta(components))}["duckdb/common (rest)"]
    assert rest["lines"] == [6, 10]


def test_the_report_compares_with_the_last_nightly_and_warns_when_coverage_drops():
    before = meta({
        "server/network": {"category": "server", **counts(900, 1000), "subdirs": {}},
        "server/utils": {"category": "server", **counts(95, 100), "subdirs": {}},
        "iresearch/index": {"category": "iresearch", **counts(500, 1000), "subdirs": {}},
    }, date="2026-10-29", sha="fedcba9876543210")
    after = meta({
        "server/network": {"category": "server", **counts(870, 1000), "subdirs": {}},
        "server/utils": {"category": "server", **counts(90, 100), "subdirs": {}},
        "iresearch/index": {"category": "iresearch", **counts(510, 1000), "subdirs": {}},
        "iresearch/store": {"category": "iresearch", **counts(1, 2), "subdirs": {}},
    })
    body, warnings = nightly.render(after, before)
    assert warnings == ["all: lines 70.0% (-1.2 since 2026-10-29), 26 more uncovered lines",
                        "server/network: lines 87.0% (-3.0 since 2026-10-29), 30 more uncovered lines"]
    assert "against the nightly of 2026-10-29 (`fedcba987`)" in body
    assert "| server/utils | 90.0% | -5.0 | 100.0% | 50.0% | 10 |" in body
    assert "| iresearch/index | 51.0% | +1.0 | 100.0% | 50.0% | 490 |" in body
    assert "| iresearch/store | 50.0% |  | 100.0% | 50.0% | 1 |" in body
    assert "<details><summary>server: 2 components</summary>" in body
    first, _ = nightly.render(after, None)
    assert "no earlier nightly coverage to compare with" in first
    assert "⚠" not in first


def test_the_last_nightly_is_the_newest_run_of_main_but_this_one():
    found = [["7", "main", "2026-10-28T05:00:00Z"], ["9", "main", "2026-10-30T05:00:00Z"],
             ["8", "mbkkt/ci", "2026-10-29T09:00:00Z"], ["6", "main", "2026-10-27T05:00:00Z"]]
    assert nightly.latest_run(found) == "9"
    assert nightly.latest_run(found, 9) == "7"
    assert nightly.latest_run([], 9) is None


def test_show_finds_a_component_its_subdirectory_or_a_path():
    text = "".join([
        record("third_party/duckdb/src/common/types/vector.cpp", 1, 2),
        record("third_party/duckdb/src/include/duckdb/common/types/vector.hpp", 2, 2),
        record("third_party/duckdb/src/storage/table/row_group.cpp", 0, 2),
    ])
    lines = text.splitlines(keepends=True)
    assert sorted(nightly.gaps(lines, "duckdb/common")) == [
        "third_party/duckdb/src/common/types/vector.cpp",
        "third_party/duckdb/src/include/duckdb/common/types/vector.hpp"]
    assert sorted(nightly.gaps(lines, "duckdb/common/types")) == sorted(nightly.gaps(lines, "duckdb/common"))
    found = nightly.gaps(lines, "third_party/duckdb/src/storage")
    assert found == {"third_party/duckdb/src/storage/table/row_group.cpp":
                     {"lines": [0, 2], "missed": [1, 2], "branches": 0}}
    assert nightly.gaps(lines, "third_party/duckdb/src/stor") == {}


def test_the_nightly_coverage_is_part_of_the_report():
    body, _ = report.build_report([], [], {}, "https://example.com/runs/1",
                                  whole_coverage="**Coverage** of `0123456789` (dev/x86_64)\n")
    assert "**Coverage** of `0123456789` (dev/x86_64)" in body
