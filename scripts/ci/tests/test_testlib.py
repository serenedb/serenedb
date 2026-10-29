import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "scripts" / "ci"), str(ROOT / "tests" / "drivers" / "harness"),
                str(ROOT / "tests" / "sqllogic")]

import ci  # noqa: E402
import generated  # noqa: E402
from testlib import cli, scheduler  # noqa: E402
from testlib.scheduler import (FAILED, INTERRUPTED, PASSED, SKIPPED, Outcome, Result,  # noqa: E402
                               Unit)
from testlib.suites import duckdb, gtest, sqllogic  # noqa: E402
from testlib.timing import Timing  # noqa: E402


def unit(uid, members=(), fixtures=(), cost=1.0, cpus=1):
    return Unit(uid, uid.split(":")[0], None, members=members, fixtures=fixtures, cost=cost,
                cpus=cpus)


def changed(monkeypatch, tmp_path, files, units, compiled=True):
    for name, text in files.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    monkeypatch.setattr(cli, "ROOT", tmp_path)
    monkeypatch.setattr(cli, "changed_files", lambda base: sorted(files))
    return cli.changed_units(units, "origin/main", compiled)


def test_a_changed_test_picks_its_units_in_every_suite(monkeypatch, tmp_path):
    units = [unit("sqllogic:pg-wire-simple:sdb/a.test"), unit("postgres:pg-wire-simple:pg/b.test"),
             unit("recovery:recovery/c.test"), unit("sqllogic:pg-wire-simple:sdb/other.test"),
             unit("duckdb:core:batch:test/sql/x.test+1",
                  members=("duckdb:core:test/sql/x.test", "duckdb:core:test/sql/y.test")),
             unit("duckdb:avro:batch:test/sql/x.test+0", members=("duckdb:avro:test/sql/x.test",)),
             unit("network:hba"), unit("drivers:python"), unit("drivers:java")]
    picked, tests = changed(monkeypatch, tmp_path, {
        "tests/sqllogic/sdb/a.test": "", "tests/sqllogic/pg/b.test": "",
        "tests/sqllogic/recovery/c.test": "", "third_party/duckdb/test/sql/x.test": "",
        "tests/drivers/network/run.sh": "", "tests/drivers/spec/select.yaml": "",
        "tests/drivers/harness/services.py": ""}, units)
    assert tests == {"sqllogic:pg-wire-simple:sdb/a.test", "postgres:pg-wire-simple:pg/b.test",
                     "recovery:recovery/c.test", "duckdb:core:test/sql/x.test", "network:hba",
                     "drivers:python", "drivers:java"}
    assert len(picked) == 7


def test_override_records_only_the_generators_whose_tests_ran(monkeypatch, tmp_path):
    base = tmp_path / "tests" / "sqllogic"
    for name in ("a", "b"):
        (base / "generators" / "sdb").mkdir(parents=True, exist_ok=True)
        (base / "generators" / "sdb" / f"{name}.py").write_text("")
    monkeypatch.setattr(cli, "ROOT", tmp_path)
    recorded = []
    monkeypatch.setattr(sqllogic.subprocess, "run", lambda cmd, **_: recorded.append(cmd[3]))
    units = [unit("sqllogic:pg-wire-simple:sdb/a.2.gen.test"), unit("sqllogic:pg-wire-simple:sdb/c.test"),
             unit("recovery:recovery/b.gen.test")]
    sqllogic.generated_tests(SimpleNamespace(root=tmp_path), "record", only=cli.generated_sources(units))
    assert recorded == [str(base / "generators" / "sdb" / "a.py")]


def test_override_records_a_shared_generator_from_its_postgres_run(monkeypatch, tmp_path):
    base = tmp_path / "tests" / "sqllogic"
    (base / "generators" / "any" / "pg").mkdir(parents=True)
    (base / "generators" / "any" / "pg" / "m.py").write_text("")
    (base / "any" / "pg").mkdir(parents=True)
    (base / "pg").mkdir()
    (base / "pg" / "any").symlink_to("../any/pg/")
    monkeypatch.setattr(cli, "ROOT", tmp_path)
    recorded = []
    monkeypatch.setattr(sqllogic.subprocess, "run", lambda cmd, **_: recorded.append(cmd[3]))
    units = [unit("postgres:pg-wire-simple:pg/any/m.gen.test")]
    sqllogic.generated_tests(SimpleNamespace(root=tmp_path), "record", only=cli.generated_sources(units))
    assert recorded == [str(base / "generators" / "any" / "pg" / "m.py")]


def test_a_shared_generator_records_each_side_of_a_difference_from_its_own_engine():
    text = ("query\nSELECT 1\n----\na\n<recorded>\n\n"
            "skipif serenedb\nquery\nSELECT 2\n----\na\n<recorded>\n\n"
            "onlyif serenedb\nquery\nSELECT 2\n----\na\n<recorded>\n")
    shared, pg_side, sdb_side = (key for key, _ in generated.recorded(text))
    postgres = text.replace("SELECT 1\n----\na\n<recorded>", "SELECT 1\n----\na\n1").replace(
        "skipif serenedb\nquery\nSELECT 2\n----\na\n<recorded>", "skipif serenedb\nquery\nSELECT 2\n----\na\n2")
    first = generated.record(text, postgres, "t", {}, "postgres", True)
    assert first == {shared: ("values", ["1"]), pg_side: ("values", ["2"])}
    serenedb = text.replace("SELECT 1\n----\na\n<recorded>", "SELECT 1\n----\na\n9").replace(
        "onlyif serenedb\nquery\nSELECT 2\n----\na\n<recorded>", "onlyif serenedb\nquery\nSELECT 2\n----\na\n3")
    assert generated.record(text, serenedb, "t", first, "serenedb", True) == {
        shared: ("values", ["1"]), pg_side: ("values", ["2"]), sdb_side: ("values", ["3"])}
    assert generated.record(text, serenedb, "t", {}, "serenedb", False) == {
        shared: ("values", ["9"]), sdb_side: ("values", ["3"])}


def test_a_query_under_two_conditions_records_its_answer_and_its_error():
    text = ("onlyif postgres\nskipif pg-wire-extended\nquery\nSELECT 1\n----\na\n<recorded>\n\n"
            "onlyif serenedb\nskipif pg-wire-extended\nquery\nSELECT 1\n----\na\n<recorded>\n")
    pg_side, sdb_side = (key for key, _ in generated.recorded(text))
    assert pg_side != sdb_side
    postgres = text.replace("pg-wire-extended\nquery\nSELECT 1\n----\na\n<recorded>",
                            "pg-wire-extended\nquery\nSELECT 1\n----\na\n1", 1)
    first = generated.record(text, postgres, "t", {}, "postgres", True)
    assert first == {pg_side: ("values", ["1"])}
    serenedb = ("onlyif postgres\nskipif pg-wire-extended\nquery\nSELECT 1\n----\na\n<recorded>\n\n"
                "onlyif serenedb\nskipif pg-wire-extended\nquery error boom\nSELECT 1\n")
    records = generated.record(text, serenedb, "t", first, "serenedb", True)
    assert records == {pg_side: ("values", ["1"]), sdb_side: ("error", "boom")}
    filled, missing = generated.fill(text, records)
    assert missing == 0
    assert filled.endswith("onlyif serenedb\nskipif pg-wire-extended\nquery error\nSELECT 1\n----\n"
                           "boom\n\n")


def test_a_query_records_its_whole_answer_one_row_per_line():
    text = ("query\nSELECT 1\n----\n<recorded rows>\n\n"
            "query\nSELECT 2\n----\na\tb\n<recorded rows>\n\n"
            "query\nSELECT 3\n----\na\n<recorded rows>\n")
    whole, rows, none = (key for key, _ in generated.recorded(text))
    overridden = ("query\nSELECT 1\n----\nx\n1\n\n"
                  "query\nSELECT 2\n----\na\tb\n1\tx\n2\ty\n\n"
                  "query\nSELECT 3\n----\na\n")
    records = generated.record(text, overridden, "t", {}, "serenedb", False)
    assert records == {whole: ("rows", ["x", "1"]), rows: ("rows", ["1\tx", "2\ty"]),
                       none: ("rows", [])}
    assert generated.dump(records) == f"{whole}\n\tx\n\t1\n{rows}\n\t1\tx\n\t2\ty\n{none}\n"
    assert generated.parse(generated.dump(records)) == records
    assert generated.fill(text, records) == (overridden, 0)


def test_a_changed_driver_test_picks_its_file_and_a_helper_every_file_of_its_language(
        monkeypatch, tmp_path):
    files = [unit(f"drivers:python:{name}") for name in ("test_copy", "test_progress", "cli_help")]
    units = [*files, unit("drivers:java")]
    picked, _ = changed(monkeypatch, tmp_path, {"tests/drivers/python/test_copy.py": ""}, units)
    assert picked == files[:1]
    picked, _ = changed(monkeypatch, tmp_path, {"tests/drivers/python/conftest.py": ""}, units)
    assert picked == files


def test_a_changed_test_source_picks_the_compiled_tests_it_defines(monkeypatch, tmp_path):
    units = [unit("gtest:serenedb-tests:batch:Plain.Works+3", members=(
                 "gtest:serenedb-tests:Plain.Works", "gtest:serenedb-tests:Plain.Other",
                 "gtest:serenedb-tests:Each/Param.Runs/0", "gtest:serenedb-tests:Typed/1.Holds")),
             unit("gtest:serenedb-tests:batch:Else.Fails+0", members=("gtest:serenedb-tests:Else.Fails",)),
             unit("duckdb:cpp:batch:Test a thing+0", members=("duckdb:cpp:Test a thing",)),
             unit("gtest:re2-tests:batch:Search.Literal+1",
                  members=("gtest:re2-tests:Search.Literal", "gtest:re2-tests:Set.Unanchored"))]
    files = {"tests/server/plain_test.cpp": "TEST(Plain, Works) {}\nTEST_P(Param, Runs) {}\n"
                                            "TYPED_TEST(Typed, Holds) {}\n",
             "third_party/duckdb/test/api/test_thing.cpp": 'TEST_CASE("Test a thing", "[api]") {}\n',
             "third_party/re2/re2/testing/search_test.cc": "TEST(Search, Literal) {}\n"}
    _, tests = changed(monkeypatch, tmp_path, files, units)
    assert tests == {"gtest:serenedb-tests:Plain.Works", "gtest:serenedb-tests:Each/Param.Runs/0",
                     "gtest:serenedb-tests:Typed/1.Holds", "duckdb:cpp:Test a thing",
                     "gtest:re2-tests:Search.Literal"}
    picked, _ = changed(monkeypatch, tmp_path, files, units, compiled=False)
    assert picked == []


@pytest.mark.parametrize("config,args,targets", [
    ("perf", [], "serened iresearch-examples serenedb-bench-micro search-benchmark-game"),
    ("dev", [], "serened iresearch-examples serenedb-bench-micro search-benchmark-game "
                "iresearch-load-tests iresearch-tests re2-tests serenedb-tests unittest"),
    ("asan", ["--suite", "sqllogic", "--suite", "gtest", "--changed", "origin/main"],
     "serened serenedb-tests"),
    ("tsan", ["--suite", "gtest", "--suite", "re2", "--suite", "duckdb", "--duckdb-suite", "core"],
     "serened re2-tests serenedb-tests unittest"),
])
def test_a_build_makes_only_the_binaries_of_the_suites_it_tests(config, args, targets):
    script = 'source scripts/ci/config.sh && sdb_config "$@" && echo "${TARGETS[*]}"'
    res = subprocess.run(["bash", "-c", script, "config.sh", config, *args], cwd=ROOT,
                         capture_output=True, text=True, check=True)
    assert res.stdout.strip() == targets


def test_the_bugfix_check_fails_a_changed_test_that_did_not_run():
    sched = scheduler.Scheduler.__new__(scheduler.Scheduler)
    sched.results = [Result("a", "sqllogic", PASSED, 1, 0, 0), Result("b", "sqllogic", FAILED, 1, 0, 0),
                     Result("c", "duckdb", SKIPPED, 0, 0, 0, "no result: the batch timed out"),
                     Result("d", "sqllogic", FAILED, 1, 0, 0)]
    sched.invert({"a", "b", "c"})
    assert [r.status for r in sched.results] == [FAILED, PASSED, FAILED, SKIPPED]


def test_a_unit_joins_the_shard_that_starts_its_fixture_unless_that_shard_is_far_ahead():
    def plan(plain_cost):
        units = [unit("plain:0", cost=plain_cost), unit("plain:1", cost=650.0),
                 *(unit(f"iceberg:{i}", fixtures=("duckdb:iceberg",), cost=50.0) for i in range(2))]
        shards = [{u.id for u in cli.shard(units, i, 2, lambda f: 600.0)} for i in range(2)]
        assert shards[0] | shards[1] == {u.id for u in units} and not shards[0] & shards[1]
        return [sorted(s & {"iceberg:0", "iceberg:1"}) for s in shards]

    assert plan(650.0) == [["iceberg:0", "iceberg:1"], []]
    assert plan(2000.0) == [[], ["iceberg:0", "iceberg:1"]]


def test_shards_balance_cpu_seconds():
    units = [unit("wide", cost=100.0, cpus=4), *(unit(f"narrow:{i}", cost=100.0) for i in range(4))]
    shards = [{u.id for u in cli.shard(units, i, 2)} for i in range(2)]
    assert {"wide"} in shards


def test_a_failure_after_the_run_was_interrupted_is_not_a_failure():
    out = scheduler._interrupted_outcome(Outcome(FAILED, "exit None", members={
        "m:a": (PASSED, 1.0, ""), "m:b": (FAILED, 0.0, "exit None")}))
    assert out.status == INTERRUPTED
    assert out.members == {"m:a": (PASSED, 1.0, ""), "m:b": (INTERRUPTED, 0.0, "the run was interrupted")}
    assert scheduler._interrupted_outcome(Outcome(PASSED)).status == PASSED


def test_test_durations_below_the_clock_resolution_are_not_zero():
    results, _ = gtest.parse_log("[ RUN      ] A.b\n[       OK ] A.b (0 ms)\n")
    assert results["A.b"][1] > 0
    members = [("/w/third_party/duckdb_avro/test/sql/a.test", "test/sql/a.test")]
    out, _, _ = duckdb._parse_events(
        "[1/1] (100%): /w/third_party/duckdb_avro/test/sql/a.test took 0.000s\n"
        '[TEST_EVENT] {"event":"end","name":"/w/third_party/duckdb_avro/test/sql/a.test","status":"ok"}\n',
        members, "duckdb:avro")
    assert out["duckdb:avro:test/sql/a.test"][0] == PASSED
    assert out["duckdb:avro:test/sql/a.test"][1] > 0


def test_an_untimed_gtest_runs_alone_and_timed_ones_batch(monkeypatch):
    timing = Timing({"gtest:b:A.timed": 1.0, "gtest:b:B.timed": 1.0})
    ctx = SimpleNamespace(timing=timing, skips=[], timeout_floor=180.0,
                          config=SimpleNamespace(slowdown=1.0),
                          build=SimpleNamespace(bin=lambda name: Path("/bin") / name), base_env={})
    monkeypatch.setattr(gtest, "list_tests", lambda *args: ["A.timed", "B.timed", "C.new", "D.new"])
    units = gtest.GtestBinary(ctx, "b").units([])
    assert [u.members for u in units] == [("gtest:b:A.timed", "gtest:b:B.timed"), ("gtest:b:C.new",),
                                          ("gtest:b:D.new",)]


def test_an_inference_test_waits_for_the_shared_model_server(monkeypatch, tmp_path):
    base = tmp_path / "tests" / "sqllogic" / "sdb"
    base.mkdir(parents=True)
    (base / "a.test").write_text("")
    (base / "b_ollama.test_slow").write_text("")
    monkeypatch.setattr(sqllogic.subprocess, "run", lambda *args, **kwargs: None)
    ctx = SimpleNamespace(root=tmp_path, timing=Timing({}), skips=[], timeout_floor=180.0,
                          config=SimpleNamespace(slowdown=1.0))
    args = SimpleNamespace(server=None, corpus=False, fast=False, biglake=False, override=False)
    units = {u.id: u for u in sqllogic.sqllogic_units(ctx, args, [])}
    assert units["sqllogic:pg-wire-simple:sdb/a.test"].timeout == 180.0
    assert units["sqllogic:pg-wire-simple:sdb/b_ollama.test_slow"].timeout == sqllogic.INFERENCE_TIMEOUT


def test_extension_test_ids_do_not_depend_on_the_checkout():
    ctx = SimpleNamespace(root=Path("/somewhere/serenedb"))
    assert duckdb._relative(ctx, "avro", "/somewhere/serenedb/third_party/duckdb_avro/test/sql/a.test") \
        == "test/sql/a.test"
    assert duckdb._relative(ctx, "core", "test/sql/a.test") == "test/sql/a.test"


def test_only_the_changed_postgres_tests_run_beside_every_other_suite(monkeypatch, tmp_path):
    units = [unit("postgres:pg-wire-simple:pg/a.test"), unit("postgres:pg-wire-simple:pg/b.test"),
             unit("sqllogic:pg-wire-simple:sdb/c.test"), unit("recovery:recovery/d.test")]
    picked, _ = changed(monkeypatch, tmp_path, {"tests/sqllogic/pg/a.test": ""}, units)
    assert cli.keep_changed_postgres(units, picked) == [units[0], units[2], units[3]]


@pytest.mark.parametrize("postgres,expected", [
    (None, ""), ("changed", "--postgres changed --changed origin/main"), ("all", "--postgres all")])
def test_select_suites_adds_the_postgres_validation(capsys, postgres, expected):
    ci.cmd_select_suites(SimpleNamespace(corpus=False, everything=True, iresearch=False, re2=False,
                                         duckdb_suites="", no_duckdb=False, postgres=postgres))
    assert capsys.readouterr().out.strip() == expected


def test_a_single_shard_runs_in_the_build_job_and_more_go_to_test_jobs():
    def shards(count, check=""):
        prefix = f"{check}-" if check else ""
        return [{"spec": f"{i}/{count}", "name": f"{prefix}{i}of{count}", "plan": f"p{check}",
                 "check": check} for i in range(1, count + 1)]

    one = ci.plan_runs(shards(1), shards(1, "biglake"))
    assert one["inline"] and one["names"] == ["1of1", "biglake-1of1"]
    assert one["runs"]["biglake-1of1"] == {"spec": "1/1", "plan": "pbiglake", "check": "biglake"}
    three = ci.plan_runs(shards(3))
    assert not three["inline"] and three["names"] == ["1of3", "2of3", "3of3"]


def test_timing_learns_main_or_a_calibration_pr():
    main = "40278761535-1-dev-x86_64-1of3.parquet"
    pr = "40278761535-1-pr1880-dev-x86_64-1of3.parquet"
    other = "40278761535-1-pr1932-dev-x86_64-1of3.parquet"

    def learns(pr_number):
        return [ci._learns_from(name, SimpleNamespace(pr=pr_number)) for name in (main, pr, other)]

    assert learns("") == [True, False, False]
    assert learns("1880") == [False, True, False]
