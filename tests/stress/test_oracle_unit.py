import pytest

import ops
import oracle
import snapshot as snap_mod
from model import Model, Outcome

RUN_TAG = "abcd"
T1 = (ops.TABLE, "sabcd_w0_t1")
T2 = (ops.TABLE, "sabcd_w0_t2")
V1 = (ops.VIEW, "sabcd_w0_v1")
I1 = (ops.INDEX, "sabcd_w0_i1")


class FakeCursor:
    def __init__(self, row):
        self._row = row

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql):
        pass

    def fetchone(self):
        return self._row


class FakeConn:
    def __init__(self, visible_all=True):
        self._row = (visible_all,)

    def cursor(self):
        return FakeCursor(self._row)


def snap(pg=None, sets=None, tokens=None, edges=(), oids=None, orphans=()):
    s = snap_mod.Snapshot(RUN_TAG)
    s.pg_objects = dict(pg or {})
    s.set_objects = dict(sets or {})
    s.pg_tokens = dict(tokens or {})
    s.edges = list(edges)
    s.all_oids = set(oids if oids is not None
                     else list(s.set_objects.values()) + list(s.pg_objects.values()))
    s.orphan_files = list(orphans)
    return s


def healthy():
    m = Model()
    m.declare_owned(T1)
    m.apply_create(T1, "tok1", Outcome.COMMITTED)
    s = snap(pg={T1: 100}, sets={T1: 100}, tokens={T1: "tok1"})
    return m, s


def kinds(findings):
    return sorted(f["kind"] for f in findings)


def run(models, s):
    return oracle.run_all(models, s, FakeConn(), oracle.OidRegistry())


def test_a_healthy_snapshot_produces_no_findings():
    m, s = healthy()
    assert run([m], s) == []


def test_mutant_committed_object_vanished():
    m, s = healthy()
    s.pg_objects.pop(T1)
    s.set_objects.pop(T1)
    s.pg_tokens.pop(T1)
    s.all_oids.discard(100)
    assert "model_disagreement_missing" in kinds(run([m], s))


def test_mutant_dropped_object_still_present():
    m = Model()
    m.declare_owned(T1)
    m.apply_create(T1, "tok1", Outcome.COMMITTED)
    m.collapse({T1: "tok1"})
    m.apply_drop(T1, Outcome.COMMITTED)
    s = snap(pg={T1: 100}, sets={T1: 100}, tokens={T1: "tok1"})
    assert "model_disagreement_unexpected_present" in kinds(run([m], s))


def test_mutant_content_token_rewritten_behind_the_model():
    m, s = healthy()
    s.pg_tokens[T1] = "someone_elses_token"
    got = kinds(run([m], s))
    assert "model_disagreement_wrong_content" in got, (
        "the oracle must notice a same-named object with the wrong content token, "
        "or drop-then-recreate is indistinguishable from survival"
    )


def test_mutant_ghost_entry_no_worker_created():
    m, s = healthy()
    s.pg_objects[T2] = 200
    s.set_objects[T2] = 200
    s.all_oids.add(200)
    assert "ghost_entry" in kinds(run([m], s))


def test_mutant_entry_present_in_port_but_not_in_pg_class():
    m, s = healthy()
    s.set_objects[V1] = 300
    s.all_oids.add(300)
    m.declare_owned(V1)
    m.apply_create(V1, "tokv", Outcome.COMMITTED)
    got = kinds(run([m], s))
    assert "entry_not_in_pg_class" in got


def test_mutant_pg_class_entry_missing_from_entry_port():
    m, s = healthy()
    s.pg_objects[V1] = 400
    s.pg_tokens[V1] = "tokv"
    s.all_oids.add(400)
    m.declare_owned(V1)
    m.apply_create(V1, "tokv", Outcome.COMMITTED)
    assert "pg_class_not_in_entry_port" in kinds(run([m], s))


def test_mutant_oid_mismatch_between_pg_class_and_entry_port():
    m, s = healthy()
    s.pg_objects[T1] = 999
    assert "oid_mismatch" in kinds(run([m], s))


def test_mutant_dangling_dependency_edges_both_directions():
    m, s = healthy()
    s.edges = [(100, 12345), (54321, 100)]
    got = kinds(run([m], s))
    assert "dangling_dependency_referenced" in got
    assert "dangling_dependency_dependent" in got


def test_mutant_orphan_artifact_on_disk():
    m, s = healthy()
    s.orphan_files = [("engine_duckdb", "777.db")]
    assert "orphan_artifact" in kinds(run([m], s))


def test_mutant_reissued_oid_is_detected_across_snapshots():
    reg = oracle.OidRegistry()
    m, s = healthy()
    assert reg.check(s) == []
    s2 = snap(pg={T2: 100}, sets={T2: 100}, tokens={T2: "tok2"})
    got = sorted(f["kind"] for f in reg.check(s2))
    assert "oid_reused" in got, "ids are never reused, so a reappearing oid is a finding"


def test_mutant_visible_contract_change_is_a_tripwire():
    m, s = healthy()
    findings = oracle.run_all([m], s, FakeConn(visible_all=False), oracle.OidRegistry())
    assert "visible_column_contract_changed" in kinds(findings)


def test_mutant_snapshot_query_failure_is_surfaced_not_swallowed():
    m, s = healthy()
    s.errors.append("sdb_catalog_sets scan failed: boom")
    assert "oracle_query_failed" in kinds(run([m], s))


def test_index_double_slot_is_not_reported_as_a_ghost():
    m = Model()
    m.declare_owned(I1)
    m.apply_create(I1, "toki", Outcome.COMMITTED)
    s = snap_mod.Snapshot(RUN_TAG)
    s.pg_objects = {I1: 500}
    s.pg_tokens = {I1: "toki"}
    s.set_objects = {I1: 500, (ops.TABLE, I1[1]): 500}
    for name in {n for (k, n) in s.set_objects if k == ops.INDEX}:
        s.set_objects.pop((ops.TABLE, name), None)
    s.all_oids = {500}
    assert run([m], s) == [], "an index's Table slot must not read as a ghost table"


def test_engine_derived_names_are_out_of_scope():
    m, s = healthy()
    s.pg_objects[(ops.INDEX, "sabcd_w0_t1_pkey")] = 600
    s.all_oids.add(600)
    assert run([m], s) == [], (
        "implicit *_pkey indexes are in pg_class but not in sdb_catalog_sets(); "
        "only generator-shaped names may be asserted on"
    )


def test_mutant_rows_lost_behind_the_models_back():
    from model import Present
    m = Model()
    m.declare_owned(T1)
    m.apply_create(T1, "tok1", Outcome.COMMITTED, rows={"r1", "r2", "r3"})
    s = snap(pg={T1: 100}, sets={T1: 100}, tokens={T1: "tok1"})
    s.row_tokens = {T1: frozenset({"r1", "r2"})}
    got = kinds(run([m], s))
    assert "model_disagreement_wrong_rows" in got, (
        "a definition that survived a crash while its rows did not is the "
        "post-flush-window assertion; it must not pass as healthy"
    )


def test_mutant_rows_gained_behind_the_models_back():
    m = Model()
    m.declare_owned(T1)
    m.apply_create(T1, "tok1", Outcome.COMMITTED, rows={"r1"})
    s = snap(pg={T1: 100}, sets={T1: 100}, tokens={T1: "tok1"})
    s.row_tokens = {T1: frozenset({"r1", "r_ghost"})}
    assert "model_disagreement_wrong_rows" in kinds(run([m], s))


def test_matching_rows_are_not_a_finding():
    m = Model()
    m.declare_owned(T1)
    m.apply_create(T1, "tok1", Outcome.COMMITTED, rows={"r1", "r2"})
    s = snap(pg={T1: 100}, sets={T1: 100}, tokens={T1: "tok1"})
    s.row_tokens = {T1: frozenset({"r1", "r2"})}
    assert run([m], s) == []
