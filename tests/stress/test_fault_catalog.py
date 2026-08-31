import ast
import os
import pathlib

import pytest

import faults

REPO = pathlib.Path(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))
CHECKER = REPO / "scripts" / "check_fault_points.py"


@pytest.fixture(scope="module")
def defined():
    found = faults.source_defined_faults(REPO)
    assert found, "no fault points found in server/ or libs/"
    return found


def _checker_set(name):
    tree = ast.parse(CHECKER.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    return set(ast.literal_eval(node.value))
    raise AssertionError(f"{name} not found in {CHECKER}")


def test_every_declared_fault_exists_in_source(defined):
    missing = sorted(f for f in faults.ALL_FAULTS if f not in defined)
    assert not missing, (
        "declared faults with no SDB_IF_FAILURE / SDB_WAIT_ON_FAILURE literal in "
        f"server/ or libs/: {missing}. SET sdb_faults accepts any string and would "
        "arm nothing, so this would silently disable the scenario."
    )


def test_declared_faults_do_not_retire_a_known_gap():
    known_untested = _checker_set("KNOWN_UNTESTED_SOURCE_FAULTS")
    overlap = sorted(faults.ALL_FAULTS & known_untested)
    assert not overlap, (
        "these faults are listed in KNOWN_UNTESTED_SOURCE_FAULTS, so merely "
        "declaring a constant here would satisfy check_fault_points.py rule 3 "
        f"without any scenario firing them: {overlap}. Arm them in a scenario "
        "and remove them from the checker's allowlist in the same change."
    )


def test_park_and_crash_sets_are_disjoint_and_declared():
    assert not (faults.PARK_FAULTS & faults.CRASH_FAULTS)
    for group in (faults.PARK_FAULTS, faults.CRASH_FAULTS,
                  faults.PROGRESS_GATED_FAULTS, faults.BACKGROUND_POOL_FAULTS):
        assert group <= faults.ALL_FAULTS


def test_progress_gated_faults_are_a_subset_of_park_faults():
    assert faults.PROGRESS_GATED_FAULTS <= faults.PARK_FAULTS


def test_crash_fault_names_match_the_classifier_prefixes():
    from classify import CRASH_FAULT_PREFIXES
    for name in faults.CRASH_FAULTS:
        assert name.startswith(CRASH_FAULT_PREFIXES), (
            f"{name} is in CRASH_FAULTS but classify() would not treat a "
            "connection loss under it as expected"
        )


def test_broker_refcounts_and_disarms_once():
    issued = []

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, sql):
            issued.append(sql)

    class FakeConn:
        closed = False
        autocommit = False

        def cursor(self):
            return FakeCursor()

        def close(self):
            FakeConn.closed = True

    broker = faults.FaultBroker(lambda: FakeConn(), defined={"f_a", "f_b"})
    name = "f_a"
    broker.arm(name)
    broker.arm(name)
    assert broker.armed() == frozenset({name})
    broker.disarm(name)
    assert broker.armed() == frozenset({name})
    broker.disarm(name)
    assert broker.armed() == frozenset()
    assert issued == [f"SET sdb_faults = '{name}'", f"SET sdb_faults = '-{name}'"]
    broker.disarm(name)
    assert issued[-1] == f"SET sdb_faults = '-{name}'"


def test_broker_refuses_an_undefined_fault():
    broker = faults.FaultBroker(lambda: None, defined={"f_a"})
    with pytest.raises(faults.FaultUnavailable):
        broker.arm("typo_not_a_real_fault")
