import coverage
import scenarios


def test_every_scenario_has_reachable_kinds():
    for name in scenarios.SCENARIOS:
        kinds = coverage.reachable_op_kinds(name, iterations=3000)
        assert kinds, f"{name} generated nothing"


def test_reachable_kinds_are_deterministic_for_a_seed():
    a = coverage.reachable_op_kinds("ddl_churn", iterations=3000, seed=5)
    b = coverage.reachable_op_kinds("ddl_churn", iterations=3000, seed=5)
    assert a == b


def test_a_family_that_never_fired_is_a_coverage_finding():
    reachable = coverage.reachable_op_kinds("tables_only", iterations=3000)
    partial = sorted(reachable)[:1]
    _data, findings = coverage.report(
        "tables_only", partial, [(100, 10.0), (100, 10.0)], [], [], 200, 2)
    kinds = [f["kind"] for f in findings]
    assert "coverage_op_family_never_attempted" in kinds


def test_full_coverage_reports_nothing():
    reachable = coverage.reachable_op_kinds("tables_only", iterations=20000)
    _data, findings = coverage.report(
        "tables_only", reachable, [(100, 10.0), (100, 10.0)], [], [], 200, 2)
    assert [f for f in findings if f["kind"].startswith("coverage_")] == []


def test_an_op_kind_the_generator_cannot_produce_is_drift():
    reachable = coverage.reachable_op_kinds("tables_only", iterations=20000)
    _data, findings = coverage.report(
        "tables_only", list(reachable) + ["create_wormhole"], [(100, 10.0), (100, 10.0)], [], [], 200, 2)
    assert "coverage_unexpected_op_kind" in [f["kind"] for f in findings]


def test_a_sliver_tail_window_is_ignored_not_reported_as_a_collapse():
    reachable = coverage.reachable_op_kinds("tables_only", iterations=20000)
    _d, findings = coverage.report(
        "tables_only", reachable,
        [(50167, 15.0), (2, 0.02)], [], [], 50169, 2)
    assert "insufficient_pressure" not in [f["kind"] for f in findings], (
        "the window after the final quiesce covers almost no time; comparing its "
        "raw count against a full window reports a collapse that never happened"
    )


def test_pressure_collapse_is_relative_not_absolute():
    reachable = coverage.reachable_op_kinds("tables_only", iterations=20000)
    _d, findings = coverage.report("tables_only", reachable, [(1000, 10.0), (5, 10.0)], [], [], 1005, 2)
    assert "insufficient_pressure" in [f["kind"] for f in findings]
    _d2, findings2 = coverage.report("tables_only", reachable, [(7, 10.0), (7, 10.0)], [], [], 14, 2)
    assert "insufficient_pressure" not in [f["kind"] for f in findings2], (
        "a genuinely slow but steady run must not trip the floor"
    )


def test_render_mentions_never_attempted_families():
    data, _ = coverage.report("tables_only", ["create_table"], [(10, 10.0), (10, 10.0)], [], [], 20, 2)
    text = coverage.render(data)
    assert "NEVER attempted" in text
