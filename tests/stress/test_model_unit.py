import pytest

from model import ABSENT, Model, ModelError, Outcome, Present

KEY = ("table", "public.w0_t1")
OTHER = ("table", "public.w0_t2")


def fresh():
    m = Model()
    m.declare_owned(KEY)
    return m


def test_committed_create_is_single_candidate():
    m = fresh()
    m.apply_create(KEY, "tok1", Outcome.COMMITTED)
    assert m.candidates(KEY) == frozenset({Present("tok1")})
    assert m.ambiguous_keys() == {}


def test_refused_create_leaves_absent():
    m = fresh()
    m.apply_create(KEY, "tok1", Outcome.REFUSED_CONFLICT)
    assert m.candidates(KEY) == frozenset({ABSENT})
    assert m.collapse({}) == []


def test_refused_permanent_leaves_state_untouched():
    m = fresh()
    m.apply_create(KEY, "tok1", Outcome.COMMITTED)
    m.apply_drop(KEY, Outcome.REFUSED_PERMANENT)
    assert m.candidates(KEY) == frozenset({Present("tok1")})


@pytest.mark.parametrize(
    "outcome",
    [Outcome.UNKNOWN_CRASH, Outcome.UNKNOWN_CANCEL, Outcome.UNKNOWN_TIMEOUT],
)
def test_ambiguous_create_admits_both_states(outcome):
    m = fresh()
    m.apply_create(KEY, "tok1", outcome)
    assert m.candidates(KEY) == frozenset({ABSENT, Present("tok1")})
    assert m.ambiguous_op_count() == 1


@pytest.mark.parametrize("observed", [ABSENT, Present("tok1")])
def test_ambiguous_collapses_to_either_member_without_finding(observed):
    m = fresh()
    m.apply_create(KEY, "tok1", Outcome.UNKNOWN_CRASH)
    findings = m.collapse({KEY: observed} if observed is not ABSENT else {})
    assert findings == []
    assert m.candidates(KEY) == frozenset({observed})


def test_ambiguous_resolving_to_third_state_is_a_finding():
    m = fresh()
    m.apply_create(KEY, "tok1", Outcome.UNKNOWN_CRASH)
    findings = m.collapse({KEY: Present("someone_elses_token")})
    assert len(findings) == 1
    assert findings[0].kind == "ambiguous_resolved_to_third_state"


def test_committed_create_that_vanished_is_a_finding():
    m = fresh()
    m.apply_create(KEY, "tok1", Outcome.COMMITTED)
    findings = m.collapse({})
    assert len(findings) == 1
    assert findings[0].kind == "model_disagreement_missing"


def test_committed_drop_that_is_still_present_is_a_finding():
    m = fresh()
    m.apply_create(KEY, "tok1", Outcome.COMMITTED)
    m.collapse({KEY: Present("tok1")})
    m.apply_drop(KEY, Outcome.COMMITTED)
    findings = m.collapse({KEY: Present("tok1")})
    assert len(findings) == 1
    assert findings[0].kind == "model_disagreement_unexpected_present"


def test_wrong_token_is_a_finding_even_when_present():
    m = fresh()
    m.apply_create(KEY, "tok1", Outcome.COMMITTED)
    findings = m.collapse({KEY: Present("tok_other")})
    assert len(findings) == 1
    assert findings[0].kind == "model_disagreement_wrong_content"


def test_right_object_with_wrong_rows_is_its_own_finding():
    m = fresh()
    m.apply_create(KEY, "tok1", Outcome.COMMITTED, rows={"r1", "r2"})
    findings = m.collapse({KEY: Present("tok1", {"r1"})})
    assert len(findings) == 1
    assert findings[0].kind == "model_disagreement_wrong_rows", (
        "a delayed commit that kept the definition but lost rows must be "
        "distinguishable from a wrong object"
    )


def test_committed_row_changes_are_tracked_exactly():
    m = fresh()
    m.apply_create(KEY, "tok1", Outcome.COMMITTED, rows={"r1"})
    m.apply_rows(KEY, added={"r2", "r3"}, outcome=Outcome.COMMITTED)
    m.apply_rows(KEY, removed={"r1"}, outcome=Outcome.COMMITTED)
    assert m.rows_of(KEY) == frozenset({"r2", "r3"})
    assert m.collapse({KEY: Present("tok1", {"r2", "r3"})}) == []


def test_refused_row_change_leaves_rows_untouched():
    m = fresh()
    m.apply_create(KEY, "tok1", Outcome.COMMITTED, rows={"r1"})
    m.apply_rows(KEY, added={"r2"}, outcome=Outcome.REFUSED_CONFLICT)
    assert m.rows_of(KEY) == frozenset({"r1"})


def test_ambiguous_row_change_admits_both_row_sets():
    m = fresh()
    m.apply_create(KEY, "tok1", Outcome.COMMITTED, rows={"r1"})
    m.apply_rows(KEY, added={"r2"}, outcome=Outcome.UNKNOWN_CRASH)
    assert m.candidates(KEY) == frozenset({
        Present("tok1", {"r1"}), Present("tok1", {"r1", "r2"})})
    assert m.collapse({KEY: Present("tok1", {"r1"})}) == []


def test_ambiguous_row_change_resolving_to_a_third_row_set_is_a_finding():
    m = fresh()
    m.apply_create(KEY, "tok1", Outcome.COMMITTED, rows={"r1"})
    m.apply_rows(KEY, added={"r2"}, outcome=Outcome.UNKNOWN_CRASH)
    findings = m.collapse({KEY: Present("tok1", {"r1", "r2", "r99"})})
    assert len(findings) == 1
    assert findings[0].kind == "ambiguous_resolved_to_third_state"


def test_row_changes_on_a_dropped_key_are_ignored():
    m = fresh()
    m.apply_create(KEY, "tok1", Outcome.COMMITTED, rows={"r1"})
    m.apply_drop(KEY, Outcome.COMMITTED)
    m.apply_rows(KEY, added={"r2"}, outcome=Outcome.COMMITTED)
    assert m.candidates(KEY) == frozenset({ABSENT})


def test_shared_keys_are_not_modelled():
    m = Model()
    m.declare_shared(OTHER)
    m.apply_create(OTHER, "tok", Outcome.COMMITTED)
    m.apply_rows(OTHER, added={"r1"}, outcome=Outcome.COMMITTED)
    assert m.candidates(OTHER) is None
    assert m.collapse({}) == []


def test_declaring_a_key_both_ways_is_rejected():
    m = fresh()
    with pytest.raises(ModelError):
        m.declare_shared(KEY)


def test_apply_on_undeclared_key_is_rejected():
    m = Model()
    with pytest.raises(ModelError):
        m.apply_create(KEY, "tok", Outcome.COMMITTED)


def test_unbounded_ambiguity_is_rejected():
    m = fresh()
    with pytest.raises(ModelError):
        for i in range(20):
            m.apply_create(KEY, f"tok{i}", Outcome.UNKNOWN_CRASH)


def test_collapse_is_idempotent():
    m = fresh()
    m.apply_create(KEY, "tok1", Outcome.COMMITTED)
    assert m.collapse({KEY: Present("tok1")}) == []
    assert m.collapse({KEY: Present("tok1")}) == []


def test_expected_present_skips_ambiguous_keys():
    m = fresh()
    m.declare_owned(OTHER)
    m.apply_create(KEY, "tok1", Outcome.COMMITTED)
    m.apply_create(OTHER, "tok2", Outcome.UNKNOWN_CRASH)
    assert m.expected_present() == {KEY: Present("tok1")}


def test_present_equality_and_hashing_are_value_based():
    assert Present("t", {"a"}) == Present("t", {"a"})
    assert Present("t", {"a"}) != Present("t", {"b"})
    assert Present("t", {"a"}) != Present("u", {"a"})
    assert len({Present("t", {"a"}), Present("t", {"a"})}) == 1


def test_resync_keeps_only_unambiguously_present_keys():
    import sys
    sys.path.insert(0, __import__("os").path.dirname(__file__))
    import ops
    import scenarios
    from ops import NameGen

    st = scenarios.WorkerState(NameGen("ab", 0))
    m = Model()
    live = ops.key_of(ops.TABLE, "sab_w0_t1")
    gone = ops.key_of(ops.TABLE, "sab_w0_t2")
    unsure = ops.key_of(ops.TABLE, "sab_w0_t3")
    for k in (live, gone, unsure):
        m.declare_owned(k)
    m.apply_create(live, "tok1", Outcome.COMMITTED, rows={"r1"})
    m.apply_create(gone, "tok2", Outcome.COMMITTED)
    m.apply_drop(gone, Outcome.COMMITTED)
    m.apply_create(unsure, "tok3", Outcome.UNKNOWN_CRASH)
    st.tables = [live, gone, unsure]
    st.rows = {live: {"stale"}, gone: set(), unsure: set()}

    kept = st.resync_from(m)
    assert st.tables == [live], "a dropped or ambiguous key must not stay targetable"
    assert kept == 1
    assert st.rows[live] == {"r1"}, "rows come back from the model, not the stale set"
