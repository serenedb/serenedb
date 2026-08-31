import pytest

from model import ABSENT, Model, ModelError, Outcome

KEY = ("table", "public.w0_t1")
OTHER = ("table", "public.w0_t2")


def fresh():
    m = Model()
    m.declare_owned(KEY)
    return m


def test_committed_create_is_single_candidate():
    m = fresh()
    m.apply_create(KEY, "tok1", Outcome.COMMITTED)
    assert m.candidates(KEY) == frozenset({"tok1"})
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
    assert m.candidates(KEY) == frozenset({"tok1"})


@pytest.mark.parametrize(
    "outcome",
    [Outcome.UNKNOWN_CRASH, Outcome.UNKNOWN_CANCEL, Outcome.UNKNOWN_TIMEOUT],
)
def test_ambiguous_create_admits_both_states(outcome):
    m = fresh()
    m.apply_create(KEY, "tok1", outcome)
    assert m.candidates(KEY) == frozenset({ABSENT, "tok1"})
    assert m.ambiguous_op_count() == 1


@pytest.mark.parametrize("observed", [ABSENT, "tok1"])
def test_ambiguous_collapses_to_either_member_without_finding(observed):
    m = fresh()
    m.apply_create(KEY, "tok1", Outcome.UNKNOWN_CRASH)
    findings = m.collapse({KEY: observed} if observed is not ABSENT else {})
    assert findings == []
    assert m.candidates(KEY) == frozenset({observed})


def test_ambiguous_resolving_to_third_state_is_a_finding():
    m = fresh()
    m.apply_create(KEY, "tok1", Outcome.UNKNOWN_CRASH)
    findings = m.collapse({KEY: "someone_elses_token"})
    assert len(findings) == 1
    assert findings[0].kind == "ambiguous_resolved_to_third_state"
    assert findings[0].observed == "someone_elses_token"


def test_committed_create_that_vanished_is_a_finding():
    m = fresh()
    m.apply_create(KEY, "tok1", Outcome.COMMITTED)
    findings = m.collapse({})
    assert len(findings) == 1
    assert findings[0].kind == "model_disagreement_missing"


def test_committed_drop_that_is_still_present_is_a_finding():
    m = fresh()
    m.apply_create(KEY, "tok1", Outcome.COMMITTED)
    m.collapse({KEY: "tok1"})
    m.apply_drop(KEY, Outcome.COMMITTED)
    findings = m.collapse({KEY: "tok1"})
    assert len(findings) == 1
    assert findings[0].kind == "model_disagreement_unexpected_present"


def test_wrong_content_is_a_finding_even_when_present():
    m = fresh()
    m.apply_create(KEY, "tok1", Outcome.COMMITTED)
    findings = m.collapse({KEY: "tok_other"})
    assert len(findings) == 1
    assert findings[0].kind == "model_disagreement_wrong_content"


def test_shared_keys_are_not_modelled():
    m = Model()
    m.declare_shared(OTHER)
    m.apply_create(OTHER, "tok", Outcome.COMMITTED)
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
    assert m.collapse({KEY: "tok1"}) == []
    assert m.collapse({KEY: "tok1"}) == []


def test_expected_present_skips_ambiguous_keys():
    m = fresh()
    m.declare_owned(OTHER)
    m.apply_create(KEY, "tok1", Outcome.COMMITTED)
    m.apply_create(OTHER, "tok2", Outcome.UNKNOWN_CRASH)
    assert m.expected_present() == {KEY: "tok1"}
