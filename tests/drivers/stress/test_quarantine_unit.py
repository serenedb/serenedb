import datetime

import pytest

import quarantine

TODAY = datetime.date(2026, 9, 1)
LATER = datetime.date(2027, 1, 1)

WEDGE = {"kind": "server_wedged", "detail": "125 threads, frozen=True, wchan={...}"}
OTHER = {"kind": "model_disagreement_missing", "detail": "table gone"}


def entry(**over):
    raw = {
        "id": "KW-1061",
        "kinds": ["server_wedged", "quiesce_abandoned_server_unhealthy"],
        "why": "catalog-commit lock inversion, fix unmerged",
        "issue": "https://example/1061",
        "expires": "2026-12-31",
        "fingerprint": {"detail_contains": ["frozen=True"], "scenarios": ["tables_only"]},
    }
    raw.update(over)
    return quarantine.Entry(raw)


def test_a_matching_finding_is_quarantined_not_surfaced():
    surviving, quarantined = quarantine.apply(
        [WEDGE], [entry()], "tables_only", 4, today=TODAY)
    assert surviving == []
    assert len(quarantined) == 1
    assert quarantined[0]["quarantined_by"] == "KW-1061"


def test_an_unrelated_finding_is_never_quarantined():
    surviving, quarantined = quarantine.apply(
        [OTHER], [entry()], "tables_only", 4, today=TODAY)
    assert [f["kind"] for f in surviving if "quarantine" not in f["kind"]] == [
        "model_disagreement_missing"]
    assert quarantined == []


def test_a_wrong_scenario_does_not_match():
    surviving, quarantined = quarantine.apply(
        [WEDGE], [entry()], "serial_churn", 4, today=TODAY)
    assert quarantined == []
    assert surviving[0]["kind"] == "server_wedged"


def test_a_detail_that_does_not_match_the_fingerprint_survives():
    finding = {"kind": "server_wedged", "detail": "something else entirely"}
    surviving, quarantined = quarantine.apply(
        [finding], [entry()], "tables_only", 4, today=TODAY)
    assert quarantined == []
    assert surviving[0]["kind"] == "server_wedged"


def test_below_min_workers_does_not_match():
    e = entry(fingerprint={"detail_contains": ["frozen=True"],
                           "scenarios": ["tables_only"], "min_workers": 8})
    surviving, quarantined = quarantine.apply([WEDGE], [e], "tables_only", 2, today=TODAY)
    assert quarantined == []


def test_an_expired_entry_lets_the_finding_through_and_reports_itself():
    surviving, quarantined = quarantine.apply(
        [WEDGE], [entry()], "tables_only", 4, today=LATER)
    assert quarantined == []
    kinds = [f["kind"] for f in surviving]
    assert "server_wedged" in kinds
    assert "quarantine_entry_expired" in kinds


def test_an_entry_that_stops_reproducing_is_reported():
    surviving, quarantined = quarantine.apply(
        [], [entry()], "tables_only", 4, today=TODAY)
    assert [f["kind"] for f in surviving] == ["quarantine_no_longer_reproduces"]


def test_a_fingerprintless_entry_is_rejected_outright():
    with pytest.raises(quarantine.QuarantineError):
        entry(fingerprint={})


def test_a_bad_expiry_is_rejected():
    with pytest.raises(quarantine.QuarantineError):
        entry(expires="not-a-date")


def test_missing_required_fields_are_rejected():
    with pytest.raises(quarantine.QuarantineError):
        quarantine.Entry({"id": "x", "kinds": ["server_wedged"]})


def test_the_checked_in_list_parses_and_every_entry_is_well_formed():
    entries = quarantine.load()
    for e in entries:
        assert e.detail_contains or e.scenarios
        assert e.expires
