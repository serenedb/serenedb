import pytest

import classify as C
from classify import Verdict, classify

ALL_KNOWN_STATES = [
    C.SERIALIZATION_FAILURE, C.OBJECT_IN_USE, C.UNDEFINED_TABLE,
    C.UNDEFINED_OBJECT, C.UNDEFINED_SCHEMA, C.DUPLICATE_TABLE,
    C.DUPLICATE_OBJECT, C.DUPLICATE_SCHEMA, C.DEPENDENT_OBJECTS_STILL_EXIST,
    C.UNIQUE_VIOLATION, C.QUERY_CANCELED, C.INTERNAL_ERROR,
    C.IN_FAILED_SQL_TRANSACTION, C.INSUFFICIENT_PRIVILEGE,
]

UNKNOWN_STATES = ["08006", "53200", "22P02", "0A000", "58030", "XX001", "ZZ999", ""]


def test_success_is_expected():
    assert classify(None).verdict is Verdict.EXPECTED
    assert classify(None).label == "ok"


def test_conflict_with_marker_is_expected_and_retryable():
    c = classify(C.SERIALIZATION_FAILURE,
                 "could not serialize access due to concurrent DDL on the same object")
    assert c.verdict is Verdict.EXPECTED
    assert c.label == "concurrent_ddl_conflict"
    assert c.retryable


@pytest.mark.parametrize("marker", list(C.PERMANENT_CONFLICT_MARKERS))
def test_permanent_conflict_is_a_finding(marker):
    c = classify(C.SERIALIZATION_FAILURE, f"boom: {marker} here")
    assert c.verdict is Verdict.FINDING
    assert c.label == "permanent_40001_reported_retryable"
    assert not c.retryable


def test_conflict_without_any_marker_is_a_finding():
    c = classify(C.SERIALIZATION_FAILURE, "something else entirely")
    assert c.verdict is Verdict.FINDING
    assert c.label == "conflict_without_marker"


def test_object_in_use_is_expected_and_retryable():
    c = classify(C.OBJECT_IN_USE, "index is being reindexed")
    assert c.verdict is Verdict.EXPECTED
    assert c.retryable


@pytest.mark.parametrize("state", sorted(C.VANISHED_STATES))
def test_vanished_depends_on_key_scope(state):
    assert classify(state, key_scope="shared").verdict is Verdict.EXPECTED
    assert classify(state, key_scope="private").verdict is Verdict.FINDING


@pytest.mark.parametrize("state", sorted(C.DUPLICATE_STATES))
def test_duplicate_depends_on_key_scope(state):
    assert classify(state, key_scope="shared").verdict is Verdict.EXPECTED
    assert classify(state, key_scope="private").verdict is Verdict.FINDING


def test_dependent_objects_depends_on_key_scope():
    s = C.DEPENDENT_OBJECTS_STILL_EXIST
    assert classify(s, key_scope="shared").verdict is Verdict.EXPECTED
    assert classify(s, key_scope="private").verdict is Verdict.FINDING


def test_dead_connection_requires_an_armed_crash_fault():
    armed = ("crash_before_catalog_commit",)
    assert classify(None, dead_connection=True, faults_armed=armed).label == "injected_crash"
    c = classify(None, dead_connection=True, faults_armed=())
    assert c.verdict is Verdict.FINDING
    assert c.label == "unexpected_connection_loss"


def test_park_fault_does_not_excuse_a_connection_loss():
    c = classify(None, dead_connection=True, faults_armed=("pause_create_index_mid_build",))
    assert c.verdict is Verdict.FINDING


def test_cancellation_requires_a_requested_cancel():
    assert classify(C.QUERY_CANCELED, cancel_requested=True).verdict is Verdict.EXPECTED
    assert classify(C.QUERY_CANCELED, cancel_requested=False).verdict is Verdict.FINDING


def test_internal_error_is_always_a_finding():
    for scope in ("private", "shared"):
        assert classify(C.INTERNAL_ERROR, key_scope=scope).verdict is Verdict.FINDING


@pytest.mark.parametrize("state", UNKNOWN_STATES)
@pytest.mark.parametrize("scope", ["private", "shared"])
def test_there_is_no_default_expected_branch(state, scope):
    c = classify(state, "any message", key_scope=scope)
    assert c.verdict is Verdict.FINDING
    assert c.label == "unclassified"


@pytest.mark.parametrize("state", ALL_KNOWN_STATES + UNKNOWN_STATES + [None])
@pytest.mark.parametrize("scope", ["private", "shared"])
def test_every_label_is_declared(state, scope):
    c = classify(state, "msg", key_scope=scope)
    pool = C.EXPECTED_LABELS if c.verdict is Verdict.EXPECTED else C.FINDING_LABELS
    assert c.label in pool


def test_label_sets_are_disjoint():
    assert not (C.EXPECTED_LABELS & C.FINDING_LABELS)


def test_classification_is_deterministic():
    for state in ALL_KNOWN_STATES + UNKNOWN_STATES:
        a = classify(state, "m", key_scope="private")
        b = classify(state, "m", key_scope="private")
        assert (a.verdict, a.label, a.retryable) == (b.verdict, b.label, b.retryable)


@pytest.mark.parametrize("marker", list(C.ENGINE_CONFLICT_MARKERS))
def test_engine_transaction_conflicts_are_expected_and_retryable(marker):
    c = classify(C.SERIALIZATION_FAILURE, marker)
    assert c.verdict is Verdict.EXPECTED
    assert c.label == "engine_transaction_conflict"
    assert c.retryable


def test_serialization_failure_is_an_operational_error_subclass():
    import psycopg
    assert issubclass(psycopg.errors.SerializationFailure, psycopg.OperationalError), (
        "worker._execute must not classify a connection loss by exception class; "
        "40001 arrives as an OperationalError subclass"
    )


def test_a_connection_loss_during_a_planned_restart_is_expected():
    c = classify(None, dead_connection=True, faults_armed=(), planned_downtime=True)
    assert c.verdict is Verdict.EXPECTED
    assert c.label == "planned_restart"


def test_planned_downtime_does_not_excuse_an_ordinary_error():
    c = classify("53200", "out of memory", planned_downtime=True)
    assert c.verdict is Verdict.FINDING
    assert c.label == "unclassified", (
        "a planned restart window must excuse a dropped connection, not every "
        "error that happens to arrive during it"
    )


@pytest.mark.parametrize("state", [C.UNDEFINED_FUNCTION, C.INVALID_CATALOG_NAME])
def test_newly_grouped_vanished_codes_are_scope_dependent(state):
    assert classify(state, key_scope="shared").verdict is Verdict.EXPECTED
    assert classify(state, key_scope="private").verdict is Verdict.FINDING


@pytest.mark.parametrize("state", [C.DUPLICATE_DATABASE, C.DUPLICATE_FUNCTION])
def test_newly_grouped_duplicate_codes_are_scope_dependent(state):
    assert classify(state, key_scope="shared").verdict is Verdict.EXPECTED
    assert classify(state, key_scope="private").verdict is Verdict.FINDING


def test_a_shape_change_is_expected_only_on_a_shared_key():
    assert classify(C.UNDEFINED_COLUMN, key_scope="shared").label == \
        "shared_key_shape_changed"
    c = classify(C.UNDEFINED_COLUMN, key_scope="private")
    assert c.verdict is Verdict.FINDING
    assert c.label == "private_key_shape_changed", (
        "an undefined column on a table only this worker touches is a bug -- it is how "
        "the slow_ctas column-shape mismatch was caught"
    )


def test_a_grant_cycle_refusal_is_expected():
    assert classify(C.INVALID_GRANT_OPERATION).verdict is Verdict.EXPECTED


def test_the_warning_class_is_expected():
    assert classify(C.WARNING, "permission denied to refresh, skipping it").label == \
        "warning_only"


def test_only_fault_arming_22023_is_excused():
    assert classify(C.INVALID_PARAMETER_VALUE,
                    "failure point 'x' already set").label == "fault_arming_race"
    assert classify(C.INVALID_PARAMETER_VALUE,
                    "failure point 'x' not set").label == "fault_arming_race"
    c = classify(C.INVALID_PARAMETER_VALUE, "Unsupported catalog type when binding")
    assert c.verdict is Verdict.FINDING, (
        "22023 is broad -- aclexplode's binding failure must not be excused by the "
        "same rule that excuses a fault-arming race"
    )


def test_the_verified_concurrency_expected_codes_no_longer_misfire():
    for state in ("22023", "42703", "01000", "0LP01", "3D000", "42883", "42P04"):
        msg = "already set" if state == "22023" else ""
        c = classify(state, msg, key_scope="shared")
        assert c.verdict is Verdict.EXPECTED, f"{state} still misfires"


@pytest.mark.parametrize("marker", list(C.CONFLICT_MARKERS) + list(C.ENGINE_CONFLICT_MARKERS))
def test_every_observed_retryable_40001_text_is_expected_and_retryable(marker):
    c = classify(C.SERIALIZATION_FAILURE, f"Failed to commit: {marker} blah")
    assert c.verdict is Verdict.EXPECTED
    assert c.retryable


@pytest.mark.parametrize("marker", list(C.PERMANENT_CONFLICT_MARKERS))
def test_every_observed_permanent_40001_text_is_a_finding(marker):
    c = classify(C.SERIALIZATION_FAILURE, f"boom: {marker}")
    assert c.verdict is Verdict.FINDING
    assert not c.retryable


def test_the_injected_append_failure_arrives_as_40001_not_58030():
    armed = (C.CATALOG_APPEND_FAILS,)
    c = classify(C.SERIALIZATION_FAILURE,
                 "Failed to commit: catalog log: could not append the transaction",
                 faults_armed=armed)
    assert c.verdict is Verdict.EXPECTED
    assert c.label == "injected_append_failure"


def test_the_append_failure_is_not_excused_when_the_fault_is_not_armed():
    c = classify(C.SERIALIZATION_FAILURE,
                 "Failed to commit: catalog log: could not append the transaction",
                 faults_armed=())
    assert c.verdict is Verdict.FINDING


def test_checkpoint_local_changes_is_permanent_not_retryable():
    c = classify(C.SERIALIZATION_FAILURE,
                 "Cannot CHECKPOINT: the current transaction has transaction local changes")
    assert c.verdict is Verdict.FINDING
    assert c.label == "permanent_40001_reported_retryable"


def test_checkpoint_other_writers_is_retryable():
    c = classify(C.SERIALIZATION_FAILURE,
                 "Cannot CHECKPOINT: there are other write transactions active.")
    assert c.verdict is Verdict.EXPECTED
    assert c.retryable


def test_22023_dispatches_on_message_not_just_code():
    assert classify(C.INVALID_PARAMETER_VALUE,
                    "role \"nosuchrole\" does not exist",
                    key_scope="shared").label == "shared_key_vanished"
    assert classify(C.INVALID_PARAMETER_VALUE,
                    "Temporary secret with name 'x' already exists!",
                    key_scope="shared").label == "shared_key_duplicate"
    assert classify(C.INVALID_PARAMETER_VALUE,
                    "invalid value for parameter \"refresh_interval\": \"-1\"",
                    key_scope="shared").verdict is Verdict.FINDING


def test_create_server_if_not_exists_losing_a_race_follows_the_condition():
    msg = ('could not connect foreign server "x": Binder Error: Failed to attach '
           'database: database with name "x" already exists')
    assert classify(C.CONNECTION_EXCEPTION, msg, key_scope="shared").label == \
        "shared_key_duplicate"
    assert classify(C.CONNECTION_EXCEPTION, msg, key_scope="private").verdict is \
        Verdict.FINDING


def test_a_real_connection_failure_is_still_a_finding():
    c = classify(C.CONNECTION_EXCEPTION, "could not connect to host 10.0.0.1: timeout",
                 key_scope="shared")
    assert c.verdict is Verdict.FINDING, (
        "only the already-exists shape is a legitimate race; an actual connection "
        "failure must not be excused by it"
    )
