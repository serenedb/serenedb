import enum

SERIALIZATION_FAILURE = "40001"
OBJECT_IN_USE = "55006"
UNDEFINED_TABLE = "42P01"
UNDEFINED_OBJECT = "42704"
UNDEFINED_SCHEMA = "3F000"
DUPLICATE_TABLE = "42P07"
DUPLICATE_OBJECT = "42710"
DUPLICATE_SCHEMA = "42P06"
DEPENDENT_OBJECTS_STILL_EXIST = "2BP01"
UNIQUE_VIOLATION = "23505"
QUERY_CANCELED = "57014"
INTERNAL_ERROR = "XX000"
IN_FAILED_SQL_TRANSACTION = "25P02"
INSUFFICIENT_PRIVILEGE = "42501"
IO_ERROR = "58030"
CONNECTION_EXCEPTION = "08000"
UNDEFINED_COLUMN = "42703"
UNDEFINED_FUNCTION = "42883"
INVALID_CATALOG_NAME = "3D000"
DUPLICATE_DATABASE = "42P04"
DUPLICATE_FUNCTION = "42723"
INVALID_GRANT_OPERATION = "0LP01"
WARNING = "01000"
INVALID_PARAMETER_VALUE = "22023"

# Every 40001 text observed. The code alone is not enough: some 40001s here are
# permanent (below), so the message decides.
CONFLICT_MARKERS = (
    "concurrent DDL",
    "concurrent update",
    "concurrent delete",
    "Catalog write-write conflict",
    "because a dependency was created after the transaction started",
    "has been altered by a different transaction",
    "but another transaction has altered this table",
    "there are other write transactions active",
)

ENGINE_CONFLICT_MARKERS = (
    "Cannot create index with outstanding updates",
    "write-write conflict",
    "Conflict on tuple deletion",
    "Conflict on update",
)

# 40001 texts that can never succeed on retry. PostgreSQL would use 0A000 / 25006 /
# 25001 for these; a client obeying the class-40 retry convention loops forever.
PERMANENT_CONFLICT_MARKERS = (
    "only write to a single attached database",
    "read-only mode",
    "the current transaction has transaction local changes",
)

# Codes that mean "the object I named is not there". The probe found these are not
# interchangeable per object -- one index name yields 42P01 from DROP INDEX, 42704 from
# REINDEX and 42704 from VACUUM (REFRESH_INDEX) -- so they are grouped by meaning, not
# by kind, and the verdict comes from whether the key was private or shared.
VANISHED_STATES = frozenset({
    UNDEFINED_TABLE, UNDEFINED_OBJECT, UNDEFINED_SCHEMA, UNDEFINED_FUNCTION,
    INVALID_CATALOG_NAME,
})

# Same-name races. The code depends on the object kind: 42P07 relations, 42710 roles and
# types, 42P06 schemas, 42P04 databases, 42723 macros.
DUPLICATE_STATES = frozenset({
    DUPLICATE_TABLE, DUPLICATE_OBJECT, DUPLICATE_SCHEMA, DUPLICATE_DATABASE,
    DUPLICATE_FUNCTION,
})

# The shape of an object changed underneath the statement. Legitimate when another
# worker owns the object; a bug when the key is the worker's own.
SHAPE_STATES = frozenset({UNDEFINED_COLUMN})

# `SET sdb_faults` raises this when a point is already armed or already gone, which two
# workers racing the broker can produce. Any OTHER 22023 stays a finding.
FAULT_ARMING_MARKERS = (
    "already set",
    "not set",
)

CRASH_FAULT_PREFIXES = ("crash_", "shutdown_")

# The catalog_append_fails injector surfaces as 40001, not as the 58030 the throw site
# suggests -- verified. Keyed on the armed fault so it is never excused otherwise.
INJECTED_ABORT_MARKER = "catalog log: could not append the transaction"
CATALOG_APPEND_FAILS = "catalog_append_fails"


class Verdict(enum.Enum):
    EXPECTED = "expected"
    FINDING = "finding"


class Classification:
    __slots__ = ("verdict", "label", "retryable")

    def __init__(self, verdict, label, retryable=False):
        self.verdict = verdict
        self.label = label
        self.retryable = retryable

    @property
    def is_finding(self):
        return self.verdict is Verdict.FINDING

    def __repr__(self):
        return f"Classification({self.verdict.value}, {self.label}, retryable={self.retryable})"


EXPECTED_LABELS = frozenset({
    "ok",
    "injected_crash",
    "planned_restart",
    "concurrent_ddl_conflict",
    "engine_transaction_conflict",
    "object_in_use",
    "shared_key_vanished",
    "shared_key_duplicate",
    "shared_key_dependents",
    "shared_key_shape_changed",
    "grant_cycle_refused",
    "warning_only",
    "fault_arming_race",
    "cancelled",
    "aborted_block",
    "injected_append_failure",
})

FINDING_LABELS = frozenset({
    "permanent_40001_reported_retryable",
    "conflict_without_marker",
    "private_key_vanished",
    "private_key_duplicate",
    "private_key_dependents",
    "private_key_shape_changed",
    "internal_error",
    "unexpected_connection_loss",
    "unexpected_cancellation",
    "unclassified",
})


def _expected(label, retryable=False):
    assert label in EXPECTED_LABELS, label
    return Classification(Verdict.EXPECTED, label, retryable)


def _finding(label):
    assert label in FINDING_LABELS, label
    return Classification(Verdict.FINDING, label)


def has_crash_fault(faults_armed):
    return any(
        name.startswith(CRASH_FAULT_PREFIXES) for name in (faults_armed or ())
    )


def classify(sqlstate, message="", op_kind="", key_scope="private",
             faults_armed=(), dead_connection=False, cancel_requested=False,
             planned_downtime=False):
    msg = message or ""
    shared = key_scope == "shared"

    if dead_connection:
        if has_crash_fault(faults_armed):
            return _expected("injected_crash")
        if planned_downtime:
            return _expected("planned_restart")
        return _finding("unexpected_connection_loss")

    if sqlstate is None:
        return _expected("ok")

    if sqlstate == SERIALIZATION_FAILURE:
        if INJECTED_ABORT_MARKER in msg \
                and CATALOG_APPEND_FAILS in (faults_armed or ()):
            return _expected("injected_append_failure")
        if any(m in msg for m in PERMANENT_CONFLICT_MARKERS):
            return _finding("permanent_40001_reported_retryable")
        if any(m in msg for m in CONFLICT_MARKERS):
            return _expected("concurrent_ddl_conflict", retryable=True)
        if any(m in msg for m in ENGINE_CONFLICT_MARKERS):
            return _expected("engine_transaction_conflict", retryable=True)
        return _finding("conflict_without_marker")

    if sqlstate == IO_ERROR and INJECTED_ABORT_MARKER in msg \
            and CATALOG_APPEND_FAILS in (faults_armed or ()):
        return _expected("injected_append_failure")

    if sqlstate == OBJECT_IN_USE:
        return _expected("object_in_use", retryable=True)

    if sqlstate == QUERY_CANCELED:
        if cancel_requested:
            return _expected("cancelled")
        return _finding("unexpected_cancellation")

    if sqlstate == IN_FAILED_SQL_TRANSACTION:
        return _expected("aborted_block")

    if sqlstate in VANISHED_STATES:
        return _expected("shared_key_vanished") if shared else _finding("private_key_vanished")

    if sqlstate in DUPLICATE_STATES:
        return _expected("shared_key_duplicate") if shared else _finding("private_key_duplicate")

    if sqlstate in SHAPE_STATES:
        return _expected("shared_key_shape_changed") if shared \
            else _finding("private_key_shape_changed")

    if sqlstate == INVALID_GRANT_OPERATION:
        return _expected("grant_cycle_refused")

    if sqlstate == WARNING:
        return _expected("warning_only")

    if sqlstate == CONNECTION_EXCEPTION and "already exists" in msg:
        # CREATE SERVER IF NOT EXISTS does not short-circuit: it still attempts the
        # underlying ATTACH and fails with 08000 plus a binder error naming a
        # database. Losing a same-name race is a legitimate outcome, so the verdict
        # follows the condition; the wrong sqlstate is a defect in its own right and
        # is reported once in the findings write-up rather than once per occurrence.
        return _expected("shared_key_duplicate") if shared \
            else _finding("private_key_duplicate")

    if sqlstate == INVALID_PARAMETER_VALUE:
        # 22023 is used for several unrelated things here. Only the shapes another
        # session can legitimately cause are excused; a binding failure or a bad
        # option value stays a finding.
        if any(m in msg for m in FAULT_ARMING_MARKERS):
            return _expected("fault_arming_race")
        if "does not exist" in msg or "non-existent" in msg:
            return _expected("shared_key_vanished") if shared \
                else _finding("private_key_vanished")
        if "already exists" in msg:
            return _expected("shared_key_duplicate") if shared \
                else _finding("private_key_duplicate")
        return _finding("unclassified")

    if sqlstate == DEPENDENT_OBJECTS_STILL_EXIST:
        return _expected("shared_key_dependents") if shared else _finding("private_key_dependents")

    if sqlstate == INTERNAL_ERROR:
        return _finding("internal_error")

    return _finding("unclassified")
