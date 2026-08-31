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

CONFLICT_MARKERS = (
    "concurrent DDL",
    "concurrent update",
    "concurrent delete",
)

ENGINE_CONFLICT_MARKERS = (
    "Cannot create index with outstanding updates",
    "write-write conflict",
    "Conflict on tuple deletion",
)

PERMANENT_CONFLICT_MARKERS = (
    "only write to a single attached database",
    "read-only mode",
)

VANISHED_STATES = frozenset({UNDEFINED_TABLE, UNDEFINED_OBJECT, UNDEFINED_SCHEMA})
DUPLICATE_STATES = frozenset({DUPLICATE_TABLE, DUPLICATE_OBJECT, DUPLICATE_SCHEMA})

CRASH_FAULT_PREFIXES = ("crash_", "shutdown_")

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

    if sqlstate == DEPENDENT_OBJECTS_STILL_EXIST:
        return _expected("shared_key_dependents") if shared else _finding("private_key_dependents")

    if sqlstate == INTERNAL_ERROR:
        return _finding("internal_error")

    return _finding("unclassified")
