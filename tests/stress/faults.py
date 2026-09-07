import pathlib
import re
import threading

PAUSE_CREATE_INDEX_MID_BUILD_FAULT = "pause_create_index_mid_build"
PAUSE_CTAS_MID_INGEST_FAULT = "pause_ctas_mid_ingest"
PAUSE_VACUUM_MID_WALK_FAULT = "pause_vacuum_mid_walk"
COMPACT_INSIDE_DDL_FAULT = "compact_inside_ddl"
COMPACT_INSIDE_DROP_FAULT = "compact_inside_drop"
CATALOG_APPEND_FAILS_FAULT = "catalog_append_fails"
CRASH_BEFORE_CATALOG_COMMIT_FAULT = "crash_before_catalog_commit"
CRASH_AFTER_CATALOG_BEFORE_DATA_FAULT = "crash_after_catalog_before_data"
CRASH_ON_DROP_FAULT = "crash_on_drop"
CRASH_ON_PACKET_FAULT = "crash_on_packet"
CRASH_BEFORE_SEARCH_COMMIT_FAULT = "crash_before_search_commit"
CRASH_AFTER_SEARCH_COMMIT_FAULT = "crash_after_search_commit"
CRASH_BEFORE_SEARCH_WAL_COMMIT_FAULT = "crash_before_search_wal_commit"
CRASH_AFTER_SEARCH_WAL_COMMIT_FAULT = "crash_after_search_wal_commit"
CRASH_SST_SINK_AFTER_INGEST_FAULT = "crash_sst_sink_after_ingest"
SLOW_SEARCH_TASK_FAULT = "slow_search_task"
UNABLE_TO_CREATE_FAULT = "unable_to_create"

PARK_FAULTS = frozenset({
    PAUSE_CREATE_INDEX_MID_BUILD_FAULT,
    PAUSE_CTAS_MID_INGEST_FAULT,
    PAUSE_VACUUM_MID_WALK_FAULT,
})

CRASH_FAULTS = frozenset({
    CRASH_BEFORE_CATALOG_COMMIT_FAULT,
    CRASH_AFTER_CATALOG_BEFORE_DATA_FAULT,
    CRASH_ON_DROP_FAULT,
    CRASH_ON_PACKET_FAULT,
    CRASH_BEFORE_SEARCH_COMMIT_FAULT,
    CRASH_AFTER_SEARCH_COMMIT_FAULT,
    CRASH_BEFORE_SEARCH_WAL_COMMIT_FAULT,
    CRASH_AFTER_SEARCH_WAL_COMMIT_FAULT,
    CRASH_SST_SINK_AFTER_INGEST_FAULT,
})

PROGRESS_GATED_FAULTS = frozenset({
    PAUSE_CREATE_INDEX_MID_BUILD_FAULT,
    PAUSE_VACUUM_MID_WALK_FAULT,
})

BACKGROUND_POOL_FAULTS = frozenset({
    SLOW_SEARCH_TASK_FAULT,
})

ALL_FAULTS = frozenset(
    v for k, v in list(globals().items())
    if k.endswith("_FAULT") and isinstance(v, str)
)

_SOURCE_LITERAL = re.compile(
    r'(?:SDB_IF_FAILURE|SDB_WAIT_ON_FAILURE|WaitWhileFailurePointDebugging)'
    r'\s*\(\s*"([^"]+)"'
)
_SOURCE_DIRS = ("server", "libs")
_SOURCE_EXTS = (".cpp", ".cc", ".hpp", ".hh", ".h", ".ipp", ".tpp")


def source_defined_faults(repo_root):
    root = pathlib.Path(repo_root)
    found = {}
    for sub in _SOURCE_DIRS:
        for path in (root / sub).rglob("*"):
            if not path.is_file() or path.suffix not in _SOURCE_EXTS:
                continue
            try:
                text = path.read_text(errors="replace")
            except OSError:
                continue
            if "FAIL" not in text and "Fail" not in text:
                continue
            for m in _SOURCE_LITERAL.finditer(text):
                found.setdefault(m.group(1), f"{path.relative_to(root)}")
    return found


class FaultUnavailable(Exception):
    pass


class FaultBroker:
    def __init__(self, connect, defined=None, enabled=True):
        self._connect = connect
        self._defined = defined
        self._enabled = enabled
        self._counts = {}
        self._lock = threading.Lock()
        self._conn = None

    def _admin(self):
        if self._conn is None or getattr(self._conn, "closed", False):
            self._conn = self._connect()
            self._conn.autocommit = True
        return self._conn

    def _exec(self, sql):
        conn = self._admin()
        with conn.cursor() as cur:
            cur.execute(sql)

    def available(self):
        return self._enabled

    def reset_all(self):
        with self._lock:
            self._counts.clear()
            self._exec("RESET sdb_faults")

    def armed(self):
        with self._lock:
            return frozenset(k for k, v in self._counts.items() if v > 0)

    def _check_known(self, name):
        if self._defined is not None and name not in self._defined:
            raise FaultUnavailable(
                f"fault '{name}' is not defined in server/ or libs/; "
                "SET sdb_faults would accept it and arm nothing"
            )

    def arm(self, name):
        self._check_known(name)
        with self._lock:
            n = self._counts.get(name, 0)
            if n == 0:
                self._exec(f"SET sdb_faults = '{name}'")
            self._counts[name] = n + 1

    def disarm(self, name):
        with self._lock:
            n = self._counts.get(name, 0)
            if n <= 0:
                return
            if n == 1:
                self._exec(f"SET sdb_faults = '-{name}'")
                self._counts.pop(name, None)
            else:
                self._counts[name] = n - 1

    def forget_all(self):
        with self._lock:
            self._counts.clear()
            self._conn = None

    def disarm_all(self):
        with self._lock:
            names = list(self._counts)
            self._counts.clear()
        for name in names:
            try:
                self._exec(f"SET sdb_faults = '-{name}'")
            except Exception:
                pass

    def hold(self, name):
        return _Held(self, name)

    def close(self):
        try:
            self.disarm_all()
        finally:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
                self._conn = None


class _Held:
    def __init__(self, broker, name):
        self._broker = broker
        self._name = name

    def __enter__(self):
        self._broker.arm(self._name)
        return self._name

    def __exit__(self, *exc):
        self._broker.disarm(self._name)
        return False
