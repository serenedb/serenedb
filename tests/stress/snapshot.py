import os
import re

import ops

RELKIND_TO_KIND = {
    "r": ops.TABLE,
    "v": ops.VIEW,
    "S": ops.SEQUENCE,
    "i": ops.INDEX,
}

ENTRY_TYPE_TO_KIND = {
    "Table": ops.TABLE,
    "View": ops.VIEW,
    "Sequence": ops.SEQUENCE,
    "Index": ops.INDEX,
}

DEPENDENCY_ENTRY_TYPE = "Dependency"


def generated_name_re(run_tag):
    return re.compile(rf"^s{re.escape(run_tag)}_w\d+_(?:t|q|v|i)\d+$")


class Snapshot:
    def __init__(self, run_tag):
        self.run_tag = run_tag
        self.pg_objects = {}
        self.pg_tokens = {}
        self.set_objects = {}
        self.set_oids = {}
        self.edges = []
        self.all_oids = set()
        self.epoch = None
        self.orphan_files = []
        self.row_tokens = {}
        self.catalog_wal_bytes = None
        self.errors = []

    def is_generated(self, name):
        return bool(generated_name_re(self.run_tag).match(name))


def _rows(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def read_row_tokens(conn, keys):
    out = {}
    for key in keys:
        if key[0] != ops.TABLE:
            continue
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT label FROM {key[1]}")
                out[key] = frozenset(
                    r[0] for r in cur.fetchall() if r[0] is not None)
        except Exception:
            continue
    return out


def catalog_wal_size(datadir):
    path = os.path.join(datadir, "engine_catalog", "catalog.wal")
    try:
        return os.path.getsize(path)
    except OSError:
        return None


def take(conn, run_tag, datadir=None, row_keys=()):
    snap = Snapshot(run_tag)
    like = f"s{run_tag}\\_%"

    try:
        for relkind, relname, oid, descr in _rows(conn, """
            SELECT c.relkind, c.relname, c.oid, d.description
            FROM pg_class c
            LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
            WHERE c.relname LIKE %s
        """, (like,)):
            kind = RELKIND_TO_KIND.get(relkind)
            if kind is None or not snap.is_generated(relname):
                continue
            snap.pg_objects[(kind, relname)] = int(oid)
            snap.pg_tokens[(kind, relname)] = descr
    except Exception as exc:
        snap.errors.append(f"pg_class scan failed: {exc}")

    try:
        for schema, entry_type, name, entry_oid, visible in _rows(conn, """
            SELECT schema_name, entry_type, name, entry_oid, visible
            FROM sdb_catalog_sets()
        """):
            snap.all_oids.add(int(entry_oid))
            if entry_type == DEPENDENCY_ENTRY_TYPE:
                snap.edges.append((int(name), int(entry_oid)))
                continue
            kind = ENTRY_TYPE_TO_KIND.get(entry_type)
            if kind is None or not snap.is_generated(name):
                continue
            snap.set_objects[(kind, name)] = int(entry_oid)
            snap.set_oids.setdefault(int(entry_oid), []).append((kind, name))
    except Exception as exc:
        snap.errors.append(f"sdb_catalog_sets scan failed: {exc}")

    for name in {n for (k, n) in snap.set_objects if k == ops.INDEX}:
        snap.set_objects.pop((ops.TABLE, name), None)

    try:
        rows = _rows(conn, "SELECT catalog_version FROM sdb_deferred_catalog()")
        snap.epoch = int(rows[0][0]) if rows else None
    except Exception as exc:
        snap.errors.append(f"sdb_deferred_catalog failed: {exc}")

    wanted = [k for k in row_keys if k in snap.pg_objects]
    if wanted:
        snap.row_tokens = read_row_tokens(conn, wanted)

    if datadir:
        snap.orphan_files = scan_datadir(datadir, snap.all_oids)
        snap.catalog_wal_bytes = catalog_wal_size(datadir)
    return snap


def scan_datadir(datadir, live_oids):
    findings = []
    duck = os.path.join(datadir, "engine_duckdb")
    if os.path.isdir(duck):
        for entry in os.listdir(duck):
            stem = entry.split(".", 1)[0]
            if not stem.isdigit():
                continue
            if int(stem) not in live_oids:
                findings.append(("engine_duckdb", entry))
    search = os.path.join(datadir, "engine_search")
    if os.path.isdir(search):
        for root, dirs, _files in os.walk(search):
            for d in dirs:
                if d.isdigit() and int(d) not in live_oids:
                    findings.append((os.path.relpath(root, datadir), d))
    return findings


def observed_states(snap):
    out = {}
    for key, oid in snap.pg_objects.items():
        token = snap.pg_tokens.get(key)
        out[key] = token if token is not None else _NO_TOKEN
    return out


_NO_TOKEN = "__present_without_token__"
NO_TOKEN = _NO_TOKEN
