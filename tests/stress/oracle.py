import ops
import snapshot as snap_mod
from model import ABSENT, Present


class OidRegistry:
    def __init__(self):
        self._seen = {}

    def check(self, snap):
        findings = []
        for key, oid in snap.set_objects.items():
            prev = self._seen.get(oid)
            if prev is not None and prev != key:
                findings.append({
                    "kind": "oid_reused",
                    "key": list(key),
                    "detail": f"oid {oid} previously named {prev}; ids are never reused",
                    "candidates": None,
                    "observed": oid,
                })
            self._seen[oid] = key
        return findings


def check_models(models, snap):
    observed = {}
    for key, oid in snap.pg_objects.items():
        token = snap.pg_tokens.get(key)
        token = token if token is not None else snap_mod.NO_TOKEN
        rows = snap.row_tokens.get(key, frozenset())
        observed[key] = Present(token, rows)
    findings = []
    for model in models:
        for f in model.collapse(observed):
            findings.append(f.as_dict())
    return findings


def check_ghosts(models, snap):
    known = set()
    for model in models:
        known |= set(model.owned_keys())
        known |= model.shared_keys()
    findings = []
    for source, table in (("pg_class", snap.pg_objects),
                          ("sdb_catalog_sets", snap.set_objects)):
        for key in table:
            if key in known or not snap.is_generated(key[1]):
                continue
            findings.append({
                "kind": "ghost_entry",
                "key": list(key),
                "detail": f"{source} holds a generated name no worker model created",
                "candidates": None,
                "observed": table[key],
            })
    return findings


def check_pg_vs_sets(snap):
    findings = []
    for key, oid in snap.set_objects.items():
        if not snap.is_generated(key[1]):
            continue
        if key not in snap.pg_objects:
            findings.append({
                "kind": "entry_not_in_pg_class",
                "key": list(key),
                "detail": "present in the entry port but absent from pg_class",
                "candidates": None, "observed": oid,
            })
        elif snap.pg_objects[key] != oid:
            findings.append({
                "kind": "oid_mismatch",
                "key": list(key),
                "detail": f"pg_class oid {snap.pg_objects[key]} != entry oid {oid}",
                "candidates": None, "observed": oid,
            })
    for key, oid in snap.pg_objects.items():
        if not snap.is_generated(key[1]):
            continue
        if key not in snap.set_objects:
            findings.append({
                "kind": "pg_class_not_in_entry_port",
                "key": list(key),
                "detail": "present in pg_class but absent from sdb_catalog_sets()",
                "candidates": None, "observed": oid,
            })
    return findings


def check_edges(snap):
    findings = []
    live = snap.all_oids
    for dependent, referenced in snap.edges:
        if dependent not in live:
            findings.append({
                "kind": "dangling_dependency_dependent",
                "key": None,
                "detail": f"dependency edge dependent oid {dependent} has no entry "
                          f"(referenced {referenced})",
                "candidates": None, "observed": dependent,
            })
        if referenced not in live:
            findings.append({
                "kind": "dangling_dependency_referenced",
                "key": None,
                "detail": f"dependency edge referenced oid {referenced} has no entry "
                          f"(dependent {dependent})",
                "candidates": None, "observed": referenced,
            })
    return findings


def check_orphan_files(snap):
    return [{
        "kind": "orphan_artifact",
        "key": None,
        "detail": f"{where}/{entry} names an oid with no catalog entry",
        "candidates": None, "observed": entry,
    } for where, entry in snap.orphan_files]


def check_visible_tripwire(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT bool_and(visible) FROM sdb_catalog_sets()")
            row = cur.fetchone()
    except Exception as exc:
        return [{"kind": "oracle_query_failed", "key": None,
                 "detail": f"visible tripwire failed: {exc}",
                 "candidates": None, "observed": None}]
    if row and row[0] is False:
        return [{
            "kind": "visible_column_contract_changed",
            "key": None,
            "detail": "sdb_catalog_sets().visible is no longer always true; the "
                      "oracle deliberately ignores it, so revisit that decision",
            "candidates": None, "observed": False,
        }]
    return []


def run_all(models, snap, conn, oid_registry):
    findings = []
    findings += [{"kind": "oracle_query_failed", "key": None, "detail": e,
                  "candidates": None, "observed": None} for e in snap.errors]
    findings += check_models(models, snap)
    findings += check_ghosts(models, snap)
    findings += check_pg_vs_sets(snap)
    findings += check_edges(snap)
    findings += check_orphan_files(snap)
    findings += oid_registry.check(snap)
    findings += check_visible_tripwire(conn)
    return findings
