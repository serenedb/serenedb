import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

from . import store

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts" / "coverage"))

import diff  # noqa: E402

KEEP = 200
CHEAPEST = 3
NO_TIMING = "SELECT NULL::VARCHAR AS unit, NULL::DOUBLE AS p95 WHERE false"


def build_map(run, config, arch, work, expected):
    work = Path(work)
    prefix = f"{config}-{arch}-"
    shards = store.fetch_all(store.run_files("attribution", run, prefix), work)
    if len(shards) < expected:
        raise SystemExit(f"attribution of {len(shards)} of {expected} {config}/{arch} shards in run "
                         f"{run}: the selection map stays as it is")
    costs = []
    for attribute in (False, True):
        local = work / f"timing-{attribute}.parquet"
        key = f"timing/{config}{'-attribute' if attribute else ''}-{arch}.parquet"
        costs.append(f"SELECT unit, p95 FROM read_parquet('{local}')" if store.get(key, local)
                     else NO_TIMING)
    dest = work / "selection.parquet"
    attributed = f"read_parquet([{store.quoted(shards)}], union_by_name = true)"
    store.shell(
        f"COPY (WITH a AS (SELECT DISTINCT regexp_replace(unit, '#[0-9]+/[0-9]+$', '') AS unit, "
        f"function FROM {attributed} WHERE function IS NOT NULL), "
        f"c AS (SELECT unit, coalesce(p.p95, q.p95) AS p95 FROM ({costs[0]}) p "
        f"FULL JOIN ({costs[1]}) q USING (unit)), "
        f"j AS (SELECT a.function, a.unit, split_part(a.unit, ':', 1) AS suite, "
        f"coalesce(c.p95, 1e9) AS cost FROM a LEFT JOIN c USING (unit)), "
        f"r AS (SELECT *, count(*) OVER (PARTITION BY function) AS covering, "
        f"row_number() OVER (PARTITION BY function, suite ORDER BY cost, unit) AS in_suite FROM j), "
        f"o AS (SELECT *, row_number() OVER (PARTITION BY function ORDER BY in_suite > 1, cost, "
        f"unit) AS rank FROM r) "
        f"SELECT function, unit, cost, covering, rank, NULL::VARCHAR AS member FROM o "
        f"WHERE covering <= {KEEP} OR rank <= {CHEAPEST} "
        f"UNION ALL SELECT DISTINCT NULL, unit, NULL, NULL, NULL, member FROM {attributed} "
        f"WHERE member IS NOT NULL ORDER BY function, rank, unit, member) "
        f"TO '{dest}' (FORMAT parquet, COMPRESSION zstd)", work)
    stats = store.shell(f"SELECT count(DISTINCT function) AS functions, count(DISTINCT unit) AS "
                        f"units, count(function) AS rows FROM read_parquet('{dest}')", work)[0]
    day = time.strftime("%Y-%m-%d", time.gmtime())
    store.put(f"selection/{config}-{arch}.parquet", dest)
    store.put(f"selection/{day}/{config}-{arch}.parquet", dest)
    return stats


def covering_units(map_path, functions, work):
    if not functions:
        return {}
    rows = store.shell(f"SELECT function, unit, covering, rank FROM read_parquet('{map_path}') "
                       f"WHERE function IN ({store.quoted(sorted(functions))}) ORDER BY rank, unit",
                       work)
    found = defaultdict(list)
    for r in rows:
        found[r["function"]].append(r)
    return found


def members_of(map_path, units, work):
    if not units:
        return {}
    rows = store.shell(f"SELECT unit, member FROM read_parquet('{map_path}') WHERE member IS NOT NULL "
                       f"AND unit IN ({store.quoted(sorted(units))}) ORDER BY unit, member", work)
    found = defaultdict(list)
    for r in rows:
        found[r["unit"]].append(r["member"])
    return found


def innermost(spans, line):
    inside = [s for s in spans if s[0] <= line <= s[1]]
    if not inside:
        return []
    start = max(s[0] for s in inside)
    return [s[2] for s in inside if s[0] == start]


def inert(path):
    return path.startswith(".claude/") or (path.endswith(".md") and not path.startswith("docs/"))


def report(units, base, changed, changed_units, claimed, map_path, spans, work, pr=""):
    work = Path(work)
    files = diff.changed_lines(base, deletions=True)
    selected = defaultdict(set)
    for u in changed_units:
        selected[u.id.split("#", 1)[0]].add("changed test")
    if pr:
        local = store.fetch_all([k for k in store.recent("failures", 14) if f"-pr{pr}-" in k],
                                work)
        if local:
            for r in store.shell(f"SELECT DISTINCT regexp_replace(unit, '#[0-9]+/[0-9]+$', '') AS "
                                 f"unit FROM read_parquet([{store.quoted(local)}], "
                                 f"union_by_name = true) WHERE pr = {store.quoted([pr])}", work):
                selected[r["unit"]].add("failed earlier in this PR")
    merge_base = subprocess.run(["git", "-C", str(ROOT), "merge-base", base, "HEAD"],
                                capture_output=True, text=True, check=True).stdout.strip()
    deleted = subprocess.run(["git", "-C", str(ROOT), "diff", "--name-only", "--diff-filter=D",
                              merge_base, "HEAD"], capture_output=True, text=True,
                             check=True).stdout.splitlines()
    everything = [f"{p} (deleted)" for p in deleted if not diff.is_test(p) and not inert(p)]
    for path in changed:
        if any(other.startswith(path + "/") for other in changed):
            continue
        if path in files or path in claimed or inert(path):
            continue
        everything.append(path)
    sources = sorted(files)
    covering = {}
    if sources and not everything:
        covering = covering_units(map_path, {s[2] for p in sources for s in spans.get(p, [])}, work)
    owner = {}
    for u in units:
        uid = u.id.split("#", 1)[0]
        for test in (uid, *u.members):
            owner[test] = uid
    picks = defaultdict(dict)
    for path in sources if not everything else ():
        for line in sorted(files[path]):
            hits = [r for f in innermost(spans.get(path, []), line) for r in covering.get(f, [])]
            if any(r["covering"] > KEEP for r in hits):
                everything.append(f"{path}:{line} (run by more than {KEEP} units)")
                break
            picks[path][line] = {r["unit"] for r in hits}
    batches = members_of(map_path, {u for lines in picks.values() for found in lines.values()
                                    for u in found if u not in owner}, work)
    for path, lines in picks.items():
        for line, found in lines.items():
            runs = {owner[t] for u in found for t in (u, *batches.get(u, ())) if t in owner}
            if not runs:
                everything.append(f"{path}:{line}")
                break
            for unit in runs:
                selected[unit].add(f"{path}:{line}")
    known = set(owner.values())
    picked = sorted(u for u in selected if u in known)
    chosen = set(picked)
    run = [u for u in units if u.id.split("#", 1)[0] in chosen]
    return {
        "mode": "everything" if everything else "selected",
        "everything_because": everything[:10],
        "units": len(units),
        "selected": len(units) if everything else len(run),
        "cost": sum(u.cost for u in units),
        "selected_cost": sum(u.cost for u in (units if everything else run)),
        "reasons": {u: sorted(selected[u])[:5] for u in picked},
    }
