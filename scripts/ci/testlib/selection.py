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
CUTOFF = 25
CHEAPEST = 3


def build_map(run, config, arch, work, expected):
    work = Path(work)
    prefix = f"{config}-{arch}-"
    shards = store.fetch_all(store.run_files("attribution", run, prefix), work)
    if len(shards) < expected:
        raise SystemExit(f"attribution of {len(shards)} of {expected} {config}/{arch} shards in run "
                         f"{run}: the selection map stays as it is")
    timing = work / "timing.parquet"
    costs = (f"SELECT unit, p95 FROM read_parquet('{timing}')"
             if store.get(f"timing/{config}-{arch}.parquet", timing)
             else "SELECT NULL::VARCHAR AS unit, NULL::DOUBLE AS p95 WHERE false")
    dest = work / "selection.parquet"
    store.shell(
        f"COPY (WITH a AS (SELECT DISTINCT regexp_replace(unit, '#[0-9]+/[0-9]+$', '') AS unit, "
        f"function FROM read_parquet([{store.quoted(shards)}])), "
        f"c AS ({costs}), "
        f"j AS (SELECT a.function, a.unit, split_part(a.unit, ':', 1) AS suite, "
        f"coalesce(c.p95, 1e9) AS cost FROM a LEFT JOIN c USING (unit)), "
        f"r AS (SELECT *, count(*) OVER (PARTITION BY function) AS covering, "
        f"row_number() OVER (PARTITION BY function, suite ORDER BY cost, unit) AS in_suite FROM j), "
        f"o AS (SELECT *, row_number() OVER (PARTITION BY function ORDER BY in_suite > 1, cost, "
        f"unit) AS rank FROM r) "
        f"SELECT function, unit, cost, covering, rank FROM o "
        f"WHERE covering <= {KEEP} OR rank <= {CHEAPEST} ORDER BY function, rank) "
        f"TO '{dest}' (FORMAT parquet, COMPRESSION zstd)", work)
    stats = store.shell(f"SELECT count(DISTINCT function) AS functions, count(DISTINCT unit) AS "
                        f"units, count(*) AS rows FROM read_parquet('{dest}')", work)[0]
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


def innermost(spans, line):
    inside = [s for s in spans if s[0] <= line <= s[1]]
    if not inside:
        return []
    start = max(s[0] for s in inside)
    return [s[2] for s in inside if s[0] == start]


def report(units, base, changed, changed_units, map_path, spans, work, pr="", cutoff=CUTOFF):
    work = Path(work)
    files = diff.changed_lines(base)
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
    everything = [f"{p} (deleted)" for p in deleted if not diff.is_test(p)]
    for path in changed:
        if any(other.startswith(path + "/") for other in changed):
            continue
        if path in files or diff.is_test(path) or (path.endswith(".md") and path.startswith("docs/")):
            continue
        everything.append(path)
    sources = sorted(files)
    covering = {}
    if sources and not everything:
        covering = covering_units(map_path, {s[2] for p in sources for s in spans.get(p, [])}, work)
    known = {u.id.split("#", 1)[0] for u in units}
    for path in sources:
        for line in sorted(files[path]):
            hits = [r for f in innermost(spans.get(path, []), line) for r in covering.get(f, [])]
            picks = {r["unit"] for r in hits if r["covering"] <= cutoff or r["rank"] <= CHEAPEST}
            if not picks & known:
                everything.append(f"{path}:{line}")
                break
            for unit in picks & known:
                selected[unit].add(f"{path}:{line}")
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
