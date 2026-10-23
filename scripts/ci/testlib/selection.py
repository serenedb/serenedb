import json
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
BATCH = 200
TEST_PREFIXES = ("tests/", "resources/tests/")


def _functions(profdata, objects, dest):
    lcov = diff.export("lcov", profdata, objects, extra=["-summary-only"])
    files = [line[3:] for line in lcov.splitlines() if line.startswith("SF:")]
    with open(dest, "w") as fh:
        for i in range(0, len(files), BATCH):
            out = diff.export("text", profdata, objects, files[i:i + BATCH],
                              extra=["-skip-expansions"])
            for fn in json.loads(out)["data"][0]["functions"]:
                if not fn["regions"] or not fn["filenames"]:
                    continue
                body = fn["regions"][0]
                fh.write(json.dumps({"function": fn["name"],
                                     "file": diff.normalize(fn["filenames"][body[5]]),
                                     "start": body[0], "end": body[2]}) + "\n")


def build_map(run, config, arch, bundle, work):
    work = Path(work)
    prefix = f"{config}-{arch}-"
    shards = store.fetch_all([f"attribution/{run}/{n}" for n in store.files(f"attribution/{run}/")
                              if n.startswith(prefix)], work)
    if not shards:
        raise SystemExit(f"no attribution for {config}/{arch} in run {run}")
    profiles = [n for n in store.files(f"profdata/{run}/") if n.startswith(prefix)]
    if not profiles:
        raise SystemExit(f"no profile for {config}/{arch} in run {run}")
    profdata = work / "any.profdata"
    store.get(f"profdata/{run}/{profiles[0]}", profdata)
    _functions(profdata, diff.objects(bundle), work / "functions.jsonl")
    timing = work / "timing.parquet"
    costs = (f"SELECT unit, p95 FROM read_parquet('{timing}')"
             if store.get(f"timing/{config}-{arch}.parquet", timing)
             else "SELECT NULL::VARCHAR AS unit, NULL::DOUBLE AS p95 WHERE false")
    dest = work / "selection.parquet"
    store.shell(
        f"COPY (WITH a AS (SELECT DISTINCT regexp_replace(unit, '#[0-9]+/[0-9]+$', '') AS unit, "
        f"function FROM read_parquet([{store.quoted(shards)}])), "
        f"f AS (SELECT DISTINCT function, file, start, \"end\" FROM "
        f"read_json('{work / 'functions.jsonl'}', format = 'newline_delimited')), "
        f"c AS ({costs}), "
        f"j AS (SELECT f.file, f.start, f.\"end\", a.function, a.unit, "
        f"split_part(a.unit, ':', 1) AS suite, coalesce(c.p95, 1e9) AS cost "
        f"FROM a JOIN f USING (function) LEFT JOIN c USING (unit)), "
        f"r AS (SELECT *, count(*) OVER (PARTITION BY function) AS covering, "
        f"row_number() OVER (PARTITION BY function, suite ORDER BY cost, unit) AS in_suite FROM j), "
        f"o AS (SELECT *, row_number() OVER (PARTITION BY function ORDER BY in_suite > 1, cost, "
        f"unit) AS rank FROM r) "
        f"SELECT file, start, \"end\", function, unit, cost, covering, rank FROM o "
        f"WHERE covering <= {KEEP} OR rank <= {CHEAPEST} ORDER BY file, start, function, rank) "
        f"TO '{dest}' (FORMAT parquet, COMPRESSION zstd)", work)
    stats = store.shell(f"SELECT count(DISTINCT function) AS functions, count(DISTINCT unit) AS "
                        f"units, count(*) AS rows FROM read_parquet('{dest}')", work)[0]
    day = time.strftime("%Y-%m-%d", time.gmtime())
    store.put(f"selection/{config}-{arch}.parquet", dest)
    store.put(f"selection/{day}/{config}-{arch}.parquet", dest)
    return stats


def _is_test(path):
    return path.startswith(TEST_PREFIXES) or (path.startswith("third_party/") and "/test/" in path)


def report(units, base, changed, changed_units, map_path, work, pr="", cutoff=CUTOFF):
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
    everything = [f"{p} (deleted)" for p in deleted if not _is_test(p)]
    tests = set()
    for path in changed:
        if any(other.startswith(path + "/") for other in changed):
            continue
        if _is_test(path):
            tests.add(path)
        elif path.endswith(".md") and path.startswith("docs/"):
            continue
        elif path not in files:
            everything.append(path)
    rows = []
    sources = sorted(p for p in files if p not in tests)
    if sources and not everything:
        rows = store.shell(f"SELECT file, start, \"end\", function, unit, covering, rank FROM "
                           f"read_parquet('{map_path}') WHERE file IN ({store.quoted(sources)})",
                           work)
    by_file = defaultdict(list)
    for r in rows:
        by_file[r["file"]].append(r)
    for path in sources:
        for line in sorted(files[path]):
            hits = [r for r in by_file[path] if r["start"] <= line <= r["end"]]
            if not hits:
                everything.append(f"{path}:{line}")
                break
            inner = max(r["start"] for r in hits)
            for r in hits:
                if r["start"] == inner and (r["covering"] <= cutoff or r["rank"] <= CHEAPEST):
                    selected[r["unit"]].add(f"{path}:{line}")
    known = {u.id.split("#", 1)[0] for u in units}
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
