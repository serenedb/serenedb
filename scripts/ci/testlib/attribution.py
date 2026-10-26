import itertools
import json
import subprocess
import threading

_lock = threading.Lock()
_ids = itertools.count()


def directory(ctx):
    return ctx.out_dir / "attribution"


def collect(ctx, unit, run, status):
    raw = sorted(run.profile_dir.glob("*.profraw"))
    if not raw:
        return
    merged = run.profile_dir / "unit.profdata"
    merge = subprocess.run(["llvm-profdata-23", "merge", "-sparse", "-failure-mode=any", "-o",
                            str(merged), *map(str, raw)], capture_output=True, text=True)
    if merge.returncode != 0:
        print(f"no attribution for {unit.id}: {merge.stderr.strip()[-300:]}", flush=True)
        return
    out = directory(ctx)
    out.mkdir(exist_ok=True)
    with _lock:
        n = next(_ids)
    with open(out / f"{n}.txt", "w") as fh:
        show = subprocess.run(["llvm-profdata-23", "show", "--covered", "--all-functions",
                               str(merged)], stdout=fh, stderr=subprocess.PIPE, text=True)
    if show.returncode != 0:
        print(f"no attribution for {unit.id}: {show.stderr.strip()[-300:]}", flush=True)
        return
    with _lock, open(out / "index.jsonl", "a") as fh:
        fh.write(json.dumps({"n": n, "unit": unit.id, "status": status,
                             "members": list(unit.members)}) + "\n")
    shard_profiles = ctx.out_dir / "profiles"
    shard_profiles.mkdir(exist_ok=True)
    merged.rename(shard_profiles / f"unit-{n}.profdata")
    for path in raw:
        path.unlink(missing_ok=True)
