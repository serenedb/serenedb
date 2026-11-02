import json
from dataclasses import asdict

from .scheduler import BAD, CRASHED, FAILED, FLAKY, INTERRUPTED, PASSED, SETUP, SKIPPED, STUCK


def counted(results):
    return [r for r in results if r.attempt == 0 and not r.wrapper and r.status != SETUP]


def failed(results):
    return any(r.status in BAD for r in results if r.attempt == 0)


def _duration(seconds):
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m{s:02d}s" if h else f"{m}m{s:02d}s"


def summary(meta, results, skips, limit=40):
    rows = counted(results)
    count = {k: 0 for k in (PASSED, FAILED, FLAKY, STUCK, CRASHED, SKIPPED, INTERRUPTED)}
    for r in rows:
        count[r.status] = count.get(r.status, 0) + 1
    skipped = count[SKIPPED] + len(skips)
    head = (f"{meta['config']}/{meta['arch']} shard {meta['shard']}: {count[PASSED]} passed, "
            f"{count[FAILED]} failed, {count[FLAKY]} flaky, {count[STUCK]} stuck, "
            f"{count[CRASHED]} crashed, {skipped} skipped"
            + (f", {count[INTERRUPTED]} interrupted" if count[INTERRUPTED] else "")
            + f" in {_duration(meta['duration'])}")
    lines = [head]
    bad = [r for r in results if r.attempt == 0 and r.status in BAD
           and not (r.wrapper and any(m.member_of == r.unit and m.status in BAD for m in rows))]
    for r in sorted(bad, key=lambda r: (r.status, r.unit))[:limit]:
        lines.append(f"  {r.status.upper():8} {r.unit}: {r.detail}")
    if len(bad) > limit:
        lines.append(f"  ... {len(bad) - limit} more in results.jsonl")
    return "\n".join(lines)


def write(out_dir, meta, results, skips):
    common = {k: meta[k] for k in ("config", "arch", "sha", "pr", "branch", "run", "check")}
    with open(out_dir / "results.jsonl", "w") as fh:
        for r in results:
            fh.write(json.dumps({**asdict(r), **common}) + "\n")
        for uid, skip in skips:
            fh.write(json.dumps({"unit": uid, "suite": uid.split(":")[0], "status": SKIPPED,
                                 "duration": 0, "attempt": 0, "started": meta["started"],
                                 "detail": f"{skip.kind}: {skip.reason}", "signature": "",
                                 "member_of": "", "dir": "", "wrapper": False, **common}) + "\n")
    (out_dir / "summary.txt").write_text(summary(meta, results, skips) + "\n")
    (out_dir / "meta.json").write_text(json.dumps(meta))
