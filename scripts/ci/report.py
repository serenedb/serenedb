#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
from collections import defaultdict
from pathlib import Path

MARKER = "<!-- serenedb-ci-report -->"
BAD = ("failed", "stuck", "crashed", "flaky")
ORDER = ("perf", "dev", "asan", "tsan", "msan")


def load(results_dir):
    rows = []
    metas = []
    for meta_path in sorted(Path(results_dir).glob("**/meta.json")):
        meta = json.loads(meta_path.read_text())
        metas.append(meta)
        res = meta_path.parent / "results.jsonl"
        if res.is_file():
            for line in res.read_text().splitlines():
                if line.strip():
                    rows.append(json.loads(line))
    return metas, rows


def _duration(seconds):
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m" if h else f"{m}m{s:02d}s"


def load_coverage(coverage_dir):
    if not coverage_dir:
        return []
    return [p.read_text().strip() for p in sorted(Path(coverage_dir).glob("**/summary.txt"))]


def load_selection(selection_dir):
    if not selection_dir:
        return None
    found = sorted(Path(selection_dir).glob("**/selection.json"))
    return json.loads(found[0].read_text()) if found else None


def selection_line(sel):
    if sel["mode"] == "no map":
        return "no selection map in the store yet"
    if sel["mode"] == "everything":
        return f"would run every unit: {', '.join(sel['everything_because'][:3])}"
    share = 100 * sel["selected_cost"] / sel["cost"] if sel["cost"] else 0
    return (f"would run {sel['selected']} of {sel['units']} units, "
            f"{share:.0f}% of the expected test time")


def build_report(metas, rows, needs, run_url, coverage=(), selection=None):
    by_config = defaultdict(lambda: defaultdict(int))
    longest = defaultdict(float)
    shards = defaultdict(int)
    for m in metas:
        key = (m["config"], m["arch"])
        longest[key] = max(longest[key], m["duration"])
        shards[key] += 1
    failures = []
    signatures = defaultdict(set)
    covered_wrappers = {(r["member_of"], r["config"], r["arch"]) for r in rows
                        if r.get("member_of") and r["status"] in BAD and r["attempt"] == 0}
    for r in rows:
        if r["attempt"] != 0:
            continue
        key = (r["config"], r["arch"])
        if not r.get("wrapper"):
            by_config[key][r["status"]] += 1
        if r["status"] in BAD:
            if r.get("wrapper") and (r["unit"], r["config"], r["arch"]) in covered_wrappers:
                continue
            failures.append(r)
            if "Sanitizer" in (r.get("signature") or ""):
                signatures[r["signature"]].add(f"{r['config']}/{r['arch']}")
    lines = [MARKER, "### CI results", ""]
    keys = sorted(set(by_config) | set(longest),
                  key=lambda k: (ORDER.index(k[0]) if k[0] in ORDER else 99, k[1]))
    for key in keys:
        c = by_config[key]
        bad = sum(c[s] for s in BAD)
        mark = "❌" if bad else "✅"
        lines.append(
            f"- {mark} **{key[0]}/{key[1]}**: {c['passed']} passed, {c['failed']} failed, "
            f"{c['flaky']} flaky, {c['stuck']} stuck, {c['crashed']} crashed, {c['skipped']} skipped; "
            f"{shards[key]} shard(s), longest {_duration(longest[key])}")
    for job, info in sorted((needs or {}).items()):
        result = info.get("result")
        if result in ("failure", "cancelled") and not any(k[0] == job for k in keys):
            lines.append(f"- ❌ **{job}**: {result} before any test result")
    if coverage:
        lines += ["", "**Coverage of the change**", "", "```", *coverage, "```"]
    if selection:
        lines += ["", f"**Test selection** (report only, every unit ran): {selection_line(selection)}"]
    if signatures:
        lines += ["", "**Sanitizer reports**", ""]
        for sig, where in sorted(signatures.items()):
            lines.append(f"- `{sig[:200]}` ({', '.join(sorted(where))})")
    if failures:
        lines += ["", f"**Failures** ({len(failures)})", "",
                  "| config | status | unit | detail |", "| --- | --- | --- | --- |"]
        for r in sorted(failures, key=lambda r: (r["config"], r["status"], r["unit"]))[:60]:
            detail = (r.get("detail") or "").replace("|", "\\|").replace("\n", " ")[:200]
            lines.append(f"| {r['config']}/{r['arch']} | {r['status']} | `{r['unit']}` | {detail} |")
        if len(failures) > 60:
            lines.append(f"\n... and {len(failures) - 60} more in the `report` artifact")
    lines += ["", f"[Run]({run_url}) · `gh run download {run_url.rsplit('/', 1)[-1]} -n report` "
              "for report.json"]
    summary = {
        "configs": {f"{k[0]}/{k[1]}": dict(by_config[k]) for k in keys},
        "failures": failures,
        "sanitizer_signatures": {k: sorted(v) for k, v in signatures.items()},
        "coverage": list(coverage),
        "selection": selection,
    }
    return "\n".join(lines) + "\n", summary


def post_comment(pr, body):
    repo = os.environ.get("GITHUB_REPOSITORY", "serenedb/serenedb")
    out = subprocess.run(["gh", "api", "--paginate", f"repos/{repo}/issues/{pr}/comments",
                          "--jq", f'.[] | select(.body | startswith("{MARKER}")) | .id'],
                         capture_output=True, text=True)
    ids = [x for x in out.stdout.split() if x.strip()]
    if ids:
        subprocess.run(["gh", "api", "-X", "PATCH", f"repos/{repo}/issues/comments/{ids[0]}",
                        "-f", f"body={body}"], check=True, capture_output=True)
    else:
        subprocess.run(["gh", "api", f"repos/{repo}/issues/{pr}/comments", "-f", f"body={body}"],
                       check=True, capture_output=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", required=True)
    ap.add_argument("--coverage", default="")
    ap.add_argument("--selection", default="")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    metas, rows = load(args.results)
    needs = json.loads(os.environ.get("NEEDS", "{}") or "{}")
    body, summary = build_report(metas, rows, needs, os.environ.get("RUN_URL", ""),
                                 load_coverage(args.coverage), load_selection(args.selection))
    Path(args.out).write_text(body)
    Path(args.out).with_suffix(".json").write_text(json.dumps(summary, indent=1))
    step = os.environ.get("GITHUB_STEP_SUMMARY")
    if step:
        with open(step, "a") as fh:
            fh.write(body)
    if os.environ.get("PR_NUMBER"):
        post_comment(os.environ["PR_NUMBER"], body)
    print(body)


if __name__ == "__main__":
    main()
