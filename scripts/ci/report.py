#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
from collections import defaultdict
from pathlib import Path

from testlib.scheduler import BAD

MARKER = "<!-- serenedb-ci-report -->"
ORDER = ("perf", "dev", "asan", "tsan", "msan")
COVERAGE_ROWS = 40


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


def _label(key):
    config, arch, check = key
    return f"{config}/{arch}" + (f" {check}" if check else "")


def _duration(seconds):
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m" if h else f"{m}m{s:02d}s"


def load_coverage(coverage_dir):
    if not coverage_dir:
        return []
    found = []
    for path in sorted(Path(coverage_dir).glob("**/summary.txt")):
        details = path.with_name("coverage.json")
        uncovered = json.loads(details.read_text())["uncovered"] if details.is_file() else []
        found.append({"summary": path.read_text().strip(), "uncovered": uncovered})
    return found


def _code(text):
    text = " ".join(text.split())[:80].replace("|", "\\|")
    return f"`{text}`" if text and "`" not in text else text


def uncovered_row(d):
    what = {"line": "line", "function": "function never called",
            "branch": f"branch never {d.get('direction')}",
            "mcdc": f"MC/DC condition {d.get('condition')}"}[d["kind"]]
    code = d.get("condition_source") if d["kind"] == "mcdc" else d.get("source")
    tests = ", ".join(f"`{t}`" for t in d.get("tests", [])) or "none in the map"
    return f"| `{d['file']}:{d['line']}` | {what} | {_code(code or '')} | {tests} |"


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
    planned = defaultdict(int)
    plans = defaultdict(set)
    for m in metas:
        key = (m["config"], m["arch"], m.get("check", ""))
        longest[key] = max(longest[key], m["duration"])
        shards[key] += 1
        planned[key] = max(planned[key], int(m["shard"].split("/")[1]))
        plans[key].add(m.get("plan"))
    failures = []
    signatures = defaultdict(set)
    covered_wrappers = {(r["member_of"], r["config"], r["arch"], r.get("check", "")) for r in rows
                        if r.get("member_of") and r["status"] in BAD and r["attempt"] == 0}
    for r in rows:
        if r["attempt"] != 0:
            continue
        key = (r["config"], r["arch"], r.get("check", ""))
        if not r.get("wrapper"):
            by_config[key][r["status"]] += 1
        if r["status"] in BAD:
            if r.get("wrapper") and (r["unit"], *key) in covered_wrappers:
                continue
            failures.append(r)
            if "Sanitizer" in (r.get("signature") or ""):
                signatures[r["signature"]].add(f"{r['config']}/{r['arch']}")
    lines = [MARKER, "### CI results", ""]
    keys = sorted(set(by_config) | set(longest),
                  key=lambda k: (ORDER.index(k[0]) if k[0] in ORDER else 99, k[1], k[2]))
    red = set()
    for key in keys:
        c = by_config[key]
        bad = sum(c[s] for s in BAD)
        missing = planned[key] - shards[key]
        split = len(plans[key]) > 1
        if bad or missing or split:
            red.add(key)
        mark = "❌" if key in red else "✅"
        lines.append(
            f"- {mark} **{_label(key)}**: {c['passed']} passed, {c['failed']} failed, "
            f"{c['flaky']} flaky, {c['stuck']} stuck, {c['crashed']} crashed, {c['skipped']} skipped; "
            f"{shards[key]} of {planned[key]} shards"
            + (f" ({missing} without results)" if missing else "")
            + (" (the shards planned different units, so some ran twice and others not at all)"
               if split else "")
            + f", longest {_duration(longest[key])}")
    for job, info in sorted((needs or {}).items()):
        result = info.get("result")
        if result not in ("failure", "cancelled"):
            continue
        mine = [k for k in keys if job in (k[0], k[2])]
        if not mine:
            lines.append(f"- ❌ **{job}**: {result} before any test result")
        elif not red.intersection(mine):
            lines.append(f"- ❌ **{job}**: {result} outside the tests")
    if coverage:
        lines += ["", "**Coverage of the change**", "", "```", *(c["summary"] for c in coverage), "```"]
        uncovered = [d for c in coverage for d in c["uncovered"]]
        if uncovered:
            lines += ["", f"<details><summary>{len(uncovered)} uncovered, with the tests that run "
                      "the code nearest to each: where a test for it goes</summary>", "",
                      "| where | not covered | code | nearest tests |", "| --- | --- | --- | --- |",
                      *(uncovered_row(d) for d in uncovered[:COVERAGE_ROWS])]
            if len(uncovered) > COVERAGE_ROWS:
                lines.append(f"\n... and {len(uncovered) - COVERAGE_ROWS} more in the "
                             "`coverage-dev-linux-amd64` artifact")
            lines += ["", "</details>"]
    if selection:
        lines += ["", f"**Test selection** (report only, every unit ran): {selection_line(selection)}"]
    if signatures:
        lines += ["", f"**Sanitizer reports** ({len(signatures)})", ""]
        ranked = sorted(signatures.items(), key=lambda kv: (-len(kv[1]), kv[0]))
        for sig, where in ranked[:40]:
            lines.append(f"- `{sig[:200]}` ({', '.join(sorted(where))})")
        if len(ranked) > 40:
            lines.append(f"\n... and {len(ranked) - 40} more in the `report` artifact")
    if failures:
        lines += ["", f"**Failures** ({len(failures)})", "",
                  "| config | status | unit | detail |", "| --- | --- | --- | --- |"]
        for r in sorted(failures, key=lambda r: (r["config"], r["status"], r["unit"]))[:60]:
            detail = r.get("detail") or ""
            if r.get("elsewhere"):
                detail = f"also failed in {r['elsewhere']} · {detail}"
            detail = detail.replace("|", "\\|").replace("\n", " ")[:300]
            lines.append(f"| {r['config']}/{r['arch']} | {r['status']} | `{r['unit']}` | {detail} |")
        if len(failures) > 60:
            lines.append(f"\n... and {len(failures) - 60} more in the `report` artifact")
    lines += ["", f"[Run]({run_url}) · `gh run download {run_url.rsplit('/', 1)[-1]} -n report` "
              "for report.json"]
    summary = {
        "configs": {_label(k): dict(by_config[k]) for k in keys},
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
                        "-F", "body=@-"], input=body, text=True, check=True, capture_output=True)
    else:
        subprocess.run(["gh", "api", f"repos/{repo}/issues/{pr}/comments", "-F", "body=@-"],
                       input=body, text=True, check=True, capture_output=True)


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
