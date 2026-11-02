#!/usr/bin/env python3
import argparse
import json
import os
import re
import subprocess
import time
from collections import defaultdict
from pathlib import Path

from testlib.scheduler import BAD

MARKER = "<!-- serenedb-ci-report -->"
HISTORY = re.compile(r"<!-- ci-history:(.*?) -->")
ORDER = ("perf", "dev", "asan", "tsan", "msan")
CHECK_JOBS = {"bugfix": "bugfix"}
COVERAGE_ROWS = 40
KEEP_RUNS = 30


def job_keys(job, keys):
    if job in CHECK_JOBS:
        return [k for k in keys if k[2] == CHECK_JOBS[job]]
    config, _, location = job.partition("-")
    arch = {"": "x86_64", "arm64": "aarch64"}.get(location)
    return [k for k in keys if (k[0], k[1]) == (config, arch) and k[2] not in CHECK_JOBS.values()]


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


def load_coverage(*dirs):
    found = []
    for path in sorted(p for d in dirs if d for p in Path(d).glob("**/coverage-*/summary.txt")):
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
    return f"| `{d['file']}:{d['line']}` | {what} | {_code(code or '')} |"


def build_report(metas, rows, needs, run_url, coverage=(), subject="", whole_coverage=""):
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
    lines = ["### CI results" + (f" · {subject}" if subject else ""), ""]
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
    failed_jobs = False
    for job, info in sorted((needs or {}).items()):
        result = info.get("result")
        if result not in ("failure", "cancelled"):
            continue
        failed_jobs = True
        mine = job_keys(job, keys)
        if not mine:
            tests = job in CHECK_JOBS or job.partition("-")[0] in ORDER
            lines.append(f"- ❌ **{job}**: {result}" + (" before any test result" if tests else ""))
        elif not red.intersection(mine):
            lines.append(f"- ❌ **{job}**: {result} outside the tests")
    counts = {s: sum(by_config[k][s] for k in keys) for s in BAD}
    verdict = ("❌ " + (", ".join(f"{n} {s}" for s, n in counts.items() if n) or "failed")
               if red or failed_jobs else "✅")
    if coverage:
        lines += ["", "**Coverage of the change**", "", "```", *(c["summary"] for c in coverage), "```"]
        uncovered = [d for c in coverage for d in c["uncovered"]]
        if uncovered:
            lines += ["", f"<details><summary>{len(uncovered)} uncovered</summary>", "",
                      "| where | not covered | code |", "| --- | --- | --- |",
                      *(uncovered_row(d) for d in uncovered[:COVERAGE_ROWS])]
            if len(uncovered) > COVERAGE_ROWS:
                lines.append(f"\n... and {len(uncovered) - COVERAGE_ROWS} more in the "
                             "`coverage.json` of the `coverage-dev-linux-amd64` artifact, or of "
                             "`results-dev-linux-amd64` when dev ran one shard")
            lines += ["", "</details>"]
    if whole_coverage:
        lines += ["", whole_coverage.rstrip()]
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
        "verdict": verdict,
        "configs": {_label(k): dict(by_config[k]) for k in keys},
        "failures": failures,
        "sanitizer_signatures": {k: sorted(v) for k, v in signatures.items()},
        "coverage": list(coverage),
    }
    return "\n".join(lines) + "\n", summary


def run_options(inputs):
    opts = sorted(name.lower().replace("_", " ") for name, value in inputs.items()
                  if value is True or value == "true")
    location = {"all": "all architectures", "dev-only": "dev only"}.get(inputs.get("LOCATION"))
    if location:
        opts.append(location)
    runs = int(json.loads(inputs.get("BUILD_CONFIG") or "{}").get("FLAKY_RUNS", 1))
    if runs > 1:
        opts.append(f"flaky check {runs} runs")
    return ", ".join(opts) or "default"


def updated_history(comment, entry):
    found = HISTORY.search(comment or "")
    history = json.loads(found.group(1)) if found else []
    return ([h for h in history if h["run"] != entry["run"]] + [entry])[-KEEP_RUNS:]


def render_comment(report_body, history):
    lines = [MARKER, f"<!-- ci-history:{json.dumps(history, separators=(',', ':'))} -->",
             report_body.rstrip()]
    previous = history[:-1]
    if previous:
        lines += ["", f"<details><summary>Previous runs ({len(previous)})</summary>", "",
                  "| run | commit | options | result | finished |", "| --- | --- | --- | --- | --- |",
                  *(f"| [{h['run']}]({h['url']}) | `{h['sha']}` | {h['options']} | {h['verdict']} | "
                    f"{h['time']} |" for h in reversed(previous)),
                  "", "</details>"]
    return "\n".join(lines) + "\n"


def post_comment(pr, report_body, entry):
    repo = os.environ.get("GITHUB_REPOSITORY", "serenedb/serenedb")
    out = subprocess.run(["gh", "api", "--paginate", f"repos/{repo}/issues/{pr}/comments",
                          "--jq", f'.[] | select(.body | startswith("{MARKER}")) | {{id, body}}'],
                         capture_output=True, text=True, check=True)
    found = [json.loads(line) for line in out.stdout.splitlines() if line.strip()]
    body = render_comment(report_body, updated_history(found[0]["body"] if found else "", entry))
    if found:
        subprocess.run(["gh", "api", "-X", "PATCH", f"repos/{repo}/issues/comments/{found[0]['id']}",
                        "-F", "body=@-"], input=body, text=True, check=True, capture_output=True)
    else:
        subprocess.run(["gh", "api", f"repos/{repo}/issues/{pr}/comments", "-F", "body=@-"],
                       input=body, text=True, check=True, capture_output=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", required=True)
    ap.add_argument("--coverage", default="")
    ap.add_argument("--whole-coverage", default="",
                    help="the nightly's coverage of the whole system, as Markdown, when it exists")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    metas, rows = load(args.results)
    needs = json.loads(os.environ.get("NEEDS", "{}") or "{}")
    run_url = os.environ.get("RUN_URL", "")
    sha = os.environ.get("REPORT_SHA", "")[:9]
    options = run_options(json.loads(os.environ.get("REPORT_INPUTS", "{}") or "{}"))
    whole = Path(args.whole_coverage) if args.whole_coverage else None
    body, summary = build_report(metas, rows, needs, run_url,
                                 load_coverage(args.coverage, args.results),
                                 " · ".join(s for s in (sha and f"`{sha}`", options) if s),
                                 whole.read_text() if whole and whole.is_file() else "")
    Path(args.out).write_text(body)
    Path(args.out).with_suffix(".json").write_text(json.dumps(summary, indent=1))
    step = os.environ.get("GITHUB_STEP_SUMMARY")
    if step:
        with open(step, "a") as fh:
            fh.write(body)
    if os.environ.get("PR_NUMBER"):
        post_comment(os.environ["PR_NUMBER"], body, {
            "run": run_url.rsplit("/", 1)[-1], "url": run_url, "sha": sha, "options": options,
            "verdict": summary["verdict"], "time": time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())})
    print(body)


if __name__ == "__main__":
    main()
