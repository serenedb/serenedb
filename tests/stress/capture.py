import glob
import json
import os
import re

from procutil import frozen_thread_report

# A sanitizer writes its own reports to stderr, or to <log_path>.<pid> when the
# *SAN_OPTIONS block sets one. Those files are NOT the server's log, so a scan that
# only reads the server log sees none of them -- a TSAN run then reports a clean
# pass while sitting on thousands of inversions.
SANITIZER_ENV = ("TSAN_OPTIONS", "ASAN_OPTIONS", "MSAN_OPTIONS", "UBSAN_OPTIONS",
                 "LSAN_OPTIONS")
_LOG_PATH = re.compile(r"log_path=([^:\s]+)")
REPORT_RE = re.compile(r"WARNING: ThreadSanitizer: ([a-z -]+)"
                       r"|WARNING: (?:Address|Memory|Leak|UndefinedBehavior)Sanitizer"
                       r"|runtime error:")


def sanitizer_log_paths(env=None):
    env = env if env is not None else os.environ
    out = []
    for name in SANITIZER_ENV:
        m = _LOG_PATH.search(env.get(name, "") or "")
        if m:
            out.extend(sorted(glob.glob(m.group(1) + "*")))
    return out


def scan_sanitizer_logs(env=None, limit=200):
    findings, totals = [], {}
    for path in sanitizer_log_paths(env):
        try:
            with open(path, errors="replace") as fh:
                for line in fh:
                    m = REPORT_RE.search(line)
                    if not m:
                        continue
                    kind = (m.group(1) or "sanitizer report").strip()
                    totals[kind] = totals.get(kind, 0) + 1
        except OSError:
            continue
    for kind, n in sorted(totals.items(), key=lambda kv: -kv[1]):
        findings.append({
            "kind": "sanitizer_report_in_sanitizer_log",
            "key": None,
            "detail": f"{n} x {kind} in the sanitizer's own log_path output; the "
                      f"server log does not contain these",
            "candidates": None, "observed": n,
        })
    return findings[:limit], totals


def write_artifacts(outdir, meta, findings, watchdog, server, journal_tail,
                    sanitizer_totals=None):
    os.makedirs(outdir, exist_ok=True)
    report = {
        "meta": meta,
        "watchdog": watchdog.as_dict() if watchdog else None,
        "findings": findings,
        "finding_count": len(findings),
        "sanitizer_totals": sanitizer_totals or {},
    }
    with open(os.path.join(outdir, "report.json"), "w") as fh:
        json.dump(report, fh, indent=1, default=str)

    if watchdog and watchdog.samples:
        s1, s2 = watchdog.samples
        with open(os.path.join(outdir, "threads.txt"), "w") as fh:
            fh.write(frozen_thread_report(s1, s2, limit=400))

    if server and server.log:
        hits = server.log.scan(since=0)
        if hits:
            with open(os.path.join(outdir, "sanitizer-hits.txt"), "w") as fh:
                fh.write("\n".join(hits[:400]))
    return report


def scan_server_log(server):
    if not server or not server.log:
        return []
    return [{
        "kind": "sanitizer_or_assert_in_server_log",
        "key": None,
        "detail": line[:300],
        "candidates": None, "observed": None,
    } for line in server.log.scan(since=0)[:50]]
