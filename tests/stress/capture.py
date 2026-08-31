import json
import os

from procutil import frozen_thread_report


def write_artifacts(outdir, meta, findings, watchdog, server, journal_tail):
    os.makedirs(outdir, exist_ok=True)
    report = {
        "meta": meta,
        "watchdog": watchdog.as_dict() if watchdog else None,
        "findings": findings,
        "finding_count": len(findings),
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
