#!/usr/bin/env python3
import json
import pathlib
import sys

SIGNALS = ("logs", "traces", "metrics")


def results(directory, signal):
    path = pathlib.Path(directory) / f"otel-{signal}.json"
    if not path.exists():
        return []
    return json.loads(path.read_text())["results"]


def row(signal, engine, r):
    if "error" in r:
        return f"| {signal} | {engine} | failed: {r['error'][:80]} | | | | | | | | |"
    visible = "timeout" if r["visible_s"] is None else f"{r['visible_s']:.1f}"
    stored = "ok" if r["stored"] == r["expected"] else \
        f"{r['stored']}/{r['expected'] or '?'} ({r['rejected']} rejected)"
    return (f"| {signal} | {engine} | {r['mb_s']:.0f} | {r['records_s'] / 1000:.0f}k "
            f"| {r['ack_s']:.1f} | {visible} | {r['server_cpu_s']:.0f} "
            f"| {r['peak_rss_bytes'] / 2 ** 20:.0f} | {r['disk_bytes'] / 2 ** 20:.0f} "
            f"| {r['p99_ms']:.0f} | {stored} |")


def main():
    if len(sys.argv) < 2:
        sys.exit("usage: table.py [LABEL=]DIR ...  (DIR holds run.py's otel-<signal>.json)")
    sources = []
    for arg in sys.argv[1:]:
        label, sep, directory = arg.partition("=")
        sources.append((label, directory) if sep else ("", arg))
    print("| signal | engine | MB/s | records/s | ack s | visible s | srv CPU s "
          "| RSS MB | disk MB | p99 ms | stored |")
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for signal in SIGNALS:
        for label, directory in sources:
            for r in results(directory, signal):
                engine = f"{r['engine']} ({label})" if label else r["engine"]
                print(row(signal, engine, r))


if __name__ == "__main__":
    main()
