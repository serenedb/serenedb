#!/usr/bin/env python3
"""Consolidate per-(binary,case,size) CSVs from the sweep into a markdown table.

Reads `<out>/raw/*.csv`, pairs main vs mine by (case, rows, phase),
emits markdown with delta% per phase and a flagged-regressions section.
"""
from __future__ import annotations

import csv
import glob
import os
import sys
from collections import defaultdict


def read_csvs(raw_dir: str) -> list[dict]:
    rows: list[dict] = []
    for path in sorted(glob.glob(os.path.join(raw_dir, "*.csv"))):
        with open(path) as f:
            for r in csv.DictReader(f):
                rows.append(r)
    return rows


def to_f(s: str | None) -> float | None:
    if not s:
        return None
    try:
        return float(s.replace(",", ""))
    except ValueError:
        return None


def fmt_pct(main: float | None, mine: float | None) -> str:
    if main is None or mine is None or main == 0:
        return "--"
    pct = (mine - main) / main * 100.0
    sign = "+" if pct >= 0 else ""
    flag = ""
    if pct >= 5:
        flag = " ⚠️"
    elif pct >= 1:
        flag = " ↑"
    elif pct <= -5:
        flag = " 🟢"
    elif pct <= -1:
        flag = " ↓"
    return f"{sign}{pct:.1f}%{flag}"


def fmt_v(v: float | None, unit: str = "s") -> str:
    if v is None:
        return "--"
    if unit == "s":
        return f"{v:.3f}s"
    if unit == "Mc":
        return f"{v / 1e6:.1f}M"
    return f"{v:.0f}"


def main():
    out_dir = sys.argv[1]
    raw = os.path.join(out_dir, "raw")
    rows = read_csvs(raw)
    # Group: (case, rows, phase) -> {binary: row}
    by_key: dict[tuple[str, str, str], dict[str, dict]] = defaultdict(dict)
    for r in rows:
        k = (r["case"], r["rows"], r["phase"])
        by_key[k][r["binary"]] = r

    # Order
    phase_order = ["setup", "backfill", "compact", "hot", "cold_open", "cold_read", "online"]
    case_order = ["english_icu", "english_simple", "ngram", "uuid", "url",
                  "integer", "bigint", "boolean", "hnsw"]
    sizes = sorted({k[1] for k in by_key.keys()}, key=int)

    out = ["# iresearch perf sweep -- main vs mine\n",
           f"_Source: `{out_dir}`_\n",
           "Delta column = `(mine - main) / main`. ↑ >=1%, ⚠️ >=5% regression. ↓ >=1%, 🟢 >=5% improvement.\n"]

    regressions: list[tuple[str, str, str, float]] = []

    for size in sizes:
        out.append(f"\n## rows = {size}\n")
        out.append("| case | phase | main wall | mine wall | Δ wall | main cpu | mine cpu | Δ cpu |")
        out.append("|---|---|---|---|---|---|---|---|")
        for case in case_order:
            for phase in phase_order:
                k = (case, size, phase)
                if k not in by_key:
                    continue
                main_r = by_key[k].get("main", {})
                mine_r = by_key[k].get("mine", {})
                m_wall = to_f(main_r.get("wall_seconds"))
                n_wall = to_f(mine_r.get("wall_seconds"))
                m_cpu = to_f(main_r.get("cpu_total"))
                n_cpu = to_f(mine_r.get("cpu_total"))
                d_wall = fmt_pct(m_wall, n_wall)
                d_cpu = fmt_pct(m_cpu, n_cpu)
                out.append(
                    f"| {case} | {phase} | {fmt_v(m_wall)} | {fmt_v(n_wall)} | {d_wall} | "
                    f"{fmt_v(m_cpu)} | {fmt_v(n_cpu)} | {d_cpu} |"
                )
                if m_wall and n_wall and m_wall > 0 and (n_wall - m_wall) / m_wall >= 0.05:
                    regressions.append((case, size, phase, (n_wall - m_wall) / m_wall * 100))

    if regressions:
        out.append("\n## ⚠️ Wall-clock regressions >=5%\n")
        out.append("| case | rows | phase | regression |")
        out.append("|---|---|---|---|")
        for case, size, phase, pct in sorted(regressions, key=lambda x: -x[3]):
            out.append(f"| {case} | {size} | {phase} | +{pct:.1f}% |")
    else:
        out.append("\n## ✅ No wall-clock regressions >=5% detected\n")

    print("\n".join(out))


if __name__ == "__main__":
    main()
