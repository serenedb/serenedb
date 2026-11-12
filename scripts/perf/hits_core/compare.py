#!/usr/bin/env python3
import csv
import pathlib
import sys

SIDES = ["main_duck", "main_search", "branch"]


def load(path):
    rows = {}
    if not path.exists():
        return rows
    with path.open() as f:
        for row in csv.reader(f, delimiter="\t"):
            if row:
                rows[row[0]] = row[1:]
    return rows


def number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def ratio(branch, best, higher_better):
    if branch is None or best is None or best == 0 or branch == 0:
        return ""
    r = branch / best if higher_better else best / branch
    return f"{r:.2f}x"


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: compare.py <validate|full> <results dir>")
    mode = sys.argv[1]
    base = pathlib.Path(sys.argv[2])
    data = {}
    for side in SIDES:
        data[side] = load(base / f"{side}.{mode}" / "results.tsv")
        data[side].update(load(base / f"{side}.{mode}" / "small.tsv"))
    names = []
    for side in SIDES:
        for name in data[side]:
            if name not in names:
                names.append(name)

    def cell(side, name, index):
        row = data[side].get(name)
        if not row:
            return None, "-"
        if row[0] == "FAIL":
            return None, "FAIL"
        value = row[index] if index < len(row) else ""
        return number(value), value or "-"

    print("time-like rows: ms (bulk/dml: wall, cpu s, background cpu s), lower is better; ratio = best main / branch")
    print(f"{'case':44} " + " ".join(f"{s:>24}" for s in SIDES) + "  branch vs best main")
    for name in names:
        if name.startswith("small."):
            continue
        cols = []
        values = []
        for side in SIDES:
            row = data[side].get(name)
            if not row:
                cols.append("-")
                values.append(None)
                continue
            if row[0] == "FAIL":
                cols.append("FAIL")
                values.append(None)
                continue
            if name.startswith(("bulk.", "dml.")):
                ms = number(row[0])
                cols.append(f"{row[0]} ({row[1]}/{row[2]})")
            else:
                ms = number(row[1])
                cols.append(f"{row[1]} (first {row[0]})")
            values.append(ms)
        best = min((v for v in values[:2] if v is not None), default=None)
        print(f"{name:44} " + " ".join(f"{c:>24}" for c in cols) + f"  {ratio(values[2], best, False)}")

    print()
    print("small transactions: writer tps / avg ms [failed, retried, aborted] | reader tps / avg ms; ratio = branch / best main writer tps")
    print(f"{'case':36} " + " ".join(f"{s:>40}" for s in SIDES) + "  branch vs best main")
    for name in names:
        if not name.startswith("small."):
            continue
        cols = []
        tps = []
        for side in SIDES:
            row = data[side].get(name)
            if not row:
                cols.append("-")
                tps.append(None)
                continue
            w = f"{row[0]}/{row[1]} [{row[2]},{row[3]},{row[4]}]"
            r = f" | {row[5]}/{row[6]}" if len(row) > 6 and row[5] else ""
            cols.append(w + r)
            tps.append(number(row[0]))
        best = max((v for v in tps[:2] if v is not None), default=None)
        print(f"{name:36} " + " ".join(f"{c:>40}" for c in cols) + f"  {ratio(tps[2], best, True)}")


if __name__ == "__main__":
    main()
