import collections
import json
import statistics
import sys

PAGE = 4096
KIB = 1024
MIB = 1024 * KIB
BLOCKS = [64 * KIB, 256 * KIB, 1 * MIB, 4 * MIB, 16 * MIB]
GET_CAP = 16 * MIB


def kind(name):
    parts = name.split(".")
    return parts[-1] if len(parts) > 1 else name


def pages_of(runs):
    for start, length in runs:
        yield from range(start, start + length)


def block_stats(pages, block):
    blocks = sorted({p * PAGE // block for p in pages})
    gets = 0
    run_len = 0
    prev = None
    per_get = max(1, GET_CAP // block)
    for b in blocks:
        if prev is not None and b == prev + 1 and run_len < per_get:
            run_len += 1
        else:
            gets += 1
            run_len = 1
        prev = b
    return len(blocks), gets


def runs_at(pages, unit):
    units = sorted({p * PAGE // unit for p in pages})
    count = 0
    prev = None
    for u in units:
        if prev is None or u != prev + 1:
            count += 1
        prev = u
    return count


def pct(values, q):
    values = sorted(values)
    if not values:
        return 0
    return values[min(len(values) - 1, int(q * (len(values) - 1) + 0.5))]


def main():
    path = sys.argv[1]
    records = [json.loads(line) for line in open(path)]
    rows = collections.defaultdict(list)
    per_kind = collections.defaultdict(lambda: collections.defaultdict(list))
    working_set = collections.defaultdict(set)
    aligned_runs = 0
    total_runs = 0
    unsupported = 0
    open_pages = None
    misaligned = set()
    for rec in records:
        if rec["result"] in ("UNSUPPORTED", "CRASH"):
            unsupported += 1
            continue
        if open_pages is None:
            open_pages = rec["open_pages"]
        for s, e, o, n in rec["maps"]:
            if (e - s) >= 2 * MIB and (s % (64 * KIB) or o):
                misaligned.add(n)
        all_pages = []
        for name, runs in rec["files"].items():
            pages = list(pages_of(runs))
            k = kind(name)
            per_kind[(rec["cmd"], k)]["pages"].append(len(pages))
            per_kind[(rec["cmd"], k)]["runs64k"].append(runs_at(pages, 64 * KIB))
            all_pages.extend((name, p) for p in pages)
            for start, length in runs:
                total_runs += 1
                aligned_runs += start % 16 == 0 and length % 16 == 0
        tag = rec["tags"][0] if rec["tags"] else "?"
        stats = {"pages": len(all_pages)}
        by_file = collections.defaultdict(list)
        for name, p in all_pages:
            by_file[name].append(p)
        stats["runs4k"] = sum(runs_at(ps, PAGE) for ps in by_file.values())
        stats["runs64k"] = sum(runs_at(ps, 64 * KIB) for ps in by_file.values())
        for block in BLOCKS:
            nb = ng = 0
            for name, ps in by_file.items():
                b, g = block_stats(ps, block)
                nb += b
                ng += g
                working_set[(rec["cmd"], block)].update((name, p * PAGE // block) for p in ps)
            stats[f"blocks{block // KIB}k"] = nb
            stats[f"gets{block // KIB}k"] = ng
        for key in ("all", tag):
            rows[(rec["cmd"], key)].append(stats)

    print(f"records: {len(records)}, unsupported: {unsupported}")
    if records:
        segments = {name.split(".")[0] for _, _, _, name in records[0]["maps"]}
        print(f"segments mapped: {len(segments)}, files mapped: {len(records[0]['maps'])}")
    print(f"touched runs aligned to 64 KiB windows: {aligned_runs}/{total_runs}")
    print(f"large mappings not 64 KiB aligned: {sorted(misaligned) or 'none'}")
    if open_pages:
        print("pages touched by opening the index:",
              {k: v for k, v in sorted(open_pages.items())}, f"total {sum(open_pages.values()) * PAGE / MIB:.1f} MiB")
    metrics = ["pages", "runs64k"] + [f"blocks{b // KIB}k" for b in BLOCKS] + [f"gets{b // KIB}k" for b in BLOCKS]
    print("\nper query: median / p90 / max (pages = 4 KiB pages touched; runs64k = separate contiguous ranges at"
          " 64 KiB; blocksN = distinct N blocks; getsN = GETs when adjacent missing N blocks merge up to 16 MiB)")
    header = "cmd | group | n | " + " | ".join(metrics)
    print(header)
    print("|".join(["---"] * (len(metrics) + 3)))
    for (cmd, group), items in sorted(rows.items(), key=lambda kv: (kv[0][0], kv[0][1] != "all", kv[0][1])):
        cells = []
        for m in metrics:
            vals = [s[m] for s in items]
            cells.append(f"{statistics.median(vals):.0f}/{pct(vals, 0.9):.0f}/{max(vals):.0f}")
        print(f"{cmd} | {group} | {len(items)} | " + " | ".join(cells))
    print("\nper file kind: mean pages / mean runs64k per query")
    for (cmd, k), d in sorted(per_kind.items()):
        n = len(rows[(cmd, "all")])
        print(f"{cmd} | {k} | {sum(d['pages']) / n:.1f} | {sum(d['runs64k']) / n:.1f}")
    print("\nworking set of all queries, MiB of distinct blocks")
    for cmd in sorted({cmd for cmd, _ in working_set}):
        cells = " | ".join(f"{b // KIB}K {len(working_set[(cmd, b)]) * b / MIB:.0f}" for b in BLOCKS)
        print(f"{cmd} | {cells}")


if __name__ == "__main__":
    main()
