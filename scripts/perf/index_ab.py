import argparse
import collections
import json
import math
import os
import re
import statistics
import subprocess
import sys
import time

COMMANDS = "count,top_10,top_100,top_100_count"
HINT_CALLS = ("madvise", "fadvise64", "mincore", "sync_file_range")
STRACE_LINE = re.compile(r"^(?:\[pid\s+\d+\]\s+)?(\w+)\((.*)\)\s+=\s+(-?\d+)")


def load_queries(path, limit):
    out = []
    with open(path) as f:
        for line in f:
            q = json.loads(line)
            out.append(q["query"])
            if limit and len(out) == limit:
                break
    return out


def node_cpus(node):
    with open(f"/sys/devices/system/node/node{node}/cpulist") as f:
        spec = f.read().strip()
    cpus = []
    for part in spec.split(","):
        lo, _, hi = part.partition("-")
        cpus.extend(range(int(lo), int(hi or lo) + 1))
    return cpus


def cpu_busy(cpus):
    busy = 0
    with open("/proc/stat") as f:
        for line in f:
            name, *fields = line.split()
            if name.startswith("cpu") and name[3:].isdigit() and int(name[3:]) in cpus:
                values = list(map(int, fields))
                busy += sum(values) - values[3] - values[4]
    return busy


def process_ticks(pid):
    with open(f"/proc/{pid}/stat") as f:
        fields = f.read().rsplit(")", 1)[1].split()
    return int(fields[11]) + int(fields[12]) + int(fields[13]) + int(fields[14])


def pinned(args):
    return ["numactl", f"--cpunodebind={args.node}", f"--membind={args.node}"]


def run_warm(binary, args, queries, commands):
    lines = [f"{cmd}\t{q}" for q in queries for cmd in commands]
    cpus = node_cpus(args.node)
    proc = subprocess.Popen(pinned(args) + [binary, args.index, "--repeat", str(args.repeat)],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    busy_before = cpu_busy(cpus)
    started = time.monotonic()
    out, _ = proc.communicate("\n".join(lines) + "\n")
    elapsed = time.monotonic() - started
    busy_after = cpu_busy(cpus)
    own = 0
    try:
        own = process_ticks(proc.pid)
    except OSError:
        pass
    hz = os.sysconf("SC_CLK_TCK")
    foreign = max(0.0, (busy_after - busy_before) / hz - elapsed) / (len(cpus) * elapsed) if elapsed else 0.0
    medians = {}
    for line, key in zip(out.splitlines(), [(cmd, i) for i in range(len(queries)) for cmd in commands]):
        parts = line.split("\t")
        if len(parts) == 2:
            medians[f"{key[0]}|{key[1]}"] = int(parts[1])
    return {"medians": medians, "elapsed": elapsed, "foreign_load": foreign, "own_ticks": own}


def evict(index):
    subprocess.run(["vmtouch", "-q", "-e", index], check=True)


def io_counters(pid):
    read_bytes = 0
    with open(f"/proc/{pid}/io") as f:
        for line in f:
            if line.startswith("read_bytes:"):
                read_bytes = int(line.split()[1])
    with open(f"/proc/{pid}/stat") as f:
        majflt = int(f.read().rsplit(")", 1)[1].split()[9])
    return read_bytes, majflt


def run_cold(binary, args, queries, commands):
    proc = subprocess.Popen(pinned(args) + [binary, args.index], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL, text=True, bufsize=1)
    result = {}
    for i, q in enumerate(queries):
        for cmd in commands:
            evict(args.index)
            before = io_counters(proc.pid)
            started = time.monotonic()
            proc.stdin.write(f"{cmd}\t{q}\n")
            proc.stdin.flush()
            proc.stdout.readline()
            elapsed = time.monotonic() - started
            after = io_counters(proc.pid)
            result[f"{cmd}|{i}"] = {"ns": int(elapsed * 1e9), "read_bytes": after[0] - before[0],
                                    "majflt": after[1] - before[1]}
    proc.stdin.close()
    proc.wait()
    return result


def run_hints(binary, args, queries, commands, scratch):
    result = {}
    for i, q in enumerate(queries):
        for cmd in commands:
            evict(args.index)
            trace = os.path.join(scratch, "trace.txt")
            proc = subprocess.Popen(["strace", "-f", "-qq", "-o", trace, "-e", "trace=" + ",".join(HINT_CALLS),
                                     binary, args.index], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                    stderr=subprocess.DEVNULL, text=True, bufsize=1)
            proc.stdin.write("noop\n")
            proc.stdin.flush()
            proc.stdout.readline()
            opened = os.path.getsize(trace)
            proc.stdin.write(f"{cmd}\t{q}\n")
            proc.stdin.flush()
            proc.stdout.readline()
            proc.stdin.close()
            proc.wait()
            counts = collections.Counter()
            with open(trace) as f:
                f.seek(opened)
                for line in f:
                    m = STRACE_LINE.match(line)
                    if not m:
                        continue
                    name, call_args = m.group(1), m.group(2).split(", ")
                    counts[name] += 1
                    length = call_args[1] if name in ("madvise", "mincore") else call_args[2] if len(call_args) > 2 else "0"
                    counts[name + "_bytes"] += int(length, 0) if length.lstrip("-").isdigit() or length.startswith("0x") else 0
            result[f"{cmd}|{i}"] = dict(counts)
    return result


def pct(values, q):
    values = sorted(values)
    return values[min(len(values) - 1, int(q * (len(values) - 1) + 0.5))]


def summarize_warm(runs, commands, queries):
    per_side = {"base": collections.defaultdict(list), "test": collections.defaultdict(list)}
    for run in runs:
        if run["discarded"]:
            continue
        for key, ns in run["result"]["medians"].items():
            per_side[run["side"]][key].append(ns)
    lines = ["command | queries | geomean ratio | p95 ratio | worst"]
    worst_all = []
    for cmd in commands:
        ratios = []
        for i in range(len(queries)):
            key = f"{cmd}|{i}"
            base, test = per_side["base"].get(key), per_side["test"].get(key)
            if base and test:
                ratio = statistics.median(test) / statistics.median(base)
                ratios.append(ratio)
                worst_all.append((ratio, cmd, queries[i]))
        if not ratios:
            continue
        geomean = math.exp(statistics.mean(math.log(r) for r in ratios))
        lines.append(f"{cmd} | {len(ratios)} | {geomean:.4f} | {pct(ratios, 0.95):.4f} | {max(ratios):.3f}")
    worst_all.sort(reverse=True)
    lines.append("")
    lines.append("worst queries: ratio | command | query")
    lines.extend(f"{r:.3f} | {c} | {q}" for r, c, q in worst_all[:20])
    return "\n".join(lines)


def summarize_pairs(base, test, metrics):
    lines = ["metric | total base | total test | queries differing"]
    for metric in metrics:
        tb = sum(v.get(metric, 0) for v in base.values())
        tt = sum(v.get(metric, 0) for v in test.values())
        differing = sum(1 for k in base if base[k].get(metric, 0) != test.get(k, {}).get(metric, 0))
        lines.append(f"{metric} | {tb} | {tt} | {differing}")
    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser(description="A/B of search-benchmark-game-query binaries over one index")
    p.add_argument("--base", required=True)
    p.add_argument("--test", required=True)
    p.add_argument("--index", required=True)
    p.add_argument("--queries", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--mode", choices=("warm", "cold", "hints"), default="warm")
    p.add_argument("--commands", default=COMMANDS)
    p.add_argument("--repeat", type=int, default=10)
    p.add_argument("--pairs", type=int, default=5)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--node", type=int, default=0)
    p.add_argument("--max-foreign-load", type=float, default=0.05)
    args = p.parse_args()
    os.makedirs(args.out, exist_ok=True)
    queries = load_queries(args.queries, args.limit)
    commands = args.commands.split(",")
    if args.mode == "warm":
        runs = []
        for pair in range(args.pairs):
            order = ("base", "test") if pair % 2 == 0 else ("test", "base")
            for side in order:
                while True:
                    result = run_warm(getattr(args, side), args, queries, commands)
                    discarded = result["foreign_load"] > args.max_foreign_load
                    runs.append({"pair": pair, "side": side, "discarded": discarded, "result": result})
                    print(f"pair {pair} {side}: {result['elapsed']:.1f} s, foreign load "
                          f"{result['foreign_load']:.3f}{' (discarded)' if discarded else ''}", file=sys.stderr)
                    if not discarded:
                        break
        summary = summarize_warm(runs, commands, queries)
        raw = runs
    elif args.mode == "cold":
        base = run_cold(args.base, args, queries, commands)
        test = run_cold(args.test, args, queries, commands)
        summary = summarize_pairs(base, test, ("read_bytes", "majflt"))
        raw = {"base": base, "test": test}
    else:
        base = run_hints(args.base, args, queries, commands, args.out)
        test = run_hints(args.test, args, queries, commands, args.out)
        metrics = [m for name in HINT_CALLS for m in (name, name + "_bytes")]
        summary = summarize_pairs(base, test, metrics)
        raw = {"base": base, "test": test}
    with open(os.path.join(args.out, f"{args.mode}.json"), "w") as f:
        json.dump(raw, f)
    with open(os.path.join(args.out, f"{args.mode}.txt"), "w") as f:
        f.write(summary + "\n")
    print(summary)


if __name__ == "__main__":
    main()
