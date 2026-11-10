import collections
import json
import random
import statistics
import sys

PAGE = 4096
KIB = 1024
MIB = 1024 * KIB
CAP = 16 * MIB


def load(path):
    out = []
    for line in open(path):
        rec = json.loads(line)
        if rec["cmd"] != "top_100" or rec["result"] in ("UNSUPPORTED", "CRASH"):
            continue
        files = {}
        for name, runs in rec["files"].items():
            pages = set()
            for start, length in runs:
                pages.update(range(start, start + length))
            files[name] = pages
        out.append(files)
    return out


def needed(query, block):
    return {name: sorted({p * PAGE // block for p in pages}) for name, pages in query.items()}


def plan(blocks_by_file, block, gap):
    gets = 0
    fetched = []
    for name, blocks in blocks_by_file.items():
        first = last = None
        for b in blocks:
            if first is not None and (b - last - 1) * block < max(gap, 1) and (b - first + 1) * block <= CAP:
                last = b
                continue
            if first is not None:
                gets += 1
                fetched.extend((name, x) for x in range(first, last + 1))
            first = last = b
        if first is not None:
            gets += 1
            fetched.extend((name, x) for x in range(first, last + 1))
    return gets, fetched


def pct(values, q):
    values = sorted(values)
    return values[min(len(values) - 1, int(q * (len(values) - 1) + 0.5))]


def cold(queries, block, gap):
    gets, mib = [], []
    for q in queries:
        g, fetched = plan(needed(q, block), block, gap)
        gets.append(g)
        mib.append(len(fetched) * block / MIB)
    return gets, mib


def stream(n, length, seed):
    rnd = random.Random(seed)
    order = []
    while len(order) < length:
        perm = list(range(n))
        rnd.shuffle(perm)
        order.extend(perm)
    return order[:length]


def warm(queries, block, gap, budget, order, warmup):
    capacity = max(1, budget // block)
    cache = collections.OrderedDict()
    gets, mib = [], []
    per_query = [needed(q, block) for q in queries]
    for i, qi in enumerate(order):
        missing = {}
        for name, blocks in per_query[qi].items():
            miss = []
            for b in blocks:
                key = (name, b)
                if key in cache:
                    cache.move_to_end(key)
                else:
                    miss.append(b)
            if miss:
                missing[name] = miss
        g, fetched = plan(missing, block, gap)
        for key in fetched:
            cache[key] = True
            cache.move_to_end(key)
            if len(cache) > capacity:
                cache.popitem(last=False)
        if i >= warmup:
            gets.append(g)
            mib.append(len(fetched) * block / MIB)
    return gets, mib


def main():
    queries = load(sys.argv[1])
    print(f"{sys.argv[1].split('/')[-2]}: {len(queries)} top_100 queries")
    print("cold | block | gap | GETs median / p90 | MiB fetched median / p90")
    for block in (64 * KIB, 256 * KIB, MIB, 4 * MIB):
        for gap in (0, MIB):
            g, m = cold(queries, block, gap)
            print(f"cold | {block // KIB}K | {gap // KIB}K | {statistics.median(g):.0f} / {pct(g, 0.9):.0f} | "
                  f"{statistics.median(m):.1f} / {pct(m, 0.9):.1f}")
    length = 20 * len(queries)
    order = stream(len(queries), length, 42)
    print("warm uniform | block | gap | cache | GETs mean / p90 | MiB fetched mean")
    for block in (64 * KIB, 256 * KIB, MIB):
        for budget in (256 * MIB, 1024 * MIB):
            g, m = warm(queries, block, MIB, budget, order, length // 5)
            print(f"warm | {block // KIB}K | 1024K | {budget // MIB}M | {statistics.mean(g):.2f} / {pct(g, 0.9)} | "
                  f"{statistics.mean(m):.2f}")


if __name__ == "__main__":
    main()
