#!/usr/bin/env python3
"""Property fuzz for index-claim NULL semantics.

Generates random SQL predicates over nullable indexed columns and asserts
that the inverted-index view returns exactly the rows the base table
returns: the base table row-executes three-valued logic, so any divergence
is a claim soundness bug (a filter that readmits or drops NULL rows).

Usage:
  ./tests/fuzz/null_semantics_fuzz.py --port 7895 [--queries 500] [--seed N]

Requires a running serened on the given port. Prints the seed on start;
on failure prints the offending predicate plus both result sets and exits
non-zero. Re-run with --seed to reproduce.
"""

import argparse
import random
import subprocess
import sys

KW_VALUES = ["a", "b", "c", "d", "e"]
NUM_VALUES = list(range(1, 10))
PAD_VALUES = ["p", "q"]


def psql(port, sql):
    return subprocess.run(
        ["psql", "-h", "127.0.0.1", "-p", str(port), "-U", "postgres",
         "-d", "postgres", "-X", "-q", "-t", "-A", "-v", "ON_ERROR_STOP=1"],
        input=sql, capture_output=True, text=True)


def setup(port, rng):
    rows = []
    for i in range(1, 41):
        kw = "NULL" if rng.random() < 0.3 else f"'{rng.choice(KW_VALUES)}'"
        num = "NULL" if rng.random() < 0.3 else str(rng.choice(NUM_VALUES))
        pad = f"'{rng.choice(PAD_VALUES)}'"
        rows.append(f"({i}, {kw}, {num}, {pad})")
    ddl = f"""
DROP TABLE IF EXISTS fz;
DROP TEXT SEARCH DICTIONARY IF EXISTS fz_kw;
CREATE TEXT SEARCH DICTIONARY fz_kw(template='keyword');
CREATE TABLE fz(id INTEGER PRIMARY KEY, kw VARCHAR, num INTEGER,
                pad VARCHAR NOT NULL);
CREATE INDEX fz_idx ON fz USING inverted(id, kw fz_kw, num, pad fz_kw)
    WITH (refresh_interval=0, compaction_interval=0);
INSERT INTO fz VALUES {', '.join(rows[:20])};
VACUUM (REFRESH_TABLE) fz;
INSERT INTO fz VALUES {', '.join(rows[20:])};
VACUUM (REFRESH_TABLE) fz;
"""
    r = psql(port, ddl)
    if r.returncode != 0:
        print("setup failed:", r.stderr, file=sys.stderr)
        sys.exit(2)


def kw_const(rng):
    return f"'{rng.choice(KW_VALUES)}'"


def num_const(rng):
    return str(rng.choice(NUM_VALUES))


def in_list(rng, const):
    n = rng.randint(1, 4)
    items = [const(rng) for _ in range(n)]
    if rng.random() < 0.25:
        items.insert(rng.randrange(len(items) + 1), "NULL")
    return ", ".join(items)


def leaf(rng):
    kind = rng.randrange(12)
    if kind == 0:
        return f"kw {rng.choice(['=', '!=', '<', '>', '<=', '>='])} {kw_const(rng)}"
    if kind == 1:
        return f"num {rng.choice(['=', '!=', '<', '>', '<=', '>='])} {num_const(rng)}"
    if kind == 2:
        return f"kw IN ({in_list(rng, kw_const)})"
    if kind == 3:
        return f"kw NOT IN ({in_list(rng, kw_const)})"
    if kind == 4:
        return f"num IN ({in_list(rng, num_const)})"
    if kind == 5:
        return f"num NOT IN ({in_list(rng, num_const)})"
    if kind == 6:
        lo, hi = sorted((rng.choice(NUM_VALUES), rng.choice(NUM_VALUES)))
        neg = "NOT " if rng.random() < 0.5 else ""
        return f"num {neg}BETWEEN {lo} AND {hi}"
    if kind == 7:
        return f"kw {rng.choice(['IS NULL', 'IS NOT NULL'])}"
    if kind == 8:
        return f"num {rng.choice(['IS NULL', 'IS NOT NULL'])}"
    if kind == 9:
        return f"kw LIKE '{rng.choice(['a%', '%b', '_c%', 'd'])}'"
    if kind == 10:
        return f"kw IS {rng.choice(['', 'NOT '])}DISTINCT FROM {kw_const(rng)}"
    return f"pad = '{rng.choice(PAD_VALUES)}'"


def predicate(rng, depth):
    if depth == 0 or rng.random() < 0.35:
        p = leaf(rng)
    else:
        op = rng.choice(["AND", "OR"])
        p = f"({predicate(rng, depth - 1)} {op} {predicate(rng, depth - 1)})"
    if rng.random() < 0.3:
        p = f"NOT ({p})"
    return p


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--queries", type=int, default=500)
    ap.add_argument("--seed", type=int, default=random.randrange(1 << 32))
    args = ap.parse_args()
    print(f"seed={args.seed} queries={args.queries}")
    rng = random.Random(args.seed)
    setup(args.port, rng)

    failures = 0
    for i in range(args.queries):
        pred = predicate(rng, 3)
        base = psql(args.port, f"SELECT id FROM fz WHERE {pred} ORDER BY id;")
        idx = psql(args.port, f"SELECT id FROM fz_idx WHERE {pred} ORDER BY id;")
        if base.returncode != 0:
            print(f"[{i}] base ERROR (grammar bug?): {pred}\n{base.stderr}")
            failures += 1
            continue
        if idx.returncode != 0:
            print(f"[{i}] index ERROR: {pred}\n{idx.stderr}")
            failures += 1
            continue
        if base.stdout != idx.stdout:
            b = base.stdout.split()
            x = idx.stdout.split()
            print(f"[{i}] MISMATCH: {pred}\n  base:  {b}\n  index: {x}\n"
                  f"  extra: {sorted(set(x) - set(b), key=int)}"
                  f" missing: {sorted(set(b) - set(x), key=int)}")
            failures += 1
    psql(args.port, "DROP TABLE fz; DROP TEXT SEARCH DICTIONARY fz_kw;")
    if failures:
        print(f"FAILED: {failures}/{args.queries} (seed={args.seed})")
        sys.exit(1)
    print(f"OK: {args.queries} predicates, index == row-level")


if __name__ == "__main__":
    main()
