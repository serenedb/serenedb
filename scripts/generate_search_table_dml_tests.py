#!/usr/bin/env python3
"""Generate the randomized search-table DML tests.

DELETE and UPDATE on a search table remove rows by the position their scan
read, and fall back to the rowid when a compaction replaced the segment in
between. The sqllogic files below drive random inserts, deletes, updates and
compactions, also from a second connection while a transaction is open, against
a search table and a plain table with the same rows, and check after every step
that both hold the same rows. The recovery file crashes the server right after a
DELETE commits and checks the row count once the log is replayed.

The files are this script's output: change the script and regenerate them.

Usage:
  python3 scripts/generate_search_table_dml_tests.py [--check]
"""

import argparse
import pathlib
import random
import sys

REPO = pathlib.Path(__file__).resolve().parent.parent
INDEX = REPO / "tests/sqllogic/sdb/pg/index"
RECOVERY = REPO / "tests/sqllogic/recovery"

PREDICATES = [
    "id = {a}",
    "id IN ({a}, {b}, {c})",
    "id BETWEEN {a} AND {a} + {w}",
    "id % {m} = {r}",
    "grp = {g}",
    "grp = {g} AND id % 3 = {r3}",
    "v = 'v{a}'",
    "v LIKE 'v{d}%'",
    "id > {a} AND id < {a} + {w}",
    "NOT (id % {m} <> {r})",
    "id IN (SELECT id FROM {ref} WHERE id % {m} = {r})",
    "id + 0 = {a}",
    "length(v) = {len}",
]


def predicate(rng, ref, hi):
    a = rng.randrange(0, max(hi, 1))
    return rng.choice(PREDICATES).format(
        a=a,
        b=rng.randrange(0, max(hi, 1)),
        c=rng.randrange(0, max(hi, 1)),
        w=rng.randrange(0, 400),
        m=rng.choice([2, 3, 7, 97, 1000, 4096]),
        r=rng.randrange(0, 2),
        g=rng.randrange(0, 50),
        r3=rng.randrange(0, 3),
        d=rng.randrange(1, 10),
        ref=ref,
        len=rng.randrange(2, 8),
    )


class DmlTest:
    def __init__(self, seed, table, ops, background):
        self.rng = random.Random(seed)
        self.t = table
        self.ref = f"{table}_ref"
        self.ops = ops
        self.background = background
        self.out = []
        self.next_id = 0

    def emit(self, sql, conn=None, kind="statement ok"):
        if conn:
            self.out.append(f"connection {conn}")
        self.out.append(kind)
        self.out.append(sql)
        self.out.append("")

    def both(self, sql, conn=None):
        self.emit(sql.format(t=self.t), conn)
        self.emit(sql.format(t=self.ref), conn)

    def check(self, conn=None):
        self.emit(f"VACUUM (REFRESH_TABLE) {self.t};", conn)
        if conn:
            self.out.append(f"connection {conn}")
        self.out += [
            "query",
            f"SELECT (SELECT count(*) FROM (SELECT id, grp, v FROM {self.t} EXCEPT ALL "
            f"SELECT id, grp, v FROM {self.ref})) + (SELECT count(*) FROM (SELECT id, grp, v FROM {self.ref} "
            f"EXCEPT ALL SELECT id, grp, v FROM {self.t})) AS diff;",
            "----",
            "diff",
            "0",
            "",
        ]

    def insert(self, conn=None):
        n = self.rng.choice([1, 5, 100, 2048, 2049, 5000, 20000])
        lo = self.next_id
        self.next_id += n
        self.both(
            f"INSERT INTO {{t}} SELECT i, i % 50, 'v' || i FROM generate_series({lo}, {lo + n - 1}) g(i);",
            conn,
        )

    def delete(self, conn=None):
        p = predicate(self.rng, self.ref, self.next_id)
        self.both(f"DELETE FROM {{t}} WHERE {p};", conn)

    def update(self, conn=None):
        p = predicate(self.rng, self.ref, self.next_id)
        setter = self.rng.choice(
            ["v = v || 'u'", "grp = grp + 100", "v = 'u' || id, grp = 0"]
        )
        self.both(f"UPDATE {{t}} SET {setter} WHERE {p};", conn)

    def compact(self, conn=None):
        self.emit(f"VACUUM (COMPACT_TABLE) {self.t};", conn)

    def render(self):
        opts = (
            "compaction_interval = 20, refresh_interval = 0"
            if self.background
            else "compaction_interval = 0, refresh_interval = 0"
        )
        self.emit(
            f"CREATE TABLE {self.t} (id BIGINT, grp INT, v TEXT) WITH (storage = 'search', {opts});"
        )
        self.emit(f"CREATE TABLE {self.ref} (id BIGINT, grp INT, v TEXT);")
        for _ in range(4):
            self.insert()
            self.emit(f"VACUUM (REFRESH_TABLE) {self.t};")
        self.check()
        for _ in range(self.ops):
            x = self.rng.random()
            if x < 0.25:
                self.insert()
                self.check()
            elif x < 0.45:
                self.delete()
                self.check()
            elif x < 0.6:
                self.update()
                self.check()
            elif x < 0.67:
                self.compact()
                self.check()
            elif x < 0.85:
                self.emit("BEGIN;", "w1")
                self.rng.choice([self.delete, self.update])("w1")
                if self.rng.random() < 0.3:
                    self.insert("w2")
                    self.emit(f"VACUUM (REFRESH_TABLE) {self.t};", "w2")
                if self.rng.random() < 0.7:
                    self.compact("w2")
                self.emit("ROLLBACK;" if self.rng.random() < 0.15 else "COMMIT;", "w1")
                self.check("w1")
            else:
                self.emit("BEGIN;", "w3")
                self.delete("w3")
                self.delete("w3")
                self.emit("COMMIT;", "w3")
                self.check("w3")
        self.emit(f"VACUUM (COMPACT_TABLE) {self.t};")
        self.check()
        self.emit(f"DROP TABLE {self.t};")
        self.emit(f"DROP TABLE {self.ref};")
        return "\n".join(self.out)


def recovery_test(seed, steps):
    rng = random.Random(seed)
    live = set()
    out = [
        "control substitution on",
        "",
        "statement ok",
        "RESET sdb_faults;",
        "",
        "statement ok retry $RETRY_ATTEMPTS backoff $BACKOFF_DURATION",
        "CREATE TABLE rl (id BIGINT, grp INT) WITH (storage = 'search', refresh_interval = 30, compaction_interval = 10,"
        " compaction_max_segments = 3);",
        "",
    ]
    conn = "start"
    next_id = 0
    crashes = 0

    def stmt(sql):
        out.extend([f"connection {conn}", "statement ok", sql, ""])
        if not sql.startswith("VACUUM"):
            out.extend(
                [f"connection {conn}", "statement ok", "VACUUM (REFRESH_TABLE) rl;", ""]
            )

    def check():
        out.extend(
            [
                f"connection {conn}",
                "statement ok",
                "VACUUM (REFRESH_TABLE) rl;",
                "",
                f"connection {conn}",
                "query",
                "SELECT count(*) AS n, coalesce(sum(id), 0) AS s FROM rl;",
                "----",
                "n\ts",
                f"{len(live)}\t{sum(live)}",
                "",
            ]
        )

    for _ in range(steps):
        x = rng.random()
        if x < 0.35 or len(live) < 1000:
            n = rng.choice([500, 2048, 3000, 9000])
            stmt(
                f"INSERT INTO rl SELECT i, i % 17 FROM generate_series({next_id}, {next_id + n - 1}) g(i);"
            )
            live |= set(range(next_id, next_id + n))
            next_id += n
        elif x < 0.7:
            m, r = rng.choice([(7, 3), (13, 1), (101, 50), (2, 1), (1000, 7)])
            stmt(f"DELETE FROM rl WHERE id % {m} = {r};")
            live = {i for i in live if i % m != r}
        elif x < 0.85:
            lo = rng.randrange(0, max(next_id, 1))
            hi = lo + rng.randrange(1, 5000)
            stmt(f"UPDATE rl SET grp = grp + 1 WHERE id BETWEEN {lo} AND {hi};")
            stmt("VACUUM (REFRESH_TABLE) rl;")
        elif x < 0.93:
            check()
        else:
            crashes += 1
            out.extend(
                [
                    f"connection {conn}",
                    "statement ok",
                    "SET sdb_faults = 'crash_after_commit';",
                    "",
                ]
            )
            m, r = rng.choice([(11, 4), (17, 9), (3, 2)])
            out.extend(
                [
                    f"connection {conn}",
                    "statement error connection closed",
                    f"DELETE FROM rl WHERE id % {m} = {r};",
                    "",
                ]
            )
            live = {i for i in live if i % m != r}
            conn = f"after_crash_{crashes}"
            out.extend(
                [
                    f"connection {conn}",
                    "query ok retry $RETRY_ATTEMPTS backoff $BACKOFF_DURATION",
                    "SELECT count(*) AS n, coalesce(sum(id), 0) AS s FROM rl;",
                    "----",
                    "n\ts",
                    f"{len(live)}\t{sum(live)}",
                    "",
                ]
            )
    check()
    out.extend([f"connection {conn}", "statement ok", "DROP TABLE rl;", ""])
    return "\n".join(out)


def outputs():
    return {
        INDEX / "search_table_positional_dml_random.test": DmlTest(11, "pd_a", 80, False).render(),
        INDEX / "search_table_positional_dml_background.test": DmlTest(23, "pd_b", 80, True).render(),
        INDEX / "search_table_positional_dml_long.test_slow": DmlTest(37, "pd_c", 200, True).render(),
        RECOVERY / "search_table_positional_removal_load.test_slow": recovery_test(5, 120),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail if a file is stale")
    args = parser.parse_args()
    stale = []
    for path, text in outputs().items():
        if args.check:
            if not path.exists() or path.read_text() != text:
                stale.append(path)
        else:
            path.write_text(text)
    for path in stale:
        print(f"{path.relative_to(REPO)} is stale: run {pathlib.Path(__file__).relative_to(REPO)}")
    return 1 if stale else 0


if __name__ == "__main__":
    sys.exit(main())
