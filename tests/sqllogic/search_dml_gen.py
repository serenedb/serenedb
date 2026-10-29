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


class Dml:
    def __init__(self, t, table, background):
        self.t = t
        self.rng = t.rng
        self.table = table
        self.ref = f"{table}_ref"
        self.background = background
        self.next_id = 0

    def statement(self, sql, conn=None):
        if conn:
            self.t.connection(conn)
        self.t.statement(sql)

    def both(self, sql, conn=None):
        self.statement(sql.format(t=self.table), conn)
        self.statement(sql.format(t=self.ref), conn)

    def check(self, conn=None):
        self.statement(f"VACUUM (REFRESH_TABLE) {self.table};", conn)
        if conn:
            self.t.connection(conn)
        self.t.expect(
            f"SELECT (SELECT count(*) FROM (SELECT id, grp, v FROM {self.table} "
            f"EXCEPT ALL SELECT id, grp, v FROM {self.ref})) + (SELECT count(*) "
            f"FROM (SELECT id, grp, v FROM {self.ref} EXCEPT ALL SELECT id, grp, "
            f"v FROM {self.table})) AS diff;",
            ["diff"], [[0]])

    def insert(self, conn=None):
        n = self.rng.choice([1, 5, 100, 2048, 2049, 5000, 20000])
        lo = self.next_id
        self.next_id += n
        self.both(
            f"INSERT INTO {{t}} SELECT i, i % 50, 'v' || i FROM "
            f"generate_series({lo}, {lo + n - 1}) g(i);",
            conn)

    def delete(self, conn=None):
        where = predicate(self.rng, self.ref, self.next_id)
        self.both(f"DELETE FROM {{t}} WHERE {where};", conn)

    def update(self, conn=None):
        where = predicate(self.rng, self.ref, self.next_id)
        setter = self.rng.choice(
            ["v = v || 'u'", "grp = grp + 100", "v = 'u' || id, grp = 0"])
        self.both(f"UPDATE {{t}} SET {setter} WHERE {where};", conn)

    def compact(self, conn=None):
        self.statement(f"VACUUM (COMPACT_TABLE) {self.table};", conn)

    def run(self, ops):
        intervals = ("compaction_interval = 20, refresh_interval = 0"
                     if self.background else
                     "compaction_interval = 0, refresh_interval = 0")
        self.statement(f"CREATE TABLE {self.table} (id BIGINT, grp INT, v TEXT) "
                       f"WITH (storage = 'search', {intervals});")
        self.statement(f"CREATE TABLE {self.ref} (id BIGINT, grp INT, v TEXT);")
        for _ in range(4):
            self.insert()
            self.statement(f"VACUUM (REFRESH_TABLE) {self.table};")
        self.check()
        for _ in range(ops):
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
                self.statement("BEGIN;", "w1")
                self.rng.choice([self.delete, self.update])("w1")
                if self.rng.random() < 0.3:
                    self.insert("w2")
                    self.statement(f"VACUUM (REFRESH_TABLE) {self.table};", "w2")
                if self.rng.random() < 0.7:
                    self.compact("w2")
                self.statement(
                    "ROLLBACK;" if self.rng.random() < 0.15 else "COMMIT;", "w1")
                self.check("w1")
            else:
                self.statement("BEGIN;", "w3")
                self.delete("w3")
                self.delete("w3")
                self.statement("COMMIT;", "w3")
                self.check("w3")
        self.statement(f"VACUUM (COMPACT_TABLE) {self.table};")
        self.check()
        self.statement(f"DROP TABLE {self.table};")
        self.statement(f"DROP TABLE {self.ref};")
