from sqllogic_gen import Test

t = Test("Random inserts, updates, deletes, refreshes and compactions, also "
         "from a second connection while a transaction is open, under several "
         "curve indexes at once, on a plain table and on a search table next "
         "to a plain copy: every box agrees with the rows after every step, so "
         "segments of one row, of one point, of deleted rows and of many sizes "
         "all answer: mismatches is always 0.", seed=1721)
rng = t.rng

CALLS = {
    "morton": "curve(ROW(x, y))",
    "hilbert": "curve(ROW(x, y), kind := 'hilbert')",
    "coarse": "curve(ROW(x, y), max_level := 5)",
    "normal": "curve(ROW(x, y), lower := ROW(0, 0), upper := ROW(255, 255))",
}
DOMAIN = 256


class Run:
    def __init__(self, table, search):
        self.table = table
        self.search = search
        self.truth = f"{table}_ref" if search else table
        self.targets = [table, self.truth] if search else [table]
        self.next_id = 0
        self.dropped = None

    def both(self, sql, connection=None):
        for target in self.targets:
            if connection:
                t.connection(connection)
            t.statement(sql.format(t=target))

    def fresh_ids(self, count):
        first = self.next_id
        self.next_id += count
        return first, self.next_id - 1

    def insert(self, connection=None):
        first, last = self.fresh_ids(rng.choice([1, 2, 5, 100, 700, 2048, 5000]))
        a, b = rng.choice([1, 3, 7, 251]), rng.choice([5, 11, 13, 241])
        self.both(f"INSERT INTO {{t}} SELECT i, i * {a} % {DOMAIN}, i * {b} % {DOMAIN}, "
                  f"i % 5 FROM generate_series({first}, {last}) g(i)", connection)

    def insert_point(self, connection=None):
        first, last = self.fresh_ids(rng.choice([1, 2, 63, 64, 65, 3000]))
        x, y = rng.randrange(DOMAIN), rng.randrange(DOMAIN)
        self.both(f"INSERT INTO {{t}} SELECT i, {x}, {y}, i % 5 "
                  f"FROM generate_series({first}, {last}) g(i)", connection)

    def where(self):
        modulo = rng.choice([2, 3, 7, 97, 1000])
        low = rng.randrange(DOMAIN)
        high = low + rng.choice([0, 3, 30])
        return rng.choice([
            f"id % {modulo} = {rng.randrange(2)}",
            f"x BETWEEN {low} AND {high}",
            f"y BETWEEN {low} AND {high}",
            f"tag = {rng.randrange(5)}",
            f"id < {rng.randrange(max(self.next_id, 1))}",
            f"id >= {rng.randrange(max(self.next_id, 1))}",
        ])

    def delete(self, connection=None):
        self.both(f"DELETE FROM {{t}} WHERE {self.where()}", connection)

    def update(self, connection=None):
        change = rng.choice([f"x = (x + 1) % {DOMAIN}",
                             f"y = ({DOMAIN} - 1) - y",
                             "x = y, y = x",
                             "tag = (tag + 1) % 5"])
        self.both(f"UPDATE {{t}} SET {change} WHERE {self.where()}", connection)

    def clear(self):
        self.both("DELETE FROM {t}")

    def refresh(self, connection=None):
        if connection:
            t.connection(connection)
        t.statement(f"VACUUM (REFRESH_TABLE) {self.table}")

    def compact(self, connection=None):
        if connection:
            t.connection(connection)
        t.statement(f"VACUUM (COMPACT_TABLE) {self.table}")

    def transaction(self):
        t.connection("w1")
        t.statement("BEGIN")
        rng.choice([self.delete, self.update])("w1")
        if rng.random() < 0.5:
            rng.choice([self.insert, self.insert_point])("w2")
            self.refresh("w2")
        if rng.random() < 0.5:
            self.compact("w2")
        t.connection("w1")
        t.statement("ROLLBACK" if rng.random() < 0.3 else "COMMIT")

    def rebuild(self):
        name, call = rng.choice(list(CALLS.items()))
        t.statement(f"DROP INDEX {self.table}_{name}")
        t.index(f"{self.table}_{name}", self.table, f"id, tag, ({call})")

    def check(self):
        self.refresh()
        for name, call in CALLS.items():
            index = f"{self.table}_{name}"
            t.same_rows(f"SELECT id FROM {index} WHERE {call} @@ "
                        "ts_between(NULL, NULL, true, true)",
                        f"SELECT id FROM {self.truth}")
            for _ in range(3):
                lx = rng.randrange(DOMAIN)
                ly = rng.randrange(DOMAIN)
                hx = lx + rng.choice([0, 1, 7, 40, 200])
                hy = ly + rng.choice([0, 1, 7, 40, 200])
                extra = rng.choice(["", " AND tag = 2", " AND id % 2 = 0"])
                t.same_rows(f"SELECT id FROM {index} WHERE {call} @@ ts_between("
                            f"ROW({lx}, {ly}), ROW({hx}, {hy}), true, true){extra}",
                            f"SELECT id FROM {self.truth} WHERE x BETWEEN {lx} AND {hx} "
                            f"AND y BETWEEN {ly} AND {hy}{extra}")
                t.same_rows(f"SELECT id FROM {index} WHERE x BETWEEN {lx} AND {hx} "
                            f"AND y BETWEEN {ly} AND {hy}{extra}",
                            f"SELECT id FROM {self.truth} WHERE x BETWEEN {lx} AND {hx} "
                            f"AND y BETWEEN {ly} AND {hy}{extra}")

    def run(self, steps):
        columns = "id INTEGER, x BIGINT, y BIGINT, tag INTEGER"
        if self.search:
            t.statement(f"CREATE TABLE {self.table} ({columns}) WITH "
                        "(storage = 'search', compaction_interval = 0, "
                        "refresh_interval = 0)")
            t.statement(f"CREATE TABLE {self.truth} ({columns})")
        else:
            t.statement(f"CREATE TABLE {self.table} ({columns})")
        for name, call in CALLS.items():
            t.index(f"{self.table}_{name}", self.table, f"id, tag, ({call})")
        self.check()
        for _ in range(3):
            self.insert()
            self.refresh()
        self.check()
        for _ in range(steps):
            x = rng.random()
            if x < 0.25:
                self.insert()
            elif x < 0.35:
                self.insert_point()
            elif x < 0.5:
                self.delete()
            elif x < 0.65:
                self.update()
            elif x < 0.72:
                self.compact()
            elif x < 0.76:
                self.clear()
            elif x < 0.8 and not self.search:
                self.rebuild()
            else:
                self.transaction()
            self.check()
        self.compact()
        self.check()


for number in range(3):
    t.part()
    Run(f"cdr_{number}", search=False).run(18)
for number in range(3, 5):
    t.part()
    Run(f"cdr_{number}", search=True).run(18)

t.write()
