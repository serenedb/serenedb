import copy
from dataclasses import dataclass

from sqllogic_gen import Test, lit

t = Test("Prepared statements of every shape on every kind of curve index, "
         "executed again after every change of the data. The answers are "
         "worked out here from the rows themselves.", seed=1317)
rng = t.rng

INDEXES = {
    "cpm_morton": ("curve(ROW(x, y))", None, False),
    "cpm_hilbert": ("curve(ROW(x, y), kind := 'hilbert')", None, False),
    "cpm_coarse": ("curve(ROW(x, y), max_level := 5)", "x, y", False),
    "cpm_normal": ("curve(ROW(x, y), lower := ROW(0, 0), upper := ROW(100, 100))",
                   None, False),
    "cpm_search": ("curve(ROW(x, y))", None, True),
}
BODIES = ["alpha", "alpha beta", "gamma", "Alpha Gamma", "beta beta"]
ROWS = 1500


@dataclass
class Row:
    id: int
    x: int
    y: int
    tag: int
    kind: int
    body: str

    def values(self):
        x = "NULL" if self.x is None else self.x
        y = "NULL" if self.y is None else self.y
        return f"({self.id}, {x}, {y}, {self.tag}, {self.kind}, '{lit(self.body)}')"


def random_row(row_id):
    return Row(row_id,
               None if row_id % 97 == 0 else rng.randrange(60),
               None if row_id % 89 == 0 else rng.randrange(60),
               row_id % 5, row_id % 3, rng.choice(BODIES))


def between(value, low, high, left=True, right=True):
    if value is None:
        return False
    if low is not None and not (value >= low if left else value > low):
        return False
    return high is None or (value <= high if right else value < high)


def box(r, lx=10, hx=30, ly=10, hy=30, left=True, right=True):
    return between(r.x, lx, hx, left, right) and between(r.y, ly, hy, left, right)


def has_word(r, word):
    return word is not None and word.lower() in r.body.lower().split()


def equal(value, parameter):
    return parameter is not None and value == parameter


SELECT = "SELECT count(*) AS n, coalesce(sum(id), 0) AS total FROM"


@dataclass
class Shape:
    where: str
    test: object
    params: list
    sql: str = "{select} {index} WHERE {where}"
    pick: object = None
    skipif: str = None


def shape(where, test, *params, sql=None, pick=None, skipif=None):
    return Shape(where, test, list(params), sql or Shape.sql, pick, skipif)


SHAPES = [
    shape("tag = $1 AND {box}", lambda r, a: equal(r.tag, a) and box(r),
          (3,), (4,), (99,), (None,)),
    shape("{box} AND tag = $1", lambda r, a: equal(r.tag, a) and box(r), (3,), (4,)),
    shape("tag = $1 AND kind = $2 AND {box}",
          lambda r, a, b: r.tag == a and r.kind == b and box(r), (1, 2), (0, 0)),
    shape("kind = $2 AND {box} AND tag = $1",
          lambda r, a, b: r.tag == a and r.kind == b and box(r), (1, 2), (4, 1)),
    shape("id >= $1 AND {box}", lambda r, a: r.id >= a and box(r),
          (0,), (700,), (100000,)),
    shape("id BETWEEN $1 AND $2 AND {box}", lambda r, a, b: a <= r.id <= b and box(r),
          (100, 900), (900, 100)),
    shape("id < $1 AND {box} AND id > $2", lambda r, a, b: b < r.id < a and box(r),
          (900, 100), (5, 900)),
    shape("tag IN ($1, $2) AND {box}", lambda r, a, b: r.tag in (a, b) and box(r),
          (1, 2), (3, 3)),
    shape("(tag = $1 OR kind = $2) AND {box}",
          lambda r, a, b: (r.tag == a or r.kind == b) and box(r), (1, 2), (0, 0)),
    shape("(tag = $1 OR $2) AND {box}", lambda r, a, b: (r.tag == a or b) and box(r),
          (1, True), (1, False)),
    shape("NOT (tag = $1) AND {box}", lambda r, a: r.tag != a and box(r), (1,), (2,)),
    shape("tag <> $1 AND {box}", lambda r, a: r.tag != a and box(r), (3,)),
    shape("tag + $1 = 3 AND {box}", lambda r, a: r.tag + a == 3 and box(r), (1,), (0,)),
    shape("$1 > 0 AND {box}", lambda r, a: a > 0 and box(r), (1,), (0,)),
    shape("x = $1 AND {box}", lambda r, a: r.x == a and box(r), (20,), (40,)),
    shape("x > $1 AND {box} AND y < $2",
          lambda r, a, b: box(r) and r.x > a and r.y < b, (15, 25), (30, 10)),
    shape("{box} AND {other}",
          lambda r: box(r) and box(r, 20, 40, 20, 40), ()),
    shape("{box} AND {other} AND tag = $1",
          lambda r, a: box(r) and box(r, 20, 40, 20, 40) and r.tag == a, (2,)),
    shape("tag = $1 AND {box} AND {other}",
          lambda r, a: box(r) and box(r, 20, 40, 20, 40) and r.tag == a, (2,)),

    shape("x BETWEEN $1 AND $2 AND y BETWEEN 10 AND 30",
          lambda r, a, b: a is not None and b is not None and box(r, a, b),
          (10, 30), (30, 10), (0, 59), (None, 30), (10, None)),
    shape("x >= $1 AND x <= 30 AND y BETWEEN 10 AND 30",
          lambda r, a: a is not None and box(r, a, 30), (10,), (None,)),
    shape("x BETWEEN 10 AND 30 AND y BETWEEN $1 AND $2",
          lambda r, a, b: box(r, ly=a, hy=b), (10, 30), (25, 26)),
    shape("x BETWEEN $1 AND $2 AND y BETWEEN $3 AND $4",
          lambda r, a, b, c, d: box(r, a, b, c, d), (10, 30, 10, 30), (0, 5, 50, 59)),
    shape("x >= $1 AND x < $2 AND y > $3 AND y <= $4",
          lambda r, a, b, c, d: between(r.x, a, b, True, False)
          and between(r.y, c, d, False, True), (10, 30, 10, 30), (0, 1, 0, 59)),
    shape("x = $1 AND y = $2", lambda r, a, b: r.x == a and r.y == b, (20, 20), (1, 2)),
    shape("x BETWEEN 10 AND 30 AND y BETWEEN 10 AND 30 AND tag = $1",
          lambda r, a: box(r) and r.tag == a, (3,), (4,)),
    shape("tag = $1 AND x BETWEEN 10 AND 30 AND y BETWEEN 10 AND 30",
          lambda r, a: box(r) and r.tag == a, (3,), (4,)),
    shape("x BETWEEN 10 AND 30 AND y BETWEEN 10 AND 30 AND y <> $1",
          lambda r, a: box(r) and r.y != a, (20,)),
    shape("x BETWEEN 10 AND 30 AND y BETWEEN 10 AND 30 AND x >= $1",
          lambda r, a: box(r) and r.x >= a, (20,)),

    shape("{call} @@ ts_between(ROW($1::BIGINT, 10), ROW($2::BIGINT, 30), true, true)",
          lambda r, a, b: box(r, a, b), (10, 30), (30, 10), (None, 30), (10, None),
          (None, None)),
    shape("{call} @@ ts_between(ROW($1::BIGINT, $2::BIGINT), ROW($3::BIGINT, $4::BIGINT), true, true) AND tag = $5",
          lambda r, a, b, c, d, e: box(r, a, c, b, d) and r.tag == e,
          (10, 10, 30, 30, 3), (0, 0, 59, 59, 1)),
    shape("tag = $5 AND {call} @@ ts_between(ROW($1::BIGINT, $2::BIGINT), ROW($3::BIGINT, $4::BIGINT), false, false)",
          lambda r, a, b, c, d, e: box(r, a, c, b, d, False, False) and r.tag == e,
          (10, 10, 30, 30, 3)),
    shape("{call} @@ ts_between(ROW(10, 10), ROW(30, 30), $1, $2)",
          lambda r, a, b: box(r, left=a, right=b),
          (True, True), (False, True), (True, False), (False, False),
          skipif="pg-wire-extended"),
    shape("tag = $3 AND {call} @@ ts_between(ROW(10, 10), ROW(30, 30), $1, $2)",
          lambda r, a, b, c: box(r, left=a, right=b) and r.tag == c,
          (True, False, 2), (False, True, 3), skipif="pg-wire-extended"),
    shape("{call} @@ ts_between(ROW(NULL, 10), ROW(NULL, 30), true, true) AND tag = $1",
          lambda r, a: r.x is not None and between(r.y, 10, 30) and r.tag == a, (3,)),
    shape("tag = $1 AND {call} @@ ts_between(ROW(10, NULL), ROW(30, NULL), true, true)",
          lambda r, a: r.y is not None and between(r.x, 10, 30) and r.tag == a, (3,)),
    shape("id >= $1 AND {call} @@ ts_between(ROW(NULL, NULL), ROW(NULL, NULL), true, true)",
          lambda r, a: r.x is not None and r.y is not None and r.id >= a, (700,)),
    shape("tag = $1 AND {call} @@ ts_between(ROW(5, 5), ROW(4, 4), true, true)",
          lambda r, a: False, (3,)),
    shape("tag = $1 AND {call} @@ ts_between(ROW(1000, 1000), ROW(2000, 2000), true, true)",
          lambda r, a: False, (3,)),
    shape("tag = $1 AND {call} @@ ts_between(ROW(10, 10), ROW(10, 10), false, true)",
          lambda r, a: False, (3,)),

    shape("body @@ $1 AND {box}", lambda r, a: has_word(r, a) and box(r),
          ("beta",), ("gamma",), ("none",)),
    shape("{box} AND body @@ 'beta' AND tag = $1",
          lambda r, a: has_word(r, "beta") and box(r) and r.tag == a, (3,), (4,)),
    shape("tag = $1 AND body @@ $2 AND {box}",
          lambda r, a, b: has_word(r, b) and box(r) and r.tag == a, (3, "gamma")),
    shape("lower(body) = $1 AND {box}",
          lambda r, a: r.body.lower() == a and box(r), ("alpha beta",), ("gamma",)),
    shape("lower(body) = 'alpha gamma' AND tag = $1 AND {box}",
          lambda r, a: r.body.lower() == "alpha gamma" and box(r) and r.tag == a, (3,)),
    shape("tag = $1 AND lower(body) = 'alpha gamma'",
          lambda r, a: r.body.lower() == "alpha gamma" and r.tag == a, (3,), (4,)),
    shape("lower(body) = 'alpha gamma' AND tag = $1",
          lambda r, a: r.body.lower() == "alpha gamma" and r.tag == a, (3,), (4,)),
    shape("lower(body) = $1 AND tag = $2",
          lambda r, a, b: r.body.lower() == a and r.tag == b, ("gamma", 3)),
    shape("tag = $2 AND lower(body) = $1",
          lambda r, a, b: r.body.lower() == a and r.tag == b, ("gamma", 3)),

    shape("tag = $1 AND {box}", lambda r, a: r.tag == a and box(r), (3,), (4,),
          sql="{select} (SELECT * FROM {index} WHERE {where}) t"),
    shape("{box}", lambda r, a: r.tag == a and box(r), (3,), (4,),
          sql="{select} (SELECT * FROM {index} WHERE {where}) t WHERE tag = $1"),
    shape("{box}", lambda r, a: r.tag == a and box(r), (3,),
          sql="WITH c AS (SELECT * FROM {index} WHERE {where}) {select} c WHERE tag = $1"),
    shape("{box}", lambda r, a: r.tag == a and box(r), (3,),
          sql="WITH c AS MATERIALIZED (SELECT * FROM {index} WHERE {where}) {select} c WHERE tag = $1"),
    shape("{box}", lambda r, a: r.tag == a and box(r), (3,),
          sql="{select} (SELECT i.id FROM {index} i JOIN cpm c ON i.id = c.id WHERE {qualified} AND c.tag = $1) t"),
    shape("{box}", lambda r, a: r.tag == a and box(r), (3,),
          sql="{select} (SELECT i.id FROM {index} i JOIN cpm c ON i.id = c.id WHERE {qualified} AND i.tag = $1) t"),
    shape("{box} AND id IN (SELECT id FROM cpm WHERE tag = $1)",
          lambda r, a: r.tag == a and box(r), (3,)),
    shape("{box} ORDER BY id LIMIT $1", None, (0,), (1,), (7,),
          sql="{select} (SELECT id FROM {index} WHERE {where}) t",
          pick=lambda ids, a: ids[:a]),
    shape("{box} ORDER BY id LIMIT 5 OFFSET $1", None, (0,), (3,),
          sql="{select} (SELECT id FROM {index} WHERE {where}) t",
          pick=lambda ids, a: ids[a:a + 5]),
    shape("tag = $1 AND {box} ORDER BY id DESC LIMIT $2", None, (3, 4), (4, 1),
          sql="{select} (SELECT id FROM {index} WHERE {where}) t",
          pick=lambda ids, a, b: [i for i in reversed(ids) if ROWS_NOW[i].tag == a][:b]),
]

ROWS_NOW = {}


def text(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return f"'{lit(value)}'"
    return str(value)


def answer(item, params):
    rows = sorted(ROWS_NOW.values(), key=lambda r: r.id)
    if item.pick:
        ids = item.pick([r.id for r in rows if box(r)], *params)
    else:
        ids = [r.id for r in rows if item.test(r, *params)]
    return [[len(ids), sum(ids)]]


def run_all(names):
    for name, item in names:
        for params in item.params:
            suffix = f"({', '.join(text(p) for p in params)})" if params else ""
            for _ in range(2):
                t.query(f"EXECUTE {name}{suffix}", ["n", "total"],
                        answer(item, params), skipif=item.skipif)


def phase(title, statements):
    t.section(title)
    for statement in statements:
        t.statement(statement)
    t.refresh("cpm")


base = [random_row(i) for i in range(ROWS)]

for index, (call, include, search) in INDEXES.items():
    t.part()
    t.section(f"{index}: {call}.")
    ROWS_NOW.clear()
    ROWS_NOW.update({r.id: r for r in copy.deepcopy(base)})
    t.dictionary("cpm_dict", "split_text(case := 'lower') WITH (frequency, position)")
    t.table("cpm", "id INTEGER PRIMARY KEY, x BIGINT, y BIGINT, tag INTEGER, "
            "kind INTEGER, body VARCHAR", search=search)
    t.insert("cpm", (r.values() for r in ROWS_NOW.values()), chunk=500)
    t.index(index, "cpm", f"id, tag, kind, body cpm_dict, ({call}), (lower(body))",
            include=include)
    t.refresh("cpm")

    prepared = []
    for k, item in enumerate(SHAPES):
        where = item.where.format(
            call=call, box=f"{call} @@ ts_between(ROW(10, 10), ROW(30, 30), true, true)",
            other=f"{call} @@ ts_between(ROW(20, 20), ROW(40, 40), true, true)")
        sql = item.sql.format(select=SELECT, index=index, where=where,
                              qualified=where.replace("ROW(x, y)", "ROW(i.x, i.y)"))
        t.statement(f"PREPARE cpm_p{k} AS {sql}")
        prepared.append((f"cpm_p{k}", item))
    run_all(prepared)

    added = [random_row(i) for i in range(ROWS, ROWS + 200)]
    phase("A new segment.", [
        "INSERT INTO cpm VALUES\n    " + ",\n    ".join(r.values() for r in added)])
    ROWS_NOW.update({r.id: r for r in added})
    run_all(prepared)

    removed = [i for i in ROWS_NOW if i % 7 == 0 or 1600 <= i < 1650]
    phase("Deleted rows.", ["DELETE FROM cpm WHERE id % 7 = 0 OR id BETWEEN 1600 AND 1649"])
    for i in removed:
        del ROWS_NOW[i]
    run_all(prepared)

    phase("Moved rows.", [
        "UPDATE cpm SET x = y, y = x, tag = (tag + 1) % 5 WHERE id % 5 = 1"])
    for r in ROWS_NOW.values():
        if r.id % 5 == 1:
            r.x, r.y, r.tag = r.y, r.x, (r.tag + 1) % 5
    run_all(prepared)

    t.section("One segment.")
    t.statement("VACUUM (COMPACT_TABLE) cpm")
    run_all(prepared)

t.write()
