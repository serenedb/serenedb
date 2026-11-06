from sqllogic_gen import Test

t = Test("Every query shape around a box, on index relations with and without "
         "the columns included, against the same query on the table: "
         "mismatches is always 0.", seed=1551)
rng = t.rng

ROWS = 1500
BOXES = 5

CALLS = {
    "plain": ("curve(ROW(x, y, z))", None),
    "included": ("curve(ROW(x, y, z))", "x, y, z, s, tag"),
    "hilbert": ("curve(ROW(x, y, z), kind := 'hilbert')", None),
    "coarse": ("curve(ROW(x, y, z), max_level := 4)", None),
}

SHAPES = [
    "SELECT id FROM {r} WHERE {p}",
    "SELECT id, x + 0 AS a, y + 0 AS b, z + 0 AS c FROM {r} WHERE {p}",
    "SELECT id, s FROM {r} WHERE {p}",
    "SELECT id, x + y AS v FROM {r} WHERE {p}",
    "SELECT id, s, z FROM {r} WHERE {p} AND tag = 2",
    "SELECT id FROM {r} WHERE tag = 2 AND {p}",
    "SELECT id FROM {r} WHERE {p} AND tag <> 2",
    "SELECT id FROM {r} WHERE {p} AND tag IN (1, 3)",
    "SELECT id FROM {r} WHERE {p} AND (tag = 1 OR tag = 3)",
    "SELECT id FROM {r} WHERE {p} AND NOT (tag = 1)",
    "SELECT id FROM {r} WHERE {p} AND x + y > 30",
    "SELECT id FROM {r} WHERE {p} AND (x > 20 OR y < 10)",
    "SELECT id FROM {r} WHERE {p} AND x <> 15",
    "SELECT id FROM {r} WHERE {p} AND s LIKE 'a%'",
    "SELECT id FROM {r} WHERE {p} AND id % 3 = 0",
    "SELECT id FROM {r} WHERE {p} AND id BETWEEN 100 AND 900",
    "SELECT id FROM {r} WHERE {p} AND s IS NOT NULL",
    "SELECT id FROM {r} WHERE {p} ORDER BY id LIMIT 5",
    "SELECT id FROM {r} WHERE {p} ORDER BY id DESC LIMIT 5 OFFSET 2",
    "SELECT id, x FROM {r} WHERE {p} ORDER BY x DESC, y, id LIMIT 3",
    "SELECT count(*) AS n FROM (SELECT id FROM {r} WHERE {p} LIMIT 5) l",
    "SELECT count(*) AS n FROM (SELECT id FROM {r} WHERE {p} LIMIT 5 OFFSET 3) l",
    "SELECT count(*) AS n FROM (SELECT id, s FROM {r} WHERE {p} LIMIT 100000) l",
    "SELECT DISTINCT tag FROM {r} WHERE {p}",
    "SELECT DISTINCT x, y FROM {r} WHERE {p}",
    "SELECT tag, count(*) AS n, sum(id) AS total, min(x) AS l, max(y) AS h "
    "FROM {r} WHERE {p} GROUP BY tag",
    "SELECT count(*) AS n FROM {r} WHERE {p}",
    "SELECT count(*) AS n, sum(id) AS total FROM {r} WHERE {p}",
    "SELECT count(s) AS n, count(DISTINCT tag) AS tags FROM {r} WHERE {p}",
    "SELECT min(x) AS l, max(x) AS h FROM {r} WHERE {p}",
    "SELECT tag FROM {r} WHERE {p} GROUP BY tag HAVING count(*) > 2",
    "SELECT a.id, d.label FROM {r} a JOIN cqs_dim d ON a.tag = d.k WHERE {p}",
    "SELECT a.id, d.label FROM cqs_dim d JOIN {r} a ON a.tag = d.k AND {p}",
    "SELECT a.id, d.label FROM {r} a LEFT JOIN cqs_dim d ON a.tag = d.k WHERE {p}",
    "SELECT d.k, a.id FROM cqs_dim d LEFT JOIN {r} a ON a.tag = d.k AND {p}",
    "SELECT d.k FROM cqs_dim d WHERE EXISTS "
    "(SELECT 1 FROM {r} a WHERE a.tag = d.k AND {p})",
    "SELECT d.k FROM cqs_dim d WHERE NOT EXISTS "
    "(SELECT 1 FROM {r} a WHERE a.tag = d.k AND {p})",
    "SELECT d.k FROM cqs_dim d WHERE d.k IN (SELECT tag FROM {r} WHERE {p})",
    "SELECT d.k FROM cqs_dim d WHERE d.k NOT IN (SELECT tag FROM {r} WHERE {p})",
    "SELECT d.k, (SELECT count(*) FROM {r} a WHERE a.tag = d.k AND {p}) AS n "
    "FROM cqs_dim d",
    "SELECT d.k, l.id FROM cqs_dim d, LATERAL (SELECT id FROM {r} a "
    "WHERE a.tag = d.k AND {p} ORDER BY id LIMIT 2) l",
    "SELECT id FROM {r} WHERE {p} UNION SELECT id FROM {r} WHERE tag = 1 AND id < 50",
    "SELECT id FROM {r} WHERE {p} UNION ALL SELECT id FROM {r} WHERE {p}",
    "SELECT id FROM {r} WHERE {p} INTERSECT SELECT id FROM cqs WHERE tag = 1",
    "SELECT id FROM cqs EXCEPT SELECT id FROM {r} WHERE {p}",
    "WITH c AS (SELECT * FROM {r} WHERE {p}) SELECT id FROM c WHERE tag = 2",
    "WITH c AS MATERIALIZED (SELECT * FROM {r} WHERE {p}) SELECT id FROM c WHERE tag = 2",
    "WITH c AS (SELECT * FROM {r} WHERE {p}) SELECT a.id FROM c a JOIN c b ON a.tag = b.tag "
    "AND a.id = b.id",
    "SELECT id FROM (SELECT * FROM {r} WHERE {p}) t WHERE tag = 2 AND x > 5",
    "SELECT id FROM (SELECT * FROM {r} WHERE tag = 2) t WHERE {p}",
    "SELECT row_number() OVER (ORDER BY id) AS n, id FROM {r} WHERE {p}",
    "SELECT id, sum(id) OVER (PARTITION BY tag) AS total FROM {r} WHERE {p}",
]


def call_on(call, alias):
    return call.replace("ROW(x, y, z)", f"ROW({alias}.x, {alias}.y, {alias}.z)")


def box(call, bounds):
    (lx, hx), (ly, hy), (lz, hz) = bounds
    return (f"{call} @@ ts_between(ROW({lx}, {ly}, {lz}), ROW({hx}, {hy}, {hz}), "
            "true, true)")


def truth(bounds, alias=""):
    (lx, hx), (ly, hy), (lz, hz) = bounds
    return (f"{alias}x BETWEEN {lx} AND {hx} AND {alias}y BETWEEN {ly} AND {hy} "
            f"AND {alias}z BETWEEN {lz} AND {hz}")


def same(actual, expected):
    t.same_rows(f"SELECT * FROM ({actual}) t", f"SELECT * FROM ({expected}) t")


def random_bounds():
    bounds = []
    for high in (40, 40, 40):
        low = rng.randrange(high)
        bounds.append((low, min(high, low + rng.choice([0, 1, 5, 12, 25]))))
    return bounds


def setup():
    t.table("cqs", "id INTEGER PRIMARY KEY, x BIGINT, y BIGINT, z DOUBLE, "
            "tag INTEGER, s VARCHAR")
    t.table("cqs_dim", "k INTEGER PRIMARY KEY, label VARCHAR")
    t.insert("cqs", (
        f"({i}, {'NULL' if i % 61 == 0 else rng.randrange(40)}, {rng.randrange(40)}, "
        f"{'NULL' if i % 67 == 0 else rng.randrange(80) / 2}, {i % 6}, "
        f"{'NULL' if i % 13 == 0 else repr(rng.choice(['abc', 'abd', 'xyz']))})"
        for i in range(ROWS)), chunk=500)
    t.insert("cqs_dim", (f"({k}, 'label{k}')" for k in range(9)))
    for name, (call, include) in CALLS.items():
        t.index(f"cqs_{name}", "cqs", f"id, tag, ({call})", include=include)
    t.refresh("cqs")


for name, (call, include) in CALLS.items():
    t.part()
    setup()
    index = f"cqs_{name}"
    t.section(f"{call}, included columns: {include}.")
    for shape in SHAPES:
        for _ in range(BOXES):
            bounds = random_bounds()
            same(shape.format(r=index, p=box(call, bounds)),
                 shape.format(r="cqs", p=truth(bounds)))
    for _ in range(BOXES):
        first, second = random_bounds(), random_bounds()
        same(f"SELECT a.id FROM {index} a JOIN {index} b ON a.id = b.id "
             f"WHERE {box(call_on(call, 'a'), first)} AND {box(call_on(call, 'b'), second)}",
             f"SELECT a.id FROM cqs a JOIN cqs b ON a.id = b.id "
             f"WHERE {truth(first, 'a.')} AND {truth(second, 'b.')}")
        same(f"SELECT a.id, b.id AS other FROM {index} a JOIN {index} b ON a.tag = b.tag "
             f"WHERE {box(call_on(call, 'a'), first)} AND {box(call_on(call, 'b'), second)}",
             f"SELECT a.id, b.id AS other FROM cqs a JOIN cqs b ON a.tag = b.tag "
             f"WHERE {truth(first, 'a.')} AND {truth(second, 'b.')}")
        same(f"SELECT id FROM {index} WHERE {box(call, first)} AND {box(call, second)}",
             f"SELECT id FROM cqs WHERE {truth(first)} AND {truth(second)}")
        same(f"SELECT id FROM {index} WHERE {box(call, first)} AND {truth(second)}",
             f"SELECT id FROM cqs WHERE {truth(first)} AND {truth(second)}")

t.part()
setup()
t.section("Ordinary ranges next to conditions the curve must leave alone.")
RANGE_SHAPES = [
    "x BETWEEN {lx} AND {hx} AND y BETWEEN {ly} AND {hy} AND z BETWEEN {lz} AND {hz}",
    "x >= {lx} AND x <= {hx} AND y >= {ly} AND y <= {hy} AND z >= {lz} AND z <= {hz}",
    "x > {lx} AND x < {hx} AND y > {ly} AND y < {hy} AND z > {lz} AND z < {hz}",
    "{lx} <= x AND {hx} >= x AND {ly} < y AND {hy} > y AND {lz} <= z AND {hz} >= z",
    "x >= {lx} AND x >= {ly} AND x <= {hx} AND x <= {hy} AND y BETWEEN {ly} AND {hy} "
    "AND z BETWEEN {lz} AND {hz}",
    "x = {lx} AND y = {ly} AND z = {lz}",
    "x = {lx} AND x = {hx} AND y = {ly} AND z = {lz}",
    "x = {lx} AND y BETWEEN {ly} AND {hy} AND z = {lz}",
    "x BETWEEN {lx} AND {hx} AND y BETWEEN {ly} AND {hy}",
    "x BETWEEN {lx} AND {hx} AND y >= {ly} AND z BETWEEN {lz} AND {hz}",
    "x BETWEEN {lx} AND {hx} AND y BETWEEN {ly} AND {hy} AND z BETWEEN {lz} AND {hz} "
    "AND x <> {lx}",
    "x BETWEEN {lx} AND {hx} AND y BETWEEN {ly} AND {hy} AND z BETWEEN {lz} AND {hz} "
    "AND y IN ({ly}, {hy})",
    "x BETWEEN {lx} AND {hx} AND y BETWEEN {ly} AND {hy} AND z BETWEEN {lz} AND {hz} "
    "AND tag = 2",
    "x BETWEEN {lx} AND {hx} AND y BETWEEN {ly} AND {hy} AND z BETWEEN {lz} AND {hz} "
    "AND (tag = 2 OR id < 100)",
    "(x BETWEEN {lx} AND {hx} AND y BETWEEN {ly} AND {hy} AND z BETWEEN {lz} AND {hz}) "
    "OR tag = 2",
    "NOT (x BETWEEN {lx} AND {hx} AND y BETWEEN {ly} AND {hy} AND z BETWEEN {lz} AND {hz})",
    "x NOT BETWEEN {lx} AND {hx} AND y BETWEEN {ly} AND {hy} AND z BETWEEN {lz} AND {hz}",
    "x IN ({lx}, {hx}) AND y BETWEEN {ly} AND {hy} AND z BETWEEN {lz} AND {hz}",
    "x BETWEEN abs(-{lx}) AND {hx} + 1 AND y BETWEEN {ly} AND {hy} AND z BETWEEN {lz} AND {hz}",
    "x BETWEEN y AND {hx} AND y BETWEEN {ly} AND {hy} AND z BETWEEN {lz} AND {hz}",
    "x BETWEEN {lx} AND y AND y BETWEEN {ly} AND {hy} AND z BETWEEN {lz} AND {hz}",
    "x + 1 BETWEEN {lx} AND {hx} AND y BETWEEN {ly} AND {hy} AND z BETWEEN {lz} AND {hz}",
    "-x BETWEEN -{hx} AND -{lx} AND y BETWEEN {ly} AND {hy} AND z BETWEEN {lz} AND {hz}",
    "x::DOUBLE BETWEEN {lx}.5 AND {hx}.5 AND y BETWEEN {ly} AND {hy} AND z BETWEEN {lz} AND {hz}",
    "x::HUGEINT BETWEEN {lx} AND {hx} AND y BETWEEN {ly} AND {hy} AND z BETWEEN {lz} AND {hz}",
    "x::TINYINT BETWEEN {lx} AND {hx} AND y BETWEEN {ly} AND {hy} AND z BETWEEN {lz} AND {hz}",
    "z::FLOAT BETWEEN {lz} AND {hz} AND x BETWEEN {lx} AND {hx} AND y BETWEEN {ly} AND {hy}",
    "coalesce(x, 7) BETWEEN {lx} AND {hx} AND y BETWEEN {ly} AND {hy} AND z BETWEEN {lz} AND {hz}",
    "x BETWEEN NULL AND {hx} AND y BETWEEN {ly} AND {hy} AND z BETWEEN {lz} AND {hz}",
    "x BETWEEN {hx} AND {lx} AND y BETWEEN {ly} AND {hy} AND z BETWEEN {lz} AND {hz}",
    "x IS NOT NULL AND x BETWEEN {lx} AND {hx} AND y BETWEEN {ly} AND {hy} AND z BETWEEN {lz} AND {hz}",
]
for index in ("cqs_plain", "cqs_hilbert", "cqs_coarse"):
    for shape in RANGE_SHAPES:
        for _ in range(BOXES):
            (lx, hx), (ly, hy), (lz, hz) = random_bounds()
            condition = shape.format(lx=lx, hx=hx, ly=ly, hy=hy, lz=lz, hz=hz)
            same(f"SELECT id FROM {index} WHERE {condition}",
                 f"SELECT id FROM cqs WHERE {condition}")

t.part()
t.section("One index over several curves, coordinates reordered, expressions "
          "and views.")
t.table("cqm", "id INTEGER PRIMARY KEY, x BIGINT, y BIGINT, z BIGINT, tag INTEGER")
t.insert("cqm", (f"({i}, {rng.randrange(30)}, {rng.randrange(30)}, {rng.randrange(30)}, {i % 4})"
                 for i in range(1200)), chunk=500)
XY = "curve(ROW(x, y))"
YZ = "curve(ROW(y, z), kind := 'hilbert')"
ZX = "curve(ROW(z, x), max_level := 3)"
SWAPPED = "curve(ROW(y, x))"
SUMS = "curve(ROW(x + y, x - y))"
STRUCT = "curve(struct_pack(a := x, b := y))"
t.index("cqm_many", "cqm", f"id, tag, ({XY}), ({YZ}), ({ZX})")
t.index("cqm_swapped", "cqm", f"id, ({SWAPPED})")
t.index("cqm_sums", "cqm", f"id, ({SUMS})")
t.index("cqm_struct", "cqm", f"id, ({STRUCT})")
t.refresh("cqm")


def edges():
    low = rng.randrange(30)
    return low, low + rng.choice([0, 2, 8, 20])


for _ in range(30):
    (a, b), (c, d), (e, f) = edges(), edges(), edges()
    xy = f"x BETWEEN {a} AND {b} AND y BETWEEN {c} AND {d}"
    yz = f"y BETWEEN {c} AND {d} AND z BETWEEN {e} AND {f}"
    zx = f"z BETWEEN {e} AND {f} AND x BETWEEN {a} AND {b}"
    same(f"SELECT id FROM cqm_many WHERE {XY} @@ ts_between(ROW({a}, {c}), ROW({b}, {d}), true, true)",
         f"SELECT id FROM cqm WHERE {xy}")
    same(f"SELECT id FROM cqm_many WHERE {YZ} @@ ts_between(ROW({c}, {e}), ROW({d}, {f}), true, true)",
         f"SELECT id FROM cqm WHERE {yz}")
    same(f"SELECT id FROM cqm_many WHERE {ZX} @@ ts_between(ROW({e}, {a}), ROW({f}, {b}), true, true)",
         f"SELECT id FROM cqm WHERE {zx}")
    same(f"SELECT id FROM cqm_many WHERE {XY} @@ ts_between(ROW({a}, {c}), ROW({b}, {d}), true, true) "
         f"AND {YZ} @@ ts_between(ROW({c}, {e}), ROW({d}, {f}), true, true)",
         f"SELECT id FROM cqm WHERE {xy} AND {yz}")
    same(f"SELECT id FROM cqm_many WHERE {XY} @@ ts_between(ROW({a}, {c}), ROW({b}, {d}), true, true) "
         f"AND {ZX} @@ ts_between(ROW({e}, NULL), ROW({f}, NULL), true, true) AND tag = 2",
         f"SELECT id FROM cqm WHERE {xy} AND z BETWEEN {e} AND {f} AND x IS NOT NULL AND tag = 2")
    for index, condition in (("cqm_many", xy), ("cqm_many", yz), ("cqm_many", zx),
                             ("cqm_many", f"{xy} AND z BETWEEN {e} AND {f}"),
                             ("cqm_swapped", xy), ("cqm_struct", xy)):
        same(f"SELECT id FROM {index} WHERE {condition}",
             f"SELECT id FROM cqm WHERE {condition}")
    same(f"SELECT id FROM cqm_swapped WHERE {SWAPPED} @@ ts_between(ROW({c}, {a}), ROW({d}, {b}), true, true)",
         f"SELECT id FROM cqm WHERE {xy}")
    same(f"SELECT id FROM cqm_sums WHERE {SUMS} @@ ts_between(ROW({a}, {c} - 15), ROW({b}, {d} - 15), true, true)",
         f"SELECT id FROM cqm WHERE x + y BETWEEN {a} AND {b} AND x - y BETWEEN {c} - 15 AND {d} - 15")
    same(f"SELECT id FROM cqm_sums WHERE x + y BETWEEN {a} AND {b} AND x - y BETWEEN {c} - 15 AND {d} - 15",
         f"SELECT id FROM cqm WHERE x + y BETWEEN {a} AND {b} AND x - y BETWEEN {c} - 15 AND {d} - 15")
    same(f"SELECT id FROM cqm_struct WHERE {STRUCT} @@ ts_between(ROW({a}, {c}), ROW({b}, {d}), true, true)",
         f"SELECT id FROM cqm WHERE {xy}")

t.statement(f"CREATE VIEW cqm_box AS SELECT * FROM cqm_many WHERE "
            f"{XY} @@ ts_between(ROW(3, 3), ROW(20, 20), true, true)")
t.statement("CREATE VIEW cqm_ranges AS SELECT * FROM cqm_many WHERE "
            "x BETWEEN 3 AND 20 AND y BETWEEN 3 AND 20")
BOXED = "x BETWEEN 3 AND 20 AND y BETWEEN 3 AND 20"
for view in ("cqm_box", "cqm_ranges"):
    for condition in ("", " WHERE tag = 2", " WHERE z > 10", " WHERE x > 7 AND y < 9",
                      " WHERE tag = 1 ORDER BY id LIMIT 4"):
        same(f"SELECT id FROM {view}{condition}",
             f"SELECT id FROM (SELECT * FROM cqm WHERE {BOXED}) t{condition}")
same(f"SELECT a.id FROM cqm_box a JOIN cqm_ranges b ON a.id = b.id WHERE a.tag = 1",
     f"SELECT id FROM cqm WHERE {BOXED} AND tag = 1")

t.write()
