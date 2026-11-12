from sqllogic_gen import Test

t = Test("The nearest rows of a vector index inside a box, literal and prepared, "
         "with exact and lossy coordinates, through new segments and deletes. "
         "The answers are worked out here from the rows themselves.", seed=1320)
rng = t.rng

INDEXES = {
    "ckm_morton": ("curve(ROW(x, y))", "x, y, tag"),
    "ckm_plain": ("curve(ROW(x, y))", None),
    "ckm_hilbert": ("curve(ROW(x, y), kind := 'hilbert')", "x, y, tag"),
    "ckm_coarse": ("curve(ROW(x, y), max_level := 3)", None),
    "ckm_normal": ("curve(ROW(x, y), lower := ROW(0, 0), upper := ROW(40, 40))", "x, y"),
}
ROWS = 1200


def in_box(row, low=10, high=30):
    return (row["x"] is not None and row["y"] is not None
            and low <= row["x"] <= high and low <= row["y"] <= high)


def make_row(row_id):
    return {"id": row_id, "x": None if row_id % 53 == 0 else row_id % 41,
            "y": (row_id * 13) % 41, "tag": row_id % 5}


def values(row):
    x = "NULL" if row["x"] is None else row["x"]
    return f"({row['id']}, {x}, {row['y']}, {row['tag']}, [{row['id']}, 0]::FLOAT[2])"


SHAPES = [
    ("{box}", in_box),
    ("tag = 2 AND {box}", lambda r: r["tag"] == 2 and in_box(r)),
    ("{box} AND tag <> 2", lambda r: r["tag"] != 2 and in_box(r)),
    ("{box} AND tag IN (1, 3)", lambda r: r["tag"] in (1, 3) and in_box(r)),
    ("x BETWEEN 10 AND 30 AND y BETWEEN 10 AND 30", in_box),
    ("tag = 2 AND x BETWEEN 10 AND 30 AND y BETWEEN 10 AND 30",
     lambda r: r["tag"] == 2 and in_box(r)),
    ("{call} @@ ts_between(ROW(NULL, 10), ROW(NULL, 30), true, true)",
     lambda r: r["x"] is not None and 10 <= r["y"] <= 30),
    ("{call} @@ ts_between(ROW(10, NULL), ROW(30, NULL), true, true) AND tag = 2",
     lambda r: r["x"] is not None and 10 <= r["x"] <= 30 and r["tag"] == 2),
    ("{call} @@ ts_between(ROW(10, 10), ROW(NULL, 30), true, true)",
     lambda r: r["x"] is not None and r["x"] >= 10 and 10 <= r["y"] <= 30),
    ("{call} @@ ts_between(ROW(NULL, 10), ROW(30, 30), true, true)",
     lambda r: r["x"] is not None and r["x"] <= 30 and 10 <= r["y"] <= 30),
    ("{box} AND {other}", lambda r: in_box(r) and in_box(r, 20, 40)),
    ("{call} @@ ts_between(ROW(1000, 1000), ROW(2000, 2000), true, true)", lambda r: False),
    ("{call} @@ ts_between(ROW(5, 5), ROW(4, 4), true, true)", lambda r: False),
]
SELECTIVE = [
    ("{call} @@ ts_between(ROW(1, 13), ROW(1, 13), true, true)",
     lambda r: r["x"] == 1 and r["y"] == 13),
    ("x = 2 AND y = 26", lambda r: r["x"] == 2 and r["y"] == 26),
]
EXACT = ("ckm_morton", "ckm_plain", "ckm_hilbert")
PREPARED = [
    ("tag = $1 AND {box}", lambda r: r["tag"] == 3 and in_box(r), "3"),
    ("{box} AND tag = $1", lambda r: r["tag"] == 3 and in_box(r), "3"),
    ("tag = $1 AND x BETWEEN 10 AND 30 AND y BETWEEN 10 AND 30",
     lambda r: r["tag"] == 3 and in_box(r), "3"),
    ("x BETWEEN $1 AND 30 AND y BETWEEN 10 AND 30", in_box, "10"),
    ("{call} @@ ts_between(ROW($1::BIGINT, 10), ROW(30, 30), true, true)", in_box, "10"),
]


def nearest(rows, test, target, limit):
    return sorted((r["id"] for r in rows if test(r)),
                  key=lambda i: abs(i - target))[:limit]


def ids(found):
    found = sorted(found)
    return "{" + ",".join(map(str, found)) + "}" if found else "NULL"


def answer(sql):
    return f"SELECT list(id ORDER BY id) AS ids FROM ({sql}) t"


for index, (call, include) in INDEXES.items():
    t.part()
    t.section(f"{call}, included columns: {include}.")
    rows = [make_row(i) for i in range(ROWS)]
    t.table("ckm", "id INTEGER PRIMARY KEY, x BIGINT, y BIGINT, tag INTEGER, emb FLOAT[2]")
    t.insert("ckm", (values(r) for r in rows), chunk=400)
    t.index(index, "ckm", f"id, tag, ({call}), emb hnsw (metric = 'l2', quant = 'none')",
            include=include)
    t.refresh("ckm")
    box = f"{call} @@ ts_between(ROW(10, 10), ROW(30, 30), true, true)"
    other = f"{call} @@ ts_between(ROW(20, 20), ROW(40, 40), true, true)"

    prepared = []
    for k, (where, test, parameter) in enumerate(PREPARED):
        condition = where.format(call=call, box=box, other=other)
        t.statement(f"PREPARE ckm_p{k} AS " + answer(
            f"SELECT id FROM {index} WHERE {condition} ORDER BY emb <-> [0.4, 0]::FLOAT[2] LIMIT 3"))
        prepared.append((f"ckm_p{k}", test, parameter))

    def settle():
        t.refresh("ckm")
        if index not in EXACT:
            t.statement("VACUUM (COMPACT_TABLE) ckm")

    def check():
        for where, test in SHAPES + SELECTIVE if index in EXACT else SHAPES:
            condition = where.format(call=call, box=box, other=other)
            for target in (0.4, 600.4):
                for limit in (1, 3, 10):
                    t.expect(answer(f"SELECT id FROM {index} WHERE {condition} "
                                    f"ORDER BY emb <-> [{target}, 0]::FLOAT[2] LIMIT {limit}"),
                             ["ids"], [[ids(nearest(rows, test, target, limit))]])
                t.expect(answer(f"SELECT id FROM {index} WHERE {condition} "
                                f"AND emb <-> [{target}, 0]::FLOAT[2] < 200"),
                         ["ids"], [[ids(r["id"] for r in rows
                                        if test(r) and abs(r["id"] - target) < 200)]])
        for name, test, parameter in prepared:
            for _ in range(2):
                t.expect(f"EXECUTE {name}({parameter})", ["ids"],
                         [[ids(nearest(rows, test, 0.4, 3))]])

    check()
    t.section("A second segment.")
    added = [make_row(i) for i in range(ROWS, ROWS + 300)]
    t.insert("ckm", (values(r) for r in added), chunk=400)
    settle()
    rows += added
    check()
    t.section("After deletes.")
    t.statement("DELETE FROM ckm WHERE id % 3 = 1")
    settle()
    rows = [r for r in rows if r["id"] % 3 != 1]
    check()

t.write()
