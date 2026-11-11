from sqllogic_gen import Test

t = Test("Boxes from one row to every row, over segments of different sizes "
         "and of rows sharing their points, so the matched rows are read in "
         "every way: alone, in conjunctions, negated, scored and limited. The "
         "rows agree with the table, and a box scores like any other condition "
         "without terms of the text: mismatches is always 0.", seed=1743)
rng = t.rng

CALL = "curve(ROW(x, y))"
HILBERT = "curve(ROW(x, y), kind := 'hilbert')"
SHAPES = [
    "SELECT id FROM {r} WHERE {p}",
    "SELECT id FROM {r} WHERE tag = 2 AND {p}",
    "SELECT id FROM {r} WHERE {p} AND tag <> 2",
    "SELECT id FROM {r} WHERE tag IN (1, 2, 4) AND {p}",
    "SELECT id FROM {r} WHERE {p} AND id BETWEEN 50 AND 4000",
    "SELECT id FROM {r} WHERE {p} AND id % 3 = 0",
    "SELECT id FROM {r} WHERE body @@ 'beta' AND {p}",
    "SELECT id FROM {r} WHERE body @@ 'alpha' AND tag = 1 AND {p}",
    "SELECT id FROM {r} WHERE {p} AND NOT body @@ 'beta'",
    "SELECT id FROM {r} WHERE (body @@ 'beta' OR tag = 3) AND {p}",
    "SELECT id FROM {r} WHERE {p} ORDER BY id LIMIT 7",
    "SELECT id FROM {r} WHERE tag = 1 AND {p} ORDER BY id DESC LIMIT 7",
    "SELECT count(*) AS id FROM {r} WHERE {p}",
    "SELECT count(*) AS id FROM {r} WHERE tag = 2 AND {p}",
    "SELECT count(*) AS id FROM {r} WHERE {p} AND tag <> 2",
    "SELECT count(*) AS id FROM {r} WHERE body @@ 'beta' AND {p}",
    "SELECT sum(id) AS id FROM {r} WHERE {p}",
]


def box(call, lx, hx, ly, hy):
    return f"{call} @@ ts_between(ROW({lx}, {ly}), ROW({hx}, {hy}), true, true)"


def truth(lx, hx, ly, hy):
    return f"x BETWEEN {lx} AND {hx} AND y BETWEEN {ly} AND {hy}"


def body(i):
    return "CASE WHEN i % 3 = 0 THEN 'alpha beta' WHEN i % 3 = 1 THEN 'alpha' ELSE 'beta beta alpha' END"


def setup(table, rows, spread, copies):
    t.dictionary(f"{table}_dict", "split_text(case := 'lower') WITH (frequency, position, offset)")
    t.table(table, "id INTEGER PRIMARY KEY, x BIGINT, y BIGINT, tag INTEGER, body VARCHAR")
    t.statement(
        f"INSERT INTO {table} SELECT i, (i // {copies}) * 7919 % {spread}, "
        f"(i // {copies}) * 104729 % {spread}, i % 5, {body('i')} FROM range({rows}) t(i)")
    keys = f"id, tag, body {table}_dict"
    t.index(f"{table}_morton", table, f"{keys}, ({CALL})")
    t.index(f"{table}_hilbert", table, f"{keys}, ({HILBERT})")
    t.index(f"{table}_plain", table, f"{keys}, x, y")
    t.refresh(table)


def widths(spread):
    width = 1
    while width < spread:
        yield width
        width *= 2
    yield spread


def check_box(table, bounds, shapes):
    for shape in shapes:
        for index, call in (("morton", CALL), ("hilbert", HILBERT)):
            actual = shape.format(r=f"{table}_{index}", p=box(call, *bounds))
            expected = shape.format(r=table, p=truth(*bounds)).replace(
                "NOT body @@ 'beta'", "body NOT LIKE '%beta%'").replace(
                "body @@ 'beta'", "body LIKE '%beta%'").replace(
                "body @@ 'alpha'", "body LIKE '%alpha%'")
            t.same_rows(f"SELECT * FROM ({actual}) t", f"SELECT * FROM ({expected}) t")


def check_rows(table, spread):
    for width in widths(spread):
        lx = rng.randrange(max(spread - width, 1))
        ly = rng.randrange(max(spread - width, 1))
        check_box(table, (lx, lx + width - 1, ly, ly + width - 1), SHAPES)
        lx -= lx % width
        ly -= ly % width
        check_box(table, (lx, lx + width - 1, ly, ly + width - 1), SHAPES[:3])


def check_scores(table, spread):
    for width in widths(spread):
        lx = rng.randrange(max(spread - width, 1))
        ly = rng.randrange(max(spread - width, 1))
        bounds = (lx, lx + width - 1, ly, ly + width - 1)
        highlighted = ("SELECT id, ts_highlight(body, ts_offsets(body)) AS h FROM {r} "
                       "WHERE body @@ 'beta' AND {p}")
        t.same_rows(highlighted.format(r=f"{table}_morton", p=box(CALL, *bounds)),
                    highlighted.format(r=f"{table}_plain", p=truth(*bounds)))
        for index, call in (("morton", CALL), ("hilbert", HILBERT)):
            t.same_scores(f"SELECT id, bm25(tableoid) AS s FROM {table}_{index} "
                          f"WHERE {box(call, *bounds)}",
                          f"SELECT id, 1::FLOAT AS s FROM {table} WHERE {truth(*bounds)}")
        for text in ("body @@ 'beta'", "body @@ 'alpha'",
                     "body @@ 'alpha' AND tag = 2",
                     "(body @@ 'beta' OR body @@ 'alpha')"):
            plain = (f"SELECT id, s FROM (SELECT id, bm25(tableoid) AS s FROM {table}_plain "
                     f"WHERE {text} AND id >= 0) t "
                     f"WHERE id IN (SELECT id FROM {table} WHERE {truth(*bounds)})")
            for index, call in (("morton", CALL), ("hilbert", HILBERT)):
                t.same_scores(f"SELECT id, bm25(tableoid) AS s FROM {table}_{index} "
                              f"WHERE {text} AND {box(call, *bounds)}", plain)
                t.same_scores(f"SELECT * FROM (SELECT id, bm25(tableoid) AS s FROM {table}_{index} "
                              f"WHERE {text} AND {box(call, *bounds)} ORDER BY s DESC, id LIMIT 7) t",
                              f"SELECT * FROM ({plain} ORDER BY s DESC, id LIMIT 7) t")


def sizes():
    yield "one point", 300, 1, 300
    yield "ten rows", 10, 64, 1
    yield "no shared points", 6000, 8192, 1
    yield "two rows a point", 6000, 4096, 2
    yield "63 rows a point", 6300, 512, 63
    yield "65 rows a point", 6500, 512, 65
    yield "a thousand rows a point", 8000, 128, 1000
    yield "a dense square", 20000, 64, 3


def build(table, title, rows, spread, copies, second=False, deletes=False):
    t.part()
    t.section(f"{title}: {rows} rows.")
    setup(table, rows, spread, copies)
    if second:
        t.section("Two segments.")
        t.statement(f"INSERT INTO {table} SELECT i + {rows}, (i // {copies}) * 251 % {spread}, "
                    f"(i // {copies}) * 241 % {spread}, i % 5, {body('i')} FROM range({rows}) t(i)")
        t.refresh(table)
    if deletes:
        t.section("After deletes.")
        t.statement(f"DELETE FROM {table} WHERE id % 5 = 1")
        t.refresh(table)


for number, (title, rows, spread, copies) in enumerate(sizes()):
    table = f"cdsm_{number}"
    data = (table, title, rows, spread, copies)
    build(*data)
    check_rows(table, spread)
    check_scores(table, spread)
    build(*data, second=True)
    check_rows(table, spread)
    check_scores(table, spread)
    build(*data, second=True, deletes=True)
    check_rows(table, spread)
    build(*data, second=True, deletes=True)
    t.section("On one thread.")
    t.statement("SET threads = 1")
    check_rows(table, spread)

t.write()
