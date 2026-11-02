from sqllogic_gen import Test

KINDS = [
    ("int4", "int4", ["1", "3", "5", "7", "9"], False),
    ("int8", "int8", ["5000000001", "5000000003", "5000000005", "5000000007", "5000000009"], False),
    ("num", "numeric", ["1.5", "2.25", "3.5", "4.75", "6"], True),
    ("date", "date", ["2020-01-01", "2020-01-03", "2020-01-05", "2020-01-07", "2020-01-09"], False),
    ("ts", "timestamp", ["2020-01-01 10:00:00", "2020-01-01 12:00:00", "2020-01-02 00:00:00",
                         "2020-01-03 06:30:00", "2020-01-05 00:00:00"], True),
    ("tstz", "timestamptz", ["2020-01-01 10:00:00+00", "2020-01-01 12:00:00+00",
                             "2020-01-02 00:00:00+00", "2020-01-03 06:30:00+00",
                             "2020-01-05 00:00:00+00"], True),
]

RANGE_PREDICATES = ["@>", "<@", "&&", "<<", ">>", "&<", "&>", "-|-"]
COMPARISONS = ["=", "<>", "<", "<=", ">", ">="]


def bound(value):
    return f'"{value}"' if value else ""


def literal(lower, upper, left="[", right=")"):
    return f"{left}{bound(lower)},{bound(upper)}{right}"


def ranges(v, continuous, infinite):
    out = ["empty", literal(v[1], v[3]), literal(v[1], v[2]), literal(v[2], v[4]),
           literal(None, v[2], "("), literal(v[3], None), literal(None, None, "("),
           literal(v[0], v[1])]
    if continuous:
        out += [literal(v[1], v[3], "[", "]"), literal(v[1], v[3], "(", ")"),
                literal(v[2], v[4], "(", "]"), literal(v[2], v[2], "[", "]"),
                literal(v[1], v[2], "(", "]"), literal(v[2], v[3], "(", ")")]
    if infinite:
        out += [literal(v[2], "infinity"), literal("-infinity", v[1], "(", "]")]
    return out


def multiranges(v, continuous):
    out = ["{}", "{" + literal(v[1], v[2]) + "," + literal(v[3], v[4]) + "}",
           "{" + literal(v[0], v[4]) + "}",
           "{" + literal(None, v[1], "(") + "," + literal(v[2], v[3]) + "}",
           "{" + literal(v[2], None) + "}", "{" + literal(v[1], v[3]) + "}",
           "{" + literal(v[0], v[1]) + "," + literal(v[2], v[3]) + "," + literal(v[4], None) + "}"]
    if continuous:
        out += ["{" + literal(v[1], v[2], "[", "]") + "," + literal(v[3], v[4], "(", ")") + "}",
                "{" + literal(v[0], v[1], "(", "]") + "}"]
    return out


def quote(text):
    return "'" + text.replace("'", "''") + "'"


def flags(expression):
    return (f"CASE WHEN {expression} THEN 't' WHEN NOT ({expression}) THEN 'f' ELSE 'n' END")


t = Test("Every range and multirange function, operator and aggregate on every range type, over "
         "ranges with every kind of bound, every answer recorded from PostgreSQL. numeric bounds "
         "keep three fractional digits in SereneDB, so where a query shows them each engine's "
         "answer is recorded from it.")
t.statement("SET TimeZone = 'UTC'")
for kind, element, v, continuous in KINDS:
    rng = f"{kind}range"
    multi = f"{kind}multirange"
    numeric = kind == "num"
    infinite = kind in ("date", "ts", "tstz")
    rows = ranges(v, continuous, infinite)
    t.statement(f"CREATE TABLE {kind}_r (id int, r {rng})")
    t.statement(f"INSERT INTO {kind}_r VALUES " + ", ".join(
        f"({i}, {quote(text)})" for i, text in enumerate(rows, 1)) + f", ({len(rows) + 1}, NULL)")
    mrows = multiranges(v, continuous)
    t.statement(f"CREATE TABLE {kind}_m (id int, m {multi})")
    t.statement(f"INSERT INTO {kind}_m VALUES " + ", ".join(
        f"({i}, {quote(text)})" for i, text in enumerate(mrows, 1)) + f", ({len(mrows) + 1}, NULL)")
    t.statement(f"CREATE TABLE {kind}_e (id int, e {element})")
    t.statement(f"INSERT INTO {kind}_e VALUES " + ", ".join(
        f"({i}, {quote(value)})" for i, value in enumerate(v, 1)) + f", ({len(v) + 1}, NULL)")

    def pairs(expression, left, right):
        t.query(f"SELECT a.id AS a, string_agg({flags(expression)}, '' ORDER BY b.id) AS r "
                f"FROM {left} a, {right} b GROUP BY a.id ORDER BY a.id", ["a", "r"], None)

    def text(expression):
        return f"coalesce(({expression})::text, 'NULL')"

    def shown(sql, columns):
        if numeric:
            t.query(sql, columns, None, skipif="serenedb")
            t.query(sql, columns, None, onlyif="serenedb")
        else:
            t.query(sql, columns, None)

    def shown_pairs(expression, left, right):
        sql = (f"SELECT a.id AS a, string_agg({text(expression)}, ' ' ORDER BY b.id) AS r "
               f"FROM {left} a, {right} b GROUP BY a.id ORDER BY a.id")
        shown(sql, ["a", "r"])

    t.query(f"SELECT id, isempty(r) AS isempty, lower_inc(r) AS lower_inc, upper_inc(r) AS upper_inc, "
            f"lower_inf(r) AS lower_inf, upper_inf(r) AS upper_inf FROM {kind}_r ORDER BY id",
            ["id", "isempty", "lower_inc", "upper_inc", "lower_inf", "upper_inf"], None)
    shown(f"SELECT id, lower(r) AS lower, upper(r) AS upper, multirange(r)::text AS multirange, "
          f"r::{multi}::text AS cast FROM {kind}_r ORDER BY id",
          ["id", "lower", "upper", "multirange", "cast"])
    for op in RANGE_PREDICATES + COMPARISONS:
        pairs(f"a.r {op} b.r", f"{kind}_r", f"{kind}_r")
    pairs("a.r @> b.e", f"{kind}_r", f"{kind}_e")
    pairs("b.e <@ a.r", f"{kind}_r", f"{kind}_e")
    shown_pairs("CASE WHEN isempty(a.r) OR isempty(b.r) OR a.r && b.r OR a.r -|- b.r "
                "THEN a.r + b.r END", f"{kind}_r", f"{kind}_r")
    shown_pairs("a.r * b.r", f"{kind}_r", f"{kind}_r")
    shown_pairs("CASE WHEN isempty(a.r) OR isempty(b.r) OR a.r &> b.r OR a.r &< b.r "
                "THEN a.r - b.r END", f"{kind}_r", f"{kind}_r")
    shown_pairs("range_merge(a.r, b.r)", f"{kind}_r", f"{kind}_r")
    for flag in ("[]", "[)", "(]", "()"):
        shown_pairs(f"CASE WHEN a.e IS NULL OR b.e IS NULL OR a.e <= b.e "
                    f"THEN {rng}(a.e, b.e, '{flag}') END", f"{kind}_e", f"{kind}_e")

    t.query(f"SELECT id, isempty(m) AS isempty, lower_inc(m) AS lower_inc, upper_inc(m) AS upper_inc, "
            f"lower_inf(m) AS lower_inf, upper_inf(m) AS upper_inf FROM {kind}_m ORDER BY id",
            ["id", "isempty", "lower_inc", "upper_inc", "lower_inf", "upper_inf"], None)
    shown(f"SELECT id, lower(m) AS lower, upper(m) AS upper, range_merge(m)::text AS merged "
          f"FROM {kind}_m ORDER BY id", ["id", "lower", "upper", "merged"])
    for op in RANGE_PREDICATES + COMPARISONS:
        pairs(f"a.m {op} b.m", f"{kind}_m", f"{kind}_m")
    for op in RANGE_PREDICATES:
        pairs(f"a.m {op} b.r", f"{kind}_m", f"{kind}_r")
        pairs(f"b.r {op} a.m", f"{kind}_m", f"{kind}_r")
    pairs("a.m @> b.e", f"{kind}_m", f"{kind}_e")
    pairs("b.e <@ a.m", f"{kind}_m", f"{kind}_e")
    for op in ("+", "*", "-"):
        shown_pairs(f"a.m {op} b.m", f"{kind}_m", f"{kind}_m")
    shown(f"SELECT {multi}()::text AS a", ["a"])
    shown_pairs(f"CASE WHEN a.r IS NOT NULL AND b.r IS NOT NULL THEN {multi}(a.r, b.r) END",
                f"{kind}_r", f"{kind}_r")
    shown(f"SELECT id, {multi}(r)::text AS m FROM {kind}_r ORDER BY id", ["id", "m"])
    shown(f"SELECT range_agg(r)::text AS a, range_intersect_agg(r)::text AS b FROM {kind}_r",
          ["a", "b"])
    shown(f"SELECT range_agg(m)::text AS a, range_intersect_agg(m)::text AS b FROM {kind}_m",
          ["a", "b"])
    shown(f"SELECT id, (range_agg(r) OVER w)::text AS a, (range_intersect_agg(r) OVER w)::text AS b "
          f"FROM {kind}_r WINDOW w AS (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) "
          "ORDER BY id", ["id", "a", "b"])
    shown(f"SELECT id, (range_agg(m) OVER w)::text AS a, (range_intersect_agg(m) OVER w)::text AS b "
          f"FROM {kind}_m WINDOW w AS (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) "
          "ORDER BY id", ["id", "a", "b"])
    shown(f"SELECT id % 3 AS g, range_agg(r)::text AS a, range_intersect_agg(r)::text AS b "
          f"FROM {kind}_r GROUP BY id % 3 ORDER BY g", ["g", "a", "b"])
    shown(f"SELECT id % 3 AS g, range_agg(m)::text AS a, range_intersect_agg(m)::text AS b "
          f"FROM {kind}_m GROUP BY id % 3 ORDER BY g", ["g", "a", "b"])
t.write()
