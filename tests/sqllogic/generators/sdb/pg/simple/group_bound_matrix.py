from sqllogic_gen import Test
from temporal_gen import diff

SORTED = 30000
SHUFFLED = 60000
ROWS = 64000

B = {
    "sorted": f"(i * 101 // {SORTED})",
    "shuffled": "((i * 37) % 101)",
    "tail": "((i * 53) % 101)",
}
H = {
    "sorted": "i",
    "shuffled": f"({SORTED} + (i * 7919) % {SHUFFLED - SORTED})",
    "tail": f"((i * 104729) % {ROWS})",
}

SPECIAL = ("CASE {b} % 10 WHEN 0 THEN 'nan'::{t} WHEN 1 THEN '-inf'::{t} WHEN 2 THEN '-0.0'::{t} "
           "WHEN 3 THEN 'inf'::{t} ELSE (({b} - 50) / 4.0)::{t} END")

COLUMNS = [
    ("k_bool", "BOOLEAN", "{b} % 2 = 0"),
    ("k_i8", "TINYINT", "({b} - 50)::TINYINT"),
    ("k_i16", "SMALLINT", "(({b} - 50) * 300)::SMALLINT"),
    ("k_i32", "INTEGER", f"({{h}} - {ROWS // 2})::INTEGER"),
    ("k_i64", "BIGINT", "({b} - 50)::BIGINT * 100000000000"),
    ("k_i128", "HUGEINT", "({b} - 50)::HUGEINT * 1000000000000000000000000000000::HUGEINT"),
    ("k_u8", "UTINYINT", "{b}::UTINYINT"),
    ("k_u16", "USMALLINT", "({b} * 600)::USMALLINT"),
    ("k_u32", "UINTEGER", "({h}::BIGINT * 40000)::UINTEGER"),
    ("k_u64", "UBIGINT", "{b}::UBIGINT * 100000000000000000::UBIGINT"),
    ("k_u128", "UHUGEINT", "{b}::UHUGEINT * 1000000000000000000000000000000000000::UHUGEINT"),
    ("k_f32", "REAL", SPECIAL.replace("{t}", "REAL")),
    ("k_f64", "DOUBLE", f"CASE WHEN {{h}} % 97 = 0 THEN {SPECIAL.replace('{t}', 'DOUBLE')} "
                        f"ELSE ({{h}} - {ROWS // 2}) / 8.0 END"),
    ("k_dec4", "DECIMAL(4, 1)", "(({b} - 50) / 2.0)::DECIMAL(4, 1)"),
    ("k_dec18", "DECIMAL(18, 3)", f"(({{h}} - {ROWS // 2}) / 8.0)::DECIMAL(18, 3)"),
    ("k_dec38", "DECIMAL(38, 10)", "(({b} - 50) / 3.0)::DECIMAL(38, 10)"),
    ("k_date", "DATE", "DATE '2000-01-01' + ({b} * 37)::INTEGER"),
    ("k_time", "TIME", "TIME '00:00:00' + {b} * INTERVAL '7 minutes'"),
    ("k_ts", "TIMESTAMP", "TIMESTAMP '2000-01-01' + {h} * INTERVAL '1 second'"),
    ("k_tstz", "TIMESTAMPTZ", "TIMESTAMPTZ '2000-01-01 00:00:00+00' + {b} * INTERVAL '1 hour'"),
    ("k_iv", "INTERVAL", "CASE {b} % 3 WHEN 0 THEN ({b} // 3) * INTERVAL '1 day' "
                         "WHEN 1 THEN ({b} // 3) * INTERVAL '24 hours' ELSE ({b} // 3) * INTERVAL '30 days' END"),
    ("k_uuid", "UUID", "md5({b}::VARCHAR)::UUID"),
    ("k_blob", "BLOB", "('b' || lpad({b}::VARCHAR, 3, '0'))::BLOB"),
    ("k_bit", "BIT", "({b} % 16)::BIT"),
    ("k_short", "VARCHAR", "'v' || lpad({b}::VARCHAR, 3, '0')"),
    ("k_long", "VARCHAR", "'a_shared_prefix_longer_than_twelve_' || lpad({b}::VARCHAR, 3, '0')"),
    ("k_empty", "VARCHAR", "CASE WHEN {b} % 5 = 0 THEN '' ELSE repeat('z', {b} % 17) END"),
    ("k_high", "VARCHAR", "'h' || lpad({h}::VARCHAR, 6, '0') || '_beyond_the_inline_bytes'"),
    ("k_list", "VARCHAR[]", "['a' || ({b} % 7)::VARCHAR, 'b' || ({b} % 3)::VARCHAR]"),
    ("k_runs", "BIGINT", "{h} // 7"),
    ("k_blocky", "BIGINT", "i // 4096"),
]
NOT_NULL = {"k_blocky"}


def value(name, expr, batch):
    expr = expr.replace("{b}", B[batch]).replace("{h}", H[batch])
    return expr if name in NOT_NULL else f"CASE WHEN i % 13 = 5 THEN NULL ELSE {expr} END"


def shown(name, sql_type, column):
    if sql_type == "INTERVAL":
        return f"epoch({column})::VARCHAR"
    if sql_type in ("REAL", "DOUBLE"):
        return f"regexp_replace({column}::VARCHAR, '^-?0(\\.0)?$', '0')"
    return f"{column}::VARCHAR"


def ordered(name, sql_type):
    k = shown(name, sql_type, "k")
    selects = []
    for order in ("ASC NULLS LAST", "DESC NULLS FIRST", "ASC NULLS FIRST", "DESC NULLS LAST"):
        for limit in (1, 5, 60, 100000):
            selects.append(f"SELECT '{name} {order} {limit}' AS shape, {k} AS k, c, s FROM (SELECT {name} AS k, "
                           f"count(*) AS c, sum(v) AS s FROM gb GROUP BY {name} ORDER BY {name} {order} "
                           f"LIMIT {limit})")
    selects.append(f"SELECT '{name} offset' AS shape, {k} AS k, c, s FROM (SELECT {name} AS k, count(*) AS c, "
                   f"sum(v) AS s FROM gb GROUP BY {name} ORDER BY {name} LIMIT 5 OFFSET 7)")
    selects.append(f"SELECT '{name} filtered' AS shape, {k} AS k, c, s FROM (SELECT {name} AS k, count(*) AS c, "
                   f"sum(v) AS s FROM gb WHERE v % 3 <> 1 GROUP BY {name} ORDER BY {name} DESC LIMIT 7)")
    selects.append(f"SELECT '{name} two keys' AS shape, {k} || '/' || g::VARCHAR AS k, c, s FROM (SELECT {name} AS k, g, "
                   f"count(*) AS c, sum(v) AS s FROM gb GROUP BY {name}, g ORDER BY {name} DESC NULLS LAST, g "
                   f"LIMIT 11)")
    selects.append(f"SELECT '{name} rows' AS shape, {k} AS k, v AS c, 0 AS s FROM (SELECT {name} AS k, v FROM gb "
                   f"ORDER BY {name} ASC NULLS FIRST, v LIMIT 9)")
    return selects


t = Test("GROUP BY ... ORDER BY its keys LIMIT, the Top-N of rows and GROUP BY ... LIMIT over every key "
         "type, with NULLs, the float specials, collated intervals, strings inlined, long and empty, and "
         "keys that reach the aggregate as storage dictionaries, runs, constants and distinct rows, over "
         "sorted, shuffled and unflushed rows: the answers with the Top-N optimizer, which plans the group "
         "bound and the dynamic filters, match those without it, on one thread and on eight; a GROUP BY ... "
         "LIMIT without an order returns complete groups.")

columns = ", ".join(f"{name} {sql_type}" for name, sql_type, _ in COLUMNS)
t.statement(f"CREATE TABLE gb (v BIGINT, g SMALLINT, {columns}) "
            "WITH (compaction_interval = 0, row_group_size = 2048)")
for batch, start, end in (("sorted", 0, SORTED), ("shuffled", SORTED, SHUFFLED), ("tail", SHUFFLED, ROWS)):
    values = ", ".join(value(name, expr, batch) for name, _, expr in COLUMNS)
    t.statement(f"INSERT INTO gb SELECT i, i % 3, {values} FROM range({start}, {end}) r(i)")
    if batch != "tail":
        t.statement("VACUUM (FLUSH_TABLE) gb")

for part in t.parts(COLUMNS, 8):
    first = part[0][0]
    selects = [select for name, sql_type, _ in part for select in ordered(name, sql_type)]
    for threads in (1, 8):
        t.section(f"Ordered shapes from {first} on, threads = {threads}.")
        t.statement(f"SET threads = {threads}")
        diff(t, f"{first}_{threads}", selects, "top_n", ["shape", "k", "c", "s"])
        t.section(f"GROUP BY ... LIMIT without an order from {first} on, threads = {threads}.")
        for name, _, _ in part:
            for limit in (3, 50):
                t.expect(f"SELECT (count(*) = least({limit}, (SELECT count(DISTINCT {name}) + "
                         f"(count(*) FILTER (WHERE {name} IS NULL) > 0)::INT FROM gb)))::INT AS complete "
                         f"FROM (SELECT {name} AS k, count(*) AS c, sum(v) AS s FROM gb GROUP BY {name} "
                         f"LIMIT {limit}) l WHERE c = (SELECT count(*) FROM gb WHERE {name} IS NOT DISTINCT FROM l.k) "
                         f"AND s = (SELECT sum(v) FROM gb WHERE {name} IS NOT DISTINCT FROM l.k)",
                         ["complete"], [[1]])
    t.statement("RESET threads")
t.write()
