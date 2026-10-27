from sqllogic_gen import lit


def keyed(keys, table):
    k = " || '|' || ".join(f"coalesce(k{i}::VARCHAR, 'null')" for i in range(len(keys)))
    columns = ", ".join(f"{key} AS k{i}" for i, key in enumerate(keys))
    groups = ", ".join(str(i + 1) for i in range(len(keys)))
    return (f"SELECT '{lit(', '.join(keys))} @{table}' AS shape, {k} AS k, c, s FROM (SELECT {columns}, "
            f"count(*) AS c, sum(v) AS s FROM {table} GROUP BY {groups})")


def shaped(label, inner):
    return f"SELECT '{lit(label)}' AS shape, k::VARCHAR AS k, c, s FROM ({inner})"


def labelled(label, expr, table):
    return shaped(label, f"SELECT {expr} AS k, count(*) AS c, sum(v) AS s FROM {table} GROUP BY 1")


def named(prefix, expr, table):
    return labelled(f"{prefix} {expr.replace('||', '!!')}", expr, table)


def predicate(label, where, table):
    return f"SELECT '{lit(label)}' AS shape, count(*) AS c, coalesce(sum(v), 0) AS s FROM {table} WHERE {where}"


def union(t, table, selects, create, chunk=None):
    chunk = chunk or len(selects)
    for start in range(0, len(selects), chunk):
        head = f"{create} {table} AS" if start == 0 else f"INSERT INTO {table}"
        t.statement(head + "\n" + "\nUNION ALL ".join(selects[start:start + chunk]) + ";")


def total(t, name, selects, disabled, chunk=100):
    t.statement(f"SET disabled_optimizers = '{disabled}';")
    union(t, f"ref_{name}", selects, "CREATE OR REPLACE TABLE", chunk)
    t.statement("SET disabled_optimizers = '';")
    union(t, f"got_{name}", selects, "CREATE OR REPLACE TABLE", chunk)
    t.query(f"SELECT count(*) AS rows, count(DISTINCT shape) AS shapes, (SELECT count(*) FROM (SELECT * FROM "
            f"ref_{name} EXCEPT ALL SELECT * FROM got_{name})) AS ref_minus_got, (SELECT count(*) FROM (SELECT * "
            f"FROM got_{name} EXCEPT ALL SELECT * FROM ref_{name})) AS got_minus_ref FROM ref_{name};",
            ["rows", "shapes", "ref_minus_got", "got_minus_ref"])


def diff(t, name, selects, disabled, columns):
    t.statement(f"SET disabled_optimizers = '{disabled}';")
    union(t, f"ref_{name}", selects, "CREATE TABLE")
    t.statement("RESET disabled_optimizers;")
    union(t, f"got_{name}", selects, "CREATE TABLE")
    listed = ", ".join(columns)
    order = ", ".join(str(i) for i in range(1, len(columns) - 1))
    for a, b in ((f"got_{name}", f"ref_{name}"), (f"ref_{name}", f"got_{name}")):
        t.query(f"SELECT {listed} FROM (SELECT * FROM {a} EXCEPT ALL SELECT * FROM {b}) ORDER BY {order};",
                columns, 0)
    if columns == ["shape", "c", "s"]:
        t.query(f"SELECT count(*) AS shapes, sum(c) AS rows FROM got_{name};", ["shapes", "rows"])
    else:
        t.query(f"SELECT count(*) AS rows, count(DISTINCT shape) AS shapes FROM got_{name};", ["rows", "shapes"])
