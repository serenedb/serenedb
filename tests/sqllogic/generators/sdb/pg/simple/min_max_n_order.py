from sqllogic_gen import Test

GROUPS = 97
COLUMNS = ("s1", "s2", "s3", "b")
FUNCTIONS = (("max", "DESC"), ("min", "ASC"))
COUNTS = (1, 3, 10)

t = Test("n-valued min, max, arg_min and arg_max keep the n best keys of every group in byte order: strings "
         "inlined and longer than the 12 bytes a string_t holds inline, sharing prefixes of up to 20 bytes, "
         "empty, multibyte, and BLOBs with runs of zero bytes, each checked against the group's sorted keys "
         "on one thread and on all of them.")

t.statement(f"CREATE TABLE mm AS SELECT i, i % {GROUPS} AS g, "
            "repeat('p', (i % 21)::INT) || chr((97 + (i * 7) % 26)::INT) || ((i * 7919) % 1000)::VARCHAR AS s1, "
            "substr(md5(i::VARCHAR), 1, (i % 33)::INT) AS s2, "
            "chr((120 + i % 140)::INT) || substr(md5(i::VARCHAR), 1, (i % 20)::INT) AS s3, "
            "unhex(lpad(to_hex((i * 7919) % 65536), 4, '0') || repeat('00', (i % 20)::INT) || "
            "lpad(to_hex(i % 256), 2, '0')) AS b "
            "FROM range(200000) t(i)")

for threads in ("1", "DEFAULT"):
    t.section(f"threads = {threads}.")
    t.statement(f"SET threads = {threads}" if threads != "DEFAULT" else "RESET threads")
    for column in COLUMNS:
        for name, order in FUNCTIONS:
            for n in COUNTS:
                want = f"(array_agg({column} ORDER BY {column} {order}) FILTER (WHERE {column} IS NOT NULL))[1:{n}]"
                for got in (f"{name}({column}, {n})", f"arg_{name}({column}, {column}, {n})"):
                    t.expect(f"SELECT count(*) FILTER (WHERE got IS DISTINCT FROM want) AS mismatches, "
                             f"count(*) AS groups FROM (SELECT g, {got} AS got, {want} AS want FROM mm GROUP BY g)",
                             ["mismatches", "groups"], [[0, GROUPS]])
t.write()
