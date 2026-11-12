from sqllogic_gen import Test

t = Test("Out-of-range vectors fail atomically before an ANN index is refreshed.")
t.statement("SET sdb_ivf_search_nprobe = 1000")


def error(sql, message):
    t.statement(f"{sql}\n----\ndb error: ERROR: {message}\n\n",
                kind="statement error")


for kind in ("hnsw", "ivf"):
    for quant in ("sq8", "sq4", "usq8", "usq4", "fp16", "bf16"):
        t.part()
        table = f"range_{kind}_{quant}"
        index = f"{table}_idx"
        opclass = f"{kind}(metric = 'l2', quant = '{quant}')"
        message = ("FP16 vector components must be finite and between -65504 and 65504"
                   if quant == "fp16" else
                   "Vector magnitude exceeds the range supported by scalar quantization")
        bad = "70000" if quant == "fp16" else "3e38"
        edge = "65504" if quant == "fp16" else "1e17"
        t.table(table, "id INTEGER PRIMARY KEY, emb FLOAT[2]")
        t.statement(f"CREATE INDEX {index} ON {table} USING inverted(id, emb {opclass}) "
                    "WITH (refresh_interval = 0, compaction_interval = 0)")
        t.statement(f"INSERT INTO {table} VALUES (0, NULL)")
        t.statement(f"INSERT INTO {table} SELECT i, [i, i + 1]::FLOAT[2] FROM range(1, 201) r(i)")
        error(f"INSERT INTO {table} VALUES (201, NULL), (202, [NULL, 0]::FLOAT[2])",
              "Vector components must not be NULL")
        for value in (bad, f"-{bad}", "'Infinity'::FLOAT", "'-Infinity'::FLOAT", "'NaN'::FLOAT"):
            error(f"INSERT INTO {table} VALUES (201, [0, 0]::FLOAT[2]), "
                  f"(202, [{value}, 0]::FLOAT[2])", message)
        error(f"UPDATE {table} SET emb = [{bad}, 0]::FLOAT[2] WHERE id = 1", message)
        t.expect(f"SELECT count(*) AS n FROM {table}", ["n"], [[201]])
        t.expect(f"SELECT emb[1] AS x FROM {table} WHERE id = 1", ["x"], [[1]])
        t.refresh(table)
        t.expect(f"SELECT id FROM {index} ORDER BY emb <-> [0, 0]::FLOAT[2] LIMIT 2",
                 ["id"], [[1], [2]])
        for value in ("3e38", "'Infinity'::FLOAT", "'NaN'::FLOAT"):
            error(f"SELECT id FROM {index} ORDER BY emb <-> [{value}, 0]::FLOAT[2] LIMIT 2",
                  "Vector magnitude exceeds the range supported by scalar quantization")
        t.statement(f"INSERT INTO {table} VALUES (203, [{edge}, 0]::FLOAT[2]), "
                    f"(204, [-{edge}, 0]::FLOAT[2])")
        t.refresh(table)
        t.expect(f"SELECT count(*) AS n FROM (SELECT id FROM {index} "
                 "ORDER BY emb <-> [0, 0]::FLOAT[2] LIMIT 300) q", ["n"], [[202]])
        if quant == "fp16":
            t.expect(f"SELECT id FROM {index} ORDER BY emb <-> [70000, 0]::FLOAT[2] LIMIT 1",
                     ["id"], [[203]])
        t.statement(f"DROP INDEX {index}")
        t.statement(f"INSERT INTO {table} VALUES (205, [{bad}, 0]::FLOAT[2])")
        error(f"CREATE INDEX {index} ON {table} USING inverted(id, emb {opclass})", message)
        t.expect(f"SELECT count(*) AS n FROM {table}", ["n"], [[204]])
        t.statement(f"DELETE FROM {table} WHERE id = 205")
        t.statement(f"CREATE INDEX {index} ON {table} USING inverted(id, emb {opclass})")
        t.refresh(table)
        t.statement("SET sdb_ann_force_exact = true")
        t.expect(f"SELECT count(*) AS n FROM (SELECT id FROM {index} "
                 "ORDER BY emb <-> [0, 0]::FLOAT[2] LIMIT 300) q", ["n"], [[202]])
        t.expect(f"SELECT id FROM {index} ORDER BY emb <-> [0, 0]::FLOAT[2] LIMIT 2",
                 ["id"], [[1], [2]])
        t.statement("RESET sdb_ann_force_exact")

for storage in ("", " WITH (storage = 'search', refresh_interval = 0, compaction_interval = 0)"):
    for quant, bad, message in (
        ("sq8", "3e38", "Vector magnitude exceeds the range supported by scalar quantization"),
        ("fp16", "70000", "FP16 vector components must be finite and between -65504 and 65504"),
    ):
        t.part()
        t.statement(f"CREATE TABLE range_expr (id INTEGER PRIMARY KEY, x FLOAT, active BOOLEAN){storage}")
        options = "" if storage else " WITH (refresh_interval = 0, compaction_interval = 0)"
        predicate = "" if storage else " WHERE active"
        expression = "CASE WHEN x IS NULL THEN NULL::FLOAT[2] ELSE [x, 0]::FLOAT[2] END"
        t.statement(f"CREATE INDEX range_expr_idx ON range_expr USING inverted "
                    f"(id, ({expression}) hnsw(metric = 'l2', quant = '{quant}')) "
                    f"{options}{predicate}")
        if storage:
            t.statement("INSERT INTO range_expr VALUES (1, NULL, false), (2, 1, true)")
            t.refresh("range_expr")
            error(f"UPDATE range_expr SET x = {bad} WHERE id = 1", message)
            t.expect("SELECT x IS NULL AS unchanged FROM range_expr WHERE id = 1",
                     ["unchanged"], [["t"]])
        else:
            t.statement(f"INSERT INTO range_expr VALUES (1, {bad}, false), (2, 1, true)")
            error("UPDATE range_expr SET active = true WHERE id = 1", message)
        t.expect("SELECT id, active FROM range_expr ORDER BY id", ["id", "active"],
                 [[1, "f"], [2, "t"]])
        t.refresh("range_expr")
        t.expect(f"SELECT id FROM range_expr_idx ORDER BY ({expression}) <-> [0, 0]::FLOAT[2] LIMIT 2",
                 ["id"], [[2]])

for quants in (("sq8", "fp16"), ("fp16", "sq8")):
    t.part()
    t.statement("CREATE TABLE range_shared (id INTEGER PRIMARY KEY, emb FLOAT[2]) "
                "WITH (storage = 'search', refresh_interval = 0, compaction_interval = 0)")
    for quant in quants:
        t.statement(f"CREATE INDEX range_shared_{quant} ON range_shared USING inverted "
                    f"(id, emb hnsw(metric = 'l2', quant = '{quant}'))")
    error("INSERT INTO range_shared VALUES (1, [70000, 0]::FLOAT[2])",
          "FP16 vector components must be finite and between -65504 and 65504")
    t.expect("SELECT count(*) AS n FROM range_shared", ["n"], [[0]])
    t.statement("INSERT INTO range_shared VALUES (1, [65504, 0]::FLOAT[2])")
    t.refresh("range_shared")
    for quant in quants:
        t.expect(f"SELECT id FROM range_shared_{quant} "
                 "ORDER BY emb <-> [0, 0]::FLOAT[2] LIMIT 1", ["id"], [[1]])

for kind in ("hnsw", "ivf"):
    t.part()
    t.table("range_null", "id INTEGER PRIMARY KEY, emb FLOAT[2]")
    t.statement(f"CREATE INDEX range_null_idx ON range_null USING inverted "
                f"(id, emb {kind}(metric = 'l2', quant = 'none')) "
                "WITH (refresh_interval = 0, compaction_interval = 0)")
    error("INSERT INTO range_null VALUES (1, NULL), (2, [NULL, 0]::FLOAT[2])",
          "Vector components must not be NULL")
    t.expect("SELECT count(*) AS n FROM range_null", ["n"], [[0]])
    t.statement("INSERT INTO range_null VALUES (1, NULL), (2, [1, 0]::FLOAT[2])")
    t.refresh("range_null")
    t.expect("SELECT id FROM range_null_idx ORDER BY emb <-> [0, 0]::FLOAT[2] LIMIT 2",
             ["id"], [[2]])

t.write()
