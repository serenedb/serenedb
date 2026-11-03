from sqllogic_gen import Test

t = Test("Uniform scalar quantization returns constant segments with finite error bounds.")
t.statement("SET sdb_ivf_search_nprobe = 1000")

for kind in ("hnsw", "ivf"):
    for metric, op in (("ip", "<#>"), ("cosine", "<=>"), ("l2", "<->")):
        for value, rows in ((0, 10), (2, 10), (2, 1)):
            t.part()
            table = f"constant_{kind}_{metric}_{value}_{rows}"
            index = f"{table}_idx"
            t.table(table, "id INTEGER PRIMARY KEY, emb FLOAT[4]")
            t.statement(f"CREATE INDEX {index} ON {table} USING inverted(id, "
                        f"emb {kind}(metric = '{metric}', quant = 'usq8')) "
                        "WITH (refresh_interval = 0, compaction_interval = 0)")
            t.statement(f"INSERT INTO {table} SELECT i, [{value}, {value}, {value}, {value}]::FLOAT[4] "
                        f"FROM range({rows}) r(i)")
            t.refresh(table)
            for width in (0, 4):
                t.statement(f"SET sdb_ann_rescore_width = {width}")
                t.expect(f"SELECT count(*) AS n FROM (SELECT id FROM {index} "
                         f"ORDER BY emb {op} [1, 2, 3, 4]::FLOAT[4] LIMIT 5) q",
                         ["n"], [[min(rows, 5)]])
            if value == 2 and rows == 1:
                t.statement(f"INSERT INTO {table} SELECT i, [i, 0, 0, 0]::FLOAT[4] "
                            "FROM range(1, 11) r(i)")
                t.refresh(table)
                t.expect(f"SELECT id FROM {index} ORDER BY emb {op} "
                         "[1, 2, 3, 4]::FLOAT[4] LIMIT 1", ["id"], [[0]])

t.write()
