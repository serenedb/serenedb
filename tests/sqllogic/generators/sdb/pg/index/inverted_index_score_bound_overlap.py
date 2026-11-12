from sqllogic_gen import Test, lit

t = Test("Top-k pruning preserves the best row when duplicate tokens share a position.")
t.dictionary("overlap_dict", "[split_text_csv(' '), keyword()] WITH (frequency, position, norm)")
t.table("overlap_docs", "id INTEGER PRIMARY KEY, body TEXT")
t.statement("INSERT INTO overlap_docs VALUES (1, 'x x')")
t.statement("INSERT INTO overlap_docs SELECT i, 'x z' || i::VARCHAR FROM range(2, 300) r(i)")
t.statement("INSERT INTO overlap_docs VALUES (300, 'x')")

for name, config, scorer in (
    ("dirichlet", "lm_dirichlet(2000)", "lm_dirichlet({index}.tableoid)"),
    ("dfi_standardized", "dfi('standardized')", "dfi({index}.tableoid, 'standardized')"),
    ("dfi_saturated", "dfi('saturated')", "dfi({index}.tableoid, 'saturated')"),
    ("dfi_chi_squared", "dfi('chi_squared')", "dfi({index}.tableoid, 'chi_squared')"),
    ("jm", "lm_jm(0.1)", "lm_jm({index}.tableoid, 0.1)"),
    ("bm25", "bm25(1.2, 0.75)", "bm25({index}.tableoid, 1.2, 0.75)"),
    ("bm25_norm", "bm25(1.2, 1)", "bm25({index}.tableoid, 1.2, 1)"),
):
    t.part()
    index = f"overlap_{name}_idx"
    t.statement(f"CREATE INDEX {index} ON overlap_docs USING inverted(id, body overlap_dict) "
                f"WITH (optimize_top_k = '{lit(config)}', "
                "refresh_interval = 0, compaction_interval = 0)")
    t.refresh("overlap_docs")
    query = (f"SELECT id FROM {index} WHERE body @@ 'x' "
             f"ORDER BY {scorer.format(index=index)} DESC LIMIT 1")
    t.statement("SET sdb_disable_top_k_optimization = true")
    t.expect(query, ["id"], [[300]])
    t.statement("RESET sdb_disable_top_k_optimization")
    t.expect(query, ["id"], [[300]])

t.write()
