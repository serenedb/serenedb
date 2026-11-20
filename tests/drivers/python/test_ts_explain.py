from __future__ import annotations

import math

import psycopg
import pytest
from spec_loader import conn_kwargs, schema_name

DRIVER_KEY = "python_ts_explain"

DDL = [
    "CREATE TEXT SEARCH DICTIONARY {s}.tx_full AS split_text(case := 'lower') "
    "WITH (frequency, position, norm)",
    "CREATE TEXT SEARCH DICTIONARY {s}.tx_freq AS split_text(case := 'lower') WITH (frequency)",
    "CREATE TEXT SEARCH DICTIONARY {s}.tx_none AS split_text(case := 'lower')",
    "CREATE TEXT SEARCH DICTIONARY {s}.tx_nonorm AS split_text(case := 'lower') "
    "WITH (frequency, position)",
    "CREATE TEXT SEARCH DICTIONARY {s}.tx_bigram AS generate_ngrams(2, 2) WITH (frequency, position)",
    "CREATE TABLE {s}.tx (id INTEGER PRIMARY KEY, body VARCHAR, tag VARCHAR, n INTEGER, "
    "a VARCHAR, b VARCHAR, c VARCHAR, g VARCHAR, emb FLOAT[2])",
    "CREATE INDEX tx_idx ON {s}.tx USING inverted (id, body {s}.tx_full, tag, n, "
    "a {s}.tx_freq, b {s}.tx_none, c {s}.tx_nonorm, g {s}.tx_bigram)",
    """INSERT INTO {s}.tx VALUES
        (1, 'the quick brown fox jumps over the lazy dog', 'animal', 1,
         'red fox fox', 'red fox fox', 'red fox fox', 'hello', [1, 0]),
        (2, 'fox fox fox in the henhouse', 'animal', 2, 'fox', 'fox', 'fox', 'help', [0, 1]),
        (3, 'a clever fox outwits two hounds in the meadow', 'story', 3,
         'clever dog', 'clever dog', 'clever dog', 'world', [1, 1]),
        (4, 'quick brown dog runs fast', 'animal', 4, 'quick dog', 'quick dog', 'quick dog',
         'held', [2, 0]),
        (5, 'the lazy dog sleeps all day', 'story', 5, 'lazy dog', 'lazy dog', 'lazy dog',
         'hell', [0, 2]),
        (6, 'brown bread and brown sugar', 'food', 6, 'bread', 'bread', 'bread', 'bell', [3, 1])""",
    """INSERT INTO {s}.tx VALUES
        (7, 'common quick fox and common dog', 'animal', 7, 'common fox', 'common fox',
         'common fox', 'yellow', [1, 3]),
        (8, 'common words common ground', 'story', 8, 'common', 'common', 'common', 'mellow',
         [2, 2]),
        (9, 'common fax machine', 'office', 9, 'common fax', 'common fax', 'common fax',
         'fellow', NULL)""",
    "CREATE INDEX tx_vl2 ON {s}.tx USING inverted (id, emb ivf (metric = 'l2'))",
    "CREATE INDEX tx_vnone ON {s}.tx USING inverted (id, emb ivf (metric = 'l2', quant = 'none'))",
    "CREATE INDEX tx_vcos ON {s}.tx USING inverted (id, emb hnsw (metric = 'cosine'))",
    "CREATE INDEX tx_vip ON {s}.tx USING inverted (id, emb hnsw (metric = 'ip'))",
    "CREATE INDEX tx_vl1 ON {s}.tx USING inverted (id, emb ivf (metric = 'l1'))",
]

TEXT_SHAPES = {
    "term": "body @@ 'fox'",
    "or": "body @@ 'fox dog'",
    "and": "body @@ 'fox' AND body @@ 'quick'",
    "and not": "body @@ 'fox' AND NOT body @@ 'lazy'",
    "nested and or": "(body @@ 'fox' OR body @@ 'dog') AND body @@ 'quick'",
    "max": "(body @@ 'fox' OR body @@ 'quick')::merge('max')",
    "max inside sum": "((body @@ 'fox' OR body @@ 'dog')::merge('max') OR body @@ 'quick')",
    "boosted term": "body @@ (ts_phrase('fox') ^ 2.0)",
    "boosted group": "(body @@ 'fox' OR body @@ 'dog')::boost(3)",
    "constant branch": "body @@ 'fox' OR (body @@ 'dog')::score('constant(5)')",
    "tfidf branch": "body @@ 'fox' OR (body @@ 'dog')::score('tfidf()')",
    "unscored branch": "body @@ 'fox' OR (body @@ 'dog')::score(NULL)",
    "phrase": "body @@ ts_phrase('quick brown')",
    "sloppy phrase": "body @@ ts_phrase('quick fox', slop := 1)",
    "phrase with a gap": "body @@ ts_phrase('quick', 1, 'fox')",
    "phrase with a prefix": "body @@ to_tsquery('\"quick br*\"')",
    "unscored phrase": "body @@ 'clever' OR (body @@ ts_phrase('lazy dog'))::score(NULL)",
    "boosted phrase": "body @@ (ts_phrase('lazy dog') ^ 3.0)",
    "prefix": "body @@ ts_starts_with('d')",
    "scored prefix": "(body @@ ts_starts_with('d'))::score('bm25()')",
    "like": "body @@ ts_like('%o%n')",
    "regexp": "body @@ ts_regexp('d.g')",
    "text range": "body @@ ts_between('a', 'c', true, true)",
    "fuzzy": "body @@ ts_levenshtein('quik', 1)",
    "fuzzy expanding": "body @@ ts_levenshtein('fox', 1)",
    "fuzzy under or": "body @@ 'quick' OR body @@ ts_levenshtein('fog', 1)",
    "prefix adding its terms up": "(body @@ ts_starts_with('d'))::merge('sum')",
    "scored prefix under or": "body @@ 'lazy' OR (body @@ ts_starts_with('d'))::score('bm25()')",
    "any with min_match": "body @@ ts_any(['fox', 'dog', 'quick'], 2)",
    "all": "body @@ ts_all(['quick', 'fox'])",
    "numeric range": "body @@ 'fox' AND n BETWEEN 1 AND 5",
    "keyword": "tag @@ 'animal'",
    "two columns": "body @@ 'fox' OR tag @@ 'story'",
    "fields of the index": "tx_idx.tableoid @@ to_tsquery('body:fox OR tag:story')",
    "term in every row of a segment": "body @@ 'common' OR body @@ 'fox'",
    "ngram": "g @@ ts_ngram('hello', 0.5)",
    "column without positions": "a @@ 'fox'",
    "column without frequency": "b @@ 'fox'",
    "column without norms": "c @@ 'fox'",
}

TOP_SHAPES = {
    "term": ("body @@ 'fox'", 2),
    "or": ("body @@ 'fox dog quick'", 2),
    "phrase": ("body @@ ts_phrase('quick brown')", 1),
    "max": ("(body @@ 'fox' OR body @@ 'quick')::merge('max')", 2),
    "and": ("body @@ 'fox' AND body @@ 'quick'", 1),
    "unscored branch": ("body @@ 'fox' OR (body @@ 'lazy')::score(NULL)", 3),
}

SCORERS = [
    "BM25(tx_idx.tableoid)",
    "BM25(tx_idx.tableoid, 1.2, 0)",
    "BM25(tx_idx.tableoid, 0, 0.75)",
    "BM25(tx_idx.tableoid, 2, 1)",
    "TFIDF(tx_idx.tableoid)",
    "TFIDF(tx_idx.tableoid, true)",
    "lm_jm(tx_idx.tableoid)",
    "lm_jm(tx_idx.tableoid, 0.5)",
    "lm_dirichlet(tx_idx.tableoid)",
    "lm_dirichlet(tx_idx.tableoid, 100)",
    "indri_dirichlet(tx_idx.tableoid)",
    "dfi(tx_idx.tableoid)",
    "dfi(tx_idx.tableoid, 'saturated')",
    "dfi(tx_idx.tableoid, 'chi_squared')",
    "raw_tf(tx_idx.tableoid)",
    "raw_dl(tx_idx.tableoid)",
    "raw_boost(tx_idx.tableoid)",
    "idf(tx_idx.tableoid)",
    "constant(tx_idx.tableoid, 2.5)",
]

SCORER_QUERIES = {
    "term": "body @@ 'fox'",
    "or": "body @@ 'fox dog'",
    "max": "(body @@ 'fox' OR body @@ 'quick')::merge('max')",
    "phrase": "body @@ ts_phrase('quick brown')",
    "sloppy phrase": "body @@ ts_phrase('quick fox', slop := 1)",
    "fuzzy": "body @@ ts_levenshtein('fox', 1)",
    "column without frequency": "b @@ 'fox'",
    "column without positions": "a @@ 'fox'",
    "ngram": "g @@ ts_ngram('hello', 0.5)",
    "one ngram in the column": "g @@ ts_ngram('hezz', 0.3)",
    "phrase under or": "body @@ ts_phrase('quick brown') OR body @@ 'fox'",
    "phrase under max": "(body @@ ts_phrase('quick brown') OR body @@ 'fox')::merge('max')",
    "sloppy phrase under or": "body @@ ts_phrase('quick fox', slop := 1) OR body @@ 'dog'",
    "sloppy phrase under max": "(body @@ ts_phrase('quick fox', slop := 1) OR body @@ 'dog')::merge('max')",
    "ngram under or": "g @@ ts_ngram('hello', 0.5) OR body @@ 'fox'",
    "ngram under max": "(g @@ ts_ngram('hello', 0.5) OR body @@ 'fox')::merge('max')",
    "one ngram under or": "g @@ ts_ngram('hezz', 0.3) OR body @@ 'fox'",
    "one ngram under max": "(g @@ ts_ngram('hezz', 0.3) OR body @@ 'fox')::merge('max')",
}

QUERY = "[1, 0.2]::FLOAT[2]"

VECTOR_SHAPES = {
    "ivf knn": ("tx_vl2", f"emb <-> {QUERY}", "ASC", "LIMIT 3", "", None),
    "ivf knn without quantization": ("tx_vnone", f"emb <-> {QUERY}", "ASC", "LIMIT 3", "", None),
    "ivf knn with nprobe": ("tx_vl2", f"emb <-> {QUERY}", "ASC", "LIMIT 3", "",
                            "sdb_ivf_search_nprobe = 2"),
    "ivf squared l2": ("tx_vl2", f"l2_sqr_distance(emb, {QUERY})", "ASC", "LIMIT 3", "", None),
    "ivf radius": ("tx_vl2", f"emb <-> {QUERY}", "ASC", "", f"WHERE emb <-> {QUERY} < 1.5", None),
    "ivf inclusive radius": ("tx_vl2", f"emb <-> {QUERY}", "ASC", "",
                             f"WHERE emb <-> {QUERY} <= 1.5", None),
    "forced exact": ("tx_vl2", f"emb <-> {QUERY}", "ASC", "LIMIT 3", "",
                     "sdb_ann_force_exact = true"),
    "distances without a limit": ("tx_vl2", f"emb <-> {QUERY}", "ASC", "", "", None),
    "l2 norm": ("tx_vl2", "l2_norm(emb)", "ASC", "", "", None),
    "hnsw cosine distance": ("tx_vcos", f"emb <=> {QUERY}", "ASC", "LIMIT 3", "", None),
    "hnsw cosine similarity": ("tx_vcos", f"cosine_similarity(emb, {QUERY})", "DESC", "LIMIT 3",
                               "", None),
    "hnsw inner product": ("tx_vip", f"inner_product(emb, {QUERY})", "DESC", "LIMIT 3", "", None),
    "hnsw negative inner product": ("tx_vip", f"emb <#> {QUERY}", "ASC", "LIMIT 3", "", None),
    "ivf l1": ("tx_vl1", f"emb <+> {QUERY}", "ASC", "LIMIT 3", "", None),
    "l1 norm": ("tx_vl1", "l1_norm(emb)", "ASC", "", "", None),
    "knn with a table filter": ("tx_vl2", f"emb <-> {QUERY}", "ASC", "LIMIT 3",
                                "WHERE body LIKE '%o%'", None),
}

FUNCTIONS = {"log": math.log, "log2": math.log2, "sqrt": math.sqrt, "max": max, "abs": abs}


@pytest.fixture(scope="module")
def schema() -> str:
    return schema_name(DRIVER_KEY)


@pytest.fixture(scope="module")
def conn(schema: str) -> psycopg.Connection:
    c = psycopg.connect(**conn_kwargs(), autocommit=True)
    with c.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        for ddl in DDL:
            cur.execute(ddl.format(s=schema))
    yield c
    with c.cursor() as cur:
        cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    c.close()


def _close(a, b, rel=3e-4, absolute=2e-6) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return abs(a - b) <= max(absolute, rel * max(abs(a), abs(b)))


def _check_tree(node: dict, scope: dict, path: str, errors: list[str]) -> None:
    value = node.get("value")
    inputs = node.get("inputs") or {}
    scope = {**scope, **{k: v["value"] for k, v in inputs.items()
                         if isinstance(v.get("value"), (int, float))}}
    expression = node.get("expression")
    if "clauses" in node:
        values = [c["value"] for c in node["clauses"] if c.get("scored", True)]
        if "constant" in inputs:
            values.append(inputs["constant"]["value"])
        expected = max([0, *values]) if expression == "max" else sum(values)
        if expression not in ("sum", "max") or not _close(value, expected, 1e-4, 1e-5):
            errors.append(f"{path}: {expression} gives {expected}, value {value}")
        for i, clause in enumerate(node["clauses"]):
            _check_tree(clause, {}, f"{path}.clauses[{i}]", errors)
    elif "parts" in node:
        expected = sum(part["value"] for part in node["parts"])
        if expression != "sum" or not _close(value, expected):
            errors.append(f"{path}: parts add up to {expected}, value {value}")
        for i, part in enumerate(node["parts"]):
            _check_tree(part, scope, f"{path}.parts[{i}]", errors)
    elif expression and "[i]" not in expression:
        names = {("lambda_" if k == "lambda" else k): v for k, v in scope.items()}
        names.setdefault("boost", 1.0)
        names.setdefault("scale", 1.0)
        code = expression.replace("^", "**").replace("lambda", "lambda_")
        expected = eval(code, {"__builtins__": {}}, {**FUNCTIONS, **names})
        if not _close(value, expected):
            errors.append(f"{path}: {expression} gives {expected}, value {value}")
    for key, child in inputs.items():
        _check_tree(child, scope, f"{path}.{key}", errors)


def _values(node: dict, out: list) -> list:
    out.append(node.get("value"))
    for child in node.get("clauses", []) + node.get("parts", []):
        _values(child, out)
    for _, child in sorted((node.get("inputs") or {}).items()):
        _values(child, out)
    return out


def _lucene_values(node: dict, out: list, path: str, errors: list[str]) -> list:
    if set(node) != {"value", "description", "details"}:
        errors.append(f"{path}: Lucene node has {sorted(node)}")
        return out
    out.append(node["value"])
    for i, child in enumerate(node["details"]):
        _lucene_values(child, out, f"{path}.details[{i}]", errors)
    return out


def _sorted_values(values: list) -> list[str]:
    return sorted("null" if v is None else f"{v:.6g}" for v in values)


def _explain(conn: psycopg.Connection, schema: str, index: str, score: str, where: str,
             order: str, setting: str | None = None) -> None:
    sql = (f"SELECT id, {score} AS score, ts_explain({index}.tableoid) AS why, "
           f"ts_explain_json(ts_explain({index}.tableoid)) AS es "
           f"FROM {schema}.{index} {where} {order}")
    with conn.cursor() as cur:
        if setting:
            cur.execute(f"SET {setting}")
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        finally:
            if setting:
                cur.execute(f"RESET {setting.split()[0]}")
    assert rows, sql
    errors: list[str] = []
    for row_id, score_value, why, es in rows:
        path = f"row {row_id}"
        if not _close(None if score_value is None else float(score_value), why.get("value"),
                      1e-6, 1e-7):
            errors.append(f"{path}: score {score_value}, value {why.get('value')}")
        _check_tree(why, {}, path, errors)
        if _sorted_values(_values(why, [])) != _sorted_values(
                _lucene_values(es, [], path, errors)):
            errors.append(f"{path}: ts_explain_json values differ")
    assert not errors, errors


@pytest.mark.parametrize("where", list(TEXT_SHAPES.values()), ids=list(TEXT_SHAPES))
def test_text_shape(conn, schema, where):
    _explain(conn, schema, "tx_idx", "BM25(tx_idx.tableoid)", f"WHERE {where}",
             "ORDER BY score DESC, id")


@pytest.mark.parametrize("where,limit", list(TOP_SHAPES.values()), ids=list(TOP_SHAPES))
def test_top_k_shape(conn, schema, where, limit):
    _explain(conn, schema, "tx_idx", "BM25(tx_idx.tableoid)", f"WHERE {where}",
             f"ORDER BY score DESC LIMIT {limit}")


@pytest.mark.parametrize("where", list(SCORER_QUERIES.values()), ids=list(SCORER_QUERIES))
@pytest.mark.parametrize("scorer", SCORERS)
def test_scorer(conn, schema, scorer, where):
    _explain(conn, schema, "tx_idx", scorer, f"WHERE {where}", "ORDER BY score DESC, id")


@pytest.mark.parametrize("index,distance,direction,limit,where,setting",
                         list(VECTOR_SHAPES.values()), ids=list(VECTOR_SHAPES))
def test_vector_shape(conn, schema, index, distance, direction, limit, where, setting):
    _explain(conn, schema, index, distance, where, f"ORDER BY {distance} {direction} {limit}",
             setting)
