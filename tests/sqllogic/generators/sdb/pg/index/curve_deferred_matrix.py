import json

from sqllogic_gen import Test, lit

t = Test("A box next to conditions the index answers in two steps: phrases "
         "without positions, wildcards over n-grams and shapes checked against "
         "the column. The rows agree with an index that answers them directly "
         "and with the table: mismatches is always 0.", seed=1743)
rng = t.rng

ROWS = 900
BOXES = 3
VOCAB = ["quick", "quack", "brown", "brawn", "fox", "box", "the", "dog", "red"]
WORDS = ["abc", "xabcy", "abd", "cab", "aXc", "abcabc", "zzz", "a_c", "100%"]
CALLS = {
    "cdf_morton": ("curve(ROW(x, y))", "body, msg"),
    "cdf_stored": ("curve(ROW(x, y))", "body, msg, x, y"),
    "cdf_coarse": ("curve(ROW(x, y), max_level := 3)", "body"),
    "cdf_hilbert": ("curve(ROW(x, y), kind := 'hilbert')", "body"),
}
PHRASES = [
    "to_tsquery('\"quick brown\"')",
    "to_tsquery('\"quick brown\"~1')",
    "to_tsquery('\"qu* brown\"')",
    "to_tsquery('\"the qu* fox\"~2')",
    "ts_phrase('brown fox', slop := 2)",
    "ts_phrase('quick', 1, 'fox')",
    "(ts_phrase('quick') ## ts_like('br%n'))",
    "(ts_phrase('the') ## ts_like('%') ## ts_phrase('fox'))",
]
WILDCARDS = [
    ("msg LIKE '%bc%'", "msg LIKE '%bc%'"),
    ("msg LIKE 'a_c%'", "msg LIKE 'a_c%'"),
    ("msg LIKE '%abc'", "msg LIKE '%abc'"),
    ("msg @@ ts_like('%ab%c%')", "msg LIKE '%ab%c%'"),
    ("msg @@ ts_regexp('.*b.*')", "regexp_full_match(msg, '.*b.*')"),
    ("msg @@ ts_regexp('(abc)+')", "regexp_full_match(msg, '(abc)+')"),
]
SHAPES = [
    {"type": "Polygon", "coordinates": [[[0, 0], [30, 0], [30, 30], [0, 30], [0, 0]]]},
    {"type": "Polygon", "coordinates": [[[10, 10], [50, 15], [45, 55], [12, 40], [10, 10]]]},
    {"type": "MultiPolygon", "coordinates": [[[[0, 0], [9, 0], [9, 9], [0, 9], [0, 0]]],
                                             [[[20, 20], [60, 20], [60, 60], [20, 60], [20, 20]]]]},
]


def shape_of(pattern):
    return f"'{lit(json.dumps(pattern, separators=(',', ':')))}'"


def setup():
    t.dictionary("cdf_words", "split_text(case := 'lower') WITH (frequency)")
    t.dictionary("cdf_words_pos", "split_text(case := 'lower') WITH (frequency, position)")
    t.dictionary("cdf_kw3", "generate_wildcard_ngrams(keyword(), 3)")
    t.dictionary("cdf_shape", "encode_geojson()")
    t.table("cdf", "id INTEGER PRIMARY KEY, x BIGINT, y BIGINT, tag INTEGER, "
            "body VARCHAR, msg VARCHAR, geo JSON")
    rows = []
    for i in range(ROWS):
        words = " ".join(rng.choice(VOCAB) for _ in range(rng.randrange(1, 9)))
        point = {"type": "Point", "coordinates": [rng.uniform(0, 60), rng.uniform(0, 60)]}
        x = "NULL" if i % 71 == 0 else rng.randrange(32)
        rows.append(f"({i}, {x}, {rng.randrange(32)}, {i % 5}, '{words}', "
                    f"'{lit(rng.choice(WORDS))}', {shape_of(point)})")
    t.insert("cdf", rows, chunk=300)
    t.index("cdf_truth", "cdf", "id, tag, body cdf_words_pos, msg cdf_kw3, geo cdf_shape")


def bounds():
    lx, ly = rng.randrange(32), rng.randrange(32)
    return lx, lx + rng.choice([0, 3, 12, 31]), ly, ly + rng.choice([0, 3, 12, 31])


def check(index, call, condition, truth, table):
    for extra in ("", " AND tag = 2", " AND id % 3 = 0"):
        for _ in range(BOXES):
            lx, hx, ly, hy = bounds()
            box = f"{call} @@ ts_between(ROW({lx}, {ly}), ROW({hx}, {hy}), true, true)"
            ranges = f"x BETWEEN {lx} AND {hx} AND y BETWEEN {ly} AND {hy}"
            expected = f"SELECT id FROM {table} WHERE {truth} AND {ranges}{extra}"
            t.same_rows(f"SELECT id FROM {index} WHERE {condition} AND {box}{extra}", expected)
            t.same_rows(f"SELECT id FROM {index} WHERE {condition} AND {ranges}{extra}", expected)


for index, (call, include) in CALLS.items():
    t.part()
    setup()
    t.index(index, "cdf", f"id, tag, body cdf_words, msg cdf_kw3, geo cdf_shape, ({call})",
            include=include)
    t.refresh("cdf")
    t.section(f"Phrases without positions, {call}.")
    for phrase in PHRASES:
        check(index, call, f"body @@ {phrase}", f"body @@ {phrase}", "cdf_truth")
    t.section("Wildcards over n-grams.")
    for condition, truth in WILDCARDS:
        check(index, call, condition, truth, "cdf")
    t.section("Shapes checked against the column.")
    for pattern in SHAPES:
        predicate = f"ST_Intersects(geo, {shape_of(pattern)})"
        check(index, call, predicate, f"({predicate} OR id < 0)", "cdf_truth")

t.write()
