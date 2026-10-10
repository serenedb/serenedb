import random
import sys

rng = random.Random(11)
out = []


def emit(*lines):
    out.extend(lines)


def stmt(sql):
    emit("statement ok", sql, "")


def lit(text):
    return text.replace("'", "''")


ALPHA = "abcxyz019"
SPECIAL = ["%", "_", "\\", "-", "/", "."]


def word():
    kind = rng.randrange(8)
    if kind == 0:
        return f"user-{rng.randrange(1000)}-{''.join(rng.choice(ALPHA) for _ in range(3))}"
    if kind == 1:
        return f"admin_{rng.randrange(100)}{rng.choice(['%', '', '_x'])}"
    if kind == 2:
        return f"/api/v{rng.randrange(3)}/{rng.choice(['users', 'items', 'abc'])}"
    if kind == 3:
        return "".join(rng.choice(ALPHA) for _ in range(rng.randrange(1, 4)))
    if kind == 4:
        return "".join(rng.choice(ALPHA + "".join(SPECIAL)) for _ in range(rng.randrange(3, 10)))
    if kind == 5:
        return rng.choice(["abc", "abcabc", "aabbcc", "xyzabc", "cba", "ABC", "AbC"])
    if kind == 6:
        return f"{rng.choice(['x', 'xy', 'xyz'])}{rng.randrange(10)}{rng.choice(['%', '_', ''])}z"
    return "".join(rng.choice(ALPHA) for _ in range(rng.randrange(6, 14)))


ROWS = 700
values = [word() for _ in range(ROWS)]
sentences = [" ".join(word() for _ in range(rng.randrange(1, 6))) for _ in range(ROWS)]
lists = [[word() for _ in range(rng.randrange(0, 4))] for _ in range(ROWS)]

LIKES = ["user-%", "%-abc", "%abc%", "abc", "a_c", "a_c%", "%a_c", "user-_-%",
         "user-__-%", "%/v_/%", "/api/%/abc", "admin\\_%", "%\\%", "%\\_x",
         "x_%z", "%z", "%", "_", "__", "___", "a%c", "a%b%c", "%9%", "%0_",
         "%ab", "ab%", "abcabc", "%cabc%", "%9\\%z", "xyz%", "%a%a%"]
REGEXPS = [".*abc.*", "user-[0-9]+-.*", "user-[0-9]{3}-[a-z]{3}", "(abc|cba)",
           "a.c", "[a-c]{3}", "/api/v[0-2]/(users|items)", "admin_[0-9]+",
           "x[yz]*[0-9].?z", ".*[%_].*", "[0-9]+", ".*9.*", "(ab)+c?", "a.*c"]


def ok_escape(p):
    return "\\" in p


def check(index_sql, truth_sql):
    emit("query",
         f"SELECT (SELECT count(*) FROM ({truth_sql})) AS matches, "
         f"(SELECT count(*) FROM (({index_sql} EXCEPT {truth_sql}) UNION ALL "
         f"({truth_sql} EXCEPT {index_sql}))) AS mismatches",
         "----", "")


def predicates(column, scope, sql_like=True):
    preds = []
    for p in LIKES:
        q = lit(p)
        esc = " ESCAPE '\\'" if ok_escape(p) else ""
        truth_like = f"{{v}} LIKE '{q}'{esc}"
        preds.append((f"{column} @@ ts_like('{q}')", truth_like))
        if not sql_like:
            continue
        preds.append((f"{column} LIKE '{q}'{esc}", truth_like))
        if not ok_escape(p):
            preds.append((f"{column} ILIKE '{q}'", f"{{v}} ILIKE '{q}'"))
    for r in REGEXPS:
        q = lit(r)
        preds.append((f"{column} @@ ts_regexp('{q}')",
                      f"regexp_full_match({{v}}, '{q}')"))
    return [(idx, scope(truth)) for idx, truth in preds]


def whole(truth):
    return truth.replace("{v}", "msg")


def any_word(truth):
    return f"len(list_filter(string_split(msg, ' '), w -> {truth.replace("{v}", "w")})) > 0"


def any_element(truth):
    return f"len(list_filter(tags, w -> {truth.replace("{v}", "w")})) > 0"


emit("# Generated: every deferred wildcard and regexp shape against the same",
     "# predicate evaluated on the table itself: mismatches is always 0.", "")
DICTS = {
    "wdm_kw3": "generate_wildcard_ngrams(keyword(), 3)",
    "wdm_kw2": "generate_wildcard_ngrams(keyword(), 2)",
    "wdm_kw4": "generate_wildcard_ngrams(keyword(), 4)",
    "wdm_kw3_pos": "generate_wildcard_ngrams(keyword(), 3) WITH (frequency, position)",
    "wdm_words": "generate_wildcard_ngrams(split_text_csv(' '), 3)",
    "wdm_words_pos": "generate_wildcard_ngrams(split_text_csv(' '), 3) WITH (frequency, position)",
}
for name, template in DICTS.items():
    stmt(f"CREATE TEXT SEARCH DICTIONARY {name} AS {template}")

stmt("CREATE TABLE wdm (id INTEGER PRIMARY KEY, x INTEGER, msg VARCHAR, tags VARCHAR[])")
for start in range(0, ROWS, 100):
    rows = ",\n    ".join(
        f"({i}, {i % 50}, '{lit(values[i])}', "
        "[" + ", ".join(chr(39) + lit(t) + chr(39) for t in lists[i]) + "]::VARCHAR[])"
        for i in range(start, min(ROWS, start + 100)))
    stmt(f"INSERT INTO wdm VALUES\n    {rows}")
for name in ("wdm_kw3", "wdm_kw2", "wdm_kw4", "wdm_kw3_pos"):
    stmt(f"CREATE INDEX {name}_idx ON wdm USING inverted (id, msg {name}) INCLUDE (x)")
stmt("CREATE INDEX wdm_tags_idx ON wdm USING inverted (id, tags wdm_kw3) INCLUDE (x)")
stmt("VACUUM (REFRESH_TABLE) wdm")

stmt("CREATE TABLE wdm_w (id INTEGER PRIMARY KEY, x INTEGER, msg VARCHAR)")
for start in range(0, ROWS, 100):
    rows = ",\n    ".join(f"({i}, {i % 50}, '{lit(sentences[i])}')"
                           for i in range(start, min(ROWS, start + 100)))
    stmt(f"INSERT INTO wdm_w VALUES\n    {rows}")
for name in ("wdm_words", "wdm_words_pos"):
    stmt(f"CREATE INDEX {name}_idx ON wdm_w USING inverted (id, msg {name}) INCLUDE (x)")
stmt("VACUUM (REFRESH_TABLE) wdm_w")

stmt("CREATE TABLE wdm_s (id INTEGER, x INTEGER, msg VARCHAR) WITH (storage = 'search')")
stmt("CREATE INDEX wdm_s_idx ON wdm_s USING inverted (id, msg wdm_kw3)")
stmt("INSERT INTO wdm_s SELECT id, x, msg FROM wdm")
stmt("VACUUM (REFRESH_TABLE) wdm_s")

CONTEXTS = [
    ("", ""),
    (" AND id % 3 = 0", " AND id % 3 = 0"),
    (" AND x > 20", " AND x > 20"),
]


def run(idx, table, column, scope, contexts=CONTEXTS, sql_like=True):
    for pred, truth in predicates(column, scope, sql_like):
        for ctx_index, ctx_truth in contexts:
            check(f"SELECT id FROM {idx} WHERE {pred}{ctx_index}",
                  f"SELECT id FROM {table} WHERE {truth}{ctx_truth}")


for name in ("wdm_kw3", "wdm_kw2", "wdm_kw4", "wdm_kw3_pos"):
    emit(f"# --- Whole values, {DICTS[name]}.", "")
    run(f"{name}_idx", "wdm", "msg", whole)
    emit(f"# --- Whole values, {DICTS[name]}, under OR and NOT (checked inline).", "")
    for pred, truth in predicates("msg", whole)[::3]:
        check(f"SELECT id FROM {name}_idx WHERE {pred} OR id = 7",
              f"SELECT id FROM wdm WHERE {truth} OR id = 7")
        check(f"SELECT id FROM {name}_idx WHERE NOT ({pred})",
              f"SELECT id FROM wdm WHERE NOT ({truth})")
    emit(f"# --- Whole values, {DICTS[name]}, two patterns.", "")
    preds = predicates("msg", whole)
    for (p1, t1), (p2, t2) in zip(preds[::4], preds[1::4]):
        check(f"SELECT id FROM {name}_idx WHERE {p1} AND {p2}",
              f"SELECT id FROM wdm WHERE {t1} AND {t2}")

for name in ("wdm_words", "wdm_words_pos"):
    emit(f"# --- Words, {DICTS[name]}: a row matches when one of its words does.", "")
    run(f"{name}_idx", "wdm_w", "msg", any_word, sql_like=False)

emit("# --- VARCHAR[]: a row matches when one of its elements does.", "")
run("wdm_tags_idx", "wdm", "tags", any_element, CONTEXTS[:2], sql_like=False)

emit("# --- Search table.", "")
run("wdm_s_idx", "wdm", "msg", whole, CONTEXTS[:1])

emit("# --- Where each shape is checked.", "")
for sql in [
    "SELECT id FROM wdm_kw3_idx WHERE msg @@ ts_like('user-%-abc')",
    "SELECT id FROM wdm_kw3_idx WHERE msg @@ ts_like('%abc%')",
    "SELECT id FROM wdm_kw3_idx WHERE msg @@ ts_like('ab%')",
    "SELECT id FROM wdm_kw3_pos_idx WHERE msg @@ ts_like('%abc%')",
    "SELECT id FROM wdm_kw3_idx WHERE msg LIKE 'admin\\_%' ESCAPE '\\'",
    "SELECT id FROM wdm_kw3_idx WHERE msg @@ ts_regexp('user-[0-9]+-.*')",
    "SELECT id FROM wdm_kw3_idx WHERE msg @@ ts_like('user-%') AND x > 20",
    "SELECT id FROM wdm_kw3_idx WHERE msg @@ ts_like('user-%') OR id = 7",
    "SELECT id FROM wdm_words_idx WHERE msg @@ ts_like('user-%-abc')",
    "SELECT id FROM wdm_tags_idx WHERE tags @@ ts_like('user-%-abc')",
    "SELECT id FROM wdm_s_idx WHERE msg @@ ts_regexp('(abc|cba)')",
]:
    emit("query", f"EXPLAIN {sql}", "----", "")

sys.stdout.write("\n".join(out) + "\n")
