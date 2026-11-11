import re

from sqllogic_gen import Test, lit

t = Test("Every deferred wildcard and regexp shape against the same predicate "
         "evaluated on the table itself: mismatches is always 0.", seed=11)
rng = t.rng

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
    if kind == 7:
        cased = "".join(rng.choice(CASED) for _ in range(rng.randrange(2, 8)))
        return re.sub("\n+", "\n", cased)
    return "".join(rng.choice(ALPHA) for _ in range(rng.randrange(6, 14)))


CASED = ["a", "A", "b", "B", "c", "C", "i", "I", "ı", "k", "K", "K",
         "s", "S", "ß", "ẞ", "σ", "Σ", "é", "É", "x", "\n"]


ROWS = 700
values = [word() for _ in range(ROWS)]
sentences = [" ".join(word() for _ in range(rng.randrange(1, 6))) for _ in range(ROWS)]
lists = [[word() for _ in range(rng.randrange(0, 4))] for _ in range(ROWS)]

LIKES = ["user-%", "%-abc", "%abc%", "abc", "a_c", "a_c%", "%a_c", "user-_-%",
         "user-__-%", "%/v_/%", "/api/%/abc", "admin\\_%", "%\\%", "%\\_x",
         "x_%z", "%z", "%", "_", "__", "___", "a%c", "a%b%c", "%9%", "%0_",
         "%ab", "ab%", "abcabc", "%cabc%", "%9\\%z", "xyz%", "%a%a%",
         "%i%", "%I%", "%k%", "%K%", "%ß%", "%ss%", "%Σ%",
         "%é%", "AB%", "%C", "%A_C%", "%\n%", "_\n%"]
REGEXPS = [".*abc.*", "user-[0-9]+-.*", "user-[0-9]{3}-[a-z]{3}", "(abc|cba)",
           "a.c", "[a-c]{3}", "/api/v[0-2]/(users|items)", "admin_[0-9]+",
           "x[yz]*[0-9].?z", ".*[%_].*", "[0-9]+", ".*9.*", "(ab)+c?", "a.*c"]
PARTIAL_REGEXPS = ["abc", "^user-", "abc$", "^a.c$", "[0-9]{2}", "(ab|AB)c",
                   "ß", "σ", "k", "i", "^AB"]


def ok_escape(p):
    return "\\" in p


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
        preds.append((f"{column} NOT LIKE '{q}'{esc}",
                      f"{{v}} NOT LIKE '{q}'{esc}"))
        truth_ilike = f"{{v}} ILIKE '{q}'{esc}"
        preds.append((f"{column} ILIKE '{q}'{esc}", truth_ilike))
        preds.append((f"{column} NOT ILIKE '{q}'{esc}",
                      f"{{v}} NOT ILIKE '{q}'{esc}"))
        hashed = q.replace("\\", "#")
        if ok_escape(p) and "#" not in p:
            preds.append((f"{column} ILIKE '{hashed}' ESCAPE '#'", truth_ilike))
    for r in REGEXPS:
        q = lit(r)
        preds.append((f"{column} @@ ts_regexp('{q}')",
                      f"regexp_full_match({{v}}, '{q}')"))
        if not sql_like:
            continue
        preds.append((f"{column} SIMILAR TO '{q}'",
                      f"{{v}} SIMILAR TO '{q}'"))
        preds.append((f"regexp_full_match({column}, '{q}', 'i')",
                      f"regexp_full_match({{v}}, '{q}', 'i')"))
    if sql_like:
        for r in PARTIAL_REGEXPS:
            q = lit(r)
            truth = f"regexp_matches({{v}}, '{q}')"
            truth_i = f"regexp_matches({{v}}, '{q}', 'i')"
            preds.append((f"{column} ~ '{q}'", truth))
            preds.append((f"{column} ~* '{q}'", truth_i))
            preds.append((f"{column} !~* '{q}'", f"NOT {truth_i}"))
            preds.append((f"regexp_matches({column}, '{q}', 'ci')", truth_i))
        for s in ["abc", "user-", "AB", "ß", "%", "_x", ""]:
            q = lit(s)
            for fn in ("starts_with", "prefix", "ends_with", "suffix",
                       "contains"):
                preds.append((f"{fn}({column}, '{q}')", f"{fn}({{v}}, '{q}')"))
    return [(idx, scope(truth)) for idx, truth in preds]


def whole(truth):
    return truth.replace("{v}", "msg")


def any_word(truth):
    word = truth.replace("{v}", "w")
    return f"len(list_filter(string_split(msg, ' '), lambda w: {word})) > 0"


def any_element(truth):
    element = truth.replace("{v}", "w")
    return f"len(list_filter(tags, lambda w: {element})) > 0"


def tags(i):
    return "[" + ", ".join(f"'{lit(tag)}'" for tag in lists[i]) + "]::VARCHAR[]"


DICTS = {
    "wdm_kw3": "generate_wildcard_ngrams(keyword(), 3)",
    "wdm_kw2": "generate_wildcard_ngrams(keyword(), 2)",
    "wdm_kw4": "generate_wildcard_ngrams(keyword(), 4)",
    "wdm_kw3_pos": "generate_wildcard_ngrams(keyword(), 3) WITH (frequency, position)",
    "wdm_words": "generate_wildcard_ngrams(split_text_csv(' '), 3)",
    "wdm_words_pos": "generate_wildcard_ngrams(split_text_csv(' '), 3) WITH (frequency, position)",
}
for name, template in DICTS.items():
    t.dictionary(name, template)

t.table("wdm", "id INTEGER PRIMARY KEY, x INTEGER, msg VARCHAR, tags VARCHAR[]")
t.insert("wdm", (f"({i}, {i % 50}, '{lit(values[i])}', {tags(i)})"
                 for i in range(ROWS)))
for name in ("wdm_kw3", "wdm_kw2", "wdm_kw4", "wdm_kw3_pos"):
    t.index(f"{name}_idx", "wdm", f"id, msg {name}", include="x")
t.index("wdm_tags_idx", "wdm", "id, tags wdm_kw3", include="x")

t.table("wdm_w", "id INTEGER PRIMARY KEY, x INTEGER, msg VARCHAR")
t.insert("wdm_w", (f"({i}, {i % 50}, '{lit(sentences[i])}')"
                   for i in range(ROWS)))
for name in ("wdm_words", "wdm_words_pos"):
    t.index(f"{name}_idx", "wdm_w", f"id, msg {name}", include="x")

t.table("wdm_s", "id INTEGER, x INTEGER, msg VARCHAR")
t.index("wdm_s_idx", "wdm_s", "id, msg wdm_kw3")
t.statement("INSERT INTO wdm_s SELECT id, x, msg FROM wdm")

CONTEXTS = [
    ("", ""),
    (" AND id % 3 = 0", " AND id % 3 = 0"),
    (" AND x > 20", " AND x > 20"),
]


def run(idx, table, column, scope, contexts=CONTEXTS, sql_like=True):
    for pred, truth in predicates(column, scope, sql_like):
        for ctx_index, ctx_truth in contexts:
            t.same_rows(f"SELECT id FROM {idx} WHERE {pred}{ctx_index}",
                        f"SELECT id FROM {table} WHERE {truth}{ctx_truth}")


for name in ("wdm_kw3", "wdm_kw2", "wdm_kw4", "wdm_kw3_pos"):
    t.part()
    t.section(f"Whole values, {DICTS[name]}.")
    run(f"{name}_idx", "wdm", "msg", whole)
    t.section(f"Whole values, {DICTS[name]}, under OR and NOT (checked inline).")
    for pred, truth in predicates("msg", whole)[::3]:
        t.same_rows(f"SELECT id FROM {name}_idx WHERE {pred} OR id = 7",
                    f"SELECT id FROM wdm WHERE {truth} OR id = 7")
        t.same_rows(f"SELECT id FROM {name}_idx WHERE NOT ({pred})",
                    f"SELECT id FROM wdm WHERE NOT ({truth})")
    t.section(f"Whole values, {DICTS[name]}, two patterns.")
    preds = predicates("msg", whole)
    for (p1, t1), (p2, t2) in zip(preds[::4], preds[1::4]):
        t.same_rows(f"SELECT id FROM {name}_idx WHERE {p1} AND {p2}",
                    f"SELECT id FROM wdm WHERE {t1} AND {t2}")

for name in ("wdm_words", "wdm_words_pos"):
    t.part()
    t.section(f"Words, {DICTS[name]}: a row matches when one of its words does.")
    run(f"{name}_idx", "wdm_w", "msg", any_word, sql_like=False)
    t.section(f"Words, {DICTS[name]}: the SQL forms other than LIKE match the whole value.")
    for pred, truth in predicates("msg", whole):
        if "@@" in pred or "LIKE '" in pred.replace("ILIKE '", ""):
            continue
        t.same_rows(f"SELECT id FROM {name}_idx WHERE {pred}",
                    f"SELECT id FROM wdm_w WHERE {truth}")

t.part()
t.section("VARCHAR[]: a row matches when one of its elements does.")
run("wdm_tags_idx", "wdm", "tags", any_element, CONTEXTS[:2], sql_like=False)

t.part()
t.section("Search table.")
run("wdm_s_idx", "wdm", "msg", whole, CONTEXTS[:1])

t.write()
