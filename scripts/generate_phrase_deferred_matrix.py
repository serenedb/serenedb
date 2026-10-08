import itertools
import sys

VOCAB = ["quick", "quack", "quiet", "brown", "brawn", "brow", "fox", "box",
         "fix", "the", "a", "dog", "dot", "red", "car", "auto", "automobile",
         "big"]

out = []


def emit(*lines):
    out.extend(lines)


def stmt(sql):
    emit("statement ok", sql, "")


def lit(q):
    return q.replace("'", "''")


def lucene(parts, slop):
    body = " ".join(parts)
    return f'"{body}"' + (f"~{slop}" if slop else "")


def tsq(parts, slop):
    return f"to_tsquery('{lit(lucene(parts, slop))}')"


def check(text_idx, pos_idx, query, extra=""):
    t = f"SELECT id FROM {text_idx} WHERE body @@ {query}{extra}"
    p = f"SELECT id FROM {pos_idx} WHERE body @@ {query}{extra}"
    emit("query",
         f"SELECT (SELECT count(*) FROM ({p})) AS matches, "
         f"(SELECT count(*) FROM (({t} EXCEPT {p}) UNION ALL ({p} EXCEPT {t}))) "
         f"AS mismatches",
         "----", "")


def check_scores(text_idx, pos_idx, query):
    t = (f"SELECT id, round(BM25({text_idx}.tableoid)::numeric, 5) AS s "
         f"FROM {text_idx} WHERE body @@ {query}")
    p = (f"SELECT id, round(BM25({pos_idx}.tableoid)::numeric, 5) AS s "
         f"FROM {pos_idx} WHERE body @@ {query}")
    emit("query",
         f"SELECT count(*) AS mismatches FROM ({t}) a FULL JOIN ({p}) b USING (id) "
         f"WHERE a.s IS DISTINCT FROM b.s",
         "----", "")


FIRST = ["quick", "qu*", "q?ick", "quik~1", "the", "t*e"]
SECOND = ["brown", "br*", "b*n", "brwn~1", "fox", "fo*", "f?x", "car", "au*"]
SLOPS = [0, 1, 2, 3]
SLOT_KINDS = {
    "quick": ["quick", "qu*", "q?ick", "quik~1"],
    "brown": ["brown", "br*", "b*n", "brwn~1"],
    "fox": ["fox", "fo*", "f?x", "fx~1"],
}
OVERLAP = [["the", "the"], ["qu*", "qu*"], ["qu*", "quick"], ["quick", "qu*"],
           ["b*", "br*"], ["the", "t*e"], ["t*", "the", "t*"],
           ["qu*", "q?ick", "quick"], ["a", "*"], ["br*", "b*n", "brawn"]]
CHAINS = [
    "ts_phrase('quick') ## ts_like('br%n')",
    "ts_phrase('quick') ## ts_starts_with('br')",
    "ts_phrase('quick') ## ts_regexp('br.*')",
    "ts_phrase('quick') ## ts_levenshtein('brwn', 1)",
    "ts_phrase('quick') ## ts_between('b', 'c', true, false)",
    "ts_starts_with('qu') ## ts_phrase('brown fox')",
    "ts_like('%ck') ## ts_starts_with('br') ## ts_like('f%')",
    "ts_regexp('qu.*') ## ts_regexp('b.*n')",
    "ts_phrase('the') ## ts_like('%') ## ts_phrase('fox')",
    "ts_starts_with('a') ## ts_starts_with('a')",
    "ts_phrase('red') ## ts_starts_with('au')",
    "ts_phrase('big') ## ts_like('%o%')",
]
GAPS = [
    "ts_phrase('quick', 1, 'fox')",
    "ts_phrase('quick', [1, 3], 'fox')",
    "ts_phrase('the', [0, 2], 'dog')",
    "ts_phrase('quick', 1, 'fox', slop := 1)",
    "ts_phrase('brown fox', slop := 2)",
    "ts_phrase('fox brown quick', slop := 3)",
]
CONTEXTS = [
    ("", ""),
    (" AND body @@ 'the'", "and_term"),
    (" AND id % 3 = 0", "and_column"),
]


def grid(text_idx, pos_idx, fuzzy_only=False):
    queries = []
    for a, b in itertools.product(FIRST, SECOND):
        for slop in SLOPS:
            queries.append(tsq([a, b], slop))
    for kinds in itertools.product(*SLOT_KINDS.values()):
        for slop in (0, 2):
            queries.append(tsq(list(kinds), slop))
    for parts in OVERLAP:
        for slop in SLOPS:
            queries.append(tsq(parts, slop))
    if fuzzy_only:
        queries = [q for q in queries if "~1" in q]
    else:
        queries += [f"({c})" for c in CHAINS] + GAPS
    return queries


emit("# Generated: every deferred phrase shape checked against a positional index.",
     "# For each query the index without positions (deferred check) must return",
     "# exactly the rows of the index with positions: mismatches is always 0.",
     "")
stmt("CREATE TEXT SEARCH DICTIONARY pdm_words AS split_text(case := 'lower') WITH (frequency)")
stmt("CREATE TEXT SEARCH DICTIONARY pdm_words_pos AS split_text(case := 'lower') WITH (frequency, position)")
stmt("CREATE TEXT SEARCH DICTIONARY pdm_syn AS\n    split_text(case := 'lower') | expand_solr_synonyms('car, automobile, auto')\n    WITH (frequency)")
stmt("CREATE TEXT SEARCH DICTIONARY pdm_syn_pos AS\n    split_text(case := 'lower') | expand_solr_synonyms('car, automobile, auto')\n    WITH (frequency, position)")
vocab = ", ".join(f"'{w}'" for w in VOCAB)
stmt("CREATE TABLE pdm (id INTEGER PRIMARY KEY, body VARCHAR)")
stmt("INSERT INTO pdm SELECT i, (SELECT string_agg("
     f"[{vocab}][1 + (hash(i, j) % {len(VOCAB)})::INTEGER], ' ') "
     "FROM range(1 + (hash(i) % 12)::INTEGER) r(j)) FROM range(600) t(i)")
stmt("CREATE INDEX pdm_text ON pdm USING inverted (id, body pdm_words) INCLUDE (body)")
stmt("CREATE INDEX pdm_pos ON pdm USING inverted (id, body pdm_words_pos)")
stmt("CREATE INDEX pdm_syn_text ON pdm USING inverted (id, body pdm_syn) INCLUDE (body)")
stmt("CREATE INDEX pdm_syn_pos ON pdm USING inverted (id, body pdm_syn_pos)")
stmt("VACUUM (REFRESH_TABLE) pdm")
stmt("CREATE TABLE pdm_s (id INTEGER, body VARCHAR) WITH (storage = 'search')")
stmt("INSERT INTO pdm_s SELECT id, body FROM pdm")
stmt("VACUUM (REFRESH_TABLE) pdm_s")
stmt("CREATE INDEX pdm_s_text ON pdm_s USING inverted (id, body pdm_words)")
stmt("CREATE INDEX pdm_s_pos ON pdm_s USING inverted (id, body pdm_words_pos)")
stmt("VACUUM (REFRESH_TABLE) pdm_s")

emit("# --- Dense dictionary, every shape, alone and next to other conditions.", "")
for q in grid("pdm_text", "pdm_pos"):
    for extra, _ in CONTEXTS:
        check("pdm_text", "pdm_pos", q, extra)
    check("pdm_text", "pdm_pos", f"({q} || 'dog')")
    check("pdm_text", "pdm_pos", f"({q})::score('constant(1)')")

emit("# --- Fuzzy parts deferred: with no Levenshtein term limit they defer too.", "")
stmt("SET sdb_levenshtein_max_terms = 0")
for q in grid("pdm_text", "pdm_pos", fuzzy_only=True):
    for extra, _ in CONTEXTS:
        check("pdm_text", "pdm_pos", q, extra)
stmt("RESET sdb_levenshtein_max_terms")

emit("# --- Ranked by BM25 (checked inline): same scores as positions. Fuzzy parts", "# are left out until the check weights them by edit distance like the index.", "")
for q in [q for q in grid("pdm_text", "pdm_pos") if "~1" not in q][::5]:
    check_scores("pdm_text", "pdm_pos", q)

emit("# --- Synonyms put several tokens at one position: slop with patterns stays inline.", "")
for q in grid("pdm_syn_text", "pdm_syn_pos"):
    check("pdm_syn_text", "pdm_syn_pos", q)

emit("# --- Search table keeps its text: same answers.", "")
for q in grid("pdm_s_text", "pdm_s_pos")[::3]:
    check("pdm_s_text", "pdm_s_pos", q)



EXPLAINS = [
    ("pdm_text", tsq(["qu*", "fox"], 2), ""),
    ("pdm_text", tsq(["q?ick", "b*n", "fo*"], 2), ""),
    ("pdm_text", tsq(["qu*", "fox"], 2), " AND body @@ 'the'"),
    ("pdm_text", f"({tsq(['qu*', 'fox'], 2)})::score('constant(1)')", ""),
    ("pdm_text", f"({tsq(['qu*', 'fox'], 2)} || 'dog')", ""),
    ("pdm_text", "(ts_phrase('quick') ## ts_between('b', 'c', true, false))", ""),
    ("pdm_syn_text", tsq(["red", "au*"], 2), ""),
    ("pdm_syn_text", "(ts_phrase('red') ## ts_starts_with('au'))", ""),
    ("pdm_s_text", tsq(["qu*", "fox"], 2), ""),
]
emit("# --- Where each shape is checked.", "")
for idx, q, extra in EXPLAINS:
    emit("query", f"EXPLAIN SELECT id FROM {idx} WHERE body @@ {q}{extra}", "----", "")
stmt("SET sdb_levenshtein_max_terms = 0")
emit("query", f"EXPLAIN SELECT id FROM pdm_text WHERE body @@ {tsq(['quik~1', 'fox'], 2)}", "----", "")
stmt("RESET sdb_levenshtein_max_terms")
sys.stdout.write("\n".join(out) + "\n")
