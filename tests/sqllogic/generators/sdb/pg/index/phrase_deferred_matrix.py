import itertools

from sqllogic_gen import Test, lit

t = Test("Every deferred phrase shape checked against a positional index. For "
         "each query the index without positions (deferred check) must return "
         "exactly the rows of the index with positions: mismatches is always 0.")

VOCAB = ["quick", "quack", "quiet", "brown", "brawn", "brow", "fox", "box",
         "fix", "the", "a", "dog", "dot", "red", "car", "auto", "automobile",
         "big"]


def lucene(parts, slop):
    body = " ".join(parts)
    return f'"{body}"' + (f"~{slop}" if slop else "")


def tsq(parts, slop):
    return f"to_tsquery('{lit(lucene(parts, slop))}')"


def check(text_idx, pos_idx, query, extra=""):
    t.same_rows(f"SELECT id FROM {text_idx} WHERE body @@ {query}{extra}",
                f"SELECT id FROM {pos_idx} WHERE body @@ {query}{extra}")


def check_scores(text_idx, pos_idx, query):
    t.same_scores(f"SELECT id, round(BM25({text_idx}.tableoid)::numeric, 5) "
                  f"AS s FROM {text_idx} WHERE body @@ {query}",
                  f"SELECT id, round(BM25({pos_idx}.tableoid)::numeric, 5) "
                  f"AS s FROM {pos_idx} WHERE body @@ {query}")


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
CONTEXTS = ["", " AND body @@ 'the'", " AND id % 3 = 0"]


def grid(fuzzy_only=False):
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
        return [q for q in queries if "~1" in q]
    return queries + [f"({c})" for c in CHAINS] + GAPS


SYNONYMS = ("split_text(case := 'lower') | "
            "expand_solr_synonyms('car, automobile, auto')")
t.dictionary("pdm_words", "split_text(case := 'lower') WITH (frequency)")
t.dictionary("pdm_words_pos",
             "split_text(case := 'lower') WITH (frequency, position)")
t.dictionary("pdm_syn", f"{SYNONYMS} WITH (frequency)")
t.dictionary("pdm_syn_pos", f"{SYNONYMS} WITH (frequency, position)")
vocab = ", ".join(f"'{w}'" for w in VOCAB)
t.table("pdm", "id INTEGER PRIMARY KEY, body VARCHAR")
t.statement("INSERT INTO pdm SELECT i, (SELECT string_agg("
            f"[{vocab}][1 + (hash(i, j) % {len(VOCAB)})::INTEGER], ' ' ORDER BY j) "
            "FROM range(1 + (hash(i) % 12)::INTEGER) r(j)) FROM range(600) t(i)")
t.index("pdm_text", "pdm", "id, body pdm_words", include="body")
t.index("pdm_pos", "pdm", "id, body pdm_words_pos")
t.index("pdm_syn_text", "pdm", "id, body pdm_syn", include="body")
t.index("pdm_syn_pos", "pdm", "id, body pdm_syn_pos")
t.refresh("pdm")
t.table("pdm_s", "id INTEGER, body VARCHAR", search=True)
t.statement("INSERT INTO pdm_s SELECT id, body FROM pdm")
t.refresh("pdm_s")
t.index("pdm_s_text", "pdm_s", "id, body pdm_words")
t.index("pdm_s_pos", "pdm_s", "id, body pdm_words_pos")
t.refresh("pdm_s")

t.section("Dense dictionary, every shape, alone and next to other conditions.")
for q in grid():
    for extra in CONTEXTS:
        check("pdm_text", "pdm_pos", q, extra)
    check("pdm_text", "pdm_pos", f"({q} || 'dog')")
    check("pdm_text", "pdm_pos", f"({q})::score('constant(1)')")

t.section("Fuzzy parts deferred: with no Levenshtein term limit they defer too.")
t.statement("SET sdb_levenshtein_max_terms = 0")
for q in grid(fuzzy_only=True):
    for extra in CONTEXTS:
        check("pdm_text", "pdm_pos", q, extra)
t.statement("RESET sdb_levenshtein_max_terms")

t.section("Ranked by BM25 (checked inline): same scores as positions. Fuzzy "
          "parts are left out until the check weights them by edit distance "
          "like the index.")
for q in [q for q in grid() if "~1" not in q][::5]:
    check_scores("pdm_text", "pdm_pos", q)

t.section("Synonyms put several tokens at one position: slop with patterns "
          "stays inline.")
for q in grid():
    check("pdm_syn_text", "pdm_syn_pos", q)

t.section("Search table keeps its text: same answers.")
for q in grid()[::3]:
    check("pdm_s_text", "pdm_s_pos", q)

t.write()
