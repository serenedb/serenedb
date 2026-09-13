---
title: Tokenizer Functions
sidebar_label: Tokenizers
sidebar_position: 2
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

<!-- markdownlint-disable MD001 -->

Every template of [`CREATE TEXT SEARCH DICTIONARY`](../../statements/create_text_search_dictionary/index.md) is also a scalar function of the same name: `split_text`, `stem_words`, `generate_ngrams`. The function takes the value to analyze first and then the template's own arguments — the same names, the same positional order, the same defaults as on the template's page — and returns the tokens as a list. It is an implicit instantiation of the tokenizer: `split_text(body, case := 'lower')` in a query produces exactly what a dictionary created `AS split_text(case := 'lower')` produces for `body`, so the functions let you analyze text ad hoc, preview a dictionary stage by stage, or tokenize a column without creating anything in the catalog. `WITH (help)` on `CREATE TEXT SEARCH DICTIONARY` prints the function form under every template.

<SqlLogicTest id="sql/functions/tokenizers/text" />

## Values, lists and chains {#values-lists-and-chains}

| Input | Behaviour |
| :--- | :--- |
| `VARCHAR` | The value is analyzed as one value, the way a dictionary analyzes one cell. |
| `VARCHAR[]` | Each element is analyzed on its own and the results are concatenated into one list, so a function applied to another function's result acts as the next stage of a pipeline. |
| `VARCHAR[]` for `generate_shingles` and `generate_wildcard_ngrams` | The list is one token stream: the elements are the base tokens the shingles or wildcard grams are built from. |
| `NULL` | A `NULL` value yields `NULL`; `NULL` elements of a list are skipped. |

Nesting the functions is the pipeline: the inner call produces the list the outer call re-analyzes element by element, so `stem_words(normalize_tokens(split_text(v, case := 'lower'), 'en_US.UTF-8', accent := false), 'en_US.UTF-8')` is the dictionary `split_text(case := 'lower') | normalize_tokens('en_US.UTF-8', accent := false) | stem_words('en_US.UTF-8')`:

<SqlLogicTest id="sql/functions/tokenizers/chain" />

<SqlLogicTest id="sql/functions/tokenizers/dictionary" />

A list literal is analyzed element by element:

<SqlLogicTest id="sql/functions/tokenizers/list" />

`generate_shingles` and `generate_wildcard_ngrams` need a token stream, so they take the output of another function; a bare `VARCHAR` counts as a single token:

<SqlLogicTest id="sql/functions/tokenizers/shingle" />

The value may come from a column; the options may not. Every option is folded to a constant when the query is bound, because the tokenizer is built once per query, so an option that varies per row fails with `<name>(): option "<option>" must be a constant`. A `NULL` option leaves it at its default, and a misspelled or missing option fails to bind with `No function matches the given name and argument types`, followed by the candidate signatures.

<SqlLogicTest id="sql/functions/tokenizers/table" />

<SqlLogicTest id="sql/functions/tokenizers/constant_option" />

The result type follows the tokenizer: `VARCHAR[]` for every text template, `BLOB[]` for [`collate_tokens`](#collate_tokens), [`encode_geopoint`](#encode_geopoint) and [`encode_geojson`](#encode_geojson), whose terms are binary. A template that stores a per-document payload alongside its terms — `generate_shingles` with `storetokens`, `encode_geojson` with a non-source coding — returns only the terms.

Four templates have no function form, because SQL already expresses them: [`keyword`](../../statements/create_text_search_dictionary/keyword.md) is the list literal `[value]`, [`pipeline`](../../statements/create_text_search_dictionary/pipeline/index.md) is nesting, [`union`](../../statements/create_text_search_dictionary/union.md) is `list_concat` of two calls, and a [`sql`](../../statements/create_text_search_dictionary/sql.md) stage is the expression itself; a stored dictionary used as a stage is [`ts_lexize`](./full-text.md#ts_lexize) on that dictionary.

## Functions {#functions}

Each signature below lists the value first and then the template's options with their defaults; a required option has none. The option semantics, constraints and error messages are those of the template page linked from each entry, and each entry names its template.

#### `split_text(value, case := 'none', break := 'alpha')` {#split_text}

The `split_text` template: split text into words on Unicode word boundaries, optionally case-folded — [`split_text`](../../statements/create_text_search_dictionary/text.md).

#### `split_text_icu(value, locale, break := 'alpha')` {#split_text_icu}

The `split_text_icu` template: segment text into words or sentences with the ICU break iterator for `locale` — [`split_text_icu`](../../statements/create_text_search_dictionary/icu_text.md).

#### `split_csv(value, delimiter)` {#split_csv}

The `split_csv` template: cut the value at every occurrence of `delimiter`, honouring `"` quoting — [`split_csv`](../../statements/create_text_search_dictionary/csv.md).

#### `split_by_delimiters(value, delimiters)` {#split_by_delimiters}

The `split_by_delimiters` template: cut the value at every occurrence of any of the `delimiters` — [`split_by_delimiters`](../../statements/create_text_search_dictionary/multi-delimiter.md).

#### `split_by_non_alpha(value, case := 'none')` {#split_by_non_alpha}

Runs of ASCII letters and digits, everything else a separator — [`split_by_non_alpha`](../../statements/create_text_search_dictionary/split_by_non_alpha.md).

A token is a maximal run of `[A-Za-z0-9]`; punctuation, whitespace, underscores and every non-ASCII byte separate, and `case := 'lower'` folds the ASCII letters. It is the dictionary-free equivalent of `regexp_split_to_array(text, '[^A-Za-z0-9]+')` without the regex engine:

<SqlLogicTest id="sql/functions/tokenizers/split_by_non_alpha" />

#### `split_by_pattern(value, pattern, group := -1)` {#split_by_pattern}

The `split_by_pattern` template: match or split with an RE2 regular expression — [`split_by_pattern`](../../statements/create_text_search_dictionary/pattern.md).

#### `expand_path(value, delimiter := '/', replacement := '', reverse := false, skip := 0)` {#expand_path}

The `expand_path` template: the cumulative prefixes of a delimited path — [`expand_path`](../../statements/create_text_search_dictionary/path-hierarchy.md).

#### `normalize_tokens(value, locale := '', case := 'none', accent := true, form := 'nfc')` {#normalize_tokens}

The `normalize_tokens` template: unicode normalization, locale-aware case folding and accent stripping of each token — [`normalize_tokens`](../../statements/create_text_search_dictionary/norm.md).

#### `stem_words(value, locale := '')` {#stem_words}

The `stem_words` template: the Snowball stem of each token for the language of `locale`, which has no usable default — [`stem_words`](../../statements/create_text_search_dictionary/stem.md).

#### `remove_stopwords(value, stopwords := [], stopwordspath := '', hex := false)` {#remove_stopwords}

The `remove_stopwords` template: drop the listed tokens — [`remove_stopwords`](../../statements/create_text_search_dictionary/stopwords.md).

#### `generate_ngrams(value, mingram := 2, maxgram := 3, preserveoriginal := false, inputtype := 'utf8', startmarker := '', endmarker := '', mode := 'all')` {#generate_ngrams}

The `generate_ngrams` template: character n-grams of each token, including the prefix-anchored edge n-grams of `mode := 'only_prefix'` — [`generate_ngrams`](../../statements/create_text_search_dictionary/ngram.md).

#### `generate_sparse_ngrams(value, maxngramlength := 16, covering := false)` {#generate_sparse_ngrams}

The `generate_sparse_ngrams` template: sparse variable-length n-grams for substring search — [`generate_sparse_ngrams`](../../statements/create_text_search_dictionary/sparse-ngram.md).

#### `generate_wildcard_ngrams(value, ngramsize := 3)` {#generate_wildcard_ngrams}

The `generate_wildcard_ngrams` template: boundary-marked n-grams for wildcard and prefix matching; a list is the token stream, a `VARCHAR` one token — [`generate_wildcard_ngrams`](../../statements/create_text_search_dictionary/wildcard.md).

#### `generate_shingles(value, mingram := 2, maxgram := 2, outputunigrams := true, outputunigramsifnoshingles := false, storetokens := true, frequentwords := [], fillertoken := '')` {#generate_shingles}

The `generate_shingles` template: word n-grams over the token stream; a list is the token stream, a `VARCHAR` one token — [`generate_shingles`](../../statements/create_text_search_dictionary/shingle.md).

#### `collate_tokens(value, locale := '')` {#collate_tokens}

The `collate_tokens` template: the collation sort key of each token for `locale`, as `BLOB[]` — [`collate_tokens`](../../statements/create_text_search_dictionary/collation.md).

#### `expand_solr_synonyms(value, synonyms)` {#expand_solr_synonyms}

The `expand_solr_synonyms` template: synonym expansion from a Solr synonyms text — [`expand_solr_synonyms`](../../statements/create_text_search_dictionary/solr-synonyms.md).

#### `expand_wordnet_synonyms(value, synonyms)` {#expand_wordnet_synonyms}

The `expand_wordnet_synonyms` template: synonym expansion from WordNet prolog text — [`expand_wordnet_synonyms`](../../statements/create_text_search_dictionary/wordnet-synonyms.md).

#### `classify_text(value, modellocation := '', topk := 1, threshold := 0)` {#classify_text}

The `classify_text` template: the labels a fastText model assigns to the value — [`classify_text`](../../statements/create_text_search_dictionary/classification.md).

#### `find_nearest_words(value, modellocation := '', topk := 1)` {#find_nearest_words}

The `find_nearest_words` template: the nearest word vectors of a fastText model — [`find_nearest_words`](../../statements/create_text_search_dictionary/nearest-neighbors.md).

#### `encode_geopoint(value, latitude := [], longitude := [], maxcells := 20, minlevel := 4, maxlevel := 23, levelmod := 1, optimizeforspace := false)` {#encode_geopoint}

The `encode_geopoint` template: the S2 cell terms of a `JSON` point, as `BLOB[]`; there is no list form — [`encode_geopoint`](../../statements/create_text_search_dictionary/geopoint.md).

#### `encode_geojson(value, type := 'shape', coding := 'source', maxcells := 20, minlevel := 4, maxlevel := 23, levelmod := 1, optimizeforspace := false)` {#encode_geojson}

The `encode_geojson` template: the S2 cell terms of a `JSON` geometry, as `BLOB[]`; there is no list form — [`encode_geojson`](../../statements/create_text_search_dictionary/geojson.md).
