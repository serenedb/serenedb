---
title: Tokenizer Functions
sidebar_label: Tokenizers
sidebar_position: 2
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

Every template of [`CREATE TEXT SEARCH DICTIONARY`](../../../statements/create_text_search_dictionary/index.md) is also a scalar function of the same name: `split_text`, `stem_words`, `generate_ngrams`. The function takes the value to analyze first and then the template's own arguments — the same names, the same positional order, the same defaults as on the template's page — and returns the tokens as a list. It is an implicit instantiation of the tokenizer: `split_text(body, case := 'lower')` in a query produces exactly what a dictionary created `AS split_text(case := 'lower')` produces for `body`, so the functions let you analyze text ad hoc, preview a dictionary stage by stage, or tokenize a column without creating anything in the catalog. `WITH (help)` on `CREATE TEXT SEARCH DICTIONARY` prints the function form under every template.

<SqlLogicTest id="sql/functions/search/tokenizers/index/text" />

## Values, lists and chains {#values-lists-and-chains}

| Input | Behaviour |
| :--- | :--- |
| `VARCHAR` | The value is analyzed as one value, the way a dictionary analyzes one cell. |
| `VARCHAR[]` | Each element is analyzed on its own and the results are concatenated into one list, so a function applied to another function's result acts as the next stage of a pipeline. |
| `VARCHAR[]` for `generate_shingles` and `generate_wildcard_ngrams` | The list is one token stream: the elements are the base tokens the shingles or wildcard grams are built from. |
| `NULL` | A `NULL` value yields `NULL`; `NULL` elements of a list are skipped. |
| `VARCHAR[N]` | A fixed-size array is read as the list it is, in the value position and in a list-valued option. |

Nesting the functions is the pipeline: the inner call produces the list the outer call re-analyzes element by element, so `stem_words(normalize_tokens(split_text(v, case := 'lower'), 'en_US.UTF-8', accent := false), 'en_US.UTF-8')` is the dictionary `split_text(case := 'lower') | normalize_tokens('en_US.UTF-8', accent := false) | stem_words('en_US.UTF-8')`:

<SqlLogicTest id="sql/functions/search/tokenizers/index/chain" />

<SqlLogicTest id="sql/functions/search/tokenizers/index/dictionary" />

A list literal is analyzed element by element:

<SqlLogicTest id="sql/functions/search/tokenizers/index/list" />

`generate_shingles` and `generate_wildcard_ngrams` need a token stream, so they take the output of another function; a bare `VARCHAR` counts as a single token:

<SqlLogicTest id="sql/functions/search/tokenizers/index/shingle" />

A list carries no token positions, only order, so the two wrappers read every element as one position and treat neighbours as adjacent. A dictionary sees the real positions instead, which differ wherever a stage emits several tokens at one position: the grams of [`generate_ngrams`](./generate_ngrams.md) are alternatives for the same word, and the expansions of [`expand_solr_synonyms`](./expand_solr_synonyms.md) are alternatives for the same token. Shingling such a stage therefore belongs in a dictionary: the function `generate_shingles(generate_ngrams(v, 2, 3), 2, 2)` pairs each gram with the next one in the list, where the dictionary `generate_shingles(generate_ngrams(2, 3), 2, 2)` pairs across positions and leaves same-position grams unpaired.

The value may come from a column; the options may not. Every option is folded to a constant when the query is bound, because the tokenizer is built once per query, so an option that varies per row fails with `<name>(): option "<option>" must be a constant`. A `NULL` option leaves it at its default, and a misspelled or missing option fails to bind with `No function matches the given name and argument types`, followed by the candidate signatures.

An option whose value is a list of strings takes either spelling of the dictionary form: the list, `remove_stopwords(v, stopwords := ['the'])`, or the string its template page documents, `remove_stopwords(v, stopwords := '"the"')` and `encode_geopoint(v, latitude := 'loc/lat')`. The candidate signature spells that out as `UNION(str VARCHAR, list VARCHAR[])`, which a fixed-size `VARCHAR[N]` also satisfies; any other type fails to bind.

<SqlLogicTest id="sql/functions/search/tokenizers/index/table" />

<SqlLogicTest id="sql/functions/search/tokenizers/index/constant_option" />

The result type follows the tokenizer: `VARCHAR[]` for every text template, `BLOB[]` for [`collate_tokens`](./collate_tokens.md), [`encode_geopoint`](./encode_geopoint.md) and [`encode_geojson`](./encode_geojson.md), whose terms are binary. A template that stores a per-document payload alongside its terms — `generate_shingles` with `store_tokens`, `encode_geojson` with a non-source coding — returns only the terms.

Four templates have no function form, because SQL already expresses them: [`keyword`](../../../statements/create_text_search_dictionary/keyword.md) is the list literal `[value]`, [`pipeline`](../../../statements/create_text_search_dictionary/pipeline/index.md) is nesting, [`union`](../../../statements/create_text_search_dictionary/union.md) is `list_concat` of two calls, and a [`sql`](../../../statements/create_text_search_dictionary/sql.md) stage is the expression itself.

A [stored dictionary](../../../statements/create_text_search_dictionary/index.md) has no function form: it is an analyzer instance rather than a template, so a query runs it through [`ts_lexize`](../full-text.md#ts_lexize) and an analyzer expression names it as a stage.

## Functions {#functions}

Each signature lists the value first and then the template's options with their
defaults; a required option has none. Option semantics, constraints and error
messages are on the page each row links to.

| Function | What it does |
| :--- | :--- |
| [`split_text(value, case := 'none', break := 'alpha')`](./split_text.md) | Split text into words on Unicode word boundaries, optionally case-folded |
| [`split_text_icu(value, locale, break := 'alpha')`](./split_text_icu.md) | Segment text into words or sentences with the ICU break iterator for `locale` |
| [`split_text_csv(value, delimiter)`](./split_text_csv.md) | Cut the value at every occurrence of `delimiter`, honouring `"` quoting |
| [`split_by_delimiters(value, delimiters)`](./split_by_delimiters.md) | Cut the value at every occurrence of any of the `delimiters` |
| [`split_by_non_alpha(value, case := 'none')`](./split_by_non_alpha.md) | Runs of ASCII letters and digits, everything else a separator |
| [`split_by_pattern(value, pattern, group := -1)`](./split_by_pattern.md) | Match or split with an RE2 regular expression |
| [`expand_path(value, delimiter := '/', replacement := '', reverse := false, skip := 0)`](./expand_path.md) | The cumulative prefixes of a delimited path |
| [`normalize_tokens(value, locale := '', case := 'none', accent := true, form := 'nfc')`](./normalize_tokens.md) | Unicode normalization, locale-aware case folding and accent stripping of each token |
| [`stem_words(value, locale := '')`](./stem_words.md) | The Snowball stem of each token for the language of `locale`, which has no usable default |
| [`remove_stopwords(value, stopwords := [], stopwords_path := '', hex := false)`](./remove_stopwords.md) | Drop the listed tokens |
| [`generate_ngrams(value, min_gram := 2, max_gram := 3, preserve_original := false, input_type := 'utf8', start_marker := '', end_marker := '', mode := 'all')`](./generate_ngrams.md) | Character n-grams of each token, including the prefix-anchored edge n-grams of `mode := 'only_prefix'` |
| [`generate_sparse_ngrams(value, max_ngram_length := 16, covering := false)`](./generate_sparse_ngrams.md) | Sparse variable-length n-grams for substring search |
| [`generate_wildcard_ngrams(value, ngram_size := 3)`](./generate_wildcard_ngrams.md) | Boundary-marked n-grams for wildcard and prefix matching; a list is the token stream, a `VARCHAR` one token |
| [`generate_shingles(value, min_gram := 2, max_gram := 2, output_unigrams := true, fallback_unigrams := false, store_tokens := true, frequent_words := [], filler_token := '', token_separator := ' ')`](./generate_shingles.md) | Word n-grams over the token stream; a list is the token stream, a `VARCHAR` one token |
| [`collate_tokens(value, locale := '')`](./collate_tokens.md) | The collation sort key of each token for `locale`, as `BLOB[]` |
| [`expand_solr_synonyms(value, synonyms)`](./expand_solr_synonyms.md) | Synonym expansion from a Solr synonyms text |
| [`expand_wordnet_synonyms(value, synonyms)`](./expand_wordnet_synonyms.md) | Synonym expansion from WordNet prolog text |
| [`classify_text(value, model_location := '', top_k := 1, threshold := 0)`](./classify_text.md) | The labels a fastText model assigns to the value |
| [`find_nearest_words(value, model_location := '', top_k := 1)`](./find_nearest_words.md) | The nearest word vectors of a fastText model |
| [`encode_geopoint(value, latitude := [], longitude := [], max_cells := 20, min_level := 4, max_level := 23, level_mod := 1, optimize_for_space := false)`](./encode_geopoint.md) | The S2 cell terms of a `JSON` point, as `BLOB[]`; there is no list form |
| [`encode_geojson(value, type := 'shape', coding := 'source', max_cells := 20, min_level := 4, max_level := 23, level_mod := 1, optimize_for_space := false)`](./encode_geojson.md) | The S2 cell terms of a `JSON` geometry, as `BLOB[]`; there is no list form |
| [`minhash(tokens, num_hashes)`](./minhash.md) | Reduce a token list to a MinHash signature; it has no template |
