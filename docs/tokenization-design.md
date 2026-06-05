# Tokenization design (inverted index rewrite) — v6

DECISION: tokenization moves out of the index into the DuckDB expression framework.
The iresearch Analyzer interface (`reset/next` + attribute pointers) is removed, for
both architectural reasons (engine-private interface, leaf logic duplicated against
SQL functions) and performance ones (per-token virtual `next()` + attribute-pointer
loads + `_pos += inc` branches + `_proc_table` indirect call, per stage — a real tax
against a hash-probe budget). A dictionary compiles to ONE bound expression; the
index consumes its result vectors and contains no tokenization code.

No new SQL syntax: `CREATE TEXT SEARCH DICTIONARY (template=..., stepN_*, ...)` and
`ts_*` functions keep their surface; templates compile to expressions underneath.

## 1. The type

```
TOKENSTREAM := LIST(STRUCT(term VARCHAR [, inc UINT32] [, off_start UINT32, off_len UINT32]))
```

A type alias over standard DuckDB types — which already gives the right physical
layout: LIST keeps the per-row structure (`list_entry_t` per input row — token→row
mapping is structural, no `row` column), and STRUCT-in-LIST is SoA — `term[]`,
`inc[]`, `off[]` are separate child vectors. The "special vector type + some info" is
this; no new vector machinery, only an alias plus functions bound to it.

Producers with a per-row stored attribute (wildcard's original-terms blob, geo's
original geometry) return the wider shape

```
STRUCT(tokens TOKENSTREAM, store BLOB)
```

— the blob is per row, so a struct sibling of the list is the type-correct encoding.
The consumer destructures by shape: list child → postings, blob child → the stored
field (`store_field_id` mechanism). Query-side asymmetry (`ts_like`'s
ngram-approximation + verify-against-blob) is a filter-builder concern and unchanged.

The struct carries exactly `supported ∩ requested` fields, fixed at dictionary
compile time: terms-only dictionaries produce effectively LIST(VARCHAR); `inc` exists
only when the positions feature is on; `off_*` only when offsets are on AND every
function in the chain supports them (DDL already validates this — offset=true is
rejected for analyzers without OffsAttr; `token_union` compiles without offset
children). Nothing unsupported is ever materialized or faked.

Encodings (the point of being columnar — standard DuckDB vector types do the work):
- `inc` is the position increment. For splitters with strictly sequential positions
  (`words`, `delimiter`, `pattern`, `segmentation`, ...) it is a **CONSTANT vector
  = 1**: zero materialization, zero per-token writes. Only `ngram` (inc=0
  continuation is its pinned semantics), expanders, and `token_union` emit a FLAT
  inc child. Consumers branch once per block on the vector type.
- `term` may be **DICTIONARY-encoded** (block-local unique child + sel vector).
  Transforms then run once per unique term per block; the sel vector passes through
  untouched. This is block-scoped standard vector encoding — it dies with the chunk
  (it is NOT a cross-segment surface-form map; no merge implications).
- Filters NULL terms **in place** (validity mask only), never compact — preserving
  the CONSTANT `inc` and DICTIONARY `term` encodings through the stage. Whether a
  NULL slot consumes a position is a compiled per-template property (§5: `text`
  skips silently = no; hole-keeping templates = yes).
- Term string data may be zero-copy views into the input vector's heap
  (`StringVector::AddHeapReference`); ≤12-byte terms inline in `string_t` anyway.

## 2. The functions

| role | signature | examples |
|---|---|---|
| splitter | `VARCHAR → TOKENSTREAM` | `words`, `icu_words(locale)`, `segmentation`, `delimiter`, `multi_delimiter`, `pattern(re,group)`, `ngram(min,max,unit)`, `path_hierarchy`, `keyword` |
| re-splitter | `TOKENSTREAM → TOKENSTREAM` | same splitters lifted: split each term, rewrite pos/off (pipeline-of-splitters, ES can't do this) |
| transform | `TOKENSTREAM → TOKENSTREAM` | `lower`, `nfc`, `unaccent`, `stem(lang)`, `collation_sortkey(locale)` — tight loop over the `term` child vector, pos/off pass through |
| filter | `TOKENSTREAM → TOKENSTREAM` | `stopwords(dict)` — NULLs or removes terms per template semantics |
| expander | `TOKENSTREAM → TOKENSTREAM` | `synonyms(dict)` — appends terms with duplicated `pos` (stack); `minhash` |
| merge | `TOKENSTREAM, TOKENSTREAM → TOKENSTREAM` | `token_union` — interleave by `pos`, sub-order preserved (the union template) |
| stored-attr producer | `VARCHAR → STRUCT(TOKENSTREAM, BLOB)` | `wildcard` (ngram tokens + original-terms blob), geo (S2 cells + geometry blob) |

One kernel per primitive: transforms are implemented as kernels over flat string
vectors `(const string_t* in, string_t* out, count, arena)` and registered twice —
as the plain `VARCHAR → VARCHAR` scalar (SQL users) and inside the TOKENSTREAM
overload (runs the same kernel over the `term` child). That is goal 2: lower/stem/
unaccent/stopword exist exactly once. Expensive state (snowball, ICU objects) lives
in function-local state, cached per function instance.

Template compilation (no DDL change, catalog-side):

```
template='text', case=lower, stemming, stopwords
  → stopwords(stem(lower(nfc(icu_words(x))), lang), dict)
template='pipeline', step1=delimiter, step2=text   → text-chain(delimiter(x))
template='union', tokenizer1=…, tokenizer2=…       → token_union(t1(x), t2(x))
template='ngram'                                   → ngram(x, min, max)
```

The compiled expression is stored on the dictionary as a serialized bound expression
(precedent: `ExpressionData` for indexed expressions,
`connector/index_expression.cpp:147-155`).

## 3. Execution

**Write path**: per insert block, the writer evaluates the dictionary expression over
the column chunk with a cached `ExpressionExecutor` (precedent:
`EvaluateExprOverChunk`; fix its per-call rebind). Every stage is one vectorized pass
over child vectors — no per-token dispatch anywhere. The inverter consumes the result
with encoding fast paths (FLAT + CONSTANT + DICTIONARY, unified fallback — the
existing per-row Vector convention):
- `inc` CONSTANT(1) → positions are ordinals, no reads;
- `term` DICTIONARY → probe the postings map once per unique term, fill a block-local
  `PostingBuilder* builders[n_unique]`, then occurrences are
  `builders[sel[i]]->add(row, pos)` — **no hash probe per occurrence** (cheaper than
  a fused per-token loop, which probes every occurrence);
- FLAT fallback: probe per non-NULL term.
BM25 `dl` = position-consuming entries of the row — same population as postings,
finalized at doc time, by construction.

**Needle path**: the same bound expression evaluated on the needle constant.
ts_phrase reads `pos` for gaps (fixing today's hardcoded {1,1}), ts_any/ts_all dedup
terms then derive min_match from the deduped count (fixing today's pre-dedup count).
ts_prefix/ts_like still do NOT evaluate the expression on patterns (pinned).

**Introspection**: `ts_lexize` ≈ the expression itself (project `term`); one
implementation, the real one.

**Overhead, stated honestly**: each stage writes its result child vectors — that is
standard vectorized execution, the same cost model as any string-expression chain in
the engine, paid as tight sequential loops over flat vectors. Versus the Analyzer
cursor it removes all per-token virtual/attribute/branch overhead; versus a
hypothetical fully-fused split→probe loop it adds the per-stage vector writes
(sequential stores, term views zero-copy). That trade is accepted: it is what
"tokenization in the expression framework" means, it amortizes over every stage, and
it is where the engine's existing optimizations (and future ones — fusion,
dictionary vectors) apply for free. The only fusion kept in mind (not built until a
benchmark demands it): a split-only dictionary may skip the intermediate list and
feed the inverter directly — an executor optimization invisible to semantics.

**Compute/storage separation (consequence, not extra design):** the dictionary is a
serialized bound expression + a behavior fingerprint — a location-independent
contract. A compute node can evaluate it over ingest blocks and ship the result
chunks (term/inc/off children + store blobs; standard vectors, wire-serializable,
DICTIONARY encoding included) to storage nodes, whose write path reduces to the
inverter consume loop — no ICU/snowball/regex/analyzers in storage. The same
fingerprint that gates segment merges validates remote tokenization; cross-node
version skew is the failure mode it catches.

## 4. Coverage of the special analyzers

- `wildcard`: `VARCHAR → STRUCT(TOKENSTREAM, BLOB)` — ngram tokens of the base
  terms + the length-prefixed original-terms blob; offset suppression falls out of
  the capability-typed struct (no `off` children). The write side is now an ordinary
  expression; `ts_like`'s query-side rewrite (ngram approximation + RE2 verify
  against the stored blob via `store_field_id`) is unchanged.
- geo (`geopoint`/`geojson`): `→ STRUCT(TOKENSTREAM, BLOB)` — S2 cell tokens +
  original geometry blob for exact post-filtering.
- `minhash`: a plain `TOKENSTREAM → TOKENSTREAM` function (consumes child tokens,
  emits signature tokens). `classification`/`nearest_neighbors`: `VARCHAR →
  TOKENSTREAM` with the model handle in function-local state.
- `union` with offsets stays rejected at DDL (pinned); `token_union` output carries
  no offset children.

Nothing remains outside the expression model.

## 5. Pinned semantics the function implementations must reproduce

(oracle: tests/sqllogic/sdb/pg/simple/tokenizers/*.test)

1. ngram emission is position-major with stacking:
   `'HeLLo' min2 max3 → {He,HeL,eL,eLL,LL,LLo,Lo}` (text_tokenizer.test:528);
   `preserve_original`, start/end markers included.
2. `text` does NOT leave holes for stopwords (silently skips; next kept word
   advances pos by 1). `pipeline`+`stopwords` abandons the branch. Hole behavior is
   per-template, encoded in how each compiled expression uses NULL-vs-remove.
3. union interleaves by min-pos in sub-index order:
   `{hello,HELLO,Hello,world,WORLD,World}` (test:1566).
4. pipeline position algebra: re-split tokens don't double-count positions
   (pipeline_tokenizer.cpp:133-142 is the reference behavior for re-splitters).
5. Offsets are byte offsets into the ORIGINAL input (UTF-16→UTF-8 remap in text);
   transforms never shift them.
6. 1:1 templates (`stem`/`norm`/`collation`/`keyword`/`stopwords`) apply to the
   whole value — compiled without a splitter.
7. Borrowed/zero-copy term data: child vectors may reference the input heap; the
   inverter consumes within the block's lifetime.

## 6. Versioning / merge compatibility

The compiled expression is index format. Fingerprint = hash(serialized expression +
function-behavior pins: snowball version, ICU, stopword/synonym contents hashed, DuckDB
serialization version). Stamped into segment metadata; consolidation refuses mixed
fingerprints (enforced at shard layer — `merge_writer` keys fields by name only and
cannot see the catalog). Dictionaries immutable once referenced (today's model).

## 7. Migration & benchmarks

- Add the TOKENSTREAM alias + function set; compile existing templates to
  expressions; validate against the tokenizer sqllogic suite (§5 is the oracle).
- Rewire the index writer: evaluate dictionary expression per block, inverter
  consumes child vectors; delete the analyzer drive path
  (`search_sink_writer` Field::GetTokens, `FieldData::invert`'s attribute plumbing).
- Rewire ts_* needle builders to evaluate the expression on constants; fix the two
  flagged divergences (ts_phrase gaps, ts_anyall min_match) deliberately.
- Benchmarks: (a) expression chain vs current analyzer path on a real corpus —
  expected win from removing per-token dispatch; (b) split-only dictionary: chain vs
  direct-feed fusion, to decide if the §3 optimization is ever needed; (c) single-row
  INSERT executor overhead with cached executor.
