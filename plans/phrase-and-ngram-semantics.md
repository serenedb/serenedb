# Phrase and n-gram: what is a match, what is a score

Every cell below was read out of the two codebases, not from documentation or
issue text. Lucene paths are `lucene/core/.../search/`; ours are
`iresearch/search/`.

Two things are separate everywhere and get confused constantly:

- **match** -- does this document qualify at all.
- **score** -- what number feeds the similarity. For BM25 that is
  `boost * idf * tf/(c1 + tf)`, so it matters a lot *where* a signal enters:
  inside `tf` it saturates, outside as `boost` it scales the score linearly.

## Phrase, every part is a single term

| case | match | our tf | Lucene tf | same? |
|---|---|---|---|---|
| fixed (adjacent, or fixed gaps) | positions land exactly on the query offsets | count of occurrences | count of occurrences | **yes** |
| variable gaps (a part may sit anywhere in a range) | position inside the declared interval | count of tuples | *no equivalent* | n/a |
| slop | `max(dᵢ-qᵢ) - min(dᵢ-qᵢ) <= slop` | count of tuples | `Σ 1/(1+matchLength)` | **no** |

Fixed gaps are not a separate behaviour: Lucene's `PhraseQuery` takes explicit
positions too, and `slop == 0` uses `ExactPhraseMatcher` either way. Variable
gaps are ours alone.

### Variable gaps, in detail

`IntervalPositionStrategy` (`phrase_matcher.hpp:160`) checks each junction
against an inclusive window `[low, high]` relative to the previous part, order
preserving.

The right way to read this is **a compact disjunction of fixed-gap phrases**:
`a <1-3> b` is `(a <1> b) OR (a <2> b) OR (a <3> b)`, written once and
evaluated in one pass instead of three.

That settles the score, and it is *not* the slop rule. Slop charges for
deviation because the user declared a phrase and the budget measures distance
from it. A range declares that every value inside it is acceptable, so a match
at gap 1 and a match at gap 3 are both exactly what was asked for -- exactly as
`a <1> b` and `a <3> b` are two different queries that each score a clean
occurrence. Charging for position inside the range would invent a preference
the user never expressed.

Today's behaviour already is the disjunction reading: one occurrence per gap
value that hits, summed. **Nothing to change here.**

Open only in this respect: "same as the OR" fixes equal treatment inside the
range, but not whether tf should be the sum over gap values or the max. That
follows from which merge the equivalent OR should use. Today it is effectively
Sum.

## Phrase, a part is a disjunction of terms

Lucene's equivalent is `MultiPhraseQuery`, and any slot may be a set of terms,
not only the first. It reuses the same two matchers over a per-slot union, so
the match and tf rules above carry over unchanged.

Ours adds a signal Lucene does not have: each expanded term may carry a boost
(how close a fuzzy/wildcard alternative was). Before this change `PhraseMatcher`
reduced it to `Σ boosts / (slots * freq)` -- the mean over every slot of every
counted match -- and published it through `BoostBlockAttr`, which BM25 applied
**outside** the saturation.

That placement is the thing to revisit. A weaker alternative is evidence of a
*partial* match, which is the same kind of signal as phrase tightness; both
belong inside `tf`, where they saturate, rather than as a linear multiplier on
the finished score.

### idf

`PhraseCollector::Finish` (`detail/collectors.cpp:72`) calls
`scorer->collect()` once per term per slot into a single stats buffer, and
BM25's `collect` accumulates `stats->idf += ...`. So idf is the sum over every
expansion of every slot: a wildcard slot matching 500 terms contributes 500
idf terms.

Lucene does the same -- `MultiPhraseQuery` hands every term's stats to
`BM25Similarity`, whose `idfExplain` sums them. So we match Lucene here, and
changing it means deliberately diverging.

### Where every expansion rule actually stands

Verified in both codebases. Three different rules exist in our tree for the same
question, and where Lucene scores an expansion at all we already match it.

| cell | us | Lucene | same? |
|---|---|---|---|
| standalone fuzzy | **max** df, one blended statistic (`multiterm_collector.hpp:150` `BlendedTermsCollector`) | **max** df (`BlendedTermQuery.java:286`) | yes |
| phrase slot (regex / wildcard / fuzzy) | **sum** of idfs (`collectors.cpp:72`) | **sum** (`MultiPhraseQuery` + `idfExplain`; fuzzy-in-phrase goes through `ComplexPhraseQueryParser` -> `SpanNearQuery` + `SpanMultiTermQueryWrapper`, and `SpanWeight.buildSimWeight` hands every term's stats to the similarity) | yes |
| standalone wildcard / prefix / range / regexp | **constant score by default** -- every such filter's constructor does `SetScorer(&DefaultConstScore())` (`wildcard_filter.hpp:100`, `prefix_filter.hpp:61`, `range_filter.hpp:97`, `regexp_filter.hpp:85`). `SetScorer` swaps in a real scorer on request, and only then does `MultiTermCollector` give each expansion its own idf. Distinct from `ForceConstScore()`, which is a property of the *data* rather than a preference: `ts_common.hpp:123` gives it to every non-VARCHAR/BLOB column, and geo filters and the null-marker term use it too, because there are no term statistics to score with | **constant score by default** (`MultiTermQuery` -> `CONSTANT_SCORE_REWRITE`), with a scoring rewrite on request | yes -- same default, same escape hatch |
| n-gram similarity | **pooled**: `ByTermsCollector(scorer, 1, ...)` puts every term in one counter, so df is summed and idf is `idf(sum df)` | no equivalent | n/a |

So every cell agrees with Lucene. The inconsistency that remains is shared with
Lucene rather than a divergence from it: a fuzzy term blends to one statistic on
its own and silently stops blending the moment it is placed inside a phrase.

So changing this is **not** a parity fix. It is deliberately doing better than
Lucene, and none of max / sum / pooled is the answer -- they are three
approximations of a quantity that can be read exactly. Per document the matched
alternative is known, so charge its idf. See #445, which must be **guarded by a
flag**: users who want Lucene-identical ranking must be able to keep it.

## N-gram similarity

The measure is Grzegorz Kondrak, *N-Gram Similarity and Distance*, SPIRE 2005
(pp. 115-126). Lucene implements it only as a string distance for the spell
checker (`lucene/suggest/.../spell/NGramDistance.java`, whose javadoc cites the
paper); there is no Lucene *query*, so there is no reference implementation to
diff against.

What the paper actually defines:

- `s_n` generalises the **longest common subsequence**:
  `s_n(G_k,l) = max(s_n(G_k-1,l), s_n(G_k,l-1), s_n(G_k-1,l-1) + s_n(G_k-n,l-n))`,
  and at `n = 1` it *is* the LCS length.
- **"Positional"** in the paper is partial credit *inside* an n-gram --
  `(1/n) * sum_u s1(x_i+u, y_j+u)`, identical characters at matching offsets
  within the two n-grams being compared. It has nothing to do with positions in
  a document.
- **Normalization**: divide by `max(K, L)` so identical strings score exactly 1.

What we implement (`ngram_matcher.hpp`): the longest run of distinct query terms
at strictly increasing document positions, gaps allowed. Order-preserving with
skips **is** the LCS, so ours is Kondrak at `n = 1`, normalised by the query
length rather than `max(K, L)`:

- `_scale = longest_sequence_len / total_terms_count` (line 510)
- `_seq_freq` = how many such sequences were found (line 508)

The denominator is the right call, not a bug: here `Y` is the whole document, so
`max(K, L)` would be the document length and the measure would collapse.

**Why positions are the point.** The obvious implementation -- a min-match over
the n-gram postings lists, "at least k of these terms are present" -- produces
*unordered* matches: it counts how many of the query's n-grams occur, as a bag.
That is a different measure, and the paper says so in its own footnote 1: it is
the **q-gram similarity** of Ukkonen 1992, "simply the number of common/distinct
q-grams between two strings", which Kondrak explicitly distinguishes from the
notion he develops. Respecting positions is what turns the bag count into the
ordered LCS, i.e. into Kondrak's measure. This is also not the paper's
*positional* variant, which is a separate refinement about partial credit inside
one n-gram.

| case | match | score |
|---|---|---|
| min_match == 1 | any one query term present | fraction = 1/total .. 1, tf = sequence count |
| min_match == k | LCS >= k | same shape, fraction >= k/total |
| min_match == all | every query term present, in order, any distance apart | fraction = 1, tf = sequence count |

`min_match == all` is **not** a phrase: in order, arbitrarily far apart.

So the measure itself is sound and matches the literature.

### Where coverage goes, and why it does not matter much

Coverage is document-side -- it varies per document and describes the match --
so by the same test that puts tightness in `tf`, it belongs there too. Measured
against the alternative on real data (query `wrote the sixth symphony` against a
four-sentence field, partial matches being the `sixht` typo variant at coverage
0.818):

| document | coverage in tf | coverage on the score |
|---|---|---|
| one exact match | 0.455 | 0.455 |
| one typo match | 0.405 | 0.372 |
| two typo matches | 0.577 | 0.511 |
| five typo matches | 0.773 | 0.660 |

The two placements never disagree on an ordering: both rank one partial below
the exact match and both flip at two. They cannot disagree much, because
`min_match` floors coverage, so the weight has a narrow range and
`sum coverage ~= count * coverage`. Contrast the phrase, where `1/(1+d)` ranges
from 1.0 down to `1/(1+slop)` and the placement does change orderings.

So it goes inside for consistency with the phrase, not because ranking demands
it. The lever that actually decides behaviour is `min_match`: at 1.0 only the
exact phrase qualifies, 0.8 admits a typo, 0.7 a reordering, 0.5 a paraphrase.
And two occurrences of anything admitted beat one exact match, whichever
placement is used -- if that is unwanted, the fix is to stop summing coverage
across occurrences, not to move it.

What remains genuinely unresolved is the idf. The quantity wanted is "how many
documents are similar to this string"; what is available is the pooled document
frequency of the query's grams, which does not move when `min_match` changes
even though the threshold is what decides how many documents qualify.

## The plan

One idea underneath all of it: every cell produces a number saying *how good
this match really is* -- phrase tightness, per-expansion similarity, n-gram
coverage. Today that number is either discarded or multiplied onto the finished
score, where it scales linearly. It belongs inside `tf`, where it saturates like
any other evidence.

**1. Slop tf.** Replace the exhaustive tuple enumeration with Lucene's one-pass
sweep and accumulate `sum 1/(1+d)`. It is exactly Lucene's number, and the sweep
reads positions straight off the iterators -- no `ReadAll`, no per-document
materialisation.

"Linear in positions, not combinatorial" is the wrong thing to aim for, and
aiming for it cost 2.6x on `"the english restoration"~2` (43.6G instructions vs
main's 10.1G, at *higher* IPC -- more work, not worse work). The old enumeration
led on the **rarest** slot and bounded the others with `lower_bound`; a sweep
that advances one position at a time is linear in *every* slot, so a dense
stopword dominates. Linear in the rarest slot beats linear in all of them.

Two rules recover it, both verified against the Lucene oracle:

- **The same-position scan is conditional.** `JoinPair` computed
  `EnforceUniqueness` once; the sweep re-scanned O(n^2) after every advance even
  when no two slots share a term group. 43.6G -> 30.9G.
- **Run-internal advances are skippable when the run cannot emit.** A position
  inside a run always has `shift <= runner <= end`, so it never raises `end`;
  and if `window > slop && runner < end - slop`, every remaining run-internal
  position gives `window >= end - runner > slop`. So the run is dead and the
  lead can `seek` straight to the boundary. 30.9G -> 12.2G, parity with main.

The second rule is easy to get wrong: skipping unconditionally, or letting the
landing position raise `end`, corrupts every later window (freq 2 where Lucene
says 1). The fuzz oracle catches it -- run it before any timing.

`slop == 0` keeps its path.

**2. A tf multiplier instead of a score multiplier.** The two-phase scored paths
(`probe/two_phase_scored.hpp:91`, `lead/two_phase_scored.hpp:81`) stop
publishing the boost channel and publish a per-document multiplier applied to
`tf` instead, so `tf_eff = freq * scale` inside the saturation.

`BoostBlockAttr` **becomes** that channel rather than gaining a sibling: one
per-document float, published by the same producers, read under one rule --

> a scorer that has a frequency applies it inside `tf`; a scorer that has none
> applies it to the score, which is the only expression it has.

Both readings already existed. `VectorSimilarityScorer` reads the channel as the
score itself (`vector_similarity_scorer.cpp:79`) because a vector clause has no
frequency; BM1, `raw_dl`, constant, idf and `raw_boost` are the same case and go
through `MakeScaleScore`. BM25/BM15/DFI/LM/Indri/tfidf/raw_tf have a frequency,
so they route it through the shared `ScaledFreq` helper instead of multiplying
the finished score. Two expressions of one meaning, not two channels.

With it, each cell fills in the same two fields:

| cell | freq | scale |
|---|---|---|
| fixed / fixed gaps | occurrences | 1 |
| variable gaps | occurrences | 1 |
| slop | Lucene's match count | mean `1/(1+d)` -- product is `sum 1/(1+d)` |
| variadic slot | as above | per-expansion similarity: `min` over the slots of one occurrence, `max` over occurrences |
| n-gram | sequence count | `LCS / query length` |

and they compose by multiplication when a query uses several at once, which is
what a variadic slop phrase needs.

**3. Fuzzy fidelity: where it goes, and why the answer is provisional.**

A fuzzy expansion's boost is a *query-side* weight. `quicm` is worth the same
0.5 in every document that contains it; the number describes the term's relation
to the query, not any document's use of it. That is the same kind of quantity as
idf, and idf multiplies outside the saturation. Tightness is the opposite --
document-side, different for every document -- which is why it belongs in `tf`.

Both our own standalone fuzzy and Lucene's agree with that reading:
`multiterm_query.cpp:65` folds the expansion's boost into the term query's
boost, so BM25 gets `num = boost * idf` and it lands outside; Lucene wraps each
expansion in a `BoostQuery` (`BlendedTermQuery.java:261`) for the same effect.

We are nonetheless putting it **inside** `tf` for phrases, with eyes open:

- Lucene's `MultiPhraseQuery` drops the fuzzy boost altogether, so it never
  faces this question for a phrase. Its ordering is therefore the one you get
  with no discount at all: five hits of a one-edit variant beat one exact hit.
- Inside keeps that ordering and merely tempers it. Outside reverses it, because
  no amount of repetition recovers from the discount.
- Inside also means one channel with one meaning, which is much simpler.

So this is deliberately "Lucene's ordering, slightly better", not the fully
principled answer. The principled one arrives with volatile idf, which is the
natural home for a query-side weight -- see below.

**4. Volatile idf** for disjunction slots -- see the issue notes below.

**5. `uint32` truncation.** Closed by item 1. The sweep emits at most one
match per advance, so `freq <= sum of positions <= the document's token
count` -- a uint32 quantity by construction. The overflow was reachable only
because tuple enumeration was combinatorial in the slot widths.

**6. Candidate: reduce occurrences by max, not sum.** Not decided, not
implemented. Recorded because it is cheap to keep open and it changes what the
issues should say.

Today `tf = sum_o w_o`, where `w_o` is the occurrence's goodness (`1/(1+d)` for
slop, coverage for n-gram). That is Lucene's number, and it means ten
occurrences at `w = 0.1` tie one at `w = 1.0`. The alternative:

```
tf = max_o w_o + 0.01 * sum_o w_o
```

One perfect occurrence scores `1.01`; ten poor ones score `0.11`. The best
single match sets the score and the count only adjusts it.

Why it is worth considering:

- **It is the shape the other channel already has.** Per-expansion fidelity is
  reduced by `min` within an occurrence and `max` across occurrences
  (`phrase_matcher.hpp:TakeBoost`, `Sweep`'s `res.boost`). So in one query the
  fidelity answer is "the best occurrence" while the tightness answer is "all of
  them added up". Two document-side goodness numbers, two different reductions.
- **Standalone fuzzy behaves the same way.** A distant variant repeated does not
  overtake a close one, because fidelity is applied once, not per hit.
- **For n-gram it may be the more faithful reading.** `NGramDistance` measures
  one alignment of two strings. Its natural lift to a field is the best
  alignment found, not the sum over every partial one.

What it costs:

- **It is not Lucene.** Needs the same `default` / `lucene` switch item 4
  already wants, and freq-parity tests have to say which mode they assert.
- **Repetition stops counting for slop > 0.** A document holding the exact
  phrase 100 times gets `tf = 2` instead of `100`. After saturation that is
  `0.62` vs `0.99` of the ceiling rather than a collapse, but it is a real loss
  of evidence BM25 is built on.
- **`0.01` is not actually a tie-break.** `sum_o w_o <= freq`, so the second
  term grows without bound; at `freq ~ 100` it overtakes the `max` term and the
  formula is back to counting. If the intent is "best match dominates, count
  only orders ties", the sum needs its own saturation --
  `max + (1 - max) * S/(S + K)` with `S = sum_o w_o` -- rather than a constant
  coefficient.

Implementation cost is close to zero, which is the point of recording it now:
`MatchResult` already carries both numbers (`weight` is `sum_o w_o`,
`best_distance` gives `max_o w_o`), and `scale` is a free per-document float on
an integer count, so any `f(freq, sum, best)` is expressible as
`scale = f(...) / freq`. Neither field should be dropped while this is open.

**7. Fold `JoinPair` into the sweep at compile-time arity.** Agreed order of
work: unify -> add repeated-term slop queries to the corpus -> optimise the
sweep for n == 2, n == 3 and large n -> final review. Items 4 and 6 are
explicitly out of scope for that sequence.

`JoinPair` (176 lines) and `Sweep` (124 lines) are the same algorithm;
`JoinPair` is what `Sweep` becomes at N == 2 -- `pick`/`refresh` collapse to one
comparison, the same-position scan to one, and `shift[]` to two registers.
`ResolveArity<kSlotArity, kSlotFloor>` already threads a compile-time N to the
fixed matchers, and the slop matcher currently spends it only on the `RunOf`
container. Templating `Sweep` on N and deleting `JoinPair` removes ~176 lines
and one duplicated algorithm. Safe to do now that the generic path is no longer
the slow one; it was not safe before.

### Repeated terms are slower than main, and that is structural

Measured on the 13 repeated-term queries added to the corpus (TOP_100, warm):
the other 38 sloppy queries sit at 1.01x of main, these at **2.45x**, worst
`"the end of the"~1` at 3.95x with identical counts on both sides -- so it is a
real cost, not main doing less work.

Why: the sweep advances one position at a time and may only jump when the
current run provably cannot emit. Under repeats the jump is additionally
clamped to the nearest same-group peer position, because a collision there has
to be seen -- resolving one moves another slot and changes `end`. When the
repeated term is dense both slots draw from the *same* postings list, so peer
positions are everywhere and every jump is short. Profile: 69% in the sweep,
26% in position iteration. Main instead anchors on the rarest slot and
`lower_bound`s the others, which is O(rarest * log) regardless of repeats.

Closing this means giving the Enforce path a different traversal, and the run
structure is exactly what produces Lucene's freq -- so it cannot be swapped for
an anchor-and-search enumeration without losing the number this whole document
is about. Worth doing, not worth doing carelessly.

Note the trade this buys: main is *wrong* on 7 of these 13 queries (it
disagrees with Lucene on the count), and patch matches Lucene on all 13.

## What is NOT established

Stated plainly, so the confident tone above is not read as covering everything.

- **Variable-gap matching** was read, not tested. There is no differential
  oracle for it because Lucene has no equivalent.
- **N-gram matching** is unverified. No reference implementation exists to check
  against -- Lucene has the measure only as a string distance, never as a query
  -- so "is our longest-run rule the right reading of Kondrak" is open.
- **Repeated terms in a sloppy phrase** have no differential oracle. The corpus
  holds 122 sloppy-phrase queries and none repeats a term, and the fuzz oracle's
  `advance_rpts` is the same algorithm as production `resolve()` rather than an
  independent transcription -- the priority-queue-vs-scan independence covers
  only the non-repeat machinery. What does cover it: the `SlopOverlapMatcher`
  cases, hand-verified against Elasticsearch. Narrow. Add repeated-term slop
  queries to the corpus, or transcribe Lucene's real `rptGroup` machinery, before
  treating `"foo bar foo"~2` as settled.
- **Pruning and min-match interaction** with any of the above is untouched here.

## Issues this research settles or changes

- **#1114** (slop with 3+ words matches differently) -- **fixed and verified**:
  0/408 corpus and 0/2438 unit disagreements after the merge, against 139/408
  before. Closes when the merge lands.
- **#1082** (tightness computed but never scored) -- the decision it asks for is
  answered: fold `1/(1+d)` into the frequency, not into a post-hoc multiplier.
  Becomes plan item 1+2.
- **#1083** (variadic slop drops per-term boosts) -- same fix. Once the scale is
  a tf multiplier, tightness and per-expansion similarity compose instead of
  competing for one channel.
- **#445** (volatile idf) -- confirmed as the right shape, and strictly better
  than any static reduction: the matched alternative is known per document, so
  its idf can be read rather than approximated.
- **#1112** (a slot must contribute exactly one statistic) and **#1113** (slot df
  should be max, not sum) -- both are *static* reductions of a quantity volatile
  idf reads exactly. If #445 lands they are largely superseded. Worth noting
  #1113's premise needs a check: Lucene's `MultiPhraseQuery` hands every term's
  stats to `BM25Similarity`, whose `idfExplain` **sums** them, so "sum" is what
  Lucene does for phrases; `max` is `BlendedTermQuery`, a different thing.
- **#1109** (expansion df combine mode) -- still live for expansions in general,
  but not for phrase slots if volatile idf lands.
- **#1081** (constant-score fast path) -- interacts: a per-document tf multiplier
  means a scored phrase is not constant-score, so the fast path must be gated on
  scale == 1.
- **#1051, #1103, #1104, #1105, #1106** (future phrase rules: combining,
  maxwidth, maxgaps, unbounded junction, unordered proximity) -- not outdated,
  but they now have a rule to follow: every new junction type must say what its
  contribution to `d` is, and a rule that declares a *range* of acceptable
  positions must not charge for position inside that range.
