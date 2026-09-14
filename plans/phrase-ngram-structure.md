# Phrase and n-gram: code structure

Companion to `phrase-and-ngram-semantics.md`. That one settles *what a match is
and what it scores*. This one settles *what the code is*. No semantic change is
proposed here -- every query must produce the same docs and the same scores
before and after.

## The one concept

A phrase or n-gram query is a **sequence of slots**:

- each slot resolves, **in a segment**, to a set of postings (0, 1 or many),
- each slot carries a position constraint `[offs_min, offs_max]`,
- the sequence carries a `slop` and a `min_match`.

Everything we have today is a corner of that cube:

| filter | slot arity | position | min_match |
|---|---|---|---|
| fixed phrase | all 1 | exact or interval, slop | n |
| variadic phrase | some > 1 | exact or interval, slop | n |
| n-gram similarity | all 1 | none | `ceil(n * threshold)` |

Two rules follow, and they are the whole design:

1. **A slot is a sub-filter.** Every `phrase_part` alternative already names a
   filter. A one-slot phrase *is* that filter.
2. **Slot arity is data, not type.** Whether a slot holds one term or many is
   known only after resolving the segment, so it must not be baked into the
   state type.

## What is wrong now

1. **Two states for one concept.** `FixedPhraseState` and `VariadicPhraseState`
   (`queries/phrase_state.hpp`) differ in layout, not in meaning.
   `VariadicPrepareSegment` ends by *copying one into the other*
   (`phrase_filter.cpp:522`) when every slot resolved to a single term. That
   copy is not an optimisation the plan layer may skip: `MakeVariadicPhraseOf`
   asserts `metas.size() != widths.size()` (`detail/phrase_of.hpp`), so the
   collapse is a **precondition** of the plan layer. A conversion between two
   spellings of the same data is where the boost reduction silently disagreed
   with the matcher's own `min`-within / `max`-across rule.

2. **Slot classification lives in five places and disagrees.**
   `ByPhraseOptions::insert` (cached counters), `simple()`, `PartExpands`
   (`:222`), `GetVisitor` (`:116`), `ComputeTermGroups`'s own `get_if` chain
   (`:282`). `LowerParts` (`:599`) rewrites the alternatives *underneath* the
   cached counters, so the cache can be stale -- a wildcard lowered to a plain
   term makes the prepare loop index its counter past the end.

3. **`MakeSinglePartFilter` is not total** (`:60`). It has no `TermSetOptions`
   case, so `GetKind` (`:235`) carves out an exception and a one-slot term set
   walks the whole variadic machinery. A term set *is* a boolean: Should-terms
   with per-term boost, `SetMinShouldMatch(min_match)`,
   `SetMergeType(merge_type)`.

4. **Term grouping is implemented twice.** `MakeFixedPositions` (`:251`) does an
   O(n^2) scan over *query* terms; `ComputeTermGroups` (`:282`) does union-find
   over *segment* terms. Same output field (`TermInterval::term_group`).

5. **Positions are built twice** -- `MakeFixedPositions` and the inline loop in
   `VariadicPrepareSegment` compute the same `lead_offset` running sum.

6. **n-gram borrows phrase plumbing sideways.** `ByNGramSimilarity` constructs a
   `PhraseCollector` with a zero-sized second half, and `NGramState` is
   `FixedPhraseState` with pointers replaced by values plus `total_terms`.

## Target

### A. `SlotKind` -- one classification, nothing cached

```cpp
enum class SlotKind : uint8_t { Term, Set, Expansion };
SlotKind KindOf(const phrase_part&) noexcept;
```

`ByPhraseOptions` stores no derived state: no `_expanded`, no
`_is_simple_term_only`. Counts are one pass over at most a handful of slots,
taken twice per query (collector sizing, prepare). `LowerParts` cannot
invalidate what does not exist.

- `simple()` = every slot is `Term`.
- stats sink: `Term` and `Set` -> counters, `Expansion` -> term map (see E).
- visited terms: `Expansion` only; `Term` and `Set` are read from the options.

### B. `MakeSinglePartFilter` becomes total

Add the `TermSetOptions` case: a `BooleanFilter` of `TermClause{field, scorer,
term, boost}` at `Occur::Should`, `SetMinShouldMatch`, `SetMergeType`. Then
`GetKind` loses its exception and a one-slot phrase is *always* `SinglePart`.
The same table drives `GetVisitor`, so its `SDB_UNREACHABLE` fallback goes too.

### C. `PhraseState` -- one resolved state

What n-gram and phrase share is the resolved postings and how to open them;
what phrase adds is the slot structure. So the shared part is the base, and
n-gram keeps its own name with nothing to add:

```cpp
struct PostingsState {
  ManagedVector<PostingMeta> metas;    // flat, slot-major
  const TermReader* reader{};
  detail::PhraseHandles handles;
};

struct NGramState : PostingsState {};

struct PhraseState : PostingsState {
  ManagedVector<score_t> boosts;       // empty <=> every boost is kNoBoost
  ManagedVector<uint32_t> offsets;     // n+1 prefix sums

  size_t Slots() const noexcept { return offsets.size() - 1; }
  bool Fixed() const noexcept { return metas.size() == Slots(); }
};
```

- prefix offsets replace `num_terms` plus the running `begin` cursor in the
  estimate loop and the plan loop,
- `boosts.empty()` replaces the `has_boosts` bool next to a vector,
- `metas` holds values, so the separate `ManagedVector<const PostingMeta*>`
  disappears -- one fewer allocation per segment per query, and the n-gram and
  phrase plan entry points take the same span,
- `Fixed()` is the assert in `MakeVariadicPhraseOf`, inverted. **The collapse
  disappears**: the filter picks which query class to construct, with no state
  copy. One remnant survives and has to: `PhraseFixedSlots` has no per-term
  boost channel, so a fixed-shaped segment with boosts folds them into the query
  boost by `min` -- the matcher's own within-occurrence rule -- and clears the
  vector. That is exact, not an approximation: one term per slot means one
  occurrence shape, so the per-occurrence boost is the same number every time
  and factors out of the score. Four lines in the filter instead of a 25-line
  state conversion.

  Boosts reach a phrase from exactly two term iterators -- levenshtein
  (`levenshtein_filter.cpp:100`) and term sets (`term_set.cpp:40`), whose boosts
  are themselves the levenshtein keys when fuzzy-with-`max_terms` lowers to a
  set. Prefix, range, wildcard and automaton have no `Boost()`, so `VisitTerms`
  gives them `kNoBoost`.

`FixedPhraseQuery` and `VariadicPhraseQuery` stay as types -- they are what
`PreparedStateVisitor` dispatches on and what keeps the `make_*.cpp` TUs
monomorphic -- but both hold `PhraseState`.

### D. One positions builder

`MakePositions(options)` computes `offs_min`, `offs_max`, `lead_offset` for
every kind. Term groups are computed once, by union-find, gated on `slop != 0`.
`Term` and `Set` slots contribute the terms **read straight from the options** --
they are query-static, so nothing is copied. Only `Expansion` slots need the
bytes found in this segment, and only then. `MakeFixedPositions`'s O(n^2)
query-term scan dies.

### E. Collectors: term bytes are held for expansions and nothing else

A term map exists to key counters by something that differs between segments.
That is true of an `Expansion` slot and of nothing else. A `Set` is
query-static: its terms, and their order, are the same in every segment and
every thread, so **the index within the set is a valid key** and the bytes stay
where they already live, in the options. A map there would copy every term's
bytes once per thread per segment, for nothing.

So the counter index is a running cursor over slots, advanced by the
**query-known** width -- `1` for `Term`, `set.size()` for `Set` -- never by the
number of terms actually found. That is what makes it stable across segments;
advancing by terms found is the original positional bug.

Three prepare shapes:

| slots present | counters | maps | prepare |
|---|---|---|---|
| `Term` only (plain phrase, n-gram) | one per slot | none | seek each term on one shared iterator |
| `Term` + `Set` | one per set member | none | walk the set, seek each, counter at `base + j` |
| + `Expansion` | as above | one per expansion slot | run the field visitor, key by term |

Two collector types, because shapes 1 and 2 differ only in how many counters
they size and how the prepare loop walks them -- `Finish` is the same linear
sweep over counters in both:

```cpp
class SlotsCollector : public FieldPrepareCollector {   // shapes 1 and 2
  TermCollector& Term(uint32_t thread, size_t i);
};
class ExpandedSlotsCollector final : public SlotsCollector {   // shape 3
  TermMap<TermCollector>& Expanded(uint32_t thread, size_t i);
};
```

No base table is needed anywhere: prepare walks slots in order with two running
cursors, and `Finish` sweeps counters linearly.

A `Set` also needs the index while visiting. `VisitTerms`
(`filter_visitor.hpp:38`) can report it -- a dormant `SetIndex` hook guarded by
`requires`, and `TermSetIterator::Index()` exists -- but going through a visitor
buys nothing here: the terms are in the options, so `Term` and `Set` slots are
driven directly by the prepare loop, one `field->iterator()` shared by every
such slot, seeking in order. n-gram's loop already does exactly this, which is
what makes shape 1 the same code for both filters.

**Zero counters must not be collected.** A slot term absent from every segment
keeps an all-zero counter, and `collect` on it yields
`log1p((docs_with_field + 0.5) / 0.5)` -- a maximal idf for a term that exists
nowhere. Today's `PhraseCollector::Finish` and n-gram's per-ngram counter both
do this; the map path never did, because an absent term never enters the map.
`Finish` skips counters with `docs_with_term == 0`, which is also what Lucene
sums over.

### F. n-gram adopts `PostingsState`

`ByNGramSimilarity` keeps its options, its query class and its scoring; its
state is `NGramState`, which is the shared base and nothing more -- n-gram never
reads `boosts` or `offsets`, so it must not allocate or fill them. `total_terms`
moves to the query, where it belongs: it is the query's ngram count, identical
in every segment.

## Executed

Landed as one change, A through F.

One thing went further than written: with slot arity decided per segment from
the data, `PhraseQueryKind::Fixed` and `Variadic` had nothing left to decide, so
the enum is `Empty | SinglePart | Phrase` and there is a single
`PhrasePrepareSegment`. `FixedPhraseQuery` and `VariadicPhraseQuery` survive
only as the two plan-layer shapes, chosen at the end of prepare by
`state.Fixed()`.

## Not in scope

- Matcher internals (`phrase_matcher.hpp`, `phrase_slop_matcher.hpp`,
  `ngram_matcher.hpp`) -- untouched.
- The `make_*.cpp` split per consumer API and per match mode -- untouched.
- Any change to which documents match or what they score. `min_match` x `slop`
  x intervals as a matcher-level cube is the student work tracked in
  `phrase-and-ngram-semantics.md`, not here.
