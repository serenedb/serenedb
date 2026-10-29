# Search tables -- PRIMARY KEY uniqueness enforcement

> **Status:** design, for review. This is **Pillar C** of
> [search_table_indexed_columns_2.md](search_table_indexed_columns_2.md) §5, whose
> Pillars A (always-synthetic rowid) and B (PK columns term-indexed) have landed and
> are prerequisites.
>
> **Written against `search_table_insert_memory_limiting.md`** (branch
> `dronplane/insert-memory-limiting`), which reshapes the serial write path into a
> single per-shard buffer replayed into iresearch at a threshold. That is assumed
> landed here; §6 and §7 depend on its shape. It also supplies two things this design
> uses directly: a `PreCommit` hook that can refuse a commit, and delete rowids kept
> resident for the whole transaction.
>
> **Premise: there is no read-your-own-writes** (§6.2). It makes the key index
> insert-only and removes a good deal of machinery. RYOW is expected eventually, so what
> it would take is designed and held in **§15**, deferred -- and §6.2 leaves a debug
> assert that fires the day the premise stops holding.
>
> Companion to [search_table_backfill.md](search_table_backfill.md), which shares the
> delete-log and writer-accounting machinery discussed in §6.4 and §10.
> Line numbers move; trust the symbol names.
>
> **Two corrections to §5 of the predecessor**, both load-bearing, both in §8: there
> is no lock spanning commit that certification can hang off -- only
> `SearchDbWal::AppendCommit` takes `_append_mu`, and the iresearch commits happen
> outside it; and §5.1's flat `PK -> (tick, tombstone)` pending map pins every key of a
> bulk load until the next refresh, so the delta is split by write path instead.

## 1. Where we are

A `WITH (storage='search')` table accepts a declared `PRIMARY KEY` and does not
enforce it. Inserting the same key twice yields two live documents, silently.

This is not a gap in one code path. Search tables never enter DuckDB's constraint
machinery at all:

- `SereneDBTableEntry` is constructed with `StorageAsGiven{}` and returns early for
  search ([duckdb_table_entry.cpp:276](server/catalog/entry/duckdb_table_entry.cpp#L276)),
  so `DuckTableEntry::CreateStorage`'s `UNIQUE -> ART` loop never runs. There is no ART.
- `SereneDBCatalog::PlanInsert` routes to `connector::SereneDBSearchInsert` without
  `bound_constraints`
  ([duckdb_catalog.cpp:1307-1319](server/catalog/ddl/duckdb_catalog.cpp#L1307)), and
  that operator contains no `Verify*` call. Neither does the update operator.

What the declared key *does* do today is term-index its columns.
`SearchTable::BuildPkInto` gives each PK column a term field whose id **is the column
id**, distinct from the ids user indexes allocate
([search_table.cpp:98-109](server/search/search_table.cpp#L98)), and boot rebuilds the
same set from the catalog
([catalog.cpp:276-285](server/catalog/ddl/catalog.cpp#L276)). That is what makes
`WHERE pk = ?` push down as a term filter. It is an index, not a constraint.

Row identity is separate and unconditional: every search-table row carries a synthetic
generated rowid, emitted as a term under
`catalog::term_dict::kPKFieldId` == `kGeneratedPKId.id()`
([column_id.h:47](server/catalog/column_id.h#L47),
[inverted_index.h:56](server/catalog/inverted_index.h#L56)) and stored as a BIGINT
column under the same id. Pillar A made this the identity even when a key is declared,
which is a prerequisite here: a PK-column `UPDATE` mints a fresh rowid rather than
reusing a term, so an old and a new version of a row are always distinguishable.

**Outcome:** a duplicate declared-PK value is rejected with SQLSTATE 23505, including
under concurrent committers, with no on-disk format change.

## 2. Prior art -- how DuckDB enforces a unique constraint

This design adopts DuckDB's model rather than inventing one, so it is worth stating in
full.

There is no single check. There are three ARTs per unique constraint and two
enforcement points:

| ART | Lives in | Holds | Enforced |
|---|---|---|---|
| main | `DataTableInfo::indexes` | committed rows only | read-only probe at statement time |
| local append | `LocalTableStorage::append_indexes` | this txn's rows, `row_id >= MAX_ROW_ID` | real insert during execution |
| local delete | `LocalTableStorage::delete_indexes` | committed rowids this txn deleted | consulted as an exception list |

Both delta ARTs are created by `CreateDeltaIndex` in the `LocalTableStorage`
constructor, unconditionally, for every constrained index
([local_storage.cpp:18-47](third_party/duckdb/src/storage/local_storage.cpp#L18)).

Keys are one composite byte string: `GenerateKeysInternal` builds column 0's keys then
`ConcatenateKeys` for each remaining column
([art.cpp:312-380](third_party/duckdb/src/execution/index/art/art.cpp#L312)).

Three mechanisms carry over here almost unchanged:

1. **The local append index is the self-conflict check.** Inserting into a unique ART
   returns `ARTConflictType::CONSTRAINT` on a duplicate, and `InsertKeys` undoes the
   partial chunk ([art.cpp:529-583](third_party/duckdb/src/execution/index/art/art.cpp#L529)).
   No separate structure.
2. **Locally-deleted rows leave the append index.** `LocalStorage::Delete` removes them
   outright, which is what keeps `INSERT K; DELETE K; INSERT K` legal.
3. **The committed probe is separate and deferred**, so the local index never has to
   answer for rows it does not own.

The definitive insert happens at commit, inside the window where
`DuckTransactionManager::CommitTransaction` holds the WAL lock, so two concurrent
transactions inserting the same key both succeed at statement time and the **second to
commit fails its COMMIT** -- first-committer-wins.

`INSERT ... ON CONFLICT` is lowered to `MERGE INTO` in this fork, and both are rejected
on search tables ([duckdb_catalog.cpp:1462-1470](server/catalog/ddl/duckdb_catalog.cpp#L1462)),
so upsert is out of scope. Enabling it later additionally requires `GetStorageInfo` to
advertise the PK as a unique index, because the binder derives the merge key from that
list.

## 3. Why the reader cannot be the arbiter

DuckDB's ART is a separate structure mutated synchronously *inside* the commit critical
section, so it always reflects every prior commit. The iresearch reader is not that. It
is the analog of DuckDB's row-group **data**, and it lags commit in both directions.

`SearchTableTransaction::Commit` appends the WAL record and then commits each iresearch
transaction ([search_table_transaction.cpp:171-222](server/search/search_table_transaction.cpp#L171)).
`trx.Commit` only moves the segment context into the writer's flush context; nothing is
published. Publication happens solely in `IndexWriter::RefreshCommit()`, driven by the
background refresh loop (default 1000 ms), `VACUUM (REFRESH_TABLE)`, or recovery.

The same is true of deletes: a removal is a **tick-bound filter**, not an eager mask,
applied only when the flush context is prepared
([index_writer.cpp:301](iresearch/index/index_writer.cpp#L301)).

So a probe against the reader alone both **misses** just-committed inserts and
**hallucinates** rows that a committed delete already removed. Three tiers must be
covered:

1. **published** -- the reader as of the last `RefreshCommit`;
2. **committed, unpublished** -- in no reader at all (§7);
3. **this transaction's own writes** (§6).

## 4. Semantics

### 4.1 Declaring a PRIMARY KEY is the opt-in

There is no `WITH` option for uniqueness. A table that wants term pushdown *without* a
constraint declares no PK and uses `CREATE INDEX ... USING inverted`, which now
backfills over non-empty search tables.

This is a behaviour change to document: today a declared PK buys only pushdown; after
this it also buys enforcement, so a table that declared one purely for lookup speed
starts paying certification.

### 4.2 Deferral boundary: end of statement

Two checks run at different times, and separating them is what makes the semantics
work:

- **the self-conflict check is immediate** -- it is the ART insert itself (§6.1), per
  row, during execution;
- **the probe against published and unpublished state is deferred** to end of
  statement (§8.2).

Only the second needs deferring, and the reason is that every search-table `UPDATE` is
delete-old + insert-new with a fresh rowid
([duckdb_physical_search_update.cpp:131-171](server/connector/duckdb_physical_search_update.cpp#L131)).
For `UPDATE t SET id = id + 1` over `{1,2,3}` the statement produces:

```
delete r1 (id=1)   insert r4 (id=2)
delete r2 (id=2)   insert r5 (id=3)
delete r3 (id=3)   insert r6 (id=4)
```

Final state `{2,3,4}` is unique, but a per-row probe against the reader fails: the first
chunk inserts key `2` while the row holding `id=2` is still live. PostgreSQL's default
PK rejects exactly this; its `DEFERRABLE INITIALLY DEFERRED` accepts it, as does this
design.

The rule that makes it work is **final-state**: for each inserted key, discard probe
hits whose rowid this transaction is deleting. `id=2` and `id=3` are found in the reader
but at rowids being deleted, so they are discarded, while
`UPDATE t SET id = 2 WHERE id = 1` against a *surviving* `id=2` is still rejected.

Note the immediate half needs no deferral: the ART holds only *this transaction's* keys,
and for that statement they are `{2,3,4}` -- distinct. The collision is with the reader,
not with ourselves.

**End of transaction would be wrong**, for a less obvious reason: under it the
statement-time pass could never reject anything, since a later statement might always
delete the offending row. That forfeits the error messages it exists for. Fixing the
boundary at end-of-statement makes it authoritative for the transaction's own writes and
narrows the in-lock pass to rows *other* transactions committed since -- which is exactly
the incremental re-certification in §8.3, so the two compose instead of overlapping.

Cost: a transaction momentarily in violation and repaired by a later statement is
rejected. Rare, and the error points at the statement that caused it.

### 4.3 A statement that changes no key is skipped

`DELETE` is skipped outright: it introduces no keys, so there is nothing to certify. It
contributes only to the delete set (§6.4).

**An `UPDATE` whose SET list leaves the PK alone introduces no new key value either**,
and skipping its probe is the difference between the common shape paying a term lookup
per row and paying nothing.

The obstacle is that the SET list is gone by plan time: `ForceUpdateDelAndInsert()`
returns `IsSearchTable()`
([duckdb_table_entry.cpp:365-369](server/catalog/entry/duckdb_table_entry.cpp#L365)), so
`BindUpdateConstraints` widens `op.columns` to *every* physical column
([table_catalog_entry.cpp:332-341](third_party/duckdb/src/catalog/catalog_entry/table_catalog_entry.cpp#L332)).

**Capture it before the widening -- done**, in `SearchTableEntry::BindUpdateConstraints`
([search_table.cpp:526-542](server/catalog/entry/search_table.cpp#L526-L542)): it records
`update.columns.size()` before widening, widens with `BindExtraColumns` rather than
`BindAllColumns` (which ends by zeroing `update_column_count`,
[logical_update.cpp:209-210](third_party/duckdb/src/planner/operator/logical_update.cpp#L209-L210)),
and restores the count after. `PlanUpdate` intersects it with the key columns. No
duckdb-side change was needed after all; the original plan follows for the record.

**Capture it before the widening.** `BindUpdateConstraints` is `virtual`
([table_catalog_entry.hpp:147](third_party/duckdb/src/include/duckdb/catalog/catalog_entry/table_catalog_entry.hpp#L147)),
so `SereneDBTableEntry` overrides it, intersects the original `update.columns` with
`GetPKColumnIndexes()` -- mapped through `GetColumns().GetColumn(li).Physical()`, since
the accessor is in `LogicalIndex` and `columns` is in `PhysicalIndex` -- records the
answer, then delegates. The binding behaviour stays ours; the only duckdb-side change is
a `bool` field on `LogicalUpdate` to carry it to `PlanUpdate` (bind-time only, so it
need not be serialized -- confirm nothing round-trips a `LogicalUpdate` on a path that
matters).

Two alternatives were checked and rejected. `update_column_count` already records the
pre-widening count, but is zeroed for del+insert and is load-bearing there --
`PhysicalUpdate` branches on `== 0` to select that path
([physical_update.cpp:154-162](third_party/duckdb/src/execution/operator/persistent/physical_update.cpp#L154)).
Inferring it at plan time from expression shape nearly works, since `BindExtraColumns`
gives each synthesized column a bare `BoundColumnRefExpression`
([table_catalog_entry.cpp:270-276](third_party/duckdb/src/catalog/catalog_entry/table_catalog_entry.cpp#L270)),
but a user's `SET pk = other_col` has the same shape -- distinguishing them means
resolving the binding back through the projection to the same physical column, which is
fragile and layout-coupled.

**Skipping the probe is not skipping maintenance.** The ART is still fed, or later
statements lose sight of the key and the self-conflict check goes blind.

> **Replaced in implementation (2026-10-29): no key probe, a rowid check instead.**
>
> The section's own reasoning turns out to settle more than it claims. A PK-unchanged
> `UPDATE` asserts no key *at all*: it puts back, on a new row, the key the row it is
> replacing already had. There is therefore nothing to put to a key probe -- published
> state can only answer with those same rows, which this transaction's delete set masks.
>
> What it does have to establish is that those rows are still its to replace. A
> transaction that removed one of them first has released that key, and may have given it
> to a row of its own, so putting it back resurrects a key somebody else owns. That is a
> question about **rowids**, and `SearchShardWrites::rewritten_rows` records exactly the
> ones at stake; `RefuseResurrection` intersects them with each unpublished generation's
> `removed`. Two short walks over ranges, no probe object, no key encoding, no reader
> over the statement's own segments.
>
> **It is also wider than the key probe it replaces.** A key probe only ever saw this
> when the other transaction happened to re-assert the key. The plain case -- A removes
> the row, B rewrites it without touching the key -- has no duplicate to find, and was
> silently resurrecting the row A deleted. Both shapes are now refused, with
> `ERRCODE_T_R_SERIALIZATION_FAILURE`, and both are in
> `recovery/search_table_pk_update_unchanged_key.test`.
>
> A transaction made entirely of such statements (`asserts_new_keys == false`) skips the
> key probe in the commit passes too, so it pays the rowid check and nothing else. It
> still records its `certified_mark` and still installs its generation carrying the
> re-asserted keys -- another transaction inserting one of those keys finds the published
> row masked by this one's `removed`, so the generation's keys are the only thing left
> that can reject it.
>
> **This is a corner of a larger bug, deliberately not fixed here: see §14.**

This is independent of §15: the flag means "no PK column was ever in the SET list", so
old equals new for every row without comparing them, and no old-PK projection is needed.

### 4.4 Key column types must be gated

`SwitchFieldImpl`'s `default` branch returns **before** `append_stored()` when
`term_dict::Classify` does not report a numeric kind
([search_sink_writer.cpp:686](server/connector/search_sink_writer.cpp#L686)), and the
LIST/ARRAY branch does the same for an unsupported child
([search_sink_writer.cpp:645-649](server/connector/search_sink_writer.cpp#L645)). So such
a column gets **no term at all**.

`Classify` ([inverted_index.h:70-107](server/catalog/inverted_index.h#L70)) supports only:
`VARCHAR`, `CHAR`, `BLOB`, `GEOMETRY`, `BOOLEAN`, `TINYINT`, `SMALLINT`, `INTEGER`,
`UTINYINT`, `USMALLINT`, `DATE`, `BIGINT`, `UINTEGER`, the `TIME*` and `TIMESTAMP*`
family, `FLOAT`, `DOUBLE`. Everything else is `Unsupported` -- including **DECIMAL, UUID,
HUGEINT, UHUGEINT, UBIGINT, ENUM, INTERVAL, BIT, VARINT** and the nested types.

For such a PK column every `Lookup` returns `docs_count == 0`, the probe's early-out
declares every key absent, and **uniqueness is silently unenforced**. DECIMAL and UUID
are mainstream key types; shipping a constraint that quietly does nothing for them is
the worst available outcome.

So `Classify` runs over every declared-PK column at CREATE TABLE and rejects unsupported
types outright, in the same place §4.6 rejects non-PK `UNIQUE`.

**ART's accepted set is wider**, which matters in §6.1: ART validates *physical* types,
so DECIMAL (INT16/32/64/128) and UUID (INT128) pass its constructor while `Classify`
rejects them. The gate must be the iresearch classification -- the narrower of the two --
after which the ART never sees a type it dislikes.

The column's **value** is unaffected: `WriteKeyedChunk` stores every column via
`AppendValueColumn` under the column id regardless, and only the term field is skipped.
That is what lets §5 verify composite keys from stored values.

### 4.5 NOT NULL

The catalog already records the implication --
`append_not_null(column->Logical().index);  // PK implies NOT NULL`
([duckdb_schema_entry.cpp:971](server/catalog/entry/duckdb_schema_entry.cpp#L971)) -- but
nothing checks it at runtime, since search tables verify no constraints at all (§1).

A NULL emits `irs::NullTerm()` into a *separate* null field, never into the column's own
term field, so `Lookup` reports `docs_count == 0`, the early-out declares the key absent,
and unlimited `(NULL, x)` rows are admitted. That matches SQL `UNIQUE` semantics and is
flatly wrong for `PRIMARY KEY`.

Fix: a validity-mask check over the PK columns per chunk in the insert and update sinks
(O(1) when `CheckAllValid`), throwing `not_null_violation`, **before** the key is built --
so neither the ART nor the probe ever reasons about NULL.

### 4.6 Scope

Declared `PRIMARY KEY` only.

A non-PK `UNIQUE` constraint is today accepted at CREATE TABLE and silently dropped:
`TableEntryPKColumns` skips any constraint where `!unique.IsPrimaryKey()`
([duckdb_table_entry.cpp:128](server/catalog/entry/duckdb_table_entry.cpp#L128)), so its
columns are neither term-indexed nor enforced. Turn that into an explicit
`ERRCODE_FEATURE_NOT_SUPPORTED` via the existing `RejectIfSearchTable` idiom.

`ALTER TABLE ... ADD PRIMARY KEY` must be rejected too -- see §14, it is a live defect.

## 5. The probe

This is the deferred half: does a key already exist in **published** or
**committed-but-unpublished** state? The transaction's own writes are not probed here --
§6 handles them.

### 5.1 Per segment, AND over the per-column PK terms

No new field, no format change. The loop is **per segment**, and the early-out is a
per-segment skip that is never valid globally -- a key absent from segment A can live in
segment B.

For each sub-reader:

1. Resolve `sub.field(col_id)` for each PK column. `nullptr` means no doc in this
   segment has a value for that column => skip the segment. (`SearchRemoveFilter` asserts
   non-null here, but that holds only for the *generated* PK; the probe must not copy
   the assumption.)
2. `Lookup(term)` each column. Any `docs_count == 0` => absent in this segment. Sound by
   the documented contract -- deleted docs inflate the count but never make an absent
   term appear ([formats.hpp:205-211](iresearch/formats/formats.hpp#L205)).
3. Otherwise `ReadDocs` the **smallest-`docs_count`** term, skip the segment and pending
   masks, read the remaining PK columns' **stored** values plus the rowid via
   `SegmentPkColumn` ([search_pk_lookup.h:42-57](server/connector/search_pk_lookup.h#L42)),
   discard hits whose rowid is in the delete set (§6.4), and return on the first
   survivor.

A single-column PK degenerates to one `ReadDocs`, so the common case stays cheap.

**Probe terms must come from the sink's own encoder**, never a reimplementation: numeric
columns are indexed as a precision-step trie, so a naive big-endian term matches nothing
and a coarse-shift term matches a whole bucket -- harmless given the stored-value verify,
but it wrecks the smallest-`docs_count` heuristic. Reuse `SetupTermClause` /
`TryEncodeTerm`
([search_filter_builder.cpp:170-215](server/connector/search_filter_builder.cpp#L170)).

#### What step 3 actually became (phase 3)

Step 3's **stored-value verify is not implemented**, and two things replace it. The
smallest-`docs_count` choice survives; the columnstore read does not.

- **Composite keys intersect in term space.** Each PK column's live docs for its term are
  collected and intersected; a doc carrying every column's term *is* a row with that key,
  since a PK column is scalar. The rarest term seeds the intersection, so the heuristic
  that step 3 used to pick a `ReadDocs` now picks the seed -- same signal, same effect on
  the dominant cost.
- **The delete set resolves to doc ids once per segment, not per hit.** A deleted rowid is
  itself a term under `kPKFieldId` -- the same lookup `SearchRemoveFilter` does -- so the
  constructor turns the delete set into a `DocumentMaskBuilder` and the probe passes it
  to the shared resolver alongside the segment mask. Deleted docs are then pruned *during*
  `ReadDocs` rather than filtered afterwards, so they never enter the intersection, and
  `SegmentPkColumn` is not needed by the probe at all.

**This made the trie warning above load-bearing in a way it was not before.** With the
stored-value verify gone, a coarse-shift term is no longer harmless: for a single-column
PK there is nothing to intersect against, so a bucket collision *is* the answer. It was
observed as a false conflict -- pk `12000` rejected against a published `11999`, which
share a bucket -- breaking six unrelated tests. The buffered key source was unaffected,
because `SetupTermClause` emits shift 0 only; the segment source (§5.3) reads the raw
dictionary and sees every shift.

So a key source that reads terms from a dictionary **must keep only shift-0 terms**:
`ForEachNumericTerm` writes `buf[0] = shift + TYPE_MAGIC`
([numeric_utils.hpp:270-284](iresearch/utils/numeric_utils.hpp#L270-L284)), so the test is
the leading byte against the column type's magic, and non-numeric types have no trie and
need no filter.

### 5.2 The live-hit resolver already exists

`SearchRemoveFilter::Next()` already implements "is this key alive?" -- `ReadDocs`, skip
`segment.docs_mask()` **and** `ctx.pending_docs_mask`, first live hit wins -- with the
dup-PK reasoning spelled out in its comment block
([search_remove_filter.cpp:105-161](server/connector/search_remove_filter.cpp#L105)).

Factor that core out and share it between removal and probing rather than writing a
second copy. The read path has no pending mask, so the shared resolver takes it as an
optional argument.

### 5.3 Where the probe's own keys come from

§5.1 and §5.2 say how to ask "does this key exist"; they never said what supplies the
keys to ask about. The answer below replaces the assumption the rest of §5 was written
under -- that the probe would carry the statement's keys itself -- and it exists because
the write buffer landed after §5 was written.

**Nothing is stashed at `Sink`.** By `Finalize`, every row the statement wrote is in one
of two places, and both can be read:

| rows | key source | cost |
|---|---|---|
| already flushed into this transaction's segments | those segments' PK term dictionaries | free -- encoded during replay, reused |
| still in the write buffer | `LocalTableChangesEntry::collection`, encoded with `SetupTermClause` | encoded once, nothing carried |
| bulk path, which has no buffer | segment dictionaries only | free |

The buffer survives to `Finalize` because `ClearBufferedRows` runs only on a flush
crossing, and `LocalTableChangesEntry` keeps what is needed to replay it -- `collection`,
`pk_segments` banded one entry per `Sink` chunk, and `column_ids`
([search_table_changes.h:75-82](server/search/search_table_changes.h#L75-L82)).

Three alternatives were rejected. **Carrying encoded terms from `Sink`** costs O(keys) of
non-spillable memory beside the ART and re-encodes rows whose terms already sit in a
dictionary. **Reading keys back out of the ART** would mean reversing composite
radix-encoded `ARTKey`s into per-column terms, which §5.1 forbids outright. **Forcing a
flush at `Finalize`** would make the segment side the only source, but it creates a
`buffer_trx` and a segment per statement -- a transaction of 10k small inserts would
leave 10k segments where the memory design intends one.

**Reading the segments needs no `FlushAndFsync`, and must not call it.** A
`DirectoryReader` cannot see these segments -- they are in no published `segments_N` -- so
they are opened directly: `SegmentReaderImpl::Open(dir, meta, options)` takes the
`SegmentMeta` *object*, never reading a `.sm` from disk, and returns a `SubReader` the
probe consumes unchanged (the pattern iresearch's own tests use,
[merge_writer_tests.cpp:283-291](tests/iresearch/index/merge_writer_tests.cpp#L283-L291)).

That works because `Flush()` and `FlushAndFsync()` differ in exactly the right place:
`SegmentContext::Flush` writes the segment's **data files** and records a
`FlushedSegment` carrying the meta in memory
([index_writer.cpp:1063-1097](iresearch/index/index_writer.cpp#L1063-L1097)); only
`FlushAndFsync` adds the `.sm` meta file and the fsync. So the rows are *readable* long
before they are *durable*, which is all a key source needs -- nothing durable depends on
what it reads, and a crash before commit discards the segments and the transaction
together.

Calling `FlushAndFsync` at `Finalize` instead would be wrong twice over: it asserts
`!entry.meta_on_disk` ("called twice on one transaction",
[index_writer.cpp:821-823](iresearch/index/index_writer.cpp#L821-L823)) and `buffer_trx`
outlives the statement, so a second statement would trip it; and the commit path pushes
`segment.filename` for everything the call returns, so a second call would duplicate
segment references in the WAL record.

What this needed was one additive accessor, `IndexWriter::Transaction::Flushed()`,
returning the current `flushed` span without flushing.

**Two implementation requirements the table above does not imply:**

- **Filter the dictionary to shift-0 terms** -- see §5.1. The segment source reads raw
  dictionary terms, so it sees every trie shift; the buffered source does not, because
  `SetupTermClause` emits shift 0 only.
- **Track per-doc column presence explicitly, not by empty term.** Inverting a dictionary
  gives term -> docs, so building doc -> key means filling a slot per column per doc. An
  absent slot cannot be detected by testing the term for emptiness: a `VARCHAR` key whose
  value is `''` encodes to an empty term, and treating that as "column missing" silently
  drops the row from certification and misses its duplicate. Use a presence mask.

Two consequences:

- **Composite keys need §6.3's treatment on the segment side.** A term dictionary holds
  distinct values of one column, not whole keys. For PK `(a, b)`: iterate the driving
  column's dictionary, `ReadDocs` within our own segment, read the remaining PK columns'
  stored values for that doc, form the key, then probe. A single-column PK degenerates to
  iterating the dictionary.
- **Dictionary terms are deduplicated, and that is harmless.** Duplicates among this
  transaction's own writes were already rejected by the ART (§6.1), so the key set
  reaching the probe is distinct by construction.

This also unifies the work: the segment-dictionary reader serves the flushed inline rows
*and* the whole bulk path, so §6.3's reader is built once and both feed it. The buffer
reader is the only piece unique to the inline path.

### 5.4 Deferred: the published probe should be a merge join, not N lookups

**A separate step, in its own `perf:` PR -- not part of this feature**, and the first
entry of §16, where the rest of the deferred optimizations are indexed. It changes
`UniqueProbe`, which predates this work and already carries the published probe for every
key shape, so it stands alone and needs end-to-end numbers against main rather than a
correctness argument.

Profiling sustained serial single-column-PK inserts put `UniqueProbe::MayHold` at the top
of certification -- several times the cost of building the buffered key source, an order of
magnitude above the candidate sort -- with the FST walk inside it accounting for most of
that. The reason is structural rather than incidental.

`TermReader::Lookup` is **stateless by design**: its own comment says it walks the
dictionary on the stack so that an exact-match probe costs no iterator at all, which is
optimal for one probe and pessimal for a sorted stream of them. `FindDuplicateKey` issues
exactly such a stream -- one probe per distinct seed term, per published segment, each
starting at the FST root.

`SeekTermIterator` resumes instead. `SeekToBlock` opens with a `SeekCached` arm that
compares the target against the position already reached and restarts from the divergence
point, loading a new block only when one is crossed
([burst_trie.cpp:1771-1790](iresearch/formats/index/burst_trie.cpp#L1771-L1790)).

**The change:** give `UniqueProbe` one persistent `SeekTermIterator` per published segment
for the seed field, created lazily, and replace the per-key `Lookup` with `seek()`. The
walk already delivers seed terms in strictly ascending order -- the same property the
candidate-window flush relies on -- so consecutive seeks resume rather than restart, and
for a numeric key, whose consecutive full-precision terms differ only in their last bytes,
almost the whole walk is skipped. `seek()` beats `seek_ge()` here because a miss is
answered as soon as the containing block reports no terms, where `seek_ge` must read the
block after it to have somewhere to stand; the probe wants a yes/no and never reads the
value.

It also removes a double walk. For a single-column key `Exists` opens with the same
`Lookup` that `MayHold` just did, so every hit pays the descent twice; an iterator already
standing on that term makes the second free.

**Three things to settle before building it:**

- It turns `UniqueProbe` from a stateless query object into one carrying an ordering
  precondition. That coupling belongs at the API, with a debug-only assert that terms
  arrive monotonically.
- `seek()` on a miss leaves the iterator wherever answering took it, so the value must not
  be read -- which this never does. Confirm it also leaves the cached state valid for the
  *next* seek before resting on it.
- The profile above is from a dev build. Re-measure under the `perf` preset on a quiet
  machine, before and after ([cpp.md](.claude/rules/cpp.md)).

## 6. This transaction's own writes

### 6.1 The key index: a unique ART, maintained during execution

Each `(transaction, shard)` with a declared PK gets one **unique `duckdb::ART`**, keyed
by the composite PK and carrying the generated rowid as its payload. It is fed in
`Sink`, per row, as the rows are produced -- and **inserting into it is the self-conflict
check**: a duplicate returns `ARTConflictType::CONSTRAINT` and `InsertKeys` undoes the
partial chunk. There is no second structure and no separate intra-statement pass.

**Its lifetime is the transaction, and it is deliberately independent of the write
buffer.** This is the single most important property, because the memory design clears
the buffer at every threshold crossing and defers `FlushAndFsync` to commit. Deriving
keys from the buffer would therefore see only the rows since the last crossing, and a
large insert would certify almost nothing. Feeding at `Sink` makes the question moot:
where a row physically sits -- buffer, in-memory segment, flushed segment -- stops being
something certification has to ask.

Two consequences worth stating, because both delete machinery:

- **This transaction's own segments are never probed *for conflicts*.** The ART already
  holds every key the transaction inserted, so self-conflicts need nothing opened,
  fsynced early or merged. (§5.3 does read those segments at `Finalize`, but only to
  recover the key *values* to probe published state with -- never to answer "is this a
  conflict". The two uses are disjoint.)
- **ART hits need no delete filtering.** §6.2 keeps the index accurate, so anything
  still in it is live. Tier (c) is a plain lookup.

**Why an ART rather than a hash map -- memory.** Everything else on the write path is
bounded: the write buffer is capped at `checkpoint_wal_size` (16 MiB by default) and is
buffer-managed, and iresearch's segments roll at `segment_memory_max`. The key index is
not bounded by either -- it grows with every row the transaction inserts, and a
long-running transaction is bounded only by its own length. ART nodes live in
`FixedSizeAllocator`s over a `BlockManager`, so they spill.

**Construction is feasible and cheap.** The expected blocker is not one:
`StorageManager::GetTableIOManager(nullptr)` ignores its argument and returns a
non-owning handle, so no `DataTable` is needed
([storage_manager.cpp:874-878](third_party/duckdb/src/storage/storage_manager.cpp#L874)).
And `FixedSizeAllocator`'s constructor allocates nothing -- two stored references, the
segment size, and a small integer loop
([fixed_size_allocator.cpp:8-35](third_party/duckdb/src/execution/index/fixed_size_allocator.cpp#L8))
-- with buffers arriving lazily. DuckDB already does strictly more on its hot OLTP path
(§2: two delta ARTs per constrained index, unconditionally), so one ART per transaction
is cheaper than what it treats as normal. **Do not build a fast path around this cost.**

**`unbound_expressions` and `column_ids`.** They define the key shape -- the constructor
derives `types` / `logical_types` from their return types, which is what ART
type-validates and `GenerateKeys` switches on -- and say how to project key columns out
of an input chunk. The indirection matters: leaves are `BoundColumnRefExpression`s whose
`ColumnBinding.table_index` is a **dummy** and whose `column_index` indexes the
**`column_ids` vector**, not the chunk
([bound_index.hpp:79-89](third_party/duckdb/src/include/duckdb/execution/index/bound_index.hpp#L79)).
`BindExpression` resolves it to `BoundReferenceExpression(column_ids[column_index])`
([bound_index.cpp:162-169](third_party/duckdb/src/execution/index/bound_index.cpp#L162)),
which indexes the chunk directly. So expression *i* -> `column_ids[i]` -> chunk position,
and **the chunk passed to `Append` must match the layout `column_ids` describes** -- here,
the sink's chunk, with `column_ids` set to the PK columns' offsets in it.

**Cache those expressions once per `SearchTable`**, built from `_pk_columns`, and hand
them to each per-transaction ART. DuckDB's delta ART borrows the parent index's
`unbound_expressions` via `CreateDeltaIndex` and so never re-binds per transaction; with
no parent ART the shard has to play that role.

Pass a fixed `StorageVersion` -- it affects key generation, and any consistent choice
works since this never persists. Create the ART lazily on the first row, so a zero-row
statement allocates nothing.

### 6.2 The index is insert-only, because there is no read-your-own-writes

Nothing this transaction inserted can ever be deleted or updated by the same
transaction, so **the ART is never removed from**. Feed at `Sink`, clear on TRUNCATE,
and that is the whole maintenance story.

The reason is visibility. Both scan sites take `GetDirectoryReader()`
([duckdb_table_entry.cpp:342](server/catalog/entry/duckdb_table_entry.cpp#L342), `:636`),
which is the published-only `_committed_reader`, and there is no overlay; a
transaction's own writes sit in the write buffer or an unflushed iresearch transaction
and reach no reader until commit *and* a refresh. `IsStableSnapshot()` then freezes even
that reader after the first DML. So an `UPDATE` or `DELETE` scan physically cannot match
a row this transaction wrote -- the write-buffer test says the same in passing, that
without a refresh "the UPDATE below sees an empty table and silently updates nothing".

What follows, and each of these is machinery this design does **not** need today:

| | needed? |
|---|---|
| ART removal by key | no -- own-row deletes cannot occur |
| a band gate distinguishing own rowids | no -- it would always answer "not ours" |
| PK columns projected on `DELETE` | no |
| old-PK values projected on a PK-changing `UPDATE` | no |
| re-pointing an entry on a PK-unchanged `UPDATE` | no |

`UPDATE` needs no special handling at all: its `Sink` calls `AddInlineInsertChunk` with
a fresh `pk_base` exactly like an insert, so the new key enters the ART and the old row
-- published, never ours -- needs nothing. `DELETE` touches the ART not at all; it
contributes only to the delete set of §6.4, which masks probe hits against published
rows.

> **Guard the premise.** Keep a debug-only `SDB_ASSERT` that a deleted rowid never falls
> inside one of this transaction's reserved `pk_base` runs. That is the no-RYOW
> assumption stated as a check, so if read-your-own-writes lands it fails loudly instead
> of the ART going quietly stale. §15 is what to do when it does.

**Confirmed empirically** on a search table with `id BIGINT PRIMARY KEY`: inside a
transaction, `SELECT count(*)` after an `INSERT` returns `0`, and an `UPDATE` targeting
that row reports `UPDATE 0`. The transaction can neither see nor modify its own writes.

Two semantics follow from the same fact, and both are worth pinning in tests because
they read like bugs otherwise -- **both were reproduced, not merely reasoned about**:

- **`INSERT K; DELETE K; INSERT K` in one transaction is rejected.** The DELETE cannot
  see the first insert, so it removes the published row with key `K` and both inserted
  rows stay live. Observed against a published `(7, 'published')`: the transaction left
  `(7,'a')` and `(7,'b')` -- two rows, one distinct id. A genuine duplicate, which the
  ART's own uniqueness catches. (Under RYOW it becomes legal -- §15.)
- **Two `UPDATE`s of the same row in one transaction are rejected.** The frozen snapshot
  makes the second re-target the same published rowid, so it inserts a second
  replacement rather than superseding the first. Observed: one row `(5,'orig')`, then
  `BEGIN; UPDATE...'first'; UPDATE...'second'; COMMIT;` left **two** rows, `(5,'first')` and
  `(5,'second')`, the original gone.

> **Accepted as a lesser wrong, not as correct behaviour.** Updating the same row twice
> in one transaction is ordinary, valid SQL and ought to succeed; raising 23505 for it
> is the wrong answer. It is accepted here only because the alternative on that path is
> silently duplicating the row, and an error beats corruption.
>
> **This feature neither causes nor fixes the underlying defect.** The duplication comes
> from the frozen snapshot, not from anything PK-specific -- the identical sequence on a
> table with **no** `PRIMARY KEY` was observed to leave the same two rows, and there
> nothing catches it, because the key index exists only on the PK path. Search tables
> have no PK by default, so the silent case is the common one.
>
> It therefore needs a separate fix that **must not depend on a declared key**, since
> most affected tables have none. RYOW (§15) is the candidate: making the second
> `UPDATE` see the first's output is exactly what stops it minting a second row. Until
> then, do not build anything on top of the 23505 -- it is a symptom being surfaced on
> one path, not a designed behaviour -- and keep it out of any claim that uniqueness
> enforcement addressed the duplicate-row problem.

### 6.3 The parallel path keeps a term-dictionary merge

A parallel `INSERT` (`GetMaxThreads() > 1`) is untouched by the memory design: no
buffer, a per-thread exclusive transaction, `FlushAndFsync` at `Combine`, segments
referenced from the record. Feeding one ART from N sink threads would contend, and the
segments are already durable before `Finalize`, so self-conflicts there are found by
**merge-scanning the transaction's own PK term dictionaries** -- sorted, via
`TermReader::iterator()`, so O(terms) and sequential with no per-key lookups.

A transaction doing both composes: the ART covers the serial keys, the merge covers the
parallel segments, and each is checked against the other.

For a composite key the merge runs on the driving column and the rest are verified from
stored values -- "column `a` has a repeated term" is not a violation for PK `(a, b)`.

#### Notes for whoever implements this (phase 2, 2026-10-21; updated phase 3)

**This is the one case still unenforced after phase 3, and §13's "parallel insert with a
self-duplicate" is a KNOWN-FAILING test until it lands.** Measured on the phase 3 build:

```sql
CREATE TABLE d (id BIGINT PRIMARY KEY, s TEXT) WITH (storage='search');
SET threads = 8;
INSERT INTO d SELECT i % 1000, repeat('z',200) FROM range(1,200000) t(i);
-- accepted: 199999 rows, 1000 distinct ids
```

The gap is where two assumptions meet. The ART (§6.1) covers self-conflicts but is fed
only on the serial branch, so bulk rows never enter it. The probe (§5) covers published
conflicts and deliberately never probes this transaction's own segments (§6.1). Bulk
**self**-conflicts fall between the two, and that is precisely what the merge below is
for.

Note the state this leaves: after phase 3 a declared PK *looks* enforced on the bulk path
-- a bulk insert duplicating a **published** key is rejected, via §5.3's segment key
source -- while duplicates **within** the same statement are silently accepted. Partial
enforcement reads as worse than none, so this should not ship without the merge.

Most of the groundwork exists. `SearchTableTransaction::CertifySegment` already
materialises doc -> key terms per segment to feed the probe, so:

- **within one segment**, a single-column PK term with `docs_count > 1` is a duplicate
  outright -- one `Lookup`, no new machinery; for a composite key the inverted map already
  built makes it a duplicate check over its values;
- **across segments**, the keys of one segment must be checked against the others, since
  parallel sink threads each write their own. That is the actual merge-scan described
  above.

**Phase 2 context, still true.** Only two `AddKeys` call sites exist, both serial: the
insert sink's `else` branch and the update sink. `if (lstate->bulk)` goes straight to
`WriteChunkToSearchSink` with nothing checking keys. And `bulk` is
`has_local_state.exchange(true) || GetMaxThreads() > 1`, so on a multi-threaded pipeline
*every* thread is bulk. `search_table_pk_duplicates.test` says nothing about this,
because literal `VALUES` inserts stay serial -- a small `INSERT ... SELECT` does too, so a
bulk test must be large enough to actually parallelise or it silently exercises the
inline path instead.

**This is DuckDB's strategy too, not a divergence from it.** The rationale above
("feeding one ART from N sink threads would contend") is true but is not the reason
DuckDB avoids it. `PlanInsert` parallelises inserts into indexed tables without
consulting the indexes at all
([plan_insert.cpp:103-125](third_party/duckdb/src/execution/physical_plan/plan_insert.cpp#L103-L125));
what it does instead is **touch no index during the parallel phase**. The serial branch
of `PhysicalInsert::Sink` calls `LocalStorage::Append`, which appends to `append_indexes`
per chunk; the parallel branch stages into a per-thread `RowGroupCollection` via an
`OptimisticDataWriter` and indexes nothing
([physical_insert.cpp:99-135](third_party/duckdb/src/execution/operator/persistent/physical_insert.cpp#L99-L135)).
Its `VerifyAppendConstraints` only sees already-visible rows, so it cannot catch a
concurrent peer. The index is populated once, single-threaded, at flush, by
`LocalTableStorage::AppendToIndexes` re-reading the staged collection
([local_storage.cpp:162](third_party/duckdb/src/storage/local_storage.cpp#L162)) -- and
that is where a duplicate between two parallel threads is actually caught.

So the model is **stage in parallel, index serially afterwards over the staged data**,
and the term-dictionary merge is that model expressed in our storage: DuckDB re-reads
staged row groups, we re-read staged segments. Ours is the cheaper half of the trade,
because the staged form is already an inverted index and the keys come back sorted.

**Reuse the rowid vector the bulk writer already builds.** `WriteChunkToSearchSink`
mints `gen_pk` from `pk_base` in its own loop
([search_sink_writer.cpp:1203-1208](server/connector/search_sink_writer.cpp#L1203-L1208))
before handing it to `WriteKeyedChunk`. Enforcement on this path should take that vector
rather than building a second one. Note the serial path has no such duplication --
`AddInlineInsertChunk` stores only `{pk_base, chunk.size()}` and materialises rowids at
flush -- which is why `SearchKeyIndex::Insert` builds its own there.

**The cross-statement probe should use DuckDB's other error form.** DuckDB words its two
paths differently: the ART insert says `PRIMARY KEY or UNIQUE constraint violation:
duplicate key "<value>"`, which phase 2 matches, while the conflict-manager path that
checks against committed data says `Duplicate key "<column>: <value>" violates primary
key constraint.` -- naming the column. §5's probe is the second case, so it should match
the second form.

### 6.4 The delete set

Deletes are tracked **by rowid**, which is what the memory design already buffers: each
buffered `Op` carries its own `std::vector<int64_t> delete_rows`, positioned against the
insert bands by a `band_watermark`
([search_table_changes.h:50-61](server/search/search_table_changes.h#L50)). Eight bytes
per delete, and no PK values anywhere on the path.

It serves the §5 probe, not the ART: a hit in the published reader or the delta must be
discarded when this transaction has deleted that row. Since a transaction can only ever
delete rows it did not write (§6.2), **every** delete it records is of exactly the kind
the probe needs to discount -- there is no own-row category to separate out.

The same set is promoted into the delta at commit, and it has to stay complete there:
a removal is a queued tick-bound filter applied at `PrepareFlush`, not at
`FlushAndFsync`, so a segment handed to the delta still contains the doc unmasked -- the
memory design describes the same effect from the replay side ("replay adopts the
unmasked segment and re-applies D1 to both"). Promoting straight from the buffered ops
gets this right without a second structure.

**A plain set, not an ART.** The transaction-resident delete memory is dominated by
something else entirely: from the first flush to commit, iresearch holds a
`SearchRemoveFilter` per removal in `SegmentContext::queries`, carrying every delete key
as an `irs::bstring`, never cleared outside `Reset()`. The memory design calls this floor
irreducible without changing iresearch. Against that, a rowid set is not the term worth
optimising, and making it spillable would move nothing.

**It must not share the backfill's `_delete_log`**
([search_table.h:228-246](server/search/search_table.h#L228)). The retention policies are
incompatible: the build drains its log at its own pace (`TakeDeleteLog` /
`SwapWithDrainedDeletes`), while uniqueness must retain a delete until it is *published*.
Sharing breaks both ways -- a build drain removes entries uniqueness still needs, so the
probe sees the row in the reader again and **rejects legal inserts**; and uniqueness
pruning removes entries the build has not consumed, those removals are never reissued,
and rows **resurrect**, which is what `search_table_backfill_concurrent_dml.test` guards.
Neither window contains the other, so neither can subsume the other.

Share the capture point instead: one walk of the buffered delete ops, fanned out to
whichever consumers are active -- `IsDeleteLogOpen()` for the build, "table has a
declared PK" for uniqueness.

## 7. The pending delta

### 7.1 What each write path leaves behind at commit

| path | at commit | contributes |
|---|---|---|
| serial, buffer crossed the threshold | remainder replayed, `FlushAndFsync` once; record carries no inline rows | `SegmentRef`s |
| serial, never crossed | pooled transaction, replayed; record carries rows inline | keys |
| parallel `INSERT` | per-thread segments, fsynced at `Combine` | `SegmentRef`s |

That split is *forced*, not chosen. `SegmentContext::Commit` only stamps ticks -- on
`queries`, `flushed_docs`, and `writer->docs_context()`, the last being docs still in the
in-memory `SegmentWriter` -- and flushes nothing
([index_writer.cpp:1203-1225](iresearch/index/index_writer.cpp#L1203)). Rows that never
reached a flushed segment cannot be named by reference, so they have to travel as keys.

It also bounds the keyed half usefully: a transaction that crossed has **all** its rows
in segments by commit, so it promotes `SegmentRef`s and **drops its ART** rather than
pinning 20M keys until the next refresh; a transaction that never crossed promotes its
ART, which is under the threshold by construction.

### 7.2 Shape

Per shard. Every field holds work from transactions that have **already committed** and
whose rows `RefreshCommit` has **not yet published** -- durable, in no reader, and
therefore invisible to a probe that consults only the reader. All of it is pruned at
publish.

One **generation per commit**, tick-tagged, dropped whole, nothing ever removed by key:

```
struct PendingGeneration {
  uint64_t                  tick;
  vector<irs::SegmentReader> segments;  // rows its transaction flushed
  vector<KeyTerms>           keys;      // rows it did not
  vector<int64_t>            removed;   // rowids it deleted
};
```

**`segments` are committed segments awaiting publication** -- not the transaction's *own*
segments, which §6.1 and §6.3 handle and which never enter the delta before commit.

A row is in exactly one of the first two fields, decided by the write path that produced
it (§7.1): a transaction whose buffer overran flushed its rows into segments, one that
stayed under the threshold left them in an unflushed iresearch transaction that nothing
can open, so its keys travel instead -- bounded by the same threshold that kept them
there.

The keyed half needs no masking: a generation holds the keys of one commit's live rows,
and a removal of one of them is a rowid in whichever generation recorded it, which only
ever masks a doc the reader still shows (§6.4).

> **Revised in implementation (2026-10-27): the keyed half is `KeyTerms`, not the
> promoted ART.** §7.1 had the committing transaction hand its ART to the delta. That
> does not compose with the certification that landed in phases 2-3, which is term-based
> end to end: `FindDuplicateKey` walks segment dictionaries and produces per-column
> terms, while an ART answers to a composite radix-encoded `ARTKey` built from DuckDB
> values. Probing one with the other needs the term-to-value bridge §5.1 forbids
> outright.
>
> So the keys are encoded once at `PrepareCommit`, from the buffer that still holds their
> values, with `EncodeRowKeys` -- the same encoder the walk's own row source uses, which
> is what makes the bytes a commit stores and the bytes a later probe looks for the same
> bytes by construction rather than by agreement.
>
> It costs one encoding pass over the surviving buffer per commit, bounded by the flush
> threshold. In exchange the ART stays purely the self-conflict check of §6.1 and never
> leaves its transaction, which retires half of §14's "two key encodings ... must stay
> consistent": the `ARTKey` form is now confined to a single transaction's own rows.

### 7.3 Eager segment references

At publish, `need_flush = was_flush || !document_mask.empty()`
([index_writer.cpp:2483-2495](iresearch/index/index_writer.cpp#L2483)); on `need_flush`
the version is bumped and `FlushIndexSegment` writes a **new** meta file, orphaning the
name a `SegmentRef` holds so `RemoveAllUnreferenced` can reclaim it before we prune.

`FlushAndFsync` deliberately sets only `meta_on_disk`, *not* `was_flush` -- its comment
says `was_flush` "means 'published before' and forces a version bump"
([index_writer.cpp:706-745](iresearch/index/index_writer.cpp#L706)) -- precisely so
WAL-referenced segments keep their filename. So the condition reduces to **"a removal
landed on this segment before it was published"**, which after the memory design is no
longer exotic: a transaction that interleaves inserts and deletes replays both into the
same iresearch transaction.

`AdoptSegment` already takes this reference for the same stated reason -- *"Refs are still
needed, or the cleaner reclaims the segment before the commit publishes it"*
([index_writer.cpp:1733-1741](iresearch/index/index_writer.cpp#L1733)). One
`directory_utils::Reference` per segment at delta-install pins the file, so the open can
stay lazy; release it when the entry is pruned.

> **Revised in implementation (2026-10-27): the generation opens its readers instead.**
> The install calls `OpenFlushedSegment` per flushed segment and holds the
> `irs::SegmentReader`, which answers the same hazard with no new API: an open reader
> keeps reading after an unlink, where a filename would be left pointing at nothing. It
> is also the call `CertifyKeys` already makes on exactly these segments, so the delta
> reads them the way the certifier already does rather than by a second route.
>
> The cost is the reader's handles held until the next refresh rather than a bare
> reference. If that shows up as fd or memory pressure under a bulk-insert load, the
> `directory_utils::Reference` above is the frugal replacement and the probe side does
> not change.

### 7.4 Pruning

After `RefreshCommit()` returns true **and the new reader is installed**, drop
generations whose tick is at or below a bound captured *before* `RefreshCommit` started --
mirroring the existing `tick_before = _wal->CurrentTick()` pattern in
`SearchTable::RefreshUnsafe`
([search_table.cpp:490-502](server/search/search_table.cpp#L490)). Dropping before install
would reopen the window.

**Why that bound is safe.** `PrepareFlush` swaps the flush context and then calls
`ctx->pending.Done(); ctx->pending.Wait()`
([index_writer.cpp:2087-2091](iresearch/index/index_writer.cpp#L2087)), so any transaction
registered on the old context blocks the publish until it settles. And
`query::Transaction::Commit` calls `RegisterFlush()` **before** `AppendCommit` assigns the
tick ([transaction.cpp:218-242](server/query/transaction.cpp#L218)). So a transaction whose
tick is at or below the bound necessarily registered before the swap, the publish waited
for it, and its rows are in the new reader. Anything else has a tick above the bound and
is not pruned.

> **Invariant to preserve:** `RegisterFlush` must keep preceding tick assignment,
> including for delete-only transactions. Reordering them silently breaks pruning, and the
> symptom is a rare missed duplicate rather than a crash. `tick_before` must also be read
> before `RefreshCommit`, and `RefreshCommit` must keep its default `kMaxTick`.

Removals are covered by the same publish: `PrepareFlush` folds queued removals into the
segment document masks, and `Finish()` installs the assembled reader with one
`atomic_store` ([index_writer.cpp:2635-2656](iresearch/index/index_writer.cpp#L2635)) -- so
there is no window where a delete is pruned but not yet masked.

Only the insert side needs this bound. Over-retaining a delete is free, since rowids are
never reused and a stale entry can only mask an already-dead doc, while under-retaining
one costs a spurious error.

## 8. Concurrency

> **Reading the rest of this section.** It was written when the serializing lock was
> `SearchDbWal::_append_mu`, which #1396 deleted along with the search WAL. Every later
> mention of `_append_mu`, `AppendCommit` or `WriteFrameLocked` means **DuckDB's
> per-database commit lock and the commit it spans**, as §8.1 re-anchors them; the
> arguments built on them are unaffected, since the two locks have the same scope and the
> same relation to the point of no return.

### 8.1 The critical section

Certification must be serialized against other committers and must happen while the
transaction can still abort. Both pin it to one place.

> **Re-anchored (2026-10-27).** This section was written against `SearchDbWal`, whose
> `_append_mu` was the one lock spanning the search commit.
> [#1396 (`09f655164`)](server/search) folded the search WAL into the DuckDB WAL and
> **deleted `SearchDbWal` outright**, so the lock named here no longer exists. What
> follows is the same argument re-anchored on the fork's commit path, which is where the
> serialization now lives. The conclusion is unchanged; the mechanism is not.

**DuckDB's per-database commit lock is the replacement.** `CommitTransaction` takes
`held_commit_lock = db.GetStorageManager().GetCommitLock()`
([duck_transaction_manager.cpp:772](third_party/duckdb/src/transaction/duck_transaction_manager.cpp#L772))
and holds it across `AppendLocalStorage`, `WriteToWAL` and the undo-buffer commit,
releasing it only at
[:907-909](third_party/duckdb/src/transaction/duck_transaction_manager.cpp#L907-L909).
It is exclusive, one per attached database, and taken by every commit that made changes
-- the same scope `_append_mu` had.

**The point of no return inside that hold is `transaction.Commit(...)`**
([:830](third_party/duckdb/src/transaction/duck_transaction_manager.cpp#L830)). Before it
an error rolls the transaction back; after it the commit is applied and a throw only
invalidates the database. So certify before it, install after it, both under the one
hold:

```
take commit lock -> certify -> AppendLocalStorage + WriteToWAL -> transaction.Commit
                 -> install the delta entries -> release
```

> **The install must be inside the lock, after the point of no return.** If the delta
> entry lands after the commit lock is released, T1 can certify, write its WAL and
> release while its rows are in neither the reader (not yet published) nor the delta -- so
> T2 certifies the same key and both commit. Installing *before* the WAL write is equally
> wrong: an I/O failure there still rolls back, and an aborted transaction's keys would
> already be visible to everyone else.

**The install point already exists.** `TransactionPreCheckpoint` fires at
[:864](third_party/duckdb/src/transaction/duck_transaction_manager.cpp#L864), under both
`transaction_lock` and the commit lock and after `transaction.Commit`, which is exactly
the post-no-return position the install wants. SereneDB already hooks it --
`StageSearch` assigns the ticks there -- so the delta install joins work that is
already on that path. `TransactionDurable`, where `PublishSearch` commits the iresearch
transactions, fires *outside* both locks
([:912](third_party/duckdb/src/transaction/duck_transaction_manager.cpp#L912),
[:935](third_party/duckdb/src/transaction/duck_transaction_manager.cpp#L935)) and is
therefore **not** a usable install point: the window between releasing the lock and
publishing is the hole the warning above describes.

**The certify point does not exist yet, and that is the one fork change this needs.**
Between taking the commit lock ([:772](third_party/duckdb/src/transaction/duck_transaction_manager.cpp#L772))
and the point of no return there is no `ClientContextState` hook on this path. The
two-phase path already has the hook in the right place -- `PrepareTransaction` takes the
commit lock and then runs `TransactionPreWalWrite`, whose throw becomes a returned
`ErrorData` and rolls the commit back
([:552-562](third_party/duckdb/src/transaction/duck_transaction_manager.cpp#L552-L562))
-- so the change is to fire the same hook at the same relative position in
`CommitTransaction`, making the two paths symmetric rather than adding a new concept.

**Both paths are reachable and neither can be skipped.** `MetaTransaction::Commit`
chooses between them on `CatalogLogForCommit`
([meta_transaction.cpp:178-208](third_party/duckdb/src/transaction/meta_transaction.cpp#L178-L208),
dispatch at [:418-421](third_party/duckdb/src/transaction/meta_transaction.cpp#L418-L421)),
which returns null -- the plain `CommitTransaction` path -- when the transaction made no
catalog changes and wrote one database. **That is the common case for a plain INSERT into
a search table**, so the fork change is on the hot path, not an edge case; DDL and
cross-database writes take the prepared path and are already covered.

Nothing that touches the writer's commit path (`GetStats`, `BufferedDocs`,
`RefreshUnsafe`) may be called under the commit lock: a slow certification already blocks
the refresh through `RegisterFlush`, and reaching back into the writer would close the
cycle.

### 8.2 Where each check runs

| check | when | covers |
|---|---|---|
| PK `NOT NULL` | `Sink`, per chunk | §4.5 |
| self-conflict | `Sink`, per row -- the ART insert itself | §6.1 |
| ART maintenance | `Sink`, per deleted/updated row | §6.2 |
| probe vs published + delta | `Finalize` | §5 |
| definitive re-probe | inside `_append_mu` | §8.3 |

The probe **cannot run in `Sink`**: the update operator deletes old rowids and inserts
new rows per chunk, so mid-statement the delete set is incomplete. For
`UPDATE t SET id = id + 1` the first chunk inserts key `2` while the row holding `id=2`
is still live and not yet in the delete set -- a probe there reports a false conflict on
the very first row. `SereneDBSearchUpdate::Finalize` already exists as an empty
passthrough
([duckdb_physical_search_update.cpp:188-193](server/connector/duckdb_physical_search_update.cpp#L188))
and is single-threaded.

The probe must use a **fresh** `GetDirectoryReader()`, never the transaction's cached
one: `IsStableSnapshot()` becomes true after any DML and `OnStatementEnd` then
deliberately skips `ResetReaders()`
([transaction.cpp:84-154](server/query/transaction.cpp#L84)), so the cached reader is
frozen. A stale reader would reject an insert against a row another transaction already
deleted.

### 8.3 Incremental re-certification

The `Finalize` probe captures a consistent `(reader, delta)` pair at tick `T_s`; the
in-lock pass re-certifies only against delta generations with `tick > T_s`. This is
complete -- everything published by `T_s` is in `reader_Ts`, everything
committed-but-unpublished at `T_s` is in `delta_<=Ts`, and everything since is in
`delta_>Ts` -- so in-lock work is bounded by the concurrent window rather than by the
number of keys inserted.

### 8.4 Probe / prune interlock

`RefreshUnsafe` does not take `_append_mu`, so pruning races probing: a prober can
snapshot the old reader, have the refresh install the new reader **and prune**, then read
the pruned delta -- missing keys that now live only in the new reader.

A per-shard `delta_mu` is held by the installer (nested inside `_append_mu`), the pruner,
and the prober -- and **the prober takes its `GetDirectoryReader()` snapshot while holding
it**, so reader and delta are always read as one consistent pair.

### 8.5 Torn commit in a mixed transaction -- withdrawn (2026-10-28)

**This section described an anomaly that no longer exists.** It was written when the
search commit was a leg of its own that `query::Transaction::Commit` ran *after* the
engine commit was durable, so a rejection there left the transactional writes committed
and only the search writes undone. [#1396 (`09f655164`)](server/search) deleted that leg.

The pass that raises the conflict now runs from `TransactionPreWalWrite`, under the
committing database's commit lock, **before** `AppendLocalStorage`, before `WriteToWAL`
and before `transaction.Commit` -- the point of no return
([duck_transaction_manager.cpp:776-786](third_party/duckdb/src/transaction/duck_transaction_manager.cpp#L776-L786)).
A throw becomes a returned `ErrorData`, and the commit path rolls the whole DuckDB
transaction back. Nothing of the transaction survives, search or transactional.

Across databases the same holds, for a different reason: two-phase commit prepares each
participant in oid order, and a `PrepareTransaction` that returns an error breaks the
loop and rolls back **every** participant, including ones that already prepared
([meta_transaction.cpp:261-285](third_party/duckdb/src/transaction/meta_transaction.cpp#L261-L285)).
The decision record `COMMIT_PREPARED` is written only once every participant has prepared
and synced, so a batch left behind by a rejected commit is undecided and replay skips it.

Pinned by `recovery/search_table_pk_mixed_transaction.test`, both paths: a transaction
writing a search table and an ordinary table in one database, and the same across two
databases. In both the loser's write to the ordinary table is gone with its transaction.

**What is left to document is the opposite of what this section claimed.** A primary-key
conflict on a search table can surface at `COMMIT` rather than at the statement, and it
takes the whole transaction with it -- so an explicit transaction that looked fine can
still fail at the end and lose work the client had no warning about. The error arrives
wrapped by DuckDB's commit path as `Failed to commit: Duplicate key on column "..."
violates primary key constraint.`, which the harness reports with sqlstate **40001**
rather than 23505. That reads correctly -- the statement was admissible and a concurrent
committer is what unmade it, which is a serialization failure -- and it is what the
earlier revision of this section asked for, arrived at by DuckDB's own wrapper rather
than by us choosing the code. It belongs in the `docs/` page of §13.

## 9. TRUNCATE

Invisible to a probe in **both** flavours, and `clears_shard` picks between them on
`IsAutoCommit()`:

- **Autocommit** -- `SearchTable::Clear(record_tick)` runs *after* `AppendCommit` returns
  ([search_table_transaction.cpp:215-218](server/search/search_table_transaction.cpp#L215)),
  so a certifier taking `_append_mu` in between sees the pre-truncate reader and raises a
  **spurious** duplicate. `IndexWriter::Clear` also switches and discards the flush
  context, making it a **publication point that bypasses `RefreshUnsafe`** -- a prune hook
  attached only there misses it.
- **In a transaction** -- now a buffered op replayed as `Remove(irs::All)`. It names no
  rowids, so the delete set cannot discount anything. After `BEGIN; TRUNCATE t; COMMIT;`,
  re-inserting any pre-existing key would be wrongly rejected for up to a refresh
  interval.

Fix: a per-shard `pending_truncate_tick` barrier, set **under `_append_mu`** rather than
after, cleared when the publication that applied it lands. While set, skip the published
tier entirely and skip delta generations at or below the barrier. Pruning must also hook
the `Clear` publication path.

**The transaction's own ART must be cleared with the buffer.** `AppendTruncate`
([search_table_changes.h:106](server/search/search_table_changes.h#L106)) deliberately
resets the collection, the bands, `ops`, `applied_ops` *and* the segment refs, so that a
truncate is always op #0. It kills rows this transaction inserted, so their keys are
dead too and must leave the ART -- this is the one place the insert-only index of §6.2
still has to drop entries.

`DROP TABLE` needs nothing extra: the delta lives in `SearchTable`, writers pin it by
`shared_ptr`, and the destructor already handles teardown.

## 10. Other publication paths -- audited

Everything that can make rows live, and why one critical section still suffices:

- **Recovery** replays and calls `SearchTable::Commit()` with no `AppendCommit`.
  Single-threaded, before any session. Sound -- and it is precisely *because*
  certification precedes `WriteFrameLocked` that durability implies certified, so replay
  never re-checks.
- **Backfill `ReplaceSegments`** rewrites existing rows only, preserves rowids, and
  publishes through `RefreshUnsafe`. No new keys. It names only segments from the
  *committed* reader, so delta segments are never replaced or stranded.
- **Compaction / `Import`** -- no new keys, published by a later `RefreshCommit`.
- **CTAS** declares no PRIMARY KEY
  ([duckdb_physical_search_insert.cpp:169](server/connector/duckdb_physical_search_insert.cpp#L169)).
- **The memory design's threshold replay** feeds iresearch mid-statement but publishes
  nothing; its `FlushAndFsync` at commit precedes `AppendCommit`, so segments are durable
  before the record names them.
- **`IndexWriter::Clear`** is the one real bypass, handled by §9.

## 11. Rejected alternatives

- **Forcing a `RefreshCommit` at commit** so the reader never lags and the delta
  disappears. Rejected: it would make single-row inserts very slow. Deferred publication
  is why search inserts are fast, and taxing every commit to simplify a ~1 s window is the
  wrong trade. This is the one alternative that would remove §7 entirely, so it is worth
  being explicit that the delta is the price of the fast path.
- **Making uniqueness opt-in via a `WITH` option.** Redundant -- declaring a `PRIMARY KEY`
  is itself the opt-in, and `CREATE INDEX ... USING inverted` covers pushdown without a
  constraint (§4.1).
- **A dedicated composite PK term field**, so uniqueness is a single `ReadDocs`. Rejected
  in favour of AND over the existing per-column terms: no format change, no extra term per
  row, and those fields are already written and already used for pushdown. The cost is
  that composite certification needs a driving column plus a stored-value verify.
- **A flat `PK -> (tick, tombstone)` pending map** (§5.1 of the predecessor). Pins every
  key of a large insert until the next refresh; replaced by the split of §7.1.
- **Deriving keys from the write buffer** rather than at `Sink`. The memory design clears
  the buffer at every threshold crossing, so this would certify only the rows since the
  last one. It also forced a non-unique ART, because entries could not be removed without
  the key. Feeding at `Sink` (§6.1) removes both problems.
- **A non-unique ART with a delete-set filter instead of removal.** Correct, and it needs
  no PK projection on `DELETE` -- but a stale entry per deleted row accumulates, repeated
  updates of one row pile N rowids under a single key, and every tier-(c) answer pays a
  filter lookup. Removal (§6.2) keeps the index exactly accurate for the cost of one
  projection.
- **An ART for the delete side**, by symmetry with §6.1. Rejected: the resident delete
  memory is dominated by iresearch's own `SegmentContext::queries`, so spilling our half
  moves nothing (§6.4).
- **Sharing the backfill's `_delete_log`.** Incompatible retention, unsafe in both
  directions (§6.4).
- **An optimistic pre-commit certification pass** to make abort atomic for mixed
  transactions. Rejected in favour of documenting the anomaly, though the memory design
  now supplies the hook it would use (§8.5).

## 12. Touch points

**New**

- `server/search/search_pending_delta.{h,cpp}` -- `PendingDelta`, generations, eager
  segment references, `delta_mu`, prune, the truncate barrier.
- `server/connector/search_unique_probe.{h,cpp}` -- the §5 probe, plus the
  term-dictionary merge for the parallel path.
- `server/connector/search_key_index.{h,cpp}` -- the per-transaction ART: construction
  from the shard's cached expressions, `Insert`, `Clear` for TRUNCATE, promotion. No
  removal path (§6.2); §15 adds one.

**Changed**

- `server/connector/search_remove_filter.{hpp,cpp}` -- factor the live-hit resolver out
  for reuse; no behaviour change.
- `server/search/search_table.{h,cpp}` -- own the `PendingDelta`; `PKColumns()` accessor
  and the cached `unbound_expressions`; prune after publish and after `Clear`.
- `third_party/duckdb` (fork PR) -- fire `TransactionPreWalWrite` in `CommitTransaction`
  at the position `PrepareTransaction` already fires it, under the commit lock and before
  the point of no return (§8.1).
- `server/connector/duckdb_client_state.cpp` -- certify in `TransactionPreWalWrite`,
  install the delta in `TransactionPreCheckpoint` beside the existing `StageSearch`.
- `server/search/search_table_transaction.{h,cpp}` -- own the per-shard ART and the
  in-transaction delete set; certify and install under the one commit-lock hold; promote
  at commit per §7.1.
- `server/connector/duckdb_physical_search_{insert,update,truncate}.cpp` -- feed the ART
  in `Sink`; probe in `Finalize`; PK `NOT NULL` check; clear the ART on TRUNCATE.
  `..._delete.cpp` gets only the debug assert of §6.2.
- `server/catalog/ddl/tables.cpp` -- gate PK column types via `term_dict::Classify`;
  reject non-PK `UNIQUE`.
- `server/catalog/entry/duckdb_schema_entry.cpp` -- add `ADD_CONSTRAINT` to the
  search-table rejection list.
- [search_table_indexed_columns_2.md](search_table_indexed_columns_2.md) -- record the two
  §5 corrections.

## 13. Tests

**gtest** (`tests/server/connector/`): the probe tiers in isolation -- published-only,
delta-segment-only, delta-keys-only -- each crossed with the delete set; the
smallest-`docs_count` driving-column choice; composite verify-by-stored-value; the
term-dictionary merge against a hand-built segment pair; generation pruning at a tick
boundary; ART insert plus clear-on-truncate.

**sqllogic** `tests/sqllogic/sdb/pg/index/search_table_pk_unique.test`, following
`search_table_pk_pushdown.test` conventions (`compaction_interval=0`, results observed
after `VACUUM (REFRESH_TABLE)`, errors asserted with the multiline `----` block form):

- duplicate within one INSERT; against a committed row; across statements in one
  transaction;
- BIGINT, VARCHAR and composite PK;
- `DELETE` then re-INSERT of the same PK **without** an intervening refresh -- the case
  the reader alone gets wrong;
- `INSERT K; DELETE K; INSERT K` in one transaction **rejected**, and two `UPDATE`s of
  one row in one transaction **rejected** -- the two no-RYOW semantics of §6.2. Comment
  each with the pre-feature behaviour (two live rows, silently) so the next reader sees
  this replaced a duplicate rather than introducing a restriction, and with a pointer to
  §15, which inverts both;
- `UPDATE t SET id = id + 1` over a contiguous range **accepted**, while
  `UPDATE t SET id = 2 WHERE id = 1` against a surviving `id = 2` is **rejected**;
- `UPDATE t SET id = <constant>` over multiple rows rejected;
- a serial insert large enough to cross the write-buffer threshold (lower it with
  `SET checkpoint_threshold = '<size>'`, alias `wal_autocheckpoint`), with a duplicate on
  either side of the crossing -- the case that fails if keys are derived from the buffer;
- a parallel insert (materialized source) with a self-duplicate -- **known-failing until
  §6.3's merge lands**; it must be large enough to actually parallelise, since a small
  `INSERT ... SELECT` takes the inline path and tests something else;
- `BEGIN; TRUNCATE t; COMMIT;` then re-inserting a pre-existing key, no refresh between;
- NULL into a PK column rejected;
- `PRIMARY KEY` on a DECIMAL / UUID column rejected at CREATE TABLE; non-PK `UNIQUE`
  rejected; `ALTER TABLE ADD PRIMARY KEY` rejected.

**Concurrency**, in `tests/sqllogic/recovery/` -- the only subtree where fault injection is
allowed. A new pause point between certify and append, driven like
[search_table_backfill_concurrent_dml.test](tests/sqllogic/recovery/search_table_backfill_concurrent_dml.test)
(`SET sdb_faults`, `statement async ok` on a parked connection, `query ok retry` to
observe, then release): assert first-committer-wins and that the loser's `COMMIT` fails. A
second case parks a committer while a refresh prunes, covering §8.4.

**Probe / cleanup, owed** (§7.3). The probe opens the delta's segments from names, under
a per-shard exclusion that holds the cleaner off from the snapshot that binds the names
until the last one is a reader. Reasoned, not demonstrated: the exclusion is currently
untested as an exclusion, and so is every other way a probe overlaps maintenance.

Three park points reproduce the race the exclusion closes, all in `recovery/`:

1. in `SearchTable::TakeCertificationSources`, between the snapshot and the opens;
2. around a refresh that publishes and prunes the parked probe's generation;
3. around the compaction that merges the published segment away.

With the probe parked at 1, run 2 and 3 and then a cleanup, and release. Without the
exclusion the probe opens a segment whose files are gone; with it the cleanup is the one
that waits. Asserting the *wait* is the awkward half -- a `statement async ok` on the
`VACUUM` plus a `query ok retry` on a metric that only moves once it finishes -- so the
reachable version asserts the probe still certifies correctly and the vacuum completes
after the release.

Worth covering in the same file, since they need the same park points: a compaction
concurrent with a probe (the probe's open readers must keep the merged-away segment
readable), and a refresh landing between the `Finalize` probe and the in-lock pass (§8.4,
which the prune floor covers -- assert the floor actually holds by pruning hard while a
committer is parked).

**Regression:** `search_table_*` and `inverted_index_*` across both engines, plus the
recovery suite. **Existing tests that insert duplicate PKs will now fail and must be
audited** -- that is the feature working, but it is the likeliest source of churn.

**Docs:** `docs/` has no search-table page, so this adds a scoped section on PK semantics
under `storage='search'` to
[create_table/index.md](docs/sql/statements/create_table/index.md), backed by a
`# DOCS_TEST:` example under `tests/sqllogic/sdb/pg/site_docs/sql/`. It must state four
things: declaring a PK is what turns enforcement on; which column types a key accepts;
that the check is statement-deferred, so `UPDATE t SET id = id + 1` succeeds where
PostgreSQL rejects it; and the mixed-transaction anomaly of §8.5.

## 14. Known debt this design creates or inherits

**Inherited, and fixed here because the feature is meaningless otherwise** (§4.4, §4.5,
§4.6): PK columns of unsupported type are silently un-indexed; PK `NOT NULL` is
unenforced; non-PK `UNIQUE` is silently dropped.

**Inherited, and fixed here because it would make the constraint inert:**
`ALTER TABLE ... ADD PRIMARY KEY` is currently **allowed** on a search table. The
rejection list covers ADD/DROP COLUMN, DROP CONSTRAINT and ALTER COLUMN TYPE but not
`ADD_CONSTRAINT`
([duckdb_schema_entry.cpp:1785-1805](server/catalog/entry/duckdb_schema_entry.cpp#L1785)),
and `SearchTable::_pk_columns` is set once in the constructor and never updated by an
ALTER -- so afterwards neither existing segments nor new rows carry the term field until
the next boot, and every probe returns "absent" over data that may already violate the
key. A backfill-and-validate implementation is its own feature.

**Created -- nothing on the `DELETE` path.** An earlier draft had `DELETE` projecting its
PK columns and called it the one place this design costs something. It does not: with no
read-your-own-writes the key index never removes (§6.2), so `DELETE` is untouched beyond
contributing rowids it already recorded. The projection work moves to §15, where it is
free for `DELETE` anyway because the values are already in the chunk.

**Created:** two key encodings -- per-column terms on disk, `ARTKey` in memory -- where
DuckDB maintains one. Harmless, since the `ARTKey` form never reaches disk, but the two
must stay consistent, which is why §5.1 insists probe terms come from the sink's encoder.

**Created:** certification cost inside `_append_mu`, bounded by the concurrent window
rather than the key count (§8.3), but still on the serialized commit path. If it bites,
the lever is shrinking the window, not moving the probe out of the lock.

**Created:** the in-transaction ART grows with the rows a transaction inserts. It spills,
so it is a disk rather than an RSS concern, and it is the one structure on the write path
the memory design's threshold does not bound.

**Deferred:** `INSERT ... ON CONFLICT` / `MERGE INTO` (§2); write-write conflict detection
(§5.2-5.3 of the predecessor -- uniqueness is its degenerate case); and read-your-own-writes,
which would let a probe see uncommitted rows and is tracked separately.

**Follow-up, scoped and recorded (2026-10-29): generalise §4.3's rowid check into the fix
for [#1414](https://redirect.github.com/serenedb/serenedb/issues/1414).**

`UPDATE` and `DELETE` on a search table scan the last *published* snapshot, so any change
that has committed without being published is invisible to them: the statement re-removes
a rowid somebody else already removed, and inserts another copy of the row. #1414 has the
single-session shape (two autocommit `UPDATE`s leaving two rows, a `DELETE` right after an
`INSERT` reporting `DELETE 0`), and the concurrent shape is the same mechanism -- verified
on this branch, and the reason §4.3's check exists at all.

§4.3 answers one corner of it: an `UPDATE` that leaves the key columns alone now refuses to
rewrite a row another transaction has removed. That is deliberately narrow. The general
fix is the same comparison with the scope taken off:

- every search-table `UPDATE` carries its old rowids, not only the PK-unchanged ones, so a
  PK-*changing* `UPDATE` is refused on the same grounds;
- a `DELETE` whose rowids a concurrent transaction already removed reports the right count
  instead of silently removing nothing -- today's `DELETE 0`;
- the statement consults the delta when it *scans*, not only when it commits, so the
  single-session shape in #1414 (where there is no second transaction at all) is fixed
  rather than detected.

Nothing in §4.3 would need undoing for it: `rewritten_rows` becomes "rows this statement
is replacing", the predicate is `RefuseResurrection` with a wider input, and the commit
passes already call it. The reason it is not done here is that the last bullet changes what
a search-table scan reads, which is a subsystem rather than a constraint, and the second
turns a silent wrong answer into an error -- both need their own decision.

**Watch:** the memory design's open point -- segments spanning a delete boundary, so replay
adopts unmasked and re-applies a removal to both -- is safe only while removals name rowids
disjoint from freshly minted inserts. This design keeps deletes rowid-keyed (§6.4) and does
not introduce remove-by-value, so it does not break that. Recorded here so the two are not
changed independently.

## 15. RYOW support -- deferred, designed

**Status: deferred. Not built, not needed until read-your-own-writes lands
([[project_search_ryow_plan]] tracks that work).** §6.2 makes the key index insert-only
on the strength of one property: a transaction cannot see, and therefore cannot delete
or update, a row it wrote itself. RYOW removes that property, and this section is what
has to be built the day it does. It is written up now because the analysis was done
while the constraint was fresh, and because the debug assert in §6.2 is what will fire.

### 15.1 What breaks

Once a DML scan can match the transaction's own uncommitted rows, an own-row delete
becomes possible, and an insert-only ART goes stale in the **dangerous** direction -- a
key whose row is gone still reads as present, so a legal insert is rejected; worse, if
the entry is dropped without the delete being recorded, a live key reads as absent and a
duplicate is *admitted*.

Two semantics also flip, and both have tests pinning the current behaviour that will
need inverting:

- `INSERT K; DELETE K; INSERT K` becomes **legal** -- the DELETE will see the first
  insert and remove it, leaving one live `K`. Today it is a genuine duplicate.
- Two `UPDATE`s of one row in a transaction become **legal** -- the second will see the
  first's output and supersede it, rather than re-targeting the same published rowid and
  producing a second replacement.

**RYOW is also the candidate fix for the duplicate-row defect of §6.2**, which makes
this section a correctness item and not only a feature. That defect is independent of
this design in both directions: uniqueness enforcement detects it only on PK tables, and
whatever fixes it must not require a declared key, since most search tables have none.
Until it lands, a double `UPDATE` in one transaction silently duplicates a row on any
keyless search table, and raises a semantically wrong 23505 on the rest.

### 15.2 Removal is by key, and the key has to be found

`ARTOperator::Delete(art, node, key, row_id)` is a single descent, O(key length), and
returns `false` cheaply when the rowid does not match, so a speculative attempt is safe
([art_operator.hpp:226-280](third_party/duckdb/src/include/duckdb/execution/index/art/art_operator.hpp#L226)).
There is no rowid-keyed removal anywhere -- `RemovalMerge` also works from a source ART's
keys -- so the key must be available wherever the removal happens.

**A band gate decides whether to try.** A deleted rowid belongs to this transaction iff
it falls in one of the `pk_base` runs it reserved -- the analog of DuckDB's
`row_id >= MAX_ROW_ID`. Committed rows fail the test and skip the ART entirely, so a
large `DELETE` of published rows pays one range check per row and nothing else. This is
exactly the assert §6.2 leaves behind, promoted from tripwire to control flow.

Where the old key comes from, by statement shape:

| | old key available? | |
|---|---|---|
| `UPDATE`, PK columns untouched | **yes** -- the projected "new" value *is* the old one | free |
| `UPDATE`, PK columns in the SET list | no -- the chunk holds `id+1`, not `id` | needs old-PK passthrough |
| `DELETE` | **yes, already** -- see below | free |

`DELETE` costs nothing: `BindRowIdColumns` already appends every id
`GetRowIdColumns()` returns, and for a search table that is the declared-PK columns as
`PKVirtualColumnId` -- materialized by the scan as the real values -- followed by the
generated rowid. `SearchPkSlots`
([duckdb_catalog.cpp:1324](server/catalog/ddl/duckdb_catalog.cpp#L1324)) simply discards
all but the last. The work is to stop discarding them and thread them to the operator.

`UPDATE` is nearly as cheap: `BindRowIdColumns` put the old values in the projection
*below* `LogicalUpdate`, so `PlanUpdate`'s existing passthrough loop
([:1434-1438](server/catalog/ddl/duckdb_catalog.cpp#L1434)) need only copy the
declared-PK slots as well. No binder change. Gate it on §4.3's PK-touching flag so
ordinary updates never widen -- that flag ships with the main feature, so it is already
available here.

### 15.3 The maintenance rule

Per row, after the band gate says the old rowid is ours: remove `(old_key, old_rowid)`,
then insert `(new_key, new_rowid)`. When the key is unchanged that pair is a re-point,
which is also what keeps a perfectly ordinary
`INSERT; UPDATE SET <non-key>` in one transaction from self-conflicting.

Letting the entry go stale instead is the variant that is wrong:

| | correctness | cost |
|---|---|---|
| remove old, insert new | correct | constant |
| leave the old entry, insert new | rejected -- the unique ART conflicts | -- |
| skip maintenance entirely | **wrong** -- a live key reads as absent => *missed duplicate* | -- |

### 15.4 What does not change

The probe (§5), the pending delta (§7), the certification point (§8) and the delete set
(§6.4) are all unaffected: they reason about published and committed-unpublished state,
which RYOW does not touch. The delete set stays rowid-keyed -- a PK-keyed one would
collapse `DELETE K(r1); INSERT K(r2); DELETE K(r2)` under `IGNORE_DUPLICATES` and lose
`r2`. And deletes must stay rowid-keyed for a second reason recorded in §14: the memory
design's segment-boundary assumption depends on removals naming rowids disjoint from
freshly minted inserts.

## 16. Deferred optimizations -- revisit after the feature is complete

**None of these ship before uniqueness enforcement works end to end.** They were found
while building it and are recorded here so the findings are not lost, not because any of
them is due. Each one needs its own `perf:` PR with end-to-end numbers against main
([cpp.md](.claude/rules/cpp.md)), and the order below is the order the evidence
suggests, not a commitment.

**The percentages quoted are from a dev build, not the `perf` preset**, taken during
sustained serial single-column-PK inserts. They are indicative of where the time goes,
not results. The one bound that is solid, because it is a difference of two end-to-end
runs rather than a profile: over 50000 rows, certification is **~90% of insert time on
the serial path** (311/303 ms with a declared key against 27.0/26.4 ms without) and
**~72% on the bulk path** (234/235 ms against 74/59 ms). So there is real headroom, and
it is worth spending once the semantics are settled.

### 16.1 The published probe's per-key dictionary walk -- §5.4

`UniqueProbe::MayHold` was the largest single line of the profile at ~15%, of which the
FST walk was ~12%. Written up in full in **§5.4**: one persistent `SeekTermIterator` per
published segment in place of a stateless `Lookup` per key, exploiting the ascending
seed-term order the walk already guarantees. It also removes the double walk a
single-column `Exists` pays after `MayHold` has just found the same term.

This is the one with by far the best evidence behind it.

### 16.2 The single-column key, specialised

A one-column primary key is the common shape, and three things in the current code pay
for generality it does not need:

- **`UniqueProbe::ColumnTerms` duplicates the key set.** For a single-column key the
  projection onto that column *is* the key, so the per-field index is a second copy of
  `_keys` -- which the constructor has already sorted. `MayHold` could binary-search
  `_keys` directly and the index could be skipped entirely. (The `unique` pass in
  `ColumnTerms` is likewise dead for one column, and is not load-bearing for several
  either: `binary_search` is correct over a sorted range with duplicates. It only
  shrinks a composite-key index.)
- **`FindDuplicateKey` re-tests what the column count already decides.** A
  `<bool SingleColumn>` parameter would let `constexpr if` remove the `Defers()` branch,
  the candidate-window machinery and the per-key `resize` from the one-column path,
  where they can never fire. The key-column count is known before the walk starts.
- **A whole key is a `std::vector<irs::bstring>` of one element**, so every candidate
  costs a vector spine plus a string allocation to carry a single term.

Keep them together: they are one specialisation seen from three angles, and splitting
them would mean touching the same hot path three times.

### 16.3 Key-type templating, for numeric keys

Templating the key representation on the column type would let a fixed-width numeric key
live inline instead of in an allocated `irs::bstring`, removing the per-key allocation
that 16.2's third point describes.

**Recorded with its measurement, which did not support it on its own:** the sort over
candidate keys was ~1.6% of the profile and building the buffered row source ~4.8%,
against ~15% for the probe. So this is worth doing *as part of* 16.2, where the key
representation is already being changed, and not worth a pass of its own.

### 16.4 A doc-level seek inside `Exists`

`UniqueProbe::Exists` intersects a composite key's columns by walking each non-seed
posting list from its start, discarding everything below the seed's first hit. The
`TODO(dronplane)` at the loop records why: `ReadDocs` is push-only and `TermPostings`
has no seek, so only `lead::Node` offers one, which means preparing a filter.

Blocked on a doc-level seek reaching the `TermReader` surface. Composite keys only, so
it does not touch the common shape.

### 16.5 The pending delta's segment references

§7.3's revision note: the delta holds an open `irs::SegmentReader` per unpublished
segment rather than a `directory_utils::Reference`, which costs the reader's handles
until the next refresh. The frugal form is already designed and the probe side does not
change. **Gate it on evidence** -- fd or memory pressure under a bulk-insert load -- rather
than doing it on principle.

### 16.6 Open decisions, not optimizations

- **The candidate flush cap.** `kCandidateFlush = 1 << 18` is a constant, where
  [cpp-serenedb.md](.claude/rules/cpp-serenedb.md) asks for a `SET` variable read through
  `query/config.h`. The practical difference: at `1 << 18` the flush path is unreachable
  by any existing test (the largest composite case builds 20000 candidates), while a
  setting would earn a permanent windowing test. Correctness does not depend on the
  value -- a poor choice costs only time -- which was verified by running the suite at
  caps 1, 4096 and `1 << 18` with identical results.
- **A DDL cap on primary-key column count.** Nothing bounds it today, and every
  per-column cost above is multiplied by it.
