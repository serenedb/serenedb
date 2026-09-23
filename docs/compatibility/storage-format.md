---
title: Storage Format
split: headings
---

# Storage Format

Search indexes are stored in SereneDB's own on-disk format, separate from the table
data itself. That format is not yet stable across releases: a release may change it
when the change buys a meaningful gain in write amplification, index size or query
speed.

## Compatibility

A SereneDB server reads only the index format its own version writes. There is no
conversion step and no read path for older layouts, so after an upgrade across a
format change the affected indexes have to be rebuilt.

Table data is unaffected — search indexes are derived, so rebuilding one loses
nothing:

```sql
DROP INDEX my_search_index;
CREATE INDEX my_search_index ON my_table USING inverted (body);
```

For a [view-backed index](../sql/indexes/inverted/views.md), `REINDEX INDEX
<name>` rebuilds in place instead.

Before upgrading, check the [release notes](../releases/index.md) for the release
you are moving to, and plan the rebuild if it changes the format.

## Segment metadata

An index is a set of segments. Each segment is described by one *segment meta*
file named `<segment>.<version>.sm` — `_1.4.sm` is version 4 of segment `_1`. A
commit that changes a segment writes a new `.sm` at the next version; the previous
one is dropped once nothing references it.

The `.sm` holds the segment's bookkeeping — how many documents it has, how big it
is, which files belong to it — and the segment's *document mask*.

### Document masks

Deleting a row does not rewrite a segment. The deleted row's id is recorded in the
segment's document mask, and queries skip masked rows. Deleting therefore costs one
small write, and the space is reclaimed later, when compaction rewrites the segment
without the masked rows.

The mask is stored as a compressed bitmap at the front of the `.sm`, and the number
of bytes it occupies is the file's last 8 bytes. A reader seeks to that trailer
first, which tells it both where the mask ends and where the metadata begins:

| Region   | Extent                | Contents                                                       |
| -------- | --------------------- | -------------------------------------------------------------- |
| mask     | `[0, mask_size)`      | compressed bitmap; absent when the segment has no masked rows   |
| metadata | `[mask_size, len-8)`  | the tagged object below                                         |
| trailer  | last 8 bytes          | `mask_size`, fixed width                                        |

### Mask chains

Rewriting the whole mask on every delete is wasteful once the mask is large: a
delete of one row would rewrite megabytes of bitmap. So a `.sm` may instead store
only the rows deleted since the previous version and link back to it, by version
number, through its `parent` field. The reader follows the links and unions the
bitmaps.

A new `.sm` extends the chain only when the mask is already larger than 4 KiB;
below that, rewriting it whole is cheaper than an extra file to open. Any write
that does not extend the chain collapses it — the new `.sm` carries the complete
mask and has no `parent`. Compaction, which rewrites the segment, always collapses
it.

Every `.sm` in a chain is listed among the segment's files, so the usual
reference counting keeps it alive. When a write collapses a chain, its links lose
their last reference and the directory cleaner removes them. Chain length is not
capped: a chain costs one extra file to open per link at segment-open time, and
each link is bounded below by the 4 KiB threshold, so a chain stays short relative
to the mask it describes.

The same link carries the file list. A segment's files only change when it is
flushed or compacted, and both of those collapse the chain, so only the root of a
chain writes them; each link leaves the list out and the reader picks it up from
the root as it walks. A `.sm` written for a deletion is therefore just its patch
and a handful of counters.

### Metadata fields

The metadata object is serialized field by field, each tagged with a numeric id:

| Id  | Field        | Type           | Notes                                                   |
| --- | ------------ | -------------- | -------------------------------------------------------- |
| 0   | `parent`     | `uint64`       | previous `.sm` version; omitted outside a chain           |
| 1   | `files`      | `list<string>` | omitted by a chain link, which inherits the root's list   |
| 2   | `docs_count` | `uint32`       | every document the segment holds, masked or not           |
| 3   | `byte_size`  | `uint64`       | segment size, not counting the mask bytes                 |

`parent` and `files` come first so that walking a chain reads two fields per link
rather than parsing each link in full.

Everything else is derived rather than stored. The segment's name and version are
already in the file's own name, so the reader takes them from there, and the live
document count is `docs_count` minus the bits in the mask.

There is no format version number and no header or footer. A field at its default
is not written at all, a reader asks for each field by id with the default to use
when it is missing, and a reader stops at the last field it knows about rather than
insisting the object end there. A later release can therefore add fields, or start
writing a field an earlier release always defaulted, without any version to bump.
Changes that reorder or repurpose an existing id still break the format.

### Uncommitted documents

A commit can publish a segment whose last documents belong to transactions that
have not committed yet. Those documents form a trailing run that queries skip, the
same as masked ones, until a later commit covers them.

The length of that run is not in the `.sm`. It is recorded in the segment's entry
in `segments_N`, next to the `.sm` file name:

| Id  | Field               | Type     | Notes                                         |
| --- | ------------------- | -------- | --------------------------------------------- |
| 0   | `filename`          | `string` | the segment's current `.sm`                   |
| 1   | `codec`             | `string` | format the segment was written with           |
| 2   | `uncommitted_count` | `uint32` | length of the trailing run; omitted when zero |

The run shrinks or disappears at a later commit while the segment itself stays the
same. Every commit rewrites and syncs `segments_N` anyway, so that commit writes and
syncs nothing else for the segment: the `.sm` it already has is reused as is. After
a crash the run stays skipped: recovery replays those transactions' rows into new
segments, so the old copies must not come back.

## Format changes

### Segment meta rewritten (unreleased)

The `.sm` gained the chain described above, so a delete against a segment with a
large mask now writes only the newly deleted rows instead of the whole mask.

Its layout changed with it:

- the mask moved to the front of the file, with its size in a new fixed-width
  trailer, and is now a compressed bitmap rather than a list of row ids;
- fields are tagged with ids and omitted at their default, replacing a fixed
  positional layout;
- the format header and footer are gone — nothing read them, and the checksum they
  carried duplicated what the storage layer already verifies;
- the document count is stored instead of the live count, which is derived from it
  and the mask;
- the file list is written only at the root of a chain, and each link inherits it
  through the link it already follows;
- the segment's name and version are no longer stored, since the file is named
  after them. A `.sm` is therefore only meaningful under its own name.

The index metadata file, `segments_N`, changed with it. Its commit payload was a
size field plus a blob and is now a single self-describing field, since the blob
already carries its own length; its generation is no longer stored, for the same
reason the segment's is not — the file is called `segments_<generation>`; and its
segment entries gained `uncommitted_count`, so a partially committed segment
records its trailing run as a bound instead of masking each of its documents, and
the commit that completes the run writes no new `.sm`.

Indexes written before this change must be rebuilt.
