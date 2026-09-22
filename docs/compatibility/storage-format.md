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

The `.sm` holds the segment's bookkeeping — its name and version, how many
documents it has, how big it is, which files belong to it — and the segment's
*document mask*.

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

| Id  | Field               | Type           | Notes                                                     |
| --- | ------------------- | -------------- | ---------------------------------------------------------- |
| 0   | `parent`            | `uint64`       | previous `.sm` version; omitted outside a chain             |
| 1   | `files`             | `list<string>` | omitted by a chain link, which inherits the root's list     |
| 2   | `name`              | `string`       | segment name                                                |
| 3   | `version`           | `uint64`       | this segment version                                        |
| 4   | `live_docs_count`   | `uint32`       | documents neither deleted nor uncommitted                   |
| 5   | `removal_count`     | `uint32`       | omitted when zero                                           |
| 6   | `uncommitted_count` | `uint32`       | omitted when zero                                           |
| 7   | `byte_size`         | `uint64`       | segment size, not counting the mask bytes                   |

`parent` and `files` come first so that walking a chain reads two fields per link
rather than parsing each link in full.

The rest is derived: a segment holds `live_docs_count + removal_count` documents,
of which `uncommitted_count` are a trailing run still being written and the other
`removal_count - uncommitted_count` are the bits in the mask. A reader that finds a
different number of bits than that rejects the file.

There is no format version number and no header or footer. A field at its default
is not written at all, a reader asks for each field by id with the default to use
when it is missing, and a reader stops at the last field it knows about rather than
insisting the object end there. A later release can therefore add fields, or start
writing a field an earlier release always defaulted, without any version to bump.
Changes that reorder or repurpose an existing id still break the format.

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
- `removal_count` and `uncommitted_count` are new, and the document count is
  computed from them rather than from the mask's contents;
- the file list is written only at the root of a chain, and each link inherits it
  through the link it already follows.

The index metadata file, `segments_N`, changed with it: its commit payload was a
size field plus a blob, and is now a single self-describing field, since the blob
already carries its own length.

Indexes written before this change must be rebuilt.
