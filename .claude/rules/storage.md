---
paths:
  - "iresearch/formats/**"
  - "third_party/duckdb/src/storage/**"
  - "third_party/duckdb/src/include/duckdb/storage/**"
---

# Storage compatibility

For everything SereneDB writes: database files and their logs, the catalog, search index files.

- Compatibility is about the contents a reader gets back, never the bytes: the same data can be written differently on every run, so tests compare contents.
- A file never carries stale memory: a compression method writes every byte of the size it reports.

## Changing a format

Older releases refuse what they don't know, so most changes need no new version:

- **New field or option:** a default that keeps today's behaviour, written only when it differs, read as the default when missing.
- **Removed field:** read as deleted (`ReadDeletedProperty`; a struct member becomes `irs::utils::Deleted<T>`).
- **New enum value, variant or codec:** appended.
- **Never** reuse a field id or reorder struct members (positional serialization numbers them by position). A changed default, type or meaning is a new field or a new version.

A new storage version only for a change older releases would misread instead of refusing, or to stop reading old data: add `serenedb_vN` to `third_party/duckdb/src/storage/version_map.json` and run `scripts/duckdb_family.sh regen`; to stop reading older data, also raise `SERENEDB_VERSION_LOWER`. It gets its own PR and a line in the release notes, and a value is never reused.

Test a format change the way a release meets old data: a fault point makes the writer produce the old shape, the test restarts and reads it back.

- Search index files: the footer and the leading `storage_version` of `segments_N` never change; an object's fields are numbered from 0 through named constants, and every field written is read.
- Database files: checkpoint and log entries take their field ids from `third_party/duckdb/src/include/duckdb/storage/serialization/*.json`; the log header never gains a field (a framing change bumps `WAL_VERSION_NUMBER`); replay refuses an entry it can't read instead of dropping it, and drops only a torn tail.

## DuckDB database files

A file with a DuckDB storage version (a plain `ATTACH`, `serened shell`) stays readable by that DuckDB release, and SereneDB reads what that release writes:

- A SereneDB field of an upstream class takes 16384 plus the id upstream's numbering would give it; state DuckDB drops too is skipped in a DuckDB file (`"version": "serenedb_v1"`), anything else is refused there.
- A SereneDB value of a stored upstream enum starts at 200 and is listed in `IsSereneDBEnumValue`.
- A new layout or compression is chosen only when the version written is SereneDB's; objects there get no owners or privileges.

`tests/duckdb/run.sh --suite interop` checks both directions against the official DuckDB image; the steps for such a change are in [tests/duckdb/README.md](../../tests/duckdb/README.md).
