# COPY reimplementation

## What it must be
COPY works across the whole matrix with no special cases:
`(in | out) × (binary | csv | text | parquet) × (file | pg-wire stdin/stdout | real stdin/stdout)`,
binary at least as fast as text, and `COPY TO` parallelized like `SELECT`.

## Design: FORMAT and TRANSPORT are orthogonal
- **FORMAT** = a DuckDB CopyFunction that only converts rows ⇄ bytes through a DuckDB `FileHandle`.
  It knows nothing about pg-wire. (csv/text/parquet are DuckDB-native; binary[PGCOPY] is ours.)
- **TRANSPORT** = the `FileHandle` the FileSystem returns:
  - real path  → OS file
  - `/dev/stdout` → `CopyOutFileHandle` → pg-wire `CopyData`
  - `/dev/stdin`  → `CopyInFileHandle`  → `CopyInBridge`
  - real stdio → OS fd 0/1 (shell)
- All pg-wire framing (`CopyInResponse`/`CopyOutResponse` + format byte, `CopyData`, `CopyDone`) lives
  **once**, in the transport. The session — which parsed the COPY statement — tells the transport the
  format.

Every matrix cell is then just FORMAT × TRANSPORT; nothing is hardwired. Today only the binary format
violates this: it writes straight to the wire send buffer and hardcodes `/dev/stdin`.

## csv / text / parquet — nothing to do
DuckDB's own CopyFunctions handle these through the FileHandle, identically across all six transports
(pg-stdio just plugs in our bridge handle; files/real-stdio are plain OS). Out of scope here.

## binary — the only format we reimplement
We detect exactly ONE thing: pg-wire stdin/stdout. There bytes move in memory (zero-copy);
everywhere else is a normal DuckDB FileHandle (we write no file/stdio code).

### TO
- **pg-stdout (incl. federation — the hot path):** drive the COPY's inner query through the SAME wire
  collector `SELECT` uses, but with a PGCOPY+CopyData encoder instead of DataRow. The collector emits
  chains **by reference** (`PushChain`/`PopChain` move `message::Buffer::Chain`s) → parallel encode +
  ordered emit + **true zero-copy** splice into the send buffer. The session brackets it with
  `CopyOutResponse`/`CopyDone`. (COPY isn't row-returning, so we run the inner query — symmetric to
  `RunCopyFromStdin`.) NOT a CopyFunction → `FileHandle::Write`, which would copy once per batch.
- **file / real-stdout:** a normal DuckDB CopyFunction; its Sink encodes PGCOPY (shared serializers)
  and writes to the FileHandle — DuckDB/OS handles file vs fd 1. No transport-specific code from us.

### FROM
One parser over a **borrowing** byte-source (yields a view of the next bytes, never copy-into-dst),
decoding straight into flat vectors (no per-field `duckdb::Value`):
- **pg-stdin:** `CopyInBridge` already holds a view of the recv bytes (`_ptr`/`_len`); the parser
  consumes it directly → true zero-copy. Only varchar payloads copy into DuckDB's string heap. (Today
  the `memcpy` in `CopyInBridge::Read` is the copy we remove.)
- **file / real-stdin:** a block-buffered reader (one syscall read per block) hands views to the same
  parser. No pg-stdin vs other-stdin split.

## Plan (each step build + test gated, tree green throughout)
1. **Decouple** — binary TO writes to the `FileHandle` (not the send buffer); binary FROM opens the
   real path (not hardcoded `/dev/stdin`); single framing site carrying the format byte.
2. **Perf** — `BATCH_COPY_TO_FILE` parallel TO; flat-vector + buffered FROM.
3. **Shell / file** fall out for free once decoupled.
4. **Verify** every matrix cell (round-trip checksums) + a binary-vs-text throughput bench on
   `build_perf`.
