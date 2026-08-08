# REINDEX delete improvements -- implementation plan

RECONSTRUCTED 2026-08-06: the original untracked file was eaten by someone
else's `git clean` on this shared checkout. Rebuilt from session context and
project memory; now `git add`ed so it survives. Some delegate STATE prose is
condensed -- trust the section verdicts, re-derive details from the tree.

Working plan agreed with Pavel (2026-08-04). Written to be self-sufficient:
an agent with no prior conversation context should be able to implement from
this document. Read §0 and §8 BEFORE touching code.

Status legend: DONE / APPROVED (green-lit, not yet implemented) / DEFERRED.

---

## WORK ORDER 4 (2026-08-06, from Pavel) -- validate & hunt on LIVE GOOGLE CLOUD. CURRENT ASSIGNMENT

The client runs on Google Cloud -- GCP numbers are the product truth, the
local rig was only a request-counting proxy. This order: run the whole
insert->queryable flow AGAINST LIVE GCP with the W3 fixes in the tree,
re-measure everything, verify the fixes show up on the wire, try the
recommended flags, and keep hunting whatever is still slow. Same
run-to-completion contract (this section IS the approval; BLOCKED + move
on; NO submodule bump; staging untouched; `git stash` forbidden;
`uptime` before every timing series -- loadavg > ~15 poisons CPU phases;
after ANY `cmake --preset` run `scripts/reapply_iceberg_fork.py`).

### CLIENT CONTEXT -- who this is for and what number matters

The client (Simon's team) builds sustainability reports on Google Cloud.
Their product flow: users upload PDF documents, and AT THE SAME TIME the
backend kicks off a fleet of async LLM agents (orchestrated by Temporal)
that fill in the reports by SEARCHING the uploaded documents. The pipeline
turns each PDF into chunk rows (extracted text + an embedding vector per
chunk); search is hybrid -- full-text AND semantic (ANN over embeddings).

Simon's own words (Slack, 2026-08-06, lightly condensed):

> "Our users typically upload documents and at the same time trigger a
> bunch of agents async that start filling in our sustainability reports.
> What's important here is that when these agents START, they have
> up-to-date search capabilities, with ALL of the latest files that were
> uploaded. How we accomplish that today: when writing to qdrant, the
> vectors are immediately retrievable in search; we only kick off the
> agents once all documents are processed and written to qdrant (we use
> Temporal as the orchestration layer). If we have a separate cronjob, we
> never really know if the index is fresh. We can for sure use a cronjob,
> but a MANUAL TRIGGER will be important as well! Do you have any idea how
> long the refresh would take in specific cases (since this may add to our
> computation time)?"

What we answered / promised them:
- The manual trigger EXISTS and is synchronous: `CALL
  serenedb_reindex('<idx>')` returns only when the index reflects
  everything committed before the call. Their Temporal workflow becomes:
  ingest all documents -> one REINDEX activity (the barrier) -> start
  agents. Deterministic: call returned == agents see everything. A
  concurrent periodic refresh is optional on top; a claim collision just
  errors ("already in progress") and a standard Temporal retry absorbs it.
- **THE number the client cares about = wall time from INSERT start to
  index-queryable** (their agents' start is gated on it; it adds directly
  to their computation time). Secondary: query latency after the barrier
  (must be ms), and per-batch scaling (they batch uploads; corpus size
  must not matter).
- Numbers we already quoted them (pre-W3-fix, our box -> EUR4): ~5-6s for
  5k-100k-chunk batches (of which ~3s is the Iceberg commit itself),
  ~17s for a 1M-chunk batch (bandwidth-bound); queries after the barrier:
  FTS/ANN core ~1-2ms. Same-region compute should land ~3.5-4.5s. Every
  second we shave off the barrier is a second off EVERY report generation
  they run -- that is why this order exists.

Difference vs their current qdrant setup they should understand (and our
messaging so far): qdrant gives per-point read-after-write; we give
per-batch visibility with an explicit, deterministic barrier -- which
matches their "process everything, then start agents" flow exactly, at
the price of the barrier seconds this order is minimizing.

### GCP CONNECTION GUIDE (everything you need is on this box)

Credentials: Pavel's Workspace account is already authenticated in
`~/google-cloud-sdk` on this machine. You cannot log in interactively,
but you don't need to -- mint tokens non-interactively:

```bash
TOK=$(~/google-cloud-sdk/bin/gcloud auth print-access-token)
```

If that fails with "Reauthentication failed / cannot prompt" the Google
session expired (their ~daily policy): write BLOCKED into this file and
ask Pavel to run `gcloud auth login --no-launch-browser`. Do not try to
work around auth.

Constants:
- project: `steady-citron-463701-i8` (number `637037796821`)
- bucket: `gs://steady-citron-463701-i8-iceberg-20260804185720`
- BigLake Iceberg REST endpoint: `https://biglake.googleapis.com/iceberg/v1/restcatalog`
- warehouse (ATTACH path): `bl://projects/steady-citron-463701-i8/catalogs/iceberg-catalog`
- REST prefix (for raw curl): `/v1/projects/637037796821/catalogs/iceberg-catalog`
- every REST/HTTP call needs header `x-goog-user-project: steady-citron-463701-i8`

Tokens live ~1h: mint a FRESH one and re-create both secrets at the start
of every measurement phase (stale token = 401 mid-bench).

Connect from serened (this exact block is proven working):

```sql
SET GLOBAL unsafe_enable_version_guessing=true;  -- iceberg_scan views need it; GLOBAL so cron ticks inherit
CREATE SECRET gcs_data (TYPE GCS, BEARER_TOKEN '<TOK>');
CREATE SECRET biglake (TYPE ICEBERG, TOKEN '<TOK>',
  ENDPOINT 'https://biglake.googleapis.com/iceberg/v1/restcatalog',
  EXTRA_HTTP_HEADERS MAP{'x-goog-user-project': 'steady-citron-463701-i8'});
ATTACH 'bl://projects/steady-citron-463701-i8/catalogs/iceberg-catalog' AS gc (
  TYPE ICEBERG,
  ENDPOINT 'https://biglake.googleapis.com/iceberg/v1/restcatalog',
  AUTHORIZATION_TYPE 'oauth2', SECRET biglake);
```

Getting a table's storage location (needed for the `iceberg_scan` view):

```bash
curl -s -H "Authorization: Bearer $TOK" \
  -H "x-goog-user-project: steady-citron-463701-i8" \
  "https://biglake.googleapis.com/iceberg/v1/restcatalog/v1/projects/637037796821/catalogs/iceberg-catalog/namespaces/<ns>/tables/<t>" \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['metadata']['location'])"
```

The full Simon-flow harness, verbatim (server + table + index + one
measured batch; adapt sizes/iterations):

```bash
D=/tmp/w4_dd; rm -rf $D
build_bench/bin/serened $D --listen='postgres://0.0.0.0:<port>' > /tmp/w4.log 2>&1 &
sleep 5
# psql -h 127.0.0.1 -p <port> -U postgres -d postgres, then the SQL block above, then:
```

```sql
CREATE SCHEMA gc.w4;
CREATE TABLE gc.w4.chunks (doc_id BIGINT, chunk_no INT, body VARCHAR, emb FLOAT[]);
-- 100k-row corpus; NOTE list_transform per row (an uncorrelated subquery
-- would give every row the SAME vector):
INSERT INTO gc.w4.chunks SELECT g/20, g%20,
  'corpus chunk paragraph ' || g,
  list_transform(range(128), x -> random()::FLOAT) FROM range(0,100000) t(g);
CREATE TEXT SEARCH DICTIONARY w4_en(template='text', locale='en_US.UTF-8',
  case='none', stemming=false, accent=false, frequency=true, position=true);
CREATE VIEW w4_v AS SELECT doc_id, chunk_no, body, emb::FLOAT[128] AS emb
  FROM iceberg_scan('<location from GetTable>');
CREATE INDEX w4_idx ON w4_v USING inverted(doc_id, chunk_no, body w4_en,
  emb ivf (metric = 'l2'));
```

One measured iteration (bash; marker verifies REAL queryability):

```bash
BASE=<unique>; MID=$((BASE+77)); T0=$(date +%s%3N)
psql ... -c "INSERT INTO gc.w4.chunks SELECT g/20, g%20,
  CASE WHEN g=$MID THEN 'zzmarker$IT photosynthesis' ELSE 'batch p '||g END,
  CASE WHEN g=$MID THEN list_transform(range(128), x -> 0.9::FLOAT)
       ELSE list_transform(range(128), x -> random()::FLOAT) END
  FROM range($BASE, $((BASE+$SZ))) t(g)"
T1=$(date +%s%3N)
psql ... -c "SET unsafe_enable_version_guessing=true" \
         -c "SELECT action FROM serenedb_reindex('w4_idx')"
T2=$(date +%s%3N)
# verify: SELECT count(*) FROM w4_idx WHERE body @@ ts_phrase('zzmarker$IT') -> 1
#         SELECT 1 FROM w4_idx ORDER BY emb <-> list_transform(range(128),
#           x -> 0.9::FLOAT)::FLOAT[128] LIMIT 1 -> the marker is ANN top-1
echo "commit=$((T1-T0))ms reindex=$((T2-T1))ms total=$((T2-T0))ms"
```

HTTP census (the wire-truth tool): `CALL enable_logging(storage='stdout')`
+ `SET GLOBAL logging_level='debug'`, then parse the server log's
`{'request':` httpfs records (per-request duration_ms; ignore the Iceberg
extension's second body-bearing line per REST call -- it double-counts).

SAFETY RULES (Alexander's bucket): never touch the `demo` namespace or any
existing metadata/data files; work ONLY in namespaces you create (`w4`...)
and DROP them via the attached catalog when done; temp non-iceberg objects
go under `access-tests/` only; no recursive deletes against the bucket.
Leftover tables from earlier sessions: `sim.chunks` (~1.65M rows, AGED --
44+ manifests, do NOT bench on it; useful only for aging studies),
`vec.docs`, `bm2.docs`.

Known GCP facts to sanity-check against (pre-W3-fix numbers in the WO3
context below): RTT ~19ms warm / ~150ms cold per request; single-stream
~60MB/s, link ~90-100MB/s; Google-side commit POST ~1.1s and NOT ours;
LoadTable GET grows with snapshot count (aging).

### W4-1. Verify the W3 fixes on the wire

Fresh table, warm connections, HTTP census of: 1-row commit, 5k commit,
REINDEX no-op, REINDEX delta. Expect vs the WO3 context table: NO
HEAD-after-PUT pairs (#10), ONE metadata LIST per bind instead of two
(#11). If either ghost survives on GCS (local Glob semantics differ!),
that's a finding -- fix or document.

### W4-2. Re-measure the sweep on live GCP

Before/after style, fresh tables, markers verified, loadavg recorded:
5k / 10k / 50k / 100k x3 runs, 1M x1-2. Compare against the WO3 context
table (which was measured WITHOUT the W3 fork fixes) -- the delta is the
fixes' real-world value. Publish as "GCP AFTER-W3" table here.

### W4-3. Try the recommended flags on GCP

- `max_table_staleness` on ATTACH (single-writer ingest): A/B the
  per-commit LoadTable GET disappearance + wall-clock effect; document
  the multi-writer caveat.
- Anything else from the W3 flag sweep worth a live A/B.

### W4-4. Keep hunting

With the census after W4-1..3: what is now the top of the commit and the
delta? Candidates already known: Google POST (~1.1s, measure variance --
maybe batch-size dependent?), LoadTable growth (quantify vs snapshot
count on a purpose-aged table), guessing LIST remainder, the CPU phase at
100k+ (profile with build_perf if the box is quiet). Fix what is ours,
document what is Google's, with numbers.

### W4-5. Gates & wrap-up

Debug reindex suites (incl. periodic) + Release full index suite +
recovery trio after any code change; update this file (GCP AFTER-W3 table,
census tables, ledger); DROP every namespace you created; final summary:
what got faster, what remains and why, projected client numbers for
same-region compute.

---

## WORK ORDER 3 (2026-08-06, from Pavel) -- cut the insert->queryable latency. DONE (see W3 RESULTS below)

Run-to-completion contract: this section IS the approval for its scope;
don't stop between items; blocking decisions get written here as BLOCKED
and you move on. NO submodule bump. Staging untouched, `git stash`
forbidden. All numbers from optimized builds (build_bench / build_perf),
and **CHECK THE BOX LOAD FIRST** -- `uptime` before every measurement
series; loadavg above ~15 on this 32-core box poisons the CPU phases (we
measured 7-20s outliers at loadavg 135 that had NOTHING to do with the
code). Trap: `cmake --preset X` runs AUTO_UPDATE_MODULES and RESETS the
duckdb_iceberg fork -- run `scripts/reapply_iceberg_fork.py` immediately
after any cmake configure, BEFORE building.

### Context -- the client workflow and what we measured on live GCP

Client flow: batch-ingest N chunk rows (text + 128-dim FLOAT[] embedding)
into an Iceberg table (Google BigLake REST catalog, data on GCS) -> one
synchronous `CALL serenedb_reindex('<idx>')` as the freshness barrier ->
agents run hybrid FTS+ANN queries. Metric = wall time from INSERT start to
index-queryable (marker chunk found via ts_phrase AND as ANN top-1). One
round trip from this box to the EUR4 bucket ~150ms cold / ~19ms on a warm
connection; single-stream bandwidth ~60MB/s, link ceiling ~90-100MB/s.

Measured (fresh tables, medians; commit + REINDEX = total, seconds):

| batch | before fix | after fix |
|---|---|---|
| 5k    | 2.7+2.4=5.1 | 2.3+1.4=3.8 |
| 10k   | 3.1+2.6=5.7 | 2.4+1.6=4.0 |
| 50k   | 3.0+3.1=6.1 | 2.5+1.2=3.7 |
| 100k  | 3.2+3.1=6.3 | 2.6+2.7=5.4 |
| 1M    | 8.2+8.5=16.7 | 6.5+9=15.5 (bandwidth-bound: 2x0.5GB) |

"Fix" = `httpfs_connection_caching` was DEFAULT OFF in duckdb_httpfs:
every remote request opened a fresh TLS connection (strace: **106
connects for a 1-row commit**). Now forced on at server boot in
`server/query/server_engine.cpp` `RegisterServerExtensions` (SET GLOBAL on
an internal connection, error-checked; session SET overrides). Do not redo
this; DO look for more such flags.

Warm-commit HTTP census -- measured via `CALL
enable_logging(storage='stdout')` + `SET GLOBAL logging_level='debug'`,
HTTP entries land in the server log with per-request `duration_ms`. This
is THE measurement tool for this order:

| op (one 1-row commit) | cost |
|---|---|
| GET LoadTable (REST) | 429ms -- metadata json 23KB at 28 snapshots; GROWS with commit count |
| PUT parquet data file | 209ms |
| PUT manifest avro | 197ms |
| HEAD manifest avro (read-back right after the PUT!) | 54ms |
| PUT manifest-list avro | 176ms |
| POST commit (REST, Google-side processing) | **1111ms** |

REINDEX side: a delta binds the view TWICE (observe + rescan pass), and
each `iceberg_scan('gs://path')` bind pays version guessing = LIST of the
metadata dir + GETs (~0.75-1.2s cold) vs **0.19s** for a bind given the
exact metadata.json path; a read through the ATTACHED catalog resolves in
one REST call (0.35-0.46s). After the connection fix: delta floor
~1.1-1.4s (50k), no-op floor 0.20-0.33s (snapshot-id early exit).

Aging tax (do not confuse with regressions): every commit adds a manifest;
delta observe reads ALL manifest avros x2 binds. Our long-lived test table
reached 44 manifests / 864KB of metadata jsons and deltas visibly slowed.
Long-lived tables need iceberg maintenance (snapshot expiry / manifest
rewrite) -- out of scope; bench on FRESH tables.

### W3-1. Local reproduction rig (you have NO GCP credentials -- go local)

Build an insert->queryable harness against the LOCAL iceberg stack the
container tests already use: minio + Iceberg REST catalog (see
`tests/sqllogic/sdb/pg/index/reindex_view_iceberg.test_slow` for the
ATTACH/secret shape and the run.sh/docker wiring that provisions
MINIO_HOST/MINIO_PORT/ICEBERG_REST_URL; bring the compose stack up the way
CI does if it is not running). Reproduce the Simon flow: table (doc_id
BIGINT, chunk_no INT, body VARCHAR, emb FLOAT[]) -> view over
`iceberg_scan(<table location>)` casting `emb::FLOAT[128]` -> hybrid index
`inverted(doc_id, chunk_no, body <tokenizer>, emb ivf (metric='l2'))` ->
batch INSERT with a marker chunk -> `serenedb_reindex` -> verify marker
(ts_phrase + ANN top-1). Localhost RTT ~0 shrinks the network floors --
optionally add them back with `tc qdisc netem delay 20ms` if permitted;
even without it, REQUEST COUNTS are the target metric locally (HTTP census
per op). Validate transferability: requests x 150ms cold / 19ms warm +
bandwidth math must reproduce the GCP table above.

### W3-2. Find and fix the wasted requests / add flags

For each item: measure (HTTP census before/after), fix or flag, A/B
locally, keep only wins. Ranked candidates from the census:

1. **HEAD-after-PUT** on every uploaded avro -- find who issues it (httpfs
   FileSync/verification? avro writer reopen? iceberg write path?) and
   whether it can be skipped. ~50-200ms x files per commit.
2. **Sequential PUTs** -- data file(s), manifest, manifest-list upload one
   after another; contents are dependent but UPLOADS are independent until
   the commit POST references them. Overlap/parallelize (fork territory:
   `third_party/duckdb_iceberg/src/catalog/rest/transaction/*`; mirror
   every fork change into `scripts/reapply_iceberg_fork.py`).
3. **LoadTable GET before every commit** -- the transaction may already
   hold current metadata; hunt redundant re-GETs within one INSERT cycle.
4. **REINDEX double bind + guessing**: (a) can the rescan pass reuse the
   observe's resolved metadata (both binds belong to the same RunReindex --
   see ResolveSource vs PassConnection::RunPass)? (b) can version guessing
   skip the version-hint.text 404 probe when
   `unsafe_enable_version_guessing` is on? (c) is duckdb's external file
   cache serving metadata/manifest GETs on the second bind (check
   `external_file_cache`, verify HITS via the census; if cold, find why --
   iceberg sets validate_external_file_cache=false hints on its reads)?
5. **Flag sweep**: inventory httpfs/iceberg/duckdb settings affecting this
   road (`http_keep_alive`, `external_file_cache`, parquet prefetch,
   httpfs timeouts/retries, options in
   `third_party/duckdb_httpfs/src/httpfs_extension.cpp` and the iceberg
   ATTACH options). For each: default, census effect, recommendation.
   Ship safe-for-server defaults the same way the connection-cache fix
   shipped (boot-time SET GLOBAL in RegisterServerExtensions,
   error-checked, comment stating why).
6. If CPU phases matter at 100k+ (tokenize+IVF), profile with build_perf
   + perf record and report -- but network floors first.

### W3 RESULTS (2026-08-06) -- ALL ITEMS DONE

**W3-1 DONE -- local rig.** `$JOB_TMP/w3_stack.sh` (minio + iceberg-rest,
mirrors run.sh provisioning) + `w3_flow.sh <rows> <label>` (the Simon flow:
batch INSERT + marker -> serenedb_reindex -> ts_phrase AND ANN-top-1
verification, per-phase wall time + HTTP census) + `w3_census.py` (counts +
duration per method/url-class from the serened debug log; parse ONLY the
`{'request':` httpfs records -- the Iceberg extension logs a second
body-bearing line per REST call that double-counts). Transferability: local
request counts x GCP RTTs reproduce the GCP table's shape (commit ~= POST
+ LoadTable GET + 3 PUTs; the delta floor = 2 binds' metadata reads).
Local wall times were load-poisoned (loadavg 18-58 all day) -- REQUEST
COUNTS are the deliverable, as planned.

**W3-2 census table (5k rows, fresh table; requests per op):**

| op | before | after | what changed |
|---|---|---|---|
| INSERT commit (first) | 7-8 (GCP shape) | **5** | -2 HEAD-after-PUT (#10) |
| INSERT commit (later) | 8-9 | **6** | ditto (+1 old-list GET, inherent) |
| CREATE INDEX | 7 | **6** | -1 guessing LIST (#11) |
| REINDEX no-op | 4 | **3** | -1 guessing LIST (#11) |
| REINDEX delta | 15 | **13** | -1 LIST x 2 binds (#11) |

Commit = GET LoadTable, PUT parquet, [GET old manifest-list], PUT manifest,
PUT manifest-list, POST commit. Delta = 2x(LIST + hint HEAD + metadata
cache-validate HEAD) + metadata GET + manifest-list GET + 2 manifest GET +
3 parquet GET. GCP projection: #10 saves ~54ms x2/commit + RTTs; #11 saves
one LIST per bind (~150ms cold GCS) x2 per delta.

**Fixes shipped (fork patches, mirrored in reapply_iceberg_fork.py):**
- **#10 HEAD-after-PUT KILLED**: `manifest_file::WriteToFile` re-opened
  every written avro just to GetFileSize() for `manifest_length` (2x per
  commit: manifest + manifest list). The avro COPY already reports
  `file_size_bytes` via copy_to_get_written_statistics -- consumed directly,
  reopen kept as fallback for stat-less copy functions.
- **#11 single-LIST version guessing**: GuessTableVersion paid one remote
  Glob per version_name_format pattern (2 by default; REST-named tables
  never match the first `v*` pattern). Now ONE `metadata/*` listing +
  client-side prefix/suffix matching (each pattern has exactly one '*').
  RESTRICTED to the guessing branch: the hint road keeps its stock
  FileExists probe -- folding the hint probe into the listing broke
  local-fs hint tables (reindex_view_iceberg_local proj rung; mechanism
  unproven, likely local Glob semantics) and was withdrawn.

**Ledger -- tried and rejected/deferred:**
- **#4a observe-bind reuse in the delta pass** (planning.source_bind +
  leaf bind_data splice): REVERTED. Zero census win -- the splice runs
  AFTER the view bind, so the second bind's remote reads already happened;
  and it broke projected views (`SELECT id, body FROM iceberg_scan` --
  Vector::Reference VARCHAR vs BIGINT: the raw fast-path bind's column
  layout differs from the view-projected leaf's). A real fix = intercept
  the leaf's BIND (pass the resolved metadata path into iceberg_scan's
  options pre-bind) -- future work, fork+planning surgery.
- **#2 parallel PUTs**: deferred. manifest -> manifest-list are
  content-dependent (length), concurrent copy_to on one ClientContext is
  unsafe, and the phase is dominated by the 1111ms Google-side commit
  POST that no client change touches.
- **Hint-probe fold into the listing** (full #11): withdrawn, see above.
- **#3 LoadTable GET per commit**: protocol-inherent for
  `/v1/transactions/commit` (204, no metadata in response). Mitigation
  exists upstream: ATTACH option **`max_table_staleness`** gates a
  table_request_cache -- for single-writer ingest it legally absorbs the
  per-commit GET (conflicts self-heal via the retry road's
  RefreshFromCatalog). RECOMMENDATION, not a code change; A/B on GCP.
- **Metadata cache-validate HEADs**: kept -- that HEAD is the external
  file cache validating etags; removing it = reading stale metadata.

**Flag recommendations (final):**
- `httpfs_connection_caching=true` -- ALREADY forced at boot (the pre-W3
  fix; 106 connects -> reused pool).
- `max_table_staleness` on ATTACH for single-writer ingest loops (saves
  the LoadTable GET per commit; do NOT set for multi-writer tables).
- `unsafe_enable_version_guessing` GLOBAL for hint-less tables + periodic
  ticks (unchanged requirement); guessing itself is now 1 LIST cheaper.
- external_file_cache: default on, WORKING (metadata json GETs are served
  from cache with one validate-HEAD; observed: delta re-bind pays no
  second metadata GET). No change.
- http_timeout/http_retries/http_retry_wait_ms/keep_alive: defaults fine
  for GCS; nothing census-visible to tune.

**W3-3 gates (all green):** Debug reindex_view*.test both engines; full
sdb/pg/index dir Debug; recovery trio 3/3 (after fixing a stale fixture
assumption: the F-section held back v2-v5 metadata but the fixture grew
v6/v7 on 08-06 -- hold list now `v[2-9]`, guessing was picking v7
legitimately, NOT a #11 bug); Release full index suite 0 failures; all 4
container slow suites both engines (the REST+guessing road end-to-end).
Box load 18-58 all session (`uptime` checked before every series) -- all
timing comparisons deferred to counts per the work-order rule.

**OUT OF SCOPE confirmed:** submodule bump; catalog-resolved fast path;
GCP-side runs; table maintenance automation. CPU profiling at 100k+
(item 6) not reached: network censuses were the assignment's ranked
targets and the box never dropped below loadavg 18 for a clean CPU
series -- carry to a follow-up order if the client's 100k+ CPU phase
matters after the request fixes land.

### W3-3. Gates & deliverables
