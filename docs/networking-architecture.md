# SereneDB Networking Architecture -- Decision & Plan

**Date:** 2026-06-03 · **Status:** plan of record (supersedes the ArangoDB-derived stack)
**Method:** ~30 candidate libraries cloned and read from source (`~/projects/network`), 32 survey reports + adversarial review + targeted follow-ups (`~/projects/network/_reports/`, incl. `_SYNTHESIS.md`). Every call is grounded in code (`file:line` in the reports).

## Philosophy -- inherit, then rewrite with proof

Build the new server by **reusing the best pieces we already have** (asio, curl, DuckDB's scheduler + #23002 async pool, nghttp2, llhttp, OpenSSL, our own `message::Buffer` + reflection serde) on a **clean coroutine foundation**, dropping the ArangoDB scheduler/tasks/lanes/cron and its copy-heavy callbacks. Then run **bounded experiments**, each of which replaces one inherited piece **only when a benchmark proves it faster / less CPU / less memory** on SereneDB's real workload (analytical, HTTP-heavy, large results). Perf is the deciding metric -- and for our CPU-heavy (DuckDB) requests the transport is third-order, so the io_uring "30-50%" is echo-benchmark lore until measured here.

Two facts that shape everything:
- **The hot per-request cost is DuckDB execution + thread handoffs, not the socket layer.** The pg send path is already zero-copy scatter-gather (`message::Buffer` -> `SequenceView` -> `async_write`).
- **Every request hits DuckDB's pool anyway**, so an IO->DuckDB handoff is unavoidable and small (µs vs the query's tens-to-hundreds of µs). That's why the initial shape reuses DuckDB's scheduler rather than building a separate one.

---

## Part A -- Initial shape (implement first; large but bounded)

A from-scratch server that **replaces the ArangoDB networking stack** while **inheriting** mature libraries. No speculative new infrastructure.

**Transport.** boost.asio, epoll. `io_context` IO threads do **send/recv only** (thin). Keep `asio::ssl`.

**Scheduler -- reuse DuckDB's, build none of our own.** Drop the ArangoDB `Scheduler` + folly pool + 4 `RequestLane` priority lanes + the `JobObserver`/fill-grade load-shed + the `runCronThread`. Protocol handling (parse, connection, bind/execute), query execution, **and result serialization** all run on the **DuckDB `TaskScheduler` (REGULAR CPU + ASYNC #23002)**. Serialization follows **issue #646**: a `PhysicalPgWireCollector` whose `LocalSinkState` owns a thread-local `message::Buffer`, encodes wire bytes **on the executor worker thread**, then O(1)-splices its chunk chain onto the connection's `_send` (`message::Buffer::SpliceCommitted`) -- encoding parallel across workers, the strand just splices + `Commit` + writes.

**Async glue.** yaclib **stackless coroutines** (already zero-extra-alloc, already on the pg path) + a **`yaclib::IExecutor` that dispatches continuations onto DuckDB's `TaskScheduler`**. So most of `pg_comm_task` runs on the DuckDB pool; only raw socket IO is on the io_context thread. **Cancellation = DuckDB's query interrupt** (already supported by operators); `CommTask` is `shared_ptr`, so a socket close just completes the suspended coroutine -- no UAF, no stop-token needed yet.

**Buffer seam -- generalize `message::Buffer` (the one currency type).** Add: the **receive path** (IO thread produces, codec consumes -- kills the pg `_packet` double-copy + O(n) erase); a **neutral `Slice`/iovec** value_type (drop the cosmetic `boost::asio::const_buffer` coupling); **`SpliceCommitted`** (O(1) N-producer->one-buffer merge -- also the answer to HTTP/2 multiplexing: per-producer SPSC buffer + single-threaded splice, no MPSC); a **chunk freelist** (replace per-chunk `::operator new`); and the **arm64 atomic fix** -- `std::atomic<BufferOffset>` is silently lock-based on the default `armv8-a` build, so require `armv8.2`/`+lse` + a `static_assert`.

**Protocols -- a codec registry** (proxygen codec/session/transaction shape + brpc "protocol = registered data"), replacing the hard-coded `TransportType` if/else in `acceptor_tcp.cpp`. Adding a protocol = one codec class + one factory line. Order: **pg-wire first** (re-home the existing mature codec; it's the live protocol), then **HTTP/1.1 (llhttp)** and **HTTP/2 (nghttp2)** as codecs. Utilities: **ada** (URL), **re2** (routing/regex -- never `std::regex`, never regex on the hot path), **simdjson** (JSON read) + the in-house **`serializer.h`/`simdjson_sink.h`** reflection serde (JSON write, into `message::Buffer`), **simdutf** (UTF-8 validation; its base64 also helps auth, which lands separately), and the in-house **`TrivialBiMap`** (`libs/basics/containers/trivial_map.h`) for HTTP header-name<->enum lookup (userver-style, compile-time, allocation-free -- faster than a hash for the fixed header set). simdutf, ada, and re2 are all Part A.

**TLS.** OpenSSL 3.5 (ClickHouse fork) via `asio::ssl`, as-is.

**Client.** **curl, as-is** (synchronous, behind DuckDB's `HTTPClient`) for both `read_parquet` and our own outbound -- **one client**, just not async yet. SigV4 stays DuckDB's own `s3fs.cpp` + mbedtls (not aws-sdk, not curl).

**Compression.** zlib-ng (gzip/deflate) + lz4; add zstd to `Accept-Encoding`; **decode off the IO thread** (CPU pool).

**Must-dos inside Part A** (not deferrable): the `message::Buffer` recv + arm64 + `SpliceCommitted` work, and re-homing pg-wire. Timers can stay on a simple mechanism initially (the hashed wheel is Experiment 1). **Auth (SCRAM-SHA-256, etc.) is a separate workstream the owner drives -- out of scope here**; Part A only leaves a clean auth hook in the connection handshake for it to slot into.

**Prerequisite:** update the vendored `serenedb/duckdb` fork to pick up #23002 + the `AsyncResult`/`InterruptState` async-scan seam. The fork is only ~1-2 weeks behind upstream and merge-based, so this is a **few hours** (a quick merge), not a rewrite -- the "961 commits" number is misleading for a merge-based fork.

> Part A is a clean rewrite of the **server skeleton** on coroutines + reused libraries. It is large, but bounded -- no new schedulers, no new clients, no new event loops, no new TLS.

---

## Part B -- Experiments (rewrite with proof; each gated on a benchmark)

Each experiment states a hypothesis, swaps one inherited piece, and **must show a measured win** (p50/p99 latency, CPU per request, memory/threads, throughput) on the CH/ES-like workload -- else it rolls back. They are independent and individually shippable behind seams.

**Experiment 0 -- Async curl client.** Build the `curl_multi_socket_action` + yaclib shim (`CurlMultiLoop`, ~300-500 LOC) -> `Future<HTTPResponse>`, driven on our io_context; make `read_parquet` async via the #23002 `AsyncResult` seam. *Hypothesis:* concurrent outbound + parquet scans share one IO thread/pool with no blocked workers. *Proof:* concurrent-request throughput and worker-occupancy vs the sync baseline. *Watch:* DuckDB upstream may land an async curl client first -- if so, adopt rather than duplicate.

**Experiment 1 -- One yaclib unified pool + timer wheel (the big consolidation).** Replace **all** of {separate io_context IO threads, DuckDB REGULAR pool, DuckDB ASYNC pool, the search refresh/compaction pools, the cron thread} with a **single yaclib work-stealing pool** (the `golang-thread-pool2` Tokio-port: per-worker WSQ, batch steal, parker) whose **workers each drive an `io_context`** (Tokio model -- a connection's IO completion and its coroutine resume on the same worker, collapsing the handoff), plus a **hashed/hierarchical timing wheel** (per-worker, feeding the park timeout; replaces cron and yaclib's blocking waits). Net-new build items: the WS-pool rebase onto the current yaclib core, the io_context-as-driver hook (the zero-timeout maintenance poll so a busy worker still services epoll), the timing wheel, and an admission/backpressure check (bounded -- no fill-grade contraption, no priority lanes; Go/Tokio prove flat works). DuckDB's pool becomes a *subsumed* consumer, or stays as the heavy-CPU pool with the WS pool as the IO/coroutine driver -- the experiment decides which by measurement. *Proof:* end-to-end latency (handoff elimination), total thread count, CPU under load. *This is the largest experiment; gate it hard.*

**Experiment 2 -- Native HTTP client (drop curl) + drop AWS SDK for SigV4.** A client built on the **server libs** (asio/io_uring + nghttp2 + llhttp + `message::Buffer` + ada + AWS-LC), swapped behind DuckDB's `HTTPClient` (clean 6-virtual seam, one factory). The base client is needed anyway for our own outbound + future server RPC; the *delta* to also cover object-store is bounded -- **async DNS** (c-ares; needed regardless), **proxy/CONNECT/auth/NO_PROXY**, **redirect-following** (+ S3 re-sign), and **S3-compatible-store quirk tolerance** (the genuinely fuzzy bit). Separately, **drop the `aws-sdk-cpp`/`aws-c-*` dependency**: SigV4 *signing* is already manual in DuckDB's `s3fs.cpp` (mbedtls); the real work is reimplementing the **credential-provider chain** (env, `~/.aws/credentials`, IMDS, STS assume-role, SSO). *Hypothesis:* lower per-request floor (no 91-setopt easy-handle tax), `message::Buffer` zero-copy receive, far fewer heavy deps. *Proof:* per-request CPU + object-store parity + deps removed. *Phased:* harden on our own paths first; the curl drop and aws-sdk drop are the endgame, not step 1.

**Experiment 3 -- OpenSSL -> AWS-LC.** Drop-in under `asio::ssl` (it carries the OpenSSL 1.1.1 API + `BIO_new_bio_pair`); gets AVX-512 AES-GCM + a normal CMake build. *Real work:* revalidate curl's + nghttp2's OpenSSL backend against AWS-LC (`HAVE_AWSLC` is pre-staged). *Proof:* handshake/crypto throughput on target hardware (the "AWS-LC is faster" claim is from asm presence, not a benchmark -- measure first).

**Experiment 4 -- Open slot (pick by need + measurement).** Candidates: a **raw io_uring reactor** under the WS pool (gated; large-HTTP egress + `SEND_ZC` + streaming first, pg-wire second; needs KTLS to beat the TLS encrypt-copy); **kTLS**; a **native async pg-wire client** for federation (vs blocking libpq); new protocols (**mysql**, **quack** on our stack, Arrow **Flight SQL** -- heavy, gRPC, walled-off if ever); **HTTP/3** (out of scope for now).

---

## Part C -- The first HTTP feature to serve (Part A's HTTP consumer)

Part A's HTTP codec needs a real first consumer -- easy to implement and test but genuinely useful. **Decision: a subset of the Elasticsearch REST/DSL API.** It advances the roadmap "elastic DSL" protocol directly, maps onto SereneDB's **iresearch** search engine, has the richest client ecosystem to test against (curl, Kibana, every ES/OpenSearch SDK, Logstash/Beats), and is cleanly *subsettable* so the first cut stays small. Implement in tiers:

- **Tier 0 -- handshake + ops (tiny):** `GET /` (cluster name/version -- ES clients ping this to confirm they're talking to Elasticsearch), `GET /_cluster/health`, and a couple of `GET /_cat/{indices,health,nodes}` (text, or `?format=json`). Brings the server up and lets ES clients connect.
- **Tier 1 -- ingest (small; exercises POST + NDJSON streaming):** `POST /_bulk` and `POST /{index}/_bulk` (newline-delimited action+doc JSON -- parse with simdjson `iterate_many`/`document_stream`), plus `PUT /{index}` / `PUT /{index}/_mapping` (create index/mapping -> SereneDB schema) and `POST /{index}/_doc`. This is how Logstash/Beats/Filebeat load data -- immediately useful.
- **Tier 2 -- minimal search (the core; subset the DSL hard):** `POST /{index}/_search` supporting **`match` / `term` / `range` / `bool`(must/should/filter)** + `size` / `from` / `sort` / `_source`, returning the standard hits envelope; plus `GET /{index}/_count` and `GET /{index}/_doc/{id}`. **Defer** aggregations, `_msearch`, scroll/PIT, scripting, and the long-tail DSL.
- **Implementation reuse:** parse the DSL + bulk NDJSON with **simdjson** + the in-house reflection serde (`ReadObject` into ES request structs); **route** `/{index}/_search` etc. with **re2** / a small trie; serialize the hits envelope with **`WriteObject` into `message::Buffer`** (the #646 streaming path for large result sets); back `_search`/`_bulk` with the **iresearch** engine. Exercises the entire HTTP path: GET + POST, NDJSON request streaming, JSON request/response, format + compression, large streamed responses. **Testable** with curl, Kibana, and any ES/OpenSearch SDK -- instant external validation with a bounded surface.
- *Alternatives considered, deferred:* ClickHouse HTTP / Trino REST (SQL-over-HTTP -- strong, but mimics a SQL competitor rather than advancing the search roadmap), InfluxDB line-protocol (easiest ingest), `quack` (DuckDB-native, coupled binary). A trivial `/health` + Prometheus `/metrics` is the pre-Tier-0 smoke test to stand the server up.

---

## Appendix -- library decisions, corrections, evidence

**Adopt / keep & evolve:** asio (-> io_uring backend later; standalone-asio drops boost), DuckDB `TaskScheduler` + #23002 async pool (the scheduler), yaclib (coroutines + IExecutor-over-TaskScheduler; the Go WS pool is Exp 1), `message::Buffer` (generalized) + `serializer.h`/`simdjson_sink.h`, nghttp2, llhttp, curl (-> async Exp 0 -> maybe drop Exp 2), ada, re2, simdjson, simdutf, zlib-ng + lz4 + zstd, OpenSSL (-> AWS-LC Exp 3), liburing (Exp 4), quack-on-our-stack, ADBC scanner (client; ~90% already vendored, no gRPC).
**Reject:** the ArangoDB scheduler/tasks/lanes/cron, folly, libuv, libevent, s2n-tls, BoringSSL (vs AWS-LC), glaze, yyjson (for our code), oatpp, cpp-httplib server, the arrow-adbc Flight-**server** path, libpq-async-wrapping, a new IOBuf-like buffer (generalize ours instead), in-pool priority lanes.
**Inspiration-only (patterns, not deps):** Seastar (IoBackend vtable, SPSC IO<->CPU queues), Tokio/Go (worker+driver+wheel, no priorities, spawn_blocking), stdexec/libunifex (cancellation + io_uring shapes), proxygen/wangle + brpc + sogou (codec/session/registry), h2o (sendvec, slab recycler, KTLS), `await` (cancellation/structured-concurrency design to port onto yaclib later -- stackless parts only; its fibers are out of scope).

**Corrections embedded in this plan (vs the first survey):** yaclib is already stackless + zero-extra-alloc (nothing to "port" there); cancellation = DuckDB query interrupt, not a UAF/lifetime problem; there is **no elastic protocol yet** and `search_sink_writer` is not a wire protocol; JSON is already in-house (drop glaze/yyjson for our code); the vendored DuckDB fork is only ~1-2 weeks behind upstream (a few-hours merge to update, not a 961-commit divergence) and **lacks** #23002; the `message::Buffer` 16-byte atomic is an arm64 correctness bug; `read_parquet` is **no longer** a permanent sync wall (#23002 makes scans async).

**Evidence:** `~/projects/network/_reports/` -- the 32-report survey + `_SYNTHESIS.md`, `A1b_yaclib_deep.md`, `A7_await.md`, `F1`-`F5`, `N1`-`N4`. Cloned sources in `~/projects/network/`.
