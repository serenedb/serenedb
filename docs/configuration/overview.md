---
title: Configuration
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB has a number of configuration options that can be used to change the behavior of the system. The defaults listed here are a server's; `serened shell` has other defaults for some options, such as `integer_division` (`false`), `preserve_identifier_case` (`preserve_case`) and the default schema (`main`).

The configuration options can be set using either the [`SET` statement](../sql/statements/set/index.md) or the [`PRAGMA` statement](../configuration/pragmas.md). They can be reset to their original values using the [`RESET` statement](../sql/statements/set/index.md#reset).

The values of configuration options can be queried via the [`current_setting()` scalar function](../sql/functions/utility.md) or the `pg_settings` view. For example:

<SqlLogicTest id="configuration/overview/example_001" />

Or:

<SqlLogicTest id="configuration/overview/example_002" />

## Examples

Set the memory limit of the system to 10 GB.

<SqlLogicTest id="configuration/overview/example_003" />

Run the queries of the current session on 1 thread:

<SqlLogicTest id="configuration/overview/example_004" />

Turn logging on and set the logging level to `debug`. For additional details on logging levels, see [Log Level](./logging/index.md#log-level).

<SqlLogicTest id="configuration/overview/example_005" />

Write a single log message with the `debug` level and a `connection` scope:

<SqlLogicTest id="configuration/overview/example_006" />

Write a single log message with a `debug` level and a `connection` scope and a custom `log_type`:

<SqlLogicTest id="configuration/overview/example_007" />

Check logs with the `DEBUG` log level:

<SqlLogicTest id="configuration/overview/example_008" />

Check logs with the `QueryLog` type:

<SqlLogicTest id="configuration/overview/example_009" />

Check current logging settings:

<SqlLogicTest id="configuration/overview/example_010" />

Set the server's default null order to `NULLS LAST`:

<SqlLogicTest id="configuration/overview/example_012" />

Return the current value of a specific setting:

<SqlLogicTest id="configuration/overview/example_013" />

Query a specific setting:

<SqlLogicTest id="configuration/overview/example_014" />

Show a list of all available settings:

<SqlLogicTest id="configuration/overview/example_015" />

Reset the memory limit of the system back to the default:

<SqlLogicTest id="configuration/overview/example_016" />

## Secrets Manager

SereneDB has a [Secrets manager](../sql/statements/create_secret/index.md), which provides a unified user interface for secrets across all backends (e.g., AWS S3) that use them.

## Configuration Reference

The Scope column gives the scope a plain `SET` changes: a `GLOBAL` option changes for every session on the server, a `SESSION` option only for the current session. `SET GLOBAL`, `SET SESSION` and `SET LOCAL` name the [scope](../sql/statements/set/index.md#scopes) explicitly, for the options that take it. The options are grouped by the part of the system they affect.

To search the reference rather than read it, query the catalog -- every option on this page is a `setting` object:

```sql
SELECT name, summary FROM sdb_docs.objects
WHERE kind = 'setting' AND name LIKE 'compaction%';
```

### Search and indexing

Background maintenance of inverted indexes, and the knobs the search and vector operators read.

| Name | Description | Type | Scope | Default value |
| --- | --- | --- | --- | --- |
| `cleanup_interval_step` | Number of automatic refreshes between background unreferenced-file cleanup passes for newly created inverted indexes and search tables. WITH (cleanup_interval_step = ...) overrides it per index or table. 0 disables the periodic passes: cleanup then follows only background compaction and, on a search table, VACUUM, and a search table keeps more write-ahead logs (see [Durability](../sql/indexes/inverted/maintenance.md#durability)). Default: 1. | `UINTEGER` | `SESSION` | `1` |
| `compaction_floor_segment_bytes` | Inverted-index segments below this size count as equal-sized for compaction candidate selection. Per-index WITH (compaction_floor_segment_bytes = ...) overrides. Default 2097152 (2MB). | `UBIGINT` | `SESSION` | `2097152` |
| `compaction_interval` | Background compaction interval (ms) for newly created inverted indexes. Per-index WITH (compaction_interval = ...) overrides. 0 disables the compaction task. Default: 1000. | `UINTEGER` | `SESSION` | `1000` |
| `compaction_max_segments` | Maximum inverted-index segments merged by one background compaction. Per-index WITH (compaction_max_segments = ...) overrides. Default 10. | `UINTEGER` | `SESSION` | `10` |
| `compaction_max_segments_bytes` | Byte budget of one background inverted-index compaction (the tier target size). Per-index WITH (compaction_max_segments_bytes = ...) overrides. Default 5368709120 (5GB). | `UBIGINT` | `SESSION` | `5368709120` |
| `refresh_interval` | Background refresh interval (ms) for newly created inverted indexes. Per-index WITH (refresh_interval = ...) overrides. 0 disables the refresh task. Default: 1000. | `UINTEGER` | `SESSION` | `1000` |
| `reindex_interval` | Periodic reindex interval (ms) for newly created VIEW-backed inverted indexes: a non-zero value creates a [job](../sql/indexes/inverted/views.md#automatic-refresh) named after the index that reindexes it, each pass starting that long after the previous one finished. Per-index WITH (reindex_interval = ...) overrides; ALTER INDEX SET retunes, creates or drops the job, and ALTER INDEX RESET applies this setting. 0 creates no job. Hint-less iceberg sources refresh from the background only when the GLOBAL unsafe_enable_version_guessing is set. Default: 0. | `UINTEGER` | `SESSION` | `0` |
| `sdb_ann_force_exact` | When true, a vector search (`ORDER BY <distance> LIMIT k`, or a distance range) returns the exact answer instead of walking the ANN index, split across the scan's workers. Every row with a vector is considered; where an `hnsw` index stores 8-bit codes, a row's stored vector is read only if its code leaves it a chance at the top `k`. Default false, which leaves the choice to the engine. | `BOOLEAN` | `SESSION` | `off` |
| `sdb_ann_oversample` | Oversample for a quantized vector-similarity query, HNSW and IVF alike: `ceil(sdb_ann_oversample * k)` of each segment's candidates are re-read at full precision and re-ordered before the segments' results are merged, which raises recall at some cost in latency. 0 disables the rescore: the query answers from the quantized codes, which is faster and caps recall at whatever the codes can tell apart -- well below 1 for a 4-bit or binary quantizer, and worse still across many segments. -1 (the default) rescores every quantized index at 1. Fractional values are allowed, but a factor between 0 and 1 is rejected. Unquantized (quant = 'none') indexes are never rescored. | `DOUBLE` | `SESSION` | `-1` |
| `sdb_compact_target_segments` | How many segments `VACUUM (COMPACT_*)` leaves in a search table. 0 or 1 merges every segment into one; a larger N splits the segments into N groups and merges each group into one segment, so no single merge holds the whole table. A table with N segments or fewer is left as it is. | `UINTEGER` | `SESSION` | `1` |
| `sdb_disable_top_k_optimization` | When true, an `ORDER BY <scorer>(...) DESC LIMIT k` query over an inverted index scores every matching document instead of skipping those that cannot reach the top k, so it runs slower. Useful to compare plans or to work around a plan regression. Default: false (optimization on). | `BOOLEAN` | `SESSION` | `off` |
| `sdb_hnsw_column_filter` | How an HNSW graph walk answers a predicate only the columnstore can: 'fold' evaluates it over the whole segment before the walk; 'read' reads the columns of each row the walk reaches; 'auto' reads while the walk is expected to reach few enough rows for that to cost less than the fold, weighing what one read costs in the columns' encodings. Default 'auto'. | `VARCHAR` | `SESSION` | `auto` |
| `sdb_hnsw_ef_search` | Search-time beam width (ef) of each segment's HNSW search. Higher values improve recall at the cost of latency. A value is used as written, floored only at what a segment must return for the query's LIMIT. -1 (the default) lets the engine choose: the index's ef_construction or the LIMIT, whichever is larger, so an untuned query keeps its recall at any LIMIT. With a quantized index every hit of the beam is rescored exactly. | `INTEGER` | `SESSION` | `-1` |
| `sdb_hnsw_filter_mode` | How an HNSW vector index answers a query with a WHERE: 'auto' picks by the predicate's estimated selectivity, and walks as 'twohop' where a vector's code is long enough that crossing a rejected row costs less than scoring it; 'scan' scores every row the predicate admits; 'walk' walks the graph scoring every neighbour and passing through rejected rows; 'prune' walks scoring and expanding admitted rows only; 'twohop' walks expanding a rejected row's neighbours in its place; 'bridge' walks scoring every neighbour but expanding a rejected row into admitted ones only. Default 'auto'. | `VARCHAR` | `SESSION` | `auto` |
| `sdb_ivf_max_search_fanout` | Upper bound on the beam of the descent through a multi-level IVF centroid tree, where each level keeps the beam's worth of its nearest centroids and scores only their children. The beam follows sdb_ivf_search_nprobe as the query resolves it; a narrower one scores fewer centroids at some recall cost. Applied after sdb_ivf_min_search_fanout. -1 (the default) leaves the beam unbounded. | `INTEGER` | `SESSION` | `-1` |
| `sdb_ivf_min_search_fanout` | Lower bound on the beam of the descent through a multi-level IVF centroid tree, where each level keeps the beam's worth of its nearest centroids and scores only their children. The beam follows sdb_ivf_search_nprobe as the query resolves it; a wider one reaches clusters a narrow descent misses, and one as wide as the segment's list count makes the descent exact. -1 (the default) leaves the beam unbounded. | `INTEGER` | `SESSION` | `-1` |
| `sdb_ivf_posting_size` | Target IVF posting-list size (leaf cap t), captured into the index config at CREATE INDEX. Smaller values force deeper multi-level centroid trees (useful for testing). Default 1024. | `INTEGER` | `SESSION` | `1024` |
| `sdb_ivf_sample_factor` | Fraction of rows used to train the IVF centroid tree (sample_size = sample_factor * N), captured into the index config at CREATE INDEX. Any value in (0, 1]. Default 0.2. | `DOUBLE` | `SESSION` | `0.2` |
| `sdb_ivf_search_nprobe` | Number of IVF cluster lists each segment scans per vector-similarity query. Higher values improve recall at the cost of latency. A value is used as written. -1 (the default) lets the engine choose per segment: 1.3 * log10(max(LIMIT, 10)) * sqrt(lists), where lists is the segment's rows over the index's posting size, which keeps recall near 0.95 at any LIMIT and segment size. | `INTEGER` | `SESSION` | `-1` |
| `sdb_levenshtein_max_terms` | The maximum number of dictionary terms a fuzzy predicate (`ts_levenshtein`) expands to per index segment. Terms closest to the query survive; the rest neither match nor contribute to scoring. Higher values improve recall on wide expansions at the cost of per-query work. 0 removes the cap, so every term within the edit distance matches. Default 50. | `INTEGER` | `SESSION` | `50` |
| `sdb_pattern_cache_size` | Bytes of compiled search patterns (`ts_regexp`, `ts_like`, LIKE and fused alternations over an inverted index) kept across queries, so a pattern is compiled once per server rather than once per query. The least recently used patterns are dropped first; a pattern in use by a running query stays alive until it finishes. 0, the default, keeps nothing: each query compiles its own patterns. Server-global. | `UBIGINT` | `GLOBAL` | `0` |
| `sdb_scan_no_split_row_groups` | A segment with at most this many row groups is always scanned whole by one worker of an inverted-index scan. | `INTEGER` | `SESSION` | `1` |
| `sdb_scan_order` | The order in which an inverted-index scan takes its segments: `largest_first` and `smallest_first` order them by live document count; `order` takes them best first by the `ORDER BY` column's row-group statistics when the query has a scan order; `auto` is `order` under a scan order and `largest_first` otherwise. | `VARCHAR` | `SESSION` | `auto` |
| `sdb_scan_split` | How an inverted-index scan shares a segment between workers: `tail` gives each worker a segment of its own and lets an idle worker join the segment with the most row groups left only once no segment is unclaimed; `always` puts every worker on the same segment until all its row groups are claimed; `never` scans each segment whole on one worker; `auto` is `always` when the query has an `ORDER BY ... LIMIT` scan order and `tail` otherwise. See [Session settings](../sql/indexes/inverted/maintenance.md#session-settings). | `VARCHAR` | `SESSION` | `auto` |
| `search_backfill_group_bytes` | How many bytes of a search table's existing segments `CREATE INDEX` rewrites before it swaps them in and moves on to the next group. The build needs extra disk of about twice one group; a value above the table's size, or 0, rewrites the whole table in one swap. | `UBIGINT` | `SESSION` | `1073741824` |
| `segment_docs_max` | Document count at which an inverted-index segment writer rolls over to a new on-disk segment. Per-index WITH (segment_docs_max = ...) overrides. 0 = unlimited (memory limit governs). | `UINTEGER` | `SESSION` | `0` |
| `segment_memory_max` | In-memory bytes an inverted-index or search-table segment writer fills before rolling over to a new on-disk segment (also the CREATE INDEX backfill commit cadence). Per-object WITH (segment_memory_max = ...) overrides. Default 268435456 (256MB). A serial write to a search table also buffers up to `checkpoint_threshold` of rows; see [Write memory](../sql/indexes/inverted/maintenance.md#write-memory). | `UBIGINT` | `SESSION` | `268435456` |

### AI functions

Secrets, retries and quotas of the [AI functions](../sql/functions/ai.md).

| Name | Description | Type | Scope | Default value |
| --- | --- | --- | --- | --- |
| `sdb_ai_allow_insecure_endpoint` | When false, AI functions refuse a secret whose base_url sends requests over plain http:// to a host other than localhost, 127.0.0.0/8 or ::1, because the prompts and the API key would travel unencrypted. Default: false. | `BOOLEAN` | `SESSION` | `false` |
| `sdb_ai_embedding_default_secret` | Name of the openai secret used by ai_embed and ai_similarity when the call does not pass secret_name. Default: '' (no default). | `VARCHAR` | `SESSION` |  |
| `sdb_ai_embedding_max_batch_size` | Maximum number of texts ai_embed and ai_similarity send in one embeddings request. Default: 100. | `UINTEGER` | `SESSION` | `100` |
| `sdb_ai_max_api_calls_per_query` | Maximum number of AI provider requests a single query may send. 0 = unlimited. Default: 0. | `UINTEGER` | `SESSION` | `0` |
| `sdb_ai_max_concurrent_requests` | Maximum number of AI provider requests a query has in flight, across all of its AI calls and threads. Requests are sent by async tasks (async_threads) and by the threads that evaluate the calls. Default: 16. | `UINTEGER` | `SESSION` | `16` |
| `sdb_ai_max_output_tokens_per_query` | Maximum number of output tokens, as reported by the provider, a single query may consume; checked before each request, so requests already in flight may exceed it. 0 = unlimited. Default: 0. | `UINTEGER` | `SESSION` | `0` |
| `sdb_ai_max_retries` | How many times an AI provider request is retried after a connection error or HTTP 408, 429, 5xx or 529. Default: 3. | `UINTEGER` | `SESSION` | `3` |
| `sdb_ai_request_timeout` | Timeout of a single AI provider request, in seconds. Default: 120. | `UINTEGER` | `SESSION` | `120` |
| `sdb_ai_retry_initial_delay_ms` | Delay before the first AI provider retry, in milliseconds; each further retry doubles it, up to 60 seconds. A Retry-After response header overrides it, up to 60 seconds. Default: 500. | `UINTEGER` | `SESSION` | `500` |
| `sdb_ai_system_one_default_secret` | Name of the typesafe secret used by ai_system_one when the call does not pass secret_name. Default: '' (no default). | `VARCHAR` | `SESSION` |  |
| `sdb_ai_text_default_secret` | Name of the openai secret used by ai_generate, ai_classify, ai_classify_labels, ai_extract, ai_filter, ai_translate, ai_redact, ai_score, ai_rerank, ai_agg and ai_summarize_agg when the call does not pass secret_name. Default: '' (no default). | `VARCHAR` | `SESSION` |  |
| `sdb_ai_throw_on_error` | When true, a row whose AI function request fails fails the query; when false, that row returns NULL. Authentication, not-found, validation (422) and exhausted-quota (429 insufficient_quota) errors always fail the query. Default: true. | `BOOLEAN` | `SESSION` | `true` |
| `sdb_ai_throw_on_quota_exceeded` | When true, exceeding sdb_ai_max_api_calls_per_query or sdb_ai_max_output_tokens_per_query fails the query; when false, the remaining rows return NULL. Default: true. | `BOOLEAN` | `SESSION` | `true` |

### Memory and threads

How much memory the engine may use and how many threads it runs.

| Name | Description | Type | Scope | Default value |
| --- | --- | --- | --- | --- |
| `allocator_background_threads` | Whether to enable the allocator background thread. | `BOOLEAN` | `GLOBAL` | `false` |
| `allocator_bulk_deallocation_flush_threshold` | If a bulk deallocation larger than this occurs, flush outstanding allocations. | `VARCHAR` | `GLOBAL` | `512.0 MiB` |
| `allocator_flush_threshold` | Peak allocation threshold at which to flush the allocator after completing a task. | `VARCHAR` | `GLOBAL` | `134217728B` |
| `async_threads` | The number of async tasks, such as I/O, a query runs at once. -1 = automatic, four times the number of CPU cores, at most 256; 0 runs the I/O on the threads that run the query (see [`SET`](../sql/statements/set/index.md#async-threads)). | `BIGINT` | `SESSION` | `-1` |
| `block_allocator_memory` | Physical memory that the block allocator is allowed to use (this memory is never freed and cannot be reduced). | `VARCHAR` | `GLOBAL` | `0 bytes` |
| `max_memory` | The maximum memory of the system (e.g. 1GB). | `VARCHAR` | `GLOBAL` | 80% of RAM |
| `max_temp_directory_size` | The maximum amount of data stored inside the 'temp_directory' (when set) (e.g., 1GB) | `VARCHAR` | `GLOBAL` | `90% of available disk space` |
| `memory_limit` | Alias of `max_memory`. | `VARCHAR` | `GLOBAL` | 80% of RAM |
| `operator_memory_limit` | The maximum memory for query intermediates (sorts, hash tables) per connection (e.g. 256MB). | `VARCHAR` | `SESSION` |  |
| `temp_file_encryption` | Encrypt all temporary files if database is encrypted | `BOOLEAN` | `GLOBAL` | `false` |
| `threads` | Caps the threads the queries of the session run on (with `SET LOCAL`, of the transaction). `SET GLOBAL threads` sets the size of the thread pool of the whole server. See [`SET`](../sql/statements/set/index.md#threads). | `BIGINT` | `SESSION` | the number of CPU cores |
| `worker_threads` | Alias of `threads`. | `BIGINT` | `SESSION` | the number of CPU cores |
| `write_buffer_row_group_memory_limit` | The maximum data (in bytes) to buffer in row groups before flushing them together. When either this limit or `write_buffer_row_group_count` is reached, the data is flushed to disk. | `VARCHAR` | `GLOBAL` | 20% of `memory_limit` divided by `threads` |

### Storage and checkpointing

Durability, the write-ahead log, and how rows are laid out on disk.

| Name | Description | Type | Scope | Default value |
| --- | --- | --- | --- | --- |
| `checkpoint_on_detach` | Whether `DETACH` checkpoints the database: `ENABLED` checkpoints it, `DISABLED` does not, and `DEFAULT` checkpoints it unless `PRAGMA disable_checkpoint_on_shutdown` has turned checkpointing on shutdown off. The checkpoint is skipped while another connection still uses the database. | `VARCHAR` | `SESSION` | `DEFAULT` |
| `checkpoint_threshold` | The WAL size threshold at which to automatically trigger a checkpoint (e.g. 1GB). Also the size of the row buffer a serial write to a search table fills before it writes the rows to the table's index; see [Write memory](../sql/indexes/inverted/maintenance.md#write-memory). | `VARCHAR` | `GLOBAL` | `16.0 MiB` |
| `default_block_size` | The block size of newly created database files. | `UBIGINT` | `GLOBAL` | `262144` |
| `disabled_compression_methods` | Disable a specific set of compression methods (comma separated) | `VARCHAR` | `GLOBAL` |  |
| `enable_optimistic_write` | Whether or not to optimistically write large appends to disk before committing. Disable this to keep bulk appends in memory. | `BOOLEAN` | `GLOBAL` | `true` |
| `fsync_mode` | How to synchronize database file changes to stable storage (`STANDARD`, `NONE` or `FULL`). `NONE` may cause database corruption after an operating system or hardware crash. | `VARCHAR` | `GLOBAL` | `STANDARD` |
| `initial_column_segment_size` | The initial memory (in bytes) reserved for the first transient column segment. Must be a power of two. Subsequent segments double in size until reaching the block size. | `UBIGINT` | `GLOBAL` | `2048` |
| `recovery_replay_depth` | Maximum number of write-ahead-log chunks each inverted index holds while recovery replays it; a larger value uses more memory during recovery. 0 = 4 x the number of threads. Recovery reads the server's global value: set it at start with the `--recovery_replay_depth` flag. | `UINTEGER` | `SESSION` | `0` |
| `row_group_size` | Default columnstore row-group size for newly created inverted indexes. Per-index WITH (row_group_size = ...) overrides it. Reads from existing indexes are unaffected. Default: 122'880. | `UINTEGER` | `SESSION` | `122880` |
| `storage_block_prefetch` | In which scenarios to use storage block prefetching | `VARCHAR` | `GLOBAL` | `REMOTE_ONLY` |
| `storage_compatibility_version` | Serialize on checkpoint with compatibility for a given duckdb version | `VARCHAR` | `GLOBAL` | `latest` |
| `wal_autocheckpoint` | Alias of `checkpoint_threshold`. | `VARCHAR` | `GLOBAL` | `16.0 MiB` |
| `wal_autocheckpoint_entries` | Trigger automatic checkpoint when WAL entry count reaches or exceeds N (0 = disabled) | `UBIGINT` | `GLOBAL` | `0` |
| `write_buffer_row_group_count` | The amount of row groups to buffer in bulk ingestion prior to flushing them together. Reducing this setting can reduce memory consumption. | `UBIGINT` | `GLOBAL` | `5` |

### Query execution and the optimizer

Planning, rewriting and the shape of query output.

| Name | Description | Type | Scope | Default value |
| --- | --- | --- | --- | --- |
| `approximate_join_order_threshold` | The minimum number of tables in a join to determine the optimal join order approximately instead of exactly. | `UBIGINT` | `SESSION` | `12` |
| `asof_loop_join_threshold` | The maximum number of rows we need on the left side of an ASOF join to use a nested loop join | `UBIGINT` | `SESSION` | `64` |
| `default_null_order` | NULL ordering used when none is specified (NULLS_FIRST or NULLS_LAST). | `VARCHAR` | `GLOBAL` | `NULLS_LAST` |
| `delim_join_as_cte` | Deprecated. Rewrite delim joins to materialized CTEs during dependent join flattening. | `BOOLEAN` | `SESSION` | `on` |
| `disabled_optimizers` | DEBUG SETTING: disable a specific set of optimizers (comma separated). | `VARCHAR` | `SESSION` |  |
| `dynamic_or_filter_threshold` | The maximum amount of OR filters we generate dynamically from a hash join | `UBIGINT` | `SESSION` | `50` |
| `enable_caching_operators` | Enables caching operators that cache intermediate results. | `BOOLEAN` | `SESSION` | `on` |
| `enable_optimizer` | Whether or not query optimization is enabled. | `BOOLEAN` | `GLOBAL` | `on` |
| `explain_output` | Output of EXPLAIN statements (`ALL`, `OPTIMIZED_ONLY`, `PHYSICAL_ONLY`) | `VARCHAR` | `SESSION` | `PHYSICAL_ONLY` |
| `explain_output_format` | Column shape of EXPLAIN output (DUCKDB_NATIVE = native two-column key/value, PG = single QUERY PLAN column with one row per line of a text or compact plan and one row per plan in other formats). | `VARCHAR` | `SESSION` | `PG` |
| `filter_reorder` | How far filter reordering may move conditions that can fail (`never`, `safe`, `fast` or `always`) | `VARCHAR` | `SESSION` | `fast` |
| `late_materialization_max_rows` | The maximum amount of rows in the LIMIT/SAMPLE for which we trigger late materialization | `UBIGINT` | `SESSION` | `50` |
| `match_recognize_max_states` | The maximum number of pattern states `MATCH_RECOGNIZE` explores for a single match before it refuses the pattern (0 = no limit). | `UBIGINT` | `SESSION` | `1000000` |
| `max_expression_depth` | The maximum expression depth limit in the parser. WARNING: increasing this setting and using very deep expressions might lead to stack overflow errors. | `UBIGINT` | `SESSION` | `1000` |
| `max_streaming_buffer_size` | The maximum number of bytes a streaming query result buffers (e.g. 1GB). | `VARCHAR` | `SESSION` | `10.0 MiB` |
| `merge_join_threshold` | The maximum number of rows on either table to choose a merge join | `UBIGINT` | `SESSION` | `1000` |
| `nested_loop_join_threshold` | The maximum number of rows on either table to choose a nested loop join | `UBIGINT` | `SESSION` | `5` |
| `null_order` | Alias of `default_null_order`. | `VARCHAR` | `GLOBAL` | `NULLS_LAST` |
| `order_by_non_integer_literal` | Allow ordering by non-integer literals - ordering by such literals has no effect. | `BOOLEAN` | `SESSION` | `false` |
| `ordered_aggregate_threshold` | The number of rows to accumulate before sorting, used for tuning | `UBIGINT` | `SESSION` | `262144` |
| `partitioned_write_flush_threshold` | The threshold in number of rows after which we flush a thread state when writing using `PARTITION_BY` | `UBIGINT` | `SESSION` | `524288` |
| `partitioned_write_max_open_files` | The maximum amount of files the system can keep open before flushing to disk when writing using `PARTITION_BY` | `UBIGINT` | `SESSION` | `100` |
| `prefer_range_joins` | Force use of range joins with mixed predicates | `BOOLEAN` | `SESSION` | `false` |
| `preserve_insertion_order` | Whether or not to preserve insertion order. If set to false the system is allowed to re-order any results that do not contain ORDER BY clauses. | `BOOLEAN` | `GLOBAL` | `true` |
| `read_ahead_depth` | The number of scan jobs prefetched ahead of decoding (-1 = automatic, bounded by a memory budget; 0 = disabled). | `BIGINT` | `GLOBAL` | `-1` |
| `scalar_subquery_error_on_multiple_rows` | Deprecated. When true, a scalar subquery that returns more than one row raises an error; when false, it returns an arbitrary one of those rows. | `BOOLEAN` | `SESSION` | `true` |
| `sdb_job_max_depth` | Maximum number of job runs in one chain of EXECUTE JOB calls, where a job's body executes another job; a run that would go deeper fails. Default 16. Server-global. | `UINTEGER` | `GLOBAL` | `16` |
| `standard_vector_size` | The vector size the server was built with (read-only). | `UBIGINT` | `GLOBAL` | `2048` |

### Session and PostgreSQL compatibility

Wire-protocol and session state that PostgreSQL clients read or set.

| Name | Description | Type | Scope | Default value |
| --- | --- | --- | --- | --- |
| `access_mode` | Access mode of the database (`AUTOMATIC`, `READ_ONLY` or `READ_WRITE`) | `VARCHAR` | `GLOBAL` | `automatic` |
| `application_name` | Sets the application name to be reported in statistics and logs. | `VARCHAR` | `SESSION` |  |
| `bytea_output` | Sets the output format for bytea. | `VARCHAR` | `SESSION` | `hex` |
| `Calendar` | The current calendar | `VARCHAR` | `SESSION` | `gregorian` |
| `client_encoding` | Sets the client's character set encoding. | `VARCHAR` | `SESSION` | `UTF8` |
| `client_min_messages` | Sets the message levels that are sent to the client. | `VARCHAR` | `SESSION` | `notice` |
| `current_transaction_invalidation_policy` | Which types of exceptions invalidate the database for the current transaction | `VARCHAR` | `GLOBAL` | `STANDARD_POLICY` |
| `custom_user_agent` | Metadata from SereneDB callers | `VARCHAR` | `GLOBAL` |  |
| `DateStyle` | Sets the display format for date and time values. | `VARCHAR` | `SESSION` | `ISO, MDY` |
| `default_transaction_invalidation_policy` | When to invalidate transactions when errors occur (SYNTACTIC_ERRORS_DO_NOT_INVALIDATE, i.e. parser and binder exceptions do not invalidate, or ALL_ERRORS_INVALIDATE_TRANSACTION). | `VARCHAR` | `GLOBAL` | `ALL_ERRORS_INVALIDATE_TRANSACTION` |
| `default_transaction_isolation` | Sets the transaction isolation level of each new transaction. | `VARCHAR` | `SESSION` | `repeatable read` |
| `default_transaction_read_only` | Sets the default read-only status of new transactions. | `BOOLEAN` | `SESSION` | `off` |
| `dialect_compatibility_mode` | Enable SQL dialect compatibility for a certain engine (e.g. `SET dialect_compatibility_mode='spark'`). | `VARCHAR` | `SESSION` | `NONE` |
| `duckdb_api` | The API name the server reports in its user agent (see `PRAGMA user_agent`). | `VARCHAR` | `GLOBAL` | `cpp` |
| `error_on_division_by_zero` | Deprecated. Throw an error instead of returning NULL when dividing by zero. | `BOOLEAN` | `SESSION` | `true` |
| `errors_as_json` | Output error messages as structured `JSON` instead of as a raw string | `BOOLEAN` | `SESSION` | `false` |
| `extra_float_digits` | Sets the number of digits displayed for floating-point values. | `INTEGER` | `SESSION` | `1` |
| `immediate_transaction_mode` | Whether transactions should be started lazily when needed, or immediately when BEGIN TRANSACTION is called | `BOOLEAN` | `GLOBAL` | `false` |
| `in_hot_standby` | Shows whether hot standby is currently active. | `BOOLEAN` | `SESSION` | `off` |
| `integer_datetimes` | Shows whether datetimes are integer based. | `BOOLEAN` | `SESSION` | `on` |
| `IntervalStyle` | Sets the display format for interval values. | `VARCHAR` | `SESSION` | `postgres` |
| `is_superuser` | Shows whether the current session's user is a superuser. | `BOOLEAN` | `SESSION` | `on` |
| `lock_configuration` | Whether or not configurations can be altered | `BOOLEAN` | `GLOBAL` | `false` |
| `max_execution_time` | The maximum execution time per query in milliseconds (0 = no limit). | `BIGINT` | `SESSION` | `0` |
| `regex_match_operator_semantics` | Deprecated. Configures whether regex match operators use partial or full string matching. | `VARCHAR` | `SESSION` | `partial` |
| `role` | Sets the current role. | `VARCHAR` | `SESSION` | `none` |
| `search_path` | Sets the default catalog search path as a comma-separated list of values | `VARCHAR` | `SESSION` | `"$user", public` |
| `server_encoding` | Shows the server (database) character set encoding. | `VARCHAR` | `SESSION` | `UTF8` |
| `server_version` | Shows the server version. | `VARCHAR` | `SESSION` | `18.3` |
| `server_version_num` | Shows the server version as an integer. | `INTEGER` | `SESSION` | `180003` |
| `session_authorization` | Sets the current session's user name. | `VARCHAR` | `SESSION` | the session's user |
| `session_replication_role` | Sets the session's behavior for triggers, as in PostgreSQL: `origin` (the default) and `local` fire the triggers enabled with `ENABLE TRIGGER`, `replica` fires only those enabled with `ENABLE REPLICA TRIGGER` and skips foreign key checks; `ENABLE ALWAYS` triggers fire in every role. Only superusers may change it. The apply worker of a subscription runs as `replica`. | `VARCHAR` | `SESSION` | `origin` |
| `show_behavior` | Configures how `SHOW` resolves a bare identifier: `auto` describes a table if one exists, else a setting (deprecated); `table` always describes a table; `setting` always describes a setting. | `VARCHAR` | `SESSION` | `SETTING` |
| `standard_conforming_strings` | Causes '...' strings to treat backslashes literally. | `BOOLEAN` | `SESSION` | `on` |
| `statement_timeout` | Aborts any statement that takes more than the specified amount of time (milliseconds without a unit; units `us`, `ms`, `s`, `min`, `h`, `d`) with SQLSTATE `57014`, as in PostgreSQL. 0 disables the timeout. Sets `max_execution_time` for the same scope. | `VARCHAR` | `SESSION` | `0` |
| `table_function_identifier_conversion` | Deprecated. Configures the use of deprecated implicit conversion of unbound identifiers to strings in table function arguments. | `VARCHAR` | `SESSION` | `DISABLE_IMPLICIT_STRING` |
| `TimeZone` | Sets the time zone for displaying and interpreting time stamps. | `VARCHAR` | `SESSION` | `Etc/UTC` |
| `transaction_isolation` | Sets the current transaction's isolation level. | `VARCHAR` | `SESSION` | `repeatable read` |

### Logical replication

Server-wide settings of [subscriptions](../sql/statements/create_subscription/index.md). Only a superuser may set them, and a change applies to running subscriptions too.

| Name | Description | Type | Scope | Default value |
| --- | --- | --- | --- | --- |
| `max_sync_workers_per_subscription` | How many tables one subscription copies at the same time during its initial synchronization, each over its own publisher connection (a walsender on the publisher). | `UINTEGER` | `GLOBAL` | `2` |
| `wal_receiver_status_interval` | How often a subscription reports its position to the publisher (seconds without a unit; units `us`, `ms`, `s`, `min`, `h`, `d`). 0 disables the periodic reports. | `VARCHAR` | `GLOBAL` | `10s` |
| `wal_receiver_timeout` | A subscription whose publisher sends nothing for this long drops the connection and reconnects (milliseconds without a unit). 0 disables the timeout. | `VARCHAR` | `GLOBAL` | `1min` |
| `wal_retrieve_retry_interval` | How long a failed subscription waits before it reconnects (milliseconds without a unit). | `VARCHAR` | `GLOBAL` | `5s` |

### Security

Authentication, authorization and what the server is allowed to reach.

| Name | Description | Type | Scope | Default value |
| --- | --- | --- | --- | --- |
| `allow_asterisks_in_http_paths` | Allow '\*' character in URLs users can query | `BOOLEAN` | `SESSION` | `false` |
| `allow_community_extensions` | Allow to load community built extensions | `BOOLEAN` | `GLOBAL` | `true` |
| `allow_extensions_metadata_mismatch` | Allow to load extensions with not compatible metadata | `BOOLEAN` | `GLOBAL` | `false` |
| `allow_persistent_secrets` | Allow the creation of persistent secrets, that are stored and loaded on restarts | `BOOLEAN` | `GLOBAL` | `true` |
| `allow_unredacted_secrets` | Allow printing unredacted secrets | `BOOLEAN` | `GLOBAL` | `false` |
| `allow_unsigned_extensions` | Allow to load extensions with invalid or missing signatures | `BOOLEAN` | `GLOBAL` | `false` |
| `enable_external_access` | Allow the database to access external state (through e.g., loading/installing modules, COPY TO/FROM, CSV readers, pandas replacement scans, etc) | `BOOLEAN` | `GLOBAL` | `true` |
| `enable_macro_dependencies` | Enable created MACROs to create dependencies on the referenced objects (such as tables) | `BOOLEAN` | `GLOBAL` | `true` |
| `hba` | Host-based authentication ruleset (pg_hba.conf syntax) as a single string: one rule per line, first match wins, applied to connections opened after the SET. A rule that lets the `postgres` superuser in without a password over local and loopback (127.0.0.1, ::1) connections always comes first, so a bad ruleset cannot lock the superuser out; other roles follow the ruleset. An empty value restores the default (SCRAM for every role). Server-global. | `VARCHAR` | `GLOBAL` |  |
| `scram_iterations` | Sets the iteration count for SCRAM secret generation. | `INTEGER` | `SESSION` | `4096` |
| `secret_directory` | Set the directory to which persistent secrets are stored | `VARCHAR` | `GLOBAL` | `<datadir>/secrets` |

### Logging, metrics and profiling

What the server records about itself.

| Name | Description | Type | Scope | Default value |
| --- | --- | --- | --- | --- |
| `enable_logging` | Enables the logger | `BOOLEAN` | `GLOBAL` | `1` |
| `enable_profiling` | Enables profiling, and sets the output format (`query_tree`, `query_tree_optimizer`, `json`, `text`, `html`, `graphviz`, `yaml`, `mermaid`, `compact` or `no_output`) | `VARCHAR` | `SESSION` | NULL |
| `enable_progress_bar` | Tracks the progress of long queries, so that `enable_progress_bar_print` can print it. On in every server session. | `BOOLEAN` | `SESSION` | `true` |
| `enable_progress_bar_print` | Prints a progress bar for long queries, when `enable_progress_bar` is on. The bar goes to the standard output of the `serened` process, not to the client. | `BOOLEAN` | `SESSION` | `false` |
| `legacy_metrics_format` | Deprecated. When enabled, profiling output uses the legacy flat format instead of the current grouped format. | `BOOLEAN` | `SESSION` | `off` |
| `log_query_path` | Specifies the path to which queries should be logged (default: NULL, queries are not logged). Setting the session's own path again reopens the file; a file another session logs to is refused. | `VARCHAR` | `SESSION` |  |
| `logging_level` | The log level which will be recorded in the log | `VARCHAR` | `GLOBAL` | `INFO` |
| `logging_mode` | Determines which types of log messages are logged | `VARCHAR` | `GLOBAL` | `LEVEL_ONLY` |
| `logging_storage` | Set the logging storage (memory/stdout/file/&lt;custom&gt;) | `VARCHAR` | `GLOBAL` | `stdout` |
| `profile_output` | Alias of `profiling_output`. | `VARCHAR` | `SESSION` |  |
| `profiling_coverage` | The profiling coverage (SELECT or `ALL`) | `VARCHAR` | `SESSION` | `SELECT` |
| `profiling_mode` | Deprecated: use `enable_profiling` and `profiling_coverage` instead. Any of `STANDARD`, `DETAILED` or `ALL` turns profiling on. | `VARCHAR` | `SESSION` | NULL |
| `profiling_output` | The file to which profile output is written. Empty writes it to the standard error of the `serened` process; the client receives nothing. | `VARCHAR` | `SESSION` |  |
| `profiling_renderer_settings` | A map of settings passed to the renderer of the profiler output (e.g. {'max_extra_lines': 100}) - settings not recognized by the active renderer are ignored. | `MAP(VARCHAR, VARCHAR)` | `SESSION` | `{}` |
| `progress_bar_time` | How long (in milliseconds) a query runs before its progress bar is printed. | `BIGINT` | `SESSION` | `2000` |
| `sdb_job_history_size` | Number of recent job runs, across all jobs, that `sdb_job_runs()` returns; the oldest run is dropped first. 0 keeps none. Default 1024. Server-global. | `UBIGINT` | `GLOBAL` | `1024` |
| `tracked_metrics` | A list of metric glob patterns to enable for collection (e.g. ['query.*', 'optimizer.*']). | `VARCHAR[]` | `SESSION` | `[*]` |

### Files, object stores and caching

Local paths, remote object stores and the external file cache.

| Name | Description | Type | Scope | Default value |
| --- | --- | --- | --- | --- |
| `azure_account_name` | Azure storage account name. When set, credentials are detected automatically. | `VARCHAR` | `SESSION` | NULL |
| `azure_context_caching` | Keep some request context per connection between queries. Turn it off if you suspect the cache causes problems. | `BOOLEAN` | `SESSION` | `true` |
| `azure_credential_chain` | Ordered list of Azure credential providers, separated by `;`, for example `'cli;workload_identity;managed_identity;env'`. | `VARCHAR` | `SESSION` | NULL |
| `azure_endpoint` | The Azure endpoint used with the Azure credential providers. | `VARCHAR` | `SESSION` | `blob.core.windows.net` |
| `azure_http_proxy` | Proxy for Azure requests, login included. | `VARCHAR` | `SESSION` | the server's `HTTP_PROXY` environment variable, if set |
| `azure_http_stats` | Include HTTP statistics of Azure Storage requests in `EXPLAIN ANALYZE`. | `BOOLEAN` | `SESSION` | `false` |
| `azure_proxy_password` | Password for the Azure HTTP proxy. | `VARCHAR` | `SESSION` | NULL |
| `azure_proxy_user_name` | User name for the Azure HTTP proxy. | `VARCHAR` | `SESSION` | NULL |
| `azure_read_buffer_size` | Size of the read buffer for Azure reads, in bytes. | `UBIGINT` | `SESSION` | `8388608` |
| `azure_storage_connection_string` | Azure connection string, used to authenticate and configure Azure requests. | `VARCHAR` | `SESSION` | NULL |
| `azure_write_block_size` | Size in bytes of each block of a Blob or DFS write; 0 restores the default (8 MiB), at most 4000 MiB. A blob holds at most 50,000 blocks, so raise it to write files larger than 50,000 x this size. | `UBIGINT` | `SESSION` | `8388608` |
| `azure_write_staged_blocks_per_commit` | How many blocks a write stages before it commits the blocks written so far; 0 commits only when the file is closed. Non-zero values make partial writes visible sooner at the cost of extra requests. | `UBIGINT` | `SESSION` | `0` |
| `ca_cert_file` | Path to a custom certificate file for self-signed certificates. | `VARCHAR` | `SESSION` |  |
| `cache_local_files` | Whether the external file cache also caches local files (remote files are always cached). | `BOOLEAN` | `GLOBAL` | `off` |
| `enable_fsst_vectors` | Allow scans on FSST compressed segments to emit compressed vectors to utilize late decompression | `BOOLEAN` | `GLOBAL` | `false` |
| `enable_server_cert_verification` | Enable server side certificate verification. | `BOOLEAN` | `SESSION` | `false` |
| `extension_directory` | Deprecated: use `extension_directories` instead. The directory to store extensions in. | `VARCHAR` | `GLOBAL` | `<datadir>/extensions` |
| `external_file_cache_local_max_block_size` | Maximum size in bytes of an external file cache block for local (non-remote) files. Larger reads are split into blocks of this size, which are fetched in parallel. | `UBIGINT` | `GLOBAL` | `16384` |
| `external_file_cache_remote_max_block_size` | Maximum size in bytes of an external file cache block for remote files (e.g. HTTP/S3). Larger reads are split into blocks of this size, which are fetched in parallel. | `UBIGINT` | `GLOBAL` | `2097152` |
| `external_file_cache_remote_min_block_size` | Reads of remote files are rounded out to aligned blocks of this size, so tiny reads do not each cost a request. | `UBIGINT` | `GLOBAL` | `4096` |
| `external_file_cache_spill` | Whether evicted external file cache blocks of remote files spill to the temporary directory instead of being dropped, so they are re-read from there rather than re-fetched. | `BOOLEAN` | `GLOBAL` | `false` |
| `extra_http_headers` | Extra HTTP headers added to every request. | `MAP(VARCHAR, VARCHAR)` | `GLOBAL` | `{}` |
| `file_search_path` | A comma separated list of directories to search for input files | `VARCHAR` | `SESSION` |  |
| `force_download` | Forces upfront download of file | `BOOLEAN` | `SESSION` | `false` |
| `gcs_user_project` | GCS billing project for Requester Pays. | `VARCHAR` | `SESSION` |  |
| `hf_max_per_page` | Debug option to limit number of items returned in list requests. | `UBIGINT` | `SESSION` | `0` |
| `home_directory` | Sets the home directory used by the system | `VARCHAR` | `SESSION` |  |
| `http_client_pool_capacity` | Capacity of the HTTP client pool. Unless set, the larger of twice the number of CPU cores (at least 16) and the number of async I/O threads (`async_threads`), at most an eighth of the server's open-file limit. | `BIGINT` | `GLOBAL` | automatic |
| `http_keep_alive` | Keep alive connections. Setting this to false can help when running into connection failures | `BOOLEAN` | `SESSION` | `true` |
| `http_proxy` | HTTP proxy host | `VARCHAR` | `GLOBAL` |  |
| `http_proxy_password` | Password for HTTP proxy | `VARCHAR` | `GLOBAL` |  |
| `http_proxy_username` | Username for HTTP proxy | `VARCHAR` | `GLOBAL` |  |
| `http_retries` | HTTP retries on I/O error | `UBIGINT` | `SESSION` | `3` |
| `http_retry_backoff` | Backoff factor for exponentially increasing retry wait time | `FLOAT` | `SESSION` | `4` |
| `http_retry_wait_ms` | Time between retries | `UBIGINT` | `SESSION` | `100` |
| `http_timeout` | HTTP timeout read/write/connection/retry (in seconds) | `UBIGINT` | `SESSION` | `30` |
| `httpfs_enable_credential_refresh` | When S3 refuses a request for its credentials, reload them, refreshing the secret where it supports refresh, and retry the request once. | `BOOLEAN` | `SESSION` | `true` |
| `s3_access_key_id` | S3 Access Key ID | `VARCHAR` | `SESSION` | NULL |
| `s3_allow_recursive_globbing` | Whether globs on S3-like storage are optimized with recursive strategy (alterative is listing) | `BOOLEAN` | `SESSION` | `true` |
| `s3_endpoint` | S3 Endpoint | `VARCHAR` | `SESSION` | NULL |
| `s3_kms_key_id` | S3 KMS Key ID | `VARCHAR` | `SESSION` | NULL |
| `s3_list_concurrency` | Maximum number of concurrent LIST requests issued while expanding an S3 glob. The window is also bounded by `async_threads + 1` and shrinks automatically when the server throttles. | `UBIGINT` | `SESSION` | `256` |
| `s3_region` | S3 Region | `VARCHAR` | `SESSION` | NULL |
| `s3_requester_pays` | S3 use requester pays mode | `BOOLEAN` | `SESSION` | `false` |
| `s3_secret_access_key` | S3 Access Key | `VARCHAR` | `SESSION` | NULL |
| `s3_session_token` | S3 Session Token | `VARCHAR` | `SESSION` | NULL |
| `s3_uploader_max_filesize` | Maximum size of a file written to S3. It must be above zero, fit in `s3_uploader_max_parts_per_file` parts and stay within the provider's largest object size. | `VARCHAR` | `SESSION` | `80GB` |
| `s3_uploader_max_parts_per_file` | S3 Uploader max parts per file (between 1 and 10000) | `UBIGINT` | `SESSION` | `10000` |
| `s3_url_compatibility_mode` | Disable Globs and Query Parameters on S3 URLs | `BOOLEAN` | `SESSION` | `false` |
| `s3_url_scheme_aliases` | Additional URL schemes routed to the S3-compatible filesystem (e.g. `['oss', 'cos']`). Can only be set globally; secrets for aliased schemes need an explicit `SCOPE`. | `VARCHAR[]` | `GLOBAL` | `[]` |
| `s3_url_style` | S3 URL style | `VARCHAR` | `SESSION` | `vhost` |
| `s3_use_ssl` | S3 use SSL | `BOOLEAN` | `SESSION` | `true` |
| `s3_version_id_pinning` | Pin S3 reads to a specific object version for consistency | `BOOLEAN` | `SESSION` | `false` |
| `temp_directory` | Set the directory to which to write temp files | `VARCHAR` | `GLOBAL` | `<datadir>/tmp` |

### Database connectors

Settings for the PostgreSQL and ClickHouse connectors. The `pg_pool_*` and `ch_pool_*` settings apply to databases attached after the change; `postgres_configure_pool()` and `clickhouse_configure_pool()` change the pool of a database that is already attached.

| Name | Description | Type | Scope | Default value |
| --- | --- | --- | --- | --- |
| `ch_binary_as_blob` | Read ClickHouse `String` and `FixedString` columns as `BLOB` instead of `VARCHAR`. | `BOOLEAN` | `SESSION` | `off` |
| `ch_connection_cache` | Reuse pooled ClickHouse connections across statements; `false` bypasses the connection pool. The change applies to the queries of every session, although other sessions keep showing their own value. | `BOOLEAN` | `GLOBAL` | `on` |
| `ch_experimental_filter_pushdown` | Push filters down to ClickHouse. | `BOOLEAN` | `SESSION` | `on` |
| `ch_order_pushdown` | Push `ORDER BY` and `LIMIT` down to ClickHouse. | `BOOLEAN` | `SESSION` | `on` |
| `ch_pool_acquire_mode` | How a query gets a connection from the pool: `force` always connects, ignoring the pool limit; `wait` blocks until one is free; `try` fails at once when none is free. | `VARCHAR` | `GLOBAL` | `force` |
| `ch_pool_enable_reaper_thread` | Periodically close pooled connections that exceed `ch_pool_max_lifetime_millis` or `ch_pool_idle_timeout_millis`. | `BOOLEAN` | `GLOBAL` | `on` |
| `ch_pool_enable_thread_local_cache` | Cache connections per thread: such a connection serves only its thread but still takes a place in the pool. | `BOOLEAN` | `GLOBAL` | `off` |
| `ch_pool_idle_timeout_millis` | Maximum milliseconds a connection may sit idle in the pool; checked when a connection is taken and, with the reaper thread on, in the background. | `UBIGINT` | `GLOBAL` | `60000` |
| `ch_pool_max_connections` | Maximum number of connections cached in the pool of each attached ClickHouse database. | `UBIGINT` | `GLOBAL` | 1.5 x the number of CPU threads, from 4 to 32 |
| `ch_pool_max_lifetime_millis` | Maximum milliseconds a connection is kept open; checked when it is taken or returned and, with the reaper thread on, in the background. 0 = no limit. | `UBIGINT` | `GLOBAL` | `0` |
| `ch_pool_wait_timeout_millis` | Maximum milliseconds to wait for a connection when every pooled connection is taken. | `UBIGINT` | `GLOBAL` | `30000` |
| `ch_statement_timeout_millis` | Maximum milliseconds a ClickHouse query may run; NULL or 0 = no limit. | `UBIGINT` | `SESSION` | NULL |
| `pg_array_as_varchar` | Read Postgres arrays as varchar - enables reading mixed dimensional arrays. | `BOOLEAN` | `SESSION` | `off` |
| `pg_connection_cache` | Deprecated: use `pg_pool_max_connections` (`0` turns pooling off) for databases attached later, or `postgres_configure_pool()` for one already attached. `false` sets the pool limit of every attached PostgreSQL database to 0, and `true` back to its default. | `BOOLEAN` | `GLOBAL` | `on` |
| `pg_connection_limit` | Deprecated: use `pg_pool_max_connections` for databases attached later, or `postgres_configure_pool()` for one already attached. Sets the connection limit of the pool of every attached PostgreSQL database. | `UBIGINT` | `GLOBAL` | 1.5 x the number of CPU threads, from 4 to 32 |
| `pg_experimental_filter_pushdown` | Whether or not to use filter pushdown. | `BOOLEAN` | `SESSION` | `on` |
| `pg_idle_in_transaction_timeout_millis` | Postgres idle in transaction timeout in milliseconds to set on scan connections. | `UINTEGER` | `SESSION` |  |
| `pg_null_byte_replacement` | When writing NULL bytes to Postgres, replace them with the given character. | `VARCHAR` | `SESSION` |  |
| `pg_numeric_as_varchar` | Read PostgreSQL `numeric` columns without precision and scale, or with a precision above 38, as `VARCHAR` instead of `DOUBLE`. | `BOOLEAN` | `SESSION` | `off` |
| `pg_numeric_nan_as_null` | Read a PostgreSQL `numeric` NaN as NULL instead of failing. | `BOOLEAN` | `SESSION` | `on` |
| `pg_oauth_token` | OAuth bearer token for PostgreSQL OAUTHBEARER authentication. Takes priority over the PGOAUTHTOKEN environment variable. | `VARCHAR` | `SESSION` |  |
| `pg_order_pushdown` | Push ORDER BY and LIMIT clauses to Postgres (default: true). | `BOOLEAN` | `SESSION` | `on` |
| `pg_pages_per_task` | The amount of pages per task. | `UBIGINT` | `SESSION` | `1000` |
| `pg_pool_acquire_mode` | How a query gets a connection from the pool: `force` always connects, ignoring the pool limit; `wait` blocks until one is free; `try` fails at once when none is free. | `VARCHAR` | `GLOBAL` | `force` |
| `pg_pool_enable_reaper_thread` | Periodically close pooled connections that exceed `pg_pool_max_lifetime_millis` or `pg_pool_idle_timeout_millis`; one of them must be non-zero for this to have an effect. | `BOOLEAN` | `GLOBAL` | `on` |
| `pg_pool_enable_thread_local_cache` | Cache connections per thread: such a connection serves only its thread but still takes a place in the pool. | `BOOLEAN` | `GLOBAL` | `off` |
| `pg_pool_health_check_query` | The query that checks a pooled connection is healthy; an empty string disables the check. | `VARCHAR` | `GLOBAL` | `SELECT 1` |
| `pg_pool_idle_timeout_millis` | Maximum milliseconds a connection may sit idle in the pool; checked when a connection is taken and, with the reaper thread on, in the background. | `UBIGINT` | `GLOBAL` | `60000` |
| `pg_pool_max_connections` | Maximum number of connections cached in the pool of each attached PostgreSQL database; parallel scans can exceed it for a while. | `UBIGINT` | `GLOBAL` | 1.5 x the number of CPU threads, from 4 to 32 |
| `pg_pool_max_lifetime_millis` | Maximum milliseconds a connection is kept open; checked when it is taken or returned and, with the reaper thread on, in the background. 0 = no limit. | `UBIGINT` | `GLOBAL` | `0` |
| `pg_pool_wait_timeout_millis` | Maximum milliseconds to wait for a connection when every pooled connection is taken. | `UBIGINT` | `GLOBAL` | `30000` |
| `pg_staleness_query` | A query that replaces the built-in check for DDL changes made in PostgreSQL when `pg_staleness_query_enabled` is on. It must contain a `${SCHEMA}` placeholder and return at least 3 columns (identity, name, revision marker). Empty uses the built-in query. | `VARCHAR` | `GLOBAL` | NULL |
| `pg_staleness_query_enabled` | Check PostgreSQL for DDL changes made outside SereneDB before using the cached table catalog of an attached database. | `BOOLEAN` | `GLOBAL` | `off` |
| `pg_statement_timeout_millis` | Postgres statement timeout in milliseconds to set on scan connections. | `UINTEGER` | `SESSION` |  |
| `pg_use_binary_copy` | Whether or not to use BINARY copy to read data. | `BOOLEAN` | `SESSION` | `on` |
| `pg_use_ctid_scan` | Whether or not to parallelize scanning using table ctids. | `BOOLEAN` | `SESSION` | `on` |
| `pg_use_information_schema_introspection` | Discover the schema of an attached database through the SQL-standard `information_schema` views instead of `pg_catalog`, for databases that speak the PostgreSQL protocol without its catalog, such as Spanner, CockroachDB or Redshift. | `BOOLEAN` | `SESSION` | `off` |
| `pg_use_text_protocol` | Whether or not to use TEXT protocol to read data. This is slower, but provides better compatibility with non-Postgres systems. | `BOOLEAN` | `SESSION` | `off` |

### File formats

Defaults for the CSV, Parquet, JSON, Arrow and Iceberg readers and writers.

| Name | Description | Type | Scope | Default value |
| --- | --- | --- | --- | --- |
| `arrow_large_buffer_size` | Whether Arrow buffers for strings, blobs, uuids and bits should be exported using large buffers | `BOOLEAN` | `GLOBAL` | `false` |
| `arrow_lossless_conversion` | Whenever a SereneDB type does not have a clear native or canonical extension match in Arrow, export the types with a duckdb.type_name extension name. | `BOOLEAN` | `GLOBAL` | `false` |
| `arrow_output_list_view` | Whether export to Arrow format should use ListView as the physical layout for LIST columns | `BOOLEAN` | `GLOBAL` | `false` |
| `arrow_output_version` | Whether strings should be produced by SereneDB in Utf8View format instead of Utf8 | `VARCHAR` | `GLOBAL` | `1.0` |
| `binary_as_string` | In Parquet files, interpret binary data as a string. | `BOOLEAN` | `SESSION` | `false` |
| `copy_csv_header_default` | Whether COPY ... TO a CSV file writes a header line when HEADER is not specified. | `BOOLEAN` | `SESSION` | `off` |
| `iceberg_default_format_version` | The Iceberg format version of a new table created without a `format-version` property: 2 or 3. | `UBIGINT` | `SESSION` | `2` |
| `iceberg_equality_delete_fast_filter` | Use the fast path for Iceberg equality deletes on byte-comparable column types; other types, and every type when off, use the general path. | `BOOLEAN` | `GLOBAL` | `on` |
| `iceberg_logging_post_body_truncate_limit` | Maximum number of characters of a REST catalog POST body included in Iceberg log messages; longer bodies end in `... (truncated)`. 0 omits the body. | `UBIGINT` | `SESSION` | `10000` |
| `iceberg_metadata_log_clock_skew_ms` | Clock-skew allowance in milliseconds when `iceberg_use_metadata_log` is on: table metadata up to this far after the transaction start counts as visible. 0 compares the timestamps strictly. | `BIGINT` | `GLOBAL` | `0` |
| `iceberg_use_metadata_log` | Read each Iceberg table as of the transaction start, using the table's metadata log, for snapshot isolation; a table whose metadata has no metadata log then fails to read at an older start. Off, a transaction reads the latest table metadata it resolves. | `BOOLEAN` | `GLOBAL` | `off` |
| `iceberg_use_server_side_scan_planning` | Let the REST catalog plan Iceberg scans, where it supports it. | `BOOLEAN` | `SESSION` | `off` |
| `json_geometry_format` | How `GEOMETRY` values are written to JSON: `wkt` for Well-Known Text, `geojson` for GeoJSON geometry objects. `COPY ... TO ... (FORMAT GEOJSON)` always writes GeoJSON. | `VARCHAR` | `SESSION` | `wkt` |
| `parquet_metadata_cache` | Cache Parquet metadata - useful when reading the same files multiple times | `BOOLEAN` | `SESSION` | `false` |
| `parquet_prefetch_column_gap` | Byte gap under which Parquet prefetch I/O ranges are coalesced (NULL lets the cost model adapt it). | `UBIGINT` | `SESSION` |  |
| `produce_arrow_string_view` | Deprecated. Whether Arrow strings should be produced by SereneDB in Utf8View format instead of Utf8 | `BOOLEAN` | `GLOBAL` | `false` |
| `unsafe_enable_version_guessing` | Find the latest metadata version of an Iceberg table without a version hint by listing its files. This can read an uncommitted version. | `BOOLEAN` | `SESSION` | `off` |

### Extensions

Installing and loading extensions.

| Name | Description | Type | Scope | Default value |
| --- | --- | --- | --- | --- |
| `allow_extension_repositories` | Whether custom trusted extension repositories are `allowed`, `forbidden` (which also distrusts existing repositories) or `undecided` (blocks adding new repositories but keeps trusting existing ones). While the server runs, it can only move from `undecided` to `allowed` or `forbidden`, or from `allowed` to `forbidden`. | `VARCHAR` | `GLOBAL` | `undecided` |
| `autoinstall_extension_repository` | Overrides the custom endpoint for extension installation on autoloading | `VARCHAR` | `GLOBAL` |  |
| `autoinstall_known_extensions` | Whether known extensions are allowed to be automatically installed when a query depends on them | `BOOLEAN` | `GLOBAL` | `false` |
| `autoload_known_extensions` | Whether known extensions are allowed to be automatically loaded when a query depends on them | `BOOLEAN` | `GLOBAL` | `false` |
| `custom_extension_repository` | Overrides the custom endpoint for remote extension installation | `VARCHAR` | `GLOBAL` |  |
| `extension_directories` | Set the directories to store extensions in | `VARCHAR[]` | `GLOBAL` | `[]` |
| `extension_repository_directory` | The directory that stores trusted extension repositories. While signature checking is on (`allow_unsigned_extensions = false`), it can only be set at start. | `VARCHAR` | `GLOBAL` |  |

### Debugging and unsafe options

Not for production. These exist to reproduce and diagnose engine behaviour, and several can corrupt data.

| Name | Description | Type | Scope | Default value |
| --- | --- | --- | --- | --- |
| `__iceberg_unsafe_struct_null_default_interp` | For testing only: read a NULL default of an Iceberg `STRUCT` field as an empty struct whose fields use their own defaults. The only non-NULL value accepted is `'{}'`. | `VARCHAR` | `GLOBAL` | NULL |
| `debug_checkpoint_scan_sleep_ms` | For testing: how long (in milliseconds) a checkpoint sleeps after it has written a table's row groups, before it installs them. | `UBIGINT` | `GLOBAL` | `0` |
| `disable_database_invalidation` | Disables invalidating the database instance when encountering a fatal error. Should be used with great care, as SereneDB cannot guarantee correct behavior after a fatal error. | `BOOLEAN` | `GLOBAL` | `true` |
| `disable_parquet_prefetching` | Disable the prefetching mechanism in Parquet | `BOOLEAN` | `SESSION` | `false` |
| `disable_timestamptz_casts` | Disable casting from timestamp to timestamptz | `BOOLEAN` | `SESSION` | `false` |
| `enable_view_dependencies` | Enable created VIEWs to create dependencies on the referenced objects (such as tables) | `BOOLEAN` | `GLOBAL` | `true` |
| `force_column_metadata_reuse` | Deprecated. Force the reuse of column metadata when checkpointing in the older storage versions 6 and 7. Files written so cannot be read by older versions. | `BOOLEAN` | `GLOBAL` | `off` |
| `force_compression` | Forces the use of one compression method for new data. | `VARCHAR` | `GLOBAL` | `auto` |
| `force_dict_fsst_mode` | For testing: forces one string compression mode (`DICTIONARY`, `DICT_FSST`, `FSST_ONLY`, `DICT_FSST_PLUS` or `FSST_PLUS`) or way of choosing it (`AUTO`, `AUTO_NATIVE`); `DEFAULT` lets the engine choose. | `VARCHAR` | `GLOBAL` | `DEFAULT` |
| `force_download_threshold` | Forces upfront download of files smaller than the given size in bytes | `UBIGINT` | `SESSION` | `0` |
| `force_mbedtls_unsafe` | Enable mbedtls for encryption (WARNING: unsafe to use) | `BOOLEAN` | `GLOBAL` | `false` |
| `force_variant_shredding` | Forces the VARIANT shredding that happens at checkpoint to use the provided schema for the shredding. | `VARCHAR` | `GLOBAL` | `INVALID` |
| `iceberg_test_force_token_expiry` | For testing: forces OAuth2 tokens of Iceberg catalogs to expire, to exercise their automatic refresh. | `BOOLEAN` | `SESSION` | `off` |
| `iceberg_unsafe_skip_puffin_verification` | Skip the structural check of Puffin deletion-vector files, to read the invalid files some other Iceberg writers produce. | `BOOLEAN` | `GLOBAL` | `off` |
| `legacy_disable_null_type` | Deprecated. When enabled, prevent the NULL type from leaving the binder (< v2.0 default behavior). | `BOOLEAN` | `SESSION` | `off` |
| `old_implicit_casting` | Deprecated. Allow implicit casting to and from VARCHAR. | `BOOLEAN` | `GLOBAL` | `false` |
| `unsafe_disable_etag_checks` | Disable checks on ETag consistency | `BOOLEAN` | `SESSION` | `false` |

### Other

Everything that does not fall into the groups above.

| Name | Description | Type | Scope | Default value |
| --- | --- | --- | --- | --- |
| `__delta_only_variant_encoding_enabled` | Enables the Parquet reader to identify a Variant structurally. | `BOOLEAN` | `GLOBAL` | `false` |
| `allowed_configs` | List of configuration options that are ALWAYS allowed to be changed - even when lock_configuration is true | `VARCHAR[]` | `GLOBAL` | `[]` |
| `allowed_directories` | List of directories/prefixes that are ALWAYS allowed to be queried - even when enable_external_access is false | `VARCHAR[]` | `GLOBAL` | `[]` |
| `allowed_paths` | List of files that are ALWAYS allowed to be queried - even when enable_external_access is false | `VARCHAR[]` | `GLOBAL` | `[]` |
| `auto_checkpoint_skip_wal_threshold` | The estimated WAL write size at which point we will skip writing to the WAL and only checkpoint. Skipping writing to the WAL means concurrent commits are blocked while the checkpoint is happening. | `UBIGINT` | `GLOBAL` | `100000` |
| `auto_fallback_to_full_download` | Allows automatically falling back to full file downloads when possible. | `BOOLEAN` | `SESSION` | `true` |
| `catalog_error_max_schemas` | The maximum number of schemas the system will scan for "did you mean..." style errors in the catalog | `UBIGINT` | `GLOBAL` | `100` |
| `default_collation` | The collation setting used when none is specified | `VARCHAR` | `GLOBAL` |  |
| `default_order` | The order type used when none is specified (`ASC` or `DESC`) | `VARCHAR` | `GLOBAL` | `ASCENDING` |
| `default_secret_storage` | Allows switching the default storage for secrets | `VARCHAR` | `GLOBAL` | `local_file` |
| `disabled_filesystems` | Disable specific file systems preventing access (e.g., LocalFileSystem) | `VARCHAR` | `GLOBAL` |  |
| `disabled_log_types` | Sets the list of disabled loggers | `VARCHAR` | `GLOBAL` |  |
| `enable_curl_server_cert_verification` | Enable server side certificate verification for CURL backend. | `BOOLEAN` | `SESSION` | `true` |
| `enable_external_file_cache` | Allow the database to cache external files (e.g., Parquet) in memory. | `BOOLEAN` | `GLOBAL` | `true` |
| `enable_geoparquet_conversion` | Attempt to decode/encode geometry data in/as GeoParquet files if the spatial extension is present. | `BOOLEAN` | `SESSION` | `true` |
| `enable_global_s3_configuration` | Automatically fetch AWS credentials from environment variables. | `BOOLEAN` | `SESSION` | `true` |
| `enable_http_metadata_cache` | Whether or not the global http metadata is used to cache HTTP metadata | `BOOLEAN` | `GLOBAL` | `false` |
| `enabled_log_types` | Sets the list of enabled loggers | `VARCHAR` | `GLOBAL` |  |
| `experimental_metadata_reuse` | Deprecated. EXPERIMENTAL: Re-use row group and table metadata when checkpointing. | `BOOLEAN` | `GLOBAL` | `true` |
| `external_threads` | The number of external threads that work on SereneDB tasks. | `UBIGINT` | `GLOBAL` | `0` |
| `geometry_always_xy` | Ignore the axis order a coordinate reference system defines and always treat coordinates as (easting, northing) instead of, for example, (latitude, longitude). | `BOOLEAN` | `SESSION` | NULL |
| `geometry_minimum_shredding_size` | Minimum size of a rowgroup to enable GEOMETRY shredding, or set to -1 to disable entirely. Defaults to 1/4th of a rowgroup | `BIGINT` | `GLOBAL` | `30000` |
| `httpfs_connection_caching` | Enable connection caching for HTTP requests | `BOOLEAN` | `GLOBAL` | `true` |
| `ieee_floating_point_ops` | Use IEEE 754 floating-point operations: dividing a floating-point number by zero returns `Infinity`, `-Infinity` or `NaN` instead of failing or returning NULL. | `BOOLEAN` | `SESSION` | `true` |
| `ignore_unknown_crs` | Ignore unknown Coordinate Reference Systems (CRS) when creating geometry types or importing geospatial data. | `BOOLEAN` | `SESSION` | `false` |
| `index_scan_max_count` | The maximum index scan count sets a threshold for index scans. If fewer than MAX(index_scan_max_count, index_scan_percentage \* total_row_count) rows match, we perform an index scan instead of a table scan. | `UBIGINT` | `GLOBAL` | `2048` |
| `index_scan_percentage` | The index scan percentage sets a threshold for index scans. If fewer than MAX(index_scan_max_count, index_scan_percentage \* total_row_count) rows match, we perform an index scan instead of a table scan. | `DOUBLE` | `GLOBAL` | `0.001` |
| `integer_division` | Whether or not the / operator defaults to integer division, or to floating point division | `BOOLEAN` | `SESSION` | `true` |
| `integer_overflow` | What integer `+`, `-`, `*` and unary minus do when the result does not fit its type: `ERROR` raises an error, `WRAP` keeps the low bits of the result (two's complement wraparound). | `VARCHAR` | `SESSION` | `ERROR` |
| `lambda_syntax` | Deprecated. Configures the use of the deprecated single arrow operator (->) for lambda functions. | `VARCHAR` | `SESSION` | `ENABLE_SINGLE_ARROW` |
| `max_vacuum_tasks` | The maximum vacuum tasks to schedule during a checkpoint. | `UBIGINT` | `GLOBAL` | `100` |
| `merge_http_secret_into_s3_request` | Merges http secret params into S3 requests | `BOOLEAN` | `SESSION` | `true` |
| `perfect_ht_threshold` | Threshold in bytes for when to use a perfect hash table | `UBIGINT` | `SESSION` | `12` |
| `pin_threads` | Whether to pin threads to cores (Linux only, default AUTO: on when there are more than 64 cores) | `VARCHAR` | `GLOBAL` | `auto` |
| `pivot_filter_threshold` | The threshold to switch from using filtered aggregates to LIST with a dedicated pivot operator | `UBIGINT` | `SESSION` | `20` |
| `pivot_limit` | The maximum number of pivot columns in a pivot statement | `UBIGINT` | `SESSION` | `100000` |
| `preserve_identifier_case` | How unquoted identifiers are folded: `lowercase` lowercases them, `preserve_case` keeps the case as written, `uppercase` uppercases them. `false` and `true` are accepted as `lowercase` and `preserve_case`. | `VARCHAR` | `SESSION` | `lowercase` |
| `rtree_index_scan_min_rows` | Has no effect: SereneDB has no R-tree index. | `UBIGINT` | `SESSION` | `8192` |
| `rtree_index_scan_ratio` | Has no effect: SereneDB has no R-tree index. | `DOUBLE` | `SESSION` | `0.075` |
| `schema` | Sets the default search schema. Equivalent to setting search_path to a single value. | `VARCHAR` | `SESSION` | `public` |
| `streaming_buffer_size` | Alias of `max_streaming_buffer_size`. | `VARCHAR` | `SESSION` | `10.0 MiB` |
| `vacuum_rebuild_indexes` | Deprecated. Maximum row count of a table whose bound ART indexes are rebuilt when a checkpoint vacuum cannot remap their row IDs; 0 disables the rebuild. | `UBIGINT` | `GLOBAL` | `0` |
| `validate_external_file_cache` | Cache validation mode: `VALIDATE_ALL` (validate all cache entries), `VALIDATE_REMOTE` (validate only remote cache entries) or `NO_VALIDATION` (disable cache validation). | `VARCHAR` | `GLOBAL` | `VALIDATE_ALL` |
| `variant_minimum_shredding_size` | Minimum size of a rowgroup to enable VARIANT shredding, or set to -1 to disable entirely. Defaults to 1/4th of a rowgroup | `BIGINT` | `GLOBAL` | `30000` |
| `warnings_as_errors` | Escalate all warnings to errors. | `BOOLEAN` | `GLOBAL` | `false` |
| `zstd_min_string_length` | The (average) length at which to enable ZSTD compression, defaults to 4096 | `UBIGINT` | `GLOBAL` | `4096` |
