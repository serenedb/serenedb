////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include "basics/assert.h"
#include "query/config.h"

namespace sdb {

constexpr auto kVariableDescription =
  frozen::make_unordered_map<std::string_view, VariableDescription>({
    {
      "search_path",
      {
        VariableType::PgSearchPath,
        "Sets the schema search order for names that are not schema-qualified.",
        "\"$user\", public",
      },
    },
    {
      "extra_float_digits",
      {
        VariableType::PgExtraFloatDigits,
        "Sets the number of digits displayed for floating-point values.",
        "1",
      },
    },
    {
      "bytea_output",
      {
        VariableType::PgByteaOutput,
        "Sets the output format for values of type bytea. Valid values are "
        "'hex' "
        "(the default) and 'escape' (the traditional PostgreSQL format).",
        "hex",
      },
    },
    {
      "client_encoding",
      {
        VariableType::String,
        "Sets the client character set encoding.",
        "UTF8",
      },
    },
    {
      "datestyle",
      {
        VariableType::String,
        "Sets the display format for date and time values.",
        "ISO, MDY",
      },
    },
    {
      "application_name",
      {
        VariableType::String,
        "Sets the application name to be reported in statistics and logs.",
        "",
      },
    },
    {
      "default_transaction_read_only",
      {
        VariableType::Bool,
        "Sets the default read-only status of new transactions.",
        "off",
      },
    },
    {
      "in_hot_standby",
      {
        VariableType::Bool,
        "Shows whether the server is currently in hot standby mode.",
        "off",
      },
    },
    {
      "integer_datetimes",
      {
        VariableType::Bool,
        "Shows whether datetimes are integer based.",
        "on",
      },
    },
    {
      "intervalstyle",
      {
        VariableType::String,
        "Sets the display format for interval values.",
        "postgres",
      },
    },
    {
      "scram_iterations",
      {
        VariableType::I32,
        "Sets the number of SCRAM iterations for password hashing.",
        "4096",
      },
    },
    {
      "server_encoding",
      {
        VariableType::String,
        "Sets the server character set encoding.",
        "UTF8",
      },
    },
    {
      "server_version",
      {
        VariableType::String,
        "Shows the server version.",
        "18.1",
      },
    },
    {
      "standard_conforming_strings",
      {
        VariableType::Bool,
        "Causes '...' strings to treat backslashes literally.",
        "on",
      },
    },
    {
      "timezone",
      {
        VariableType::String,
        "Sets the time zone for displaying and interpreting time stamps.",
        "Etc/UTC",
      },
    },
  });

constexpr auto kVeloxVariableDescription =
  frozen::make_unordered_map<std::string_view, VariableDescription>({
    {
      "query_max_memory_per_node",
      {
        VariableType::String,
        "Maximum memory that a query can use on a single host.",
        "0B",
      },
    },
    {
      "session_timezone",
      {
        VariableType::String,
        "User provided session timezone. Stores a string with the actual "
        "timezone name, e.g: \"America/Los_Angeles\".",
        std::string_view{},
      },
    },
    {
      "adjust_timestamp_to_session_timezone",
      {
        VariableType::Bool,
        "If true, timezone-less timestamp conversions (e.g. string to "
        "timestamp, when the string does not specify a timezone) will be "
        "adjusted to the user provided session timezone (if any). False by "
        "default.",
        "false",
      },
    },
    {
      "expression.eval_simplified",
      {
        VariableType::Bool,
        "Whether to use the simplified expression evaluation path. False by "
        "default.",
        "false",
      },
    },
    {
      "expression.track_cpu_usage",
      {
        VariableType::Bool,
        "Whether to track CPU usage for individual expressions (supported by "
        "call and cast expressions). False by default. Can be expensive when "
        "processing small batches, e.g. < 10K rows.",
        "false",
      },
    },
    {
      "track_operator_cpu_usage",
      {
        VariableType::Bool,
        "Whether to track CPU usage for stages of individual operators. True "
        "by "
        "default. Can be expensive when processing small batches, e.g. < 10K "
        "rows.",
        "true",
      },
    },
    {
      "legacy_cast",
      {
        VariableType::Bool,
        "Flags used to configure the CAST operator",
        "false",
      },
    },
    {
      "cast_match_struct_by_name",
      {
        VariableType::Bool,
        "This flag makes the Row conversion to by applied in a way that the "
        "casting row field are matched by name instead of position.",
        "false",
      },
    },
    {
      "expression.max_array_size_in_reduce",
      {
        VariableType::U64,
        "Reduce() function will throw an error if encountered an array of size "
        "greater than this.",
        "100000",
      },
    },
    {
      "expression.max_compiled_regexes",
      {
        VariableType::U64,
        "Controls maximum number of compiled regular expression patterns per "
        "function instance per thread of execution.",
        "100",
      },
    },
    {
      "max_local_exchange_buffer_size",
      {
        VariableType::U64,
        "Used for backpressure to block local exchange producers when the "
        "local "
        "exchange buffer reaches or exceeds this size.",
        "33554432",
      },
    },
    {
      "max_local_exchange_partition_count",
      {
        VariableType::U32,
        "Limits the number of partitions created by a local exchange. "
        "Partitioning data too granularly can lead to poor performance. This "
        "setting allows increasing the task concurrency for all pipelines "
        "except the ones that require a local partitioning. Affects the number "
        "of drivers for pipelines containing LocalPartitionNode and cannot "
        "exceed the maximum number of pipeline drivers configured for the "
        "task.",
        "4294967295",
      },
    },
    {
      "min_local_exchange_partition_count_to_use_partition_buffer",
      {
        VariableType::U32,
        "Minimum number of local exchange output partitions to use buffered "
        "partitioning.",
        "33",
      },
    },
    {
      "max_local_exchange_partition_buffer_size",
      {
        VariableType::U32,
        "Maximum size in bytes to accumulate for a single partition of a local "
        "exchange before flushing.",
        "65536",
      },
    },
    {
      "local_exchange_partition_buffer_preserve_encoding",
      {
        VariableType::Bool,
        "Try to preserve the encoding of the input vector when copying it to "
        "the buffer.",
        "false",
      },
    },
    {
      "local_merge_source_queue_size",
      {
        VariableType::U32,
        "Maximum number of vectors buffered in each local merge source before "
        "blocking to wait for consumers.",
        "2",
      },
    },
    {
      "exchange.max_buffer_size",
      {
        VariableType::U64,
        "Maximum size in bytes to accumulate in ExchangeQueue. Enforced "
        "approximately, not strictly.",
        "33554432",
      },
    },
    {
      "merge_exchange.max_buffer_size",
      {
        VariableType::U64,
        "Maximum size in bytes to accumulate among all sources of the merge "
        "exchange. Enforced approximately, not strictly.",
        "134217728",
      },
    },
    {
      "min_exchange_output_batch_bytes",
      {
        VariableType::U64,
        "The minimum number of bytes to accumulate in the ExchangeQueue before "
        "unblocking a consumer. This is used to avoid creating tiny batches "
        "which may have a negative impact on performance when the cost of "
        "creating vectors is high (for example, when there are many columns). "
        "To avoid latency degradation, the exchange client unblocks a consumer "
        "when 1% of the data size observed so far is accumulated.",
        "2097152",
      },
    },
    {
      "max_partial_aggregation_memory",
      {
        VariableType::U64,
        "",
        "16777216",
      },
    },
    {
      "max_extended_partial_aggregation_memory",
      {
        VariableType::U64,
        "",
        "67108864",
      },
    },
    {
      "abandon_partial_aggregation_min_rows",
      {
        VariableType::I32,
        "",
        "100000",
      },
    },
    {
      "abandon_partial_aggregation_min_pct",
      {
        VariableType::I32,
        "",
        "80",
      },
    },
    {
      "abandon_partial_topn_row_number_min_rows",
      {
        VariableType::I32,
        "",
        "100000",
      },
    },
    {
      "abandon_partial_topn_row_number_min_pct",
      {
        VariableType::I32,
        "",
        "80",
      },
    },
    {
      "max_elements_size_in_repeat_and_sequence",
      {
        VariableType::I32,
        "",
        "10000",
      },
    },
    {
      "max_page_partitioning_buffer_size",
      {
        VariableType::U64,
        "The maximum number of bytes to buffer in PartitionedOutput operator "
        "to "
        "avoid creating tiny SerializedPages.",
        "33554432",
      },
    },
    {
      "max_output_buffer_size",
      {
        VariableType::U64,
        "The maximum size in bytes for the task's buffered output. The "
        "producer "
        "Drivers are blocked when the buffered size exceeds this. The Drivers "
        "are resumed when the buffered size goes below "
        "OutputBufferManager::kContinuePct % of this.",
        "33554432",
      },
    },
    {
      "preferred_output_batch_bytes",
      {
        VariableType::U64,
        "Preferred size of batches in bytes to be returned by operators from "
        "Operator::getOutput. It is used when an estimate of average row size "
        "is known. Otherwise kPreferredOutputBatchRows is used.",
        "10485760",
      },
    },
    {
      "preferred_output_batch_rows",
      {
        VariableType::I32,
        "Preferred number of rows to be returned by operators from "
        "Operator::getOutput. It is used when an estimate of average row size "
        "is not known. When the estimate of average row size is known, "
        "kPreferredOutputBatchBytes is used.",
        "1024",
      },
    },
    {
      "max_output_batch_rows",
      {
        VariableType::I32,
        "Max number of rows that could be return by operators from "
        "Operator::getOutput. It is used when an estimate of average row size "
        "is known and kPreferredOutputBatchBytes is used to compute the number "
        "of output rows.",
        "10000",
      },
    },
    {
      "table_scan_getoutput_time_limit_ms",
      {
        VariableType::U64,
        "TableScan operator will exit getOutput() method after this many "
        "milliseconds even if it has no data to return yet. Zero means 'no "
        "time "
        "limit'.",
        "5000",
      },
    },
    {
      "hash_adaptivity_enabled",
      {
        VariableType::Bool,
        "If false, the 'group by' code is forced to use generic hash mode "
        "hashtable.",
        "true",
      },
    },
    {
      "adaptive_filter_reordering_enabled",
      {
        VariableType::Bool,
        "If true, the conjunction expression can reorder inputs based on the "
        "time taken to calculate them.",
        "true",
      },
    },
    {
      "spill_enabled",
      {
        VariableType::Bool,
        "Global enable spilling flag.",
        "false",
      },
    },
    {
      "aggregation_spill_enabled",
      {
        VariableType::Bool,
        "Aggregation spilling flag, only applies if \"spill_enabled\" flag is "
        "set.",
        "true",
      },
    },
    {
      "join_spill_enabled",
      {
        VariableType::Bool,
        "Join spilling flag, only applies if \"spill_enabled\" flag is set.",
        "true",
      },
    },
    {
      "mixed_grouped_mode_hash_join_spill_enabled",
      {
        VariableType::Bool,
        "Config to enable hash join spill for mixed grouped execution mode.",
        "false",
      },
    },
    {
      "order_by_spill_enabled",
      {
        VariableType::Bool,
        "OrderBy spilling flag, only applies if \"spill_enabled\" flag is set.",
        "true",
      },
    },
    {
      "window_spill_enabled",
      {
        VariableType::Bool,
        "Window spilling flag, only applies if \"spill_enabled\" flag is set.",
        "true",
      },
    },
    {
      "writer_spill_enabled",
      {
        VariableType::Bool,
        "If true, the memory arbitrator will reclaim memory from table writer "
        "by flushing its buffered data to disk. only applies if "
        "\"spill_enabled\" flag is set.",
        "true",
      },
    },
    {
      "row_number_spill_enabled",
      {
        VariableType::Bool,
        "RowNumber spilling flag, only applies if \"spill_enabled\" flag is "
        "set.",
        "true",
      },
    },
    {
      "topn_row_number_spill_enabled",
      {
        VariableType::Bool,
        "TopNRowNumber spilling flag, only applies if \"spill_enabled\" flag "
        "is "
        "set.",
        "true",
      },
    },
    {
      "local_merge_spill_enabled",
      {
        VariableType::Bool,
        "LocalMerge spilling flag, only applies if \"spill_enabled\" flag is "
        "set.",
        "false",
      },
    },
    {
      "local_merge_max_num_merge_sources",
      {
        VariableType::U32,
        "Specify the max number of local sources to merge at a time.",
        "4294967295",
      },
    },
    {
      "max_spill_run_rows",
      {
        VariableType::U64,
        "The max row numbers to fill and spill for each spill run. This is "
        "used "
        "to cap the memory used for spilling. If it is zero, then there is no "
        "limit and spilling might run out of memory. Based on offline test "
        "results, the default value is set to 12 million rows which uses "
        "~128MB "
        "memory when to fill a spill run.",
        "12582912",
      },
    },
    {
      "max_spill_bytes",
      {
        VariableType::U64,
        "The max spill bytes limit set for each query. This is used to cap the "
        "storage used for spilling. If it is zero, then there is no limit and "
        "spilling might exhaust the storage or takes too long to run. The "
        "default value is set to 100 GB.",
        "107374182400",
      },
    },
    {
      "max_spill_level",
      {
        VariableType::I32,
        "The max allowed spilling level with zero being the initial spilling "
        "level. This only applies for hash build spilling which might trigger "
        "recursive spilling when the build table is too big. If it is set to "
        "-1, then there is no limit and then some extreme large query might "
        "run "
        "out of spilling partition bits (see kSpillPartitionBits) at the end. "
        "The max spill level is used in production to prevent some bad user "
        "queries from using too much io and cpu resources.",
        "1",
      },
    },
    {
      "max_spill_file_size",
      {
        VariableType::U64,
        "The max allowed spill file size. If it is zero, then there is no "
        "limit.",
        "0",
      },
    },
    {
      "spill_compression_codec",
      {
        VariableType::String,
        "",
        "none",
      },
    },
    {
      "spill_prefixsort_enabled",
      {
        VariableType::Bool,
        "Enable the prefix sort or fallback to timsort in spill. The prefix "
        "sort is faster than std::sort but requires the memory to build "
        "normalized prefix keys, which might have potential risk of running "
        "out "
        "of server memory.",
        "false",
      },
    },
    {
      "spill_write_buffer_size",
      {
        VariableType::U64,
        "Specifies spill write buffer size in bytes. The spiller tries to "
        "buffer serialized spill data up to the specified size before write to "
        "storage underneath for io efficiency. If it is set to zero, then "
        "spill "
        "write buffering is disabled.",
        "1048576",
      },
    },
    {
      "spill_read_buffer_size",
      {
        VariableType::U64,
        "Specifies the buffer size in bytes to read from one spilled file. If "
        "the underlying filesystem supports async read, we do read-ahead with "
        "double buffering, which doubles the buffer used to read from each "
        "spill file.",
        "1048576",
      },
    },
    {
      "spill_file_create_config",
      {
        VariableType::String,
        "Config used to create spill files. This config is provided to "
        "underlying file system and the config is free form. The form should "
        "be "
        "defined by the underlying file system.",
        "",
      },
    },
    {
      "spiller_start_partition_bit",
      {
        VariableType::U8,
        "Default offset spill start partition bit. It is used with "
        "'kSpillNumPartitionBits' together to calculate the spilling partition "
        "number for join spill or aggregation spill.",
        "48",
      },
    },
    {
      "spiller_num_partition_bits",
      {
        VariableType::U8,
        "Default number of spill partition bits. It is the number of bits used "
        "to calculate the spill partition number for hash join and RowNumber. "
        "The number of spill partitions will be power of two. NOTE: as for "
        "now, "
        "we only support up to 8-way spill partitioning.",
        "3",
      },
    },
    {
      "min_spillable_reservation_pct",
      {
        VariableType::I32,
        "The minimal available spillable memory reservation in percentage of "
        "the current memory usage. Suppose the current memory usage size of M, "
        "available memory reservation size of N and min reservation percentage "
        "of P, if M * P / 100 > N, then spiller operator needs to grow the "
        "memory reservation with percentage of "
        "spillableReservationGrowthPct(). "
        "This ensures we have sufficient amount of memory reservation to "
        "process the large input outlier.",
        "5",
      },
    },
    {
      "spillable_reservation_growth_pct",
      {
        VariableType::I32,
        "The spillable memory reservation growth percentage of the previous "
        "memory reservation size. 10 means exponential growth along a series "
        "of "
        "integer powers of 11/10. The reservation grows by this much until it "
        "no longer can, after which it starts spilling.",
        "10",
      },
    },
    {
      "writer_flush_threshold_bytes",
      {
        VariableType::I64,
        "Minimum memory footprint size required to reclaim memory from a file "
        "writer by flushing its buffered data to disk.",
        "100663296",
      },
    },
    {
      "presto.array_agg.ignore_nulls",
      {
        VariableType::Bool,
        "If true, array_agg() aggregation function will ignore nulls in the "
        "input.",
        "false",
      },
    },
    {
      "spark.ansi_enabled",
      {
        VariableType::Bool,
        "If true, Spark function's behavior is ANSI-compliant, e.g. throws "
        "runtime exception instead of returning null on invalid inputs. Note: "
        "This feature is still under development to achieve full ANSI "
        "compliance. Users can refer to the Spark function documentation to "
        "verify the current support status of a specific function.",
        "false",
      },
    },
    {
      "spark.bloom_filter.expected_num_items",
      {
        VariableType::I64,
        "The default number of expected items for the bloomfilter.",
        "1000000",
      },
    },
    {
      "spark.bloom_filter.num_bits",
      {
        VariableType::I64,
        "The default number of bits to use for the bloom filter.",
        "8388608",
      },
    },
    {
      "spark.bloom_filter.max_num_bits",
      {
        VariableType::I64,
        "The max number of bits to use for the bloom filter.",
        "4194304",
      },
    },
    {
      "spark.partition_id",
      {
        VariableType::I32,
        "The current spark partition id.",
        "",
      },
    },
    {
      "spark.legacy_date_formatter",
      {
        VariableType::Bool,
        "If true, simple date formatter is used for time formatting and "
        "parsing. Joda date formatter is used by default.",
        "false",
      },
    },
    {
      "spark.legacy_statistical_aggregate",
      {
        VariableType::Bool,
        "If true, Spark statistical aggregation functions including skewness, "
        "kurtosis, stddev, stddev_samp, variance, var_samp, covar_samp and "
        "corr "
        "will return NaN instead of NULL when dividing by zero during "
        "expression evaluation.",
        "false",
      },
    },
    {
      "spark.json_ignore_null_fields",
      {
        VariableType::Bool,
        "If true, ignore null fields when generating JSON string. If false, "
        "null fields are included with a null value.",
        "true",
      },
    },
    {
      "task_writer_count",
      {
        VariableType::U32,
        "The number of local parallel table writer operators per task.",
        "4",
      },
    },
    {
      "task_partitioned_writer_count",
      {
        VariableType::U32,
        "The number of local parallel table writer operators per task for "
        "partitioned writes. If not set, use \"task_writer_count\".",
        "4",
      },
    },
    {
      "hash_probe_finish_early_on_empty_build",
      {
        VariableType::Bool,
        "If true, finish the hash probe on an empty build table for a specific "
        "set of hash joins.",
        "false",
      },
    },
    {
      "min_table_rows_for_parallel_join_build",
      {
        VariableType::U32,
        "The minimum number of table rows that can trigger the parallel hash "
        "join table build.",
        "1000",
      },
    },
    {
      "debug.validate_output_from_operators",
      {
        VariableType::Bool,
        "If set to true, then during execution of tasks, the output vectors of "
        "every operator are validated for consistency. This is an expensive "
        "check so should only be used for debugging. It can help debug issues "
        "where malformed vector cause failures or crashes by helping identify "
        "which operator is generating them.",
        "false",
      },
    },
    {
      "enable_expression_evaluation_cache",
      {
        VariableType::Bool,
        "If true, enable caches in expression evaluation for performance, "
        "including ExecCtx::vectorPool_, ExecCtx::decodedVectorPool_, "
        "ExecCtx::selectivityVectorPool_, Expr::baseDictionary_, "
        "Expr::dictionaryCache_, and Expr::cachedDictionaryIndices_. "
        "Otherwise, "
        "disable the caches.",
        "true",
      },
    },
    {
      "max_shared_subexpr_results_cached",
      {
        VariableType::U32,
        "For a given shared subexpression, the maximum distinct sets of inputs "
        "we cache results for. Lambdas can call the same expression with "
        "different inputs many times, causing the results we cache to explode "
        "in size. Putting a limit contains the memory usage.",
        "10",
      },
    },
    {
      "max_split_preload_per_driver",
      {
        VariableType::I32,
        "Maximum number of splits to preload. Set to 0 to disable preloading.",
        "2",
      },
    },
    {
      "driver_cpu_time_slice_limit_ms",
      {
        VariableType::U32,
        "If not zero, specifies the cpu time slice limit in ms that a driver "
        "thread can continuously run without yielding. If it is zero, then "
        "there is no limit.",
        "0",
      },
    },
    {
      "prefixsort_normalized_key_max_bytes",
      {
        VariableType::U32,
        "Maximum number of bytes to use for the normalized key in prefix-sort. "
        "Use 0 to disable prefix-sort.",
        "128",
      },
    },
    {
      "prefixsort_min_rows",
      {
        VariableType::U32,
        "Minimum number of rows to use prefix-sort. The default value has been "
        "derived using micro-benchmarking.",
        "128",
      },
    },
    {
      "prefixsort_max_string_prefix_length",
      {
        VariableType::U32,
        "Maximum number of bytes to be stored in prefix-sort buffer for a "
        "string key.",
        "16",
      },
    },
    {
      "query_trace_enabled",
      {
        VariableType::Bool,
        "Enable query tracing flag.",
        "false",
      },
    },
    {
      "query_trace_dir",
      {
        VariableType::String,
        "Base dir of a query to store tracing data.",
        "",
      },
    },
    {
      "query_trace_node_id",
      {
        VariableType::String,
        "The plan node id whose input data will be traced. Empty string if "
        "only "
        "want to trace the query metadata.",
        "",
      },
    },
    {
      "query_trace_max_bytes",
      {
        VariableType::U64,
        "The max trace bytes limit. Tracing is disabled if zero.",
        "0",
      },
    },
    {
      "query_trace_task_reg_exp",
      {
        VariableType::String,
        "The regexp of traced task id. We only enable trace on a task if its "
        "id "
        "matches.",
        "",
      },
    },
    {
      "query_trace_dry_run",
      {
        VariableType::Bool,
        "If true, we only collect the input trace for a given operator but "
        "without the actual execution.",
        "false",
      },
    },
    {
      "op_trace_directory_create_config",
      {
        VariableType::String,
        "Config used to create operator trace directory. This config is "
        "provided to underlying file system and the config is free form. The "
        "form should be defined by the underlying file system.",
        "",
      },
    },
    {
      "debug_disable_expression_with_peeling",
      {
        VariableType::Bool,
        "Disable optimization in expression evaluation to peel common "
        "dictionary layer from inputs.",
        "false",
      },
    },
    {
      "debug_disable_common_sub_expressions",
      {
        VariableType::Bool,
        "Disable optimization in expression evaluation to re-use cached "
        "results "
        "for common sub-expressions.",
        "false",
      },
    },
    {
      "debug_disable_expression_with_memoization",
      {
        VariableType::Bool,
        "Disable optimization in expression evaluation to re-use cached "
        "results "
        "between subsequent input batches that are dictionary encoded and have "
        "the same alphabet(underlying flat vector).",
        "false",
      },
    },
    {
      "debug_disable_expression_with_lazy_inputs",
      {
        VariableType::Bool,
        "Disable optimization in expression evaluation to delay loading of "
        "lazy "
        "inputs unless required.",
        "false",
      },
    },
    {
      "debug_aggregation_approx_percentile_fixed_random_seed",
      {
        VariableType::U32,
        "Fix the random seed used to create data structure used in "
        "approx_percentile. This makes the query result deterministic on "
        "single "
        "node; multi-node partial aggregation is still subject to "
        "non-determinism due to non-deterministic merge order.",
        std::string_view{},
      },
    },
    {
      "debug_memory_pool_name_regex",
      {
        VariableType::Bool,
        "When debug is enabled for memory manager, this is used to match the "
        "memory pools that need allocation callsites tracking. Default to "
        "track "
        "nothing.",
        "",
      },
    },
    {
      "debug_memory_pool_warn_threshold_bytes",
      {
        VariableType::U64,
        "Warning threshold in bytes for debug memory pools. When set to a "
        "non-zero value, a warning will be logged once per memory pool when "
        "allocations cause the pool to exceed this threshold. This is useful "
        "for identifying memory usage patterns during debugging. Requires "
        "allocation tracking to be enabled via `debug_memory_pool_name_regex` "
        "for the pool. A value of 0 means no warning threshold is enforced.",
        "0",
      },
    },
    {
      "debug_lambda_function_evaluation_batch_size",
      {
        VariableType::I32,
        "Some lambda functions over arrays and maps are evaluated in batches "
        "of "
        "the underlying elements that comprise the arrays/maps. This is done "
        "to "
        "make the batch size manageable as array vectors can have thousands of "
        "elements each and hit scaling limits as implementations typically "
        "expect BaseVectors to a couple of thousand entries. This lets up tune "
        "those batch sizes.",
        "10000",
      },
    },
    {
      "debug_bing_tile_children_max_zoom_shift",
      {
        VariableType::U8,
        "The UDF `bing_tile_children` generates the children of a Bing tile "
        "based on a specified target zoom level. The number of children "
        "produced is determined by the difference between the target zoom "
        "level "
        "and the zoom level of the input tile. This configuration limits the "
        "number of children by capping the maximum zoom level difference, with "
        "a default value set to 5. This cap is necessary to prevent "
        "excessively "
        "large array outputs, which can exceed the size limits of the elements "
        "vector in the Velox array vector.",
        "5",
      },
    },
    {
      "selective_nimble_reader_enabled",
      {
        VariableType::Bool,
        "Temporary flag to control whether selective Nimble reader should be "
        "used in this query or not. Will be removed after the selective Nimble "
        "reader is fully rolled out.",
        "false",
      },
    },
    {
      "scaled_writer_rebalance_max_memory_usage_ratio",
      {
        VariableType::F64,
        "The max ratio of a query used memory to its max capacity, and the "
        "scale writer exchange stops scaling writer processing if the query's "
        "current memory usage exceeds this ratio. The value is in the range of "
        "(0, 1].",
        "0.7",
      },
    },
    {
      "scaled_writer_max_partitions_per_writer",
      {
        VariableType::U32,
        "The max number of logical table partitions that can be assigned to a "
        "single table writer thread. The logical table partition is used by "
        "local exchange writer for writer scaling, and multiple physical table "
        "partitions can be mapped to the same logical table partition based on "
        "the hash value of calculated partitioned ids.",
        "128",
      },
    },
    {
      "scaled_writer_min_partition_processed_bytes_rebalance_threshold",
      {
        VariableType::U64,
        "Minimum amount of data processed by a logical table partition to "
        "trigger writer scaling if it is detected as overloaded by scale "
        "wrirer "
        "exchange.",
        "134217728",
      },
    },
    {
      "scaled_writer_min_processed_bytes_rebalance_threshold",
      {
        VariableType::U64,
        "Minimum amount of data processed by all the logical table partitions "
        "to trigger skewed partition rebalancing by scale writer exchange.",
        "268435456",
      },
    },
    {
      "table_scan_scaled_processing_enabled",
      {
        VariableType::Bool,
        "If true, enables the scaled table scan processing. For each table "
        "scan "
        "plan node, a scan controller is used to control the number of running "
        "scan threads based on the query memory usage. It keeps increasing the "
        "number of running threads until the query memory usage exceeds the "
        "threshold defined by 'table_scan_scale_up_memory_usage_ratio'.",
        "false",
      },
    },
    {
      "table_scan_scale_up_memory_usage_ratio",
      {
        VariableType::F64,
        "The query memory usage ratio used by scan controller to decide if it "
        "can increase the number of running scan threads. When the query "
        "memory "
        "usage is below this ratio, the scan controller keeps increasing the "
        "running scan thread for scale up, and stop once exceeds this ratio. "
        "The value is in the range of [0, 1]. NOTE: this only applies if "
        "'table_scan_scaled_processing_enabled' is true.",
        "0.7",
      },
    },
    {
      "shuffle_compression_codec",
      {
        VariableType::String,
        "Specifies the shuffle compression kind which is defined by "
        "CompressionKind. If it is CompressionKind_NONE, then no compression.",
        "none",
      },
    },
    {
      "throw_exception_on_duplicate_map_keys",
      {
        VariableType::Bool,
        "If a key is found in multiple given maps, by default that key's value "
        "in the resulting map comes from the last one of those maps. When "
        "true, "
        "throw exception on duplicate map key.",
        "false",
      },
    },
    {
      "index_lookup_join_max_prefetch_batches",
      {
        VariableType::U32,
        "Specifies the max number of input batches to prefetch to do index "
        "lookup ahead. If it is zero, then process one input batch at a time.",
        "0",
      },
    },
    {
      "index_lookup_join_split_output",
      {
        VariableType::Bool,
        "If this is true, then the index join operator might split output for "
        "each input batch based on the output batch size control. Otherwise, "
        "it "
        "tries to produce a single output for each input batch.",
        "true",
      },
    },
    {
      "request_data_sizes_max_wait_sec",
      {
        VariableType::I32,
        "Max wait time for exchange request in seconds.",
        "10",
      },
    },
    {
      "streaming_aggregation_min_output_batch_rows",
      {
        VariableType::I32,
        "In streaming aggregation, wait until we have enough number of output "
        "rows to produce a batch of size specified by this. If set to 0, then "
        "Operator::outputBatchRows will be used as the min output batch rows.",
        "0",
      },
    },
    {
      "streaming_aggregation_eager_flush",
      {
        VariableType::Bool,
        "TODO: Remove after dependencies are cleaned up.",
        "false",
      },
    },
    {
      "field_names_in_json_cast_enabled",
      {
        VariableType::Bool,
        "If this is true, then it allows you to get the struct field names as "
        "json element names when casting a row to json.",
        "false",
      },
    },
    {
      "operator_track_expression_stats",
      {
        VariableType::Bool,
        "If this is true, then operators that evaluate expressions will track "
        "stats for expressions that are not special forms and return them as "
        "part of their operator stats. Tracking these stats can be expensive "
        "(especially if operator stats are retrieved frequently) and this "
        "allows the user to explicitly enable it.",
        "false",
      },
    },
    {
      "enable_operator_batch_size_stats",
      {
        VariableType::Bool,
        "If this is true, enable the operator input/output batch size stats "
        "collection in driver execution. This can be expensive for data types "
        "with a large number of columns (e.g., ROW types) as it calls "
        "estimateFlatSize() which recursively calculates sizes for all child "
        "vectors.",
        "true",
      },
    },
    {
      "unnest_split_output",
      {
        VariableType::Bool,
        "If this is true, then the unnest operator might split output for each "
        "input batch based on the output batch size control. Otherwise, it "
        "produces a single output for each input batch.",
        "true",
      },
    },
    {
      "query_memory_reclaimer_priority",
      {
        VariableType::I32,
        "Priority of the query in the memory pool reclaimer. Lower value means "
        "higher priority. This is used in global arbitration victim selection.",
        "2147483647",
      },
    },
    {
      "max_num_splits_listened_to",
      {
        VariableType::I32,
        "The max number of input splits to listen to by SplitListener per "
        "table "
        "scan node per worker. It's up to the SplitListener implementation to "
        "respect this config.",
        "0",
      },
    },
    {
      "source",
      {
        VariableType::String,
        "Source of the query. Used by Presto to identify the file system "
        "username.",
        "",
      },
    },
    {
      "client_tags",
      {
        VariableType::String,
        "Client tags of the query. Used by Presto to identify the file system "
        "username.",
        "",
      },
    },
  });

std::optional<std::pair<std::string_view, VariableDescription>> GetDefault(
  std::string_view name) {
  {
    auto it = kVariableDescription.find(name);
    if (it != kVariableDescription.end()) {
      return *it;
    }
  }

  {
    auto it = kVeloxVariableDescription.find(name);
    if (it != kVeloxVariableDescription.end()) {
      return *it;
    }
  }
  return std::nullopt;
}

std::optional<VariableDescription> GetDefaultDescription(
  std::string_view name) {
  return GetDefault(name).and_then(
    [](auto info) { return std::optional{info.second}; });
}

std::string_view GetDefaultVariable(std::string_view name) {
  auto desc = GetDefaultDescription(name);
  if (!desc) {
    return {};
  }
  return desc->default_value;
}

std::string_view GetOriginalName(std::string_view name) {
  auto info = GetDefault(name);
  if (!info) {
    return {};
  }
  return info->first;
}

std::unordered_map<std::string, std::string> Config::rawConfigsCopy() const {
  std::unordered_map<std::string, std::string> result;
  // Since .emplace insert only if key doesn't exist in map,
  // the order of insertation is from transaction to default
  for (const auto& [key, value] : _transaction) {
    result.emplace(key, value.value);
  }

  for (const auto& [key, value] : _session) {
    result.emplace(key, value);
  }

  // there are no default variables from velox query config,
  // because rawConfigsCopy() using only by velox to copy config.
  // So, there is no need to copy default velox variables

  for (const auto& [key, description] : kVariableDescription) {
    result.emplace(key, description.default_value);
  }

  return result;
}

void Config::VisitFullDescription(
  absl::FunctionRef<void(std::string_view, std::string_view, std::string_view)>
    f) const {
  for (const auto& [name, description] : kVariableDescription) {
    std::string_view value = GetNonDefault(name);
    if (!value.data()) {
      value = description.default_value;
    }
    f(name, description.default_value, description.description);
  }

  for (const auto& [name, description] : kVeloxVariableDescription) {
    std::string_view value = GetNonDefault(name);
    if (!value.data()) {
      value = description.default_value;
    }
    f(name, std::string{description.default_value}, description.description);
  }
}

}  // namespace sdb
