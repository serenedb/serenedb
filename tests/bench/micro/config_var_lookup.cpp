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

// Benchmarks several approaches for case-insensitive lookup in the config
// variable map.  Uses the real full key set (~139 entries from both
// kVariableDescription and kVeloxVariableDescription).
//
// Workload patterns:
//   - Wire protocol startup: 13 canonical-case keys from
//   kParameterStatusVariables
//   - SQL SET/SHOW path:     lowercase keys (SQL parser always lowercases)
//   - Velox config path:     velox-specific lowercase keys
//   - Miss:                  keys not present, varied lengths
//
// Approaches under test:
//   1. frozen::unordered_map  – lowercase keys,  AsciiStrToLower on input
//   2. frozen::unordered_map  – canonical keys,  case-insensitive hash/equal
//   3. absl::flat_hash_map    – lowercase keys,  AsciiStrToLower on input
//   4. absl::flat_hash_map    – canonical keys,  case-insensitive hash/equal
//   5. linear scan            – canonical keys,  EqualsIgnoreCase   (baseline)
//   6. utils::TrivialBiMap    – lowercase keys,  ICase switch-on-length
//
// Approaches 1 and 2 require building with -DSDB_BENCH_FROZEN=1.

#include <absl/container/flat_hash_map.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/string_view.h>
#include <benchmark/benchmark.h>
#ifdef SDB_BENCH_FROZEN
#include <frozen/unordered_map.h>
#endif

#include <string_view>

#include "basics/containers/trivial_map.h"

namespace {

using Value = int;

// ---------------------------------------------------------------------------
// kLowerKeys – all keys in lowercase (for maps 1/3/6 and as canonical array
// for the linear scan).  Mirrors kVariableDescription +
// kVeloxVariableDescription exactly, in their definition order.
// ---------------------------------------------------------------------------
// clang-format off
static constexpr std::string_view kLowerKeys[] = {
  // --- kVariableDescription (21 keys) ---
  "execution_threads",
  "join_order_algorithm",
  "search_path",
  "extra_float_digits",
  "bytea_output",
  "client_encoding",
  "application_name",
  "default_transaction_read_only",
  "in_hot_standby",
  "integer_datetimes",
  "scram_iterations",
  "server_encoding",
  "server_version",
  "standard_conforming_strings",
  "datestyle",
  "intervalstyle",
  "timezone",
  "sdb_write_conflict_policy",
  "sdb_read_your_own_writes",
  "default_transaction_isolation",
  "transaction_isolation",
  // --- kVeloxVariableDescription (118 keys) ---
  "query_max_memory_per_node",
  "session_timezone",
  "adjust_timestamp_to_session_timezone",
  "expression.eval_simplified",
  "expression.track_cpu_usage",
  "track_operator_cpu_usage",
  "legacy_cast",
  "cast_match_struct_by_name",
  "expression.max_array_size_in_reduce",
  "expression.max_compiled_regexes",
  "max_local_exchange_buffer_size",
  "max_local_exchange_partition_count",
  "min_local_exchange_partition_count_to_use_partition_buffer",
  "max_local_exchange_partition_buffer_size",
  "local_exchange_partition_buffer_preserve_encoding",
  "local_merge_source_queue_size",
  "exchange.max_buffer_size",
  "merge_exchange.max_buffer_size",
  "min_exchange_output_batch_bytes",
  "max_partial_aggregation_memory",
  "max_extended_partial_aggregation_memory",
  "abandon_partial_aggregation_min_rows",
  "abandon_partial_aggregation_min_pct",
  "abandon_partial_topn_row_number_min_rows",
  "abandon_partial_topn_row_number_min_pct",
  "max_elements_size_in_repeat_and_sequence",
  "max_page_partitioning_buffer_size",
  "max_output_buffer_size",
  "preferred_output_batch_bytes",
  "preferred_output_batch_rows",
  "max_output_batch_rows",
  "table_scan_getoutput_time_limit_ms",
  "hash_adaptivity_enabled",
  "adaptive_filter_reordering_enabled",
  "spill_enabled",
  "aggregation_spill_enabled",
  "join_spill_enabled",
  "mixed_grouped_mode_hash_join_spill_enabled",
  "order_by_spill_enabled",
  "window_spill_enabled",
  "writer_spill_enabled",
  "row_number_spill_enabled",
  "topn_row_number_spill_enabled",
  "local_merge_spill_enabled",
  "local_merge_max_num_merge_sources",
  "max_spill_run_rows",
  "max_spill_bytes",
  "max_spill_level",
  "max_spill_file_size",
  "spill_compression_codec",
  "spill_prefixsort_enabled",
  "spill_write_buffer_size",
  "spill_read_buffer_size",
  "spill_file_create_config",
  "spiller_start_partition_bit",
  "spiller_num_partition_bits",
  "min_spillable_reservation_pct",
  "spillable_reservation_growth_pct",
  "writer_flush_threshold_bytes",
  "presto.array_agg.ignore_nulls",
  "spark.ansi_enabled",
  "spark.bloom_filter.expected_num_items",
  "spark.bloom_filter.num_bits",
  "spark.bloom_filter.max_num_bits",
  "spark.partition_id",
  "spark.legacy_date_formatter",
  "spark.legacy_statistical_aggregate",
  "spark.json_ignore_null_fields",
  "task_writer_count",
  "task_partitioned_writer_count",
  "hash_probe_finish_early_on_empty_build",
  "min_table_rows_for_parallel_join_build",
  "debug.validate_output_from_operators",
  "enable_expression_evaluation_cache",
  "max_shared_subexpr_results_cached",
  "max_split_preload_per_driver",
  "driver_cpu_time_slice_limit_ms",
  "prefixsort_normalized_key_max_bytes",
  "prefixsort_min_rows",
  "prefixsort_max_string_prefix_length",
  "query_trace_enabled",
  "query_trace_dir",
  "query_trace_node_id",
  "query_trace_max_bytes",
  "query_trace_task_reg_exp",
  "query_trace_dry_run",
  "op_trace_directory_create_config",
  "debug_disable_expression_with_peeling",
  "debug_disable_common_sub_expressions",
  "debug_disable_expression_with_memoization",
  "debug_disable_expression_with_lazy_inputs",
  "debug_aggregation_approx_percentile_fixed_random_seed",
  "debug_memory_pool_name_regex",
  "debug_memory_pool_warn_threshold_bytes",
  "debug_lambda_function_evaluation_batch_size",
  "debug_bing_tile_children_max_zoom_shift",
  "selective_nimble_reader_enabled",
  "scaled_writer_rebalance_max_memory_usage_ratio",
  "scaled_writer_max_partitions_per_writer",
  "scaled_writer_min_partition_processed_bytes_rebalance_threshold",
  "scaled_writer_min_processed_bytes_rebalance_threshold",
  "table_scan_scaled_processing_enabled",
  "table_scan_scale_up_memory_usage_ratio",
  "shuffle_compression_codec",
  "throw_exception_on_duplicate_map_keys",
  "index_lookup_join_max_prefetch_batches",
  "index_lookup_join_split_output",
  "request_data_sizes_max_wait_sec",
  "streaming_aggregation_min_output_batch_rows",
  "streaming_aggregation_eager_flush",
  "field_names_in_json_cast_enabled",
  "operator_track_expression_stats",
  "enable_operator_batch_size_stats",
  "unnest_split_output",
  "query_memory_reclaimer_priority",
  "max_num_splits_listened_to",
  "source",
  "client_tags",
};
// clang-format on

static constexpr auto kN = std::size(kLowerKeys);

// kCanonicalKeys – same as kLowerKeys but with PostgreSQL canonical casing
// for DateStyle / IntervalStyle / TimeZone.
// clang-format off
static constexpr std::string_view kCanonicalKeys[] = {
  "execution_threads", "join_order_algorithm", "search_path",
  "extra_float_digits", "bytea_output", "client_encoding",
  "application_name", "default_transaction_read_only", "in_hot_standby",
  "integer_datetimes", "scram_iterations", "server_encoding",
  "server_version", "standard_conforming_strings",
  "DateStyle", "IntervalStyle", "TimeZone",
  "sdb_write_conflict_policy", "sdb_read_your_own_writes",
  "default_transaction_isolation", "transaction_isolation",
  "query_max_memory_per_node", "session_timezone",
  "adjust_timestamp_to_session_timezone", "expression.eval_simplified",
  "expression.track_cpu_usage", "track_operator_cpu_usage", "legacy_cast",
  "cast_match_struct_by_name", "expression.max_array_size_in_reduce",
  "expression.max_compiled_regexes", "max_local_exchange_buffer_size",
  "max_local_exchange_partition_count",
  "min_local_exchange_partition_count_to_use_partition_buffer",
  "max_local_exchange_partition_buffer_size",
  "local_exchange_partition_buffer_preserve_encoding",
  "local_merge_source_queue_size", "exchange.max_buffer_size",
  "merge_exchange.max_buffer_size", "min_exchange_output_batch_bytes",
  "max_partial_aggregation_memory",
  "max_extended_partial_aggregation_memory",
  "abandon_partial_aggregation_min_rows",
  "abandon_partial_aggregation_min_pct",
  "abandon_partial_topn_row_number_min_rows",
  "abandon_partial_topn_row_number_min_pct",
  "max_elements_size_in_repeat_and_sequence",
  "max_page_partitioning_buffer_size", "max_output_buffer_size",
  "preferred_output_batch_bytes", "preferred_output_batch_rows",
  "max_output_batch_rows", "table_scan_getoutput_time_limit_ms",
  "hash_adaptivity_enabled", "adaptive_filter_reordering_enabled",
  "spill_enabled", "aggregation_spill_enabled", "join_spill_enabled",
  "mixed_grouped_mode_hash_join_spill_enabled", "order_by_spill_enabled",
  "window_spill_enabled", "writer_spill_enabled", "row_number_spill_enabled",
  "topn_row_number_spill_enabled", "local_merge_spill_enabled",
  "local_merge_max_num_merge_sources", "max_spill_run_rows",
  "max_spill_bytes", "max_spill_level", "max_spill_file_size",
  "spill_compression_codec", "spill_prefixsort_enabled",
  "spill_write_buffer_size", "spill_read_buffer_size",
  "spill_file_create_config", "spiller_start_partition_bit",
  "spiller_num_partition_bits", "min_spillable_reservation_pct",
  "spillable_reservation_growth_pct", "writer_flush_threshold_bytes",
  "presto.array_agg.ignore_nulls", "spark.ansi_enabled",
  "spark.bloom_filter.expected_num_items", "spark.bloom_filter.num_bits",
  "spark.bloom_filter.max_num_bits", "spark.partition_id",
  "spark.legacy_date_formatter", "spark.legacy_statistical_aggregate",
  "spark.json_ignore_null_fields", "task_writer_count",
  "task_partitioned_writer_count", "hash_probe_finish_early_on_empty_build",
  "min_table_rows_for_parallel_join_build",
  "debug.validate_output_from_operators",
  "enable_expression_evaluation_cache", "max_shared_subexpr_results_cached",
  "max_split_preload_per_driver", "driver_cpu_time_slice_limit_ms",
  "prefixsort_normalized_key_max_bytes", "prefixsort_min_rows",
  "prefixsort_max_string_prefix_length", "query_trace_enabled",
  "query_trace_dir", "query_trace_node_id", "query_trace_max_bytes",
  "query_trace_task_reg_exp", "query_trace_dry_run",
  "op_trace_directory_create_config",
  "debug_disable_expression_with_peeling",
  "debug_disable_common_sub_expressions",
  "debug_disable_expression_with_memoization",
  "debug_disable_expression_with_lazy_inputs",
  "debug_aggregation_approx_percentile_fixed_random_seed",
  "debug_memory_pool_name_regex", "debug_memory_pool_warn_threshold_bytes",
  "debug_lambda_function_evaluation_batch_size",
  "debug_bing_tile_children_max_zoom_shift",
  "selective_nimble_reader_enabled",
  "scaled_writer_rebalance_max_memory_usage_ratio",
  "scaled_writer_max_partitions_per_writer",
  "scaled_writer_min_partition_processed_bytes_rebalance_threshold",
  "scaled_writer_min_processed_bytes_rebalance_threshold",
  "table_scan_scaled_processing_enabled",
  "table_scan_scale_up_memory_usage_ratio", "shuffle_compression_codec",
  "throw_exception_on_duplicate_map_keys",
  "index_lookup_join_max_prefetch_batches", "index_lookup_join_split_output",
  "request_data_sizes_max_wait_sec",
  "streaming_aggregation_min_output_batch_rows",
  "streaming_aggregation_eager_flush", "field_names_in_json_cast_enabled",
  "operator_track_expression_stats", "enable_operator_batch_size_stats",
  "unnest_split_output", "query_memory_reclaimer_priority",
  "max_num_splits_listened_to", "source", "client_tags",
};
// clang-format on

static_assert(std::size(kCanonicalKeys) == kN);

// Wire protocol: all 13 keys from kParameterStatusVariables (canonical case).
static constexpr std::string_view kWireKeys[] = {
  "application_name", "client_encoding",
  "DateStyle",        "default_transaction_read_only",
  "in_hot_standby",   "integer_datetimes",
  "IntervalStyle",    "scram_iterations",
  "search_path",      "server_encoding",
  "server_version",   "standard_conforming_strings",
  "TimeZone",
};

// SQL SET/SHOW path: typical keys the SQL parser sends (lowercase).
// Mix of pg vars, the canonical-cased ones, and velox-specific vars.
static constexpr std::string_view kSqlKeys[] = {
  "datestyle",
  "timezone",
  "intervalstyle",
  "search_path",
  "application_name",
  "server_version",
  "standard_conforming_strings",
  "session_timezone",
  "spill_enabled",
  "max_partial_aggregation_memory",
  "query_max_memory_per_node",
  "max_split_preload_per_driver",
};

// Miss keys – not in the map, with lengths that collide with real keys.
// "spill_enabled" is 13 chars, so "no_such_param" (13) is a real collision.
static constexpr std::string_view kMissKeys[] = {
  "no_such_param",     // len 13 – same as "spill_enabled"
  "unknown_variable",  // len 16 – same as "scram_iterations", "server_version"
  "nonexistent_config_key",  // len 22
  "xyz",                     // short, len 3
};

// ===========================================================================
// Approach 1 – frozen::unordered_map, lowercase keys  [#ifdef SDB_BENCH_FROZEN]
// ===========================================================================

#ifdef SDB_BENCH_FROZEN
// clang-format off
constexpr auto kFrozenLower =
  frozen::make_unordered_map<std::string_view, Value>({
    {"execution_threads", 0}, {"join_order_algorithm", 1},
    {"search_path", 2}, {"extra_float_digits", 3}, {"bytea_output", 4},
    {"client_encoding", 5}, {"application_name", 6},
    {"default_transaction_read_only", 7}, {"in_hot_standby", 8},
    {"integer_datetimes", 9}, {"scram_iterations", 10},
    {"server_encoding", 11}, {"server_version", 12},
    {"standard_conforming_strings", 13}, {"datestyle", 14},
    {"intervalstyle", 15}, {"timezone", 16},
    {"sdb_write_conflict_policy", 17}, {"sdb_read_your_own_writes", 18},
    {"default_transaction_isolation", 19}, {"transaction_isolation", 20},
    {"query_max_memory_per_node", 21}, {"session_timezone", 22},
    {"adjust_timestamp_to_session_timezone", 23},
    {"expression.eval_simplified", 24}, {"expression.track_cpu_usage", 25},
    {"track_operator_cpu_usage", 26}, {"legacy_cast", 27},
    {"cast_match_struct_by_name", 28},
    {"expression.max_array_size_in_reduce", 29},
    {"expression.max_compiled_regexes", 30},
    {"max_local_exchange_buffer_size", 31},
    {"max_local_exchange_partition_count", 32},
    {"min_local_exchange_partition_count_to_use_partition_buffer", 33},
    {"max_local_exchange_partition_buffer_size", 34},
    {"local_exchange_partition_buffer_preserve_encoding", 35},
    {"local_merge_source_queue_size", 36},
    {"exchange.max_buffer_size", 37}, {"merge_exchange.max_buffer_size", 38},
    {"min_exchange_output_batch_bytes", 39},
    {"max_partial_aggregation_memory", 40},
    {"max_extended_partial_aggregation_memory", 41},
    {"abandon_partial_aggregation_min_rows", 42},
    {"abandon_partial_aggregation_min_pct", 43},
    {"abandon_partial_topn_row_number_min_rows", 44},
    {"abandon_partial_topn_row_number_min_pct", 45},
    {"max_elements_size_in_repeat_and_sequence", 46},
    {"max_page_partitioning_buffer_size", 47},
    {"max_output_buffer_size", 48}, {"preferred_output_batch_bytes", 49},
    {"preferred_output_batch_rows", 50}, {"max_output_batch_rows", 51},
    {"table_scan_getoutput_time_limit_ms", 52},
    {"hash_adaptivity_enabled", 53},
    {"adaptive_filter_reordering_enabled", 54},
    {"spill_enabled", 55}, {"aggregation_spill_enabled", 56},
    {"join_spill_enabled", 57},
    {"mixed_grouped_mode_hash_join_spill_enabled", 58},
    {"order_by_spill_enabled", 59}, {"window_spill_enabled", 60},
    {"writer_spill_enabled", 61}, {"row_number_spill_enabled", 62},
    {"topn_row_number_spill_enabled", 63}, {"local_merge_spill_enabled", 64},
    {"local_merge_max_num_merge_sources", 65},
    {"max_spill_run_rows", 66}, {"max_spill_bytes", 67},
    {"max_spill_level", 68}, {"max_spill_file_size", 69},
    {"spill_compression_codec", 70}, {"spill_prefixsort_enabled", 71},
    {"spill_write_buffer_size", 72}, {"spill_read_buffer_size", 73},
    {"spill_file_create_config", 74}, {"spiller_start_partition_bit", 75},
    {"spiller_num_partition_bits", 76},
    {"min_spillable_reservation_pct", 77},
    {"spillable_reservation_growth_pct", 78},
    {"writer_flush_threshold_bytes", 79},
    {"presto.array_agg.ignore_nulls", 80}, {"spark.ansi_enabled", 81},
    {"spark.bloom_filter.expected_num_items", 82},
    {"spark.bloom_filter.num_bits", 83},
    {"spark.bloom_filter.max_num_bits", 84},
    {"spark.partition_id", 85}, {"spark.legacy_date_formatter", 86},
    {"spark.legacy_statistical_aggregate", 87},
    {"spark.json_ignore_null_fields", 88},
    {"task_writer_count", 89}, {"task_partitioned_writer_count", 90},
    {"hash_probe_finish_early_on_empty_build", 91},
    {"min_table_rows_for_parallel_join_build", 92},
    {"debug.validate_output_from_operators", 93},
    {"enable_expression_evaluation_cache", 94},
    {"max_shared_subexpr_results_cached", 95},
    {"max_split_preload_per_driver", 96},
    {"driver_cpu_time_slice_limit_ms", 97},
    {"prefixsort_normalized_key_max_bytes", 98},
    {"prefixsort_min_rows", 99}, {"prefixsort_max_string_prefix_length", 100},
    {"query_trace_enabled", 101}, {"query_trace_dir", 102},
    {"query_trace_node_id", 103}, {"query_trace_max_bytes", 104},
    {"query_trace_task_reg_exp", 105}, {"query_trace_dry_run", 106},
    {"op_trace_directory_create_config", 107},
    {"debug_disable_expression_with_peeling", 108},
    {"debug_disable_common_sub_expressions", 109},
    {"debug_disable_expression_with_memoization", 110},
    {"debug_disable_expression_with_lazy_inputs", 111},
    {"debug_aggregation_approx_percentile_fixed_random_seed", 112},
    {"debug_memory_pool_name_regex", 113},
    {"debug_memory_pool_warn_threshold_bytes", 114},
    {"debug_lambda_function_evaluation_batch_size", 115},
    {"debug_bing_tile_children_max_zoom_shift", 116},
    {"selective_nimble_reader_enabled", 117},
    {"scaled_writer_rebalance_max_memory_usage_ratio", 118},
    {"scaled_writer_max_partitions_per_writer", 119},
    {"scaled_writer_min_partition_processed_bytes_rebalance_threshold", 120},
    {"scaled_writer_min_processed_bytes_rebalance_threshold", 121},
    {"table_scan_scaled_processing_enabled", 122},
    {"table_scan_scale_up_memory_usage_ratio", 123},
    {"shuffle_compression_codec", 124},
    {"throw_exception_on_duplicate_map_keys", 125},
    {"index_lookup_join_max_prefetch_batches", 126},
    {"index_lookup_join_split_output", 127},
    {"request_data_sizes_max_wait_sec", 128},
    {"streaming_aggregation_min_output_batch_rows", 129},
    {"streaming_aggregation_eager_flush", 130},
    {"field_names_in_json_cast_enabled", 131},
    {"operator_track_expression_stats", 132},
    {"enable_operator_batch_size_stats", 133},
    {"unnest_split_output", 134}, {"query_memory_reclaimer_priority", 135},
    {"max_num_splits_listened_to", 136}, {"source", 137},
    {"client_tags", 138},
  });
// clang-format on
#endif  // SDB_BENCH_FROZEN

// ===========================================================================
// Approach 2 – frozen::unordered_map, canonical keys, icase hash/equal
//              [IcaseHash/IcaseEqual also used by approach 4]
// ===========================================================================

struct IcaseHash {
  constexpr std::size_t operator()(std::string_view v) const noexcept {
    std::size_t d = 5381;
    for (char c : v) {
      if (c >= 'A' && c <= 'Z') {
        c = static_cast<char>(c - 'A' + 'a');
      }
      d = d * 33 + static_cast<unsigned char>(c);
    }
    return d;
  }
  constexpr std::size_t operator()(std::string_view v,
                                   std::size_t seed) const noexcept {
    std::size_t d = (0x811c9dc5 ^ seed) * static_cast<std::size_t>(0x01000193);
    for (char c : v) {
      if (c >= 'A' && c <= 'Z') {
        c = static_cast<char>(c - 'A' + 'a');
      }
      d = (d ^ static_cast<unsigned char>(c)) *
          static_cast<std::size_t>(0x01000193);
    }
    return d >> 8;
  }
  using is_transparent = void;
};

struct IcaseEqual {
  constexpr bool operator()(std::string_view a,
                            std::string_view b) const noexcept {
    if (a.size() != b.size()) {
      return false;
    }
    for (std::size_t i = 0; i < a.size(); ++i) {
      char ca = a[i] >= 'A' && a[i] <= 'Z' ? a[i] - 'A' + 'a' : a[i];
      char cb = b[i] >= 'A' && b[i] <= 'Z' ? b[i] - 'A' + 'a' : b[i];
      if (ca != cb) {
        return false;
      }
    }
    return true;
  }
  using is_transparent = void;
};

#ifdef SDB_BENCH_FROZEN
// clang-format off
constexpr auto kFrozenIcase =
  frozen::make_unordered_map<std::string_view, Value>(
    {
      {"execution_threads", 0}, {"join_order_algorithm", 1},
      {"search_path", 2}, {"extra_float_digits", 3}, {"bytea_output", 4},
      {"client_encoding", 5}, {"application_name", 6},
      {"default_transaction_read_only", 7}, {"in_hot_standby", 8},
      {"integer_datetimes", 9}, {"scram_iterations", 10},
      {"server_encoding", 11}, {"server_version", 12},
      {"standard_conforming_strings", 13}, {"DateStyle", 14},
      {"IntervalStyle", 15}, {"TimeZone", 16},
      {"sdb_write_conflict_policy", 17}, {"sdb_read_your_own_writes", 18},
      {"default_transaction_isolation", 19}, {"transaction_isolation", 20},
      {"query_max_memory_per_node", 21}, {"session_timezone", 22},
      {"adjust_timestamp_to_session_timezone", 23},
      {"expression.eval_simplified", 24}, {"expression.track_cpu_usage", 25},
      {"track_operator_cpu_usage", 26}, {"legacy_cast", 27},
      {"cast_match_struct_by_name", 28},
      {"expression.max_array_size_in_reduce", 29},
      {"expression.max_compiled_regexes", 30},
      {"max_local_exchange_buffer_size", 31},
      {"max_local_exchange_partition_count", 32},
      {"min_local_exchange_partition_count_to_use_partition_buffer", 33},
      {"max_local_exchange_partition_buffer_size", 34},
      {"local_exchange_partition_buffer_preserve_encoding", 35},
      {"local_merge_source_queue_size", 36},
      {"exchange.max_buffer_size", 37}, {"merge_exchange.max_buffer_size", 38},
      {"min_exchange_output_batch_bytes", 39},
      {"max_partial_aggregation_memory", 40},
      {"max_extended_partial_aggregation_memory", 41},
      {"abandon_partial_aggregation_min_rows", 42},
      {"abandon_partial_aggregation_min_pct", 43},
      {"abandon_partial_topn_row_number_min_rows", 44},
      {"abandon_partial_topn_row_number_min_pct", 45},
      {"max_elements_size_in_repeat_and_sequence", 46},
      {"max_page_partitioning_buffer_size", 47},
      {"max_output_buffer_size", 48}, {"preferred_output_batch_bytes", 49},
      {"preferred_output_batch_rows", 50}, {"max_output_batch_rows", 51},
      {"table_scan_getoutput_time_limit_ms", 52},
      {"hash_adaptivity_enabled", 53},
      {"adaptive_filter_reordering_enabled", 54},
      {"spill_enabled", 55}, {"aggregation_spill_enabled", 56},
      {"join_spill_enabled", 57},
      {"mixed_grouped_mode_hash_join_spill_enabled", 58},
      {"order_by_spill_enabled", 59}, {"window_spill_enabled", 60},
      {"writer_spill_enabled", 61}, {"row_number_spill_enabled", 62},
      {"topn_row_number_spill_enabled", 63},
      {"local_merge_spill_enabled", 64},
      {"local_merge_max_num_merge_sources", 65},
      {"max_spill_run_rows", 66}, {"max_spill_bytes", 67},
      {"max_spill_level", 68}, {"max_spill_file_size", 69},
      {"spill_compression_codec", 70}, {"spill_prefixsort_enabled", 71},
      {"spill_write_buffer_size", 72}, {"spill_read_buffer_size", 73},
      {"spill_file_create_config", 74}, {"spiller_start_partition_bit", 75},
      {"spiller_num_partition_bits", 76},
      {"min_spillable_reservation_pct", 77},
      {"spillable_reservation_growth_pct", 78},
      {"writer_flush_threshold_bytes", 79},
      {"presto.array_agg.ignore_nulls", 80}, {"spark.ansi_enabled", 81},
      {"spark.bloom_filter.expected_num_items", 82},
      {"spark.bloom_filter.num_bits", 83},
      {"spark.bloom_filter.max_num_bits", 84},
      {"spark.partition_id", 85}, {"spark.legacy_date_formatter", 86},
      {"spark.legacy_statistical_aggregate", 87},
      {"spark.json_ignore_null_fields", 88},
      {"task_writer_count", 89}, {"task_partitioned_writer_count", 90},
      {"hash_probe_finish_early_on_empty_build", 91},
      {"min_table_rows_for_parallel_join_build", 92},
      {"debug.validate_output_from_operators", 93},
      {"enable_expression_evaluation_cache", 94},
      {"max_shared_subexpr_results_cached", 95},
      {"max_split_preload_per_driver", 96},
      {"driver_cpu_time_slice_limit_ms", 97},
      {"prefixsort_normalized_key_max_bytes", 98},
      {"prefixsort_min_rows", 99},
      {"prefixsort_max_string_prefix_length", 100},
      {"query_trace_enabled", 101}, {"query_trace_dir", 102},
      {"query_trace_node_id", 103}, {"query_trace_max_bytes", 104},
      {"query_trace_task_reg_exp", 105}, {"query_trace_dry_run", 106},
      {"op_trace_directory_create_config", 107},
      {"debug_disable_expression_with_peeling", 108},
      {"debug_disable_common_sub_expressions", 109},
      {"debug_disable_expression_with_memoization", 110},
      {"debug_disable_expression_with_lazy_inputs", 111},
      {"debug_aggregation_approx_percentile_fixed_random_seed", 112},
      {"debug_memory_pool_name_regex", 113},
      {"debug_memory_pool_warn_threshold_bytes", 114},
      {"debug_lambda_function_evaluation_batch_size", 115},
      {"debug_bing_tile_children_max_zoom_shift", 116},
      {"selective_nimble_reader_enabled", 117},
      {"scaled_writer_rebalance_max_memory_usage_ratio", 118},
      {"scaled_writer_max_partitions_per_writer", 119},
      {"scaled_writer_min_partition_processed_bytes_rebalance_threshold", 120},
      {"scaled_writer_min_processed_bytes_rebalance_threshold", 121},
      {"table_scan_scaled_processing_enabled", 122},
      {"table_scan_scale_up_memory_usage_ratio", 123},
      {"shuffle_compression_codec", 124},
      {"throw_exception_on_duplicate_map_keys", 125},
      {"index_lookup_join_max_prefetch_batches", 126},
      {"index_lookup_join_split_output", 127},
      {"request_data_sizes_max_wait_sec", 128},
      {"streaming_aggregation_min_output_batch_rows", 129},
      {"streaming_aggregation_eager_flush", 130},
      {"field_names_in_json_cast_enabled", 131},
      {"operator_track_expression_stats", 132},
      {"enable_operator_batch_size_stats", 133},
      {"unnest_split_output", 134},
      {"query_memory_reclaimer_priority", 135},
      {"max_num_splits_listened_to", 136}, {"source", 137},
      {"client_tags", 138},
    },
    IcaseHash{}, IcaseEqual{});
// clang-format on
#endif  // SDB_BENCH_FROZEN

// ===========================================================================
// Approach 3 – absl::flat_hash_map, lowercase keys
// ===========================================================================

const absl::flat_hash_map<std::string, Value> kAbslLower = [] {
  absl::flat_hash_map<std::string, Value> m;
  m.reserve(kN);
  for (std::size_t i = 0; i < kN; ++i) {
    m.emplace(std::string{kLowerKeys[i]}, static_cast<Value>(i));
  }
  return m;
}();

// ===========================================================================
// Approach 4 – absl::flat_hash_map, canonical keys, icase hash/equal
// ===========================================================================

const absl::flat_hash_map<std::string, Value, IcaseHash, IcaseEqual>
  kAbslIcase = [] {
    absl::flat_hash_map<std::string, Value, IcaseHash, IcaseEqual> m;
    m.reserve(kN);
    for (std::size_t i = 0; i < kN; ++i) {
      m.emplace(std::string{kCanonicalKeys[i]}, static_cast<Value>(i));
    }
    return m;
  }();

// ===========================================================================
// Approach 6 – utils::TrivialBiMap, lowercase keys
// ===========================================================================

static constexpr Value kTrivialValues[] = {
  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  11,  12,  13,
  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,
  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,  41,
  42,  43,  44,  45,  46,  47,  48,  49,  50,  51,  52,  53,  54,  55,
  56,  57,  58,  59,  60,  61,  62,  63,  64,  65,  66,  67,  68,  69,
  70,  71,  72,  73,  74,  75,  76,  77,  78,  79,  80,  81,  82,  83,
  84,  85,  86,  87,  88,  89,  90,  91,  92,  93,  94,  95,  96,  97,
  98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111,
  112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125,
  126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138,
};
static_assert(std::size(kTrivialValues) == kN);

constexpr auto kTrivialMap =
  sdb::containers::MakeTrivialBiMap<kLowerKeys, kTrivialValues>();

// ===========================================================================
// Helpers
// ===========================================================================

template<typename Map>
[[gnu::noinline]] static void LookupAll(benchmark::State& state, const Map& map,
                                        const std::string_view* keys,
                                        std::size_t n, bool tolower_input) {
  for (auto _ : state) {
    for (std::size_t i = 0; i < n; ++i) {
      if (tolower_input) {
        std::string lower = absl::AsciiStrToLower(keys[i]);
        auto it = map.find(std::string_view{lower});
        benchmark::DoNotOptimize(it);
      } else {
        auto it = map.find(keys[i]);
        benchmark::DoNotOptimize(it);
      }
    }
  }
}

[[gnu::noinline]] static void LinearLookup(benchmark::State& state,
                                           const std::string_view* keys,
                                           std::size_t n) {
  for (auto _ : state) {
    for (std::size_t i = 0; i < n; ++i) {
      std::string_view result;
      for (std::size_t j = 0; j < kN; ++j) {
        if (absl::EqualsIgnoreCase(kCanonicalKeys[j], keys[i])) {
          result = kCanonicalKeys[j];
          break;
        }
      }
      benchmark::DoNotOptimize(result);
    }
  }
}

[[gnu::noinline]] static void TrivialLookup(benchmark::State& state,
                                            const std::string_view* keys,
                                            std::size_t n) {
  for (auto _ : state) {
    for (std::size_t i = 0; i < n; ++i) {
      auto idx = kTrivialMap.TryFindICaseByFirst(keys[i]);
      // Simulate production: use index to fetch canonical key from array.
      auto key = idx ? kCanonicalKeys[*idx] : std::string_view{};
      benchmark::DoNotOptimize(key);
    }
  }
}

// ===========================================================================
// Wire-protocol path (13 canonical-case keys, happens once per connection)
// ===========================================================================

#ifdef SDB_BENCH_FROZEN
void BmFrozenLower_Wire(benchmark::State& s) {
  LookupAll(s, kFrozenLower, kWireKeys, std::size(kWireKeys), true);
}
void BmFrozenIcase_Wire(benchmark::State& s) {
  LookupAll(s, kFrozenIcase, kWireKeys, std::size(kWireKeys), false);
}
#endif  // SDB_BENCH_FROZEN
void BmAbslLower_Wire(benchmark::State& s) {
  LookupAll(s, kAbslLower, kWireKeys, std::size(kWireKeys), true);
}
void BmAbslIcase_Wire(benchmark::State& s) {
  LookupAll(s, kAbslIcase, kWireKeys, std::size(kWireKeys), false);
}
void BmLinear_Wire(benchmark::State& s) {
  LinearLookup(s, kWireKeys, std::size(kWireKeys));
}
void BmTrivial_Wire(benchmark::State& s) {
  TrivialLookup(s, kWireKeys, std::size(kWireKeys));
}

#ifdef SDB_BENCH_FROZEN
BENCHMARK(BmFrozenLower_Wire);
BENCHMARK(BmFrozenIcase_Wire);
#endif  // SDB_BENCH_FROZEN
BENCHMARK(BmAbslLower_Wire);
BENCHMARK(BmAbslIcase_Wire);
BENCHMARK(BmLinear_Wire);
BENCHMARK(BmTrivial_Wire);

// ===========================================================================
// SQL SET/SHOW path (lowercase input)
// ===========================================================================

#ifdef SDB_BENCH_FROZEN
void BmFrozenLower_Sql(benchmark::State& s) {
  LookupAll(s, kFrozenLower, kSqlKeys, std::size(kSqlKeys), false);
}
void BmFrozenIcase_Sql(benchmark::State& s) {
  LookupAll(s, kFrozenIcase, kSqlKeys, std::size(kSqlKeys), false);
}
#endif  // SDB_BENCH_FROZEN
void BmAbslLower_Sql(benchmark::State& s) {
  LookupAll(s, kAbslLower, kSqlKeys, std::size(kSqlKeys), false);
}
void BmAbslIcase_Sql(benchmark::State& s) {
  LookupAll(s, kAbslIcase, kSqlKeys, std::size(kSqlKeys), false);
}
void BmLinear_Sql(benchmark::State& s) {
  LinearLookup(s, kSqlKeys, std::size(kSqlKeys));
}
void BmTrivial_Sql(benchmark::State& s) {
  TrivialLookup(s, kSqlKeys, std::size(kSqlKeys));
}

#ifdef SDB_BENCH_FROZEN
BENCHMARK(BmFrozenLower_Sql);
BENCHMARK(BmFrozenIcase_Sql);
#endif  // SDB_BENCH_FROZEN
BENCHMARK(BmAbslLower_Sql);
BENCHMARK(BmAbslIcase_Sql);
BENCHMARK(BmLinear_Sql);
BENCHMARK(BmTrivial_Sql);

// ===========================================================================
// Miss cases (varied key lengths to exercise length-dispatch fairly)
// ===========================================================================

#ifdef SDB_BENCH_FROZEN
void BmFrozenLower_Miss(benchmark::State& s) {
  LookupAll(s, kFrozenLower, kMissKeys, std::size(kMissKeys), false);
}
void BmFrozenIcase_Miss(benchmark::State& s) {
  LookupAll(s, kFrozenIcase, kMissKeys, std::size(kMissKeys), false);
}
#endif  // SDB_BENCH_FROZEN
void BmAbslLower_Miss(benchmark::State& s) {
  LookupAll(s, kAbslLower, kMissKeys, std::size(kMissKeys), false);
}
void BmAbslIcase_Miss(benchmark::State& s) {
  LookupAll(s, kAbslIcase, kMissKeys, std::size(kMissKeys), false);
}
void BmLinear_Miss(benchmark::State& s) {
  LinearLookup(s, kMissKeys, std::size(kMissKeys));
}
void BmTrivial_Miss(benchmark::State& s) {
  TrivialLookup(s, kMissKeys, std::size(kMissKeys));
}

#ifdef SDB_BENCH_FROZEN
BENCHMARK(BmFrozenLower_Miss);
BENCHMARK(BmFrozenIcase_Miss);
#endif  // SDB_BENCH_FROZEN
BENCHMARK(BmAbslLower_Miss);
BENCHMARK(BmAbslIcase_Miss);
BENCHMARK(BmLinear_Miss);
BENCHMARK(BmTrivial_Miss);

}  // namespace

BENCHMARK_MAIN();
