////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "auth/function_privileges.h"

#include <string_view>

namespace sdb::auth {
namespace {

constexpr std::string_view kFileFreeFunctions[] = {
  "aclexplode",
  "check_peg_parser",
  "disable_profiling",
  "docs",
  "duckdb_approx_database_count",
  "duckdb_available_metrics",
  "duckdb_columns",
  "duckdb_connection_count",
  "duckdb_constraints",
  "duckdb_coordinate_systems",
  "duckdb_dependencies",
  "duckdb_eviction_queues",
  "duckdb_extension_repositories",
  "duckdb_functions",
  "duckdb_indexes",
  "duckdb_keywords",
  "duckdb_memory",
  "duckdb_optimizers",
  "duckdb_prepared_statements",
  "duckdb_schemas",
  "duckdb_secret_type_parameters",
  "duckdb_secret_types",
  "duckdb_sequences",
  "duckdb_settings",
  "duckdb_tables",
  "duckdb_triggers",
  "duckdb_types",
  "duckdb_variables",
  "duckdb_views",
  "enable_profiling",
  "es_bulk",
  "es_bulk_source",
  "es_cat_indices",
  "es_create_index",
  "es_doc",
  "es_drop_index",
  "es_mapping",
  "es_refresh",
  "execute_job",
  "generate_series",
  "icu_calendar_names",
  "iresearch_scan",
  "json_array_elements",
  "json_array_elements_text",
  "json_each",
  "json_each_text",
  "json_execute_serialized_sql",
  "json_object_keys",
  "json_populate_recordset",
  "json_tree",
  "object",
  "objects",
  "otel_parse_logs",
  "otel_parse_metrics_exponential_histogram",
  "otel_parse_metrics_gauge",
  "otel_parse_metrics_histogram",
  "otel_parse_metrics_sum",
  "otel_parse_metrics_summary",
  "otel_parse_traces",
  "otel_source_logs",
  "otel_source_metrics_exponential_histogram",
  "otel_source_metrics_gauge",
  "otel_source_metrics_histogram",
  "otel_source_metrics_sum",
  "otel_source_metrics_summary",
  "otel_source_traces",
  "pg_extension_update_paths",
  "pg_prepared_statement",
  "pg_show_all_settings",
  "pg_stat_get_subscription",
  "pg_stat_get_subscription_stats",
  "pg_tablespace_databases",
  "pg_timezone_names",
  "pragma_collations",
  "pragma_platform",
  "pragma_show",
  "pragma_table_info",
  "pragma_user_agent",
  "pragma_version",
  "query",
  "query_table",
  "range",
  "range_st",
  "repeat",
  "repeat_row",
  "repl_src",
  "sdb_approx_database_count",
  "sdb_available_metrics",
  "sdb_catalog_sets",
  "sdb_columns",
  "sdb_connection_count",
  "sdb_constraints",
  "sdb_coordinate_systems",
  "sdb_deferred_catalog",
  "sdb_dependencies",
  "sdb_eviction_queues",
  "sdb_extension_repositories",
  "sdb_functions",
  "sdb_indexes",
  "sdb_job_runs",
  "sdb_jobs",
  "sdb_keywords",
  "sdb_memory",
  "sdb_optimizers",
  "sdb_prepared_statements",
  "sdb_schemas",
  "sdb_secret_type_parameters",
  "sdb_secret_types",
  "sdb_sequences",
  "sdb_settings",
  "sdb_tables",
  "sdb_triggers",
  "sdb_types",
  "sdb_variables",
  "sdb_views",
  "search",
  "seq_scan",
  "serenedb_reindex",
  "serenedb_vacuum",
  "sql_tokenize",
  "summary",
  "system_table_scan",
  "test_all_types",
  "test_vector_types",
  "tpcds_answers",
  "tpcds_queries",
  "tpch_answers",
  "tpch_queries",
  "unnest",
};

}  // namespace

void RegisterFunctionPrivileges(duckdb::DBConfig& config) {
  for (const auto name : kFileFreeFunctions) {
    config.file_free_functions.emplace(duckdb::Identifier{name});
  }
}

}  // namespace sdb::auth
