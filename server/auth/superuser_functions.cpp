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

#include "auth/superuser_functions.h"

#include <string_view>

namespace sdb::auth {
namespace {

constexpr std::string_view kSuperuserFunctions[] = {
  "add_parquet_key",
  "checkpoint",
  "clickhouse_clear_cache",
  "clickhouse_configure_pool",
  "clickhouse_execute",
  "clickhouse_lookup",
  "clickhouse_query",
  "clickhouse_scan",
  "clickhouse_scan_pushdown",
  "create_external_resource",
  "dbgen",
  "deregister_external_resource",
  "destroy_external_resource",
  "disable_checkpoint_on_shutdown",
  "disable_logging",
  "dsdgen",
  "duckdb_databases",
  "duckdb_external_file_cache",
  "duckdb_external_resource_types",
  "duckdb_external_resources",
  "duckdb_log_contexts",
  "duckdb_logs",
  "duckdb_secrets",
  "duckdb_table_sample",
  "duckdb_temporary_files",
  "enable_checkpoint_on_shutdown",
  "enable_logging",
  "force_checkpoint",
  "iceberg_to_ducklake",
  "json_serialize_plan",
  "pg_clear_cache",
  "pg_show_replication_origin_status",
  "postgres_attach",
  "postgres_configure_pool",
  "postgres_execute",
  "postgres_lookup",
  "postgres_query",
  "postgres_scan",
  "postgres_scan_pushdown",
  "pragma_database_size",
  "pragma_metadata_info",
  "pragma_storage_info",
  "register_external_resource",
  "register_external_resource_type",
  "sdb_databases",
  "sdb_external_file_cache",
  "sdb_external_resource_types",
  "sdb_external_resources",
  "sdb_log_contexts",
  "sdb_logs",
  "sdb_secrets",
  "sdb_table_sample",
  "sdb_temporary_files",
  "truncate_duckdb_logs",
  "truncate_sdb_logs",
  "which_secret",
};

}  // namespace

void RegisterSuperuserFunctions(duckdb::DBConfig& config) {
  for (const auto name : kSuperuserFunctions) {
    config.superuser_functions.emplace(duckdb::Identifier{name});
  }
}

}  // namespace sdb::auth
