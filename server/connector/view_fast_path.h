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

#pragma once

#include <duckdb/common/case_insensitive_map.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/function/table_function.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "catalog/pk_spec.h"

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::catalog {

class PgSqlView;

}  // namespace sdb::catalog
namespace sdb::connector {

struct CatalogTableRef {
  std::string catalog;
  std::string schema;
  std::string table;
};

// One column of an ExternalColumnKey: same shape for engine PK metadata
// (clickhouse) and user WITH (key_columns).
struct ExternalKeyColumn {
  std::string name;               // source column name (WHERE + re-fetch)
  duckdb::column_t source_index;  // position in the source table (projection)
  duckdb::LogicalType type;       // projected + stored type
};

struct ViewFastPath {
  duckdb::vector<duckdb::Value> args;
  duckdb::named_parameter_map_t named_params;
  std::optional<CatalogTableRef> catalog_ref;
  // Source-side names post CAST-peel. Empty for `SELECT *`.
  std::vector<std::string> projection_columns;
  std::string function_name;
  bool is_glob = false;
  // 0 = not pinned. Set at query time from the index's commit payload.
  int64_t pinned_iceberg_snapshot_id = 0;
  catalog::PkSpec pk_spec;
  // ExternalColumnKey: the key columns in order (any types, count >= 1);
  // empty for ExternalPostgresCtid (keyed on the virtual duckdb rowid).
  std::vector<ExternalKeyColumn> key_columns;
  // Whether the backing reader's lookup applies pushed table filters (parquet /
  // duckdb yes; csv / json / text no). Drives filter pushdown -- see
  // IResearchSupportsPushdownFilter.
  bool supports_filters = false;
};

// key_columns: user lookup key columns; empty = auto (pg ctid / CH PK).
// Build and lookup must pass the SAME value (CREATE INDEX / persisted opts).
std::optional<ViewFastPath> ResolveViewFastPath(
  duckdb::ClientContext& context, const catalog::PgSqlView& view,
  std::span<const std::string> key_columns = {});

std::vector<duckdb::column_t> BackfillPkVirtualColumns(const ViewFastPath& fp);

// The parsed `key_columns` CREATE INDEX option from an options map, or {} when
// absent.
std::vector<std::string> KeyColumnsFromOptions(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options);

duckdb::TableFunction MakeFastPathLookupFunction(const ViewFastPath& fp);

duckdb::unique_ptr<duckdb::FunctionData> BindFastPathSource(
  duckdb::ClientContext& context, const ViewFastPath& fp);

// 0 for non-iceberg.
int64_t ExtractIcebergSnapshotId(duckdb::FunctionData& bind_data) noexcept;

void EnableIcebergSort(duckdb::FunctionData* bind_data) noexcept;

std::string FormatLookupLabel(const ViewFastPath& fp);

}  // namespace sdb::connector
