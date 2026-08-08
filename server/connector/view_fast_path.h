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
#include <duckdb/common/open_file_info.hpp>
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

// The stored pk column's type for an ExternalColumnKey index: the key
// columns packed in resolution order, each field under its own column name.
inline duckdb::LogicalType ExternalKeyStructType(
  std::span<const ExternalKeyColumn> keys) {
  duckdb::child_list_t<duckdb::LogicalType> fields;
  fields.reserve(keys.size());
  for (const auto& key : keys) {
    fields.emplace_back(key.name, key.type);
  }
  return duckdb::LogicalType::STRUCT(std::move(fields));
}

// The file-shaped pk specs: row identity comes from (file, row-ish) -- the
// shapes whose builds capture a file manifest. NOTE: the stored pk COLUMN
// is the two-component struct below only for the glob variants (pk_term
// indexes); single-file sources store a scalar row pk.
constexpr bool IsFilePkSpec(catalog::PkSpec spec) noexcept {
  switch (spec) {
    case catalog::PkSpec::FileRowNumber:
    case catalog::PkSpec::FileIndexPlusRowNumber:
    case catalog::PkSpec::FileOffset:
    case catalog::PkSpec::FileIndexPlusOffset:
    case catalog::PkSpec::FileIndexPlusDuckDBRowId:
      return true;
    case catalog::PkSpec::DuckDBRowId:
    case catalog::PkSpec::ExternalPostgresCtid:
    case catalog::PkSpec::ExternalColumnKey:
      return false;
  }
}

inline const duckdb::LogicalType& FileIndexRowNumberStructType() {
  static const auto kType = [] {
    duckdb::child_list_t<duckdb::LogicalType> fields;
    fields.emplace_back("file_index", duckdb::LogicalType::UBIGINT);
    fields.emplace_back("row_number", duckdb::LogicalType::BIGINT);
    return duckdb::LogicalType::STRUCT(std::move(fields));
  }();
  return kType;
}

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
  // The view supports per-file DELTA refresh -- it decomposes exactly per
  // source file: glob pk shape, no union_by_name, no LIMIT. LIMIT is the
  // only admitted construct that couples rows ACROSS files (GROUP BY /
  // HAVING / QUALIFY / SAMPLE / CTEs / DISTINCT never get a fast path at
  // all; WHERE re-applies when a pass binds the view narrowed to one file;
  // ORDER BY drops no rows). Everything else refreshes by rebuild.
  bool supports_delta = false;

  // The stored pk column's type -- what the create sink stages and
  // generated_pk declares/projects: one case per pk spec. A new spec must
  // decide its exposure here.
  duckdb::LogicalType GeneratedPkType() const;
};

// key_columns: user lookup key columns; empty = auto (pg ctid / CH PK).
// Build and lookup must pass the SAME value (CREATE INDEX / persisted opts).
std::optional<ViewFastPath> ResolveViewFastPath(
  duckdb::ClientContext& context, const catalog::PgSqlView& view,
  std::span<const std::string> key_columns);

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
