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

#include <cstdint>
#include <duckdb/common/case_insensitive_map.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/function/table_function.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace duckdb {

class ClientContext;
struct CreateViewInfo;

}  // namespace duckdb
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

// How a view-backed index identifies a source row, and therefore what its
// generated pk column carries.
enum class PkSpec : uint8_t {
  DuckDBRowId,
  FileRowNumber,
  FileIndexPlusRowNumber,
  FileOffset,
  FileIndexPlusOffset,
  FileIndexPlusDuckDBRowId,
  ExternalPostgresCtid,
  ExternalColumnKey,
};

// The multi-file pk specs: the key carries a file index, so the index
// decomposes per source file and can refresh one file at a time.
constexpr bool IsGlobPK(PkSpec spec) noexcept {
  switch (spec) {
    case PkSpec::FileIndexPlusRowNumber:
    case PkSpec::FileIndexPlusOffset:
    case PkSpec::FileIndexPlusDuckDBRowId:
      return true;
    case PkSpec::DuckDBRowId:
    case PkSpec::FileRowNumber:
    case PkSpec::FileOffset:
    case PkSpec::ExternalPostgresCtid:
    case PkSpec::ExternalColumnKey:
      return false;
  }
}

// Row identity supplied by the remote engine rather than by the scan: the
// lookup re-fetches by key instead of by position.
constexpr bool IsExternalPK(PkSpec spec) noexcept {
  switch (spec) {
    case PkSpec::ExternalPostgresCtid:
    case PkSpec::ExternalColumnKey:
      return true;
    case PkSpec::DuckDBRowId:
    case PkSpec::FileRowNumber:
    case PkSpec::FileIndexPlusRowNumber:
    case PkSpec::FileOffset:
    case PkSpec::FileIndexPlusOffset:
    case PkSpec::FileIndexPlusDuckDBRowId:
      return false;
  }
}

// The file-shaped pk specs: row identity comes from (file, row-ish) -- the
// shapes whose builds capture a file manifest. NOTE: the stored pk COLUMN
// is the two-component struct below only for the glob variants;
// single-file sources store a scalar row pk.
constexpr bool IsFilePkSpec(PkSpec spec) noexcept {
  switch (spec) {
    case PkSpec::FileRowNumber:
    case PkSpec::FileIndexPlusRowNumber:
    case PkSpec::FileOffset:
    case PkSpec::FileIndexPlusOffset:
    case PkSpec::FileIndexPlusDuckDBRowId:
      return true;
    case PkSpec::DuckDBRowId:
    case PkSpec::ExternalPostgresCtid:
    case PkSpec::ExternalColumnKey:
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
  PkSpec pk_spec;
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
  duckdb::ClientContext& context, const duckdb::CreateViewInfo& view,
  std::span<const std::string> key_columns);

std::vector<duckdb::column_t> BackfillPkVirtualColumns(const ViewFastPath& fp);

// The parsed `key_columns` CREATE INDEX option from an options map, or {} when
// absent.
std::vector<std::string> KeyColumnsFromOptions(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options);

duckdb::TableFunction MakeFastPathLookupFunction(const ViewFastPath& fp);

duckdb::unique_ptr<duckdb::FunctionData> BindFastPathSource(
  duckdb::ClientContext& context, const ViewFastPath& fp);

// The scan side of a fast path -- the reader function, its bind and the schema
// it returns. A build reads through this; a point lookup goes through
// MakeFastPathLookupFunction instead.
struct FastPathScan {
  duckdb::TableFunction function;
  duckdb::unique_ptr<duckdb::FunctionData> bind_data;
  duckdb::vector<duckdb::LogicalType> types;
  duckdb::vector<duckdb::Identifier> names;
  duckdb::virtual_column_map_t virtual_columns;
};

std::optional<FastPathScan> BindFastPathScan(duckdb::ClientContext& context,
                                             const ViewFastPath& fp);

void EnableIcebergSort(duckdb::FunctionData* bind_data) noexcept;

std::string FormatLookupLabel(const ViewFastPath& fp);

}  // namespace sdb::connector
