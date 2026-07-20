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
#include <string_view>
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

// One column of a PkSpec::ExternalColumnKey lookup key. The list is the same
// whether the columns came from the engine's PK metadata (clickhouse) or the
// user's WITH (key_columns = '...').
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
  duckdb::vector<std::string> projection_columns;
  std::string function_name;
  bool is_glob = false;
  // 0 = not pinned. Set at query time from the index's commit payload.
  int64_t pinned_iceberg_snapshot_id = 0;
  catalog::PkSpec pk_spec;
  // For PkSpec::ExternalColumnKey: the key columns, in order (any types, any
  // count >= 1). Empty for PkSpec::ExternalRowId (postgres ctid -- keyed on the
  // virtual rowid, no real columns). The lookup renders `WHERE rowid IN (...)`
  // (ExternalRowId, pushed as a `ctid IN (...)` TID scan) or `WHERE col IN
  // (...)` / an OR of per-row equalities (ExternalColumnKey).
  duckdb::vector<ExternalKeyColumn> key_columns;
  // Whether the backing reader's lookup applies pushed table filters (parquet /
  // duckdb yes; csv / json / text no). Drives filter pushdown -- see
  // IResearchSupportsPushdownType.
  bool supports_filters = false;
};

// An external key is stored as a single BIGINT column (I64) when it is the ctid
// rowid or a single BIGINT column -- both fit the int64 fast path. Any other
// shape (a non-BIGINT column, or more than one column) is stored as a STRUCT
// column of the key columns' types.
inline bool ExternalKeyIsI64(const ViewFastPath& fp) noexcept {
  return fp.pk_spec == catalog::PkSpec::ExternalRowId ||
         (fp.pk_spec == catalog::PkSpec::ExternalColumnKey &&
          fp.key_columns.size() == 1 &&
          fp.key_columns[0].type.id() == duckdb::LogicalTypeId::BIGINT);
}

// `key_columns`: user-specified external-DB lookup key columns (CREATE INDEX
// WITH key_columns). Empty = auto-detect the default key (postgres ctid /
// clickhouse PK). Build and lookup must pass the SAME value (from the CREATE
// INDEX options and the persisted index options respectively).
std::optional<ViewFastPath> ResolveViewFastPath(
  duckdb::ClientContext& context, const catalog::PgSqlView& view,
  std::span<const std::string> key_columns = {});

std::vector<duckdb::column_t> BackfillPkVirtualColumns(const ViewFastPath& fp);

// Split a `key_columns` CREATE INDEX option value ("a, b, c") into trimmed
// column names. Empty/absent -> {}.
std::vector<std::string> ParseKeyColumns(std::string_view text);

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
