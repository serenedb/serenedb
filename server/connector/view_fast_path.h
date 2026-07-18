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

#include <duckdb/common/types.hpp>
#include <duckdb/function/table_function.hpp>
#include <memory>
#include <optional>
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

// What the source engine promises about the lookup key's uniqueness.
enum class PkUniqueness {
  // Only a sorting/ordering hint (a ClickHouse MergeTree "primary key") --
  // duplicates are possible, CREATE INDEX must handle them (on_conflict).
  Unverified,
  // The engine enforces uniqueness itself (a postgres PRIMARY KEY).
  Enforced,
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
  // For PkSpec::ExternalDBKey: the engine-metadata PK column the index keys on
  // and re-fetches by (`WHERE pk_column_name IN (...)`). pk_column_index is the
  // column's position in the source table (for BackfillPkVirtualColumns).
  duckdb::column_t pk_column_index = 0;
  std::string pk_column_name;
  PkUniqueness pk_uniqueness = PkUniqueness::Unverified;
  // For PkSpec::ExternalDBKey over postgres: key on the row's physical locator
  // (ctid, surfaced as the duckdb rowid) instead of a PK column -- universal (no
  // PK needed) and unique within the static index's snapshot. The build projects
  // the rowid; the lookup renders `rowid IN (...)`, pushed as a `ctid IN (...)`
  // TID scan. pk_column_index/pk_column_name are unused in this mode.
  bool pk_is_rowid = false;
  // Whether the backing reader's lookup applies pushed table filters (parquet /
  // duckdb yes; csv / json / text no). Drives filter pushdown -- see
  // IResearchSupportsPushdownType.
  bool supports_filters = false;
};

std::optional<ViewFastPath> ResolveViewFastPath(duckdb::ClientContext& context,
                                                const catalog::PgSqlView& view);

std::vector<duckdb::column_t> BackfillPkVirtualColumns(const ViewFastPath& fp);

duckdb::TableFunction MakeFastPathLookupFunction(const ViewFastPath& fp);

duckdb::unique_ptr<duckdb::FunctionData> BindFastPathSource(
  duckdb::ClientContext& context, const ViewFastPath& fp);

// 0 for non-iceberg.
int64_t ExtractIcebergSnapshotId(duckdb::FunctionData& bind_data) noexcept;

void EnableIcebergSort(duckdb::FunctionData* bind_data) noexcept;

std::string FormatLookupLabel(const ViewFastPath& fp);

}  // namespace sdb::connector
