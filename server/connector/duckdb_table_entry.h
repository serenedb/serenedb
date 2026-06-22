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

#include <duckdb.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>

#include "catalog/table.h"

namespace sdb::connector {

// Virtual column ID for tableoid (PG system column). Always returns 0.
// Placed in the special-identifier range alongside COLUMN_IDENTIFIER_ROW_*.
// COLUMN_IDENTIFIER_ROW_NUMBER is 2^64-3, this is 2^64-4.
inline constexpr duckdb::column_t kColumnIdentifierTableOid =
  UINT64_C(18446744073709551612);

// Virtual column ID for the synthetic primary key on tables without a
// declared PK. Reuses the role that DuckDB's COLUMN_IDENTIFIER_ROW_ID
// would otherwise serve for: row identity in UPDATE/DELETE plans and
// the source bytes for the kGeneratedPKId catalog column on backfill.
// Distinct from COLUMN_IDENTIFIER_ROW_ID so we can advertise it
// explicitly only on no-PK tables/views; PK-bearing relations use
// their PK virtual columns for row identity and don't expose rowid at
// all.
// 2^64-5, one slot below kColumnIdentifierTableOid.
inline constexpr duckdb::column_t kColumnIdentifierGeneratedPk =
  UINT64_C(18446744073709551611);

class SereneDBTableEntry final : public duckdb::TableCatalogEntry {
 public:
  // indexed_col_indices: table column indices that are part of any index.
  // The "FROM index_name" pattern is handled by SereneDBIndexScanEntry, NOT
  // by passing index info here.
  SereneDBTableEntry(duckdb::Catalog& catalog,
                     duckdb::SchemaCatalogEntry& schema,
                     duckdb::CreateTableInfo& info,
                     std::shared_ptr<catalog::Table> sdb_table,
                     std::vector<size_t> indexed_col_indices = {});

  duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(
    duckdb::ClientContext& context, duckdb::column_t column_id) final;

  duckdb::TableFunction GetScanFunction(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::FunctionData>& bind_data) final;

  duckdb::virtual_column_map_t GetVirtualColumns() const override;

  duckdb::Catalog& GetStorageCatalog(duckdb::ClientContext& context) override;

  duckdb::DuckTableEntry& GetStorageTableEntry(
    duckdb::ClientContext& context) override;

  duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext& context) final;

  // Resolves the hidden store table backing this facade entry.
  duckdb::TableCatalogEntry& ResolveStoreEntry(
    duckdb::ClientContext& context) const;

  // Helpers shared with SereneDBIndexScanEntry. These compute virtual
  // columns / rowid columns / storage info from the underlying SereneDB
  // table only -- no entry-instance state is needed -- so they're static
  // and reusable across catalog entry types that wrap the same table.
  static duckdb::vector<duckdb::column_t> BuildRowIdColumns(
    const catalog::Table& table,
    const std::vector<size_t>& indexed_col_indices);
  static duckdb::virtual_column_map_t BuildVirtualColumns(
    const catalog::Table& table,
    const std::vector<size_t>& indexed_col_indices);
  static duckdb::TableStorageInfo BuildStorageInfo(const catalog::Table& table);
  // Convert a virtual column ID (VIRTUAL_COLUMN_START + i) back to a real
  // column index. Returns DConstants::INVALID_INDEX if not a PK virtual col.
  static duckdb::column_t VirtualToPKColumnIndex(duckdb::column_t virtual_id);

  const std::shared_ptr<catalog::Table>& GetSereneDBTable() const {
    return _sdb_table;
  }

 private:
  std::shared_ptr<catalog::Table> _sdb_table;
  std::vector<size_t> _indexed_col_indices;
};

// Casts `table` to SereneDBTableEntry or throws an ERRCODE_WRONG_OBJECT_TYPE
// error. Use for DML / CREATE INDEX paths that only support base tables so
// that running them on an index entry produces a friendly error instead of a
// reinterpret_cast assertion.
SereneDBTableEntry& RequireBaseTable(duckdb::TableCatalogEntry& table);

}  // namespace sdb::connector
