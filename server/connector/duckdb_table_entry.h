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
    duckdb::ClientContext& context, duckdb::column_t column_id) override;

  duckdb::TableFunction GetScanFunction(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::FunctionData>& bind_data) override;

  duckdb::TableStorageInfo GetStorageInfo(
    duckdb::ClientContext& context) override;

  duckdb::vector<duckdb::column_t> GetRowIdColumns() const override;
  duckdb::virtual_column_map_t GetVirtualColumns() const override;

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
  static duckdb::TableStorageInfo BuildStorageInfo(
    const catalog::Table& table);

  void BindUpdateConstraints(duckdb::Binder& binder, duckdb::LogicalGet& get,
                             duckdb::LogicalProjection& proj,
                             duckdb::LogicalUpdate& update,
                             duckdb::ClientContext& context) override;

  // Convert a virtual column ID (VIRTUAL_COLUMN_START + i) back to a real
  // column index. Returns DConstants::INVALID_INDEX if not a PK virtual col.
  static duckdb::column_t VirtualToPKColumnIndex(duckdb::column_t virtual_id);

  const std::shared_ptr<catalog::Table>& GetSereneDBTable() const {
    return _sdb_table;
  }

  const std::vector<size_t>& GetIndexedColumnIndices() const {
    return _indexed_col_indices;
  }

 private:
  std::shared_ptr<catalog::Table> _sdb_table;
  std::vector<size_t> _indexed_col_indices;
};

}  // namespace sdb::connector
