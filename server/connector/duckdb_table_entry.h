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

// Convert a SereneDB Velox type to DuckDB LogicalType.
duckdb::LogicalType VeloxTypeToDuckDB(const velox::TypePtr& type);

class SereneDBTableEntry final : public duckdb::TableCatalogEntry {
 public:
  SereneDBTableEntry(duckdb::Catalog& catalog,
                     duckdb::SchemaCatalogEntry& schema,
                     duckdb::CreateTableInfo& info,
                     std::shared_ptr<catalog::Table> sdb_table);

  duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(
    duckdb::ClientContext& context, duckdb::column_t column_id) override;

  duckdb::TableFunction GetScanFunction(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::FunctionData>& bind_data) override;

  duckdb::TableStorageInfo GetStorageInfo(
    duckdb::ClientContext& context) override;

  duckdb::vector<duckdb::column_t> GetRowIdColumns() const override;

  const std::shared_ptr<catalog::Table>& GetSereneDBTable() const {
    return _sdb_table;
  }

 private:
  std::shared_ptr<catalog::Table> _sdb_table;
};

}  // namespace sdb::connector
