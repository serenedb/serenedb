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

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>

namespace duckdb {

struct CreateTableInfo;

}  // namespace duckdb
namespace sdb::pg {

class VirtualTable;

}  // namespace sdb::pg
namespace sdb::catalog {

class SereneDBCatalog;

class SystemTableEntry final : public duckdb::TableCatalogEntry {
 public:
  SystemTableEntry(duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
                   duckdb::CreateTableInfo& info,
                   const pg::VirtualTable& table);

  duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(
    duckdb::ClientContext& context, duckdb::column_t column_id) override;

  duckdb::TableFunction GetScanFunction(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::FunctionData>& bind_data) override;

  duckdb::TableStorageInfo GetStorageInfo(
    duckdb::ClientContext& context) override;

  duckdb::virtual_column_map_t GetVirtualColumns() const override;

  const pg::VirtualTable& Table() const noexcept { return _table; }

 private:
  const pg::VirtualTable& _table;
};

void MountSystemSchemas(SereneDBCatalog& catalog);

}  // namespace sdb::catalog
