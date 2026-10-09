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
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/storage/table_storage_info.hpp>
#include <memory>

#include "pg/catalog/engine/registry.h"

namespace duckdb {

struct CreateTableInfo;

}  // namespace duckdb
namespace sdb::pg {

class SystemTable;

}  // namespace sdb::pg
namespace sdb::catalog {

class SereneDBCatalog;

class SystemTableEntry final : public duckdb::TableCatalogEntry {
 public:
  SystemTableEntry(duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
                   duckdb::CreateTableInfo& info, const pg::SystemTable& table);

  const duckdb::ColumnList& GetColumns() const final;

  duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(
    duckdb::ClientContext&, duckdb::column_t) final {
    return nullptr;
  }

  duckdb::TableFunction GetScanFunction(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::FunctionData>& bind_data) final;

  duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext&) final {
    return {};
  }

  duckdb::virtual_column_map_t GetVirtualColumns() const final;

  duckdb::Catalog& GetStorageCatalog(duckdb::ClientContext& context) final;

  const pg::SystemTable& Table() const noexcept { return _table; }

 private:
  const pg::SystemTable& _table;
};

class SystemViewEntry final : public duckdb::ViewCatalogEntry {
 public:
  SystemViewEntry(duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
                  duckdb::CreateViewInfo& info,
                  std::shared_ptr<pg::ViewBinding> binding);

  duckdb::shared_ptr<duckdb::ViewColumnInfo> GetColumnInfo() const final;

  void BindView(duckdb::ClientContext& context,
                duckdb::BindViewAction action) final;

  void UpdateBinding(const duckdb::vector<duckdb::LogicalType>& types,
                     const duckdb::vector<duckdb::Identifier>& names) final;

 private:
  std::shared_ptr<pg::ViewBinding> _binding;
};

void MountSystemSchemas(SereneDBCatalog& catalog);

}  // namespace sdb::catalog
