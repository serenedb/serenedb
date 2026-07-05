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

#include "catalog/object.h"
#include "catalog/virtual_table.h"

namespace sdb::connector {

class SystemRelationObject final : public catalog::Object {
 public:
  SystemRelationObject(ObjectId id, std::string_view name, catalog::Acl acl)
    : Object{catalog::Permissions{id::kRootUser, std::move(acl)}, ObjectId{},
             id, std::string{name}, catalog::ObjectType::Table} {}

  void Serialize(duckdb::Serializer&) const final {}
  std::shared_ptr<catalog::Object> Clone() const final { return nullptr; }
};

// DuckDB table entry for SereneDB system tables (pg_catalog,
// information_schema). Wraps a VirtualTable and provides a scan function that
// reads data from VirtualTableSnapshot::GetData().
class SystemTableEntry final : public duckdb::TableCatalogEntry {
 public:
  SystemTableEntry(duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
                   duckdb::CreateTableInfo& info,
                   const catalog::VirtualTable& virtual_table);

  duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(
    duckdb::ClientContext& context, duckdb::column_t column_id) final;

  duckdb::TableFunction GetScanFunction(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::FunctionData>& bind_data) final;

  duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext& context) final;

  duckdb::virtual_column_map_t GetVirtualColumns() const final;

  std::shared_ptr<catalog::VirtualTableSnapshot> CreateSnapshot(
    duckdb::ClientContext& context);

  const catalog::Object& GetSystemObject() const { return _system_object; }

 private:
  const catalog::VirtualTable& _virtual_table;
  SystemRelationObject _system_object;
};

}  // namespace sdb::connector
