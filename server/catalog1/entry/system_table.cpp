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

#include "catalog1/entry/system_table.h"

#include <duckdb/catalog/catalog_entry/duck_schema_entry.hpp>
#include <duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/default/default_generator.hpp>
#include <duckdb/catalog/default/default_schemas.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/storage/table_storage_info.hpp>

#include "basics/static_strings.h"
#include "catalog1/catalog.h"
#include "connector/column_id.h"
#include "connector/duckdb_table_function.h"
#include "pg/pg_types.h"
#include "pg/system_catalog.h"
#include "pg/virtual_table.h"

namespace sdb::catalog {
namespace {

duckdb::unique_ptr<duckdb::CatalogEntry> MakeTable(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  const pg::VirtualTable& table) {
  duckdb::CreateTableInfo info{schema, duckdb::Identifier{table.GetName()}};
  for (const auto& [name, type] :
       duckdb::StructType::GetChildTypes(table.RowType())) {
    info.columns.AddColumn(duckdb::ColumnDefinition{name, type});
  }
  return duckdb::make_uniq<SystemTableEntry>(catalog, schema, info, table);
}

duckdb::unique_ptr<duckdb::CatalogEntry> MakeView(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  const pg::StaticView& view) {
  if (!view.info) {
    return nullptr;
  }
  auto info = view.info->Copy();
  auto entry = duckdb::make_uniq<duckdb::ViewCatalogEntry>(
    catalog, schema, info->Cast<duckdb::CreateViewInfo>());
  entry->permissions = view.permissions;
  return entry;
}

duckdb::unique_ptr<duckdb::CatalogEntry> MakeMacro(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  const pg::StaticFunction& function, duckdb::MacroType kind) {
  const auto& [info, permissions] = function;
  if (!info || info->macros[0]->type != kind) {
    return nullptr;
  }
  auto copy = info->Copy();
  auto& macro_info = copy->Cast<duckdb::CreateMacroInfo>();
  duckdb::unique_ptr<duckdb::CatalogEntry> entry;
  if (kind == duckdb::MacroType::SCALAR_MACRO) {
    entry = duckdb::make_uniq<duckdb::ScalarMacroCatalogEntry>(catalog, schema,
                                                               macro_info);
  } else {
    entry = duckdb::make_uniq<duckdb::TableMacroCatalogEntry>(catalog, schema,
                                                              macro_info);
  }
  entry->permissions = permissions;
  return entry;
}

duckdb::MacroType MacroKindOf(duckdb::CatalogType set) noexcept {
  return set == duckdb::CatalogType::MACRO_ENTRY
           ? duckdb::MacroType::SCALAR_MACRO
           : duckdb::MacroType::TABLE_MACRO;
}

class SystemEntryGenerator final : public duckdb::DefaultGenerator {
 public:
  SystemEntryGenerator(duckdb::Catalog& catalog,
                       duckdb::SchemaCatalogEntry& schema,
                       duckdb::CatalogType set)
    : DefaultGenerator{catalog}, _schema{schema}, _set{set} {}

  duckdb::unique_ptr<duckdb::CatalogEntry> CreateDefaultEntry(
    duckdb::CatalogTransaction, const duckdb::Identifier& name) override {
    const auto& schema = _schema.name.GetIdentifierName();
    const auto& entry = name.GetIdentifierName();
    if (_set != duckdb::CatalogType::TABLE_ENTRY) {
      return MakeMacro(catalog, _schema, pg::GetSystemFunction(schema, entry),
                       MacroKindOf(_set));
    }
    if (const auto* table = pg::GetSystemTable(schema, entry)) {
      return MakeTable(catalog, _schema, *table);
    }
    return MakeView(catalog, _schema, pg::GetSystemView(schema, entry));
  }

  duckdb::vector<duckdb::Identifier> GetDefaultEntries() override {
    duckdb::vector<duckdb::Identifier> names;
    const auto& schema = _schema.name.GetIdentifierName();
    if (_set != duckdb::CatalogType::TABLE_ENTRY) {
      const auto kind = MacroKindOf(_set);
      pg::VisitSystemFunctions(schema, [&](const pg::StaticFunction& function) {
        if (function.first->macros[0]->type == kind) {
          names.push_back(function.first->GetFunctionName());
        }
      });
      return names;
    }
    pg::VisitSystemTables(schema, [&](const pg::VirtualTable& table) {
      names.emplace_back(table.GetName());
    });
    pg::VisitSystemViews(schema, [&](const pg::StaticView& view) {
      names.push_back(view.info->GetViewName());
    });
    return names;
  }

 private:
  duckdb::SchemaCatalogEntry& _schema;
  duckdb::CatalogType _set;
};

class SystemSchemaGenerator final : public duckdb::DefaultGenerator {
 public:
  using DefaultGenerator::DefaultGenerator;

  duckdb::unique_ptr<duckdb::CatalogEntry> CreateDefaultEntry(
    duckdb::CatalogTransaction, const duckdb::Identifier& name) override {
    if (!duckdb::DefaultSchemaGenerator::IsDefaultSchema(name)) {
      return nullptr;
    }
    duckdb::CreateSchemaInfo info;
    info.SetQualifiedName(duckdb::QualifiedName({name}, duckdb::Identifier()));
    info.internal = true;
    auto schema = duckdb::make_uniq<duckdb::DuckSchemaEntry>(catalog, info);
    for (const auto set :
         {duckdb::CatalogType::TABLE_ENTRY, duckdb::CatalogType::MACRO_ENTRY,
          duckdb::CatalogType::TABLE_MACRO_ENTRY}) {
      schema->GetCatalogSet(set).SetDefaultGenerator(
        duckdb::make_uniq<SystemEntryGenerator>(catalog, *schema, set));
    }
    return schema;
  }

  duckdb::vector<duckdb::Identifier> GetDefaultEntries() override {
    return {duckdb::Identifier{StaticStrings::kPgCatalogSchema},
            duckdb::Identifier{StaticStrings::kInformationSchema}};
  }
};

}  // namespace

SystemTableEntry::SystemTableEntry(duckdb::Catalog& catalog,
                                   duckdb::SchemaCatalogEntry& schema,
                                   duckdb::CreateTableInfo& info,
                                   const pg::VirtualTable& table)
  : duckdb::TableCatalogEntry{catalog, schema, info}, _table{table} {
  internal = true;
  permissions.owner = pg::kRootUser;
  const auto acl = table.GetAcl();
  permissions.acl.assign(acl.begin(), acl.end());
}

duckdb::unique_ptr<duckdb::BaseStatistics> SystemTableEntry::GetStatistics(
  duckdb::ClientContext&, duckdb::column_t) {
  return nullptr;
}

duckdb::TableFunction SystemTableEntry::GetScanFunction(
  duckdb::ClientContext&, duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  return connector::BindSystemTableScan(*this, bind_data);
}

duckdb::TableStorageInfo SystemTableEntry::GetStorageInfo(
  duckdb::ClientContext&) {
  return {};
}

duckdb::virtual_column_map_t SystemTableEntry::GetVirtualColumns() const {
  duckdb::virtual_column_map_t result;
  result.insert({connector::kColumnIdentifierTableOid,
                 duckdb::TableColumn{duckdb::Identifier{"tableoid"},
                                     duckdb::LogicalType::BIGINT}});
  return result;
}

void MountSystemSchemas(SereneDBCatalog& catalog) {
  catalog.GetSchemaCatalogSet().SetDefaultGenerator(
    duckdb::make_uniq<SystemSchemaGenerator>(catalog));
}

}  // namespace sdb::catalog
