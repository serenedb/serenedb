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

#include "catalog/entry/system_table.h"

#include <duckdb/catalog/catalog_entry/duck_schema_entry.hpp>
#include <duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/default/default_generator.hpp>
#include <duckdb/catalog/default/default_schemas.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>

#include "catalog/catalog.h"
#include "connector/column_id.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/engine/scan_function.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/oids.h"

namespace sdb::catalog {
namespace {

duckdb::unique_ptr<duckdb::CatalogEntry> MakeTable(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  const pg::SystemTable& table) {
  duckdb::CreateTableInfo info{schema, duckdb::Identifier{table.Sql().name}};
  info.oid = table.Sql().oid;
  for (duckdb::idx_t i = 0; i < table.Sql().columns.size(); ++i) {
    if (table.Sql().columns[i].not_null) {
      info.constraints.emplace_back(
        duckdb::make_uniq<duckdb::NotNullConstraint>(duckdb::LogicalIndex{i}));
    }
  }
  return duckdb::make_uniq<SystemTableEntry>(catalog, schema, info, table);
}

duckdb::unique_ptr<duckdb::CatalogEntry> MakeView(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  const pg::StaticView* view) {
  if (!view) {
    return nullptr;
  }
  auto info = view->info->Copy();
  info->oid = view->oid;
  auto entry = duckdb::make_uniq<SystemViewEntry>(
    catalog, schema, info->Cast<duckdb::CreateViewInfo>(), view->binding);
  entry->permissions = view->permissions;
  return entry;
}

duckdb::unique_ptr<duckdb::CatalogEntry> MakeMacro(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  const pg::StaticFunction& function, duckdb::MacroType kind) {
  if (!function) {
    return nullptr;
  }
  auto copy = function->Copy();
  auto& macro_info = copy->Cast<duckdb::CreateMacroInfo>();
  if (kind == duckdb::MacroType::SCALAR_MACRO) {
    return duckdb::make_uniq<duckdb::ScalarMacroCatalogEntry>(catalog, schema,
                                                              macro_info);
  }
  return duckdb::make_uniq<duckdb::TableMacroCatalogEntry>(catalog, schema,
                                                           macro_info);
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
    duckdb::CatalogTransaction, const duckdb::Identifier& name) final {
    const auto& schema = _schema.name.GetIdentifierName();
    const auto& entry = name.GetIdentifierName();
    if (_set != duckdb::CatalogType::TABLE_ENTRY) {
      const auto kind = MacroKindOf(_set);
      return MakeMacro(catalog, _schema,
                       pg::GetSystemFunction(schema, entry, kind), kind);
    }
    if (const auto* table = pg::GetSystemTable(schema, entry)) {
      return MakeTable(catalog, _schema, *table);
    }
    return MakeView(catalog, _schema, pg::GetSystemView(schema, entry));
  }

  duckdb::vector<duckdb::Identifier> GetDefaultEntries() final {
    duckdb::vector<duckdb::Identifier> names;
    const auto& schema = _schema.name.GetIdentifierName();
    if (_set != duckdb::CatalogType::TABLE_ENTRY) {
      pg::VisitSystemFunctions(
        schema, MacroKindOf(_set),
        [&](std::string_view function, const duckdb::CreateMacroInfo&) {
          names.emplace_back(function);
        });
      return names;
    }
    pg::VisitSystemTables(schema, [&](const pg::SystemTable& table) {
      names.emplace_back(table.Sql().name);
    });
    pg::VisitSystemViews(schema, [&](const pg::StaticView& view) {
      names.emplace_back(view.name);
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
    duckdb::CatalogTransaction, const duckdb::Identifier& name) final {
    if (!duckdb::DefaultSchemaGenerator::IsDefaultSchema(name)) {
      return nullptr;
    }
    duckdb::CreateSchemaInfo info;
    info.SetQualifiedName(duckdb::QualifiedName({name}, duckdb::Identifier()));
    info.internal = true;
    info.oid = name == duckdb::Identifier{irs::StaticStrings::kPgCatalogSchema}
                 ? pg::kPgCatalogSchema
                 : pg::kPgInformationSchema;
    info.permissions = pg::SchemaPermissions();
    auto schema = duckdb::make_uniq<duckdb::DuckSchemaEntry>(catalog, info);
    for (const auto set :
         {duckdb::CatalogType::TABLE_ENTRY, duckdb::CatalogType::MACRO_ENTRY,
          duckdb::CatalogType::TABLE_MACRO_ENTRY}) {
      schema->GetCatalogSet(set).SetDefaultGenerator(
        duckdb::make_uniq<SystemEntryGenerator>(catalog, *schema, set));
    }
    return schema;
  }

  duckdb::vector<duckdb::Identifier> GetDefaultEntries() final {
    return {duckdb::Identifier{irs::StaticStrings::kPgCatalogSchema},
            duckdb::Identifier{irs::StaticStrings::kInformationSchema}};
  }
};

}  // namespace

SystemTableEntry::SystemTableEntry(duckdb::Catalog& catalog,
                                   duckdb::SchemaCatalogEntry& schema,
                                   duckdb::CreateTableInfo& info,
                                   const pg::SystemTable& table)
  : duckdb::TableCatalogEntry{catalog, schema, info}, _table{table} {
  internal = true;
  permissions = pg::SystemPermissions(table.Sql().superuser_only);
}

const duckdb::ColumnList& SystemTableEntry::GetColumns() const {
  return _table.Columns();
}

duckdb::TableFunction SystemTableEntry::GetScanFunction(
  duckdb::ClientContext&, duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  return connector::BindSystemTableScan(*this, bind_data);
}

duckdb::virtual_column_map_t SystemTableEntry::GetVirtualColumns() const {
  duckdb::virtual_column_map_t result;
  result.insert({connector::kColumnIdentifierTableOid,
                 duckdb::TableColumn{duckdb::Identifier{"tableoid"},
                                     duckdb::LogicalType::BIGINT}});
  return result;
}

void RefuseSystemCatalog(const duckdb::CatalogEntry& relation) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
    ERR_MSG("permission denied: \"", relation.name.GetIdentifierName(),
            "\" is a system catalog"));
}

duckdb::Catalog& SystemTableEntry::GetStorageCatalog(duckdb::ClientContext&) {
  RefuseSystemCatalog(*this);
}

SystemViewEntry::SystemViewEntry(duckdb::Catalog& catalog,
                                 duckdb::SchemaCatalogEntry& schema,
                                 duckdb::CreateViewInfo& info,
                                 std::shared_ptr<pg::ViewBinding> binding)
  : duckdb::ViewCatalogEntry{catalog, schema, info},
    _binding{std::move(binding)} {}

duckdb::shared_ptr<duckdb::ViewColumnInfo> SystemViewEntry::GetColumnInfo()
  const {
  return _binding->columns.atomic_load();
}

void SystemViewEntry::BindView(duckdb::ClientContext& context,
                               duckdb::BindViewAction action) {
  if (action == duckdb::BindViewAction::BIND_IF_UNBOUND && GetColumnInfo()) {
    return;
  }
  duckdb::ViewCatalogEntry::BindView(context, action);
  _binding->columns.atomic_store(duckdb::ViewCatalogEntry::GetColumnInfo());
}

void SystemViewEntry::UpdateBinding(
  const duckdb::vector<duckdb::LogicalType>& types,
  const duckdb::vector<duckdb::Identifier>& names) {
  const auto columns = GetColumnInfo();
  if (columns && columns->types == types && columns->names == names) {
    return;
  }
  duckdb::ViewCatalogEntry::UpdateBinding(types, names);
  _binding->columns.atomic_store(duckdb::ViewCatalogEntry::GetColumnInfo());
}

void MountSystemSchemas(SereneDBCatalog& catalog) {
  catalog.GetSchemaCatalogSet().SetDefaultGenerator(
    duckdb::make_uniq<SystemSchemaGenerator>(catalog));
}

}  // namespace sdb::catalog
