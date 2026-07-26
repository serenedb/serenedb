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

#include "connector/duckdb_static_schema.h"

#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>

#include "basics/static_strings.h"
#include "catalog/function.h"
#include "catalog/view.h"
#include "catalog/virtual_table.h"
#include "connector/duckdb_entry_builders.h"
#include "connector/duckdb_system_table_entry.h"
#include "pg/system_catalog.h"

namespace sdb::connector {
namespace {

duckdb::unique_ptr<duckdb::CatalogEntry> MakeSystemTableEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  std::string_view entry_name, const catalog::VirtualTable& table) {
  auto info = duckdb::make_uniq<duckdb::CreateTableInfo>();
  info->SetTableName(duckdb::Identifier{entry_name});
  info->SetSchema(schema.name);
  for (auto& [column_name, column_type] :
       duckdb::StructType::GetChildTypes(table.RowType())) {
    info->columns.AddColumn(duckdb::ColumnDefinition(column_name, column_type));
  }
  return duckdb::make_uniq<SystemTableEntry>(catalog, schema, *info, table);
}

class StaticRelationGenerator final : public duckdb::DefaultGenerator {
 public:
  StaticRelationGenerator(duckdb::Catalog& catalog,
                          duckdb::SchemaCatalogEntry& schema)
    : duckdb::DefaultGenerator{catalog},
      _schema{schema},
      _info_schema{schema.name.GetIdentifierName() ==
                   StaticStrings::kInformationSchema} {}

  duckdb::unique_ptr<duckdb::CatalogEntry> CreateDefaultEntry(
    duckdb::CatalogTransaction /*transaction*/,
    const duckdb::Identifier& entry_name) override {
    const auto name = entry_name.GetIdentifierName();
    if (const auto* table =
          pg::GetSystemTable(_schema.name.GetIdentifierName(), name)) {
      return MakeSystemTableEntry(catalog, _schema, name, *table);
    }
    auto view = _info_schema ? pg::GetInfoSchemaView(name) : pg::GetView(name);
    if (view.first) {
      return MakeViewEntry(catalog, _schema, name, *view.first,
                           std::move(view.second));
    }
    return nullptr;
  }

  duckdb::vector<duckdb::Identifier> GetDefaultEntries() override {
    duckdb::vector<duckdb::Identifier> names;
    const auto add = [&](std::string_view name) { names.emplace_back(name); };
    if (_info_schema) {
      pg::VisitInfoSchemaTables(
        [&](const catalog::VirtualTable& table) { add(table.GetName()); });
      pg::VisitInfoSchemaViews([&](const pg::StaticView& view) {
        add(catalog::ViewName(*view.first));
      });
    } else {
      pg::VisitPgCatalogTables(
        [&](const catalog::VirtualTable& table) { add(table.GetName()); });
      pg::VisitPgCatalogViews([&](const pg::StaticView& view) {
        add(catalog::ViewName(*view.first));
      });
    }
    return names;
  }

 private:
  duckdb::SchemaCatalogEntry& _schema;
  bool _info_schema;
};

class StaticFunctionGenerator final : public duckdb::DefaultGenerator {
 public:
  StaticFunctionGenerator(duckdb::Catalog& catalog,
                          duckdb::SchemaCatalogEntry& schema,
                          bool table_functions)
    : duckdb::DefaultGenerator{catalog},
      _schema{schema},
      _info_schema{schema.name.GetIdentifierName() ==
                   StaticStrings::kInformationSchema},
      _table_functions{table_functions} {}

  duckdb::unique_ptr<duckdb::CatalogEntry> CreateDefaultEntry(
    duckdb::CatalogTransaction /*transaction*/,
    const duckdb::Identifier& entry_name) override {
    const auto name = entry_name.GetIdentifierName();
    auto function = _info_schema ? pg::GetInfoSchemaFunction(name)
                                 : pg::GetPgCatalogFunction(name);
    if (!function.first || !Belongs(*function.first)) {
      return nullptr;
    }
    return MakeMacroEntry(catalog, _schema, name, /*internal=*/true,
                          *function.first, std::move(function.second));
  }

  duckdb::vector<duckdb::Identifier> GetDefaultEntries() override {
    duckdb::vector<duckdb::Identifier> names;
    const auto add = [&](const pg::StaticFunction& function) {
      if (Belongs(*function.first)) {
        names.emplace_back(
          function.first->GetFunctionName().GetIdentifierName());
      }
    };
    if (_info_schema) {
      pg::VisitInfoSchemaFunctions(add);
    } else {
      pg::VisitPgCatalogFunctions(add);
    }
    return names;
  }

 private:
  // A table macro and a scalar macro are one SereneDB kind and two duckdb
  // sets; only the info says which set the entry belongs in.
  bool Belongs(const duckdb::CreateMacroInfo& function) const {
    return (function.type == duckdb::CatalogType::TABLE_MACRO_ENTRY) ==
           _table_functions;
  }

  duckdb::SchemaCatalogEntry& _schema;
  bool _info_schema;
  bool _table_functions;
};

}  // namespace

bool IsStaticSchema(std::string_view schema_name) noexcept {
  return schema_name == StaticStrings::kPgCatalogSchema ||
         schema_name == StaticStrings::kInformationSchema;
}

duckdb::unique_ptr<duckdb::DefaultGenerator> MakeStaticRelationGenerator(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema) {
  if (!IsStaticSchema(schema.name.GetIdentifierName())) {
    return nullptr;
  }
  return duckdb::make_uniq_base<duckdb::DefaultGenerator,
                                StaticRelationGenerator>(catalog, schema);
}

duckdb::unique_ptr<duckdb::DefaultGenerator> MakeStaticFunctionGenerator(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  bool table_functions) {
  if (!IsStaticSchema(schema.name.GetIdentifierName())) {
    return nullptr;
  }
  return duckdb::make_uniq_base<duckdb::DefaultGenerator,
                                StaticFunctionGenerator>(catalog, schema,
                                                         table_functions);
}

}  // namespace sdb::connector
