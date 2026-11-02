////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "pg/catalog/engine/registry.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <array>
#include <deque>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/default/default_functions.hpp>
#include <duckdb/catalog/default/default_table_functions.hpp>
#include <duckdb/catalog/default/default_types.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/optimizer/expression_rewriter.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/parser/statement/select_statement.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <ranges>

#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/functions/functions.h"
#include "pg/catalog/tables/tables.h"
#include "pg/catalog/views/system_views.h"
#include "pg/types/logical.h"
#include "pg/types/mapping.h"

namespace sdb::pg {

#include "pg/catalog/generated/registry.gen.inc"

namespace {

class SystemEmptyScan final : public SystemScan {
 public:
  using SystemScan::SystemScan;

  static constexpr std::tuple<> kSources{};
};

std::vector<SystemTable*> gTables;
std::deque<SystemTable> gEmptyTables;

const duckdb::AclItem kOwnerAcl{.grantee = kRootUser,
                                .grantor = kRootUser,
                                .privs = duckdb::Permissions::AllPrivileges(
                                  duckdb::CatalogType::TABLE_ENTRY)};

using Key = std::pair<std::string_view, std::string_view>;
using FunctionKey =
  std::tuple<std::string_view, std::string_view, duckdb::MacroType>;

irs::containers::FlatHashMap<Key, const SystemTable*> gTablesByName;
irs::containers::FlatHashMap<duckdb::idx_t, const SystemTable*> gTablesByOid;
irs::containers::NodeHashMap<FunctionKey,
                             duckdb::unique_ptr<duckdb::CreateMacroInfo>>
  gFunctions;
std::vector<StaticView> gViews;
irs::containers::FlatHashMap<Key, const StaticView*> gViewsByName;
irs::containers::FlatHashMap<duckdb::idx_t, const StaticView*> gViewsByOid;

std::string_view ViewSql(const SystemView& view) {
  const auto* written =
    absl::c_find_if(kSystemViewSql, [&](const SystemViewSql& sql) {
      return sql.schema == view.schema && sql.name == view.name;
    });
  return written == std::end(kSystemViewSql) ? view.sql : written->sql;
}

void AddSystemView(duckdb::Parser& parser, const SystemView& view) {
  auto info = duckdb::make_uniq<duckdb::CreateViewInfo>();
  info->SetSchema(duckdb::Identifier{view.schema});
  info->SetViewName(duckdb::Identifier{view.name});
  info->sql = ViewSql(view);
  info->temporary = true;
  info->internal = true;
  parser.statements.clear();
  info->query = duckdb::CreateViewInfo::ParseSelect(parser, info->sql);
  const auto& stored = gViews.emplace_back(
    view.schema, view.name, std::move(info),
    SystemPermissions(view.superuser_only), view.oid, view.options);
  gViewsByName.emplace(Key{view.schema, view.name}, &stored);
  gViewsByOid.emplace(view.oid, &stored);
}

duckdb::unique_ptr<duckdb::CreateMacroInfo> ParseMacro(
  duckdb::Parser& parser, const SystemMacro& macro) {
  auto sql =
    absl::StrCat("CREATE FUNCTION ", macro.name, macro.macro_definition);
  parser.statements.clear();
  parser.ParseQuery(sql);
  SDB_ASSERT(parser.statements.size() == 1 &&
             parser.statements[0]->type ==
               duckdb::StatementType::CREATE_STATEMENT);
  auto& create = parser.statements[0]->Cast<duckdb::CreateStatement>();
  SDB_ASSERT(create.info->type == duckdb::CatalogType::MACRO_ENTRY ||
             create.info->type == duckdb::CatalogType::TABLE_MACRO_ENTRY);
  auto info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateMacroInfo>(
      std::move(create.info));
  info->SetSchema(duckdb::Identifier{macro.schema});
  info->temporary = true;
  info->internal = true;
  for (auto& m : info->macros) {
    for (auto* types : {&m->types, &m->return_types}) {
      for (auto& type : *types) {
        if (type.IsUnbound()) {
          type = duckdb::UnboundType::TryDefaultBind(type);
        }
      }
    }
  }
  return info;
}

struct ScalarStub {
  std::string_view name;
  std::array<int32_t, 4> arguments;
  int32_t result;
  duckdb::FunctionStability stability;
};

constexpr ScalarStub kScalarStubs[] = {
#include "pg/catalog/generated/scalar_stubs.gen.inc"
};

constexpr SystemMacro kTableStubs[] = {
#include "pg/catalog/generated/function_stubs.gen.inc"
};

static_assert(absl::c_is_sorted(kScalarStubs,
                                [](const auto& lhs, const auto& rhs) consteval {
                                  return lhs.name < rhs.name;
                                }));

duckdb::unique_ptr<duckdb::Expression> BindZero(
  duckdb::FunctionBindExpressionInput& input) {
  auto zero = duckdb::Value::INTEGER(0).DefaultCastAs(
    input.bound_function.GetReturnType());
  if (input.children.empty()) {
    return duckdb::make_uniq<duckdb::BoundConstantExpression>(std::move(zero));
  }
  return duckdb::ExpressionRewriter::ConstantOrNull(
    input.context, std::move(input.children), std::move(zero));
}

}  // namespace

std::span<const duckdb::DefaultType> ExternalTypes() {
  static const std::vector<duckdb::DefaultType> kTypes = [] {
    std::vector<duckdb::DefaultType> result;
    for (const auto& mapping : PgTypeMappings()) {
      if (mapping.ddl) {
        result.emplace_back(FindBuiltinType(mapping.oid)->name.data(),
                            mapping.logical(), nullptr);
      }
    }
    result.emplace_back(
      "pg_statistic", SystemRowType(kPgStatisticSql).WithAlias("pg_statistic"),
      nullptr);
    result.emplace_back("void", VOID(), nullptr);
    return result;
  }();
  return kTypes;
}

void InitSystemTables() {
  gTables.assign(std::begin(kSystemTables), std::end(kSystemTables));
  for (auto* table : gTables) {
    gTablesByOid.emplace(table->Sql().oid, table);
  }
  for (const auto* sql : kGeneratedTables) {
    if (const auto [it, inserted] = gTablesByOid.try_emplace(sql->oid);
        inserted) {
      auto& table =
        gEmptyTables.emplace_back(std::type_identity<SystemEmptyScan>{}, *sql);
      gTables.emplace_back(&table);
      it->second = &table;
    }
  }
  for (auto* table : gTables) {
    table->Init();
    gTablesByName.emplace(Key{table->Sql().schema, table->Sql().name}, table);
  }
  for (auto* table : kSystemTableFunctions) {
    table->Init();
  }
}

void InitSystemViews(duckdb::Parser& parser) {
  gViews.reserve(std::size(kExternalViews) + std::size(kSdbViews));
  for (const auto& view : kExternalViews) {
    AddSystemView(parser, view);
  }
  for (const auto* view : kSdbViews) {
    AddSystemView(parser, *view);
  }
}

void InitSystemFunctions(duckdb::DatabaseInstance& db, duckdb::Parser& parser) {
  const auto add = [&](const SystemMacro& macro) {
    auto info = ParseMacro(parser, macro);
    const FunctionKey key{macro.schema, macro.name, info->macros[0]->type};
    if (const auto [it, inserted] =
          gFunctions.try_emplace(key, std::move(info));
        !inserted) {
      for (auto& m : info->macros) {
        it->second->macros.emplace_back(std::move(m));
      }
    }
  };
  for (const auto macros : {kPgCatalogMacros, kInformationSchemaMacros}) {
    absl::c_for_each(macros, add);
  }

  irs::containers::FlatHashSet<std::string> defined;
  for (const auto& [key, info] : gFunctions) {
    defined.emplace(std::get<1>(key));
  }
  auto& catalog =
    duckdb::Catalog::GetSystemCatalog(db).Cast<duckdb::DuckCatalog>();
  catalog.ScanSchemas([&](duckdb::SchemaCatalogEntry& schema) {
    const auto collect = [&](duckdb::CatalogEntry& entry) {
      defined.emplace(entry.name.GetIdentifierName());
    };
    schema.Scan(duckdb::CatalogType::SCALAR_FUNCTION_ENTRY, collect);
    schema.Scan(duckdb::CatalogType::TABLE_FUNCTION_ENTRY, collect);
    for (const auto& names :
         {duckdb::DefaultFunctionGenerator{catalog, schema}.GetDefaultEntries(),
          duckdb::DefaultTableFunctionGenerator{catalog, schema}
            .GetDefaultEntries()}) {
      for (const auto& name : names) {
        defined.emplace(name.GetIdentifierName());
      }
    }
  });
  for (const auto& stub : kTableStubs) {
    if (!defined.contains(stub.name)) {
      add(stub);
    }
  }
  duckdb::ExtensionLoader loader{db, "serenedb"};
  for (const auto stubs :
       kScalarStubs |
         std::views::chunk_by([](const auto& lhs, const auto& rhs) {
           return lhs.name == rhs.name;
         })) {
    if (defined.contains(stubs.front().name)) {
      continue;
    }
    duckdb::ScalarFunctionSet set{duckdb::Identifier{stubs.front().name}};
    for (const auto& stub : stubs) {
      duckdb::vector<duckdb::LogicalType> arguments;
      for (const auto argument : stub.arguments) {
        if (argument == 0) {
          break;
        }
        arguments.emplace_back(SystemColumnType(argument));
      }
      auto result = SystemColumnType(stub.result);
      const bool zero = result.IsNumeric();
      duckdb::ScalarFunction function{std::move(arguments), std::move(result),
                                      nullptr};
      function.SetBindExpressionCallback(zero ? BindZero : BindNull);
      set.AddFunction(std::move(function));
    }
    Register(loader, irs::StaticStrings::kPgCatalogSchema,
             stubs.front().stability, std::move(set));
  }
}

const SystemTable* GetSystemTable(std::string_view schema,
                                  std::string_view name) {
  const auto it = gTablesByName.find(Key{schema, name});
  return it == gTablesByName.end() ? nullptr : it->second;
}

const SystemTable* FindSystemTable(duckdb::idx_t oid) {
  const auto it = gTablesByOid.find(oid);
  return it == gTablesByOid.end() ? nullptr : it->second;
}

std::span<SystemTable* const> SystemTableFunctions() {
  return kSystemTableFunctions;
}

const StaticView* GetSystemView(std::string_view schema,
                                std::string_view name) {
  const auto it = gViewsByName.find(Key{schema, name});
  return it == gViewsByName.end() ? nullptr : it->second;
}

const StaticView* FindSystemView(duckdb::idx_t oid) {
  const auto it = gViewsByOid.find(oid);
  return it == gViewsByOid.end() ? nullptr : it->second;
}

duckdb::idx_t SystemRelationOid(std::string_view schema,
                                std::string_view name) {
  if (const auto* table = GetSystemTable(schema, name)) {
    return table->Sql().oid;
  }
  if (const auto* view = GetSystemView(schema, name)) {
    return view->oid;
  }
  return kInvalidOid;
}

const duckdb::CreateMacroInfo* GetSystemFunction(std::string_view schema,
                                                 std::string_view name,
                                                 duckdb::MacroType kind) {
  const auto it = gFunctions.find(FunctionKey{schema, name, kind});
  return it == gFunctions.end() ? nullptr : it->second.get();
}

const duckdb::Permissions& SystemPermissions(bool superuser_only) {
  static const duckdb::Permissions kPublic{.owner = kRootUser,
                                           .acl = {kOwnerAcl, kPublicSelect}};
  static const duckdb::Permissions kSuperuser{.owner = kRootUser,
                                              .acl = {kOwnerAcl}};
  return superuser_only ? kSuperuser : kPublic;
}

const duckdb::Permissions& SchemaPermissions() {
  static const duckdb::Permissions kPermissions{
    .owner = kRootUser,
    .acl = {{.grantee = kRootUser,
             .grantor = kRootUser,
             .privs = duckdb::AclMode::Usage | duckdb::AclMode::Create},
            {.grantee = kPublicGrantee,
             .grantor = kRootUser,
             .privs = duckdb::AclMode::Usage}}};
  return kPermissions;
}

void VisitSystemTables(std::string_view schema,
                       absl::FunctionRef<void(const SystemTable&)> visitor) {
  for (const auto* table : gTables) {
    if (table->Sql().schema == schema) {
      visitor(*table);
    }
  }
}

void VisitSystemViews(std::string_view schema,
                      absl::FunctionRef<void(const StaticView&)> visitor) {
  for (const auto& view : gViews) {
    if (view.schema == schema) {
      visitor(view);
    }
  }
}

void VisitSystemFunctions(
  std::string_view schema, duckdb::MacroType kind,
  absl::FunctionRef<void(std::string_view, const duckdb::CreateMacroInfo&)>
    visitor) {
  for (const auto& [key, function] : gFunctions) {
    const auto& [function_schema, name, function_kind] = key;
    if (function_schema == schema && function_kind == kind) {
      visitor(name, *function);
    }
  }
}

}  // namespace sdb::pg
