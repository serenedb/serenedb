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

#include <deque>
#include <duckdb/catalog/default/default_types.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/parser/statement/select_statement.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <iresearch/utils/static_strings.hpp>

#include "connector/pg_logical_types.h"
#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/functions/macros_sql.h"
#include "pg/catalog/lookup.h"
#include "pg/catalog/tables/tables.h"
#include "pg/catalog/views/system_views.h"
#include "pg/types.h"

namespace sdb::pg {

extern SystemTable gInfoColumns;
extern SystemTable gInfoSqlFeatures;
extern SystemTable gInfoSqlImplementationInfo;
extern SystemTable gInfoSqlParts;
extern SystemTable gInfoSqlSizing;
extern SystemTable gPgAggregate;
extern SystemTable gPgAm;
extern SystemTable gPgAttrdef;
extern SystemTable gPgAttribute;
extern SystemTable gPgAuthMembers;
extern SystemTable gPgAuthid;
extern SystemTable gPgClass;
extern SystemTable gPgCollation;
extern SystemTable gPgConstraint;
extern SystemTable gPgDatabase;
extern SystemTable gPgDbRoleSetting;
extern SystemTable gPgDefaultAcl;
extern SystemTable gPgDepend;
extern SystemTable gPgDescription;
extern SystemTable gPgEnum;
extern SystemTable gPgForeignServer;
extern SystemTable gPgHbaFileRules;
extern SystemTable gPgIndex;
extern SystemTable gPgLanguage;
extern SystemTable gPgNamespace;
extern SystemTable gPgOpclass;
extern SystemTable gPgProc;
extern SystemTable gPgRewrite;
extern SystemTable gPgSequence;
extern SystemTable gPgSettings;
extern SystemTable gPgShdepend;
extern SystemTable gPgTablespace;
extern SystemTable gPgTrigger;
extern SystemTable gPgTsDict;
extern SystemTable gPgType;
extern SystemTable gSdbMetrics;
extern SystemTable gSdbProgress;
extern SystemTable gSdbSettings;

namespace {

std::vector<SystemTable*> gTables;
std::deque<SystemTable> gEmptyTables;

SystemTable* const kSystemTables[] = {
  &gInfoColumns,
  &gInfoSqlFeatures,
  &gInfoSqlImplementationInfo,
  &gInfoSqlParts,
  &gInfoSqlSizing,
  &gPgAggregate,
  &gPgAm,
  &gPgAttrdef,
  &gPgAttribute,
  &gPgAuthMembers,
  &gPgAuthid,
  &gPgClass,
  &gPgCollation,
  &gPgConstraint,
  &gPgDatabase,
  &gPgDbRoleSetting,
  &gPgDefaultAcl,
  &gPgDepend,
  &gPgDescription,
  &gPgEnum,
  &gPgForeignServer,
  &gPgHbaFileRules,
  &gPgIndex,
  &gPgLanguage,
  &gPgNamespace,
  &gPgOpclass,
  &gPgProc,
  &gPgRewrite,
  &gPgSequence,
  &gPgSettings,
  &gPgShdepend,
  &gPgTablespace,
  &gPgTrigger,
  &gPgTsDict,
  &gPgType,
  &gSdbMetrics,
  &gSdbProgress,
  &gSdbSettings,
};

constexpr duckdb::AclItem kOwnerAcl{
  .grantee = kRootUser,
  .grantor = kRootUser,
  .privs = duckdb::AclMode::Insert | duckdb::AclMode::Select |
           duckdb::AclMode::Update | duckdb::AclMode::Delete |
           duckdb::AclMode::Truncate | duckdb::AclMode::References |
           duckdb::AclMode::Trigger | duckdb::AclMode::Maintain};

constexpr duckdb::AclItem kPublicSelect{.grantee = kPublicGrantee,
                                        .grantor = kRootUser,
                                        .privs = duckdb::AclMode::Select};

using Key = std::pair<std::string_view, std::string_view>;
using FunctionKey =
  std::tuple<std::string_view, std::string_view, duckdb::MacroType>;

irs::containers::FlatHashMap<Key, const SystemTable*> gTablesByName;
irs::containers::FlatHashMap<duckdb::idx_t, const SystemTable*> gTablesByOid;
irs::containers::NodeHashMap<FunctionKey, StaticFunction> gFunctions;
std::vector<StaticView> gViews;
irs::containers::FlatHashMap<Key, const StaticView*> gViewsByName;
irs::containers::FlatHashMap<duckdb::idx_t, const StaticView*> gViewsByOid;
irs::containers::FlatHashSet<Key> gRelationNames;

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
    for (auto& type : m->types) {
      if (type.IsUnbound()) {
        type = duckdb::UnboundType::TryDefaultBind(type);
      }
    }
  }
  return info;
}

std::vector<duckdb::DefaultType>& ExternalTypeList() {
  static std::vector<duckdb::DefaultType> gTypes = [] {
    std::vector<duckdb::DefaultType> result;
    for (const auto& mapping : PgTypeMappings()) {
      if (mapping.ddl) {
        result.emplace_back(FindBuiltinType(mapping.oid)->name.data(),
                            mapping.logical(), nullptr);
      }
    }
    result.emplace_back("pg_statistic", duckdb::LogicalType::INVALID, nullptr);
    result.emplace_back("serial", SERIAL(), nullptr);
    result.emplace_back("bigserial", BIGSERIAL(), nullptr);
    result.emplace_back("smallserial", SMALLSERIAL(), nullptr);
    return result;
  }();
  return gTypes;
}

}  // namespace

std::span<const duckdb::DefaultType> ExternalTypes() {
  return ExternalTypeList();
}

void InitSystemTables() {
  gTables.assign(std::begin(kSystemTables), std::end(kSystemTables));
  for (auto* table : gTables) {
    gTablesByOid.emplace(table->Sql().oid, table);
  }
  for (const auto* sql : kGeneratedTables) {
    if (!gTablesByOid.contains(sql->oid)) {
      auto* table =
        &gEmptyTables.emplace_back(*sql, std::span<const SystemCell>{});
      gTables.emplace_back(table);
      gTablesByOid.emplace(sql->oid, table);
    }
  }
  for (auto* table : gTables) {
    table->Init();
    gTablesByName.emplace(Key{table->Sql().schema, table->Sql().name}, table);
    gRelationNames.emplace(table->Sql().schema, table->Sql().name);
  }
  for (auto& type : ExternalTypeList()) {
    if (std::string_view{type.name} == kPgStatisticSql.name) {
      type.type = SystemRowType(kPgStatisticSql).WithAlias(type.name);
    }
  }
}

void InitSystemViews(duckdb::Parser& parser) {
  gViews.reserve(std::size(kExternalViews));
  for (const auto& view : kExternalViews) {
    auto info = duckdb::make_uniq<duckdb::CreateViewInfo>();
    info->SetSchema(duckdb::Identifier{view.schema});
    info->SetViewName(duckdb::Identifier{view.name});
    info->sql = view.sql;
    info->temporary = true;
    info->internal = true;
    parser.statements.clear();
    info->query = duckdb::CreateViewInfo::ParseSelect(parser, info->sql);
    const auto& stored = gViews.emplace_back(StaticView{
      .schema = view.schema,
      .name = view.name,
      .info = std::shared_ptr<const duckdb::CreateViewInfo>{info.release()},
      .permissions = SystemPermissions(view.superuser_only),
      .oid = view.oid,
      .binding = std::make_shared<ViewBinding>(),
    });
    gViewsByName.emplace(Key{view.schema, view.name}, &stored);
    gViewsByOid.emplace(view.oid, &stored);
    gRelationNames.emplace(view.schema, view.name);
  }
}

void InitSystemFunctions(duckdb::Parser& parser) {
  irs::containers::NodeHashMap<FunctionKey,
                               duckdb::unique_ptr<duckdb::CreateMacroInfo>>
    built;
  for (const auto& macro : kExternalMacros) {
    auto info = ParseMacro(parser, macro);
    FunctionKey key{macro.schema, macro.name, info->macros[0]->type};
    const auto it = built.find(key);
    if (it == built.end()) {
      built.emplace(key, std::move(info));
      continue;
    }
    auto& existing = *it->second;
    for (auto& m : info->macros) {
      existing.macros.emplace_back(std::move(m));
    }
    if (existing.type == duckdb::CatalogType::MACRO_ENTRY) {
      existing.type = info->type;
    }
  }
  for (auto& [key, info] : built) {
    gFunctions.emplace(key, StaticFunction{info.release()});
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

const StaticView* GetSystemView(std::string_view schema,
                                std::string_view name) {
  const auto it = gViewsByName.find(Key{schema, name});
  return it == gViewsByName.end() ? nullptr : it->second;
}

const StaticView* FindSystemView(duckdb::idx_t oid) {
  const auto it = gViewsByOid.find(oid);
  return it == gViewsByOid.end() ? nullptr : it->second;
}

bool IsSystemRelation(std::string_view schema, std::string_view name) {
  return gRelationNames.contains(Key{schema, name});
}

StaticFunction GetSystemFunction(std::string_view schema, std::string_view name,
                                 duckdb::MacroType kind) {
  const auto it = gFunctions.find(FunctionKey{schema, name, kind});
  return it == gFunctions.end() ? StaticFunction{} : it->second;
}

duckdb::Permissions SystemPermissions(bool superuser_only) {
  duckdb::Permissions permissions{.owner = kRootUser};
  permissions.acl.emplace_back(kOwnerAcl);
  if (!superuser_only) {
    permissions.acl.emplace_back(kPublicSelect);
  }
  return permissions;
}

duckdb::Permissions SchemaPermissions() {
  return {.owner = kRootUser,
          .acl = {{.grantee = kRootUser,
                   .grantor = kRootUser,
                   .privs = duckdb::AclMode::Usage | duckdb::AclMode::Create},
                  {.grantee = kPublicGrantee,
                   .grantor = kRootUser,
                   .privs = duckdb::AclMode::Usage}}};
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
