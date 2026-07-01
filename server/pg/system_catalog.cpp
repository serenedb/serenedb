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

#include "pg/system_catalog.h"

#include <absl/strings/str_cat.h>

#include <boost/pfr.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/parser/statement/select_statement.hpp>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/serializer.h"
#include "basics/static_strings.h"
#include "catalog/function.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/view.h"
#include "pg/information_schema/sql_features.h"
#include "pg/information_schema/sql_implementation_info.h"
#include "pg/information_schema/sql_parts.h"
#include "pg/information_schema/sql_sizing.h"
#include "pg/pg_catalog/pg_aggregate.h"
#include "pg/pg_catalog/pg_am.h"
#include "pg/pg_catalog/pg_amop.h"
#include "pg/pg_catalog/pg_amproc.h"
#include "pg/pg_catalog/pg_attrdef.h"
#include "pg/pg_catalog/pg_attribute.h"
#include "pg/pg_catalog/pg_auth_members.h"
#include "pg/pg_catalog/pg_authid.h"
#include "pg/pg_catalog/pg_cast.h"
#include "pg/pg_catalog/pg_class.h"
#include "pg/pg_catalog/pg_collation.h"
#include "pg/pg_catalog/pg_constraint.h"
#include "pg/pg_catalog/pg_conversion.h"
#include "pg/pg_catalog/pg_database.h"
#include "pg/pg_catalog/pg_db_role_setting.h"
#include "pg/pg_catalog/pg_default_acl.h"
#include "pg/pg_catalog/pg_depend.h"
#include "pg/pg_catalog/pg_description.h"
#include "pg/pg_catalog/pg_enum.h"
#include "pg/pg_catalog/pg_event_trigger.h"
#include "pg/pg_catalog/pg_extension.h"
#include "pg/pg_catalog/pg_foreign_data_wrapper.h"
#include "pg/pg_catalog/pg_foreign_server.h"
#include "pg/pg_catalog/pg_foreign_table.h"
#include "pg/pg_catalog/pg_index.h"
#include "pg/pg_catalog/pg_inherits.h"
#include "pg/pg_catalog/pg_init_privs.h"
#include "pg/pg_catalog/pg_language.h"
#include "pg/pg_catalog/pg_largeobject.h"
#include "pg/pg_catalog/pg_largeobject_metadata.h"
#include "pg/pg_catalog/pg_namespace.h"
#include "pg/pg_catalog/pg_opclass.h"
#include "pg/pg_catalog/pg_operator.h"
#include "pg/pg_catalog/pg_opfamily.h"
#include "pg/pg_catalog/pg_parameter_acl.h"
#include "pg/pg_catalog/pg_partitioned_table.h"
#include "pg/pg_catalog/pg_policy.h"
#include "pg/pg_catalog/pg_proc.h"
#include "pg/pg_catalog/pg_publication.h"
#include "pg/pg_catalog/pg_publication_namespace.h"
#include "pg/pg_catalog/pg_publication_rel.h"
#include "pg/pg_catalog/pg_range.h"
#include "pg/pg_catalog/pg_replication_origin.h"
#include "pg/pg_catalog/pg_rewrite.h"
#include "pg/pg_catalog/pg_seclabel.h"
#include "pg/pg_catalog/pg_sequence.h"
#include "pg/pg_catalog/pg_shdepend.h"
#include "pg/pg_catalog/pg_shdescription.h"
#include "pg/pg_catalog/pg_shseclabel.h"
#include "pg/pg_catalog/pg_stat_progress.h"
#include "pg/pg_catalog/pg_statistic.h"
#include "pg/pg_catalog/pg_statistic_ext.h"
#include "pg/pg_catalog/pg_statistic_ext_data.h"
#include "pg/pg_catalog/pg_subscription.h"
#include "pg/pg_catalog/pg_subscription_rel.h"
#include "pg/pg_catalog/pg_tablespace.h"
#include "pg/pg_catalog/pg_transform.h"
#include "pg/pg_catalog/pg_trigger.h"
#include "pg/pg_catalog/pg_ts_config.h"
#include "pg/pg_catalog/pg_ts_config_map.h"
#include "pg/pg_catalog/pg_ts_dict.h"
#include "pg/pg_catalog/pg_ts_parser.h"
#include "pg/pg_catalog/pg_ts_template.h"
#include "pg/pg_catalog/pg_type.h"
#include "pg/pg_catalog/pg_user_mapping.h"
#include "pg/sdb_catalog/sdb_metrics.h"
#include "pg/sdb_catalog/sdb_settings.h"
#include "pg/system_functions.h"
#include "pg/system_table.h"
#include "pg/system_views.h"

namespace sdb::pg {
namespace {

using namespace catalog;

struct HashEq {
  using is_transparent = void;

  size_t operator()(std::string_view str) const { return absl::HashOf(str); }

  size_t operator()(const VirtualTable* table) const {
    return (*this)(table->GetName());
  }

  bool operator()(const VirtualTable* table, std::string_view str) const {
    return table->GetName() == str;
  }

  bool operator()(const VirtualTable* l, const VirtualTable* r) const {
    return l == r;
  }
};

using PgSystemSchema =
  containers::FlatHashSet<const VirtualTable*, HashEq, HashEq>;

template<typename T>
const VirtualTable* MakeTable() {
  static const T kTable;
  return &kTable;
}

// clang-format off
const PgSystemSchema kPgCatalog{
  MakeTable<SystemTable<PgAggregate>>(),
  MakeTable<SystemTable<PgAm>>(),
  MakeTable<SystemTable<PgAmop>>(),
  MakeTable<SystemTable<PgAmproc>>(),
  MakeTable<SystemTable<PgAttrdef>>(),
  MakeTable<SystemTable<PgAttribute>>(),
  MakeTable<SystemTable<PgAuthMembers>>(),
  MakeTable<SystemTable<PgAuthid>>(),
  MakeTable<SystemTable<PgCast>>(),
  MakeTable<SystemTable<PgClass>>(),
  MakeTable<SystemTable<PgCollation>>(),
  MakeTable<SystemTable<PgConstraint>>(),
  MakeTable<SystemTable<PgConversion>>(),
  MakeTable<SystemTable<PgDatabase>>(),
  MakeTable<SystemTable<PgDbRoleSetting>>(),
  MakeTable<SystemTable<PgDefaultAcl>>(),
  MakeTable<SystemTable<PgDepend>>(),
  MakeTable<SystemTable<PgDescription>>(),
  MakeTable<SystemTable<PgEnum>>(),
  MakeTable<SystemTable<PgEventTrigger>>(),
  MakeTable<SystemTable<PgExtension>>(),
  MakeTable<SystemTable<PgForeignDataWrapper>>(),
  MakeTable<SystemTable<PgForeignServer>>(),
  MakeTable<SystemTable<PgForeignTable>>(),
  MakeTable<SystemTable<PgIndex>>(),
  MakeTable<SystemTable<PgInherits>>(),
  MakeTable<SystemTable<PgInitPrivs>>(),
  MakeTable<SystemTable<PgLanguage>>(),
  MakeTable<SystemTable<PgLargeobject>>(),
  MakeTable<SystemTable<PgLargeobjectMetadata>>(),
  MakeTable<SystemTable<PgNamespace>>(),
  MakeTable<SystemTable<PgOpclass>>(),
  MakeTable<SystemTable<PgOperator>>(),
  MakeTable<SystemTable<PgOpfamily>>(),
  MakeTable<SystemTable<PgParameterAcl>>(),
  MakeTable<SystemTable<PgPartitionedTable>>(),
  MakeTable<SystemTable<PgPolicy>>(),
  MakeTable<SystemTable<PgProc>>(),
  MakeTable<SystemTable<PgPublication>>(),
  MakeTable<SystemTable<PgPublicationNamespace>>(),
  MakeTable<SystemTable<PgPublicationRel>>(),
  MakeTable<SystemTable<PgRange>>(),
  MakeTable<SystemTable<PgReplicationOrigin>>(),
  MakeTable<SystemTable<PgRewrite>>(),
  MakeTable<SystemTable<PgSeclabel>>(),
  MakeTable<SystemTable<PgSequence>>(),
  MakeTable<SystemTable<PgShdepend>>(),
  MakeTable<SystemTable<PgShdescription>>(),
  MakeTable<SystemTable<PgShseclabel>>(),
  MakeTable<SystemTable<PgStatistic>>(),
  MakeTable<SystemTable<PgStatisticExt>>(),
  MakeTable<SystemTable<SdbStatProgress>>(),
  MakeTable<SystemTable<PgStatisticExtData>>(),
  MakeTable<SystemTable<PgSubscription>>(),
  MakeTable<SystemTable<PgSubscriptionRel>>(),
  MakeTable<SystemTable<PgTablespace>>(),
  MakeTable<SystemTable<PgTransform>>(),
  MakeTable<SystemTable<PgTrigger>>(),
  MakeTable<SystemTable<PgTsConfig>>(),
  MakeTable<SystemTable<PgTsConfigMap>>(),
  MakeTable<SystemTable<PgTsDict>>(),
  MakeTable<SystemTable<PgTsParser>>(),
  MakeTable<SystemTable<PgTsTemplate>>(),
  MakeTable<SystemTable<PgType>>(),
  MakeTable<SystemTable<PgUserMapping>>(),
  MakeTable<SystemTable<SdbMetrics>>(),
  MakeTable<SystemTable<SdbSettings>>(),
};

const PgSystemSchema kInformationSchema{
   MakeTable<SystemTable<SqlFeatures>>(),
   MakeTable<SystemTable<SqlImplementationInfo>>(),
   MakeTable<SystemTable<SqlParts>>(),
   MakeTable<SystemTable<SqlSizing>>(),
};
// clang-format on

const VirtualTable* GetTableFromSchema(std::string_view name,
                                       const PgSystemSchema& schema) {
  auto it = schema.find(name);
  return it == schema.end() ? nullptr : *it;
}

containers::FlatHashMap<std::string, std::shared_ptr<PgSqlFunction>>
  gPgCatalogFunctions;
containers::FlatHashMap<std::string, std::shared_ptr<PgSqlFunction>>
  gInfoSchemaFunctions;
containers::FlatHashMap<std::string, std::shared_ptr<PgSqlView>>
  gPgCatalogViews;
containers::FlatHashMap<std::string, std::shared_ptr<PgSqlView>>
  gInfoSchemaViews;

}  // namespace

const VirtualTable* GetSystemTable(std::string_view schema,
                                   std::string_view name) {
  if (schema == StaticStrings::kPgCatalogSchema) {
    return GetTableFromSchema(name, kPgCatalog);
  } else if (schema == StaticStrings::kInformationSchema) {
    return GetTableFromSchema(name, kInformationSchema);
  } else {
    SDB_UNREACHABLE();
  }
}
const VirtualTable* GetTable(std::string_view name) {
  if (name.starts_with("pg_") || name.starts_with("sdb_")) {
    return GetTableFromSchema(name, kPgCatalog);
  }
  return nullptr;
}

void VisitSystemTables(
  absl::FunctionRef<void(const VirtualTable&, Oid)> visitor) {
  for (const auto* table : kPgCatalog) {
    SDB_ASSERT(table);
    visitor(*table, id::kPgCatalogSchema.id());
  }
  for (const auto* table : kInformationSchema) {
    SDB_ASSERT(table);
    visitor(*table, id::kPgInformationSchema.id());
  }
}

void VisitSystemViews(
  absl::FunctionRef<void(const catalog::PgSqlView&, Oid)> visitor) {
  for (const auto& [name, view] : gPgCatalogViews) {
    SDB_ASSERT(view);
    visitor(*view, id::kPgCatalogSchema.id());
  }
  for (const auto& [name, view] : gInfoSchemaViews) {
    SDB_ASSERT(view);
    visitor(*view, id::kPgInformationSchema.id());
  }
}

void VisitPgCatalogTables(
  absl::FunctionRef<void(const VirtualTable&)> visitor) {
  for (const auto* table : kPgCatalog) {
    visitor(*table);
  }
}

void VisitPgCatalogViews(
  absl::FunctionRef<void(const catalog::PgSqlView&)> visitor) {
  for (const auto& [_, view] : gPgCatalogViews) {
    visitor(*view);
  }
}

void VisitPgCatalogFunctions(
  absl::FunctionRef<void(const catalog::PgSqlFunction&)> visitor) {
  for (const auto& [_, f] : gPgCatalogFunctions) {
    visitor(*f);
  }
}

void VisitInfoSchemaTables(
  absl::FunctionRef<void(const VirtualTable&)> visitor) {
  for (const auto* table : kInformationSchema) {
    visitor(*table);
  }
}

void VisitInfoSchemaViews(
  absl::FunctionRef<void(const catalog::PgSqlView&)> visitor) {
  for (const auto& [_, view] : gInfoSchemaViews) {
    visitor(*view);
  }
}

void VisitInfoSchemaFunctions(
  absl::FunctionRef<void(const catalog::PgSqlFunction&)> visitor) {
  for (const auto& [_, f] : gInfoSchemaFunctions) {
    visitor(*f);
  }
}

std::shared_ptr<catalog::PgSqlFunction> GetInfoSchemaFunction(
  std::string_view name) {
  auto it = gInfoSchemaFunctions.find(name);
  return it != gInfoSchemaFunctions.end() ? it->second : nullptr;
}

std::shared_ptr<catalog::PgSqlFunction> GetPgCatalogFunction(
  std::string_view name) {
#ifndef SDB_GTEST
#endif
  auto it = gPgCatalogFunctions.find(name);
  return it != gPgCatalogFunctions.end() ? it->second : nullptr;
}

std::shared_ptr<PgSqlView> GetInfoSchemaView(std::string_view name) {
  auto it = gInfoSchemaViews.find(name);
  if (it == gInfoSchemaViews.end()) {
    return nullptr;
  }
  return it->second;
}

std::shared_ptr<PgSqlView> GetView(std::string_view name) {
  auto it = gPgCatalogViews.find(name);
  if (it == gPgCatalogViews.end()) {
    return nullptr;
  }
  return it->second;
}

void InitSystemViews(duckdb::Parser& parser) {
  for (const auto& view : kExternalViews) {
    auto info = duckdb::make_uniq<duckdb::CreateViewInfo>();
    info->schema = view.schema;
    info->view_name = view.name;
    info->sql = view.sql;
    info->temporary = true;
    info->internal = true;

    parser.statements.clear();
    parser.ParseQuery(info->sql);
    SDB_ASSERT(parser.statements.size() == 1);
    SDB_ASSERT(parser.statements[0]->type ==
               duckdb::StatementType::SELECT_STATEMENT);
    info->query =
      duckdb::unique_ptr_cast<duckdb::SQLStatement, duckdb::SelectStatement>(
        std::move(parser.statements[0]));

    catalog::Acl acl;
    if (!view.superuser_only) {
      acl.push_back(catalog::kSystemPublicSelect);
    }
    auto entry = std::make_shared<catalog::PgSqlView>(
      catalog::Permissions{id::kRootUser, std::move(acl)}, ObjectId{},
      ObjectId{}, view.name, std::move(info));

    auto& map = (view.schema == StaticStrings::kInformationSchema)
                  ? gInfoSchemaViews
                  : gPgCatalogViews;
    map[view.name] = std::move(entry);
  }
}

static duckdb::unique_ptr<duckdb::CreateMacroInfo> ParseMacro(
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
  info->schema = macro.schema;
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

void InitSystemFunctions(duckdb::Parser& parser) {
  for (const auto& macro : kExternalMacros) {
    auto info = ParseMacro(parser, macro);

    // DEFAULT_SCHEMA schema macros go into pg_catalog because in PG,
    // pg_catalog is always implicitly searched -- functions like current_user,
    // overlay, etc. should be findable without schema qualification.
    auto& map = (macro.schema == StaticStrings::kInformationSchema)
                  ? gInfoSchemaFunctions
                  : gPgCatalogFunctions;

    auto it = map.find(macro.name);
    if (it != map.end()) {
      auto& existing = it->second->GetInfo();
      for (auto& m : info->macros) {
        existing.macros.push_back(std::move(m));
      }
      if (existing.type == duckdb::CatalogType::MACRO_ENTRY) {
        existing.type = info->type;
      }
    } else {
      map[macro.name] = std::make_shared<catalog::PgSqlFunction>(
        catalog::Permissions{}, ObjectId{}, ObjectId{}, macro.name,
        std::move(info));
    }
  }
}

}  // namespace sdb::pg
