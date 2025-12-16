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

#include <frozen/unordered_map.h>

#include <boost/pfr.hpp>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "catalog/function.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/view.h"
#include "general_server/state.h"
#include "pg/commands.h"
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
#include "pg/pg_feature.h"
#include "pg/sdb_catalog/sdb_log.h"
#include "pg/sql_parser.h"
#include "pg/system_table.h"
#include "pg/system_views.h"
#include "vpack/serializer.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/parsenodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {
namespace {
using namespace catalog;

struct HashEq {
  using is_transparent = void;

  size_t operator()(std::string_view str) const { return absl::HashOf(str); }

  size_t operator()(const VirtualTable* table) const {
    return (*this)(table->Name());
  }

  bool operator()(const VirtualTable* table, std::string_view str) const {
    return table->Name() == str;
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
  MakeTable<SystemTable<SdbLog>>(),
};

const PgSystemSchema kInformationSchema{
   MakeTable<SystemTable<SqlFeatures>>(),
   MakeTable<SystemTable<SqlImplementationInfo>>(),
   MakeTable<SystemTable<SqlParts>>(),
   MakeTable<SystemTable<SqlSizing>>(),
};
// clang-format on

struct VeloxFunction {
  std::string_view name;
  bool table = false;
  FunctionLanguage language = FunctionLanguage::VeloxNative;
  FunctionKind kind = FunctionKind::Scalar;
};

constexpr auto kMapping =
  frozen::make_unordered_map<std::string_view, VeloxFunction>({
    {"generate_series", {"presto_sequence", true}},
    {"unnest", {"unnest", true, FunctionLanguage::Decorator}},
    // Scalars
    // String functions
    {"chr", {"presto_chr", false}},
    {"concat", {"presto_concat", false}},
    {"length", {"presto_length", false}},
    {"lower", {"presto_lower", false}},
    {"upper", {"presto_upper", false}},
    {"ltrim", {"presto_ltrim", false}},
    {"rtrim", {"presto_rtrim", false}},
    {"btrim", {"presto_trim", false}},
    {"lpad", {"presto_lpad", false}},
    {"rpad", {"presto_rpad", false}},
    {"replace", {"presto_replace", false}},
    {"reverse", {"presto_reverse", false}},
    {"substring", {"presto_substring", false}},
    {"substr", {"presto_substr", false}},
    {"strpos", {"presto_strpos", false}},
    {"split_part", {"presto_split_part", false}},
    {"regexp_replace", {"presto_regexp_replace", false}},
    {"similar_to_escape",
     {"pg_similar_to_escape", false, FunctionLanguage::VeloxNative,
      FunctionKind::Scalar}},
    {"like_escape",
     {"pg_like_escape", false, FunctionLanguage::VeloxNative,
      FunctionKind::Scalar}},
    // Math functions
    {"abs", {"presto_abs", false}},
    {"acos", {"presto_acos", false}},
    {"asin", {"presto_asin", false}},
    {"atan", {"presto_atan", false}},
    {"atan2", {"presto_atan2", false}},
    {"cbrt", {"presto_cbrt", false}},
    {"ceil", {"presto_ceil", false}},
    {"ceiling", {"presto_ceiling", false}},
    {"cos", {"presto_cos", false}},
    {"degrees", {"presto_degrees", false}},
    {"exp", {"presto_exp", false}},
    {"floor", {"presto_floor", false}},
    {"ln", {"presto_ln", false}},
    {"mod", {"presto_mod", false}},
    {"pi", {"presto_pi", false}},
    {"pow", {"presto_pow", false}},
    {"power", {"presto_power", false}},
    {"radians", {"presto_radians", false}},
    {"random", {"presto_random", false}},
    {"round", {"presto_round", false}},
    {"sign", {"presto_sign", false}},
    {"sin", {"presto_sin", false}},
    {"sqrt", {"presto_sqrt", false}},
    {"tan", {"presto_tan", false}},
    {"trunc", {"presto_truncate", false}},
    {"width_bucket", {"presto_width_bucket", false}},
    // Date/Time functions
    {"date_trunc", {"presto_date_trunc", false}},
    {"extract", {"pg_extract", false}},
    // Array functions
    {"array_position", {"presto_array_position", false}},
    {"cardinality", {"presto_cardinality", false}},
    {"array_to_string",
     {"presto_array_join", false, FunctionLanguage::VeloxNative,
      FunctionKind::Scalar}},
    // Aggregates
    {"avg",
     {"presto_avg", false, FunctionLanguage::VeloxNative,
      FunctionKind::Aggregate}},
    {"count",
     {"presto_count", false, FunctionLanguage::VeloxNative,
      FunctionKind::Aggregate}},
    {"max",
     {"presto_max", false, FunctionLanguage::VeloxNative,
      FunctionKind::Aggregate}},
    {"min",
     {"presto_min", false, FunctionLanguage::VeloxNative,
      FunctionKind::Aggregate}},
    {"sum",
     {"presto_sum", false, FunctionLanguage::VeloxNative,
      FunctionKind::Aggregate}},
    {"bool_and",
     {"presto_bool_and", false, FunctionLanguage::VeloxNative,
      FunctionKind::Aggregate}},
    {"bool_or",
     {"presto_bool_or", false, FunctionLanguage::VeloxNative,
      FunctionKind::Aggregate}},
    {"every",
     {"presto_every", false, FunctionLanguage::VeloxNative,
      FunctionKind::Aggregate}},
    {"stddev",
     {"presto_stddev", false, FunctionLanguage::VeloxNative,
      FunctionKind::Aggregate}},
    {"stddev_pop",
     {"presto_stddev_pop", false, FunctionLanguage::VeloxNative,
      FunctionKind::Aggregate}},
    {"stddev_samp",
     {"presto_stddev_samp", false, FunctionLanguage::VeloxNative,
      FunctionKind::Aggregate}},
    {"variance",
     {"presto_variance", false, FunctionLanguage::VeloxNative,
      FunctionKind::Aggregate}},
    {"var_pop",
     {"presto_var_pop", false, FunctionLanguage::VeloxNative,
      FunctionKind::Aggregate}},
    {"var_samp",
     {"presto_var_samp", false, FunctionLanguage::VeloxNative,
      FunctionKind::Aggregate}},
    {"covar_pop",
     {"presto_covar_pop", false, FunctionLanguage::VeloxNative,
      FunctionKind::Aggregate}},
    {"covar_samp",
     {"presto_covar_samp", false, FunctionLanguage::VeloxNative,
      FunctionKind::Aggregate}},
    {"array_agg",
     {"presto_array_agg", false, FunctionLanguage::VeloxNative,
      FunctionKind::Aggregate}},
    {"string_agg",
     {"presto_array_join", false, FunctionLanguage::VeloxNative,
      FunctionKind::Aggregate}},
    {"bit_and",
     {"presto_bitwise_and_agg", false, FunctionLanguage::VeloxNative,
      FunctionKind::Aggregate}},
    {"bit_or",
     {"presto_bitwise_or_agg", false, FunctionLanguage::VeloxNative,
      FunctionKind::Aggregate}},
    {"bit_xor",
     {"presto_bitwise_xor_agg", false, FunctionLanguage::VeloxNative,
      FunctionKind::Aggregate}},
    {"any_value",
     {"presto_any_value", false, FunctionLanguage::VeloxNative,
      FunctionKind::Aggregate}},
    // Window functions
    {"row_number",
     {"presto_row_number", false, FunctionLanguage::VeloxNative,
      FunctionKind::Window}},
    {"rank",
     {"presto_rank", false, FunctionLanguage::VeloxNative,
      FunctionKind::Window}},
    {"dense_rank",
     {"presto_dense_rank", false, FunctionLanguage::VeloxNative,
      FunctionKind::Window}},
    {"percent_rank",
     {"presto_percent_rank", false, FunctionLanguage::VeloxNative,
      FunctionKind::Window}},
    {"cume_dist",
     {"presto_cume_dist", false, FunctionLanguage::VeloxNative,
      FunctionKind::Window}},
    {"ntile",
     {"presto_ntile", false, FunctionLanguage::VeloxNative,
      FunctionKind::Window}},
    {"nth_value",
     {"presto_nth_value", false, FunctionLanguage::VeloxNative,
      FunctionKind::Window}},
    // PostgreSQL system functions
    {"current_schema",
     {"pg_current_schema", false, FunctionLanguage::VeloxNative,
      FunctionKind::Scalar}},
    {"current_schemas",
     {"pg_current_schemas", false, FunctionLanguage::VeloxNative,
      FunctionKind::Scalar}},
    {"pg_get_userbyid",
     {"pg_get_userbyid", false, FunctionLanguage::VeloxNative,
      FunctionKind::Scalar}},
    {"pg_get_viewdef",
     {"pg_get_viewdef", false, FunctionLanguage::VeloxNative,
      FunctionKind::Scalar}},
    {"pg_get_ruledef",
     {"pg_get_ruledef", false, FunctionLanguage::VeloxNative,
      FunctionKind::Scalar}},
    {"pg_table_is_visible",
     {"pg_table_is_visible", false, FunctionLanguage::VeloxNative,
      FunctionKind::Scalar}},
    {"json_extract_path",
     {"pg_json_extract_path", false, FunctionLanguage::VeloxNative,
      FunctionKind::Scalar}},
  });
const VirtualTable* GetTableFromSchema(std::string_view name,
                                       const PgSystemSchema& schema) {
  auto it = schema.find(name);
  return it == schema.end() ? nullptr : *it;
}
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
  SDB_ASSERT(SerenedServer::Instance().isEnabled<pg::PostgresFeature>());

  if (name.starts_with("pg_") || name.starts_with("sdb_")) {
    return GetTableFromSchema(name, kPgCatalog);
  }
  return nullptr;
}

void VisitSystemTables(
  absl::FunctionRef<void(const VirtualTable&, Oid)> visitor) {
  SDB_ASSERT(SerenedServer::Instance().isEnabled<pg::PostgresFeature>());
  for (const auto* table : kPgCatalog) {
    SDB_ASSERT(table);
    visitor(*table, id::kPgCatalogSchema.id());
  }
  for (const auto* table : kInformationSchema) {
    SDB_ASSERT(table);
    visitor(*table, id::kPgInformationSchema.id());
  }
}

std::shared_ptr<catalog::Function> GetFunction(std::string_view name) {
  SDB_ASSERT(SerenedServer::Instance().isEnabled<pg::PostgresFeature>());
  FunctionLanguage language = FunctionLanguage::VeloxNative;
  bool table = false;
  FunctionKind kind = FunctionKind::Scalar;
  if (!name.starts_with("serene_") && !name.starts_with("presto_") &&
      !name.starts_with("spark_")) {
    auto it = kMapping.find(name);
    if (it == kMapping.end()) {
      return nullptr;
    }
    name = it->second.name;
    language = it->second.language;
    table = it->second.table;
    kind = it->second.kind;
  }
  return std::make_shared<catalog::Function>(
    name, FunctionSignature{},
    FunctionOptions{
      .language = language,
      .state = FunctionState::Immutable,
      .parallel = FunctionParallel::Safe,
      .table = table,
      .kind = kind,
    });
}

containers::FlatHashMap<std::string, std::shared_ptr<View>> gSystemViews;

std::shared_ptr<View> GetView(std::string_view name) {
  SDB_ASSERT(SerenedServer::Instance().isEnabled<pg::PostgresFeature>());
  auto it = gSystemViews.find(name);
  if (it == gSystemViews.end()) {
    return nullptr;
  }
  return it->second;
}

void RegisterSystemViews() {
  for (const auto system_view_query : kSystemViewsQueries) {
    auto stmt = pg::ParseSystemView(system_view_query);
    const auto* raw_stmt = castNode(RawStmt, stmt.tree.GetRoot());
    const auto* view_stmt = castNode(ViewStmt, raw_stmt->stmt);
    auto system_view = pg::CreateSystemView(*view_stmt);
    gSystemViews[system_view->GetName()] = system_view;
  }
}

}  // namespace sdb::pg
