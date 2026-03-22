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

#include <vpack/serializer.h>

#include <boost/pfr.hpp>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/trivial_map.h"
#include "catalog/function.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/sql_function_impl.h"
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
#include "pg/pg_catalog/pg_settings.h"
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
#include "pg/system_functions.h"
#include "pg/system_table.h"
#include "pg/system_views.h"
#include "search/functions.hpp"

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
  MakeTable<SystemTable<SdbShowAllSettings>>(),
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

constexpr containers::TrivialBiMap kMapping = [](auto selector) {
  return selector()
    .Case("generate_series", VeloxFunction{"presto_sequence", true})
    .Case("unnest", VeloxFunction{"unnest", true, FunctionLanguage::Decorator})
    // Scalars
    // String functions
    .Case("chr", VeloxFunction{"presto_chr", false})
    .Case("concat", VeloxFunction{"presto_concat", false})
    .Case("length", VeloxFunction{"presto_length", false})
    .Case("lower", VeloxFunction{"presto_lower", false})
    .Case("upper", VeloxFunction{"presto_upper", false})
    .Case("ltrim", VeloxFunction{"presto_ltrim", false})
    .Case("rtrim", VeloxFunction{"presto_rtrim", false})
    .Case("btrim", VeloxFunction{"presto_trim", false})
    .Case("lpad", VeloxFunction{"presto_lpad", false})
    .Case("rpad", VeloxFunction{"presto_rpad", false})
    .Case("replace", VeloxFunction{"presto_replace", false})
    .Case("reverse", VeloxFunction{"presto_reverse", false})
    .Case("substring", VeloxFunction{"presto_substring", false})
    .Case("substr", VeloxFunction{"presto_substr", false})
    .Case("strpos", VeloxFunction{"presto_strpos", false})
    .Case("split_part", VeloxFunction{"presto_split_part", false})
    .Case("regexp_replace", VeloxFunction{"presto_regexp_replace", false})
    .Case("similar_to_escape",
          VeloxFunction{"pg_similar_to_escape", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("like_escape",
          VeloxFunction{"pg_like_escape", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    // Math functions
    .Case("abs", VeloxFunction{"presto_abs", false})
    .Case("acos", VeloxFunction{"presto_acos", false})
    .Case("asin", VeloxFunction{"presto_asin", false})
    .Case("atan", VeloxFunction{"presto_atan", false})
    .Case("atan2", VeloxFunction{"presto_atan2", false})
    .Case("cbrt", VeloxFunction{"presto_cbrt", false})
    .Case("ceil", VeloxFunction{"presto_ceil", false})
    .Case("ceiling", VeloxFunction{"presto_ceiling", false})
    .Case("cos", VeloxFunction{"presto_cos", false})
    .Case("degrees", VeloxFunction{"presto_degrees", false})
    .Case("exp", VeloxFunction{"presto_exp", false})
    .Case("floor", VeloxFunction{"presto_floor", false})
    .Case("ln", VeloxFunction{"presto_ln", false})
    .Case("mod", VeloxFunction{"presto_mod", false})
    .Case("pi", VeloxFunction{"presto_pi", false})
    .Case("pow", VeloxFunction{"presto_pow", false})
    .Case("power", VeloxFunction{"presto_power", false})
    .Case("radians", VeloxFunction{"presto_radians", false})
    .Case("random", VeloxFunction{"presto_random", false})
    .Case("round", VeloxFunction{"presto_round", false})
    .Case("sign", VeloxFunction{"presto_sign", false})
    .Case("sin", VeloxFunction{"presto_sin", false})
    .Case("sqrt", VeloxFunction{"presto_sqrt", false})
    .Case("tan", VeloxFunction{"presto_tan", false})
    .Case("trunc", VeloxFunction{"presto_truncate", false})
    .Case("width_bucket", VeloxFunction{"presto_width_bucket", false})
    .Case("fail", VeloxFunction{"pg_error", false})
    // Date/Time functions
    .Case("date_trunc", VeloxFunction{"presto_date_trunc", false})
    .Case("extract", VeloxFunction{"pg_extract", false})
    // Array functions
    .Case("array_position", VeloxFunction{"presto_array_position", false})
    .Case("cardinality", VeloxFunction{"presto_cardinality", false})
    .Case("array_to_string",
          VeloxFunction{"presto_array_join", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    // Aggregates
    .Case("avg",
          VeloxFunction{"presto_avg", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Aggregate})
    .Case("count",
          VeloxFunction{"presto_count", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Aggregate})
    .Case("max",
          VeloxFunction{"presto_max", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Aggregate})
    .Case("min",
          VeloxFunction{"presto_min", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Aggregate})
    .Case("sum",
          VeloxFunction{"presto_sum", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Aggregate})
    .Case("bool_and",
          VeloxFunction{"presto_bool_and", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Aggregate})
    .Case("bool_or",
          VeloxFunction{"presto_bool_or", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Aggregate})
    .Case("every",
          VeloxFunction{"presto_every", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Aggregate})
    .Case("stddev",
          VeloxFunction{"presto_stddev", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Aggregate})
    .Case("stddev_pop",
          VeloxFunction{"presto_stddev_pop", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Aggregate})
    .Case("stddev_samp",
          VeloxFunction{"presto_stddev_samp", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Aggregate})
    .Case("variance",
          VeloxFunction{"presto_variance", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Aggregate})
    .Case("var_pop",
          VeloxFunction{"presto_var_pop", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Aggregate})
    .Case("var_samp",
          VeloxFunction{"presto_var_samp", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Aggregate})
    .Case("covar_pop",
          VeloxFunction{"presto_covar_pop", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Aggregate})
    .Case("covar_samp",
          VeloxFunction{"presto_covar_samp", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Aggregate})
    .Case("array_agg",
          VeloxFunction{"presto_array_agg", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Aggregate})
    .Case("string_agg",
          VeloxFunction{"presto_array_join", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Aggregate})
    .Case("bit_and",
          VeloxFunction{"presto_bitwise_and_agg", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Aggregate})
    .Case("bit_or",
          VeloxFunction{"presto_bitwise_or_agg", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Aggregate})
    .Case("bit_xor",
          VeloxFunction{"presto_bitwise_xor_agg", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Aggregate})
    .Case("any_value",
          VeloxFunction{"presto_any_value", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Aggregate})
    // Window functions
    .Case("row_number",
          VeloxFunction{"presto_row_number", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Window})
    .Case("rank",
          VeloxFunction{"presto_rank", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Window})
    .Case("dense_rank",
          VeloxFunction{"presto_dense_rank", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Window})
    .Case("percent_rank",
          VeloxFunction{"presto_percent_rank", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Window})
    .Case("cume_dist",
          VeloxFunction{"presto_cume_dist", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Window})
    .Case("ntile",
          VeloxFunction{"presto_ntile", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Window})
    .Case("nth_value",
          VeloxFunction{"presto_nth_value", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Window})
    // PostgreSQL system functions
    .Case("current_schema",
          VeloxFunction{"pg_current_schema", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("current_schemas",
          VeloxFunction{"pg_current_schemas", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_get_userbyid",
          VeloxFunction{"pg_get_userbyid", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("pg_get_viewdef",
          VeloxFunction{"pg_get_viewdef", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("pg_get_ruledef",
          VeloxFunction{"pg_get_ruledef", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("pg_table_is_visible",
          VeloxFunction{"pg_table_is_visible", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("json_extract_path",
          VeloxFunction{"pg_json_extract_path", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("json_extract_path_text",
          VeloxFunction{"pg_json_extract_path_text", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_schema_size",
          VeloxFunction{"pg_schema_size", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("pg_database_size",
          VeloxFunction{"pg_database_size", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_table_size",
          VeloxFunction{"pg_table_size", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("ts_lexize",
          VeloxFunction{"pg_ts_lexize", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    // Search functions
    .Case("phrase", VeloxFunction{search::functions::kPhrase, false})
    .Case("term_eq", VeloxFunction{search::functions::kTermEq, false})
    .Case("term_lt", VeloxFunction{search::functions::kTermLt, false})
    .Case("term_lte", VeloxFunction{search::functions::kTermLe, false})
    .Case("term_gte", VeloxFunction{search::functions::kTermGe, false})
    .Case("term_gt", VeloxFunction{search::functions::kTermGt, false})
    .Case("term_in", VeloxFunction{search::functions::kTermIn, false})
    .Case("term_like", VeloxFunction{search::functions::kTermLike, false});
};

const VirtualTable* GetTableFromSchema(std::string_view name,
                                       const PgSystemSchema& schema) {
  auto it = schema.find(name);
  return it == schema.end() ? nullptr : *it;
}

containers::FlatHashMap<std::string, std::shared_ptr<Function>>
  gSystemFunctions;
containers::FlatHashMap<std::string, std::shared_ptr<View>> gSystemViews;

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
#ifndef SDB_GTEST
  // For query building tests we need to run this without feature
  SDB_ASSERT(SerenedServer::Instance().isEnabled<pg::PostgresFeature>());
#endif
  if (auto it = gSystemFunctions.find(name); it != gSystemFunctions.end()) {
    return it->second;
  }
  FunctionLanguage language = FunctionLanguage::VeloxNative;
  bool table = false;
  FunctionKind kind = FunctionKind::Scalar;
  if (!name.starts_with("serene_") && !name.starts_with("presto_") &&
      !name.starts_with("spark_")) {
    auto it = kMapping.TryFindByFirst(name);
    if (!it) {
      return nullptr;
    }
    name = it->name;
    language = it->language;
    table = it->table;
    kind = it->kind;
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
    const auto* raw_stmt = ParseSystemObject(system_view_query);
    const auto* view_stmt = castNode(ViewStmt, raw_stmt->stmt);
    SDB_ASSERT(view_stmt);
    auto system_view = CreateSystemView(*view_stmt);
    gSystemViews[system_view->GetName()] = std::move(system_view);
  }
}

void RegisterSystemFunctions() {
  for (const auto system_func_query : kSystemFunctionsQueries) {
    const auto* raw_stmt = ParseSystemObject(system_func_query);
    const auto* create_func_stmt = castNode(CreateFunctionStmt, raw_stmt->stmt);
    SDB_ASSERT(create_func_stmt);
    auto func = CreateSystemFunction(*create_func_stmt);
    gSystemFunctions[func->GetName()] = std::move(func);
  }
}

}  // namespace sdb::pg
