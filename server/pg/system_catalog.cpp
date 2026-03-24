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
    .Case("pg_typeof",
          VeloxFunction{"pg_typeof", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    // Session information functions
    .Case("current_user",
          VeloxFunction{"pg_current_user", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("current_database",
          VeloxFunction{"pg_current_database", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("version",
          VeloxFunction{"pg_version", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("pg_backend_pid",
          VeloxFunction{"pg_backend_pid", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("pg_trigger_depth",
          VeloxFunction{"pg_trigger_depth", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_jit_available",
          VeloxFunction{"pg_jit_available", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_my_temp_schema",
          VeloxFunction{"pg_my_temp_schema", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_is_other_temp_schema",
          VeloxFunction{"pg_is_other_temp_schema", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("inet_client_addr",
          VeloxFunction{"pg_inet_client_addr", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("inet_client_port",
          VeloxFunction{"pg_inet_client_port", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("inet_server_addr",
          VeloxFunction{"pg_inet_server_addr", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("inet_server_port",
          VeloxFunction{"pg_inet_server_port", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("current_query",
          VeloxFunction{"pg_current_query", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_blocking_pids",
          VeloxFunction{"pg_blocking_pids", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_safe_snapshot_blocking_pids",
          VeloxFunction{"pg_safe_snapshot_blocking_pids", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_conf_load_time",
          VeloxFunction{"pg_conf_load_time", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_postmaster_start_time",
          VeloxFunction{"pg_postmaster_start_time", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_current_logfile",
          VeloxFunction{"pg_current_logfile", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_numa_available",
          VeloxFunction{"pg_numa_available", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_notification_queue_usage",
          VeloxFunction{"pg_notification_queue_usage", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    // Access privilege inquiry functions
    .Case("has_schema_privilege",
          VeloxFunction{"pg_has_schema_privilege", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("has_table_privilege",
          VeloxFunction{"pg_has_table_privilege", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("has_database_privilege",
          VeloxFunction{"pg_has_database_privilege", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("has_column_privilege",
          VeloxFunction{"pg_has_column_privilege", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("has_any_column_privilege",
          VeloxFunction{"pg_has_any_column_privilege", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("has_function_privilege",
          VeloxFunction{"pg_has_function_privilege", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("has_language_privilege",
          VeloxFunction{"pg_has_language_privilege", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("has_sequence_privilege",
          VeloxFunction{"pg_has_sequence_privilege", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("has_server_privilege",
          VeloxFunction{"pg_has_server_privilege", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("has_tablespace_privilege",
          VeloxFunction{"pg_has_tablespace_privilege", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("has_type_privilege",
          VeloxFunction{"pg_has_type_privilege", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("has_foreign_data_wrapper_privilege",
          VeloxFunction{"pg_has_foreign_data_wrapper_privilege", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("has_parameter_privilege",
          VeloxFunction{"pg_has_parameter_privilege", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("has_largeobject_privilege",
          VeloxFunction{"pg_has_largeobject_privilege", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_has_role",
          VeloxFunction{"pg_has_role", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("acldefault",
          VeloxFunction{"pg_acldefault", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("aclexplode",
          VeloxFunction{"pg_aclexplode", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("makeaclitem",
          VeloxFunction{"pg_makeaclitem", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("row_security_active",
          VeloxFunction{"pg_row_security_active", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    // Schema visibility inquiry functions
    .Case("pg_function_is_visible",
          VeloxFunction{"pg_function_is_visible", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_collation_is_visible",
          VeloxFunction{"pg_collation_is_visible", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_conversion_is_visible",
          VeloxFunction{"pg_conversion_is_visible", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_operator_is_visible",
          VeloxFunction{"pg_operator_is_visible", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_opclass_is_visible",
          VeloxFunction{"pg_opclass_is_visible", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_opfamily_is_visible",
          VeloxFunction{"pg_opfamily_is_visible", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_statistics_obj_is_visible",
          VeloxFunction{"pg_statistics_obj_is_visible", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_ts_config_is_visible",
          VeloxFunction{"pg_ts_config_is_visible", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_ts_dict_is_visible",
          VeloxFunction{"pg_ts_dict_is_visible", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_ts_parser_is_visible",
          VeloxFunction{"pg_ts_parser_is_visible", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_ts_template_is_visible",
          VeloxFunction{"pg_ts_template_is_visible", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_type_is_visible",
          VeloxFunction{"pg_type_is_visible", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    // System catalog information functions
    .Case("col_description",
          VeloxFunction{"pg_col_description", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("obj_description",
          VeloxFunction{"pg_obj_description", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("shobj_description",
          VeloxFunction{"pg_shobj_description", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("format_type",
          VeloxFunction{"pg_format_type", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    // 9.27.4 System Catalog Information Functions (additional)
    .Case("pg_char_to_encoding",
          VeloxFunction{"pg_char_to_encoding", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_encoding_to_char",
          VeloxFunction{"pg_encoding_to_char", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_get_constraintdef",
          VeloxFunction{"pg_get_constraintdef", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_get_functiondef",
          VeloxFunction{"pg_get_functiondef", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_get_function_arguments",
          VeloxFunction{"pg_get_function_arguments", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_get_function_identity_arguments",
          VeloxFunction{"pg_get_function_identity_arguments", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_get_function_result",
          VeloxFunction{"pg_get_function_result", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_get_indexdef",
          VeloxFunction{"pg_get_indexdef", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("pg_get_partkeydef",
          VeloxFunction{"pg_get_partkeydef", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_get_partition_constraintdef",
          VeloxFunction{"pg_get_partition_constraintdef", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_get_serial_sequence",
          VeloxFunction{"pg_get_serial_sequence", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_get_statisticsobjdef",
          VeloxFunction{"pg_get_statisticsobjdef", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_get_triggerdef",
          VeloxFunction{"pg_get_triggerdef", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_index_column_has_property",
          VeloxFunction{"pg_index_column_has_property", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_index_has_property",
          VeloxFunction{"pg_index_has_property", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_indexam_has_property",
          VeloxFunction{"pg_indexam_has_property", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_settings_get_flags",
          VeloxFunction{"pg_settings_get_flags", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_tablespace_location",
          VeloxFunction{"pg_tablespace_location", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("to_regclass",
          VeloxFunction{"pg_to_regclass", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("to_regcollation",
          VeloxFunction{"pg_to_regcollation", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("to_regnamespace",
          VeloxFunction{"pg_to_regnamespace", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("to_regoper",
          VeloxFunction{"pg_to_regoper", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("to_regoperator",
          VeloxFunction{"pg_to_regoperator", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("to_regproc",
          VeloxFunction{"pg_to_regproc", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("to_regprocedure",
          VeloxFunction{"pg_to_regprocedure", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("to_regrole",
          VeloxFunction{"pg_to_regrole", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("to_regtype",
          VeloxFunction{"pg_to_regtype", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("to_regtypemod",
          VeloxFunction{"pg_to_regtypemod", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_basetype",
          VeloxFunction{"pg_basetype", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("pg_get_catalog_foreign_keys",
          VeloxFunction{"pg_get_catalog_foreign_keys", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_get_keywords",
          VeloxFunction{"pg_get_keywords", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("pg_tablespace_databases",
          VeloxFunction{"pg_tablespace_databases", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_options_to_table",
          VeloxFunction{"pg_options_to_table", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    // 9.27.5 Object Information and Addressing Functions
    .Case("pg_describe_object",
          VeloxFunction{"pg_describe_object", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_get_acl",
          VeloxFunction{"pg_get_acl", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("pg_identify_object",
          VeloxFunction{"pg_identify_object", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_identify_object_as_address",
          VeloxFunction{"pg_identify_object_as_address", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_get_object_address",
          VeloxFunction{"pg_get_object_address", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    // 9.27.7 Data Validity Checking Functions
    .Case("pg_input_is_valid",
          VeloxFunction{"pg_input_is_valid", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_input_error_info",
          VeloxFunction{"pg_input_error_info", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_column_compression",
          VeloxFunction{"pg_column_compression", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_column_size",
          VeloxFunction{"pg_column_size", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    // 9.27.8 Transaction ID and Snapshot Information Functions
    .Case("pg_current_xact_id",
          VeloxFunction{"pg_current_xact_id", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_current_xact_id_if_assigned",
          VeloxFunction{"pg_current_xact_id_if_assigned", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_xact_status",
          VeloxFunction{"pg_xact_status", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("pg_current_snapshot",
          VeloxFunction{"pg_current_snapshot", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_snapshot_xip",
          VeloxFunction{"pg_snapshot_xip", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("pg_snapshot_xmax",
          VeloxFunction{"pg_snapshot_xmax", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_snapshot_xmin",
          VeloxFunction{"pg_snapshot_xmin", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_visible_in_snapshot",
          VeloxFunction{"pg_visible_in_snapshot", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("mxid_age",
          VeloxFunction{"pg_mxid_age", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("pg_get_multixact_members",
          VeloxFunction{"pg_get_multixact_members", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_get_multixact_stats",
          VeloxFunction{"pg_get_multixact_stats", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    // Deprecated txid_* functions
    .Case("txid_current",
          VeloxFunction{"pg_txid_current", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("txid_current_if_assigned",
          VeloxFunction{"pg_txid_current_if_assigned", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("txid_current_snapshot",
          VeloxFunction{"pg_txid_current_snapshot", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("txid_snapshot_xip",
          VeloxFunction{"pg_txid_snapshot_xip", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("txid_snapshot_xmax",
          VeloxFunction{"pg_txid_snapshot_xmax", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("txid_snapshot_xmin",
          VeloxFunction{"pg_txid_snapshot_xmin", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("txid_visible_in_snapshot",
          VeloxFunction{"pg_txid_visible_in_snapshot", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("txid_status",
          VeloxFunction{"pg_txid_status", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    // 9.27.9 Committed Transaction Information Functions
    .Case("pg_xact_commit_timestamp",
          VeloxFunction{"pg_xact_commit_timestamp", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_xact_commit_timestamp_origin",
          VeloxFunction{"pg_xact_commit_timestamp_origin", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_last_committed_xact",
          VeloxFunction{"pg_last_committed_xact", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    // 9.27.10 Control Data Functions
    .Case("pg_control_checkpoint",
          VeloxFunction{"pg_control_checkpoint", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_control_system",
          VeloxFunction{"pg_control_system", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_control_init",
          VeloxFunction{"pg_control_init", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("pg_control_recovery",
          VeloxFunction{"pg_control_recovery", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    // 9.27.11 Version Information Functions
    .Case("unicode_version",
          VeloxFunction{"pg_unicode_version", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("icu_unicode_version",
          VeloxFunction{"pg_icu_unicode_version", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    // 9.27.12 WAL Summarization Information Functions
    .Case("pg_available_wal_summaries",
          VeloxFunction{"pg_available_wal_summaries", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_get_wal_summarizer_state",
          VeloxFunction{"pg_get_wal_summarizer_state", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_wal_summary_contents",
          VeloxFunction{"pg_wal_summary_contents", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    // 9.27.1 Set-returning functions (not supported as SRFs)
    .Case("pg_listening_channels",
          VeloxFunction{"pg_listening_channels", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("pg_get_loaded_modules",
          VeloxFunction{"pg_get_loaded_modules", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    // Search functions
    .Case("phrase", VeloxFunction{search::functions::kPhrase, false})
    .Case("term_eq", VeloxFunction{search::functions::kTermEq, false})
    .Case("term_lt", VeloxFunction{search::functions::kTermLt, false})
    .Case("term_lte", VeloxFunction{search::functions::kTermLe, false})
    .Case("term_gte", VeloxFunction{search::functions::kTermGe, false})
    .Case("term_gt", VeloxFunction{search::functions::kTermGt, false})
    .Case("term_in", VeloxFunction{search::functions::kTermIn, false})
    .Case("term_like", VeloxFunction{search::functions::kTermLike, false})
    .Case("ngram_match", {search::functions::kNgramMatch, false})
    .Case("levenshtein_match", {search::functions::kLevenshteinMatch, false});
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
