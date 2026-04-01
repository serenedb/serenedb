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
    .Case("generate_series", VeloxFunction{"pg_generate_series", true})
    .Case("generate_subscripts", VeloxFunction{"pg_generate_subscripts", true})
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
    .Case("position", VeloxFunction{"presto_strpos", false})
    .Case("split_part", VeloxFunction{"presto_split_part", false})
    .Case("regexp_replace", VeloxFunction{"presto_regexp_replace", false})
    .Case("regexp_match", VeloxFunction{"pg_regexp_match", false})
    .Case("regexp_count", VeloxFunction{"pg_regexp_count", false})
    .Case("regexp_instr", VeloxFunction{"pg_regexp_instr", false})
    .Case("regexp_substr", VeloxFunction{"pg_regexp_substr", false})
    .Case("regexp_split_to_array", VeloxFunction{"presto_regexp_split", false})
    .Case("starts_with", VeloxFunction{"presto_starts_with", false})
    .Case("concat_ws", VeloxFunction{"pg_concat_ws", false})
    .Case("format", VeloxFunction{"pg_format", false})
    .Case("bit_length", VeloxFunction{"presto_bit_length", false})
    .Case("char_length", VeloxFunction{"presto_length", false})
    .Case("character_length", VeloxFunction{"presto_length", false})
    .Case("initcap", VeloxFunction{"spark_initcap", false})
    .Case("repeat", VeloxFunction{"spark_repeat", false})
    .Case("translate", VeloxFunction{"spark_translate", false})
    .Case("ascii", VeloxFunction{"spark_ascii", false})
    .Case("left", VeloxFunction{"spark_left", false})
    .Case("overlay", VeloxFunction{"spark_overlay", false})
    .Case("octet_length", VeloxFunction{"pg_octet_length", false})
    .Case("regexp_like", VeloxFunction{"presto_regexp_like", false})
    .Case("md5", VeloxFunction{"pg_md5", false})
    .Case("to_hex", VeloxFunction{"pg_to_hex", false})
    .Case("right", VeloxFunction{"pg_right", false})
    .Case("string_to_array", VeloxFunction{"pg_string_to_array", false})
    .Case("to_bin", VeloxFunction{"pg_to_bin", false})
    .Case("to_oct", VeloxFunction{"pg_to_oct", false})
    .Case("encode", VeloxFunction{"pg_encode", false})
    .Case("decode", VeloxFunction{"pg_decode", false})
    .Case("get_byte", VeloxFunction{"pg_get_byte", false})
    .Case("set_byte", VeloxFunction{"pg_set_byte", false})
    .Case("get_bit", VeloxFunction{"pg_get_bit", false})
    .Case("set_bit", VeloxFunction{"pg_set_bit", false})
    .Case("sha224", VeloxFunction{"pg_sha224", false})
    .Case("sha384", VeloxFunction{"pg_sha384", false})
    .Case("convert_from", VeloxFunction{"pg_convert_from", false})
    .Case("convert_to", VeloxFunction{"pg_convert_to", false})
    .Case("normalize", VeloxFunction{"presto_normalize", false})
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
    .Case("log", VeloxFunction{"pg_log", false})
    .Case("div", VeloxFunction{"pg_div", false})
    .Case("gcd", VeloxFunction{"pg_gcd", false})
    .Case("lcm", VeloxFunction{"pg_lcm", false})
    .Case("cosh", VeloxFunction{"presto_cosh", false})
    .Case("tanh", VeloxFunction{"presto_tanh", false})
    .Case("log10", VeloxFunction{"presto_log10", false})
    .Case("cot", VeloxFunction{"spark_cot", false})
    .Case("factorial", VeloxFunction{"spark_factorial", false})
    .Case("acosh", VeloxFunction{"spark_acosh", false})
    .Case("asinh", VeloxFunction{"spark_asinh", false})
    .Case("atanh", VeloxFunction{"spark_atanh", false})
    .Case("sinh", VeloxFunction{"spark_sinh", false})
    .Case("sind", VeloxFunction{"pg_sind", false})
    .Case("cosd", VeloxFunction{"pg_cosd", false})
    .Case("tand", VeloxFunction{"pg_tand", false})
    .Case("cotd", VeloxFunction{"pg_cotd", false})
    .Case("asind", VeloxFunction{"pg_asind", false})
    .Case("acosd", VeloxFunction{"pg_acosd", false})
    .Case("atand", VeloxFunction{"pg_atand", false})
    .Case("atan2d", VeloxFunction{"pg_atan2d", false})
    .Case("setseed", VeloxFunction{"pg_setseed", false})
    .Case("erf", VeloxFunction{"pg_erf", false})
    .Case("erfc", VeloxFunction{"pg_erfc", false})
    .Case("random_normal", VeloxFunction{"pg_random_normal", false})
    .Case("fail", VeloxFunction{"pg_error", false})
    // Date/Time functions
    .Case("date_trunc", VeloxFunction{"presto_date_trunc", false})
    .Case("extract", VeloxFunction{"pg_extract", false})
    .Case("make_date", VeloxFunction{"pg_make_date", false})
    .Case("make_timestamp", VeloxFunction{"pg_make_timestamp", false})
    .Case("to_timestamp", VeloxFunction{"pg_to_timestamp", false})
    .Case("clock_timestamp", VeloxFunction{"pg_clock_timestamp", false})
    .Case("timeofday", VeloxFunction{"pg_timeofday", false})
    .Case("isfinite", VeloxFunction{"pg_isfinite", false})
    .Case("transaction_timestamp", VeloxFunction{"presto_now", false})
    .Case("statement_timestamp", VeloxFunction{"presto_now", false})
    .Case("age", VeloxFunction{"pg_age", false})
    .Case("date_bin", VeloxFunction{"pg_date_bin", false})
    .Case("gen_random_uuid", VeloxFunction{"presto_uuid", false})
    // Array functions
    .Case("array_position", VeloxFunction{"pg_array_position", false})
    .Case("cardinality", VeloxFunction{"presto_cardinality", false})
    .Case("array_length", VeloxFunction{"pg_array_length", false})
    .Case("array_to_string",
          VeloxFunction{"presto_array_join", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("array_sort", VeloxFunction{"presto_array_sort", false})
    .Case("array_remove", VeloxFunction{"presto_array_remove", false})
    .Case("trim_array", VeloxFunction{"presto_trim_array", false})
    .Case("array_reverse", VeloxFunction{"presto_reverse", false})
    .Case("array_append", VeloxFunction{"spark_array_append", false})
    .Case("array_positions", VeloxFunction{"pg_array_positions", false})
    .Case("array_replace", VeloxFunction{"pg_array_replace", false})
    .Case("array_cat", VeloxFunction{"presto_concat", false})
    .Case("array_prepend", VeloxFunction{"spark_array_prepend", false})
    .Case("array_ndims", VeloxFunction{"pg_array_ndims", false})
    .Case("array_lower", VeloxFunction{"pg_array_lower", false})
    .Case("array_upper", VeloxFunction{"pg_array_upper", false})
    .Case("array_dims", VeloxFunction{"pg_array_dims", false})
    // JSON functions
    .Case("json_array_length", VeloxFunction{"presto_json_array_length", false})
    .Case("json_typeof", VeloxFunction{"pg_json_typeof", false})
    .Case("json_strip_nulls", VeloxFunction{"pg_json_strip_nulls", false})
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
    .Case("corr",
          VeloxFunction{"presto_corr", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Aggregate})
    .Case("regr_slope",
          VeloxFunction{"presto_regr_slope", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Aggregate})
    .Case("regr_intercept",
          VeloxFunction{"presto_regr_intercept", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Aggregate})
    .Case("regr_count",
          VeloxFunction{"presto_regr_count", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Aggregate})
    .Case("regr_r2",
          VeloxFunction{"presto_regr_r2", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Aggregate})
    .Case("regr_avgx",
          VeloxFunction{"presto_regr_avgx", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Aggregate})
    .Case("regr_avgy",
          VeloxFunction{"presto_regr_avgy", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Aggregate})
    .Case("regr_sxx",
          VeloxFunction{"presto_regr_sxx", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Aggregate})
    .Case("regr_sxy",
          VeloxFunction{"presto_regr_sxy", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Aggregate})
    .Case("regr_syy",
          VeloxFunction{"presto_regr_syy", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Aggregate})
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
    .Case("lag",
          VeloxFunction{"presto_lag", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Window})
    .Case("lead",
          VeloxFunction{"presto_lead", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Window})
    .Case("first_value",
          VeloxFunction{"presto_first_value", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Window})
    .Case("last_value",
          VeloxFunction{"presto_last_value", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Window})
    // PostgreSQL system functions
    .Case("num_nonnulls", VeloxFunction{"pg_num_nonnulls", false})
    .Case("num_nulls", VeloxFunction{"pg_num_nulls", false})
    .Case("current_schema",
          VeloxFunction{"pg_current_schema", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("current_schemas",
          VeloxFunction{"pg_current_schemas", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("set_config",
          VeloxFunction{"pg_set_config", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("current_setting",
          VeloxFunction{"pg_current_setting", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("json_extract_path",
          VeloxFunction{"pg_json_extract_path", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("json_extract_path_text",
          VeloxFunction{"pg_json_extract_path_text", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("ts_lexize",
          VeloxFunction{"pg_ts_lexize", false, FunctionLanguage::VeloxNative,
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
    .Case("quote_ident",
          VeloxFunction{"pg_quote_ident", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("quote_literal",
          VeloxFunction{"pg_quote_literal", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("quote_nullable",
          VeloxFunction{"pg_quote_nullable", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("format_type",
          VeloxFunction{"pg_format_type", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
    .Case("nameconcatoid",
          VeloxFunction{"pg_nameconcatoid", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    // 9.27.4 System Catalog Information Functions (additional)
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
    // 9.27.5 Object Information and Addressing Functions
    // 9.27.7 Data Validity Checking Functions
    // 9.27.8 Transaction ID and Snapshot Information Functions
    .Case("mxid_age",
          VeloxFunction{"pg_mxid_age", false, FunctionLanguage::VeloxNative,
                        FunctionKind::Scalar})
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
    // 9.27.10 Control Data Functions
    // 9.27.11 Version Information Functions
    .Case("unicode_version",
          VeloxFunction{"pg_unicode_version", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    .Case("icu_unicode_version",
          VeloxFunction{"pg_icu_unicode_version", false,
                        FunctionLanguage::VeloxNative, FunctionKind::Scalar})
    // 9.27.12 WAL Summarization Information Functions
    // 9.27.1 Set-returning functions (not supported as SRFs)
    // pg_stat_get_* scalar stub functions (used by pg_stat_* views)
    // Functions taking OID, returning bigint
    // Functions taking OID, returning double precision
    // Functions taking OID, returning timestamp
    // Parameterless functions returning bigint
    // Parameterless functions returning double precision
    // Parameterless functions returning timestamp
    // Other scalar stubs
    .Case("getdatabaseencoding", VeloxFunction{"pg_getdatabaseencoding"})
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
  gPgCatalogFunctions;
containers::FlatHashMap<std::string, std::shared_ptr<Function>>
  gInfoSchemaFunctions;
containers::FlatHashMap<std::string, std::shared_ptr<View>> gPgCatalogViews;
containers::FlatHashMap<std::string, std::shared_ptr<View>> gInfoSchemaViews;

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

void VisitSystemViews(
  absl::FunctionRef<void(const catalog::View&, Oid)> visitor) {
  SDB_ASSERT(SerenedServer::Instance().isEnabled<pg::PostgresFeature>());
  for (const auto& [name, view] : gPgCatalogViews) {
    SDB_ASSERT(view);
    visitor(*view, id::kPgCatalogSchema.id());
  }
  for (const auto& [name, view] : gInfoSchemaViews) {
    SDB_ASSERT(view);
    visitor(*view, id::kPgInformationSchema.id());
  }
}

std::shared_ptr<catalog::Function> GetInfoSchemaFunction(
  std::string_view name) {
  auto it = gInfoSchemaFunctions.find(name);
  if (it != gInfoSchemaFunctions.end()) {
    return it->second;
  }
  return nullptr;
}

std::shared_ptr<catalog::Function> GetFunction(std::string_view name) {
#ifndef SDB_GTEST
  // For query building tests we need to run this without feature
  SDB_ASSERT(SerenedServer::Instance().isEnabled<pg::PostgresFeature>());
#endif
  if (auto it = gPgCatalogFunctions.find(name);
      it != gPgCatalogFunctions.end()) {
    return it->second;
  }
  FunctionLanguage language = FunctionLanguage::VeloxNative;
  bool table = false;
  FunctionKind kind = FunctionKind::Scalar;
  if (!name.starts_with("pg_") && !name.starts_with("sdb_") &&
      !name.starts_with("presto_") && !name.starts_with("spark_")) {
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

std::shared_ptr<View> GetInfoSchemaView(std::string_view name) {
  auto it = gInfoSchemaViews.find(name);
  if (it == gInfoSchemaViews.end()) {
    return nullptr;
  }
  return it->second;
}

std::shared_ptr<View> GetView(std::string_view name) {
  SDB_ASSERT(SerenedServer::Instance().isEnabled<pg::PostgresFeature>());
  auto it = gPgCatalogViews.find(name);
  if (it == gPgCatalogViews.end()) {
    return nullptr;
  }
  return it->second;
}

void RegisterSystemViews() {
  for (const auto system_view_query : kSystemViewsQueries) {
    const auto* raw_stmt = ParseSystemObject(system_view_query);
    SDB_ASSERT(raw_stmt);
    SDB_ASSERT(raw_stmt->stmt);
    SDB_ASSERT(IsA(raw_stmt->stmt, ViewStmt));
    const auto* view_stmt = castNode(ViewStmt, raw_stmt->stmt);
    auto system_view = CreateSystemView(*view_stmt);
    auto name = system_view->GetName();
    if (name.starts_with("pg_")) {
      gPgCatalogViews[name] = std::move(system_view);
    } else {
      gInfoSchemaViews[name] = std::move(system_view);
    }
  }
}

void RegisterSystemFunctions() {
  for (const auto system_func_query : kSystemFunctionsQueries) {
    const auto* raw_stmt = ParseSystemObject(system_func_query);
    SDB_ASSERT(raw_stmt);
    SDB_ASSERT(raw_stmt->stmt);
    SDB_ASSERT(IsA(raw_stmt->stmt, CreateFunctionStmt));
    const auto* create_func_stmt = castNode(CreateFunctionStmt, raw_stmt->stmt);
    auto func = CreateSystemFunction(*create_func_stmt);
    auto name = func->GetName();
    if (name.starts_with("_pg_")) {
      gInfoSchemaFunctions[name] = std::move(func);
    } else {
      gPgCatalogFunctions[name] = std::move(func);
    }
  }
}

}  // namespace sdb::pg
