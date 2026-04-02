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

#include "pg/functions/stub.h"

#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <velox/expression/DecodedArgs.h>
#include <velox/expression/FunctionMetadata.h>
#include <velox/expression/VectorFunction.h>
#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/functions/prestosql/DateTimeImpl.h>
#include <velox/type/SimpleFunctionApi.h>
#include <velox/vector/ComplexVector.h>

#include "basics/fwd.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "query/types.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg::functions {
namespace {

[[noreturn]] inline void ThrowNotSupported() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                  ERR_MSG("Function is not supported in SereneDB"));
}

template<typename T>
struct NotSupportedFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>&) {
    ThrowNotSupported();
  }
  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>&) { ThrowNotSupported(); }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>&,
                                const arg_type<int64_t>&) {
    ThrowNotSupported();
  }
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&) {
    ThrowNotSupported();
  }
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>&,
                                const arg_type<velox::Array<velox::Varchar>>&) {
    ThrowNotSupported();
  }
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>&,
                                const arg_type<velox::Array<int64_t>>&) {
    ThrowNotSupported();
  }
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Timestamp>&,
                                const arg_type<int64_t>&) {
    ThrowNotSupported();
  }
  FOLLY_ALWAYS_INLINE void call(out_type<int32_t>&, const arg_type<int64_t>&) {
    ThrowNotSupported();
  }
  FOLLY_ALWAYS_INLINE void call(out_type<int32_t>&,
                                const arg_type<velox::Varchar>&) {
    ThrowNotSupported();
  }
  FOLLY_ALWAYS_INLINE void call(out_type<Interval>&,
                                const arg_type<velox::Timestamp>&) {
    ThrowNotSupported();
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&) {
    ThrowNotSupported();
  }
  FOLLY_ALWAYS_INLINE void call(out_type<bool>&, const arg_type<int64_t>&,
                                const arg_type<int64_t>&) {
    ThrowNotSupported();
  }
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Array<velox::Varchar>>&,
                                const arg_type<int8_t>&,
                                const arg_type<int64_t>&) {
    ThrowNotSupported();
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>&,
                                const arg_type<int64_t>&,
                                const arg_type<int64_t>&,
                                const arg_type<int64_t>&) {
    ThrowNotSupported();
  }
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>&,
                                const arg_type<int64_t>&,
                                const arg_type<int64_t>&,
                                const arg_type<int32_t>&) {
    ThrowNotSupported();
  }
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Array<velox::Varchar>>&,
                                const arg_type<velox::Array<velox::Varchar>>&) {
    ThrowNotSupported();
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>&,
                                const arg_type<int64_t>&,
                                const arg_type<int64_t>&,
                                const arg_type<velox::Varchar>&,
                                const arg_type<bool>&) {
    ThrowNotSupported();
  }
};

void registerNotSupportedFunctions(const std::string& prefix) {
  velox::registerFunction<NotSupportedFunction, velox::Varchar>(
    {prefix + "get_loaded_modules"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar>(
    {prefix + "listening_channels"});
  velox::registerFunction<NotSupportedFunction, velox::Array<velox::Varchar>,
                          int8_t, OidCustomType>({prefix + "acldefault"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar,
                          velox::Array<int64_t>>({prefix + "aclexplode"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar,
                          velox::Array<velox::Varchar>>(
    {prefix + "aclexplode"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar, OidCustomType,
                          OidCustomType, velox::Varchar, bool>(
    {prefix + "makeaclitem"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar,
                          RegtypeCustomType>({prefix + "basetype"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar>(
    {prefix + "get_catalog_foreign_keys"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar>(
    {prefix + "get_keywords"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar,
                          velox::Array<velox::Varchar>>(
    {prefix + "options_to_table"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar, OidCustomType>(
    {prefix + "tablespace_databases"});
  velox::registerFunction<NotSupportedFunction, int32_t, velox::Varchar>(
    {prefix + "to_regtypemod"});

  // 9.27.5 Object Information and Addressing Functions

  velox::registerFunction<NotSupportedFunction, velox::Varchar, OidCustomType,
                          OidCustomType, int32_t>({prefix + "get_acl"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar, OidCustomType,
                          OidCustomType, int32_t>({prefix + "identify_object"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar, OidCustomType,
                          OidCustomType, int32_t>(
    {prefix + "identify_object_as_address"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar, velox::Varchar,
                          velox::Array<velox::Varchar>,
                          velox::Array<velox::Varchar>>(
    {prefix + "get_object_address"});

  // 9.27.7 Data Validity Checking Functions

  velox::registerFunction<NotSupportedFunction, velox::Varchar, velox::Varchar,
                          velox::Varchar>({prefix + "input_error_info"});

  // 9.27.8 Transaction ID and Snapshot Information Functions

  velox::registerFunction<NotSupportedFunction, int32_t, XidCustomType>(
    {prefix + "age"});
  velox::registerFunction<NotSupportedFunction, Interval, velox::Timestamp>(
    {prefix + "age"});
  velox::registerFunction<NotSupportedFunction, int32_t, XidCustomType>(
    {prefix + "mxid_age"});
  velox::registerFunction<NotSupportedFunction, Xid8CustomType>(
    {prefix + "current_xact_id"});
  velox::registerFunction<NotSupportedFunction, Xid8CustomType>(
    {prefix + "current_xact_id_if_assigned"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar, Xid8CustomType>(
    {prefix + "xact_status"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar>(
    {prefix + "current_snapshot"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar, Xid8CustomType>(
    {prefix + "snapshot_xip"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar, Xid8CustomType>(
    {prefix + "snapshot_xmax"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar, Xid8CustomType>(
    {prefix + "snapshot_xmin"});
  velox::registerFunction<NotSupportedFunction, bool, Xid8CustomType,
                          Xid8CustomType>({prefix + "visible_in_snapshot"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar, XidCustomType>(
    {prefix + "get_multixact_members"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar>(
    {prefix + "get_multixact_stats"});
  velox::registerFunction<NotSupportedFunction, Xid8CustomType>(
    {prefix + "txid_current"});
  velox::registerFunction<NotSupportedFunction, Xid8CustomType>(
    {prefix + "txid_current_if_assigned"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar>(
    {prefix + "txid_current_snapshot"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar, Xid8CustomType>(
    {prefix + "txid_snapshot_xip"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar, Xid8CustomType>(
    {prefix + "txid_snapshot_xmax"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar, Xid8CustomType>(
    {prefix + "txid_snapshot_xmin"});
  velox::registerFunction<NotSupportedFunction, bool, Xid8CustomType,
                          Xid8CustomType>(
    {prefix + "txid_visible_in_snapshot"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar, Xid8CustomType>(
    {prefix + "txid_status"});

  // 9.27.9 Committed Transaction Information Functions

  velox::registerFunction<NotSupportedFunction, velox::Timestamp,
                          XidCustomType>({prefix + "xact_commit_timestamp"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar, XidCustomType>(
    {prefix + "xact_commit_timestamp_origin"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar>(
    {prefix + "last_committed_xact"});

  // 9.27.10 Control Data Functions

  velox::registerFunction<NotSupportedFunction, velox::Varchar>(
    {prefix + "control_checkpoint"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar>(
    {prefix + "control_system"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar>(
    {prefix + "control_init"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar>(
    {prefix + "control_recovery"});

  // 9.27.12 WAL Summarization Information Functions

  velox::registerFunction<NotSupportedFunction, velox::Varchar>(
    {prefix + "available_wal_summaries"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar, int64_t,
                          int64_t, int64_t>({prefix + "wal_summary_contents"});
  velox::registerFunction<NotSupportedFunction, velox::Varchar>(
    {prefix + "get_wal_summarizer_state"});
}

template<typename T>
struct NullFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>&) { return false; }
  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Timestamp>&) { return false; }
  FOLLY_ALWAYS_INLINE bool call(out_type<int32_t>&) { return false; }

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&) {
    return false;
  }
  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>&,
                                const arg_type<int64_t>&) {
    return false;
  }
  FOLLY_ALWAYS_INLINE bool call(out_type<int64_t>&,
                                const arg_type<velox::Varchar>&) {
    return false;
  }
  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Timestamp>&,
                                const arg_type<int64_t>&) {
    return false;
  }
  FOLLY_ALWAYS_INLINE bool call(out_type<int64_t>&, const arg_type<int64_t>&) {
    return false;
  }
  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Array<velox::Varchar>>&,
                                const arg_type<int64_t>&) {
    return false;
  }

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&) {
    return false;
  }
  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>&,
                                const arg_type<int64_t>&,
                                const arg_type<int64_t>&) {
    return false;
  }
  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>&,
                                const arg_type<int64_t>&,
                                const arg_type<velox::Varchar>&) {
    return false;
  }
};

void registerNullFunctions(const std::string& prefix) {
  velox::registerFunction<NullFunction, velox::Varchar, velox::Varchar,
                          velox::Varchar>({prefix + "get_serial_sequence"});
  velox::registerFunction<NullFunction, RegclassCustomType, velox::Varchar>(
    {prefix + "to_regclass"});
  velox::registerFunction<NullFunction, RegcollationCustomType, velox::Varchar>(
    {prefix + "to_regcollation"});
  velox::registerFunction<NullFunction, RegnamespaceCustomType, velox::Varchar>(
    {prefix + "to_regnamespace"});
  velox::registerFunction<NullFunction, RegoperCustomType, velox::Varchar>(
    {prefix + "to_regoper"});
  velox::registerFunction<NullFunction, RegoperatorCustomType, velox::Varchar>(
    {prefix + "to_regoperator"});
  velox::registerFunction<NullFunction, RegprocCustomType, velox::Varchar>(
    {prefix + "to_regproc"});
  velox::registerFunction<NullFunction, RegprocedureCustomType, velox::Varchar>(
    {prefix + "to_regprocedure"});
  velox::registerFunction<NullFunction, RegroleCustomType, velox::Varchar>(
    {prefix + "to_regrole"});
  velox::registerFunction<NullFunction, RegtypeCustomType, velox::Varchar>(
    {prefix + "to_regtype"});

  // 9.27.6 Comment Information Functions
  velox::registerFunction<NullFunction, velox::Varchar, OidCustomType, int32_t>(
    {prefix + "col_description"});
  velox::registerFunction<NullFunction, velox::Varchar, OidCustomType,
                          NameCustomType>({prefix + "obj_description"});
  velox::registerFunction<NullFunction, velox::Varchar, OidCustomType>(
    {prefix + "obj_description"});
  velox::registerFunction<NullFunction, velox::Varchar, OidCustomType,
                          NameCustomType>({prefix + "shobj_description"});

  velox::registerFunction<NullFunction, int64_t, RegclassCustomType>(
    {prefix + "sequence_last_value"});

  velox::registerFunction<NullFunction, velox::Varchar, OidCustomType, int32_t>(
    {prefix + "get_function_arg_default"});

  velox::registerFunction<NullFunction, velox::Array<velox::Varchar>,
                          OidCustomType>(
    {prefix + "get_statisticsobjdef_expressions"});

  velox::registerFunction<NullFunction, velox::Varchar>(
    {prefix + "current_logfile"});
  velox::registerFunction<NullFunction, velox::Varchar, velox::Varchar>(
    {prefix + "current_logfile"});

  velox::registerFunction<NullFunction, velox::Varchar>(
    {prefix + "inet_client_addr"});
  velox::registerFunction<NullFunction, int32_t>({prefix + "inet_client_port"});
  velox::registerFunction<NullFunction, velox::Varchar>(
    {prefix + "inet_server_addr"});
  velox::registerFunction<NullFunction, int32_t>({prefix + "inet_server_port"});
  // system_user: not supported (parser lacks SVFOP_SYSTEM_USER)

  for (const auto& name : {
         "stat_get_lastscan",
         "stat_get_last_vacuum_time",
         "stat_get_last_autovacuum_time",
         "stat_get_last_analyze_time",
         "stat_get_last_autoanalyze_time",
         "stat_get_stat_reset_time",
         "stat_get_function_stat_reset_time",
         "stat_get_db_stat_reset_time",
         "stat_get_db_checksum_last_failure",
       }) {
    velox::registerFunction<NullFunction, velox::Timestamp, OidCustomType>(
      {prefix + name});
  }

  for (const auto& name : {
         "stat_get_bgwriter_stat_reset_time",
         "stat_get_checkpointer_stat_reset_time",
       }) {
    velox::registerFunction<NullFunction, velox::Timestamp>({prefix + name});
  }
}

template<typename T>
struct AlwaysTrueFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& r,
                                const arg_type<velox::Varchar>&) {
    r = true;
  }
  FOLLY_ALWAYS_INLINE void call(out_type<bool>& r, const arg_type<int64_t>&) {
    r = true;
  }

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& r,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&) {
    r = true;
  }
  FOLLY_ALWAYS_INLINE void call(out_type<bool>& r, const arg_type<int64_t>&,
                                const arg_type<velox::Varchar>&) {
    r = true;
  }

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& r,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&) {
    r = true;
  }
  FOLLY_ALWAYS_INLINE void call(out_type<bool>& r, const arg_type<int64_t>&,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&) {
    r = true;
  }
  FOLLY_ALWAYS_INLINE void call(out_type<bool>& r,
                                const arg_type<velox::Varchar>&,
                                const arg_type<int64_t>&,
                                const arg_type<velox::Varchar>&) {
    r = true;
  }
  FOLLY_ALWAYS_INLINE void call(out_type<bool>& r, const arg_type<int64_t>&,
                                const arg_type<int64_t>&,
                                const arg_type<velox::Varchar>&) {
    r = true;
  }
  FOLLY_ALWAYS_INLINE void call(out_type<bool>& r, const arg_type<int64_t>&,
                                const arg_type<int16_t>&,
                                const arg_type<velox::Varchar>&) {
    r = true;
  }
  FOLLY_ALWAYS_INLINE void call(out_type<bool>& r, const arg_type<int64_t>&,
                                const arg_type<int16_t>&,
                                const arg_type<bool>&) {
    r = true;
  }
  FOLLY_ALWAYS_INLINE void call(out_type<bool>& r,
                                const arg_type<velox::Varchar>&,
                                const arg_type<int16_t>&,
                                const arg_type<velox::Varchar>&) {
    r = true;
  }

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& r,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&) {
    r = true;
  }
  FOLLY_ALWAYS_INLINE void call(out_type<bool>& r,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&,
                                const arg_type<int16_t>&,
                                const arg_type<velox::Varchar>&) {
    r = true;
  }
  FOLLY_ALWAYS_INLINE void call(out_type<bool>& r,
                                const arg_type<velox::Varchar>&,
                                const arg_type<int64_t>&,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&) {
    r = true;
  }
  FOLLY_ALWAYS_INLINE void call(out_type<bool>& r,
                                const arg_type<velox::Varchar>&,
                                const arg_type<int64_t>&,
                                const arg_type<int16_t>&,
                                const arg_type<velox::Varchar>&) {
    r = true;
  }
  FOLLY_ALWAYS_INLINE void call(out_type<bool>& r, const arg_type<int64_t>&,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&) {
    r = true;
  }
  FOLLY_ALWAYS_INLINE void call(out_type<bool>& r, const arg_type<int64_t>&,
                                const arg_type<velox::Varchar>&,
                                const arg_type<int16_t>&,
                                const arg_type<velox::Varchar>&) {
    r = true;
  }
  FOLLY_ALWAYS_INLINE void call(out_type<bool>& r, const arg_type<int64_t>&,
                                const arg_type<int64_t>&,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&) {
    r = true;
  }
  FOLLY_ALWAYS_INLINE void call(out_type<bool>& r, const arg_type<int64_t>&,
                                const arg_type<int64_t>&,
                                const arg_type<int16_t>&,
                                const arg_type<velox::Varchar>&) {
    r = true;
  }
};

void registerAlwaysTrueFunctions(const std::string& prefix) {
  // 9.27.2 Access Privilege Inquiry Functions

  // ([name/oid user] x text/oid table x text privilege)
  velox::registerFunction<AlwaysTrueFunction, bool, velox::Varchar,
                          velox::Varchar>(
    {prefix + "has_any_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar>(
    {prefix + "has_any_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_any_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_any_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_any_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_any_column_privilege"});

  // ([name/oid user] x text/oid table x text/smallint column x text privilege)
  velox::registerFunction<AlwaysTrueFunction, bool, velox::Varchar,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, velox::Varchar, int16_t,
                          velox::Varchar>({prefix + "has_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType, int16_t,
                          velox::Varchar>({prefix + "has_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          velox::Varchar, velox::Varchar, velox::Varchar>(
    {prefix + "has_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          velox::Varchar, int16_t, velox::Varchar>(
    {prefix + "has_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          OidCustomType, velox::Varchar, velox::Varchar>(
    {prefix + "has_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          OidCustomType, int16_t, velox::Varchar>(
    {prefix + "has_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar, velox::Varchar, velox::Varchar>(
    {prefix + "has_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar, int16_t, velox::Varchar>(
    {prefix + "has_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          OidCustomType, velox::Varchar, velox::Varchar>(
    {prefix + "has_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          OidCustomType, int16_t, velox::Varchar>(
    {prefix + "has_column_privilege"});

  // ([name/oid user] x text/oid database x text privilege)
  velox::registerFunction<AlwaysTrueFunction, bool, velox::Varchar,
                          velox::Varchar>({prefix + "has_database_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar>({prefix + "has_database_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_database_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_database_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_database_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_database_privilege"});

  velox::registerFunction<AlwaysTrueFunction, bool, velox::Varchar,
                          velox::Varchar>(
    {prefix + "has_foreign_data_wrapper_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar>(
    {prefix + "has_foreign_data_wrapper_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_foreign_data_wrapper_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_foreign_data_wrapper_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_foreign_data_wrapper_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_foreign_data_wrapper_privilege"});

  velox::registerFunction<AlwaysTrueFunction, bool, velox::Varchar,
                          velox::Varchar>({prefix + "has_function_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar>({prefix + "has_function_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_function_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_function_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_function_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_function_privilege"});

  velox::registerFunction<AlwaysTrueFunction, bool, velox::Varchar,
                          velox::Varchar>({prefix + "has_language_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar>({prefix + "has_language_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_language_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_language_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_language_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_language_privilege"});

  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar>(
    {prefix + "has_largeobject_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_largeobject_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_largeobject_privilege"});

  velox::registerFunction<AlwaysTrueFunction, bool, velox::Varchar,
                          velox::Varchar>({prefix + "has_parameter_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_parameter_privilege"});

  velox::registerFunction<AlwaysTrueFunction, bool, velox::Varchar,
                          velox::Varchar>({prefix + "has_schema_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar>({prefix + "has_schema_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_schema_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_schema_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_schema_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_schema_privilege"});

  velox::registerFunction<AlwaysTrueFunction, bool, velox::Varchar,
                          velox::Varchar>({prefix + "has_sequence_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar>({prefix + "has_sequence_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_sequence_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_sequence_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_sequence_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_sequence_privilege"});

  velox::registerFunction<AlwaysTrueFunction, bool, velox::Varchar,
                          velox::Varchar>({prefix + "has_server_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar>({prefix + "has_server_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_server_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_server_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_server_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_server_privilege"});

  velox::registerFunction<AlwaysTrueFunction, bool, velox::Varchar,
                          velox::Varchar>({prefix + "has_table_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar>({prefix + "has_table_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_table_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_table_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_table_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_table_privilege"});

  velox::registerFunction<AlwaysTrueFunction, bool, velox::Varchar,
                          velox::Varchar>(
    {prefix + "has_tablespace_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar>(
    {prefix + "has_tablespace_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_tablespace_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_tablespace_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_tablespace_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_tablespace_privilege"});

  velox::registerFunction<AlwaysTrueFunction, bool, velox::Varchar,
                          velox::Varchar>({prefix + "has_type_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar>({prefix + "has_type_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_type_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_type_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_type_privilege"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          OidCustomType, velox::Varchar>(
    {prefix + "has_type_privilege"});

  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          velox::Varchar>({prefix + "has_role"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          velox::Varchar>({prefix + "has_role"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          NameCustomType, velox::Varchar>(
    {prefix + "has_role"});
  velox::registerFunction<AlwaysTrueFunction, bool, NameCustomType,
                          OidCustomType, velox::Varchar>({prefix + "has_role"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          NameCustomType, velox::Varchar>(
    {prefix + "has_role"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType,
                          OidCustomType, velox::Varchar>({prefix + "has_role"});

  velox::registerFunction<AlwaysTrueFunction, bool, velox::Varchar>(
    {prefix + "row_security_active"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType>(
    {prefix + "row_security_active"});

  // 9.27.3 Schema Visibility Inquiry Functions

  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType>(
    {prefix + "table_is_visible"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType>(
    {prefix + "function_is_visible"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType>(
    {prefix + "type_is_visible"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType>(
    {prefix + "operator_is_visible"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType>(
    {prefix + "opclass_is_visible"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType>(
    {prefix + "opfamily_is_visible"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType>(
    {prefix + "collation_is_visible"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType>(
    {prefix + "conversion_is_visible"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType>(
    {prefix + "statistics_obj_is_visible"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType>(
    {prefix + "ts_config_is_visible"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType>(
    {prefix + "ts_dict_is_visible"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType>(
    {prefix + "ts_parser_is_visible"});
  velox::registerFunction<AlwaysTrueFunction, bool, OidCustomType>(
    {prefix + "ts_template_is_visible"});

  velox::registerFunction<AlwaysTrueFunction, bool, RegclassCustomType, int16_t,
                          bool>({prefix + "column_is_updatable"});

  velox::registerFunction<AlwaysTrueFunction, bool, velox::Varchar,
                          velox::Varchar>({prefix + "input_is_valid"});
}

template<typename T>
struct AlwaysFalseFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& result) { result = false; }
  FOLLY_ALWAYS_INLINE void call(out_type<bool>& result,
                                const arg_type<int64_t>&) {
    result = false;
  }
  FOLLY_ALWAYS_INLINE void call(out_type<bool>& result,
                                const arg_type<int64_t>&,
                                const arg_type<velox::Varchar>&) {
    result = false;
  }
  FOLLY_ALWAYS_INLINE void call(out_type<bool>& result,
                                const arg_type<int64_t>&,
                                const arg_type<int64_t>&,
                                const arg_type<velox::Varchar>&) {
    result = false;
  }
};

void registerAlwaysFalseFunctions(const std::string& prefix) {
  velox::registerFunction<AlwaysFalseFunction, bool, OidCustomType>(
    {prefix + "is_other_temp_schema"});
  velox::registerFunction<AlwaysFalseFunction, bool>(
    {prefix + "jit_available"});
  velox::registerFunction<AlwaysFalseFunction, bool>(
    {prefix + "numa_available"});
  velox::registerFunction<AlwaysFalseFunction, bool, RegclassCustomType,
                          int32_t, velox::Varchar>(
    {prefix + "index_column_has_property"});
  velox::registerFunction<AlwaysFalseFunction, bool, RegclassCustomType,
                          velox::Varchar>({prefix + "index_has_property"});
  velox::registerFunction<AlwaysFalseFunction, bool, OidCustomType,
                          velox::Varchar>({prefix + "indexam_has_property"});
}

template<typename T>
struct EmptyStringFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& r,
                                const arg_type<int64_t>&) {
    r = "";
  }
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& r,
                                const arg_type<velox::Varchar>&) {
    r = "";
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& r,
                                const arg_type<int64_t>&,
                                const arg_type<bool>&) {
    r = "";
  }
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& r,
                                const arg_type<int64_t>&,
                                const arg_type<int64_t>&) {
    r = "";
  }
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& r,
                                const arg_type<velox::Varchar>&,
                                const arg_type<bool>&) {
    r = "";
  }
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& r,
                                const arg_type<velox::Varchar>&,
                                const arg_type<int64_t>&) {
    r = "";
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& r,
                                const arg_type<int64_t>&,
                                const arg_type<int32_t>&) {
    r = "";
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& r,
                                const arg_type<int64_t>&,
                                const arg_type<int64_t>&,
                                const arg_type<bool>&) {
    r = "";
  }
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& r,
                                const arg_type<int64_t>&,
                                const arg_type<int64_t>&,
                                const arg_type<int64_t>&) {
    r = "";
  }
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& r,
                                const arg_type<int64_t>&,
                                const arg_type<int64_t>&,
                                const arg_type<int32_t>&) {
    r = "";
  }
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& r,
                                const arg_type<velox::Varchar>&,
                                const arg_type<int64_t>&,
                                const arg_type<bool>&) {
    r = "";
  }
};

void registerEmptyStringFunctions(const std::string& prefix) {
  velox::registerFunction<EmptyStringFunction, velox::Varchar, velox::Varchar,
                          OidCustomType>({prefix + "get_expr"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, velox::Varchar,
                          OidCustomType, bool>({prefix + "get_expr"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, OidCustomType>(
    {prefix + "get_constraintdef"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, OidCustomType,
                          bool>({prefix + "get_constraintdef"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, OidCustomType>(
    {prefix + "get_functiondef"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, OidCustomType>(
    {prefix + "get_function_arguments"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, OidCustomType>(
    {prefix + "get_function_identity_arguments"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, OidCustomType>(
    {prefix + "get_function_result"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, OidCustomType>(
    {prefix + "get_indexdef"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, OidCustomType,
                          int32_t, bool>({prefix + "get_indexdef"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, OidCustomType>(
    {prefix + "get_partition_constraintdef"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, OidCustomType>(
    {prefix + "get_partkeydef"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, OidCustomType>(
    {prefix + "get_ruledef"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, OidCustomType,
                          bool>({prefix + "get_ruledef"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, OidCustomType>(
    {prefix + "get_statisticsobjdef"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, OidCustomType>(
    {prefix + "get_triggerdef"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, OidCustomType,
                          bool>({prefix + "get_triggerdef"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, OidCustomType>(
    {prefix + "get_viewdef"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, OidCustomType,
                          bool>({prefix + "get_viewdef"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, OidCustomType,
                          int32_t>({prefix + "get_viewdef"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, velox::Varchar>(
    {prefix + "get_viewdef"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, velox::Varchar,
                          bool>({prefix + "get_viewdef"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, OidCustomType>(
    {prefix + "tablespace_location"});
  velox::registerFunction<EmptyStringFunction, velox::Varchar, OidCustomType,
                          OidCustomType, int32_t>({prefix + "describe_object"});
}

template<typename T>
struct AlwaysZeroFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<int32_t>& r) { r = 0; }
  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& r) { r = 0; }
  FOLLY_ALWAYS_INLINE void call(out_type<double>& r) { r = 0.0; }
  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& r,
                                const arg_type<int64_t>&) {
    r = 0;
  }
  FOLLY_ALWAYS_INLINE void call(out_type<double>& r, const arg_type<int64_t>&) {
    r = 0.0;
  }
};

void registerAlwaysZeroFunctions(const std::string& prefix) {
  velox::registerFunction<AlwaysZeroFunction, OidCustomType>(
    {prefix + "my_temp_schema"});
  velox::registerFunction<AlwaysZeroFunction, double>(
    {prefix + "notification_queue_usage"});
  velox::registerFunction<AlwaysZeroFunction, int32_t>(
    {prefix + "trigger_depth"});

  // pg_stat_get_* scalar stub functions (used by pg_stat_* views)
  // These return 0 or NULL as stubs.

  for (const auto& name : {
         "stat_get_numscans",
         "stat_get_tuples_returned",
         "stat_get_tuples_fetched",
         "stat_get_tuples_inserted",
         "stat_get_tuples_updated",
         "stat_get_tuples_deleted",
         "stat_get_tuples_hot_updated",
         "stat_get_tuples_newpage_updated",
         "stat_get_live_tuples",
         "stat_get_dead_tuples",
         "stat_get_mod_since_analyze",
         "stat_get_ins_since_vacuum",
         "stat_get_vacuum_count",
         "stat_get_autovacuum_count",
         "stat_get_analyze_count",
         "stat_get_autoanalyze_count",
         "stat_get_blocks_fetched",
         "stat_get_blocks_hit",
         "stat_get_xact_numscans",
         "stat_get_xact_tuples_returned",
         "stat_get_xact_tuples_fetched",
         "stat_get_xact_tuples_inserted",
         "stat_get_xact_tuples_updated",
         "stat_get_xact_tuples_deleted",
         "stat_get_xact_tuples_hot_updated",
         "stat_get_xact_tuples_newpage_updated",
         "stat_get_function_calls",
         "stat_get_xact_function_calls",
         "stat_get_db_numbackends",
         "stat_get_db_xact_commit",
         "stat_get_db_xact_rollback",
         "stat_get_db_blocks_fetched",
         "stat_get_db_blocks_hit",
         "stat_get_db_tuples_returned",
         "stat_get_db_tuples_fetched",
         "stat_get_db_tuples_inserted",
         "stat_get_db_tuples_updated",
         "stat_get_db_tuples_deleted",
         "stat_get_db_conflict_all",
         "stat_get_db_temp_files",
         "stat_get_db_temp_bytes",
         "stat_get_db_deadlocks",
         "stat_get_db_checksum_failures",
         "stat_get_db_sessions",
         "stat_get_db_sessions_abandoned",
         "stat_get_db_sessions_fatal",
         "stat_get_db_sessions_killed",
         "stat_get_db_parallel_workers_to_launch",
         "stat_get_db_parallel_workers_launched",
         "stat_get_db_conflict_tablespace",
         "stat_get_db_conflict_lock",
         "stat_get_db_conflict_snapshot",
         "stat_get_db_conflict_bufferpin",
         "stat_get_db_conflict_startup_deadlock",
         "stat_get_db_conflict_logicalslot",
       }) {
    velox::registerFunction<AlwaysZeroFunction, int64_t, OidCustomType>(
      {prefix + name});
  }

  // Functions taking OID, returning double precision (0.0)
  for (const auto& name : {
         "stat_get_total_vacuum_time",
         "stat_get_total_autovacuum_time",
         "stat_get_total_analyze_time",
         "stat_get_total_autoanalyze_time",
         "stat_get_function_total_time",
         "stat_get_function_self_time",
         "stat_get_xact_function_total_time",
         "stat_get_xact_function_self_time",
         "stat_get_db_blk_read_time",
         "stat_get_db_blk_write_time",
         "stat_get_db_session_time",
         "stat_get_db_active_time",
         "stat_get_db_idle_in_transaction_time",
       }) {
    velox::registerFunction<AlwaysZeroFunction, double, OidCustomType>(
      {prefix + name});
  }

  for (const auto& name : {
         "stat_get_bgwriter_buf_written_clean",
         "stat_get_bgwriter_maxwritten_clean",
         "stat_get_buf_alloc",
         "stat_get_checkpointer_num_timed",
         "stat_get_checkpointer_num_requested",
         "stat_get_checkpointer_num_performed",
         "stat_get_checkpointer_restartpoints_timed",
         "stat_get_checkpointer_restartpoints_requested",
         "stat_get_checkpointer_restartpoints_performed",
         "stat_get_checkpointer_buffers_written",
         "stat_get_checkpointer_slru_written",
       }) {
    velox::registerFunction<AlwaysZeroFunction, int64_t>({prefix + name});
  }

  for (const auto& name : {
         "stat_get_checkpointer_write_time",
         "stat_get_checkpointer_sync_time",
       }) {
    velox::registerFunction<AlwaysZeroFunction, double>({prefix + name});
  }
}

template<typename T>
struct PgBlockingPidsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Array<int32_t>>&,
                                const arg_type<int32_t>&) {}
};

template<typename T>
struct EmptyTextArrayFromText {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Array<velox::Varchar>>& result,
                                const arg_type<velox::Varchar>&) {}
};

}  // namespace

void registerStubFunctions(const std::string& prefix) {
  registerNotSupportedFunctions(prefix);
  registerNullFunctions(prefix);
  registerAlwaysTrueFunctions(prefix);
  registerAlwaysFalseFunctions(prefix);
  registerEmptyStringFunctions(prefix);
  registerAlwaysZeroFunctions(prefix);

  velox::registerFunction<PgBlockingPidsFunction, velox::Array<int32_t>,
                          int32_t>({prefix + "blocking_pids"});
  velox::registerFunction<PgBlockingPidsFunction, velox::Array<int32_t>,
                          int32_t>({prefix + "safe_snapshot_blocking_pids"});

  velox::registerFunction<EmptyTextArrayFromText, velox::Array<velox::Varchar>,
                          velox::Varchar>({prefix + "settings_get_flags"});
}

}  // namespace sdb::pg::functions
