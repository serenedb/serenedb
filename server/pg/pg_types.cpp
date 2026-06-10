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

#include "pg/pg_types.h"

#include <absl/base/internal/endian.h>
#include <absl/container/flat_hash_map.h>
#include <absl/strings/escaping.h>
#include <absl/strings/numbers.h>
#include <absl/time/civil_time.h>

#include <duckdb/common/extension_type_info.hpp>
#include <duckdb/common/types/time.hpp>
#include <duckdb/common/types/timestamp.hpp>

#include "basics/down_cast.h"
#include "catalog/catalog.h"
#include "catalog/user_type.h"
#include "catalog/virtual_table.h"
#include "connector/pg_logical_types.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/parse_array.h"
#include "pg/parse_record.h"
#include "pg/serialize.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "pg/system_catalog.h"

namespace sdb::pg {

#define SDB_REGTYPE_OUT(oid, type_name) \
  case PgTypeOID::oid:                  \
    return type_name;

#define SDB_REGTYPE_WITH_ARRAY_OUT(oid, type_name) \
  case PgTypeOID::oid:                             \
    return type_name;                              \
  case PgTypeOID::oid##Array:                      \
    return type_name "[]";

#define SDB_REGTYPE_IN(type_name, oid) Case(type_name, oid)

#define SDB_REGTYPE_WITH_ARRAY_IN(type_name, oid) \
  Case(type_name, oid)                            \
    .Case(type_name "[]", oid##Array)             \
    .Case("_" type_name, oid##Array)

#define SDB_OID2TYPE(oid, type_expr) \
  case oid:                          \
    return (type_expr);              \
  case oid##Array:                   \
    return LogicalType::LIST(type_expr);

int32_t Type2Oid(const duckdb::LogicalType& type, bool in_array) {
  switch (type.id()) {
    using enum duckdb::LogicalTypeId;
    using enum PgTypeOID;
    case BOOLEAN:
      return in_array ? kBoolArray : kBool;
    case TINYINT:
    case UTINYINT:
    case SMALLINT:
      return in_array ? kInt2Array : kInt2;
    case USMALLINT:
    case INTEGER:
      return in_array ? kInt4Array : kInt4;
    case UINTEGER:
      return in_array ? kInt8Array : kInt8;
    case BIGINT:
      if (IsOid(type)) {
        return in_array ? kOidArray : kOid;
      }
      if (IsXid(type)) {
        return in_array ? kXidArray : kXid;
      }
      if (IsCid(type)) {
        return in_array ? kCidArray : kCid;
      }
      if (IsTid(type)) {
        return in_array ? kTidArray : kTid;
      }
      if (IsXid8(type)) {
        return in_array ? kXid8Array : kXid8;
      }
      if (IsRegproc(type)) {
        return in_array ? kRegprocArray : kRegproc;
      }
      if (IsRegprocedure(type)) {
        return in_array ? kRegprocedureArray : kRegprocedure;
      }
      if (IsRegoper(type)) {
        return in_array ? kRegoperArray : kRegoper;
      }
      if (IsRegoperator(type)) {
        return in_array ? kRegoperatorArray : kRegoperator;
      }
      if (IsRegclass(type)) {
        return in_array ? kRegclassArray : kRegclass;
      }
      if (IsRegtype(type)) {
        return in_array ? kRegtypeArray : kRegtype;
      }
      if (IsRegrole(type)) {
        return in_array ? kRegroleArray : kRegrole;
      }
      if (IsRegnamespace(type)) {
        return in_array ? kRegnamespaceArray : kRegnamespace;
      }
      if (IsRegconfig(type)) {
        return in_array ? kRegconfigArray : kRegconfig;
      }
      if (IsRegdictionary(type)) {
        return in_array ? kRegdictionaryArray : kRegdictionary;
      }
      if (IsRegcollation(type)) {
        return in_array ? kRegcollationArray : kRegcollation;
      }
      return in_array ? kInt8Array : kInt8;
    case HUGEINT:
    case UHUGEINT:
    case BIGNUM:
    case DECIMAL:
      return in_array ? kNumericArray : kNumeric;
    case DATE:
      return in_array ? kDateArray : kDate;
    case TIME:
    case TIME_NS:
      return in_array ? kTimeArray : kTime;
    case TIMESTAMP_SEC:
    case TIMESTAMP_MS:
    case TIMESTAMP:
    case TIMESTAMP_NS:
      return in_array ? kTimestampArray : kTimestamp;
    case FLOAT:
      return in_array ? kFloat4Array : kFloat4;
    case DOUBLE:
      return in_array ? kFloat8Array : kFloat8;
    case CHAR:
      return in_array ? kTextArray : kText;
    case BLOB:
      return in_array ? kByteaArray : kBytea;
    case INTERVAL:
      return in_array ? kIntervalArray : kInterval;
    case TIMESTAMP_TZ:
      return in_array ? kTimestampTzArray : kTimestampTz;
    case TIME_TZ:
      return in_array ? kTimeTzArray : kTimeTz;
    case BIT:
      return in_array ? kVarbitArray : kVarbit;
    case UUID:
      return in_array ? kUuidArray : kUuid;
    case ENUM: {
      auto ext = type.GetExtensionInfo();
      SDB_ASSERT(ext);
      auto it = ext->properties.find(catalog::kPgSqlTypeOidProp);
      SDB_ASSERT(it != ext->properties.end());
      const ObjectId oid{it->second.GetValue<uint64_t>()};
      return (in_array ? catalog::PgSqlType::ToArrayOid(oid) : oid).id();
    }
    case STRUCT: {
      auto ext = type.GetExtensionInfo();
      // null in case of anonymous record types (e.g. SELECT ROW(1, 2))
      if (ext) {
        auto it = ext->properties.find(catalog::kPgSqlTypeOidProp);
        if (it != ext->properties.end()) {
          const ObjectId oid{it->second.GetValue<uint64_t>()};
          return (in_array ? catalog::PgSqlType::ToArrayOid(oid) : oid).id();
        }
      }
      return in_array ? kRecordArray : kRecord;
    }
    case MAP:
      return kRecordArray;
    case VARCHAR: {
      if (type.IsJSONType()) {
        return in_array ? kJsonArray : kJson;
      }
      if (IsName(type)) {
        return in_array ? kNameArray : kName;
      }
      if (IsChar(type)) {
        return in_array ? kCharArray : kChar;
      }
      return in_array ? kTextArray : kText;
    }
    case UBIGINT:
      return in_array ? kNumericArray : kNumeric;
    case LIST:
      return Type2Oid(duckdb::ListType::GetChildType(type), true);
    case ARRAY:
      return Type2Oid(duckdb::ArrayType::GetChildType(type), true);
    case VARIANT:
      return in_array ? kVariantArray : kVariant;
    default:
      return kUnknown;
  }
}

duckdb::LogicalType Oid2Type(int32_t oid, const catalog::Snapshot& snapshot) {
  switch (oid) {
    using enum PgTypeOID;
    using duckdb::LogicalType;
    SDB_OID2TYPE(kBool, LogicalType::BOOLEAN)
    SDB_OID2TYPE(kBytea, LogicalType::BLOB)
    SDB_OID2TYPE(kChar, LogicalType::TINYINT)
    SDB_OID2TYPE(kName, NAME())
    SDB_OID2TYPE(kInt8, LogicalType::BIGINT)
    SDB_OID2TYPE(kInt2, LogicalType::SMALLINT)
    SDB_OID2TYPE(kInt4, LogicalType::INTEGER)
    SDB_OID2TYPE(kRegproc, REGPROC())
    SDB_OID2TYPE(kText, LogicalType::VARCHAR)
    SDB_OID2TYPE(kOid, OID())
    SDB_OID2TYPE(kTid, TID())
    SDB_OID2TYPE(kXid, XID())
    SDB_OID2TYPE(kCid, CID())
    SDB_OID2TYPE(kJson, LogicalType::JSON())
    SDB_OID2TYPE(kXid8, XID8())
    SDB_OID2TYPE(kFloat4, LogicalType::FLOAT)
    SDB_OID2TYPE(kFloat8, LogicalType::DOUBLE)
    SDB_OID2TYPE(kDate, LogicalType::DATE)
    SDB_OID2TYPE(kTime, LogicalType::TIME)
    SDB_OID2TYPE(kTimestamp, LogicalType::TIMESTAMP)
    SDB_OID2TYPE(kTimestampTz, LogicalType::TIMESTAMP_TZ)
    SDB_OID2TYPE(kInterval, LogicalType::INTERVAL)
    SDB_OID2TYPE(kTimeTz, LogicalType::TIME_TZ)
    SDB_OID2TYPE(kBit, LogicalType::BIT)
    SDB_OID2TYPE(kVarbit, LogicalType::BIT)
    SDB_OID2TYPE(kVarchar, LogicalType::VARCHAR)
    SDB_OID2TYPE(kRegprocedure, REGPROCEDURE())
    SDB_OID2TYPE(kRegoper, REGOPER())
    SDB_OID2TYPE(kRegoperator, REGOPERATOR())
    SDB_OID2TYPE(kRegclass, REGCLASS())
    SDB_OID2TYPE(kRegcollation, REGCOLLATION())
    SDB_OID2TYPE(kRegtype, REGTYPE())
    SDB_OID2TYPE(kRegrole, REGROLE())
    SDB_OID2TYPE(kRegnamespace, REGNAMESPACE())
    SDB_OID2TYPE(kUuid, LogicalType::UUID)
    SDB_OID2TYPE(kRegconfig, REGCONFIG())
    SDB_OID2TYPE(kRegdictionary, REGDICTIONARY())
    SDB_OID2TYPE(kVariant, LogicalType::VARIANT())
    default: {
      if (auto obj = snapshot.GetObject(ObjectId{static_cast<uint64_t>(oid)});
          obj && obj->GetType() == catalog::ObjectType::PgSqlType) {
        return basics::downCast<catalog::PgSqlType>(obj)->GetLogicalType();
      }
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                      ERR_MSG("cache lookup failed for type ", oid));
    }
  }
}

std::string RegtypeOut(uint64_t oid) {
  switch (static_cast<PgTypeOID>(oid)) {
    SDB_REGTYPE_WITH_ARRAY_OUT(kBool, "boolean")
    SDB_REGTYPE_WITH_ARRAY_OUT(kBytea, "bytea")
    SDB_REGTYPE_WITH_ARRAY_OUT(kChar, "char")
    SDB_REGTYPE_WITH_ARRAY_OUT(kName, "name")
    SDB_REGTYPE_WITH_ARRAY_OUT(kInt8, "bigint")
    SDB_REGTYPE_WITH_ARRAY_OUT(kInt2, "smallint")
    SDB_REGTYPE_WITH_ARRAY_OUT(kInt2Vector, "int2vector")
    SDB_REGTYPE_WITH_ARRAY_OUT(kInt4, "integer")
    SDB_REGTYPE_WITH_ARRAY_OUT(kRegproc, "regproc")
    SDB_REGTYPE_WITH_ARRAY_OUT(kText, "text")
    SDB_REGTYPE_WITH_ARRAY_OUT(kOid, "oid")
    SDB_REGTYPE_WITH_ARRAY_OUT(kTid, "tid")
    SDB_REGTYPE_WITH_ARRAY_OUT(kXid, "xid")
    SDB_REGTYPE_WITH_ARRAY_OUT(kCid, "cid")
    SDB_REGTYPE_WITH_ARRAY_OUT(kOidvector, "oidvector")
    SDB_REGTYPE_WITH_ARRAY_OUT(kPgType, "pg_type")
    SDB_REGTYPE_WITH_ARRAY_OUT(kPgAttribute, "pg_attribute")
    SDB_REGTYPE_WITH_ARRAY_OUT(kPgProc, "pg_proc")
    SDB_REGTYPE_WITH_ARRAY_OUT(kPgClass, "pg_class")
    SDB_REGTYPE_WITH_ARRAY_OUT(kJson, "json")
    SDB_REGTYPE_WITH_ARRAY_OUT(kXml, "xml")
    SDB_REGTYPE_OUT(kPgNodeTree, "pg_node_tree")
    SDB_REGTYPE_OUT(kPgNdistinct, "pg_ndistinct")
    SDB_REGTYPE_OUT(kPgDependencies, "pg_dependencies")
    SDB_REGTYPE_OUT(kPgMcvList, "pg_mcv_list")
    SDB_REGTYPE_OUT(kPgDdlCommand, "pg_ddl_command")
    SDB_REGTYPE_WITH_ARRAY_OUT(kXid8, "xid8")
    SDB_REGTYPE_WITH_ARRAY_OUT(kPoint, "point")
    SDB_REGTYPE_WITH_ARRAY_OUT(kLseg, "lseg")
    SDB_REGTYPE_WITH_ARRAY_OUT(kPath, "path")
    SDB_REGTYPE_WITH_ARRAY_OUT(kBox, "box")
    SDB_REGTYPE_WITH_ARRAY_OUT(kPolygon, "polygon")
    SDB_REGTYPE_WITH_ARRAY_OUT(kFloat4, "real")
    SDB_REGTYPE_WITH_ARRAY_OUT(kFloat8, "double precision")
    SDB_REGTYPE_OUT(kUnknown, "unknown")
    SDB_REGTYPE_WITH_ARRAY_OUT(kCircle, "circle")
    SDB_REGTYPE_WITH_ARRAY_OUT(kMoney, "money")
    SDB_REGTYPE_WITH_ARRAY_OUT(kMacaddr, "macaddr")
    SDB_REGTYPE_WITH_ARRAY_OUT(kInet, "inet")
    SDB_REGTYPE_WITH_ARRAY_OUT(kCidr, "cidr")
    SDB_REGTYPE_WITH_ARRAY_OUT(kMacaddr8, "macaddr8")
    SDB_REGTYPE_WITH_ARRAY_OUT(kAclitem, "aclitem")
    SDB_REGTYPE_WITH_ARRAY_OUT(kBpchar, "character")
    SDB_REGTYPE_WITH_ARRAY_OUT(kVarchar, "character varying")
    SDB_REGTYPE_WITH_ARRAY_OUT(kDate, "date")
    SDB_REGTYPE_WITH_ARRAY_OUT(kTime, "time")
    SDB_REGTYPE_WITH_ARRAY_OUT(kTimestamp, "timestamp without time zone")
    SDB_REGTYPE_WITH_ARRAY_OUT(kTimestampTz, "timestamp with time zone")
    SDB_REGTYPE_WITH_ARRAY_OUT(kInterval, "interval")
    SDB_REGTYPE_WITH_ARRAY_OUT(kTimeTz, "timetz")
    SDB_REGTYPE_WITH_ARRAY_OUT(kBit, "bit")
    SDB_REGTYPE_WITH_ARRAY_OUT(kVarbit, "varbit")
    SDB_REGTYPE_WITH_ARRAY_OUT(kNumeric, "numeric")
    SDB_REGTYPE_WITH_ARRAY_OUT(kRefcursor, "refcursor")
    SDB_REGTYPE_WITH_ARRAY_OUT(kRegprocedure, "regprocedure")
    SDB_REGTYPE_WITH_ARRAY_OUT(kRegoper, "regoper")
    SDB_REGTYPE_WITH_ARRAY_OUT(kRegoperator, "regoperator")
    SDB_REGTYPE_WITH_ARRAY_OUT(kRegclass, "regclass")
    SDB_REGTYPE_WITH_ARRAY_OUT(kRegcollation, "regcollation")
    SDB_REGTYPE_WITH_ARRAY_OUT(kRegtype, "regtype")
    SDB_REGTYPE_WITH_ARRAY_OUT(kRegrole, "regrole")
    SDB_REGTYPE_WITH_ARRAY_OUT(kRegnamespace, "regnamespace")
    SDB_REGTYPE_WITH_ARRAY_OUT(kUuid, "uuid")
    SDB_REGTYPE_WITH_ARRAY_OUT(kPgLsn, "pg_lsn")
    SDB_REGTYPE_WITH_ARRAY_OUT(kTsvector, "tsvector")
    SDB_REGTYPE_WITH_ARRAY_OUT(kGtsvector, "gtsvector")
    SDB_REGTYPE_WITH_ARRAY_OUT(kTsquery, "tsquery")
    SDB_REGTYPE_WITH_ARRAY_OUT(kRegconfig, "regconfig")
    SDB_REGTYPE_WITH_ARRAY_OUT(kRegdictionary, "regdictionary")
    SDB_REGTYPE_WITH_ARRAY_OUT(kJsonb, "jsonb")
    SDB_REGTYPE_WITH_ARRAY_OUT(kJsonpath, "jsonpath")
    SDB_REGTYPE_WITH_ARRAY_OUT(kTxidSnapshot, "txid_snapshot")
    SDB_REGTYPE_WITH_ARRAY_OUT(kPgSnapshot, "pg_snapshot")
    SDB_REGTYPE_WITH_ARRAY_OUT(kInt4Range, "int4range")
    SDB_REGTYPE_WITH_ARRAY_OUT(kNumrange, "numrange")
    SDB_REGTYPE_WITH_ARRAY_OUT(kTsrange, "tsrange")
    SDB_REGTYPE_WITH_ARRAY_OUT(kTstzrange, "tstzrange")
    SDB_REGTYPE_WITH_ARRAY_OUT(kDaterange, "daterange")
    SDB_REGTYPE_WITH_ARRAY_OUT(kInt8Range, "int8range")
    SDB_REGTYPE_WITH_ARRAY_OUT(kInt4Multirange, "int4multirange")
    SDB_REGTYPE_WITH_ARRAY_OUT(kNummultirange, "nummultirange")
    SDB_REGTYPE_WITH_ARRAY_OUT(kTsmultirange, "tsmultirange")
    SDB_REGTYPE_WITH_ARRAY_OUT(kTstzmultirange, "tstzmultirange")
    SDB_REGTYPE_WITH_ARRAY_OUT(kDatemultirange, "datemultirange")
    SDB_REGTYPE_WITH_ARRAY_OUT(kInt8Multirange, "int8multirange")
    SDB_REGTYPE_WITH_ARRAY_OUT(kRecord, "record")
    SDB_REGTYPE_WITH_ARRAY_OUT(kCstring, "cstring")
    SDB_REGTYPE_OUT(kAny, "any")
    SDB_REGTYPE_OUT(kAnyarray, "anyarray")
    SDB_REGTYPE_OUT(kVoid, "void")
    SDB_REGTYPE_OUT(kTrigger, "trigger")
    SDB_REGTYPE_OUT(kEventTrigger, "event_trigger")
    SDB_REGTYPE_OUT(kLanguageHandler, "language_handler")
    SDB_REGTYPE_OUT(kInternal, "internal")
    SDB_REGTYPE_OUT(kAnyelement, "anyelement")
    SDB_REGTYPE_OUT(kAnynonarray, "anynonarray")
    SDB_REGTYPE_OUT(kAnyenum, "anyenum")
    SDB_REGTYPE_OUT(kFdwHandler, "fdw_handler")
    SDB_REGTYPE_OUT(kIndexAmHandler, "index_am_handler")
    SDB_REGTYPE_OUT(kTsmHandler, "tsm_handler")
    SDB_REGTYPE_OUT(kTableAmHandler, "table_am_handler")
    SDB_REGTYPE_OUT(kAnyrange, "anyrange")
    SDB_REGTYPE_OUT(kAnycompatible, "anycompatible")
    SDB_REGTYPE_OUT(kAnycompatiblearray, "anycompatiblearray")
    SDB_REGTYPE_OUT(kAnycompatiblenonarray, "anycompatiblenonarray")
    SDB_REGTYPE_OUT(kAnycompatiblerange, "anycompatiblerange")
    SDB_REGTYPE_OUT(kAnymultirange, "anymultirange")
    SDB_REGTYPE_OUT(kAnycompatiblemultirange, "anycompatiblemultirange")
    SDB_REGTYPE_OUT(kPgBrinBloomSummary, "pg_brin_bloom_summary")
    SDB_REGTYPE_OUT(kPgBrinMinmaxMultiSummary, "pg_brin_minmax_multi_summary")
    SDB_REGTYPE_WITH_ARRAY_OUT(kVariant, "variant")
  }
  return absl::StrCat(oid);
}

uint64_t RegtypeIn(std::string_view name) {
  static const absl::flat_hash_map<std::string_view, PgTypeOID> kTypeNameToOid =
    [] {
      struct Builder {
        absl::flat_hash_map<std::string_view, PgTypeOID> map;
        Builder& Case(std::string_view name, PgTypeOID oid) {
          map.emplace(name, oid);
          return *this;
        }
      } builder;
      using enum PgTypeOID;
      builder.SDB_REGTYPE_WITH_ARRAY_IN("bool", kBool)
        .SDB_REGTYPE_WITH_ARRAY_IN("boolean", kBool)
        .SDB_REGTYPE_WITH_ARRAY_IN("bytea", kBytea)
        .SDB_REGTYPE_WITH_ARRAY_IN("char", kChar)
        .SDB_REGTYPE_WITH_ARRAY_IN("name", kName)
        .SDB_REGTYPE_WITH_ARRAY_IN("int8", kInt8)
        .SDB_REGTYPE_WITH_ARRAY_IN("bigint", kInt8)
        .SDB_REGTYPE_WITH_ARRAY_IN("int2", kInt2)
        .SDB_REGTYPE_WITH_ARRAY_IN("smallint", kInt2)
        .SDB_REGTYPE_WITH_ARRAY_IN("int2vector", kInt2Vector)
        .SDB_REGTYPE_WITH_ARRAY_IN("int", kInt4)
        .SDB_REGTYPE_WITH_ARRAY_IN("int4", kInt4)
        .SDB_REGTYPE_WITH_ARRAY_IN("integer", kInt4)
        .SDB_REGTYPE_WITH_ARRAY_IN("regproc", kRegproc)
        .SDB_REGTYPE_WITH_ARRAY_IN("text", kText)
        .SDB_REGTYPE_WITH_ARRAY_IN("oid", kOid)
        .SDB_REGTYPE_WITH_ARRAY_IN("tid", kTid)
        .SDB_REGTYPE_WITH_ARRAY_IN("xid", kXid)
        .SDB_REGTYPE_WITH_ARRAY_IN("cid", kCid)
        .SDB_REGTYPE_WITH_ARRAY_IN("oidvector", kOidvector)
        .SDB_REGTYPE_WITH_ARRAY_IN("pg_type", kPgType)
        .SDB_REGTYPE_WITH_ARRAY_IN("pg_attribute", kPgAttribute)
        .SDB_REGTYPE_WITH_ARRAY_IN("pg_proc", kPgProc)
        .SDB_REGTYPE_WITH_ARRAY_IN("pg_class", kPgClass)
        .SDB_REGTYPE_WITH_ARRAY_IN("json", kJson)
        .SDB_REGTYPE_WITH_ARRAY_IN("xml", kXml)
        .SDB_REGTYPE_IN("pg_node_tree", kPgNodeTree)
        .SDB_REGTYPE_IN("pg_ndistinct", kPgNdistinct)
        .SDB_REGTYPE_IN("pg_dependencies", kPgDependencies)
        .SDB_REGTYPE_IN("pg_mcv_list", kPgMcvList)
        .SDB_REGTYPE_IN("pg_ddl_command", kPgDdlCommand)
        .SDB_REGTYPE_WITH_ARRAY_IN("xid8", kXid8)
        .SDB_REGTYPE_WITH_ARRAY_IN("point", kPoint)
        .SDB_REGTYPE_WITH_ARRAY_IN("lseg", kLseg)
        .SDB_REGTYPE_WITH_ARRAY_IN("path", kPath)
        .SDB_REGTYPE_WITH_ARRAY_IN("box", kBox)
        .SDB_REGTYPE_WITH_ARRAY_IN("polygon", kPolygon)
        .SDB_REGTYPE_WITH_ARRAY_IN("float4", kFloat4)
        .SDB_REGTYPE_WITH_ARRAY_IN("real", kFloat4)
        .SDB_REGTYPE_WITH_ARRAY_IN("float8", kFloat8)
        .SDB_REGTYPE_WITH_ARRAY_IN("double precision", kFloat8)
        .SDB_REGTYPE_IN("unknown", kUnknown)
        .SDB_REGTYPE_WITH_ARRAY_IN("circle", kCircle)
        .SDB_REGTYPE_WITH_ARRAY_IN("money", kMoney)
        .SDB_REGTYPE_WITH_ARRAY_IN("macaddr", kMacaddr)
        .SDB_REGTYPE_WITH_ARRAY_IN("inet", kInet)
        .SDB_REGTYPE_WITH_ARRAY_IN("cidr", kCidr)
        .SDB_REGTYPE_WITH_ARRAY_IN("macaddr8", kMacaddr8)
        .SDB_REGTYPE_WITH_ARRAY_IN("aclitem", kAclitem)
        .SDB_REGTYPE_WITH_ARRAY_IN("bpchar", kBpchar)
        .SDB_REGTYPE_WITH_ARRAY_IN("character", kBpchar)
        .SDB_REGTYPE_WITH_ARRAY_IN("varchar", kVarchar)
        .SDB_REGTYPE_WITH_ARRAY_IN("character varying", kVarchar)
        .SDB_REGTYPE_WITH_ARRAY_IN("date", kDate)
        .SDB_REGTYPE_WITH_ARRAY_IN("timestamp", kTimestamp)
        .SDB_REGTYPE_WITH_ARRAY_IN("timestamp without time zone", kTimestamp)
        .SDB_REGTYPE_WITH_ARRAY_IN("timestamptz", kTimestampTz)
        .SDB_REGTYPE_WITH_ARRAY_IN("timestamp with time zone", kTimestampTz)
        .SDB_REGTYPE_WITH_ARRAY_IN("time", kTime)
        .SDB_REGTYPE_WITH_ARRAY_IN("time without time zone", kTime)
        .SDB_REGTYPE_WITH_ARRAY_IN("timetz", kTimeTz)
        .SDB_REGTYPE_WITH_ARRAY_IN("time with time zone", kTimeTz)
        .SDB_REGTYPE_WITH_ARRAY_IN("interval", kInterval)
        .SDB_REGTYPE_WITH_ARRAY_IN("bit", kBit)
        .SDB_REGTYPE_WITH_ARRAY_IN("varbit", kVarbit)
        .SDB_REGTYPE_WITH_ARRAY_IN("numeric", kNumeric)
        .SDB_REGTYPE_WITH_ARRAY_IN("refcursor", kRefcursor)
        .SDB_REGTYPE_WITH_ARRAY_IN("regprocedure", kRegprocedure)
        .SDB_REGTYPE_WITH_ARRAY_IN("regoper", kRegoper)
        .SDB_REGTYPE_WITH_ARRAY_IN("regoperator", kRegoperator)
        .SDB_REGTYPE_WITH_ARRAY_IN("regclass", kRegclass)
        .SDB_REGTYPE_WITH_ARRAY_IN("regcollation", kRegcollation)
        .SDB_REGTYPE_WITH_ARRAY_IN("regtype", kRegtype)
        .SDB_REGTYPE_WITH_ARRAY_IN("regrole", kRegrole)
        .SDB_REGTYPE_WITH_ARRAY_IN("regnamespace", kRegnamespace)
        .SDB_REGTYPE_WITH_ARRAY_IN("uuid", kUuid)
        .SDB_REGTYPE_WITH_ARRAY_IN("pg_lsn", kPgLsn)
        .SDB_REGTYPE_WITH_ARRAY_IN("tsvector", kTsvector)
        .SDB_REGTYPE_WITH_ARRAY_IN("gtsvector", kGtsvector)
        .SDB_REGTYPE_WITH_ARRAY_IN("tsquery", kTsquery)
        .SDB_REGTYPE_WITH_ARRAY_IN("regconfig", kRegconfig)
        .SDB_REGTYPE_WITH_ARRAY_IN("regdictionary", kRegdictionary)
        .SDB_REGTYPE_WITH_ARRAY_IN("jsonb", kJsonb)
        .SDB_REGTYPE_WITH_ARRAY_IN("jsonpath", kJsonpath)
        .SDB_REGTYPE_WITH_ARRAY_IN("txid_snapshot", kTxidSnapshot)
        .SDB_REGTYPE_WITH_ARRAY_IN("pg_snapshot", kPgSnapshot)
        .SDB_REGTYPE_WITH_ARRAY_IN("int4range", kInt4Range)
        .SDB_REGTYPE_WITH_ARRAY_IN("numrange", kNumrange)
        .SDB_REGTYPE_WITH_ARRAY_IN("tsrange", kTsrange)
        .SDB_REGTYPE_WITH_ARRAY_IN("tstzrange", kTstzrange)
        .SDB_REGTYPE_WITH_ARRAY_IN("daterange", kDaterange)
        .SDB_REGTYPE_WITH_ARRAY_IN("int8range", kInt8Range)
        .SDB_REGTYPE_WITH_ARRAY_IN("int4multirange", kInt4Multirange)
        .SDB_REGTYPE_WITH_ARRAY_IN("nummultirange", kNummultirange)
        .SDB_REGTYPE_WITH_ARRAY_IN("tsmultirange", kTsmultirange)
        .SDB_REGTYPE_WITH_ARRAY_IN("tstzmultirange", kTstzmultirange)
        .SDB_REGTYPE_WITH_ARRAY_IN("datemultirange", kDatemultirange)
        .SDB_REGTYPE_WITH_ARRAY_IN("int8multirange", kInt8Multirange)
        .SDB_REGTYPE_WITH_ARRAY_IN("record", kRecord)
        .SDB_REGTYPE_WITH_ARRAY_IN("cstring", kCstring)
        .SDB_REGTYPE_IN("any", kAny)
        .SDB_REGTYPE_IN("anyarray", kAnyarray)
        .SDB_REGTYPE_IN("void", kVoid)
        .SDB_REGTYPE_IN("trigger", kTrigger)
        .SDB_REGTYPE_IN("event_trigger", kEventTrigger)
        .SDB_REGTYPE_IN("language_handler", kLanguageHandler)
        .SDB_REGTYPE_IN("internal", kInternal)
        .SDB_REGTYPE_IN("anyelement", kAnyelement)
        .SDB_REGTYPE_IN("anynonarray", kAnynonarray)
        .SDB_REGTYPE_IN("anyenum", kAnyenum)
        .SDB_REGTYPE_IN("fdw_handler", kFdwHandler)
        .SDB_REGTYPE_IN("index_am_handler", kIndexAmHandler)
        .SDB_REGTYPE_IN("tsm_handler", kTsmHandler)
        .SDB_REGTYPE_IN("table_am_handler", kTableAmHandler)
        .SDB_REGTYPE_IN("anyrange", kAnyrange)
        .SDB_REGTYPE_IN("anycompatible", kAnycompatible)
        .SDB_REGTYPE_IN("anycompatiblearray", kAnycompatiblearray)
        .SDB_REGTYPE_IN("anycompatiblenonarray", kAnycompatiblenonarray)
        .SDB_REGTYPE_IN("anycompatiblerange", kAnycompatiblerange)
        .SDB_REGTYPE_IN("anymultirange", kAnymultirange)
        .SDB_REGTYPE_IN("anycompatiblemultirange", kAnycompatiblemultirange)
        .SDB_REGTYPE_IN("pg_brin_bloom_summary", kPgBrinBloomSummary)
        .SDB_REGTYPE_IN("pg_brin_minmax_multi_summary",
                        kPgBrinMinmaxMultiSummary)
        .SDB_REGTYPE_WITH_ARRAY_IN("variant", kVariant);
      return std::move(builder.map);
    }();
  if (auto it = kTypeNameToOid.find(name); it != kTypeNameToOid.end()) {
    return static_cast<uint64_t>(it->second);
  }
  return kInvalidOid;
}

namespace {

// Decode PG numeric binary format to a decimal string.
// Format: int16 ndigits, int16 weight, uint16 sign, int16 dscale,
// then ndigitsxint16 base-10000 digits.
std::string PgNumericToString(std::string_view data) {
  if (data.size() < 8) {
    return "";
  }
  const int16_t ndigits =
    static_cast<int16_t>(absl::big_endian::Load16(data.data()));
  const int16_t weight =
    static_cast<int16_t>(absl::big_endian::Load16(data.data() + 2));
  const uint16_t sign = absl::big_endian::Load16(data.data() + 4);
  const int16_t dscale =
    static_cast<int16_t>(absl::big_endian::Load16(data.data() + 6));
  if (static_cast<size_t>(8 + ndigits * 2) > data.size()) {
    return "";
  }
  if (sign == 0xC000) {
    return "NaN";
  }

  std::string result;
  if (sign == 0x4000) {
    result += '-';
  }

  auto load_digit = [&](int i) -> unsigned {
    if (i < 0 || i >= ndigits) {
      return 0;
    }
    return absl::big_endian::Load16(data.data() + 8 + i * 2);
  };

  // Integer part: digit groups at positions 0..weight
  if (weight >= 0) {
    result += std::to_string(load_digit(0));
    for (int i = 1; i <= weight; ++i) {
      char buf[5];
      snprintf(buf, sizeof(buf), "%04u", load_digit(i));
      result += buf;
    }
  } else {
    result += '0';
  }

  if (dscale > 0) {
    result += '.';
    int written = 0;
    int frac_start = weight + 1;
    // Gap of leading zeros when first digit is below the decimal point by >1
    if (frac_start < 0) {
      int leading = (-frac_start) * 4;
      int take = std::min(leading, (int)dscale);
      result.append(take, '0');
      written += take;
      frac_start = 0;
    }
    for (int i = frac_start; i < ndigits && written < dscale; ++i) {
      char buf[5];
      snprintf(buf, sizeof(buf), "%04u", load_digit(i));
      int take = std::min(4, dscale - written);
      result.append(buf, take);
      written += take;
    }
    while (written < dscale) {
      result += '0';
      ++written;
    }
  }
  return result;
}

std::unexpected<DeserializeError> MakeInvalid() {
  return std::unexpected{DeserializeError::InvalidRepresentation};
}

std::expected<duckdb::Value, DeserializeError> DeserializeBinaryParameter(
  const duckdb::LogicalType& type, std::string_view data,
  const catalog::Snapshot& snapshot) {
  switch (type.id()) {
    using enum duckdb::LogicalTypeId;
    case BOOLEAN: {
      if (data.size() == 1) {
        return duckdb::Value::BOOLEAN(data[0] != 0);
      }
    } break;
    case TINYINT: {
      if (data.size() == 2) {
        return duckdb::Value::TINYINT(
          static_cast<int8_t>(absl::big_endian::Load16(data.data())));
      }
    } break;
    case SMALLINT: {
      if (data.size() == 2) {
        return duckdb::Value::SMALLINT(
          absl::big_endian::Load<int16_t>(data.data()));
      }
    } break;
    case INTEGER: {
      if (data.size() == 4) {
        return duckdb::Value::INTEGER(
          absl::big_endian::Load<int32_t>(data.data()));
      }
    } break;
    case BIGINT: {
      if (IsOidLike(type)) {
        if (data.size() == 4) {
          return duckdb::Value::BIGINT(
            static_cast<int64_t>(absl::big_endian::Load<int32_t>(data.data())));
        }
      } else if (data.size() == 8) {
        return duckdb::Value::BIGINT(
          absl::big_endian::Load<int64_t>(data.data()));
      }
    } break;
    case UTINYINT: {
      if (data.size() == 2) {
        return duckdb::Value::UTINYINT(
          static_cast<uint8_t>(absl::big_endian::Load16(data.data())));
      }
    } break;
    case USMALLINT: {
      if (data.size() == 4) {
        return duckdb::Value::USMALLINT(
          static_cast<uint16_t>(absl::big_endian::Load32(data.data())));
      }
    } break;
    case UINTEGER: {
      if (data.size() == 8) {
        return duckdb::Value::UINTEGER(
          static_cast<uint32_t>(absl::big_endian::Load64(data.data())));
      }
    } break;
    case FLOAT: {
      if (data.size() == 4) {
        return duckdb::Value::FLOAT(absl::big_endian::Load<float>(data.data()));
      }
    } break;
    case DOUBLE: {
      if (data.size() == 8) {
        return duckdb::Value::DOUBLE(
          absl::big_endian::Load<double>(data.data()));
      }
    } break;
    case VARCHAR: {
      return duckdb::Value{data};
    }
    case BLOB: {
      return duckdb::Value::BLOB(duckdb::const_data_ptr_cast(data.data()),
                                 data.size());
    }
    case TIMESTAMP: {
      if (data.size() == 8) {
        const auto us = absl::big_endian::Load<int64_t>(data.data());
        return duckdb::Value::TIMESTAMP(duckdb::timestamp_t{us + kGapUs});
      }
    } break;
    case TIMESTAMP_TZ: {
      if (data.size() == 8) {
        const auto us = absl::big_endian::Load<int64_t>(data.data());
        return duckdb::Value::TIMESTAMPTZ(duckdb::timestamp_tz_t{us + kGapUs});
      }
    } break;
    case TIME: {
      if (data.size() == 8) {
        const auto us = absl::big_endian::Load<int64_t>(data.data());
        return duckdb::Value::TIME(duckdb::dtime_t{us});
      }
    } break;
    case TIME_TZ: {
      if (data.size() != 12) {
        return MakeInvalid();
      }
      const auto us = absl::big_endian::Load<int64_t>(data.data());
      // PG zone is seconds WEST; DuckDB offset() is seconds EAST -- negate.
      const auto pg_zone = absl::big_endian::Load<int32_t>(data.data() + 8);
      return duckdb::Value::TIMETZ(
        duckdb::dtime_tz_t{duckdb::dtime_t{us}, -pg_zone});
    }
    case DATE: {
      if (data.size() != 4) {
        return MakeInvalid();
      }
      const auto days = absl::big_endian::Load<int32_t>(data.data());
      if (days == std::numeric_limits<int32_t>::max()) {
        return duckdb::Value::DATE(duckdb::date_t::infinity());
      }
      if (days == std::numeric_limits<int32_t>::min()) {
        return duckdb::Value::DATE(duckdb::date_t::ninfinity());
      }
      return duckdb::Value::DATE(duckdb::date_t(days + kGapDays));
    }
    case UUID: {
      if (data.size() != 16) {
        return MakeInvalid();
      }
      duckdb::hugeint_t val;
      uint64_t raw_high = absl::big_endian::Load64(data.data());
      // Inverse of serialize: flip top bit back
      val.upper = static_cast<int64_t>(raw_high ^ (uint64_t{1} << 63));
      val.lower = absl::big_endian::Load64(data.data() + 8);
      return duckdb::Value::UUID(val);
    }
    case INTERVAL: {
      if (data.size() != 16) {
        return MakeInvalid();
      }
      duckdb::interval_t interval;
      interval.micros =
        static_cast<int64_t>(absl::big_endian::Load64(data.data()));
      interval.days =
        static_cast<int32_t>(absl::big_endian::Load32(data.data() + 8));
      interval.months =
        static_cast<int32_t>(absl::big_endian::Load32(data.data() + 12));
      return duckdb::Value::INTERVAL(interval.months, interval.days,
                                     interval.micros);
    }
    case BIT: {
      if (data.size() < 4) {
        return MakeInvalid();
      }
      const auto nbits = absl::big_endian::Load<int32_t>(data.data());
      if (nbits < 0) {
        return MakeInvalid();
      }
      size_t nbytes = (static_cast<size_t>(nbits) + 7) / 8;
      if (data.size() < 4 + nbytes) {
        return MakeInvalid();
      }
      std::string bits;
      bits.reserve(nbits);
      for (int32_t b = 0; b < nbits; ++b) {
        uint8_t byte_val = static_cast<uint8_t>(data[4 + b / 8]);
        bits += ((byte_val >> (7 - b % 8)) & 1) ? '1' : '0';
      }
      return duckdb::Value::BIT(bits);
    }
    case DECIMAL:
    case HUGEINT:
    case UHUGEINT: {
      auto str = PgNumericToString(data);
      if (str.empty()) {
        return MakeInvalid();
      }
      return duckdb::Value{str}.DefaultCastAs(type);
    }
    case LIST: {
      if (data.size() < 12) {
        return MakeInvalid();
      }
      const auto ndims = absl::big_endian::Load<int32_t>(data.data());
      // flags at +4 (ignored)
      const auto elem_oid = absl::big_endian::Load<int32_t>(data.data() + 8);
      auto elem_type = Oid2Type(elem_oid, snapshot);
      if (ndims == 0) {
        return duckdb::Value::LIST(elem_type, {});
      }
      if (data.size() < 12 + static_cast<size_t>(ndims) * 8) {
        return MakeInvalid();
      }
      const auto elem_count = absl::big_endian::Load<int32_t>(data.data() + 12);
      // skip ndims x (int32 size + int32 lower_bound)
      size_t offset = 12 + static_cast<size_t>(ndims) * 8;
      std::vector<duckdb::Value> values;
      values.reserve(elem_count);
      for (int32_t i = 0; i < elem_count; ++i) {
        if (offset + 4 > data.size()) {
          return MakeInvalid();
        }
        const auto len = absl::big_endian::Load<int32_t>(data.data() + offset);
        offset += 4;
        if (len == -1) {
          values.emplace_back(elem_type);  // NULL
        } else {
          if (static_cast<size_t>(len) > data.size() - offset) {
            return MakeInvalid();
          }
          auto elem = DeserializeBinaryParameter(
            elem_type, data.substr(offset, len), snapshot);
          if (!elem) {
            return std::unexpected{elem.error()};
          }
          values.push_back(std::move(*elem));
          offset += len;
        }
      }
      return duckdb::Value::LIST(elem_type, std::move(values));
    }
    case ENUM: {
      // PG enum binary: raw label bytes (matches text format).
      duckdb::string_t key{data.data(), static_cast<uint32_t>(data.size())};
      const auto pos = duckdb::EnumType::GetPos(type, key);
      if (pos < 0) {
        return MakeInvalid();
      }
      return duckdb::Value::ENUM(static_cast<uint64_t>(pos), type);
    }
    case STRUCT: {
      // PG record binary: int32 nfields; for each field
      // { int32 type_oid, int32 length (-1 = NULL), length bytes }.
      if (data.size() < 4) {
        return MakeInvalid();
      }
      const auto nfields = absl::big_endian::Load<int32_t>(data.data());
      const auto& children = duckdb::StructType::GetChildTypes(type);
      if (nfields < 0 || static_cast<size_t>(nfields) != children.size()) {
        return MakeInvalid();
      }
      size_t offset = 4;
      duckdb::child_list_t<duckdb::Value> fields;
      fields.reserve(children.size());
      for (size_t i = 0; i < children.size(); ++i) {
        if (offset + 8 > data.size()) {
          return MakeInvalid();
        }
        // field type OID at +0 (ignored; trust caller-declared type)
        offset += 4;
        const auto len = absl::big_endian::Load<int32_t>(data.data() + offset);
        offset += 4;
        const auto& child_type = children[i].second;
        if (len == -1) {
          fields.emplace_back(children[i].first, duckdb::Value{child_type});
          continue;
        }
        if (len < 0 || static_cast<size_t>(len) > data.size() - offset) {
          return MakeInvalid();
        }
        auto field = DeserializeBinaryParameter(
          child_type, data.substr(offset, len), snapshot);
        if (!field) {
          return std::unexpected{field.error()};
        }
        fields.emplace_back(children[i].first, std::move(*field));
        offset += len;
      }
      return duckdb::Value::STRUCT(std::move(fields));
    }
    default:
      // An unsupported binary type is bad client input (e.g. a binary COPY into
      // an exotic column type), not a programmer error: return an error the
      // caller maps to a clean SQL error rather than terminating the server.
      return MakeInvalid();
  }
  return MakeInvalid();
}

std::expected<duckdb::Value, DeserializeError> DeserializeTextParameter(
  const duckdb::LogicalType& type, std::string_view data,
  const catalog::Snapshot& snapshot) {
  switch (type.id()) {
    using enum duckdb::LogicalTypeId;
    case BOOLEAN: {
      if (data == "t" || data == "1" || data == "on" || data == "yes" ||
          data == "true") {
        return duckdb::Value::BOOLEAN(true);
      }
      if (data == "f" || data == "0" || data == "off" || data == "no" ||
          data == "false") {
        return duckdb::Value::BOOLEAN(false);
      }
    } break;
    case TINYINT: {
      if (int8_t v; absl::SimpleAtoi(data, &v)) {
        return duckdb::Value::TINYINT(v);
      }
    } break;
    case SMALLINT: {
      if (int16_t v; absl::SimpleAtoi(data, &v)) {
        return duckdb::Value::SMALLINT(v);
      }
    } break;
    case INTEGER: {
      if (int32_t v; absl::SimpleAtoi(data, &v)) {
        return duckdb::Value::INTEGER(v);
      }
    } break;
    case BIGINT: {
      if (int64_t v; absl::SimpleAtoi(data, &v)) {
        return duckdb::Value::BIGINT(v);
      }
    } break;
    case UTINYINT: {
      if (uint8_t v; absl::SimpleAtoi(data, &v)) {
        return duckdb::Value::UTINYINT(v);
      }
    } break;
    case USMALLINT: {
      if (uint16_t v; absl::SimpleAtoi(data, &v)) {
        return duckdb::Value::USMALLINT(v);
      }
    } break;
    case UINTEGER: {
      if (uint32_t v; absl::SimpleAtoi(data, &v)) {
        return duckdb::Value::UINTEGER(v);
      }
    } break;
    case UBIGINT: {
      if (uint64_t v; absl::SimpleAtoi(data, &v)) {
        return duckdb::Value::UBIGINT(v);
      }
    } break;
    case FLOAT: {
      if (float v; absl::SimpleAtof(data, &v)) {
        return duckdb::Value::FLOAT(v);
      }
    } break;
    case DOUBLE: {
      if (double v; absl::SimpleAtod(data, &v)) {
        return duckdb::Value::DOUBLE(v);
      }
    } break;
    case CHAR:
    case VARCHAR:
      return duckdb::Value{data};
    case BLOB: {
      if (data.size() > 2 && data.starts_with("\\x")) {
        if (std::string bytes; absl::HexStringToBytes(data.substr(2), &bytes)) {
          return duckdb::Value::BLOB(std::move(bytes));
        }
      } else {
        return duckdb::Value::BLOB(duckdb::const_data_ptr_cast(data.data()),
                                   data.size());
      }
    } break;
    case TIMESTAMP: {
      auto timestamp = duckdb::Timestamp::FromString(std::string{data}, false);
      return duckdb::Value::TIMESTAMP(timestamp);
    }
    case TIMESTAMP_TZ: {
      auto timestamp = duckdb::Timestamp::FromString(std::string{data}, false);
      return duckdb::Value::TIMESTAMPTZ(duckdb::timestamp_tz_t{timestamp});
    }
    case TIME: {
      auto time = duckdb::Time::FromString(std::string{data});
      return duckdb::Value::TIME(time);
    }
    case TIME_TZ: {
      duckdb::dtime_tz_t tz;
      duckdb::idx_t pos = 0;
      bool has_offset = false;
      if (duckdb::Time::TryConvertTimeTZ(data.data(), data.size(), pos, tz,
                                         has_offset)) {
        return duckdb::Value::TIMETZ(tz);
      }
    } break;
    case DATE: {
      auto date = duckdb::Date::FromString(std::string{data});
      return duckdb::Value::DATE(date);
    }
    case UUID: {
      return duckdb::Value::UUID(std::string{data});
    }
    case BIT: {
      return duckdb::Value::BIT(std::string{data});
    }
    case LIST: {
      auto& elem_type = duckdb::ListType::GetChildType(type);
      std::vector<duckdb::Value> values;
      std::optional<DeserializeError> error;
      ParsePgTextArray(
        data,
        [&](std::string_view token, bool is_null) {
          if (error) {
            return;
          }
          if (is_null) {
            values.emplace_back(elem_type);
            return;
          }
          auto res = DeserializeTextParameter(elem_type, token, snapshot);
          if (!res) {
            error = res.error();
          } else {
            values.push_back(std::move(*res));
          }
        },
        [&](std::string_view) {
          error = DeserializeError::InvalidRepresentation;
        });
      if (error) {
        return std::unexpected{*error};
      }
      return duckdb::Value::LIST(elem_type, std::move(values));
    }
    case ENUM: {
      duckdb::string_t key{data.data(), static_cast<uint32_t>(data.size())};
      const auto pos = duckdb::EnumType::GetPos(type, key);
      if (pos < 0) {
        return MakeInvalid();
      }
      return duckdb::Value::ENUM(static_cast<uint64_t>(pos), type);
    }
    case STRUCT: {
      const auto& children = duckdb::StructType::GetChildTypes(type);
      duckdb::child_list_t<duckdb::Value> fields;
      fields.reserve(children.size());
      std::optional<DeserializeError> error;

      ParsePgTextRecord(
        data,
        [&](std::string_view token, bool is_null) {
          if (error) {
            return;
          }
          if (fields.size() >= children.size()) {
            error = DeserializeError::InvalidRepresentation;
            return;
          }
          const auto& [child_name, child_type] = children[fields.size()];
          if (is_null) {
            fields.emplace_back(child_name, duckdb::Value{child_type});
            return;
          }
          auto field = DeserializeTextParameter(child_type, token, snapshot);
          if (!field) {
            error = field.error();
            return;
          }
          fields.emplace_back(child_name, std::move(*field));
        },
        [&](std::string_view) {
          error = DeserializeError::InvalidRepresentation;
        });
      if (error) {
        return std::unexpected{*error};
      }
      if (fields.size() != children.size()) {
        return MakeInvalid();
      }
      return duckdb::Value::STRUCT(std::move(fields));
    }
    default:
      return duckdb::Value{data}.DefaultCastAs(type);
  }
  return MakeInvalid();
}

}  // namespace

std::expected<duckdb::Value, DeserializeError> DeserializeParameter(
  const duckdb::LogicalType& type, VarFormat format, std::string_view data,
  const catalog::Snapshot& snapshot) {
  return format == VarFormat::Text
           ? DeserializeTextParameter(type, data, snapshot)
           : DeserializeBinaryParameter(type, data, snapshot);
}

std::string RegclassOut(const catalog::Snapshot& snapshot, uint64_t oid) {
  auto object = snapshot.GetObject(ObjectId{oid});
  if (object) {
    return std::string{object->GetName()};
  }
  std::string result;
  VisitSystemTables([&](const catalog::VirtualTable& table, Oid) {
    if (table.Id() == oid) {
      result = table.GetName();
    }
  });
  if (!result.empty()) {
    return result;
  }
  return absl::StrCat(oid);
}

uint64_t RegclassIn(const ConnectionContext& ctx, std::string_view name) {
  auto snapshot = ctx.EnsureCatalogSnapshot();
  auto current_schema = ctx.GetCurrentSchema();
  auto object_name = ParseObjectName(name, current_schema);
  auto relation = snapshot->GetRelation(ctx.GetDatabaseId(), object_name.schema,
                                        object_name.relation);
  if (relation) {
    return relation->GetId();
  }
  auto* system_table = GetTable(object_name.relation);
  if (system_table) {
    return system_table->Id();
  }
  return kInvalidOid;
}

std::string RegnamespaceOut(const catalog::Snapshot& snapshot, uint64_t oid) {
  if (oid == id::kPgCatalogSchema.id()) {
    return "pg_catalog";
  }
  if (oid == id::kPgInformationSchema.id()) {
    return "information_schema";
  }
  auto object = snapshot.GetObject(ObjectId{oid});
  if (object && object->GetType() == catalog::ObjectType::Schema) {
    return std::string{object->GetName()};
  }
  return absl::StrCat(oid);
}

uint64_t RegnamespaceIn(const ConnectionContext& ctx, std::string_view name) {
  if (name == "pg_catalog") {
    return id::kPgCatalogSchema.id();
  }
  if (name == "information_schema") {
    return id::kPgInformationSchema.id();
  }
  auto snapshot = ctx.EnsureCatalogSnapshot();
  auto schema = snapshot->GetSchema(ctx.GetDatabaseId(), name);
  if (schema) {
    return schema->GetId();
  }
  return kInvalidOid;
}

std::string ToPgTypeString(const duckdb::LogicalType& type) {
  return type.ToString();
}

}  // namespace sdb::pg
