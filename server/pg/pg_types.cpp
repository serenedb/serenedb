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
#include <absl/strings/escaping.h>
#include <absl/strings/numbers.h>
#include <absl/time/civil_time.h>

#include <duckdb/common/extension_type_info.hpp>
#include <duckdb/common/types/time.hpp>
#include <duckdb/common/types/timestamp.hpp>

#include "basics/containers/flat_hash_map.h"
#include "basics/down_cast.h"
#include "catalog/catalog.h"
#include "catalog/user_type.h"
#include "catalog/virtual_table.h"
#include "connector/pg_logical_types.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
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

PgTypeInfo Logical2Pg(const duckdb::LogicalType& type, bool in_array) {
  // Arrays are varlena (typlen -1); a scalar carries its fixed width or -1. The
  // typmod (DECIMAL precision/scale, else -1) is the element's and survives the
  // array wrapping.
  auto make = [in_array](int32_t scalar_oid, int32_t array_oid, int16_t typlen,
                         int32_t typmod = -1) -> PgTypeInfo {
    return {in_array ? array_oid : scalar_oid,
            in_array ? static_cast<int16_t>(-1) : typlen, typmod};
  };
  switch (type.id()) {
    using enum duckdb::LogicalTypeId;
    using enum PgTypeOID;
    case BOOLEAN:
      return make(kBool, kBoolArray, sizeof(bool));
    case TINYINT:
    case UTINYINT:
    case SMALLINT:
      return make(kInt2, kInt2Array, sizeof(int16_t));
    case USMALLINT:
    case INTEGER:
      return make(kInt4, kInt4Array, sizeof(int32_t));
    case UINTEGER:
      return make(kInt8, kInt8Array, sizeof(int64_t));
    case BIGINT:
      if (IsOid(type)) {
        return make(kOid, kOidArray, sizeof(int32_t));
      }
      if (IsXid(type)) {
        return make(kXid, kXidArray, sizeof(int32_t));
      }
      if (IsCid(type)) {
        return make(kCid, kCidArray, sizeof(int32_t));
      }
      if (IsTid(type)) {
        return make(kTid, kTidArray, sizeof(int32_t) + sizeof(int16_t));
      }
      if (IsXid8(type)) {
        return make(kXid8, kXid8Array, sizeof(int64_t));
      }
      if (IsRegproc(type)) {
        return make(kRegproc, kRegprocArray, sizeof(int32_t));
      }
      if (IsRegprocedure(type)) {
        return make(kRegprocedure, kRegprocedureArray, sizeof(int32_t));
      }
      if (IsRegoper(type)) {
        return make(kRegoper, kRegoperArray, sizeof(int32_t));
      }
      if (IsRegoperator(type)) {
        return make(kRegoperator, kRegoperatorArray, sizeof(int32_t));
      }
      if (IsRegclass(type)) {
        return make(kRegclass, kRegclassArray, sizeof(int32_t));
      }
      if (IsRegtype(type)) {
        return make(kRegtype, kRegtypeArray, sizeof(int32_t));
      }
      if (IsRegrole(type)) {
        return make(kRegrole, kRegroleArray, sizeof(int32_t));
      }
      if (IsRegnamespace(type)) {
        return make(kRegnamespace, kRegnamespaceArray, sizeof(int32_t));
      }
      if (IsRegconfig(type)) {
        return make(kRegconfig, kRegconfigArray, sizeof(int32_t));
      }
      if (IsRegdictionary(type)) {
        return make(kRegdictionary, kRegdictionaryArray, sizeof(int32_t));
      }
      if (IsRegcollation(type)) {
        return make(kRegcollation, kRegcollationArray, sizeof(int32_t));
      }
      return make(kInt8, kInt8Array, sizeof(int64_t));
    case HUGEINT:
    case UHUGEINT:
    case BIGNUM:
      return make(kNumeric, kNumericArray, -1);
    case DECIMAL:
      return make(
        kNumeric, kNumericArray, -1,
        ((static_cast<int32_t>(duckdb::DecimalType::GetWidth(type)) << 16) |
         duckdb::DecimalType::GetScale(type)) +
          4);
    case DATE:
      return make(kDate, kDateArray, sizeof(int32_t));
    case TIME:
    case TIME_NS:
      return make(kTime, kTimeArray, sizeof(int64_t));
    case TIMESTAMP_SEC:
    case TIMESTAMP_MS:
    case TIMESTAMP:
    case TIMESTAMP_NS:
      return make(kTimestamp, kTimestampArray, sizeof(int64_t));
    case FLOAT:
      return make(kFloat4, kFloat4Array, sizeof(float));
    case DOUBLE:
      return make(kFloat8, kFloat8Array, sizeof(double));
    case CHAR:
      return make(kText, kTextArray, -1);
    case BLOB:
      return make(kBytea, kByteaArray, -1);
    case INTERVAL:
      return make(kInterval, kIntervalArray, sizeof(int64_t) + sizeof(int64_t));
    case TIMESTAMP_TZ:
      return make(kTimestampTz, kTimestampTzArray, sizeof(int64_t));
    case TIME_TZ:
      return make(kTimeTz, kTimeTzArray, sizeof(int64_t) + sizeof(int32_t));
    case BIT:
      return make(kVarbit, kVarbitArray, -1);
    case UUID:
      return make(kUuid, kUuidArray, 16);
    case ENUM: {
      auto ext = type.GetExtensionInfo();
      // null for anonymous/derived enums not registered as a pg custom type
      // (e.g. enum_range); their value is just the string label on the wire.
      if (ext) {
        auto it = ext->properties.find(catalog::kPgSqlTypeOidProp);
        if (it != ext->properties.end()) {
          const ObjectId oid{it->second.GetValue<uint64_t>()};
          // Enum types are int4-backed (typlen 4).
          return {
            static_cast<int32_t>(
              (in_array ? catalog::PgSqlType::ToArrayOid(oid) : oid).id()),
            in_array ? static_cast<int16_t>(-1) : static_cast<int16_t>(4), -1};
        }
      }
      return make(kText, kTextArray, -1);
    }
    case STRUCT: {
      if (IsInet(type)) {
        return make(kInet, kInetArray, -1);
      }
      auto ext = type.GetExtensionInfo();
      // null in case of anonymous record types (e.g. SELECT ROW(1, 2))
      if (ext) {
        auto it = ext->properties.find(catalog::kPgSqlTypeOidProp);
        if (it != ext->properties.end()) {
          const ObjectId oid{it->second.GetValue<uint64_t>()};
          return {
            static_cast<int32_t>(
              (in_array ? catalog::PgSqlType::ToArrayOid(oid) : oid).id()),
            static_cast<int16_t>(-1), -1};
        }
      }
      return make(kRecord, kRecordArray, -1);
    }
    case MAP:
      return {kRecordArray, static_cast<int16_t>(-1), -1};
    case VARCHAR: {
      if (type.IsJSONType()) {
        return make(kJson, kJsonArray, -1);
      }
      if (IsName(type)) {
        return make(kName, kNameArray, 64);
      }
      if (IsChar(type)) {
        return make(kChar, kCharArray, 1);
      }
      return make(kText, kTextArray, -1);
    }
    case UBIGINT:
      return make(kNumeric, kNumericArray, -1);
    case LIST:
      return Logical2Pg(duckdb::ListType::GetChildType(type), true);
    case ARRAY:
      return Logical2Pg(duckdb::ArrayType::GetChildType(type), true);
    case VARIANT:
      return make(kVariant, kVariantArray, -1);
    default:
      return {kUnknown, static_cast<int16_t>(-1), -1};
  }
}

int32_t Type2Oid(const duckdb::LogicalType& type, bool in_array) {
  return Logical2Pg(type, in_array).oid;
}

duckdb::LogicalType Oid2Type(int32_t oid, const catalog::Snapshot& snapshot) {
  switch (oid) {
    using enum PgTypeOID;
    using duckdb::LogicalType;
    SDB_OID2TYPE(kBool, LogicalType::BOOLEAN)
    SDB_OID2TYPE(kBytea, LogicalType::BLOB)
    SDB_OID2TYPE(kChar, CHAR())
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
    SDB_OID2TYPE(kInet, INET())
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

static const containers::FlatHashMap<std::string_view, PgTypeOID>
  kTypeNameToOid = [] {
    struct Builder {
      containers::FlatHashMap<std::string_view, PgTypeOID> map;
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
      .SDB_REGTYPE_IN("pg_brin_minmax_multi_summary", kPgBrinMinmaxMultiSummary)
      .SDB_REGTYPE_WITH_ARRAY_IN("variant", kVariant);
    return std::move(builder.map);
  }();

uint64_t RegtypeIn(std::string_view name) {
  if (auto it = kTypeNameToOid.find(name); it != kTypeNameToOid.end()) {
    return static_cast<uint64_t>(it->second);
  }
  return kInvalidOid;
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
  auto relation =
    snapshot->GetRelation(catalog::NoAccessCheck(), ctx.GetDatabaseId(),
                          object_name.schema, object_name.relation);
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

}  // namespace sdb::pg
