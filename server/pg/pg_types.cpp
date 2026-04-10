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
#include <absl/strings/numbers.h>
#include <absl/time/civil_time.h>
#include <velox/functions/prestosql/types/JsonType.h>
#include <velox/functions/prestosql/types/TimestampWithTimeZoneType.h>
#include <velox/functions/prestosql/types/UuidType.h>
#include <velox/type/Timestamp.h>

#include "catalog/catalog.h"
#include "catalog/virtual_table.h"
#include "pg/connection_context.h"
#include "pg/functions/interval.h"
#include "pg/parse_array.h"
#include "pg/serialize.h"
#include "pg/sql_collector.h"
#include "pg/system_catalog.h"
#include "query/types.h"

namespace sdb::pg {

int32_t Type2Oid(const duckdb::LogicalType& type, bool in_array) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      return in_array ? PgTypeOID::kBoolArray : PgTypeOID::kBool;
    case duckdb::LogicalTypeId::SMALLINT:
      return in_array ? PgTypeOID::kInt2Array : PgTypeOID::kInt2;
    case duckdb::LogicalTypeId::INTEGER:
      return in_array ? PgTypeOID::kInt4Array : PgTypeOID::kInt4;
    case duckdb::LogicalTypeId::BIGINT:
      return in_array ? PgTypeOID::kInt8Array : PgTypeOID::kInt8;
    case duckdb::LogicalTypeId::DATE:
      return in_array ? PgTypeOID::kDateArray : PgTypeOID::kDate;
    case duckdb::LogicalTypeId::TIMESTAMP:
      return in_array ? PgTypeOID::kTimestampArray : PgTypeOID::kTimestamp;
    case duckdb::LogicalTypeId::DECIMAL:
      return in_array ? PgTypeOID::kNumericArray : PgTypeOID::kNumeric;
    case duckdb::LogicalTypeId::FLOAT:
      return in_array ? PgTypeOID::kFloat4Array : PgTypeOID::kFloat4;
    case duckdb::LogicalTypeId::DOUBLE:
      return in_array ? PgTypeOID::kFloat8Array : PgTypeOID::kFloat8;
    case duckdb::LogicalTypeId::VARCHAR:
      return in_array ? PgTypeOID::kTextArray : PgTypeOID::kText;
    case duckdb::LogicalTypeId::BLOB:
      return in_array ? PgTypeOID::kByteaArray : PgTypeOID::kBytea;
    case duckdb::LogicalTypeId::INTERVAL:
      return in_array ? PgTypeOID::kIntervalArray : PgTypeOID::kInterval;
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      return in_array ? PgTypeOID::kTimestampTzArray : PgTypeOID::kTimestampTz;
    case duckdb::LogicalTypeId::HUGEINT:
      return in_array ? PgTypeOID::kNumericArray : PgTypeOID::kNumeric;
    case duckdb::LogicalTypeId::UUID:
      return in_array ? PgTypeOID::kUuidArray : PgTypeOID::kUuid;
    case duckdb::LogicalTypeId::ARRAY:
      return Type2Oid(duckdb::ListType::GetChildType(type), true);
    default:
      return -1;
  }
}

duckdb::LogicalType Oid2Type(int32_t oid) {
  switch (oid) {
      // clang-format off
    case PgTypeOID::kBool:                return duckdb::LogicalType::BOOLEAN;
    case PgTypeOID::kChar:                return duckdb::LogicalType::TINYINT;
    case PgTypeOID::kInt2:                return duckdb::LogicalType::SMALLINT;
    case PgTypeOID::kInt4:                return duckdb::LogicalType::INTEGER;
    case PgTypeOID::kInt8:                return duckdb::LogicalType::BIGINT;
    case PgTypeOID::kFloat4:              return duckdb::LogicalType::FLOAT;
    case PgTypeOID::kFloat8:              return duckdb::LogicalType::DOUBLE;
    case PgTypeOID::kText:                return duckdb::LogicalType::VARCHAR;
    // case PgTypeOID::kName:                return PGNAME();
    case PgTypeOID::kBytea:               return duckdb::LogicalType::BLOB;
    case PgTypeOID::kDate:                return duckdb::LogicalType::DATE;
    case PgTypeOID::kTimestamp:           return duckdb::LogicalType::TIMESTAMP;
    // case PgTypeOID::kOid:                 return PGOID();
    // case PgTypeOID::kXid:                 return PGXID();
    // case PgTypeOID::kCid:                 return PGCID();
    // case PgTypeOID::kTid:                 return PGTID();
    // case PgTypeOID::kXid8:                return PGXID8();
    // case PgTypeOID::kRegproc:             return REGPROC();
    // case PgTypeOID::kRegtype:             return REGTYPE();
    // case PgTypeOID::kRegclass:            return REGCLASS();
    // case PgTypeOID::kRegnamespace:        return REGNAMESPACE();
    // case PgTypeOID::kRegoper:             return REGOPER();
    // case PgTypeOID::kRegoperator:         return REGOPERATOR();
    // case PgTypeOID::kRegprocedure:        return REGPROCEDURE();
    // case PgTypeOID::kRegrole:             return REGROLE();
    // case PgTypeOID::kRegconfig:           return REGCONFIG();
    // case PgTypeOID::kRegdictionary:       return REGDICTIONARY();
    // case PgTypeOID::kRegcollation:        return REGCOLLATION();
    case PgTypeOID::kJson:                return duckdb::LogicalType::JSON();
    case PgTypeOID::kUuid:                return duckdb::LogicalType::UUID;
    case PgTypeOID::kTimestampTz:         return duckdb::LogicalType::TIMESTAMP_TZ;
    case PgTypeOID::kInterval:            return duckdb::LogicalType::INTERVAL;

    case PgTypeOID::kBoolArray:           return duckdb::LogicalType::LIST(duckdb::LogicalType::BOOLEAN);
    case PgTypeOID::kCharArray:           return duckdb::LogicalType::LIST(duckdb::LogicalType::TINYINT);
    case PgTypeOID::kInt2Array:           return duckdb::LogicalType::LIST(duckdb::LogicalType::SMALLINT);
    case PgTypeOID::kInt4Array:           return duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER);
    case PgTypeOID::kInt8Array:           return duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);
    case PgTypeOID::kFloat4Array:         return duckdb::LogicalType::LIST(duckdb::LogicalType::FLOAT);
    case PgTypeOID::kFloat8Array:         return duckdb::LogicalType::LIST(duckdb::LogicalType::DOUBLE);
    case PgTypeOID::kTextArray:           return duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
    // case PgTypeOID::kNameArray:           return duckdb::LogicalType::LIST(PGNAME());
    case PgTypeOID::kByteaArray:          return duckdb::LogicalType::LIST(duckdb::LogicalType::BLOB);
    case PgTypeOID::kDateArray:           return duckdb::LogicalType::LIST(duckdb::LogicalType::DATE);
    case PgTypeOID::kTimestampArray:      return duckdb::LogicalType::LIST(duckdb::LogicalType::TIMESTAMP);
    // case PgTypeOID::kOidArray:            return duckdb::LogicalType::LIST(PGOID());
    // case PgTypeOID::kXidArray:            return duckdb::LogicalType::LIST(PGXID());
    // case PgTypeOID::kCidArray:            return duckdb::LogicalType::LIST(PGCID());
    // case PgTypeOID::kTidArray:            return duckdb::LogicalType::LIST(PGTID());
    // case PgTypeOID::kXid8Array:           return duckdb::LogicalType::LIST(PGXID8());
    // case PgTypeOID::kRegprocArray:        return duckdb::LogicalType::LIST(REGPROC());
    // case PgTypeOID::kRegtypeArray:        return duckdb::LogicalType::LIST(REGTYPE());
    // case PgTypeOID::kRegclassArray:       return duckdb::LogicalType::LIST(REGCLASS());
    // case PgTypeOID::kRegnamespaceArray:   return duckdb::LogicalType::LIST(REGNAMESPACE());
    // case PgTypeOID::kRegoperArray:        return duckdb::LogicalType::LIST(REGOPER());
    // case PgTypeOID::kRegoperatorArray:    return duckdb::LogicalType::LIST(REGOPERATOR());
    // case PgTypeOID::kRegprocedureArray:   return duckdb::LogicalType::LIST(REGPROCEDURE());
    // case PgTypeOID::kRegroleArray:        return duckdb::LogicalType::LIST(REGROLE());
    // case PgTypeOID::kRegconfigArray:      return duckdb::LogicalType::LIST(REGCONFIG());
    // case PgTypeOID::kRegdictionaryArray:  return duckdb::LogicalType::LIST(REGDICTIONARY());
    // case PgTypeOID::kRegcollationArray:   return duckdb::LogicalType::LIST(REGCOLLATION());
    case PgTypeOID::kJsonArray:           return duckdb::LogicalType::LIST(duckdb::LogicalType::JSON());
    case PgTypeOID::kUuidArray:           return duckdb::LogicalType::LIST(duckdb::LogicalType::UUID);
    case PgTypeOID::kTimestampTzArray:    return duckdb::LogicalType::LIST(duckdb::LogicalType::TIMESTAMP_TZ);
    case PgTypeOID::kIntervalArray:       return duckdb::LogicalType::LIST(duckdb::LogicalType::INTERVAL);
    default:                              return {};
      // clang-format on
  }
}

std::string ToPgTypeString(const duckdb::LogicalType& type) {
  return type.ToString();
}

// clang-format off
#define REGTYPE_OUT(oid, type_name)                        \
    case PgTypeOID::oid: return type_name;                 \
    case PgTypeOID::oid##Array: return type_name "[]";

std::string RegtypeOut(uint64_t oid) {
  switch (static_cast<PgTypeOID>(oid)) {
    REGTYPE_OUT(kRegproc, "regproc")
    REGTYPE_OUT(kOid, "oid")
    REGTYPE_OUT(kXid, "xid")
    REGTYPE_OUT(kName, "name")
    REGTYPE_OUT(kTid, "tid")
    REGTYPE_OUT(kCid, "cid")
    REGTYPE_OUT(kXid8, "xid8")
    REGTYPE_OUT(kBool, "boolean")
    REGTYPE_OUT(kBytea, "bytea")
    REGTYPE_OUT(kChar, "character")
    REGTYPE_OUT(kInt2, "smallint")
    REGTYPE_OUT(kInt4, "integer")
    REGTYPE_OUT(kInt8, "bigint")
    REGTYPE_OUT(kFloat4, "real")
    REGTYPE_OUT(kFloat8, "double precision")
    REGTYPE_OUT(kText, "text")
    REGTYPE_OUT(kJson, "json")
    REGTYPE_OUT(kUuid, "uuid")
    REGTYPE_OUT(kNumeric, "numeric")
    REGTYPE_OUT(kDate, "date")
    REGTYPE_OUT(kTimestamp, "timestamp without time zone")
    REGTYPE_OUT(kTimestampTz, "timestamp with time zone")
    REGTYPE_OUT(kInterval, "interval")
    REGTYPE_OUT(kRegprocedure, "regprocedure")
    REGTYPE_OUT(kRegoper, "regoper")
    REGTYPE_OUT(kRegoperator, "regoperator")
    REGTYPE_OUT(kRegclass, "regclass")
    REGTYPE_OUT(kRegtype, "regtype")
    REGTYPE_OUT(kRegconfig, "regconfig")
    REGTYPE_OUT(kRegdictionary, "regdictionary")
    REGTYPE_OUT(kRegnamespace, "regnamespace")
    REGTYPE_OUT(kRegrole, "regrole")
    REGTYPE_OUT(kRegcollation, "regcollation")
  }
  return absl::StrCat(oid);
}
#undef REGTYPE_OUT

#define SDB_REGTYPE_IN(oid, type_name)             \
    {type_name, PgTypeOID::oid},               \
    {type_name "[]", PgTypeOID::oid##Array},

uint64_t RegtypeIn(std::string_view name) {
  static const containers::FlatHashMap<std::string_view, uint64_t>
    kTypeNameToOid = {
      SDB_REGTYPE_IN(kRegproc, "regproc")
      SDB_REGTYPE_IN(kOid, "oid")
      SDB_REGTYPE_IN(kXid, "xid")
      SDB_REGTYPE_IN(kBool, "boolean")
      SDB_REGTYPE_IN(kBool, "bool")
      SDB_REGTYPE_IN(kBytea, "bytea")
      SDB_REGTYPE_IN(kChar, "character")
      SDB_REGTYPE_IN(kChar, "char")
      SDB_REGTYPE_IN(kInt2, "smallint")
      SDB_REGTYPE_IN(kInt2, "int2")
      SDB_REGTYPE_IN(kInt4, "integer")
      SDB_REGTYPE_IN(kInt4, "int4")
      SDB_REGTYPE_IN(kInt4, "int")
      SDB_REGTYPE_IN(kInt8, "bigint")
      SDB_REGTYPE_IN(kInt8, "int8")
      SDB_REGTYPE_IN(kFloat4, "real")
      SDB_REGTYPE_IN(kFloat4, "float4")
      SDB_REGTYPE_IN(kFloat8, "double precision")
      SDB_REGTYPE_IN(kFloat8, "float8")
      SDB_REGTYPE_IN(kText, "text")
      SDB_REGTYPE_IN(kJson, "json")
      SDB_REGTYPE_IN(kUuid, "uuid")
      SDB_REGTYPE_IN(kNumeric, "numeric")
      SDB_REGTYPE_IN(kDate, "date")
      SDB_REGTYPE_IN(kTimestamp, "timestamp without time zone")
      SDB_REGTYPE_IN(kTimestamp, "timestamp")
      SDB_REGTYPE_IN(kTimestampTz, "timestamp with time zone")
      SDB_REGTYPE_IN(kTimestampTz, "timestamptz")
      SDB_REGTYPE_IN(kInterval, "interval")
      SDB_REGTYPE_IN(kRegprocedure, "regprocedure")
      SDB_REGTYPE_IN(kRegoper, "regoper")
      SDB_REGTYPE_IN(kRegoperator, "regoperator")
      SDB_REGTYPE_IN(kRegclass, "regclass")
      SDB_REGTYPE_IN(kRegtype, "regtype")
      SDB_REGTYPE_IN(kRegconfig, "regconfig")
      SDB_REGTYPE_IN(kRegdictionary, "regdictionary")
      SDB_REGTYPE_IN(kRegnamespace, "regnamespace")
      SDB_REGTYPE_IN(kRegrole, "regrole")
      SDB_REGTYPE_IN(kRegcollation, "regcollation")
    };
  auto it = kTypeNameToOid.find(name);
  if (it != kTypeNameToOid.end()) {
    return it->second;
  }
  return kInvalidOid;
}
#undef SDB_REGTYPE_IN
// clang-format on

std::expected<duckdb::Value, DeserializeError> DuckDBDeserializeParameter(
  const duckdb::LogicalType& type, VarFormat format, std::string_view data) {
  if (format == VarFormat::Binary) {
    switch (type.id()) {
      case duckdb::LogicalTypeId::BOOLEAN: {
        if (data.size() != 1) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        return duckdb::Value::BOOLEAN(data[0] != 0);
      }
      case duckdb::LogicalTypeId::TINYINT: {
        if (data.size() != 1) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        return duckdb::Value::TINYINT(static_cast<int8_t>(data[0]));
      }
      case duckdb::LogicalTypeId::SMALLINT: {
        if (data.size() != 2) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        int16_t val = absl::big_endian::Load16(data.data());
        return duckdb::Value::SMALLINT(val);
      }
      case duckdb::LogicalTypeId::INTEGER: {
        if (data.size() != 4) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        int32_t val = absl::big_endian::Load32(data.data());
        return duckdb::Value::INTEGER(val);
      }
      case duckdb::LogicalTypeId::BIGINT: {
        if (data.size() != 8) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        int64_t val = absl::big_endian::Load64(data.data());
        return duckdb::Value::BIGINT(val);
      }
      case duckdb::LogicalTypeId::FLOAT: {
        if (data.size() != 4) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        uint32_t bits = absl::big_endian::Load32(data.data());
        float val = std::bit_cast<float>(bits);
        return duckdb::Value::FLOAT(val);
      }
      case duckdb::LogicalTypeId::DOUBLE: {
        if (data.size() != 8) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        uint64_t bits = absl::big_endian::Load64(data.data());
        double val = std::bit_cast<double>(bits);
        return duckdb::Value::DOUBLE(val);
      }
      case duckdb::LogicalTypeId::VARCHAR: {
        return duckdb::Value(std::string{data});
      }
      case duckdb::LogicalTypeId::BLOB: {
        return duckdb::Value::BLOB(std::string{data});
      }
      case duckdb::LogicalTypeId::TIMESTAMP:
      case duckdb::LogicalTypeId::TIMESTAMP_TZ: {
        if (data.size() != 8) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        // PG binary: microseconds since 2000-01-01
        int64_t pg_us = absl::big_endian::Load64(data.data());
        // Convert to microseconds since epoch (1970-01-01)
        static constexpr int64_t kGapUs =
          946684800LL * 1000000LL;  // 2000-01-01 in us since epoch
        return duckdb::Value::TIMESTAMP(duckdb::timestamp_t(pg_us + kGapUs));
      }
      case duckdb::LogicalTypeId::DATE: {
        if (data.size() != 4) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        // PG binary: days since 2000-01-01
        int32_t pg_days = absl::big_endian::Load32(data.data());
        static constexpr int32_t kGapDays = 10957;  // 2000-01-01 - 1970-01-01
        return duckdb::Value::DATE(duckdb::date_t(pg_days + kGapDays));
      }
      case duckdb::LogicalTypeId::UUID: {
        if (data.size() != 16) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        duckdb::hugeint_t val;
        val.upper = absl::big_endian::Load64(data.data());
        val.lower = absl::big_endian::Load64(data.data() + 8);
        return duckdb::Value::UUID(val);
      }
      case duckdb::LogicalTypeId::INTERVAL: {
        if (data.size() != 16) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        duckdb::interval_t interval;
        interval.micros = absl::big_endian::Load64(data.data());
        interval.days = absl::big_endian::Load32(data.data() + 8);
        interval.months = absl::big_endian::Load32(data.data() + 12);
        return duckdb::Value::INTERVAL(interval.months, interval.days,
                                       interval.micros);
      }
      // TODO: ARRAY binary deserialization
      default:
        SDB_ASSERT(false, "unsupported binary parameter type");
        return std::unexpected{DeserializeError::InvalidRepresentation};
    }
  }

  // Text format -- DuckDB can parse most text representations via
  // Value::CreateValue
  if (format == VarFormat::Text) {
    switch (type.id()) {
      case duckdb::LogicalTypeId::BOOLEAN: {
        if (data == "t" || data == "true" || data == "1") {
          return duckdb::Value::BOOLEAN(true);
        } else if (data == "f" || data == "false" || data == "0") {
          return duckdb::Value::BOOLEAN(false);
        }
        return std::unexpected{DeserializeError::InvalidRepresentation};
      }
      case duckdb::LogicalTypeId::TINYINT: {
        int8_t val;
        if (!absl::SimpleAtoi(data, &val)) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        return duckdb::Value::TINYINT(val);
      }
      case duckdb::LogicalTypeId::SMALLINT: {
        int16_t val;
        if (!absl::SimpleAtoi(data, &val)) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        return duckdb::Value::SMALLINT(val);
      }
      case duckdb::LogicalTypeId::INTEGER: {
        int32_t val;
        if (!absl::SimpleAtoi(data, &val)) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        return duckdb::Value::INTEGER(val);
      }
      case duckdb::LogicalTypeId::BIGINT: {
        int64_t val;
        if (!absl::SimpleAtoi(data, &val)) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        return duckdb::Value::BIGINT(val);
      }
      case duckdb::LogicalTypeId::FLOAT: {
        float val;
        if (!absl::SimpleAtof(data, &val)) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        return duckdb::Value::FLOAT(val);
      }
      case duckdb::LogicalTypeId::DOUBLE: {
        double val;
        if (!absl::SimpleAtod(data, &val)) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        return duckdb::Value::DOUBLE(val);
      }
      case duckdb::LogicalTypeId::VARCHAR: {
        return duckdb::Value(std::string{data});
      }
      case duckdb::LogicalTypeId::BLOB: {
        return duckdb::Value::BLOB(std::string{data});
      }
      case duckdb::LogicalTypeId::TIMESTAMP:
      case duckdb::LogicalTypeId::TIMESTAMP_TZ: {
        auto ts = duckdb::Timestamp::FromString(std::string{data}, false);
        return duckdb::Value::TIMESTAMP(ts);
      }
      case duckdb::LogicalTypeId::DATE: {
        auto dt = duckdb::Date::FromString(std::string{data});
        return duckdb::Value::DATE(dt);
      }
      case duckdb::LogicalTypeId::UUID: {
        return duckdb::Value::UUID(std::string{data});
      }
      case duckdb::LogicalTypeId::INTERVAL: {
        // DuckDB can parse interval strings
        return duckdb::Value(std::string{data})
          .DefaultCastAs(duckdb::LogicalType::INTERVAL);
      }
      case duckdb::LogicalTypeId::DECIMAL: {
        // DuckDB handles decimal parsing
        return duckdb::Value(std::string{data}).DefaultCastAs(type);
      }
      // TODO: ARRAY text deserialization (ParsePgTextArray equivalent)
      default:
        // Fallback: let DuckDB try to parse as string and cast
        return duckdb::Value(std::string{data}).DefaultCastAs(type);
    }
  }

  return std::unexpected{DeserializeError::InvalidRepresentation};
}

std::string RegclassOut(const catalog::Snapshot& snapshot, uint64_t oid) {
  auto object = snapshot.GetObject(ObjectId{oid});
  if (object) {
    return std::string{object->GetName()};
  }
  std::string result;
  VisitSystemTables([&](const catalog::VirtualTable& table, Oid) {
    if (table.Id() == oid) {
      result = table.Name();
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

}  // namespace sdb::pg
