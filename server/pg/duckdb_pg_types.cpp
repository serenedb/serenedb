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

#include "pg/duckdb_pg_types.h"

#include <absl/base/internal/endian.h>
#include <absl/strings/numbers.h>

#include <bit>
#include <cstring>

#include "basics/assert.h"

namespace sdb::pg {

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

int32_t DuckDBTypeToOid(const duckdb::LogicalType& type) {
  // Adapted from pg_types.cpp Type2Oid
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      return 16;  // BOOLOID
    case duckdb::LogicalTypeId::TINYINT:
      return 21;  // INT2OID (PG has no int1)
    case duckdb::LogicalTypeId::SMALLINT:
      return 21;  // INT2OID
    case duckdb::LogicalTypeId::INTEGER:
      return 23;  // INT4OID
    case duckdb::LogicalTypeId::BIGINT:
      return 20;  // INT8OID
    case duckdb::LogicalTypeId::FLOAT:
      return 700;  // FLOAT4OID
    case duckdb::LogicalTypeId::DOUBLE:
      return 701;  // FLOAT8OID
    case duckdb::LogicalTypeId::DECIMAL:
      return 1700;  // NUMERICOID
    case duckdb::LogicalTypeId::VARCHAR:
      return 25;  // TEXTOID
    case duckdb::LogicalTypeId::BLOB:
      return 17;  // BYTEAOID
    case duckdb::LogicalTypeId::TIMESTAMP:
      return 1114;  // TIMESTAMPOID
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      return 1184;  // TIMESTAMPTZOID
    case duckdb::LogicalTypeId::DATE:
      return 1082;  // DATEOID
    case duckdb::LogicalTypeId::TIME:
      return 1083;  // TIMEOID
    case duckdb::LogicalTypeId::TIME_TZ:
      return 1266;  // TIMETZOID
    case duckdb::LogicalTypeId::INTERVAL:
      return 1186;  // INTERVALOID
    case duckdb::LogicalTypeId::UUID:
      return 2950;  // UUIDOID
    case duckdb::LogicalTypeId::HUGEINT:
      return 1700;  // NUMERICOID
    case duckdb::LogicalTypeId::LIST: {
      // Array OID depends on element type
      auto& child = duckdb::ListType::GetChildType(type);
      switch (child.id()) {
        case duckdb::LogicalTypeId::BOOLEAN:
          return 1000;  // BOOLARRAYOID
        case duckdb::LogicalTypeId::SMALLINT:
          return 1005;  // INT2ARRAYOID
        case duckdb::LogicalTypeId::INTEGER:
          return 1007;  // INT4ARRAYOID
        case duckdb::LogicalTypeId::BIGINT:
          return 1016;  // INT8ARRAYOID
        case duckdb::LogicalTypeId::FLOAT:
          return 1021;  // FLOAT4ARRAYOID
        case duckdb::LogicalTypeId::DOUBLE:
          return 1022;  // FLOAT8ARRAYOID
        case duckdb::LogicalTypeId::VARCHAR:
          return 1009;  // TEXTARRAYOID
        case duckdb::LogicalTypeId::TIMESTAMP:
          return 1115;  // TIMESTAMPARRAYOID
        case duckdb::LogicalTypeId::DATE:
          return 1182;  // DATEARRAYOID
        case duckdb::LogicalTypeId::UUID:
          return 2951;  // UUIDARRAYOID
        case duckdb::LogicalTypeId::DECIMAL:
          return 1231;  // NUMERICARRAYOID
        default:
          return 2277;  // ANYARRAYOID
      }
    }
    case duckdb::LogicalTypeId::STRUCT:
    case duckdb::LogicalTypeId::MAP:
      return 2249;  // RECORDOID
    default:
      return 25;  // TEXTOID fallback
  }
}

duckdb::LogicalType OidToDuckDBType(int32_t oid) {
  switch (oid) {
    case 16:
      return duckdb::LogicalType::BOOLEAN;
    case 21:
      return duckdb::LogicalType::SMALLINT;
    case 23:
      return duckdb::LogicalType::INTEGER;
    case 20:
      return duckdb::LogicalType::BIGINT;
    case 700:
      return duckdb::LogicalType::FLOAT;
    case 701:
      return duckdb::LogicalType::DOUBLE;
    case 1700:
      return duckdb::LogicalType::DECIMAL(38, 18);
    case 25:
      return duckdb::LogicalType::VARCHAR;
    case 17:
      return duckdb::LogicalType::BLOB;
    case 1114:
      return duckdb::LogicalType::TIMESTAMP;
    case 1184:
      return duckdb::LogicalType::TIMESTAMP_TZ;
    case 1082:
      return duckdb::LogicalType::DATE;
    case 1083:
      return duckdb::LogicalType::TIME;
    case 1266:
      return duckdb::LogicalType::TIME_TZ;
    case 1186:
      return duckdb::LogicalType::INTERVAL;
    case 2950:
      return duckdb::LogicalType::UUID;
    default:
      return duckdb::LogicalType::VARCHAR;
  }
}

}  // namespace sdb::pg
