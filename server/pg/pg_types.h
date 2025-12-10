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

#pragma once

#include <frozen/unordered_map.h>
#include <velox/type/Type.h>

#include <expected>

#include "basics/exceptions.h"
#include "basics/fwd.h"

namespace sdb::pg {

using ParamIndex = int16_t;

enum PgTypeOID : int32_t {
  kBool = 16,
  kChar = 18,
  kInt2 = 21,
  kInt4 = 23,
  kInt8 = 20,
  kFloat4 = 700,
  kFloat8 = 701,
  kText = 25,
  kVarchar = 1043,
  kBytea = 17,
  kJson = 114,
  kUuid = 2950,
  kNumeric = 1700,
  kDate = 1082,
  kTimestamp = 1114,
  kTimestampTz = 1184,
  kInterval = 1186,
  kUnknown = 706,

  // Array types
  kBoolArray = 1000,
  kCharArray = 1002,
  kInt2Array = 1005,
  kInt4Array = 1007,
  kInt8Array = 1016,
  kFloat4Array = 1021,
  kFloat8Array = 1022,
  kTextArray = 1009,
  kVarcharArray = 1015,
  kByteaArray = 1001,
  kJsonArray = 199,
  kUuidArray = 2951,
  kNumericArray = 1231,
  kDateArray = 1182,
  kTimestampArray = 1115,
  kTimestampTzArray = 1185,
  kIntervalArray = 1187,
};

constexpr int32_t GetPrimitiveTypeOID(velox::TypeKind kind, bool in_array) {
  switch (kind) {
    case velox::TypeKind::UNKNOWN: {
      return in_array ? PgTypeOID::kTextArray : PgTypeOID::kText;
    }
    case velox::TypeKind::BOOLEAN: {
      return in_array ? PgTypeOID::kBoolArray : PgTypeOID::kBool;
    }
    case velox::TypeKind::TINYINT: {
      return in_array ? PgTypeOID::kCharArray : PgTypeOID::kChar;
    }
    case velox::TypeKind::SMALLINT: {
      return in_array ? PgTypeOID::kInt2Array : PgTypeOID::kInt2;
    }
    case velox::TypeKind::INTEGER: {
      return in_array ? PgTypeOID::kInt4Array : PgTypeOID::kInt4;
    }
    case velox::TypeKind::BIGINT: {
      return in_array ? PgTypeOID::kInt8Array : PgTypeOID::kInt8;
    }
    case velox::TypeKind::REAL: {
      return in_array ? PgTypeOID::kFloat4Array : PgTypeOID::kFloat4;
    }
    case velox::TypeKind::DOUBLE: {
      return in_array ? PgTypeOID::kFloat8Array : PgTypeOID::kFloat8;
    }
    case velox::TypeKind::TIMESTAMP: {
      return in_array ? PgTypeOID::kTimestampArray : PgTypeOID::kTimestamp;
    }
    case velox::TypeKind::VARCHAR: {
      return in_array ? PgTypeOID::kTextArray : PgTypeOID::kText;
    }
    case velox::TypeKind::VARBINARY: {
      return in_array ? PgTypeOID::kByteaArray : PgTypeOID::kBytea;
    }
    default:
      SDB_THROW(ERROR_NOT_IMPLEMENTED,
                "unsupported converting velox -> pg type kind: ");
  }
}

int32_t GetTypeOID(const velox::TypePtr& type, bool in_array = false);

enum class VarFormat : int16_t;

enum class DeserializeError {
  InvalidRepresentation,
};

std::expected<velox::Variant, DeserializeError> DeserializeParameter(
  const velox::Type& type, VarFormat format, std::string_view data);

}  // namespace sdb::pg
