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

#include <velox/type/Type.h>

#include <expected>
#include <magic_enum/magic_enum.hpp>

#include "basics/exceptions.h"
#include "basics/fwd.h"

namespace sdb {
namespace catalog {

struct Snapshot;

}

class ConnectionContext;

}  // namespace sdb
namespace sdb::pg {

using ParamIndex = int16_t;

enum PgTypeOID : int32_t {
  kBool = 16,
  kChar = 18,
  kName = 19,
  kInt2 = 21,
  kInt4 = 23,
  kRegproc = 24,
  kText = 25,
  kOid = 26,
  kTid = 27,
  kXid = 28,
  kCid = 29,
  kInt8 = 20,
  kFloat4 = 700,
  kFloat8 = 701,
  kBytea = 17,
  kJson = 114,
  kUuid = 2950,
  kNumeric = 1700,
  kDate = 1082,
  kTimestamp = 1114,
  kTimestampTz = 1184,
  kInterval = 1186,
  kRegprocedure = 2202,
  kRegoper = 2203,
  kRegoperator = 2204,
  kRegclass = 2205,
  kRegtype = 2206,
  kRegconfig = 3734,
  kRegdictionary = 3769,
  kRegnamespace = 4089,
  kRegrole = 4096,
  kRegcollation = 4191,
  kXid8 = 5069,

  // Array types
  kBoolArray = 1000,
  kByteaArray = 1001,
  kCharArray = 1002,
  kNameArray = 1003,
  kInt2Array = 1005,
  kInt4Array = 1007,
  kRegprocArray = 1008,
  kTextArray = 1009,
  kTidArray = 1010,
  kXidArray = 1011,
  kCidArray = 1012,
  kInt8Array = 1016,
  kOidArray = 1028,
  kFloat4Array = 1021,
  kFloat8Array = 1022,
  kJsonArray = 199,
  kUuidArray = 2951,
  kNumericArray = 1231,
  kDateArray = 1182,
  kTimestampArray = 1115,
  kTimestampTzArray = 1185,
  kIntervalArray = 1187,
  kRegprocedureArray = 2207,
  kRegoperArray = 2208,
  kRegoperatorArray = 2209,
  kRegclassArray = 2210,
  kRegtypeArray = 2211,
  kRegconfigArray = 3735,
  kRegdictionaryArray = 3770,
  kRegnamespaceArray = 4090,
  kRegroleArray = 4097,
  kRegcollationArray = 4192,
  kXid8Array = 271,
};

constexpr int32_t Kind2Oid(velox::TypeKind kind, bool in_array) {
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
                "unsupported converting velox -> pg type kind: ",
                magic_enum::enum_name(kind));
  }
}

int32_t Type2Oid(const velox::TypePtr& type, bool in_array = false);
velox::TypePtr Oid2Type(int32_t oid);

std::string ToPgTypeString(const velox::Type& type);
std::string ToPgTypeString(const velox::TypePtr& type);

inline constexpr uint64_t kInvalidOid = 0;

std::string RegtypeOut(uint64_t oid);
uint64_t RegtypeIn(std::string_view name);

std::string RegclassOut(const catalog::Snapshot& snapshot, uint64_t oid);
uint64_t RegclassIn(const ConnectionContext& ctx, std::string_view name);

std::string RegnamespaceOut(const catalog::Snapshot& snapshot, uint64_t oid);
uint64_t RegnamespaceIn(const ConnectionContext& ctx, std::string_view name);

enum class VarFormat : int16_t;

enum class DeserializeError {
  InvalidRepresentation,
};

std::expected<velox::Variant, DeserializeError> DeserializeParameter(
  const velox::Type& type, VarFormat format, std::string_view data);

}  // namespace sdb::pg
