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

#include <duckdb/common/types/value.hpp>
#include <expected>
#include <magic_enum/magic_enum.hpp>

#include "basics/exceptions.h"
#include "basics/fwd.h"

namespace sdb {
namespace catalog {

struct Snapshot;

}  // namespace catalog

class ConnectionContext;

namespace pg {

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

int32_t Type2Oid(const duckdb::LogicalType& type, bool in_array = false);
duckdb::LogicalType Oid2Type(int32_t oid);

std::string ToPgTypeString(const duckdb::LogicalType& type);

inline constexpr uint64_t kInvalidOid = 0;

std::string RegtypeOut(uint64_t oid);
uint64_t RegtypeIn(std::string_view name);

std::string RegclassOut(const catalog::Snapshot& snapshot, uint64_t oid);
uint64_t RegclassIn(const ConnectionContext& ctx, std::string_view name);

std::string RegnamespaceOut(const catalog::Snapshot& snapshot, uint64_t oid);
uint64_t RegnamespaceIn(const ConnectionContext& ctx, std::string_view name);

enum class VarFormat : int16_t;

enum class DeserializeError { InvalidRepresentation };

// Deserialize a PG wire protocol parameter value into a DuckDB Value.
std::expected<duckdb::Value, DeserializeError> DuckDBDeserializeParameter(
  const duckdb::LogicalType& type, VarFormat format, std::string_view data);

}  // namespace pg
}  // namespace sdb
