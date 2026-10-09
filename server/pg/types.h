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

#include <absl/time/civil_time.h>

#include <cstdint>
#include <duckdb/common/types.hpp>
#include <duckdb/main/client_context.hpp>
#include <span>

#include "connector/pg_logical_types.h"
#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/oids.h"

namespace sdb::pg {

using ParamIndex = int16_t;

// Postgres stores date/time/timestamp from 2000-01-01
inline constexpr int64_t kGapDays =
  absl::CivilDay{2000, 1, 1} - absl::CivilDay{1970, 1, 1};
inline constexpr int64_t kGapSec = kGapDays * 24 * 60 * 60;
inline constexpr int64_t kGapMs = kGapSec * 1000;
inline constexpr int64_t kGapUs = kGapMs * 1000;
inline constexpr int64_t kGapNs = kGapUs * 1000;

enum PgTypeOID : int32_t {
#include "pg/catalog/builtin/type_oids.gen.inc"
  kVariant = kMinSystem + 100,
  kVariantArray = kMinSystem + 101,
  kTsquery = kMinSystem + 102,
  kTsqueryArray = kMinSystem + 103,
  kUnion = kMinSystem + 104,
  kUnionArray = kMinSystem + 105,
  kGeometry = kMinSystem + 106,
  kGeometryArray = kMinSystem + 107,
};

// A column's pg_type identity for RowDescription: the type OID, typlen (the
// fixed byte width of a fixed-length type, or -1 for a varlena type), and
// typmod (the type modifier, e.g. DECIMAL precision/scale, or -1 for none).
struct PgTypeInfo {
  uint32_t oid;
  int16_t typlen;
  int32_t typmod;
};
PgTypeInfo Logical2Pg(const duckdb::LogicalType& type, bool in_array = false);
PgTypeInfo WireType(const duckdb::LogicalType& type);
uint32_t Type2Oid(const duckdb::LogicalType& type, bool in_array = false);
duckdb::LogicalType Oid2Type(uint64_t oid, duckdb::ClientContext& context);

struct PgTypeMapping {
  int32_t oid;
  duckdb::LogicalType (*logical)();
  bool ddl;
};

struct ColumnType {
  uint32_t oid;
  int32_t typmod;
  int16_t len;
  bool byval;
  char align;
  char storage;
  int32_t collation;
  int16_t ndims;
};

std::span<const PgTypeMapping> PgTypeMappings();
duckdb::LogicalType BuiltinLogicalType(const BuiltinType& type);
ColumnType DescribeColumnType(const duckdb::LogicalType& type);

enum class VarFormat : int16_t;

}  // namespace sdb::pg
