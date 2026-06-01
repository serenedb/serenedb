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

#include "catalog/identifiers/object_id.h"

namespace sdb {
namespace catalog {

struct Snapshot;

}  // namespace catalog

class ConnectionContext;

namespace pg {

using ParamIndex = int16_t;

inline constexpr uint64_t kInvalidOid = 0;

// Postgres stores date/time/timestamp from 2000-01-01
inline constexpr int64_t kGapDays =
  absl::CivilDay{2000, 1, 1} - absl::CivilDay{1970, 1, 1};
inline constexpr int64_t kGapSec = kGapDays * 24 * 60 * 60;
inline constexpr int64_t kGapMs = kGapSec * 1000;
inline constexpr int64_t kGapUs = kGapMs * 1000;
inline constexpr int64_t kGapNs = kGapUs * 1000;

enum PgTypeOID : int32_t {
  kBool = 16,
  kBoolArray = 1000,
  kBytea = 17,
  kByteaArray = 1001,
  kChar = 18,
  kCharArray = 1002,
  kName = 19,
  kNameArray = 1003,
  kInt8 = 20,
  kInt8Array = 1016,
  kInt2 = 21,
  kInt2Array = 1005,
  kInt2Vector = 22,
  kInt2VectorArray = 1006,
  kInt4 = 23,
  kInt4Array = 1007,
  kRegproc = 24,
  kRegprocArray = 1008,
  kText = 25,
  kTextArray = 1009,
  kOid = 26,
  kOidArray = 1028,
  kTid = 27,
  kTidArray = 1010,
  kXid = 28,
  kXidArray = 1011,
  kCid = 29,
  kCidArray = 1012,
  kOidvector = 30,
  kOidvectorArray = 1013,
  kPgType = 71,
  kPgTypeArray = 210,
  kPgAttribute = 75,
  kPgAttributeArray = 270,
  kPgProc = 81,
  kPgProcArray = 272,
  kPgClass = 83,
  kPgClassArray = 273,
  kJson = 114,
  kJsonArray = 199,
  kXml = 142,
  kXmlArray = 143,
  kPgNodeTree = 194,
  kPgNdistinct = 3361,
  kPgDependencies = 3402,
  kPgMcvList = 5017,
  kPgDdlCommand = 32,
  kXid8 = 5069,
  kXid8Array = 271,
  kPoint = 600,
  kPointArray = 1017,
  kLseg = 601,
  kLsegArray = 1018,
  kPath = 602,
  kPathArray = 1019,
  kBox = 603,
  kBoxArray = 1020,
  kPolygon = 604,
  kPolygonArray = 1027,
  kFloat4 = 700,
  kFloat4Array = 1021,
  kFloat8 = 701,
  kFloat8Array = 1022,
  kUnknown = 705,
  kCircle = 718,
  kCircleArray = 719,
  kMoney = 790,
  kMoneyArray = 791,
  kMacaddr = 829,
  kMacaddrArray = 1040,
  kInet = 869,
  kInetArray = 1041,
  kCidr = 650,
  kCidrArray = 651,
  kMacaddr8 = 774,
  kMacaddr8Array = 775,
  kAclitem = 1033,
  kAclitemArray = 1034,
  kBpchar = 1042,
  kBpcharArray = 1014,
  kVarchar = 1043,
  kVarcharArray = 1015,
  kDate = 1082,
  kDateArray = 1182,
  kTime = 1083,
  kTimeArray = 1183,
  kTimestamp = 1114,
  kTimestampArray = 1115,
  kTimestampTz = 1184,
  kTimestampTzArray = 1185,
  kInterval = 1186,
  kIntervalArray = 1187,
  kTimeTz = 1266,
  kTimeTzArray = 1270,
  kBit = 1560,
  kBitArray = 1561,
  kVarbit = 1562,
  kVarbitArray = 1563,
  kNumeric = 1700,
  kNumericArray = 1231,
  kRefcursor = 1790,
  kRefcursorArray = 2201,
  kRegprocedure = 2202,
  kRegprocedureArray = 2207,
  kRegoper = 2203,
  kRegoperArray = 2208,
  kRegoperator = 2204,
  kRegoperatorArray = 2209,
  kRegclass = 2205,
  kRegclassArray = 2210,
  kRegcollation = 4191,
  kRegcollationArray = 4192,
  kRegtype = 2206,
  kRegtypeArray = 2211,
  kRegrole = 4096,
  kRegroleArray = 4097,
  kRegnamespace = 4089,
  kRegnamespaceArray = 4090,
  kUuid = 2950,
  kUuidArray = 2951,
  kPgLsn = 3220,
  kPgLsnArray = 3221,
  kTsvector = 3614,
  kTsvectorArray = 3643,
  kGtsvector = 3642,
  kGtsvectorArray = 3644,
  kTsquery = 3615,
  kTsqueryArray = 3645,
  kRegconfig = 3734,
  kRegconfigArray = 3735,
  kRegdictionary = 3769,
  kRegdictionaryArray = 3770,
  kJsonb = 3802,
  kJsonbArray = 3807,
  kJsonpath = 4072,
  kJsonpathArray = 4073,
  kTxidSnapshot = 2970,
  kTxidSnapshotArray = 2949,
  kPgSnapshot = 5038,
  kPgSnapshotArray = 5039,
  kInt4Range = 3904,
  kInt4RangeArray = 3905,
  kNumrange = 3906,
  kNumrangeArray = 3907,
  kTsrange = 3908,
  kTsrangeArray = 3909,
  kTstzrange = 3910,
  kTstzrangeArray = 3911,
  kDaterange = 3912,
  kDaterangeArray = 3913,
  kInt8Range = 3926,
  kInt8RangeArray = 3927,
  kInt4Multirange = 4451,
  kInt4MultirangeArray = 6150,
  kNummultirange = 4532,
  kNummultirangeArray = 6151,
  kTsmultirange = 4533,
  kTsmultirangeArray = 6152,
  kTstzmultirange = 4534,
  kTstzmultirangeArray = 6153,
  kDatemultirange = 4535,
  kDatemultirangeArray = 6155,
  kInt8Multirange = 4536,
  kInt8MultirangeArray = 6157,
  kRecord = 2249,
  kRecordArray = 2287,
  kCstring = 2275,
  kCstringArray = 1263,
  kAny = 2276,
  kAnyarray = 2277,
  kVoid = 2278,
  kTrigger = 2279,
  kEventTrigger = 3838,
  kLanguageHandler = 2280,
  kInternal = 2281,
  kAnyelement = 2283,
  kAnynonarray = 2776,
  kAnyenum = 3500,
  kFdwHandler = 3115,
  kIndexAmHandler = 325,
  kTsmHandler = 3310,
  kTableAmHandler = 269,
  kAnyrange = 3831,
  kAnycompatible = 5077,
  kAnycompatiblearray = 5078,
  kAnycompatiblenonarray = 5079,
  kAnycompatiblerange = 5080,
  kAnymultirange = 4537,
  kAnycompatiblemultirange = 4538,
  kPgBrinBloomSummary = 4600,
  kPgBrinMinmaxMultiSummary = 4601,
  kVariant = id::kCustomOidBase.id(),
  kVariantArray,
  kMaxPgTypeOID,
};

static_assert(kMaxPgTypeOID < id::kCustomOidMax.id());

int32_t Type2Oid(const duckdb::LogicalType& type, bool in_array = false);
duckdb::LogicalType Oid2Type(int32_t oid, const catalog::Snapshot& snapshot);

std::string RegtypeOut(uint64_t oid);
uint64_t RegtypeIn(std::string_view name);

std::string RegclassOut(const catalog::Snapshot& snapshot, uint64_t oid);
uint64_t RegclassIn(const ConnectionContext& ctx, std::string_view name);

std::string RegnamespaceOut(const catalog::Snapshot& snapshot, uint64_t oid);
uint64_t RegnamespaceIn(const ConnectionContext& ctx, std::string_view name);

std::string ToPgTypeString(const duckdb::LogicalType& type);

enum class VarFormat : int16_t;

enum class DeserializeError { InvalidRepresentation };

// Deserialize a PG wire protocol parameter value into a DuckDB Value.
// Snapshot is consulted for inner Oid2Type calls on nested types (e.g. the
// element OID inside a binary-format array).
std::expected<duckdb::Value, DeserializeError> DeserializeParameter(
  const duckdb::LogicalType& type, VarFormat format, std::string_view data,
  const catalog::Snapshot& snapshot);

}  // namespace pg
}  // namespace sdb
