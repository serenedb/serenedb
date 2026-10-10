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

#include <duckdb/common/optional_ptr.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/qualified_name.hpp>
#include <expected>
#include <iresearch/utils/assert.hpp>
#include <magic_enum/magic_enum.hpp>
#include <string>
#include <string_view>

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb {

class ConnectionContext;

namespace pg {

using ParamIndex = int16_t;

inline constexpr uint64_t kInvalidOid = 0;

constexpr uint32_t WireOid(uint64_t oid) { return static_cast<uint32_t>(oid); }

constexpr uint64_t OidFromSql(int64_t value) {
  return static_cast<uint64_t>(value);
}

constexpr int64_t OidToSql(uint64_t oid) { return static_cast<int64_t>(oid); }

// Postgres' PUBLIC pseudo-role. It is not a role id at all: 0 is the oid no
// pg_authid row can carry, which is what lets an acl item name "everybody".
inline constexpr duckdb::idx_t kPublicGrantee = 0;

inline constexpr duckdb::idx_t kMinSystem = 16384;
inline constexpr duckdb::idx_t kMaxSystem = 65536;

inline constexpr duckdb::idx_t kPgCatalogSchema = 11;
inline constexpr duckdb::idx_t kPgInformationSchema = kMinSystem + 3;
inline constexpr duckdb::idx_t kPgPublicSchema = 2200;
inline constexpr duckdb::idx_t kPgMainSchema = kMinSystem + 4;
inline constexpr duckdb::idx_t kPgPostgresDatabase = 5;

inline constexpr duckdb::idx_t kRootUser = kMinSystem;

inline constexpr duckdb::idx_t kPgAmInverted = kMinSystem + 300;
inline constexpr duckdb::idx_t kPgAmIResearch = kMinSystem + 301;
inline constexpr duckdb::idx_t kPgAmSecondary = kMinSystem + 303;

inline constexpr duckdb::idx_t kPgOpclassIvf = kMinSystem + 200;
inline constexpr duckdb::idx_t kPgOpclassIncluded = kMinSystem + 201;
inline constexpr duckdb::idx_t kPgOpclassHnsw = kMinSystem + 202;
inline constexpr duckdb::idx_t kPgOpclassCurve = kMinSystem + 203;
inline constexpr duckdb::idx_t kPgOpclassCartesian = kMinSystem + 204;

inline constexpr duckdb::idx_t kFirstSystemView = kMinSystem + 1000;
inline constexpr duckdb::idx_t kFirstBuiltinFunction = kMinSystem + 10'000;

inline uint64_t TypeArrayOid(uint64_t element_oid) {
  SDB_ASSERT(element_oid > kMaxSystem);
  return element_oid - 1;
}

// Postgres stores date/time/timestamp from 2000-01-01
inline constexpr int64_t kGapDays =
  absl::CivilDay{2000, 1, 1} - absl::CivilDay{1970, 1, 1};
inline constexpr int64_t kGapSec = kGapDays * 24 * 60 * 60;
inline constexpr int64_t kGapMs = kGapSec * 1000;
inline constexpr int64_t kGapUs = kGapMs * 1000;
inline constexpr int64_t kGapNs = kGapUs * 1000;

enum PgTypeOID : uint64_t {
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
  uint64_t oid;
  int16_t typlen;
  int32_t typmod;
};
PgTypeInfo Logical2Pg(const duckdb::LogicalType& type, bool in_array = false);
uint64_t Type2Oid(const duckdb::LogicalType& type, bool in_array = false);
duckdb::LogicalType Oid2Type(uint64_t oid, duckdb::ClientContext& context);

std::string RegtypeOut(duckdb::ClientContext* context, uint64_t oid);
uint64_t RegtypeIn(std::string_view name);

std::string RegclassOut(duckdb::ClientContext* context, uint64_t oid);
uint64_t RegclassIn(const ConnectionContext& ctx, std::string_view name);

uint64_t ResolveRelation(duckdb::ClientContext& context,
                         const duckdb::QualifiedName& name);
std::string RelationName(duckdb::ClientContext& context,
                         std::string_view schema, std::string_view name,
                         uint64_t oid);

std::string RegnamespaceOut(duckdb::ClientContext* context, uint64_t oid);
uint64_t RegnamespaceIn(const ConnectionContext& ctx, std::string_view name);

enum class VarFormat : int16_t;

}  // namespace pg
}  // namespace sdb
