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

#include "pg/types.h"

#include <absl/algorithm/container.h>

#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/common/extension_type_info.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <span>
#include <string>

#include "connector/functions/ts_query_codec.h"
#include "connector/pg_logical_types.h"
#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/functions/pg_catalog/format_type.h"
#include "pg/catalog/utils/lookup.h"

namespace sdb::pg {
namespace {

using TypeId = duckdb::LogicalTypeId;

// clang-format off
constexpr PgTypeMapping kMappings[] = {
  {kBool, []() -> duckdb::LogicalType { return duckdb::LogicalType::BOOLEAN; }, false},
  {kBytea, []() -> duckdb::LogicalType { return duckdb::LogicalType::BLOB; }, false},
  {kChar, &CHAR, true},
  {kName, &NAME, true},
  {kInt8, []() -> duckdb::LogicalType { return duckdb::LogicalType::BIGINT; }, false},
  {kInt2, []() -> duckdb::LogicalType { return duckdb::LogicalType::SMALLINT; }, false},
  {kInt2Vector, &INT2VECTOR, true},
  {kInt4, []() -> duckdb::LogicalType { return duckdb::LogicalType::INTEGER; }, false},
  {kRegproc, &REGPROC, true},
  {kText, []() -> duckdb::LogicalType { return duckdb::LogicalType::VARCHAR; }, false},
  {kOid, &OID, true},
  {kTid, &TID, true},
  {kXid, &XID, true},
  {kCid, &CID, true},
  {kOidvector, &OIDVECTOR, true},
  {kJson, []() -> duckdb::LogicalType { return duckdb::LogicalType::JSON(); }, false},
  {kPgNodeTree, &PG_NODE_TREE, true},
  {kFloat4, []() -> duckdb::LogicalType { return duckdb::LogicalType::FLOAT; }, false},
  {kFloat8, []() -> duckdb::LogicalType { return duckdb::LogicalType::DOUBLE; }, false},
  {kUnknown, []() -> duckdb::LogicalType { return duckdb::LogicalType::UNKNOWN; }, false},
  {kInet, &INET, false},
  {kAclitem, &ACLITEM, true},
  {kBpchar, []() -> duckdb::LogicalType { return duckdb::LogicalType::VARCHAR; }, false},
  {kVarchar, []() -> duckdb::LogicalType { return duckdb::LogicalType::VARCHAR; }, false},
  {kDate, []() -> duckdb::LogicalType { return duckdb::LogicalType::DATE; }, false},
  {kTime, []() -> duckdb::LogicalType { return duckdb::LogicalType::TIME; }, false},
  {kTimestamp, []() -> duckdb::LogicalType { return duckdb::LogicalType::TIMESTAMP; }, false},
  {kTimestamptz, []() -> duckdb::LogicalType { return duckdb::LogicalType::TIMESTAMP_TZ; }, false},
  {kInterval, []() -> duckdb::LogicalType { return duckdb::LogicalType::INTERVAL; }, false},
  {kTimetz, []() -> duckdb::LogicalType { return duckdb::LogicalType::TIME_TZ; }, false},
  {kBit, []() -> duckdb::LogicalType { return duckdb::LogicalType::BIT; }, false},
  {kVarbit, []() -> duckdb::LogicalType { return duckdb::LogicalType::BIT; }, true},
  {kNumeric, []() -> duckdb::LogicalType { return duckdb::LogicalType::DECIMAL(18, 3); }, false},
  {kRegprocedure, &REGPROCEDURE, true},
  {kRegoper, &REGOPER, true},
  {kRegoperator, &REGOPERATOR, true},
  {kRegclass, &REGCLASS, true},
  {kRegtype, &REGTYPE, true},
  {kAnyarray, &ANYARRAY, false},
  {kVoid, &VOID, true},
  {kUuid, []() -> duckdb::LogicalType { return duckdb::LogicalType::UUID; }, false},
  {kPgLsn, &PG_LSN, false},
  {kPgNdistinct, &PG_NDISTINCT, false},
  {kPgDependencies, &PG_DEPENDENCIES, false},
  {kRegconfig, &REGCONFIG, true},
  {kRegdictionary, &REGDICTIONARY, true},
  {kJsonb, []() -> duckdb::LogicalType { return duckdb::LogicalType::JSON(); }, false},
  {kRegnamespace, &REGNAMESPACE, true},
  {kRegrole, &REGROLE, true},
  {kRegcollation, &REGCOLLATION, true},
  {kPgMcvList, &PG_MCV_LIST, false},
  {kXid8, &XID8, true},
  {kCardinalNumber, &CARDINALNUMBER, true},
  {kCharacterData, &CHARACTERDATA, true},
  {kSqlIdentifier, &SQLIDENTIFIER, true},
  {kTimeStamp, &TIMESTAMP, true},
  {kYesOrNo, &YESORNO, true},
  {kVariant, []() -> duckdb::LogicalType { return duckdb::LogicalType::VARIANT(); }, false},
  {kUnion, []() -> duckdb::LogicalType { return duckdb::LogicalType::VARCHAR; }, false},
  {kGeometry, []() -> duckdb::LogicalType { return duckdb::LogicalType::GEOMETRY(); }, false},
};
// clang-format on

const PgTypeMapping* FindMapping(int32_t oid) {
  const auto* it = absl::c_find_if(
    kMappings,
    [&](const PgTypeMapping& mapping) { return mapping.oid == oid; });
  return it == std::end(kMappings) ? nullptr : it;
}

std::optional<uint64_t> UserTypeOid(const duckdb::LogicalType& type) {
  const auto ext = type.GetExtensionInfo();
  if (!ext) {
    return std::nullopt;
  }
  const auto it =
    ext->properties.find(duckdb::ExtensionTypeInfo::CATALOG_OID_PROPERTY);
  if (it == ext->properties.end()) {
    return std::nullopt;
  }
  return it->second.GetValue<uint64_t>();
}

PgTypeInfo Logical2Pg(const duckdb::LogicalType& type, bool in_array) {
  int32_t typmod = -1;
  const auto make = [&](int32_t oid) -> PgTypeInfo {
    const auto& builtin = *FindBuiltinType(oid);
    if (!in_array) {
      return {static_cast<uint32_t>(builtin.oid), builtin.len, typmod};
    }
    if (builtin.array == 0) {
      return Logical2Pg(duckdb::LogicalType{type.id()}, true);
    }
    return {static_cast<uint32_t>(builtin.array), -1, typmod};
  };
  switch (type.id()) {
    using enum TypeId;
    case BOOLEAN:
      return make(kBool);
    case TINYINT:
    case UTINYINT:
    case SMALLINT:
      return make(kInt2);
    case USMALLINT:
      return make(kInt4);
    case INTEGER:
      if (IsCardinalNumber(type)) {
        return make(kCardinalNumber);
      }
      return make(kInt4);
    case UINTEGER:
      return make(kInt8);
    case BIGINT:
      if (IsOid(type)) {
        return make(kOid);
      }
      if (IsXid(type)) {
        return make(kXid);
      }
      if (IsCid(type)) {
        return make(kCid);
      }
      if (IsTid(type)) {
        return make(kTid);
      }
      if (IsXid8(type)) {
        return make(kXid8);
      }
      if (IsRegproc(type)) {
        return make(kRegproc);
      }
      if (IsRegprocedure(type)) {
        return make(kRegprocedure);
      }
      if (IsRegoper(type)) {
        return make(kRegoper);
      }
      if (IsRegoperator(type)) {
        return make(kRegoperator);
      }
      if (IsRegclass(type)) {
        return make(kRegclass);
      }
      if (IsRegtype(type)) {
        return make(kRegtype);
      }
      if (IsRegrole(type)) {
        return make(kRegrole);
      }
      if (IsRegnamespace(type)) {
        return make(kRegnamespace);
      }
      if (IsRegconfig(type)) {
        return make(kRegconfig);
      }
      if (IsRegdictionary(type)) {
        return make(kRegdictionary);
      }
      if (IsRegcollation(type)) {
        return make(kRegcollation);
      }
      return make(kInt8);
    case UBIGINT:
      if (IsPgLsn(type)) {
        return make(kPgLsn);
      }
      return make(kNumeric);
    case HUGEINT:
    case UHUGEINT:
    case BIGNUM:
      return make(kNumeric);
    case DECIMAL:
      typmod = NumericTypmod(duckdb::DecimalType::GetWidth(type),
                             duckdb::DecimalType::GetScale(type));
      return make(kNumeric);
    case DATE:
      return make(kDate);
    case TIME:
    case TIME_NS:
      return make(kTime);
    case TIMESTAMP_SEC:
    case TIMESTAMP_MS:
    case TIMESTAMP:
    case TIMESTAMP_NS:
      return make(kTimestamp);
    case TIMESTAMP_TZ:
      if (IsTimeStamp(type)) {
        return make(kTimeStamp);
      }
      return make(kTimestamptz);
    case TIMESTAMP_TZ_NS:
      return make(kTimestamptz);
    case TIME_TZ:
      return make(kTimetz);
    case FLOAT:
      return make(kFloat4);
    case DOUBLE:
      return make(kFloat8);
    case CHAR:
      return make(kText);
    case BLOB:
      return make(kBytea);
    case INTERVAL:
      return make(kInterval);
    case BIT:
      return make(kVarbit);
    case UUID:
      return make(kUuid);
    case VARCHAR:
      if (type.IsJSONType()) {
        return make(kJson);
      }
      if (IsName(type)) {
        return make(kName);
      }
      if (IsChar(type)) {
        return make(kChar);
      }
      if (IsAclitem(type)) {
        return make(kAclitem);
      }
      if (IsPgNodeTree(type)) {
        return make(kPgNodeTree);
      }
      if (IsAnyarray(type)) {
        return make(kAnyarray);
      }
      if (IsPgNdistinct(type)) {
        return make(kPgNdistinct);
      }
      if (IsPgDependencies(type)) {
        return make(kPgDependencies);
      }
      if (IsPgMcvList(type)) {
        return make(kPgMcvList);
      }
      if (IsCharacterData(type)) {
        return make(kCharacterData);
      }
      if (IsSqlIdentifier(type)) {
        return make(kSqlIdentifier);
      }
      if (IsYesOrNo(type)) {
        return make(kYesOrNo);
      }
      return make(kText);
    case ENUM:
      if (const auto oid = UserTypeOid(type)) {
        return {static_cast<uint32_t>(in_array ? TypeArrayOid(*oid) : *oid),
                static_cast<int16_t>(in_array ? -1 : 4), -1};
      }
      return make(kText);
    case STRUCT:
    case TUPLE:
      if (IsInet(type)) {
        return make(kInet);
      }
      if (connector::IsTSQueryStructType(type)) {
        return make(kText);
      }
      if (const auto oid = UserTypeOid(type)) {
        return {static_cast<uint32_t>(in_array ? TypeArrayOid(*oid) : *oid), -1,
                -1};
      }
      for (const auto& schema : kSystemNamespaces) {
        if (const auto* row = FindBuiltinType(schema.oid, type.GetAlias());
            row && row->relid != 0) {
          return make(row->oid);
        }
      }
      return make(kRecord);
    case MAP:
      return {kRecordArray, -1, -1};
    case LIST:
      if (IsInt2vector(type)) {
        return make(kInt2Vector);
      }
      if (IsOidvector(type)) {
        return make(kOidvector);
      }
      return Logical2Pg(duckdb::ListType::GetChildType(type), true);
    case ARRAY:
      return Logical2Pg(duckdb::ArrayType::GetChildType(type), true);
    case SQLNULL:
      if (IsVoid(type)) {
        return make(kVoid);
      }
      return {kUnknown, -1, -1};
    case GEOMETRY:
      return make(kGeometry);
    case VARIANT:
      return make(kVariant);
    case UNION:
      return make(kUnion);
    case INVALID:
    case UNKNOWN:
    case ANY:
    case UNBOUND:
    case TEMPLATE:
    case TYPE:
    case STRING_LITERAL:
    case INTEGER_LITERAL:
    case POINTER:
    case VALIDITY:
    case TABLE:
    case LEGACY_AGGREGATE_STATE:
    case LAMBDA:
      return {kUnknown, -1, -1};
  }
}

}  // namespace

PgTypeInfo WireType(const duckdb::LogicalType& type) {
  auto info = Logical2Pg(type, false);
  if (info.oid == kAclitem) {
    return {kText, -1, -1};
  }
  if (info.oid == kAclitemArray) {
    return {kTextArray, -1, -1};
  }
  if (const auto* domain = FindBuiltinType(info.oid);
      domain && domain->type == 'd') {
    const auto& base = *FindBuiltinType(domain->basetype);
    info.oid = base.oid;
    info.typlen = base.len;
  }
  return info;
}

uint32_t Type2Oid(const duckdb::LogicalType& type) {
  return Logical2Pg(type, false).oid;
}

duckdb::LogicalType Oid2Type(uint64_t oid, duckdb::ClientContext& context) {
  if (oid < kMaxSystem) {
    if (const auto* builtin = FindBuiltinType(static_cast<int32_t>(oid))) {
      if (auto type = BuiltinLogicalType(*builtin);
          type.id() != TypeId::INVALID) {
        return type;
      }
    }
  }
  const auto session = MakeSession(&context);
  if (auto entry = EntryByOid(session, oid);
      entry && entry->type == duckdb::CatalogType::TYPE_ENTRY) {
    return entry->Cast<duckdb::TypeCatalogEntry>().user_type;
  }
  if (const auto element = ArrayElementOid(session, oid);
      element != kInvalidOid) {
    return duckdb::LogicalType::LIST(Oid2Type(element, context));
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                  ERR_MSG("cache lookup failed for type ", oid));
}

std::span<const PgTypeMapping> PgTypeMappings() {
#ifdef SDB_DEV
  static const bool kRoundTrips =
    absl::c_all_of(kMappings, [](const PgTypeMapping& mapping) {
      const auto logical = mapping.logical();
      const auto* back = FindMapping(static_cast<int32_t>(Type2Oid(logical)));
      return back && back->logical() == logical;
    });
  SDB_ASSERT(kRoundTrips);
#endif
  return kMappings;
}

duckdb::LogicalType BuiltinLogicalType(const BuiltinType& type) {
  if (const auto* mapping = FindMapping(type.oid)) {
    return mapping->logical();
  }
  if (type.IsArray()) {
    if (const auto* element = FindBuiltinType(type.elem)) {
      auto child = BuiltinLogicalType(*element);
      if (child.id() != TypeId::INVALID) {
        return duckdb::LogicalType::LIST(std::move(child));
      }
    }
  }
  return TypeId::INVALID;
}

ColumnType DescribeColumnType(const duckdb::LogicalType& type) {
  const auto info = Logical2Pg(type, false);
  int16_t ndims = 0;
  const auto* element = &type;
  while ((element->id() == TypeId::LIST || element->id() == TypeId::ARRAY) &&
         !IsInt2vector(*element) && !IsOidvector(*element)) {
    ++ndims;
    element = element->id() == TypeId::LIST
                ? &duckdb::ListType::GetChildType(*element)
                : &duckdb::ArrayType::GetChildType(*element);
  }
  ColumnType result{.oid = info.oid,
                    .typmod = info.typmod,
                    .len = -1,
                    .byval = false,
                    .align = 'i',
                    .storage = 'x',
                    .collation = 0,
                    .ndims = ndims};
  if (const auto* builtin = FindBuiltinType(info.oid)) {
    result.len = builtin->len;
    result.byval = builtin->byval;
    result.align = builtin->align;
    result.storage = builtin->storage;
    const auto collation = element->id() == TypeId::VARCHAR
                             ? duckdb::StringType::GetCollation(*element)
                             : std::string{};
    result.collation =
      collation.empty() ? builtin->collation : CollationOid(collation);
    return result;
  }
  const bool is_enum = element->id() == TypeId::ENUM;
  result.align = is_enum ? 'i' : 'd';
  if (ndims == 0 && is_enum) {
    result.len = 4;
    result.byval = true;
    result.storage = 'p';
  }
  return result;
}

}  // namespace sdb::pg
