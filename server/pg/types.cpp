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
#include <absl/container/flat_hash_map.h>

#include <array>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/common/extension_type_info.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <span>
#include <string>
#include <utility>

#include "connector/functions/ts_query_codec.h"
#include "connector/pg_logical_types.h"
#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/functions/format_type.h"
#include "pg/catalog/lookup.h"

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

constexpr std::pair<TypeId, int32_t> kIdTypes[] = {
  {TypeId::BOOLEAN, kBool},
  {TypeId::TINYINT, kInt2},
  {TypeId::UTINYINT, kInt2},
  {TypeId::SMALLINT, kInt2},
  {TypeId::USMALLINT, kInt4},
  {TypeId::INTEGER, kInt4},
  {TypeId::UINTEGER, kInt8},
  {TypeId::BIGINT, kInt8},
  {TypeId::HUGEINT, kNumeric},
  {TypeId::UHUGEINT, kNumeric},
  {TypeId::UBIGINT, kNumeric},
  {TypeId::BIGNUM, kNumeric},
  {TypeId::DECIMAL, kNumeric},
  {TypeId::DATE, kDate},
  {TypeId::TIME, kTime},
  {TypeId::TIME_NS, kTime},
  {TypeId::TIMESTAMP_SEC, kTimestamp},
  {TypeId::TIMESTAMP_MS, kTimestamp},
  {TypeId::TIMESTAMP, kTimestamp},
  {TypeId::TIMESTAMP_NS, kTimestamp},
  {TypeId::TIMESTAMP_TZ, kTimestamptz},
  {TypeId::TIMESTAMP_TZ_NS, kTimestamptz},
  {TypeId::TIME_TZ, kTimetz},
  {TypeId::FLOAT, kFloat4},
  {TypeId::DOUBLE, kFloat8},
  {TypeId::CHAR, kText},
  {TypeId::VARCHAR, kText},
  {TypeId::ENUM, kText},
  {TypeId::BLOB, kBytea},
  {TypeId::INTERVAL, kInterval},
  {TypeId::BIT, kVarbit},
  {TypeId::UUID, kUuid},
  {TypeId::STRUCT, kRecord},
  {TypeId::TUPLE, kRecord},
  {TypeId::GEOMETRY, kGeometry},
  {TypeId::VARIANT, kVariant},
  {TypeId::UNION, kUnion},
};
// clang-format on

const PgTypeMapping* FindMapping(int32_t oid) {
  const auto* it = absl::c_find_if(
    kMappings,
    [&](const PgTypeMapping& mapping) { return mapping.oid == oid; });
  return it == std::end(kMappings) ? nullptr : it;
}

const BuiltinType* AliasType(const duckdb::LogicalType& type) {
  static const auto kAliases = [] {
    absl::flat_hash_map<std::string, std::pair<TypeId, const BuiltinType*>>
      aliases;
    for (const auto& mapping : kMappings) {
      if (const auto logical = mapping.logical(); logical.HasAlias()) {
        aliases.try_emplace(logical.GetAlias(), logical.id(),
                            FindBuiltinType(mapping.oid));
      }
    }
    for (const auto& builtin : BuiltinTypes()) {
      if (builtin.relid != 0) {
        aliases.try_emplace(std::string{builtin.name}, TypeId::STRUCT,
                            &builtin);
      }
    }
    return aliases;
  }();
  if (!type.HasAlias()) {
    return nullptr;
  }
  const auto it = kAliases.find(type.GetTypeInfo().alias);
  return it != kAliases.end() && it->second.first == type.id()
           ? it->second.second
           : nullptr;
}

const BuiltinType* IdType(TypeId id) {
  static const auto kIds = [] {
    std::array<const BuiltinType*, 256> ids{};
    for (const auto& [type, oid] : kIdTypes) {
      ids[std::to_underlying(type)] = FindBuiltinType(oid);
    }
    return ids;
  }();
  return kIds[std::to_underlying(id)];
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
  const auto id = type.id();
  const auto* builtin = AliasType(type);
  int32_t typmod = -1;
  if (!builtin) {
    if (id == TypeId::LIST) {
      return Logical2Pg(duckdb::ListType::GetChildType(type), true);
    }
    if (id == TypeId::ARRAY) {
      return Logical2Pg(duckdb::ArrayType::GetChildType(type), true);
    }
    if (id == TypeId::MAP) {
      return {kRecordArray, -1, -1};
    }
    if (id == TypeId::ENUM || id == TypeId::STRUCT || id == TypeId::TUPLE) {
      if (const auto oid = UserTypeOid(type)) {
        return {static_cast<uint32_t>(in_array ? TypeArrayOid(*oid) : *oid),
                static_cast<int16_t>(in_array || id != TypeId::ENUM ? -1 : 4),
                -1};
      }
    }
    if (id == TypeId::DECIMAL) {
      typmod = NumericTypmod(duckdb::DecimalType::GetWidth(type),
                             duckdb::DecimalType::GetScale(type));
    }
    builtin =
      IdType(connector::IsTSQueryStructType(type) ? TypeId::VARCHAR : id);
  }
  if (!builtin) {
    return {kUnknown, -1, -1};
  }
  if (!in_array) {
    return {static_cast<uint32_t>(builtin->oid), builtin->len, typmod};
  }
  if (builtin->array == 0) {
    return Logical2Pg(duckdb::LogicalType{id}, true);
  }
  return {static_cast<uint32_t>(builtin->array), -1, typmod};
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

std::span<const PgTypeMapping> PgTypeMappings() { return kMappings; }

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
         !AliasType(*element)) {
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
