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
#include "pg/serialize.h"
#include "pg/sql_collector.h"
#include "pg/system_catalog.h"
#include "query/types.h"

namespace sdb::pg {
namespace {

int32_t GetCompositeOID(const velox::TypePtr& type, bool in_array) {
  if (type->isTimestamp()) {
    return in_array ? PgTypeOID::kTimestampArray : PgTypeOID::kTimestamp;
  } else if (isJsonType(type)) {
    return in_array ? PgTypeOID::kJsonArray : PgTypeOID::kJson;
  } else if (isUuidType(type)) {
    return in_array ? PgTypeOID::kUuidArray : PgTypeOID::kUuid;
  } else if (type->isDecimal()) {
    return in_array ? PgTypeOID::kNumericArray : PgTypeOID::kNumeric;
  } else if (type->isArray()) {
    return GetTypeOID(type->asArray().elementType(), true);
  } else if (isTimestampWithTimeZoneType(type)) {
    return in_array ? PgTypeOID::kTimestampTzArray : PgTypeOID::kTimestampTz;
  } else if (type->isDate()) {
    return in_array ? PgTypeOID::kDateArray : PgTypeOID::kDate;
  } else if (IsInterval(type)) {
    return in_array ? PgTypeOID::kIntervalArray : PgTypeOID::kInterval;
  } else if (IsRegtype(type)) {
    return in_array ? PgTypeOID::kRegtypeArray : PgTypeOID::kRegtype;
  } else if (IsRegclass(type)) {
    return in_array ? PgTypeOID::kRegclassArray : PgTypeOID::kRegclass;
  }
  return -1;
}

}  // namespace

int32_t GetTypeOID(const velox::TypePtr& type, bool in_array) {
  int32_t composite_oid = GetCompositeOID(type, in_array);
  if (composite_oid >= 0) {
    return composite_oid;
  }
  return GetPrimitiveTypeOID(type->kind(), in_array);
}

std::string ToPgTypeString(const velox::Type& type) {
  return ToPgTypeString(velox::TypePtr{velox::TypePtr{}, &type});
}

std::string ToPgTypeString(const velox::TypePtr& type) {
  if (!type) [[unlikely]] {
    return "unknown";
  }
  if (type->isArray()) {
    return ToPgTypeString(type->asArray().elementType()) + "[]";
  }
  if (type->isDecimal()) {
    return "numeric";
  }
  if (type->isDate()) {
    return "date";
  }
  if (IsInterval(type)) {
    return "interval";
  }
  if (IsRegtype(type)) {
    return "regtype";
  }
  if (IsRegclass(type)) {
    return "regclass";
  }
  if (isUuidType(type)) {
    return "uuid";
  }
  if (isJsonType(type)) {
    return "json";
  }
  if (isTimestampWithTimeZoneType(type)) {
    return "timestamp with time zone";
  }
  switch (type->kind()) {
    case velox::TypeKind::BOOLEAN:
      return "boolean";
    case velox::TypeKind::TINYINT:
      return "character";
    case velox::TypeKind::SMALLINT:
      return "smallint";
    case velox::TypeKind::INTEGER:
      return "integer";
    case velox::TypeKind::BIGINT:
      return "bigint";
    case velox::TypeKind::REAL:
      return "real";
    case velox::TypeKind::DOUBLE:
      return "double precision";
    case velox::TypeKind::VARCHAR:
      return "text";
    case velox::TypeKind::VARBINARY:
      return "bytea";
    case velox::TypeKind::TIMESTAMP:
      return "timestamp without time zone";
    case velox::TypeKind::UNKNOWN:
      return "unknown";
    default:
      SDB_ASSERT(false);  // better to specify the name
      return "unknown";
  }
}

// clang-format off
#define REGTYPE_OUT(oid, type_name)                        \
    case PgTypeOID::oid: return type_name;                 \
    case PgTypeOID::oid##Array: return type_name "[]";

std::string RegtypeOut(int32_t oid) {
  switch (static_cast<PgTypeOID>(oid)) {
    REGTYPE_OUT(kBool, "boolean")
    REGTYPE_OUT(kBytea, "bytea")
    REGTYPE_OUT(kChar, "character")
    REGTYPE_OUT(kInt2, "smallint")
    REGTYPE_OUT(kInt4, "integer")
    REGTYPE_OUT(kInt8, "bigint")
    REGTYPE_OUT(kFloat4, "real")
    REGTYPE_OUT(kFloat8, "double precision")
    REGTYPE_OUT(kText, "text")
    REGTYPE_OUT(kVarchar, "character varying")
    REGTYPE_OUT(kJson, "json")
    REGTYPE_OUT(kUuid, "uuid")
    REGTYPE_OUT(kNumeric, "numeric")
    REGTYPE_OUT(kDate, "date")
    REGTYPE_OUT(kTimestamp, "timestamp without time zone")
    REGTYPE_OUT(kTimestampTz, "timestamp with time zone")
    REGTYPE_OUT(kInterval, "interval")
    REGTYPE_OUT(kRegclass, "regclass")
    REGTYPE_OUT(kRegtype, "regtype")
  }
  return absl::StrCat(oid);
}
#undef REGTYPE_OUT

#define SDB_REGTYPE_IN(oid, type_name)             \
    {type_name, PgTypeOID::oid},               \
    {type_name "[]", PgTypeOID::oid##Array},

int32_t RegtypeIn(std::string_view name) {
  static const containers::FlatHashMap<std::string_view, int32_t>
    kTypeNameToOid = {
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
      SDB_REGTYPE_IN(kVarchar, "character varying")
      SDB_REGTYPE_IN(kVarchar, "varchar")
      SDB_REGTYPE_IN(kJson, "json")
      SDB_REGTYPE_IN(kUuid, "uuid")
      SDB_REGTYPE_IN(kNumeric, "numeric")
      SDB_REGTYPE_IN(kDate, "date")
      SDB_REGTYPE_IN(kTimestamp, "timestamp without time zone")
      SDB_REGTYPE_IN(kTimestamp, "timestamp")
      SDB_REGTYPE_IN(kTimestampTz, "timestamp with time zone")
      SDB_REGTYPE_IN(kTimestampTz, "timestamptz")
      SDB_REGTYPE_IN(kInterval, "interval")
      SDB_REGTYPE_IN(kRegclass, "regclass")
      SDB_REGTYPE_IN(kRegtype, "regtype")
    };
  auto it = kTypeNameToOid.find(name);
  if (it != kTypeNameToOid.end()) {
    return it->second;
  }
  return kInvalidOid;
}
#undef SDB_REGTYPE_IN
// clang-format on

namespace {

const velox::Type& GetNestedArrayBaseElementType(const velox::Type& type) {
  if (type.kind() == velox::TypeKind::ARRAY) {
    return GetNestedArrayBaseElementType(*type.asArray().elementType());
  }
  return type;
}

velox::Variant BuildNestedArray(
  const std::vector<velox::Variant>& flat_elements,
  const std::vector<int32_t>& dimensions, size_t dim_index,
  size_t& element_index) {
  if (dim_index == dimensions.size() - 1) {
    std::vector<velox::Variant> inner;
    inner.reserve(dimensions[dim_index]);
    for (int32_t i = 0; i < dimensions[dim_index]; ++i) {
      inner.push_back(flat_elements[element_index++]);
    }
    return velox::Variant::array(std::move(inner));
  }

  std::vector<velox::Variant> outer;
  outer.reserve(dimensions[dim_index]);
  for (int32_t i = 0; i < dimensions[dim_index]; ++i) {
    outer.push_back(BuildNestedArray(flat_elements, dimensions, dim_index + 1,
                                     element_index));
  }
  return velox::Variant::array(std::move(outer));
}

std::expected<std::vector<velox::Variant>, DeserializeError>
DeserializeArrayBinary(const velox::Type& element_type, std::string_view data) {
  if (data.size() < 12) {
    return std::unexpected{DeserializeError::InvalidRepresentation};
  }

  int32_t ndim = absl::big_endian::Load32(data.data());
  [[maybe_unused]] int32_t has_nulls =
    absl::big_endian::Load32(data.data() + 4);
  [[maybe_unused]] int32_t elem_oid = absl::big_endian::Load32(data.data() + 8);

  size_t offset = 12;

  if (ndim == 0) {
    return std::vector<velox::Variant>{};
  }

  if (offset + ndim * 8 > data.size()) {
    return std::unexpected{DeserializeError::InvalidRepresentation};
  }

  std::vector<int32_t> dimensions;
  dimensions.reserve(ndim);
  int32_t total_elements = 1;
  for (int32_t d = 0; d < ndim; ++d) {
    int32_t dim_size = absl::big_endian::Load32(data.data() + offset);
    [[maybe_unused]] int32_t lower_bound =
      absl::big_endian::Load32(data.data() + offset + 4);
    offset += 8;

    if (dim_size < 0) {
      return std::unexpected{DeserializeError::InvalidRepresentation};
    }
    dimensions.push_back(dim_size);
    total_elements *= dim_size;
  }

  const velox::Type& base_type = GetNestedArrayBaseElementType(element_type);

  // First, deserialize all elements into a flat vector, then nest them
  // accordingly
  std::vector<velox::Variant> flat_elements;
  flat_elements.reserve(total_elements);

  for (int32_t i = 0; i < total_elements; ++i) {
    if (offset + 4 > data.size()) {
      return std::unexpected{DeserializeError::InvalidRepresentation};
    }

    int32_t elem_len = absl::big_endian::Load32(data.data() + offset);
    offset += 4;

    if (elem_len == -1) {
      flat_elements.emplace_back(velox::Variant::null(base_type.kind()));
      continue;
    }

    if (elem_len < 0 || offset + elem_len > data.size()) {
      return std::unexpected{DeserializeError::InvalidRepresentation};
    }

    std::string_view elem_data{data.data() + offset,
                               static_cast<size_t>(elem_len)};
    auto result = DeserializeParameter(base_type, VarFormat::Binary, elem_data);
    if (!result) {
      return std::unexpected{result.error()};
    }

    flat_elements.emplace_back(*result);
    offset += elem_len;
  }

  if (ndim == 1) {
    return flat_elements;
  }

  size_t element_index = 0;
  std::vector<velox::Variant> result;
  result.reserve(dimensions[0]);
  for (int32_t i = 0; i < dimensions[0]; ++i) {
    result.push_back(
      BuildNestedArray(flat_elements, dimensions, 1, element_index));
  }

  return result;
}

}  // namespace

std::expected<velox::Variant, DeserializeError> DeserializeParameter(
  const velox::Type& type, VarFormat format, std::string_view data) {
  if (format == VarFormat::Binary) {
    if (IsInterval(type)) {
      velox::int128_t packed = absl::big_endian::Load128(data.data());
      return velox::Variant{packed};
    }

    switch (type.kind()) {
      case velox::TypeKind::BOOLEAN: {
        if (data.size() != 1) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        return velox::Variant{data[0] != 0};
      }
      case velox::TypeKind::TINYINT: {
        if (data.size() != 1) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        return velox::Variant{static_cast<int8_t>(data[0])};
      }
      case velox::TypeKind::SMALLINT: {
        if (data.size() != 2) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        int16_t val = absl::big_endian::Load16(data.data());
        return velox::Variant{val};
      }
      case velox::TypeKind::INTEGER: {
        if (data.size() != 4) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        int32_t val = absl::big_endian::Load32(data.data());
        return velox::Variant{val};
      }
      case velox::TypeKind::BIGINT: {
        if (data.size() != 8) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        int64_t val = absl::big_endian::Load64(data.data());
        return velox::Variant{val};
      }
      case velox::TypeKind::REAL: {
        if (data.size() != 4) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        uint32_t bits = absl::big_endian::Load32(data.data());
        float val = std::bit_cast<float>(bits);
        return velox::Variant{val};
      }
      case velox::TypeKind::DOUBLE: {
        if (data.size() != 8) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        uint64_t bits = absl::big_endian::Load64(data.data());
        double val = std::bit_cast<double>(bits);
        return velox::Variant{val};
      }
      case velox::TypeKind::VARCHAR:
      case velox::TypeKind::VARBINARY: {
        return velox::Variant{std::string{data.data(), data.size()}};
      }
      case velox::TypeKind::ARRAY: {
        const auto& array_type = type.asArray();
        const auto& element_type = array_type.elementType();

        auto elements_result = DeserializeArrayBinary(*element_type, data);
        if (!elements_result) {
          return std::unexpected{elements_result.error()};
        }

        return velox::Variant::array(std::move(*elements_result));
      }
      default:
        SDB_THROW(ERROR_NOT_IMPLEMENTED,
                  "unsupported binary format type: ", type.toString());
    }
  }

  if (format == VarFormat::Text) {
    if (IsInterval(type)) {
      auto packed = IntervalIn(data, /*range=*/0, /*precision=*/6);
      return velox::Variant{packed};
    }

    switch (type.kind()) {
      case velox::TypeKind::BOOLEAN: {
        if (data == "t" || data == "true" || data == "1") {
          return velox::Variant{true};
        } else if (data == "f" || data == "false" || data == "0") {
          return velox::Variant{false};
        }
        return std::unexpected{DeserializeError::InvalidRepresentation};
      }
      case velox::TypeKind::TINYINT: {
        int8_t val;
        if (!absl::SimpleAtoi(data, &val)) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        return velox::Variant{static_cast<int8_t>(val)};
      }
      case velox::TypeKind::SMALLINT: {
        int16_t val;
        if (!absl::SimpleAtoi(data, &val)) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        return velox::Variant{val};
      }
      case velox::TypeKind::INTEGER: {
        int32_t val;
        if (!absl::SimpleAtoi(data, &val)) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        return velox::Variant{val};
      }
      case velox::TypeKind::BIGINT: {
        int64_t val;
        if (!absl::SimpleAtoi(data, &val)) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        return velox::Variant{val};
      }
      case velox::TypeKind::REAL: {
        float val;
        if (!absl::SimpleAtof(data, &val)) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        return velox::Variant{val};
      }
      case velox::TypeKind::DOUBLE: {
        double val;
        if (!absl::SimpleAtod(data, &val)) {
          return std::unexpected{DeserializeError::InvalidRepresentation};
        }
        return velox::Variant{val};
      }
      // case velox::TypeKind::VARBINARY:
      // TODO: use pg_byteain (make helper function for the existing one)
      case velox::TypeKind::VARCHAR: {
        return velox::Variant{std::string{data}};
      }
      default:
        SDB_THROW(ERROR_NOT_IMPLEMENTED,
                  "unsupported text format type: ", type.toString());
    }
  }

  SDB_THROW(ERROR_NOT_IMPLEMENTED, "unsupported parameter format");
}

// TODO(codeworse): use snapshot from query
std::string RegclassOut(int32_t oid) {
  auto snapshot = catalog::GetCatalog().GetSnapshot();
  auto object = snapshot->GetObject(ObjectId{static_cast<uint64_t>(oid)});
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

// TODO(codeworse): use snapshot from query
int32_t RegclassIn(const ConnectionContext& ctx, std::string_view name) {
  auto snapshot = catalog::GetCatalog().GetSnapshot();
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

}  // namespace sdb::pg
