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
#include <frozen/unordered_map.h>
#include <velox/functions/prestosql/types/JsonType.h>
#include <velox/functions/prestosql/types/TimestampWithTimeZoneType.h>
#include <velox/functions/prestosql/types/UuidType.h>
#include <velox/type/Timestamp.h>

#include <charconv>

#include "basics/assert.h"
#include "basics/logger/logger.h"
#include "basics/system-compiler.h"
#include "pg/interval.h"
#include "pg/serialize.h"
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
  } else if (IsUnknown(type)) {
    return PgTypeOID::kUnknown;
  }
  return -1;
}

}  // namespace

int32_t GetTypeOID(const velox::TypePtr& type, bool in_array) {
  // SDB_PRINT("TYPE = ", type->name());
  int32_t composite_oid = GetCompositeOID(type, in_array);
  if (composite_oid >= 0) {
    return composite_oid;
  }
  auto x = GetPrimitiveTypeOID(type->kind(), in_array);
  // SDB_PRINT("OID = ", x);
  return x;
}

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

}  // namespace sdb::pg
