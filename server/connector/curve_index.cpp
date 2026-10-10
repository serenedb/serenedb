////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "connector/curve_index.h"

#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

namespace sdb::connector {
namespace {

enum class Family : uint8_t {
  Null,
  Signed,
  Unsigned,
  Float,
  Date,
  Timestamp,
  TimestampTz,
  Unsupported,
};

Family CoordinateFamily(duckdb::LogicalTypeId id) noexcept {
  using enum duckdb::LogicalTypeId;
  switch (id) {
    case SQLNULL:
      return Family::Null;
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
      return Family::Signed;
    case UTINYINT:
    case USMALLINT:
    case UINTEGER:
    case UBIGINT:
      return Family::Unsigned;
    case FLOAT:
    case DOUBLE:
      return Family::Float;
    case DATE:
      return Family::Date;
    case TIMESTAMP:
      return Family::Timestamp;
    case TIMESTAMP_TZ:
      return Family::TimestampTz;
    default:
      return Family::Unsupported;
  }
}

bool IsInteger(Family family) noexcept {
  return family == Family::Signed || family == Family::Unsigned;
}

bool BoundFits(Family bound, Family coordinate) noexcept {
  return bound == Family::Null || coordinate == Family::Null ||
         bound == coordinate ||
         (coordinate == Family::Float && IsInteger(bound));
}

constexpr std::string_view kCurvePointTypes =
  "a ROW of 2 to 8 integer, FLOAT, DOUBLE, DATE, TIMESTAMP or TIMESTAMPTZ "
  "coordinates";

bool IsCurveTuple(const duckdb::LogicalType& type) {
  if (!duckdb::StructType::IsStruct(type)) {
    return false;
  }
  const auto& children = duckdb::StructType::GetChildTypes(type);
  return children.size() >= 2 && children.size() <= irs::curve::kMaxDimensions;
}

bool IsCurvePoint(const duckdb::LogicalType& type) {
  if (!IsCurveTuple(type)) {
    return false;
  }
  for (const auto& child : duckdb::StructType::GetChildTypes(type)) {
    if (CoordinateFamily(child.second.id()) == Family::Unsupported) {
      return false;
    }
  }
  return true;
}

template<typename T>
uint64_t EncodeNumber(T value, bool as_double) noexcept {
  if constexpr (std::is_floating_point_v<T>) {
    return irs::curve::EncodeDouble(value);
  } else {
    if (as_double) {
      return irs::curve::EncodeDouble(static_cast<double>(value));
    }
    if constexpr (std::is_signed_v<T>) {
      return irs::curve::EncodeSigned(value);
    } else {
      return value;
    }
  }
}

template<typename T>
T Load(const duckdb::UnifiedVectorFormat& format, duckdb::idx_t index) {
  return duckdb::UnifiedVectorFormat::GetDataUnsafe<T>(format)[index];
}

}  // namespace

void ValidateCurveType(std::string_view label,
                       const duckdb::LogicalType& type) {
  if (!IsCurvePoint(type)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
                    ERR_MSG("Column '", label, "': curve opclass requires ",
                            kCurvePointTypes, ", got ", type.ToString()));
  }
}

void ValidateCurveBounds(const duckdb::LogicalType& point,
                         const duckdb::LogicalType& lower,
                         const duckdb::LogicalType& upper) {
  if (!IsCurvePoint(point)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
                    ERR_MSG("sdb_box_contains: the point must be ",
                            kCurvePointTypes, ", got ", point.ToString()));
  }
  const auto& coordinates = duckdb::StructType::GetChildTypes(point);
  for (const auto* bound : {&lower, &upper}) {
    if (!IsCurveTuple(*bound) ||
        duckdb::StructType::GetChildCount(*bound) != coordinates.size()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
        ERR_MSG("sdb_box_contains: bounds must be ROWs with ",
                coordinates.size(), " coordinates like the point ",
                point.ToString(), ", got ", bound->ToString()));
    }
    const auto& limits = duckdb::StructType::GetChildTypes(*bound);
    for (size_t axis = 0; axis < coordinates.size(); ++axis) {
      const auto& coordinate = coordinates[axis].second;
      const auto& limit = limits[axis].second;
      if (!BoundFits(CoordinateFamily(limit.id()),
                     CoordinateFamily(coordinate.id()))) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
          ERR_MSG("sdb_box_contains: bound ", limit.ToString(),
                  " does not match coordinate ", axis + 1, " of type ",
                  coordinate.ToString(), "; cast the bound explicitly"));
      }
    }
  }
}

CurveTuples::CurveTuples(const duckdb::Vector& tuples,
                         const duckdb::LogicalType& point) {
  tuples.ToUnifiedFormat(_tuples);
  if (!duckdb::StructType::IsStruct(tuples.GetType())) {
    return;
  }
  const auto& entries = duckdb::StructVector::GetEntries(tuples);
  const auto& coordinates = duckdb::StructType::GetChildTypes(point);
  SDB_ASSERT(entries.size() == coordinates.size());
  for (size_t axis = 0; axis < entries.size(); ++axis) {
    auto& target = _axes[axis];
    entries[axis].ToUnifiedFormat(target.format);
    target.type = entries[axis].GetType().id();
    target.as_double =
      CoordinateFamily(coordinates[axis].second.id()) == Family::Float &&
      IsInteger(CoordinateFamily(target.type));
  }
}

uint64_t CurveTuples::Get(duckdb::idx_t row, uint32_t axis) const {
  using enum duckdb::LogicalTypeId;
  const auto& target = _axes[axis];
  const auto& format = target.format;
  const auto index = format.sel->get_index(row);
  switch (target.type) {
    case TINYINT:
      return EncodeNumber(Load<int8_t>(format, index), target.as_double);
    case SMALLINT:
      return EncodeNumber(Load<int16_t>(format, index), target.as_double);
    case INTEGER:
    case DATE:
      return EncodeNumber(Load<int32_t>(format, index), target.as_double);
    case BIGINT:
    case TIMESTAMP:
    case TIMESTAMP_TZ:
      return EncodeNumber(Load<int64_t>(format, index), target.as_double);
    case UTINYINT:
      return EncodeNumber(Load<uint8_t>(format, index), target.as_double);
    case USMALLINT:
      return EncodeNumber(Load<uint16_t>(format, index), target.as_double);
    case UINTEGER:
      return EncodeNumber(Load<uint32_t>(format, index), target.as_double);
    case UBIGINT:
      return EncodeNumber(Load<uint64_t>(format, index), target.as_double);
    case FLOAT:
      return EncodeNumber(Load<float>(format, index), false);
    case DOUBLE:
      return EncodeNumber(Load<double>(format, index), false);
    default:
      SDB_ASSERT(false);
      return 0;
  }
}

irs::curve::Box CurveBox(const CurveTuples& lower, duckdb::idx_t lower_row,
                         const CurveTuples& upper, duckdb::idx_t upper_row,
                         uint32_t dimensions) {
  irs::curve::Box box;
  for (uint32_t axis = 0; axis < dimensions; ++axis) {
    box.min[axis] =
      lower.IsValid(lower_row, axis) ? lower.Get(lower_row, axis) : 0;
    box.max[axis] = upper.IsValid(upper_row, axis)
                      ? upper.Get(upper_row, axis)
                      : std::numeric_limits<uint64_t>::max();
  }
  return box;
}

irs::curve::Box CurveBox(const duckdb::LogicalType& point,
                         const duckdb::Value& lower,
                         const duckdb::Value& upper) {
  const duckdb::Vector lower_vector{lower, duckdb::count_t{1}};
  const duckdb::Vector upper_vector{upper, duckdb::count_t{1}};
  return CurveBox(
    CurveTuples{lower_vector, point}, 0, CurveTuples{upper_vector, point}, 0,
    static_cast<uint32_t>(duckdb::StructType::GetChildCount(point)));
}

void PackCurvePoints(const duckdb::Vector& points, duckdb::idx_t count,
                     uint32_t dimensions, duckdb::Vector& packed) {
  const CurveTuples tuples{points, points.GetType()};
  auto* out = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(packed);
  std::array<char, sizeof(irs::curve::Point)> bytes;
  for (duckdb::idx_t row = 0; row < count; ++row) {
    if (!tuples.IsValid(row)) {
      duckdb::FlatVector::SetNull(packed, row, true);
      continue;
    }
    for (uint32_t axis = 0; axis < dimensions; ++axis) {
      const auto value =
        tuples.IsValid(row, axis) ? tuples.Get(row, axis) : uint64_t{0};
      for (uint32_t byte = 0; byte < sizeof(uint64_t); ++byte) {
        bytes[axis * sizeof(uint64_t) + byte] =
          static_cast<char>(value >> (56 - byte * 8));
      }
    }
    out[row] = duckdb::StringVector::AddStringOrBlob(
      packed, bytes.data(), dimensions * sizeof(uint64_t));
  }
}

irs::curve::Point CurveTokenizer::UnpackPoint(
  std::string_view bytes) const noexcept {
  SDB_ASSERT(bytes.size() == _options.dimensions * sizeof(uint64_t));
  irs::curve::Point point;
  point.fill(0);
  for (uint32_t axis = 0; axis < _options.dimensions; ++axis) {
    for (uint32_t byte = 0; byte < sizeof(uint64_t); ++byte) {
      point[axis] =
        (point[axis] << 8) |
        static_cast<unsigned char>(bytes[axis * sizeof(uint64_t) + byte]);
    }
  }
  return point;
}

}  // namespace sdb::connector
