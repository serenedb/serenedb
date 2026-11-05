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

#pragma once

#include <duckdb/common/types.hpp>
#include <iresearch/index/field_meta.hpp>
#include <iresearch/types.hpp>

#include "connector/column_id.h"
#include "pg/types/logical.h"

namespace sdb::connector::term_dict {

// The pk term dictionary and the stored pk column are written under the same
// reserved field id.
inline constexpr irs::field_id kPKFieldId = kGeneratedPKId;

// The types the sink indexes as numbers; a query encodes its bounds at the
// same width, so it rebuilds the exact same terms.
constexpr bool IsNumeric(duckdb::LogicalTypeId id) noexcept {
  using enum duckdb::LogicalTypeId;
  switch (id) {
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case UTINYINT:
    case USMALLINT:
    case DATE:
    case BIGINT:
    case UINTEGER:
    case TIME:
    case TIME_TZ:
    case TIME_NS:
    case TIMESTAMP:
    case TIMESTAMP_TZ:
    case TIMESTAMP_SEC:
    case TIMESTAMP_MS:
    case TIMESTAMP_NS:
    case TIMESTAMP_TZ_NS:
    case FLOAT:
    case DOUBLE:
    case UBIGINT:
    case HUGEINT:
    case UHUGEINT:
    case UUID:
    case DECIMAL:
      return true;
    default:
      return false;
  }
}

inline duckdb::LogicalTypeId IndexedKind(const duckdb::LogicalType& type) {
  using enum duckdb::LogicalTypeId;
  if (type.id() != DECIMAL) {
    return type.id();
  }
  switch (type.InternalType()) {
    case duckdb::PhysicalType::INT16:
      return SMALLINT;
    case duckdb::PhysicalType::INT32:
      return INTEGER;
    case duckdb::PhysicalType::INT64:
      return BIGINT;
    default:
      return HUGEINT;
  }
}

constexpr bool IsIndexable(duckdb::LogicalTypeId id) noexcept {
  using enum duckdb::LogicalTypeId;
  switch (id) {
    case SQLNULL:
    case BOOLEAN:
    case VARCHAR:
    case BLOB:
    case GEOMETRY:
      return true;
    default:
      return IsNumeric(id);
  }
}

inline bool IsIndexable(const duckdb::LogicalType& type) {
  return pg::IsInetOrCidr(type) || IsIndexable(type.id());
}

// The layout a value of `type` is written with: `json_leaves` when a JSON
// value is indexed leaf by leaf.
inline irs::DictLayout LayoutOf(const duckdb::LogicalType& type,
                                bool json_leaves) {
  using enum duckdb::LogicalTypeId;
  if (type.id() == LIST || type.id() == ARRAY) {
    const auto& child = type.id() == LIST
                          ? duckdb::ListType::GetChildType(type)
                          : duckdb::ArrayType::GetChildType(type);
    if (json_leaves && child.IsJSONType()) {
      return irs::DictLayout::JsonList;
    }
    if (child.id() == BOOLEAN) {
      return irs::DictLayout::BoolList;
    }
    return IsNumeric(child.id()) ? irs::DictLayout::NumericList
                                 : irs::DictLayout::StringList;
  }
  if (json_leaves && type.IsJSONType()) {
    return irs::DictLayout::Json;
  }
  if (type.id() == BOOLEAN) {
    return irs::DictLayout::Bool;
  }
  return IsNumeric(type.id()) ? irs::DictLayout::Numeric
                              : irs::DictLayout::String;
}

}  // namespace sdb::connector::term_dict
