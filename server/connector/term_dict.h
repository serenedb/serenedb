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

#include <cstdint>
#include <duckdb/common/types.hpp>
#include <iresearch/index/field_meta.hpp>
#include <iresearch/types.hpp>
#include <limits>

#include "connector/column_id.h"
#include "pg/types/logical.h"

namespace sdb::connector::term_dict {

// The pk term dictionary and the stored pk column are written under the same
// reserved field id.
inline constexpr irs::field_id kPKFieldId = kGeneratedPKId;

// How a duckdb type reaches the term dictionary. The numeric kinds name the
// width the sink encoded the value at, so a query rebuilds the exact same
// terms; anything the sink cannot index at all is Unsupported.
enum class Kind : uint8_t {
  Unsupported = 0,
  Null,
  Bool,
  String,
  NumericI32,
  NumericI64,
  NumericF32,
  NumericF64,
  Inet,
};

constexpr Kind Classify(duckdb::LogicalTypeId id) noexcept {
  using enum duckdb::LogicalTypeId;
  switch (id) {
    case SQLNULL:
      return Kind::Null;
    case BOOLEAN:
      return Kind::Bool;
    case VARCHAR:
    case BLOB:
    case GEOMETRY:
      return Kind::String;
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case UTINYINT:
    case USMALLINT:
    case DATE:
      return Kind::NumericI32;
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
      return Kind::NumericI64;
    case FLOAT:
      return Kind::NumericF32;
    case DOUBLE:
      return Kind::NumericF64;
    default:
      return Kind::Unsupported;
  }
}

inline Kind Classify(const duckdb::LogicalType& type) {
  return pg::IsInetOrCidr(type) ? Kind::Inet : Classify(type.id());
}

constexpr bool IsNumeric(Kind kind) noexcept {
  switch (kind) {
    case Kind::NumericI32:
    case Kind::NumericI64:
    case Kind::NumericF32:
    case Kind::NumericF64:
      return true;
    default:
      return false;
  }
}

constexpr bool IsSupported(Kind kind) noexcept {
  return kind != Kind::Unsupported;
}

inline constexpr uint8_t kNoMember = std::numeric_limits<uint8_t>::max();

constexpr irs::DictLayout ScalarLayout(Kind kind) noexcept {
  if (kind == Kind::Bool) {
    return irs::DictLayout::Bool;
  }
  return IsNumeric(kind) ? irs::DictLayout::Numeric : irs::DictLayout::String;
}

constexpr irs::DictLayout ListLayout(Kind element) noexcept {
  if (element == Kind::Bool) {
    return irs::DictLayout::BoolList;
  }
  return IsNumeric(element) ? irs::DictLayout::NumericList
                            : irs::DictLayout::StringList;
}

constexpr irs::DictRef NoMember(irs::DictLayout layout) noexcept {
  return {.layout = layout, .member = kNoMember};
}

constexpr irs::DictRef NullsOf(irs::DictLayout layout) noexcept {
  switch (layout) {
    case irs::DictLayout::Bool:
      return irs::BoolLayout::kNulls;
    case irs::DictLayout::Numeric:
      return irs::NumericLayout::kNulls;
    case irs::DictLayout::String:
      return irs::StringLayout::kNulls;
    case irs::DictLayout::BoolList:
      return irs::BoolListLayout::kNulls;
    case irs::DictLayout::NumericList:
      return irs::NumericListLayout::kNulls;
    case irs::DictLayout::StringList:
      return irs::StringListLayout::kNulls;
    case irs::DictLayout::Json:
      return irs::JsonLayout::kNulls;
  }
  return NoMember(layout);
}

constexpr irs::DictRef NullElementsOf(irs::DictLayout layout) noexcept {
  switch (layout) {
    case irs::DictLayout::BoolList:
      return irs::BoolListLayout::kNullElements;
    case irs::DictLayout::NumericList:
      return irs::NumericListLayout::kNullElements;
    case irs::DictLayout::StringList:
      return irs::StringListLayout::kNullElements;
    default:
      return NoMember(layout);
  }
}

constexpr irs::DictRef StringsOf(irs::DictLayout layout) noexcept {
  switch (layout) {
    case irs::DictLayout::String:
      return irs::StringLayout::kStrings;
    case irs::DictLayout::StringList:
      return irs::StringListLayout::kStrings;
    case irs::DictLayout::Json:
      return irs::JsonLayout::kStrings;
    default:
      return NoMember(layout);
  }
}

constexpr irs::DictRef NumbersOf(irs::DictLayout layout) noexcept {
  switch (layout) {
    case irs::DictLayout::Numeric:
      return irs::NumericLayout::kNumbers;
    case irs::DictLayout::NumericList:
      return irs::NumericListLayout::kNumbers;
    case irs::DictLayout::Json:
      return irs::JsonLayout::kNumbers;
    default:
      return NoMember(layout);
  }
}

constexpr irs::DictRef BoolsOf(irs::DictLayout layout, bool value) noexcept {
  switch (layout) {
    case irs::DictLayout::Bool:
      return value ? irs::BoolLayout::kTrue : irs::BoolLayout::kFalse;
    case irs::DictLayout::BoolList:
      return value ? irs::BoolListLayout::kTrue : irs::BoolListLayout::kFalse;
    case irs::DictLayout::Json:
      return value ? irs::JsonLayout::kTrue : irs::JsonLayout::kFalse;
    default:
      return NoMember(layout);
  }
}

constexpr irs::DictRef ValuesOf(irs::DictLayout layout, Kind kind) noexcept {
  return IsNumeric(kind) ? NumbersOf(layout) : StringsOf(layout);
}

enum class Member : uint8_t {
  None = 0,
  String,
  False,
  True,
  Numeric,
  SqlNull,
  JsonNull,
  NullElement,
};

constexpr Member MemberOf(irs::DictRef dict) noexcept {
  const auto layout = dict.layout;
  if (!irs::Contains(dict)) {
    return Member::None;
  }
  if (dict == StringsOf(layout)) {
    return Member::String;
  }
  if (dict == NumbersOf(layout)) {
    return Member::Numeric;
  }
  if (dict == BoolsOf(layout, false)) {
    return Member::False;
  }
  if (dict == BoolsOf(layout, true)) {
    return Member::True;
  }
  if (dict == NullsOf(layout)) {
    return Member::SqlNull;
  }
  if (dict == NullElementsOf(layout)) {
    return Member::NullElement;
  }
  return dict == irs::JsonLayout::kJsonNulls ? Member::JsonNull : Member::None;
}

}  // namespace sdb::connector::term_dict
