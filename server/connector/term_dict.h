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
#include <iresearch/types.hpp>

#include "connector/column_id.h"

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

}  // namespace sdb::connector::term_dict
