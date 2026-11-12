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
#include <duckdb/common/types/datetime.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <iresearch/index/field_meta.hpp>
#include <iresearch/types.hpp>
#include <type_traits>
#include <utility>

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

inline int64_t TimeTzIndexTerm(int64_t raw_bits) noexcept {
  return static_cast<int64_t>(
    duckdb::dtime_tz_t{static_cast<uint64_t>(raw_bits)}.sort_key());
}

// A value at the width its terms are built from, read as `load<T>()` of the
// physical type T. Whoever reads a column must read it this way: a value
// written at one width and looked up at another is a different term, so the
// sink and the query share this one definition.
template<duckdb::LogicalTypeId Kind, typename Load>
auto NumericValueOf(Load&& load) {
  if constexpr (Kind == duckdb::LogicalTypeId::TINYINT) {
    return static_cast<int32_t>(load.template operator()<int8_t>());
  } else if constexpr (Kind == duckdb::LogicalTypeId::SMALLINT) {
    return static_cast<int32_t>(load.template operator()<int16_t>());
  } else if constexpr (Kind == duckdb::LogicalTypeId::INTEGER ||
                       Kind == duckdb::LogicalTypeId::DATE) {
    return load.template operator()<int32_t>();
  } else if constexpr (Kind == duckdb::LogicalTypeId::TIME_TZ) {
    return TimeTzIndexTerm(load.template operator()<int64_t>());
  } else if constexpr (Kind == duckdb::LogicalTypeId::BIGINT ||
                       Kind == duckdb::LogicalTypeId::TIME ||
                       Kind == duckdb::LogicalTypeId::TIME_NS ||
                       Kind == duckdb::LogicalTypeId::TIMESTAMP ||
                       Kind == duckdb::LogicalTypeId::TIMESTAMP_TZ ||
                       Kind == duckdb::LogicalTypeId::TIMESTAMP_SEC ||
                       Kind == duckdb::LogicalTypeId::TIMESTAMP_MS ||
                       Kind == duckdb::LogicalTypeId::TIMESTAMP_NS ||
                       Kind == duckdb::LogicalTypeId::TIMESTAMP_TZ_NS) {
    return load.template operator()<int64_t>();
  } else if constexpr (Kind == duckdb::LogicalTypeId::UTINYINT) {
    return static_cast<int32_t>(load.template operator()<uint8_t>());
  } else if constexpr (Kind == duckdb::LogicalTypeId::USMALLINT) {
    return static_cast<int32_t>(load.template operator()<uint16_t>());
  } else if constexpr (Kind == duckdb::LogicalTypeId::UINTEGER) {
    return load.template operator()<uint32_t>();
  } else if constexpr (Kind == duckdb::LogicalTypeId::UBIGINT) {
    return load.template operator()<uint64_t>();
  } else if constexpr (Kind == duckdb::LogicalTypeId::HUGEINT ||
                       Kind == duckdb::LogicalTypeId::UUID) {
    return static_cast<__int128>(
      load.template operator()<duckdb::hugeint_t>().ToInt128());
  } else if constexpr (Kind == duckdb::LogicalTypeId::UHUGEINT) {
    return static_cast<unsigned __int128>(
      load.template operator()<duckdb::uhugeint_t>().ToUint128());
  } else if constexpr (Kind == duckdb::LogicalTypeId::FLOAT) {
    return load.template operator()<float>();
  } else {
    static_assert(Kind == duckdb::LogicalTypeId::DOUBLE,
                  "NumericValueOf: unsupported Kind");
    return load.template operator()<double>();
  }
}

template<duckdb::LogicalTypeId Kind>
auto ExtractNumericValue(const duckdb::UnifiedVectorFormat& fmt,
                         duckdb::idx_t idx) {
  return NumericValueOf<Kind>([&]<typename T>() {
    return duckdb::UnifiedVectorFormat::GetData<T>(fmt)[idx];
  });
}

template<typename T>
auto PromoteNumericValue(T value) {
  if constexpr (std::is_same_v<T, float>) {
#ifdef FLOAT_T_IS_DOUBLE_T
    return static_cast<double>(value);
#else
    return value;
#endif
  } else if constexpr (std::is_same_v<T, uint32_t>) {
    return static_cast<int64_t>(value);
  } else {
    return value;
  }
}

template<duckdb::LogicalTypeId Kind>
using PromotedNumeric = decltype(PromoteNumericValue(ExtractNumericValue<Kind>(
  std::declval<const duckdb::UnifiedVectorFormat&>(), 0)));

template<duckdb::LogicalTypeId... Kinds, typename F>
bool DispatchKind(duckdb::LogicalTypeId kind, F&& f) {
  return (((kind == Kinds) && (f.template operator()<Kinds>(), true)) || ...);
}

template<duckdb::LogicalTypeId... Extra, typename F>
bool DispatchNumericKind(duckdb::LogicalTypeId kind, F&& f) {
  using enum duckdb::LogicalTypeId;
  return DispatchKind<Extra..., TINYINT, SMALLINT, INTEGER, BIGINT, UTINYINT,
                      USMALLINT, UINTEGER, UBIGINT, HUGEINT, UHUGEINT, UUID,
                      FLOAT, DOUBLE, DATE, TIME, TIME_TZ, TIME_NS, TIMESTAMP,
                      TIMESTAMP_TZ, TIMESTAMP_SEC, TIMESTAMP_MS, TIMESTAMP_NS,
                      TIMESTAMP_TZ_NS>(kind, std::forward<F>(f));
}

// The kinds a value is inverted under its own member and term: what every
// indexable list/array child shares, and what a whole value can be looked up
// by. GEOMETRY and inet are indexable but absent: they yield several terms per
// value, which no single lookup rebuilds.
template<typename F>
bool DispatchValueKind(duckdb::LogicalTypeId kind, F&& f) {
  using enum duckdb::LogicalTypeId;
  return DispatchNumericKind<VARCHAR, BLOB, BOOLEAN>(kind, std::forward<F>(f));
}

}  // namespace sdb::connector::term_dict
