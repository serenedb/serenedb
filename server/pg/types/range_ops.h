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

#include <absl/algorithm/container.h>

#include <cstdint>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/date.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <duckdb/function/aggregate_function.hpp>
#include <duckdb/function/cast/cast_function_set.hpp>
#include <duckdb/function/function_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "connector/common.h"
#include "pg/types/cast.h"
#include "pg/types/function.h"
#include "pg/types/range.h"

namespace sdb::pg::ranges {

template<typename T>
struct Bound {
  T value;
  bool infinite;
  bool inclusive;
  bool lower;
};

template<typename T>
struct Range {
  Bound<T> lower;
  Bound<T> upper;
  bool empty;
};

template<typename T>
using Multirange = std::vector<Range<T>>;

struct Int4Range {
  using Element = int32_t;
  static constexpr std::string_view kName = kInt4rangeAlias;
  static constexpr std::string_view kMultirangeName = kInt4multirangeAlias;
  static constexpr bool kDiscrete = true;
  static duckdb::LogicalType Type() { return INT4RANGE(); }
  static duckdb::LogicalType MultirangeType() { return INT4MULTIRANGE(); }
  static bool IsFinite(int32_t) { return true; }
  static std::optional<irs::pg::SqlErrorData> Increment(int32_t& value) {
    if (__builtin_add_overflow(value, 1, &value)) {
      return SQL_ERROR_DATA(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                            ERR_MSG("integer out of range"));
    }
    return std::nullopt;
  }
};

struct Int8Range {
  using Element = int64_t;
  static constexpr std::string_view kName = kInt8rangeAlias;
  static constexpr std::string_view kMultirangeName = kInt8multirangeAlias;
  static constexpr bool kDiscrete = true;
  static duckdb::LogicalType Type() { return INT8RANGE(); }
  static duckdb::LogicalType MultirangeType() { return INT8MULTIRANGE(); }
  static bool IsFinite(int64_t) { return true; }
  static std::optional<irs::pg::SqlErrorData> Increment(int64_t& value) {
    if (__builtin_add_overflow(value, 1, &value)) {
      return SQL_ERROR_DATA(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                            ERR_MSG("bigint out of range"));
    }
    return std::nullopt;
  }
};

struct NumRange {
  using Element = int64_t;
  static constexpr std::string_view kName = kNumrangeAlias;
  static constexpr std::string_view kMultirangeName = kNummultirangeAlias;
  static constexpr bool kDiscrete = false;
  static duckdb::LogicalType Type() { return NUMRANGE(); }
  static duckdb::LogicalType MultirangeType() { return NUMMULTIRANGE(); }
};

struct DateRange {
  using Element = duckdb::date_t;
  static constexpr std::string_view kName = kDaterangeAlias;
  static constexpr std::string_view kMultirangeName = kDatemultirangeAlias;
  static constexpr bool kDiscrete = true;
  static duckdb::LogicalType Type() { return DATERANGE(); }
  static duckdb::LogicalType MultirangeType() { return DATEMULTIRANGE(); }
  static bool IsFinite(duckdb::date_t value) { return value.IsFinite(); }
  static std::optional<irs::pg::SqlErrorData> Increment(duckdb::date_t& value) {
    if (++value.days == duckdb::date_t::infinity().days) {
      return SQL_ERROR_DATA(ERR_CODE(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                            ERR_MSG("date out of range"));
    }
    return std::nullopt;
  }
};

struct TsRange {
  using Element = duckdb::timestamp_t;
  static constexpr std::string_view kName = kTsrangeAlias;
  static constexpr std::string_view kMultirangeName = kTsmultirangeAlias;
  static constexpr bool kDiscrete = false;
  static duckdb::LogicalType Type() { return TSRANGE(); }
  static duckdb::LogicalType MultirangeType() { return TSMULTIRANGE(); }
};

struct TstzRange {
  using Element = duckdb::timestamp_tz_t;
  static constexpr std::string_view kName = kTstzrangeAlias;
  static constexpr std::string_view kMultirangeName = kTstzmultirangeAlias;
  static constexpr bool kDiscrete = false;
  static duckdb::LogicalType Type() { return TSTZRANGE(); }
  static duckdb::LogicalType MultirangeType() { return TSTZMULTIRANGE(); }
};

template<typename Visit>
void ForEachRangeKind(Visit&& visit) {
  visit(std::type_identity<Int4Range>{});
  visit(std::type_identity<Int8Range>{});
  visit(std::type_identity<NumRange>{});
  visit(std::type_identity<DateRange>{});
  visit(std::type_identity<TsRange>{});
  visit(std::type_identity<TstzRange>{});
}

template<typename Visit>
decltype(auto) VisitRangeKind(const duckdb::LogicalType& element,
                              Visit&& visit) {
  using enum duckdb::LogicalTypeId;
  const auto id = element.id();
  if (id == INTEGER) {
    return visit(std::type_identity<Int4Range>{});
  }
  if (id == BIGINT) {
    return visit(std::type_identity<Int8Range>{});
  }
  if (id == DECIMAL) {
    return visit(std::type_identity<NumRange>{});
  }
  if (id == DATE) {
    return visit(std::type_identity<DateRange>{});
  }
  if (id == TIMESTAMP) {
    return visit(std::type_identity<TsRange>{});
  }
  SDB_ASSERT(id == TIMESTAMP_TZ);
  return visit(std::type_identity<TstzRange>{});
}

inline const duckdb::LogicalType& ElementType(
  const duckdb::LogicalType& range) {
  return duckdb::StructType::GetChildType(range, kRangeLowerChild);
}

duckdb::LogicalType RangeStorageType(const duckdb::LogicalType& element);

template<typename T>
int Compare(const T& left, const T& right) {
  return left < right ? -1 : right < left ? 1 : 0;
}

template<typename T>
int CompareBoundValues(const Bound<T>& left, const Bound<T>& right) {
  if (left.infinite || right.infinite) {
    if (left.infinite && right.infinite && left.lower == right.lower) {
      return 0;
    }
    return (left.infinite ? left.lower : !right.lower) ? -1 : 1;
  }
  return Compare(left.value, right.value);
}

template<typename T>
int CompareBounds(const Bound<T>& left, const Bound<T>& right) {
  const auto cmp = CompareBoundValues(left, right);
  if (cmp != 0 || left.infinite) {
    return cmp;
  }
  if (left.inclusive == right.inclusive) {
    if (left.inclusive || left.lower == right.lower) {
      return 0;
    }
    return left.lower ? 1 : -1;
  }
  if (!left.inclusive) {
    return left.lower ? 1 : -1;
  }
  return right.lower ? -1 : 1;
}

template<typename T>
int CompareRanges(const Range<T>& left, const Range<T>& right) {
  if (left.empty || right.empty) {
    return left.empty == right.empty ? 0 : left.empty ? -1 : 1;
  }
  const auto cmp = CompareBounds(left.lower, right.lower);
  return cmp != 0 ? cmp : CompareBounds(left.upper, right.upper);
}

template<typename T>
Range<T> FromFlags(uint8_t flags, T lower, T upper) {
  return {
    .lower = {.value = lower,
              .infinite = (flags & kRangeLowerInfinite) != 0,
              .inclusive = (flags & kRangeLowerInclusive) != 0,
              .lower = true},
    .upper = {.value = upper,
              .infinite = (flags & kRangeUpperInfinite) != 0,
              .inclusive = (flags & kRangeUpperInclusive) != 0,
              .lower = false},
    .empty = (flags & kRangeEmpty) != 0,
  };
}

template<typename T>
Range<T> EmptyRange() {
  return FromFlags<T>(kRangeEmpty, T{}, T{});
}

template<typename T>
bool IsClosedPoint(const Range<T>& range) {
  return CompareBoundValues(range.lower, range.upper) == 0 &&
         !(range.lower.inclusive && range.upper.inclusive);
}

template<typename Traits>
std::optional<irs::pg::SqlErrorData> MakeRange(
  Range<typename Traits::Element>& range) {
  if (!range.empty) {
    if (CompareBoundValues(range.lower, range.upper) > 0) {
      return SQL_ERROR_DATA(ERR_CODE(ERRCODE_DATA_EXCEPTION),
                            ERR_MSG("range lower bound must be less than or "
                                    "equal to range upper bound"));
    }
    range.empty = IsClosedPoint(range);
  }
  range.lower.inclusive &= !range.lower.infinite;
  range.upper.inclusive &= !range.upper.infinite;
  if constexpr (Traits::kDiscrete) {
    if (range.empty) {
      return std::nullopt;
    }
    if (!range.lower.infinite && !range.lower.inclusive &&
        Traits::IsFinite(range.lower.value)) {
      if (auto error = Traits::Increment(range.lower.value)) {
        return error;
      }
      range.lower.inclusive = true;
    }
    if (!range.upper.infinite && range.upper.inclusive &&
        Traits::IsFinite(range.upper.value)) {
      if (auto error = Traits::Increment(range.upper.value)) {
        return error;
      }
      range.upper.inclusive = false;
    }
    range.empty = IsClosedPoint(range);
  }
  return std::nullopt;
}

template<typename Traits>
Range<typename Traits::Element> Make(Range<typename Traits::Element> range) {
  if (auto error = MakeRange<Traits>(range)) {
    THROW_SQL_ERROR_FROM_DATA(std::move(*error));
  }
  return range;
}

template<typename T>
bool BoundsContain(const Bound<T>& outer_lower, const Bound<T>& outer_upper,
                   const Bound<T>& inner_lower, const Bound<T>& inner_upper) {
  return CompareBounds(outer_lower, inner_lower) <= 0 &&
         CompareBounds(outer_upper, inner_upper) >= 0;
}

template<typename T>
bool BoundsOverlap(const Bound<T>& lower1, const Bound<T>& upper1,
                   const Bound<T>& lower2, const Bound<T>& upper2) {
  return CompareBounds(lower1, upper2) <= 0 &&
         CompareBounds(lower2, upper1) <= 0;
}

template<typename T>
bool Contains(const Range<T>& outer, const Range<T>& inner) {
  if (inner.empty) {
    return true;
  }
  return !outer.empty &&
         BoundsContain(outer.lower, outer.upper, inner.lower, inner.upper);
}

template<typename T>
bool ContainedBy(const Range<T>& inner, const Range<T>& outer) {
  return Contains(outer, inner);
}

template<typename T>
bool ContainsElement(const Range<T>& range, const T& value) {
  if (range.empty) {
    return false;
  }
  if (!range.lower.infinite) {
    const auto cmp = Compare(range.lower.value, value);
    if (cmp > 0 || (cmp == 0 && !range.lower.inclusive)) {
      return false;
    }
  }
  if (!range.upper.infinite) {
    const auto cmp = Compare(range.upper.value, value);
    if (cmp < 0 || (cmp == 0 && !range.upper.inclusive)) {
      return false;
    }
  }
  return true;
}

template<typename T>
bool ElementContainedBy(const T& value, const Range<T>& range) {
  return ContainsElement(range, value);
}

template<typename T>
bool Before(const Range<T>& left, const Range<T>& right) {
  return !left.empty && !right.empty &&
         CompareBounds(left.upper, right.lower) < 0;
}

template<typename T>
bool After(const Range<T>& left, const Range<T>& right) {
  return Before(right, left);
}

template<typename T>
bool OverLeft(const Range<T>& left, const Range<T>& right) {
  return !left.empty && !right.empty &&
         CompareBounds(left.upper, right.upper) <= 0;
}

template<typename T>
bool OverRight(const Range<T>& left, const Range<T>& right) {
  return !left.empty && !right.empty &&
         CompareBounds(left.lower, right.lower) >= 0;
}

template<typename T>
bool Overlaps(const Range<T>& left, const Range<T>& right) {
  return !left.empty && !right.empty &&
         BoundsOverlap(left.lower, left.upper, right.lower, right.upper);
}

template<typename T>
bool BoundsAdjacent(const Bound<T>& upper, const Bound<T>& lower) {
  return CompareBoundValues(upper, lower) == 0 &&
         upper.inclusive != lower.inclusive;
}

template<typename T>
bool Adjacent(const Range<T>& left, const Range<T>& right) {
  return !left.empty && !right.empty &&
         (BoundsAdjacent(left.upper, right.lower) ||
          BoundsAdjacent(right.upper, left.lower));
}

template<typename Traits, bool kStrict>
Range<typename Traits::Element> Union(
  const Range<typename Traits::Element>& left,
  const Range<typename Traits::Element>& right) {
  if (left.empty) {
    return right;
  }
  if (right.empty) {
    return left;
  }
  if constexpr (kStrict) {
    if (!Overlaps(left, right) && !Adjacent(left, right)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_EXCEPTION),
                      ERR_MSG("result of range union would not be contiguous"));
    }
  }
  return Make<Traits>({
    .lower =
      CompareBounds(left.lower, right.lower) < 0 ? left.lower : right.lower,
    .upper =
      CompareBounds(left.upper, right.upper) > 0 ? left.upper : right.upper,
    .empty = false,
  });
}

template<typename Traits>
Range<typename Traits::Element> Intersect(
  const Range<typename Traits::Element>& left,
  const Range<typename Traits::Element>& right) {
  if (!Overlaps(left, right)) {
    return EmptyRange<typename Traits::Element>();
  }
  return Make<Traits>({
    .lower =
      CompareBounds(left.lower, right.lower) >= 0 ? left.lower : right.lower,
    .upper =
      CompareBounds(left.upper, right.upper) <= 0 ? left.upper : right.upper,
    .empty = false,
  });
}

template<typename T>
Bound<T> Flip(Bound<T> bound) {
  bound.inclusive = !bound.inclusive;
  bound.lower = !bound.lower;
  return bound;
}

template<typename Traits>
Range<typename Traits::Element> Minus(
  const Range<typename Traits::Element>& left,
  const Range<typename Traits::Element>& right) {
  if (left.empty || right.empty) {
    return left;
  }
  const auto lower_lower = CompareBounds(left.lower, right.lower);
  const auto lower_upper = CompareBounds(left.lower, right.upper);
  const auto upper_lower = CompareBounds(left.upper, right.lower);
  const auto upper_upper = CompareBounds(left.upper, right.upper);
  if (lower_lower < 0 && upper_upper > 0) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DATA_EXCEPTION),
      ERR_MSG("result of range difference would not be contiguous"));
  }
  if (lower_upper > 0 || upper_lower < 0) {
    return left;
  }
  if (lower_lower >= 0 && upper_upper <= 0) {
    return EmptyRange<typename Traits::Element>();
  }
  if (lower_lower <= 0 && upper_upper <= 0) {
    return Make<Traits>(
      {.lower = left.lower, .upper = Flip(right.lower), .empty = false});
  }
  SDB_ASSERT(lower_lower >= 0 && upper_upper >= 0 && lower_upper <= 0);
  return Make<Traits>(
    {.lower = Flip(right.upper), .upper = left.upper, .empty = false});
}

template<typename Traits>
Multirange<typename Traits::Element> Normalize(
  Multirange<typename Traits::Element> ranges) {
  absl::c_sort(ranges, [](const auto& left, const auto& right) {
    return CompareRanges(left, right) < 0;
  });
  Multirange<typename Traits::Element> result;
  for (const auto& range : ranges) {
    if (range.empty) {
      continue;
    }
    if (result.empty() ||
        (Before(result.back(), range) && !Adjacent(result.back(), range))) {
      result.push_back(range);
    } else {
      result.back() = Union<Traits, false>(result.back(), range);
    }
  }
  return result;
}

template<typename T>
Range<T> Span(const Multirange<T>& ranges) {
  if (ranges.empty()) {
    return EmptyRange<T>();
  }
  return {.lower = ranges.front().lower,
          .upper = ranges.back().upper,
          .empty = false};
}

template<typename Traits>
Multirange<typename Traits::Element> MultirangeUnion(
  const Multirange<typename Traits::Element>& left,
  const Multirange<typename Traits::Element>& right) {
  auto ranges = left;
  ranges.insert(ranges.end(), right.begin(), right.end());
  return Normalize<Traits>(std::move(ranges));
}

template<typename Traits>
Multirange<typename Traits::Element> MultirangeIntersect(
  const Multirange<typename Traits::Element>& left,
  const Multirange<typename Traits::Element>& right) {
  Multirange<typename Traits::Element> result;
  auto other = right.begin();
  for (const auto& range : left) {
    while (other != right.end() && Before(*other, range)) {
      ++other;
    }
    while (other != right.end() && Overlaps(range, *other)) {
      result.push_back(Intersect<Traits>(range, *other));
      if (!OverLeft(*other, range)) {
        break;
      }
      ++other;
    }
    if (other == right.end()) {
      break;
    }
  }
  return Normalize<Traits>(std::move(result));
}

template<typename Traits>
Multirange<typename Traits::Element> MultirangeMinus(
  const Multirange<typename Traits::Element>& left,
  const Multirange<typename Traits::Element>& right) {
  Multirange<typename Traits::Element> result;
  auto other = right.begin();
  for (auto range : left) {
    while (other != right.end() && Before(*other, range)) {
      ++other;
    }
    while (other != right.end()) {
      if (CompareBounds(range.lower, other->lower) < 0 &&
          CompareBounds(range.upper, other->upper) > 0) {
        result.push_back(Make<Traits>(
          {.lower = range.lower, .upper = Flip(other->lower), .empty = false}));
        range = Make<Traits>(
          {.lower = Flip(other->upper), .upper = range.upper, .empty = false});
        ++other;
      } else if (Overlaps(range, *other)) {
        range = Minus<Traits>(range, *other);
        if (range.empty || Before(range, *other)) {
          break;
        }
        ++other;
      } else {
        break;
      }
    }
    result.push_back(range);
  }
  return Normalize<Traits>(std::move(result));
}

template<typename T>
bool MultirangeContainsElement(const Multirange<T>& ranges, const T& value) {
  return absl::c_any_of(ranges, [&](const Range<T>& range) {
    return ContainsElement(range, value);
  });
}

template<typename T>
bool ElementContainedByMultirange(const T& value, const Multirange<T>& ranges) {
  return MultirangeContainsElement(ranges, value);
}

template<typename T>
bool MultirangeContainsRange(const Multirange<T>& ranges,
                             const Range<T>& range) {
  return range.empty || absl::c_any_of(ranges, [&](const Range<T>& outer) {
           return Contains(outer, range);
         });
}

template<typename T>
bool RangeContainedByMultirange(const Range<T>& range,
                                const Multirange<T>& ranges) {
  return MultirangeContainsRange(ranges, range);
}

template<typename T>
bool RangeContainsMultirange(const Range<T>& range,
                             const Multirange<T>& ranges) {
  return ranges.empty() || Contains(range, Span(ranges));
}

template<typename T>
bool MultirangeContainedByRange(const Multirange<T>& ranges,
                                const Range<T>& range) {
  return RangeContainsMultirange(range, ranges);
}

template<typename T>
bool MultirangeContains(const Multirange<T>& outer,
                        const Multirange<T>& inner) {
  auto range = outer.begin();
  for (const auto& needle : inner) {
    while (range != outer.end() &&
           CompareBounds(range->upper, needle.lower) < 0) {
      ++range;
    }
    if (range == outer.end() || !Contains(*range, needle)) {
      return false;
    }
  }
  return true;
}

template<typename T>
bool MultirangeContainedBy(const Multirange<T>& inner,
                           const Multirange<T>& outer) {
  return MultirangeContains(outer, inner);
}

template<typename T>
bool RangeOverlapsMultirange(const Range<T>& range,
                             const Multirange<T>& ranges) {
  return absl::c_any_of(
    ranges, [&](const Range<T>& other) { return Overlaps(range, other); });
}

template<typename T>
bool MultirangeOverlapsRange(const Multirange<T>& ranges,
                             const Range<T>& range) {
  return RangeOverlapsMultirange(range, ranges);
}

template<typename T>
bool MultirangesOverlap(const Multirange<T>& left, const Multirange<T>& right) {
  auto range = left.begin();
  for (const auto& other : right) {
    while (range != left.end() &&
           CompareBounds(range->upper, other.lower) < 0) {
      ++range;
    }
    if (range == left.end()) {
      return false;
    }
    if (Overlaps(*range, other)) {
      return true;
    }
  }
  return false;
}

template<typename T>
std::optional<T> LowerValue(const Range<T>& range) {
  if (range.empty || range.lower.infinite) {
    return std::nullopt;
  }
  return range.lower.value;
}

template<typename T>
std::optional<T> UpperValue(const Range<T>& range) {
  if (range.empty || range.upper.infinite) {
    return std::nullopt;
  }
  return range.upper.value;
}

template<typename T>
bool IsEmpty(const Range<T>& range) {
  return range.empty;
}

template<typename T>
bool LowerInclusive(const Range<T>& range) {
  return !range.empty && range.lower.inclusive;
}

template<typename T>
bool UpperInclusive(const Range<T>& range) {
  return !range.empty && range.upper.inclusive;
}

template<typename T>
bool LowerInfinite(const Range<T>& range) {
  return !range.empty && range.lower.infinite;
}

template<typename T>
bool UpperInfinite(const Range<T>& range) {
  return !range.empty && range.upper.infinite;
}

template<typename T, auto Apply>
auto OnSpan(const Multirange<T>& ranges) {
  return Apply(Span(ranges));
}

template<typename T, auto Apply>
bool OnSpans(const Multirange<T>& left, const Multirange<T>& right) {
  return Apply(Span(left), Span(right));
}

template<typename T, auto Apply>
bool OnSpanLeft(const Multirange<T>& left, const Range<T>& right) {
  return Apply(Span(left), right);
}

template<typename T, auto Apply>
bool OnSpanRight(const Range<T>& left, const Multirange<T>& right) {
  return Apply(left, Span(right));
}

template<typename V>
V Field(const duckdb::RecursiveUnifiedVectorFormat& range, duckdb::idx_t child,
        duckdb::idx_t index) {
  const auto& unified = range.children[child].unified;
  return duckdb::UnifiedVectorFormat::GetData<V>(
    unified)[unified.sel->get_index(index)];
}

template<typename T>
Range<T> ReadRange(const duckdb::RecursiveUnifiedVectorFormat& range,
                   duckdb::idx_t index) {
  const bool lower_finite = Field<bool>(range, kRangeLowerFiniteChild, index);
  return {
    .lower = {.value = Field<T>(range, kRangeLowerChild, index),
              .infinite = !lower_finite,
              .inclusive =
                lower_finite &&
                !Field<bool>(range, kRangeLowerExclusiveChild, index),
              .lower = true},
    .upper = {.value = Field<T>(range, kRangeUpperChild, index),
              .infinite = Field<bool>(range, kRangeUpperInfiniteChild, index),
              .inclusive = Field<bool>(range, kRangeUpperInclusiveChild, index),
              .lower = false},
    .empty = !Field<bool>(range, kRangeNonemptyChild, index),
  };
}

template<typename T>
void StoreValue(duckdb::Vector& vector, duckdb::idx_t row,
                const Range<T>& range) {
  auto& children = duckdb::StructVector::GetEntries(vector);
  const auto store = [&]<typename V>(duckdb::idx_t child, V value) {
    duckdb::FlatVector::GetDataMutable<V>(children[child])[row] = value;
  };
  const bool lower = !range.empty && !range.lower.infinite;
  const bool upper = !range.empty && !range.upper.infinite;
  store(kRangeNonemptyChild, !range.empty);
  store(kRangeLowerFiniteChild, lower);
  store(kRangeLowerChild, lower ? range.lower.value : T{});
  store(kRangeLowerExclusiveChild, lower && !range.lower.inclusive);
  store(kRangeUpperInfiniteChild, !range.empty && range.upper.infinite);
  store(kRangeUpperChild, upper ? range.upper.value : T{});
  store(kRangeUpperInclusiveChild, upper && range.upper.inclusive);
}

template<typename T>
void StoreValue(duckdb::Vector& vector, duckdb::idx_t row,
                const Multirange<T>& ranges) {
  StoreList<Range<T>>(vector, row, ranges);
}

template<typename T>
Multirange<T> ReadMultirange(const duckdb::RecursiveUnifiedVectorFormat& format,
                             duckdb::idx_t index) {
  const auto entry = duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(
    format.unified)[index];
  const auto& ranges = format.children[0];
  Multirange<T> result;
  result.reserve(entry.length);
  for (duckdb::idx_t i = 0; i < entry.length; ++i) {
    result.push_back(
      ReadRange<T>(ranges, ranges.unified.sel->get_index(entry.offset + i)));
  }
  return result;
}

template<typename T>
using RangeArg = RecursiveArg<Range<T>, ReadRange<T>>;

template<typename T>
using MultirangeArg = RecursiveArg<Multirange<T>, ReadMultirange<T>>;

struct RangeCastData final : duckdb::BoundCastData {
  explicit RangeCastData(duckdb::BoundCastInfo element)
    : element{std::move(element)} {}

  duckdb::unique_ptr<duckdb::BoundCastData> Copy() const final {
    return duckdb::make_uniq<RangeCastData>(element.Copy());
  }

  duckdb::BoundCastInfo element;
};

struct RangeCastState final : InputState {
  duckdb::unique_ptr<duckdb::FunctionLocalState> element;
};

duckdb::unique_ptr<duckdb::FunctionLocalState> InitRangeCast(
  duckdb::CastLocalStateParameters& parameters);

bool CastElements(duckdb::Vector& source, duckdb::Vector& result,
                  duckdb::idx_t count, duckdb::CastParameters& parameters);

template<typename Traits>
bool TextToRange(duckdb::Vector& source, duckdb::Vector& result,
                 duckdb::idx_t count, duckdb::CastParameters& parameters) {
  using T = typename Traits::Element;
  duckdb::Vector lower_text{duckdb::LogicalType::VARCHAR, count};
  duckdb::Vector upper_text{duckdb::LogicalType::VARCHAR, count};
  std::vector<uint8_t> flags(count);
  bool converted = true;
  {
    auto lowers =
      duckdb::FlatVector::Writer<duckdb::string_t>(lower_text, count);
    auto uppers =
      duckdb::FlatVector::Writer<duckdb::string_t>(upper_text, count);
    const auto write = [](auto& writer, const ParsedRange& range,
                          uint8_t infinite, const std::string& bound) {
      if (range.flags & (kRangeEmpty | infinite)) {
        writer.WriteNull();
      } else {
        writer.WriteValue(
          duckdb::string_t{bound.data(), static_cast<uint32_t>(bound.size())});
      }
    };
    ParsedRange range;
    const auto texts = source.Values<duckdb::string_t>();
    for (duckdb::idx_t row = 0; row < count; ++row) {
      const auto text = texts[row];
      if (text.IsValid()) {
        auto error = ParseRange(connector::AsView(text.GetValue()), range);
        if (!error) {
          flags[row] = range.flags;
          write(lowers, range, kRangeLowerInfinite, range.lower);
          write(uppers, range, kRangeUpperInfinite, range.upper);
          continue;
        }
        InputError(std::move(*error), parameters);
        converted = false;
      }
      duckdb::FlatVector::SetNull(result, row, true);
      lowers.WriteNull();
      uppers.WriteNull();
    }
  }
  const auto& element = ElementType(result.GetType());
  duckdb::Vector lower_values{element, count};
  duckdb::Vector upper_values{element, count};
  converted &= CastElements(lower_text, lower_values, count, parameters);
  converted &= CastElements(upper_text, upper_values, count, parameters);
  const ScalarArg<T> lowers{lower_values};
  const ScalarArg<T> uppers{upper_values};
  for (duckdb::idx_t row = 0; row < count; ++row) {
    if (duckdb::FlatVector::IsNull(result, row)) {
      continue;
    }
    const auto lower = lowers[row];
    const auto upper = uppers[row];
    const auto failed = [&](const auto& value, uint8_t infinite) {
      return !(flags[row] & (kRangeEmpty | infinite)) && !value;
    };
    if (failed(lower, kRangeLowerInfinite) ||
        failed(upper, kRangeUpperInfinite)) {
      duckdb::FlatVector::SetNull(result, row, true);
      continue;
    }
    auto range =
      FromFlags<T>(flags[row], lower.value_or(T{}), upper.value_or(T{}));
    if (auto error = MakeRange<Traits>(range)) {
      InputError(std::move(*error), parameters);
      converted = false;
      duckdb::FlatVector::SetNull(result, row, true);
      continue;
    }
    StoreValue(result, row, range);
  }
  return converted;
}

bool RangeToText(duckdb::Vector& source, duckdb::Vector& result,
                 duckdb::idx_t count, duckdb::CastParameters& parameters);

void RegisterMultiranges(duckdb::CastFunctionSet& casts, Functions& functions);

}  // namespace sdb::pg::ranges
