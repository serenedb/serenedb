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

#include "pg/types/range.h"

#include <absl/algorithm/container.h>
#include <absl/container/btree_map.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>

#include <array>
#include <duckdb/common/types/date.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <duckdb/function/cast/cast_function_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <vector>

#include "connector/common.h"
#include "pg/types/cast.h"

namespace sdb::pg {
namespace {

using connector::AsView;

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

struct Int4Range {
  using Element = int32_t;
  static constexpr std::string_view kName = kInt4rangeAlias;
  static constexpr bool kDiscrete = true;
  static duckdb::LogicalType Type() { return INT4RANGE(); }
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
  static constexpr bool kDiscrete = true;
  static duckdb::LogicalType Type() { return INT8RANGE(); }
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
  static constexpr bool kDiscrete = false;
  static duckdb::LogicalType Type() { return NUMRANGE(); }
};

struct DateRange {
  using Element = duckdb::date_t;
  static constexpr std::string_view kName = kDaterangeAlias;
  static constexpr bool kDiscrete = true;
  static duckdb::LogicalType Type() { return DATERANGE(); }
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
  static constexpr bool kDiscrete = false;
  static duckdb::LogicalType Type() { return TSRANGE(); }
};

struct TstzRange {
  using Element = duckdb::timestamp_tz_t;
  static constexpr std::string_view kName = kTstzrangeAlias;
  static constexpr bool kDiscrete = false;
  static duckdb::LogicalType Type() { return TSTZRANGE(); }
};

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
  if (cmp != 0 || left.infinite || right.infinite) {
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
bool Contains(const Range<T>& outer, const Range<T>& inner) {
  if (inner.empty) {
    return true;
  }
  return !outer.empty && CompareBounds(outer.lower, inner.lower) <= 0 &&
         CompareBounds(outer.upper, inner.upper) >= 0;
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
bool Before(const Range<T>& left, const Range<T>& right) {
  return !left.empty && !right.empty &&
         CompareBounds(left.upper, right.lower) < 0;
}

template<typename T>
bool After(const Range<T>& left, const Range<T>& right) {
  return !left.empty && !right.empty &&
         CompareBounds(left.lower, right.upper) > 0;
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
  if (left.empty || right.empty) {
    return false;
  }
  const auto starts_within = [](const Range<T>& range, const Range<T>& other) {
    return CompareBounds(range.lower, other.lower) >= 0 &&
           CompareBounds(range.lower, other.upper) <= 0;
  };
  return starts_within(left, right) || starts_within(right, left);
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
  if (kStrict && !Overlaps(left, right) && !Adjacent(left, right)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_EXCEPTION),
                    ERR_MSG("result of range union would not be contiguous"));
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
  if (lower_lower <= 0 && upper_lower >= 0 && upper_upper <= 0) {
    auto upper = right.lower;
    upper.inclusive = !upper.inclusive;
    upper.lower = false;
    return Make<Traits>({.lower = left.lower, .upper = upper, .empty = false});
  }
  SDB_ASSERT(lower_lower >= 0 && upper_upper >= 0 && lower_upper <= 0);
  auto lower = right.upper;
  lower.inclusive = !lower.inclusive;
  lower.lower = true;
  return Make<Traits>({.lower = lower, .upper = left.upper, .empty = false});
}

template<typename T>
using RangeStruct =
  duckdb::VectorStructType<bool, bool, T, bool, bool, T, bool>;

template<typename T>
using RangeEntry = typename duckdb::VectorIterator<RangeStruct<T>>::ValueEntry;

template<duckdb::idx_t I, typename Entry>
auto Child(const Entry& entry) {
  return entry.template GetChildValue<I>().GetValueUnsafe();
}

template<typename T>
Range<T> ReadRange(const RangeEntry<T>& entry) {
  const bool lower_finite = Child<kRangeLowerFiniteChild>(entry);
  return {
    .lower = {.value = Child<kRangeLowerChild>(entry),
              .infinite = !lower_finite,
              .inclusive =
                lower_finite && !Child<kRangeLowerExclusiveChild>(entry),
              .lower = true},
    .upper = {.value = Child<kRangeUpperChild>(entry),
              .infinite = Child<kRangeUpperInfiniteChild>(entry),
              .inclusive = Child<kRangeUpperInclusiveChild>(entry),
              .lower = false},
    .empty = !Child<kRangeNonemptyChild>(entry),
  };
}

template<typename T>
void StoreRange(duckdb::Vector& vector, duckdb::idx_t row,
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

void Store(duckdb::Vector& result, duckdb::idx_t row, bool value) {
  duckdb::FlatVector::GetDataMutable<bool>(result)[row] = value;
}

template<typename T>
void Store(duckdb::Vector& result, duckdb::idx_t row, const Range<T>& range) {
  StoreRange(result, row, range);
}

const duckdb::LogicalType& ElementType(const duckdb::LogicalType& range) {
  return duckdb::StructType::GetChildType(range, kRangeLowerChild);
}

duckdb::LogicalType MakeRangeType(const duckdb::LogicalType& element,
                                  std::string_view alias) {
  const auto& boolean = duckdb::LogicalType::BOOLEAN;
  return duckdb::LogicalType::STRUCT({
                                       {"nonempty", boolean},
                                       {"lower_finite", boolean},
                                       {"lower", element},
                                       {"lower_exclusive", boolean},
                                       {"upper_infinite", boolean},
                                       {"upper", element},
                                       {"upper_inclusive", boolean},
                                     })
    .WithAlias(std::string{alias});
}

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
  duckdb::CastLocalStateParameters& parameters) {
  auto state = duckdb::make_uniq<RangeCastState>();
  const auto& element = parameters.cast_data->Cast<RangeCastData>().element;
  if (element.HasInitLocalState()) {
    duckdb::CastLocalStateParameters element_parameters{parameters,
                                                        element.GetCastData()};
    state->element = element.InitLocalState(element_parameters);
  }
  return state;
}

bool CastElements(duckdb::Vector& source, duckdb::Vector& result,
                  duckdb::idx_t count, duckdb::CastParameters& parameters) {
  const auto& element = parameters.cast_data->Cast<RangeCastData>().element;
  SDB_ASSERT(parameters.local_state);
  auto& state = parameters.local_state->Cast<RangeCastState>();
  duckdb::CastParameters element_parameters{parameters, element.GetCastData(),
                                            state.element.get()};
  return element.Cast(source, result, count, element_parameters);
}

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
    for (auto text : source.Values<duckdb::string_t>()) {
      const auto row = text.GetIndex();
      if (text.IsValid()) {
        auto error = ParseRange(AsView(text.GetValue()), range);
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
  auto lowers = lower_values.Values<T>();
  auto uppers = upper_values.Values<T>();
  for (duckdb::idx_t row = 0; row < count; ++row) {
    if (duckdb::FlatVector::IsNull(result, row)) {
      continue;
    }
    const auto lower = lowers[row];
    const auto upper = uppers[row];
    const auto failed = [&](const auto& value, uint8_t infinite) {
      return !(flags[row] & (kRangeEmpty | infinite)) && !value.IsValid();
    };
    if (failed(lower, kRangeLowerInfinite) ||
        failed(upper, kRangeUpperInfinite)) {
      duckdb::FlatVector::SetNull(result, row, true);
      continue;
    }
    auto range =
      FromFlags<T>(flags[row], lower.GetValueUnsafe(), upper.GetValueUnsafe());
    if (auto error = MakeRange<Traits>(range)) {
      InputError(std::move(*error), parameters);
      converted = false;
      duckdb::FlatVector::SetNull(result, row, true);
      continue;
    }
    StoreRange(result, row, range);
  }
  return converted;
}

template<typename Traits>
duckdb::BoundCastInfo BindTextToRange(duckdb::BindCastInput& input,
                                      const duckdb::LogicalType&,
                                      const duckdb::LogicalType& target) {
  return {TextToRange<Traits>,
          duckdb::make_uniq<RangeCastData>(input.GetCastFunction(
            duckdb::LogicalType::VARCHAR, ElementType(target))),
          InitRangeCast};
}

void AppendBound(std::string& out, std::string_view value) {
  const bool quote =
    value.empty() || absl::c_any_of(value, [](char c) {
      return c == '"' || c == '\\' || c == '(' || c == ')' || c == '[' ||
             c == ']' || c == ',' ||
             absl::ascii_isspace(static_cast<unsigned char>(c));
    });
  if (quote) {
    out.push_back('"');
  }
  for (const char c : value) {
    if (c == '"' || c == '\\') {
      out.push_back(c);
    }
    out.push_back(c);
  }
  if (quote) {
    out.push_back('"');
  }
}

bool RangeToText(duckdb::Vector& source, duckdb::Vector& result,
                 duckdb::idx_t count, duckdb::CastParameters& parameters) {
  auto& children = duckdb::StructVector::GetEntries(source);
  duckdb::Vector lower_text{duckdb::LogicalType::VARCHAR, count};
  duckdb::Vector upper_text{duckdb::LogicalType::VARCHAR, count};
  CastElements(children[kRangeLowerChild], lower_text, count, parameters);
  CastElements(children[kRangeUpperChild], upper_text, count, parameters);
  duckdb::RecursiveUnifiedVectorFormat ranges;
  duckdb::Vector::RecursiveToUnifiedFormat(source, ranges);
  auto lowers = lower_text.Values<duckdb::string_t>();
  auto uppers = upper_text.Values<duckdb::string_t>();
  auto writer = duckdb::FlatVector::Writer<duckdb::string_t>(result, count);
  std::string text;
  for (duckdb::idx_t row = 0; row < count; ++row) {
    const auto index = ranges.unified.sel->get_index(row);
    if (!ranges.unified.validity.RowIsValid(index)) {
      writer.WriteNull();
      continue;
    }
    const auto flags = ReadRangeFlags(ranges, index);
    if (flags & kRangeEmpty) {
      writer.WriteValue(duckdb::string_t{"empty"});
      continue;
    }
    text.assign(flags & kRangeLowerInclusive ? "[" : "(");
    if (!(flags & kRangeLowerInfinite)) {
      AppendBound(text, AsView(lowers[row].GetValue()));
    }
    text.push_back(',');
    if (!(flags & kRangeUpperInfinite)) {
      AppendBound(text, AsView(uppers[row].GetValue()));
    }
    text.push_back(flags & kRangeUpperInclusive ? ']' : ')');
    writer.WriteValue(
      duckdb::string_t{text.data(), static_cast<uint32_t>(text.size())});
  }
  return true;
}

duckdb::BoundCastInfo BindRangeToText(duckdb::BindCastInput& input,
                                      const duckdb::LogicalType& source,
                                      const duckdb::LogicalType&) {
  return {RangeToText,
          duckdb::make_uniq<RangeCastData>(input.GetCastFunction(
            ElementType(source), duckdb::LogicalType::VARCHAR)),
          InitRangeCast};
}

uint8_t ParseFlags(std::string_view text) {
  if (text.size() != 2 || (text[0] != '[' && text[0] != '(') ||
      (text[1] != ']' && text[1] != ')')) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR), ERR_MSG("invalid range bound flags"),
      ERR_HINT("Valid values are \"[]\", \"[)\", \"(]\", and \"()\"."));
  }
  return static_cast<uint8_t>((text[0] == '[' ? kRangeLowerInclusive : 0) |
                              (text[1] == ']' ? kRangeUpperInclusive : 0));
}

template<typename Traits, bool kFlags>
void ConstructRange(duckdb::DataChunk& args, duckdb::ExpressionState&,
                    duckdb::Vector& result) {
  using T = typename Traits::Element;
  auto lowers = args.data[0].Values<T>();
  auto uppers = args.data[1].Values<T>();
  std::optional<duckdb::VectorIterator<duckdb::string_t>> texts;
  if constexpr (kFlags) {
    texts.emplace(args.data[2]);
  }
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    uint8_t flags = kRangeLowerInclusive;
    if constexpr (kFlags) {
      const auto text = (*texts)[row];
      if (!text.IsValid()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_DATA_EXCEPTION),
          ERR_MSG("range constructor flags argument must not be null"));
      }
      flags = ParseFlags(AsView(text.GetValue()));
    }
    const auto lower = lowers[row];
    const auto upper = uppers[row];
    flags |= (lower.IsValid() ? 0 : kRangeLowerInfinite) |
             (upper.IsValid() ? 0 : kRangeUpperInfinite);
    StoreRange(result, row,
               Make<Traits>(FromFlags<T>(flags, lower.GetValueUnsafe(),
                                         upper.GetValueUnsafe())));
  }
}

template<typename T, bool kUpper>
void BoundValueFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                        duckdb::Vector& result) {
  auto writer = duckdb::FlatVector::Writer<T>(result, args.size());
  for (auto entry : args.data[0].Values<RangeStruct<T>>()) {
    if (!entry.IsValid()) {
      writer.WriteNull();
      continue;
    }
    const auto range = ReadRange<T>(entry);
    const auto& bound = kUpper ? range.upper : range.lower;
    if (range.empty || bound.infinite) {
      writer.WriteNull();
    } else {
      writer.WriteValue(bound.value);
    }
  }
}

template<uint8_t kFlag>
void FlagFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                  duckdb::Vector& result) {
  duckdb::RecursiveUnifiedVectorFormat ranges;
  duckdb::Vector::RecursiveToUnifiedFormat(args.data[0], ranges);
  auto writer = duckdb::FlatVector::Writer<bool>(result, args.size());
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto index = ranges.unified.sel->get_index(row);
    if (!ranges.unified.validity.RowIsValid(index)) {
      writer.WriteNull();
      continue;
    }
    writer.WriteValue((ReadRangeFlags(ranges, index) & kFlag) != 0);
  }
}

template<typename T, auto Apply>
void RangeBinary(duckdb::DataChunk& args, duckdb::ExpressionState&,
                 duckdb::Vector& result) {
  auto lefts = args.data[0].Values<RangeStruct<T>>();
  auto rights = args.data[1].Values<RangeStruct<T>>();
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto left = lefts[row];
    const auto right = rights[row];
    if (!left.IsValid() || !right.IsValid()) {
      duckdb::FlatVector::SetNull(result, row, true);
      continue;
    }
    Store(result, row, Apply(ReadRange<T>(left), ReadRange<T>(right)));
  }
}

template<typename T, bool kElementFirst>
void ElementFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                     duckdb::Vector& result) {
  auto ranges = args.data[kElementFirst ? 1 : 0].Values<RangeStruct<T>>();
  auto elements = args.data[kElementFirst ? 0 : 1].Values<T>();
  auto writer = duckdb::FlatVector::Writer<bool>(result, args.size());
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto range = ranges[row];
    const auto element = elements[row];
    if (!range.IsValid() || !element.IsValid()) {
      writer.WriteNull();
      continue;
    }
    writer.WriteValue(ContainsElement(ReadRange<T>(range), element.GetValue()));
  }
}

template<typename Traits>
bool Finish(duckdb::Vector& vector, duckdb::idx_t row, uint8_t flags) {
  using T = typename Traits::Element;
  auto& children = duckdb::StructVector::GetEntries(vector);
  const auto value = [&](duckdb::idx_t child, uint8_t infinite) {
    return flags & (kRangeEmpty | infinite)
             ? T{}
             : duckdb::FlatVector::GetData<T>(children[child])[row];
  };
  auto range = FromFlags<T>(flags, value(kRangeLowerChild, kRangeLowerInfinite),
                            value(kRangeUpperChild, kRangeUpperInfinite));
  if (MakeRange<Traits>(range)) {
    return false;
  }
  StoreRange(vector, row, range);
  return true;
}

struct RangeFunctions {
  absl::btree_map<std::string_view, duckdb::ScalarFunctionSet> overloads;
  absl::btree_map<std::string_view, duckdb::ScalarFunctionSet> created;
};

void Add(absl::btree_map<std::string_view, duckdb::ScalarFunctionSet>& sets,
         std::string_view name, duckdb::ScalarFunction function) {
  sets.try_emplace(name, duckdb::Identifier{name})
    .first->second.AddFunction(std::move(function));
}

duckdb::ScalarFunction Fallible(duckdb::ScalarFunction function) {
  function.SetFallible();
  return function;
}

template<typename Traits>
void RegisterRangeType(duckdb::CastFunctionSet& casts,
                       RangeFunctions& functions) {
  using T = typename Traits::Element;
  const auto range = Traits::Type();
  const auto element = ElementType(range);
  const auto& boolean = duckdb::LogicalType::BOOLEAN;
  const auto& varchar = duckdb::LogicalType::VARCHAR;
  RegisterTextInput(casts, range, BindTextToRange<Traits>);
  casts.RegisterCastFunction(range, varchar, BindRangeToText, 50);
  RefuseOtherCasts(casts, range);

  for (auto constructor : {
         duckdb::ScalarFunction{
           {element, element}, range, ConstructRange<Traits, false>},
         duckdb::ScalarFunction{
           {element, element, varchar}, range, ConstructRange<Traits, true>},
       }) {
    constructor.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    Add(functions.created, Traits::kName, Fallible(std::move(constructor)));
  }

  Add(functions.overloads, "lower",
      {{range}, element, BoundValueFunction<T, false>});
  Add(functions.overloads, "upper",
      {{range}, element, BoundValueFunction<T, true>});
  Add(functions.created, "isempty",
      {{range}, boolean, FlagFunction<kRangeEmpty>});
  Add(functions.created, "lower_inc",
      {{range}, boolean, FlagFunction<kRangeLowerInclusive>});
  Add(functions.created, "upper_inc",
      {{range}, boolean, FlagFunction<kRangeUpperInclusive>});
  Add(functions.created, "lower_inf",
      {{range}, boolean, FlagFunction<kRangeLowerInfinite>});
  Add(functions.created, "upper_inf",
      {{range}, boolean, FlagFunction<kRangeUpperInfinite>});

  Add(functions.overloads, "@>",
      {{range, range}, boolean, RangeBinary<T, &Contains<T>>});
  Add(functions.overloads, "@>",
      {{range, element}, boolean, ElementFunction<T, false>});
  Add(functions.overloads, "<@",
      {{range, range}, boolean, RangeBinary<T, &ContainedBy<T>>});
  Add(functions.overloads, "<@",
      {{element, range}, boolean, ElementFunction<T, true>});
  Add(functions.overloads, "&&",
      {{range, range}, boolean, RangeBinary<T, &Overlaps<T>>});
  Add(functions.overloads, "<<",
      {{range, range}, boolean, RangeBinary<T, &Before<T>>});
  Add(functions.overloads, ">>",
      {{range, range}, boolean, RangeBinary<T, &After<T>>});
  Add(functions.created, "&<",
      {{range, range}, boolean, RangeBinary<T, &OverLeft<T>>});
  Add(functions.created, "&>",
      {{range, range}, boolean, RangeBinary<T, &OverRight<T>>});
  Add(functions.created, "-|-",
      {{range, range}, boolean, RangeBinary<T, &Adjacent<T>>});
  Add(functions.overloads, "+",
      Fallible({{range, range}, range, RangeBinary<T, &Union<Traits, true>>}));
  Add(functions.overloads, "*",
      {{range, range}, range, RangeBinary<T, &Intersect<Traits>>});
  Add(functions.overloads, "-",
      Fallible({{range, range}, range, RangeBinary<T, &Minus<Traits>>}));
  Add(functions.created, "range_merge",
      {{range, range}, range, RangeBinary<T, &Union<Traits, false>>});
}

}  // namespace

duckdb::LogicalType INT4RANGE() {
  static const auto kType =
    MakeRangeType(duckdb::LogicalType::INTEGER, kInt4rangeAlias);
  return kType;
}

duckdb::LogicalType INT8RANGE() {
  static const auto kType =
    MakeRangeType(duckdb::LogicalType::BIGINT, kInt8rangeAlias);
  return kType;
}

duckdb::LogicalType NUMRANGE() {
  static const auto kType =
    MakeRangeType(duckdb::LogicalType::DECIMAL(18, 3), kNumrangeAlias);
  return kType;
}

duckdb::LogicalType DATERANGE() {
  static const auto kType =
    MakeRangeType(duckdb::LogicalType::DATE, kDaterangeAlias);
  return kType;
}

duckdb::LogicalType TSRANGE() {
  static const auto kType =
    MakeRangeType(duckdb::LogicalType::TIMESTAMP, kTsrangeAlias);
  return kType;
}

duckdb::LogicalType TSTZRANGE() {
  static const auto kType =
    MakeRangeType(duckdb::LogicalType::TIMESTAMP_TZ, kTstzrangeAlias);
  return kType;
}

bool IsRange(const duckdb::LogicalType& type) {
  static constexpr std::array kAliases = {
    kInt4rangeAlias, kInt8rangeAlias, kNumrangeAlias,
    kDaterangeAlias, kTsrangeAlias,   kTstzrangeAlias,
  };
  return type.id() == duckdb::LogicalTypeId::STRUCT && type.HasAlias() &&
         absl::c_linear_search(kAliases, type.GetAlias());
}

uint8_t ReadRangeFlags(const duckdb::RecursiveUnifiedVectorFormat& range,
                       duckdb::idx_t row) {
  const auto flag = [&](duckdb::idx_t child) {
    const auto& unified = range.children[child].unified;
    return duckdb::UnifiedVectorFormat::GetData<bool>(
      unified)[unified.sel->get_index(row)];
  };
  if (!flag(kRangeNonemptyChild)) {
    return kRangeEmpty;
  }
  uint8_t flags = 0;
  if (!flag(kRangeLowerFiniteChild)) {
    flags |= kRangeLowerInfinite;
  } else if (!flag(kRangeLowerExclusiveChild)) {
    flags |= kRangeLowerInclusive;
  }
  if (flag(kRangeUpperInfiniteChild)) {
    flags |= kRangeUpperInfinite;
  } else if (flag(kRangeUpperInclusiveChild)) {
    flags |= kRangeUpperInclusive;
  }
  return flags;
}

std::optional<irs::pg::SqlErrorData> ParseRange(std::string_view text,
                                                ParsedRange& range) {
  const auto malformed = [&](std::string_view detail) {
    return SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                          ERR_MSG("malformed range literal: \"", text, "\""),
                          ERR_DETAIL(detail));
  };
  size_t i = 0;
  const auto skip_spaces = [&] {
    while (i < text.size() &&
           absl::ascii_isspace(static_cast<unsigned char>(text[i]))) {
      ++i;
    }
  };
  const auto at = [&](char symbol) {
    return i < text.size() && text[i] == symbol;
  };
  const auto at_delimiter = [&] { return at(',') || at(')') || at(']'); };
  skip_spaces();
  range.flags = 0;
  if (absl::StartsWithIgnoreCase(text.substr(i), "empty")) {
    i += sizeof("empty") - 1;
    skip_spaces();
    if (i != text.size()) {
      return malformed("Junk after \"empty\" key word.");
    }
    range.flags = kRangeEmpty;
    return std::nullopt;
  }
  if (at('[')) {
    range.flags |= kRangeLowerInclusive;
  } else if (!at('(')) {
    return malformed("Missing left parenthesis or bracket.");
  }
  ++i;
  const auto bound = [&](std::string& out, uint8_t infinite) {
    out.clear();
    if (at_delimiter()) {
      range.flags |= infinite;
      return true;
    }
    bool quoted = false;
    while (quoted || !at_delimiter()) {
      if (i == text.size()) {
        return false;
      }
      const char c = text[i++];
      if (c == '\\') {
        if (i == text.size()) {
          return false;
        }
        out.push_back(text[i++]);
      } else if (c != '"') {
        out.push_back(c);
      } else if (!quoted) {
        quoted = true;
      } else if (at('"')) {
        out.push_back(text[i++]);
      } else {
        quoted = false;
      }
    }
    return true;
  };
  if (!bound(range.lower, kRangeLowerInfinite)) {
    return malformed("Unexpected end of input.");
  }
  if (!at(',')) {
    return malformed("Missing comma after lower bound.");
  }
  ++i;
  if (!bound(range.upper, kRangeUpperInfinite)) {
    return malformed("Unexpected end of input.");
  }
  if (at(']')) {
    range.flags |= kRangeUpperInclusive;
  } else if (!at(')')) {
    return malformed("Too many commas.");
  }
  ++i;
  skip_spaces();
  if (i != text.size()) {
    return malformed("Junk after right parenthesis or bracket.");
  }
  return std::nullopt;
}

bool FinishRange(duckdb::Vector& range, duckdb::idx_t row, uint8_t flags) {
  using enum duckdb::LogicalTypeId;
  const auto element = ElementType(range.GetType()).id();
  if (element == INTEGER) {
    return Finish<Int4Range>(range, row, flags);
  }
  if (element == BIGINT) {
    return Finish<Int8Range>(range, row, flags);
  }
  if (element == DECIMAL) {
    return Finish<NumRange>(range, row, flags);
  }
  if (element == DATE) {
    return Finish<DateRange>(range, row, flags);
  }
  if (element == TIMESTAMP) {
    return Finish<TsRange>(range, row, flags);
  }
  SDB_ASSERT(element == TIMESTAMP_TZ);
  return Finish<TstzRange>(range, row, flags);
}

void RegisterRange(duckdb::DatabaseInstance& db) {
  auto& casts = duckdb::DBConfig::GetConfig(db).GetCastFunctions();
  RangeFunctions functions;
  RegisterRangeType<Int4Range>(casts, functions);
  RegisterRangeType<Int8Range>(casts, functions);
  RegisterRangeType<NumRange>(casts, functions);
  RegisterRangeType<DateRange>(casts, functions);
  RegisterRangeType<TsRange>(casts, functions);
  RegisterRangeType<TstzRange>(casts, functions);
  duckdb::ExtensionLoader loader{db, "serenedb"};
  for (auto& [_, set] : functions.overloads) {
    loader.AddFunctionOverload(std::move(set));
  }
  for (auto& [_, set] : functions.created) {
    loader.RegisterFunction(std::move(set));
  }
}

}  // namespace sdb::pg
