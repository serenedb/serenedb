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
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>

#include <array>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/common/vector/vector_writer.hpp>

#include "connector/common.h"
#include "pg/types/cast.h"
#include "pg/types/range_ops.h"

namespace sdb::pg {
namespace ranges {
namespace {

using connector::AsView;

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
  SDB_ASSERT(!value.empty() && !absl::c_any_of(value, [](char c) {
    return c == '"' || c == '\\' || c == '(' || c == ')' || c == '[' ||
           c == ']' || c == ',';
  }));
  const bool quote = absl::c_any_of(value, [](char c) {
    return absl::ascii_isspace(static_cast<unsigned char>(c));
  });
  if (quote) {
    out.push_back('"');
  }
  out.append(value);
  if (quote) {
    out.push_back('"');
  }
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
  const ScalarArg<T> lowers{args.data[0]};
  const ScalarArg<T> uppers{args.data[1]};
  std::optional<ScalarArg<duckdb::string_t>> texts;
  if constexpr (kFlags) {
    texts.emplace(args.data[2]);
  }
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    uint8_t flags = kRangeLowerInclusive;
    if constexpr (kFlags) {
      const auto text = (*texts)[row];
      if (!text) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_DATA_EXCEPTION),
          ERR_MSG("range constructor flags argument must not be null"));
      }
      flags = ParseFlags(AsView(*text));
    }
    const auto lower = lowers[row];
    const auto upper = uppers[row];
    flags |=
      (lower ? 0 : kRangeLowerInfinite) | (upper ? 0 : kRangeUpperInfinite);
    StoreValue(result, row,
               Make<Traits>(FromFlags<T>(flags, lower.value_or(T{}),
                                         upper.value_or(T{}))));
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
  StoreValue(vector, row, range);
  return true;
}

template<typename Traits>
bool StorageToRange(duckdb::Vector& source, duckdb::Vector& result,
                    duckdb::idx_t count, duckdb::CastParameters& parameters) {
  const RangeArg<typename Traits::Element> ranges{source};
  bool converted = true;
  for (duckdb::idx_t row = 0; row < count; ++row) {
    auto range = ranges[row];
    if (range) {
      auto error = MakeRange<Traits>(*range);
      if (!error) {
        StoreValue(result, row, *range);
        continue;
      }
      InputError(std::move(*error), parameters);
      converted = false;
    }
    duckdb::FlatVector::SetNull(result, row, true);
  }
  return converted;
}

template<typename Traits>
void RegisterRangeType(duckdb::CastFunctionSet& casts, Functions& functions) {
  using T = typename Traits::Element;
  using R = RangeArg<T>;
  const auto range = Traits::Type();
  const auto element = ElementType(range);
  const auto& boolean = duckdb::LogicalType::BOOLEAN;
  const auto& varchar = duckdb::LogicalType::VARCHAR;
  RegisterTextInput(casts, range, BindTextToRange<Traits>);
  casts.RegisterCastFunction(range, varchar, BindRangeToText, 50);
  RegisterStorageCast<StorageToRange<Traits>>(casts, RangeStorageType(element),
                                              range);

  for (auto constructor : {
         duckdb::ScalarFunction{
           {element, element}, range, ConstructRange<Traits, false>},
         duckdb::ScalarFunction{
           {element, element, varchar}, range, ConstructRange<Traits, true>},
       }) {
    constructor.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    Add(functions, Traits::kName, Fallible(std::move(constructor)));
  }

  Add(functions, "lower", {{range}, element, Unary<R, &LowerValue<T>>});
  Add(functions, "upper", {{range}, element, Unary<R, &UpperValue<T>>});
  Add(functions, "isempty", {{range}, boolean, Unary<R, &IsEmpty<T>>});
  Add(functions, "lower_inc", {{range}, boolean, Unary<R, &LowerInclusive<T>>});
  Add(functions, "upper_inc", {{range}, boolean, Unary<R, &UpperInclusive<T>>});
  Add(functions, "lower_inf", {{range}, boolean, Unary<R, &LowerInfinite<T>>});
  Add(functions, "upper_inf", {{range}, boolean, Unary<R, &UpperInfinite<T>>});

  Add(functions, "@>", {{range, range}, boolean, Binary<R, R, &Contains<T>>});
  Add(
    functions, "@>",
    {{range, element}, boolean, Binary<R, ScalarArg<T>, &ContainsElement<T>>});
  Add(functions, "<@",
      {{range, range}, boolean, Binary<R, R, &ContainedBy<T>>});
  Add(functions, "<@",
      {{element, range},
       boolean,
       Binary<ScalarArg<T>, R, &ElementContainedBy<T>>});
  Add(functions, "&&", {{range, range}, boolean, Binary<R, R, &Overlaps<T>>});
  Add(functions, "<<", {{range, range}, boolean, Binary<R, R, &Before<T>>});
  Add(functions, ">>", {{range, range}, boolean, Binary<R, R, &After<T>>});
  Add(functions, "&<", {{range, range}, boolean, Binary<R, R, &OverLeft<T>>});
  Add(functions, "&>", {{range, range}, boolean, Binary<R, R, &OverRight<T>>});
  Add(functions, "-|-", {{range, range}, boolean, Binary<R, R, &Adjacent<T>>});
  Add(functions, "+",
      Fallible({{range, range}, range, Binary<R, R, &Union<Traits, true>>}));
  Add(functions, "*",
      {{range, range}, range, Binary<R, R, &Intersect<Traits>>});
  Add(functions, "-",
      Fallible({{range, range}, range, Binary<R, R, &Minus<Traits>>}));
  Add(functions, "range_merge",
      {{range, range}, range, Binary<R, R, &Union<Traits, false>>});
}

}  // namespace

duckdb::LogicalType RangeStorageType(const duckdb::LogicalType& element) {
  const auto& boolean = duckdb::LogicalType::BOOLEAN;
  return duckdb::LogicalType::STRUCT({
    {"nonempty", boolean},
    {"lower_finite", boolean},
    {"lower", element},
    {"lower_exclusive", boolean},
    {"upper_infinite", boolean},
    {"upper", element},
    {"upper_inclusive", boolean},
  });
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

}  // namespace ranges

duckdb::LogicalType INT4RANGE() {
  static const auto kType =
    ranges::RangeStorageType(duckdb::LogicalType::INTEGER)
      .WithAlias(std::string{kInt4rangeAlias});
  return kType;
}

duckdb::LogicalType INT8RANGE() {
  static const auto kType =
    ranges::RangeStorageType(duckdb::LogicalType::BIGINT)
      .WithAlias(std::string{kInt8rangeAlias});
  return kType;
}

duckdb::LogicalType NUMRANGE() {
  static const auto kType =
    ranges::RangeStorageType(duckdb::LogicalType::DECIMAL(18, 3))
      .WithAlias(std::string{kNumrangeAlias});
  return kType;
}

duckdb::LogicalType DATERANGE() {
  static const auto kType = ranges::RangeStorageType(duckdb::LogicalType::DATE)
                              .WithAlias(std::string{kDaterangeAlias});
  return kType;
}

duckdb::LogicalType TSRANGE() {
  static const auto kType =
    ranges::RangeStorageType(duckdb::LogicalType::TIMESTAMP)
      .WithAlias(std::string{kTsrangeAlias});
  return kType;
}

duckdb::LogicalType TSTZRANGE() {
  static const auto kType =
    ranges::RangeStorageType(duckdb::LogicalType::TIMESTAMP_TZ)
      .WithAlias(std::string{kTstzrangeAlias});
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
    return ranges::Field<bool>(range, child, row);
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
  return ranges::VisitRangeKind(
    ranges::ElementType(range.GetType()),
    [&]<typename Traits>(std::type_identity<Traits>) {
      return ranges::Finish<Traits>(range, row, flags);
    });
}

void RegisterRange(duckdb::CastFunctionSet& casts, Functions& functions) {
  ranges::ForEachRangeKind([&]<typename Traits>(std::type_identity<Traits>) {
    ranges::RegisterRangeType<Traits>(casts, functions);
  });
  ranges::RegisterMultiranges(casts, functions);
}

}  // namespace sdb::pg
