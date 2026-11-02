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

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>

#include <array>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <duckdb/function/aggregate_function.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <vector>

#include "connector/common.h"
#include "pg/types/cast.h"
#include "pg/types/range.h"
#include "pg/types/range_ops.h"

namespace sdb::pg {
namespace ranges {
namespace {

using connector::AsView;

template<typename Traits>
bool TextToMultirange(duckdb::Vector& source, duckdb::Vector& result,
                      duckdb::idx_t count, duckdb::CastParameters& parameters) {
  using T = typename Traits::Element;
  std::vector<duckdb::list_entry_t> entries(count);
  std::vector<std::string_view> texts;
  std::vector<std::string_view> row_texts;
  bool converted = true;
  const auto inputs = source.Values<duckdb::string_t>();
  for (duckdb::idx_t row = 0; row < count; ++row) {
    const auto text = inputs[row];
    if (text.IsValid()) {
      auto error = SplitMultirange(AsView(text.GetValue()), row_texts);
      if (!error) {
        entries[row] = {texts.size(), row_texts.size()};
        texts.insert(texts.end(), row_texts.begin(), row_texts.end());
        continue;
      }
      InputError(std::move(*error), parameters);
      converted = false;
    }
    duckdb::FlatVector::SetNull(result, row, true);
  }
  const auto range_count = texts.size();
  duckdb::Vector range_text{duckdb::LogicalType::VARCHAR, range_count};
  duckdb::Vector parsed{Traits::Type(), range_count};
  if (range_count != 0) {
    {
      auto writer =
        duckdb::FlatVector::Writer<duckdb::string_t>(range_text, range_count);
      for (const auto text : texts) {
        writer.WriteValue(
          duckdb::string_t{text.data(), static_cast<uint32_t>(text.size())});
      }
    }
    converted &=
      TextToRange<Traits>(range_text, parsed, range_count, parameters);
  }
  const RangeArg<T> ranges{parsed};
  for (duckdb::idx_t row = 0; row < count; ++row) {
    if (duckdb::FlatVector::IsNull(result, row)) {
      continue;
    }
    const auto entry = entries[row];
    Multirange<T> values;
    values.reserve(entry.length);
    for (duckdb::idx_t i = 0; i < entry.length; ++i) {
      if (const auto range = ranges[entry.offset + i]) {
        values.push_back(*range);
      } else {
        break;
      }
    }
    if (values.size() != entry.length) {
      duckdb::FlatVector::SetNull(result, row, true);
      continue;
    }
    StoreValue(result, row, Normalize<Traits>(std::move(values)));
  }
  return converted;
}

template<typename Traits>
duckdb::BoundCastInfo BindTextToMultirange(duckdb::BindCastInput& input,
                                           const duckdb::LogicalType&,
                                           const duckdb::LogicalType&) {
  return {TextToMultirange<Traits>,
          duckdb::make_uniq<RangeCastData>(input.GetCastFunction(
            duckdb::LogicalType::VARCHAR, ElementType(Traits::Type()))),
          InitRangeCast};
}

bool MultirangeToText(duckdb::Vector& source, duckdb::Vector& result,
                      duckdb::idx_t count, duckdb::CastParameters& parameters) {
  auto& child = duckdb::ListVector::GetChildMutable(source);
  const auto range_count = duckdb::ListVector::GetListSize(source);
  duckdb::Vector range_text{duckdb::LogicalType::VARCHAR, range_count};
  if (range_count != 0) {
    RangeToText(child, range_text, range_count, parameters);
  }
  duckdb::UnifiedVectorFormat lists;
  source.ToUnifiedFormat(lists);
  const auto* entries =
    duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(lists);
  auto texts = range_text.Values<duckdb::string_t>();
  auto writer = duckdb::FlatVector::Writer<duckdb::string_t>(result, count);
  std::string text;
  for (duckdb::idx_t row = 0; row < count; ++row) {
    const auto index = lists.sel->get_index(row);
    if (!lists.validity.RowIsValid(index)) {
      writer.WriteNull();
      continue;
    }
    const auto entry = entries[index];
    text.assign("{");
    for (duckdb::idx_t i = 0; i < entry.length; ++i) {
      if (i != 0) {
        text.push_back(',');
      }
      text.append(AsView(texts[entry.offset + i].GetValue()));
    }
    text.push_back('}');
    writer.WriteValue(
      duckdb::string_t{text.data(), static_cast<uint32_t>(text.size())});
  }
  return true;
}

duckdb::BoundCastInfo BindMultirangeToText(duckdb::BindCastInput& input,
                                           const duckdb::LogicalType& source,
                                           const duckdb::LogicalType&) {
  return {MultirangeToText,
          duckdb::make_uniq<RangeCastData>(input.GetCastFunction(
            ElementType(duckdb::ListType::GetChildType(source)),
            duckdb::LogicalType::VARCHAR)),
          InitRangeCast};
}

template<typename Traits>
bool StorageToMultirange(duckdb::Vector& source, duckdb::Vector& result,
                         duckdb::idx_t count,
                         duckdb::CastParameters& parameters) {
  const MultirangeArg<typename Traits::Element> multiranges{source};
  bool converted = true;
  for (duckdb::idx_t row = 0; row < count; ++row) {
    auto ranges = multiranges[row];
    if (ranges) {
      std::optional<irs::pg::SqlErrorData> error;
      for (auto& range : *ranges) {
        error = MakeRange<Traits>(range);
        if (error) {
          break;
        }
      }
      if (!error) {
        StoreValue(result, row, Normalize<Traits>(std::move(*ranges)));
        continue;
      }
      InputError(std::move(*error), parameters);
      converted = false;
    }
    duckdb::FlatVector::SetNull(result, row, true);
  }
  return converted;
}

template<typename T>
Multirange<T> ToMultirange(const Range<T>& range) {
  if (range.empty) {
    return {};
  }
  return {range};
}

template<typename T>
bool RangeToMultirange(duckdb::Vector& source, duckdb::Vector& result,
                       duckdb::idx_t count, duckdb::CastParameters&) {
  const RangeArg<T> ranges{source};
  for (duckdb::idx_t row = 0; row < count; ++row) {
    if (const auto range = ranges[row]) {
      StoreValue(result, row, ToMultirange(*range));
    } else {
      duckdb::FlatVector::SetNull(result, row, true);
    }
  }
  return true;
}

template<typename Traits>
void ConstructMultirange(duckdb::DataChunk& args, duckdb::ExpressionState&,
                         duckdb::Vector& result) {
  using T = typename Traits::Element;
  std::vector<RangeArg<T>> inputs;
  inputs.reserve(args.ColumnCount());
  for (auto& column : args.data) {
    inputs.emplace_back(column);
  }
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    Multirange<T> ranges;
    ranges.reserve(inputs.size());
    for (const auto& input : inputs) {
      if (const auto range = input[row]) {
        ranges.push_back(*range);
      } else if (inputs.size() != 1) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_NULL_VALUE_NOT_ALLOWED),
          ERR_MSG("multirange values cannot contain null members"));
      }
    }
    if (ranges.size() != inputs.size()) {
      duckdb::FlatVector::SetNull(result, row, true);
      continue;
    }
    StoreValue(result, row, Normalize<Traits>(std::move(ranges)));
  }
}

template<typename T>
struct RangeAggState {
  Multirange<T>* ranges;
};

template<typename Traits, typename Input>
struct RangeAgg {
  using T = typename Traits::Element;
  using State = RangeAggState<T>;

  static void Initialize(State& state) { state.ranges = nullptr; }

  static void Update(duckdb::Vector inputs[], duckdb::AggregateInputData&,
                     duckdb::idx_t, duckdb::Vector& states,
                     duckdb::idx_t count) {
    const Input input{inputs[0]};
    duckdb::UnifiedVectorFormat format;
    states.ToUnifiedFormat(format);
    auto* const* targets = duckdb::UnifiedVectorFormat::GetData<State*>(format);
    for (duckdb::idx_t row = 0; row < count; ++row) {
      const auto value = input[row];
      if (!value) {
        continue;
      }
      auto& state = *targets[format.sel->get_index(row)];
      if (!state.ranges) {
        state.ranges = new Multirange<T>;
      }
      if constexpr (std::is_same_v<Input, RangeArg<T>>) {
        state.ranges->push_back(*value);
      } else if (value->empty()) {
        state.ranges->push_back(EmptyRange<T>());
      } else {
        state.ranges->insert(state.ranges->end(), value->begin(), value->end());
      }
    }
  }

  template<typename, typename>
  static void Combine(const State& source, State& target,
                      duckdb::AggregateInputData&) {
    if (!source.ranges) {
      return;
    }
    if (!target.ranges) {
      target.ranges = new Multirange<T>;
    }
    target.ranges->insert(target.ranges->end(), source.ranges->begin(),
                          source.ranges->end());
  }

  template<typename>
  static void Finalize(State& state, duckdb::AggregateFinalizeData& data) {
    if (!state.ranges) {
      data.ReturnNull();
      return;
    }
    StoreValue(data.result, data.result_idx, Normalize<Traits>(*state.ranges));
  }

  template<typename>
  static void Destroy(State& state, duckdb::AggregateInputData&) {
    delete state.ranges;
    state.ranges = nullptr;
  }
};

template<typename T>
struct RangeIntersectState {
  Range<T> range;
  bool is_set;
};

template<typename Traits>
struct RangeIntersectAgg {
  using T = typename Traits::Element;
  using State = RangeIntersectState<T>;

  static void Initialize(State& state) { state.is_set = false; }

  static void Merge(State& state, const Range<T>& range) {
    state.range = state.is_set ? Intersect<Traits>(state.range, range) : range;
    state.is_set = true;
  }

  static void Update(duckdb::Vector inputs[], duckdb::AggregateInputData&,
                     duckdb::idx_t, duckdb::Vector& states,
                     duckdb::idx_t count) {
    const RangeArg<T> input{inputs[0]};
    duckdb::UnifiedVectorFormat format;
    states.ToUnifiedFormat(format);
    auto* const* targets = duckdb::UnifiedVectorFormat::GetData<State*>(format);
    for (duckdb::idx_t row = 0; row < count; ++row) {
      if (const auto value = input[row]) {
        Merge(*targets[format.sel->get_index(row)], *value);
      }
    }
  }

  template<typename, typename>
  static void Combine(const State& source, State& target,
                      duckdb::AggregateInputData&) {
    if (source.is_set) {
      Merge(target, source.range);
    }
  }

  template<typename>
  static void Finalize(State& state, duckdb::AggregateFinalizeData& data) {
    if (!state.is_set) {
      data.ReturnNull();
      return;
    }
    StoreValue(data.result, data.result_idx, state.range);
  }
};

template<typename Traits>
struct MultirangeIntersectAgg {
  using T = typename Traits::Element;
  using State = RangeAggState<T>;

  static void Initialize(State& state) { state.ranges = nullptr; }

  static void Merge(State& state, const Multirange<T>& ranges) {
    if (!state.ranges) {
      state.ranges = new Multirange<T>(ranges);
    } else {
      *state.ranges = MultirangeIntersect<Traits>(*state.ranges, ranges);
    }
  }

  static void Update(duckdb::Vector inputs[], duckdb::AggregateInputData&,
                     duckdb::idx_t, duckdb::Vector& states,
                     duckdb::idx_t count) {
    const MultirangeArg<T> input{inputs[0]};
    duckdb::UnifiedVectorFormat format;
    states.ToUnifiedFormat(format);
    auto* const* targets = duckdb::UnifiedVectorFormat::GetData<State*>(format);
    for (duckdb::idx_t row = 0; row < count; ++row) {
      if (const auto value = input[row]) {
        Merge(*targets[format.sel->get_index(row)], *value);
      }
    }
  }

  template<typename, typename>
  static void Combine(const State& source, State& target,
                      duckdb::AggregateInputData&) {
    if (source.ranges) {
      Merge(target, *source.ranges);
    }
  }

  template<typename>
  static void Finalize(State& state, duckdb::AggregateFinalizeData& data) {
    if (!state.ranges) {
      data.ReturnNull();
      return;
    }
    StoreValue(data.result, data.result_idx, *state.ranges);
  }

  template<typename>
  static void Destroy(State& state, duckdb::AggregateInputData&) {
    delete state.ranges;
    state.ranges = nullptr;
  }
};

template<typename Op>
duckdb::AggregateFunction Aggregate(const duckdb::LogicalType& input,
                                    const duckdb::LogicalType& result) {
  using State = typename Op::State;
  duckdb::aggregate_destructor_t destructor = nullptr;
  if constexpr (requires(State& state, duckdb::AggregateInputData& data) {
                  Op::template Destroy<State>(state, data);
                }) {
    destructor = duckdb::AggregateFunction::StateDestroy<State, Op>;
  }
  return duckdb::AggregateFunction{
    {input},
    result,
    duckdb::AggregateFunction::StateSize<State>,
    duckdb::AggregateFunction::StateInitialize<State, Op>,
    Op::Update,
    duckdb::AggregateFunction::StateCombine<State, Op>,
    duckdb::AggregateFunction::StateVoidFinalize<State, Op>,
    duckdb::FunctionNullHandling::DEFAULT_NULL_HANDLING,
    nullptr,
    nullptr,
    destructor,
  };
}

duckdb::LogicalType MakeMultirangeType(const duckdb::LogicalType& range,
                                       std::string_view alias) {
  return duckdb::LogicalType::LIST(range).WithAlias(std::string{alias});
}

template<typename Traits>
void RegisterMultirangeType(duckdb::CastFunctionSet& casts,
                            Functions& functions) {
  using T = typename Traits::Element;
  using R = RangeArg<T>;
  using M = MultirangeArg<T>;
  using E = ScalarArg<T>;
  const auto range = Traits::Type();
  const auto multirange = Traits::MultirangeType();
  const auto element = ElementType(range);
  const auto& boolean = duckdb::LogicalType::BOOLEAN;
  RegisterTextInput(casts, multirange, BindTextToMultirange<Traits>);
  casts.RegisterCastFunction(multirange, duckdb::LogicalType::VARCHAR,
                             BindMultirangeToText, 50);
  casts.RegisterCastFunction(range, multirange, RangeToMultirange<T>, -1);
  RegisterStorageCast<StorageToMultirange<Traits>>(
    casts, duckdb::LogicalType::LIST(RangeStorageType(element)), multirange);

  duckdb::ScalarFunction constructor{
    {},      multirange, ConstructMultirange<Traits>, nullptr, nullptr,
    nullptr, range};
  constructor.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  Add(functions, Traits::kMultirangeName, Fallible(std::move(constructor)));
  Add(functions, "multirange",
      {{range}, multirange, Unary<R, &ToMultirange<T>>});

  Add(functions, "lower",
      {{multirange}, element, Unary<M, &OnSpan<T, &LowerValue<T>>>});
  Add(functions, "upper",
      {{multirange}, element, Unary<M, &OnSpan<T, &UpperValue<T>>>});
  Add(functions, "isempty",
      {{multirange}, boolean, Unary<M, &OnSpan<T, &IsEmpty<T>>>});
  Add(functions, "lower_inc",
      {{multirange}, boolean, Unary<M, &OnSpan<T, &LowerInclusive<T>>>});
  Add(functions, "upper_inc",
      {{multirange}, boolean, Unary<M, &OnSpan<T, &UpperInclusive<T>>>});
  Add(functions, "lower_inf",
      {{multirange}, boolean, Unary<M, &OnSpan<T, &LowerInfinite<T>>>});
  Add(functions, "upper_inf",
      {{multirange}, boolean, Unary<M, &OnSpan<T, &UpperInfinite<T>>>});
  Add(functions, "range_merge", {{multirange}, range, Unary<M, &Span<T>>});

  Add(
    functions, "@>",
    {{multirange, multirange}, boolean, Binary<M, M, &MultirangeContains<T>>});
  Add(
    functions, "@>",
    {{multirange, range}, boolean, Binary<M, R, &MultirangeContainsRange<T>>});
  Add(functions, "@>",
      {{multirange, element},
       boolean,
       Binary<M, E, &MultirangeContainsElement<T>>});
  Add(
    functions, "@>",
    {{range, multirange}, boolean, Binary<R, M, &RangeContainsMultirange<T>>});
  Add(functions, "<@",
      {{multirange, multirange},
       boolean,
       Binary<M, M, &MultirangeContainedBy<T>>});
  Add(functions, "<@",
      {{range, multirange},
       boolean,
       Binary<R, M, &RangeContainedByMultirange<T>>});
  Add(functions, "<@",
      {{element, multirange},
       boolean,
       Binary<E, M, &ElementContainedByMultirange<T>>});
  Add(functions, "<@",
      {{multirange, range},
       boolean,
       Binary<M, R, &MultirangeContainedByRange<T>>});
  Add(
    functions, "&&",
    {{multirange, multirange}, boolean, Binary<M, M, &MultirangesOverlap<T>>});
  Add(
    functions, "&&",
    {{multirange, range}, boolean, Binary<M, R, &MultirangeOverlapsRange<T>>});
  Add(
    functions, "&&",
    {{range, multirange}, boolean, Binary<R, M, &RangeOverlapsMultirange<T>>});
  const auto spans = [&]<auto Apply>(std::string_view name) {
    Add(functions, name,
        {{multirange, multirange}, boolean, Binary<M, M, &OnSpans<T, Apply>>});
    Add(functions, name,
        {{multirange, range}, boolean, Binary<M, R, &OnSpanLeft<T, Apply>>});
    Add(functions, name,
        {{range, multirange}, boolean, Binary<R, M, &OnSpanRight<T, Apply>>});
  };
  spans.template operator()<&Before<T>>("<<");
  spans.template operator()<&After<T>>(">>");
  spans.template operator()<&OverLeft<T>>("&<");
  spans.template operator()<&OverRight<T>>("&>");
  spans.template operator()<&Adjacent<T>>("-|-");
  Add(functions, "+",
      {{multirange, multirange},
       multirange,
       Binary<M, M, &MultirangeUnion<Traits>>});
  Add(functions, "*",
      {{multirange, multirange},
       multirange,
       Binary<M, M, &MultirangeIntersect<Traits>>});
  Add(functions, "-",
      {{multirange, multirange},
       multirange,
       Binary<M, M, &MultirangeMinus<Traits>>});

  Add(functions, "range_agg",
      Aggregate<RangeAgg<Traits, R>>(range, multirange));
  Add(functions, "range_agg",
      Aggregate<RangeAgg<Traits, M>>(multirange, multirange));
  Add(functions, "range_intersect_agg",
      Aggregate<RangeIntersectAgg<Traits>>(range, range));
  Add(functions, "range_intersect_agg",
      Aggregate<MultirangeIntersectAgg<Traits>>(multirange, multirange));
}

}  // namespace

void RegisterMultiranges(duckdb::CastFunctionSet& casts, Functions& functions) {
  ForEachRangeKind([&]<typename Traits>(std::type_identity<Traits>) {
    RegisterMultirangeType<Traits>(casts, functions);
  });
}

}  // namespace ranges

duckdb::LogicalType INT4MULTIRANGE() {
  static const auto kType =
    ranges::MakeMultirangeType(INT4RANGE(), kInt4multirangeAlias);
  return kType;
}

duckdb::LogicalType INT8MULTIRANGE() {
  static const auto kType =
    ranges::MakeMultirangeType(INT8RANGE(), kInt8multirangeAlias);
  return kType;
}

duckdb::LogicalType NUMMULTIRANGE() {
  static const auto kType =
    ranges::MakeMultirangeType(NUMRANGE(), kNummultirangeAlias);
  return kType;
}

duckdb::LogicalType DATEMULTIRANGE() {
  static const auto kType =
    ranges::MakeMultirangeType(DATERANGE(), kDatemultirangeAlias);
  return kType;
}

duckdb::LogicalType TSMULTIRANGE() {
  static const auto kType =
    ranges::MakeMultirangeType(TSRANGE(), kTsmultirangeAlias);
  return kType;
}

duckdb::LogicalType TSTZMULTIRANGE() {
  static const auto kType =
    ranges::MakeMultirangeType(TSTZRANGE(), kTstzmultirangeAlias);
  return kType;
}

bool IsMultirange(const duckdb::LogicalType& type) {
  static constexpr std::array kAliases = {
    kInt4multirangeAlias, kInt8multirangeAlias, kNummultirangeAlias,
    kDatemultirangeAlias, kTsmultirangeAlias,   kTstzmultirangeAlias,
  };
  return type.id() == duckdb::LogicalTypeId::LIST && type.HasAlias() &&
         absl::c_linear_search(kAliases, type.GetAlias());
}

std::optional<irs::pg::SqlErrorData> SplitMultirange(
  std::string_view text, std::vector<std::string_view>& ranges) {
  const auto malformed = [&](std::string_view detail) {
    return SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
      ERR_MSG("malformed multirange literal: \"", text, "\""),
      ERR_DETAIL(detail));
  };
  enum class State : uint8_t {
    kBeforeRange,
    kInRange,
    kInRangeEscaped,
    kInRangeQuoted,
    kInRangeQuotedEscaped,
    kAfterRange,
    kFinished,
  };
  const auto is_space = [&](size_t i) {
    return absl::ascii_isspace(static_cast<unsigned char>(text[i]));
  };
  ranges.clear();
  size_t i = 0;
  while (i < text.size() && is_space(i)) {
    ++i;
  }
  if (i == text.size() || text[i] != '{') {
    return malformed("Missing left brace.");
  }
  bool seen = false;
  size_t begin = 0;
  for (auto state = State::kBeforeRange; state != State::kFinished;) {
    if (++i == text.size()) {
      return malformed("Unexpected end of input.");
    }
    if (is_space(i)) {
      continue;
    }
    const char c = text[i];
    switch (state) {
      case State::kBeforeRange:
        if (c == '[' || c == '(') {
          begin = i;
          state = State::kInRange;
        } else if (c == '}' && !seen) {
          state = State::kFinished;
        } else if (absl::StartsWithIgnoreCase(text.substr(i), "empty")) {
          seen = true;
          i += sizeof("empty") - 2;
          state = State::kAfterRange;
        } else {
          return malformed("Expected range start.");
        }
        break;
      case State::kInRange:
        if (c == ']' || c == ')') {
          ranges.push_back(text.substr(begin, i - begin + 1));
          seen = true;
          state = State::kAfterRange;
        } else if (c == '"') {
          state = State::kInRangeQuoted;
        } else if (c == '\\') {
          state = State::kInRangeEscaped;
        }
        break;
      case State::kInRangeEscaped:
        state = State::kInRange;
        break;
      case State::kInRangeQuoted:
        if (c == '"') {
          if (i + 1 < text.size() && text[i + 1] == '"') {
            ++i;
          } else {
            state = State::kInRange;
          }
        } else if (c == '\\') {
          state = State::kInRangeQuotedEscaped;
        }
        break;
      case State::kInRangeQuotedEscaped:
        state = State::kInRangeQuoted;
        break;
      case State::kAfterRange:
        if (c == ',') {
          state = State::kBeforeRange;
        } else if (c == '}') {
          state = State::kFinished;
        } else {
          return malformed("Expected comma or end of multirange.");
        }
        break;
      case State::kFinished:
        SDB_UNREACHABLE();
    }
  }
  ++i;
  while (i < text.size() && is_space(i)) {
    ++i;
  }
  if (i != text.size()) {
    return malformed("Junk after closing right brace.");
  }
  return std::nullopt;
}

void FinishMultirange(duckdb::Vector& multirange, duckdb::idx_t row) {
  const auto& range = duckdb::ListType::GetChildType(multirange.GetType());
  ranges::VisitRangeKind(
    ranges::ElementType(range),
    [&]<typename Traits>(std::type_identity<Traits>) {
      using T = typename Traits::Element;
      auto& entry = duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(
        multirange)[row];
      auto& child = duckdb::ListVector::GetChildMutable(multirange);
      duckdb::RecursiveUnifiedVectorFormat format;
      duckdb::Vector::RecursiveToUnifiedFormat(child, format);
      ranges::Multirange<T> values;
      values.reserve(entry.length);
      for (duckdb::idx_t i = 0; i < entry.length; ++i) {
        values.push_back(ranges::ReadRange<T>(
          format, format.unified.sel->get_index(entry.offset + i)));
      }
      values = ranges::Normalize<Traits>(std::move(values));
      for (duckdb::idx_t i = 0; i < values.size(); ++i) {
        ranges::StoreValue(child, entry.offset + i, values[i]);
      }
      entry.length = values.size();
    });
}

}  // namespace sdb::pg
