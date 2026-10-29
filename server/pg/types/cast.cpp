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

#include "pg/types/cast.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <fast_float/fast_float.h>

#include <array>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/operator/cast_operators.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <string>

#include "pg/types/geometric.h"
#include "pg/types/logical.h"
#include "pg/types/range.h"

namespace sdb::pg {
namespace {

bool ReportsInputErrors(const duckdb::LogicalType& type) {
  static constexpr std::array kAliases = {
    kRegprocAlias,
    kRegprocedureAlias,
    kRegoperAlias,
    kRegoperatorAlias,
    kRegclassAlias,
    kRegtypeAlias,
    kRegcollationAlias,
    kRegconfigAlias,
    kRegdictionaryAlias,
    kRegroleAlias,
    kRegnamespaceAlias,
    kTidAlias,
    kPgLsnAlias,
    kMacaddrAlias,
    kMacaddr8Alias,
    kInt2vectorAlias,
    kOidvectorAlias,
    kMoneyAlias,
    kXmlAlias,
    kInt4rangeAlias,
    kInt8rangeAlias,
    kNumrangeAlias,
    kDaterangeAlias,
    kTsrangeAlias,
    kTstzrangeAlias,
    kInt4multirangeAlias,
    kInt8multirangeAlias,
    kNummultirangeAlias,
    kDatemultirangeAlias,
    kTsmultirangeAlias,
    kTstzmultirangeAlias,
    kPointAlias,
    kLineAlias,
    kLsegAlias,
    kBoxAlias,
    kPathAlias,
    kPolygonAlias,
    kCircleAlias,
    kAclitemAlias,
  };
  if (type.id() == duckdb::LogicalTypeId::BLOB) {
    return !type.HasAlias();
  }
  return absl::c_linear_search(kAliases, type.GetAlias());
}

duckdb::BoundCastInfo TextCast(duckdb::ClientContext& context,
                               const duckdb::LogicalType& type) {
  duckdb::GetCastFunctionInput input{context};
  return duckdb::CastFunctionSet::Get(context).GetCastFunction(
    duckdb::LogicalType::VARCHAR, type, input);
}

constexpr std::array kStringAliases = {&NAME, &CHARACTERDATA, &SQLIDENTIFIER,
                                       &YESORNO};

template<typename Cast, typename TextCost>
void RegisterInputs(duckdb::CastFunctionSet& casts,
                    const duckdb::LogicalType& type, const Cast& cast,
                    const TextCost& text_cost) {
  for (const auto text : {duckdb::LogicalTypeId::VARCHAR,
                          duckdb::LogicalTypeId::STRING_LITERAL}) {
    const duckdb::LogicalType source{text};
    casts.RegisterCastFunction(source, type, cast(), text_cost(source));
  }
  for (const auto string : kStringAliases) {
    casts.RegisterCastFunction(string(), type, cast(), -1);
  }
}

struct StorageCastData final : duckdb::BoundCastData {
  StorageCastData(duckdb::LogicalType storage, duckdb::BoundCastInfo convert,
                  duckdb::cast_function_t check)
    : storage{std::move(storage)}, convert{std::move(convert)}, check{check} {}

  duckdb::unique_ptr<duckdb::BoundCastData> Copy() const final {
    return duckdb::make_uniq<StorageCastData>(storage, convert.Copy(), check);
  }

  duckdb::LogicalType storage;
  duckdb::BoundCastInfo convert;
  duckdb::cast_function_t check;
};

struct StorageCastState final : duckdb::FunctionLocalState {
  duckdb::unique_ptr<duckdb::FunctionLocalState> convert;
};

duckdb::unique_ptr<duckdb::FunctionLocalState> InitStorageCast(
  duckdb::CastLocalStateParameters& parameters) {
  auto state = duckdb::make_uniq<StorageCastState>();
  const auto& convert = parameters.cast_data->Cast<StorageCastData>().convert;
  if (convert.HasInitLocalState()) {
    duckdb::CastLocalStateParameters child{parameters, convert.GetCastData()};
    state->convert = convert.InitLocalState(child);
  }
  return state;
}

bool StorageCast(duckdb::Vector& source, duckdb::Vector& result,
                 duckdb::idx_t count, duckdb::CastParameters& parameters) {
  const auto& data = parameters.cast_data->Cast<StorageCastData>();
  duckdb::Vector storage{data.storage, count};
  duckdb::CastParameters convert{
    parameters, data.convert.GetCastData(),
    parameters.local_state->Cast<StorageCastState>().convert.get()};
  const bool converted = data.convert.Cast(source, storage, count, convert);
  duckdb::CastParameters check{parameters, nullptr, nullptr};
  return data.check(storage, result, count, check) && converted;
}

}  // namespace

TextInput::TextInput(duckdb::ClientContext& context,
                     const duckdb::LogicalType& type)
  : _reports{ReportsInputErrors(type)},
    _cast{TextCast(context, type)},
    _cache{duckdb::Allocator::Get(context), type, 1},
    _source{duckdb::LogicalType::VARCHAR, 1},
    _result{_cache} {
  duckdb::FlatVector::SetSize(_source, 1);
  if (_cast.HasInitLocalState()) {
    duckdb::CastLocalStateParameters parameters{context, _cast.GetCastData()};
    _state = _cast.InitLocalState(parameters);
  }
  SDB_ASSERT(!_reports || _state);
}

bool TextInput::Cast(duckdb::string_t text, std::string* message) {
  _result.ResetFromCache(_cache);
  duckdb::FlatVector::GetDataMutable<duckdb::string_t>(_source)[0] = text;
  if (_reports) {
    _state->Cast<InputState>().error.reset();
  }
  duckdb::CastParameters parameters{_cast.GetCastData(), false, message,
                                    _state.get()};
  return _cast.Cast(_source, _result, 1, parameters);
}

bool TextInput::Valid(duckdb::string_t text) {
  std::string message;
  return Cast(text, &message);
}

std::optional<irs::pg::SqlErrorData> TextInput::Error(duckdb::string_t text) {
  if (!_reports) {
    try {
      Cast(text, nullptr);
    } catch (const duckdb::Exception& exception) {
      const duckdb::ErrorData error{exception};
      if (error.Type() != duckdb::ExceptionType::CONVERSION) {
        throw;
      }
      auto data = SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                                 ERR_MSG(error.RawMessage()));
      if (auto it = error.ExtraInfo().find("detail");
          it != error.ExtraInfo().end()) {
        data.errdetail = it->second;
      }
      return data;
    }
    return std::nullopt;
  }
  std::string message;
  if (Cast(text, &message)) {
    return std::nullopt;
  }
  if (auto& error = _state->Cast<InputState>().error) {
    return std::move(*error);
  }
  return SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        ERR_MSG(message));
}

ParsedNumber ParseNumber(std::string_view text, size_t start) {
  const auto* begin = text.data();
  const auto* end = begin + text.size();
  const auto* p = begin + start;
  while (p != end && absl::ascii_isspace(static_cast<unsigned char>(*p))) {
    ++p;
  }
  const bool negative = p != end && *p == '-';
  if (p != end && (*p == '+' || *p == '-')) {
    ++p;
  }
  uint64_t magnitude = 0;
  const auto [digits_end, error] = fast_float::from_chars(p, end, magnitude);
  if (error == std::errc::invalid_argument) {
    return {
      .magnitude = 0, .negative = false, .out_of_range = false, .end = start};
  }
  return {.magnitude = magnitude,
          .negative = negative,
          .out_of_range = error == std::errc::result_out_of_range,
          .end = static_cast<size_t>(digits_end - begin)};
}

duckdb::unique_ptr<duckdb::FunctionLocalState> InitInput(
  duckdb::CastLocalStateParameters&) {
  return duckdb::make_uniq<InputState>();
}

void InputError(irs::pg::SqlErrorData&& error,
                duckdb::CastParameters& parameters) {
  if (!parameters.error_message) {
    THROW_SQL_ERROR_FROM_DATA(std::move(error));
  }
  duckdb::HandleCastError::AssignError(error.errmsg, parameters);
  if (!parameters.local_state) {
    return;
  }
  if (auto& state = parameters.local_state->Cast<InputState>(); !state.error) {
    state.error = std::move(error);
  }
}

void RegisterTextInput(duckdb::CastFunctionSet& casts,
                       const duckdb::LogicalType& type,
                       duckdb::cast_function_t input,
                       duckdb::init_cast_local_state_t init_local_state) {
  RegisterInputs(
    casts, type,
    [&] { return duckdb::BoundCastInfo{input, nullptr, init_local_state}; },
    [](const duckdb::LogicalType&) -> int64_t { return 50; });
}

void RegisterTextInput(duckdb::CastFunctionSet& casts,
                       const duckdb::LogicalType& type,
                       duckdb::bind_cast_function_t bind) {
  RegisterInputs(
    casts, type, [&] { return bind; },
    [](const duckdb::LogicalType&) -> int64_t { return 50; });
}

void ReplaceTextInput(duckdb::CastFunctionSet& casts,
                      const duckdb::LogicalType& type,
                      duckdb::cast_function_t input) {
  RegisterInputs(
    casts, type,
    [&] { return duckdb::BoundCastInfo{input, nullptr, InitInput}; },
    [&](const duckdb::LogicalType& source) {
      return casts.ImplicitCastCost(nullptr, source, type);
    });
}

duckdb::BoundCastInfo BindStorageCast(duckdb::BindCastInput& input,
                                      const duckdb::LogicalType& source,
                                      const duckdb::LogicalType& target,
                                      duckdb::cast_function_t check) {
  auto storage = target.WithAlias("");
  auto convert = input.GetCastFunction(source, storage);
  return {StorageCast,
          duckdb::make_uniq<StorageCastData>(std::move(storage),
                                             std::move(convert), check),
          InitStorageCast};
}

void RegisterStorageCasts(duckdb::CastFunctionSet& casts,
                          const duckdb::LogicalType& storage,
                          const duckdb::LogicalType& type,
                          duckdb::cast_function_t check,
                          duckdb::bind_cast_function_t bind) {
  casts.RegisterCastFunction(storage, type, check, -1);
  auto sources =
    storage.id() == duckdb::LogicalTypeId::LIST
      ? std::vector{duckdb::LogicalType::LIST(duckdb::LogicalType::ANY),
                    duckdb::LogicalType::ARRAY(duckdb::LogicalType::ANY,
                                               duckdb::optional_idx{})}
      : std::vector{
          duckdb::LogicalType::STRUCT({{"any", duckdb::LogicalType::ANY}}),
          duckdb::LogicalType::TUPLE({duckdb::LogicalType::ANY})};
  sources.push_back(duckdb::LogicalType::JSON());
  sources.push_back(duckdb::LogicalType::VARIANT());
  for (const auto& source : sources) {
    casts.RegisterCastFunction(source, type, bind, -1);
  }
}

}  // namespace sdb::pg
