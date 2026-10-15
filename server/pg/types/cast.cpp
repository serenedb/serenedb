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
#include <duckdb/common/operator/cast_operators.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <string>

#include "pg/types/logical.h"

namespace sdb::pg {
namespace {

bool ReportsInputErrors(const duckdb::LogicalType& type) {
  static constexpr std::array kAliases = {
    kRegprocAlias,      kRegprocedureAlias, kRegoperAlias,
    kRegoperatorAlias,  kRegclassAlias,     kRegtypeAlias,
    kRegcollationAlias, kRegconfigAlias,    kRegdictionaryAlias,
    kRegroleAlias,      kRegnamespaceAlias, kOidAlias,
    kTidAlias,          kPgLsnAlias,        kCidrAlias,
    kMacaddrAlias,      kMacaddr8Alias,     kInt2vectorAlias,
    kOidvectorAlias,
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
}

std::optional<irs::pg::SqlErrorData> TextInput::Error(duckdb::string_t text) {
  _result.ResetFromCache(_cache);
  duckdb::FlatVector::GetDataMutable<duckdb::string_t>(_source)[0] = text;
  std::string message;
  duckdb::CastParameters parameters{_cast.GetCastData(), false, &message,
                                    _state.get()};
  if (_cast.Cast(_source, _result, 1, parameters)) {
    return std::nullopt;
  }
  if (_reports) {
    if (auto& state = _state->Cast<InputState>(); state.error) {
      auto error = std::move(*state.error);
      state.error.reset();
      return error;
    }
  }
  return SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        ERR_MSG(message));
}

ParsedNumber ParseNumber(std::string_view text, size_t start, int base) {
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
  if (base == 0) {
    if (end - p > 2 && p[0] == '0' && (p[1] == 'x' || p[1] == 'X') &&
        absl::ascii_isxdigit(static_cast<unsigned char>(p[2]))) {
      base = 16;
      p += 2;
    } else {
      base = p != end && *p == '0' ? 8 : 10;
    }
  }
  uint64_t magnitude = 0;
  const auto [digits_end, error] =
    fast_float::from_chars(p, end, magnitude, base);
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
  for (const auto text : {duckdb::LogicalTypeId::VARCHAR,
                          duckdb::LogicalTypeId::STRING_LITERAL}) {
    casts.RegisterCastFunction(
      duckdb::LogicalType{text}, type,
      duckdb::BoundCastInfo{input, nullptr, init_local_state}, 50);
  }
}

void ReplaceTextInput(duckdb::CastFunctionSet& casts,
                      const duckdb::LogicalType& type,
                      duckdb::cast_function_t input) {
  for (const auto text : {duckdb::LogicalTypeId::VARCHAR,
                          duckdb::LogicalTypeId::STRING_LITERAL}) {
    const duckdb::LogicalType source{text};
    casts.RegisterCastFunction(source, type,
                               duckdb::BoundCastInfo{input, nullptr, InitInput},
                               casts.ImplicitCastCost(nullptr, source, type));
  }
}

}  // namespace sdb::pg
