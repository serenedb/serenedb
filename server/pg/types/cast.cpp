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

#include <absl/strings/ascii.h>
#include <fast_float/fast_float.h>

#include <duckdb/common/operator/cast_operators.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

namespace sdb::pg {

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

void InputError(irs::pg::SqlErrorData&& error,
                duckdb::CastParameters& parameters) {
  if (!parameters.error_message) {
    THROW_SQL_ERROR_FROM_DATA(std::move(error));
  }
  duckdb::HandleCastError::AssignError(error.errmsg, parameters);
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
    casts.RegisterCastFunction(source, type, input,
                               casts.ImplicitCastCost(nullptr, source, type));
  }
}

}  // namespace sdb::pg
