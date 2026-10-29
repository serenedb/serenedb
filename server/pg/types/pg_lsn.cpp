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

#include "pg/types/pg_lsn.h"

#include <absl/strings/str_format.h>
#include <fast_float/fast_float.h>

#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/function/function_binder.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "pg/types/cast.h"
#include "pg/types/logical.h"

namespace sdb::pg {
namespace {

constexpr size_t kLsnPartDigits = 8;

std::optional<uint32_t> ParseLsnPart(std::string_view digits) {
  if (digits.empty() || digits.size() > kLsnPartDigits) {
    return std::nullopt;
  }
  uint32_t value = 0;
  const auto* end = digits.data() + digits.size();
  const auto [ptr, ec] = fast_float::from_chars(digits.data(), end, value, 16);
  if (ec != std::errc{} || ptr != end) {
    return std::nullopt;
  }
  return value;
}

std::optional<irs::pg::SqlErrorData> TryParseLsn(std::string_view text,
                                                 uint64_t& lsn) {
  const auto parsed = ParseLsn(text);
  if (!parsed) {
    return SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
      ERR_MSG("invalid input syntax for type pg_lsn: \"", text, "\""));
  }
  lsn = *parsed;
  return std::nullopt;
}

bool LsnToText(duckdb::Vector& source, duckdb::Vector& result,
               duckdb::idx_t count, duckdb::CastParameters&) {
  duckdb::UnaryExecutor::Execute<uint64_t, duckdb::string_t>(
    source, result, count, [&](uint64_t lsn) {
      char buffer[kLsnBufferSize];
      return duckdb::StringVector::AddString(result, buffer,
                                             WriteLsn(lsn, buffer));
    });
  return true;
}

duckdb::unique_ptr<duckdb::Expression> BindDecimalOperator(
  duckdb::FunctionBindExpressionInput& input, std::string_view name) {
  for (auto& child : input.children) {
    if (child->GetReturnType() == PG_LSN()) {
      child = duckdb::BoundCastExpression::AddCastToType(
        input.context, std::move(child), duckdb::LogicalType::DECIMAL(20, 0));
    }
  }
  return duckdb::FunctionBinder{input.context}.BindScalarFunction(
    duckdb::Identifier{DEFAULT_SCHEMA}, duckdb::Identifier{name},
    std::move(input.children), true);
}

duckdb::unique_ptr<duckdb::Expression> LsnDifference(
  duckdb::FunctionBindExpressionInput& input) {
  return BindDecimalOperator(input, "-");
}

template<char kOperator>
duckdb::unique_ptr<duckdb::Expression> LsnOffset(
  duckdb::FunctionBindExpressionInput& input) {
  static constexpr char kName[] = {kOperator, '\0'};
  return duckdb::BoundCastExpression::AddCastToType(
    input.context, BindDecimalOperator(input, kName), PG_LSN());
}

void RegisterFunctions(Functions& functions) {
  const auto lsn = PG_LSN();
  const duckdb::LogicalType offset{duckdb::LogicalType::BIGINT};
  const auto add = [&](std::string_view name,
                       duckdb::vector<duckdb::LogicalType> arguments,
                       duckdb::LogicalType result,
                       duckdb::function_bind_expression_t rewrite) {
    duckdb::ScalarFunction function{std::move(arguments), std::move(result),
                                    nullptr};
    function.SetBindExpressionCallback(rewrite);
    Add(functions, name, std::move(function));
  };
  add("-", {lsn, lsn}, duckdb::LogicalType::DECIMAL(21, 0), LsnDifference);
  add("+", {lsn, offset}, lsn, LsnOffset<'+'>);
  add("+", {offset, lsn}, lsn, LsnOffset<'+'>);
  add("-", {lsn, offset}, lsn, LsnOffset<'-'>);
}

}  // namespace

std::optional<uint64_t> ParseLsn(std::string_view text) {
  const auto slash = text.find('/');
  if (slash == std::string_view::npos) {
    return std::nullopt;
  }
  const auto high = ParseLsnPart(text.substr(0, slash));
  const auto low = ParseLsnPart(text.substr(slash + 1));
  if (!high || !low) {
    return std::nullopt;
  }
  return (static_cast<uint64_t>(*high) << 32) | *low;
}

size_t WriteLsn(uint64_t lsn, char* buffer) {
  return static_cast<size_t>(absl::SNPrintF(buffer, kLsnBufferSize, "%X/%X",
                                            static_cast<uint32_t>(lsn >> 32),
                                            static_cast<uint32_t>(lsn)));
}

std::string FormatLsn(uint64_t lsn) {
  char buffer[kLsnBufferSize];
  return {buffer, WriteLsn(lsn, buffer)};
}

void RegisterPgLsn(duckdb::CastFunctionSet& casts, Functions& functions) {
  RegisterTextInput(casts, PG_LSN(), TextInputCast<uint64_t, TryParseLsn>,
                    InitInput);
  casts.RegisterCastFunction(PG_LSN(), duckdb::LogicalType::VARCHAR, LsnToText,
                             50);
  RegisterFunctions(functions);
}

}  // namespace sdb::pg
