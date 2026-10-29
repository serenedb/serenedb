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

#include "pg/types/vector.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>

#include <cstdint>
#include <duckdb/common/operator/cast_operators.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_error.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "pg/types/cast.h"
#include "pg/types/logical.h"

namespace sdb::pg {
namespace {

std::optional<irs::pg::SqlErrorData> TryParseInt2At(std::string_view text,
                                                    size_t& pos,
                                                    int16_t& value) {
  const auto rest = text.substr(pos);
  const auto syntax_error = [&] {
    return SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
      ERR_MSG("invalid input syntax for type smallint: \"", rest, "\""));
  };
  const auto number = ParseNumber(text, pos);
  if (number.end == pos) {
    return syntax_error();
  }
  const uint64_t limit = number.negative ? 32768 : 32767;
  if (number.out_of_range || number.magnitude > limit) {
    return SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
      ERR_MSG("value \"", rest, "\" is out of range for type smallint"));
  }
  if (number.end != text.size() && text[number.end] != ' ') {
    return syntax_error();
  }
  const auto magnitude = static_cast<int32_t>(number.magnitude);
  value = static_cast<int16_t>(number.negative ? -magnitude : magnitude);
  pos = number.end;
  return std::nullopt;
}

std::optional<irs::pg::SqlErrorData> TryParseOidAt(std::string_view text,
                                                   size_t& pos,
                                                   int64_t& value) {
  auto end = pos;
  while (end != text.size() &&
         !absl::ascii_isspace(static_cast<unsigned char>(text[end]))) {
    ++end;
  }
  const auto token = text.substr(pos, end - pos);
  if (!duckdb::TryCast::Operation<duckdb::string_t, int64_t>(
        duckdb::string_t{token.data(), static_cast<uint32_t>(token.size())},
        value)) {
    return SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
      ERR_MSG("invalid input syntax for type oid: \"", token, "\""));
  }
  pos = end;
  return std::nullopt;
}

template<typename T, auto ParseAt>
std::optional<irs::pg::SqlErrorData> ParseVector(std::string_view text,
                                                 std::vector<T>& values) {
  values.clear();
  for (size_t pos = 0;;) {
    while (pos != text.size() &&
           absl::ascii_isspace(static_cast<unsigned char>(text[pos]))) {
      ++pos;
    }
    if (pos == text.size()) {
      return std::nullopt;
    }
    T value{};
    if (auto error = ParseAt(text, pos, value)) {
      return error;
    }
    values.push_back(value);
  }
}

template<typename T, auto Parse>
bool TextToVector(duckdb::Vector& source, duckdb::Vector& result,
                  duckdb::idx_t count, duckdb::CastParameters& parameters) {
  std::vector<T> values;
  bool converted = true;
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::list_entry_t>(
    source, result, count,
    [&](duckdb::string_t raw) -> std::optional<duckdb::list_entry_t> {
      if (auto error = Parse({raw.GetData(), raw.GetSize()}, values)) {
        InputError(std::move(*error), parameters);
        converted = false;
        return std::nullopt;
      }
      const auto offset = duckdb::ListVector::GetListSize(result);
      duckdb::ListVector::Reserve(result, offset + values.size());
      absl::c_copy(values, duckdb::FlatVector::GetDataMutable<T>(
                             duckdb::ListVector::GetChildMutable(result)) +
                             offset);
      duckdb::ListVector::SetListSize(result, offset + values.size());
      return duckdb::list_entry_t{offset, values.size()};
    });
  return converted;
}

template<typename T>
bool VectorToText(duckdb::Vector& source, duckdb::Vector& result,
                  duckdb::idx_t count, duckdb::CastParameters&) {
  const auto elements = duckdb::ListVector::GetChild(source).Values<T>();
  std::string text;
  duckdb::UnaryExecutor::Execute<duckdb::list_entry_t, duckdb::string_t>(
    source, result, count, [&](duckdb::list_entry_t list) {
      text.clear();
      for (duckdb::idx_t i = 0; i < list.length; ++i) {
        absl::StrAppend(&text, i == 0 ? "" : " ",
                        elements[list.offset + i].GetValueUnsafe());
      }
      return duckdb::StringVector::AddString(result, text);
    });
  return true;
}

}  // namespace

std::optional<irs::pg::SqlErrorData> ParseInt2Vector(
  std::string_view text, std::vector<int16_t>& values) {
  return ParseVector<int16_t, TryParseInt2At>(text, values);
}

std::optional<irs::pg::SqlErrorData> ParseOidVector(
  std::string_view text, std::vector<int64_t>& values) {
  return ParseVector<int64_t, TryParseOidAt>(text, values);
}

void RegisterVectors(duckdb::CastFunctionSet& casts) {
  ReplaceTextInput(casts, INT2VECTOR(), TextToVector<int16_t, ParseInt2Vector>);
  ReplaceTextInput(casts, OIDVECTOR(), TextToVector<int64_t, ParseOidVector>);
  casts.RegisterCastFunction(INT2VECTOR(), duckdb::LogicalType::VARCHAR,
                             VectorToText<int16_t>);
  casts.RegisterCastFunction(OIDVECTOR(), duckdb::LogicalType::VARCHAR,
                             VectorToText<int64_t>);
}

}  // namespace sdb::pg
