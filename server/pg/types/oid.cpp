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

#include "pg/types/oid.h"

#include <absl/strings/ascii.h>

#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "pg/types/cast.h"
#include "pg/types/logical.h"

namespace sdb::pg {
namespace {

irs::pg::SqlErrorData SyntaxError(std::string_view text) {
  return SQL_ERROR_DATA(
    ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
    ERR_MSG("invalid input syntax for type oid: \"", text, "\""));
}

irs::pg::SqlErrorData RangeError(std::string_view text) {
  return SQL_ERROR_DATA(
    ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
    ERR_MSG("value \"", text, "\" is out of range for type oid"));
}

std::optional<irs::pg::SqlErrorData> ParseOid(std::string_view text,
                                              size_t& pos, bool whole,
                                              int64_t& oid) {
  const auto rest = text.substr(pos);
  const auto number = ParseNumber(text, pos, 0);
  if (number.end == pos) {
    return SyntaxError(rest);
  }
  if (number.out_of_range) {
    return RangeError(rest);
  }
  auto end = number.end;
  if (whole) {
    while (end != text.size() &&
           absl::ascii_isspace(static_cast<unsigned char>(text[end]))) {
      ++end;
    }
    if (end != text.size()) {
      return SyntaxError(rest);
    }
  }
  const auto value =
    number.negative ? uint64_t{0} - number.magnitude : number.magnitude;
  const auto narrowed = static_cast<uint32_t>(value);
  if (value != narrowed &&
      value != static_cast<uint64_t>(static_cast<int32_t>(narrowed))) {
    return RangeError(rest);
  }
  oid = narrowed;
  pos = end;
  return std::nullopt;
}

}  // namespace

std::optional<irs::pg::SqlErrorData> TryParseOid(std::string_view text,
                                                 int64_t& oid) {
  size_t pos = 0;
  return ParseOid(text, pos, true, oid);
}

std::optional<irs::pg::SqlErrorData> TryParseOidAt(std::string_view text,
                                                   size_t& pos, int64_t& oid) {
  return ParseOid(text, pos, false, oid);
}

void RegisterOid(duckdb::CastFunctionSet& casts) {
  ReplaceTextInput(casts, OID(), TextInput<int64_t, TryParseOid>);
}

}  // namespace sdb::pg
