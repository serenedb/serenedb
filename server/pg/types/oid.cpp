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

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <fast_float/fast_float.h>

#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>

#include "pg/types/cast.h"
#include "pg/types/logical.h"

namespace sdb::pg {
namespace {

bool IsSpace(char c) {
  return absl::ascii_isspace(static_cast<unsigned char>(c));
}

irs::pg::SqlErrorData OutOfRange(std::string_view text, std::string_view type) {
  return SQL_ERROR_DATA(
    ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
    ERR_MSG("value \"", text, "\" is out of range for type ", type));
}

std::optional<irs::pg::SqlErrorData> TryParseUnsigned(std::string_view text,
                                                      std::string_view type,
                                                      uint64_t& value) {
  const auto* p = text.data();
  const auto* end = p + text.size();
  while (p != end && IsSpace(*p)) {
    ++p;
  }
  const bool negative = p != end && *p == '-';
  if (p != end && (*p == '+' || *p == '-')) {
    ++p;
  }
  int base = 10;
  if (p != end && *p == '0') {
    base = 8;
    if (end - p > 2 && (p[1] == 'x' || p[1] == 'X') &&
        absl::ascii_isxdigit(static_cast<unsigned char>(p[2]))) {
      base = 16;
      p += 2;
    }
  }
  uint64_t magnitude = 0;
  const auto [digits_end, error] =
    fast_float::from_chars(p, end, magnitude, base);
  if (error == std::errc::invalid_argument) {
    return SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
      ERR_MSG("invalid input syntax for type ", type, ": \"", text, "\""));
  }
  if (error == std::errc::result_out_of_range) {
    return OutOfRange(text, type);
  }
  if (!absl::c_all_of(std::string_view{digits_end, end}, IsSpace)) {
    return SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
      ERR_MSG("invalid input syntax for type ", type, ": \"", text, "\""));
  }
  value = negative ? uint64_t{0} - magnitude : magnitude;
  return std::nullopt;
}

std::optional<uint32_t> Narrow(uint64_t value) {
  const auto narrow = static_cast<uint32_t>(value);
  if (value != narrow &&
      value != static_cast<uint64_t>(static_cast<int32_t>(narrow))) {
    return std::nullopt;
  }
  return narrow;
}

template<const std::string_view& Type>
std::optional<irs::pg::SqlErrorData> TryParse32(std::string_view text,
                                                int64_t& value) {
  uint64_t parsed = 0;
  if (auto error = TryParseUnsigned(text, Type, parsed)) {
    return error;
  }
  const auto narrow = Narrow(parsed);
  if (!narrow) {
    return OutOfRange(text, Type);
  }
  value = *narrow;
  return std::nullopt;
}

}  // namespace

std::optional<irs::pg::SqlErrorData> TryParseOid(std::string_view text,
                                                 int64_t& value) {
  uint64_t parsed = 0;
  if (auto error = TryParseUnsigned(text, kOidAlias, parsed)) {
    return error;
  }
  if (parsed <= std::numeric_limits<int64_t>::max()) {
    value = static_cast<int64_t>(parsed);
    return std::nullopt;
  }
  const auto narrow = Narrow(parsed);
  if (!narrow) {
    return OutOfRange(text, kOidAlias);
  }
  value = *narrow;
  return std::nullopt;
}

std::optional<irs::pg::SqlErrorData> TryParseXid(std::string_view text,
                                                 int64_t& value) {
  return TryParse32<kXidAlias>(text, value);
}

std::optional<irs::pg::SqlErrorData> TryParseCid(std::string_view text,
                                                 int64_t& value) {
  return TryParse32<kCidAlias>(text, value);
}

std::optional<irs::pg::SqlErrorData> TryParseXid8(std::string_view text,
                                                  uint64_t& value) {
  return TryParseUnsigned(text, kXid8Alias, value);
}

void RegisterOidTypes(duckdb::CastFunctionSet& casts) {
  ReplaceTextInput(casts, OID(), TextInputCast<int64_t, TryParseOid>);
  ReplaceTextInput(casts, XID(), TextInputCast<int64_t, TryParseXid>);
  ReplaceTextInput(casts, CID(), TextInputCast<int64_t, TryParseCid>);
  ReplaceTextInput(casts, XID8(), TextInputCast<uint64_t, TryParseXid8>);
}

}  // namespace sdb::pg
