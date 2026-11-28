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

#include "pg/input_error.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <fast_float/fast_float.h>

#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/error_mapping.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <source_location>
#include <string>

#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/functions/pg_catalog/regproc.h"
#include "pg/integer_text.h"
#include "pg/types/logical.h"
#include "pg/types/mapping.h"

namespace sdb::pg {
namespace {

template<typename... Args>
std::exception_ptr Error(int code, Args&&... message) {
  return std::make_exception_ptr(irs::SqlException{
    SQL_ERROR_DATA(ERR_CODE(code), ERR_MSG(std::forward<Args>(message)...)),
    std::source_location::current()});
}

std::exception_ptr DecimalOverflow(const duckdb::LogicalType& type) {
  const auto width = duckdb::DecimalType::GetWidth(type);
  const auto scale = duckdb::DecimalType::GetScale(type);
  const auto digits = width - scale;
  return std::make_exception_ptr(irs::SqlException{
    SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
      ERR_MSG("numeric field overflow"),
      ERR_DETAIL("A field with precision ", width, ", scale ", scale,
                 " must round to an absolute value less than ",
                 digits != 0 ? absl::StrCat("10^", digits) : "1", ".")),
    std::source_location::current()});
}

std::string TypeName(duckdb::ClientContext& context,
                     const duckdb::LogicalType& type) {
  const auto oid = Type2Oid(type);
  if (const auto* builtin = FindBuiltinType(oid)) {
    if (BuiltinLogicalType(*builtin).id() != type.id()) {
      return absl::AsciiStrToLower(type.ToString());
    }
    if (const auto name = SqlTypeName(static_cast<int32_t>(oid));
        !name.empty()) {
      return std::string{name};
    }
  }
  std::string name;
  const auto format = [&] {
    name = AsView(FormatTypeOut(MakeSession(&context), oid, std::nullopt));
  };
  if (context.transaction.HasActiveTransaction()) {
    format();
  } else {
    context.RunFunctionInTransaction(format);
  }
  return name;
}

std::string_view DateTimeTypeName(duckdb::LogicalTypeId type) {
  switch (type) {
    case duckdb::LogicalTypeId::DATE:
      return "date";
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_SEC:
    case duckdb::LogicalTypeId::TIMESTAMP_MS:
    case duckdb::LogicalTypeId::TIMESTAMP_NS:
      return "timestamp";
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      return "timestamp with time zone";
    case duckdb::LogicalTypeId::TIME:
      return "time";
    case duckdb::LogicalTypeId::TIME_TZ:
      return "time with time zone";
    case duckdb::LogicalTypeId::INTERVAL:
      return "interval";
    default:
      return {};
  }
}

bool Digits(std::string_view text, char separator) {
  return !text.empty() && absl::c_all_of(text, [&](char c) {
    return absl::ascii_isdigit(c) || c == separator;
  });
}

bool MonthOutOfRange(std::string_view date) {
  const auto first = date.find('-');
  const auto month =
    date.substr(first + 1, date.find('-', first + 1) - first - 1);
  uint32_t value = 0;
  const auto [_, ec] =
    fast_float::from_chars(month.data(), month.data() + month.size(), value);
  return ec != std::errc{} || value < 1 || value > 12;
}

std::string_view Unsigned(std::string_view text) {
  if (text.starts_with('-') || text.starts_with('+')) {
    text.remove_prefix(1);
  }
  return text;
}

bool Number(std::string_view text) {
  text = Unsigned(text);
  const auto exponent = text.find_first_of("eE");
  if (exponent != std::string_view::npos &&
      !Digits(Unsigned(text.substr(exponent + 1)), '_')) {
    return false;
  }
  text = text.substr(0, exponent);
  const auto point = text.find('.');
  if (point == std::string_view::npos) {
    return Digits(text, '_');
  }
  const auto whole = text.substr(0, point);
  const auto fraction = text.substr(point + 1);
  return (!whole.empty() || !fraction.empty()) &&
         (whole.empty() || Digits(whole, '_')) &&
         (fraction.empty() || Digits(fraction, '_'));
}

bool IntegerOutOfRange(const duckdb::LogicalType& type, std::string_view text) {
  const auto range = [&]<typename T>() {
    T value;
    return ParseIntegerText(text, value) == IntegerText::Range;
  };
  switch (type.InternalType()) {
    case duckdb::PhysicalType::INT8:
      return range.template operator()<int8_t>();
    case duckdb::PhysicalType::INT16:
      return range.template operator()<int16_t>();
    case duckdb::PhysicalType::INT32:
      return range.template operator()<int32_t>();
    case duckdb::PhysicalType::INT64:
      return range.template operator()<int64_t>();
    case duckdb::PhysicalType::UINT8:
      return range.template operator()<uint8_t>();
    case duckdb::PhysicalType::UINT16:
      return range.template operator()<uint16_t>();
    case duckdb::PhysicalType::UINT32:
      return range.template operator()<uint32_t>();
    case duckdb::PhysicalType::UINT64:
      return range.template operator()<uint64_t>();
    default:
      return Digits(Unsigned(absl::StripAsciiWhitespace(text)), '_');
  }
}

}  // namespace

std::exception_ptr InputError(duckdb::ClientContext& context,
                              const duckdb::LogicalType& type,
                              std::string_view value) {
  const auto trimmed = absl::StripAsciiWhitespace(value);
  if (const auto datetime = DateTimeTypeName(type.id()); !datetime.empty()) {
    if (Digits(trimmed, '-') && absl::c_count(trimmed, '-') == 2) {
      if (MonthOutOfRange(trimmed)) {
        return std::make_exception_ptr(irs::SqlException{
          SQL_ERROR_DATA(
            ERR_CODE(ERRCODE_DATETIME_FIELD_OVERFLOW),
            ERR_MSG("date/time field value out of range: \"", value, "\""),
            ERR_HINT("Perhaps you need a different \"DateStyle\" setting.")),
          std::source_location::current()});
      }
      return Error(ERRCODE_DATETIME_FIELD_OVERFLOW,
                   "date/time field value out of range: \"", value, "\"");
    }
    return Error(ERRCODE_INVALID_DATETIME_FORMAT,
                 "invalid input syntax for type ", datetime, ": \"", value,
                 "\"");
  }
  if (type.id() == duckdb::LogicalTypeId::BLOB && trimmed.starts_with("\\x")) {
    const auto hex = trimmed.substr(2);
    if (const auto bad =
          absl::c_find_if(hex, [](char c) { return !absl::ascii_isxdigit(c); });
        bad != hex.end()) {
      return Error(ERRCODE_INVALID_PARAMETER_VALUE,
                   "invalid hexadecimal digit: \"",
                   hex.substr(static_cast<size_t>(bad - hex.begin()), 1), "\"");
    }
    return Error(ERRCODE_INVALID_PARAMETER_VALUE,
                 "invalid hexadecimal data: odd number of digits");
  }
  const auto decimal = type.id() == duckdb::LogicalTypeId::DECIMAL;
  if (decimal && !IsMoney(type) && Number(trimmed)) {
    return DecimalOverflow(type);
  }
  const auto name = TypeName(context, type);
  if ((decimal && Number(trimmed)) ||
      (type.IsIntegral() && IntegerOutOfRange(type, value))) {
    return Error(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, "value \"", value,
                 "\" is out of range for type ", name);
  }
  return Error(ERRCODE_INVALID_TEXT_REPRESENTATION,
               "invalid input syntax for type ", name, ": \"", value, "\"");
}

void ThrowInContext(const std::exception_ptr& error, std::string context) {
  irs::pg::SqlErrorData data;
  try {
    std::rethrow_exception(error);
  } catch (const std::exception& e) {
    data = irs::pg::ToSqlError(e);
  }
  data.context = std::move(context);
  THROW_SQL_ERROR_FROM_DATA(std::move(data));
}

}  // namespace sdb::pg
