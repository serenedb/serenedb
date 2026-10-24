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

#include "pg/types/mac.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>

#include <array>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/function/cast/cast_function_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_error.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <string>

#include "pg/types/cast.h"
#include "pg/types/logical.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kMacaddrMask = 0xFFFF'FFFF'FFFF;
constexpr uint64_t kEui48Marker = 0xFFFE;
constexpr std::string_view kHexDigits = "0123456789abcdef";

irs::pg::SqlErrorData SyntaxError(std::string_view type,
                                  std::string_view text) {
  return SQL_ERROR_DATA(
    ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
    ERR_MSG("invalid input syntax for type ", type, ": \"", text, "\""));
}

int HexValue(char c) {
  if (c >= '0' && c <= '9') {
    return c - '0';
  }
  const auto lower = absl::ascii_tolower(static_cast<unsigned char>(c));
  return lower >= 'a' && lower <= 'f' ? lower - 'a' + 10 : -1;
}

bool ScanHex(const char*& p, const char* end, size_t width, int64_t& value) {
  while (p != end && absl::ascii_isspace(static_cast<unsigned char>(*p))) {
    ++p;
  }
  const auto* limit =
    width == 0 || static_cast<size_t>(end - p) < width ? end : p + width;
  const auto* start = p;
  bool negative = false;
  if (p != limit && (*p == '+' || *p == '-')) {
    negative = *p == '-';
    ++p;
  }
  if (limit - p > 2 && p[0] == '0' && (p[1] == 'x' || p[1] == 'X') &&
      HexValue(p[2]) >= 0) {
    p += 2;
  }
  int64_t parsed = 0;
  const auto* digits = p;
  for (int digit; p != limit && (digit = HexValue(*p)) >= 0; ++p) {
    parsed = parsed > 0xFFFF ? parsed : parsed * 16 + digit;
  }
  if (p == digits) {
    p = start;
    return false;
  }
  value = negative ? -parsed : parsed;
  return true;
}

constexpr std::array<std::string_view, 7> kMacaddrShapes = {
  "x:x:x:x:x:x", "x-x-x-x-x-x", "XXX:XXX", "XXX-XXX",
  "XX.XX.XX",    "XX-XX-XX",    "XXXXXX",
};

bool MatchShape(std::string_view text, std::string_view shape,
                std::array<int64_t, 6>& octets) {
  const auto* p = text.data();
  const auto* end = text.data() + text.size();
  size_t octet = 0;
  for (const char c : shape) {
    if (c == 'x' || c == 'X') {
      if (!ScanHex(p, end, c == 'X' ? 2 : 0, octets[octet++])) {
        return false;
      }
    } else if (p == end || *p++ != c) {
      return false;
    }
  }
  while (p != end && absl::ascii_isspace(static_cast<unsigned char>(*p))) {
    ++p;
  }
  return p == end;
}

}  // namespace

std::optional<irs::pg::SqlErrorData> TryParseMacaddr(std::string_view text,
                                                     uint64_t& mac) {
  std::array<int64_t, 6> octets{};
  if (!absl::c_any_of(kMacaddrShapes, [&](std::string_view shape) {
        return MatchShape(text, shape, octets);
      })) {
    return SyntaxError("macaddr", text);
  }
  mac = 0;
  for (const auto octet : octets) {
    if (octet < 0 || octet > 255) {
      return SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
        ERR_MSG("invalid octet value in \"macaddr\" value: \"", text, "\""));
    }
    mac = (mac << 8) | static_cast<uint64_t>(octet);
  }
  return std::nullopt;
}

std::optional<irs::pg::SqlErrorData> TryParseMacaddr8(std::string_view text,
                                                      uint64_t& mac) {
  size_t i = 0;
  while (i < text.size() &&
         absl::ascii_isspace(static_cast<unsigned char>(text[i]))) {
    ++i;
  }
  uint64_t value = 0;
  size_t count = 0;
  char spacer = '\0';
  while (i + 1 < text.size()) {
    if (++count > 8) {
      return SyntaxError("macaddr8", text);
    }
    const auto high = HexValue(text[i]);
    const auto low = HexValue(text[i + 1]);
    if (high < 0 || low < 0) {
      return SyntaxError("macaddr8", text);
    }
    value = (value << 8) | static_cast<uint64_t>(high * 16 + low);
    i += 2;
    if (i < text.size() &&
        (text[i] == ':' || text[i] == '-' || text[i] == '.')) {
      if (spacer == '\0') {
        spacer = text[i];
      } else if (spacer != text[i]) {
        return SyntaxError("macaddr8", text);
      }
      ++i;
    }
    if ((count == 6 || count == 8) && i < text.size() &&
        absl::ascii_isspace(static_cast<unsigned char>(text[i]))) {
      while (i < text.size() &&
             absl::ascii_isspace(static_cast<unsigned char>(text[i]))) {
        ++i;
      }
      if (i < text.size()) {
        return SyntaxError("macaddr8", text);
      }
    }
  }
  if (count == 6) {
    value = ((value >> 24) << 40) | (kEui48Marker << 24) | (value & 0xFFFFFF);
  } else if (count != 8) {
    return SyntaxError("macaddr8", text);
  }
  mac = value;
  return std::nullopt;
}

namespace {

size_t WriteHexBytes(uint64_t value, size_t bytes, char* buffer) {
  size_t size = 0;
  for (size_t i = bytes; i-- > 0;) {
    const auto byte = (value >> (8 * i)) & 0xFF;
    buffer[size++] = kHexDigits[byte >> 4];
    buffer[size++] = kHexDigits[byte & 0xF];
    if (i != 0) {
      buffer[size++] = ':';
    }
  }
  return size;
}

uint64_t LoadBigEndian(std::string_view bytes) {
  uint64_t value = 0;
  for (const char byte : bytes) {
    value = (value << 8) | static_cast<uint8_t>(byte);
  }
  return value;
}

uint64_t Macaddr8FromMacaddr(uint64_t mac) {
  return ((mac >> 24) << 40) | (kEui48Marker << 24) | (mac & 0xFFFFFF);
}

uint64_t MacaddrFromMacaddr8(uint64_t mac) {
  if (((mac >> 24) & 0xFFFF) != kEui48Marker) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
      ERR_MSG("macaddr8 data out of range to convert to macaddr"),
      ERR_HINT("Only addresses that have FF and FE as values in the 4th and "
               "5th bytes from the left, for example xx:xx:xx:ff:fe:xx:xx:xx, "
               "are eligible to be converted from macaddr8 to macaddr."));
  }
  return ((mac >> 40) << 24) | (mac & 0xFFFFFF);
}

template<bool kMac8>
bool MacToText(duckdb::Vector& source, duckdb::Vector& result,
               duckdb::idx_t count, duckdb::CastParameters&) {
  duckdb::UnaryExecutor::Execute<uint64_t, duckdb::string_t>(
    source, result, count, [&](uint64_t mac) {
      char buffer[kMacaddr8Text];
      const auto size =
        kMac8 ? WriteMacaddr8(mac, buffer) : WriteMacaddr(mac, buffer);
      return duckdb::StringVector::AddString(result, buffer, size);
    });
  return true;
}

template<auto Convert>
bool MacConvert(duckdb::Vector& source, duckdb::Vector& result,
                duckdb::idx_t count, duckdb::CastParameters&) {
  duckdb::UnaryExecutor::Execute<uint64_t, uint64_t>(source, result, count,
                                                     Convert);
  return true;
}

void RegisterCasts(duckdb::CastFunctionSet& casts) {
  RegisterTextInput(casts, MACADDR(), TextInputCast<uint64_t, TryParseMacaddr>,
                    InitInput);
  RegisterTextInput(casts, MACADDR8(),
                    TextInputCast<uint64_t, TryParseMacaddr8>, InitInput);
  casts.RegisterCastFunction(MACADDR(), duckdb::LogicalType::VARCHAR,
                             MacToText<false>, 50);
  casts.RegisterCastFunction(MACADDR8(), duckdb::LogicalType::VARCHAR,
                             MacToText<true>, 50);
  casts.RegisterCastFunction(MACADDR(), MACADDR8(),
                             MacConvert<&Macaddr8FromMacaddr>, 1);
  casts.RegisterCastFunction(MACADDR8(), MACADDR(),
                             MacConvert<&MacaddrFromMacaddr8>, 1);
}

void RegisterFunctions(Functions& functions) {
  const auto mac = MACADDR();
  const auto mac8 = MACADDR8();
  Add(functions, "trunc",
      {{mac}, mac, UnaryExecute<uint64_t, uint64_t, [](uint64_t v) -> uint64_t {
         return v & 0xFFFF'FF00'0000;
       }>});
  Add(
    functions, "trunc",
    {{mac8}, mac8, UnaryExecute<uint64_t, uint64_t, [](uint64_t v) -> uint64_t {
       return v & 0xFFFF'FF00'0000'0000;
     }>});
  Add(
    functions, "macaddr8_set7bit",
    {{mac8}, mac8, UnaryExecute<uint64_t, uint64_t, [](uint64_t v) -> uint64_t {
       return v | (uint64_t{0x02} << 56);
     }>});
  Add(functions, "macaddr",
      Fallible(
        {{mac8}, mac, UnaryExecute<uint64_t, uint64_t, &MacaddrFromMacaddr8>}));
  Add(functions, "macaddr8",
      {{mac}, mac8, UnaryExecute<uint64_t, uint64_t, &Macaddr8FromMacaddr>});
  Add(functions, "~",
      {{mac}, mac, UnaryExecute<uint64_t, uint64_t, [](uint64_t v) -> uint64_t {
         return ~v & kMacaddrMask;
       }>});
  Add(
    functions, "~",
    {{mac8}, mac8, UnaryExecute<uint64_t, uint64_t, [](uint64_t v) -> uint64_t {
       return ~v;
     }>});
  for (const auto& type : {mac, mac8}) {
    Add(functions, "&",
        {{type, type},
         type,
         BinaryExecute<uint64_t, uint64_t, uint64_t,
                       [](uint64_t l, uint64_t r) -> uint64_t {
                         return l & r;
                       }>});
    Add(functions, "|",
        {{type, type},
         type,
         BinaryExecute<uint64_t, uint64_t, uint64_t,
                       [](uint64_t l, uint64_t r) -> uint64_t {
                         return l | r;
                       }>});
  }
}

}  // namespace

size_t WriteMacaddr(uint64_t mac, char* buffer) {
  return WriteHexBytes(mac, 6, buffer);
}

size_t WriteMacaddr8(uint64_t mac, char* buffer) {
  return WriteHexBytes(mac, 8, buffer);
}

std::optional<uint64_t> MacaddrRecv(std::string_view bytes) {
  if (bytes.size() != kMacaddrBytes) {
    return std::nullopt;
  }
  return LoadBigEndian(bytes);
}

std::optional<uint64_t> Macaddr8Recv(std::string_view bytes) {
  if (bytes.size() == kMacaddrBytes) {
    return Macaddr8FromMacaddr(LoadBigEndian(bytes));
  }
  if (bytes.size() != kMacaddr8Bytes) {
    return std::nullopt;
  }
  return LoadBigEndian(bytes);
}

void MacSend(uint64_t mac, size_t bytes, uint8_t* buffer) {
  for (size_t i = 0; i < bytes; ++i) {
    buffer[i] = static_cast<uint8_t>(mac >> (8 * (bytes - 1 - i)));
  }
}

void RegisterMacaddr(duckdb::CastFunctionSet& casts, Functions& functions) {
  RegisterCasts(casts);
  RegisterFunctions(functions);
}

}  // namespace sdb::pg
