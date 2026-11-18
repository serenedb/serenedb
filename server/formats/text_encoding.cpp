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

#include "formats/text_encoding.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <simdutf.h>

#include <array>
#include <bit>
#include <cstring>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <source_location>
#include <string>

namespace sdb::formats {
namespace {

constexpr std::array<std::string_view, 39> kOtherPgEncodings{
  "big5",         "euccn",    "eucjis2004", "eucjp",    "euckr",
  "euctw",        "gb18030",  "gbk",        "iso88595", "iso88596",
  "iso88597",     "iso88598", "johab",      "koi8r",    "koi8u",
  "latin2",       "latin3",   "latin4",     "latin5",   "latin6",
  "latin7",       "latin8",   "latin9",     "latin10",  "muleinternal",
  "shiftjis2004", "sjis",     "uhc",        "win1250",  "win1251",
  "win1252",      "win1253",  "win1254",    "win1255",  "win1256",
  "win1257",      "win1258",  "win866",     "win874",
};

size_t Utf8Length(uint8_t lead) noexcept {
  if (lead < 0x80) {
    return 1;
  }
  if (lead >= 0xF0 && lead < 0xF8) {
    return 4;
  }
  if (lead >= 0xE0) {
    return lead < 0xF0 ? 3 : 1;
  }
  return lead >= 0xC0 ? 2 : 1;
}

std::string HexBytes(std::string_view bytes) {
  std::string result;
  for (const auto byte : bytes) {
    absl::StrAppend(&result, result.empty() ? "" : " ",
                    absl::StrFormat("0x%02x", static_cast<uint8_t>(byte)));
  }
  return result;
}

[[noreturn]] void ThrowInvalid(TextEncoding encoding, std::string_view detail) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
                  ERR_MSG("invalid byte sequence for encoding \"",
                          TextEncodingName(encoding), "\": ", detail));
}

[[noreturn]] void ThrowUntranslatable(TextEncoding encoding,
                                      std::string_view utf8) {
  const auto valid = std::min(FindInvalidUtf8(utf8), utf8.size());
  for (size_t at = 0; at < valid;) {
    const auto length = Utf8Length(static_cast<uint8_t>(utf8[at]));
    if (length > 2 || (length == 2 && static_cast<uint8_t>(utf8[at]) > 0xC3)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNTRANSLATABLE_CHARACTER),
        ERR_MSG("character with byte sequence ",
                HexBytes(utf8.substr(at, length)),
                " in encoding \"UTF8\" has no equivalent in encoding \"",
                TextEncodingName(encoding), "\""));
    }
    at += length;
  }
  SDB_ASSERT(valid < utf8.size());
  std::rethrow_exception(InvalidUtf8Error(utf8, valid));
}

template<typename Unit>
std::basic_string<Unit> Units(std::string_view bytes, bool swap) {
  std::basic_string<Unit> units(bytes.size() / sizeof(Unit), Unit{});
  std::memcpy(units.data(), bytes.data(), units.size() * sizeof(Unit));
  if (swap) {
    for (auto& unit : units) {
      unit = std::byteswap(unit);
    }
  }
  return units;
}

template<typename Unit>
void AppendUnits(std::basic_string<Unit>& units, bool swap, std::string& out) {
  if (swap) {
    for (auto& unit : units) {
      unit = std::byteswap(unit);
    }
  }
  out.append(reinterpret_cast<const char*>(units.data()),
             units.size() * sizeof(Unit));
}

}  // namespace

TextEncoding ParseTextEncoding(std::string_view name) {
  std::string key;
  for (const auto c : name) {
    if (c != '-' && c != '_') {
      key.push_back(absl::ascii_tolower(c));
    }
  }
  if (key == "utf8" || key == "unicode" || key == "sqlascii") {
    return TextEncoding::Utf8;
  }
  if (key == "latin1" || key == "iso88591") {
    return TextEncoding::Latin1;
  }
  if (key == "utf16" || key == "utf16le") {
    return TextEncoding::Utf16Le;
  }
  if (key == "utf16be") {
    return TextEncoding::Utf16Be;
  }
  if (key == "utf32" || key == "utf32le") {
    return TextEncoding::Utf32Le;
  }
  if (key == "utf32be") {
    return TextEncoding::Utf32Be;
  }
  if (absl::c_linear_search(kOtherPgEncodings, key)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("COPY ENCODING \"", name, "\" is not supported"));
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
    ERR_MSG("argument to option \"encoding\" must be a valid encoding name"));
}

std::string_view TextEncodingName(TextEncoding encoding) {
  switch (encoding) {
    case TextEncoding::Utf8:
      return "UTF8";
    case TextEncoding::Latin1:
      return "LATIN1";
    case TextEncoding::Utf16Le:
      return "UTF16LE";
    case TextEncoding::Utf16Be:
      return "UTF16BE";
    case TextEncoding::Utf32Le:
      return "UTF32LE";
    case TextEncoding::Utf32Be:
      return "UTF32BE";
  }
  return {};
}

void Utf8Decoder::Decode(std::string_view input, std::string& out) {
  const auto old = out.size();
  switch (_encoding) {
    case TextEncoding::Utf8:
      out.append(input);
      return;
    case TextEncoding::Latin1:
      out.resize(old +
                 simdutf::utf8_length_from_latin1(input.data(), input.size()));
      std::ignore = simdutf::convert_latin1_to_utf8(input.data(), input.size(),
                                                    out.data() + old);
      return;
    case TextEncoding::Utf16Le:
    case TextEncoding::Utf16Be: {
      _carry.append(input);
      auto units = Units<char16_t>(_carry, _encoding == TextEncoding::Utf16Be);
      if (!units.empty() && units.back() >= 0xD800 && units.back() < 0xDC00) {
        units.pop_back();
      }
      out.resize(old +
                 simdutf::utf8_length_from_utf16le(units.data(), units.size()));
      const auto written = simdutf::convert_utf16le_to_utf8(
        units.data(), units.size(), out.data() + old);
      if (written == 0 && !units.empty()) {
        ThrowInvalid(_encoding, "invalid UTF-16 code unit");
      }
      out.resize(old + written);
      _carry.erase(0, units.size() * sizeof(char16_t));
      return;
    }
    case TextEncoding::Utf32Le:
    case TextEncoding::Utf32Be: {
      _carry.append(input);
      const auto units =
        Units<char32_t>(_carry, _encoding == TextEncoding::Utf32Be);
      out.resize(old +
                 simdutf::utf8_length_from_utf32(units.data(), units.size()));
      const auto written = simdutf::convert_utf32_to_utf8(
        units.data(), units.size(), out.data() + old);
      if (written == 0 && !units.empty()) {
        ThrowInvalid(_encoding, "invalid UTF-32 code point");
      }
      out.resize(old + written);
      _carry.erase(0, units.size() * sizeof(char32_t));
      return;
    }
  }
}

void Utf8Decoder::Finish() const {
  if (!_carry.empty()) {
    ThrowInvalid(_encoding, "incomplete character at the end of the input");
  }
}

void Utf8Encoder::Encode(std::string_view input, std::string& out) {
  std::string joined;
  if (!_carry.empty()) {
    joined = absl::StrCat(_carry, input);
    input = joined;
  }
  size_t end = input.size();
  size_t start = end;
  while (start != 0 && end - start < 4 &&
         (static_cast<uint8_t>(input[start - 1]) & 0xC0) == 0x80) {
    --start;
  }
  if (start != 0 &&
      Utf8Length(static_cast<uint8_t>(input[start - 1])) > end - start + 1) {
    end = start - 1;
  }
  const auto data = input.substr(0, end);
  _carry.assign(input.substr(end));
  switch (_encoding) {
    case TextEncoding::Utf8:
      out.append(data);
      return;
    case TextEncoding::Latin1: {
      const auto old = out.size();
      out.resize(old + data.size());
      const auto written = simdutf::convert_utf8_to_latin1(
        data.data(), data.size(), out.data() + old);
      if (written == 0 && !data.empty()) {
        ThrowUntranslatable(_encoding, data);
      }
      out.resize(old + written);
      return;
    }
    case TextEncoding::Utf16Le:
    case TextEncoding::Utf16Be: {
      std::u16string units(
        simdutf::utf16_length_from_utf8(data.data(), data.size()), u'\0');
      std::ignore = simdutf::convert_utf8_to_utf16le(data.data(), data.size(),
                                                     units.data());
      AppendUnits(units, _encoding == TextEncoding::Utf16Be, out);
      return;
    }
    case TextEncoding::Utf32Le:
    case TextEncoding::Utf32Be: {
      std::u32string units(
        simdutf::utf32_length_from_utf8(data.data(), data.size()), U'\0');
      std::ignore =
        simdutf::convert_utf8_to_utf32(data.data(), data.size(), units.data());
      AppendUnits(units, _encoding == TextEncoding::Utf32Be, out);
      return;
    }
  }
}

size_t FindInvalidUtf8(std::string_view bytes) noexcept {
  const auto result =
    simdutf::validate_utf8_with_errors(bytes.data(), bytes.size());
  return result.error == simdutf::error_code::SUCCESS ? std::string_view::npos
                                                      : result.count;
}

std::exception_ptr InvalidUtf8Error(std::string_view bytes, size_t offset) {
  const auto length = Utf8Length(static_cast<uint8_t>(bytes[offset]));
  return std::make_exception_ptr(irs::SqlException{
    SQL_ERROR_DATA(ERR_CODE(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
                   ERR_MSG("invalid byte sequence for encoding \"UTF8\": ",
                           HexBytes(bytes.substr(offset, length)))),
    std::source_location::current()});
}

}  // namespace sdb::formats
