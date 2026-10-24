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

#include "pg/encoding.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/utf8_utils.hpp>
#include <utf8proc_wrapper.hpp>

namespace sdb::pg {
namespace {

constexpr std::string_view kEncodings[] = {
  "SQL_ASCII",    "EUC_JP",         "EUC_CN",        "EUC_KR",     "EUC_TW",
  "EUC_JIS_2004", "UTF8",           "MULE_INTERNAL", "LATIN1",     "LATIN2",
  "LATIN3",       "LATIN4",         "LATIN5",        "LATIN6",     "LATIN7",
  "LATIN8",       "LATIN9",         "LATIN10",       "WIN1256",    "WIN1258",
  "WIN866",       "WIN874",         "KOI8R",         "WIN1251",    "WIN1252",
  "ISO_8859_5",   "ISO_8859_6",     "ISO_8859_7",    "ISO_8859_8", "WIN1250",
  "WIN1253",      "WIN1254",        "WIN1255",       "WIN1257",    "KOI8U",
  "SJIS",         "BIG5",           "GBK",           "UHC",        "GB18030",
  "JOHAB",        "SHIFT_JIS_2004",
};

struct EncodingAlias {
  std::string_view alias;
  int32_t encoding;
};

constexpr EncodingAlias kEncodingAliases[] = {
  {"abc", 19},         {"alt", 20},         {"big5", 36},
  {"euccn", 2},        {"eucjis2004", 5},   {"eucjp", 1},
  {"euckr", 3},        {"euctw", 4},        {"gb18030", 39},
  {"gbk", 37},         {"iso88591", 8},     {"iso885910", 13},
  {"iso885913", 14},   {"iso885914", 15},   {"iso885915", 16},
  {"iso885916", 17},   {"iso88592", 9},     {"iso88593", 10},
  {"iso88594", 11},    {"iso88595", 25},    {"iso88596", 26},
  {"iso88597", 27},    {"iso88598", 28},    {"iso88599", 12},
  {"johab", 40},       {"koi8", 22},        {"koi8r", 22},
  {"koi8u", 34},       {"latin1", 8},       {"latin10", 17},
  {"latin2", 9},       {"latin3", 10},      {"latin4", 11},
  {"latin5", 12},      {"latin6", 13},      {"latin7", 14},
  {"latin8", 15},      {"latin9", 16},      {"mskanji", 35},
  {"muleinternal", 7}, {"shiftjis", 35},    {"shiftjis2004", 41},
  {"sjis", 35},        {"sqlascii", 0},     {"tcvn", 19},
  {"tcvn5712", 19},    {"uhc", 38},         {"unicode", 6},
  {"utf8", 6},         {"vscii", 19},       {"win", 23},
  {"win1250", 29},     {"win1251", 23},     {"win1252", 24},
  {"win1253", 30},     {"win1254", 31},     {"win1255", 32},
  {"win1256", 18},     {"win1257", 33},     {"win1258", 19},
  {"win866", 20},      {"win874", 21},      {"win932", 35},
  {"win936", 37},      {"win949", 38},      {"win950", 36},
  {"windows1250", 29}, {"windows1251", 23}, {"windows1252", 24},
  {"windows1253", 30}, {"windows1254", 31}, {"windows1255", 32},
  {"windows1256", 18}, {"windows1257", 33}, {"windows1258", 19},
  {"windows866", 20},  {"windows874", 21},  {"windows932", 35},
  {"windows936", 37},  {"windows949", 38},  {"windows950", 36},
};

constexpr size_t kMaxNameLength = 63;
constexpr size_t kMaxReportedBytes = 8;
constexpr int32_t kKoi8uEncoding = 34;

bool IsSingleByte(int32_t encoding) {
  return encoding == kSqlAsciiEncoding ||
         (encoding >= kLatin1Encoding && encoding <= kKoi8uEncoding);
}

bool CanVerify(int32_t encoding) {
  return encoding == kUtf8Encoding || IsSingleByte(encoding);
}

size_t CharLength(int32_t encoding, unsigned char lead) {
  if (encoding != kUtf8Encoding) {
    return 1;
  }
  return std::max<size_t>(irs::utf8_utils::LengthFromChar8<0>(lead), 1);
}

std::string ByteSequence(int32_t encoding, std::string_view bytes) {
  const auto length = std::min(
    {CharLength(encoding, bytes.front()), bytes.size(), kMaxReportedBytes});
  std::string text;
  for (size_t i = 0; i < length; ++i) {
    absl::StrAppend(
      &text, i == 0 ? "" : " ", "0x",
      absl::Hex(static_cast<unsigned char>(bytes[i]), absl::kZeroPad2));
  }
  return text;
}

[[noreturn]] void ThrowUnsupported(int32_t from, int32_t to) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                  ERR_MSG("conversion between ", EncodingName(from), " and ",
                          EncodingName(to), " is not supported"));
}

void Verify(int32_t encoding, std::string_view bytes) {
  const auto end = std::min(bytes.find('\0'), bytes.size());
  const auto valid = encoding == kUtf8Encoding
                       ? duckdb::Utf8Proc::ValidPrefixLength(bytes.data(), end)
                       : end;
  if (valid != bytes.size()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
      ERR_MSG("invalid byte sequence for encoding \"", EncodingName(encoding),
              "\": ", ByteSequence(encoding, bytes.substr(valid))));
  }
}

std::string_view Utf8ToLatin1(std::string_view input, std::string& buffer) {
  buffer.clear();
  for (size_t i = 0; i < input.size();) {
    const auto lead = static_cast<unsigned char>(input[i]);
    const auto length = CharLength(kUtf8Encoding, lead);
    if (length == 2 && lead < 0xC4) {
      buffer.push_back(
        static_cast<char>(((lead & 0x1F) << 6) |
                          (static_cast<unsigned char>(input[i + 1]) & 0x3F)));
    } else if (length == 1) {
      buffer.push_back(input[i]);
    } else {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNTRANSLATABLE_CHARACTER),
        ERR_MSG("character with byte sequence ",
                ByteSequence(kUtf8Encoding, input.substr(i)),
                " in encoding \"UTF8\" has no equivalent in encoding "
                "\"LATIN1\""));
    }
    i += length;
  }
  return buffer;
}

std::string_view Latin1ToUtf8(std::string_view input, std::string& buffer) {
  buffer.clear();
  for (const auto c : input) {
    const auto byte = static_cast<unsigned char>(c);
    if (byte < 0x80) {
      buffer.push_back(c);
    } else {
      buffer.push_back(static_cast<char>(0xC0 | (byte >> 6)));
      buffer.push_back(static_cast<char>(0x80 | (byte & 0x3F)));
    }
  }
  return buffer;
}

}  // namespace

int32_t EncodingByName(std::string_view name) {
  std::array<char, kMaxNameLength> key{};
  size_t size = 0;
  for (const auto c : name.substr(0, kMaxNameLength)) {
    if (absl::ascii_isalnum(c)) {
      key[size++] = absl::ascii_tolower(c);
    }
  }
  const std::string_view cleaned{key.data(), size};
  const auto* it = absl::c_find_if(
    kEncodingAliases,
    [&](const EncodingAlias& alias) { return alias.alias == cleaned; });
  return it == std::end(kEncodingAliases) ? -1 : it->encoding;
}

std::string_view EncodingName(int32_t encoding) {
  if (encoding < 0 || static_cast<size_t>(encoding) >= std::size(kEncodings)) {
    return {};
  }
  return kEncodings[encoding];
}

std::string_view ConvertEncoding(std::string_view input, int32_t from,
                                 int32_t to, std::string& buffer) {
  if (input.empty()) {
    return input;
  }
  if (!CanVerify(from)) {
    ThrowUnsupported(from, to);
  }
  Verify(from, input);
  if (from == to || to == kSqlAsciiEncoding) {
    return input;
  }
  if (from == kSqlAsciiEncoding) {
    if (!CanVerify(to)) {
      ThrowUnsupported(from, to);
    }
    Verify(to, input);
    return input;
  }
  if (from == kUtf8Encoding && to == kLatin1Encoding) {
    return Utf8ToLatin1(input, buffer);
  }
  if (from == kLatin1Encoding && to == kUtf8Encoding) {
    return Latin1ToUtf8(input, buffer);
  }
  ThrowUnsupported(from, to);
}

}  // namespace sdb::pg
