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

#include <gtest/gtest.h>

#include <cstddef>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "formats/text_encoding.h"

namespace {

using namespace std::string_view_literals;
using sdb::formats::ParseTextEncoding;
using sdb::formats::TextEncoding;
using sdb::formats::TextEncodingName;
using sdb::formats::Utf8Encoder;

constexpr auto kText = "a\xC3\xA9\xE2\x82\xAC\xF0\x9F\x98\x80z"sv;

const std::vector<std::pair<TextEncoding, std::string_view>> kEncoded{
  {TextEncoding::Utf8, kText},
  {TextEncoding::Utf16Le, "\x61\x00\xE9\x00\xAC\x20\x3D\xD8\x00\xDE\x7A\x00"sv},
  {TextEncoding::Utf16Be, "\x00\x61\x00\xE9\x20\xAC\xD8\x3D\xDE\x00\x00\x7A"sv},
  {TextEncoding::Utf32Le,
   "\x61\x00\x00\x00\xE9\x00\x00\x00\xAC\x20\x00\x00\x00\xF6\x01\x00"
   "\x7A\x00\x00\x00"sv},
  {TextEncoding::Utf32Be,
   "\x00\x00\x00\x61\x00\x00\x00\xE9\x00\x00\x20\xAC\x00\x01\xF6\x00"
   "\x00\x00\x00\x7A"sv},
};

std::string Encode(TextEncoding encoding, std::string_view utf8, size_t chunk) {
  Utf8Encoder encoder{encoding};
  std::string out;
  for (size_t at = 0; at < utf8.size(); at += chunk) {
    encoder.Encode(utf8.substr(at, chunk), out);
  }
  return out;
}

template<typename F>
std::string ErrorOf(F&& f) {
  try {
    f();
  } catch (const irs::SqlException& e) {
    return e.message();
  }
  return "no error";
}

TEST(TextEncoding, EncodesInAnyChunking) {
  for (const auto& [encoding, encoded] : kEncoded) {
    for (size_t chunk = 1; chunk <= kText.size(); ++chunk) {
      EXPECT_EQ(Encode(encoding, kText, chunk), encoded)
        << TextEncodingName(encoding) << " chunk " << chunk;
    }
  }
}

TEST(TextEncoding, Latin1InAnyChunking) {
  constexpr auto kUtf8 = "caf\xC3\xA9 na\xC3\xAFve \xC3\xBF"sv;
  constexpr auto kLatin1 = "caf\xE9 na\xEFve \xFF"sv;
  for (size_t chunk = 1; chunk <= kUtf8.size(); ++chunk) {
    EXPECT_EQ(Encode(TextEncoding::Latin1, kUtf8, chunk), kLatin1) << chunk;
  }
}

TEST(TextEncoding, NamesTheCharacterLatin1CannotHold) {
  EXPECT_EQ(
    ErrorOf([] { Encode(TextEncoding::Latin1, "a\xC3\xA9\xE2\x82\xAC", 64); }),
    "character with byte sequence 0xe2 0x82 0xac in encoding \"UTF8\" "
    "has no equivalent in encoding \"LATIN1\"");
  EXPECT_EQ(
    ErrorOf([] { Encode(TextEncoding::Latin1, "\xC3\xBF\xC4\x80", 64); }),
    "character with byte sequence 0xc4 0x80 in encoding \"UTF8\" has "
    "no equivalent in encoding \"LATIN1\"");
  EXPECT_EQ(
    ErrorOf([] { Encode(TextEncoding::Latin1, "\xF0\x9F\x98\x80", 1); }),
    "character with byte sequence 0xf0 0x9f 0x98 0x80 in encoding "
    "\"UTF8\" has no equivalent in encoding \"LATIN1\"");
  EXPECT_EQ(ErrorOf([] { Encode(TextEncoding::Latin1, "ab\x80", 64); }),
            "invalid byte sequence for encoding \"UTF8\": 0x80");
}

TEST(TextEncoding, ParsesPostgresNames) {
  const std::vector<std::pair<std::string_view, TextEncoding>> names{
    {"UTF8", TextEncoding::Utf8},        {"utf-8", TextEncoding::Utf8},
    {"Unicode", TextEncoding::Utf8},     {"SQL_ASCII", TextEncoding::Utf8},
    {"LATIN1", TextEncoding::Latin1},    {"ISO_8859_1", TextEncoding::Latin1},
    {"UTF16", TextEncoding::Utf16Le},    {"utf-16le", TextEncoding::Utf16Le},
    {"UTF-16BE", TextEncoding::Utf16Be}, {"utf32", TextEncoding::Utf32Le},
    {"UTF32LE", TextEncoding::Utf32Le},  {"utf_32_be", TextEncoding::Utf32Be},
  };
  for (const auto& [name, encoding] : names) {
    EXPECT_EQ(ParseTextEncoding(name), encoding) << name;
  }
  EXPECT_EQ(ErrorOf([] { ParseTextEncoding("WIN1252"); }),
            "COPY ENCODING \"WIN1252\" is not supported");
  EXPECT_EQ(ErrorOf([] { ParseTextEncoding("nope"); }),
            "argument to option \"encoding\" must be a valid encoding name");
}

TEST(TextEncoding, NamesEveryEncoding) {
  EXPECT_EQ(TextEncodingName(TextEncoding::Utf8), "UTF8");
  EXPECT_EQ(TextEncodingName(TextEncoding::Latin1), "LATIN1");
  EXPECT_EQ(TextEncodingName(TextEncoding::Utf16Le), "UTF16LE");
  EXPECT_EQ(TextEncodingName(TextEncoding::Utf16Be), "UTF16BE");
  EXPECT_EQ(TextEncodingName(TextEncoding::Utf32Le), "UTF32LE");
  EXPECT_EQ(TextEncodingName(TextEncoding::Utf32Be), "UTF32BE");
}

}  // namespace
