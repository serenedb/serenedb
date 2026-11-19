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

#include <absl/strings/str_cat.h>
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
using sdb::formats::Utf8Decoder;
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

std::string Decode(TextEncoding encoding, std::string_view bytes,
                   size_t chunk) {
  Utf8Decoder decoder{encoding};
  std::string out;
  for (size_t at = 0; at < bytes.size(); at += chunk) {
    decoder.Decode(bytes.substr(at, chunk), out);
  }
  decoder.Finish();
  return out;
}

std::string DecodeSplit(TextEncoding encoding, std::string_view bytes,
                        size_t split) {
  Utf8Decoder decoder{encoding};
  std::string out;
  decoder.Decode(bytes.substr(0, split), out);
  decoder.Decode(bytes.substr(split), out);
  decoder.Finish();
  return out;
}

size_t UnitSize(TextEncoding encoding) {
  switch (encoding) {
    case TextEncoding::Utf16Le:
    case TextEncoding::Utf16Be:
      return 2;
    case TextEncoding::Utf32Le:
    case TextEncoding::Utf32Be:
      return 4;
    case TextEncoding::Utf8:
    case TextEncoding::Latin1:
      return 1;
  }
  return 1;
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

TEST(TextEncoding, DecodesInAnyChunking) {
  for (const auto& [encoding, encoded] : kEncoded) {
    for (size_t chunk = 1; chunk <= encoded.size(); ++chunk) {
      EXPECT_EQ(Decode(encoding, encoded, chunk), kText)
        << TextEncodingName(encoding) << " chunk " << chunk;
    }
  }
}

TEST(TextEncoding, DecodesSplitAtEveryOffset) {
  for (const auto& [encoding, encoded] : kEncoded) {
    const auto bytes = absl::StrCat(encoded, encoded, encoded);
    const auto text = absl::StrCat(kText, kText, kText);
    for (size_t split = 0; split <= bytes.size(); ++split) {
      EXPECT_EQ(DecodeSplit(encoding, bytes, split), text)
        << TextEncodingName(encoding) << " split " << split;
    }
  }
}

TEST(TextEncoding, DecodesMisalignedInput) {
  for (const auto& [encoding, encoded] : kEncoded) {
    const auto unit = encoded.substr(0, UnitSize(encoding));
    for (size_t lead = 0; lead < 6; ++lead) {
      std::string bytes;
      std::string text;
      for (size_t i = 0; i < lead; ++i) {
        absl::StrAppend(&bytes, unit);
        text.push_back('a');
      }
      for (size_t i = 0; i < 1000; ++i) {
        absl::StrAppend(&bytes, encoded);
        absl::StrAppend(&text, kText);
      }
      for (size_t shift = 0; shift < 4; ++shift) {
        const auto storage = absl::StrCat(std::string(shift, '\0'), bytes);
        const auto input = std::string_view{storage}.substr(shift);
        EXPECT_EQ(Decode(encoding, input, input.size()), text)
          << TextEncodingName(encoding) << " lead " << lead << " shift "
          << shift;
      }
    }
  }
}

TEST(TextEncoding, RefusesInvalidUnitsSplitAnywhere) {
  constexpr auto kUnpairedHigh = "\x61\x00\x3D\xD8\x62\x00\x63\x00"sv;
  constexpr auto kLoneLow = "\x00\x61\x00\x62\xDC\x00\x00\x63"sv;
  constexpr auto kSurrogate = "\x61\x00\x00\x00\x00\xD8\x00\x00"sv;
  for (size_t split = 0; split <= kUnpairedHigh.size(); ++split) {
    EXPECT_EQ(ErrorOf([&] {
                DecodeSplit(TextEncoding::Utf16Le, kUnpairedHigh, split);
              }),
              "invalid byte sequence for encoding \"UTF16LE\": invalid UTF-16 "
              "code unit")
      << split;
    EXPECT_EQ(
      ErrorOf([&] { DecodeSplit(TextEncoding::Utf16Be, kLoneLow, split); }),
      "invalid byte sequence for encoding \"UTF16BE\": invalid UTF-16 "
      "code unit")
      << split;
    EXPECT_EQ(
      ErrorOf([&] { DecodeSplit(TextEncoding::Utf32Le, kSurrogate, split); }),
      "invalid byte sequence for encoding \"UTF32LE\": invalid UTF-32 code "
      "point")
      << split;
  }
}

TEST(TextEncoding, DecodesUtf32UpToTheLastCodePoint) {
  constexpr auto kLast = "\xF4\x8F\xBF\xBF"sv;
  for (size_t chunk = 1; chunk <= 8; ++chunk) {
    EXPECT_EQ(Decode(TextEncoding::Utf32Le, "\xFF\xFF\x10\x00"sv, chunk), kLast)
      << chunk;
    EXPECT_EQ(Decode(TextEncoding::Utf32Be, "\x00\x10\xFF\xFF"sv, chunk), kLast)
      << chunk;
    EXPECT_EQ(ErrorOf([&] {
                Decode(TextEncoding::Utf32Le,
                       "\x61\x00\x00\x00\x00\x00\x11\x00"sv, chunk);
              }),
              "invalid byte sequence for encoding \"UTF32LE\": invalid UTF-32 "
              "code point")
      << chunk;
    EXPECT_EQ(ErrorOf([&] {
                Decode(TextEncoding::Utf32Be,
                       "\x00\x00\x00\x61\xFF\xFF\xFF\xFF"sv, chunk);
              }),
              "invalid byte sequence for encoding \"UTF32BE\": invalid UTF-32 "
              "code point")
      << chunk;
  }
}

TEST(TextEncoding, DecodesEveryLatin1Byte) {
  std::string latin1;
  std::string utf8;
  for (size_t byte = 0; byte < 256; ++byte) {
    latin1.push_back(static_cast<char>(byte));
    if (byte < 0x80) {
      utf8.push_back(static_cast<char>(byte));
    } else {
      utf8.push_back(static_cast<char>(0xC0 | (byte >> 6)));
      utf8.push_back(static_cast<char>(0x80 | (byte & 0x3F)));
    }
  }
  for (size_t chunk = 1; chunk <= latin1.size(); ++chunk) {
    EXPECT_EQ(Decode(TextEncoding::Latin1, latin1, chunk), utf8) << chunk;
  }
}

TEST(TextEncoding, Latin1InAnyChunking) {
  constexpr auto kUtf8 = "caf\xC3\xA9 na\xC3\xAFve \xC3\xBF"sv;
  constexpr auto kLatin1 = "caf\xE9 na\xEFve \xFF"sv;
  for (size_t chunk = 1; chunk <= kUtf8.size(); ++chunk) {
    EXPECT_EQ(Encode(TextEncoding::Latin1, kUtf8, chunk), kLatin1) << chunk;
  }
  for (size_t chunk = 1; chunk <= kLatin1.size(); ++chunk) {
    EXPECT_EQ(Decode(TextEncoding::Latin1, kLatin1, chunk), kUtf8) << chunk;
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

TEST(TextEncoding, RefusesInvalidUnits) {
  EXPECT_EQ(
    ErrorOf([] { Decode(TextEncoding::Utf16Le, "\x61\x00\x00\xDC"sv, 4); }),
    "invalid byte sequence for encoding \"UTF16LE\": invalid UTF-16 "
    "code unit");
  EXPECT_EQ(ErrorOf([] { Decode(TextEncoding::Utf16Be, "\xDC\x00"sv, 2); }),
            "invalid byte sequence for encoding \"UTF16BE\": invalid UTF-16 "
            "code unit");
  EXPECT_EQ(
    ErrorOf([] { Decode(TextEncoding::Utf32Le, "\x00\x00\x11\x00"sv, 4); }),
    "invalid byte sequence for encoding \"UTF32LE\": invalid UTF-32 code "
    "point");
  EXPECT_EQ(
    ErrorOf([] { Decode(TextEncoding::Utf32Be, "\x00\x00\xD8\x00"sv, 4); }),
    "invalid byte sequence for encoding \"UTF32BE\": invalid UTF-32 code "
    "point");
}

TEST(TextEncoding, RefusesAnIncompleteLastCharacter) {
  EXPECT_EQ(ErrorOf([] { Decode(TextEncoding::Utf16Le, "\x61\x00\x62"sv, 1); }),
            "invalid byte sequence for encoding \"UTF16LE\": incomplete "
            "character at the end of the input");
  EXPECT_EQ(ErrorOf([] { Decode(TextEncoding::Utf16Be, "\xD8\x3D"sv, 2); }),
            "invalid byte sequence for encoding \"UTF16BE\": incomplete "
            "character at the end of the input");
  EXPECT_EQ(
    ErrorOf([] { Decode(TextEncoding::Utf32Le, "\x61\x00\x00\x00\x62"sv, 3); }),
    "invalid byte sequence for encoding \"UTF32LE\": incomplete character at "
    "the end of the input");
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
