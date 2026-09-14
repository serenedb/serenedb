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

#include "tokenizer_fuzz_corpus.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <iterator>

namespace tests::fuzz {
namespace {

constexpr std::string_view kAsciiWords[] = {"the",
                                            "quick",
                                            "brown",
                                            "fox",
                                            "jumps",
                                            "over",
                                            "lazy",
                                            "dog",
                                            "a",
                                            "an",
                                            "of",
                                            "and",
                                            "running",
                                            "runner",
                                            "ran",
                                            "stemming",
                                            "Berlin",
                                            "SereneDB",
                                            "x",
                                            "ab",
                                            "abc",
                                            "abcdefghi",
                                            "abcdefghijkl",
                                            "abcdefghijklm",
                                            "abcdefghijklmnopqrstuvwxyz"};

constexpr std::string_view kUtf8Words[] = {
  "привет",  "мир",     "ёжик",     "Straße",      "über", "café",
  "naïve",   "ωμέγα",   "ελληνικά", "İstanbul",    "ﬁnal", "ǅungla",
  "e\u0301", "a\u0308", "\u00e9",   "\u0065\u0301"};

constexpr std::string_view kWideWords[] = {
  "中文测试", "日本語", "テスト",  "한국어", "แบบทดสอบ",
  "العربية",  "עברית",  "देवनागरी", "ㄱㄴㄷ", "・。、"};

constexpr std::string_view kEmoji[] = {
  "😀", "🙈", "👨‍👩‍👧‍👦", "🇩🇪", "👍🏽", "🧑‍🚀",
  "❤️",  "0️⃣"};

constexpr std::string_view kBadUtf8[] = {
  "\xC3",         "\xE2\x82",         "\xF0\x9F\x92", "\x80",
  "\xBF",         "\xC0\xAF",         "\xE0\x80\xAF", "\xED\xA0\x80",
  "\xED\xBF\xBF", "\xF5\x80\x80\x80", "\xFE",         "\xFF",
  "\xC2",         "\xF4\x90\x80\x80"};

constexpr std::string_view kDelims[] = {
  ",", ";", "|", "\t", "\n", "\r", " ",  "/",  "\\", ":",
  ".", "-", "_", "=",  "::", "--", "<>", "\"", "'",  "(",
  ")", "[", "]", "{",  "}",  "#",  "@",  "&",  "+",  "*"};

std::string_view Pick(const std::string_view* table, size_t n, size_t i) {
  return table[i % n];
}

}  // namespace

std::string_view KindName(Kind kind) noexcept {
  switch (kind) {
    case Kind::Empty:
      return "empty";
    case Kind::AsciiWord:
      return "ascii_word";
    case Kind::AsciiSentence:
      return "ascii_sentence";
    case Kind::AsciiLong:
      return "ascii_long";
    case Kind::MixedCase:
      return "mixed_case";
    case Kind::Utf8Text:
      return "utf8_text";
    case Kind::Utf8Wide:
      return "utf8_wide";
    case Kind::Emoji:
      return "emoji";
    case Kind::InvalidUtf8:
      return "invalid_utf8";
    case Kind::Binary:
      return "binary";
    case Kind::DelimiterSoup:
      return "delimiter_soup";
    case Kind::Quoted:
      return "quoted";
    case Kind::PathLike:
      return "path_like";
    case Kind::Numeric:
      return "numeric";
    case Kind::Json:
      return "json";
    case Kind::Repeat:
      return "repeat";
    case Kind::Whitespace:
      return "whitespace";
    case Kind::ManyTokens:
      return "many_tokens";
    case Kind::HugeToken:
      return "huge_token";
    case Kind::Control:
      return "control";
  }
  return "unknown";
}

size_t ValueGen::Length(size_t soft_max) {
  static constexpr size_t kBoundaries[] = {
    0,   1,   2,    3,    4,    7,    8,    11,   12,   13,   15,  16,  17,
    31,  32,  33,   63,   64,   65,   127,  128,  129,  255,  256, 257, 511,
    512, 513, 1023, 1024, 1025, 2047, 2048, 2049, 4095, 4096, 4097};
  const auto cap = std::min(soft_max, _cap);
  if (Chance(2)) {
    return std::min(kBoundaries[Below(std::size(kBoundaries))], cap);
  }
  return Below(cap + 1);
}

char ValueGen::Letter() {
  static constexpr std::string_view kAlphabet =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  return kAlphabet[Below(kAlphabet.size())];
}

void ValueGen::AppendWord(std::string& out, size_t len) {
  for (size_t i = 0; i < len; ++i) {
    out.push_back(Letter());
  }
}

void ValueGen::AppendUtf8Word(std::string& out) {
  const auto roll = Below(3);
  if (roll == 0) {
    out +=
      Pick((kUtf8Words), std::size(kUtf8Words), Below(std::size(kUtf8Words)));
  } else if (roll == 1) {
    out +=
      Pick((kWideWords), std::size(kWideWords), Below(std::size(kWideWords)));
  } else {
    out += Pick((kEmoji), std::size(kEmoji), Below(std::size(kEmoji)));
  }
}

std::string ValueGen::Next() {
  return Next(static_cast<Kind>(Below(kKindCount)));
}

std::string ValueGen::Next(Kind kind) {
  std::string out;
  switch (kind) {
    case Kind::Empty:
      return out;
    case Kind::AsciiWord:
      AppendWord(out, 1 + Length(48));
      return out;
    case Kind::AsciiSentence: {
      const auto words = 1 + Below(24);
      for (size_t i = 0; i < words; ++i) {
        if (i != 0) {
          out += Pick((kDelims), std::size(kDelims), Below(std::size(kDelims)));
        }
        out += Pick((kAsciiWords), std::size(kAsciiWords),
                    Below(std::size(kAsciiWords)));
      }
      return out;
    }
    case Kind::AsciiLong: {
      const auto target =
        std::max<size_t>(std::min<size_t>(64, _cap), Length(_cap));
      while (out.size() < target) {
        out += Pick((kAsciiWords), std::size(kAsciiWords),
                    Below(std::size(kAsciiWords)));
        out.push_back(' ');
      }
      out.resize(target);
      return out;
    }
    case Kind::MixedCase: {
      AppendWord(out, 1 + Length(64));
      for (auto& c : out) {
        if (Chance(2)) {
          c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        }
      }
      return out;
    }
    case Kind::Utf8Text: {
      const auto words = 1 + Below(16);
      for (size_t i = 0; i < words; ++i) {
        if (i != 0) {
          out.push_back(' ');
        }
        AppendUtf8Word(out);
      }
      return out;
    }
    case Kind::Utf8Wide: {
      const auto words = 1 + Below(24);
      for (size_t i = 0; i < words; ++i) {
        out += Pick((kWideWords), std::size(kWideWords),
                    Below(std::size(kWideWords)));
      }
      return out;
    }
    case Kind::Emoji: {
      const auto n = 1 + Below(24);
      for (size_t i = 0; i < n; ++i) {
        out += Pick((kEmoji), std::size(kEmoji), Below(std::size(kEmoji)));
      }
      return out;
    }
    case Kind::InvalidUtf8: {
      const auto n = 1 + Below(24);
      for (size_t i = 0; i < n; ++i) {
        if (Chance(3)) {
          AppendWord(out, 1 + Below(6));
          continue;
        }
        if (Chance(3)) {
          AppendUtf8Word(out);
          continue;
        }
        out +=
          Pick((kBadUtf8), std::size(kBadUtf8), Below(std::size(kBadUtf8)));
      }
      return out;
    }
    case Kind::Binary: {
      const auto n = Length(1024);
      out.reserve(n);
      for (size_t i = 0; i < n; ++i) {
        out.push_back(static_cast<char>(Below(256)));
      }
      return out;
    }
    case Kind::DelimiterSoup: {
      const auto n = 1 + Length(96);
      for (size_t i = 0; i < n; ++i) {
        out += Pick((kDelims), std::size(kDelims), Below(std::size(kDelims)));
      }
      return out;
    }
    case Kind::Quoted: {
      const auto fields = 1 + Below(8);
      for (size_t i = 0; i < fields; ++i) {
        if (i != 0) {
          out.push_back(',');
        }
        if (Chance(2)) {
          out.push_back('"');
          AppendWord(out, Below(8));
          if (Chance(2)) {
            out += "\"\"";
          }
          if (Chance(3)) {
            out.push_back(',');
          }
          AppendWord(out, Below(8));
          if (!Chance(6)) {
            out.push_back('"');
          }
          continue;
        }
        AppendWord(out, Below(10));
      }
      return out;
    }
    case Kind::PathLike: {
      const auto segs = Below(10);
      if (Chance(2)) {
        out.push_back('/');
      }
      for (size_t i = 0; i < segs; ++i) {
        if (i != 0) {
          out += Chance(6) ? "//" : "/";
        }
        if (Chance(8)) {
          out += "..";
          continue;
        }
        AppendWord(out, Below(8));
      }
      if (Chance(3)) {
        out.push_back('/');
      }
      return out;
    }
    case Kind::Numeric: {
      static constexpr std::string_view kShapes[] = {"0",
                                                     "-0",
                                                     "1",
                                                     "-1",
                                                     "42",
                                                     "3.14159",
                                                     "-2.5e-9",
                                                     "1e308",
                                                     "NaN",
                                                     "inf",
                                                     "0x1f",
                                                     "007",
                                                     "9223372036854775807",
                                                     "-9223372036854775808",
                                                     "18446744073709551615",
                                                     "1.",
                                                     ".5",
                                                     "1_000"};
      out += kShapes[Below(std::size(kShapes))];
      if (Chance(3)) {
        out.push_back(',');
        out += kShapes[Below(std::size(kShapes))];
      }
      return out;
    }
    case Kind::Json: {
      static constexpr std::string_view kShapes[] = {
        R"({"a":1,"b":[1,2,3]})",
        R"({"location":{"lat":52.5,"lng":13.4}})",
        R"([1.0, 2.0])",
        R"([52.5])",
        R"({})",
        R"([])",
        R"({"type":"Point","coordinates":[13.4,52.5]})",
        R"({"type":"LineString","coordinates":[[0,0],[1,1]]})",
        R"({"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]})",
        R"({"type":"Bogus","coordinates":null})",
        R"({"a":)",
        R"({"a":"\ud800"})",
        "{oops",
        "null",
        "true"};
      out += kShapes[Below(std::size(kShapes))];
      return out;
    }
    case Kind::Repeat: {
      const auto n = 1 + Length(_cap);
      const char c = Chance(3) ? Letter()
                               : Pick((kDelims), std::size(kDelims),
                                      Below(std::size(kDelims)))[0];
      out.assign(n, c);
      return out;
    }
    case Kind::Whitespace:
      out.assign(1 + Length(64), ' ');
      for (auto& c : out) {
        if (Chance(4)) {
          c = "\t\n\r\v\f"[Below(5)];
        }
      }
      return out;
    case Kind::ManyTokens: {
      const auto want = 1024 + Below(2048);
      const auto words = std::max<size_t>(1, std::min<size_t>(want, _cap / 4));
      const auto delim = Pick((kDelims), std::size(kDelims), Below(2));
      for (size_t i = 0; i < words; ++i) {
        if (i != 0) {
          out += delim;
        }
        out.push_back(static_cast<char>('a' + (i % 26)));
        if (i % 7 == 0) {
          out += std::to_string(i % 97);
        }
      }
      return out;
    }
    case Kind::HugeToken:
      AppendWord(out, std::max<size_t>(1, std::min<size_t>(_cap, 2048)));
      return out;
    case Kind::Control: {
      const auto n = 1 + Length(96);
      for (size_t i = 0; i < n; ++i) {
        if (Chance(2)) {
          out.push_back(Letter());
          continue;
        }
        out.push_back(static_cast<char>(Below(0x20)));
      }
      return out;
    }
  }
  return out;
}

const std::vector<std::string>& EdgeCases() {
  static const std::vector<std::string> kValues = [] {
    std::vector<std::string> v = {
      "",
      " ",
      "  ",
      "\t",
      "\n",
      "\r\n",
      "a",
      "ab",
      "abc",
      "abcdefghijk",
      "abcdefghijkl",
      "abcdefghijklm",
      "abcdefghijklmn",
      "A",
      "AB",
      "ABCDEFGHIJKLM",
      "aBcDeFgHiJkLmNoP",
      ",",
      ",,",
      ",,,",
      "a,",
      ",a",
      ",a,",
      "a,,b",
      "a,b,c",
      "/",
      "//",
      "///",
      "/a",
      "a/",
      "/a/b/",
      "a//b",
      "..",
      "../..",
      "/../a/./b",
      "\"\"",
      "\"a\"",
      "\"a,b\"",
      "\"a\"\"b\"",
      "\"unterminated",
      "a\"b",
      "-",
      "--",
      "::",
      "a::b",
      "the",
      "the the the",
      "The Quick Brown Fox",
      "running runner ran runs",
      "привет мир",
      "ПРИВЕТ МИР",
      "Straße",
      "STRASSE",
      "café",
      "cafe\u0301",
      "中文测试",
      "日本語のテスト。二番目の文。",
      "แบบทดสอบ",
      "العربية والعربية",
      "😀😀😀",
      "👨‍👩‍👧‍👦",
      "a😀b",
      "\xC3",
      "\xE2\x82",
      "\xF0\x9F\x92",
      "\x80",
      "\xFF\xFE",
      "\xED\xA0\x80",
      "\xC0\xAF",
      "a\xC3z",
      "a\xFFz",
      "ipod",
      "i-pod",
      "i pod",
      "sea biscuit",
      "gb",
      "angry",
      "furious",
      "come",
      "baking",
      "Why not put knives in the dishwasher?",
      "[1.0, 2.0]",
      "[40.7, -73.9]",
      "[1.0]",
      "{}",
      "{oops",
      R"({"location":{"lat":52.5,"lng":13.4}})",
      R"({"type":"Point","coordinates":[13.4,52.5]})",
      R"({"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]})",
      "upper",
      "a b",
      "a  b",
      " a ",
      "1",
      "-1",
      "3.14",
      "0",
      "1e10",
      "not-wkb",
      "_",
      "__",
      "___",
      "_ _",
      "__/;[#]<>---@{|.",
      "a__b",
      "0_",
      "_0",
      "__\r",
      "..__..",
    };
    v.emplace_back(1, '\0');
    v.emplace_back("a\0b", 3);
    v.emplace_back("\0\0\0", 3);
    v.emplace_back(std::string(12, 'x'));
    v.emplace_back(std::string(13, 'x'));
    v.emplace_back(std::string(1023, 'y'));
    v.emplace_back(std::string(1024, 'y'));
    v.emplace_back(std::string(1025, 'y'));
    v.emplace_back(std::string(4096, 'z'));
    {
      std::string many;
      for (size_t i = 0; i < 2000; ++i) {
        if (i != 0) {
          many.push_back(',');
        }
        many += "t";
        many += std::to_string(i % 131);
      }
      v.push_back(std::move(many));
    }
    {
      std::string commas(2000, ',');
      v.push_back(std::move(commas));
    }
    {
      std::string spaced;
      for (size_t i = 0; i < 1500; ++i) {
        spaced += "w ";
      }
      v.push_back(std::move(spaced));
    }
    for (const auto& g : GeoJsonValues()) {
      v.push_back(g);
    }
    for (const auto& g : GeoWkbValues()) {
      v.push_back(g);
    }
    return v;
  }();
  return kValues;
}

namespace {

std::string PointWkb(double lng, double lat) {
  std::string wkb;
  const auto append = [&](const void* p, size_t n) {
    wkb.append(static_cast<const char*>(p), n);
  };
  const uint8_t little_endian = 1;
  const uint32_t point_type = 1;
  append(&little_endian, 1);
  append(&point_type, 4);
  append(&lng, 8);
  append(&lat, 8);
  return wkb;
}

std::string PolygonWkb() {
  std::string wkb;
  const auto append = [&](const void* p, size_t n) {
    wkb.append(static_cast<const char*>(p), n);
  };
  const uint8_t little_endian = 1;
  const uint32_t polygon_type = 3;
  const uint32_t nrings = 1;
  const uint32_t npoints = 5;
  append(&little_endian, 1);
  append(&polygon_type, 4);
  append(&nrings, 4);
  append(&npoints, 4);
  const double ring[5][2] = {
    {0.0, 0.0}, {0.0, 1.0}, {1.0, 1.0}, {1.0, 0.0}, {0.0, 0.0}};
  for (const auto& p : ring) {
    append(&p[0], 8);
    append(&p[1], 8);
  }
  return wkb;
}

}  // namespace

const std::vector<std::string>& GeoJsonValues() {
  static const std::vector<std::string> kValues = {
    R"({"type":"Point","coordinates":[13.4,52.5]})",
    R"({"type":"Point","coordinates":[-73.9,40.7]})",
    R"({"type":"Point","coordinates":[0,0]})",
    R"({"type":"Point","coordinates":[181.0,91.0]})",
    R"({"type":"Point","coordinates":[]})",
    R"({"type":"LineString","coordinates":[[0,0],[1,1],[2,2]]})",
    R"({"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]})",
    R"({"type":"MultiPoint","coordinates":[[0,0],[1,1]]})",
    R"({"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1]]]})",
    R"({"type":"GeometryCollection","geometries":[]})",
    R"({"type":"Point"})",
    R"({"coordinates":[1,2]})",
    R"({"type":42,"coordinates":[1,2]})",
    "[1.0, 2.0]",
    "[40.7, -73.9]",
    "[91.0, 181.0]",
    "[1.0]",
    "[]",
    "{}",
    "{oops",
    "",
    R"({"location":{"lat":52.5,"lng":13.4}})",
    R"({"location":{"lat":52.5}})",
    R"({"location":[52.5,13.4]})",
  };
  return kValues;
}

const std::vector<std::string>& GeoWkbValues() {
  static const std::vector<std::string> kValues = [] {
    std::vector<std::string> v = {PointWkb(2.0, 1.0),
                                  PointWkb(-73.9, 40.7),
                                  PointWkb(0.0, 0.0),
                                  PointWkb(181.0, 91.0),
                                  PolygonWkb(),
                                  "not-wkb",
                                  ""};
    v.push_back(PointWkb(13.4, 52.5).substr(0, 8));
    std::string bad_endian = PointWkb(1.0, 2.0);
    bad_endian[0] = static_cast<char>(0x7F);
    v.push_back(std::move(bad_endian));
    return v;
  }();
  return kValues;
}

const std::vector<std::string>& PathValues() {
  static const std::vector<std::string> kValues = {
    "/usr/local/bin",
    "usr/local/bin",
    "/usr//local///bin",
    "/",
    "//",
    "a",
    "/a",
    "a/",
    "/a/b/c/d/e/f/g",
    "..",
    "/../a",
    "a/../b",
    "C:\\Windows\\x",
    "\\\\srv\\share",
    "a::b::c",
    "",
    "///",
    "/a/b/",
  };
  return kValues;
}

const std::vector<std::string>& CsvValues() {
  static const std::vector<std::string> kValues = {
    "a,b,c",
    "a,,c",
    ",a,",
    ",,,",
    "\"a,b\",c",
    "\"a\"\"b\",c",
    "\"unterminated,a",
    "a\"b,c",
    "\"\",\"\"",
    "a",
    "",
    "\"a\nb\",c",
    "a;b|c\td",
    "a::b::c",
  };
  return kValues;
}

const std::vector<std::string>& WordValues() {
  static const std::vector<std::string> kValues = {
    "the quick brown fox jumps over the lazy dog",
    "running runner ran runs runnable",
    "The Quick Brown FOX",
    "a an the of and",
    "ipod i-pod i pod",
    "sea biscuit seabiscuit",
    "angry furious mad",
    "happy glad",
    "come",
    "gb gigabyte",
    "hello world",
    "привет мир как дела",
    "по вечерам ежик ходил к медвежонку считать звезды",
    "中文测试 日本語のテスト",
    "one",
    "",
  };
  return kValues;
}

std::vector<std::string> MakeCorpus(uint64_t seed, size_t count,
                                    const std::vector<std::string>& seeded,
                                    size_t size_cap) {
  std::vector<std::string> out;
  out.reserve(EdgeCases().size() + seeded.size() + count);
  for (const auto& v : EdgeCases()) {
    out.push_back(v);
  }
  for (const auto& v : seeded) {
    out.push_back(v);
  }
  ValueGen gen{seed, size_cap};
  for (size_t i = 0; i < count; ++i) {
    out.push_back(gen.Next());
  }
  return out;
}

uint64_t EnvU64(const char* name, uint64_t fallback) {
  const char* v = std::getenv(name);
  if (v == nullptr || *v == '\0') {
    return fallback;
  }
  return std::strtoull(v, nullptr, 10);
}

std::string Describe(std::string_view value) {
  std::string out;
  out += "size=";
  out += std::to_string(value.size());
  out += " bytes=[";
  const auto shown = std::min<size_t>(value.size(), 64);
  static constexpr char kHex[] = "0123456789abcdef";
  for (size_t i = 0; i < shown; ++i) {
    const auto c = static_cast<unsigned char>(value[i]);
    if (c >= 0x20 && c < 0x7F && c != '\\') {
      out.push_back(static_cast<char>(c));
      continue;
    }
    out += "\\x";
    out.push_back(kHex[c >> 4]);
    out.push_back(kHex[c & 0xF]);
  }
  if (shown != value.size()) {
    out += "...";
  }
  out.push_back(']');
  return out;
}

}  // namespace tests::fuzz
