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

#include <duckdb/common/allocator.hpp>
#include <duckdb/storage/arena_allocator.hpp>
#include <iresearch/utils/json_leaf.hpp>
#include <iterator>
#include <random>
#include <string>
#include <string_view>

namespace {

using irs::JsonLeafKind;
using irs::JsonLeafReader;

class JsonLeafReaderTest : public ::testing::Test {
 protected:
  duckdb::ArenaAllocator _strings{duckdb::Allocator::DefaultAllocator()};
  JsonLeafReader _reader{_strings};
};

TEST_F(JsonLeafReaderTest, plain_string_is_a_view_of_the_input) {
  const std::string json = R"("quick brown fox")";
  const auto leaf = _reader.Read(json);
  ASSERT_EQ(JsonLeafKind::String, leaf.kind);
  EXPECT_EQ("quick brown fox", leaf.string);
  EXPECT_EQ(json.data() + 1, leaf.string.data());
}

TEST_F(JsonLeafReaderTest, escaped_and_padded_strings_are_decoded) {
  struct Case {
    std::string_view json;
    std::string_view text;
  };
  for (const auto& [json, text] : {
         Case{R"("the \"quick\" fox")", R"(the "quick" fox)"},
         Case{R"("a\\b")", R"(a\b)"},
         Case{R"("line\nbreak")", "line\nbreak"},
         Case{"\"caf\\u00e9\"", "caf\xC3\xA9"},
         Case{"\"\\ud83e\\udd8a\"", "\xF0\x9F\xA6\x8A"},
         Case{R"("")", ""},
         Case{" \t\"padded\"\r\n", "padded"},
       }) {
    const auto leaf = _reader.Read(json);
    ASSERT_EQ(JsonLeafKind::String, leaf.kind) << json;
    EXPECT_EQ(text, leaf.string) << json;
  }
}

TEST_F(JsonLeafReaderTest, decoded_strings_live_until_the_arena_resets) {
  const std::string longer(4096, 'y');
  const auto first = _reader.Read(R"("internationalization\n")");
  ASSERT_EQ(JsonLeafKind::Composite, _reader.Read(" [1, 2] ").kind);
  const auto second = _reader.Read(R"("zzzzzzzzzzzzzzzzzzzz\n")");
  ASSERT_EQ(JsonLeafKind::Number, _reader.Read("12345.5").kind);
  const auto third = _reader.Read("\"" + longer + "\\n\"");
  ASSERT_EQ(JsonLeafKind::String, first.kind);
  ASSERT_EQ(JsonLeafKind::String, second.kind);
  ASSERT_EQ(JsonLeafKind::String, third.kind);
  EXPECT_EQ("internationalization\n", first.string);
  EXPECT_EQ("zzzzzzzzzzzzzzzzzzzz\n", second.string);
  EXPECT_EQ(longer + "\n", third.string);
  _strings.Reset();
  const auto after = _reader.Read(R"("cleared\t")");
  ASSERT_EQ(JsonLeafKind::String, after.kind);
  EXPECT_EQ("cleared\t", after.string);
}

TEST_F(JsonLeafReaderTest, scalars) {
  auto leaf = _reader.Read("42");
  ASSERT_EQ(JsonLeafKind::Number, leaf.kind);
  EXPECT_DOUBLE_EQ(42, leaf.number);
  leaf = _reader.Read(" -1.5e3 ");
  ASSERT_EQ(JsonLeafKind::Number, leaf.kind);
  EXPECT_DOUBLE_EQ(-1500, leaf.number);
  leaf = _reader.Read("true");
  ASSERT_EQ(JsonLeafKind::Bool, leaf.kind);
  EXPECT_TRUE(leaf.boolean);
  leaf = _reader.Read("false");
  ASSERT_EQ(JsonLeafKind::Bool, leaf.kind);
  EXPECT_FALSE(leaf.boolean);
  EXPECT_EQ(JsonLeafKind::Null, _reader.Read("null").kind);
}

TEST_F(JsonLeafReaderTest, composites) {
  for (const std::string_view json :
       {R"({"title":"quick"})", R"(["quick"])", "[]", "{}", " [1, 2] "}) {
    EXPECT_EQ(JsonLeafKind::Composite, _reader.Read(json).kind) << json;
  }
}

TEST_F(JsonLeafReaderTest, malformed_is_none) {
  for (const std::string_view json :
       {"", " ", "\"", R"("open)", R"("abc\)", "1 2", "tru"}) {
    EXPECT_EQ(JsonLeafKind::None, _reader.Read(json).kind) << json;
  }
}

TEST_F(JsonLeafReaderTest, escaped_closing_quote_stays_in_bounds) {
  auto leaf = _reader.Read(R"("a\")");
  ASSERT_EQ(JsonLeafKind::String, leaf.kind);
  EXPECT_EQ(R"(a")", leaf.string);
  leaf = _reader.Read(R"("a\\\")");
  ASSERT_EQ(JsonLeafKind::String, leaf.kind);
  EXPECT_EQ(R"(a\")", leaf.string);
}

TEST_F(JsonLeafReaderTest, escaped_strings_agree_with_simdjson) {
  constexpr std::string_view kPieces[] = {
    "a",       "quick", " ",     "\xC3\xA9", "\xF0\x9F\xA6\x8A",
    R"(\")",   R"(\\)", R"(\n)", R"(\/)",    R"(\t)",
    R"(\b)",   R"(\f)", R"(\r)", "\\u00e9",  "\\ud83e\\udd8a",
    "\\u0000", "'",     "{",     "]",        ",",
  };
  constexpr std::string_view kSpaces[] = {"", " ", "\t", "\r\n", " \n "};
  std::mt19937 rng{20261015};
  simdjson::ondemand::parser parser;
  for (int i = 0; i < 10000; ++i) {
    std::string json{kSpaces[i % std::size(kSpaces)]};
    json += '"';
    for (auto n = std::uniform_int_distribution<int>{1, 12}(rng); n != 0; --n) {
      json += kPieces[std::uniform_int_distribution<size_t>{
        0, std::size(kPieces) - 1}(rng)];
    }
    json += '"';
    json += kSpaces[(i / std::size(kSpaces)) % std::size(kSpaces)];
    const simdjson::padded_string padded{json};
    std::string_view expected;
    ASSERT_EQ(simdjson::SUCCESS,
              parser.iterate(padded).get_string().get(expected))
      << json;
    const auto leaf = _reader.Read(json);
    ASSERT_EQ(JsonLeafKind::String, leaf.kind) << json;
    EXPECT_EQ(expected, leaf.string) << json;
  }
}

TEST_F(JsonLeafReaderTest, view_agrees_with_parser) {
  constexpr std::string_view kPieces[] = {
    "a",     "quick", " ",     "\xC3\xA9", "\xF0\x9F\xA6\x8A",
    R"(\")", R"(\\)", R"(\n)", R"(\/)",    "\\u00e9",
    "'",     "{",     "]",     ":",        ",",
  };
  std::mt19937 rng{20261011};
  for (int i = 0; i < 10000; ++i) {
    std::string json = "\"";
    for (auto n = std::uniform_int_distribution<int>{0, 8}(rng); n != 0; --n) {
      json += kPieces[std::uniform_int_distribution<size_t>{
        0, std::size(kPieces) - 1}(rng)];
    }
    json += '"';
    const auto direct = _reader.Read(json);
    ASSERT_EQ(JsonLeafKind::String, direct.kind) << json;
    const std::string text{direct.string};
    const std::string padded = " " + json;
    const auto parsed = _reader.Read(padded);
    ASSERT_EQ(JsonLeafKind::String, parsed.kind) << json;
    EXPECT_EQ(text, parsed.string) << json;
  }
}

}  // namespace
