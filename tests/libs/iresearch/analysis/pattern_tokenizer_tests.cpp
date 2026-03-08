////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <vpack/common.h>
#include <vpack/parser.h>

#include "gtest/gtest.h"
#include "iresearch/analysis/pattern_tokenizer.hpp"

namespace {

class PatternTokenizerTests : public ::testing::Test {};

}  // namespace

TEST_F(PatternTokenizerTests, consts) {
  static_assert("pattern" ==
                irs::Type<irs::analysis::PatternTokenizer>::name());
}

TEST_F(PatternTokenizerTests, test_split_mode) {
  std::string_view data("foo,bar,baz");
  irs::analysis::PatternTokenizer stream(",", -1);
  ASSERT_EQ(irs::Type<irs::analysis::PatternTokenizer>::id(), stream.type());

  ASSERT_TRUE(stream.reset(data));

  auto* offset = irs::get<irs::OffsAttr>(stream);
  auto* term = irs::get<irs::TermAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(3, offset->end);
  ASSERT_EQ("foo", irs::ViewCast<char>(term->value));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(4, offset->start);
  ASSERT_EQ(7, offset->end);
  ASSERT_EQ("bar", irs::ViewCast<char>(term->value));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(8, offset->start);
  ASSERT_EQ(11, offset->end);
  ASSERT_EQ("baz", irs::ViewCast<char>(term->value));

  ASSERT_FALSE(stream.next());
}

TEST_F(PatternTokenizerTests, test_split_whitespace) {
  std::string_view data("hello world test");
  irs::analysis::PatternTokenizer stream("\\s+", -1);

  ASSERT_TRUE(stream.reset(data));

  auto* offset = irs::get<irs::OffsAttr>(stream);
  auto* term = irs::get<irs::TermAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(5, offset->end);
  ASSERT_EQ("hello", irs::ViewCast<char>(term->value));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(6, offset->start);
  ASSERT_EQ(11, offset->end);
  ASSERT_EQ("world", irs::ViewCast<char>(term->value));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(12, offset->start);
  ASSERT_EQ(16, offset->end);
  ASSERT_EQ("test", irs::ViewCast<char>(term->value));

  ASSERT_FALSE(stream.next());
}

TEST_F(PatternTokenizerTests, test_group_extraction_0) {
  std::string_view data("aaa 'bbb' 'ccc'");
  irs::analysis::PatternTokenizer stream("'([^']+)'", 0);

  ASSERT_TRUE(stream.reset(data));

  auto* offset = irs::get<irs::OffsAttr>(stream);
  auto* term = irs::get<irs::TermAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(4, offset->start);
  ASSERT_EQ(9, offset->end);
  ASSERT_EQ("'bbb'", irs::ViewCast<char>(term->value));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(10, offset->start);
  ASSERT_EQ(15, offset->end);
  ASSERT_EQ("'ccc'", irs::ViewCast<char>(term->value));

  ASSERT_FALSE(stream.next());
}

TEST_F(PatternTokenizerTests, test_group_extraction_1) {
  std::string_view data("aaa 'bbb' 'ccc'");
  irs::analysis::PatternTokenizer stream("'([^']+)'", 1);

  ASSERT_TRUE(stream.reset(data));

  auto* offset = irs::get<irs::OffsAttr>(stream);
  auto* term = irs::get<irs::TermAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(5, offset->start);
  ASSERT_EQ(8, offset->end);
  ASSERT_EQ("bbb", irs::ViewCast<char>(term->value));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(11, offset->start);
  ASSERT_EQ(14, offset->end);
  ASSERT_EQ("ccc", irs::ViewCast<char>(term->value));

  ASSERT_FALSE(stream.next());
}

TEST_F(PatternTokenizerTests, test_digits_extraction) {
  std::string_view data("foo123bar456baz789");
  irs::analysis::PatternTokenizer stream("([0-9]+)", 1);

  ASSERT_TRUE(stream.reset(data));

  auto* offset = irs::get<irs::OffsAttr>(stream);
  auto* term = irs::get<irs::TermAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(3, offset->start);
  ASSERT_EQ(6, offset->end);
  ASSERT_EQ("123", irs::ViewCast<char>(term->value));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(9, offset->start);
  ASSERT_EQ(12, offset->end);
  ASSERT_EQ("456", irs::ViewCast<char>(term->value));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(15, offset->start);
  ASSERT_EQ(18, offset->end);
  ASSERT_EQ("789", irs::ViewCast<char>(term->value));

  ASSERT_FALSE(stream.next());
}

TEST_F(PatternTokenizerTests, test_empty_input) {
  std::string_view data("");
  irs::analysis::PatternTokenizer stream(",", -1);

  ASSERT_TRUE(stream.reset(data));

  ASSERT_FALSE(stream.next());
}

TEST_F(PatternTokenizerTests, test_no_match) {
  std::string_view data("hello world");
  irs::analysis::PatternTokenizer stream("[0-9]+", 0);

  ASSERT_TRUE(stream.reset(data));

  ASSERT_FALSE(stream.next());
}

TEST_F(PatternTokenizerTests, test_reset) {
  irs::analysis::PatternTokenizer stream(",", -1);

  std::string_view data1("a,b");
  ASSERT_TRUE(stream.reset(data1));

  auto* offset = irs::get<irs::OffsAttr>(stream);
  auto* term = irs::get<irs::TermAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(1, offset->end);
  ASSERT_EQ("a", irs::ViewCast<char>(term->value));
  ASSERT_TRUE(stream.next());
  ASSERT_EQ(2, offset->start);
  ASSERT_EQ(3, offset->end);
  ASSERT_EQ("b", irs::ViewCast<char>(term->value));
  ASSERT_FALSE(stream.next());

  std::string_view data2("x,y,z");
  ASSERT_TRUE(stream.reset(data2));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(1, offset->end);
  ASSERT_EQ("x", irs::ViewCast<char>(term->value));
  ASSERT_TRUE(stream.next());
  ASSERT_EQ(2, offset->start);
  ASSERT_EQ(3, offset->end);
  ASSERT_EQ("y", irs::ViewCast<char>(term->value));
  ASSERT_TRUE(stream.next());
  ASSERT_EQ(4, offset->start);
  ASSERT_EQ(5, offset->end);
  ASSERT_EQ("z", irs::ViewCast<char>(term->value));
  ASSERT_FALSE(stream.next());
}

TEST_F(PatternTokenizerTests, test_splitting_double_dash) {
  std::string_view data("aaa--bbb--ccc");
  irs::analysis::PatternTokenizer stream("--", -1);

  ASSERT_TRUE(stream.reset(data));

  auto* offset = irs::get<irs::OffsAttr>(stream);
  auto* term = irs::get<irs::TermAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(3, offset->end);
  ASSERT_EQ("aaa", irs::ViewCast<char>(term->value));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(5, offset->start);
  ASSERT_EQ(8, offset->end);
  ASSERT_EQ("bbb", irs::ViewCast<char>(term->value));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(10, offset->start);
  ASSERT_EQ(13, offset->end);
  ASSERT_EQ("ccc", irs::ViewCast<char>(term->value));

  ASSERT_FALSE(stream.next());
}

TEST_F(PatternTokenizerTests, test_splitting_single_char) {
  std::string_view data("boo:and:foo");
  irs::analysis::PatternTokenizer stream("o", -1);

  ASSERT_TRUE(stream.reset(data));

  auto* offset = irs::get<irs::OffsAttr>(stream);
  auto* term = irs::get<irs::TermAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(1, offset->end);
  ASSERT_EQ("b", irs::ViewCast<char>(term->value));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(3, offset->start);
  ASSERT_EQ(9, offset->end);
  ASSERT_EQ(":and:f", irs::ViewCast<char>(term->value));

  ASSERT_FALSE(stream.next());
}

TEST_F(PatternTokenizerTests, test_group_zero_matches) {
  std::string_view data("boo:and:foo");
  irs::analysis::PatternTokenizer stream(":", 0);

  ASSERT_TRUE(stream.reset(data));

  auto* offset = irs::get<irs::OffsAttr>(stream);
  auto* term = irs::get<irs::TermAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(3, offset->start);
  ASSERT_EQ(4, offset->end);
  ASSERT_EQ(":", irs::ViewCast<char>(term->value));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(7, offset->start);
  ASSERT_EQ(8, offset->end);
  ASSERT_EQ(":", irs::ViewCast<char>(term->value));

  ASSERT_FALSE(stream.next());
}

TEST_F(PatternTokenizerTests, test_offset_with_complex_pattern) {
  std::string_view data("hello world test");
  irs::analysis::PatternTokenizer stream("[,;/\\s]+", -1);

  ASSERT_TRUE(stream.reset(data));

  auto* offset = irs::get<irs::OffsAttr>(stream);
  auto* term = irs::get<irs::TermAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(5, offset->end);
  ASSERT_EQ("hello", irs::ViewCast<char>(term->value));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(6, offset->start);
  ASSERT_EQ(11, offset->end);
  ASSERT_EQ("world", irs::ViewCast<char>(term->value));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(12, offset->start);
  ASSERT_EQ(16, offset->end);
  ASSERT_EQ("test", irs::ViewCast<char>(term->value));

  ASSERT_FALSE(stream.next());
}

TEST_F(PatternTokenizerTests, test_consecutive_delimiters) {
  std::string_view data("a,,b");
  irs::analysis::PatternTokenizer stream(",", -1);

  ASSERT_TRUE(stream.reset(data));

  auto* offset = irs::get<irs::OffsAttr>(stream);
  auto* term = irs::get<irs::TermAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(1, offset->end);
  ASSERT_EQ("a", irs::ViewCast<char>(term->value));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(3, offset->start);
  ASSERT_EQ(4, offset->end);
  ASSERT_EQ("b", irs::ViewCast<char>(term->value));

  ASSERT_FALSE(stream.next());
}

TEST_F(PatternTokenizerTests, test_delimiter_at_boundaries) {
  std::string_view data(",hello,world,");
  irs::analysis::PatternTokenizer stream(",", -1);

  ASSERT_TRUE(stream.reset(data));

  auto* offset = irs::get<irs::OffsAttr>(stream);
  auto* term = irs::get<irs::TermAttr>(stream);

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(1, offset->start);
  ASSERT_EQ(6, offset->end);
  ASSERT_EQ("hello", irs::ViewCast<char>(term->value));

  ASSERT_TRUE(stream.next());
  ASSERT_EQ(7, offset->start);
  ASSERT_EQ(12, offset->end);
  ASSERT_EQ("world", irs::ViewCast<char>(term->value));

  ASSERT_FALSE(stream.next());
}
