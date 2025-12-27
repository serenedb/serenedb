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

#include <gtest/gtest.h>
#include <iresearch/parser/parser.h>

#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/levenshtein_filter.hpp>
#include <iresearch/search/phrase_filter.hpp>
#include <iresearch/search/prefix_filter.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/wildcard_filter.hpp>
#include <iresearch/utils/string.hpp>

#include "basics/down_cast.h"

namespace {

class LuceneParserTest : public ::testing::Test {
 protected:
  irs::Or root;
  sdb::ParserContext ctx{root, "content"};

  void SetUp() override { root.clear(); }

  template<typename T>
  const T& GetFilter(size_t index) {
    auto it = root.begin();
    std::advance(it, index);
    return sdb::basics::downCast<T>(**it);
  }
};

TEST_F(LuceneParserTest, SimpleTerm) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello").ok());
  ASSERT_EQ(1, root.size());

  const auto& term = GetFilter<irs::ByTerm>(0);
  EXPECT_EQ("content", term.field());
  EXPECT_EQ("hello", irs::ViewCast<char>(irs::bytes_view{term.options().term}));
}

TEST_F(LuceneParserTest, SimplePhrase) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "\"hello world\"").ok());
  ASSERT_EQ(1, root.size());

  const auto& phrase = GetFilter<irs::ByPhrase>(0);
  EXPECT_EQ("content", phrase.field());
}

TEST_F(LuceneParserTest, PrefixQuery) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hel*").ok());
  ASSERT_EQ(1, root.size());

  const auto& prefix = GetFilter<irs::ByPrefix>(0);
  EXPECT_EQ("content", prefix.field());
  EXPECT_EQ("hel", irs::ViewCast<char>(irs::bytes_view{prefix.options().term}));
}

TEST_F(LuceneParserTest, WildcardQuery) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "h*llo").ok());
  ASSERT_EQ(1, root.size());

  const auto& wildcard = GetFilter<irs::ByWildcard>(0);
  EXPECT_EQ("content", wildcard.field());
  EXPECT_EQ("h*llo", irs::ViewCast<char>(irs::bytes_view{wildcard.options().term}));
}

TEST_F(LuceneParserTest, FieldSpecificTerm) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:hello").ok());
  ASSERT_EQ(1, root.size());

  const auto& term = GetFilter<irs::ByTerm>(0);
  EXPECT_EQ("title", term.field());
  EXPECT_EQ("hello", irs::ViewCast<char>(irs::bytes_view{term.options().term}));
}

TEST_F(LuceneParserTest, FieldSpecificPhrase) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:\"hello world\"").ok());
  ASSERT_EQ(1, root.size());

  const auto& phrase = GetFilter<irs::ByPhrase>(0);
  EXPECT_EQ("title", phrase.field());
}

TEST_F(LuceneParserTest, BoostedTerm) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello^2").ok());
  ASSERT_EQ(1, root.size());

  const auto& term = GetFilter<irs::ByTerm>(0);
  EXPECT_EQ("content", term.field());
  EXPECT_FLOAT_EQ(2.0f, term.Boost());
}

TEST_F(LuceneParserTest, BoostedTermFloat) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello^1.5").ok());
  ASSERT_EQ(1, root.size());

  const auto& term = GetFilter<irs::ByTerm>(0);
  EXPECT_FLOAT_EQ(1.5f, term.Boost());
}

TEST_F(LuceneParserTest, FuzzyTerm) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello~").ok());
  ASSERT_EQ(1, root.size());

  const auto& fuzzy = GetFilter<irs::ByEditDistance>(0);
  EXPECT_EQ("content", fuzzy.field());
  EXPECT_EQ("hello", irs::ViewCast<char>(irs::bytes_view{fuzzy.options().term}));
  EXPECT_EQ(2, fuzzy.options().max_distance);
}

TEST_F(LuceneParserTest, FuzzyTermWithDistance) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello~1").ok());
  ASSERT_EQ(1, root.size());

  const auto& fuzzy = GetFilter<irs::ByEditDistance>(0);
  EXPECT_EQ(1, fuzzy.options().max_distance);
}

TEST_F(LuceneParserTest, RangeInclusive) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "[alpha TO omega]").ok());
  ASSERT_EQ(1, root.size());

  const auto& range = GetFilter<irs::ByRange>(0);
  EXPECT_EQ("content", range.field());
  EXPECT_EQ("alpha", irs::ViewCast<char>(irs::bytes_view{range.options().range.min}));
  EXPECT_EQ("omega", irs::ViewCast<char>(irs::bytes_view{range.options().range.max}));
  EXPECT_EQ(irs::BoundType::Inclusive, range.options().range.min_type);
  EXPECT_EQ(irs::BoundType::Inclusive, range.options().range.max_type);
}

TEST_F(LuceneParserTest, RangeExclusive) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "{alpha TO omega}").ok());
  ASSERT_EQ(1, root.size());

  const auto& range = GetFilter<irs::ByRange>(0);
  EXPECT_EQ(irs::BoundType::Exclusive, range.options().range.min_type);
  EXPECT_EQ(irs::BoundType::Exclusive, range.options().range.max_type);
}

TEST_F(LuceneParserTest, RangeUnbounded) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "[* TO omega]").ok());
  ASSERT_EQ(1, root.size());

  const auto& range = GetFilter<irs::ByRange>(0);
  EXPECT_EQ(irs::BoundType::Unbounded, range.options().range.min_type);
  EXPECT_EQ(irs::BoundType::Inclusive, range.options().range.max_type);
}

TEST_F(LuceneParserTest, ImplicitOr) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello world").ok());
  ASSERT_EQ(2, root.size());

  const auto& term1 = GetFilter<irs::ByTerm>(0);
  EXPECT_EQ("hello", irs::ViewCast<char>(irs::bytes_view{term1.options().term}));

  const auto& term2 = GetFilter<irs::ByTerm>(1);
  EXPECT_EQ("world", irs::ViewCast<char>(irs::bytes_view{term2.options().term}));
}

TEST_F(LuceneParserTest, ExplicitOr) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello OR world").ok());
  ASSERT_EQ(2, root.size());

  const auto& term1 = GetFilter<irs::ByTerm>(0);
  EXPECT_EQ("hello", irs::ViewCast<char>(irs::bytes_view{term1.options().term}));

  const auto& term2 = GetFilter<irs::ByTerm>(1);
  EXPECT_EQ("world", irs::ViewCast<char>(irs::bytes_view{term2.options().term}));
}

TEST_F(LuceneParserTest, AndOperator) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello AND world").ok());
  ASSERT_EQ(2, root.size());

  // First: term "hello"
  const auto& term1 = GetFilter<irs::ByTerm>(0);
  EXPECT_EQ("content", term1.field());
  EXPECT_EQ("hello", irs::ViewCast<char>(irs::bytes_view{term1.options().term}));

  // Second: And containing "world"
  const auto& and_filter = GetFilter<irs::And>(1);
  ASSERT_EQ(1, and_filter.size());

  const auto& term2 = sdb::basics::downCast<irs::ByTerm>(**and_filter.begin());
  EXPECT_EQ("content", term2.field());
  EXPECT_EQ("world", irs::ViewCast<char>(irs::bytes_view{term2.options().term}));
}

TEST_F(LuceneParserTest, NotOperator) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "NOT hello").ok());
  ASSERT_EQ(1, root.size());

  // Not wraps an Or which contains the term
  const auto& not_filter = GetFilter<irs::Not>(0);
  const auto* inner_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, inner_or);
  ASSERT_EQ(1, inner_or->size());

  const auto& term = sdb::basics::downCast<irs::ByTerm>(**inner_or->begin());
  EXPECT_EQ("content", term.field());
  EXPECT_EQ("hello", irs::ViewCast<char>(irs::bytes_view{term.options().term}));
}

TEST_F(LuceneParserTest, MinusOperator) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "-hello").ok());
  ASSERT_EQ(1, root.size());

  // Minus is same as Not: wraps an Or which contains the term
  const auto& not_filter = GetFilter<irs::Not>(0);
  const auto* inner_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, inner_or);
  ASSERT_EQ(1, inner_or->size());

  const auto& term = sdb::basics::downCast<irs::ByTerm>(**inner_or->begin());
  EXPECT_EQ("content", term.field());
  EXPECT_EQ("hello", irs::ViewCast<char>(irs::bytes_view{term.options().term}));
}

TEST_F(LuceneParserTest, PlusOperator) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+hello").ok());
  ASSERT_EQ(1, root.size());

  // Single + creates an And with the term inside
  const auto& and_filter = GetFilter<irs::And>(0);
  ASSERT_EQ(1, and_filter.size());

  const auto& term = sdb::basics::downCast<irs::ByTerm>(**and_filter.begin());
  EXPECT_EQ("content", term.field());
  EXPECT_EQ("hello", irs::ViewCast<char>(irs::bytes_view{term.options().term}));
}

TEST_F(LuceneParserTest, MultiplePlusOperators) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+foo +bar").ok());
  ASSERT_EQ(1, root.size());

  // Should be wrapped in And
  const auto& and_filter = GetFilter<irs::And>(0);
  ASSERT_EQ(2, and_filter.size());

  auto it = and_filter.begin();
  const auto& term1 = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("content", term1.field());
  EXPECT_EQ("foo", irs::ViewCast<char>(irs::bytes_view{term1.options().term}));

  ++it;
  const auto& term2 = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("content", term2.field());
  EXPECT_EQ("bar", irs::ViewCast<char>(irs::bytes_view{term2.options().term}));
}

TEST_F(LuceneParserTest, GroupedQuery) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(hello OR world)").ok());
  ASSERT_EQ(1, root.size());

  // Group creates a sub-Or
  const auto& sub_or = GetFilter<irs::Or>(0);
  ASSERT_EQ(2, sub_or.size());

  auto it = sub_or.begin();
  const auto& term1 = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("content", term1.field());
  EXPECT_EQ("hello", irs::ViewCast<char>(irs::bytes_view{term1.options().term}));

  ++it;
  const auto& term2 = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("content", term2.field());
  EXPECT_EQ("world", irs::ViewCast<char>(irs::bytes_view{term2.options().term}));
}

TEST_F(LuceneParserTest, FieldWithGroup) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:(hello world)").ok());
  ASSERT_EQ(1, root.size());

  // Group creates a sub-Or
  const auto& sub_or = GetFilter<irs::Or>(0);
  ASSERT_EQ(2, sub_or.size());

  auto it = sub_or.begin();
  const auto& term1 = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("title", term1.field());
  EXPECT_EQ("hello", irs::ViewCast<char>(irs::bytes_view{term1.options().term}));

  ++it;
  const auto& term2 = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("title", term2.field());
  EXPECT_EQ("world", irs::ViewCast<char>(irs::bytes_view{term2.options().term}));
}

TEST_F(LuceneParserTest, NestedFieldQuery) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:hello author:world").ok());
  ASSERT_EQ(2, root.size());

  const auto& term1 = GetFilter<irs::ByTerm>(0);
  EXPECT_EQ("title", term1.field());
  EXPECT_EQ("hello", irs::ViewCast<char>(irs::bytes_view{term1.options().term}));

  const auto& term2 = GetFilter<irs::ByTerm>(1);
  EXPECT_EQ("author", term2.field());
  EXPECT_EQ("world", irs::ViewCast<char>(irs::bytes_view{term2.options().term}));
}

TEST_F(LuceneParserTest, FieldRestoresAfterGroup) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:(a b) c").ok());
  ASSERT_EQ(2, root.size());

  // First element is the group (sub-Or with title field terms)
  const auto& sub_or = GetFilter<irs::Or>(0);
  ASSERT_EQ(2, sub_or.size());

  auto it = sub_or.begin();
  const auto& term1 = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("title", term1.field());
  EXPECT_EQ("a", irs::ViewCast<char>(irs::bytes_view{term1.options().term}));

  ++it;
  const auto& term2 = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("title", term2.field());
  EXPECT_EQ("b", irs::ViewCast<char>(irs::bytes_view{term2.options().term}));

  // Second element uses default field (restored after group)
  const auto& term3 = GetFilter<irs::ByTerm>(1);
  EXPECT_EQ("content", term3.field());
  EXPECT_EQ("c", irs::ViewCast<char>(irs::bytes_view{term3.options().term}));
}

TEST_F(LuceneParserTest, ComplexQuery) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:hello^2 AND content:world~1").ok());
  ASSERT_EQ(2, root.size());

  // First: boosted term title:hello^2
  const auto& term = GetFilter<irs::ByTerm>(0);
  EXPECT_EQ("title", term.field());
  EXPECT_EQ("hello", irs::ViewCast<char>(irs::bytes_view{term.options().term}));
  EXPECT_FLOAT_EQ(2.0f, term.Boost());

  // Second: And containing fuzzy term content:world~1
  const auto& and_filter = GetFilter<irs::And>(1);
  ASSERT_EQ(1, and_filter.size());

  const auto& fuzzy =
      sdb::basics::downCast<irs::ByEditDistance>(**and_filter.begin());
  EXPECT_EQ("content", fuzzy.field());
  EXPECT_EQ("world", irs::ViewCast<char>(irs::bytes_view{fuzzy.options().term}));
  EXPECT_EQ(1, fuzzy.options().max_distance);
}

TEST_F(LuceneParserTest, BoostedGroup) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(foo bar)^2.5").ok());
  ASSERT_EQ(1, root.size());

  // Group is a sub-Or that can be boosted
  const auto& sub_or = GetFilter<irs::Or>(0);
  EXPECT_FLOAT_EQ(2.5f, sub_or.Boost());
  ASSERT_EQ(2, sub_or.size());

  auto it = sub_or.begin();
  const auto& term1 = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("foo", irs::ViewCast<char>(irs::bytes_view{term1.options().term}));

  ++it;
  const auto& term2 = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("bar", irs::ViewCast<char>(irs::bytes_view{term2.options().term}));
}

TEST_F(LuceneParserTest, ParseError) {
  auto result = sdb::ParseQuery(ctx, "[unclosed");
  ASSERT_TRUE(result.fail());
  EXPECT_FALSE(result.errorMessage().empty());
}

// Invalid grammar tests

TEST_F(LuceneParserTest, ParseError_UnclosedParenthesis) {
  auto result = sdb::ParseQuery(ctx, "(hello world");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_UnclosedParenthesisNested) {
  auto result = sdb::ParseQuery(ctx, "((foo bar)");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_ExtraClosingParenthesis) {
  auto result = sdb::ParseQuery(ctx, "hello world)");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_UnclosedBracket) {
  auto result = sdb::ParseQuery(ctx, "[alpha TO omega");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_UnclosedBrace) {
  auto result = sdb::ParseQuery(ctx, "{alpha TO omega");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

TEST_F(LuceneParserTest, RangeMixedBrackets) {
  // Mixed brackets are valid: [ is inclusive, } is exclusive
  ASSERT_TRUE(sdb::ParseQuery(ctx, "[alpha TO omega}").ok());
  ASSERT_EQ(1, root.size());

  const auto& range = GetFilter<irs::ByRange>(0);
  EXPECT_EQ(irs::BoundType::Inclusive, range.options().range.min_type);
  EXPECT_EQ(irs::BoundType::Exclusive, range.options().range.max_type);
}

TEST_F(LuceneParserTest, ParseError_RangeMissingTO) {
  auto result = sdb::ParseQuery(ctx, "[alpha omega]");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_RangeMissingMinBound) {
  auto result = sdb::ParseQuery(ctx, "[TO omega]");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_RangeMissingMaxBound) {
  auto result = sdb::ParseQuery(ctx, "[alpha TO]");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_TrailingAND) {
  auto result = sdb::ParseQuery(ctx, "hello AND");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_TrailingOR) {
  auto result = sdb::ParseQuery(ctx, "hello OR");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_TrailingNOT) {
  auto result = sdb::ParseQuery(ctx, "hello NOT");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_LeadingAND) {
  auto result = sdb::ParseQuery(ctx, "AND hello");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_LeadingOR) {
  auto result = sdb::ParseQuery(ctx, "OR hello");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_DoubleAND) {
  auto result = sdb::ParseQuery(ctx, "hello AND AND world");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_FieldMissingValue) {
  auto result = sdb::ParseQuery(ctx, "title:");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_BoostMissingValue) {
  auto result = sdb::ParseQuery(ctx, "hello^");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_EmptyParentheses) {
  auto result = sdb::ParseQuery(ctx, "()");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_DoubleColon) {
  auto result = sdb::ParseQuery(ctx, "title::hello");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_BoostNonNumeric) {
  auto result = sdb::ParseQuery(ctx, "hello^abc");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

}  // namespace
