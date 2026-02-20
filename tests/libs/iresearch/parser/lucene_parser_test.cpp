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

#include "basics/down_cast.h"
#include "iresearch/analysis/segmentation_tokenizer.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/mixed_boolean_filter.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/range_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/wildcard_filter.hpp"
#include "iresearch/utils/string.hpp"

namespace {

void AssertTerm(const irs::Filter& f, std::string_view field,
                std::string_view value, float boost = 0.0f) {
  const auto& term = sdb::basics::downCast<irs::ByTerm>(f);
  EXPECT_EQ(field, term.field());
  EXPECT_EQ(value, irs::ViewCast<char>(irs::bytes_view{term.options().term}));
  if (boost > 0.0f) {
    EXPECT_FLOAT_EQ(boost, term.Boost());
  }
}

void AssertPhrase(const irs::Filter& f, std::string_view field,
                  float boost = 0.0f) {
  const auto& phrase = sdb::basics::downCast<irs::ByPhrase>(f);
  EXPECT_EQ(field, phrase.field());
  if (boost > 0.0f) {
    EXPECT_FLOAT_EQ(boost, phrase.Boost());
  }
}

void AssertPrefix(const irs::Filter& f, std::string_view field,
                  std::string_view value, float boost = 0.0f) {
  const auto& prefix = sdb::basics::downCast<irs::ByPrefix>(f);
  EXPECT_EQ(field, prefix.field());
  EXPECT_EQ(value, irs::ViewCast<char>(irs::bytes_view{prefix.options().term}));
  if (boost > 0.0f) {
    EXPECT_FLOAT_EQ(boost, prefix.Boost());
  }
}

void AssertWildcard(const irs::Filter& f, std::string_view field,
                    std::string_view value, float boost = 0.0f) {
  const auto& wc = sdb::basics::downCast<irs::ByWildcard>(f);
  EXPECT_EQ(field, wc.field());
  EXPECT_EQ(value, irs::ViewCast<char>(irs::bytes_view{wc.options().term}));
  if (boost > 0.0f)
    EXPECT_FLOAT_EQ(boost, wc.Boost());
}

void AssertFuzzy(const irs::Filter& f, std::string_view field,
                 std::string_view value, int distance, float boost = 0.0f) {
  const auto& fuzzy = sdb::basics::downCast<irs::ByEditDistance>(f);
  EXPECT_EQ(field, fuzzy.field());
  EXPECT_EQ(value, irs::ViewCast<char>(irs::bytes_view{fuzzy.options().term}));
  EXPECT_EQ(distance, fuzzy.options().max_distance);
  if (boost > 0.0f)
    EXPECT_FLOAT_EQ(boost, fuzzy.Boost());
}

void AssertRange(const irs::Filter& f, std::string_view field,
                 std::string_view min, irs::BoundType min_type,
                 std::string_view max, irs::BoundType max_type,
                 float boost = 0.0f) {
  const auto& range = sdb::basics::downCast<irs::ByRange>(f);
  EXPECT_EQ(field, range.field());
  EXPECT_EQ(min_type, range.options().range.min_type);
  if (min_type != irs::BoundType::Unbounded) {
    EXPECT_EQ(min,
              irs::ViewCast<char>(irs::bytes_view{range.options().range.min}));
  }
  EXPECT_EQ(max_type, range.options().range.max_type);
  if (max_type != irs::BoundType::Unbounded) {
    EXPECT_EQ(max,
              irs::ViewCast<char>(irs::bytes_view{range.options().range.max}));
  }
  if (boost > 0.0f) {
    EXPECT_FLOAT_EQ(boost, range.Boost());
  }
}

class LuceneParserTest : public ::testing::Test {
 protected:
  irs::MixedBooleanFilter root;
  irs::analysis::SegmentationTokenizer::ptr tokenizer{
    irs::analysis::SegmentationTokenizer::make(
      irs::analysis::SegmentationTokenizer::Options{})};

  sdb::ParserContext ctx{root, "content", *tokenizer};

  auto& OptionalRoot() { return root.GetOptional(); }
  auto& RequiredRoot() { return root.GetRequired(); }
};

TEST_F(LuceneParserTest, SimpleTerm) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertTerm(OptionalRoot()[0], "content", "hello");
}

TEST_F(LuceneParserTest, SimplePhrase) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "\"hello world\"").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertPhrase(OptionalRoot()[0], "content");
}

TEST_F(LuceneParserTest, PrefixQuery) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hel*").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertPrefix(OptionalRoot()[0], "content", "hel");
}

TEST_F(LuceneParserTest, WildcardQuery) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "h*llo").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertWildcard(OptionalRoot()[0], "content", "h*llo");
}

TEST_F(LuceneParserTest, FieldSpecificTerm) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:hello").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertTerm(OptionalRoot()[0], "title", "hello");
}

TEST_F(LuceneParserTest, FieldSpecificPhrase) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:\"hello world\"").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertPhrase(OptionalRoot()[0], "title");
}

TEST_F(LuceneParserTest, BoostedTerm) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello^2").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertTerm(OptionalRoot()[0], "content", "hello", 2.0f);
}

TEST_F(LuceneParserTest, BoostedTermFloat) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello^1.5").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertTerm(OptionalRoot()[0], "content", "hello", 1.5f);
}

TEST_F(LuceneParserTest, FuzzyTerm) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello~").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertFuzzy(OptionalRoot()[0], "content", "hello", 2);
}

TEST_F(LuceneParserTest, FuzzyTermWithDistance) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello~1").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertFuzzy(OptionalRoot()[0], "content", "hello", 1);
}

TEST_F(LuceneParserTest, RangeInclusive) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "[alpha TO omega]").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertRange(OptionalRoot()[0], "content", "alpha", irs::BoundType::Inclusive,
              "omega", irs::BoundType::Inclusive);
}

TEST_F(LuceneParserTest, RangeExclusive) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "{alpha TO omega}").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertRange(OptionalRoot()[0], "content", "alpha", irs::BoundType::Exclusive,
              "omega", irs::BoundType::Exclusive);
}

TEST_F(LuceneParserTest, RangeUnbounded) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "[* TO omega]").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertRange(OptionalRoot()[0], "content", "", irs::BoundType::Unbounded,
              "omega", irs::BoundType::Inclusive);
}

TEST_F(LuceneParserTest, ImplicitOr) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello world").ok());
  ASSERT_EQ(2, OptionalRoot().size());
  AssertTerm(OptionalRoot()[0], "content", "hello");
  AssertTerm(OptionalRoot()[1], "content", "world");
}

TEST_F(LuceneParserTest, ExplicitOr) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello OR world").ok());
  ASSERT_EQ(2, OptionalRoot().size());
  AssertTerm(OptionalRoot()[0], "content", "hello");
  AssertTerm(OptionalRoot()[1], "content", "world");
}

TEST_F(LuceneParserTest, AndOperator) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello AND world").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(2, RequiredRoot().size());
  AssertTerm(RequiredRoot()[0], "content", "hello");
  AssertTerm(RequiredRoot()[1], "content", "world");
}

TEST_F(LuceneParserTest, ChainedAndOperator) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND b AND c").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(3, RequiredRoot().size());
  AssertTerm(RequiredRoot()[0], "content", "a");
  AssertTerm(RequiredRoot()[1], "content", "b");
  AssertTerm(RequiredRoot()[2], "content", "c");
}

TEST_F(LuceneParserTest, MixedPlusMinusOperators) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+foo -bar +foobar -foobaz").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(4, RequiredRoot().size());
  AssertTerm(RequiredRoot()[0], "content", "foo");
  const auto& not1 = sdb::basics::downCast<irs::Not>(RequiredRoot()[1]);
  const auto* not1_or = not1.filter<irs::Or>();
  ASSERT_NE(nullptr, not1_or);
  ASSERT_EQ(1, not1_or->size());
  AssertTerm((*not1_or)[0], "content", "bar");
  AssertTerm(RequiredRoot()[2], "content", "foobar");
  const auto& not2 = sdb::basics::downCast<irs::Not>(RequiredRoot()[3]);
  const auto* not2_or = not2.filter<irs::Or>();
  ASSERT_NE(nullptr, not2_or);
  ASSERT_EQ(1, not2_or->size());
  AssertTerm((*not2_or)[0], "content", "foobaz");
}

TEST_F(LuceneParserTest, MixedPlusMinusWithImplicitOr) {
  // +foo bar -baz +foobar foobaz
  // + terms go to Required, plain terms go to Optional
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+foo bar -baz +foobar foobaz").ok());
  ASSERT_EQ(3, RequiredRoot().size());
  ASSERT_EQ(2, OptionalRoot().size());

  // Required: [foo, Not(baz), foobar]

  AssertTerm(RequiredRoot()[0], "content", "foo");

  const auto& not_filter = sdb::basics::downCast<irs::Not>(RequiredRoot()[1]);
  const auto* not_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  ASSERT_EQ(1, not_or->size());
  AssertTerm((*not_or)[0], "content", "baz");
  AssertTerm(RequiredRoot()[2], "content", "foobar");

  // Optional: [bar, foobaz]

  AssertTerm(OptionalRoot()[0], "content", "bar");
  AssertTerm(OptionalRoot()[1], "content", "foobaz");
}

TEST_F(LuceneParserTest, DeepNestedGroups) {
  // (a AND (b OR (c AND d)))
  // AND promotes a and the subgroup to Required within the outer group
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(a AND (b OR (c AND d)))").ok());
  // Outer group is a MixedBooleanFilter
  ASSERT_EQ(1, OptionalRoot().size());
  const auto& outer_mixed =
    sdb::basics::downCast<irs::MixedBooleanFilter>(OptionalRoot()[0]);
  ASSERT_TRUE(outer_mixed.GetOptional().empty());
  const auto& outer_req = outer_mixed.GetRequired();
  ASSERT_EQ(2, outer_req.size());

  AssertTerm(outer_req[0], "content", "a");

  // Second element is the group (b OR (c AND d))
  const auto& middle_mixed =
    sdb::basics::downCast<irs::MixedBooleanFilter>(outer_req[1]);
  const auto& middle_or = middle_mixed.GetOptional();

  // Inside: b, and (c AND d) group
  ASSERT_EQ(2, middle_or.size());

  AssertTerm(middle_or[0], "content", "b");

  const auto& inner_mixed =
    sdb::basics::downCast<irs::MixedBooleanFilter>(middle_or[1]);
  // c AND d promotes both to Required within the inner group
  ASSERT_TRUE(inner_mixed.GetOptional().empty());
  const auto& inner_req = inner_mixed.GetRequired();
  ASSERT_EQ(2, inner_req.size());

  AssertTerm(inner_req[0], "content", "c");
  AssertTerm(inner_req[1], "content", "d");
}

TEST_F(LuceneParserTest, GroupsWithAndOr) {
  // (a b) AND (c d) - AND promotes both groups to Required
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(a b) AND (c d)").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(2, RequiredRoot().size());

  const auto& group1 =
    sdb::basics::downCast<irs::MixedBooleanFilter>(RequiredRoot()[0]);
  const auto& group1_or = group1.GetOptional();
  ASSERT_EQ(2, group1_or.size());

  AssertTerm(group1_or[0], "content", "a");
  AssertTerm(group1_or[1], "content", "b");

  const auto& group2 =
    sdb::basics::downCast<irs::MixedBooleanFilter>(RequiredRoot()[1]);
  const auto& group2_or = group2.GetOptional();
  ASSERT_EQ(2, group2_or.size());

  AssertTerm(group2_or[0], "content", "c");
  AssertTerm(group2_or[1], "content", "d");
}

TEST_F(LuceneParserTest, PlusMinusWithGroups) {
  // +(foo bar) -baz - required group, excluded term
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+(foo bar) -baz").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(2, RequiredRoot().size());

  // First: group (foo bar) as MixedBooleanFilter
  const auto& group =
    sdb::basics::downCast<irs::MixedBooleanFilter>(RequiredRoot()[0]);
  const auto& group_or = group.GetOptional();
  ASSERT_EQ(2, group_or.size());

  AssertTerm(group_or[0], "content", "foo");
  AssertTerm(group_or[1], "content", "bar");

  // Second: Not(baz)
  const auto& not_filter = sdb::basics::downCast<irs::Not>(RequiredRoot()[1]);
  const auto* not_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  ASSERT_EQ(1, not_or->size());
  AssertTerm((*not_or)[0], "content", "baz");
}

TEST_F(LuceneParserTest, FieldWithPlusMinusGroups) {
  // +title:(hello world) -author:john
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+title:(hello world) -author:john").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(2, RequiredRoot().size());

  // First: group with title field
  const auto& group =
    sdb::basics::downCast<irs::MixedBooleanFilter>(RequiredRoot()[0]);
  const auto& group_or = group.GetOptional();
  ASSERT_EQ(2, group_or.size());

  AssertTerm(group_or[0], "title", "hello");
  AssertTerm(group_or[1], "title", "world");

  // Second: Not(author:john)
  const auto& not_filter = sdb::basics::downCast<irs::Not>(RequiredRoot()[1]);
  const auto* not_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  ASSERT_EQ(1, not_or->size());
  AssertTerm((*not_or)[0], "author", "john");
}

TEST_F(LuceneParserTest, ComplexMixedQuery) {
  // (a OR b) AND +(c d) -e
  // AND promotes (a OR b) to Required; +(c d) goes to Required; -e goes to
  // Required
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(a OR b) AND +(c d) -e").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(3, RequiredRoot().size());

  // First: (a OR b)
  const auto& group_ab =
    sdb::basics::downCast<irs::MixedBooleanFilter>(RequiredRoot()[0]);
  ASSERT_EQ(2, group_ab.GetOptional().size());
  AssertTerm(group_ab.GetOptional()[0], "content", "a");
  AssertTerm(group_ab.GetOptional()[1], "content", "b");

  // Second: (c d)
  const auto& group_cd =
    sdb::basics::downCast<irs::MixedBooleanFilter>(RequiredRoot()[1]);
  ASSERT_EQ(2, group_cd.GetOptional().size());
  AssertTerm(group_cd.GetOptional()[0], "content", "c");
  AssertTerm(group_cd.GetOptional()[1], "content", "d");

  // Third: Not(e)
  const auto& not_e = sdb::basics::downCast<irs::Not>(RequiredRoot()[2]);
  const auto* not_or = not_e.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  ASSERT_EQ(1, not_or->size());
  AssertTerm((*not_or)[0], "content", "e");
}

TEST_F(LuceneParserTest, ComplexMixedQueryGrouped) {
  // (a OR b) AND (+(c d) -e) - AND promotes both to Required
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(a OR b) AND (+(c d) -e)").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(2, RequiredRoot().size());

  // First: group (a OR b)
  const auto& group_ab =
    sdb::basics::downCast<irs::MixedBooleanFilter>(RequiredRoot()[0]);
  const auto& ab_or = group_ab.GetOptional();
  ASSERT_EQ(2, ab_or.size());

  AssertTerm(ab_or[0], "content", "a");
  AssertTerm(ab_or[1], "content", "b");

  // Second: group with +(c d) -e
  const auto& group2 =
    sdb::basics::downCast<irs::MixedBooleanFilter>(RequiredRoot()[1]);
  const auto& group2_req = group2.GetRequired();
  ASSERT_EQ(2, group2_req.size());

  const auto& group_cd =
    sdb::basics::downCast<irs::MixedBooleanFilter>(group2_req[0]);
  ASSERT_EQ(2, group_cd.GetOptional().size());

  AssertTerm(group_cd.GetOptional()[0], "content", "c");
  AssertTerm(group_cd.GetOptional()[1], "content", "d");

  const auto& not_e = sdb::basics::downCast<irs::Not>(group2_req[1]);
  const auto* not_or = not_e.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  ASSERT_EQ(1, not_or->size());
  AssertTerm((*not_or)[0], "content", "e");
}

TEST_F(LuceneParserTest, NotOperator) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "NOT hello").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(1, RequiredRoot().size());

  const auto& not_filter = sdb::basics::downCast<irs::Not>(RequiredRoot()[0]);
  const auto* inner_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, inner_or);
  ASSERT_EQ(1, inner_or->size());

  AssertTerm((*inner_or)[0], "content", "hello");
}

TEST_F(LuceneParserTest, MinusOperator) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "-hello").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(1, RequiredRoot().size());

  const auto& not_filter = sdb::basics::downCast<irs::Not>(RequiredRoot()[0]);
  const auto* inner_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, inner_or);
  ASSERT_EQ(1, inner_or->size());

  AssertTerm((*inner_or)[0], "content", "hello");
}

TEST_F(LuceneParserTest, PlusOperator) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+hello").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(1, RequiredRoot().size());

  AssertTerm(RequiredRoot()[0], "content", "hello");
}

TEST_F(LuceneParserTest, MultiplePlusOperators) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+foo +bar").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(2, RequiredRoot().size());

  AssertTerm(RequiredRoot()[0], "content", "foo");
  AssertTerm(RequiredRoot()[1], "content", "bar");
}

TEST_F(LuceneParserTest, GroupedQuery) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(hello OR world)").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  const auto& group =
    sdb::basics::downCast<irs::MixedBooleanFilter>(OptionalRoot()[0]);
  ASSERT_TRUE(group.GetRequired().empty());
  const auto& sub_or = group.GetOptional();
  ASSERT_EQ(2, sub_or.size());
  AssertTerm(sub_or[0], "content", "hello");
  AssertTerm(sub_or[1], "content", "world");
}

TEST_F(LuceneParserTest, FieldWithGroup) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:(hello world)").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  const auto& group =
    sdb::basics::downCast<irs::MixedBooleanFilter>(OptionalRoot()[0]);
  ASSERT_TRUE(group.GetRequired().empty());
  const auto& sub_or = group.GetOptional();
  ASSERT_EQ(2, sub_or.size());
  AssertTerm(sub_or[0], "title", "hello");
  AssertTerm(sub_or[1], "title", "world");
}

TEST_F(LuceneParserTest, NestedFieldQuery) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:hello author:world").ok());
  ASSERT_EQ(2, OptionalRoot().size());
  AssertTerm(OptionalRoot()[0], "title", "hello");
  AssertTerm(OptionalRoot()[1], "author", "world");
}

TEST_F(LuceneParserTest, FieldRestoresAfterGroup) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:(a b) c").ok());
  // First element is the group (MixedBooleanFilter with title field terms)
  ASSERT_EQ(2, OptionalRoot().size());
  const auto& group =
    sdb::basics::downCast<irs::MixedBooleanFilter>(OptionalRoot()[0]);
  ASSERT_TRUE(group.GetRequired().empty());
  const auto& sub_or = group.GetOptional();
  ASSERT_EQ(2, sub_or.size());
  AssertTerm(sub_or[0], "title", "a");
  AssertTerm(sub_or[1], "title", "b");
  // Second element uses default field (restored after group)
  AssertTerm(OptionalRoot()[1], "content", "c");
}

TEST_F(LuceneParserTest, ComplexQuery) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:hello^2 AND content:world~1").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(2, RequiredRoot().size());
  AssertTerm(RequiredRoot()[0], "title", "hello", 2.0f);
  AssertFuzzy(RequiredRoot()[1], "content", "world", 1);
}

TEST_F(LuceneParserTest, BoostedGroup) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(foo bar)^2.5").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  const auto& group =
    sdb::basics::downCast<irs::MixedBooleanFilter>(OptionalRoot()[0]);
  EXPECT_FLOAT_EQ(2.5f, group.Boost());
  ASSERT_TRUE(group.GetRequired().empty());
  const auto& sub_or = group.GetOptional();
  ASSERT_EQ(2, sub_or.size());
  AssertTerm(sub_or[0], "content", "foo");
  AssertTerm(sub_or[1], "content", "bar");
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
  ASSERT_TRUE(sdb::ParseQuery(ctx, "[alpha TO omega}").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertRange(OptionalRoot()[0], "content", "alpha", irs::BoundType::Inclusive,
              "omega", irs::BoundType::Exclusive);
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

TEST_F(LuceneParserTest, NotBetweenTerms) {
  // guinea NOT pig → Optional[guinea], Required[Not(pig)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "guinea NOT pig").ok());
  ASSERT_EQ(1, RequiredRoot().size());

  ASSERT_EQ(1, OptionalRoot().size());
  AssertTerm(OptionalRoot()[0], "content", "guinea");

  const auto& not_filter = sdb::basics::downCast<irs::Not>(RequiredRoot()[0]);
  const auto* not_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  ASSERT_EQ(1, not_or->size());
  AssertTerm((*not_or)[0], "content", "pig");
}

TEST_F(LuceneParserTest, MinusBetweenTerms) {
  // guinea -pig → Optional[guinea], Required[Not(pig)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "guinea -pig").ok());
  ASSERT_EQ(1, RequiredRoot().size());

  ASSERT_EQ(1, OptionalRoot().size());
  AssertTerm(OptionalRoot()[0], "content", "guinea");

  const auto& not_filter = sdb::basics::downCast<irs::Not>(RequiredRoot()[0]);
  const auto* not_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  ASSERT_EQ(1, not_or->size());
  AssertTerm((*not_or)[0], "content", "pig");
}

TEST_F(LuceneParserTest, PlusBetweenTerms) {
  // guinea +pig → Optional[guinea], Required[pig]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "guinea +pig").ok());
  ASSERT_EQ(1, RequiredRoot().size());
  AssertTerm(RequiredRoot()[0], "content", "pig");

  ASSERT_EQ(1, OptionalRoot().size());
  AssertTerm(OptionalRoot()[0], "content", "guinea");
}

TEST_F(LuceneParserTest, AndThenOr) {
  // a AND b OR c → Required[a, b], Optional[c]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND b OR c").ok());
  ASSERT_EQ(2, RequiredRoot().size());
  AssertTerm(RequiredRoot()[0], "content", "a");
  AssertTerm(RequiredRoot()[1], "content", "b");

  ASSERT_EQ(1, OptionalRoot().size());
  AssertTerm(OptionalRoot()[0], "content", "c");
}

TEST_F(LuceneParserTest, OrThenAnd) {
  // a OR b AND c → Required[b, c], Optional[a]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a OR b AND c").ok());
  ASSERT_EQ(2, RequiredRoot().size());
  AssertTerm(RequiredRoot()[0], "content", "b");
  AssertTerm(RequiredRoot()[1], "content", "c");

  ASSERT_EQ(1, OptionalRoot().size());
  AssertTerm(OptionalRoot()[0], "content", "a");
}

TEST_F(LuceneParserTest, FourChainedAnd) {
  // a AND b AND c AND d → Required[a, b, c, d]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND b AND c AND d").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(4, RequiredRoot().size());
  AssertTerm(RequiredRoot()[0], "content", "a");
  AssertTerm(RequiredRoot()[1], "content", "b");
  AssertTerm(RequiredRoot()[2], "content", "c");
  AssertTerm(RequiredRoot()[3], "content", "d");
}

TEST_F(LuceneParserTest, NotBeforeAnd) {
  // NOT a AND b → Required[Not(a), b]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "NOT a AND b").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(2, RequiredRoot().size());

  const auto& not_filter = sdb::basics::downCast<irs::Not>(RequiredRoot()[0]);
  const auto* not_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  ASSERT_EQ(1, not_or->size());
  AssertTerm((*not_or)[0], "content", "a");

  AssertTerm(RequiredRoot()[1], "content", "b");
}

TEST_F(LuceneParserTest, AndBeforeNot) {
  // a AND NOT b → Required[a, Not(b)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND NOT b").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(2, RequiredRoot().size());

  AssertTerm(RequiredRoot()[0], "content", "a");

  const auto& not_filter = sdb::basics::downCast<irs::Not>(RequiredRoot()[1]);
  const auto* not_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  ASSERT_EQ(1, not_or->size());
  AssertTerm((*not_or)[0], "content", "b");
}

TEST_F(LuceneParserTest, NotBetweenMultipleTerms) {
  // a NOT b c → Optional[a, c], Required[Not(b)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a NOT b c").ok());
  ASSERT_EQ(2, OptionalRoot().size());
  ASSERT_EQ(1, RequiredRoot().size());

  AssertTerm(OptionalRoot()[0], "content", "a");
  AssertTerm(OptionalRoot()[1], "content", "c");

  const auto& not_filter = sdb::basics::downCast<irs::Not>(RequiredRoot()[0]);
  const auto* not_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  ASSERT_EQ(1, not_or->size());
  AssertTerm((*not_or)[0], "content", "b");
}

TEST_F(LuceneParserTest, AndWithMinusModifier) {
  // a AND -b → Required[a, Not(b)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND -b").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(2, RequiredRoot().size());

  AssertTerm(RequiredRoot()[0], "content", "a");

  const auto& not_filter = sdb::basics::downCast<irs::Not>(RequiredRoot()[1]);
  const auto* not_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  ASSERT_EQ(1, not_or->size());
  AssertTerm((*not_or)[0], "content", "b");
}

TEST_F(LuceneParserTest, AndWithPlusModifier) {
  // a AND +b → Required[a, b]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND +b").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(2, RequiredRoot().size());

  AssertTerm(RequiredRoot()[0], "content", "a");
  AssertTerm(RequiredRoot()[1], "content", "b");
}

TEST_F(LuceneParserTest, ComplexAndNotChain) {
  // a AND -b NOT c NOT d AND e → Required[a, Not(b), Not(c), Not(d), e]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND -b NOT c NOT d AND e").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(5, RequiredRoot().size());

  AssertTerm(RequiredRoot()[0], "content", "a");

  const auto& not_b = sdb::basics::downCast<irs::Not>(RequiredRoot()[1]);
  const auto* not_b_or = not_b.filter<irs::Or>();
  ASSERT_NE(nullptr, not_b_or);
  ASSERT_EQ(1, not_b_or->size());
  AssertTerm((*not_b_or)[0], "content", "b");

  const auto& not_c = sdb::basics::downCast<irs::Not>(RequiredRoot()[2]);
  const auto* not_c_or = not_c.filter<irs::Or>();
  ASSERT_NE(nullptr, not_c_or);
  ASSERT_EQ(1, not_c_or->size());
  AssertTerm((*not_c_or)[0], "content", "c");

  const auto& not_d = sdb::basics::downCast<irs::Not>(RequiredRoot()[3]);
  const auto* not_d_or = not_d.filter<irs::Or>();
  ASSERT_NE(nullptr, not_d_or);
  ASSERT_EQ(1, not_d_or->size());
  AssertTerm((*not_d_or)[0], "content", "d");

  AssertTerm(RequiredRoot()[4], "content", "e");
}

TEST_F(LuceneParserTest, MinusAndChain) {
  // -a AND -b AND -c → Required[Not(a), Not(b), Not(c)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "-a AND -b AND -c").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(3, RequiredRoot().size());

  const auto& not_a = sdb::basics::downCast<irs::Not>(RequiredRoot()[0]);
  const auto* not_a_or = not_a.filter<irs::Or>();
  ASSERT_NE(nullptr, not_a_or);
  AssertTerm((*not_a_or)[0], "content", "a");

  const auto& not_b = sdb::basics::downCast<irs::Not>(RequiredRoot()[1]);
  const auto* not_b_or = not_b.filter<irs::Or>();
  ASSERT_NE(nullptr, not_b_or);
  AssertTerm((*not_b_or)[0], "content", "b");

  const auto& not_c = sdb::basics::downCast<irs::Not>(RequiredRoot()[2]);
  const auto* not_c_or = not_c.filter<irs::Or>();
  ASSERT_NE(nullptr, not_c_or);
  AssertTerm((*not_c_or)[0], "content", "c");
}

TEST_F(LuceneParserTest, OrWithMinusModifier) {
  // a OR -b → Optional[a], Required[Not(b)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a OR -b").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  ASSERT_EQ(1, RequiredRoot().size());

  AssertTerm(OptionalRoot()[0], "content", "a");

  const auto& not_filter = sdb::basics::downCast<irs::Not>(RequiredRoot()[0]);
  const auto* not_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  ASSERT_EQ(1, not_or->size());
  AssertTerm((*not_or)[0], "content", "b");
}

TEST_F(LuceneParserTest, OrWithPlusModifier) {
  // a OR +b → Optional[a], Required[b]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a OR +b").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  ASSERT_EQ(1, RequiredRoot().size());

  AssertTerm(OptionalRoot()[0], "content", "a");
  AssertTerm(RequiredRoot()[0], "content", "b");
}

TEST_F(LuceneParserTest, MinusOrChain) {
  // -a OR -b → Required[Not(a), Not(b)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "-a OR -b").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(2, RequiredRoot().size());

  const auto& not_a = sdb::basics::downCast<irs::Not>(RequiredRoot()[0]);
  const auto* not_a_or = not_a.filter<irs::Or>();
  ASSERT_NE(nullptr, not_a_or);
  ASSERT_EQ(1, not_a_or->size());
  AssertTerm((*not_a_or)[0], "content", "a");

  const auto& not_b = sdb::basics::downCast<irs::Not>(RequiredRoot()[1]);
  const auto* not_b_or = not_b.filter<irs::Or>();
  ASSERT_NE(nullptr, not_b_or);
  ASSERT_EQ(1, not_b_or->size());
  AssertTerm((*not_b_or)[0], "content", "b");
}

TEST_F(LuceneParserTest, OrWithMultipleMinusModifiers) {
  // a OR -b OR -c → Optional[a], Required[Not(b), Not(c)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a OR -b OR -c").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  ASSERT_EQ(2, RequiredRoot().size());

  AssertTerm(OptionalRoot()[0], "content", "a");

  const auto& not_b = sdb::basics::downCast<irs::Not>(RequiredRoot()[0]);
  const auto* not_b_or = not_b.filter<irs::Or>();
  ASSERT_NE(nullptr, not_b_or);
  ASSERT_EQ(1, not_b_or->size());
  AssertTerm((*not_b_or)[0], "content", "b");

  const auto& not_c = sdb::basics::downCast<irs::Not>(RequiredRoot()[1]);
  const auto* not_c_or = not_c.filter<irs::Or>();
  ASSERT_NE(nullptr, not_c_or);
  ASSERT_EQ(1, not_c_or->size());
  AssertTerm((*not_c_or)[0], "content", "c");
}

TEST_F(LuceneParserTest, MixedAndOrSimple) {
  // a AND b OR c → Required[a, b], Optional[c]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND b OR c").ok());
  ASSERT_EQ(2, RequiredRoot().size());
  ASSERT_EQ(1, OptionalRoot().size());

  AssertTerm(RequiredRoot()[0], "content", "a");
  AssertTerm(RequiredRoot()[1], "content", "b");
  AssertTerm(OptionalRoot()[0], "content", "c");
}

TEST_F(LuceneParserTest, MixedOrAndSimple) {
  // a OR b AND c → Optional[a], Required[b, c]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a OR b AND c").ok());
  ASSERT_EQ(2, RequiredRoot().size());
  ASSERT_EQ(1, OptionalRoot().size());

  AssertTerm(OptionalRoot()[0], "content", "a");
  AssertTerm(RequiredRoot()[0], "content", "b");
  AssertTerm(RequiredRoot()[1], "content", "c");
}

TEST_F(LuceneParserTest, AndWithMinusThenOr) {
  // a AND -b OR c → Required[a, Not(b)], Optional[c]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND -b OR c").ok());
  ASSERT_EQ(2, RequiredRoot().size());
  ASSERT_EQ(1, OptionalRoot().size());

  AssertTerm(RequiredRoot()[0], "content", "a");

  const auto& not_b = sdb::basics::downCast<irs::Not>(RequiredRoot()[1]);
  const auto* not_b_or = not_b.filter<irs::Or>();
  ASSERT_NE(nullptr, not_b_or);
  ASSERT_EQ(1, not_b_or->size());
  AssertTerm((*not_b_or)[0], "content", "b");

  AssertTerm(OptionalRoot()[0], "content", "c");
}

TEST_F(LuceneParserTest, OrWithMinusThenAnd) {
  // a OR -b AND c → Required[Not(b), a, c]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a OR -b AND c").ok());
  ASSERT_EQ(3, RequiredRoot().size());
  ASSERT_TRUE(OptionalRoot().empty());

  const auto& not_b = sdb::basics::downCast<irs::Not>(RequiredRoot()[0]);
  const auto* not_b_or = not_b.filter<irs::Or>();
  ASSERT_NE(nullptr, not_b_or);
  ASSERT_EQ(1, not_b_or->size());
  AssertTerm((*not_b_or)[0], "content", "b");

  AssertTerm(RequiredRoot()[1], "content", "a");
  AssertTerm(RequiredRoot()[2], "content", "c");
}

TEST_F(LuceneParserTest, ComplexMixedAndOrWithModifiers) {
  // +a AND b OR -c AND d → Required[a, b, Not(c), d]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+a AND b OR -c AND d").ok());
  ASSERT_EQ(4, RequiredRoot().size());
  ASSERT_TRUE(OptionalRoot().empty());

  AssertTerm(RequiredRoot()[0], "content", "a");
  AssertTerm(RequiredRoot()[1], "content", "b");

  const auto& not_c = sdb::basics::downCast<irs::Not>(RequiredRoot()[2]);
  const auto* not_c_or = not_c.filter<irs::Or>();
  ASSERT_NE(nullptr, not_c_or);
  ASSERT_EQ(1, not_c_or->size());
  AssertTerm((*not_c_or)[0], "content", "c");

  AssertTerm(RequiredRoot()[3], "content", "d");
}

TEST_F(LuceneParserTest, PlusOrMinusAnd) {
  // +a OR -b AND c → Required[a, Not(b), c]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+a OR -b AND c").ok());
  ASSERT_EQ(3, RequiredRoot().size());
  ASSERT_TRUE(OptionalRoot().empty());

  AssertTerm(RequiredRoot()[0], "content", "a");

  const auto& not_b = sdb::basics::downCast<irs::Not>(RequiredRoot()[1]);
  const auto* not_b_or = not_b.filter<irs::Or>();
  ASSERT_NE(nullptr, not_b_or);
  ASSERT_EQ(1, not_b_or->size());
  AssertTerm((*not_b_or)[0], "content", "b");

  AssertTerm(RequiredRoot()[2], "content", "c");
}

TEST_F(LuceneParserTest, AndOrAndFlat) {
  // a AND b OR -c AND d → Required[a, b, Not(c), d]
  // Flat Lucene-like behavior: modifiers create MUST/MUST_NOT regardless of OR
  // This is NOT grouped as (a AND b) OR (-c AND d) - it's flat!
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND b OR -c AND d").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(4, RequiredRoot().size());

  AssertTerm(RequiredRoot()[0], "content", "a");
  AssertTerm(RequiredRoot()[1], "content", "b");

  const auto& not_c = sdb::basics::downCast<irs::Not>(RequiredRoot()[2]);
  const auto* not_c_or = not_c.filter<irs::Or>();
  ASSERT_NE(nullptr, not_c_or);
  ASSERT_EQ(1, not_c_or->size());
  AssertTerm((*not_c_or)[0], "content", "c");

  AssertTerm(RequiredRoot()[3], "content", "d");
}

TEST_F(LuceneParserTest, ManyImplicitOr) {
  // a b c d e → Optional[a, b, c, d, e]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a b c d e").ok());
  ASSERT_TRUE(RequiredRoot().empty());
  ASSERT_EQ(5, OptionalRoot().size());

  const char* expected[] = {"a", "b", "c", "d", "e"};
  for (size_t i = 0; i < 5; ++i) {
    const auto& term = sdb::basics::downCast<irs::ByTerm>(OptionalRoot()[i]);
    EXPECT_EQ(expected[i],
              irs::ViewCast<char>(irs::bytes_view{term.options().term}));
  }
}

TEST_F(LuceneParserTest, AllExcluded) {
  // -a -b → Required[Not(a), Not(b)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "-a -b").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(2, RequiredRoot().size());

  const auto& not1 = sdb::basics::downCast<irs::Not>(RequiredRoot()[0]);
  const auto* not1_or = not1.filter<irs::Or>();
  ASSERT_NE(nullptr, not1_or);
  AssertTerm((*not1_or)[0], "content", "a");

  const auto& not2 = sdb::basics::downCast<irs::Not>(RequiredRoot()[1]);
  const auto* not2_or = not2.filter<irs::Or>();
  ASSERT_NE(nullptr, not2_or);
  AssertTerm((*not2_or)[0], "content", "b");
}

TEST_F(LuceneParserTest, AllRequired) {
  // +a +b +c → Required[a, b, c]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+a +b +c").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(3, RequiredRoot().size());

  AssertTerm(RequiredRoot()[0], "content", "a");
  AssertTerm(RequiredRoot()[1], "content", "b");
  AssertTerm(RequiredRoot()[2], "content", "c");
}

TEST_F(LuceneParserTest, BoostedPhrase) {
  // "hello world"^2 → Optional[phrase^2]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "\"hello world\"^2").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertPhrase(OptionalRoot()[0], "content", 2.0f);
}

TEST_F(LuceneParserTest, BoostedPhraseFloat) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "\"hello world\"^1.5").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertPhrase(OptionalRoot()[0], "content", 1.5f);
}

TEST_F(LuceneParserTest, FieldWithBoost) {
  // title:hello^3 → Optional[title:hello^3]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:hello^3").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertTerm(OptionalRoot()[0], "title", "hello", 3.0f);
}

TEST_F(LuceneParserTest, FieldWithRange) {
  // date:[aaa TO zzz] → Optional[date:range]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "date:[aaa TO zzz]").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertRange(OptionalRoot()[0], "date", "aaa", irs::BoundType::Inclusive,
              "zzz", irs::BoundType::Inclusive);
}

TEST_F(LuceneParserTest, FieldWithExclusiveRange) {
  // price:{low TO high} → Optional[price:range exclusive]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "price:{low TO high}").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertRange(OptionalRoot()[0], "price", "low", irs::BoundType::Exclusive,
              "high", irs::BoundType::Exclusive);
}

TEST_F(LuceneParserTest, FieldWithGroupedAnd) {
  // title:(a AND b) → Optional[group(Required[title:a, title:b])]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:(a AND b)").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  const auto& group =
    sdb::basics::downCast<irs::MixedBooleanFilter>(OptionalRoot()[0]);
  ASSERT_TRUE(group.GetOptional().empty());
  const auto& req = group.GetRequired();
  ASSERT_EQ(2, req.size());

  AssertTerm(req[0], "title", "a");
  AssertTerm(req[1], "title", "b");
}

TEST_F(LuceneParserTest, NotGroup) {
  // NOT (a b) → Required[Not(group)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "NOT (a b)").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(1, RequiredRoot().size());

  const auto& not_filter = sdb::basics::downCast<irs::Not>(RequiredRoot()[0]);
  const auto* not_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  ASSERT_EQ(1, not_or->size());

  const auto& group =
    sdb::basics::downCast<irs::MixedBooleanFilter>((*not_or)[0]);
  ASSERT_EQ(2, group.GetOptional().size());
}

TEST_F(LuceneParserTest, BoostedFuzzy) {
  // hello~2^3 → Optional[fuzzy(hello, dist=2, boost=3)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello~2^3").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertFuzzy(OptionalRoot()[0], "content", "hello", 2, 3.0f);
}

TEST_F(LuceneParserTest, BoostedFuzzyFloat) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello~1^0.5").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertFuzzy(OptionalRoot()[0], "content", "hello", 1, 0.5f);
}

TEST_F(LuceneParserTest, FieldWithFuzzy) {
  // title:hello~1 → Optional[title:fuzzy(hello, 1)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:hello~1").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertFuzzy(OptionalRoot()[0], "title", "hello", 1);
}

TEST_F(LuceneParserTest, FieldWithPrefix) {
  // title:hel* → Optional[title:prefix(hel)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:hel*").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertPrefix(OptionalRoot()[0], "title", "hel");
}

TEST_F(LuceneParserTest, BoostedPrefix) {
  // hel*^2 → Optional[prefix(hel)^2]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hel*^2").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertPrefix(OptionalRoot()[0], "content", "hel", 2.0f);
}

TEST_F(LuceneParserTest, BoostedPrefixFloat) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hel*^0.8").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertPrefix(OptionalRoot()[0], "content", "hel", 0.8f);
}

TEST_F(LuceneParserTest, MixedAndImplicitOrAnd) {
  // a AND b c AND d → Required[a, b, c, d]
  // AND grabs its immediate neighbors; second AND also promotes c
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND b c AND d").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(4, RequiredRoot().size());

  AssertTerm(RequiredRoot()[0], "content", "a");
  AssertTerm(RequiredRoot()[1], "content", "b");
  AssertTerm(RequiredRoot()[2], "content", "c");
  AssertTerm(RequiredRoot()[3], "content", "d");
}

TEST_F(LuceneParserTest, PlusAndMinusGroup) {
  // +(a b) -(c d) → Required[group(a,b), Not(group(c,d))]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+(a b) -(c d)").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(2, RequiredRoot().size());

  const auto& group1 =
    sdb::basics::downCast<irs::MixedBooleanFilter>(RequiredRoot()[0]);
  ASSERT_EQ(2, group1.GetOptional().size());

  const auto& not_filter = sdb::basics::downCast<irs::Not>(RequiredRoot()[1]);
  const auto* not_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  ASSERT_EQ(1, not_or->size());
  const auto& group2 =
    sdb::basics::downCast<irs::MixedBooleanFilter>((*not_or)[0]);
  ASSERT_EQ(2, group2.GetOptional().size());
}

TEST_F(LuceneParserTest, FieldWithWildcard) {
  // title:h*llo → Optional[title:wildcard(h*llo)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:h*llo").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertWildcard(OptionalRoot()[0], "title", "h*llo");
}

TEST_F(LuceneParserTest, RangeWithUnboundedMax) {
  // [alpha TO *] → Optional[range(alpha, unbounded)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "[alpha TO *]").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertRange(OptionalRoot()[0], "content", "alpha", irs::BoundType::Inclusive,
              "", irs::BoundType::Unbounded);
}

TEST_F(LuceneParserTest, RangeFullyUnbounded) {
  // [* TO *] → Optional[range(unbounded, unbounded)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "[* TO *]").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertRange(OptionalRoot()[0], "content", "", irs::BoundType::Unbounded, "",
              irs::BoundType::Unbounded);
}

TEST_F(LuceneParserTest, MultipleFieldQueries) {
  // title:foo AND author:bar AND year:[start TO end]
  ASSERT_TRUE(
    sdb::ParseQuery(ctx, "title:foo AND author:bar AND year:[start TO end]")
      .ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(3, RequiredRoot().size());

  AssertTerm(RequiredRoot()[0], "title", "foo");

  AssertTerm(RequiredRoot()[1], "author", "bar");

  AssertRange(RequiredRoot()[2], "year", "start", irs::BoundType::Inclusive,
              "end", irs::BoundType::Inclusive);
}

TEST_F(LuceneParserTest, NestedGroupsWithModifiers) {
  // +(a (b OR c)) -d → Required[group, Not(d)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+(a (b OR c)) -d").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(2, RequiredRoot().size());

  const auto& group =
    sdb::basics::downCast<irs::MixedBooleanFilter>(RequiredRoot()[0]);
  const auto& group_or = group.GetOptional();
  ASSERT_EQ(2, group_or.size());

  AssertTerm(group_or[0], "content", "a");
  const auto& inner =
    sdb::basics::downCast<irs::MixedBooleanFilter>(group_or[1]);
  ASSERT_EQ(2, inner.GetOptional().size());

  const auto& not_filter = sdb::basics::downCast<irs::Not>(RequiredRoot()[1]);
  const auto* not_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  AssertTerm((*not_or)[0], "content", "d");
}

TEST_F(LuceneParserTest, PhraseWithSlop) {
  // "hello world"~3 → Optional[phrase with slop]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "\"hello world\"~3").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertPhrase(OptionalRoot()[0], "content");
}

TEST_F(LuceneParserTest, PhraseWithSlopAndBoost) {
  // "hello world"~3^2 → Optional[phrase with slop and boost]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "\"hello world\"~3^2").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertPhrase(OptionalRoot()[0], "content", 2.0f);
}

TEST_F(LuceneParserTest, FieldPhraseWithSlop) {
  // title:"hello world"~4 → Optional[title:phrase with slop]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:\"hello world\"~4").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertPhrase(OptionalRoot()[0], "title");
}

TEST_F(LuceneParserTest, AndOrChain) {
  // a AND b OR c AND d → Required[a, b, c, d]
  // First AND promotes a,b; OR leaves c in Optional; second AND promotes c,d
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND b OR c AND d").ok());
  // After "a AND b": Required[a, b], Optional[]
  // After "OR c": Required[a, b], Optional[c]
  // After "AND d": Required[a, b, c, d], Optional[]
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(4, RequiredRoot().size());

  AssertTerm(RequiredRoot()[0], "content", "a");
  AssertTerm(RequiredRoot()[1], "content", "b");
  AssertTerm(RequiredRoot()[2], "content", "c");
  AssertTerm(RequiredRoot()[3], "content", "d");
}

TEST_F(LuceneParserTest, ParseError_TrailingPlus) {
  auto result = sdb::ParseQuery(ctx, "hello +");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_TrailingMinus) {
  auto result = sdb::ParseQuery(ctx, "hello -");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_DoubleOR) {
  auto result = sdb::ParseQuery(ctx, "hello OR OR world");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_AndOr) {
  auto result = sdb::ParseQuery(ctx, "hello AND OR world");
  ASSERT_TRUE(result.fail());
  EXPECT_NE(std::string::npos, result.errorMessage().find("syntax error"));
}

TEST_F(LuceneParserTest, QuestionMarkWildcard) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "Te?m").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertWildcard(OptionalRoot()[0], "content", "Te?m");
}

TEST_F(LuceneParserTest, MultipleQuestionMarkWildcard) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "T??m").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertWildcard(OptionalRoot()[0], "content", "T??m");
}

TEST_F(LuceneParserTest, FieldQuestionMarkWildcard) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:Te?m").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertWildcard(OptionalRoot()[0], "title", "Te?m");
}

TEST_F(LuceneParserTest, SuffixQuery) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "*suffix").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertWildcard(OptionalRoot()[0], "content", "*suffix");
}

TEST_F(LuceneParserTest, FieldSuffixQuery) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:*suffix").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertWildcard(OptionalRoot()[0], "title", "*suffix");
}

TEST_F(LuceneParserTest, EscapedMinus) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a\\-b").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertTerm(OptionalRoot()[0], "content", "a\\-b");
}

TEST_F(LuceneParserTest, EscapedColon) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a\\:b").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertTerm(OptionalRoot()[0], "content", "a\\:b");
}

TEST_F(LuceneParserTest, EscapedStar) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a\\*b").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertTerm(OptionalRoot()[0], "content", "a\\*b");
}

TEST_F(LuceneParserTest, DoubleAmpersandAnd) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello && world").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(2, RequiredRoot().size());

  AssertTerm(RequiredRoot()[0], "content", "hello");
  AssertTerm(RequiredRoot()[1], "content", "world");
}

TEST_F(LuceneParserTest, DoublePipeOr) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello || world").ok());
  ASSERT_EQ(2, OptionalRoot().size());
  AssertTerm(OptionalRoot()[0], "content", "hello");
  AssertTerm(OptionalRoot()[1], "content", "world");
}

TEST_F(LuceneParserTest, ExclamationNot) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "!hello").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(1, RequiredRoot().size());

  const auto& not_filter = sdb::basics::downCast<irs::Not>(RequiredRoot()[0]);
  const auto* not_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  ASSERT_EQ(1, not_or->size());
  AssertTerm((*not_or)[0], "content", "hello");
}

TEST_F(LuceneParserTest, BoostedRange) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "[a TO z]^2").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertRange(OptionalRoot()[0], "content", "a", irs::BoundType::Inclusive, "z",
              irs::BoundType::Inclusive, 2.0f);
}

TEST_F(LuceneParserTest, BoostedRangeFloat) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "[a TO z]^0.5").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertRange(OptionalRoot()[0], "content", "a", irs::BoundType::Inclusive, "z",
              irs::BoundType::Inclusive, 0.5f);
}

TEST_F(LuceneParserTest, BoostedWildcard) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "h*llo^2").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertWildcard(OptionalRoot()[0], "content", "h*llo", 2.0f);
}

TEST_F(LuceneParserTest, BoostedWildcardFloat) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "h*llo^1.7").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertWildcard(OptionalRoot()[0], "content", "h*llo", 1.7f);
}

TEST_F(LuceneParserTest, TabSeparator) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello\tworld").ok());
  ASSERT_EQ(2, OptionalRoot().size());
  AssertTerm(OptionalRoot()[0], "content", "hello");
  AssertTerm(OptionalRoot()[1], "content", "world");
}

TEST_F(LuceneParserTest, NewlineSeparator) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello\nworld").ok());
  ASSERT_EQ(2, OptionalRoot().size());
  AssertTerm(OptionalRoot()[0], "content", "hello");
  AssertTerm(OptionalRoot()[1], "content", "world");
}

TEST_F(LuceneParserTest, TermStartingWithDigits) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "2024abc").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertTerm(OptionalRoot()[0], "content", "2024abc");
}

TEST_F(LuceneParserTest, RangeMixedBraceToSquare) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "{alpha TO omega]").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertRange(OptionalRoot()[0], "content", "alpha", irs::BoundType::Exclusive,
              "omega", irs::BoundType::Inclusive);
}

TEST_F(LuceneParserTest, RangeAndTerm) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "[a TO z] AND foo").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(2, RequiredRoot().size());

  AssertRange(RequiredRoot()[0], "content", "a", irs::BoundType::Inclusive, "z",
              irs::BoundType::Inclusive);

  AssertTerm(RequiredRoot()[1], "content", "foo");
}

TEST_F(LuceneParserTest, SingleCharTerm) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  AssertTerm(OptionalRoot()[0], "content", "a");
}

TEST_F(LuceneParserTest, ParseError_StandaloneNumber) {
  auto result = sdb::ParseQuery(ctx, "123");
  ASSERT_TRUE(result.fail());
}

TEST_F(LuceneParserTest, ParseError_EmptyQuery) {
  auto result = sdb::ParseQuery(ctx, "");
  ASSERT_TRUE(result.fail());
}

TEST_F(LuceneParserTest, ParseError_WhitespaceOnly) {
  auto result = sdb::ParseQuery(ctx, "   ");
  ASSERT_TRUE(result.fail());
}

TEST_F(LuceneParserTest, FieldRestoresAfterSingleTerm) {
  // title:hello world → hello=title, world=content (default)
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:hello world").ok());
  ASSERT_EQ(2, OptionalRoot().size());
  AssertTerm(OptionalRoot()[0], "title", "hello");

  AssertTerm(OptionalRoot()[1], "content", "world");
}

TEST_F(LuceneParserTest, FieldScopeWithAnd) {
  // title:a AND b → a=title, b=content; both Required
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:a AND b").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(2, RequiredRoot().size());

  AssertTerm(RequiredRoot()[0], "title", "a");

  AssertTerm(RequiredRoot()[1], "content", "b");
}

TEST_F(LuceneParserTest, DifferentFieldsWithAnd) {
  // title:a AND author:b → a=title, b=author; both Required
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:a AND author:b").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(2, RequiredRoot().size());

  AssertTerm(RequiredRoot()[0], "title", "a");

  AssertTerm(RequiredRoot()[1], "author", "b");
}

TEST_F(LuceneParserTest, TwoAndGroupsOrd) {
  // (a AND b) OR (c AND d) → Optional[group(Req[a,b]), group(Req[c,d])]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(a AND b) OR (c AND d)").ok());
  ASSERT_EQ(2, OptionalRoot().size());
  const auto& g1 =
    sdb::basics::downCast<irs::MixedBooleanFilter>(OptionalRoot()[0]);
  ASSERT_TRUE(g1.GetOptional().empty());
  ASSERT_EQ(2, g1.GetRequired().size());
  AssertTerm(g1.GetRequired()[0], "content", "a");
  AssertTerm(g1.GetRequired()[1], "content", "b");

  const auto& g2 =
    sdb::basics::downCast<irs::MixedBooleanFilter>(OptionalRoot()[1]);
  ASSERT_TRUE(g2.GetOptional().empty());
  ASSERT_EQ(2, g2.GetRequired().size());
  AssertTerm(g2.GetRequired()[0], "content", "c");
  AssertTerm(g2.GetRequired()[1], "content", "d");
}

TEST_F(LuceneParserTest, NotAndGroup) {
  // NOT (a AND b) → Required[Not(group(Req[a,b]))]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "NOT (a AND b)").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(1, RequiredRoot().size());

  const auto& not_filter = sdb::basics::downCast<irs::Not>(RequiredRoot()[0]);
  const auto* not_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  ASSERT_EQ(1, not_or->size());

  const auto& group =
    sdb::basics::downCast<irs::MixedBooleanFilter>((*not_or)[0]);
  ASSERT_TRUE(group.GetOptional().empty());
  ASSERT_EQ(2, group.GetRequired().size());
}

TEST_F(LuceneParserTest, ModifiersInsideFieldGroup) {
  // field:(+a -b c) → group with a=Required, Not(b)=Required, c=Optional
  ASSERT_TRUE(sdb::ParseQuery(ctx, "field:(+a -b c)").ok());
  ASSERT_EQ(1, OptionalRoot().size());
  const auto& group =
    sdb::basics::downCast<irs::MixedBooleanFilter>(OptionalRoot()[0]);
  ASSERT_EQ(1, group.GetOptional().size());
  ASSERT_EQ(2, group.GetRequired().size());

  AssertTerm(group.GetOptional()[0], "field", "c");

  AssertTerm(group.GetRequired()[0], "field", "a");

  const auto& not_filter =
    sdb::basics::downCast<irs::Not>(group.GetRequired()[1]);
  const auto* not_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  AssertTerm((*not_or)[0], "field", "b");
}

TEST_F(LuceneParserTest, AndWithGroupInMiddle) {
  // a AND (b OR c) AND d → Required[a, group(Opt[b,c]), d]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND (b OR c) AND d").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(3, RequiredRoot().size());

  AssertTerm(RequiredRoot()[0], "content", "a");

  const auto& group =
    sdb::basics::downCast<irs::MixedBooleanFilter>(RequiredRoot()[1]);
  ASSERT_EQ(2, group.GetOptional().size());

  AssertTerm(RequiredRoot()[2], "content", "d");
}

TEST_F(LuceneParserTest, DeeplyNestedFieldGroups) {
  // title:(author:(a b) c) d
  // a,b = author field inside inner group
  // c = title field in outer group
  // d = default content field
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:(author:(a b) c) d").ok());
  // First: outer group (title-scoped)
  ASSERT_EQ(2, OptionalRoot().size());
  const auto& outer =
    sdb::basics::downCast<irs::MixedBooleanFilter>(OptionalRoot()[0]);
  ASSERT_TRUE(outer.GetRequired().empty());
  ASSERT_EQ(2, outer.GetOptional().size());

  // Inner group (author-scoped)
  const auto& inner =
    sdb::basics::downCast<irs::MixedBooleanFilter>(outer.GetOptional()[0]);
  ASSERT_TRUE(inner.GetRequired().empty());
  ASSERT_EQ(2, inner.GetOptional().size());
  AssertTerm(inner.GetOptional()[0], "author", "a");
  AssertTerm(inner.GetOptional()[1], "author", "b");

  AssertTerm(outer.GetOptional()[1], "title", "c");

  // Second: d with default field
  AssertTerm(OptionalRoot()[1], "content", "d");
}

TEST_F(LuceneParserTest, ThreeLevelNestedGroups) {
  // ((a AND b) OR c) AND d
  // Inner group: Req[a,b]. Middle group: Opt[inner, c]. AND promotes middle+d.
  ASSERT_TRUE(sdb::ParseQuery(ctx, "((a AND b) OR c) AND d").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(2, RequiredRoot().size());

  const auto& middle =
    sdb::basics::downCast<irs::MixedBooleanFilter>(RequiredRoot()[0]);
  ASSERT_EQ(2, middle.GetOptional().size());

  const auto& inner =
    sdb::basics::downCast<irs::MixedBooleanFilter>(middle.GetOptional()[0]);
  ASSERT_TRUE(inner.GetOptional().empty());
  ASSERT_EQ(2, inner.GetRequired().size());
  AssertTerm(inner.GetRequired()[0], "content", "a");
  AssertTerm(inner.GetRequired()[1], "content", "b");

  AssertTerm(middle.GetOptional()[1], "content", "c");

  AssertTerm(RequiredRoot()[1], "content", "d");
}

TEST_F(LuceneParserTest, NestedGroupsWithMixedOperators) {
  // (+(a b) AND (c OR d)) OR e
  // Inner: +group(a,b) AND group(c,d) → all Required in outer group
  // Then OR e at top level
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(+(a b) AND (c OR d)) OR e").ok());
  ASSERT_EQ(2, OptionalRoot().size());
  const auto& outer =
    sdb::basics::downCast<irs::MixedBooleanFilter>(OptionalRoot()[0]);
  ASSERT_TRUE(outer.GetOptional().empty());
  ASSERT_EQ(2, outer.GetRequired().size());

  const auto& g1 =
    sdb::basics::downCast<irs::MixedBooleanFilter>(outer.GetRequired()[0]);
  ASSERT_EQ(2, g1.GetOptional().size());
  AssertTerm(g1.GetOptional()[0], "content", "a");
  AssertTerm(g1.GetOptional()[1], "content", "b");

  const auto& g2 =
    sdb::basics::downCast<irs::MixedBooleanFilter>(outer.GetRequired()[1]);
  ASSERT_EQ(2, g2.GetOptional().size());
  AssertTerm(g2.GetOptional()[0], "content", "c");
  AssertTerm(g2.GetOptional()[1], "content", "d");

  AssertTerm(OptionalRoot()[1], "content", "e");
}

TEST_F(LuceneParserTest, DeeplyNestedNotGroups) {
  // NOT (NOT (a AND b))
  // Outer NOT wraps a group that contains inner NOT wrapping AND group
  ASSERT_TRUE(sdb::ParseQuery(ctx, "NOT (NOT (a AND b))").ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(1, RequiredRoot().size());

  // Outer: Not(group)
  const auto& outer_not = sdb::basics::downCast<irs::Not>(RequiredRoot()[0]);
  const auto* outer_or = outer_not.filter<irs::Or>();
  ASSERT_NE(nullptr, outer_or);
  ASSERT_EQ(1, outer_or->size());

  // Middle group contains Required[Not(inner_group)]
  const auto& middle =
    sdb::basics::downCast<irs::MixedBooleanFilter>((*outer_or)[0]);
  ASSERT_TRUE(middle.GetOptional().empty());
  ASSERT_EQ(1, middle.GetRequired().size());

  const auto& inner_not =
    sdb::basics::downCast<irs::Not>(middle.GetRequired()[0]);
  const auto* inner_or = inner_not.filter<irs::Or>();
  ASSERT_NE(nullptr, inner_or);
  ASSERT_EQ(1, inner_or->size());

  const auto& inner =
    sdb::basics::downCast<irs::MixedBooleanFilter>((*inner_or)[0]);
  ASSERT_TRUE(inner.GetOptional().empty());
  ASSERT_EQ(2, inner.GetRequired().size());
  AssertTerm(inner.GetRequired()[0], "content", "a");
  AssertTerm(inner.GetRequired()[1], "content", "b");
}

TEST_F(LuceneParserTest, ComplexMultiFieldNested) {
  // title:(+hello -world) AND author:(foo OR bar)^2
  ASSERT_TRUE(
    sdb::ParseQuery(ctx, "title:(+hello -world) AND author:(foo OR bar)^2")
      .ok());
  ASSERT_TRUE(OptionalRoot().empty());
  ASSERT_EQ(2, RequiredRoot().size());

  const auto& g1 =
    sdb::basics::downCast<irs::MixedBooleanFilter>(RequiredRoot()[0]);
  ASSERT_TRUE(g1.GetOptional().empty());
  ASSERT_EQ(2, g1.GetRequired().size());
  AssertTerm(g1.GetRequired()[0], "title", "hello");
  const auto& not_world = sdb::basics::downCast<irs::Not>(g1.GetRequired()[1]);
  const auto* nw_or = not_world.filter<irs::Or>();
  ASSERT_NE(nullptr, nw_or);
  AssertTerm((*nw_or)[0], "title", "world");

  const auto& g2 =
    sdb::basics::downCast<irs::MixedBooleanFilter>(RequiredRoot()[1]);
  EXPECT_FLOAT_EQ(2.0f, g2.Boost());
  ASSERT_EQ(2, g2.GetOptional().size());
  AssertTerm(g2.GetOptional()[0], "author", "foo");
  AssertTerm(g2.GetOptional()[1], "author", "bar");
}

}  // namespace
