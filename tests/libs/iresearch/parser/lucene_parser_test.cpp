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

#include <iresearch/analysis/segmentation_tokenizer.hpp>
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
  irs::analysis::SegmentationTokenizer::ptr tokenizer{
    irs::analysis::SegmentationTokenizer::make(
      irs::analysis::SegmentationTokenizer::Options{})};

  sdb::ParserContext ctx{root, "content", *tokenizer};

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
  EXPECT_EQ("h*llo",
            irs::ViewCast<char>(irs::bytes_view{wildcard.options().term}));
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
  EXPECT_EQ("hello",
            irs::ViewCast<char>(irs::bytes_view{fuzzy.options().term}));
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
  EXPECT_EQ("alpha",
            irs::ViewCast<char>(irs::bytes_view{range.options().range.min}));
  EXPECT_EQ("omega",
            irs::ViewCast<char>(irs::bytes_view{range.options().range.max}));
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
  EXPECT_EQ("hello",
            irs::ViewCast<char>(irs::bytes_view{term1.options().term}));

  const auto& term2 = GetFilter<irs::ByTerm>(1);
  EXPECT_EQ("world",
            irs::ViewCast<char>(irs::bytes_view{term2.options().term}));
}

TEST_F(LuceneParserTest, ExplicitOr) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello OR world").ok());
  ASSERT_EQ(2, root.size());

  const auto& term1 = GetFilter<irs::ByTerm>(0);
  EXPECT_EQ("hello",
            irs::ViewCast<char>(irs::bytes_view{term1.options().term}));

  const auto& term2 = GetFilter<irs::ByTerm>(1);
  EXPECT_EQ("world",
            irs::ViewCast<char>(irs::bytes_view{term2.options().term}));
}

TEST_F(LuceneParserTest, AndOperator) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello AND world").ok());
  ASSERT_EQ(1, root.size());

  // And containing both terms
  const auto& and_filter = GetFilter<irs::And>(0);
  ASSERT_EQ(2, and_filter.size());

  auto it = and_filter.begin();
  const auto& term1 = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("content", term1.field());
  EXPECT_EQ("hello",
            irs::ViewCast<char>(irs::bytes_view{term1.options().term}));

  ++it;
  const auto& term2 = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("content", term2.field());
  EXPECT_EQ("world",
            irs::ViewCast<char>(irs::bytes_view{term2.options().term}));
}

TEST_F(LuceneParserTest, ChainedAndOperator) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND b AND c").ok());
  ASSERT_EQ(1, root.size());

  // Outer And containing "a" and inner And
  const auto& outer_and = GetFilter<irs::And>(0);
  ASSERT_EQ(2, outer_and.size());

  auto it = outer_and.begin();
  const auto& inner_and = sdb::basics::downCast<irs::And>(**it);
  ASSERT_EQ(2, inner_and.size());

  auto inner_it = inner_and.begin();
  const auto& term_a = sdb::basics::downCast<irs::ByTerm>(**inner_it);
  EXPECT_EQ("a", irs::ViewCast<char>(irs::bytes_view{term_a.options().term}));

  ++inner_it;
  const auto& term_b = sdb::basics::downCast<irs::ByTerm>(**inner_it);
  EXPECT_EQ("b", irs::ViewCast<char>(irs::bytes_view{term_b.options().term}));

  ++it;
  const auto& term_c = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("c", irs::ViewCast<char>(irs::bytes_view{term_c.options().term}));
}

TEST_F(LuceneParserTest, MixedPlusMinusOperators) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+foo -bar +foobar -foobaz").ok());
  ASSERT_EQ(1, root.size());

  // All wrapped in And: [foo, Not(bar), foobar, Not(foobaz)]
  const auto& and_filter = GetFilter<irs::And>(0);
  ASSERT_EQ(4, and_filter.size());

  auto it = and_filter.begin();

  // First: foo
  const auto& term1 = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("foo", irs::ViewCast<char>(irs::bytes_view{term1.options().term}));

  // Second: Not(bar)
  ++it;
  const auto& not1 = sdb::basics::downCast<irs::Not>(**it);
  const auto* not1_or = not1.filter<irs::Or>();
  ASSERT_NE(nullptr, not1_or);
  ASSERT_EQ(1, not1_or->size());
  const auto& term2 = sdb::basics::downCast<irs::ByTerm>(**not1_or->begin());
  EXPECT_EQ("bar", irs::ViewCast<char>(irs::bytes_view{term2.options().term}));

  // Third: foobar
  ++it;
  const auto& term3 = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("foobar",
            irs::ViewCast<char>(irs::bytes_view{term3.options().term}));

  // Fourth: Not(foobaz)
  ++it;
  const auto& not2 = sdb::basics::downCast<irs::Not>(**it);
  const auto* not2_or = not2.filter<irs::Or>();
  ASSERT_NE(nullptr, not2_or);
  ASSERT_EQ(1, not2_or->size());
  const auto& term4 = sdb::basics::downCast<irs::ByTerm>(**not2_or->begin());
  EXPECT_EQ("foobaz",
            irs::ViewCast<char>(irs::bytes_view{term4.options().term}));
}

TEST_F(LuceneParserTest, MixedPlusMinusWithImplicitOr) {
  // +foo bar -baz +foobar foobaz
  // Once + is used, all subsequent terms go into the required And
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+foo bar -baz +foobar foobaz").ok());
  ASSERT_EQ(1, root.size());

  // Everything wrapped in And: [foo, bar, Not(baz), foobar, foobaz]
  const auto& and_filter = GetFilter<irs::And>(0);
  ASSERT_EQ(5, and_filter.size());

  auto it = and_filter.begin();

  // First: foo
  const auto& term1 = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("foo", irs::ViewCast<char>(irs::bytes_view{term1.options().term}));

  // Second: bar (implicit OR term, but added to And because required_and is
  // active)
  ++it;
  const auto& term2 = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("bar", irs::ViewCast<char>(irs::bytes_view{term2.options().term}));

  // Third: Not(baz)
  ++it;
  const auto& not_filter = sdb::basics::downCast<irs::Not>(**it);
  const auto* not_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  ASSERT_EQ(1, not_or->size());
  const auto& term3 = sdb::basics::downCast<irs::ByTerm>(**not_or->begin());
  EXPECT_EQ("baz", irs::ViewCast<char>(irs::bytes_view{term3.options().term}));

  // Fourth: foobar
  ++it;
  const auto& term4 = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("foobar",
            irs::ViewCast<char>(irs::bytes_view{term4.options().term}));

  // Fifth: foobaz (implicit OR term, also in And)
  ++it;
  const auto& term5 = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("foobaz",
            irs::ViewCast<char>(irs::bytes_view{term5.options().term}));
}

TEST_F(LuceneParserTest, DeepNestedGroups) {
  // (a AND (b OR (c AND d)))
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(a AND (b OR (c AND d)))").ok());
  ASSERT_EQ(1, root.size());

  // Outer group creates an Or
  const auto& outer_or = GetFilter<irs::Or>(0);

  // The AND pops "a" and creates And[a, ...]
  ASSERT_EQ(1, outer_or.size());
  const auto& outer_and = sdb::basics::downCast<irs::And>(**outer_or.begin());
  ASSERT_EQ(2, outer_and.size());

  auto it = outer_and.begin();
  const auto& term_a = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("a", irs::ViewCast<char>(irs::bytes_view{term_a.options().term}));

  // Second element of And is the group (b OR (c AND d))
  ++it;
  const auto& middle_or = sdb::basics::downCast<irs::Or>(**it);

  // Inside: b, and (c AND d)
  ASSERT_EQ(2, middle_or.size());

  auto mid_it = middle_or.begin();
  const auto& term_b = sdb::basics::downCast<irs::ByTerm>(**mid_it);
  EXPECT_EQ("b", irs::ViewCast<char>(irs::bytes_view{term_b.options().term}));

  ++mid_it;
  const auto& inner_or = sdb::basics::downCast<irs::Or>(**mid_it);
  ASSERT_EQ(1, inner_or.size());

  const auto& inner_and = sdb::basics::downCast<irs::And>(**inner_or.begin());
  ASSERT_EQ(2, inner_and.size());

  auto inner_it = inner_and.begin();
  const auto& term_c = sdb::basics::downCast<irs::ByTerm>(**inner_it);
  EXPECT_EQ("c", irs::ViewCast<char>(irs::bytes_view{term_c.options().term}));

  ++inner_it;
  const auto& term_d = sdb::basics::downCast<irs::ByTerm>(**inner_it);
  EXPECT_EQ("d", irs::ViewCast<char>(irs::bytes_view{term_d.options().term}));
}

TEST_F(LuceneParserTest, GroupsWithAndOr) {
  // (a b) AND (c d) - two groups connected by AND
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(a b) AND (c d)").ok());
  ASSERT_EQ(1, root.size());

  const auto& and_filter = GetFilter<irs::And>(0);
  ASSERT_EQ(2, and_filter.size());

  auto it = and_filter.begin();
  const auto& group1 = sdb::basics::downCast<irs::Or>(**it);
  ASSERT_EQ(2, group1.size());

  auto g1_it = group1.begin();
  const auto& term_a = sdb::basics::downCast<irs::ByTerm>(**g1_it);
  EXPECT_EQ("a", irs::ViewCast<char>(irs::bytes_view{term_a.options().term}));
  ++g1_it;
  const auto& term_b = sdb::basics::downCast<irs::ByTerm>(**g1_it);
  EXPECT_EQ("b", irs::ViewCast<char>(irs::bytes_view{term_b.options().term}));

  ++it;
  const auto& group2 = sdb::basics::downCast<irs::Or>(**it);
  ASSERT_EQ(2, group2.size());

  auto g2_it = group2.begin();
  const auto& term_c = sdb::basics::downCast<irs::ByTerm>(**g2_it);
  EXPECT_EQ("c", irs::ViewCast<char>(irs::bytes_view{term_c.options().term}));
  ++g2_it;
  const auto& term_d = sdb::basics::downCast<irs::ByTerm>(**g2_it);
  EXPECT_EQ("d", irs::ViewCast<char>(irs::bytes_view{term_d.options().term}));
}

TEST_F(LuceneParserTest, PlusMinusWithGroups) {
  // +(foo bar) -baz - required group, excluded term
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+(foo bar) -baz").ok());
  ASSERT_EQ(1, root.size());

  const auto& and_filter = GetFilter<irs::And>(0);
  ASSERT_EQ(2, and_filter.size());

  auto it = and_filter.begin();

  // First: group (foo bar)
  const auto& group = sdb::basics::downCast<irs::Or>(**it);
  ASSERT_EQ(2, group.size());

  auto g_it = group.begin();
  const auto& term_foo = sdb::basics::downCast<irs::ByTerm>(**g_it);
  EXPECT_EQ("foo",
            irs::ViewCast<char>(irs::bytes_view{term_foo.options().term}));
  ++g_it;
  const auto& term_bar = sdb::basics::downCast<irs::ByTerm>(**g_it);
  EXPECT_EQ("bar",
            irs::ViewCast<char>(irs::bytes_view{term_bar.options().term}));

  // Second: Not(baz)
  ++it;
  const auto& not_filter = sdb::basics::downCast<irs::Not>(**it);
  const auto* not_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  ASSERT_EQ(1, not_or->size());
  const auto& term_baz = sdb::basics::downCast<irs::ByTerm>(**not_or->begin());
  EXPECT_EQ("baz",
            irs::ViewCast<char>(irs::bytes_view{term_baz.options().term}));
}

TEST_F(LuceneParserTest, FieldWithPlusMinusGroups) {
  // +title:(hello world) -author:john
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+title:(hello world) -author:john").ok());
  ASSERT_EQ(1, root.size());

  const auto& and_filter = GetFilter<irs::And>(0);
  ASSERT_EQ(2, and_filter.size());

  auto it = and_filter.begin();

  // First: group with title field
  const auto& group = sdb::basics::downCast<irs::Or>(**it);
  ASSERT_EQ(2, group.size());

  auto g_it = group.begin();
  const auto& term_hello = sdb::basics::downCast<irs::ByTerm>(**g_it);
  EXPECT_EQ("title", term_hello.field());
  EXPECT_EQ("hello",
            irs::ViewCast<char>(irs::bytes_view{term_hello.options().term}));
  ++g_it;
  const auto& term_world = sdb::basics::downCast<irs::ByTerm>(**g_it);
  EXPECT_EQ("title", term_world.field());
  EXPECT_EQ("world",
            irs::ViewCast<char>(irs::bytes_view{term_world.options().term}));

  // Second: Not(author:john)
  ++it;
  const auto& not_filter = sdb::basics::downCast<irs::Not>(**it);
  const auto* not_or = not_filter.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  ASSERT_EQ(1, not_or->size());
  const auto& term_john = sdb::basics::downCast<irs::ByTerm>(**not_or->begin());
  EXPECT_EQ("author", term_john.field());
  EXPECT_EQ("john",
            irs::ViewCast<char>(irs::bytes_view{term_john.options().term}));
}

TEST_F(LuceneParserTest, ComplexMixedQuery) {
  // (a OR b) AND +(c d) -e
  // Note: AND only takes single clause +(c d), then -e is separate at top level
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(a OR b) AND +(c d) -e").ok());
  ASSERT_EQ(2, root.size());

  // First: And[(a OR b), And[(c d)]]
  const auto& outer_and = GetFilter<irs::And>(0);
  ASSERT_EQ(2, outer_and.size());

  auto it = outer_and.begin();
  const auto& group_ab = sdb::basics::downCast<irs::Or>(**it);
  ASSERT_EQ(2, group_ab.size());

  ++it;
  const auto& inner_and = sdb::basics::downCast<irs::And>(**it);
  ASSERT_EQ(1, inner_and.size());

  const auto& group_cd = sdb::basics::downCast<irs::Or>(**inner_and.begin());
  ASSERT_EQ(2, group_cd.size());

  // Second: Not(e) at root level
  const auto& not_e = GetFilter<irs::Not>(1);
  const auto* not_or = not_e.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  ASSERT_EQ(1, not_or->size());
  const auto& term_e = sdb::basics::downCast<irs::ByTerm>(**not_or->begin());
  EXPECT_EQ("e", irs::ViewCast<char>(irs::bytes_view{term_e.options().term}));
}

TEST_F(LuceneParserTest, ComplexMixedQueryGrouped) {
  // Use parentheses to group +(c d) -e together after AND
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(a OR b) AND (+(c d) -e)").ok());
  ASSERT_EQ(1, root.size());

  // Structure: And[(a OR b), Or[And[(c d), Not(e)]]]
  const auto& outer_and = GetFilter<irs::And>(0);
  ASSERT_EQ(2, outer_and.size());

  auto it = outer_and.begin();

  // First: group (a OR b)
  const auto& group_ab = sdb::basics::downCast<irs::Or>(**it);
  ASSERT_EQ(2, group_ab.size());

  auto ab_it = group_ab.begin();
  const auto& term_a = sdb::basics::downCast<irs::ByTerm>(**ab_it);
  EXPECT_EQ("a", irs::ViewCast<char>(irs::bytes_view{term_a.options().term}));
  ++ab_it;
  const auto& term_b = sdb::basics::downCast<irs::ByTerm>(**ab_it);
  EXPECT_EQ("b", irs::ViewCast<char>(irs::bytes_view{term_b.options().term}));

  // Second: group containing And[(c d), Not(e)]
  ++it;
  const auto& group2 = sdb::basics::downCast<irs::Or>(**it);
  ASSERT_EQ(1, group2.size());

  const auto& inner_and = sdb::basics::downCast<irs::And>(**group2.begin());
  ASSERT_EQ(2, inner_and.size());

  auto inner_it = inner_and.begin();
  const auto& group_cd = sdb::basics::downCast<irs::Or>(**inner_it);
  ASSERT_EQ(2, group_cd.size());

  auto cd_it = group_cd.begin();
  const auto& term_c = sdb::basics::downCast<irs::ByTerm>(**cd_it);
  EXPECT_EQ("c", irs::ViewCast<char>(irs::bytes_view{term_c.options().term}));
  ++cd_it;
  const auto& term_d = sdb::basics::downCast<irs::ByTerm>(**cd_it);
  EXPECT_EQ("d", irs::ViewCast<char>(irs::bytes_view{term_d.options().term}));

  ++inner_it;
  const auto& not_e = sdb::basics::downCast<irs::Not>(**inner_it);
  const auto* not_or = not_e.filter<irs::Or>();
  ASSERT_NE(nullptr, not_or);
  ASSERT_EQ(1, not_or->size());
  const auto& term_e = sdb::basics::downCast<irs::ByTerm>(**not_or->begin());
  EXPECT_EQ("e", irs::ViewCast<char>(irs::bytes_view{term_e.options().term}));
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
  EXPECT_EQ("hello",
            irs::ViewCast<char>(irs::bytes_view{term1.options().term}));

  ++it;
  const auto& term2 = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("content", term2.field());
  EXPECT_EQ("world",
            irs::ViewCast<char>(irs::bytes_view{term2.options().term}));
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
  EXPECT_EQ("hello",
            irs::ViewCast<char>(irs::bytes_view{term1.options().term}));

  ++it;
  const auto& term2 = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("title", term2.field());
  EXPECT_EQ("world",
            irs::ViewCast<char>(irs::bytes_view{term2.options().term}));
}

TEST_F(LuceneParserTest, NestedFieldQuery) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:hello author:world").ok());
  ASSERT_EQ(2, root.size());

  const auto& term1 = GetFilter<irs::ByTerm>(0);
  EXPECT_EQ("title", term1.field());
  EXPECT_EQ("hello",
            irs::ViewCast<char>(irs::bytes_view{term1.options().term}));

  const auto& term2 = GetFilter<irs::ByTerm>(1);
  EXPECT_EQ("author", term2.field());
  EXPECT_EQ("world",
            irs::ViewCast<char>(irs::bytes_view{term2.options().term}));
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
  ASSERT_EQ(1, root.size());

  // And containing both terms
  const auto& and_filter = GetFilter<irs::And>(0);
  ASSERT_EQ(2, and_filter.size());

  auto it = and_filter.begin();
  const auto& term = sdb::basics::downCast<irs::ByTerm>(**it);
  EXPECT_EQ("title", term.field());
  EXPECT_EQ("hello", irs::ViewCast<char>(irs::bytes_view{term.options().term}));
  EXPECT_FLOAT_EQ(2.0f, term.Boost());

  ++it;
  const auto& fuzzy = sdb::basics::downCast<irs::ByEditDistance>(**it);
  EXPECT_EQ("content", fuzzy.field());
  EXPECT_EQ("world",
            irs::ViewCast<char>(irs::bytes_view{fuzzy.options().term}));
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
