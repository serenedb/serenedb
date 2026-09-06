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

#include <string>

#include "basics/down_cast.h"
#include "iresearch/analysis/segmentation_tokenizer.hpp"
#include "iresearch/parser/parser.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/range_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/wildcard_filter.hpp"
#include "iresearch/utils/string.hpp"

namespace {

// Default field id used by the test fixture's ParserContext. The parser no
// longer maps the `field:` prefix to per-field ids (fields are
// catalog-allocated post-rewrite), so every emitted filter carries the
// context's default field id regardless of any literal field name in the query
// string.
constexpr irs::field_id kFieldId = 1;

const irs::BooleanFilter& AsBoolean(const irs::Filter& f) {
  return sdb::basics::downCast<irs::BooleanFilter>(f);
}

const irs::Clauses& SubOptional(const irs::Filter& f) {
  return AsBoolean(f).Bucket(irs::Occur::Should);
}

const irs::Clauses& SubRequired(const irs::Filter& f) {
  return AsBoolean(f).Bucket(irs::Occur::Must);
}

const irs::Clauses& SubExcluded(const irs::Filter& f) {
  return AsBoolean(f).Bucket(irs::Occur::MustNot);
}

void AssertTerm(const irs::TermClause& clause, irs::field_id field,
                std::string_view value, float boost = 0.0f) {
  EXPECT_EQ(field, clause.field);
  EXPECT_EQ(value, irs::ViewCast<char>(irs::bytes_view{clause.term}));
  if (boost > 0.0f) {
    EXPECT_FLOAT_EQ(boost, clause.boost);
  }
}

void AssertPhrase(const irs::Filter& f, irs::field_id field, float boost = 0.0f,
                  irs::PosAttr::value_t slop = 0) {
  const auto& phrase = sdb::basics::downCast<irs::ByPhrase>(f);
  EXPECT_EQ(field, phrase.field_id());
  if (boost > 0.0f) {
    EXPECT_FLOAT_EQ(boost, phrase.GetBoost());
  }
  EXPECT_EQ(slop, phrase.options().slop());
}

void AssertPrefix(const irs::Filter& f, irs::field_id field,
                  std::string_view value, float boost = 0.0f) {
  const auto& prefix = sdb::basics::downCast<irs::ByPrefix>(f);
  EXPECT_EQ(field, prefix.field_id());
  EXPECT_EQ(value, irs::ViewCast<char>(irs::bytes_view{prefix.options().term}));
  if (boost > 0.0f) {
    EXPECT_FLOAT_EQ(boost, prefix.GetBoost());
  }
}

void AssertWildcard(const irs::Filter& f, irs::field_id field,
                    std::string_view value, float boost = 0.0f) {
  const auto& wc = sdb::basics::downCast<irs::ByWildcard>(f);
  EXPECT_EQ(field, wc.field_id());
  EXPECT_EQ(value, irs::ViewCast<char>(irs::bytes_view{wc.options().term}));
  if (boost > 0.0f) {
    EXPECT_FLOAT_EQ(boost, wc.GetBoost());
  }
}

void AssertFuzzy(const irs::Filter& f, irs::field_id field,
                 std::string_view value, int distance, float boost = 0.0f) {
  const auto& fuzzy = sdb::basics::downCast<irs::ByEditDistance>(f);
  EXPECT_EQ(field, fuzzy.field_id());
  EXPECT_EQ(value, irs::ViewCast<char>(irs::bytes_view{fuzzy.options().term}));
  EXPECT_EQ(distance, fuzzy.options().max_distance);
  EXPECT_EQ(50, fuzzy.options().max_terms);
  if (boost > 0.0f) {
    EXPECT_FLOAT_EQ(boost, fuzzy.GetBoost());
  }
}

void AssertRange(const irs::Filter& f, irs::field_id field,
                 std::string_view min, irs::BoundType min_type,
                 std::string_view max, irs::BoundType max_type,
                 float boost = 0.0f) {
  const auto& range = sdb::basics::downCast<irs::ByRange>(f);
  EXPECT_EQ(field, range.field_id());
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
    EXPECT_FLOAT_EQ(boost, range.GetBoost());
  }
}

class LuceneParserTest : public ::testing::Test {
 protected:
  irs::BooleanFilter root;
  irs::analysis::SegmentationTokenizer::ptr tokenizer{
    irs::analysis::SegmentationTokenizer::Make(
      irs::analysis::SegmentationTokenizer::Options{})};

  sdb::ParserContext ctx{root, kFieldId, *tokenizer};

  LuceneParserTest() {
    // strict_field tests pin the prefix to "content"; tracking the name
    // alongside `default_field_id` lets the parser accept a redundant
    // `content:` prefix and reject any other.
    ctx.default_field_name = "content";
  }

  const irs::Clauses& Optional() const {
    return root.Bucket(irs::Occur::Should);
  }
  const irs::Clauses& Required() const { return root.Bucket(irs::Occur::Must); }
  const irs::Clauses& Excluded() const {
    return root.Bucket(irs::Occur::MustNot);
  }
};

TEST_F(LuceneParserTest, SimpleTerm) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "hello");
}

TEST_F(LuceneParserTest, SimplePhrase) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "\"hello world\""));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertPhrase(*Optional().filters[0], kFieldId);
}

TEST_F(LuceneParserTest, PrefixQuery) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hel*"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertPrefix(*Optional().filters[0], kFieldId, "hel");
}

TEST_F(LuceneParserTest, WildcardQuery) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "h*llo"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertWildcard(*Optional().filters[0], kFieldId, "h%llo");
}

// strict_field=true: a field-prefix is rejected unless it exactly
// matches the default field. The SQL `to_tsquery(...)` embed flips
// this on because the column is already pinned by the enclosing @@
// predicate -- a different field would silently miss because indexed
// fields are mangled by column id, not user-facing name.
TEST_F(LuceneParserTest, StrictField_AllowsBareTerm) {
  ctx.strict_field = true;
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "hello");
}

TEST_F(LuceneParserTest, StrictField_AllowsPhrase) {
  ctx.strict_field = true;
  ASSERT_TRUE(sdb::ParseQuery(ctx, "\"hello world\""));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertPhrase(*Optional().filters[0], kFieldId);
}

TEST_F(LuceneParserTest, StrictField_AllowsBoolean) {
  ctx.strict_field = true;
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello AND world"));
}

TEST_F(LuceneParserTest, StrictField_AllowsMatchingFieldPrefix) {
  // Same-name prefix is redundant but not wrong -- accept it.
  ctx.strict_field = true;
  ASSERT_TRUE(sdb::ParseQuery(ctx, "content:hello"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "hello");
}

TEST_F(LuceneParserTest, StrictField_AllowsMatchingFieldInBoolean) {
  ctx.strict_field = true;
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello AND content:world"));
}

TEST_F(LuceneParserTest, StrictField_RejectsDifferentFieldPrefix) {
  ctx.strict_field = true;
  ASSERT_FALSE(sdb::ParseQuery(ctx, "title:hello"));
  ASSERT_NE(
    ctx.error_message.find("field-prefix in strict-field mode must match the "
                           "default field"),
    std::string::npos)
    << "got: " << ctx.error_message;
  // Failed parse leaves no clauses on the root.
  ASSERT_EQ(0, Optional().size());
  ASSERT_EQ(0, Required().size());
  ASSERT_EQ(0, Excluded().size());
}

TEST_F(LuceneParserTest, StrictField_RejectsDifferentFieldInBoolean) {
  // Mismatched prefix anywhere in the tree is rejected, not just at the top.
  ctx.strict_field = true;
  ASSERT_FALSE(sdb::ParseQuery(ctx, "hello AND title:world"));
  ASSERT_NE(ctx.error_message.find("field-prefix"), std::string::npos)
    << "got: " << ctx.error_message;
}

TEST_F(LuceneParserTest, StrictField_RejectsDifferentFieldInGroup) {
  ctx.strict_field = true;
  ASSERT_FALSE(sdb::ParseQuery(ctx, "(foo OR title:bar)"));
  ASSERT_NE(ctx.error_message.find("field-prefix"), std::string::npos)
    << "got: " << ctx.error_message;
}

TEST_F(LuceneParserTest, BoostedTerm) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello^2"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "hello", 2.0f);
}

TEST_F(LuceneParserTest, BoostedTermFloat) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello^1.5"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "hello", 1.5f);
}

TEST_F(LuceneParserTest, FuzzyTerm) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello~"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertFuzzy(*Optional().filters[0], kFieldId, "hello", 2);
}

TEST_F(LuceneParserTest, SloppyPhraseBareTilde) {
  // `"..."~` without a number keeps the exact phrase (slop 0).
  ASSERT_TRUE(sdb::ParseQuery(ctx, "\"hello world\"~"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertPhrase(*Optional().filters[0], kFieldId);
}

TEST_F(LuceneParserTest, FuzzyTermWithDistance) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello~1"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertFuzzy(*Optional().filters[0], kFieldId, "hello", 1);
}

TEST_F(LuceneParserTest, FuzzyTermLimitFromContext) {
  ctx.fuzzy_max_terms = 7;
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello~1"));
  ASSERT_EQ(1, Optional().filters.size());
  const auto& fuzzy =
    sdb::basics::downCast<irs::ByEditDistance>(*Optional().filters[0]);
  EXPECT_EQ(7, fuzzy.options().max_terms);
}

TEST_F(LuceneParserTest, RangeInclusive) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "[alpha TO omega]"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertRange(*Optional().filters[0], kFieldId, "alpha",
              irs::BoundType::Inclusive, "omega", irs::BoundType::Inclusive);
}

TEST_F(LuceneParserTest, RangeExclusive) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "{alpha TO omega}"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertRange(*Optional().filters[0], kFieldId, "alpha",
              irs::BoundType::Exclusive, "omega", irs::BoundType::Exclusive);
}

TEST_F(LuceneParserTest, RangeUnbounded) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "[* TO omega]"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertRange(*Optional().filters[0], kFieldId, "", irs::BoundType::Unbounded,
              "omega", irs::BoundType::Inclusive);
}

TEST_F(LuceneParserTest, ImplicitOr) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello world"));
  ASSERT_EQ(2, Optional().size());
  ASSERT_EQ(2, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "hello");
  AssertTerm(Optional().terms[1], kFieldId, "world");
}

TEST_F(LuceneParserTest, ExplicitOr) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello OR world"));
  ASSERT_EQ(2, Optional().size());
  ASSERT_EQ(2, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "hello");
  AssertTerm(Optional().terms[1], kFieldId, "world");
}

TEST_F(LuceneParserTest, AndOperator) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello AND world"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_TRUE(Excluded().empty());
  ASSERT_EQ(2, Required().size());
  ASSERT_EQ(2, Required().terms.size());
  AssertTerm(Required().terms[0], kFieldId, "hello");
  AssertTerm(Required().terms[1], kFieldId, "world");
}

TEST_F(LuceneParserTest, ChainedAndOperator) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND b AND c"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_TRUE(Excluded().empty());
  ASSERT_EQ(3, Required().size());
  ASSERT_EQ(3, Required().terms.size());
  AssertTerm(Required().terms[0], kFieldId, "a");
  AssertTerm(Required().terms[1], kFieldId, "b");
  AssertTerm(Required().terms[2], kFieldId, "c");
}

TEST_F(LuceneParserTest, MixedPlusMinusOperators) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+foo -bar +foobar -foobaz"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(2, Required().size());
  ASSERT_EQ(2, Required().terms.size());
  AssertTerm(Required().terms[0], kFieldId, "foo");
  AssertTerm(Required().terms[1], kFieldId, "foobar");

  ASSERT_EQ(2, Excluded().size());
  ASSERT_EQ(2, Excluded().terms.size());
  AssertTerm(Excluded().terms[0], kFieldId, "bar");
  AssertTerm(Excluded().terms[1], kFieldId, "foobaz");
}

TEST_F(LuceneParserTest, MixedPlusMinusWithImplicitOr) {
  // +foo bar -baz +foobar foobaz
  // + terms go to Required, plain terms go to Optional
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+foo bar -baz +foobar foobaz"));
  ASSERT_EQ(2, Required().size());
  ASSERT_EQ(1, Excluded().size());
  ASSERT_EQ(2, Optional().size());

  // Required: [foo, foobar]

  ASSERT_EQ(2, Required().terms.size());
  AssertTerm(Required().terms[0], kFieldId, "foo");
  AssertTerm(Required().terms[1], kFieldId, "foobar");

  // Excluded: [baz]

  ASSERT_EQ(1, Excluded().terms.size());
  AssertTerm(Excluded().terms[0], kFieldId, "baz");

  // Optional: [bar, foobaz]

  ASSERT_EQ(2, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "bar");
  AssertTerm(Optional().terms[1], kFieldId, "foobaz");
}

TEST_F(LuceneParserTest, DeepNestedGroups) {
  // (a AND (b OR (c AND d)))
  // AND promotes a and the subgroup to Required within the outer group
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(a AND (b OR (c AND d)))"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& outer = *Optional().filters[0];
  ASSERT_TRUE(SubOptional(outer).empty());
  ASSERT_EQ(2, SubRequired(outer).size());

  ASSERT_EQ(1, SubRequired(outer).terms.size());
  AssertTerm(SubRequired(outer).terms[0], kFieldId, "a");

  // The other required clause is the group (b OR (c AND d))
  ASSERT_EQ(1, SubRequired(outer).filters.size());
  const auto& middle = *SubRequired(outer).filters[0];

  // Inside: b, and (c AND d) group
  ASSERT_EQ(2, SubOptional(middle).size());

  ASSERT_EQ(1, SubOptional(middle).terms.size());
  AssertTerm(SubOptional(middle).terms[0], kFieldId, "b");

  ASSERT_EQ(1, SubOptional(middle).filters.size());
  const auto& inner = *SubOptional(middle).filters[0];
  // c AND d promotes both to Required within the inner group
  ASSERT_TRUE(SubOptional(inner).empty());
  ASSERT_EQ(2, SubRequired(inner).size());
  ASSERT_EQ(2, SubRequired(inner).terms.size());
  AssertTerm(SubRequired(inner).terms[0], kFieldId, "c");
  AssertTerm(SubRequired(inner).terms[1], kFieldId, "d");
}

TEST_F(LuceneParserTest, GroupsWithAndOr) {
  // (a b) AND (c d) - AND promotes both groups to Required
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(a b) AND (c d)"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(2, Required().size());
  ASSERT_EQ(2, Required().filters.size());

  const auto& group1 = *Required().filters[0];
  ASSERT_EQ(2, SubOptional(group1).size());
  ASSERT_EQ(2, SubOptional(group1).terms.size());
  AssertTerm(SubOptional(group1).terms[0], kFieldId, "a");
  AssertTerm(SubOptional(group1).terms[1], kFieldId, "b");

  const auto& group2 = *Required().filters[1];
  ASSERT_EQ(2, SubOptional(group2).size());
  ASSERT_EQ(2, SubOptional(group2).terms.size());
  AssertTerm(SubOptional(group2).terms[0], kFieldId, "c");
  AssertTerm(SubOptional(group2).terms[1], kFieldId, "d");
}

TEST_F(LuceneParserTest, PlusMinusWithGroups) {
  // +(foo bar) -baz - required group, excluded term
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+(foo bar) -baz"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(1, Required().size());
  ASSERT_EQ(1, Required().filters.size());

  // Required: group (foo bar)
  const auto& group = *Required().filters[0];
  ASSERT_EQ(2, SubOptional(group).size());
  ASSERT_EQ(2, SubOptional(group).terms.size());
  AssertTerm(SubOptional(group).terms[0], kFieldId, "bar");
  AssertTerm(SubOptional(group).terms[1], kFieldId, "foo");

  // Excluded: baz
  ASSERT_EQ(1, Excluded().size());
  ASSERT_EQ(1, Excluded().terms.size());
  AssertTerm(Excluded().terms[0], kFieldId, "baz");
}

TEST_F(LuceneParserTest, ComplexMixedQuery) {
  // (a OR b) AND +(c d) -e
  // AND promotes (a OR b) to Required; +(c d) goes to Required; -e is excluded
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(a OR b) AND +(c d) -e"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(2, Required().size());
  ASSERT_EQ(2, Required().filters.size());

  // First: (a OR b)
  const auto& group_ab = *Required().filters[0];
  ASSERT_EQ(2, SubOptional(group_ab).size());
  ASSERT_EQ(2, SubOptional(group_ab).terms.size());
  AssertTerm(SubOptional(group_ab).terms[0], kFieldId, "a");
  AssertTerm(SubOptional(group_ab).terms[1], kFieldId, "b");

  // Second: (c d)
  const auto& group_cd = *Required().filters[1];
  ASSERT_EQ(2, SubOptional(group_cd).size());
  ASSERT_EQ(2, SubOptional(group_cd).terms.size());
  AssertTerm(SubOptional(group_cd).terms[0], kFieldId, "c");
  AssertTerm(SubOptional(group_cd).terms[1], kFieldId, "d");

  // Excluded: e
  ASSERT_EQ(1, Excluded().size());
  ASSERT_EQ(1, Excluded().terms.size());
  AssertTerm(Excluded().terms[0], kFieldId, "e");
}

TEST_F(LuceneParserTest, ComplexMixedQueryGrouped) {
  // (a OR b) AND (+(c d) -e) - AND promotes both to Required
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(a OR b) AND (+(c d) -e)"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_TRUE(Excluded().empty());
  ASSERT_EQ(2, Required().size());
  ASSERT_EQ(2, Required().filters.size());

  // First: group (a OR b)
  const auto& group_ab = *Required().filters[0];
  ASSERT_EQ(2, SubOptional(group_ab).size());
  ASSERT_EQ(2, SubOptional(group_ab).terms.size());
  AssertTerm(SubOptional(group_ab).terms[0], kFieldId, "a");
  AssertTerm(SubOptional(group_ab).terms[1], kFieldId, "b");

  // Second: group with +(c d) -e
  const auto& group2 = *Required().filters[1];
  ASSERT_EQ(1, SubRequired(group2).size());
  ASSERT_EQ(1, SubRequired(group2).filters.size());

  const auto& group_cd = *SubRequired(group2).filters[0];
  ASSERT_EQ(2, SubOptional(group_cd).size());
  ASSERT_EQ(2, SubOptional(group_cd).terms.size());
  AssertTerm(SubOptional(group_cd).terms[0], kFieldId, "c");
  AssertTerm(SubOptional(group_cd).terms[1], kFieldId, "d");

  ASSERT_EQ(1, SubExcluded(group2).size());
  ASSERT_EQ(1, SubExcluded(group2).terms.size());
  AssertTerm(SubExcluded(group2).terms[0], kFieldId, "e");
}

TEST_F(LuceneParserTest, NotOperator) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "NOT hello"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_TRUE(Required().empty());
  ASSERT_EQ(1, Excluded().size());
  ASSERT_EQ(1, Excluded().terms.size());

  AssertTerm(Excluded().terms[0], kFieldId, "hello");
}

TEST_F(LuceneParserTest, MinusOperator) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "-hello"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_TRUE(Required().empty());
  ASSERT_EQ(1, Excluded().size());
  ASSERT_EQ(1, Excluded().terms.size());

  AssertTerm(Excluded().terms[0], kFieldId, "hello");
}

TEST_F(LuceneParserTest, PlusOperator) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+hello"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(1, Required().size());
  ASSERT_EQ(1, Required().terms.size());

  AssertTerm(Required().terms[0], kFieldId, "hello");
}

TEST_F(LuceneParserTest, MultiplePlusOperators) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+foo +bar"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(2, Required().size());
  ASSERT_EQ(2, Required().terms.size());

  AssertTerm(Required().terms[0], kFieldId, "bar");
  AssertTerm(Required().terms[1], kFieldId, "foo");
}

TEST_F(LuceneParserTest, GroupedQuery) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(hello OR world)"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& group = *Optional().filters[0];
  ASSERT_TRUE(SubRequired(group).empty());
  ASSERT_EQ(2, SubOptional(group).size());
  ASSERT_EQ(2, SubOptional(group).terms.size());
  AssertTerm(SubOptional(group).terms[0], kFieldId, "hello");
  AssertTerm(SubOptional(group).terms[1], kFieldId, "world");
}

TEST_F(LuceneParserTest, BoostedGroup) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(foo bar)^2.5"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& group = *Optional().filters[0];
  EXPECT_FLOAT_EQ(2.5f, group.GetBoost());
  ASSERT_TRUE(SubRequired(group).empty());
  ASSERT_EQ(2, SubOptional(group).size());
  ASSERT_EQ(2, SubOptional(group).terms.size());
  AssertTerm(SubOptional(group).terms[0], kFieldId, "bar");
  AssertTerm(SubOptional(group).terms[1], kFieldId, "foo");
}

TEST_F(LuceneParserTest, ParseError) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "[unclosed"));
  EXPECT_FALSE(ctx.error_message.empty());
}

// Invalid grammar tests

TEST_F(LuceneParserTest, ParseError_UnclosedParenthesis) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "(hello world"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_UnclosedParenthesisNested) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "((foo bar)"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_ExtraClosingParenthesis) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "hello world)"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_UnclosedBracket) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "[alpha TO omega"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_UnclosedBrace) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "{alpha TO omega"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, RangeMixedBrackets) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "[alpha TO omega}"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertRange(*Optional().filters[0], kFieldId, "alpha",
              irs::BoundType::Inclusive, "omega", irs::BoundType::Exclusive);
}

TEST_F(LuceneParserTest, ParseError_RangeMissingTO) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "[alpha omega]"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_RangeMissingMinBound) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "[TO omega]"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_RangeMissingMaxBound) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "[alpha TO]"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_TrailingAND) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "hello AND"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_TrailingOR) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "hello OR"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_TrailingNOT) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "hello NOT"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_LeadingAND) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "AND hello"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_LeadingOR) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "OR hello"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_DoubleAND) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "hello AND AND world"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_FieldMissingValue) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "title:"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_BoostMissingValue) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "hello^"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_EmptyParentheses) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "()"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_DoubleColon) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "title::hello"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_BoostNonNumeric) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "hello^abc"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, NotBetweenTerms) {
  // guinea NOT pig -> Optional[guinea], Excluded[pig]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "guinea NOT pig"));
  ASSERT_TRUE(Required().empty());
  ASSERT_EQ(1, Excluded().size());

  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "guinea");

  ASSERT_EQ(1, Excluded().terms.size());
  AssertTerm(Excluded().terms[0], kFieldId, "pig");
}

TEST_F(LuceneParserTest, MinusBetweenTerms) {
  // guinea -pig -> Optional[guinea], Excluded[pig]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "guinea -pig"));
  ASSERT_TRUE(Required().empty());
  ASSERT_EQ(1, Excluded().size());

  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "guinea");

  ASSERT_EQ(1, Excluded().terms.size());
  AssertTerm(Excluded().terms[0], kFieldId, "pig");
}

TEST_F(LuceneParserTest, PlusBetweenTerms) {
  // guinea +pig -> Optional[guinea], Required[pig]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "guinea +pig"));
  ASSERT_EQ(1, Required().size());
  ASSERT_EQ(1, Required().terms.size());
  AssertTerm(Required().terms[0], kFieldId, "pig");

  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "guinea");
}

TEST_F(LuceneParserTest, AndThenOr) {
  // a AND b OR c -> Required[a, b], Optional[c]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND b OR c"));
  ASSERT_EQ(2, Required().size());
  ASSERT_EQ(2, Required().terms.size());
  AssertTerm(Required().terms[0], kFieldId, "a");
  AssertTerm(Required().terms[1], kFieldId, "b");

  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "c");
}

TEST_F(LuceneParserTest, OrThenAnd) {
  // a OR b AND c -> Required[b, c], Optional[a]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a OR b AND c"));
  ASSERT_EQ(2, Required().size());
  ASSERT_EQ(2, Required().terms.size());
  AssertTerm(Required().terms[0], kFieldId, "b");
  AssertTerm(Required().terms[1], kFieldId, "c");

  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "a");
}

TEST_F(LuceneParserTest, FourChainedAnd) {
  // a AND b AND c AND d -> Required[a, b, c, d]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND b AND c AND d"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(4, Required().size());
  ASSERT_EQ(4, Required().terms.size());
  AssertTerm(Required().terms[0], kFieldId, "a");
  AssertTerm(Required().terms[1], kFieldId, "b");
  AssertTerm(Required().terms[2], kFieldId, "c");
  AssertTerm(Required().terms[3], kFieldId, "d");
}

TEST_F(LuceneParserTest, NotBeforeAnd) {
  // NOT a AND b -> Excluded[a], Required[b]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "NOT a AND b"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(1, Excluded().size());
  ASSERT_EQ(1, Excluded().terms.size());
  AssertTerm(Excluded().terms[0], kFieldId, "a");

  ASSERT_EQ(1, Required().size());
  ASSERT_EQ(1, Required().terms.size());
  AssertTerm(Required().terms[0], kFieldId, "b");
}

TEST_F(LuceneParserTest, AndBeforeNot) {
  // a AND NOT b -> Required[a], Excluded[b]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND NOT b"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(1, Required().size());
  ASSERT_EQ(1, Required().terms.size());
  AssertTerm(Required().terms[0], kFieldId, "a");

  ASSERT_EQ(1, Excluded().size());
  ASSERT_EQ(1, Excluded().terms.size());
  AssertTerm(Excluded().terms[0], kFieldId, "b");
}

TEST_F(LuceneParserTest, NotBetweenMultipleTerms) {
  // a NOT b c -> Optional[a, c], Excluded[b]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a NOT b c"));
  ASSERT_EQ(2, Optional().size());
  ASSERT_EQ(2, Optional().terms.size());
  ASSERT_EQ(1, Excluded().size());
  ASSERT_EQ(1, Excluded().terms.size());

  AssertTerm(Optional().terms[0], kFieldId, "a");
  AssertTerm(Optional().terms[1], kFieldId, "c");

  AssertTerm(Excluded().terms[0], kFieldId, "b");
}

TEST_F(LuceneParserTest, AndWithMinusModifier) {
  // a AND -b -> Required[a], Excluded[b]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND -b"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(1, Required().size());
  ASSERT_EQ(1, Required().terms.size());
  AssertTerm(Required().terms[0], kFieldId, "a");

  ASSERT_EQ(1, Excluded().size());
  ASSERT_EQ(1, Excluded().terms.size());
  AssertTerm(Excluded().terms[0], kFieldId, "b");
}

TEST_F(LuceneParserTest, AndWithPlusModifier) {
  // a AND +b -> Required[a, b]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND +b"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_TRUE(Excluded().empty());
  ASSERT_EQ(2, Required().size());
  ASSERT_EQ(2, Required().terms.size());

  AssertTerm(Required().terms[0], kFieldId, "a");
  AssertTerm(Required().terms[1], kFieldId, "b");
}

TEST_F(LuceneParserTest, ComplexAndNotChain) {
  // a AND -b NOT c NOT d AND e -> Required[a, e], Excluded[b, c, d]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND -b NOT c NOT d AND e"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(2, Required().size());
  ASSERT_EQ(2, Required().terms.size());

  AssertTerm(Required().terms[0], kFieldId, "a");
  AssertTerm(Required().terms[1], kFieldId, "e");

  ASSERT_EQ(3, Excluded().size());
  ASSERT_EQ(3, Excluded().terms.size());
  AssertTerm(Excluded().terms[0], kFieldId, "b");
  AssertTerm(Excluded().terms[1], kFieldId, "c");
  AssertTerm(Excluded().terms[2], kFieldId, "d");
}

TEST_F(LuceneParserTest, MinusAndChain) {
  // -a AND -b AND -c -> Excluded[a, b, c]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "-a AND -b AND -c"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_TRUE(Required().empty());
  ASSERT_EQ(3, Excluded().size());
  ASSERT_EQ(3, Excluded().terms.size());

  AssertTerm(Excluded().terms[0], kFieldId, "a");
  AssertTerm(Excluded().terms[1], kFieldId, "b");
  AssertTerm(Excluded().terms[2], kFieldId, "c");
}

TEST_F(LuceneParserTest, OrWithMinusModifier) {
  // a OR -b -> Optional[a], Excluded[b]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a OR -b"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  ASSERT_EQ(1, Excluded().size());
  ASSERT_EQ(1, Excluded().terms.size());

  AssertTerm(Optional().terms[0], kFieldId, "a");
  AssertTerm(Excluded().terms[0], kFieldId, "b");
}

TEST_F(LuceneParserTest, OrWithPlusModifier) {
  // a OR +b -> Optional[a], Required[b]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a OR +b"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  ASSERT_EQ(1, Required().size());
  ASSERT_EQ(1, Required().terms.size());

  AssertTerm(Optional().terms[0], kFieldId, "a");
  AssertTerm(Required().terms[0], kFieldId, "b");
}

TEST_F(LuceneParserTest, MinusOrChain) {
  // -a OR -b -> Excluded[a, b]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "-a OR -b"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_TRUE(Required().empty());
  ASSERT_EQ(2, Excluded().size());
  ASSERT_EQ(2, Excluded().terms.size());

  AssertTerm(Excluded().terms[0], kFieldId, "a");
  AssertTerm(Excluded().terms[1], kFieldId, "b");
}

TEST_F(LuceneParserTest, OrWithMultipleMinusModifiers) {
  // a OR -b OR -c -> Optional[a], Excluded[b, c]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a OR -b OR -c"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  ASSERT_EQ(2, Excluded().size());
  ASSERT_EQ(2, Excluded().terms.size());

  AssertTerm(Optional().terms[0], kFieldId, "a");

  AssertTerm(Excluded().terms[0], kFieldId, "b");
  AssertTerm(Excluded().terms[1], kFieldId, "c");
}

TEST_F(LuceneParserTest, MixedAndOrSimple) {
  // a AND b OR c -> Required[a, b], Optional[c]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND b OR c"));
  ASSERT_EQ(2, Required().size());
  ASSERT_EQ(2, Required().terms.size());
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());

  AssertTerm(Required().terms[0], kFieldId, "a");
  AssertTerm(Required().terms[1], kFieldId, "b");
  AssertTerm(Optional().terms[0], kFieldId, "c");
}

TEST_F(LuceneParserTest, MixedOrAndSimple) {
  // a OR b AND c -> Optional[a], Required[b, c]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a OR b AND c"));
  ASSERT_EQ(2, Required().size());
  ASSERT_EQ(2, Required().terms.size());
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());

  AssertTerm(Optional().terms[0], kFieldId, "a");
  AssertTerm(Required().terms[0], kFieldId, "b");
  AssertTerm(Required().terms[1], kFieldId, "c");
}

TEST_F(LuceneParserTest, AndWithMinusThenOr) {
  // a AND -b OR c -> Required[a], Excluded[b], Optional[c]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND -b OR c"));
  ASSERT_EQ(1, Required().size());
  ASSERT_EQ(1, Required().terms.size());
  ASSERT_EQ(1, Excluded().size());
  ASSERT_EQ(1, Excluded().terms.size());
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());

  AssertTerm(Required().terms[0], kFieldId, "a");
  AssertTerm(Excluded().terms[0], kFieldId, "b");
  AssertTerm(Optional().terms[0], kFieldId, "c");
}

TEST_F(LuceneParserTest, OrWithMinusThenAnd) {
  // a OR -b AND c -> Optional[a], Excluded[b], Required[c]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a OR -b AND c"));
  ASSERT_EQ(1, Required().size());
  ASSERT_EQ(1, Required().terms.size());
  ASSERT_EQ(1, Excluded().size());
  ASSERT_EQ(1, Excluded().terms.size());
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  ASSERT_EQ(0, root.MinShouldMatch());

  AssertTerm(Required().terms[0], kFieldId, "c");
  AssertTerm(Excluded().terms[0], kFieldId, "b");
  AssertTerm(Optional().terms[0], kFieldId, "a");
}

TEST_F(LuceneParserTest, ComplexMixedAndOrWithModifiers) {
  // +a AND b OR -c AND d -> Required[a, b, d], Excluded[c]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+a AND b OR -c AND d"));
  ASSERT_EQ(3, Required().size());
  ASSERT_EQ(3, Required().terms.size());
  ASSERT_EQ(1, Excluded().size());
  ASSERT_EQ(1, Excluded().terms.size());
  ASSERT_TRUE(Optional().empty());

  AssertTerm(Required().terms[0], kFieldId, "a");
  AssertTerm(Required().terms[1], kFieldId, "b");
  AssertTerm(Required().terms[2], kFieldId, "d");

  AssertTerm(Excluded().terms[0], kFieldId, "c");
}

TEST_F(LuceneParserTest, PlusOrMinusAnd) {
  // +a OR -b AND c -> Required[a, c], Excluded[b]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+a OR -b AND c"));
  ASSERT_EQ(2, Required().size());
  ASSERT_EQ(2, Required().terms.size());
  ASSERT_EQ(1, Excluded().size());
  ASSERT_EQ(1, Excluded().terms.size());
  ASSERT_TRUE(Optional().empty());

  AssertTerm(Required().terms[0], kFieldId, "a");
  AssertTerm(Excluded().terms[0], kFieldId, "b");
  AssertTerm(Required().terms[1], kFieldId, "c");
}

TEST_F(LuceneParserTest, AndOrAndFlat) {
  // a AND b OR -c AND d -> Required[a, b, d], Excluded[c]
  // Flat Lucene-like behavior: modifiers create MUST/MUST_NOT regardless of OR
  // This is NOT grouped as (a AND b) OR (-c AND d) - it's flat!
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND b OR -c AND d"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(3, Required().size());
  ASSERT_EQ(3, Required().terms.size());
  ASSERT_EQ(1, Excluded().size());
  ASSERT_EQ(1, Excluded().terms.size());

  AssertTerm(Required().terms[0], kFieldId, "a");
  AssertTerm(Required().terms[1], kFieldId, "b");

  AssertTerm(Excluded().terms[0], kFieldId, "c");

  AssertTerm(Required().terms[2], kFieldId, "d");
}

TEST_F(LuceneParserTest, ManyImplicitOr) {
  // a b c d e -> Optional[a, b, c, d, e]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a b c d e"));
  ASSERT_TRUE(Required().empty());
  ASSERT_TRUE(Excluded().empty());
  ASSERT_EQ(5, Optional().size());
  ASSERT_EQ(5, Optional().terms.size());

  const char* expected[] = {"a", "b", "c", "d", "e"};
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_EQ(expected[i],
              irs::ViewCast<char>(irs::bytes_view{Optional().terms[i].term}));
  }
}

TEST_F(LuceneParserTest, AllExcluded) {
  // -a -b -> Excluded[a, b]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "-a -b"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_TRUE(Required().empty());
  ASSERT_EQ(2, Excluded().size());
  ASSERT_EQ(2, Excluded().terms.size());

  AssertTerm(Excluded().terms[0], kFieldId, "a");
  AssertTerm(Excluded().terms[1], kFieldId, "b");
}

TEST_F(LuceneParserTest, AllRequired) {
  // +a +b +c -> Required[a, b, c]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+a +b +c"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(3, Required().size());
  ASSERT_EQ(3, Required().terms.size());

  AssertTerm(Required().terms[0], kFieldId, "a");
  AssertTerm(Required().terms[1], kFieldId, "b");
  AssertTerm(Required().terms[2], kFieldId, "c");
}

TEST_F(LuceneParserTest, BoostedPhrase) {
  // "hello world"^2 -> Optional[phrase^2]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "\"hello world\"^2"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertPhrase(*Optional().filters[0], kFieldId, 2.0f);
}

TEST_F(LuceneParserTest, BoostedPhraseFloat) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "\"hello world\"^1.5"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertPhrase(*Optional().filters[0], kFieldId, 1.5f);
}

TEST_F(LuceneParserTest, FieldWithBoost) {
  // title:hello^3 -> Optional[title:hello^3]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:hello^3"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "hello", 3.0f);
}

TEST_F(LuceneParserTest, FieldWithRange) {
  // date:[aaa TO zzz] -> Optional[date:range]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "date:[aaa TO zzz]"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertRange(*Optional().filters[0], kFieldId, "aaa",
              irs::BoundType::Inclusive, "zzz", irs::BoundType::Inclusive);
}

TEST_F(LuceneParserTest, FieldWithExclusiveRange) {
  // price:{low TO high} -> Optional[price:range exclusive]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "price:{low TO high}"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertRange(*Optional().filters[0], kFieldId, "low",
              irs::BoundType::Exclusive, "high", irs::BoundType::Exclusive);
}

TEST_F(LuceneParserTest, FieldWithGroupedAnd) {
  // title:(a AND b) -> Optional[group(Required[title:a, title:b])]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:(a AND b)"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& group = *Optional().filters[0];
  ASSERT_TRUE(SubOptional(group).empty());
  ASSERT_EQ(2, SubRequired(group).size());
  ASSERT_EQ(2, SubRequired(group).terms.size());

  AssertTerm(SubRequired(group).terms[0], kFieldId, "a");
  AssertTerm(SubRequired(group).terms[1], kFieldId, "b");
}

TEST_F(LuceneParserTest, NotGroup) {
  // NOT (a b) -> Excluded[group]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "NOT (a b)"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_TRUE(Required().empty());
  ASSERT_EQ(1, Excluded().size());
  ASSERT_EQ(1, Excluded().filters.size());

  const auto& group = *Excluded().filters[0];
  ASSERT_EQ(2, SubOptional(group).size());
}

TEST_F(LuceneParserTest, BoostedFuzzy) {
  // hello~2^3 -> Optional[fuzzy(hello, dist=2, boost=3)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello~2^3"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertFuzzy(*Optional().filters[0], kFieldId, "hello", 2, 3.0f);
}

TEST_F(LuceneParserTest, BoostedFuzzyFloat) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello~1^0.5"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertFuzzy(*Optional().filters[0], kFieldId, "hello", 1, 0.5f);
}

TEST_F(LuceneParserTest, FieldWithFuzzy) {
  // title:hello~1 -> Optional[title:fuzzy(hello, 1)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:hello~1"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertFuzzy(*Optional().filters[0], kFieldId, "hello", 1);
}

TEST_F(LuceneParserTest, FieldWithPrefix) {
  // title:hel* -> Optional[title:prefix(hel)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:hel*"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertPrefix(*Optional().filters[0], kFieldId, "hel");
}

TEST_F(LuceneParserTest, BoostedPrefix) {
  // hel*^2 -> Optional[prefix(hel)^2]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hel*^2"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertPrefix(*Optional().filters[0], kFieldId, "hel", 2.0f);
}

TEST_F(LuceneParserTest, BoostedPrefixFloat) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hel*^0.8"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertPrefix(*Optional().filters[0], kFieldId, "hel", 0.8f);
}

TEST_F(LuceneParserTest, MixedAndImplicitOrAnd) {
  // a AND b c AND d -> Required[a, b, c, d]
  // AND grabs its immediate neighbors; second AND also promotes c
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND b c AND d"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(4, Required().size());
  ASSERT_EQ(4, Required().terms.size());

  AssertTerm(Required().terms[0], kFieldId, "a");
  AssertTerm(Required().terms[1], kFieldId, "b");
  AssertTerm(Required().terms[2], kFieldId, "c");
  AssertTerm(Required().terms[3], kFieldId, "d");
}

TEST_F(LuceneParserTest, PlusAndMinusGroup) {
  // +(a b) -(c d) -> Required[group(a,b)], Excluded[group(c,d)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+(a b) -(c d)"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(1, Required().size());
  ASSERT_EQ(1, Required().filters.size());

  const auto& group1 = *Required().filters[0];
  ASSERT_EQ(2, SubOptional(group1).size());

  ASSERT_EQ(1, Excluded().size());
  ASSERT_EQ(1, Excluded().filters.size());
  const auto& group2 = *Excluded().filters[0];
  ASSERT_EQ(2, SubOptional(group2).size());
}

TEST_F(LuceneParserTest, FieldWithWildcard) {
  // title:h*llo -> Optional[title:wildcard(h*llo)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:h*llo"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertWildcard(*Optional().filters[0], kFieldId, "h%llo");
}

TEST_F(LuceneParserTest, RangeWithUnboundedMax) {
  // [alpha TO *] -> Optional[range(alpha, unbounded)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "[alpha TO *]"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertRange(*Optional().filters[0], kFieldId, "alpha",
              irs::BoundType::Inclusive, "", irs::BoundType::Unbounded);
}

TEST_F(LuceneParserTest, RangeFullyUnbounded) {
  // [* TO *] -> Optional[range(unbounded, unbounded)]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "[* TO *]"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertRange(*Optional().filters[0], kFieldId, "", irs::BoundType::Unbounded,
              "", irs::BoundType::Unbounded);
}

TEST_F(LuceneParserTest, MultipleFieldQueries) {
  // title:foo AND author:bar AND year:[start TO end]
  ASSERT_TRUE(
    sdb::ParseQuery(ctx, "title:foo AND author:bar AND year:[start TO end]"))
    << ctx.error_message;
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(3, Required().size());
  ASSERT_EQ(2, Required().terms.size());
  ASSERT_EQ(1, Required().filters.size());

  AssertTerm(Required().terms[0], kFieldId, "bar");

  AssertTerm(Required().terms[1], kFieldId, "foo");

  AssertRange(*Required().filters[0], kFieldId, "start",
              irs::BoundType::Inclusive, "end", irs::BoundType::Inclusive);
}

TEST_F(LuceneParserTest, NestedGroupsWithModifiers) {
  // +(a (b OR c)) -d -> Required[group], Excluded[d]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+(a (b OR c)) -d"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(1, Required().size());
  ASSERT_EQ(1, Required().filters.size());

  const auto& group = *Required().filters[0];
  ASSERT_EQ(2, SubOptional(group).size());

  ASSERT_EQ(1, SubOptional(group).terms.size());
  AssertTerm(SubOptional(group).terms[0], kFieldId, "a");
  ASSERT_EQ(1, SubOptional(group).filters.size());
  const auto& inner = *SubOptional(group).filters[0];
  ASSERT_EQ(2, SubOptional(inner).size());

  ASSERT_EQ(1, Excluded().size());
  ASSERT_EQ(1, Excluded().terms.size());
  AssertTerm(Excluded().terms[0], kFieldId, "d");
}

TEST_F(LuceneParserTest, PhraseWithSlop) {
  // "hello world"~3 -> Optional[phrase with slop]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "\"hello world\"~3"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertPhrase(*Optional().filters[0], kFieldId, 0.0f, 3);
}

TEST_F(LuceneParserTest, PhraseWithSlopAndBoost) {
  // "hello world"~3^2 -> Optional[phrase with slop and boost]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "\"hello world\"~3^2"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertPhrase(*Optional().filters[0], kFieldId, 2.0f, 3);
}

TEST_F(LuceneParserTest, FieldPhraseWithSlop) {
  // title:"hello world"~4 -> Optional[title:phrase with slop]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:\"hello world\"~4"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertPhrase(*Optional().filters[0], kFieldId, 0.0f, 4);
}

TEST_F(LuceneParserTest, AndOrChain) {
  // a AND b OR c AND d -> Required[a, b, c, d]
  // First AND promotes a,b; OR leaves c in Optional; second AND promotes c,d
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND b OR c AND d"));
  // After "a AND b": Required[a, b], Optional[]
  // After "OR c": Required[a, b], Optional[c]
  // After "AND d": Required[a, b, c, d], Optional[]
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(4, Required().size());
  ASSERT_EQ(4, Required().terms.size());

  AssertTerm(Required().terms[0], kFieldId, "a");
  AssertTerm(Required().terms[1], kFieldId, "b");
  AssertTerm(Required().terms[2], kFieldId, "c");
  AssertTerm(Required().terms[3], kFieldId, "d");
}

TEST_F(LuceneParserTest, ParseError_TrailingPlus) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "hello +"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_TrailingMinus) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "hello -"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_DoubleOR) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "hello OR OR world"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, ParseError_AndOr) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "hello AND OR world"));
  EXPECT_NE(std::string::npos, ctx.error_message.find("syntax error"));
}

TEST_F(LuceneParserTest, QuestionMarkWildcard) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "Te?m"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertWildcard(*Optional().filters[0], kFieldId, "te_m");
}

TEST_F(LuceneParserTest, MultipleQuestionMarkWildcard) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "T??m"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertWildcard(*Optional().filters[0], kFieldId, "t__m");
}

TEST_F(LuceneParserTest, FieldQuestionMarkWildcard) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:Te?m"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertWildcard(*Optional().filters[0], kFieldId, "te_m");
}

TEST_F(LuceneParserTest, SuffixQuery) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "*suffix"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertWildcard(*Optional().filters[0], kFieldId, "%suffix");
}

TEST_F(LuceneParserTest, FieldSuffixQuery) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:*suffix"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertWildcard(*Optional().filters[0], kFieldId, "%suffix");
}

TEST_F(LuceneParserTest, EscapedMinus) {
  // `a-b` is one term to the query and two words to the analyzer, which is
  // what Lucene asks for under the default operator
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a\\-b"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& parts = SubOptional(*Optional().filters[0]);
  ASSERT_EQ(2, parts.size());
  ASSERT_EQ(2, parts.terms.size());
  AssertTerm(parts.terms[0], kFieldId, "a");
  AssertTerm(parts.terms[1], kFieldId, "b");
}

TEST_F(LuceneParserTest, EscapedColon) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a\\:b"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "a:b");
}

TEST_F(LuceneParserTest, EscapedStar) {
  // an escaped star is a star, not the place a pattern begins
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a\\*b"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& parts = SubOptional(*Optional().filters[0]);
  ASSERT_EQ(2, parts.size());
  ASSERT_EQ(2, parts.terms.size());
  AssertTerm(parts.terms[0], kFieldId, "a");
  AssertTerm(parts.terms[1], kFieldId, "b");
}

TEST_F(LuceneParserTest, DoubleAmpersandAnd) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello && world"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(2, Required().size());
  ASSERT_EQ(2, Required().terms.size());

  AssertTerm(Required().terms[0], kFieldId, "hello");
  AssertTerm(Required().terms[1], kFieldId, "world");
}

TEST_F(LuceneParserTest, DoublePipeOr) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello || world"));
  ASSERT_EQ(2, Optional().size());
  ASSERT_EQ(2, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "hello");
  AssertTerm(Optional().terms[1], kFieldId, "world");
}

TEST_F(LuceneParserTest, ExclamationNot) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "!hello"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_TRUE(Required().empty());
  ASSERT_EQ(1, Excluded().size());
  ASSERT_EQ(1, Excluded().terms.size());
  AssertTerm(Excluded().terms[0], kFieldId, "hello");
}

TEST_F(LuceneParserTest, BoostedRange) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "[a TO z]^2"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertRange(*Optional().filters[0], kFieldId, "a", irs::BoundType::Inclusive,
              "z", irs::BoundType::Inclusive, 2.0f);
}

TEST_F(LuceneParserTest, BoostedRangeFloat) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "[a TO z]^0.5"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertRange(*Optional().filters[0], kFieldId, "a", irs::BoundType::Inclusive,
              "z", irs::BoundType::Inclusive, 0.5f);
}

TEST_F(LuceneParserTest, BoostedWildcard) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "h*llo^2"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertWildcard(*Optional().filters[0], kFieldId, "h%llo", 2.0f);
}

TEST_F(LuceneParserTest, BoostedWildcardFloat) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "h*llo^1.7"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertWildcard(*Optional().filters[0], kFieldId, "h%llo", 1.7f);
}

TEST_F(LuceneParserTest, TabSeparator) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello\tworld"));
  ASSERT_EQ(2, Optional().size());
  ASSERT_EQ(2, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "hello");
  AssertTerm(Optional().terms[1], kFieldId, "world");
}

TEST_F(LuceneParserTest, NewlineSeparator) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "hello\nworld"));
  ASSERT_EQ(2, Optional().size());
  ASSERT_EQ(2, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "hello");
  AssertTerm(Optional().terms[1], kFieldId, "world");
}

TEST_F(LuceneParserTest, TermStartingWithDigits) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "2024abc"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "2024abc");
}

TEST_F(LuceneParserTest, RangeMixedBraceToSquare) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "{alpha TO omega]"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertRange(*Optional().filters[0], kFieldId, "alpha",
              irs::BoundType::Exclusive, "omega", irs::BoundType::Inclusive);
}

TEST_F(LuceneParserTest, RangeAndTerm) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "[a TO z] AND foo"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(2, Required().size());
  ASSERT_EQ(1, Required().filters.size());
  ASSERT_EQ(1, Required().terms.size());

  AssertRange(*Required().filters[0], kFieldId, "a", irs::BoundType::Inclusive,
              "z", irs::BoundType::Inclusive);

  AssertTerm(Required().terms[0], kFieldId, "foo");
}

TEST_F(LuceneParserTest, SingleCharTerm) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "a");
}

TEST_F(LuceneParserTest, StandaloneNumber) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "123"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "123");
}

TEST_F(LuceneParserTest, ParseError_EmptyQuery) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, ""));
}

TEST_F(LuceneParserTest, ParseError_WhitespaceOnly) {
  ASSERT_FALSE(sdb::ParseQuery(ctx, "   "));
}

TEST_F(LuceneParserTest, FieldRestoresAfterSingleTerm) {
  // title:hello world -> hello=title, world=content (default)
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:hello world"));
  ASSERT_EQ(2, Optional().size());
  ASSERT_EQ(2, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "hello");

  AssertTerm(Optional().terms[1], kFieldId, "world");
}

TEST_F(LuceneParserTest, FieldScopeWithAnd) {
  // title:a AND b -> a=title, b=content; both Required
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:a AND b"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(2, Required().size());
  ASSERT_EQ(2, Required().terms.size());

  AssertTerm(Required().terms[0], kFieldId, "a");

  AssertTerm(Required().terms[1], kFieldId, "b");
}

TEST_F(LuceneParserTest, DifferentFieldsWithAnd) {
  // title:a AND author:b -> a=title, b=author; both Required
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:a AND author:b"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(2, Required().size());
  ASSERT_EQ(2, Required().terms.size());

  AssertTerm(Required().terms[0], kFieldId, "a");

  AssertTerm(Required().terms[1], kFieldId, "b");
}

TEST_F(LuceneParserTest, TwoAndGroupsOrd) {
  // (a AND b) OR (c AND d) -> Optional[group(Req[a,b]), group(Req[c,d])]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(a AND b) OR (c AND d)"));
  ASSERT_EQ(2, Optional().size());
  ASSERT_EQ(2, Optional().filters.size());
  const auto& g1 = *Optional().filters[0];
  ASSERT_TRUE(SubOptional(g1).empty());
  ASSERT_EQ(2, SubRequired(g1).size());
  ASSERT_EQ(2, SubRequired(g1).terms.size());
  AssertTerm(SubRequired(g1).terms[0], kFieldId, "a");
  AssertTerm(SubRequired(g1).terms[1], kFieldId, "b");

  const auto& g2 = *Optional().filters[1];
  ASSERT_TRUE(SubOptional(g2).empty());
  ASSERT_EQ(2, SubRequired(g2).size());
  ASSERT_EQ(2, SubRequired(g2).terms.size());
  AssertTerm(SubRequired(g2).terms[0], kFieldId, "c");
  AssertTerm(SubRequired(g2).terms[1], kFieldId, "d");
}

TEST_F(LuceneParserTest, NotAndGroup) {
  // NOT (a AND b) -> Excluded[group(Req[a,b])]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "NOT (a AND b)"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_TRUE(Required().empty());
  ASSERT_EQ(1, Excluded().size());
  ASSERT_EQ(1, Excluded().filters.size());

  const auto& group = *Excluded().filters[0];
  ASSERT_TRUE(SubOptional(group).empty());
  ASSERT_EQ(2, SubRequired(group).size());
}

TEST_F(LuceneParserTest, ModifiersInsideFieldGroup) {
  // field:(+a -b c) -> group with a=Required, b=Excluded, c=Optional
  ASSERT_TRUE(sdb::ParseQuery(ctx, "field:(+a -b c)"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& group = *Optional().filters[0];
  ASSERT_EQ(1, SubOptional(group).size());
  ASSERT_EQ(1, SubRequired(group).size());
  ASSERT_EQ(1, SubExcluded(group).size());

  ASSERT_EQ(1, SubOptional(group).terms.size());
  AssertTerm(SubOptional(group).terms[0], kFieldId, "c");

  ASSERT_EQ(1, SubRequired(group).terms.size());
  AssertTerm(SubRequired(group).terms[0], kFieldId, "a");

  ASSERT_EQ(1, SubExcluded(group).terms.size());
  AssertTerm(SubExcluded(group).terms[0], kFieldId, "b");
}

TEST_F(LuceneParserTest, AndWithGroupInMiddle) {
  // a AND (b OR c) AND d -> Required[a, group(Opt[b,c]), d]
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a AND (b OR c) AND d"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(3, Required().size());
  ASSERT_EQ(2, Required().terms.size());
  ASSERT_EQ(1, Required().filters.size());

  AssertTerm(Required().terms[0], kFieldId, "a");

  const auto& group = *Required().filters[0];
  ASSERT_EQ(2, SubOptional(group).size());

  AssertTerm(Required().terms[1], kFieldId, "d");
}

TEST_F(LuceneParserTest, DeeplyNestedFieldGroups) {
  // title:(author:(a b) c) d
  // a,b = author field inside inner group
  // c = title field in outer group
  // d = default content field
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title:(author:(a b) c) d"));
  // First: outer group (title-scoped)
  ASSERT_EQ(2, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  ASSERT_EQ(1, Optional().terms.size());
  const auto& outer = *Optional().filters[0];
  ASSERT_TRUE(SubRequired(outer).empty());
  ASSERT_EQ(2, SubOptional(outer).size());

  // Inner group (author-scoped)
  ASSERT_EQ(1, SubOptional(outer).filters.size());
  const auto& inner = *SubOptional(outer).filters[0];
  ASSERT_TRUE(SubRequired(inner).empty());
  ASSERT_EQ(2, SubOptional(inner).size());
  ASSERT_EQ(2, SubOptional(inner).terms.size());
  AssertTerm(SubOptional(inner).terms[0], kFieldId, "a");
  AssertTerm(SubOptional(inner).terms[1], kFieldId, "b");

  ASSERT_EQ(1, SubOptional(outer).terms.size());
  AssertTerm(SubOptional(outer).terms[0], kFieldId, "c");

  // Second: d with default field
  AssertTerm(Optional().terms[0], kFieldId, "d");
}

TEST_F(LuceneParserTest, ThreeLevelNestedGroups) {
  // ((a AND b) OR c) AND d
  // Inner group: Req[a,b]. Middle group: Opt[inner, c]. AND promotes middle+d.
  ASSERT_TRUE(sdb::ParseQuery(ctx, "((a AND b) OR c) AND d"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(2, Required().size());
  ASSERT_EQ(1, Required().filters.size());
  ASSERT_EQ(1, Required().terms.size());

  const auto& middle = *Required().filters[0];
  ASSERT_EQ(2, SubOptional(middle).size());

  ASSERT_EQ(1, SubOptional(middle).filters.size());
  const auto& inner = *SubOptional(middle).filters[0];
  ASSERT_TRUE(SubOptional(inner).empty());
  ASSERT_EQ(2, SubRequired(inner).size());
  ASSERT_EQ(2, SubRequired(inner).terms.size());
  AssertTerm(SubRequired(inner).terms[0], kFieldId, "a");
  AssertTerm(SubRequired(inner).terms[1], kFieldId, "b");

  ASSERT_EQ(1, SubOptional(middle).terms.size());
  AssertTerm(SubOptional(middle).terms[0], kFieldId, "c");

  AssertTerm(Required().terms[0], kFieldId, "d");
}

TEST_F(LuceneParserTest, NestedGroupsWithMixedOperators) {
  // (+(a b) AND (c OR d)) OR e
  // Inner: +group(a,b) AND group(c,d) -> all Required in outer group
  // Then OR e at top level
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(+(a b) AND (c OR d)) OR e"));
  ASSERT_EQ(2, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  ASSERT_EQ(1, Optional().terms.size());
  const auto& outer = *Optional().filters[0];
  ASSERT_TRUE(SubOptional(outer).empty());
  ASSERT_EQ(2, SubRequired(outer).size());
  ASSERT_EQ(2, SubRequired(outer).filters.size());

  const auto& g1 = *SubRequired(outer).filters[0];
  ASSERT_EQ(2, SubOptional(g1).size());
  ASSERT_EQ(2, SubOptional(g1).terms.size());
  AssertTerm(SubOptional(g1).terms[0], kFieldId, "a");
  AssertTerm(SubOptional(g1).terms[1], kFieldId, "b");

  const auto& g2 = *SubRequired(outer).filters[1];
  ASSERT_EQ(2, SubOptional(g2).size());
  ASSERT_EQ(2, SubOptional(g2).terms.size());
  AssertTerm(SubOptional(g2).terms[0], kFieldId, "c");
  AssertTerm(SubOptional(g2).terms[1], kFieldId, "d");

  AssertTerm(Optional().terms[0], kFieldId, "e");
}

TEST_F(LuceneParserTest, DeeplyNestedNotGroups) {
  // NOT (NOT (a AND b))
  // Outer exclusion holds a group that excludes the AND group in turn
  ASSERT_TRUE(sdb::ParseQuery(ctx, "NOT (NOT (a AND b))"));
  ASSERT_TRUE(Optional().empty());
  ASSERT_TRUE(Required().empty());
  ASSERT_EQ(1, Excluded().size());
  ASSERT_EQ(1, Excluded().filters.size());

  // Middle group excludes the inner group
  const auto& middle = *Excluded().filters[0];
  ASSERT_TRUE(SubOptional(middle).empty());
  ASSERT_TRUE(SubRequired(middle).empty());
  ASSERT_EQ(1, SubExcluded(middle).size());
  ASSERT_EQ(1, SubExcluded(middle).filters.size());

  const auto& inner = *SubExcluded(middle).filters[0];
  ASSERT_TRUE(SubOptional(inner).empty());
  ASSERT_EQ(2, SubRequired(inner).size());
  ASSERT_EQ(2, SubRequired(inner).terms.size());
  AssertTerm(SubRequired(inner).terms[0], kFieldId, "a");
  AssertTerm(SubRequired(inner).terms[1], kFieldId, "b");
}

TEST_F(LuceneParserTest, ComplexMultiFieldNested) {
  // title:(+hello -world) AND author:(foo OR bar)^2
  ASSERT_TRUE(
    sdb::ParseQuery(ctx, "title:(+hello -world) AND author:(foo OR bar)^2"))
    << ctx.error_message;
  ASSERT_TRUE(Optional().empty());
  ASSERT_EQ(2, Required().size());
  ASSERT_EQ(2, Required().filters.size());

  const auto& g1 = *Required().filters[0];
  ASSERT_TRUE(SubOptional(g1).empty());
  ASSERT_EQ(1, SubRequired(g1).size());
  ASSERT_EQ(1, SubRequired(g1).terms.size());
  AssertTerm(SubRequired(g1).terms[0], kFieldId, "hello");
  ASSERT_EQ(1, SubExcluded(g1).size());
  ASSERT_EQ(1, SubExcluded(g1).terms.size());
  AssertTerm(SubExcluded(g1).terms[0], kFieldId, "world");

  const auto& g2 = *Required().filters[1];
  EXPECT_FLOAT_EQ(2.0f, g2.GetBoost());
  ASSERT_EQ(2, SubOptional(g2).size());
  ASSERT_EQ(2, SubOptional(g2).terms.size());
  AssertTerm(SubOptional(g2).terms[0], kFieldId, "bar");
  AssertTerm(SubOptional(g2).terms[1], kFieldId, "foo");
}

// Query: "+open source software licenses"
// Expected: required=[open], optional=[licenses, software, source]
TEST_F(LuceneParserTest, RequiredWithOptionals) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+open source software licenses"))
    << ctx.error_message;
  ASSERT_EQ(1, Required().size());
  ASSERT_EQ(1, Required().terms.size());
  ASSERT_EQ(3, Optional().size());
  ASSERT_EQ(3, Optional().terms.size());

  AssertTerm(Required().terms[0], kFieldId, "open");
  AssertTerm(Optional().terms[0], kFieldId, "licenses");
  AssertTerm(Optional().terms[1], kFieldId, "software");
  AssertTerm(Optional().terms[2], kFieldId, "source");
}

// Query: "+open" -- required only, no optional
TEST_F(LuceneParserTest, RequiredOnly) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "+open"));
  ASSERT_EQ(1, Required().size());
  ASSERT_EQ(1, Required().terms.size());
  ASSERT_TRUE(Optional().empty());
  AssertTerm(Required().terms[0], kFieldId, "open");
}

// Query: "open source" -- optional only (no + prefix), no required
TEST_F(LuceneParserTest, OptionalOnly) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "open source"));
  ASSERT_TRUE(Required().empty());
  ASSERT_EQ(2, Optional().size());
  ASSERT_EQ(2, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "open");
  AssertTerm(Optional().terms[1], kFieldId, "source");
}

}  // namespace

// What a term is: everything the syntax has not claimed, run through the
// analyzer -- Lucene's `_TERM_START_CHAR` and `getFieldQuery`.

TEST_F(LuceneParserTest, TermIsAnalyzed) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "Hello"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "hello");
}

TEST_F(LuceneParserTest, TermNotAscii) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "Z\xc3\xbcrich"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "z\xc3\xbcrich");
}

TEST_F(LuceneParserTest, TermWithDots) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "u.s.a"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "u.s.a");
}

TEST_F(LuceneParserTest, TermWithApostrophe) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "don't"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "don't");
}

TEST_F(LuceneParserTest, PrefixIsNormalized) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "Hel*"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertPrefix(*Optional().filters[0], kFieldId, "hel");
}

TEST_F(LuceneParserTest, FuzzyIsNormalized) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "Hello~1"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertFuzzy(*Optional().filters[0], kFieldId, "hello", 1);
}

TEST_F(LuceneParserTest, RegexKeepsItsPattern) {
  // `.` and `*` mean what a regular expression means by them
  ASSERT_TRUE(sdb::ParseQuery(ctx, "/hel.o/"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& regex =
    sdb::basics::downCast<irs::ByRegexp>(*Optional().filters[0]);
  EXPECT_EQ(kFieldId, regex.field_id());
  EXPECT_EQ("hel.o",
            irs::ViewCast<char>(irs::bytes_view{regex.options().pattern}));
}

// Phrases are read as a list of parts rather than one string taken apart
// afterwards.

TEST_F(LuceneParserTest, PhraseKeepsPunctuation) {
  // the analyzer decides what `rock-n-roll` is, not the lexer
  ASSERT_TRUE(sdb::ParseQuery(ctx, "\"rock-n-roll\""));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertPhrase(*Optional().filters[0], kFieldId);
  const auto& phrase =
    sdb::basics::downCast<irs::ByPhrase>(*Optional().filters[0]);
  EXPECT_EQ(3, phrase.options().size());
}

TEST_F(LuceneParserTest, PhraseWithNumber) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "\"world war 2\""));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& phrase =
    sdb::basics::downCast<irs::ByPhrase>(*Optional().filters[0]);
  EXPECT_EQ(3, phrase.options().size());
}

TEST_F(LuceneParserTest, PhraseWithGap) {
  // `1-3` says how far the part after it may sit from the part before
  ASSERT_TRUE(sdb::ParseQuery(ctx, "\"alpha 1-3 beta\""));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& phrase =
    sdb::basics::downCast<irs::ByPhrase>(*Optional().filters[0]);
  EXPECT_EQ(2, phrase.options().size());
}

TEST_F(LuceneParserTest, PhraseWithPrefixPart) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "\"alpha bet*\""));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& phrase =
    sdb::basics::downCast<irs::ByPhrase>(*Optional().filters[0]);
  EXPECT_EQ(2, phrase.options().size());
}

TEST_F(LuceneParserTest, PhraseWithFuzzyPart) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "\"alpha beta~1\""));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& phrase =
    sdb::basics::downCast<irs::ByPhrase>(*Optional().filters[0]);
  EXPECT_EQ(2, phrase.options().size());
}

// What the flexible parser adds: a minimum match, comparison bounds, and the
// `fn:` family.

TEST_F(LuceneParserTest, GroupMinMatch) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(alpha beta gamma)@2"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& group = AsBoolean(*Optional().filters[0]);
  EXPECT_EQ(3, group.Size(irs::Occur::Should));
  EXPECT_EQ(2, group.MinShouldMatch());
}

TEST_F(LuceneParserTest, GroupMinMatchBesideRequired) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(+alpha beta gamma)@2"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& group = AsBoolean(*Optional().filters[0]);
  EXPECT_EQ(0, group.Size(irs::Occur::Should));
  ASSERT_EQ(2, group.Size(irs::Occur::Must));
  const auto& required = group.Bucket(irs::Occur::Must);
  ASSERT_EQ(1, required.terms.size());
  AssertTerm(required.terms[0], kFieldId, "alpha");
  ASSERT_EQ(1, required.filters.size());
  const auto& threshold = AsBoolean(*required.filters[0]);
  EXPECT_EQ(2, threshold.Size(irs::Occur::Should));
  EXPECT_EQ(2, threshold.MinShouldMatch());
}

TEST_F(LuceneParserTest, GroupMinMatchZeroWithExclusion) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "(alpha -beta)@0"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& group = AsBoolean(*Optional().filters[0]);
  EXPECT_EQ(0, group.Size(irs::Occur::Should));
  ASSERT_EQ(1, group.Size(irs::Occur::Must));
  EXPECT_EQ(irs::Type<irs::All>::id(),
            group.Bucket(irs::Occur::Must).filters[0]->type());
  ASSERT_EQ(1, group.Size(irs::Occur::MustNot));
  AssertTerm(group.Bucket(irs::Occur::MustNot).terms[0], kFieldId, "beta");
}

TEST_F(LuceneParserTest, ComparisonLess) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title<beta"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertRange(*Optional().filters[0], kFieldId, "", irs::BoundType::Unbounded,
              "beta", irs::BoundType::Exclusive);
}

TEST_F(LuceneParserTest, ComparisonGreaterOrEqual) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "title>=alpha"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertRange(*Optional().filters[0], kFieldId, "alpha",
              irs::BoundType::Inclusive, "", irs::BoundType::Unbounded);
}

TEST_F(LuceneParserTest, FnOrIsADisjunction) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "fn:or(Alpha beta)"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& any = SubOptional(*Optional().filters[0]);
  ASSERT_EQ(2, any.size());
  ASSERT_EQ(2, any.terms.size());
  // the terms of a function are terms of the index, like every other term
  AssertTerm(any.terms[0], kFieldId, "alpha");
  AssertTerm(any.terms[1], kFieldId, "beta");
}

TEST_F(LuceneParserTest, FnUnorderedIsAConjunction) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "fn:unordered(alpha beta)"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  EXPECT_EQ(2, SubRequired(*Optional().filters[0]).size());
}

TEST_F(LuceneParserTest, FnOrderedIsAPhrase) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "fn:ordered(alpha beta)"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& phrase =
    sdb::basics::downCast<irs::ByPhrase>(*Optional().filters[0]);
  EXPECT_EQ(2, phrase.options().size());
}

TEST_F(LuceneParserTest, FnAtLeast) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "fn:atLeast(2 alpha beta gamma)"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& any = AsBoolean(*Optional().filters[0]);
  EXPECT_EQ(3, any.Size(irs::Occur::Should));
  EXPECT_EQ(2, any.MinShouldMatch());
}

TEST_F(LuceneParserTest, NGram) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "fn:ngram(0.6 alpha beta gamma)"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& ngram =
    sdb::basics::downCast<irs::ByNGramSimilarity>(*Optional().filters[0]);
  EXPECT_EQ(kFieldId, ngram.field_id());
  EXPECT_FLOAT_EQ(0.6f, ngram.options().threshold);
  EXPECT_EQ(3, ngram.options().ngrams.size());
}

// What is read and refused, so that the message names what was asked for.

TEST_F(LuceneParserTest, FnWithoutAnAlgebraIsRefused) {
  EXPECT_ANY_THROW(sdb::ParseQuery(ctx, "fn:before(alpha beta)"));
}

TEST_F(LuceneParserTest, FnOverANonTermIsRefused) {
  EXPECT_ANY_THROW(sdb::ParseQuery(ctx, "fn:ordered(alpha \"beta gamma\")"));
}

TEST_F(LuceneParserTest, MaxGapsOverMoreThanAPairIsRefused) {
  // over one pair a gap bound is a distance; over more it bounds a total,
  // which a phrase cannot say
  EXPECT_ANY_THROW(
    sdb::ParseQuery(ctx, "fn:maxgaps(2 fn:ordered(alpha beta gamma))"));
}

TEST_F(LuceneParserTest, FieldExistenceIsRefused) {
  EXPECT_ANY_THROW(sdb::ParseQuery(ctx, "title:*"));
}

// Rules the grammar has that nothing else here reaches.

TEST_F(LuceneParserTest, UnicodeEscape) {
  // `\uXXXX` is the character it names -- Lucene's `discardEscapeChar`
  ASSERT_TRUE(sdb::ParseQuery(ctx, "caf\\u00e9"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "caf\xc3\xa9");
}

TEST_F(LuceneParserTest, EscapeKeepsWhatItProtected) {
  // the `+` is part of the term rather than an operator, and the analyzer
  // makes two words of what it protected
  ASSERT_TRUE(sdb::ParseQuery(ctx, "a\\+b"));
  ASSERT_TRUE(Required().empty());
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& parts = SubOptional(*Optional().filters[0]);
  ASSERT_EQ(2, parts.size());
  ASSERT_EQ(2, parts.terms.size());
  AssertTerm(parts.terms[0], kFieldId, "a");
  AssertTerm(parts.terms[1], kFieldId, "b");
}

TEST_F(LuceneParserTest, LoneOperatorIsATerm) {
  // what was pasted in is searched for, rather than read as an operator with
  // nothing to apply to
  ASSERT_TRUE(sdb::ParseQuery(ctx, "alpha + beta"));
  ASSERT_EQ(3, Optional().size());
  ASSERT_EQ(3, Optional().terms.size());
  AssertTerm(Optional().terms[0], kFieldId, "+");
  AssertTerm(Optional().terms[1], kFieldId, "alpha");
  AssertTerm(Optional().terms[2], kFieldId, "beta");
}

TEST_F(LuceneParserTest, MatchAll) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "*:*"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  EXPECT_EQ(irs::Type<irs::All>::id(), Optional().filters[0]->type());
}

TEST_F(LuceneParserTest, RangeWithQuotedBound) {
  // a quoted bound holds what a bare one cannot: spaces, and the word TO
  ASSERT_TRUE(sdb::ParseQuery(ctx, "[\"alpha beta\" TO gamma]"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertRange(*Optional().filters[0], kFieldId, "alpha beta",
              irs::BoundType::Inclusive, "gamma", irs::BoundType::Inclusive);
}

TEST_F(LuceneParserTest, FnPhrase) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "fn:phrase(alpha beta)"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& phrase =
    sdb::basics::downCast<irs::ByPhrase>(*Optional().filters[0]);
  EXPECT_EQ(2, phrase.options().size());
}

TEST_F(LuceneParserTest, FnWildcard) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "fn:wildcard(al*ha)"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertWildcard(*Optional().filters[0], kFieldId, "al%ha");
}

TEST_F(LuceneParserTest, FnFuzzyTerm) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "fn:fuzzyTerm(alpha)"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertFuzzy(*Optional().filters[0], kFieldId, "alpha", 2);
}

TEST_F(LuceneParserTest, FnFuzzyTermWithDistance) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "fn:fuzzyTerm(alpha 1)"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  AssertFuzzy(*Optional().filters[0], kFieldId, "alpha", 1);
}

TEST_F(LuceneParserTest, FnMaxGapsOverAPair) {
  // over one pair a gap bound is a distance, which a phrase can say
  ASSERT_TRUE(sdb::ParseQuery(ctx, "fn:maxgaps(2 fn:ordered(alpha beta))"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& phrase =
    sdb::basics::downCast<irs::ByPhrase>(*Optional().filters[0]);
  EXPECT_EQ(2, phrase.options().size());
}

TEST_F(LuceneParserTest, FnMaxWidthOverAPair) {
  ASSERT_TRUE(sdb::ParseQuery(ctx, "fn:maxwidth(3 fn:ordered(alpha beta))"));
  ASSERT_EQ(1, Optional().size());
  ASSERT_EQ(1, Optional().filters.size());
  const auto& phrase =
    sdb::basics::downCast<irs::ByPhrase>(*Optional().filters[0]);
  EXPECT_EQ(2, phrase.options().size());
}

TEST_F(LuceneParserTest, FnMaxWidthTooNarrowIsRefused) {
  EXPECT_ANY_THROW(sdb::ParseQuery(ctx, "fn:maxwidth(1 fn:ordered(a b))"));
}
