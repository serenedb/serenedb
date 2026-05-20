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

#include "filter_test_case_base.hpp"
#include "iresearch/search/filter_rules.hpp"
#include "tests_shared.hpp"

namespace {

template<typename Filter>
auto MakeFilter(std::string_view field, std::string_view term) -> Filter {
  Filter q;
  *q.mutable_field() = field;
  q.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  return q;
}

auto MakeLevenshtein(std::string_view field, std::string_view term,
                     std::string_view prefix = {},
                     uint8_t max_distance = 1) -> irs::ByEditDistance {
  irs::ByEditDistance q;
  *q.mutable_field() = field;
  q.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  q.mutable_options()->prefix = irs::ViewCast<irs::byte_type>(prefix);
  q.mutable_options()->max_distance = max_distance;
  return q;
}

auto AsBytes(std::string_view s) -> irs::bstring {
  auto view = irs::ViewCast<irs::byte_type>(s);
  return irs::bstring{view.data(), view.size()};
}

class FilterRuleTestCase : public tests::FilterTestCaseBase {};

TEST_P(FilterRuleTestCase, NotFilterRule_DoubleNegationCollapses) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::NotFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::Not>();
  auto& not_filter = sdb::basics::downCast<irs::Not>(*root);
  not_filter.filter<irs::Not>().filter<irs::Empty>();
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::Empty>::id());
}

TEST_P(FilterRuleTestCase, NotFilterRule_SingleNegationUnchanged) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::NotFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::Not>();
  sdb::basics::downCast<irs::Not>(*root).filter<irs::Empty>();
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::Not>::id());
  const auto& result = sdb::basics::downCast<irs::Not>(*root);
  ASSERT_NE(result.filter(), nullptr);
  ASSERT_EQ(result.filter()->type(), irs::Type<irs::Empty>::id());
}

TEST_P(FilterRuleTestCase, NotFilterRule_TripleNegationCollapsesToSingle) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::NotFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::Not>();
  auto& not_filter = sdb::basics::downCast<irs::Not>(*root);
  not_filter.filter<irs::Not>().filter<irs::Not>().filter<irs::Empty>();
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::Not>::id());
  const auto& result = sdb::basics::downCast<irs::Not>(*root);
  ASSERT_EQ(result.filter()->type(), irs::Type<irs::Empty>::id());
}

TEST_P(FilterRuleTestCase, NotFilterRule_QuadrupleNegationCollapses) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::NotFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::Not>();
  auto& not_filter = sdb::basics::downCast<irs::Not>(*root);
  not_filter.filter<irs::Not>()
    .filter<irs::Not>()
    .filter<irs::Not>()
    .filter<irs::Empty>();
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::Empty>::id());
}

TEST_P(FilterRuleTestCase, AndFlatteningFilterRule_FlattensSingleNestedAnd) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AndFlatteningFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& and_filter = sdb::basics::downCast<irs::And>(*root);
  auto& sub_and_filter = and_filter.add<irs::And>();
  sub_and_filter.add<irs::Empty>();
  sub_and_filter.add<irs::Empty>();
  and_filter.add<irs::Empty>();
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::And>::id());
  const auto& result = sdb::basics::downCast<irs::And>(*root);
  ASSERT_EQ(result.size(), 3);
  for (size_t i = 0; i < result.size(); ++i) {
    ASSERT_EQ(result[i].type(), irs::Type<irs::Empty>::id());
  }
}

TEST_P(FilterRuleTestCase, AndFlatteningFilterRule_FlattensMultipleNestedAnds) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AndFlatteningFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& and_filter = sdb::basics::downCast<irs::And>(*root);
  auto& left = and_filter.add<irs::And>();
  left.add<irs::Empty>();
  left.add<irs::Empty>();
  auto& right = and_filter.add<irs::And>();
  right.add<irs::Empty>();
  right.add<irs::Empty>();
  root = constructor.Apply(std::move(root));

  const auto& result = sdb::basics::downCast<irs::And>(*root);
  ASSERT_EQ(result.size(), 4);
  for (size_t i = 0; i < result.size(); ++i) {
    ASSERT_EQ(result[i].type(), irs::Type<irs::Empty>::id());
  }
}

TEST_P(FilterRuleTestCase, AndFlatteningFilterRule_NoNestedAndUnchanged) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AndFlatteningFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& and_filter = sdb::basics::downCast<irs::And>(*root);
  and_filter.add<irs::Empty>();
  and_filter.add<irs::Empty>();
  root = constructor.Apply(std::move(root));

  const auto& result = sdb::basics::downCast<irs::And>(*root);
  ASSERT_EQ(result.size(), 2);
}

TEST_P(FilterRuleTestCase, AndFlatteningFilterRule_DeeplyNestedFlattens) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AndFlatteningFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& level1 = sdb::basics::downCast<irs::And>(*root);
  auto& level2 = level1.add<irs::And>();
  auto& level3 = level2.add<irs::And>();
  level3.add<irs::Empty>();
  level3.add<irs::Empty>();
  root = constructor.Apply(std::move(root));

  const auto& result = sdb::basics::downCast<irs::And>(*root);
  ASSERT_EQ(result.size(), 2);
  for (size_t i = 0; i < result.size(); ++i) {
    ASSERT_EQ(result[i].type(), irs::Type<irs::Empty>::id());
  }
}

TEST_P(FilterRuleTestCase, OrFlatteningFilterRule_FlattensSingleNestedOr) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::OrFlatteningFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::Or>();
  auto& or_filter = sdb::basics::downCast<irs::Or>(*root);
  auto& sub_or_filter = or_filter.add<irs::Or>();
  sub_or_filter.add<irs::Empty>();
  sub_or_filter.add<irs::Empty>();
  or_filter.add<irs::Empty>();
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::Or>::id());
  const auto& result = sdb::basics::downCast<irs::Or>(*root);
  ASSERT_EQ(result.size(), 3);
  for (size_t i = 0; i < result.size(); ++i) {
    ASSERT_EQ(result[i].type(), irs::Type<irs::Empty>::id());
  }
}

TEST_P(FilterRuleTestCase, OrFlatteningFilterRule_NoNestedOrUnchanged) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::OrFlatteningFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::Or>();
  auto& or_filter = sdb::basics::downCast<irs::Or>(*root);
  or_filter.add<irs::Empty>();
  or_filter.add<irs::Empty>();
  root = constructor.Apply(std::move(root));

  const auto& result = sdb::basics::downCast<irs::Or>(*root);
  ASSERT_EQ(result.size(), 2);
}

TEST_P(FilterRuleTestCase, ByTermsFilterRule_AndMergesSameFieldTerms) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::ByTermsFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& and_filter = sdb::basics::downCast<irs::And>(*root);
  and_filter.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>("animal", "cat");
  and_filter.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>("animal", "dog");
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::ByTerms>::id());
  const auto& by_terms = sdb::basics::downCast<irs::ByTerms>(*root);
  ASSERT_EQ(by_terms.field(), "animal");

  const auto& options = by_terms.options();
  ASSERT_EQ(options.min_match, 2);
  ASSERT_EQ(options.terms.size(), 2);
  ASSERT_TRUE(options.terms.contains(irs::ByTermsOptions::SearchTerm{AsBytes("cat")}));
  ASSERT_TRUE(options.terms.contains(irs::ByTermsOptions::SearchTerm{AsBytes("dog")}));
}

TEST_P(FilterRuleTestCase, ByTermsFilterRule_AndKeepsDifferentFieldsSeparate) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::ByTermsFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& and_filter = sdb::basics::downCast<irs::And>(*root);
  and_filter.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>("animal", "cat");
  and_filter.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>("color", "blue");
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::And>::id());
  const auto& result = sdb::basics::downCast<irs::And>(*root);
  ASSERT_EQ(result.size(), 2);
  for (size_t i = 0; i < result.size(); ++i) {
    ASSERT_EQ(result[i].type(), irs::Type<irs::ByTerm>::id());
  }
}

TEST_P(FilterRuleTestCase, ByTermsFilterRule_AndPreservesNonByTermSiblings) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::ByTermsFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& and_filter = sdb::basics::downCast<irs::And>(*root);
  and_filter.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>("animal", "cat");
  and_filter.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>("animal", "dog");
  and_filter.add<irs::Empty>();
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::And>::id());
  const auto& result = sdb::basics::downCast<irs::And>(*root);
  ASSERT_EQ(result.size(), 2);

  bool has_by_terms = false;
  bool has_empty = false;
  for (size_t i = 0; i < result.size(); ++i) {
    if (result[i].type() == irs::Type<irs::ByTerms>::id()) {
      has_by_terms = true;
      const auto& by_terms = sdb::basics::downCast<irs::ByTerms>(result[i]);
      ASSERT_EQ(by_terms.field(), "animal");
      ASSERT_EQ(by_terms.options().min_match, 2);
    } else if (result[i].type() == irs::Type<irs::Empty>::id()) {
      has_empty = true;
    }
  }
  ASSERT_TRUE(has_by_terms);
  ASSERT_TRUE(has_empty);
}

TEST_P(FilterRuleTestCase, ByTermsFilterRule_OrMergesSameFieldWithMinMatchOne) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::ByTermsFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::Or>();
  auto& or_filter = sdb::basics::downCast<irs::Or>(*root);
  or_filter.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>("animal", "cat");
  or_filter.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>("animal", "dog");
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::ByTerms>::id());
  const auto& by_terms = sdb::basics::downCast<irs::ByTerms>(*root);
  ASSERT_EQ(by_terms.field(), "animal");
  ASSERT_EQ(by_terms.options().min_match, 1);
  ASSERT_EQ(by_terms.options().terms.size(), 2);
}

TEST_P(FilterRuleTestCase, ByTermsFilterRule_SingleTermPerFieldStaysAsByTerm) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::ByTermsFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& and_filter = sdb::basics::downCast<irs::And>(*root);
  and_filter.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>("animal", "cat");
  and_filter.add<irs::Empty>();
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::And>::id());
  const auto& result = sdb::basics::downCast<irs::And>(*root);
  ASSERT_EQ(result.size(), 2);

  bool has_by_term = false;
  for (size_t i = 0; i < result.size(); ++i) {
    if (result[i].type() == irs::Type<irs::ByTerm>::id()) {
      has_by_term = true;
    }
  }
  ASSERT_TRUE(has_by_term);
}

TEST_P(FilterRuleTestCase, LevenshteinPrefixFilterRule_FoldsPrefixIntoLevenshtein) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::LevenshteinPrefixFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& and_filter = sdb::basics::downCast<irs::And>(*root);
  and_filter.add<irs::ByPrefix>() = MakeFilter<irs::ByPrefix>("word", "ab");
  and_filter.add<irs::ByEditDistance>() = MakeLevenshtein("word", "abcd");
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::ByEditDistance>::id());
  const auto& result = sdb::basics::downCast<irs::ByEditDistance>(*root);
  ASSERT_EQ(result.field(), "word");
  ASSERT_EQ(result.options().term, AsBytes("cd"));
  ASSERT_EQ(result.options().prefix, AsBytes("ab"));
}

TEST_P(FilterRuleTestCase, LevenshteinPrefixFilterRule_NonMatchingPrefixIsReverted) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::LevenshteinPrefixFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& and_filter = sdb::basics::downCast<irs::And>(*root);
  and_filter.add<irs::ByPrefix>() = MakeFilter<irs::ByPrefix>("word", "xy");
  and_filter.add<irs::ByEditDistance>() = MakeLevenshtein("word", "abcd");
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::And>::id());
  const auto& result = sdb::basics::downCast<irs::And>(*root);
  ASSERT_EQ(result.size(), 2);
}

TEST_P(FilterRuleTestCase, LevenshteinPrefixFilterRule_MultiplePrefixesReverted) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::LevenshteinPrefixFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& and_filter = sdb::basics::downCast<irs::And>(*root);
  and_filter.add<irs::ByPrefix>() = MakeFilter<irs::ByPrefix>("word", "ab");
  and_filter.add<irs::ByPrefix>() = MakeFilter<irs::ByPrefix>("word", "xy");
  and_filter.add<irs::ByEditDistance>() = MakeLevenshtein("word", "abcd");
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::And>::id());
  const auto& result = sdb::basics::downCast<irs::And>(*root);
  ASSERT_EQ(result.size(), 3);
}

TEST_P(FilterRuleTestCase, LevenshteinPrefixFilterRule_MissingPrefixOrLevenshteinReverted) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::LevenshteinPrefixFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& and_filter = sdb::basics::downCast<irs::And>(*root);
  and_filter.add<irs::ByPrefix>() = MakeFilter<irs::ByPrefix>("word", "ab");
  and_filter.add<irs::Empty>();
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::And>::id());
  const auto& result = sdb::basics::downCast<irs::And>(*root);
  ASSERT_EQ(result.size(), 2);
}

TEST_P(FilterRuleTestCase, FilterRulesConstructor_ChainsMultipleRules) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AndFlatteningFilterRule>();
  constructor.Add<irs::ByTermsFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& and_filter = sdb::basics::downCast<irs::And>(*root);
  auto& sub_and = and_filter.add<irs::And>();
  sub_and.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>("animal", "cat");
  sub_and.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>("animal", "dog");
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::ByTerms>::id());
  const auto& by_terms = sdb::basics::downCast<irs::ByTerms>(*root);
  ASSERT_EQ(by_terms.field(), "animal");
  ASSERT_EQ(by_terms.options().min_match, 2);
  ASSERT_EQ(by_terms.options().terms.size(), 2);
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(rule_filter_test, FilterRuleTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd")),
                         FilterRuleTestCase::to_string);

}  // namespace
