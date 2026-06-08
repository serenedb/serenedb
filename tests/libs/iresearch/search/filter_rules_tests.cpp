#include "filter_test_case_base.hpp"
#include "iresearch/search/filter_rules.hpp"
#include "tests_shared.hpp"

namespace {

template<typename Filter>
Filter MakeFilter(std::string_view field, std::string_view term) {
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
  ASSERT_TRUE(
    options.terms.contains(irs::ByTermsOptions::SearchTerm{AsBytes("cat")}));
  ASSERT_TRUE(
    options.terms.contains(irs::ByTermsOptions::SearchTerm{AsBytes("dog")}));
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

TEST_P(FilterRuleTestCase,
       LevenshteinPrefixFilterRule_FoldsPrefixIntoLevenshtein) {
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

TEST_P(FilterRuleTestCase,
       LevenshteinPrefixFilterRule_NonMatchingPrefixIsReverted) {
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

TEST_P(FilterRuleTestCase,
       LevenshteinPrefixFilterRule_MultiplePrefixesReverted) {
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

TEST_P(FilterRuleTestCase,
       LevenshteinPrefixFilterRule_MissingPrefixOrLevenshteinReverted) {
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

irs::ByWildcard MakeWildcard(std::string_view field, std::string_view pattern) {
  irs::ByWildcard f;
  *f.mutable_field() = field;
  f.mutable_options()->term = irs::ViewCast<irs::byte_type>(pattern);
  return f;
}

irs::ByRegexp MakeRegexp(std::string_view field, std::string_view pattern) {
  irs::ByRegexp f;
  *f.mutable_field() = field;
  f.mutable_options()->pattern = irs::ViewCast<irs::byte_type>(pattern);
  return f;
}

TEST_P(FilterRuleTestCase,
       AutomatonFilterRule_Intersection_MergesTwoWildcardsOnSameField) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AutomatonFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& f = sdb::basics::downCast<irs::And>(*root);
  f.add<irs::ByWildcard>() = MakeWildcard("body", "c%");
  f.add<irs::ByWildcard>() = MakeWildcard("body", "%at");
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::ByAutomaton>::id());
  const auto& ba = sdb::basics::downCast<irs::ByAutomaton>(*root);
  ASSERT_EQ(ba.field(), "body");

  const auto& nodes = ba.options().nodes;
  ASSERT_EQ(nodes.size(), 3);
  ASSERT_EQ(nodes[0].kind, irs::AutomatonNode::Kind::Leaf);
  ASSERT_EQ(nodes[0].pattern.kind, irs::AutomatonPattern::Kind::Wildcard);
  ASSERT_EQ(nodes[1].kind, irs::AutomatonNode::Kind::Leaf);
  ASSERT_EQ(nodes[1].pattern.kind, irs::AutomatonPattern::Kind::Wildcard);
  ASSERT_EQ(nodes[2].kind, irs::AutomatonNode::Kind::Intersection);
}

TEST_P(FilterRuleTestCase,
       AutomatonFilterRule_Intersection_MergesWildcardAndRegexpOnSameField) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AutomatonFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& f = sdb::basics::downCast<irs::And>(*root);
  f.add<irs::ByWildcard>() = MakeWildcard("body", "pro%");
  f.add<irs::ByRegexp>() = MakeRegexp("body", "^[a-z]{5,8}$");
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::ByAutomaton>::id());
  const auto& ba = sdb::basics::downCast<irs::ByAutomaton>(*root);
  ASSERT_EQ(ba.field(), "body");

  const auto& nodes = ba.options().nodes;
  ASSERT_EQ(nodes.size(), 3);
  const bool has_wildcard =
    nodes[0].pattern.kind == irs::AutomatonPattern::Kind::Wildcard ||
    nodes[1].pattern.kind == irs::AutomatonPattern::Kind::Wildcard;
  const bool has_regexp =
    nodes[0].pattern.kind == irs::AutomatonPattern::Kind::Regexp ||
    nodes[1].pattern.kind == irs::AutomatonPattern::Kind::Regexp;
  ASSERT_TRUE(has_wildcard);
  ASSERT_TRUE(has_regexp);
  ASSERT_EQ(nodes[2].kind, irs::AutomatonNode::Kind::Intersection);
}

TEST_P(FilterRuleTestCase,
       AutomatonFilterRule_Intersection_ThreeFiltersProduceCorrectPostfix) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AutomatonFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& f = sdb::basics::downCast<irs::And>(*root);
  f.add<irs::ByWildcard>() = MakeWildcard("word", "c%");
  f.add<irs::ByWildcard>() = MakeWildcard("word", "%at");
  f.add<irs::ByWildcard>() = MakeWildcard("word", "c_t");
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::ByAutomaton>::id());
  const auto& nodes =
    sdb::basics::downCast<irs::ByAutomaton>(*root).options().nodes;

  ASSERT_EQ(nodes.size(), 5);
  ASSERT_EQ(nodes[0].kind, irs::AutomatonNode::Kind::Leaf);
  ASSERT_EQ(nodes[1].kind, irs::AutomatonNode::Kind::Leaf);
  ASSERT_EQ(nodes[2].kind, irs::AutomatonNode::Kind::Intersection);
  ASSERT_EQ(nodes[3].kind, irs::AutomatonNode::Kind::Leaf);
  ASSERT_EQ(nodes[4].kind, irs::AutomatonNode::Kind::Intersection);
}

TEST_P(FilterRuleTestCase,
       AutomatonFilterRule_Intersection_DifferentFieldsKeptSeparate) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AutomatonFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& f = sdb::basics::downCast<irs::And>(*root);
  f.add<irs::ByWildcard>() = MakeWildcard("title", "c%");
  f.add<irs::ByWildcard>() = MakeWildcard("body", "%at");
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::And>::id());
  const auto& result = sdb::basics::downCast<irs::And>(*root);
  ASSERT_EQ(result.size(), 2);
  for (size_t i = 0; i < 2; ++i) {
    ASSERT_EQ(result[i].type(), irs::Type<irs::ByWildcard>::id());
  }
}

TEST_P(FilterRuleTestCase,
       AutomatonFilterRule_Intersection_SingleFilterNotMerged) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AutomatonFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& f = sdb::basics::downCast<irs::And>(*root);
  f.add<irs::ByWildcard>() = MakeWildcard("body", "c%");
  f.add<irs::Empty>();
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::And>::id());
  const auto& result = sdb::basics::downCast<irs::And>(*root);
  ASSERT_EQ(result.size(), 2);
}

TEST_P(FilterRuleTestCase,
       AutomatonFilterRule_Intersection_FlattensByAutomatonSubTree) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AutomatonFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& f = sdb::basics::downCast<irs::And>(*root);

  auto& existing = f.add<irs::ByAutomaton>();
  *existing.mutable_field() = "body";
  existing.mutable_options()->nodes = {
    irs::AutomatonNode::MakeLeaf(
      {AsBytes("cat%"), irs::AutomatonPattern::Kind::Wildcard}),
    irs::AutomatonNode::MakeLeaf(
      {AsBytes("c_t"), irs::AutomatonPattern::Kind::Wildcard}),
    irs::AutomatonNode::MakeIntersection(),
  };

  f.add<irs::ByWildcard>() = MakeWildcard("body", "%t");
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::ByAutomaton>::id());
  const auto& nodes =
    sdb::basics::downCast<irs::ByAutomaton>(*root).options().nodes;

  ASSERT_EQ(nodes.size(), 5);
  const size_t intersect_count =
    std::count_if(nodes.begin(), nodes.end(), [](const irs::AutomatonNode& n) {
      return n.kind == irs::AutomatonNode::Kind::Intersection;
    });
  ASSERT_EQ(intersect_count, 2);
  ASSERT_EQ(nodes.back().kind, irs::AutomatonNode::Kind::Intersection);
}

TEST_P(FilterRuleTestCase,
       AutomatonFilterRule_Intersection_PartialMerge_TwoFieldsOneGetssMerged) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AutomatonFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& f = sdb::basics::downCast<irs::And>(*root);
  f.add<irs::ByWildcard>() = MakeWildcard("body", "c%");
  f.add<irs::ByWildcard>() = MakeWildcard("body", "%at");
  f.add<irs::ByWildcard>() = MakeWildcard("title", "news%");
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::And>::id());
  const auto& result = sdb::basics::downCast<irs::And>(*root);
  ASSERT_EQ(result.size(), 2);

  size_t automaton_count = 0;
  size_t wildcard_count = 0;
  for (size_t i = 0; i < result.size(); ++i) {
    if (result[i].type() == irs::Type<irs::ByAutomaton>::id()) {
      ++automaton_count;
    } else if (result[i].type() == irs::Type<irs::ByWildcard>::id()) {
      ++wildcard_count;
    }
  }
  ASSERT_EQ(automaton_count, 1);
  ASSERT_EQ(wildcard_count, 1);
}

TEST_P(FilterRuleTestCase,
       AutomatonFilterRule_Union_MergesTwoWildcardsOnSameField) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AutomatonFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::Or>();
  auto& f = sdb::basics::downCast<irs::Or>(*root);
  f.add<irs::ByWildcard>() = MakeWildcard("body", "cat%");
  f.add<irs::ByWildcard>() = MakeWildcard("body", "dog%");
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::ByAutomaton>::id());
  const auto& ba = sdb::basics::downCast<irs::ByAutomaton>(*root);
  ASSERT_EQ(ba.field(), "body");

  const auto& nodes = ba.options().nodes;
  ASSERT_EQ(nodes.size(), 3);
  ASSERT_EQ(nodes[0].kind, irs::AutomatonNode::Kind::Leaf);
  ASSERT_EQ(nodes[1].kind, irs::AutomatonNode::Kind::Leaf);
  ASSERT_EQ(nodes[2].kind, irs::AutomatonNode::Kind::Union);
  ASSERT_EQ(nodes[2].union_method,
            irs::AutomatonUnionMethod::RefinedDeterminize);
}

TEST_P(FilterRuleTestCase,
       AutomatonFilterRule_Union_DeMorganVariantStoredInNode) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AutomatonFilterRule>(
    irs::AutomatonUnionMethod::DeMorgan);

  irs::Filter::ptr root = std::make_unique<irs::Or>();
  auto& f = sdb::basics::downCast<irs::Or>(*root);
  f.add<irs::ByWildcard>() = MakeWildcard("body", "cat%");
  f.add<irs::ByWildcard>() = MakeWildcard("body", "dog%");
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::ByAutomaton>::id());
  const auto& nodes =
    sdb::basics::downCast<irs::ByAutomaton>(*root).options().nodes;
  ASSERT_EQ(nodes[2].kind, irs::AutomatonNode::Kind::Union);
  ASSERT_EQ(nodes[2].union_method, irs::AutomatonUnionMethod::DeMorgan);
}

TEST_P(FilterRuleTestCase, AutomatonFilterRule_Union_MixedWildcardAndRegexp) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AutomatonFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::Or>();
  auto& f = sdb::basics::downCast<irs::Or>(*root);
  f.add<irs::ByWildcard>() = MakeWildcard("code", "SKU-%");
  f.add<irs::ByRegexp>() = MakeRegexp("code", "^[0-9]{8,13}$");
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::ByAutomaton>::id());
  const auto& nodes =
    sdb::basics::downCast<irs::ByAutomaton>(*root).options().nodes;
  ASSERT_EQ(nodes.size(), 3);
  ASSERT_EQ(nodes[2].kind, irs::AutomatonNode::Kind::Union);
}

TEST_P(FilterRuleTestCase,
       AutomatonFilterRule_Union_DifferentFieldsKeptSeparate) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AutomatonFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::Or>();
  auto& f = sdb::basics::downCast<irs::Or>(*root);
  f.add<irs::ByWildcard>() = MakeWildcard("title", "cat%");
  f.add<irs::ByWildcard>() = MakeWildcard("body", "dog%");
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::Or>::id());
  const auto& result = sdb::basics::downCast<irs::Or>(*root);
  ASSERT_EQ(result.size(), 2);
  for (size_t i = 0; i < 2; ++i) {
    ASSERT_EQ(result[i].type(), irs::Type<irs::ByWildcard>::id());
  }
}

TEST_P(FilterRuleTestCase,
       AutomatonFilterRule_Union_FlattensByAutomatonSubTree) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AutomatonFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::Or>();
  auto& f = sdb::basics::downCast<irs::Or>(*root);

  auto& existing = f.add<irs::ByAutomaton>();
  *existing.mutable_field() = "body";
  existing.mutable_options()->nodes = {
    irs::AutomatonNode::MakeLeaf(
      {AsBytes("cat%"), irs::AutomatonPattern::Kind::Wildcard}),
    irs::AutomatonNode::MakeLeaf(
      {AsBytes("dog%"), irs::AutomatonPattern::Kind::Wildcard}),
    irs::AutomatonNode::MakeUnion(),
  };

  f.add<irs::ByWildcard>() = MakeWildcard("body", "fish%");
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::ByAutomaton>::id());
  const auto& nodes =
    sdb::basics::downCast<irs::ByAutomaton>(*root).options().nodes;

  ASSERT_EQ(nodes.size(), 5);
  const size_t union_count =
    std::count_if(nodes.begin(), nodes.end(), [](const irs::AutomatonNode& n) {
      return n.kind == irs::AutomatonNode::Kind::Union;
    });
  ASSERT_EQ(union_count, 2);
  ASSERT_EQ(nodes.back().kind, irs::AutomatonNode::Kind::Union);
}

TEST_P(FilterRuleTestCase,
       AutomatonFilterRule_Union_ThreeFiltersProduceCorrectPostfix) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AutomatonFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::Or>();
  auto& f = sdb::basics::downCast<irs::Or>(*root);
  f.add<irs::ByWildcard>() = MakeWildcard("tag", "cat%");
  f.add<irs::ByWildcard>() = MakeWildcard("tag", "dog%");
  f.add<irs::ByRegexp>() = MakeRegexp("tag", "^fish");
  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::ByAutomaton>::id());
  const auto& nodes =
    sdb::basics::downCast<irs::ByAutomaton>(*root).options().nodes;

  ASSERT_EQ(nodes.size(), 5);
  ASSERT_EQ(nodes[2].kind, irs::AutomatonNode::Kind::Union);
  ASSERT_EQ(nodes[4].kind, irs::AutomatonNode::Kind::Union);
}

TEST_P(FilterRuleTestCase, AutomatonRules_BothRulesChained_AndOfOrExpressions) {
  irs::FilterRulesConstructor constructor;
  constructor.Add<irs::AutomatonFilterRule>();

  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& and_f = sdb::basics::downCast<irs::And>(*root);

  auto& or1 = and_f.add<irs::Or>();
  or1.add<irs::ByWildcard>() = MakeWildcard("body", "cat%");
  or1.add<irs::ByWildcard>() = MakeWildcard("body", "dog%");

  auto& or2 = and_f.add<irs::Or>();
  or2.add<irs::ByRegexp>() = MakeRegexp("body", "^[a-z]+$");
  or2.add<irs::ByRegexp>() = MakeRegexp("body", "^[0-9]+$");

  root = constructor.Apply(std::move(root));

  ASSERT_EQ(root->type(), irs::Type<irs::ByAutomaton>::id());
  const auto& nodes =
    sdb::basics::downCast<irs::ByAutomaton>(*root).options().nodes;

  ASSERT_GE(nodes.size(), 3);
  ASSERT_EQ(nodes.back().kind, irs::AutomatonNode::Kind::Intersection);
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(rule_filter_test, FilterRuleTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_gsimd")),
                         FilterRuleTestCase::to_string);

}  // namespace
