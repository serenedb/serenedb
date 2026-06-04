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
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/bm25.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/mixed_boolean_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/terms_filter.hpp"
#include "tests_shared.hpp"

namespace {

template<typename Filter>
Filter MakeFilter(std::string_view field, std::string_view term) {
  Filter q;
  *q.mutable_field() = field;
  q.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  return q;
}

template<typename Filter>
Filter& Configure(Filter& sub, std::string_view name, std::string_view term) {
  *sub.mutable_field() = name;
  sub.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  return sub;
}

template<typename Filter = irs::ByTerm>
std::unique_ptr<Filter> MakeTerm(std::string_view name, std::string_view term) {
  return tests::Make<Filter>([&](Filter& sub) { Configure(sub, name, term); });
}

std::unique_ptr<irs::ByTerms> MakeByTerms(
  std::string_view field, std::initializer_list<std::string_view> terms,
  size_t min_match) {
  auto by_terms = std::make_unique<irs::ByTerms>();
  *by_terms->mutable_field() = field;
  for (const auto term : terms) {
    by_terms->mutable_options()->terms.emplace(
      irs::ViewCast<irs::byte_type>(term));
  }
  by_terms->mutable_options()->min_match = min_match;
  return by_terms;
}

}  // namespace
namespace tests {

TEST(filter_optimizer_test, not_is_preserved) {
  irs::Filter::ptr root = std::make_unique<irs::Not>(
    std::make_unique<irs::ByTerm>(MakeFilter<irs::ByTerm>("name", "A")));
  auto& node = sdb::basics::downCast<irs::Not>(*root);
  node.boost(2.5F);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Not>::id(), root->type());
  auto& not_node = sdb::basics::downCast<irs::Not>(*root);
  ASSERT_EQ(2.5F, not_node.Boost());
  ASSERT_NE(nullptr, not_node.filter());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), not_node.filter()->type());
}

TEST(filter_optimizer_test, empty_not_becomes_empty) {
  irs::Filter::ptr root = std::make_unique<irs::Not>();

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Empty>::id(), root->type());
}

TEST(filter_optimizer_test, not_all_becomes_empty) {
  irs::Filter::ptr root =
    std::make_unique<irs::Not>(std::make_unique<irs::All>());

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Empty>::id(), root->type());
}

TEST(filter_optimizer_test, double_negation_unwraps) {
  auto term =
    std::make_unique<irs::ByTerm>(MakeFilter<irs::ByTerm>("name", "A"));
  const auto* term_ptr = term.get();
  irs::Filter::ptr root =
    std::make_unique<irs::Not>(std::make_unique<irs::Not>(std::move(term)));

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), root->type());
  ASSERT_EQ(term_ptr, root.get());
}

TEST(filter_optimizer_test, triple_negation_keeps_one) {
  irs::Filter::ptr root = std::make_unique<irs::Not>(
    std::make_unique<irs::Not>(std::make_unique<irs::Not>(
      std::make_unique<irs::ByTerm>(MakeFilter<irs::ByTerm>("name", "A")))));

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Not>::id(), root->type());
  auto& not_node = sdb::basics::downCast<irs::Not>(*root);
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), not_node.filter()->type());
}

TEST(filter_optimizer_test, deep_negation_chain) {
  irs::Filter::ptr root =
    std::make_unique<irs::ByTerm>(MakeFilter<irs::ByTerm>("name", "A"));
  for (size_t i = 0; i < 50; ++i) {
    root = std::make_unique<irs::Not>(std::move(root));
  }

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), root->type());
}

TEST(filter_optimizer_test, not_inside_and) {
  irs::Filter::ptr root =
    MakeAnd(MakeTerm("name", "A"), MakeNot(MakeTerm("name", "B")));
  auto& and_root = sdb::basics::downCast<irs::And>(*root);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::And>::id(), root->type());
  ASSERT_EQ(1, and_root.size());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[0].type());
  ASSERT_EQ(1, and_root.ExcludesSize());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root.Exclude(0).type());
}

TEST(filter_optimizer_test, flatten_and) {
  irs::Filter::ptr root = MakeAnd(
    MakeAnd(MakeTerm("f1", "A"), MakeTerm("f2", "B")), MakeTerm("f3", "C"));
  auto& and_root = sdb::basics::downCast<irs::And>(*root);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::And>::id(), root->type());
  ASSERT_EQ(3, and_root.size());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[0].type());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[1].type());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[2].type());
}

TEST(filter_optimizer_test, flatten_and_deep) {
  irs::Filter::ptr root =
    MakeAnd(MakeAnd(MakeAnd(MakeTerm("f1", "A")), MakeTerm("f2", "B")),
            MakeTerm("f3", "C"));
  auto& and_root = sdb::basics::downCast<irs::And>(*root);

  irs::Optimize(root);

  ASSERT_EQ(3, and_root.size());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[0].type());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[1].type());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[2].type());
}

TEST(filter_optimizer_test, flatten_and_boost_gate) {
  irs::Filter::ptr root = MakeAnd(
    [] {
      std::vector<irs::Filter::ptr> children;
      children.emplace_back(MakeTerm("name", "A"));
      children.emplace_back(MakeTerm("name", "B"));
      auto inner = std::make_unique<irs::And>(std::move(children));
      inner->boost(2.F);
      return inner;
    }(),
    MakeTerm("name", "C"));
  auto& and_root = sdb::basics::downCast<irs::And>(*root);

  irs::Optimize(root);

  ASSERT_EQ(2, and_root.size());
  ASSERT_EQ(irs::Type<irs::And>::id(), and_root[0].type());
}

TEST(filter_optimizer_test, flatten_and_merge_type_gate) {
  irs::Filter::ptr root = MakeAnd(
    [] {
      std::vector<irs::Filter::ptr> children;
      children.emplace_back(MakeTerm("name", "A"));
      children.emplace_back(MakeTerm("name", "B"));
      auto inner = std::make_unique<irs::And>(std::move(children),
                                              irs::ScoreMergeType::Max);
      return inner;
    }(),
    MakeTerm("name", "C"));
  auto& and_root = sdb::basics::downCast<irs::And>(*root);

  irs::Optimize(root);

  ASSERT_EQ(2, and_root.size());
  ASSERT_EQ(irs::Type<irs::And>::id(), and_root[0].type());
}

TEST(filter_optimizer_test, flatten_and_empty_inner_gate) {
  irs::Filter::ptr root = MakeAnd(MakeAnd(), MakeTerm("name", "C"));
  auto& and_root = sdb::basics::downCast<irs::And>(*root);

  irs::Optimize(root);

  ASSERT_EQ(2, and_root.size());
  ASSERT_EQ(irs::Type<irs::And>::id(), and_root[0].type());
  ASSERT_EQ(0, sdb::basics::downCast<irs::And>(and_root[0]).size());
}

TEST(filter_optimizer_test, flatten_or) {
  irs::Filter::ptr root = MakeOr(
    MakeOr(MakeTerm("f1", "A"), MakeTerm("f2", "B")), MakeTerm("f3", "C"));
  auto& or_root = sdb::basics::downCast<irs::Or>(*root);

  irs::Optimize(root);

  ASSERT_EQ(3, or_root.size());
}

TEST(filter_optimizer_test, flatten_or_parent_min_match_gate) {
  irs::Filter::ptr root = std::make_unique<irs::Or>(
    Filters(MakeOr(MakeTerm("name", "A"), MakeTerm("name", "B")),
            MakeTerm("name", "C")),
    irs::ScoreMergeType::Sum, 2);
  auto& or_root = sdb::basics::downCast<irs::Or>(*root);

  irs::Optimize(root);

  ASSERT_EQ(2, or_root.size());
}

TEST(filter_optimizer_test, flatten_or_inner_min_match_gate) {
  irs::Filter::ptr root = MakeOr(
    [] {
      std::vector<irs::Filter::ptr> children;
      children.emplace_back(MakeTerm("name", "A"));
      children.emplace_back(MakeTerm("name", "B"));
      return std::make_unique<irs::Or>(std::move(children),
                                       irs::ScoreMergeType::Sum, 2);
    }(),
    MakeTerm("name", "C"));
  auto& or_root = sdb::basics::downCast<irs::Or>(*root);

  irs::Optimize(root);

  ASSERT_EQ(2, or_root.size());
}

TEST(filter_optimizer_test, flatten_or_zero_min_match_gate) {
  irs::Filter::ptr root = MakeOr(
    [] {
      std::vector<irs::Filter::ptr> children;
      children.emplace_back(MakeTerm("name", "A"));
      return std::make_unique<irs::Or>(std::move(children),
                                       irs::ScoreMergeType::Sum, 0);
    }(),
    MakeTerm("name", "C"));
  auto& or_root = sdb::basics::downCast<irs::Or>(*root);

  irs::Optimize(root);

  ASSERT_EQ(2, or_root.size());
}

TEST(filter_optimizer_test, mixed_boolean_filter_subtrees) {
  std::vector<irs::Filter::ptr> required;
  required.emplace_back(std::make_unique<irs::Not>(
    std::make_unique<irs::ByTerm>(MakeFilter<irs::ByTerm>("name", "A"))));
  std::vector<irs::Filter::ptr> optional;
  optional.emplace_back(std::make_unique<irs::Not>(
    std::make_unique<irs::ByTerm>(MakeFilter<irs::ByTerm>("name", "B"))));
  irs::Filter::ptr root = std::make_unique<irs::MixedBooleanFilter>(
    std::move(required), std::move(optional));
  auto& mixed = sdb::basics::downCast<irs::MixedBooleanFilter>(*root);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::MixedBooleanFilter>::id(), root->type());
  ASSERT_EQ(irs::Type<irs::And>::id(), mixed.RequiredSlot()->type());
  ASSERT_EQ(irs::Type<irs::Not>::id(), mixed.OptionalSlot()->type());
  ASSERT_FALSE(mixed.empty());
}

TEST(filter_optimizer_test, mixed_boolean_filter_keeps_multi_clause_slots) {
  std::vector<irs::Filter::ptr> required;
  required.emplace_back(MakeTerm("f1", "A"));
  required.emplace_back(MakeTerm("f2", "B"));
  std::vector<irs::Filter::ptr> optional;
  optional.emplace_back(MakeTerm("f1", "C"));
  optional.emplace_back(MakeTerm("f2", "D"));
  irs::Filter::ptr root = std::make_unique<irs::MixedBooleanFilter>(
    std::move(required), std::move(optional));
  auto& mixed = sdb::basics::downCast<irs::MixedBooleanFilter>(*root);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::And>::id(), mixed.RequiredSlot()->type());
  ASSERT_EQ(irs::Type<irs::Or>::id(), mixed.OptionalSlot()->type());
}

TEST(filter_optimizer_test, idempotent) {
  const auto make = [] {
    irs::Filter::ptr root =
      MakeAnd(MakeAnd(MakeTerm("name", "A"), MakeNot(MakeTerm("name", "B"))),
              MakeOr(MakeOr(MakeTerm("name", "C")),
                     MakeNot(MakeNot(MakeTerm("name", "D")))));
    return root;
  };

  auto once = make();
  auto twice = make();
  irs::Optimize(once);
  irs::Optimize(twice);
  irs::Optimize(twice);

  ASSERT_TRUE(*once == *twice);
}

TEST(filter_optimizer_test, leaf_root_pointer_identity) {
  irs::Filter::ptr root =
    std::make_unique<irs::ByTerm>(MakeFilter<irs::ByTerm>("name", "A"));
  const auto* raw = root.get();

  irs::Optimize(root);

  ASSERT_EQ(raw, root.get());
}

TEST(filter_optimizer_test, custom_rule_subset) {
  irs::Filter::ptr root =
    MakeAnd(MakeAnd(MakeTerm("name", "A"), MakeTerm("name", "B")),
            MakeNot(MakeTerm("name", "C")));
  auto& and_root = sdb::basics::downCast<irs::And>(*root);

  irs::Optimize(root, {}, irs::kDefaultRules.subspan(1));

  ASSERT_EQ(3, and_root.size());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[0].type());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[1].type());
  ASSERT_EQ(irs::Type<irs::Not>::id(), and_root[2].type());
}

TEST(filter_optimizer_test, and_with_empty_child_collapses) {
  irs::Filter::ptr root = MakeAnd(MakeTerm("name", "A"), Make<irs::Empty>());

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Empty>::id(), root->type());
}

TEST(filter_optimizer_test, or_empty_children_removed) {
  irs::Filter::ptr root =
    MakeOr(MakeTerm("f1", "A"), Make<irs::Empty>(), MakeTerm("f2", "B"));
  auto& or_root = sdb::basics::downCast<irs::Or>(*root);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Or>::id(), root->type());
  ASSERT_EQ(2, or_root.size());
}

TEST(filter_optimizer_test, or_all_empty_children_become_empty) {
  irs::Filter::ptr root = MakeOr(Make<irs::Empty>(), Make<irs::Empty>());

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Empty>::id(), root->type());
}

TEST(filter_optimizer_test, and_all_fold_unscored_drops_alls) {
  irs::Filter::ptr root =
    MakeAnd(MakeTerm("name", "A"),
            Make<irs::All>([](irs::All& all) { all.boost(2.F); }),
            Make<irs::All>([](irs::All& all) { all.boost(3.F); }));

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), root->type());
}

TEST(filter_optimizer_test, and_all_fold_scored_merges) {
  irs::Filter::ptr root =
    MakeAnd(MakeTerm("name", "A"),
            Make<irs::All>([](irs::All& all) { all.boost(2.F); }),
            Make<irs::All>([](irs::All& all) { all.boost(3.F); }));
  auto& and_root = sdb::basics::downCast<irs::And>(*root);

  const irs::BM25 scorer;
  irs::Optimize(root, {.scorer = &scorer});

  ASSERT_EQ(irs::Type<irs::And>::id(), root->type());
  ASSERT_EQ(2, and_root.size());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[0].type());
  ASSERT_EQ(irs::Type<irs::All>::id(), and_root[1].type());
  ASSERT_EQ(5.F, sdb::basics::downCast<irs::All>(and_root[1]).Boost());
}

TEST(filter_optimizer_test, and_only_alls_becomes_all) {
  irs::Filter::ptr root =
    MakeAnd(Make<irs::All>([](irs::All& all) { all.boost(2.F); }),
            Make<irs::All>([](irs::All& all) { all.boost(3.F); }));

  const irs::BM25 scorer;
  irs::Optimize(root, {.scorer = &scorer});

  ASSERT_EQ(irs::Type<irs::All>::id(), root->type());
  ASSERT_EQ(5.F, sdb::basics::downCast<irs::All>(*root).Boost());
}

TEST(filter_optimizer_test, and_single_all_scored_unchanged) {
  irs::Filter::ptr root =
    MakeAnd(MakeTerm("name", "A"),
            Make<irs::All>([](irs::All& all) { all.boost(2.F); }));
  auto& and_root = sdb::basics::downCast<irs::And>(*root);

  const irs::BM25 scorer;
  irs::Optimize(root, {.scorer = &scorer});

  ASSERT_EQ(irs::Type<irs::And>::id(), root->type());
  ASSERT_EQ(2, and_root.size());
}

TEST(filter_optimizer_test, or_all_fold_unscored_prunes_to_all) {
  irs::Filter::ptr root = MakeOr(MakeTerm("name", "A"), Make<irs::All>());

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::All>::id(), root->type());
}

TEST(filter_optimizer_test, or_all_fold_scored_merges_and_adjusts_min_match) {
  irs::Filter::ptr root = std::make_unique<irs::Or>(
    Filters(MakeTerm("f1", "A"), MakeTerm("f2", "B"),
            Make<irs::All>([](irs::All& all) { all.boost(2.F); }),
            Make<irs::All>([](irs::All& all) { all.boost(3.F); })),
    irs::ScoreMergeType::Sum, 3);

  const irs::BM25 scorer;
  irs::Optimize(root, {.scorer = &scorer});

  ASSERT_EQ(irs::Type<irs::Or>::id(), root->type());
  auto& or_root = sdb::basics::downCast<irs::Or>(*root);
  ASSERT_EQ(3, or_root.size());
  ASSERT_EQ(2, or_root.MinMatchCount());
  ASSERT_EQ(irs::Type<irs::All>::id(), or_root[2].type());
  ASSERT_EQ(5.F, sdb::basics::downCast<irs::All>(or_root[2]).Boost());
}

TEST(filter_optimizer_test, or_single_all_scored_unchanged) {
  irs::Filter::ptr root =
    MakeOr(MakeTerm("name", "A"),
           Make<irs::All>([](irs::All& all) { all.boost(2.F); }));
  auto& or_root = sdb::basics::downCast<irs::Or>(*root);

  const irs::BM25 scorer;
  irs::Optimize(root, {.scorer = &scorer});

  ASSERT_EQ(irs::Type<irs::Or>::id(), root->type());
  ASSERT_EQ(2, or_root.size());
}

TEST(filter_optimizer_test, single_child_and_unwraps) {
  auto term = MakeTerm("name", "A");
  const auto* term_ptr = term.get();
  irs::Filter::ptr root = MakeAnd(std::move(term));

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), root->type());
  ASSERT_EQ(term_ptr, root.get());
}

TEST(filter_optimizer_test, single_child_or_min_match_gate) {
  irs::Filter::ptr root = std::make_unique<irs::Or>(
    Filters(MakeTerm("name", "A")), irs::ScoreMergeType::Sum, 2);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Or>::id(), root->type());
}

TEST(filter_optimizer_test, single_child_boost_gate) {
  const auto make = []() -> irs::Filter::ptr {
    auto root = MakeAnd(MakeTerm("name", "A"));
    root->boost(2.F);
    return root;
  };

  auto scored = make();
  const irs::BM25 scorer;
  irs::Optimize(scored, {.scorer = &scorer});
  ASSERT_EQ(irs::Type<irs::And>::id(), scored->type());

  auto unscored = make();
  irs::Optimize(unscored);
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), unscored->type());
}

TEST(filter_optimizer_test, by_terms_and) {
  auto and_root = MakeAnd(MakeTerm("name", "A"), MakeTerm("name", "B"));
  and_root->boost(2.F);
  irs::Filter::ptr root = std::move(and_root);

  const irs::BM25 scorer;
  irs::Optimize(root, {.scorer = &scorer});

  ASSERT_EQ(irs::Type<irs::ByTerms>::id(), root->type());
  auto& by_terms = sdb::basics::downCast<irs::ByTerms>(*root);
  ASSERT_EQ("name", by_terms.field());
  ASSERT_EQ(2, by_terms.options().terms.size());
  ASSERT_EQ(2, by_terms.options().min_match);
  ASSERT_EQ(2.F, by_terms.Boost());
}

TEST(filter_optimizer_test, by_terms_or) {
  irs::Filter::ptr root = MakeOr(MakeTerm("name", "A"), MakeTerm("name", "B"));

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::ByTerms>::id(), root->type());
  auto& by_terms = sdb::basics::downCast<irs::ByTerms>(*root);
  ASSERT_EQ(2, by_terms.options().terms.size());
  ASSERT_EQ(1, by_terms.options().min_match);
}

TEST(filter_optimizer_test, by_terms_field_gate) {
  irs::Filter::ptr root =
    MakeAnd(MakeTerm("name", "A"), MakeTerm("other", "B"));
  auto& and_root = sdb::basics::downCast<irs::And>(*root);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::And>::id(), root->type());
  ASSERT_EQ(2, and_root.size());
}

TEST(filter_optimizer_test, by_terms_or_duplicate_gate) {
  irs::Filter::ptr root = std::make_unique<irs::Or>(
    Filters(MakeTerm("name", "A"), MakeTerm("name", "A"),
            MakeTerm("name", "B")),
    irs::ScoreMergeType::Sum, 2);
  auto& or_root = sdb::basics::downCast<irs::Or>(*root);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Or>::id(), root->type());
  ASSERT_EQ(3, or_root.size());
}

TEST(filter_optimizer_test, or_min_match_zero_becomes_all) {
  auto or_node = std::make_unique<irs::Or>(Filters(MakeTerm("name", "A")),
                                           irs::ScoreMergeType::Sum, 0);
  or_node->boost(2.F);
  irs::Filter::ptr root = std::move(or_node);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::All>::id(), root->type());
  ASSERT_EQ(2.F, sdb::basics::downCast<irs::All>(*root).Boost());
}

TEST(filter_optimizer_test, or_min_match_zero_no_children_becomes_all) {
  irs::Filter::ptr root =
    std::make_unique<irs::Or>(Filters(), irs::ScoreMergeType::Sum, 0);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::All>::id(), root->type());
}

TEST(filter_optimizer_test, or_unsat_min_match_becomes_empty) {
  irs::Filter::ptr root = std::make_unique<irs::Or>(
    Filters(MakeTerm("name", "A")), irs::ScoreMergeType::Sum, 3);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Empty>::id(), root->type());
}

TEST(filter_optimizer_test, or_no_clauses_becomes_empty) {
  irs::Filter::ptr root = MakeOr();

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Empty>::id(), root->type());
}

TEST(filter_optimizer_test, or_all_required_becomes_and) {
  auto or_node =
    std::make_unique<irs::Or>(Filters(MakeTerm("f1", "A"), MakeTerm("f2", "B")),
                              irs::ScoreMergeType::Max, 2);
  or_node->boost(2.F);
  irs::Filter::ptr root = std::move(or_node);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::And>::id(), root->type());
  auto& and_root = sdb::basics::downCast<irs::And>(*root);
  ASSERT_EQ(2, and_root.size());
  ASSERT_EQ(irs::ScoreMergeType::Max, and_root.MergeType());
  ASSERT_EQ(2.F, and_root.Boost());
}

TEST(filter_optimizer_test, or_all_required_unwraps_not_child) {
  irs::Filter::ptr root = std::make_unique<irs::Or>(
    Filters(MakeTerm("f1", "A"), MakeNot(MakeTerm("f2", "B"))),
    irs::ScoreMergeType::Sum, 2);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::And>::id(), root->type());
  auto& and_root = sdb::basics::downCast<irs::And>(*root);
  ASSERT_EQ(1, and_root.size());
  ASSERT_EQ(1, and_root.ExcludesSize());
}

TEST(filter_optimizer_test, or_all_required_single_child_unaffected) {
  irs::Filter::ptr root = std::make_unique<irs::Or>(
    Filters(MakeTerm("name", "A")), irs::ScoreMergeType::Sum, 1);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), root->type());
}

TEST(filter_optimizer_test, mixed_empty_required_becomes_optional) {
  std::vector<irs::Filter::ptr> optional;
  optional.emplace_back(MakeTerm("f1", "A"));
  optional.emplace_back(MakeTerm("f2", "B"));
  irs::Filter::ptr root =
    std::make_unique<irs::MixedBooleanFilter>(Filters(), std::move(optional));

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Or>::id(), root->type());
  ASSERT_EQ(2, sdb::basics::downCast<irs::Or>(*root).size());
}

TEST(filter_optimizer_test, mixed_empty_optional_becomes_required) {
  std::vector<irs::Filter::ptr> required;
  required.emplace_back(MakeTerm("f1", "A"));
  required.emplace_back(MakeTerm("f2", "B"));
  irs::Filter::ptr root =
    std::make_unique<irs::MixedBooleanFilter>(std::move(required), Filters());

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::And>::id(), root->type());
  ASSERT_EQ(2, sdb::basics::downCast<irs::And>(*root).size());
}

TEST(filter_optimizer_test, by_terms_min_match_zero_unscored_becomes_all) {
  irs::Filter::ptr root = MakeByTerms("name", {"A", "B"}, 0);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::All>::id(), root->type());
}

TEST(filter_optimizer_test, by_terms_min_match_zero_scored_becomes_or) {
  auto by_terms = MakeByTerms("name", {"A", "B"}, 0);
  by_terms->boost(2.F);
  irs::Filter::ptr root = std::move(by_terms);

  const irs::BM25 scorer;
  irs::Optimize(root, {.scorer = &scorer});

  ASSERT_EQ(irs::Type<irs::Or>::id(), root->type());
  auto& or_root = sdb::basics::downCast<irs::Or>(*root);
  ASSERT_EQ(2, or_root.size());
  ASSERT_EQ(irs::Type<irs::All>::id(), or_root[0].type());
  ASSERT_EQ(0.F, sdb::basics::downCast<irs::All>(or_root[0]).Boost());
  ASSERT_EQ(irs::Type<irs::ByTerms>::id(), or_root[1].type());
  auto& terms = sdb::basics::downCast<irs::ByTerms>(or_root[1]);
  ASSERT_EQ(1, terms.options().min_match);
  ASSERT_EQ(2.F, terms.Boost());
}

TEST(filter_optimizer_test, by_terms_min_match_zero_empty_terms_unchanged) {
  irs::Filter::ptr root = MakeByTerms("name", {}, 0);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::ByTerms>::id(), root->type());
}

class FilterOptimizerTestCase : public FilterTestCaseBase {};

TEST_P(FilterOptimizerTestCase, optimized_equals_naive) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  Docs all_docs(32);
  std::iota(all_docs.begin(), all_docs.end(), 1);

  Docs all_but_first(31);
  std::iota(all_but_first.begin(), all_but_first.end(), 2);

  {
    const auto make = [] {
      irs::Filter::ptr root = std::make_unique<irs::Not>(
        std::make_unique<irs::ByTerm>(MakeFilter<irs::ByTerm>("name", "A")));
      return root;
    };
    auto naive = make();
    CheckQuery(*naive, all_but_first, rdr);
    auto optimized = make();
    irs::Optimize(optimized);
    CheckQuery(*optimized, all_but_first, rdr);
  }

  {
    const auto make = [] {
      irs::Filter::ptr root =
        MakeAnd(MakeTerm("duplicated", "abcd"), MakeNot(MakeTerm("name", "A")));
      return root;
    };
    auto naive = make();
    CheckQuery(*naive, Docs{5, 11, 21, 27, 31}, rdr);
    auto optimized = make();
    irs::Optimize(optimized);
    CheckQuery(*optimized, Docs{5, 11, 21, 27, 31}, rdr);
  }

  {
    const auto make = [] {
      irs::Filter::ptr root =
        MakeOr(MakeTerm("duplicated", "abcd"), MakeNot(MakeTerm("name", "A")));
      return root;
    };
    auto naive = make();
    CheckQuery(*naive, all_docs, rdr);
    auto optimized = make();
    irs::Optimize(optimized);
    CheckQuery(*optimized, all_docs, rdr);
  }

  {
    const auto make = [] {
      irs::Filter::ptr root =
        std::make_unique<irs::Not>(std::make_unique<irs::Not>(
          std::make_unique<irs::ByTerm>(MakeFilter<irs::ByTerm>("name", "A"))));
      return root;
    };
    auto naive = make();
    CheckQuery(*naive, Docs{1}, rdr);
    auto optimized = make();
    irs::Optimize(optimized);
    CheckQuery(*optimized, Docs{1}, rdr);
  }

  {
    const auto make = [] {
      irs::Filter::ptr root =
        MakeOr(MakeTerm("name", "V"), MakeNot(Make<irs::All>()));
      return root;
    };
    auto naive = make();
    CheckQuery(*naive, Docs{22}, rdr);
    auto optimized = make();
    irs::Optimize(optimized);
    CheckQuery(*optimized, Docs{22}, rdr);
  }

  {
    const auto make = [] {
      irs::Filter::ptr root =
        MakeAnd(MakeTerm("name", "V"), MakeNot(Make<irs::All>()));
      return root;
    };
    auto naive = make();
    CheckQuery(*naive, Docs{}, rdr);
    auto optimized = make();
    irs::Optimize(optimized);
    CheckQuery(*optimized, Docs{}, rdr);
  }

  {
    const auto make = [] {
      irs::Filter::ptr root = MakeAnd(
        MakeAnd(MakeTerm("duplicated", "abcd"), MakeTerm("same", "xyz")),
        MakeNot(MakeTerm("name", "A")));
      return root;
    };
    auto naive = make();
    CheckQuery(*naive, Docs{5, 11, 21, 27, 31}, rdr);
    auto optimized = make();
    irs::Optimize(optimized);
    ASSERT_EQ(3, sdb::basics::downCast<irs::And>(*optimized).size());
    CheckQuery(*optimized, Docs{5, 11, 21, 27, 31}, rdr);
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(filter_optimizer_test, FilterOptimizerTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd")),
                         FilterOptimizerTestCase::to_string);

}  // namespace tests
