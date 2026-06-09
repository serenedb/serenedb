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

inline constexpr irs::field_id kName = tests::FieldIdFor("name");
inline constexpr irs::field_id kOther = tests::FieldIdFor("other");
inline constexpr irs::field_id kF1 = tests::FieldIdFor("f1");
inline constexpr irs::field_id kF2 = tests::FieldIdFor("f2");
inline constexpr irs::field_id kF3 = tests::FieldIdFor("f3");
inline constexpr irs::field_id kF4 = tests::FieldIdFor("f4");
inline constexpr irs::field_id kDuplicated = tests::FieldIdFor("duplicated");
inline constexpr irs::field_id kSame = tests::FieldIdFor("same");

std::unique_ptr<irs::ByTerm> MakeTerm(irs::field_id field,
                                      std::string_view term) {
  auto f = std::make_unique<irs::ByTerm>();
  *f->mutable_field_id() = field;
  f->mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  return f;
}

std::unique_ptr<irs::ByTerms> MakeByTerms(
  irs::field_id field, std::initializer_list<std::string_view> terms,
  size_t min_match) {
  auto by_terms = std::make_unique<irs::ByTerms>();
  *by_terms->mutable_field_id() = field;
  for (const auto term : terms) {
    by_terms->mutable_options()->terms.emplace(
      irs::ViewCast<irs::byte_type>(term));
  }
  by_terms->mutable_options()->min_match = min_match;
  return by_terms;
}

irs::Filter::ptr MakeNot(irs::Filter::ptr child) {
  return std::make_unique<irs::Not>(std::move(child));
}

irs::Filter::ptr MakeExclude(irs::Filter::ptr include,
                             irs::Filter::ptr exclude) {
  auto ex = std::make_unique<irs::Exclusion>();
  if (include) {
    ex->include(std::move(include));
  }
  ex->exclude(std::move(exclude));
  return ex;
}

template<typename... Ts>
std::vector<irs::Filter::ptr> Filters(Ts&&... children) {
  std::vector<irs::Filter::ptr> result;
  result.reserve(sizeof...(children));
  (result.emplace_back(std::forward<Ts>(children)), ...);
  return result;
}

std::unique_ptr<irs::And> MakeAndV(
  std::vector<irs::Filter::ptr> children,
  irs::ScoreMergeType merge_type = irs::ScoreMergeType::Sum) {
  auto node = std::make_unique<irs::And>();
  node->merge_type(merge_type);
  for (auto& child : children) {
    node->add(std::move(child));
  }
  return node;
}

std::unique_ptr<irs::Or> MakeOrV(
  std::vector<irs::Filter::ptr> children,
  irs::ScoreMergeType merge_type = irs::ScoreMergeType::Sum,
  size_t min_match = 1) {
  auto node = std::make_unique<irs::Or>();
  node->merge_type(merge_type);
  node->min_match_count(min_match);
  for (auto& child : children) {
    node->add(std::move(child));
  }
  return node;
}

template<typename... Ts>
std::unique_ptr<irs::And> MakeAnd(Ts&&... children) {
  return MakeAndV(Filters(std::forward<Ts>(children)...));
}

template<typename... Ts>
std::unique_ptr<irs::Or> MakeOr(Ts&&... children) {
  return MakeOrV(Filters(std::forward<Ts>(children)...));
}

template<typename T, typename Fn>
std::unique_ptr<T> Make(Fn&& fn) {
  auto filter = std::make_unique<T>();
  fn(*filter);
  return filter;
}

template<typename T>
std::unique_ptr<T> Make() {
  return std::make_unique<T>();
}

std::unique_ptr<irs::All> MakeAll(irs::score_t boost = irs::kNoBoost) {
  auto all = std::make_unique<irs::All>();
  all->boost(boost);
  return all;
}

}  // namespace
namespace tests {

TEST(filter_optimizer_test, exclusion_is_preserved) {
  irs::Filter::ptr root = MakeNot(MakeTerm(kName, "A"));

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Exclusion>::id(), root->type());
  auto& node = sdb::basics::downCast<irs::Exclusion>(*root);
  ASSERT_EQ(nullptr, node.include());
  ASSERT_NE(nullptr, node.exclude());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), node.exclude()->type());
}

TEST(filter_optimizer_test, empty_exclusion_becomes_all) {
  irs::Filter::ptr root = std::make_unique<irs::Exclusion>();

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::All>::id(), root->type());
}

TEST(filter_optimizer_test, not_all_becomes_empty) {
  irs::Filter::ptr root = MakeNot(MakeAll());

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Empty>::id(), root->type());
}

TEST(filter_optimizer_test, double_negation_unwraps) {
  auto term = MakeTerm(kName, "A");
  const auto* term_ptr = term.get();
  irs::Filter::ptr root = MakeNot(MakeNot(std::move(term)));

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), root->type());
  ASSERT_EQ(term_ptr, root.get());
}

TEST(filter_optimizer_test, triple_negation_keeps_one) {
  irs::Filter::ptr root = MakeNot(MakeNot(MakeNot(MakeTerm(kName, "A"))));

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Exclusion>::id(), root->type());
  auto& node = sdb::basics::downCast<irs::Exclusion>(*root);
  ASSERT_NE(nullptr, node.exclude());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), node.exclude()->type());
}

TEST(filter_optimizer_test, deep_negation_chain) {
  irs::Filter::ptr root = MakeTerm(kName, "A");
  for (size_t i = 0; i < 50; ++i) {
    root = MakeNot(std::move(root));
  }

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), root->type());
}

TEST(filter_optimizer_test, not_inside_and) {
  irs::Filter::ptr root =
    MakeAnd(MakeTerm(kName, "A"), MakeNot(MakeTerm(kName, "B")));

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Exclusion>::id(), root->type());
  auto& node = sdb::basics::downCast<irs::Exclusion>(*root);
  ASSERT_NE(nullptr, node.include());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), node.include()->type());
  ASSERT_NE(nullptr, node.exclude());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), node.exclude()->type());
}

TEST(filter_optimizer_test, and_coalesces_multiple_nots) {
  irs::Filter::ptr root =
    MakeAnd(MakeTerm(kName, "A"), MakeTerm(kName, "B"),
            MakeNot(MakeTerm(kName, "X")), MakeNot(MakeTerm(kName, "Y")));

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Exclusion>::id(), root->type());
  auto& node = sdb::basics::downCast<irs::Exclusion>(*root);
  ASSERT_NE(nullptr, node.include());
  ASSERT_EQ(irs::Type<irs::And>::id(), node.include()->type());
  ASSERT_EQ(2, sdb::basics::downCast<irs::And>(*node.include()).size());
  ASSERT_NE(nullptr, node.exclude());
  ASSERT_EQ(irs::Type<irs::Or>::id(), node.exclude()->type());
  ASSERT_EQ(2, sdb::basics::downCast<irs::Or>(*node.exclude()).size());
}

TEST(filter_optimizer_test, and_only_nots_coalesce) {
  irs::Filter::ptr root =
    MakeAnd(MakeNot(MakeTerm(kName, "X")), MakeNot(MakeTerm(kName, "Y")));

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Exclusion>::id(), root->type());
  auto& node = sdb::basics::downCast<irs::Exclusion>(*root);
  ASSERT_EQ(nullptr, node.include());
  ASSERT_NE(nullptr, node.exclude());
  ASSERT_EQ(irs::Type<irs::Or>::id(), node.exclude()->type());
  ASSERT_EQ(2, sdb::basics::downCast<irs::Or>(*node.exclude()).size());
}

TEST(filter_optimizer_test, and_single_bare_not) {
  irs::Filter::ptr root = MakeAnd(MakeNot(MakeTerm(kName, "X")));

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Exclusion>::id(), root->type());
  auto& node = sdb::basics::downCast<irs::Exclusion>(*root);
  ASSERT_EQ(nullptr, node.include());
  ASSERT_NE(nullptr, node.exclude());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), node.exclude()->type());
}

TEST(filter_optimizer_test, flatten_and) {
  irs::Filter::ptr root = MakeAnd(
    MakeAnd(MakeTerm(kF1, "A"), MakeTerm(kF2, "B")), MakeTerm(kF3, "C"));

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::And>::id(), root->type());
  auto& and_root = sdb::basics::downCast<irs::And>(*root);
  ASSERT_EQ(3, and_root.size());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[0].type());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[1].type());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[2].type());
}

TEST(filter_optimizer_test, flatten_and_deep) {
  irs::Filter::ptr root =
    MakeAnd(MakeAnd(MakeAnd(MakeTerm(kF1, "A")), MakeTerm(kF2, "B")),
            MakeTerm(kF3, "C"));

  irs::Optimize(root);

  auto& and_root = sdb::basics::downCast<irs::And>(*root);
  ASSERT_EQ(3, and_root.size());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[0].type());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[1].type());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[2].type());
}

TEST(filter_optimizer_test, flatten_and_boost_gate) {
  auto inner = MakeAnd(MakeTerm(kF1, "A"), MakeTerm(kF2, "B"));
  inner->boost(2.F);
  irs::Filter::ptr root = MakeAnd(std::move(inner), MakeTerm(kF3, "C"));

  irs::Optimize(root);

  auto& and_root = sdb::basics::downCast<irs::And>(*root);
  ASSERT_EQ(2, and_root.size());
  ASSERT_EQ(irs::Type<irs::And>::id(), and_root[0].type());
}

TEST(filter_optimizer_test, flatten_and_merge_type_gate) {
  auto inner = MakeAndV(Filters(MakeTerm(kF1, "A"), MakeTerm(kF2, "B")),
                        irs::ScoreMergeType::Max);
  irs::Filter::ptr root = MakeAnd(std::move(inner), MakeTerm(kF3, "C"));

  irs::Optimize(root);

  auto& and_root = sdb::basics::downCast<irs::And>(*root);
  ASSERT_EQ(2, and_root.size());
  ASSERT_EQ(irs::Type<irs::And>::id(), and_root[0].type());
}

TEST(filter_optimizer_test, flatten_and_empty_inner_gate) {
  irs::Filter::ptr root = MakeAnd(MakeAnd(), MakeTerm(kName, "C"));

  irs::Optimize(root);

  auto& and_root = sdb::basics::downCast<irs::And>(*root);
  ASSERT_EQ(2, and_root.size());
  ASSERT_EQ(irs::Type<irs::And>::id(), and_root[0].type());
  ASSERT_EQ(0, sdb::basics::downCast<irs::And>(and_root[0]).size());
}

TEST(filter_optimizer_test, flatten_or) {
  irs::Filter::ptr root =
    MakeOr(MakeOr(MakeTerm(kF1, "A"), MakeTerm(kF2, "B")), MakeTerm(kF3, "C"));

  irs::Optimize(root);

  auto& or_root = sdb::basics::downCast<irs::Or>(*root);
  ASSERT_EQ(3, or_root.size());
}

TEST(filter_optimizer_test, flatten_or_parent_min_match_gate) {
  irs::Filter::ptr root =
    MakeOrV(Filters(MakeOr(MakeTerm(kF1, "A"), MakeTerm(kF2, "B")),
                    MakeTerm(kF3, "C"), MakeTerm(kF4, "D")),
            irs::ScoreMergeType::Sum, 2);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Or>::id(), root->type());
  auto& or_root = sdb::basics::downCast<irs::Or>(*root);
  ASSERT_EQ(3, or_root.size());
}

TEST(filter_optimizer_test, flatten_or_inner_min_match_gate) {
  auto inner = MakeOrV(Filters(MakeTerm(kName, "A"), MakeTerm(kName, "B")),
                       irs::ScoreMergeType::Sum, 2);
  irs::Filter::ptr root = MakeOr(std::move(inner), MakeTerm(kName, "C"));

  irs::Optimize(root);

  auto& or_root = sdb::basics::downCast<irs::Or>(*root);
  ASSERT_EQ(2, or_root.size());
}

TEST(filter_optimizer_test, flatten_or_zero_min_match_gate) {
  auto inner =
    MakeOrV(Filters(MakeTerm(kName, "A")), irs::ScoreMergeType::Sum, 0);
  irs::Filter::ptr root = MakeOr(std::move(inner), MakeTerm(kName, "C"));

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::All>::id(), root->type());
}

TEST(filter_optimizer_test, mixed_boolean_filter_subtrees) {
  irs::Filter::ptr root = std::make_unique<irs::MixedBooleanFilter>(
    Filters(MakeNot(MakeTerm(kName, "A"))),
    Filters(MakeNot(MakeTerm(kName, "B"))));
  auto& mixed = sdb::basics::downCast<irs::MixedBooleanFilter>(*root);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::MixedBooleanFilter>::id(), root->type());
  ASSERT_EQ(irs::Type<irs::Exclusion>::id(), mixed.RequiredSlot()->type());
  ASSERT_EQ(irs::Type<irs::Exclusion>::id(), mixed.OptionalSlot()->type());
  ASSERT_FALSE(mixed.empty());
}

TEST(filter_optimizer_test, mixed_boolean_filter_keeps_multi_clause_slots) {
  irs::Filter::ptr root = std::make_unique<irs::MixedBooleanFilter>(
    Filters(MakeTerm(kF1, "A"), MakeTerm(kF2, "B")),
    Filters(MakeTerm(kF1, "C"), MakeTerm(kF2, "D")));
  auto& mixed = sdb::basics::downCast<irs::MixedBooleanFilter>(*root);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::And>::id(), mixed.RequiredSlot()->type());
  ASSERT_EQ(irs::Type<irs::Or>::id(), mixed.OptionalSlot()->type());
}

TEST(filter_optimizer_test, idempotent) {
  const auto make = []() -> irs::Filter::ptr {
    return MakeAnd(MakeAnd(MakeTerm(kName, "A"), MakeNot(MakeTerm(kName, "B"))),
                   MakeOr(MakeOr(MakeTerm(kName, "C")),
                          MakeNot(MakeNot(MakeTerm(kName, "D")))));
  };

  irs::Filter::ptr once = make();
  irs::Filter::ptr twice = make();
  irs::Optimize(once);
  irs::Optimize(twice);
  irs::Optimize(twice);

  ASSERT_TRUE(*once == *twice);
}

TEST(filter_optimizer_test, leaf_root_pointer_identity) {
  irs::Filter::ptr root = MakeTerm(kName, "A");
  const auto* raw = root.get();

  irs::Optimize(root);

  ASSERT_EQ(raw, root.get());
}

TEST(filter_optimizer_test, custom_rule_subset) {
  irs::Filter::ptr root =
    MakeAnd(MakeAnd(MakeTerm(kF1, "A"), MakeTerm(kF2, "B")),
            MakeNot(MakeTerm(kName, "C")));

  irs::Optimize(root, {}, irs::kDefaultRules.subspan(1));

  ASSERT_EQ(irs::Type<irs::Exclusion>::id(), root->type());
  auto& node = sdb::basics::downCast<irs::Exclusion>(*root);
  ASSERT_NE(nullptr, node.include());
  ASSERT_EQ(irs::Type<irs::And>::id(), node.include()->type());
  ASSERT_EQ(2, sdb::basics::downCast<irs::And>(*node.include()).size());
  ASSERT_NE(nullptr, node.exclude());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), node.exclude()->type());
}

TEST(filter_optimizer_test, and_with_empty_child_collapses) {
  irs::Filter::ptr root = MakeAnd(MakeTerm(kName, "A"), Make<irs::Empty>());

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Empty>::id(), root->type());
}

TEST(filter_optimizer_test, or_empty_children_removed) {
  irs::Filter::ptr root =
    MakeOr(MakeTerm(kF1, "A"), Make<irs::Empty>(), MakeTerm(kF2, "B"));

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Or>::id(), root->type());
  ASSERT_EQ(2, sdb::basics::downCast<irs::Or>(*root).size());
}

TEST(filter_optimizer_test, or_all_empty_children_become_empty) {
  irs::Filter::ptr root = MakeOr(Make<irs::Empty>(), Make<irs::Empty>());

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Empty>::id(), root->type());
}

TEST(filter_optimizer_test, and_all_fold_unscored_drops_alls) {
  irs::Filter::ptr root =
    MakeAnd(MakeTerm(kName, "A"), MakeAll(2.F), MakeAll(3.F));

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), root->type());
}

TEST(filter_optimizer_test, and_all_fold_scored_merges) {
  irs::Filter::ptr root =
    MakeAnd(MakeTerm(kName, "A"), MakeAll(2.F), MakeAll(3.F));

  const irs::BM25 scorer;
  irs::Optimize(root, {.scorer = &scorer});

  ASSERT_EQ(irs::Type<irs::And>::id(), root->type());
  auto& and_root = sdb::basics::downCast<irs::And>(*root);
  ASSERT_EQ(2, and_root.size());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[0].type());
  ASSERT_EQ(irs::Type<irs::All>::id(), and_root[1].type());
  ASSERT_EQ(5.F, sdb::basics::downCast<irs::All>(and_root[1]).Boost());
}

TEST(filter_optimizer_test, and_only_alls_becomes_all) {
  irs::Filter::ptr root = MakeAnd(MakeAll(2.F), MakeAll(3.F));

  const irs::BM25 scorer;
  irs::Optimize(root, {.scorer = &scorer});

  ASSERT_EQ(irs::Type<irs::All>::id(), root->type());
  ASSERT_EQ(5.F, sdb::basics::downCast<irs::All>(*root).Boost());
}

TEST(filter_optimizer_test, and_single_all_scored_unchanged) {
  irs::Filter::ptr root = MakeAnd(MakeTerm(kName, "A"), MakeAll(2.F));

  const irs::BM25 scorer;
  irs::Optimize(root, {.scorer = &scorer});

  ASSERT_EQ(irs::Type<irs::And>::id(), root->type());
  ASSERT_EQ(2, sdb::basics::downCast<irs::And>(*root).size());
}

TEST(filter_optimizer_test, or_all_fold_unscored_prunes_to_all) {
  irs::Filter::ptr root = MakeOr(MakeTerm(kName, "A"), MakeAll());

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::All>::id(), root->type());
}

TEST(filter_optimizer_test, or_all_fold_scored_merges_and_adjusts_min_match) {
  irs::Filter::ptr root = MakeOrV(
    Filters(MakeTerm(kF1, "A"), MakeTerm(kF2, "B"), MakeAll(2.F), MakeAll(3.F)),
    irs::ScoreMergeType::Sum, 3);

  const irs::BM25 scorer;
  irs::Optimize(root, {.scorer = &scorer});

  ASSERT_EQ(irs::Type<irs::Or>::id(), root->type());
  auto& or_root = sdb::basics::downCast<irs::Or>(*root);
  ASSERT_EQ(3, or_root.size());
  ASSERT_EQ(2, or_root.min_match_count());
  ASSERT_EQ(irs::Type<irs::All>::id(), or_root[2].type());
  ASSERT_EQ(5.F, sdb::basics::downCast<irs::All>(or_root[2]).Boost());
}

TEST(filter_optimizer_test, or_single_all_scored_unchanged) {
  irs::Filter::ptr root = MakeOr(MakeTerm(kName, "A"), MakeAll(2.F));

  const irs::BM25 scorer;
  irs::Optimize(root, {.scorer = &scorer});

  ASSERT_EQ(irs::Type<irs::Or>::id(), root->type());
  ASSERT_EQ(2, sdb::basics::downCast<irs::Or>(*root).size());
}

TEST(filter_optimizer_test, single_child_and_unwraps) {
  auto term = MakeTerm(kName, "A");
  const auto* term_ptr = term.get();
  irs::Filter::ptr root = MakeAnd(std::move(term));

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), root->type());
  ASSERT_EQ(term_ptr, root.get());
}

TEST(filter_optimizer_test, single_child_or_min_match_gate) {
  irs::Filter::ptr root =
    MakeOrV(Filters(MakeTerm(kName, "A")), irs::ScoreMergeType::Sum, 2);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Empty>::id(), root->type());
}

TEST(filter_optimizer_test, single_child_boost_gate) {
  const auto make = []() -> irs::Filter::ptr {
    auto root = MakeAnd(MakeTerm(kName, "A"));
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
  auto and_root = MakeAnd(MakeTerm(kName, "A"), MakeTerm(kName, "B"));
  and_root->boost(2.F);
  irs::Filter::ptr root = std::move(and_root);

  const irs::BM25 scorer;
  irs::Optimize(root, {.scorer = &scorer});

  ASSERT_EQ(irs::Type<irs::ByTerms>::id(), root->type());
  auto& by_terms = sdb::basics::downCast<irs::ByTerms>(*root);
  ASSERT_EQ(kName, by_terms.field_id());
  ASSERT_EQ(2, by_terms.options().terms.size());
  ASSERT_EQ(2, by_terms.options().min_match);
  ASSERT_EQ(2.F, by_terms.Boost());
}

TEST(filter_optimizer_test, by_terms_or) {
  irs::Filter::ptr root = MakeOr(MakeTerm(kName, "A"), MakeTerm(kName, "B"));

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::ByTerms>::id(), root->type());
  auto& by_terms = sdb::basics::downCast<irs::ByTerms>(*root);
  ASSERT_EQ(2, by_terms.options().terms.size());
  ASSERT_EQ(1, by_terms.options().min_match);
}

TEST(filter_optimizer_test, by_terms_field_gate) {
  irs::Filter::ptr root = MakeAnd(MakeTerm(kName, "A"), MakeTerm(kOther, "B"));

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::And>::id(), root->type());
  ASSERT_EQ(2, sdb::basics::downCast<irs::And>(*root).size());
}

TEST(filter_optimizer_test, by_terms_or_duplicate_gate) {
  irs::Filter::ptr root = MakeOrV(
    Filters(MakeTerm(kName, "A"), MakeTerm(kName, "A"), MakeTerm(kName, "B")),
    irs::ScoreMergeType::Sum, 2);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Or>::id(), root->type());
  ASSERT_EQ(3, sdb::basics::downCast<irs::Or>(*root).size());
}

TEST(filter_optimizer_test, or_min_match_zero_becomes_all) {
  auto or_node =
    MakeOrV(Filters(MakeTerm(kName, "A")), irs::ScoreMergeType::Sum, 0);
  or_node->boost(2.F);
  irs::Filter::ptr root = std::move(or_node);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::All>::id(), root->type());
  ASSERT_EQ(2.F, sdb::basics::downCast<irs::All>(*root).Boost());
}

TEST(filter_optimizer_test, or_min_match_zero_no_children_becomes_all) {
  irs::Filter::ptr root = MakeOrV(Filters(), irs::ScoreMergeType::Sum, 0);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::All>::id(), root->type());
}

TEST(filter_optimizer_test, or_unsat_min_match_becomes_empty) {
  irs::Filter::ptr root =
    MakeOrV(Filters(MakeTerm(kName, "A")), irs::ScoreMergeType::Sum, 3);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Empty>::id(), root->type());
}

TEST(filter_optimizer_test, or_no_clauses_becomes_empty) {
  irs::Filter::ptr root = MakeOr();

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Empty>::id(), root->type());
}

TEST(filter_optimizer_test, or_all_required_becomes_and) {
  auto or_node = MakeOrV(Filters(MakeTerm(kF1, "A"), MakeTerm(kF2, "B")),
                         irs::ScoreMergeType::Max, 2);
  or_node->boost(2.F);
  irs::Filter::ptr root = std::move(or_node);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::And>::id(), root->type());
  auto& and_root = sdb::basics::downCast<irs::And>(*root);
  ASSERT_EQ(2, and_root.size());
  ASSERT_EQ(irs::ScoreMergeType::Max, and_root.merge_type());
  ASSERT_EQ(2.F, and_root.Boost());
}

TEST(filter_optimizer_test, or_all_required_unwraps_not_child) {
  irs::Filter::ptr root =
    MakeOrV(Filters(MakeTerm(kF1, "A"), MakeNot(MakeTerm(kF2, "B"))),
            irs::ScoreMergeType::Sum, 2);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Exclusion>::id(), root->type());
  auto& node = sdb::basics::downCast<irs::Exclusion>(*root);
  ASSERT_NE(nullptr, node.include());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), node.include()->type());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), node.exclude()->type());
}

TEST(filter_optimizer_test, or_all_required_single_child_unaffected) {
  irs::Filter::ptr root =
    MakeOrV(Filters(MakeTerm(kName, "A")), irs::ScoreMergeType::Sum, 1);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), root->type());
}

TEST(filter_optimizer_test, mixed_empty_required_becomes_optional) {
  irs::Filter::ptr root = std::make_unique<irs::MixedBooleanFilter>(
    Filters(), Filters(MakeTerm(kF1, "A"), MakeTerm(kF2, "B")));

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Or>::id(), root->type());
  ASSERT_EQ(2, sdb::basics::downCast<irs::Or>(*root).size());
}

TEST(filter_optimizer_test, mixed_empty_optional_becomes_required) {
  irs::Filter::ptr root = std::make_unique<irs::MixedBooleanFilter>(
    Filters(MakeTerm(kF1, "A"), MakeTerm(kF2, "B")), Filters());

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::And>::id(), root->type());
  ASSERT_EQ(2, sdb::basics::downCast<irs::And>(*root).size());
}

TEST(filter_optimizer_test, by_terms_min_match_zero_unscored_becomes_all) {
  irs::Filter::ptr root = MakeByTerms(kName, {"A", "B"}, 0);

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::All>::id(), root->type());
}

TEST(filter_optimizer_test, by_terms_min_match_zero_scored_becomes_or) {
  auto by_terms = MakeByTerms(kName, {"A", "B"}, 0);
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
  irs::Filter::ptr root = MakeByTerms(kName, {}, 0);

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
    auto naive = MakeExclude(nullptr, MakeTerm(kName, "A"));
    CheckQuery(*naive, all_but_first, rdr);
    irs::Filter::ptr optimized = MakeNot(MakeTerm(kName, "A"));
    irs::Optimize(optimized);
    CheckQuery(*optimized, all_but_first, rdr);
  }

  {
    auto naive = MakeAnd(MakeTerm(kDuplicated, "abcd"),
                         MakeExclude(nullptr, MakeTerm(kName, "A")));
    CheckQuery(*naive, Docs{5, 11, 21, 27, 31}, rdr);
    irs::Filter::ptr optimized =
      MakeAnd(MakeTerm(kDuplicated, "abcd"), MakeNot(MakeTerm(kName, "A")));
    irs::Optimize(optimized);
    CheckQuery(*optimized, Docs{5, 11, 21, 27, 31}, rdr);
  }

  {
    auto naive = MakeOr(MakeTerm(kDuplicated, "abcd"),
                        MakeExclude(nullptr, MakeTerm(kName, "A")));
    CheckQuery(*naive, all_docs, rdr);
    irs::Filter::ptr optimized =
      MakeOr(MakeTerm(kDuplicated, "abcd"), MakeNot(MakeTerm(kName, "A")));
    irs::Optimize(optimized);
    CheckQuery(*optimized, all_docs, rdr);
  }

  {
    auto naive =
      MakeExclude(nullptr, MakeExclude(nullptr, MakeTerm(kName, "A")));
    CheckQuery(*naive, Docs{1}, rdr);
    irs::Filter::ptr optimized = MakeNot(MakeNot(MakeTerm(kName, "A")));
    irs::Optimize(optimized);
    CheckQuery(*optimized, Docs{1}, rdr);
  }

  {
    auto naive = MakeOr(MakeTerm(kName, "V"), MakeExclude(nullptr, MakeAll()));
    CheckQuery(*naive, Docs{22}, rdr);
    irs::Filter::ptr optimized =
      MakeOr(MakeTerm(kName, "V"), MakeNot(MakeAll()));
    irs::Optimize(optimized);
    CheckQuery(*optimized, Docs{22}, rdr);
  }

  {
    auto naive = MakeAnd(MakeTerm(kName, "V"), MakeExclude(nullptr, MakeAll()));
    CheckQuery(*naive, Docs{}, rdr);
    irs::Filter::ptr optimized =
      MakeAnd(MakeTerm(kName, "V"), MakeNot(MakeAll()));
    irs::Optimize(optimized);
    CheckQuery(*optimized, Docs{}, rdr);
  }

  {
    auto naive =
      MakeAnd(MakeAnd(MakeTerm(kDuplicated, "abcd"), MakeTerm(kSame, "xyz")),
              MakeExclude(nullptr, MakeTerm(kName, "A")));
    CheckQuery(*naive, Docs{5, 11, 21, 27, 31}, rdr);
    irs::Filter::ptr optimized =
      MakeAnd(MakeAnd(MakeTerm(kDuplicated, "abcd"), MakeTerm(kSame, "xyz")),
              MakeNot(MakeTerm(kName, "A")));
    irs::Optimize(optimized);
    ASSERT_EQ(irs::Type<irs::Exclusion>::id(), optimized->type());
    auto& ex = sdb::basics::downCast<irs::Exclusion>(*optimized);
    ASSERT_EQ(irs::Type<irs::And>::id(), ex.include()->type());
    ASSERT_EQ(2, sdb::basics::downCast<irs::And>(*ex.include()).size());
    CheckQuery(*optimized, Docs{5, 11, 21, 27, 31}, rdr);
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(filter_optimizer_test, FilterOptimizerTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd")),
                         FilterOptimizerTestCase::to_string);

}  // namespace tests
