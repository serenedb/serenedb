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

#include <optional>
#include <variant>

#include "filter_test_case_base.hpp"
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/automaton_filter.hpp"
#include "iresearch/search/bm25.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/granular_range_filter.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/mixed_boolean_filter.hpp"
#include "iresearch/search/ngram_similarity_filter.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/range_filter.hpp"
#include "iresearch/search/regexp_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/terms_filter.hpp"
#include "iresearch/search/wildcard_filter.hpp"
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
                                      std::string_view term,
                                      irs::score_t boost = irs::kNoBoost) {
  auto f = std::make_unique<irs::ByTerm>();
  *f->mutable_field_id() = field;
  f->mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  f->boost(boost);
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

std::unique_ptr<irs::ByWildcard> MakeWildcard(irs::field_id field,
                                              std::string_view term) {
  auto f = std::make_unique<irs::ByWildcard>();
  *f->mutable_field_id() = field;
  f->mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  return f;
}

std::unique_ptr<irs::ByRegexp> MakeRegexp(irs::field_id field,
                                          std::string_view pattern) {
  auto f = std::make_unique<irs::ByRegexp>();
  *f->mutable_field_id() = field;
  f->mutable_options()->pattern = irs::ViewCast<irs::byte_type>(pattern);
  return f;
}

std::unique_ptr<irs::ByEditDistance> MakeEditDistance(
  irs::field_id field, std::string_view term, irs::byte_type max_distance,
  std::string_view prefix = "", size_t max_terms = 0) {
  auto f = std::make_unique<irs::ByEditDistance>();
  *f->mutable_field_id() = field;
  f->mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  f->mutable_options()->prefix = irs::ViewCast<irs::byte_type>(prefix);
  f->mutable_options()->max_distance = max_distance;
  f->mutable_options()->max_terms = max_terms;
  return f;
}

std::unique_ptr<irs::ByPhrase> MakePhraseWildcard(irs::field_id field,
                                                  std::string_view term) {
  auto f = std::make_unique<irs::ByPhrase>();
  *f->mutable_field_id() = field;
  auto& part = f->mutable_options()->push_back<irs::ByWildcardOptions>();
  part.term = irs::ViewCast<irs::byte_type>(term);
  return f;
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

irs::TypeInfo::type_id TypeOf(const irs::Filter& f) { return f.type(); }

template<typename T>
const T& As(const irs::Filter& f) {
  return sdb::basics::downCast<T>(f);
}

}  // namespace
namespace tests {

// Fixture providing the optimizer helpers and a lazily-built index over
// simple_sequential.json so the score-relevant rules can be checked against
// real document scores.
class FilterOptimizerTest : public FilterTestCaseBase {
 protected:
  static constexpr irs::doc_id_t kDocCount = 32;

  static void Optimize(irs::Filter::ptr& filter,
                       const irs::Scorer* scorer = nullptr) {
    irs::Optimize(filter, {.scorer = scorer});
  }

  const irs::IndexReader& Reader() {
    if (!_reader) {
      tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                  &tests::GenericJsonFieldFactory);
      add_segment(gen);
      _reader = open_reader();
    }
    return *_reader;
  }

  // Executes `filter` with the boost-as-score scorer (tests::sort::Boost) and
  // asserts the matched documents and their scores.
  void CheckBoostScores(const irs::Filter& filter, const ScoredDocs& expected) {
    std::array<irs::Scorer::ptr, 1> order{std::make_unique<sort::Boost>()};
    CheckQuery(filter, order, expected, Reader());
  }

  // Builds an expectation where every document scores `score`.
  static ScoredDocs AllScored(irs::score_t score,
                              irs::doc_id_t count = kDocCount) {
    ScoredDocs expected;
    expected.reserve(count);
    for (irs::doc_id_t doc = 1; doc <= count; ++doc) {
      expected.emplace_back(doc, std::vector<irs::score_t>{score});
    }
    return expected;
  }

 private:
  std::optional<irs::DirectoryReader> _reader;
};

// ExclusionRule: an exclusion with no exclude side collapses to its include
// (or all-docs), but must keep its boost when scoring.
TEST_P(FilterOptimizerTest, ExclusionRule) {
  // empty exclusion -> all-docs
  {
    irs::Filter::ptr root = std::make_unique<irs::Exclusion>();
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::All>::id(), TypeOf(*root));
  }

  // include-only, no boost -> the include itself
  {
    auto ex = std::make_unique<irs::Exclusion>();
    ex->include(MakeTerm(kName, "A"));
    irs::Filter::ptr root = std::move(ex);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), TypeOf(*root));
  }

  // include-only, boosted, unscored -> still collapses (boost is irrelevant)
  {
    auto ex = std::make_unique<irs::Exclusion>();
    ex->include(MakeTerm(kName, "A"));
    ex->boost(2.F);
    irs::Filter::ptr root = std::move(ex);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), TypeOf(*root));
  }

  // include-only, boosted, scored -> folds into the include, boost preserved.
  {
    auto ex = std::make_unique<irs::Exclusion>();
    ex->include(MakeTerm(kName, "A"));
    ex->boost(2.F);
    irs::Filter::ptr root = std::move(ex);

    const sort::Boost scorer;
    Optimize(root, &scorer);

    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), TypeOf(*root));
    ASSERT_EQ(2.F, As<irs::ByTerm>(*root).Boost());
    // doc "A" is document 1; its score must carry the exclusion boost of 2.
    CheckBoostScores(*root, ScoredDocs{{1, {2.F}}});
  }

  {
    auto ex = std::make_unique<irs::Exclusion>();
    ex->boost(2.F);
    irs::Filter::ptr root = std::move(ex);

    const sort::Boost scorer;
    Optimize(root, &scorer);

    ASSERT_EQ(irs::Type<irs::All>::id(), TypeOf(*root));
    ASSERT_EQ(2.F, As<irs::All>(*root).Boost());
    CheckBoostScores(*root, AllScored(2.F));
  }
}

// NotSimplifyRule: collapses double negations and folds Not(all)/Not(empty).
TEST_P(FilterOptimizerTest, NotSimplifyRule) {
  // Not(all) -> empty
  {
    irs::Filter::ptr root = MakeNot(MakeAll());
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Empty>::id(), TypeOf(*root));
  }

  // Not(Not(term)) -> term, preserving pointer identity
  {
    auto term = MakeTerm(kName, "A");
    const auto* term_ptr = term.get();
    irs::Filter::ptr root = MakeNot(MakeNot(std::move(term)));
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), TypeOf(*root));
    ASSERT_EQ(term_ptr, root.get());
  }

  // odd negation count keeps a single negation (lowered to exclusion)
  {
    irs::Filter::ptr root = MakeNot(MakeNot(MakeNot(MakeTerm(kName, "A"))));
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Exclusion>::id(), TypeOf(*root));
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(),
              As<irs::Exclusion>(*root).exclude()->type());
  }

  // deep, even chain fully cancels
  {
    irs::Filter::ptr root = MakeTerm(kName, "A");
    for (size_t i = 0; i < 50; ++i) {
      root = MakeNot(std::move(root));
    }
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), TypeOf(*root));
  }
}

// NotLowerRule: any surviving Not becomes an exclusion with an empty include.
TEST_P(FilterOptimizerTest, NotLowerRule) {
  irs::Filter::ptr root = MakeNot(MakeTerm(kName, "A"));
  Optimize(root);

  ASSERT_EQ(irs::Type<irs::Exclusion>::id(), TypeOf(*root));
  auto& node = As<irs::Exclusion>(*root);
  ASSERT_EQ(nullptr, node.include());
  ASSERT_NE(nullptr, node.exclude());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), node.exclude()->type());

  // matches every document except "A" (document 1).
  Docs all_but_first(kDocCount - 1);
  std::iota(all_but_first.begin(), all_but_first.end(), 2);
  CheckQuery(*root, all_but_first, Reader());
}

// AndEmptyRule: a conjunction with an empty clause is unsatisfiable.
TEST_P(FilterOptimizerTest, AndEmptyRule) {
  irs::Filter::ptr root = MakeAnd(MakeTerm(kName, "A"), Make<irs::Empty>());
  Optimize(root);
  ASSERT_EQ(irs::Type<irs::Empty>::id(), TypeOf(*root));
}

// OrEmptyRule: empty clauses are dropped from a disjunction; an all-empty
// disjunction is empty -- unless min_match is 0, where it is all-docs (#1).
TEST_P(FilterOptimizerTest, OrEmptyRule) {
  // empties removed (distinct fields -> stays a disjunction)
  {
    irs::Filter::ptr root =
      MakeOr(MakeTerm(kF1, "A"), Make<irs::Empty>(), MakeTerm(kF2, "B"));
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Or>::id(), TypeOf(*root));
    ASSERT_EQ(2, As<irs::Or>(*root).size());
  }

  // all empties (default min_match=1) -> empty
  {
    irs::Filter::ptr root = MakeOr(Make<irs::Empty>(), Make<irs::Empty>());
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Empty>::id(), TypeOf(*root));
  }

  // all empties with min_match=0 -> all-docs, not empty (#1 regression)
  {
    irs::Filter::ptr root =
      MakeOrV(Filters(Make<irs::Empty>(), Make<irs::Empty>()),
              irs::ScoreMergeType::Sum, 0);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::All>::id(), TypeOf(*root));
  }
}

// OrMinMatchZeroRule: min_match==0 matches everything, keeping the boost.
TEST_P(FilterOptimizerTest, OrMinMatchZeroRule) {
  // with children
  {
    auto or_node =
      MakeOrV(Filters(MakeTerm(kName, "A")), irs::ScoreMergeType::Sum, 0);
    or_node->boost(2.F);
    irs::Filter::ptr root = std::move(or_node);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::All>::id(), TypeOf(*root));
    ASSERT_EQ(2.F, As<irs::All>(*root).Boost());
    CheckBoostScores(*root, AllScored(2.F));
  }

  // no children
  {
    irs::Filter::ptr root = MakeOrV(Filters(), irs::ScoreMergeType::Sum, 0);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::All>::id(), TypeOf(*root));
  }
}

// OrUnsatRule: an unsatisfiable disjunction (min_match > clauses) is empty.
TEST_P(FilterOptimizerTest, OrUnsatRule) {
  // min_match greater than clause count
  {
    irs::Filter::ptr root =
      MakeOrV(Filters(MakeTerm(kName, "A")), irs::ScoreMergeType::Sum, 3);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Empty>::id(), TypeOf(*root));
  }

  // no clauses, default min_match=1
  {
    irs::Filter::ptr root = MakeOr();
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Empty>::id(), TypeOf(*root));
  }
}

// AndAllFoldRule: all-docs clauses are redundant in a conjunction; dropped when
// unscored, merged into a single boosted all-docs when scored.
TEST_P(FilterOptimizerTest, AndAllFoldRule) {
  // unscored -> all-docs clauses removed entirely
  {
    irs::Filter::ptr root =
      MakeAnd(MakeTerm(kName, "A"), MakeAll(2.F), MakeAll(3.F));
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), TypeOf(*root));
  }

  // scored -> all-docs merged (2 + 3) into one, term kept
  {
    irs::Filter::ptr root =
      MakeAnd(MakeTerm(kName, "A"), MakeAll(2.F), MakeAll(3.F));
    const sort::Boost scorer;
    Optimize(root, &scorer);

    ASSERT_EQ(irs::Type<irs::And>::id(), TypeOf(*root));
    auto& and_root = As<irs::And>(*root);
    ASSERT_EQ(2, and_root.size());
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[0].type());
    ASSERT_EQ(irs::Type<irs::All>::id(), and_root[1].type());
    ASSERT_EQ(5.F, As<irs::All>(and_root[1]).Boost());
    // doc "A" (1) scores term(1) + merged all-docs(5) == 6.
    CheckBoostScores(*root, ScoredDocs{{1, {6.F}}});
  }

  // scored, only all-docs clauses -> single all-docs with summed boost
  {
    irs::Filter::ptr root = MakeAnd(MakeAll(2.F), MakeAll(3.F));
    const sort::Boost scorer;
    Optimize(root, &scorer);
    ASSERT_EQ(irs::Type<irs::All>::id(), TypeOf(*root));
    ASSERT_EQ(5.F, As<irs::All>(*root).Boost());
    CheckBoostScores(*root, AllScored(5.F));
  }

  // scored, single all-docs clause -> nothing to merge, left as-is
  {
    irs::Filter::ptr root = MakeAnd(MakeTerm(kName, "A"), MakeAll(2.F));
    const sort::Boost scorer;
    Optimize(root, &scorer);
    ASSERT_EQ(irs::Type<irs::And>::id(), TypeOf(*root));
    ASSERT_EQ(2, As<irs::And>(*root).size());
  }
}

// OrAllFoldRule: an all-docs clause satisfies a disjunction when unscored;
// when scored the all-docs clauses merge and min_match is reduced.
TEST_P(FilterOptimizerTest, OrAllFoldRule) {
  // unscored -> any all-docs makes the whole disjunction all-docs
  {
    irs::Filter::ptr root = MakeOr(MakeTerm(kName, "A"), MakeAll());
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::All>::id(), TypeOf(*root));
  }

  // scored -> merge the two all-docs and drop min_match by (count - 1)
  {
    irs::Filter::ptr root =
      MakeOrV(Filters(MakeTerm(kF1, "A"), MakeTerm(kF2, "B"), MakeAll(2.F),
                      MakeAll(3.F)),
              irs::ScoreMergeType::Sum, 3);
    const sort::Boost scorer;
    Optimize(root, &scorer);

    ASSERT_EQ(irs::Type<irs::Or>::id(), TypeOf(*root));
    auto& or_root = As<irs::Or>(*root);
    ASSERT_EQ(3, or_root.size());
    ASSERT_EQ(2, or_root.min_match_count());
    ASSERT_EQ(irs::Type<irs::All>::id(), or_root[2].type());
    ASSERT_EQ(5.F, As<irs::All>(or_root[2]).Boost());
  }

  // scored, single all-docs -> nothing to merge
  {
    irs::Filter::ptr root = MakeOr(MakeTerm(kName, "A"), MakeAll(2.F));
    const sort::Boost scorer;
    Optimize(root, &scorer);
    ASSERT_EQ(irs::Type<irs::Or>::id(), TypeOf(*root));
    ASSERT_EQ(2, As<irs::Or>(*root).size());
  }
}

// FlattenAnd: nested conjunctions are spliced into the parent, gated by boost,
// merge_type and non-emptiness.
TEST_P(FilterOptimizerTest, FlattenAnd) {
  // simple splice
  {
    irs::Filter::ptr root = MakeAnd(
      MakeAnd(MakeTerm(kF1, "A"), MakeTerm(kF2, "B")), MakeTerm(kF3, "C"));
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::And>::id(), TypeOf(*root));
    ASSERT_EQ(3, As<irs::And>(*root).size());
  }

  // recursively flattens
  {
    irs::Filter::ptr root =
      MakeAnd(MakeAnd(MakeAnd(MakeTerm(kF1, "A")), MakeTerm(kF2, "B")),
              MakeTerm(kF3, "C"));
    Optimize(root);
    ASSERT_EQ(3, As<irs::And>(*root).size());
  }

  // boosted inner is NOT spliced
  {
    auto inner = MakeAnd(MakeTerm(kF1, "A"), MakeTerm(kF2, "B"));
    inner->boost(2.F);
    irs::Filter::ptr root = MakeAnd(std::move(inner), MakeTerm(kF3, "C"));
    Optimize(root);
    auto& and_root = As<irs::And>(*root);
    ASSERT_EQ(2, and_root.size());
    ASSERT_EQ(irs::Type<irs::And>::id(), and_root[0].type());
  }

  // differing merge_type is NOT spliced
  {
    auto inner = MakeAndV(Filters(MakeTerm(kF1, "A"), MakeTerm(kF2, "B")),
                          irs::ScoreMergeType::Max);
    irs::Filter::ptr root = MakeAnd(std::move(inner), MakeTerm(kF3, "C"));
    Optimize(root);
    auto& and_root = As<irs::And>(*root);
    ASSERT_EQ(2, and_root.size());
    ASSERT_EQ(irs::Type<irs::And>::id(), and_root[0].type());
  }

  // empty inner conjunction folds to empty (EmptyAndRule), collapsing the
  // enclosing conjunction via AndEmptyRule
  {
    irs::Filter::ptr root = MakeAnd(MakeAnd(), MakeTerm(kName, "C"));
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Empty>::id(), TypeOf(*root));
  }
}

// FlattenOr: nested plain disjunctions are spliced, gated by min_match on both
// the parent and the inner node.
TEST_P(FilterOptimizerTest, FlattenOr) {
  // simple splice
  {
    irs::Filter::ptr root = MakeOr(
      MakeOr(MakeTerm(kF1, "A"), MakeTerm(kF2, "B")), MakeTerm(kF3, "C"));
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Or>::id(), TypeOf(*root));
    ASSERT_EQ(3, As<irs::Or>(*root).size());
  }

  // parent min_match > 1 blocks the splice
  {
    irs::Filter::ptr root =
      MakeOrV(Filters(MakeOr(MakeTerm(kF1, "A"), MakeTerm(kF2, "B")),
                      MakeTerm(kF3, "C"), MakeTerm(kF4, "D")),
              irs::ScoreMergeType::Sum, 2);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Or>::id(), TypeOf(*root));
    ASSERT_EQ(3, As<irs::Or>(*root).size());
  }

  // inner min_match > 1 blocks the splice
  {
    auto inner = MakeOrV(Filters(MakeTerm(kName, "A"), MakeTerm(kName, "B")),
                         irs::ScoreMergeType::Sum, 2);
    irs::Filter::ptr root = MakeOr(std::move(inner), MakeTerm(kName, "C"));
    Optimize(root);
    ASSERT_EQ(2, As<irs::Or>(*root).size());
  }
}

// AndExclusionCoalesceRule: And mixing positive and negated clauses becomes an
// exclusion (or a bare negation when there are no positive clauses).
TEST_P(FilterOptimizerTest, AndExclusionCoalesceRule) {
  // one positive + one negative -> include/exclude
  {
    irs::Filter::ptr root =
      MakeAnd(MakeTerm(kName, "A"), MakeNot(MakeTerm(kName, "B")));
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Exclusion>::id(), TypeOf(*root));
    auto& node = As<irs::Exclusion>(*root);
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), node.include()->type());
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), node.exclude()->type());
  }

  // multiple of each -> And include, Or exclude
  {
    irs::Filter::ptr root =
      MakeAnd(MakeTerm(kName, "A"), MakeTerm(kName, "B"),
              MakeNot(MakeTerm(kName, "X")), MakeNot(MakeTerm(kName, "Y")));
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Exclusion>::id(), TypeOf(*root));
    auto& node = As<irs::Exclusion>(*root);
    ASSERT_EQ(irs::Type<irs::And>::id(), node.include()->type());
    ASSERT_EQ(2, As<irs::And>(*node.include()).size());
    ASSERT_EQ(irs::Type<irs::Or>::id(), node.exclude()->type());
    ASSERT_EQ(2, As<irs::Or>(*node.exclude()).size());
  }

  // only negatives -> exclusion with empty include
  {
    irs::Filter::ptr root =
      MakeAnd(MakeNot(MakeTerm(kName, "X")), MakeNot(MakeTerm(kName, "Y")));
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Exclusion>::id(), TypeOf(*root));
    auto& node = As<irs::Exclusion>(*root);
    ASSERT_EQ(nullptr, node.include());
    ASSERT_EQ(irs::Type<irs::Or>::id(), node.exclude()->type());
    ASSERT_EQ(2, As<irs::Or>(*node.exclude()).size());
  }

  // two exclusion children -> And of includes, Or of excludes
  {
    irs::Filter::ptr root =
      MakeAnd(MakeExclude(MakeTerm(kName, "A"), MakeTerm(kName, "B")),
              MakeExclude(MakeTerm(kName, "C"), MakeTerm(kName, "D")));
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Exclusion>::id(), TypeOf(*root));
    auto& node = As<irs::Exclusion>(*root);
    ASSERT_EQ(irs::Type<irs::And>::id(), node.include()->type());
    ASSERT_EQ(2, As<irs::And>(*node.include()).size());
    ASSERT_EQ(irs::Type<irs::Or>::id(), node.exclude()->type());
    ASSERT_EQ(2, As<irs::Or>(*node.exclude()).size());
  }

  // exclusion child mixed with a Not -> excludes merge together
  {
    irs::Filter::ptr root =
      MakeAnd(MakeExclude(MakeTerm(kName, "A"), MakeTerm(kName, "B")),
              MakeNot(MakeTerm(kName, "X")));
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Exclusion>::id(), TypeOf(*root));
    auto& node = As<irs::Exclusion>(*root);
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), node.include()->type());
    ASSERT_EQ(irs::Type<irs::Or>::id(), node.exclude()->type());
    ASSERT_EQ(2, As<irs::Or>(*node.exclude()).size());
  }

  // a boosted exclusion child is left intact under a scorer (boost preserved)
  {
    auto boosted = std::make_unique<irs::Exclusion>();
    boosted->include(MakeTerm(kName, "A"));
    boosted->exclude(MakeTerm(kName, "B"));
    boosted->boost(2.F);
    irs::Filter::ptr root =
      MakeAnd(std::move(boosted), MakeNot(MakeTerm(kName, "X")));

    const sort::Boost scorer;
    Optimize(root, &scorer);

    ASSERT_EQ(irs::Type<irs::Exclusion>::id(), TypeOf(*root));
    auto& node = As<irs::Exclusion>(*root);
    ASSERT_EQ(irs::Type<irs::Exclusion>::id(), node.include()->type());
    ASSERT_EQ(2.F, As<irs::Exclusion>(*node.include()).Boost());
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), node.exclude()->type());
  }
}

// OrAllRequiredRule: a disjunction where every clause is required is a
// conjunction; merge_type and boost are preserved.
TEST_P(FilterOptimizerTest, OrAllRequiredRule) {
  // min_match == size -> And
  {
    auto or_node = MakeOrV(Filters(MakeTerm(kF1, "A"), MakeTerm(kF2, "B")),
                           irs::ScoreMergeType::Max, 2);
    or_node->boost(2.F);
    irs::Filter::ptr root = std::move(or_node);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::And>::id(), TypeOf(*root));
    auto& and_root = As<irs::And>(*root);
    ASSERT_EQ(irs::ScoreMergeType::Max, and_root.merge_type());
    ASSERT_EQ(2.F, and_root.Boost());
  }

  // becomes And then coalesces the negated clause into an exclusion
  {
    irs::Filter::ptr root =
      MakeOrV(Filters(MakeTerm(kF1, "A"), MakeNot(MakeTerm(kF2, "B"))),
              irs::ScoreMergeType::Sum, 2);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Exclusion>::id(), TypeOf(*root));
    auto& node = As<irs::Exclusion>(*root);
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), node.include()->type());
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), node.exclude()->type());
  }
}

// ByTermsRule: same-field term clauses collapse into a single ByTerms; the
// boost of duplicate terms is summed, not multiplied (#3).
TEST_P(FilterOptimizerTest, ByTermsRule) {
  // And -> min_match == term count, boost preserved
  {
    auto and_root = MakeAnd(MakeTerm(kName, "A"), MakeTerm(kName, "B"));
    and_root->boost(2.F);
    irs::Filter::ptr root = std::move(and_root);
    const irs::BM25 scorer;
    Optimize(root, &scorer);
    ASSERT_EQ(irs::Type<irs::ByTerms>::id(), TypeOf(*root));
    auto& by_terms = As<irs::ByTerms>(*root);
    ASSERT_EQ(kName, by_terms.field_id());
    ASSERT_EQ(2, by_terms.options().terms.size());
    ASSERT_EQ(2, by_terms.options().min_match);
    ASSERT_EQ(2.F, by_terms.Boost());
  }

  // Or -> min_match == 1
  {
    irs::Filter::ptr root = MakeOr(MakeTerm(kName, "A"), MakeTerm(kName, "B"));
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::ByTerms>::id(), TypeOf(*root));
    auto& by_terms = As<irs::ByTerms>(*root);
    ASSERT_EQ(2, by_terms.options().terms.size());
    ASSERT_EQ(1, by_terms.options().min_match);
  }

  // different fields are not merged
  {
    irs::Filter::ptr root =
      MakeAnd(MakeTerm(kName, "A"), MakeTerm(kOther, "B"));
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::And>::id(), TypeOf(*root));
    ASSERT_EQ(2, As<irs::And>(*root).size());
  }

  // duplicate term in a min_match>1 disjunction is not merged
  {
    irs::Filter::ptr root = MakeOrV(
      Filters(MakeTerm(kName, "A"), MakeTerm(kName, "A"), MakeTerm(kName, "B")),
      irs::ScoreMergeType::Sum, 2);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Or>::id(), TypeOf(*root));
    ASSERT_EQ(3, As<irs::Or>(*root).size());
  }

  // duplicate term boosts are summed: 2 + 3 == 5 (not multiplied to 6) (#3);
  // the single coalesced term then lowers to a ByTerm (ByTermsDegenerateRule)
  // carrying the summed boost
  {
    irs::Filter::ptr root =
      MakeOr(MakeTerm(kName, "A", 2.F), MakeTerm(kName, "A", 3.F));
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), TypeOf(*root));
    auto& by_term = As<irs::ByTerm>(*root);
    ASSERT_EQ(kName, by_term.field_id());
    ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view{"A"}),
              irs::bytes_view{by_term.options().term});
    ASSERT_EQ(5.F, by_term.Boost());
  }
}

// ByTermsMinMatchZeroRule: ByTerms with min_match==0 matches everything;
// unscored folds to all-docs, scored expands to an all-docs OR terms.
TEST_P(FilterOptimizerTest, ByTermsMinMatchZeroRule) {
  // unscored -> all-docs
  {
    irs::Filter::ptr root = MakeByTerms(kName, {"A", "B"}, 0);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::All>::id(), TypeOf(*root));
  }

  // scored -> Or(all-docs[0], ByTerms[min_match=1])
  {
    auto by_terms = MakeByTerms(kName, {"A", "B"}, 0);
    by_terms->boost(2.F);
    irs::Filter::ptr root = std::move(by_terms);
    const irs::BM25 scorer;
    Optimize(root, &scorer);

    ASSERT_EQ(irs::Type<irs::Or>::id(), TypeOf(*root));
    auto& or_root = As<irs::Or>(*root);
    ASSERT_EQ(2, or_root.size());
    ASSERT_EQ(irs::Type<irs::All>::id(), or_root[0].type());
    ASSERT_EQ(0.F, As<irs::All>(or_root[0]).Boost());
    ASSERT_EQ(irs::Type<irs::ByTerms>::id(), or_root[1].type());
    auto& terms = As<irs::ByTerms>(or_root[1]);
    ASSERT_EQ(1, terms.options().min_match);
    ASSERT_EQ(2.F, terms.Boost());
  }

  // empty term set folds to empty (ByTermsDegenerateRule): no terms is
  // unsatisfiable, matching ByTerms::Prepare's empty short-circuit
  {
    irs::Filter::ptr root = MakeByTerms(kName, {}, 0);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Empty>::id(), TypeOf(*root));
  }
}

// SingleChildRule: a one-clause And/Or is replaced by its child, gated by
// boost when scoring and by min_match for disjunctions.
TEST_P(FilterOptimizerTest, SingleChildRule) {
  // And with one child -> the child (pointer preserved)
  {
    auto term = MakeTerm(kName, "A");
    const auto* term_ptr = term.get();
    irs::Filter::ptr root = MakeAnd(std::move(term));
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), TypeOf(*root));
    ASSERT_EQ(term_ptr, root.get());
  }

  // Or with min_match != 1 is not unwrapped (here it is unsatisfiable)
  {
    irs::Filter::ptr root =
      MakeOrV(Filters(MakeTerm(kName, "A")), irs::ScoreMergeType::Sum, 2);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Empty>::id(), TypeOf(*root));
  }

  // boost folds into the child whether scoring or not
  {
    const auto make = []() -> irs::Filter::ptr {
      auto root = MakeAnd(MakeTerm(kName, "A"));
      root->boost(2.F);
      return root;
    };

    auto scored = make();
    const irs::BM25 scorer;
    Optimize(scored, &scorer);
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), TypeOf(*scored));
    ASSERT_EQ(2.F, As<irs::ByTerm>(*scored).Boost());

    auto unscored = make();
    Optimize(unscored);
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), TypeOf(*unscored));
  }

  {
    auto root = MakeAnd(MakeTerm(kName, "A"));
    root->boost(3.F);
    irs::Filter::ptr filter = std::move(root);

    const sort::Boost scorer;
    Optimize(filter, &scorer);
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), TypeOf(*filter));
    CheckBoostScores(*filter, ScoredDocs{{1, {3.F}}});
  }
}

// MixedDegenerateRule: a mixed boolean with one empty slot collapses to the
// other; recursion still optimizes both slots when both are populated.
TEST_P(FilterOptimizerTest, MixedDegenerateRule) {
  // empty required -> optional (Or)
  {
    irs::Filter::ptr root = std::make_unique<irs::MixedBooleanFilter>(
      Filters(), Filters(MakeTerm(kF1, "A"), MakeTerm(kF2, "B")));
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Or>::id(), TypeOf(*root));
    ASSERT_EQ(2, As<irs::Or>(*root).size());
  }

  // empty optional -> required (And)
  {
    irs::Filter::ptr root = std::make_unique<irs::MixedBooleanFilter>(
      Filters(MakeTerm(kF1, "A"), MakeTerm(kF2, "B")), Filters());
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::And>::id(), TypeOf(*root));
    ASSERT_EQ(2, As<irs::And>(*root).size());
  }

  // both slots populated -> kept, and each slot is optimized recursively
  {
    irs::Filter::ptr root = std::make_unique<irs::MixedBooleanFilter>(
      Filters(MakeNot(MakeTerm(kName, "A"))),
      Filters(MakeNot(MakeTerm(kName, "B"))));
    auto& mixed = sdb::basics::downCast<irs::MixedBooleanFilter>(*root);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::MixedBooleanFilter>::id(), TypeOf(*root));
    ASSERT_EQ(irs::Type<irs::Exclusion>::id(), mixed.RequiredSlot()->type());
    ASSERT_EQ(irs::Type<irs::Exclusion>::id(), mixed.OptionalSlot()->type());
  }
}

// WildcardLowerRule: a wildcard term lowers to a term, prefix or automaton.
TEST_P(FilterOptimizerTest, WildcardLowerRule) {
  // no wildcard -> exact term
  {
    irs::Filter::ptr root = MakeWildcard(kName, "foo");
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), TypeOf(*root));
  }

  // trailing % -> prefix
  {
    irs::Filter::ptr root = MakeWildcard(kName, "foo%");
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::ByPrefix>::id(), TypeOf(*root));
  }

  // single-character wildcard -> automaton
  {
    irs::Filter::ptr root = MakeWildcard(kName, "f_o");
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::AutomatonFilter>::id(), TypeOf(*root));
  }
}

// RegexpLowerRule: a regexp lowers to a term for a literal, automaton for a
// genuinely complex pattern.
TEST_P(FilterOptimizerTest, RegexpLowerRule) {
  // literal pattern -> exact term
  {
    irs::Filter::ptr root = MakeRegexp(kName, "foo");
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), TypeOf(*root));
  }

  // complex pattern -> automaton
  {
    irs::Filter::ptr root = MakeRegexp(kName, "f.o");
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::AutomatonFilter>::id(), TypeOf(*root));
  }
}

TEST_P(FilterOptimizerTest, EditDistanceLowerRule) {
  {
    irs::Filter::ptr root = MakeEditDistance(kName, "bar", 0, "foo");
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), TypeOf(*root));
    ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view{"foobar"}),
              As<irs::ByTerm>(*root).options().term);
  }

  {
    auto filter = MakeEditDistance(kName, "foo", 1);
    filter->boost(1.5F);
    irs::Filter::ptr root = std::move(filter);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::LevenshteinAutomatonFilter>::id(), TypeOf(*root));
    const auto& lowered = As<irs::LevenshteinAutomatonFilter>(*root);
    ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view{"foo"}),
              lowered.options().target);
    ASSERT_EQ(3, lowered.options().utf8_target_size);
    ASSERT_EQ(2, lowered.options().no_distance);
    ASSERT_EQ(1.5F, lowered.Boost());
  }

  {
    irs::Filter::ptr root = MakeEditDistance(kName, "foo", 5);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Empty>::id(), TypeOf(*root));
  }
}

// PhraseLowerRule: wildcard phrase parts are lowered in place; the phrase node
// itself is preserved.
TEST_P(FilterOptimizerTest, PhraseLowerRule) {
  // wildcard part with no wildcard -> term part
  {
    irs::Filter::ptr root = MakePhraseWildcard(kName, "foo");
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::ByPhrase>::id(), TypeOf(*root));
    const auto& opts = As<irs::ByPhrase>(*root).options();
    ASSERT_EQ(1, opts.size());
    ASSERT_TRUE(std::holds_alternative<irs::ByTermOptions>(opts.begin()->part));
  }

  // wildcard part with trailing % -> prefix part
  {
    irs::Filter::ptr root = MakePhraseWildcard(kName, "foo%");
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::ByPhrase>::id(), TypeOf(*root));
    const auto& opts = As<irs::ByPhrase>(*root).options();
    ASSERT_EQ(1, opts.size());
    ASSERT_TRUE(
      std::holds_alternative<irs::ByPrefixOptions>(opts.begin()->part));
  }
}

// EmptyAndRule: a childless conjunction is unsatisfiable and folds to empty,
// matching BooleanFilter::PrepareImpl's empty short-circuit.
TEST_P(FilterOptimizerTest, EmptyAndRule) {
  // childless And -> empty
  {
    irs::Filter::ptr root = MakeAnd();
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Empty>::id(), TypeOf(*root));
  }

  // nested childless And collapses the enclosing conjunction too
  {
    irs::Filter::ptr root = MakeAnd(MakeAnd(), MakeTerm(kName, "A"));
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Empty>::id(), TypeOf(*root));
  }
}

// ByTermsDegenerateRule: a single-term ByTerms lowers to a ByTerm; an empty or
// unsatisfiable term set folds to empty (mirrors ByTerms::Prepare).
TEST_P(FilterOptimizerTest, ByTermsDegenerateRule) {
  // single term -> ByTerm, folding the filter boost
  {
    auto by_terms = MakeByTerms(kName, {"A"}, 1);
    by_terms->boost(2.F);
    irs::Filter::ptr root = std::move(by_terms);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), TypeOf(*root));
    auto& by_term = As<irs::ByTerm>(*root);
    ASSERT_EQ(kName, by_term.field_id());
    ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view{"A"}),
              irs::bytes_view{by_term.options().term});
    ASSERT_EQ(2.F, by_term.Boost());
  }

  // min_match greater than term count -> empty
  {
    irs::Filter::ptr root = MakeByTerms(kName, {"A", "B"}, 3);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Empty>::id(), TypeOf(*root));
  }

  // multi-term, satisfiable -> left as ByTerms
  {
    irs::Filter::ptr root = MakeByTerms(kName, {"A", "B"}, 2);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::ByTerms>::id(), TypeOf(*root));
    ASSERT_EQ(2, As<irs::ByTerms>(*root).options().terms.size());
  }
}

// RangeDegenerateRule: a single-point range lowers to a ByTerm when both bounds
// are inclusive, otherwise it is unsatisfiable and folds to empty.
TEST_P(FilterOptimizerTest, RangeDegenerateRule) {
  const auto make_range = [](irs::BoundType min_type, irs::BoundType max_type) {
    auto range = std::make_unique<irs::ByRange>();
    *range->mutable_field_id() = kName;
    auto& rng = range->mutable_options()->range;
    rng.min = irs::ViewCast<irs::byte_type>(std::string_view{"A"});
    rng.max = irs::ViewCast<irs::byte_type>(std::string_view{"A"});
    rng.min_type = min_type;
    rng.max_type = max_type;
    return range;
  };

  // [A, A] both inclusive -> ByTerm
  {
    auto range =
      make_range(irs::BoundType::Inclusive, irs::BoundType::Inclusive);
    range->boost(2.F);
    irs::Filter::ptr root = std::move(range);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), TypeOf(*root));
    auto& by_term = As<irs::ByTerm>(*root);
    ASSERT_EQ(kName, by_term.field_id());
    ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view{"A"}),
              irs::bytes_view{by_term.options().term});
    ASSERT_EQ(2.F, by_term.Boost());
  }

  // [A, A) with an exclusive bound -> empty
  {
    irs::Filter::ptr root =
      make_range(irs::BoundType::Inclusive, irs::BoundType::Exclusive);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Empty>::id(), TypeOf(*root));
  }
}

// GranularRangeDegenerateRule: the granular analogue of RangeDegenerateRule,
// comparing the most precise boundary terms.
TEST_P(FilterOptimizerTest, GranularRangeDegenerateRule) {
  const auto make_range = [](irs::BoundType min_type, irs::BoundType max_type) {
    auto range = std::make_unique<irs::ByGranularRange>();
    *range->mutable_field_id() = kName;
    auto& rng = range->mutable_options()->range;
    rng.min.emplace_back(irs::ViewCast<irs::byte_type>(std::string_view{"A"}));
    rng.max.emplace_back(irs::ViewCast<irs::byte_type>(std::string_view{"A"}));
    rng.min_type = min_type;
    rng.max_type = max_type;
    return range;
  };

  // most precise terms equal, both inclusive -> ByTerm
  {
    irs::Filter::ptr root =
      make_range(irs::BoundType::Inclusive, irs::BoundType::Inclusive);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), TypeOf(*root));
    ASSERT_EQ(kName, As<irs::ByTerm>(*root).field_id());
  }

  // exclusive bound -> empty
  {
    irs::Filter::ptr root =
      make_range(irs::BoundType::Inclusive, irs::BoundType::Exclusive);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Empty>::id(), TypeOf(*root));
  }
}

// NGramSimilarityLowerRule: ngram similarity reduces to a plain ByTerms when
// unscored with min_match 1, and to a phrase when every ngram must match.
TEST_P(FilterOptimizerTest, NGramSimilarityLowerRule) {
  const auto make_ngram = [](float_t threshold) {
    auto ngram = std::make_unique<irs::ByNGramSimilarity>();
    *ngram->mutable_field_id() = kName;
    auto& opts = *ngram->mutable_options();
    opts.ngrams.emplace_back(
      irs::ViewCast<irs::byte_type>(std::string_view{"ab"}));
    opts.ngrams.emplace_back(
      irs::ViewCast<irs::byte_type>(std::string_view{"bc"}));
    opts.threshold = threshold;
    return ngram;
  };

  // empty ngrams -> empty
  {
    irs::Filter::ptr root = std::make_unique<irs::ByNGramSimilarity>();
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Empty>::id(), TypeOf(*root));
  }

  // unscored, low threshold (min_match == 1) -> ByTerms
  {
    irs::Filter::ptr root = make_ngram(0.1F);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::ByTerms>::id(), TypeOf(*root));
    auto& by_terms = As<irs::ByTerms>(*root);
    ASSERT_EQ(2, by_terms.options().terms.size());
    ASSERT_EQ(1, by_terms.options().min_match);
  }

  // threshold 1.0 (min_match == term count) -> phrase
  {
    irs::Filter::ptr root = make_ngram(1.F);
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::ByPhrase>::id(), TypeOf(*root));
    ASSERT_EQ(2, As<irs::ByPhrase>(*root).options().size());
  }
}

// ExclusionDoubleNegationRule: nested exclude-only exclusions collapse by
// negation parity (even -> inner, odd -> single exclusion).
TEST_P(FilterOptimizerTest, ExclusionDoubleNegationRule) {
  // even negations -> inner filter
  {
    irs::Filter::ptr root =
      MakeExclude(nullptr, MakeExclude(nullptr, MakeTerm(kName, "A")));
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), TypeOf(*root));
  }

  // odd negations -> a single exclude-only exclusion
  {
    irs::Filter::ptr root = MakeExclude(
      nullptr,
      MakeExclude(nullptr, MakeExclude(nullptr, MakeTerm(kName, "A"))));
    Optimize(root);
    ASSERT_EQ(irs::Type<irs::Exclusion>::id(), TypeOf(*root));
    auto& ex = As<irs::Exclusion>(*root);
    ASSERT_EQ(nullptr, ex.include());
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), ex.exclude()->type());
  }
}

// General optimizer properties that span multiple rules.
TEST_P(FilterOptimizerTest, General) {
  // optimization is idempotent
  {
    const auto make = []() -> irs::Filter::ptr {
      return MakeAnd(
        MakeAnd(MakeTerm(kName, "A"), MakeNot(MakeTerm(kName, "B"))),
        MakeOr(MakeOr(MakeTerm(kName, "C")),
               MakeNot(MakeNot(MakeTerm(kName, "D")))));
    };
    irs::Filter::ptr once = make();
    irs::Filter::ptr twice = make();
    Optimize(once);
    Optimize(twice);
    Optimize(twice);
    ASSERT_TRUE(*once == *twice);
  }

  // a leaf root keeps its identity
  {
    irs::Filter::ptr root = MakeTerm(kName, "A");
    const auto* raw = root.get();
    Optimize(root);
    ASSERT_EQ(raw, root.get());
  }

  // a custom rule subset (here without ExclusionRule) is honored
  {
    irs::Filter::ptr root =
      MakeAnd(MakeAnd(MakeTerm(kF1, "A"), MakeTerm(kF2, "B")),
              MakeNot(MakeTerm(kName, "C")));
    irs::Optimize(root, {}, irs::kDefaultRules.subspan(1));
    ASSERT_EQ(irs::Type<irs::Exclusion>::id(), TypeOf(*root));
    auto& node = As<irs::Exclusion>(*root);
    ASSERT_EQ(irs::Type<irs::And>::id(), node.include()->type());
    ASSERT_EQ(2, As<irs::And>(*node.include()).size());
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), node.exclude()->type());
  }
}

static constexpr auto kOptimizerDirs =
  tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(filter_optimizer_test, FilterOptimizerTest,
                         ::testing::Combine(::testing::ValuesIn(kOptimizerDirs),
                                            ::testing::Values("1_5simd")),
                         FilterOptimizerTest::to_string);

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
