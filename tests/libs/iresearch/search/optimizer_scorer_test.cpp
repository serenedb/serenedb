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

#include "basics/down_cast.h"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/raw_tf.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/unscored.hpp"
#include "iresearch/search/wildcard_filter.hpp"
#include "tests_shared.hpp"

namespace {

inline constexpr irs::field_id kBodyId = 1;

std::unique_ptr<irs::ByTerm> Term(std::string_view term) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field_id() = kBodyId;
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  return filter;
}

irs::Filter::ptr Scored(std::string_view term, const irs::Scorer& scorer) {
  auto filter = Term(term);
  filter->SetScorer(&scorer);
  return filter;
}

std::span<const irs::TermClause> OptionalTerms(const irs::Filter& filter) {
  return sdb::basics::downCast<irs::BooleanFilter>(filter).Terms(
    irs::Occur::Should);
}

size_t CountScoredBy(std::span<const irs::TermClause> terms,
                     const irs::Scorer* scorer) {
  return static_cast<size_t>(std::count_if(
    terms.begin(), terms.end(), [scorer](const irs::TermClause& clause) {
      return clause.scorer == scorer;
    }));
}

irs::Filter::ptr Optimized(irs::Filter::ptr filter) {
  irs::Optimize(filter, {.scored = true});
  return filter;
}

TEST(optimizer_scorer_test, single_child_collapse_keeps_the_scorer) {
  irs::RawTF tf;

  auto root = std::make_unique<irs::BooleanFilter>();
  root->Add(Scored("fox", tf), irs::Occur::Should);
  root->SetMinShouldMatch(1);

  auto f = Optimized(std::move(root));

  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), f->type());
  ASSERT_EQ(&tf, f->GetScorer());
}

TEST(optimizer_scorer_test, term_clauses_keep_an_overridden_sibling_apart) {
  irs::RawTF tf;

  auto root = std::make_unique<irs::BooleanFilter>();
  root->Add(Scored("fox", tf), irs::Occur::Should);
  root->Add(Term("dog"), irs::Occur::Should);
  root->SetMinShouldMatch(1);

  auto f = Optimized(std::move(root));

  ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), f->type());
  const auto terms = OptionalTerms(*f);
  ASSERT_EQ(2u, terms.size());
  ASSERT_EQ(1u, CountScoredBy(terms, &tf));
  ASSERT_EQ(1u, CountScoredBy(terms, nullptr));
}

TEST(optimizer_scorer_test, term_clauses_group_around_an_override) {
  irs::RawTF tf;

  auto root = std::make_unique<irs::BooleanFilter>();
  root->Add(Scored("fox", tf), irs::Occur::Should);
  root->Add(Term("dog"), irs::Occur::Should);
  root->Add(Term("cat"), irs::Occur::Should);
  root->Add(Term("owl"), irs::Occur::Should);
  root->SetMinShouldMatch(1);

  auto f = Optimized(std::move(root));

  ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), f->type());
  const auto terms = OptionalTerms(*f);
  ASSERT_EQ(4u, terms.size());
  ASSERT_EQ(1u, CountScoredBy(terms, &tf));
  ASSERT_EQ(3u, CountScoredBy(terms, nullptr));
}

TEST(optimizer_scorer_test, term_clauses_group_per_scorer) {
  irs::RawTF tf;
  irs::Unscored unscored;

  auto root = std::make_unique<irs::BooleanFilter>();
  root->Add(Scored("fox", tf), irs::Occur::Should);
  root->Add(Scored("dog", tf), irs::Occur::Should);
  root->Add(Scored("cat", unscored), irs::Occur::Should);
  root->Add(Scored("owl", unscored), irs::Occur::Should);
  root->SetMinShouldMatch(1);

  auto f = Optimized(std::move(root));

  ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), f->type());
  const auto terms = OptionalTerms(*f);
  ASSERT_EQ(4u, terms.size());
  ASSERT_EQ(2u, CountScoredBy(terms, &tf));
  ASSERT_EQ(2u, CountScoredBy(terms, &unscored));
}

TEST(optimizer_scorer_test, term_clauses_survive_a_partial_min_match) {
  irs::RawTF tf;

  auto root = std::make_unique<irs::BooleanFilter>();
  root->Add(Scored("fox", tf), irs::Occur::Should);
  root->Add(Term("dog"), irs::Occur::Should);
  root->Add(Term("cat"), irs::Occur::Should);
  root->Add(Term("owl"), irs::Occur::Should);
  root->SetMinShouldMatch(2);

  auto f = Optimized(std::move(root));

  ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), f->type());
  const auto terms = OptionalTerms(*f);
  ASSERT_EQ(4u, terms.size());
  ASSERT_EQ(1u, CountScoredBy(terms, &tf));
}

TEST(optimizer_scorer_test, term_clauses_unaffected_without_an_override) {
  auto root = std::make_unique<irs::BooleanFilter>();
  root->Add(Term("fox"), irs::Occur::Should);
  root->Add(Term("dog"), irs::Occur::Should);
  root->SetMinShouldMatch(1);

  auto f = Optimized(std::move(root));

  ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), f->type());
  const auto terms = OptionalTerms(*f);
  ASSERT_EQ(2u, terms.size());
  ASSERT_EQ(2u, CountScoredBy(terms, nullptr));
}

TEST(optimizer_scorer_test, term_clauses_keep_agreeing_overrides) {
  irs::RawTF tf;

  auto root = std::make_unique<irs::BooleanFilter>();
  root->Add(Scored("fox", tf), irs::Occur::Should);
  root->Add(Scored("dog", tf), irs::Occur::Should);
  root->SetMinShouldMatch(1);

  auto f = Optimized(std::move(root));

  ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), f->type());
  const auto terms = OptionalTerms(*f);
  ASSERT_EQ(2u, terms.size());
  ASSERT_EQ(2u, CountScoredBy(terms, &tf));
}

TEST(optimizer_scorer_test, term_clauses_keep_differing_overrides) {
  irs::RawTF tf;
  irs::Unscored unscored;

  auto root = std::make_unique<irs::BooleanFilter>();
  root->Add(Scored("fox", tf), irs::Occur::Should);
  root->Add(Scored("dog", unscored), irs::Occur::Should);
  root->SetMinShouldMatch(1);

  auto f = Optimized(std::move(root));

  ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), f->type());
  const auto terms = OptionalTerms(*f);
  ASSERT_EQ(2u, terms.size());
  ASSERT_EQ(1u, CountScoredBy(terms, &tf));
  ASSERT_EQ(1u, CountScoredBy(terms, &unscored));
}

// A rule that replaces the node an override sits on must carry the scorer over
// to the replacement, or the `::score` is silently lost.
TEST(optimizer_scorer_test, rules_replacing_a_node_carry_the_scorer_over) {
  irs::RawTF tf;

  auto root = std::make_unique<irs::BooleanFilter>();
  root->Add(Term("fox"), irs::Occur::Should);
  root->Add(Term("dog"), irs::Occur::Should);
  root->SetMinShouldMatch(1);
  root->SetScorer(&tf);

  auto f = Optimized(std::move(root));

  ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), f->type());
  ASSERT_EQ(&tf, f->GetScorer());
  ASSERT_EQ(2u, OptionalTerms(*f).size());
}

// Lowering replaces the node outright -- a wildcard becomes an automaton, an
// edit distance becomes a multiterm -- and it runs in a pass of its own, after
// the rules. The scorer has to survive that too, or `::score` on any lowered
// predicate would silently fall back to the query's scorer.
TEST(optimizer_scorer_test, lowering_a_wildcard_keeps_the_scorer) {
  irs::RawTF tf;

  auto filter = std::make_unique<irs::ByWildcard>();
  *filter->mutable_field_id() = kBodyId;
  filter->mutable_options()->term =
    irs::ViewCast<irs::byte_type>(std::string_view{"fo%"});
  filter->SetScorer(&tf);

  auto f = Optimized(std::move(filter));

  ASSERT_NE(irs::Type<irs::ByWildcard>::id(), f->type());
  ASSERT_EQ(&tf, f->GetScorer());
}

TEST(optimizer_scorer_test, lowering_an_edit_distance_keeps_the_scorer) {
  irs::Unscored unscored;

  auto filter = std::make_unique<irs::ByEditDistance>();
  *filter->mutable_field_id() = kBodyId;
  auto* opts = filter->mutable_options();
  opts->term = irs::ViewCast<irs::byte_type>(std::string_view{"fox"});
  opts->max_distance = 1;
  filter->SetScorer(&unscored);

  auto f = Optimized(std::move(filter));

  ASSERT_NE(irs::Type<irs::ByEditDistance>::id(), f->type());
  ASSERT_EQ(&unscored, f->GetScorer());
}

// The replacement wins if it brought a scorer of its own -- inheriting would
// override a more specific choice.
TEST(optimizer_scorer_test, a_replacement_keeps_its_own_scorer) {
  irs::RawTF tf;
  irs::Unscored unscored;

  auto root = std::make_unique<irs::BooleanFilter>();
  root->Add(Scored("fox", unscored), irs::Occur::Should);
  root->SetMinShouldMatch(1);
  root->SetScorer(&tf);

  auto f = Optimized(std::move(root));

  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), f->type());
  ASSERT_EQ(&unscored, f->GetScorer());
}

}  // namespace
