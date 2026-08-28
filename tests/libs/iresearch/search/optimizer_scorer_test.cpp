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

#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/raw_tf.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/terms_filter.hpp"
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

irs::Filter::ptr Optimized(irs::Filter::ptr filter) {
  irs::Optimize(filter, {.scored = true});
  return filter;
}

TEST(optimizer_scorer_test, single_child_collapse_keeps_the_scorer) {
  irs::RawTF tf;

  auto root = std::make_unique<irs::Or>();
  root->add(Scored("fox", tf));

  auto f = Optimized(std::move(root));

  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), f->type());
  ASSERT_EQ(&tf, f->GetScorer());
}

TEST(optimizer_scorer_test, terms_fusion_declines_an_overridden_sibling) {
  irs::RawTF tf;

  auto root = std::make_unique<irs::Or>();
  root->add(Scored("fox", tf));
  root->add(Term("dog"));

  auto f = Optimized(std::move(root));

  ASSERT_EQ(irs::Type<irs::Or>::id(), f->type());
  ASSERT_EQ(2u, f->GetChildren().size());
}

TEST(optimizer_scorer_test, terms_fusion_groups_around_an_override) {
  irs::RawTF tf;

  auto root = std::make_unique<irs::Or>();
  root->add(Scored("fox", tf));
  root->add(Term("dog"));
  root->add(Term("cat"));
  root->add(Term("owl"));

  auto f = Optimized(std::move(root));

  ASSERT_EQ(irs::Type<irs::Or>::id(), f->type());
  auto children = f->GetChildren();
  ASSERT_EQ(2u, children.size());

  size_t overridden = 0;
  size_t fused = 0;
  for (auto& child : children) {
    if (child->GetScorer()) {
      ++overridden;
      ASSERT_EQ(&tf, child->GetScorer());
      ASSERT_EQ(irs::Type<irs::ByTerm>::id(), child->type());
    } else if (child->type() == irs::Type<irs::ByTerms>::id()) {
      ++fused;
      ASSERT_EQ(
        3u, sdb::basics::downCast<irs::ByTerms>(*child).options().terms.size());
    }
  }
  ASSERT_EQ(1u, overridden);
  ASSERT_EQ(1u, fused);
}

TEST(optimizer_scorer_test, terms_fusion_groups_per_scorer) {
  irs::RawTF tf;
  irs::Unscored unscored;

  auto root = std::make_unique<irs::Or>();
  root->add(Scored("fox", tf));
  root->add(Scored("dog", tf));
  root->add(Scored("cat", unscored));
  root->add(Scored("owl", unscored));

  auto f = Optimized(std::move(root));

  ASSERT_EQ(irs::Type<irs::Or>::id(), f->type());
  auto children = f->GetChildren();
  ASSERT_EQ(2u, children.size());

  for (auto& child : children) {
    ASSERT_EQ(irs::Type<irs::ByTerms>::id(), child->type());
    ASSERT_TRUE(child->GetScorer() == &tf || child->GetScorer() == &unscored);
    ASSERT_EQ(
      2u, sdb::basics::downCast<irs::ByTerms>(*child).options().terms.size());
  }
}

TEST(optimizer_scorer_test, terms_fusion_skips_partial_under_min_match) {
  irs::RawTF tf;

  auto root = std::make_unique<irs::Or>();
  root->add(Scored("fox", tf));
  root->add(Term("dog"));
  root->add(Term("cat"));
  root->add(Term("owl"));
  root->min_match_count(2);

  auto f = Optimized(std::move(root));

  ASSERT_EQ(irs::Type<irs::Or>::id(), f->type());
  ASSERT_EQ(4u, f->GetChildren().size());
}

TEST(optimizer_scorer_test, terms_fusion_unaffected_without_an_override) {
  auto root = std::make_unique<irs::Or>();
  root->add(Term("fox"));
  root->add(Term("dog"));

  auto f = Optimized(std::move(root));

  ASSERT_EQ(irs::Type<irs::ByTerms>::id(), f->type());
}

TEST(optimizer_scorer_test, terms_fusion_allowed_when_overrides_agree) {
  irs::RawTF tf;

  auto root = std::make_unique<irs::Or>();
  root->add(Scored("fox", tf));
  root->add(Scored("dog", tf));

  auto f = Optimized(std::move(root));

  ASSERT_EQ(irs::Type<irs::ByTerms>::id(), f->type());
  ASSERT_EQ(&tf, f->GetScorer());
  ASSERT_EQ(2u, sdb::basics::downCast<irs::ByTerms>(*f).options().terms.size());
}

TEST(optimizer_scorer_test, terms_fusion_declines_differing_overrides) {
  irs::RawTF tf;
  irs::Unscored unscored;

  auto root = std::make_unique<irs::Or>();
  root->add(Scored("fox", tf));
  root->add(Scored("dog", unscored));

  auto f = Optimized(std::move(root));

  ASSERT_EQ(irs::Type<irs::Or>::id(), f->type());
  ASSERT_EQ(2u, f->GetChildren().size());
}

// A rule that replaces the node an override sits on must carry the scorer over
// to the replacement, or the `::score` is silently lost.
TEST(optimizer_scorer_test, rules_replacing_a_node_carry_the_scorer_over) {
  irs::RawTF tf;

  auto root = std::make_unique<irs::Or>();
  root->add(Term("fox"));
  root->add(Term("dog"));
  root->SetScorer(&tf);

  auto f = Optimized(std::move(root));

  ASSERT_EQ(irs::Type<irs::ByTerms>::id(), f->type());
  ASSERT_EQ(&tf, f->GetScorer());
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

  auto root = std::make_unique<irs::Or>();
  root->add(Scored("fox", unscored));
  root->add(Scored("dog", unscored));
  root->SetScorer(&tf);

  auto f = Optimized(std::move(root));

  ASSERT_EQ(irs::Type<irs::ByTerms>::id(), f->type());
  ASSERT_EQ(&unscored, f->GetScorer());
}

}  // namespace
