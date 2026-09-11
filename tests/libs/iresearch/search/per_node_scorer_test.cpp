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

#include <duckdb/common/allocator.hpp>
#include <map>

#include "filter_test_case_base.hpp"
#include "index/index_tests.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/constant_score.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/ngram_similarity_filter.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/raw_boost.hpp"
#include "iresearch/search/raw_tf.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/unscored.hpp"
#include "iresearch/search/wildcard_filter.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests;

inline constexpr irs::field_id kBodyId = 1;

irs::Filter::ptr ScoredTerm(std::string_view term, const irs::Scorer& scorer) {
  auto inner = std::make_unique<irs::ByTerm>();
  *inner->mutable_field_id() = kBodyId;
  inner->mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  inner->SetScorer(&scorer);
  return inner;
}

irs::Filter::ptr Scored(irs::Filter::ptr inner, const irs::Scorer& scorer) {
  inner->SetScorer(&scorer);
  return inner;
}

std::unique_ptr<irs::ByPhrase> Phrase(
  std::initializer_list<std::string_view> terms) {
  auto phrase = std::make_unique<irs::ByPhrase>();
  *phrase->mutable_field_id() = kBodyId;
  for (auto term : terms) {
    phrase->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(term);
  }
  return phrase;
}

std::unique_ptr<irs::ByNGramSimilarity> NGram(
  std::initializer_list<std::string_view> ngrams, float_t threshold) {
  auto filter = std::make_unique<irs::ByNGramSimilarity>();
  *filter->mutable_field_id() = kBodyId;
  auto* opts = filter->mutable_options();
  for (auto ngram : ngrams) {
    opts->ngrams.emplace_back(irs::ViewCast<irs::byte_type>(ngram));
  }
  opts->threshold = threshold;
  opts->allow_phrase = false;
  return filter;
}

void AddTerm(irs::BooleanFilter& filter, std::string_view term) {
  filter.Add(
    irs::TermClause{
      .field = kBodyId,
      .term = irs::bstring{irs::ViewCast<irs::byte_type>(term)},
    },
    irs::Occur::Should);
}

class PerNodeScorerTest : public IndexTestBase {
 protected:
  void BuildFixture();

  std::map<irs::doc_id_t, irs::score_t> Score(const irs::Filter& filter,
                                              const irs::Scorer& root);
};

void PerNodeScorerTest::BuildFixture() {
  using TextField = tests::TextField<std::string>;

  auto make_body = [&](std::string value) {
    auto field =
      std::make_shared<TextField>("body", std::move(value),
                                  /*payload=*/false, irs::IndexFeatures::Norm);
    field->id = kBodyId;
    return field;
  };

  tests::Document doc1;
  doc1.insert(make_body("fox fox fox dog"), true, false);
  tests::Document doc2;
  doc2.insert(make_body("cat cat"), true, false);
  tests::Document doc3;
  doc3.insert(make_body("fox cat"), true, false);

  auto writer = open_writer(irs::kOmCreate, irs::IndexWriterOptions{});
  ASSERT_NE(nullptr, writer);
  ASSERT_TRUE(tests::Insert(*writer, doc1.indexed.begin(), doc1.indexed.end()));
  ASSERT_TRUE(tests::Insert(*writer, doc2.indexed.begin(), doc2.indexed.end()));
  ASSERT_TRUE(tests::Insert(*writer, doc3.indexed.begin(), doc3.indexed.end()));
  writer->RefreshCommit();
}

std::map<irs::doc_id_t, irs::score_t> PerNodeScorerTest::Score(
  const irs::Filter& filter, const irs::Scorer& root) {
  auto index = open_reader();
  EXPECT_EQ(1, index->size());

  MaxMemoryCounter counter;
  tests::PreparedFilter prepared{filter, *index, &root, counter};

  irs::ColumnArgsFetcher fetcher;
  auto docs = prepared.ExecuteScored(0, fetcher);
  auto score = docs->PrepareScore();

  std::map<irs::doc_id_t, irs::score_t> seen;
  while (!irs::doc_limits::eof(docs->Advance())) {
    docs->FetchScoreArgs(0);
    fetcher.Fetch(docs->Value());
    irs::score_t s{};
    score.Score(&s, 1);
    seen.emplace(docs->Value(), s);
  }
  return seen;
}

TEST_P(PerNodeScorerTest, branch_scorer_overrides_the_query_scorer) {
  BuildFixture();

  irs::RawTF raw_tf;
  irs::RawBoost raw_boost;

  {
    irs::BooleanFilter filter;
    AddTerm(filter, "fox");
    AddTerm(filter, "cat");
    filter.SetMinShouldMatch(1);

    const auto scores = Score(filter, raw_tf);
    ASSERT_EQ(3u, scores.size());
    auto it = scores.begin();
    ASSERT_FLOAT_EQ(3.f, (it++)->second);
    ASSERT_FLOAT_EQ(2.f, (it++)->second);
    ASSERT_FLOAT_EQ(2.f, (it++)->second);
  }

  {
    irs::BooleanFilter filter;
    AddTerm(filter, "fox");
    filter.Add(ScoredTerm("cat", raw_boost), irs::Occur::Should);
    filter.SetMinShouldMatch(1);

    const auto scores = Score(filter, raw_tf);
    ASSERT_EQ(3u, scores.size());
    auto it = scores.begin();
    ASSERT_FLOAT_EQ(3.f, (it++)->second);
    ASSERT_FLOAT_EQ(1.f, (it++)->second);
    ASSERT_FLOAT_EQ(2.f, (it++)->second);
  }
}

TEST_P(PerNodeScorerTest, an_unscored_node_keeps_the_callers_scorer_out) {
  irs::RawTF raw_tf;
  irs::RawBoost raw_boost;

  auto term = ScoredTerm("cat", raw_boost);
  ASSERT_EQ(&raw_boost, term->GetScorer());

  auto& allocator = duckdb::Allocator::DefaultAllocator();
  irs::StatsArena stats_arena{allocator};

  // The node's own scorer wins for its whole subtree.
  auto scored = term->MakeCollector(raw_tf, stats_arena, 1);
  ASSERT_NE(nullptr, scored);
  EXPECT_EQ(&raw_boost, scored->GetScorer());

  // `Unscored` on the node is the one spelling of "this subtree does not
  // score", so nothing is collected for it however the caller scores. A
  // caller that does not score itself never asks in the first place.
  auto none = ScoredTerm("cat", irs::Unscored::Instance());
  EXPECT_EQ(nullptr, none->MakeCollector(raw_tf, stats_arena, 1));
}

TEST_P(PerNodeScorerTest, override_on_an_excluded_branch_changes_nothing) {
  BuildFixture();

  irs::RawTF raw_tf;
  irs::RawBoost raw_boost;

  auto build = [&](bool with_override) {
    auto root = std::make_unique<irs::BooleanFilter>();
    root->Add(
      irs::TermClause{
        .field = kBodyId,
        .term =
          irs::bstring{irs::ViewCast<irs::byte_type>(std::string_view("fox"))},
      },
      irs::Occur::Must);
    if (with_override) {
      root->Add(ScoredTerm("cat", raw_boost), irs::Occur::MustNot);
    } else {
      root->Add(
        irs::TermClause{
          .field = kBodyId,
          .term = irs::bstring{irs::ViewCast<irs::byte_type>(
            std::string_view("cat"))},
        },
        irs::Occur::MustNot);
    }
    irs::Filter::ptr filter = std::move(root);
    irs::Optimize(filter, {.scored = true});
    return filter;
  };

  const auto plain = build(false);
  const auto overridden = build(true);
  const auto plain_scores = Score(*plain, raw_tf);
  const auto overridden_scores = Score(*overridden, raw_tf);

  ASSERT_FALSE(plain_scores.empty());
  ASSERT_EQ(plain_scores, overridden_scores);
}

TEST_P(PerNodeScorerTest, unscored_branch_contributes_no_score) {
  BuildFixture();

  irs::RawTF raw_tf;
  irs::Unscored unscored;

  irs::BooleanFilter filter;
  AddTerm(filter, "fox");
  filter.Add(ScoredTerm("cat", unscored), irs::Occur::Should);
  filter.SetMinShouldMatch(1);

  const auto scores = Score(filter, raw_tf);
  ASSERT_EQ(3u, scores.size());
  auto it = scores.begin();
  ASSERT_FLOAT_EQ(3.f, (it++)->second);
  ASSERT_FLOAT_EQ(0.f, (it++)->second);
  ASSERT_FLOAT_EQ(1.f, (it++)->second);
}

TEST_P(PerNodeScorerTest, phrase_branch_scorer_overrides_the_query_scorer) {
  BuildFixture();

  irs::RawTF raw_tf;
  irs::RawBoost raw_boost;

  {
    irs::BooleanFilter filter;
    AddTerm(filter, "cat");
    filter.Add(Phrase({"fox"}), irs::Occur::Should);
    filter.SetMinShouldMatch(1);

    const auto scores = Score(filter, raw_tf);
    ASSERT_EQ(3u, scores.size());
    auto it = scores.begin();
    ASSERT_FLOAT_EQ(3.f, (it++)->second);
    ASSERT_FLOAT_EQ(2.f, (it++)->second);
    ASSERT_FLOAT_EQ(2.f, (it++)->second);
  }

  {
    irs::BooleanFilter filter;
    AddTerm(filter, "cat");
    filter.Add(Scored(Phrase({"fox"}), raw_boost), irs::Occur::Should);
    filter.SetMinShouldMatch(1);

    const auto scores = Score(filter, raw_tf);
    ASSERT_EQ(3u, scores.size());
    auto it = scores.begin();
    ASSERT_FLOAT_EQ(1.f, (it++)->second);
    ASSERT_FLOAT_EQ(2.f, (it++)->second);
    ASSERT_FLOAT_EQ(2.f, (it++)->second);
  }
}

TEST_P(PerNodeScorerTest, unscored_phrase_branch_contributes_no_score) {
  BuildFixture();

  irs::RawTF raw_tf;
  irs::Unscored unscored;

  irs::BooleanFilter filter;
  AddTerm(filter, "cat");
  filter.Add(Scored(Phrase({"fox"}), unscored), irs::Occur::Should);
  filter.SetMinShouldMatch(1);

  const auto scores = Score(filter, raw_tf);
  ASSERT_EQ(3u, scores.size());
  auto it = scores.begin();
  ASSERT_FLOAT_EQ(0.f, (it++)->second);
  ASSERT_FLOAT_EQ(2.f, (it++)->second);
  ASSERT_FLOAT_EQ(1.f, (it++)->second);
}

TEST_P(PerNodeScorerTest, all_branch_honours_the_override) {
  BuildFixture();

  irs::RawTF raw_tf;
  irs::ConstantScore constant{42.f};

  irs::BooleanFilter baseline;
  AddTerm(baseline, "fox");
  baseline.Add(std::make_unique<irs::All>(), irs::Occur::Should);
  baseline.SetMinShouldMatch(1);
  const auto before = Score(baseline, raw_tf);
  ASSERT_EQ(3u, before.size());
  {
    auto it = before.begin();
    ASSERT_FLOAT_EQ(3.f, (it++)->second);
    ASSERT_FLOAT_EQ(0.f, (it++)->second);
    ASSERT_FLOAT_EQ(1.f, (it++)->second);
  }

  irs::BooleanFilter filter;
  AddTerm(filter, "fox");
  filter.Add(Scored(std::make_unique<irs::All>(), constant),
             irs::Occur::Should);
  filter.SetMinShouldMatch(1);
  const auto after = Score(filter, raw_tf);

  ASSERT_EQ(3u, after.size());
  auto it = after.begin();
  ASSERT_FLOAT_EQ(45.f, (it++)->second);
  ASSERT_FLOAT_EQ(42.f, (it++)->second);
  ASSERT_FLOAT_EQ(43.f, (it++)->second);
  ASSERT_NE(before, after);
}

TEST_P(PerNodeScorerTest, lowered_wildcard_branch_honours_the_override) {
  BuildFixture();

  irs::RawTF raw_tf;
  irs::Unscored unscored;

  // `fo%` lowers to a prefix before any segment is touched; the node's own
  // scorer has to survive the rewrite.
  const auto wildcard = [] {
    auto node = std::make_unique<irs::ByWildcard>();
    *node->mutable_field_id() = kBodyId;
    node->mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view{"fo%"});
    return node;
  };

  irs::BooleanFilter baseline;
  AddTerm(baseline, "cat");
  baseline.Add(wildcard(), irs::Occur::Should);
  baseline.SetMinShouldMatch(1);
  const auto before =
    Score(*tests::Optimized(std::move(baseline), &raw_tf), raw_tf);

  irs::BooleanFilter filter;
  AddTerm(filter, "cat");
  filter.Add(Scored(wildcard(), unscored), irs::Occur::Should);
  filter.SetMinShouldMatch(1);
  const auto after =
    Score(*tests::Optimized(std::move(filter), &raw_tf), raw_tf);

  ASSERT_EQ(before.size(), after.size());
  ASSERT_NE(before, after);
}

TEST_P(PerNodeScorerTest, ngram_branch_honours_the_override) {
  BuildFixture();

  irs::RawTF raw_tf;
  irs::Unscored unscored;

  irs::BooleanFilter baseline;
  AddTerm(baseline, "cat");
  baseline.Add(NGram({"fox", "dog"}, 0.5f), irs::Occur::Should);
  baseline.SetMinShouldMatch(1);
  const auto before = Score(baseline, raw_tf);
  ASSERT_EQ(3u, before.size());

  irs::BooleanFilter filter;
  AddTerm(filter, "cat");
  filter.Add(Scored(NGram({"fox", "dog"}, 0.5f), unscored), irs::Occur::Should);
  filter.SetMinShouldMatch(1);
  const auto after = Score(filter, raw_tf);

  ASSERT_EQ(3u, after.size());
  auto it = after.begin();
  ASSERT_FLOAT_EQ(0.f, (it++)->second);
  ASSERT_FLOAT_EQ(2.f, (it++)->second);
  ASSERT_FLOAT_EQ(1.f, (it++)->second);
  ASSERT_NE(before, after);
}

TEST_P(PerNodeScorerTest, override_on_a_group_reaches_a_phrase_leaf) {
  BuildFixture();

  irs::RawTF raw_tf;
  irs::Unscored unscored;

  auto group = std::make_unique<irs::BooleanFilter>();
  AddTerm(*group, "cat");
  group->Add(Phrase({"fox"}), irs::Occur::Should);
  group->SetMinShouldMatch(1);

  const auto overridden = Scored(std::move(group), unscored);

  const auto scores = Score(*overridden, raw_tf);
  ASSERT_EQ(3u, scores.size());
  for (const auto& [doc, score] : scores) {
    ASSERT_FLOAT_EQ(0.f, score) << "doc " << doc;
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(per_node_scorer_test, PerNodeScorerTest,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd")),
                         PerNodeScorerTest::to_string);

}  // namespace
