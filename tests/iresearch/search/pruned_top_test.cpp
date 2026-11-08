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

#include <absl/container/flat_hash_map.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <duckdb/common/allocator.hpp>
#include <iresearch/search/detail/doc_collector.hpp>
#include <iresearch/search/detail/exclusion_of.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/phrase_filter.hpp>
#include <iresearch/search/filters/prefix_filter.hpp>
#include <iresearch/search/scorers/bm25.hpp>
#include <iresearch/search/scorers/constant_score.hpp>
#include <iresearch/search/scorers/indri_dirichlet.hpp>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "index/index_tests.hpp"
#include "tests_shared.hpp"

namespace {

using irs::doc_id_t;

inline constexpr irs::field_id kBodyId = 1;
inline constexpr doc_id_t kDocs = 150000;

uint64_t Mix(uint64_t x) noexcept {
  x ^= x >> 33;
  x *= 0xff51afd7ed558ccdULL;
  x ^= x >> 33;
  x *= 0xc4ceb3f99c2b4ad7ULL;
  x ^= x >> 33;
  return x;
}

bool Chance(uint64_t salt, doc_id_t i, uint64_t permille) noexcept {
  return Mix(salt * kDocs + i) % 1000 < permille;
}

uint32_t Freq(uint64_t salt, doc_id_t i) noexcept {
  return 1 + static_cast<uint32_t>(
               std::countr_zero(Mix(~salt * kDocs + i) | (uint64_t{1} << 11)));
}

uint32_t Padding(doc_id_t i) noexcept {
  return static_cast<uint32_t>(Mix(uint64_t{i} * 7919 + 13) % 48);
}

struct Spec {
  std::string_view term;
  uint64_t salt;
  bool (*has)(doc_id_t);
  uint32_t (*freq)(doc_id_t) = nullptr;
};

inline constexpr Spec kSpecs[]{
  {"run", 11, [](doc_id_t i) { return i >= 10000 && i < 30000; }},
  {"half", 12, [](doc_id_t i) { return Chance(1, i, 500); }},
  {"dense", 13, [](doc_id_t i) { return Chance(2, i, 300); }},
  {"mid", 14, [](doc_id_t i) { return Chance(3, i, 50); }},
  {"sparse", 15, [](doc_id_t i) { return Chance(4, i, 10); }},
  {"rare", 16, [](doc_id_t i) { return Chance(5, i, 1); }},
  {"single", 17, [](doc_id_t i) { return i == 77777; }},
  {"holes", 18,
   [](doc_id_t i) { return i >= 50000 && i < 60000 && i % 37 != 0; }},
  {"cluster", 19,
   [](doc_id_t i) {
     return i >= 100000 && i < 110000 ? Chance(6, i, 500) : Chance(6, i, 5);
   }},
  {"edge", 20, [](doc_id_t i) { return i == 5000 || i == 10000; }},
  {"common", 21, [](doc_id_t i) { return Chance(9, i, 900); }},
  {"burst", 22, [](doc_id_t i) { return Chance(10, i, 400); },
   [](doc_id_t i) -> uint32_t { return i >= 2000 && i < 3000 ? 8 : 1; }},
};

irs::TermClause Clause(std::string_view value) {
  return {.field = kBodyId,
          .term = irs::bstring{irs::ViewCast<irs::byte_type>(value)}};
}

bool AlmostEqual(irs::score_t lhs, irs::score_t rhs) {
  return testing::internal::FloatingPoint<irs::score_t>{lhs}.AlmostEquals(
    testing::internal::FloatingPoint<irs::score_t>{rhs});
}

bool HasExpansion(doc_id_t i) noexcept { return Chance(7, i, 300); }

std::string ExpansionTerm(doc_id_t i) {
  return absl::StrCat("pfx", Mix(uint64_t{i} * 31 + 7) % 200);
}

std::string TermsText(doc_id_t i) {
  std::string text = "doc";
  for (const auto& spec : kSpecs) {
    if (spec.has(i)) {
      for (auto n = spec.freq ? spec.freq(i) : Freq(spec.salt, i); n != 0;
           --n) {
        absl::StrAppend(&text, " ", spec.term);
      }
    }
  }
  for (auto n = Padding(i); n != 0; --n) {
    absl::StrAppend(&text, " pad");
  }
  if (HasExpansion(i)) {
    absl::StrAppend(&text, " ", ExpansionTerm(i));
  }
  return text;
}

std::string PhrasesText(doc_id_t i) {
  std::string text = "doc";
  const auto repeat = [&](uint64_t salt, uint64_t permille,
                          std::string_view words) {
    if (Chance(salt, i, permille)) {
      for (auto n = Freq(salt, i); n != 0; --n) {
        absl::StrAppend(&text, " ", words);
      }
    }
  };
  repeat(39, 30, "club");
  repeat(31, 2, "harbor town");
  repeat(32, 600, "town");
  repeat(33, 5, "medal of honor");
  repeat(34, 10, "medal");
  repeat(35, 20, "honor");
  repeat(36, 700, "of");
  repeat(40, 10, "brass brass brass");
  repeat(41, 2, "brass band");
  repeat(42, 500, "band");
  repeat(43, 1, "zephyr alpha beta");
  repeat(44, 8, "zephyr");
  repeat(45, 200, "alpha");
  repeat(46, 200, "beta");
  repeat(47, 10, "omega alpha");
  if (Chance(37, i, 3)) {
    absl::StrAppend(&text, " visit port", Mix(uint64_t{i} * 13 + 5) % 40);
  }
  if (Chance(38, i, 500)) {
    absl::StrAppend(&text, " port", Mix(uint64_t{i} * 17 + 3) % 40);
  }
  for (auto n = Padding(i); n != 0; --n) {
    absl::StrAppend(&text, " pad");
  }
  return text;
}

bool HasPhrase(std::string_view text, std::span<const std::string_view> parts) {
  const std::vector<std::string_view> tokens = absl::StrSplit(text, ' ');
  return std::search(tokens.begin(), tokens.end(), parts.begin(),
                     parts.end()) != tokens.end();
}

struct Shape {
  std::vector<std::string_view> must;
  std::vector<std::string_view> must_not;
  std::vector<std::vector<std::string_view>> any;
  std::vector<std::string_view> should;
  std::vector<std::string_view> must_not_prefixes;
  std::vector<std::string_view> should_prefixes;
};

irs::Filter::ptr PrefixOf(std::string_view prefix) {
  auto filter = std::make_unique<irs::ByPrefix>();
  *filter->mutable_field_id() = kBodyId;
  filter->mutable_options()->term =
    irs::bstring{irs::ViewCast<irs::byte_type>(prefix)};
  return filter;
}

irs::Filter::ptr PhraseOf(std::span<const std::string_view> parts) {
  auto filter = std::make_unique<irs::ByPhrase>();
  *filter->mutable_field_id() = kBodyId;
  for (const auto part : parts) {
    if (part.ends_with('*')) {
      filter->mutable_options()->push_back<irs::ByPrefixOptions>().term =
        irs::bstring{
          irs::ViewCast<irs::byte_type>(part.substr(0, part.size() - 1))};
    } else {
      filter->mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::bstring{irs::ViewCast<irs::byte_type>(part)};
    }
  }
  return filter;
}

class PrunedTopTest : public tests::IndexTestBase {
 protected:
  void Build(std::string (*text)(doc_id_t) = TermsText) {
    irs::IndexWriterOptions options;
    options.reader_options.scorer = &_scorer;
    auto writer = open_writer(tests::kOmCreate, std::move(options));
    auto body = std::make_shared<tests::TextField<std::string>>(
      "body", false, irs::IndexFeatures::Norm);
    body->id = kBodyId;
    tests::Document doc;
    doc.insert(body, true, false);
    for (doc_id_t i = 0; i != kDocs; ++i) {
      body->value(text(i));
      ASSERT_TRUE(Insert(*writer, doc));
    }
    writer->RefreshCommit();
    _reader = writer->GetSnapshot();
    ASSERT_EQ(1, _reader.size());
  }

  std::vector<irs::QueryBuilder::ptr> Prepare(const irs::Filter& filter) {
    irs::PreparedCollector collector{filter, *_ranking, _stats, 1};
    std::vector<irs::QueryBuilder::ptr> queries;
    for (const auto& segment : _reader) {
      queries.emplace_back(
        filter.PrepareSegment(segment, {.collector = collector.Get()}));
    }
    collector.Finish();
    return queries;
  }

  absl::flat_hash_map<doc_id_t, irs::score_t> Matches(
    const irs::Filter& filter) {
    absl::flat_hash_map<doc_id_t, irs::score_t> out;
    const auto queries = Prepare(filter);
    if (!queries[0] || irs::QueryBuilder::IsEmpty(*queries[0])) {
      return out;
    }
    irs::ColumnArgsFetcher fetcher;
    auto plan =
      queries[0]->PlanScored({.scorer = *_ranking, .fetcher = fetcher});
    constexpr uint32_t kBatch = irs::doc_limits::kMinCapacity;
    irs::SlackBuf<doc_id_t, kBatch, irs::doc_limits::kDocsSlack> docs;
    irs::SlackBuf<irs::score_t, kBatch, irs::doc_limits::kScoresSlack> scores;
    const auto end = irs::doc_limits::min() + kDocs;
    for (auto min = irs::doc_limits::min(); min < end; min += kBatch) {
      const auto n = plan->Run(min, std::min<doc_id_t>(min + kBatch, end),
                               docs.data(), scores.data());
      for (uint32_t j = 0; j != n; ++j) {
        out.emplace(docs.data()[j], scores.data()[j]);
      }
    }
    return out;
  }

  std::vector<irs::ScoreDoc> Top(const irs::Filter& filter, size_t k,
                                 doc_id_t span, bool prune, bool bounds,
                                 bool reuse, uint64_t& total) {
    const auto queries = Prepare(filter);
    std::vector<irs::ScoreDoc> hits(k);
    std::atomic<irs::score_t> threshold{
      std::numeric_limits<irs::score_t>::lowest()};
    irs::LoserScoreCollector collector{threshold, hits};
    collector.SetSegment(0);
    if (queries[0] && !irs::QueryBuilder::IsEmpty(*queries[0])) {
      const auto end = irs::doc_limits::min() + kDocs;
      const auto plan_top = [&](irs::ColumnArgsFetcher& fetcher) {
        return queries[0]->PlanTop({.scorer = *_ranking,
                                    .fetcher = fetcher,
                                    .prune = prune,
                                    .bounds = bounds,
                                    .k = static_cast<uint32_t>(k)});
      };
      irs::ColumnArgsFetcher shared;
      auto reused = reuse ? plan_top(shared) : nullptr;
      for (auto from = irs::doc_limits::min(); from < end; from += span) {
        irs::ColumnArgsFetcher fetcher;
        auto own = reuse ? nullptr : plan_top(fetcher);
        auto* const plan = reuse ? reused.get() : own.get();
        if (!plan) {
          break;
        }
        const auto to = std::min<uint64_t>(uint64_t{from} + span, end);
        plan->Run(from, static_cast<doc_id_t>(to), collector);
      }
    }
    collector.SortHits();
    total = collector.TotalMatches();
    hits.resize(std::min(k, collector.AcceptedCount()));
    return hits;
  }

  void Check(const Shape& shape) {
    SCOPED_TRACE(
      absl::StrCat("+", absl::StrJoin(shape.must, " +"), " -",
                   absl::StrJoin(shape.must_not, " -"), " any ",
                   shape.any.size(), " ", absl::StrJoin(shape.should, " "),
                   " -", absl::StrJoin(shape.must_not_prefixes, "* -"), "* ",
                   absl::StrJoin(shape.should_prefixes, "* ")));
    irs::BooleanFilter filter;
    for (const auto term : shape.must) {
      filter.Add(Clause(term), irs::Occur::Must);
    }
    for (const auto term : shape.must_not) {
      filter.Add(Clause(term), irs::Occur::MustNot);
    }
    for (const auto prefix : shape.must_not_prefixes) {
      filter.Add(PrefixOf(prefix), irs::Occur::MustNot);
    }
    for (const auto term : shape.should) {
      filter.Add(Clause(term), irs::Occur::Should);
    }
    for (const auto prefix : shape.should_prefixes) {
      filter.Add(PrefixOf(prefix), irs::Occur::Should);
    }
    if (shape.must.empty() && shape.any.empty()) {
      filter.SetMinShouldMatch(1);
    }
    for (const auto& terms : shape.any) {
      auto group = std::make_unique<irs::BooleanFilter>();
      for (const auto term : terms) {
        group->Add(Clause(term), irs::Occur::Should);
      }
      group->SetMinShouldMatch(1);
      filter.Add(std::move(group), irs::Occur::Must);
    }
    CheckRanking(filter);
  }

  struct Mixed {
    std::vector<std::vector<std::string_view>> phrases;
    std::vector<std::string_view> terms;
    std::vector<std::string_view> excluded;
  };

  void CheckMixed(const Mixed& shape, irs::Occur occur) {
    irs::BooleanFilter filter;
    std::vector<std::string> trace;
    for (const auto& parts : shape.phrases) {
      filter.Add(PhraseOf(parts), occur);
      trace.push_back(absl::StrCat("\"", absl::StrJoin(parts, " "), "\""));
    }
    for (const auto term : shape.terms) {
      filter.Add(Clause(term), occur);
      trace.emplace_back(term);
    }
    for (const auto term : shape.excluded) {
      filter.Add(Clause(term), irs::Occur::MustNot);
      trace.push_back(absl::StrCat("-", term));
    }
    if (occur == irs::Occur::Should) {
      filter.SetMinShouldMatch(1);
    }
    SCOPED_TRACE(absl::StrJoin(trace, " "));
    CheckRanking(filter);
  }

  void CheckRanking(const irs::Filter& filter) {
    const auto matches = Matches(filter);
    std::vector<irs::score_t> ranked;
    ranked.reserve(matches.size());
    for (const auto& [doc, score] : matches) {
      ranked.push_back(score);
    }
    absl::c_sort(ranked, std::greater<>{});
    for (const size_t k : {size_t{1}, size_t{10}, size_t{100}, size_t{1000}}) {
      for (const auto [span, prune, bounds, reuse] :
           {std::tuple{doc_id_t{kDocs}, true, true, false},
            std::tuple{doc_id_t{9973}, true, true, false},
            std::tuple{doc_id_t{9973}, true, true, true},
            std::tuple{doc_id_t{kDocs}, true, false, false},
            std::tuple{doc_id_t{9973}, true, false, true},
            std::tuple{doc_id_t{kDocs}, false, false, false},
            std::tuple{doc_id_t{9973}, false, false, false}}) {
        SCOPED_TRACE(absl::StrCat("k ", k, " span ", span, " prune ", prune,
                                  " bounds ", bounds, " reuse ", reuse));
        uint64_t total = 0;
        const auto hits = Top(filter, k, span, prune, bounds, reuse, total);
        if (!prune) {
          ASSERT_EQ(matches.size(), total);
        }
        ASSERT_EQ(std::min(k, matches.size()), hits.size());
        if (hits.empty()) {
          continue;
        }
        const auto kth = ranked[hits.size() - 1];
        absl::flat_hash_map<doc_id_t, irs::score_t> seen;
        for (size_t i = 0; i != hits.size(); ++i) {
          const auto it = matches.find(hits[i].doc);
          ASSERT_NE(it, matches.end()) << hits[i].doc;
          ASSERT_TRUE(AlmostEqual(it->second, hits[i].score))
            << hits[i].doc << " " << it->second << " " << hits[i].score;
          ASSERT_TRUE(i == 0 || hits[i - 1].score >= hits[i].score) << i;
          seen.emplace(hits[i].doc, hits[i].score);
        }
        ASSERT_TRUE(AlmostEqual(kth, hits.back().score))
          << kth << " " << hits.back().score;
        for (const auto& [doc, score] : matches) {
          if (score > kth && !AlmostEqual(score, kth)) {
            ASSERT_TRUE(seen.contains(doc)) << doc << " " << score;
          }
        }
      }
    }
  }

  irs::BM25 _scorer;
  const irs::Scorer* _ranking = &_scorer;
  irs::StatsArena _stats{duckdb::Allocator::DefaultAllocator()};
  irs::DirectoryReader _reader;
};

TEST_P(PrunedTopTest, conjunctions_across_block_kinds) {
  Build();
  for (const auto& shape : std::vector<Shape>{
         {{"mid", "dense"}, {}},
         {{"sparse", "half"}, {}},
         {{"rare", "dense"}, {}},
         {{"dense", "half"}, {}},
         {{"run", "mid"}, {}},
         {{"run", "half"}, {}},
         {{"holes", "sparse"}, {}},
         {{"holes", "dense"}, {}},
         {{"single", "half"}, {}},
         {{"rare", "sparse"}, {}},
         {{"cluster", "half"}, {}},
         {{"mid", "dense", "half"}, {}},
         {{"sparse", "mid", "dense", "half"}, {}},
         {{"rare", "mid", "run"}, {}},
         {{"cluster", "mid", "dense"}, {}},
       }) {
    Check(shape);
  }
}

TEST_P(PrunedTopTest, leads_skip_runs_of_non_competitive_blocks) {
  Build();
  for (const auto& shape : std::vector<Shape>{
         {{"burst", "common"}, {}},
         {{"burst", "dense"}, {}},
         {{"mid", "burst", "common"}, {}},
         {{"burst", "common"}, {"rare"}},
         {.must = {"burst"}, .should = {"common"}},
         {.must = {"burst"}, .should = {"common", "mid"}},
       }) {
    Check(shape);
  }
}

TEST_P(PrunedTopTest, conjunctions_with_exclusions) {
  Build();
  for (const auto& shape : std::vector<Shape>{
         {{"mid", "half"}, {"sparse"}},
         {{"sparse", "dense"}, {"run", "holes"}},
         {{"cluster", "dense"}, {"half"}},
         {{"mid", "dense", "half"}, {"rare", "cluster"}},
       }) {
    Check(shape);
  }
}

TEST_P(PrunedTopTest, windowed_conjunctions_join_windows_narrowed_to_a_block) {
  Build();
  for (const auto& shape : std::vector<Shape>{
         {{"mid", "holes"}, {}},
         {{"cluster", "mid", "holes"}, {}},
         {{"cluster", "mid", "dense"}, {"sparse"}},
         {{"mid", "holes", "half"}, {"rare", "run"}},
       }) {
    Check(shape);
  }
}

TEST_P(PrunedTopTest,
       single_terms_score_frequencies_that_can_beat_the_threshold) {
  Build();
  for (const auto& shape : std::vector<Shape>{
         {{"half"}, {}},
         {{"dense"}, {}},
         {{"mid"}, {}},
         {{"sparse"}, {}},
         {{"common"}, {}},
         {{"burst"}, {}},
         {{"cluster"}, {}},
         {{"run"}, {}},
         {{"holes"}, {}},
       }) {
    Check(shape);
  }
}

TEST_P(PrunedTopTest, single_terms_with_exclusions) {
  Build();
  for (const auto& shape : std::vector<Shape>{
         {{"rare"}, {"half"}},
         {{"sparse"}, {"dense", "mid"}},
         {{"mid"}, {"half", "single"}},
         {{"cluster"}, {"half", "run"}},
         {{"sparse"}, {"holes", "cluster"}},
         {{"half"}, {"sparse", "rare"}},
         {{"common"}, {"rare"}},
         {{"burst"}, {"edge"}},
         {{"common"}, {"single", "edge"}},
       }) {
    Check(shape);
  }
}

TEST(PrunedExcludeCosts, sparse_term_under_a_dense_lead_is_probed) {
  constexpr irs::doc_id_t kSegmentDocs = 5032104;
  constexpr uint64_t kLeadDocs = 4168066;
  constexpr uint64_t kCandidates = 9335;
  irs::PostingMeta excluded;
  excluded.docs_count = 2751;
  const irs::detail::PostingClause clause{irs::TermState{nullptr, excluded}};
  const irs::detail::ExcludeCosts<irs::detail::PostingClause> costs{
    std::span{&clause, 1}, {},
    kCandidates,           kLeadDocs,
    kSegmentDocs,          irs::detail::ExcludeUse::PerBlock};
  EXPECT_EQ(costs.Probed(true), irs::detail::ExcludeForm::Probes);
}

TEST(PrunedPhraseGate, bound_checks_pay_when_checked_docs_reach_the_bound) {
  EXPECT_GT(
    irs::detail::BoundCheckSaving(7.28, irs::detail::kLeafReadCost,
                                  irs::detail::kStepBaseCost, 0.36, 0.97),
    0);
  EXPECT_GT(
    irs::detail::BoundCheckSaving(8.02, irs::detail::kLeafDocsReadCost,
                                  irs::detail::kStepBaseCost, 0.03, 0.45),
    0);
}

TEST(PrunedPhraseGate, bound_checks_cost_when_cold_leaves_are_read_anyway) {
  EXPECT_LT(
    irs::detail::BoundCheckSaving(6.54, irs::detail::kLeafDocsReadCost,
                                  irs::detail::kStepBaseCost, 0.5, 0.13),
    0);
  EXPECT_LT(
    irs::detail::BoundCheckSaving(4.78, irs::detail::kLeafDocsReadCost,
                                  irs::detail::kStepBaseCost, 0.7, 0.53),
    0);
}

TEST(PrunedPhraseGate, union_checks_pay_while_they_reject_term_probes) {
  EXPECT_GT(irs::detail::BoundCheckSaving(0, 2.6, 30.1, 0.38, 0.54), 0);
  EXPECT_LT(irs::detail::BoundCheckSaving(0, 2.6, 30.1, 0.9, 0.54), 0);
}

TEST_P(PrunedTopTest, conjunctions_with_nested_disjunctions) {
  Build();
  for (const auto& shape : std::vector<Shape>{
         {{"mid"}, {}, {{"half", "dense"}}},
         {{"sparse"}, {}, {{"half", "cluster"}}},
         {{"mid"}, {"rare"}, {{"dense", "holes"}}},
         {{"cluster"}, {}, {{"half", "run"}, {"dense", "mid"}}},
       }) {
    Check(shape);
  }
}

TEST_P(PrunedTopTest, unions_across_block_kinds) {
  Build();
  for (const auto& shape : std::vector<Shape>{
         {.should = {"rare", "half"}},
         {.should = {"sparse", "dense"}},
         {.should = {"mid", "half", "dense"}},
         {.should = {"run", "sparse"}},
         {.should = {"holes", "mid"}},
         {.should = {"single", "dense"}},
         {.should = {"cluster", "half"}},
         {.should = {"rare", "sparse", "mid", "dense", "half"}},
         {.should = {"run", "holes", "cluster"}},
         {.should = {"edge", "run"}},
       }) {
    Check(shape);
  }
}

TEST_P(PrunedTopTest, unions_with_exclusions) {
  Build();
  for (const auto& shape : std::vector<Shape>{
         {.must_not = {"mid"}, .should = {"sparse", "half"}},
         {.must_not = {"holes"}, .should = {"rare", "dense", "run"}},
         {.must_not = {"rare", "cluster"}, .should = {"mid", "dense"}},
       }) {
    Check(shape);
  }
}

TEST_P(PrunedTopTest, unions_merging_by_max) {
  Build();
  for (const auto& terms : std::vector<std::vector<std::string_view>>{
         {"single", "rare", "mid"},
         {"single", "edge", "dense"},
         {"run", "holes", "sparse"},
         {"cluster", "half"},
       }) {
    SCOPED_TRACE(absl::StrJoin(terms, " "));
    irs::BooleanFilter filter;
    absl::flat_hash_map<doc_id_t, irs::score_t> best;
    for (const auto term : terms) {
      filter.Add(Clause(term), irs::Occur::Should);
      irs::BooleanFilter alone;
      alone.Add(Clause(term), irs::Occur::Should);
      alone.SetMinShouldMatch(1);
      for (const auto& [doc, score] : Matches(alone)) {
        auto [it, fresh] = best.try_emplace(doc, score);
        if (!fresh) {
          it->second = std::max(it->second, score);
        }
      }
    }
    filter.SetMinShouldMatch(1);
    filter.SetMergeType(irs::ScoreMergeType::Max);
    const auto matches = Matches(filter);
    ASSERT_EQ(best.size(), matches.size());
    for (const auto& [doc, score] : matches) {
      const auto it = best.find(doc);
      ASSERT_NE(it, best.end()) << doc;
      ASSERT_TRUE(AlmostEqual(it->second, score))
        << doc << " " << it->second << " " << score;
    }
    CheckRanking(filter);
  }
}

TEST_P(PrunedTopTest, nested_groups_score_as_their_parts) {
  Build();
  using Scores = absl::flat_hash_map<doc_id_t, irs::score_t>;
  const auto alone = [&](irs::Filter::ptr leaf) {
    irs::BooleanFilter filter;
    filter.Add(std::move(leaf), irs::Occur::Should);
    filter.SetMinShouldMatch(1);
    return Matches(filter);
  };
  const auto term = [&](std::string_view value) {
    irs::BooleanFilter filter;
    filter.Add(Clause(value), irs::Occur::Should);
    filter.SetMinShouldMatch(1);
    return Matches(filter);
  };
  const auto both = [](const Scores& lhs, const Scores& rhs) {
    Scores out;
    for (const auto& [doc, score] : lhs) {
      if (const auto it = rhs.find(doc); it != rhs.end()) {
        out.emplace(doc, score + it->second);
      }
    }
    return out;
  };
  const auto either = [](Scores lhs, const Scores& rhs,
                         irs::ScoreMergeType merge) {
    for (const auto& [doc, score] : rhs) {
      auto [it, fresh] = lhs.try_emplace(doc, score);
      if (!fresh) {
        it->second = merge == irs::ScoreMergeType::Max
                       ? std::max(it->second, score)
                       : it->second + score;
      }
    }
    return lhs;
  };
  const auto group = [](irs::Occur occur, std::string_view a,
                        std::string_view b, irs::ScoreMergeType merge) {
    auto filter = std::make_unique<irs::BooleanFilter>();
    filter->Add(Clause(a), occur);
    filter->Add(Clause(b), occur);
    if (occur == irs::Occur::Should) {
      filter->SetMinShouldMatch(1);
    }
    filter->SetMergeType(merge);
    return filter;
  };
  const auto check = [&](const irs::Filter& filter, const Scores& expected) {
    const auto matches = Matches(filter);
    ASSERT_EQ(expected.size(), matches.size());
    for (const auto& [doc, score] : matches) {
      const auto it = expected.find(doc);
      ASSERT_NE(it, expected.end()) << doc;
      ASSERT_TRUE(AlmostEqual(it->second, score))
        << doc << " " << it->second << " " << score;
    }
    CheckRanking(filter);
  };
  for (const auto& [x, a, b] : std::vector<std::array<std::string_view, 3>>{
         {"mid", "dense", "half"},
         {"sparse", "common", "dense"},
         {"doc", "single", "edge"},
       }) {
    SCOPED_TRACE(absl::StrCat(x, " ", a, " ", b));
    const auto xs = term(x);
    const auto as = term(a);
    const auto bs = term(b);
    {
      irs::BooleanFilter filter;
      filter.Add(Clause(x), irs::Occur::Should);
      filter.Add(group(irs::Occur::Must, a, b, irs::ScoreMergeType::Sum),
                 irs::Occur::Should);
      filter.SetMinShouldMatch(1);
      check(filter, either(xs, both(as, bs), irs::ScoreMergeType::Sum));
    }
    for (const auto merge :
         {irs::ScoreMergeType::Sum, irs::ScoreMergeType::Max}) {
      irs::BooleanFilter filter;
      filter.Add(Clause(x), irs::Occur::Must);
      filter.Add(group(irs::Occur::Should, a, b, merge), irs::Occur::Must);
      check(filter, both(xs, either(as, bs, merge)));
    }
  }
  {
    SCOPED_TRACE("mid (all dense)");
    irs::BooleanFilter filter;
    filter.Add(Clause("mid"), irs::Occur::Must);
    auto inner = std::make_unique<irs::BooleanFilter>();
    inner->Add(std::make_unique<irs::All>(), irs::Occur::Should);
    inner->Add(Clause("dense"), irs::Occur::Should);
    inner->SetMinShouldMatch(1);
    filter.Add(std::move(inner), irs::Occur::Must);
    check(filter,
          both(term("mid"), either(alone(std::make_unique<irs::All>()),
                                   term("dense"), irs::ScoreMergeType::Sum)));
  }
}

TEST_P(PrunedTopTest, absorbed_all_docs_and_constant_leads) {
  Build();
  const irs::ConstantScore constant{2.f};
  const auto all_and = [](std::string_view a, std::string_view b) {
    auto filter = std::make_unique<irs::BooleanFilter>();
    filter->Add(std::make_unique<irs::All>(), irs::Occur::Must);
    filter->Add(Clause(a), irs::Occur::Must);
    filter->Add(Clause(b), irs::Occur::Must);
    return filter;
  };
  for (const auto& [x, a, b] : std::vector<std::array<std::string_view, 3>>{
         {"mid", "dense", "half"},
         {"sparse", "common", "dense"},
       }) {
    SCOPED_TRACE(absl::StrCat(x, " ", a, " ", b));
    CheckRanking(*all_and(a, b));
    for (const auto merge :
         {irs::ScoreMergeType::Sum, irs::ScoreMergeType::Max}) {
      irs::BooleanFilter filter;
      filter.Add(Clause(x), irs::Occur::Should);
      filter.Add(all_and(a, b), irs::Occur::Should);
      filter.SetMinShouldMatch(1);
      filter.SetMergeType(merge);
      CheckRanking(filter);
    }
  }
  for (const auto merge :
       {irs::ScoreMergeType::Sum, irs::ScoreMergeType::Max}) {
    SCOPED_TRACE(merge == irs::ScoreMergeType::Max ? "max" : "sum");
    for (const auto& [a, b] : std::vector<std::array<std::string_view, 2>>{
           {"rare", "sparse"},
           {"sparse", "mid"},
         }) {
      SCOPED_TRACE(absl::StrCat("+all +", a, " +", b, " -dense"));
      auto sparse = std::make_unique<irs::BooleanFilter>();
      sparse->Add(std::make_unique<irs::All>(), irs::Occur::Must);
      sparse->Add(Clause(a), irs::Occur::Must);
      sparse->Add(Clause(b), irs::Occur::Must);
      sparse->Add(Clause("dense"), irs::Occur::MustNot);
      CheckRanking(*sparse);
      irs::BooleanFilter either;
      either.Add(Clause("edge"), irs::Occur::Should);
      either.Add(std::move(sparse), irs::Occur::Should);
      either.SetMinShouldMatch(1);
      either.SetMergeType(merge);
      CheckRanking(either);
    }
    auto excluding = std::make_unique<irs::BooleanFilter>();
    excluding->Add(std::make_unique<irs::All>(), irs::Occur::Must);
    excluding->Add(Clause("dense"), irs::Occur::Must);
    excluding->Add(Clause("half"), irs::Occur::Must);
    excluding->Add(Clause("mid"), irs::Occur::MustNot);
    CheckRanking(*excluding);
    irs::BooleanFilter outer;
    outer.Add(Clause("sparse"), irs::Occur::Should);
    outer.Add(std::move(excluding), irs::Occur::Should);
    outer.SetMinShouldMatch(1);
    outer.SetMergeType(merge);
    CheckRanking(outer);

    auto group = std::make_unique<irs::BooleanFilter>();
    auto constant_member = Clause("dense");
    constant_member.scorer = &constant;
    group->Add(std::move(constant_member), irs::Occur::Should);
    group->Add(Clause("half"), irs::Occur::Should);
    group->SetMinShouldMatch(1);
    group->SetMergeType(merge);
    irs::BooleanFilter nested;
    nested.Add(Clause("half"), irs::Occur::Must);
    nested.Add(std::move(group), irs::Occur::Must);
    CheckRanking(nested);
  }
  for (const auto merge :
       {irs::ScoreMergeType::Sum, irs::ScoreMergeType::Max}) {
    for (const bool constant_lead : {false, true}) {
      SCOPED_TRACE(absl::StrCat("constant lead ", constant_lead, " max ",
                                merge == irs::ScoreMergeType::Max));
      irs::BooleanFilter filter;
      auto lead = Clause("half");
      if (constant_lead) {
        lead.scorer = &constant;
      }
      filter.Add(std::move(lead), irs::Occur::Must);
      filter.Add(Clause("mid"), irs::Occur::Should);
      filter.Add(Clause("dense"), irs::Occur::Should);
      filter.SetMergeType(merge);
      CheckRanking(filter);
    }
  }
}

TEST_P(PrunedTopTest, phrases_left_alone_in_unions) {
  Build();
  for (const auto other : {"edge", "single", "holes"}) {
    SCOPED_TRACE(other);
    irs::BooleanFilter filter;
    filter.Add(PhraseOf({"run", "run"}), irs::Occur::Should);
    filter.Add(Clause(other), irs::Occur::Should);
    filter.SetMinShouldMatch(1);
    CheckRanking(filter);
  }
}

TEST_P(PrunedTopTest, phrases_matching_most_candidates_in_unions) {
  Build(PhrasesText);
  for (const auto& terms : std::vector<std::vector<std::string_view>>{
         {"of"},
         {"band", "town"},
         {"medal", "honor", "club"},
       }) {
    SCOPED_TRACE(absl::StrJoin(terms, " "));
    irs::BooleanFilter filter;
    filter.Add(PhraseOf({"pad", "pad"}), irs::Occur::Should);
    for (const auto term : terms) {
      filter.Add(Clause(term), irs::Occur::Should);
    }
    filter.SetMinShouldMatch(1);
    CheckRanking(filter);
  }
}

TEST_P(PrunedTopTest, sloppy_phrases_in_unions) {
  Build(PhrasesText);
  auto phrase = PhraseOf({"brass", "band"});
  static_cast<irs::ByPhrase&>(*phrase).mutable_options()->set_slop(1);
  irs::BooleanFilter filter;
  filter.Add(std::move(phrase), irs::Occur::Should);
  filter.Add(Clause("town"), irs::Occur::Should);
  filter.SetMinShouldMatch(1);
  CheckRanking(filter);
}

TEST_P(PrunedTopTest, constant_roots_rank_like_the_exhaustive_plan) {
  Build();
  CheckRanking(irs::All{});
  irs::BooleanFilter filter;
  filter.Add(std::make_unique<irs::All>(), irs::Occur::Must);
  filter.Add(Clause("dense"), irs::Occur::MustNot);
  CheckRanking(filter);
}

TEST_P(PrunedTopTest, prefix_exclusions) {
  Build();
  for (const auto& shape : std::vector<Shape>{
         {.should = {"mid", "dense"}, .must_not_prefixes = {"pfx"}},
         {.should = {"rare", "half", "sparse"}, .must_not_prefixes = {"pfx"}},
         {.must = {"mid", "dense"}, .must_not_prefixes = {"pfx"}},
         {.must = {"half"}, .must_not_prefixes = {"pfx1"}},
         {.must = {"dense", "half"}, .must_not_prefixes = {"pfx"}},
         {.must = {"common"}, .must_not_prefixes = {"pfx"}},
         {.must = {"rare"}, .must_not_prefixes = {"pfx"}},
       }) {
    Check(shape);
  }
}

TEST_P(PrunedTopTest, prefix_unions_across_block_kinds) {
  Build();
  for (const auto& shape : std::vector<Shape>{
         {.should_prefixes = {"pfx"}},
         {.should_prefixes = {"m"}},
         {.should_prefixes = {"s"}},
         {.should_prefixes = {"h"}},
         {.should_prefixes = {"r"}},
         {.should_prefixes = {"c"}},
         {.should_prefixes = {"d"}},
         {.should_prefixes = {"pfx1", "m"}},
         {.should = {"half"}, .should_prefixes = {"pfx"}},
       }) {
    Check(shape);
  }
}

TEST_P(PrunedTopTest, phrases_bound_candidates_before_reading_dense_terms) {
  Build(PhrasesText);
  for (const auto& parts : std::vector<std::vector<std::string_view>>{
         {"harbor", "town"},
         {"medal", "of", "honor"},
         {"visit", "port*"},
         {"harb*", "town"},
         {"town", "of"},
       }) {
    SCOPED_TRACE(absl::StrJoin(parts, " "));
    CheckRanking(*PhraseOf(parts));
  }
}

TEST_P(PrunedTopTest, phrases_probe_dense_terms_when_most_candidates_pass) {
  Build(PhrasesText);
  CheckRanking(*PhraseOf(std::vector<std::string_view>{"brass", "band"}));
}

TEST_P(PrunedTopTest, phrases_ranked_by_a_scorer_without_bounds) {
  Build(PhrasesText);
  const irs::IndriDirichlet indri;
  _ranking = &indri;
  for (const auto& parts : std::vector<std::vector<std::string_view>>{
         {"brass", "band"},
         {"of", "of"},
       }) {
    SCOPED_TRACE(absl::StrJoin(parts, " "));
    CheckRanking(*PhraseOf(parts));
  }
  irs::BooleanFilter phrases;
  phrases.Add(PhraseOf({"brass", "band"}), irs::Occur::Should);
  phrases.Add(PhraseOf({"harbor", "town"}), irs::Occur::Should);
  phrases.SetMinShouldMatch(1);
  CheckRanking(phrases);
}

TEST_P(PrunedTopTest, a_lone_phrase_beside_an_unscored_all) {
  Build(PhrasesText);
  irs::BooleanFilter filter;
  auto all = std::make_unique<irs::All>();
  all->SetBoost(0.f);
  filter.Add(std::move(all), irs::Occur::Must);
  filter.Add(PhraseOf({"brass", "band"}), irs::Occur::Must);
  CheckRanking(filter);
}

TEST_P(PrunedTopTest, phrases_with_repeated_terms) {
  Build(PhrasesText);
  for (const auto& parts : std::vector<std::vector<std::string_view>>{
         {"brass", "brass"},
         {"brass", "brass", "band"},
         {"of", "of"},
         {"brass", "brass", "brass", "brass"},
       }) {
    SCOPED_TRACE(absl::StrJoin(parts, " "));
    CheckRanking(*PhraseOf(parts));
  }
  for (const auto occur : {irs::Occur::Should, irs::Occur::Must}) {
    CheckMixed({{{"brass", "brass"}}, {"band"}, {}}, occur);
    CheckMixed({{{"of", "of"}}, {"town"}, {}}, occur);
  }
}

TEST_P(PrunedTopTest, phrases_defer_positions_of_terms_denser_than_the_lead) {
  Build(PhrasesText);
  for (const auto& parts : std::vector<std::vector<std::string_view>>{
         {"zephyr", "alpha", "beta"},
         {"zephyr", "alpha"},
         {"beta", "zephyr"},
         {"omega", "alpha"},
       }) {
    SCOPED_TRACE(absl::StrJoin(parts, " "));
    const auto filter = PhraseOf(parts);
    const auto matches = Matches(*filter);
    size_t expected = 0;
    for (doc_id_t i = 0; i != kDocs; ++i) {
      if (HasPhrase(PhrasesText(i), parts)) {
        ++expected;
        ASSERT_TRUE(matches.contains(irs::doc_limits::min() + i)) << i;
      }
    }
    ASSERT_EQ(expected, matches.size());
    CheckRanking(*filter);
  }
  for (const auto occur : {irs::Occur::Should, irs::Occur::Must}) {
    CheckMixed({{{"zephyr", "alpha", "beta"}}, {"town"}, {}}, occur);
  }
}

TEST_P(PrunedTopTest, unions_with_phrases) {
  Build(PhrasesText);
  for (const auto& shape : std::vector<Mixed>{
         {{{"harbor", "town"}}, {"of"}, {}},
         {{{"medal", "of", "honor"}, {"harbor", "town"}}, {"town"}, {}},
         {{{"visit", "port*"}}, {"pad"}, {}},
         {{{"harbor", "town"}, {"medal", "of", "honor"}}, {}, {}},
         {{{"harb*", "town"}}, {"of", "medal"}, {}},
         {{{"harbor", "town"}}, {"of", "honor"}, {"medal"}},
       }) {
    CheckMixed(shape, irs::Occur::Should);
  }
}

TEST_P(PrunedTopTest, conjunctions_with_phrases) {
  Build(PhrasesText);
  for (const auto& shape : std::vector<Mixed>{
         {{{"town", "of"}}, {"club"}, {}},
         {{{"town", "of"}}, {"club", "pad"}, {}},
         {{{"town", "of"}}, {"club"}, {"harbor"}},
         {{{"town", "port*"}}, {"club"}, {}},
         {{{"harbor", "town"}}, {"of"}, {}},
         {{{"medal", "of", "honor"}}, {"town"}, {}},
       }) {
    CheckMixed(shape, irs::Occur::Must);
  }
}

TEST_P(PrunedTopTest, required_terms_with_optional_ones) {
  Build();
  for (const auto& shape : std::vector<Shape>{
         {.must = {"mid"}, .should = {"half", "dense"}},
         {.must = {"sparse"}, .should = {"rare", "cluster"}},
         {.must = {"cluster", "dense"}, .should = {"half"}},
       }) {
    Check(shape);
  }
}

TEST_P(PrunedTopTest, required_term_merges_optional_ones_sparser_than_it) {
  Build();
  for (const auto& shape : std::vector<Shape>{
         {.must = {"half"}, .should = {"dense", "common"}},
         {.must = {"dense"}, .should = {"half"}},
         {.must = {"common"}, .should = {"dense", "mid"}},
         {.must = {"half"}, .should = {"burst"}},
         {.must = {"common"}, .should = {"half", "cluster", "run"}},
         {.must = {"sparse"}, .should = {"mid"}},
         {.must = {"rare"}, .should = {"mid", "sparse"}},
         {.must = {"mid"}, .should = {"sparse", "cluster", "holes"}},
       }) {
    Check(shape);
  }
}

TEST_P(PrunedTopTest, required_term_with_sparser_optional_ones) {
  Build();
  for (const auto& shape : std::vector<Shape>{
         {.must = {"half"}, .should = {"mid", "sparse"}},
         {.must = {"dense"}, .should = {"rare", "cluster", "run"}},
         {.must = {"half"}, .should = {"holes"}},
         {.must = {"dense"}, .should = {"mid", "single"}},
         {.must = {"half"}, .should = {"edge", "rare"}},
       }) {
    Check(shape);
  }
}

INSTANTIATE_TEST_SUITE_P(pruned_top_test, PrunedTopTest,
                         ::testing::Combine(::testing::Values(
                           &tests::Directory<&tests::MemoryDirectory>,
                           &tests::Directory<&tests::MmapDirectory>)),
                         PrunedTopTest::to_string);

}  // namespace
