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

#include <algorithm>
#include <atomic>
#include <bit>
#include <duckdb/common/allocator.hpp>
#include <iresearch/search/detail/doc_collector.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/scorers/bm25.hpp>
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
};

irs::TermClause Clause(std::string_view value) {
  return {.field = kBodyId,
          .term = irs::bstring{irs::ViewCast<irs::byte_type>(value)}};
}

bool AlmostEqual(irs::score_t lhs, irs::score_t rhs) {
  return testing::internal::FloatingPoint<irs::score_t>{lhs}.AlmostEquals(
    testing::internal::FloatingPoint<irs::score_t>{rhs});
}

struct Shape {
  std::vector<std::string_view> must;
  std::vector<std::string_view> must_not;
  std::vector<std::vector<std::string_view>> any;
};

class PrunedConjunctionTest : public tests::IndexTestBase {
 protected:
  void Build() {
    irs::IndexWriterOptions options;
    options.reader_options.scorer = &_scorer;
    auto writer = open_writer(irs::kOmCreate, std::move(options));
    auto body = std::make_shared<tests::TextField<std::string>>(
      "body", false, irs::IndexFeatures::Norm);
    body->id = kBodyId;
    tests::Document doc;
    doc.insert(body, true, false);
    for (doc_id_t i = 0; i != kDocs; ++i) {
      std::string text = "doc";
      for (const auto& spec : kSpecs) {
        if (spec.has(i)) {
          for (auto n = Freq(spec.salt, i); n != 0; --n) {
            absl::StrAppend(&text, " ", spec.term);
          }
        }
      }
      for (auto n = Padding(i); n != 0; --n) {
        absl::StrAppend(&text, " pad");
      }
      body->value(text);
      ASSERT_TRUE(Insert(*writer, doc));
    }
    writer->RefreshCommit();
    _reader = writer->GetSnapshot();
    ASSERT_EQ(1, _reader.size());
  }

  std::vector<irs::QueryBuilder::ptr> Prepare(const irs::Filter& filter) {
    irs::PreparedCollector collector{filter, _scorer, _stats, 1};
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
    auto plan = queries[0]->PlanScored({.scorer = _scorer, .fetcher = fetcher});
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
                                 doc_id_t span) {
    const auto queries = Prepare(filter);
    std::vector<irs::ScoreDoc> hits(k);
    std::atomic<irs::score_t> threshold{
      std::numeric_limits<irs::score_t>::lowest()};
    irs::LoserScoreCollector collector{threshold, hits};
    collector.SetSegment(0);
    if (queries[0] && !irs::QueryBuilder::IsEmpty(*queries[0])) {
      const auto end = irs::doc_limits::min() + kDocs;
      for (auto from = irs::doc_limits::min(); from < end; from += span) {
        irs::ColumnArgsFetcher fetcher;
        auto plan = queries[0]->PlanTop({.scorer = _scorer,
                                         .fetcher = fetcher,
                                         .prune = true,
                                         .k = static_cast<uint32_t>(k)});
        if (!plan) {
          break;
        }
        const auto to = std::min<uint64_t>(uint64_t{from} + span, end);
        plan->Run(from, static_cast<doc_id_t>(to), collector);
      }
    }
    collector.SortHits();
    hits.resize(std::min(k, collector.AcceptedCount()));
    return hits;
  }

  void Check(const Shape& shape) {
    SCOPED_TRACE(absl::StrCat("+", absl::StrJoin(shape.must, " +"), " -",
                              absl::StrJoin(shape.must_not, " -"), " any ",
                              shape.any.size()));
    irs::BooleanFilter filter;
    for (const auto term : shape.must) {
      filter.Add(Clause(term), irs::Occur::Must);
    }
    for (const auto term : shape.must_not) {
      filter.Add(Clause(term), irs::Occur::MustNot);
    }
    for (const auto& terms : shape.any) {
      auto group = std::make_unique<irs::BooleanFilter>();
      for (const auto term : terms) {
        group->Add(Clause(term), irs::Occur::Should);
      }
      group->SetMinShouldMatch(1);
      filter.Add(std::move(group), irs::Occur::Must);
    }
    const auto matches = Matches(filter);
    std::vector<irs::score_t> ranked;
    ranked.reserve(matches.size());
    for (const auto& [doc, score] : matches) {
      ranked.push_back(score);
    }
    absl::c_sort(ranked, std::greater<>{});
    for (const size_t k : {size_t{1}, size_t{10}, size_t{100}, size_t{1000}}) {
      for (const doc_id_t span : {doc_id_t{kDocs}, doc_id_t{9973}}) {
        SCOPED_TRACE(absl::StrCat("k ", k, " span ", span));
        const auto hits = Top(filter, k, span);
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
  irs::StatsArena _stats{duckdb::Allocator::DefaultAllocator()};
  irs::DirectoryReader _reader;
};

TEST_P(PrunedConjunctionTest, conjunctions_across_block_kinds) {
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

TEST_P(PrunedConjunctionTest, conjunctions_with_exclusions) {
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

TEST_P(PrunedConjunctionTest, conjunctions_with_nested_disjunctions) {
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

INSTANTIATE_TEST_SUITE_P(pruned_conjunction_test, PrunedConjunctionTest,
                         ::testing::Combine(::testing::Values(
                           &tests::Directory<&tests::MemoryDirectory>)),
                         PrunedConjunctionTest::to_string);

}  // namespace
