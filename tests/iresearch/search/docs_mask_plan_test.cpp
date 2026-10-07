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
/// Copyright holder is SereneDB GmbH
////////////////////////////////////////////////////////////////////////////////

#include <algorithm>
#include <atomic>
#include <cmath>
#include <functional>
#include <iresearch/index/document_mask.hpp>
#include <iresearch/search/count/root.hpp>
#include <iresearch/search/detail/doc_collector.hpp>
#include <iresearch/search/detail/window.hpp>
#include <iresearch/search/docs/root.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/search/queries/boolean_query.hpp>
#include <iresearch/search/queries/docs_mask_query.hpp>
#include <iresearch/search/scorers/bm25.hpp>
#include <iresearch/utils/bit_utils.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "index/index_tests.hpp"
#include "tests_shared.hpp"

namespace {

using irs::doc_id_t;

constexpr size_t kDocs = 196607;
constexpr irs::field_id kThird = 1;
constexpr irs::field_id kRare = 2;
constexpr irs::field_id kBlock = 3;
constexpr irs::field_id kBucket = 4;
constexpr irs::field_id kCluster = 5;
constexpr irs::field_id kRange = 6;
constexpr irs::field_id kHalf = 7;

enum class Pattern {
  UniformSparse,
  UniformMid,
  UniformDense,
  Clustered,
  Range,
  Gapped,
  Mixed,
};

irs::Filter::ptr Term(irs::field_id field, std::string_view value) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field_id() = field;
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
  return filter;
}

irs::TermClause Clause(irs::field_id field, std::string_view value) {
  return {.field = field,
          .term = irs::bstring{irs::ViewCast<irs::byte_type>(value)}};
}

struct Query {
  std::string name;
  irs::Filter::ptr filter;
};

std::vector<Query> Queries() {
  std::vector<Query> queries;
  queries.push_back({"third", Term(kThird, "y")});
  queries.push_back({"rare", Term(kRare, "y")});
  queries.push_back({"block", Term(kBlock, "y")});
  queries.push_back({"all", std::make_unique<irs::All>()});
  {
    auto filter = std::make_unique<irs::BooleanFilter>();
    filter->Add(Clause(kThird, "y"), irs::Occur::Must);
    filter->Add(Clause(kHalf, "y"), irs::Occur::Must);
    queries.push_back({"third_and_half", std::move(filter)});
  }
  {
    auto filter = std::make_unique<irs::BooleanFilter>();
    filter->Add(Clause(kThird, "y"), irs::Occur::Should);
    filter->Add(Clause(kRare, "y"), irs::Occur::Should);
    filter->SetMinShouldMatch(1);
    queries.push_back({"third_or_rare", std::move(filter)});
  }
  {
    auto filter = std::make_unique<irs::BooleanFilter>();
    filter->Add(Clause(kThird, "y"), irs::Occur::Must);
    filter->Add(Clause(kRare, "y"), irs::Occur::Should);
    queries.push_back({"third_plus_rare", std::move(filter)});
  }
  {
    auto filter = std::make_unique<irs::BooleanFilter>();
    filter->Add(Clause(kThird, "y"), irs::Occur::Should);
    filter->Add(Clause(kRare, "y"), irs::Occur::Should);
    filter->Add(Clause(kHalf, "y"), irs::Occur::Should);
    filter->SetMinShouldMatch(2);
    queries.push_back({"two_of_three", std::move(filter)});
  }
  {
    auto filter = std::make_unique<irs::BooleanFilter>();
    filter->Add(Clause(kThird, "y"), irs::Occur::Must);
    filter->Add(Clause(kRare, "y"), irs::Occur::MustNot);
    queries.push_back({"third_not_rare", std::move(filter)});
  }
  {
    auto filter = std::make_unique<irs::BooleanFilter>();
    filter->Add(std::make_unique<irs::All>(), irs::Occur::Must);
    filter->Add(Clause(kRare, "y"), irs::Occur::MustNot);
    queries.push_back({"not_rare", std::move(filter)});
  }
  {
    auto filter = std::make_unique<irs::BooleanFilter>();
    filter->Add(Clause(kBlock, "y"), irs::Occur::Must);
    filter->Add(Clause(kThird, "y"), irs::Occur::Must);
    filter->Add(Clause(kRare, "y"), irs::Occur::MustNot);
    queries.push_back({"block_and_third_not_rare", std::move(filter)});
  }
  return queries;
}

std::vector<irs::ScoreDoc> ScoreHits(const irs::DirectoryReader& reader,
                                     const irs::Filter& filter,
                                     const irs::Scorer& scorer, bool masked,
                                     bool prune, size_t k) {
  auto& allocator = duckdb::Allocator::DefaultAllocator();
  irs::StatsArena stats_arena{allocator};
  irs::PreparedCollector collector_tree{filter, scorer, stats_arena, 1};
  std::vector<irs::QueryBuilder::ptr> queries;
  for (const auto& segment : reader) {
    const irs::PrepareContext ctx{.collector = collector_tree.Get()};
    queries.emplace_back(masked ? irs::PrepareMasked(filter, segment, ctx)
                                : filter.PrepareSegment(segment, ctx));
  }
  collector_tree.Finish();
  std::vector<irs::ScoreDoc> hits(k);
  std::atomic<irs::score_t> threshold{
    std::numeric_limits<irs::score_t>::lowest()};
  irs::LoserScoreCollector collector{threshold, hits};
  irs::ColumnArgsFetcher fetcher;
  for (uint32_t i = 0; i != queries.size(); ++i) {
    fetcher.Clear();
    collector.SetSegment(i);
    if (!queries[i]) {
      continue;
    }
    auto plan =
      irs::top::MakeRoot(*queries[i], {.scorer = scorer,
                                       .fetcher = fetcher,
                                       .prune = prune,
                                       .k = static_cast<uint32_t>(k)});
    if (plan) {
      plan->Run(irs::doc_limits::min(), irs::doc_limits::eof(), collector);
    }
  }
  hits.resize(collector.AcceptedCount());
  return hits;
}

void SortByDoc(std::vector<irs::ScoreDoc>& hits) {
  std::sort(hits.begin(), hits.end(), [](const auto& l, const auto& r) {
    return std::pair{l.segment_idx, l.doc} < std::pair{r.segment_idx, r.doc};
  });
}

std::vector<irs::score_t> TopScores(std::vector<irs::ScoreDoc> hits, size_t k) {
  std::vector<irs::score_t> scores;
  for (const auto& hit : hits) {
    scores.push_back(hit.score);
  }
  std::sort(scores.begin(), scores.end(), std::greater<>{});
  scores.resize(std::min(k, scores.size()));
  return scores;
}

std::vector<doc_id_t> Drain(irs::lead::Node& lead) {
  std::vector<doc_id_t> docs;
  for (auto doc = lead.Next(); !irs::doc_limits::eof(doc); doc = lead.Next()) {
    docs.push_back(doc);
  }
  return docs;
}

class DocsMaskPlanTest : public tests::IndexTestBase {
 protected:
  void Build(Pattern pattern, uint32_t segment_docs) {
    auto options = tests::CsDefaultWriterOptions();
    options.segment_docs_max = segment_docs;
    auto writer = open_writer(irs::kOmCreate, std::move(options));
    std::vector<std::shared_ptr<tests::StringField>> fields;
    tests::Document doc;
    for (const auto id :
         {kThird, kRare, kBlock, kBucket, kCluster, kRange, kHalf}) {
      auto field = std::make_shared<tests::StringField>(
        "f" + std::to_string(id), irs::IndexFeatures::Freq);
      field->id = id;
      doc.insert(field);
      fields.push_back(std::move(field));
    }
    for (size_t i = 0; i != kDocs; ++i) {
      fields[0]->value(i % 3 == 0 ? "y" : "n");
      fields[1]->value(i % 17 == 0 ? "y" : "n");
      fields[2]->value(i >= 50000 && i < 52000 ? "y" : "n");
      fields[3]->value(std::to_string(i * 7919 % 10000));
      fields[4]->value(std::to_string((i / 512) % 40));
      fields[5]->value(std::to_string(i * 50 / kDocs));
      fields[6]->value(i % 2 == 0 ? "y" : "n");
      ASSERT_TRUE(Insert(*writer, doc));
    }
    writer->RefreshCommit();

    std::vector<std::pair<irs::field_id, std::string>> removals;
    switch (pattern) {
      case Pattern::UniformSparse:
        removals = {{kBucket, "7"}};
        break;
      case Pattern::UniformMid:
        for (int b = 0; b != 10; ++b) {
          removals.emplace_back(kBucket, std::to_string(b));
        }
        break;
      case Pattern::UniformDense:
        for (int b = 0; b != 1200; ++b) {
          removals.emplace_back(kBucket, std::to_string(b));
        }
        break;
      case Pattern::Clustered:
        removals = {{kCluster, "3"}, {kCluster, "17"}};
        break;
      case Pattern::Range:
        removals = {{kRange, "10"}, {kRange, "11"}};
        break;
      case Pattern::Gapped:
        removals = {{kRange, "1"}, {kRange, "49"}};
        break;
      case Pattern::Mixed:
        removals = {{kBucket, "7"}, {kRange, "20"}};
        break;
    }
    for (const auto& [field, value] : removals) {
      auto trx = writer->GetBatch();
      trx.Remove(Term(field, value));
      trx.Commit();
    }
    writer->RefreshCommit();
  }

  void Check(std::optional<irs::MaskKind> kind) {
    auto reader = open_reader();
    ASSERT_GT(reader.size(), 0);
    if (kind) {
      ASSERT_EQ(1, reader.size());
      ASSERT_NE(nullptr, reader[0].docs_mask());
      ASSERT_EQ(*kind, reader[0].docs_mask()->Kind());
    } else {
      ASSERT_LT(1, reader.size());
    }
    CheckQueries(reader);
  }

 private:
  void CheckQueries(const irs::DirectoryReader& reader) {
    const auto scorer = irs::BM25::Make(irs::BM25::Options{});
    for (const auto& query : Queries()) {
      SCOPED_TRACE(query.name);
      uint64_t total = 0;
      for (const auto& segment : reader) {
        ASSERT_LT(segment.live_docs_count(), segment.docs_count());
        const auto expected = Expected(*query.filter, segment);
        total += expected.size();
        CheckSegment(*query.filter, segment, expected);
      }
      for (const bool prune : {false, true}) {
        std::vector<irs::ScoreDoc> hits(10);
        ASSERT_EQ(total, irs::ExecuteTopK(reader, *query.filter, *scorer,
                                          hits.size(), prune, hits));
      }
      CheckScores(reader, *query.filter, *scorer, total);
    }
    CheckFlattened(reader, *scorer);
  }

  static void CheckScores(const irs::DirectoryReader& reader,
                          const irs::Filter& filter, const irs::Scorer& scorer,
                          uint64_t total) {
    const auto unmasked_k = static_cast<size_t>(reader.docs_count()) + 1;
    auto expected = ScoreHits(reader, filter, scorer, false, false, unmasked_k);
    std::erase_if(expected, [&](const irs::ScoreDoc& hit) {
      const auto& segment = reader[hit.segment_idx];
      const auto* mask = segment.docs_mask();
      return hit.doc >= segment.Meta().visible_end ||
             (mask != nullptr && mask->Contains(hit.doc));
    });
    ASSERT_EQ(total, expected.size());
    SortByDoc(expected);
    auto actual = ScoreHits(reader, filter, scorer, true, false,
                            std::max<size_t>(total, 1));
    SortByDoc(actual);
    ASSERT_EQ(expected.size(), actual.size());
    for (size_t i = 0; i != expected.size(); ++i) {
      ASSERT_EQ(expected[i].segment_idx, actual[i].segment_idx) << i;
      ASSERT_EQ(expected[i].doc, actual[i].doc) << i;
      ASSERT_NEAR(expected[i].score, actual[i].score,
                  1e-5f * std::max(1.0f, std::abs(expected[i].score)))
        << expected[i].doc;
    }
    constexpr size_t kTop = 10;
    const auto best = TopScores(expected, kTop);
    const auto pruned =
      TopScores(ScoreHits(reader, filter, scorer, true, true, kTop), kTop);
    ASSERT_EQ(best.size(), pruned.size());
    for (size_t i = 0; i != best.size(); ++i) {
      ASSERT_NEAR(best[i], pruned[i], 1e-5f * std::max(1.0f, std::abs(best[i])))
        << i;
    }
  }

  static void CheckFlattened(const irs::DirectoryReader& reader,
                             const irs::Scorer& scorer) {
    irs::BooleanFilter filter;
    filter.Add(Clause(kThird, "y"), irs::Occur::Should);
    filter.Add(Clause(kRare, "y"), irs::Occur::Should);
    filter.SetMinShouldMatch(1);
    auto& allocator = duckdb::Allocator::DefaultAllocator();
    irs::StatsArena stats_arena{allocator};
    irs::PreparedCollector collector_tree{filter, scorer, stats_arena, 1};
    for (const auto& segment : reader) {
      auto nested = irs::PrepareMasked(filter, segment, {});
      ASSERT_NE(nullptr, nested);
      ASSERT_EQ(irs::QueryKind::Boolean, nested->Kind());
      const auto& outer = irs::utils::downCast<irs::BooleanQuery>(*nested);
      ASSERT_TRUE(outer.Terms(irs::Occur::Should).empty());
      ASSERT_TRUE(outer.Queries(irs::Occur::Should).empty());
      const auto inner = outer.Queries(irs::Occur::Must);
      ASSERT_EQ(1, inner.size());
      ASSERT_EQ(irs::QueryKind::Boolean, inner.front()->Kind());
      const auto masks = outer.Queries(irs::Occur::MustNot);
      ASSERT_EQ(1, masks.size());
      ASSERT_EQ(irs::QueryKind::DocsMask, masks.front()->Kind());

      auto query = irs::PrepareMasked(filter, segment,
                                      {.collector = collector_tree.Get()});
      ASSERT_NE(nullptr, query);
      ASSERT_EQ(irs::QueryKind::Boolean, query->Kind());
      const auto& flat = irs::utils::downCast<irs::BooleanQuery>(*query);
      ASSERT_EQ(1, flat.MinShouldMatch());
      ASSERT_TRUE(flat.Terms(irs::Occur::Must).empty());
      ASSERT_TRUE(flat.Queries(irs::Occur::Must).empty());
      ASSERT_EQ(2, flat.Terms(irs::Occur::Should).size());
      const auto excludes = flat.Queries(irs::Occur::MustNot);
      ASSERT_EQ(1, excludes.size());
      ASSERT_EQ(irs::QueryKind::DocsMask, excludes.front()->Kind());
    }
    collector_tree.Finish();
  }

  static std::vector<doc_id_t> Expected(const irs::Filter& filter,
                                        const irs::SubReader& segment) {
    auto unmasked = filter.PrepareSegment(segment, {});
    std::vector<doc_id_t> docs;
    if (!unmasked) {
      return docs;
    }
    auto lead = unmasked->PlanLead({});
    if (!lead) {
      return docs;
    }
    const auto* mask = segment.docs_mask();
    const auto visible_end = segment.Meta().visible_end;
    for (const auto doc : Drain(*lead)) {
      if (doc < visible_end && (mask == nullptr || !mask->Contains(doc))) {
        docs.push_back(doc);
      }
    }
    return docs;
  }

  static void CheckSegment(const irs::Filter& filter,
                           const irs::SubReader& segment,
                           const std::vector<doc_id_t>& expected) {
    auto query = irs::PrepareMasked(filter, segment, {});
    ASSERT_NE(nullptr, query);
    const auto end =
      static_cast<doc_id_t>(irs::doc_limits::min() + segment.docs_count());

    auto lead = query->PlanLead({});
    if (expected.empty() && !lead) {
      return;
    }
    ASSERT_NE(nullptr, lead);
    ASSERT_EQ(expected, Drain(*lead));

    {
      auto seeker = query->PlanLead({});
      ASSERT_NE(nullptr, seeker);
      auto it = expected.begin();
      for (doc_id_t target = 1; target < end + 5; target += 997) {
        it = std::lower_bound(it, expected.end(), target);
        const auto want = it == expected.end() ? irs::doc_limits::eof() : *it;
        ASSERT_EQ(want, seeker->Seek(target)) << target;
      }
    }

    {
      auto count = query->PlanCount({});
      ASSERT_NE(nullptr, count);
      ASSERT_EQ(expected.size(),
                count->Run(irs::doc_limits::min(), irs::doc_limits::eof()) +
                  count->Finish());
      auto split = query->PlanCount({.partial = true});
      ASSERT_NE(nullptr, split);
      constexpr doc_id_t kBounds[]{
        irs::doc_limits::min(), 777, 30001, 65536, 70001,
        irs::doc_limits::eof()};
      uint64_t parts = 0;
      for (size_t i = 1; i != std::size(kBounds); ++i) {
        parts += split->Run(kBounds[i - 1], kBounds[i]);
      }
      parts += split->Finish();
      ASSERT_EQ(expected.size(), parts);

      auto fine = query->PlanCount({.partial = true});
      ASSERT_NE(nullptr, fine);
      uint64_t pieces = 0;
      doc_id_t from = irs::doc_limits::min();
      for (doc_id_t step = 1; from < end; step = step * 7 % 1013 + 1) {
        const auto to = std::min<doc_id_t>(from + step, end);
        pieces += fine->Run(from, to);
        from = to;
      }
      pieces += fine->Run(end, irs::doc_limits::eof()) + fine->Finish();
      ASSERT_EQ(expected.size(), pieces);
    }

    {
      auto docs = query->PlanDocs({});
      ASSERT_NE(nullptr, docs);
      constexpr doc_id_t kWindow = 3000;
      std::vector<doc_id_t> buf(kWindow + irs::doc_limits::kDocsSlack);
      std::vector<doc_id_t> actual;
      for (doc_id_t min = irs::doc_limits::min(); min < end; min += kWindow) {
        const auto n = docs->Run(min, std::min(min + kWindow, end), buf.data());
        actual.insert(actual.end(), buf.begin(), buf.begin() + n);
      }
      ASSERT_EQ(expected, actual);
    }

    {
      auto probe = query->PlanProbe({}, expected.size() + 1);
      ASSERT_NE(nullptr, probe);
      auto it = expected.begin();
      for (doc_id_t target = 1; target < end; target += 1 + target % 13) {
        it = std::lower_bound(it, expected.end(), target);
        const auto next = it == expected.end() ? irs::doc_limits::eof() : *it;
        const auto bound = probe->Probe(target);
        ASSERT_GE(bound, target);
        ASSERT_EQ(next == target, bound == target) << target;
        ASSERT_LE(bound, next) << target;
      }
    }

    {
      auto fill = query->PlanFill({}, irs::ScoreMergeType::Noop);
      ASSERT_NE(nullptr, fill);
      std::vector<doc_id_t> actual;
      for (doc_id_t min = irs::doc_limits::min(); min < end;
           min += irs::detail::kWindowDocs) {
        const auto max =
          std::min<doc_id_t>(min + irs::detail::kWindowDocs, end);
        uint64_t words[irs::detail::kWindowWords]{};
        fill->FillOr(min, max, words);
        for (doc_id_t doc = min; doc < max; ++doc) {
          const auto offset = doc - min;
          if (irs::CheckBit(words[offset / 64], offset % 64)) {
            actual.push_back(doc);
          }
        }
      }
      ASSERT_EQ(expected, actual);
    }
  }
};

TEST_P(DocsMaskPlanTest, uniform_sparse) {
  Build(Pattern::UniformSparse, 0);
  Check(irs::MaskKind::Arrays);
}

TEST_P(DocsMaskPlanTest, uniform_mid_densified) {
  Build(Pattern::UniformMid, 0);
  Check(irs::MaskKind::Bitsets);
}

TEST_P(DocsMaskPlanTest, uniform_dense) {
  Build(Pattern::UniformDense, 0);
  Check(irs::MaskKind::Bitsets);
}

TEST_P(DocsMaskPlanTest, clustered) {
  Build(Pattern::Clustered, 0);
  Check(irs::MaskKind::Runs);
}

TEST_P(DocsMaskPlanTest, range) {
  Build(Pattern::Range, 0);
  Check(irs::MaskKind::Run);
}

TEST_P(DocsMaskPlanTest, gapped) {
  Build(Pattern::Gapped, 0);
  Check(irs::MaskKind::Runs);
}

TEST_P(DocsMaskPlanTest, mixed) {
  Build(Pattern::Mixed, 0);
  Check(irs::MaskKind::Mixed);
}

TEST_P(DocsMaskPlanTest, small_segments_uniform_dense) {
  Build(Pattern::UniformDense, 30000);
  Check(std::nullopt);
}

TEST_P(DocsMaskPlanTest, small_segments_mixed) {
  Build(Pattern::Mixed, 30000);
  Check(std::nullopt);
}

TEST_P(DocsMaskPlanTest, small_segments_clustered) {
  Build(Pattern::Clustered, 30000);
  Check(std::nullopt);
}

INSTANTIATE_TEST_SUITE_P(docs_mask_plan_test, DocsMaskPlanTest,
                         ::testing::Combine(::testing::Values(
                           &tests::Directory<&tests::MemoryDirectory>)),
                         DocsMaskPlanTest::to_string);

}  // namespace
