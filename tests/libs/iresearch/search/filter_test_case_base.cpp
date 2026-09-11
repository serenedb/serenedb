////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "filter_test_case_base.hpp"

#include <algorithm>
#include <compare>
#include <duckdb/common/allocator.hpp>

#include "iresearch/search/column_collector.hpp"

namespace tests {

PreparedFilter::PreparedFilter(const irs::Filter& filter,
                               const irs::IndexReader& index,
                               const irs::Scorer* scorer,
                               irs::IResourceManager& memory,
                               const irs::AttributeProvider* ctx,
                               CollectMode mode, irs::IResourceManager*)
  : _scorer{scorer} {
  _queries.reserve(index.size());

  // No scorer means this fixture does not score, and a consumer that does not
  // score builds no collector at all -- so it lands on the same path as an
  // explicit `NoCollector`, with empty statistics.
  const bool collects = scorer != nullptr && !irs::IsUnscored(*scorer) &&
                        mode != CollectMode::NoCollector;

  if (!collects) {
    for (const auto& sub : index) {
      _queries.emplace_back(filter.PrepareSegment(sub, {
                                                         .collector = nullptr,
                                                         .memory = memory,
                                                         .ctx = ctx,
                                                       }));
    }
    return;
  }

  const auto segments = std::max<uint32_t>(1, index.size());
  const auto threads = [&] {
    switch (mode) {
      case CollectMode::Single:
        return uint32_t{1};
      case CollectMode::PairThreads:
        return std::min<uint32_t>(2, segments);
      default:
        return segments;
    }
  }();
  auto& allocator = duckdb::Allocator::DefaultAllocator();
  _stats.emplace(allocator);
  _collector.emplace(filter, *scorer, *_stats, threads);
  uint32_t seg = 0;
  for (const auto& sub : index) {
    _queries.emplace_back(
      filter.PrepareSegment(sub, {
                                   .collector = _collector->Get(),
                                   .memory = memory,
                                   .ctx = ctx,
                                   .thread = seg++ % threads,
                                 }));
  }
  _collector->Finish();
}

void FilterTestCaseBase::GetQueryResult(const PreparedFilter& prepared,
                                        const irs::IndexReader& rdr,
                                        Docs& result, Costs& result_costs,
                                        std::string_view source_location) {
  SCOPED_TRACE(source_location);
  result_costs.reserve(rdr.size());

  for (size_t i = 0, n = prepared.size(); i < n; ++i) {
    auto random_docs = prepared.Execute(i);
    ASSERT_NE(nullptr, random_docs);
    auto sequential_docs = prepared.Execute(i);
    ASSERT_NE(nullptr, sequential_docs);

    result_costs.emplace_back(prepared.Estimate(i));

    while (!irs::doc_limits::eof(sequential_docs->Advance())) {
      auto stateless_random_docs = prepared.Execute(i);
      ASSERT_NE(nullptr, stateless_random_docs);
      ASSERT_EQ(sequential_docs->Value(),
                random_docs->Seek(sequential_docs->Value()));
      ASSERT_EQ(sequential_docs->Value(), random_docs->Value());
      ASSERT_EQ(sequential_docs->Value(),
                random_docs->Seek(sequential_docs->Value()));
      ASSERT_EQ(sequential_docs->Value(), random_docs->Value());
      ASSERT_EQ(sequential_docs->Value(),
                stateless_random_docs->Seek(sequential_docs->Value()));
      ASSERT_EQ(sequential_docs->Value(), stateless_random_docs->Value());
      ASSERT_EQ(sequential_docs->Value(),
                stateless_random_docs->Seek(sequential_docs->Value()));
      ASSERT_EQ(sequential_docs->Value(), stateless_random_docs->Value());

      result.push_back(sequential_docs->Value());
    }
    ASSERT_FALSE(!irs::doc_limits::eof(sequential_docs->Advance()));
    ASSERT_FALSE(!irs::doc_limits::eof(random_docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(sequential_docs->Value()));

    // seek to eof
    ASSERT_TRUE(
      irs::doc_limits::eof(prepared.Execute(i)->Seek(irs::doc_limits::eof())));
  }
}

void FilterTestCaseBase::GetQueryResult(const PreparedFilter& prepared,
                                        const irs::IndexReader& rdr,
                                        ScoredDocs& result, Costs& result_costs,
                                        std::string_view source_location) {
  SCOPED_TRACE(source_location);
  result_costs.reserve(rdr.size());

  for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
    irs::ColumnArgsFetcher random_fetcher;
    irs::ColumnArgsFetcher sequential_fetcher;
    auto random_docs = prepared.ExecuteScored(i, random_fetcher);
    ASSERT_NE(nullptr, random_docs);
    auto random_score = random_docs->PrepareScore();
    auto sequential_docs = prepared.ExecuteScored(i, sequential_fetcher);
    ASSERT_NE(nullptr, sequential_docs);

    auto score = sequential_docs->PrepareScore();

    result_costs.emplace_back(prepared.Estimate(i));

    while (!irs::doc_limits::eof(sequential_docs->Advance())) {
      irs::ColumnArgsFetcher stateless_fetcher;
      auto stateless_random_docs = prepared.ExecuteScored(i, stateless_fetcher);
      auto stateless_random_score = stateless_random_docs->PrepareScore();

      ASSERT_NE(nullptr, stateless_random_docs);
      ASSERT_EQ(sequential_docs->Value(),
                random_docs->Seek(sequential_docs->Value()));
      ASSERT_EQ(sequential_docs->Value(),
                random_docs->Seek(sequential_docs->Value()));
      ASSERT_EQ(sequential_docs->Value(), random_docs->Value());
      ASSERT_EQ(sequential_docs->Value(),
                stateless_random_docs->Seek(sequential_docs->Value()));
      ASSERT_EQ(sequential_docs->Value(),
                stateless_random_docs->Seek(sequential_docs->Value()));
      ASSERT_EQ(sequential_docs->Value(), stateless_random_docs->Value());

      sequential_docs->FetchScoreArgs(0);
      stateless_random_docs->FetchScoreArgs(0);
      random_docs->FetchScoreArgs(0);

      sequential_fetcher.Fetch(sequential_docs->Value());
      stateless_fetcher.Fetch(stateless_random_docs->Value());
      random_fetcher.Fetch(random_docs->Value());

      irs::score_t score_value{-1};
      score.Score(&score_value, 1);
      irs::score_t stateless_score_value{-2};
      stateless_random_score.Score(&stateless_score_value, 1);
      irs::score_t random_score_value{-3};
      random_score.Score(&random_score_value, 1);
      ASSERT_EQ(score_value, stateless_score_value);
      ASSERT_EQ(score_value, random_score_value);

      result.emplace_back(sequential_docs->Value(),
                          std::vector<irs::score_t>{score_value});
    }
    ASSERT_FALSE(!irs::doc_limits::eof(sequential_docs->Advance()));
    ASSERT_FALSE(!irs::doc_limits::eof(random_docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(sequential_docs->Value()));

    // seek to eof
    irs::ColumnArgsFetcher eof_fetcher;
    ASSERT_TRUE(irs::doc_limits::eof(
      prepared.ExecuteScored(i, eof_fetcher)->Seek(irs::doc_limits::eof())));
    ++i;
  }
}

void FilterTestCaseBase::CheckQuery(const irs::Filter& filter,
                                    const Docs& expected,
                                    const Costs& expected_costs,
                                    const irs::IndexReader& index,
                                    std::string_view source_location) {
  SCOPED_TRACE(source_location);
  Docs result;
  Costs result_costs;
  PreparedFilter prepared{filter, index};
  GetQueryResult(prepared, index, result, result_costs, source_location);
  ASSERT_EQ(expected, result);
  ASSERT_EQ(expected_costs, result_costs);
}

void FilterTestCaseBase::CheckQuery(const irs::Filter& filter,
                                    std::span<const irs::Scorer::ptr> order,
                                    const std::vector<Tests>& tests,
                                    const irs::IndexReader& rdr,
                                    std::string_view source_location) {
  SCOPED_TRACE(source_location);
  auto* scorer = order.empty() ? nullptr : order.front().get();
  PreparedFilter prepared{filter, rdr, scorer};
  irs::ColumnArgsFetcher fetcher;

  auto assert_equal_scores = [&](const std::vector<irs::score_t>& expected,
                                 auto& score) {
    if (!expected.empty()) {
      ASSERT_EQ(1, expected.size());
      irs::score_t actual;
      score.Score(&actual, 1);
      ASSERT_EQ(expected[0], actual);
    }
  };

  auto assert_iterator = [&](auto& test, auto& it, auto& score) {
    std::visit(
      [&it, expected = test.expected]<typename A>(A action) {
        if constexpr (std::is_same_v<A, Seek>) {
          ASSERT_EQ(expected, it.Seek(action.target));
        } else if constexpr (std::is_same_v<A, Next>) {
          ASSERT_EQ(!irs::doc_limits::eof(expected),
                    !irs::doc_limits::eof(it.Advance()));
        } else if constexpr (std::is_same_v<A, Skip>) {
          for (auto count = action.count; count; --count) {
            it.Advance();
          }
        }
      },
      test.action);
    ASSERT_EQ(test.expected, it.Value());
    if (!irs::doc_limits::eof(test.expected)) {
      if constexpr (requires { it.FetchScoreArgs(0); }) {
        it.FetchScoreArgs(0);
        fetcher.Fetch(it.Value());
      }
      assert_equal_scores(test.score, score);
    }
  };

  auto test = std::begin(tests);
  for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
    ASSERT_NE(test, std::end(tests));
    fetcher.Clear();
    if (scorer == nullptr) {
      auto random_docs = prepared.Execute(i);
      ASSERT_NE(nullptr, random_docs);
      irs::ScoreFunction random_score;
      for (auto& step : *test) {
        assert_iterator(step, *random_docs, random_score);
      }
    } else {
      auto random_docs = prepared.ExecuteScored(i, fetcher);
      ASSERT_NE(nullptr, random_docs);
      auto random_score = random_docs->PrepareScore();
      for (auto& step : *test) {
        assert_iterator(step, *random_docs, random_score);
      }
    }

    ++test;
    ++i;
  }
}

void FilterTestCaseBase::CheckQuery(const irs::Filter& filter,
                                    std::span<const irs::Scorer::ptr> order,
                                    const ScoredDocs& expected,
                                    const irs::IndexReader& index,
                                    std::string_view source_location) {
  SCOPED_TRACE(source_location);
  ScoredDocs result;
  Costs result_costs;
  auto* scorer = order.empty() ? nullptr : order.front().get();
  PreparedFilter prepared{filter, index, scorer};
  GetQueryResult(prepared, index, result, result_costs, source_location);
  ASSERT_EQ(expected, result);
}

void FilterTestCaseBase::CheckQuery(const irs::Filter& filter,
                                    const Docs& expected,
                                    const irs::IndexReader& index,
                                    std::string_view source_location) {
  SCOPED_TRACE(source_location);
  Docs result;
  Costs result_costs;
  PreparedFilter prepared{filter, index};
  GetQueryResult(prepared, index, result, result_costs, source_location);
  ASSERT_EQ(expected, result);
}

void FilterTestCaseBase::MakeResult(const irs::Filter& filter,
                                    std::span<const irs::Scorer::ptr> order,
                                    const irs::IndexReader& rdr,
                                    std::vector<irs::doc_id_t>& result,
                                    bool score_must_be_present, bool reverse) {
  auto* scorer = order.empty() ? nullptr : order.front().get();
  PreparedFilter prepared{filter, rdr, scorer};
  auto score_less =
    [reverse](const std::pair<irs::score_t, irs::doc_id_t>& lhs,
              const std::pair<irs::score_t, irs::doc_id_t>& rhs) -> bool {
    const auto& [lhs_score, lhs_doc] = lhs;
    const auto& [rhs_score, rhs_doc] = rhs;

    const auto r = (lhs_score <=> rhs_score);

    if (r < 0) {
      return !reverse;
    }

    if (r > 0) {
      return reverse;
    }

    return lhs_doc < rhs_doc;
  };

  std::multiset<std::pair<irs::score_t, irs::doc_id_t>, decltype(score_less)>
    scored_result{score_less};
  irs::ColumnArgsFetcher fetcher;

  for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
    fetcher.Clear();
    irs::score_t score_value{};

    if (score_must_be_present) {
      auto docs = prepared.ExecuteScored(i, fetcher);
      ASSERT_NE(nullptr, docs);
      auto score = docs->PrepareScore();

      while (!irs::doc_limits::eof(docs->Advance())) {
        docs->FetchScoreArgs(0);
        fetcher.Fetch(docs->Value());
        score.Score(&score_value, 1);
        scored_result.emplace(score_value, docs->Value());
      }
      ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    } else {
      auto docs = prepared.Execute(i);
      ASSERT_NE(nullptr, docs);

      while (!irs::doc_limits::eof(docs->Advance())) {
        scored_result.emplace(score_value, docs->Value());
      }
      ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    }
    ++i;
  }

  result.clear();
  for (auto& entry : scored_result) {
    result.emplace_back(entry.second);
  }
}

void FilterTestCaseBase::CheckQuery(const irs::Filter& filter,
                                    std::span<const irs::Scorer::ptr> order,
                                    const std::vector<irs::doc_id_t>& expected,
                                    const irs::IndexReader& rdr,
                                    bool score_must_be_present, bool reverse) {
  std::vector<irs::doc_id_t> result;
  MakeResult(filter, order, rdr, result, score_must_be_present, reverse);
  ASSERT_EQ(expected, result);
}

}  // namespace tests
