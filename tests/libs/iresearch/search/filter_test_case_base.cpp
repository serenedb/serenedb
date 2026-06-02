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

#include <compare>

#include "iresearch/search/column_collector.hpp"

namespace tests {

PreparedFilter::PreparedFilter(const irs::Filter& filter,
                               const irs::IndexReader& index,
                               const irs::Scorer* scorer,
                               irs::IResourceManager& memory,
                               const irs::AttributeProvider* ctx,
                               CollectMode mode)
  : _scorer{scorer} {
  _queries.reserve(index.size());

  if (mode == CollectMode::Single) {
    _collector = filter.MakeCollector(scorer);
    for (const auto& sub : index) {
      _queries.emplace_back(
        filter.PrepareSegment(sub, {
                                     .collector = _collector.get(),
                                     .memory = memory,
                                     .ctx = ctx,
                                   }));
    }
    _stats.emplace(_collector->Finish(memory));
  } else {
    _perseg.reserve(index.size());
    for (const auto& sub : index) {
      auto collector = filter.MakeCollector(scorer);
      _queries.emplace_back(
        filter.PrepareSegment(sub, {
                                     .collector = collector.get(),
                                     .memory = memory,
                                     .ctx = ctx,
                                   }));
      _perseg.emplace_back(std::move(collector));
    }
    if (_perseg.empty()) {
      _perseg.emplace_back(filter.MakeCollector(scorer));
    }
    for (size_t i = 1, n = _perseg.size(); i < n; ++i) {
      _perseg.front()->Merge(std::move(*_perseg[i]));
    }
    _stats.emplace(_perseg.front()->Finish(memory));
  }

  _exec.emplace(irs::ExecutionContext{
    .memory = memory,
    .stats = &*_stats,
    .ctx = ctx,
  });
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

    result_costs.emplace_back(irs::CostAttr::extract(*sequential_docs));

    while (sequential_docs->next()) {
      auto stateless_random_docs = prepared.Execute(i);
      ASSERT_NE(nullptr, stateless_random_docs);
      ASSERT_EQ(sequential_docs->value(),
                random_docs->seek(sequential_docs->value()));
      ASSERT_EQ(sequential_docs->value(), random_docs->value());
      ASSERT_EQ(sequential_docs->value(),
                random_docs->seek(sequential_docs->value()));
      ASSERT_EQ(sequential_docs->value(), random_docs->value());
      ASSERT_EQ(sequential_docs->value(),
                stateless_random_docs->seek(sequential_docs->value()));
      ASSERT_EQ(sequential_docs->value(), stateless_random_docs->value());
      ASSERT_EQ(sequential_docs->value(),
                stateless_random_docs->seek(sequential_docs->value()));
      ASSERT_EQ(sequential_docs->value(), stateless_random_docs->value());

      result.push_back(sequential_docs->value());
    }
    ASSERT_FALSE(sequential_docs->next());
    ASSERT_FALSE(random_docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(sequential_docs->value()));

    // seek to eof
    ASSERT_TRUE(
      irs::doc_limits::eof(prepared.Execute(i)->seek(irs::doc_limits::eof())));
  }
}

void FilterTestCaseBase::GetQueryResult(const PreparedFilter& prepared,
                                        const irs::IndexReader& rdr,
                                        ScoredDocs& result, Costs& result_costs,
                                        std::string_view source_location) {
  SCOPED_TRACE(source_location);
  const auto* scorer = prepared.Scorer();
  result_costs.reserve(rdr.size());

  for (size_t i = 0; const auto& sub : rdr) {
    auto random_docs = prepared.Execute(i);
    ASSERT_NE(nullptr, random_docs);
    auto random_score = random_docs->PrepareScore({
      .scorer = scorer,
      .segment = &sub,
    });
    auto sequential_docs = prepared.Execute(i);
    ASSERT_NE(nullptr, sequential_docs);

    auto score = sequential_docs->PrepareScore({
      .scorer = scorer,
      .segment = &sub,
    });

    result_costs.emplace_back(irs::CostAttr::extract(*sequential_docs));

    while (sequential_docs->next()) {
      auto stateless_random_docs = prepared.Execute(i);
      auto stateless_random_score = stateless_random_docs->PrepareScore({
        .scorer = scorer,
        .segment = &sub,
      });

      ASSERT_NE(nullptr, stateless_random_docs);
      ASSERT_EQ(sequential_docs->value(),
                random_docs->seek(sequential_docs->value()));
      ASSERT_EQ(sequential_docs->value(),
                random_docs->seek(sequential_docs->value()));
      ASSERT_EQ(sequential_docs->value(), random_docs->value());
      ASSERT_EQ(sequential_docs->value(),
                stateless_random_docs->seek(sequential_docs->value()));
      ASSERT_EQ(sequential_docs->value(),
                stateless_random_docs->seek(sequential_docs->value()));
      ASSERT_EQ(sequential_docs->value(), stateless_random_docs->value());

      sequential_docs->FetchScoreArgs(0);
      stateless_random_docs->FetchScoreArgs(0);
      random_docs->FetchScoreArgs(0);

      irs::score_t score_value{-1};
      score.Score(&score_value, 1);
      irs::score_t stateless_score_value{-2};
      stateless_random_score.Score(&stateless_score_value, 1);
      irs::score_t random_score_value{-3};
      random_score.Score(&random_score_value, 1);
      ASSERT_EQ(score_value, stateless_score_value);
      ASSERT_EQ(score_value, random_score_value);

      result.emplace_back(sequential_docs->value(),
                          std::vector<irs::score_t>{score_value});
    }
    ASSERT_FALSE(sequential_docs->next());
    ASSERT_FALSE(random_docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(sequential_docs->value()));

    // seek to eof
    ASSERT_TRUE(
      irs::doc_limits::eof(prepared.Execute(i)->seek(irs::doc_limits::eof())));
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
          ASSERT_EQ(expected, it.seek(action.target));
        } else if constexpr (std::is_same_v<A, Next>) {
          ASSERT_EQ(!irs::doc_limits::eof(expected), it.next());
        } else if constexpr (std::is_same_v<A, Skip>) {
          for (auto count = action.count; count; --count) {
            it.next();
          }
        }
      },
      test.action);
    ASSERT_EQ(test.expected, it.value());
    if (!irs::doc_limits::eof(test.expected)) {
      it.FetchScoreArgs(0);
      assert_equal_scores(test.score, score);
    }
  };

  auto test = std::begin(tests);
  for (size_t i = 0; const auto& sub : rdr) {
    ASSERT_NE(test, std::end(tests));
    auto random_docs = prepared.Execute(i);
    ASSERT_NE(nullptr, random_docs);

    auto random_score = scorer == nullptr ? irs::ScoreFunction{}
                                          : random_docs->PrepareScore({
                                              .scorer = scorer,
                                              .segment = &sub,
                                            });

    for (auto& step : *test) {
      assert_iterator(step, *random_docs, random_score);
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

  for (size_t i = 0; const auto& sub : rdr) {
    fetcher.Clear();
    auto docs = prepared.Execute(i);

    irs::ScoreFunction score;
    if (score_must_be_present) {
      score = docs->PrepareScore({
        .scorer = scorer,
        .segment = &sub,
        .fetcher = &fetcher,
      });
    }

    irs::score_t score_value{};

    while (docs->next()) {
      docs->FetchScoreArgs(0);
      fetcher.Fetch(docs->value());
      score.Score(&score_value, 1);
      scored_result.emplace(score_value, docs->value());
    }
    ASSERT_FALSE(docs->next());
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
