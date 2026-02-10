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

void FilterTestCaseBase::GetQueryResult(const irs::Filter::Query::ptr& q,
                                        const irs::IndexReader& rdr,
                                        Docs& result, Costs& result_costs,
                                        std::string_view source_location) {
  SCOPED_TRACE(source_location);
  result_costs.reserve(rdr.size());

  for (const auto& sub : rdr) {
    auto random_docs = q->execute({.segment = sub});
    ASSERT_NE(nullptr, random_docs);
    auto sequential_docs = q->execute({.segment = sub});
    ASSERT_NE(nullptr, sequential_docs);

    auto* doc = irs::get<irs::DocAttr>(*sequential_docs);
    ASSERT_NE(nullptr, doc);

    result_costs.emplace_back(irs::CostAttr::extract(*sequential_docs));

    while (sequential_docs->next()) {
      auto stateless_random_docs = q->execute({.segment = sub});
      ASSERT_NE(nullptr, stateless_random_docs);
      ASSERT_EQ(sequential_docs->value(), doc->value);
      ASSERT_EQ(doc->value, random_docs->seek(doc->value));
      ASSERT_EQ(doc->value, random_docs->value());
      ASSERT_EQ(doc->value, random_docs->seek(doc->value));
      ASSERT_EQ(doc->value, random_docs->value());
      ASSERT_EQ(doc->value, stateless_random_docs->seek(doc->value));
      ASSERT_EQ(doc->value, stateless_random_docs->value());
      ASSERT_EQ(doc->value, stateless_random_docs->seek(doc->value));
      ASSERT_EQ(doc->value, stateless_random_docs->value());

      result.push_back(sequential_docs->value());
    }
    ASSERT_FALSE(sequential_docs->next());
    ASSERT_FALSE(random_docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(sequential_docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(doc->value));

    // seek to eof
    ASSERT_TRUE(irs::doc_limits::eof(
      q->execute({.segment = sub})->seek(irs::doc_limits::eof())));
  }
}

void FilterTestCaseBase::GetQueryResult(const irs::Filter::Query::ptr& q,
                                        const irs::IndexReader& rdr,
                                        const irs::Scorers& ord,
                                        ScoredDocs& result, Costs& result_costs,
                                        std::string_view source_location) {
  SCOPED_TRACE(source_location);
  result_costs.reserve(rdr.size());

  for (const auto& sub : rdr) {
    auto random_docs = q->execute({.segment = sub, .scorers = ord});
    ASSERT_NE(nullptr, random_docs);
    auto random_score = random_docs->PrepareScore({
      .scorer = ord.buckets().front().bucket,
      .segment = &sub,
    });
    auto sequential_docs = q->execute({.segment = sub, .scorers = ord});
    ASSERT_NE(nullptr, sequential_docs);

    auto* doc = irs::get<irs::DocAttr>(*sequential_docs);
    ASSERT_NE(nullptr, doc);

    auto score = sequential_docs->PrepareScore({
      .scorer = ord.buckets().front().bucket,
      .segment = &sub,
    });

    result_costs.emplace_back(irs::CostAttr::extract(*sequential_docs));

    while (sequential_docs->next()) {
      auto stateless_random_docs = q->execute({.segment = sub, .scorers = ord});
      auto stateless_random_score = stateless_random_docs->PrepareScore({
        .scorer = ord.buckets().front().bucket,
        .segment = &sub,
      });

      ASSERT_NE(nullptr, stateless_random_docs);
      ASSERT_EQ(sequential_docs->value(), doc->value);
      ASSERT_EQ(doc->value, random_docs->seek(doc->value));
      ASSERT_EQ(doc->value, random_docs->seek(doc->value));
      ASSERT_EQ(doc->value, random_docs->value());
      ASSERT_EQ(doc->value, stateless_random_docs->seek(doc->value));
      ASSERT_EQ(doc->value, stateless_random_docs->seek(doc->value));
      ASSERT_EQ(doc->value, stateless_random_docs->value());

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
    ASSERT_TRUE(irs::doc_limits::eof(doc->value));

    // seek to eof
    ASSERT_TRUE(irs::doc_limits::eof(
      q->execute({.segment = sub})->seek(irs::doc_limits::eof())));
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
  GetQueryResult(filter.prepare({.index = index}), index, result, result_costs,
                 source_location);
  ASSERT_EQ(expected, result);
  ASSERT_EQ(expected_costs, result_costs);
}

void FilterTestCaseBase::CheckQuery(const irs::Filter& filter,
                                    std::span<const irs::Scorer::ptr> order,
                                    const std::vector<Tests>& tests,
                                    const irs::IndexReader& rdr,
                                    std::string_view source_location) {
  SCOPED_TRACE(source_location);
  auto ord = irs::Scorers::Prepare(order);
  auto q = filter.prepare({.index = rdr, .scorers = ord});
  ASSERT_NE(nullptr, q);

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
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_NE(nullptr, doc);
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
    ASSERT_EQ(test.expected, doc->value);
    if (!irs::doc_limits::eof(test.expected)) {
      it.FetchScoreArgs(0);
      assert_equal_scores(test.score, score);
    }
  };

  auto test = std::begin(tests);
  for (const auto& sub : rdr) {
    ASSERT_NE(test, std::end(tests));
    auto random_docs = q->execute({.segment = sub, .scorers = ord});
    ASSERT_NE(nullptr, random_docs);

    auto random_score = ord.buckets().empty()
                          ? irs::ScoreFunction{}
                          : random_docs->PrepareScore({
                              .scorer = ord.buckets().front().bucket,
                              .segment = &sub,
                            });

    for (auto& test : *test) {
      assert_iterator(test, *random_docs, random_score);
    }

    ++test;
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
  auto prepared = irs::Scorers::Prepare(order);
  GetQueryResult(filter.prepare({.index = index, .scorers = prepared}), index,
                 prepared, result, result_costs, source_location);
  ASSERT_EQ(expected, result);
}

void FilterTestCaseBase::CheckQuery(const irs::Filter& filter,
                                    const Docs& expected,
                                    const irs::IndexReader& index,
                                    std::string_view source_location) {
  SCOPED_TRACE(source_location);
  Docs result;
  Costs result_costs;
  GetQueryResult(filter.prepare({.index = index}), index, result, result_costs,
                 source_location);
  ASSERT_EQ(expected, result);
}

void FilterTestCaseBase::MakeResult(const irs::Filter& filter,
                                    std::span<const irs::Scorer::ptr> order,
                                    const irs::IndexReader& rdr,
                                    std::vector<irs::doc_id_t>& result,
                                    bool score_must_be_present, bool reverse) {
  auto prepared_order = irs::Scorers::Prepare(order);
  auto prepared_filter =
    filter.prepare({.index = rdr, .scorers = prepared_order});
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
  irs::ColumnCollector columns;

  for (const auto& sub : rdr) {
    columns.Clear();
    auto docs = prepared_filter->execute({
      .segment = sub,
      .scorers = prepared_order,
    });

    auto* doc = irs::get<irs::DocAttr>(*docs);
    // ensure all iterators contain "document" attribute
    ASSERT_TRUE(bool(doc));

    irs::ScoreFunction score;
    if (score_must_be_present) {
      score = docs->PrepareScore({
        .scorer = prepared_order.buckets().front().bucket,
        .segment = &sub,
        .collector = &columns,
      });
    }

    irs::score_t score_value{};

    while (docs->next()) {
      ASSERT_EQ(docs->value(), doc->value);
      docs->FetchScoreArgs(0);
      columns.Collect(docs->value());
      score.Score(&score_value, 1);
      scored_result.emplace(score_value, docs->value());
    }
    ASSERT_FALSE(docs->next());
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
