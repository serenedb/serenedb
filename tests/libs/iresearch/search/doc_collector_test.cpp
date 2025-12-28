////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <absl/algorithm/container.h>

#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/doc_collector.hpp>
#include <iresearch/search/score.hpp>
#include <iresearch/search/scorers.hpp>
#include <iresearch/search/term_filter.hpp>

#include "index/index_tests.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests;

struct DocIdScorer : irs::ScorerBase<void> {
  irs::IndexFeatures GetIndexFeatures() const final {
    return irs::IndexFeatures::None;
  }

  irs::ScoreFunction PrepareScorer(const irs::ColumnProvider&,
                                   const irs::FieldProperties&,
                                   const irs::byte_type*,
                                   const irs::AttributeProvider& attrs,
                                   irs::score_t) const final {
    struct ScorerContext final : irs::ScoreCtx {
      explicit ScorerContext(const irs::DocAttr* doc) noexcept : doc{doc} {}

      const irs::DocAttr* doc;
    };

    auto* doc = irs::get<irs::DocAttr>(attrs);
    EXPECT_NE(nullptr, doc);

    return irs::ScoreFunction::Make<ScorerContext>(
      [](irs::ScoreCtx* ctx, irs::score_t* res) noexcept {
        ASSERT_NE(nullptr, res);
        ASSERT_NE(nullptr, ctx);
        const auto& state = *static_cast<ScorerContext*>(ctx);
        *res = state.doc->value;
      },
      irs::ScoreFunction::DefaultMin, doc);
  }
};

auto constexpr kScoreDescending = [](const auto& lhs,
                                     const auto& rhs) noexcept {
  return lhs.first > rhs.first;
};

class DocCollectorTestCase : public IndexTestBase {};

TEST_P(DocCollectorTestCase, test_execute_topk_basic) {
  // Create index with documents
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  DocIdScorer scorer;
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  auto& segment = *reader.begin();
  auto total_docs = segment.docs_count();

  // Test basic top-k retrieval with All filter
  {
    irs::All filter;

    std::vector<std::pair<irs::score_t, irs::doc_id_t>> results;
    size_t count = irs::ExecuteTopK(reader, filter, prepared_order, 5, results);

    ASSERT_EQ(total_docs, count);
    ASSERT_EQ(5, results.size());
    ASSERT_TRUE(absl::c_is_sorted(results, kScoreDescending));
    // With DocIdScorer, score equals doc_id
    for (size_t i = 0; i < results.size(); ++i) {
      ASSERT_EQ(results[i].first, results[i].second);
    }
  }
}

TEST_P(DocCollectorTestCase, test_execute_topk_larger_k) {
  // Create index with documents
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  DocIdScorer scorer;
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  auto& segment = *reader.begin();
  auto total_docs = segment.docs_count();

  // Test with k larger than matching documents
  {
    irs::All filter;

    std::vector<std::pair<irs::score_t, irs::doc_id_t>> results;
    size_t count =
      irs::ExecuteTopK(reader, filter, prepared_order, 1000, results);

    ASSERT_EQ(total_docs, count);
    ASSERT_EQ(total_docs, results.size());
    ASSERT_TRUE(absl::c_is_sorted(results, kScoreDescending));
  }
}

TEST_P(DocCollectorTestCase, test_execute_topk_empty_results) {
  // Create index with documents
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  DocIdScorer scorer;
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());

  // Test with non-matching filter
  {
    irs::ByTerm filter;
    *filter.mutable_field() = "name";
    filter.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("nonexistent_term_xyz"));

    std::vector<std::pair<irs::score_t, irs::doc_id_t>> results;
    size_t count =
      irs::ExecuteTopK(reader, filter, prepared_order, 10, results);

    ASSERT_EQ(0, count);
    ASSERT_TRUE(results.empty());
  }
}

TEST_P(DocCollectorTestCase, test_execute_topk_all_filter) {
  // Create index with documents
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  DocIdScorer scorer;
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  auto& segment = *reader.begin();
  auto total_docs = segment.docs_count();

  // Test with All filter
  {
    irs::All filter;

    std::vector<std::pair<irs::score_t, irs::doc_id_t>> results;
    size_t count =
      irs::ExecuteTopK(reader, filter, prepared_order, 10, results);

    ASSERT_EQ(total_docs, count);
    ASSERT_EQ(10, results.size());
    ASSERT_TRUE(absl::c_is_sorted(results, kScoreDescending));
  }
}

TEST_P(DocCollectorTestCase, test_execute_topk_multi_segment) {
  // Create index with multiple segments
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    auto writer = open_writer(irs::kOmCreate);
    const Document* doc;

    // Add first segment (even docs)
    {
      gen.reset();
      while ((doc = gen.next())) {
        ASSERT_TRUE(Insert(*writer, doc->indexed.begin(), doc->indexed.end(),
                           doc->stored.begin(), doc->stored.end()));
        gen.next();  // skip 1 doc
      }
      writer->Commit();
      AssertSnapshotEquality(*writer);
    }

    // Add second segment (odd docs)
    {
      gen.reset();
      gen.next();  // skip 1 doc
      while ((doc = gen.next())) {
        ASSERT_TRUE(Insert(*writer, doc->indexed.begin(), doc->indexed.end(),
                           doc->stored.begin(), doc->stored.end()));
        gen.next();  // skip 1 doc
      }
      writer->Commit();
      AssertSnapshotEquality(*writer);
    }
  }

  DocIdScorer scorer;
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(2, reader.size());

  size_t total_docs = 0;
  for (auto& segment : reader) {
    total_docs += segment.docs_count();
  }

  // Test across multiple segments
  {
    irs::All filter;

    std::vector<std::pair<irs::score_t, irs::doc_id_t>> results;
    size_t count = irs::ExecuteTopK(reader, filter, prepared_order, 5, results);

    ASSERT_EQ(total_docs, count);
    ASSERT_EQ(5, results.size());
    // Results should be sorted by score descending (may have equal scores
    // from different segments since doc_ids restart per segment)
    ASSERT_TRUE(absl::c_is_sorted(results, kScoreDescending));
  }
}

TEST_P(DocCollectorTestCase, test_execute_topk_term_filter) {
  // Create index with documents
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  DocIdScorer scorer;
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());

  // Test with term filter
  {
    irs::ByTerm filter;
    *filter.mutable_field() = "prefix";
    filter.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("abcd"));

    std::vector<std::pair<irs::score_t, irs::doc_id_t>> results;
    size_t count = irs::ExecuteTopK(reader, filter, prepared_order, 3, results);

    ASSERT_GT(count, 0);
    ASSERT_LE(results.size(), 3);
    ASSERT_TRUE(absl::c_is_sorted(results, kScoreDescending));
  }
}

TEST_P(DocCollectorTestCase, test_execute_topk_disjunction) {
  // Create index with documents
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  DocIdScorer scorer;
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());

  // Test with disjunction filter (OR)
  {
    irs::Or filter;
    {
      auto& sub = filter.add<irs::ByTerm>();
      *sub.mutable_field() = "prefix";
      sub.mutable_options()->term =
        irs::ViewCast<irs::byte_type>(std::string_view("abcd"));
    }
    {
      auto& sub = filter.add<irs::ByTerm>();
      *sub.mutable_field() = "prefix";
      sub.mutable_options()->term =
        irs::ViewCast<irs::byte_type>(std::string_view("abcde"));
    }

    std::vector<std::pair<irs::score_t, irs::doc_id_t>> results;
    size_t count = irs::ExecuteTopK(reader, filter, prepared_order, 5, results);

    ASSERT_GT(count, 0);
    ASSERT_LE(results.size(), 5);
    ASSERT_TRUE(absl::c_is_sorted(results, kScoreDescending));
  }
}

TEST_P(DocCollectorTestCase, test_execute_topk_k_equals_one) {
  // Create index with documents
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  DocIdScorer scorer;
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  auto& segment = *reader.begin();
  auto total_docs = segment.docs_count();

  // Test with k=1
  {
    irs::All filter;

    std::vector<std::pair<irs::score_t, irs::doc_id_t>> results;
    size_t count = irs::ExecuteTopK(reader, filter, prepared_order, 1, results);

    ASSERT_EQ(total_docs, count);
    ASSERT_EQ(1, results.size());
    // The single result should have score equal to doc_id (highest doc_id)
    ASSERT_EQ(results[0].first, results[0].second);
    ASSERT_EQ(total_docs, results[0].second);
  }
}

TEST_P(DocCollectorTestCase, test_execute_topk_verifies_top_docs) {
  // Create index with documents
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  DocIdScorer scorer;
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  auto& segment = *reader.begin();
  auto total_docs = segment.docs_count();

  // Test that top-k returns the highest scoring documents
  {
    irs::All filter;

    std::vector<std::pair<irs::score_t, irs::doc_id_t>> results;
    size_t count = irs::ExecuteTopK(reader, filter, prepared_order, 3, results);

    ASSERT_EQ(total_docs, count);
    ASSERT_EQ(3, results.size());
    ASSERT_TRUE(absl::c_is_sorted(results, kScoreDescending));

    // With DocIdScorer, top 3 should be docs with highest doc_ids
    // Doc IDs start from 1, so for N docs, top 3 are N, N-1, N-2
    ASSERT_EQ(total_docs, results[0].second);
    ASSERT_EQ(total_docs - 1, results[1].second);
    ASSERT_EQ(total_docs - 2, results[2].second);
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(doc_collector_test, DocCollectorTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5")),
                         DocCollectorTestCase::to_string);

}  // namespace
