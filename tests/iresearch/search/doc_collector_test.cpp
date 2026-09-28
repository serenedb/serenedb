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

#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/search/count/root.hpp>
#include <iresearch/search/detail/doc_collector.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/search/queries/docs_mask_query.hpp>
#include <iresearch/search/scorers/score_function.hpp>
#include <iresearch/search/scorers/scorer.hpp>
#include <iresearch/types.hpp>
#include <span>

#include "index/index_tests.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests;

// Scorer that returns doc_id as score, optionally with modulo divisor.
// When divisor is 0, returns doc_id directly. Otherwise returns doc_id %
// divisor.
struct DocIdScorer : irs::ScorerBase<void> {
  explicit DocIdScorer(irs::doc_id_t divisor = 0) noexcept : divisor{divisor} {}

  irs::IndexFeatures GetIndexFeatures() const final {
    return irs::IndexFeatures::Freq;
  }

  struct ScorerContext : irs::ScoreOperator {
    explicit ScorerContext(irs::doc_id_t divisor) noexcept : divisor{divisor} {}

    template<irs::ScoreMergeType MergeType = irs::ScoreMergeType::Noop>
    void ScoreImpl(irs::score_t* res, irs::scores_size_t n) const noexcept {
      ASSERT_NE(nullptr, res);
      for (size_t i = 0; i < n; ++i) {
        auto doc_id = next_doc++;
        irs::Merge<MergeType>(
          res[i], divisor == 0 ? static_cast<irs::score_t>(doc_id)
                               : static_cast<irs::score_t>(doc_id % divisor));
      }
    }

    void Score(irs::score_t* res, irs::scores_size_t n) const noexcept final {
      ScoreImpl(res, n);
    }
    void ScoreSum(irs::score_t* res,
                  irs::scores_size_t n) const noexcept final {
      ScoreImpl<irs::ScoreMergeType::Sum>(res, n);
    }
    void ScoreMax(irs::score_t* res,
                  irs::scores_size_t n) const noexcept final {
      ScoreImpl<irs::ScoreMergeType::Max>(res, n);
    }

    irs::doc_id_t divisor;
    mutable irs::doc_id_t next_doc{irs::doc_limits::min()};
  };

  irs::ScoreFunction PrepareScorer(const irs::ScoreContext&) const final {
    return irs::ScoreFunction::Make<ScorerContext>(divisor);
  }

  irs::doc_id_t divisor;
};

constexpr auto kScoreDescending = [](const auto& l, const auto& r) noexcept {
  return l.score > r.score;
};

constexpr irs::field_id kNameFieldId = 1;
constexpr irs::field_id kPrefixFieldId = 2;
constexpr irs::field_id kSeqFieldId = 3;
constexpr irs::field_id kSameFieldId = 4;
constexpr irs::field_id kValueFieldId = 5;
constexpr irs::field_id kDuplicatedFieldId = 6;

// Wrap tests::GenericJsonFieldFactory to assign per-name field ids to the
// freshly inserted indexed field. The factory itself is shared with other
// tests, so we cannot modify it directly.
auto WrapFactory = [](tests::Document& doc, const std::string& name,
                      const tests::JsonDocGenerator::JsonValue& data) {
  const auto before = doc.indexed.size();
  tests::GenericJsonFieldFactory(doc, name, data);
  if (doc.indexed.size() == before) {
    return;
  }
  auto& f = doc.indexed.back<tests::FieldBase>();
  if (name == "name") {
    f.id = kNameFieldId;
  } else if (name == "prefix") {
    f.id = kPrefixFieldId;
  } else if (name == "seq") {
    f.id = kSeqFieldId;
  } else if (name == "same") {
    f.id = kSameFieldId;
  } else if (name == "value") {
    f.id = kValueFieldId;
  } else if (name == "duplicated") {
    f.id = kDuplicatedFieldId;
  }
};

constexpr irs::field_id kFreqFieldId = 7;

auto FreqFactory = [](tests::Document& doc, const std::string& name,
                      const tests::JsonDocGenerator::JsonValue& data) {
  tests::GenericJsonFieldFactory(doc, name, data);
  if (name != "seq" || !data.is_number()) {
    return;
  }
  auto field =
    std::make_shared<tests::TextField<std::string>>("freq", std::string{"tok"});
  field->id = kFreqFieldId;
  doc.insert(std::move(field));
};

class DocCollectorTestCase : public IndexTestBase {};

TEST_P(DocCollectorTestCase, test_execute_topk_basic) {
  // Create index with documents
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                FreqFactory);
    add_segment(gen);
  }

  DocIdScorer scorer;

  auto reader =
    irs::DirectoryReader(dir(), codec(), tests::CsDefaultReaderOptions());
  auto& segment = *reader.begin();
  auto total_docs = segment.docs_count();

  // Test basic top-k retrieval with All filter
  {
    irs::ByTerm filter;
    *filter.mutable_field_id() = kFreqFieldId;
    filter.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("tok"));
    constexpr size_t k = 5;

    std::vector<irs::ScoreDoc> results(k);
    size_t count =
      irs::ExecuteTopK(reader, filter, scorer, k, false, std::span{results});

    ASSERT_EQ(total_docs, count);
    auto result_count = std::min(count, k);
    ASSERT_EQ(5, result_count);
    ASSERT_TRUE(absl::c_is_sorted(std::span{results}.first(result_count),
                                  kScoreDescending));
    // With DocIdScorer, score equals doc_id
    for (size_t i = 0; i < result_count; ++i) {
      ASSERT_EQ(results[i].doc, results[i].score);
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

  auto reader =
    irs::DirectoryReader(dir(), codec(), tests::CsDefaultReaderOptions());
  auto& segment = *reader.begin();
  auto total_docs = segment.docs_count();

  // Test with k larger than matching documents
  {
    irs::All filter;
    constexpr size_t k = 1000;

    std::vector<irs::ScoreDoc> results(k);
    size_t count =
      irs::ExecuteTopK(reader, filter, scorer, k, false, std::span{results});

    ASSERT_EQ(total_docs, count);
    auto result_count = std::min(count, k);
    ASSERT_EQ(total_docs, result_count);
    ASSERT_TRUE(absl::c_is_sorted(std::span{results}.first(result_count),
                                  kScoreDescending));
  }
}

TEST_P(DocCollectorTestCase, test_execute_topk_empty_results) {
  // Create index with documents; WrapFactory pins indexed `name` to
  // `kNameFieldId` so the negative-match filter below targets the same id
  // the docs were indexed under.
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                WrapFactory);
    add_segment(gen);
  }

  DocIdScorer scorer;

  auto reader =
    irs::DirectoryReader(dir(), codec(), tests::CsDefaultReaderOptions());

  // Test with non-matching filter
  {
    irs::ByTerm filter;
    *filter.mutable_field_id() = kNameFieldId;
    filter.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("nonexistent_term_xyz"));
    constexpr size_t k = 10;

    std::vector<irs::ScoreDoc> results(k);
    size_t count =
      irs::ExecuteTopK(reader, filter, scorer, k, false, std::span{results});

    ASSERT_EQ(0, count);
    ASSERT_EQ(0, std::min(count, k));
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

  auto reader =
    irs::DirectoryReader(dir(), codec(), tests::CsDefaultReaderOptions());
  auto& segment = *reader.begin();
  auto total_docs = segment.docs_count();

  // Test with All filter
  {
    irs::All filter;
    constexpr size_t k = 10;

    std::vector<irs::ScoreDoc> results(k);
    size_t count =
      irs::ExecuteTopK(reader, filter, scorer, k, false, std::span{results});

    ASSERT_EQ(total_docs, count);
    auto result_count = std::min(count, k);
    ASSERT_EQ(10, result_count);
    ASSERT_TRUE(absl::c_is_sorted(std::span{results}.first(result_count),
                                  kScoreDescending));
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
        ASSERT_TRUE(Insert(*writer, doc->indexed.begin(), doc->indexed.end()));
        gen.next();  // skip 1 doc
      }
      writer->RefreshCommit();
      AssertSnapshotEquality(*writer);
    }

    // Add second segment (odd docs)
    {
      gen.reset();
      gen.next();  // skip 1 doc
      while ((doc = gen.next())) {
        ASSERT_TRUE(Insert(*writer, doc->indexed.begin(), doc->indexed.end()));
        gen.next();  // skip 1 doc
      }
      writer->RefreshCommit();
      AssertSnapshotEquality(*writer);
    }
  }

  DocIdScorer scorer;

  auto reader =
    irs::DirectoryReader(dir(), codec(), tests::CsDefaultReaderOptions());
  ASSERT_EQ(2, reader.size());

  size_t total_docs = 0;
  for (auto& segment : reader) {
    total_docs += segment.docs_count();
  }

  // Test across multiple segments
  {
    irs::All filter;
    constexpr size_t k = 5;

    std::vector<irs::ScoreDoc> results(k);
    size_t count =
      irs::ExecuteTopK(reader, filter, scorer, k, false, std::span{results});

    ASSERT_EQ(total_docs, count);
    auto result_count = std::min(count, k);
    ASSERT_EQ(5, result_count);
    // Results should be sorted by score descending (may have equal scores
    // from different segments since doc_ids restart per segment)
    ASSERT_TRUE(absl::c_is_sorted(std::span{results}.first(result_count),
                                  kScoreDescending));
  }
}

TEST_P(DocCollectorTestCase, test_execute_topk_term_filter) {
  // Create index with documents
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                WrapFactory);
    add_segment(gen);
  }

  DocIdScorer scorer;

  auto reader =
    irs::DirectoryReader(dir(), codec(), tests::CsDefaultReaderOptions());

  // Test with term filter
  {
    irs::ByTerm filter;
    *filter.mutable_field_id() = kPrefixFieldId;
    filter.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("abcd"));
    constexpr size_t k = 3;

    std::vector<irs::ScoreDoc> results(k);
    size_t count =
      irs::ExecuteTopK(reader, filter, scorer, k, false, std::span{results});

    ASSERT_GT(count, 0);
    auto result_count = std::min(count, k);
    ASSERT_LE(result_count, 3);
    ASSERT_TRUE(absl::c_is_sorted(std::span{results}.first(result_count),
                                  kScoreDescending));
  }
}

TEST_P(DocCollectorTestCase, test_execute_topk_skips_deleted) {
  auto writer = open_writer(irs::kOmCreate);
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                WrapFactory);
    const Document* doc;
    while ((doc = gen.next())) {
      ASSERT_TRUE(Insert(*writer, doc->indexed.begin(), doc->indexed.end()));
    }
    writer->RefreshCommit();
  }

  size_t before = 0;
  {
    auto reader =
      irs::DirectoryReader(dir(), codec(), tests::CsDefaultReaderOptions());
    for (auto& segment : reader) {
      before += segment.docs_count();
    }
  }
  ASSERT_GT(before, 3);

  constexpr std::string_view kRemoved[]{"A", "B", "C"};
  for (const auto name : kRemoved) {
    auto trx = writer->GetBatch();
    auto removal = std::make_unique<irs::ByTerm>();
    *removal->mutable_field_id() = kNameFieldId;
    removal->mutable_options()->term = irs::ViewCast<irs::byte_type>(name);
    trx.Remove(irs::Filter::ptr{std::move(removal)});
    trx.Commit();
  }
  writer->RefreshCommit();

  auto reader =
    irs::DirectoryReader(dir(), codec(), tests::CsDefaultReaderOptions());
  size_t live = 0;
  for (auto& segment : reader) {
    live += segment.live_docs_count();
  }
  ASSERT_EQ(before - std::size(kRemoved), live);

  DocIdScorer scorer;
  irs::All filter;
  const size_t k = live + 8;

  std::vector<irs::ScoreDoc> results(k);
  const size_t count =
    irs::ExecuteTopK(reader, filter, scorer, k, false, std::span{results});

  ASSERT_EQ(live, count);
  for (size_t i = 0; i != std::min(count, k); ++i) {
    auto masked = reader[results[i].segment_idx].MaskedDocs();
    ASSERT_FALSE(masked.Contains(results[i].doc))
      << "deleted doc " << results[i].doc << " reached the top-k";
  }
}

TEST_P(DocCollectorTestCase, test_lead_all_walks_live_docs) {
  auto writer = open_writer(irs::kOmCreate);
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                WrapFactory);
    const Document* doc;
    while ((doc = gen.next())) {
      ASSERT_TRUE(Insert(*writer, doc->indexed.begin(), doc->indexed.end()));
    }
    writer->RefreshCommit();
  }

  constexpr std::string_view kRemoved[]{"A", "C", "D", "Q"};
  for (const auto name : kRemoved) {
    auto trx = writer->GetBatch();
    auto removal = std::make_unique<irs::ByTerm>();
    *removal->mutable_field_id() = kNameFieldId;
    removal->mutable_options()->term = irs::ViewCast<irs::byte_type>(name);
    trx.Remove(irs::Filter::ptr{std::move(removal)});
    trx.Commit();
  }
  writer->RefreshCommit();

  auto reader =
    irs::DirectoryReader(dir(), codec(), tests::CsDefaultReaderOptions());
  irs::All filter;
  size_t walked = 0;
  for (auto& segment : reader) {
    ASSERT_LT(segment.live_docs_count(), segment.docs_count());
    auto query = irs::PrepareMasked(filter, segment, {});
    ASSERT_NE(nullptr, query);
    auto lead = query->PlanLead({});
    ASSERT_NE(nullptr, lead);

    std::vector<irs::doc_id_t> expected;
    auto live = segment.docs_iterator();
    for (auto doc = live->Next(); !irs::doc_limits::eof(doc);
         doc = live->Next()) {
      expected.push_back(doc);
    }
    std::vector<irs::doc_id_t> actual;
    for (auto doc = lead->Next(); !irs::doc_limits::eof(doc);
         doc = lead->Next()) {
      actual.push_back(doc);
    }
    ASSERT_EQ(expected, actual);
    ASSERT_EQ(segment.live_docs_count(), actual.size());
    walked += actual.size();
  }
  ASSERT_GT(walked, 0);
}

TEST_P(DocCollectorTestCase, test_count_negation_skips_deleted) {
  auto writer = open_writer(irs::kOmCreate);
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                WrapFactory);
    const Document* doc;
    while ((doc = gen.next())) {
      ASSERT_TRUE(Insert(*writer, doc->indexed.begin(), doc->indexed.end()));
    }
    writer->RefreshCommit();
  }

  auto by_name = [](std::string_view name) {
    auto filter = std::make_unique<irs::ByTerm>();
    *filter->mutable_field_id() = kNameFieldId;
    filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(name);
    return filter;
  };

  constexpr std::string_view kRemoved[]{"A", "C", "D", "Q"};
  for (const auto name : kRemoved) {
    auto trx = writer->GetBatch();
    trx.Remove(irs::Filter::ptr{by_name(name)});
    trx.Commit();
  }
  writer->RefreshCommit();

  auto reader =
    irs::DirectoryReader(dir(), codec(), tests::CsDefaultReaderOptions());
  const std::vector<std::vector<std::string_view>> excluded{
    {"B"}, {"B", "E"}, {"A", "B"}};
  for (const auto& names : excluded) {
    irs::BooleanFilter filter;
    filter.Add(std::make_unique<irs::All>(), irs::Occur::Must);
    for (const auto name : names) {
      filter.Add(by_name(name), irs::Occur::MustNot);
    }
    size_t counted = 0;
    for (auto& segment : reader) {
      ASSERT_LT(segment.live_docs_count(), segment.docs_count());
      auto query = irs::PrepareMasked(filter, segment, {});
      ASSERT_NE(nullptr, query);

      size_t expected = 0;
      auto lead = query->PlanLead({});
      ASSERT_NE(nullptr, lead);
      while (!irs::doc_limits::eof(lead->Next())) {
        ++expected;
      }

      auto count = query->PlanCount({});
      ASSERT_NE(nullptr, count);
      const auto actual =
        count->Run(irs::doc_limits::min(), irs::doc_limits::eof());
      ASSERT_EQ(expected, actual);
      counted += actual;
    }
    const auto live_excluded =
      static_cast<size_t>(absl::c_count_if(names, [&](auto name) {
        return !absl::c_linear_search(kRemoved, name);
      }));
    ASSERT_EQ(reader.live_docs_count() - live_excluded, counted);
  }
}

TEST_P(DocCollectorTestCase, test_count_all_skips_deleted) {
  auto writer = open_writer(irs::kOmCreate);
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                WrapFactory);
    const Document* doc;
    while ((doc = gen.next())) {
      ASSERT_TRUE(Insert(*writer, doc->indexed.begin(), doc->indexed.end()));
    }
    writer->RefreshCommit();
  }

  constexpr std::string_view kRemoved[]{"A", "C", "D", "Q"};
  for (const auto name : kRemoved) {
    auto trx = writer->GetBatch();
    auto removal = std::make_unique<irs::ByTerm>();
    *removal->mutable_field_id() = kNameFieldId;
    removal->mutable_options()->term = irs::ViewCast<irs::byte_type>(name);
    trx.Remove(irs::Filter::ptr{std::move(removal)});
    trx.Commit();
  }
  writer->RefreshCommit();

  auto reader =
    irs::DirectoryReader(dir(), codec(), tests::CsDefaultReaderOptions());
  irs::All filter;
  size_t counted = 0;
  for (auto& segment : reader) {
    ASSERT_LT(segment.live_docs_count(), segment.docs_count());
    auto query = irs::PrepareMasked(filter, segment, {});
    ASSERT_NE(nullptr, query);

    auto count = query->PlanCount({});
    ASSERT_NE(nullptr, count);
    ASSERT_EQ(segment.live_docs_count(),
              count->Run(irs::doc_limits::min(), irs::doc_limits::eof()));

    auto split = query->PlanCount({.partial = true});
    ASSERT_NE(nullptr, split);
    constexpr irs::doc_id_t kBounds[]{irs::doc_limits::min(), 3, 20,
                                      irs::doc_limits::eof()};
    uint64_t total = 0;
    for (size_t i = 1; i != std::size(kBounds); ++i) {
      total += split->Run(kBounds[i - 1], kBounds[i]);
    }
    total += split->Finish();
    ASSERT_EQ(segment.live_docs_count(), total);
    counted += total;
  }
  ASSERT_EQ(reader.live_docs_count(), counted);
}

TEST_P(DocCollectorTestCase, test_count_split_single_doc_term) {
  constexpr size_t kDocs = 4096;
  constexpr size_t kSingle = 3001;
  {
    auto writer = open_writer(irs::kOmCreate);
    auto field = std::make_shared<tests::StringField>("name");
    field->id = kNameFieldId;
    tests::Document doc;
    doc.insert(field);
    for (size_t i = 0; i != kDocs; ++i) {
      field->value(i == kSingle ? "b" : i % 10 == 0 || i % 10 == 3 ? "c" : "a");
      ASSERT_TRUE(Insert(*writer, doc));
    }
    writer->RefreshCommit();
  }

  auto reader =
    irs::DirectoryReader(dir(), codec(), tests::CsDefaultReaderOptions());
  ASSERT_EQ(1, reader.size());
  const auto& segment = reader[0];

  irs::BooleanFilter filter;
  for (const std::string_view term : {"b", "c"}) {
    filter.Add(
      irs::TermClause{
        .field = kNameFieldId,
        .term = irs::bstring{irs::ViewCast<irs::byte_type>(term)}},
      irs::Occur::Should);
  }
  filter.SetMinShouldMatch(1);
  auto query = irs::PrepareMasked(filter, segment, {});
  ASSERT_NE(nullptr, query);

  const auto mid =
    static_cast<irs::doc_id_t>(irs::doc_limits::min() + kDocs / 2);
  uint64_t expected_front = 0;
  uint64_t expected_back = 0;
  auto lead = query->PlanLead({});
  ASSERT_NE(nullptr, lead);
  for (auto doc = lead->Next(); !irs::doc_limits::eof(doc);
       doc = lead->Next()) {
    ++(doc < mid ? expected_front : expected_back);
  }
  ASSERT_NE(0, expected_back);

  for (const bool partial : {false, true}) {
    auto front = query->PlanCount({.partial = partial});
    ASSERT_NE(nullptr, front);
    auto back = query->PlanCount({.partial = partial});
    ASSERT_NE(nullptr, back);
    const auto front_count =
      front->Run(irs::doc_limits::min(), mid) + front->Finish();
    const auto back_count =
      back->Run(mid, irs::doc_limits::eof()) + back->Finish();
    EXPECT_EQ(expected_front, front_count);
    EXPECT_EQ(expected_back, back_count);
  }
}

TEST_P(DocCollectorTestCase, test_execute_topk_disjunction) {
  // Create index with documents
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                WrapFactory);
    add_segment(gen);
  }

  DocIdScorer scorer;

  auto reader =
    irs::DirectoryReader(dir(), codec(), tests::CsDefaultReaderOptions());

  // Test with disjunction filter (OR)
  {
    irs::BooleanFilter filter;
    filter.Add(
      irs::TermClause{.field = kPrefixFieldId,
                      .term = irs::bstring{irs::ViewCast<irs::byte_type>(
                        std::string_view("abcd"))}},
      irs::Occur::Should);
    filter.Add(
      irs::TermClause{.field = kPrefixFieldId,
                      .term = irs::bstring{irs::ViewCast<irs::byte_type>(
                        std::string_view("abcde"))}},
      irs::Occur::Should);
    filter.SetMinShouldMatch(1);
    constexpr size_t k = 5;

    std::vector<irs::ScoreDoc> results(k);
    size_t count =
      irs::ExecuteTopK(reader, filter, scorer, k, false, std::span{results});

    ASSERT_GT(count, 0);
    auto result_count = std::min(count, k);
    ASSERT_LE(result_count, 5);
    ASSERT_TRUE(absl::c_is_sorted(std::span{results}.first(result_count),
                                  kScoreDescending));
  }
}

TEST_P(DocCollectorTestCase, test_execute_topk_k_equals_one) {
  // Create index with documents
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                FreqFactory);
    add_segment(gen);
  }

  DocIdScorer scorer;

  auto reader =
    irs::DirectoryReader(dir(), codec(), tests::CsDefaultReaderOptions());
  auto& segment = *reader.begin();
  auto total_docs = segment.docs_count();

  // Test with k=1
  {
    irs::ByTerm filter;
    *filter.mutable_field_id() = kFreqFieldId;
    filter.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("tok"));
    constexpr size_t k = 1;

    std::vector<irs::ScoreDoc> results(k);
    size_t count =
      irs::ExecuteTopK(reader, filter, scorer, k, false, std::span{results});

    ASSERT_EQ(total_docs, count);
    auto result_count = std::min(count, k);
    ASSERT_EQ(1, result_count);
    // The single result should have score equal to doc_id (highest doc_id)
    ASSERT_EQ(results[0].doc, results[0].score);
    ASSERT_EQ(total_docs, results[0].doc);
  }
}

TEST_P(DocCollectorTestCase, test_execute_topk_verifies_top_docs) {
  // Create index with documents
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                FreqFactory);
    add_segment(gen);
  }

  DocIdScorer scorer;

  auto reader =
    irs::DirectoryReader(dir(), codec(), tests::CsDefaultReaderOptions());
  auto& segment = *reader.begin();
  auto total_docs = segment.docs_count();

  // Test that top-k returns the highest scoring documents
  {
    irs::ByTerm filter;
    *filter.mutable_field_id() = kFreqFieldId;
    filter.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("tok"));
    constexpr size_t k = 3;

    std::vector<irs::ScoreDoc> results(k);
    size_t count =
      irs::ExecuteTopK(reader, filter, scorer, k, false, std::span{results});

    ASSERT_EQ(total_docs, count);
    auto result_count = std::min(count, k);
    ASSERT_EQ(3, result_count);
    ASSERT_TRUE(absl::c_is_sorted(std::span{results}.first(result_count),
                                  kScoreDescending));

    // With DocIdScorer, top 3 should be docs with highest doc_ids
    // Doc IDs start from 1, so for N docs, top 3 are N, N-1, N-2
    ASSERT_EQ(total_docs, results[0].doc);
    ASSERT_EQ(total_docs - 1, results[1].doc);
    ASSERT_EQ(total_docs - 2, results[2].doc);
  }
}

TEST_P(DocCollectorTestCase, test_execute_topk_similar_scores) {
  // Create index with documents
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                FreqFactory);
    add_segment(gen);
  }

  // Use DocIdScorer with divisor 3, so scores are 0, 1, or 2
  // This creates many documents with identical scores
  DocIdScorer scorer{3};

  auto reader =
    irs::DirectoryReader(dir(), codec(), tests::CsDefaultReaderOptions());
  auto& segment = *reader.begin();
  auto total_docs = segment.docs_count();

  // Test top-k with many duplicate scores
  {
    irs::ByTerm filter;
    *filter.mutable_field_id() = kFreqFieldId;
    filter.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("tok"));
    constexpr size_t k = 5;

    std::vector<irs::ScoreDoc> results(k);
    size_t count =
      irs::ExecuteTopK(reader, filter, scorer, k, false, std::span{results});

    ASSERT_EQ(total_docs, count);
    auto result_count = std::min(count, k);
    ASSERT_EQ(5, result_count);
    // Results should still be sorted by score descending
    ASSERT_TRUE(absl::c_is_sorted(std::span{results}.first(result_count),
                                  kScoreDescending));
    // All top results should have score 2 (the maximum score from mod 3)
    for (size_t i = 0; i < result_count; ++i) {
      ASSERT_EQ(2, results[i].score);
    }
  }

  // Test with k larger than documents with max score
  {
    irs::ByTerm filter;
    *filter.mutable_field_id() = kFreqFieldId;
    filter.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("tok"));
    constexpr size_t k = 10;

    std::vector<irs::ScoreDoc> results(k);
    size_t count =
      irs::ExecuteTopK(reader, filter, scorer, k, false, std::span{results});

    ASSERT_EQ(total_docs, count);
    auto result_count = std::min(count, k);
    ASSERT_EQ(10, result_count);
    ASSERT_TRUE(absl::c_is_sorted(std::span{results}.first(result_count),
                                  kScoreDescending));
    // Verify scores are valid (0, 1, or 2)
    for (size_t i = 0; i < result_count; ++i) {
      ASSERT_GE(results[i].score, 0);
      ASSERT_LE(results[i].score, 2);
    }
  }
}

TEST_P(DocCollectorTestCase, test_execute_topk_all_same_score) {
  // Create index with documents
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  // Use DocIdScorer with divisor 1, so all scores are 0
  DocIdScorer scorer{1};

  auto reader =
    irs::DirectoryReader(dir(), codec(), tests::CsDefaultReaderOptions());
  auto& segment = *reader.begin();
  auto total_docs = segment.docs_count();

  // Test top-k when all documents have identical score
  {
    irs::All filter;
    constexpr size_t k = 5;

    std::vector<irs::ScoreDoc> results(k);
    size_t count =
      irs::ExecuteTopK(reader, filter, scorer, k, false, std::span{results});

    ASSERT_EQ(total_docs, count);
    auto result_count = std::min(count, k);
    ASSERT_EQ(5, result_count);
    // All scores should be 0
    for (size_t i = 0; i < result_count; ++i) {
      ASSERT_EQ(0, results[i].score);
    }
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(doc_collector_test, DocCollectorTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd")),
                         DocCollectorTestCase::to_string);

}  // namespace
