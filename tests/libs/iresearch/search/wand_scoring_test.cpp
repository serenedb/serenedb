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

#include <optional>
#include <ostream>
#include <span>
#include <utility>

template<typename T1, typename T2>
std::ostream& operator<<(std::ostream& os, const std::pair<T1, T2>& p) {
  return os << "(" << p.first << ", " << p.second << ")";
}

#include <duckdb/main/connection.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>

#include "basics/duckdb_engine.h"
#include "index/index_tests.hpp"
#include "iresearch/analysis/analyzer.hpp"
#include "iresearch/analysis/delimited_tokenizer.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/index/table_filter_iterator.hpp"
#include "iresearch/parser/parser.hpp"
#include "iresearch/search/bm25.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/doc_collector.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/tfidf.hpp"
#include "iresearch/types.hpp"
#include "tests_shared.hpp"

namespace {

inline constexpr irs::field_id kContentId = 1;
inline constexpr irs::field_id kTopicId = 2;
inline constexpr irs::field_id kCategoryId = 3;
inline constexpr irs::field_id kTagsId = 4;
inline constexpr irs::field_id kSeqId = 5;

irs::field_id ColumnIdFor(std::string_view name) {
  if (name == "topic") {
    return kTopicId;
  }
  if (name == "category") {
    return kCategoryId;
  }
  if (name == "tags") {
    return kTagsId;
  }
  if (name == "content") {
    return kContentId;
  }
  if (name == "seq") {
    return kSeqId;
  }
  return irs::field_limits::invalid();
}

using namespace tests;

// Top-k executor that wraps the WAND iterator in a TableFilterDocIterator --
// the production row-filter path -- applying a score cutoff: docs with score >=
// `reject_score` are dropped before they reach the collector, so we exercise
// WAND block-max skipping together with a table filter. The filter runs after
// scoring (inside Collect), so WAND is unaware of it and skips purely on the
// collector threshold (the kth passing score), exactly as in production.
uint64_t ExecuteTopKFiltered(const irs::DirectoryReader& reader,
                             const irs::Filter& filter,
                             const irs::Scorer& scorer, size_t k,
                             irs::WandContext wand, irs::score_t reject_score,
                             std::span<irs::ScoreDoc> hits) {
  SDB_ASSERT(irs::BlockSize(k) == hits.size());

  // Score filter `score < reject_score` as an ExpressionFilter over the single
  // score column -- the shape TableFilterDocIterator applies for `is_score`.
  auto cmp = duckdb::BoundComparisonExpression::Create(
    duckdb::ExpressionType::COMPARE_LESSTHAN,
    duckdb::make_uniq<duckdb::BoundReferenceExpression>(
      duckdb::LogicalType::FLOAT, 0),
    duckdb::make_uniq<duckdb::BoundConstantExpression>(
      duckdb::Value::FLOAT(reject_score)));
  duckdb::ExpressionFilter score_filter{std::move(cmp)};
  const sdb::connector::TableFilterDocIterator::FilterSpec spec{
    .field = 0, .filter = &score_filter, .is_score = true};

  duckdb::Connection con{sdb::DuckDBEngine::Instance().instance()};
  duckdb::ClientContext& ctx = *con.context;

  auto prepare_collector = filter.MakeCollector(&scorer);
  std::vector<irs::QueryBuilder::ptr> queries;
  queries.reserve(reader.size());
  for (auto& segment : reader) {
    queries.emplace_back(
      filter.PrepareSegment(segment, {.collector = prepare_collector.get()}));
  }
  const auto stats = prepare_collector->Finish(irs::IResourceManager::gNoop);

  irs::score_t score_threshold = std::numeric_limits<irs::score_t>::min();
  irs::NthPartitionScoreCollector collector{score_threshold, k, hits};
  irs::ColumnArgsFetcher fetcher;
  uint32_t seg_idx = 0;
  for (auto& segment : reader) {
    fetcher.Clear();
    auto& query = queries[seg_idx];
    collector.SetSegment(seg_idx++);
    if (!query) {
      continue;
    }

    const auto* col_reader = segment.GetColReader();
    SDB_ASSERT(col_reader != nullptr);
    irs::DocIterator::ptr it =
      irs::memory::make_managed<sdb::connector::TableFilterDocIterator>(
        query->Execute({.wand = wand}, stats), *col_reader,
        std::span<const sdb::connector::TableFilterDocIterator::FilterSpec>{
          &spec, 1},
        ctx);

    auto score_func = it->PrepareScore({
      .scorer = &scorer,
      .segment = &segment,
      .fetcher = &fetcher,
    });
    if (auto* threshold = irs::GetMutable<irs::ScoreThresholdAttr>(it.get())) {
      collector.SetScoreThreshold(threshold->value);
    }
    it->Collect(score_func, fetcher, collector);
    collector.SetScoreThreshold(score_threshold);
  }

  std::sort(hits.data(), hits.data() + collector.AcceptedCount(),
            [](const irs::ScoreDoc& l, const irs::ScoreDoc& r) {
              return l.score > r.score;
            });
  return collector.TotalMatches();
}

// Space-tokenized text field with Freq | Norm, so a term's bm25 score varies
// with document length (unlike a single-term StringField, which scores every
// match identically). Used for `content` to give the filter tests real score
// spread.
class TokenizedField final : public tests::Ifield {
 public:
  TokenizedField(irs::field_id id, std::string_view value)
    : _id{id},
      _value{value},
      _tokenizer{std::make_unique<irs::analysis::DelimitedTokenizer>(" ")} {}

  irs::field_id Id() const final { return _id; }
  std::string_view Name() const final { return {}; }
  irs::Tokenizer& GetTokens() const final {
    _tokenizer->reset(_value);
    return *_tokenizer;
  }
  irs::IndexFeatures GetIndexFeatures() const noexcept final {
    return irs::IndexFeatures::Freq | irs::IndexFeatures::Norm;
  }
  bool Write(irs::DataOutput& out) const final {
    irs::WriteStr(out, _value);
    return true;
  }

 private:
  irs::field_id _id;
  std::string _value;
  mutable std::unique_ptr<irs::analysis::DelimitedTokenizer> _tokenizer;
};

void WandScoringFieldFactory(tests::Document& doc, const std::string& name,
                             const tests::JsonDocGenerator::JsonValue& data) {
  if (JsonDocGenerator::ValueType::STRING == data.vt) {
    if (name == "content") {
      doc.insert(std::make_shared<TokenizedField>(ColumnIdFor(name), data.str));
    } else {
      auto field = std::make_shared<tests::StringField>(
        name, data.str, irs::IndexFeatures::Norm);
      field->id = ColumnIdFor(name);
      doc.insert(std::move(field));
    }
  } else if (JsonDocGenerator::ValueType::NIL == data.vt) {
    doc.insert(std::make_shared<BinaryField>());
    auto& field = (doc.indexed.end() - 1).as<BinaryField>();
    field.Name(name);
    field.id = ColumnIdFor(name);
    field.value(
      irs::ViewCast<irs::byte_type>(irs::NullTokenizer::value_null()));
  } else if (JsonDocGenerator::ValueType::BOOL == data.vt && data.b) {
    doc.insert(std::make_shared<BinaryField>());
    auto& field = (doc.indexed.end() - 1).as<BinaryField>();
    field.Name(name);
    field.id = ColumnIdFor(name);
    field.value(
      irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value_true()));
  } else if (JsonDocGenerator::ValueType::BOOL == data.vt && !data.b) {
    doc.insert(std::make_shared<BinaryField>());
    auto& field = (doc.indexed.end() - 1).as<BinaryField>();
    field.Name(name);
    field.id = ColumnIdFor(name);
    field.value(
      irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value_true()));
  } else if (data.is_number()) {
    doc.insert(std::make_shared<DoubleField>());
    auto& field = (doc.indexed.end() - 1).as<DoubleField>();
    field.Name(name);
    field.id = ColumnIdFor(name);
    field.value(data.as_number<double_t>());
  }
}

class WandScoringTestCase : public IndexTestBase {
 protected:
  void WriteSegment(irs::IndexWriter& writer, auto& gens) {
    auto& index = const_cast<tests::index_t&>(this->index());
    for (auto& gen : gens) {
      index.emplace_back();
      write_segment(writer, index.back(), gen);
    }
    writer.RefreshCommit();
  }

  // Single segment with multiplier * 420 docs.
  irs::DirectoryReader CreateLargeIndex(const irs::Scorer& scorer,
                                        size_t multiplier = 1) {
    irs::IndexWriterOptions opts;
    opts.reader_options.scorer = &scorer;
    auto writer = open_writer(irs::kOmCreate, opts);

    std::vector<tests::JsonDocGenerator> gens;
    for (size_t i = 0; i < multiplier; ++i) {
      gens.emplace_back(resource("block_scoring_segment1.json"),
                        &WandScoringFieldFactory);
      gens.emplace_back(resource("block_scoring_segment2.json"),
                        &WandScoringFieldFactory);
      gens.emplace_back(resource("block_scoring_segment3.json"),
                        &WandScoringFieldFactory);
    }

    WriteSegment(*writer, gens);

    return writer->GetSnapshot();
  }

  // 3 segments, each with multiplier * 140 docs.
  irs::DirectoryReader CreateMultiSegmentIndex(const irs::Scorer& scorer,
                                               size_t multiplier = 1) {
    irs::IndexWriterOptions opts;
    opts.reader_options.scorer = &scorer;
    auto writer = open_writer(irs::kOmCreate, opts);
    auto& index_ref = const_cast<tests::index_t&>(index());

    const std::string files[] = {
      "block_scoring_segment1.json",
      "block_scoring_segment2.json",
      "block_scoring_segment3.json",
    };

    for (const auto& file : files) {
      for (size_t i = 0; i < multiplier; ++i) {
        tests::JsonDocGenerator gen(resource(file), &WandScoringFieldFactory);
        index_ref.emplace_back();
        write_segment(*writer, index_ref.back(), gen);
      }
      writer->RefreshCommit();
    }

    return writer->GetSnapshot();
  }

  // Minimal whitespace-delimited query parser.
  //
  // Each whitespace-separated token may carry an optional `+` (required) or
  // `-` (negated) modifier, followed by an optional `<field>:` prefix that
  // names the target column, then the term value. Unlike `sdb::ParseQuery`,
  // the `<field>:` prefix is honored -- we resolve it to a `field_id` via
  // `ColumnIdFor`. The grammar parser ignores the prefix and always pins
  // queries to the default field, which is unsuitable for these tests.
  irs::Filter::ptr ParseQuery(std::string_view query,
                              std::string_view default_field = "content") {
    auto root = std::make_unique<irs::MixedBooleanFilter>();
    const irs::field_id default_field_id = ColumnIdFor(default_field);

    size_t pos = 0;
    while (pos < query.size()) {
      while (pos < query.size() && query[pos] == ' ') {
        ++pos;
      }
      if (pos >= query.size()) {
        break;
      }
      const size_t start = pos;
      while (pos < query.size() && query[pos] != ' ') {
        ++pos;
      }
      std::string_view token = query.substr(start, pos - start);

      bool required = false;
      bool negated = false;
      if (!token.empty() && token.front() == '+') {
        required = true;
        token.remove_prefix(1);
      } else if (!token.empty() && token.front() == '-') {
        negated = true;
        token.remove_prefix(1);
      }

      irs::field_id field_id = default_field_id;
      std::string_view term = token;
      const auto colon = token.find(':');
      if (colon != std::string_view::npos) {
        field_id = ColumnIdFor(token.substr(0, colon));
        term = token.substr(colon + 1);
      }

      if (required) {
        auto& by_term = root->GetRequired().add<irs::ByTerm>();
        *by_term.mutable_field_id() = field_id;
        by_term.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
      } else if (negated) {
        auto& neg =
          root->GetRequired().add<irs::Exclusion>().exclude<irs::ByTerm>();
        *neg.mutable_field_id() = field_id;
        neg.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
      } else {
        auto& by_term = root->GetOptional().add<irs::ByTerm>();
        *by_term.mutable_field_id() = field_id;
        by_term.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
      }
    }

    irs::Filter::ptr f = std::move(root);
    irs::Optimize(f);
    return f;
  }

  // Compare WAND vs non-WAND results for a single-term query.
  // WAND top-K must match non-WAND top-K.
  void CompareWandVsNonWand(const irs::DirectoryReader& reader,
                            const irs::Filter& filter,
                            const irs::Scorer& scorer, size_t k) {
    std::vector<irs::ScoreDoc> baseline_hits(irs::BlockSize(k));
    std::vector<irs::ScoreDoc> wand_hits(irs::BlockSize(k));

    size_t baseline_count = irs::ExecuteTopKWithCount(reader, filter, scorer, k,
                                                      std::span{baseline_hits});
    size_t wand_count = irs::ExecuteTopK(reader, filter, scorer, k,
                                         {.wand_enabled = true, .strict = true},
                                         std::span{wand_hits});

    auto baseline_k = std::min(baseline_count, k);
    auto wand_k = std::min(wand_count, k);

    std::cout << "baseline=" << baseline_count << " wand=" << wand_count
              << " k=" << k << std::endl;

    // WAND must return the same number of top-K results
    ASSERT_EQ(baseline_k, wand_k) << "WAND top-K size differs from baseline";

    // WAND may process fewer total docs (block pruning)
    EXPECT_LE(wand_count, baseline_count)
      << "WAND count should not exceed baseline count";

    // Compare actual top-K docs and scores
    for (size_t i = 0; i < baseline_k; ++i) {
      EXPECT_EQ(baseline_hits[i].doc, wand_hits[i].doc)
        << "Doc ID mismatch at position " << i;
      EXPECT_FLOAT_EQ(baseline_hits[i].score, wand_hits[i].score)
        << "Score mismatch at position " << i;
    }
  }

  void VerifyScoresAndDocs(auto docs, size_t result_count) {
    for (size_t i = 0; i < result_count; ++i) {
      EXPECT_GT(docs[i].score, 0)
        << "Score at position " << i << " should be positive";
      if (i > 0) {
        EXPECT_GE(docs[i - 1].score, docs[i].score)
          << "Scores should be in descending order at position " << i;
      }
      ASSERT_TRUE(!irs::doc_limits::eof(docs[i].doc) &&
                  docs[i].doc != irs::doc_limits::invalid())
        << "Doc ID at position " << i << " should be valid, got "
        << docs[i].doc;
    }
  }
};

// TFIDF single-term, 4200 docs (~1260 matching "database" = ~10 blocks)
TEST_P(WandScoringTestCase, TfidfWandVsBaseline) {
  auto scorer = irs::TFIDF{true};
  auto reader = CreateLargeIndex(scorer, 10);

  auto filter = ParseQuery("topic:database");
  ASSERT_NE(nullptr, filter);

  CompareWandVsNonWand(reader, *filter, scorer, 10);
}

// BM25 single-term, 4200 docs (~840 matching "search" = ~6 blocks)
TEST_P(WandScoringTestCase, Bm25WandVsBaseline) {
  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto reader = CreateLargeIndex(scorer, 10);

  auto filter = ParseQuery("topic:search");
  ASSERT_NE(nullptr, filter);

  CompareWandVsNonWand(reader, *filter, scorer, 15);
}

// Anti-correlated row filter: the highest-scoring docs all FAIL the filter and
// only lower-scoring docs pass. Block-max WAND must NOT skip the (low-scoring)
// passing blocks just because high scorers dominate -- because the threshold is
// the kth-best *passing* score, the rejected high scorers never lift it. Proven
// by equality with the non-WAND baseline (which cannot skip): if WAND wrongly
// skipped a passing block, its top-k would differ.
TEST_P(WandScoringTestCase, FilteredAntiCorrelatedKeepsLowScorers) {
  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto reader = CreateLargeIndex(scorer, 10);  // single segment, ~4200 docs
  ASSERT_EQ(1, reader.size());

  // `content` is space-tokenized with Freq | Norm, so a term's bm25 score
  // varies with document length -- unlike the single-term topic/category
  // fields, this gives the score cutoff below something to partition.
  auto filter = ParseQuery("content:quantum");
  ASSERT_NE(nullptr, filter);

  // 1. Identify the top scorers with a brute-force (non-WAND) pass.
  constexpr size_t kReject = 150;  // > kBlockSize (128): rejects > a full block
  std::vector<irs::ScoreDoc> top(irs::BlockSize(kReject));
  const auto df =
    irs::ExecuteTopKWithCount(reader, *filter, scorer, kReject, std::span{top});
  ASSERT_GT(df, irs::doc_limits::kBlockSize)
    << "term df must exceed kBlockSize so block-max skip can engage";
  ASSERT_GE(df, kReject);

  // 2. Reject the top scorers via a score cutoff (the kReject-th best score):
  // `score < cutoff` keeps only the strictly lower scorers. `top` is sorted
  // descending, so top[kReject - 1] is the cutoff.
  const irs::score_t cutoff = top[kReject - 1].score;
  ASSERT_LT(cutoff, top[0].score) << "need score variation to reject a subset";

  // 3. Filtered top-k: WAND (block-max skip ON) vs baseline (skip OFF), both
  // through the TableFilterDocIterator score filter.
  constexpr size_t kTopK = 10;
  std::vector<irs::ScoreDoc> wand_hits(irs::BlockSize(kTopK));
  std::vector<irs::ScoreDoc> base_hits(irs::BlockSize(kTopK));

  const auto wand_count = ExecuteTopKFiltered(
    reader, *filter, scorer, kTopK, {.wand_enabled = true, .strict = true},
    cutoff, std::span{wand_hits});
  const auto base_count =
    ExecuteTopKFiltered(reader, *filter, scorer, kTopK, {.wand_enabled = false},
                        cutoff, std::span{base_hits});

  const auto wand_k = std::min<size_t>(wand_count, kTopK);
  const auto base_k = std::min<size_t>(base_count, kTopK);

  ASSERT_GT(base_k, 0u)
    << "lower-scoring passing docs must exist below the top";
  ASSERT_EQ(base_k, wand_k) << "WAND top-k size differs from baseline";

  // 4. WAND must return exactly the baseline's filtered top-k, and none of the
  //    rejected high scorers (score >= cutoff) may leak through.
  for (size_t i = 0; i < base_k; ++i) {
    EXPECT_EQ(base_hits[i].doc, wand_hits[i].doc)
      << "WAND dropped/reordered a passing doc at position " << i;
    EXPECT_FLOAT_EQ(base_hits[i].score, wand_hits[i].score)
      << "score mismatch at position " << i;
    EXPECT_LT(wand_hits[i].score, cutoff)
      << "a rejected high scorer leaked into the filtered top-k at position "
      << i;
  }
}

// BM25 with small k=3, 4200 docs (~2100 matching "tech" = ~16 blocks)
TEST_P(WandScoringTestCase, WandSmallK) {
  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto reader = CreateLargeIndex(scorer, 10);

  auto filter = ParseQuery("category:tech");
  ASSERT_NE(nullptr, filter);

  CompareWandVsNonWand(reader, *filter, scorer, 3);
}

// WAND with k larger than matches -- no pruning expected
TEST_P(WandScoringTestCase, WandLargeK) {
  auto scorer = irs::TFIDF{true};
  auto reader = CreateLargeIndex(scorer);

  auto filter = ParseQuery("topic:chemistry");
  ASSERT_NE(nullptr, filter);

  CompareWandVsNonWand(reader, *filter, scorer, 1000);
}

// BM15 (b=0), 4200 docs (~850 matching "physics" = ~6 blocks)
TEST_P(WandScoringTestCase, WandBm15) {
  auto scorer = irs::BM25{irs::BM25::K(), 0.0f};
  auto reader = CreateLargeIndex(scorer, 10);

  auto filter = ParseQuery("topic:physics");
  ASSERT_NE(nullptr, filter);

  CompareWandVsNonWand(reader, *filter, scorer, 10);
}

// k=1 -- aggressive threshold, 4200 docs
TEST_P(WandScoringTestCase, WandKOne) {
  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto reader = CreateLargeIndex(scorer, 10);

  auto filter = ParseQuery("category:tech");
  ASSERT_NE(nullptr, filter);

  CompareWandVsNonWand(reader, *filter, scorer, 1);
}

// Multi-segment TFIDF, 3 segments x 1400 docs each
TEST_P(WandScoringTestCase, WandMultisegTfidf) {
  auto scorer = irs::TFIDF{true};
  auto reader = CreateMultiSegmentIndex(scorer, 10);
  ASSERT_EQ(3, reader.size());

  auto filter = ParseQuery("topic:database");
  ASSERT_NE(nullptr, filter);

  CompareWandVsNonWand(reader, *filter, scorer, 15);
}

// Multi-segment BM25, 3 segments x 1400 docs each
TEST_P(WandScoringTestCase, WandMultisegBm25) {
  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto reader = CreateMultiSegmentIndex(scorer, 10);
  ASSERT_EQ(3, reader.size());

  auto filter = ParseQuery("topic:search");
  ASSERT_NE(nullptr, filter);

  CompareWandVsNonWand(reader, *filter, scorer, 20);
}

// WAND with empty result set
TEST_P(WandScoringTestCase, WandEmptyResults) {
  auto scorer = irs::TFIDF{true};
  auto reader = CreateLargeIndex(scorer);

  auto filter = ParseQuery("topic:xyznonexistent123");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;
  std::vector<irs::ScoreDoc> hits(irs::BlockSize(kTopK));

  size_t count =
    irs::ExecuteTopK(reader, *filter, scorer, kTopK,
                     {.wand_enabled = true, .strict = true}, std::span{hits});
  ASSERT_EQ(0, count);
}

// Verify WAND returns valid results with correct scores
TEST_P(WandScoringTestCase, WandResultValues) {
  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto reader = CreateLargeIndex(scorer, 10);
  ASSERT_EQ(1, reader.size());

  auto filter = ParseQuery("topic:database");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;
  std::vector<irs::ScoreDoc> hits(irs::BlockSize(kTopK));

  size_t count =
    irs::ExecuteTopK(reader, *filter, scorer, kTopK,
                     {.wand_enabled = true, .strict = true}, std::span{hits});
  ASSERT_GT(count, 0);
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(hits, result_count);
}

// Multi-segment WAND with result value verification
TEST_P(WandScoringTestCase, WandMultisegResultValues) {
  auto scorer = irs::TFIDF{true};
  auto reader = CreateMultiSegmentIndex(scorer, 10);
  ASSERT_EQ(3, reader.size());

  auto filter = ParseQuery("topic:physics");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;
  std::vector<irs::ScoreDoc> hits(irs::BlockSize(kTopK));

  size_t count =
    irs::ExecuteTopK(reader, *filter, scorer, kTopK,
                     {.wand_enabled = true, .strict = true}, std::span{hits});
  ASSERT_GT(count, 0);
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(hits, result_count);
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(WandScoringTest, WandScoringTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd")),
                         WandScoringTestCase::to_string);

}  // namespace
