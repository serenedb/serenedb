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
#include "iresearch/analysis/delimited_tokenizer.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/formats/posting/score_bound_writer.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/index/table_filter_iterator.hpp"
#include "iresearch/index/typed_terms.hpp"
#include "iresearch/parser/parser.hpp"
#include "iresearch/search/bm25.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/dfi.hpp"
#include "iresearch/search/doc_collector.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/indri_dirichlet.hpp"
#include "iresearch/search/lm_dirichlet.hpp"
#include "iresearch/search/lm_jelinek_mercer.hpp"
#include "iresearch/search/raw_tf.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/scorer_options.hpp"
#include "iresearch/search/terms_filter.hpp"
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

// Top-k executor that wraps the pruning iterator in a TableFilterDocIterator
// -- the production row-filter path -- applying a score cutoff: docs with
// score >= `reject_score` are dropped before they reach the collector, so we
// exercise block-max score pruning together with a table filter. The filter
// runs after scoring (inside Collect), so pruning is unaware of it and skips
// purely on the collector threshold (the kth passing score), exactly as in
// production.
uint64_t ExecuteTopKFiltered(const irs::DirectoryReader& reader,
                             const irs::Filter& filter,
                             const irs::Scorer& scorer, size_t k,
                             bool score_prune, irs::score_t reject_score,
                             std::span<irs::ScoreDoc> hits) {
  SDB_ASSERT(k == hits.size());

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
  irs::LoserScoreCollector collector{score_threshold, hits};
  irs::ColumnArgsFetcher fetcher;
  sdb::connector::ColFilterStateCache filter_states;
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
        query->Execute({.prune_scorer = score_prune ? &scorer : nullptr},
                       stats),
        *col_reader,
        std::span<const sdb::connector::TableFilterDocIterator::FilterSpec>{
          &spec, 1},
        ctx, filter_states);

    auto score_func = it->PrepareScore({
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
  irs::analysis::Tokenizer& GetTokens() const final { return *_tokenizer; }
  std::string_view Value() const final { return _value; }
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

void ScorePruneFieldFactory(tests::Document& doc, const std::string& name,
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
    field.value(irs::ViewCast<irs::byte_type>(irs::kNullTerm));
  } else if (JsonDocGenerator::ValueType::BOOL == data.vt && data.b) {
    doc.insert(std::make_shared<BinaryField>());
    auto& field = (doc.indexed.end() - 1).as<BinaryField>();
    field.Name(name);
    field.id = ColumnIdFor(name);
    field.value(irs::ViewCast<irs::byte_type>(irs::kTrueTerm));
  } else if (JsonDocGenerator::ValueType::BOOL == data.vt && !data.b) {
    doc.insert(std::make_shared<BinaryField>());
    auto& field = (doc.indexed.end() - 1).as<BinaryField>();
    field.Name(name);
    field.id = ColumnIdFor(name);
    field.value(irs::ViewCast<irs::byte_type>(irs::kTrueTerm));
  } else if (data.is_number()) {
    doc.insert(std::make_shared<DoubleField>());
    auto& field = (doc.indexed.end() - 1).as<DoubleField>();
    field.Name(name);
    field.id = ColumnIdFor(name);
    field.value(data.as_number<double_t>());
  }
}

class ScorePruneScoringTestCase : public IndexTestBase {
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
                        &ScorePruneFieldFactory);
      gens.emplace_back(resource("block_scoring_segment2.json"),
                        &ScorePruneFieldFactory);
      gens.emplace_back(resource("block_scoring_segment3.json"),
                        &ScorePruneFieldFactory);
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
        tests::JsonDocGenerator gen(resource(file), &ScorePruneFieldFactory);
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

  // Compare pruned vs unpruned results for a single-term query.
  // The pruned top-K must match the unpruned top-K.
  void ComparePrunedVsBaseline(const irs::DirectoryReader& reader,
                               const irs::Filter& filter,
                               const irs::Scorer& scorer, size_t k) {
    std::vector<irs::ScoreDoc> baseline_hits(k);
    std::vector<irs::ScoreDoc> pruned_hits(k);

    size_t baseline_count = irs::ExecuteTopKWithCount(reader, filter, scorer, k,
                                                      std::span{baseline_hits});
    size_t pruned_count =
      irs::ExecuteTopK(reader, filter, scorer, k, true, std::span{pruned_hits});

    auto baseline_k = std::min(baseline_count, k);
    auto pruned_k = std::min(pruned_count, k);

    std::cout << "baseline=" << baseline_count << " pruned=" << pruned_count
              << " k=" << k << std::endl;

    // Pruning must return the same number of top-K results
    ASSERT_EQ(baseline_k, pruned_k)
      << "pruned top-K size differs from baseline";

    // Pruning may process fewer total docs (block skipping)
    EXPECT_LE(pruned_count, baseline_count)
      << "pruned count should not exceed baseline count";

    // Compare actual top-K docs and scores
    for (size_t i = 0; i < baseline_k; ++i) {
      EXPECT_EQ(baseline_hits[i].doc, pruned_hits[i].doc)
        << "Doc ID mismatch at position " << i;
      EXPECT_FLOAT_EQ(baseline_hits[i].score, pruned_hits[i].score)
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
TEST_P(ScorePruneScoringTestCase, TfidfPrunedVsBaseline) {
  auto scorer = irs::TFIDF{true};
  auto reader = CreateLargeIndex(scorer, 10);

  auto filter = ParseQuery("topic:database");
  ASSERT_NE(nullptr, filter);

  ComparePrunedVsBaseline(reader, *filter, scorer, 10);
}

// Scorers beyond bm25/tfidf that persist score bounds. Each is monotone in the
// quantity its bound stores, so the pruned top-K has to match the unpruned one.
TEST_P(ScorePruneScoringTestCase, LmJelinekMercerPrunedVsBaseline) {
  auto scorer = irs::LMJelinekMercer{0.7f};
  auto reader = CreateLargeIndex(scorer, 10);
  auto filter = ParseQuery("topic:database");
  ASSERT_NE(nullptr, filter);
  ComparePrunedVsBaseline(reader, *filter, scorer, 10);
}

TEST_P(ScorePruneScoringTestCase, LmDirichletPrunedVsBaseline) {
  auto scorer = irs::LMDirichlet{2000.f};
  auto reader = CreateLargeIndex(scorer, 10);
  auto filter = ParseQuery("topic:database");
  ASSERT_NE(nullptr, filter);
  ComparePrunedVsBaseline(reader, *filter, scorer, 10);
}

TEST_P(ScorePruneScoringTestCase, DfiPrunedVsBaseline) {
  auto scorer = irs::DFI{irs::DFIMeasure::Standardized};
  auto reader = CreateLargeIndex(scorer, 10);
  auto filter = ParseQuery("topic:database");
  ASSERT_NE(nullptr, filter);
  ComparePrunedVsBaseline(reader, *filter, scorer, 10);
}

TEST_P(ScorePruneScoringTestCase, RawTfPrunedVsBaseline) {
  auto scorer = irs::RawTF{};
  auto reader = CreateLargeIndex(scorer, 10);
  auto filter = ParseQuery("topic:database");
  ASSERT_NE(nullptr, filter);
  ComparePrunedVsBaseline(reader, *filter, scorer, 10);
}

// Matching the baseline is only meaningful if pruning actually skipped
// something, so each of these must also reach fewer documents than the run
// that cannot skip.
TEST_P(ScorePruneScoringTestCase, PruningIsTakenForBoundedScorers) {
  auto reached = [&](const irs::DirectoryReader& reader,
                     const irs::Filter& filter, const irs::Scorer& scorer,
                     bool prune) {
    constexpr size_t k = 10;
    std::vector<irs::ScoreDoc> hits(k);
    return prune ? irs::ExecuteTopK(reader, filter, scorer, k, true,
                                    std::span{hits})
                 : irs::ExecuteTopKWithCount(reader, filter, scorer, k,
                                             std::span{hits});
  };

  auto check = [&](const irs::Scorer& scorer, std::string_view name) {
    SCOPED_TRACE(name);
    auto reader = CreateLargeIndex(scorer, 10);
    auto filter = ParseQuery("topic:database");
    ASSERT_NE(nullptr, filter);
    EXPECT_LT(reached(reader, *filter, scorer, true),
              reached(reader, *filter, scorer, false));
  };

  check(irs::LMJelinekMercer{0.7f}, "lm_jelinek_mercer");
  check(irs::LMDirichlet{2000.f}, "lm_dirichlet");
  check(irs::DFI{irs::DFIMeasure::Standardized}, "dfi");
  check(irs::RawTF{}, "raw_tf");
}

// indri_dirichlet has the same monotonicity as lm_dirichlet but no floor at
// zero, and it scores negative whenever tf < mu * P(t|C). Pruning drops rows in
// that regime, so it must claim no bound and build no writer -- restoring them
// needs a fix for negative scores first.
TEST_P(ScorePruneScoringTestCase, IndriDirichletClaimsNoBounds) {
  const irs::IndriDirichlet scorer{2000.f};
  EXPECT_EQ(nullptr, scorer.PrepareScoreBoundWriter(4));
  EXPECT_EQ(nullptr, scorer.PrepareScoreBoundSource());

  auto reader = CreateLargeIndex(scorer, 10);
  auto filter = ParseQuery("topic:database");
  ASSERT_NE(nullptr, filter);
  ComparePrunedVsBaseline(reader, *filter, scorer, 10);
}

// The bound type is what gates pruning, so pin the classification each scorer
// reports and that the ones claiming a bound can build the writer and source
// that back it.
TEST(score_prune_bound_type_test, reported_bound_types) {
  // One persisted options set per bound type, probed through Compatible: that
  // is what actually gates pruning. bm25(1.2, 0.75) also stands in for a
  // parameterised MinNorm writer, which only an equal-b bm25 may read.
  const irs::ScorerOptions max_freq{irs::RawTF::Options{}};
  const irs::ScorerOptions div_norm{irs::LMJelinekMercer::Options{}};
  const irs::ScorerOptions min_norm{irs::LMDirichlet::Options{}};
  const irs::ScorerOptions min_norm_b{
    irs::BM25::Options{.k1 = 1.2f, .b = 0.75f}};
  const irs::ScorerOptions none{irs::IndriDirichlet::Options{}};

  // `param_free` is false for a scorer whose argmax is parameterised, which is
  // only ever bm25 under MinNorm: it may read no parameter-free pair, and no
  // parameter-free scorer may read its pair.
  auto check = [&](const irs::Scorer& scorer,
                   irs::Scorer::ScoreBoundType expected,
                   bool param_free = true) {
    using BT = irs::Scorer::ScoreBoundType;
    EXPECT_EQ(expected == BT::MaxFreq && param_free,
              scorer.Compatible(max_freq));
    EXPECT_EQ(expected == BT::DivNorm && param_free,
              scorer.Compatible(div_norm));
    EXPECT_EQ(expected == BT::MinNorm && param_free,
              scorer.Compatible(min_norm));
    EXPECT_FALSE(scorer.Compatible(none));
    if (expected == irs::Scorer::ScoreBoundType::None) {
      EXPECT_EQ(nullptr, scorer.PrepareScoreBoundWriter(4));
      EXPECT_EQ(nullptr, scorer.PrepareScoreBoundSource());
    } else {
      EXPECT_NE(nullptr, scorer.PrepareScoreBoundWriter(4));
      EXPECT_NE(nullptr, scorer.PrepareScoreBoundSource());
    }
  };

  using BoundType = irs::Scorer::ScoreBoundType;
  check(irs::RawTF{}, BoundType::MaxFreq);
  check(irs::LMJelinekMercer{0.7f}, BoundType::DivNorm);
  check(irs::LMDirichlet{2000.f}, BoundType::MinNorm);
  check(irs::DFI{irs::DFIMeasure::Standardized}, BoundType::MinNorm);
  check(irs::DFI{irs::DFIMeasure::Saturated}, BoundType::MinNorm);
  check(irs::DFI{irs::DFIMeasure::ChiSquared}, BoundType::MinNorm);
  check(irs::IndriDirichlet{2000.f}, BoundType::None);
  check(irs::TFIDF{true}, BoundType::DivNorm);
  check(irs::TFIDF{false}, BoundType::MaxFreq);
  check(irs::BM25{1.2f, 0.75f}, BoundType::MinNorm, /*param_free=*/false);
  check(irs::BM25{1.2f, 0.f}, BoundType::MaxFreq);
  check(irs::BM25{1.2f, 1.f}, BoundType::DivNorm);
  check(irs::BM25{0.f, 0.75f}, BoundType::None);

  // A MinNorm pair written for bm25(b=0.75) is its argmax, not anyone else's.
  EXPECT_TRUE((irs::BM25{2.0f, 0.75f}.Compatible(min_norm_b)));
  EXPECT_FALSE((irs::BM25{1.2f, 0.5f}.Compatible(min_norm_b)));
  EXPECT_FALSE((irs::LMDirichlet{2000.f}.Compatible(min_norm_b)));
  EXPECT_FALSE(
    (irs::DFI{irs::DFIMeasure::Standardized}.Compatible(min_norm_b)));

  // kScoreBoundAvgDL and kScoreBoundBM25 both report MinNorm but pick a
  // different argmax, so the avg_dl mode has to agree as well.
  EXPECT_FALSE((irs::BM25{1.2f, 0.75f, false, /*approximate=*/false}.Compatible(
    min_norm_b)));
}

// Pruning has to do something, not merely be harmless: the pruned run reaches
// fewer documents than the one that cannot skip. Which iterator serves it is
// not visible from here -- the load test watches that, through the documents
// TOP_100 reports over an index with score bounds.
TEST_P(ScorePruneScoringTestCase, PruningIsTaken) {
  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto reader = CreateLargeIndex(scorer, 10);
  constexpr size_t k = 10;

  auto reached = [&](const irs::Filter& filter, bool prune) {
    std::vector<irs::ScoreDoc> hits(k);
    return prune ? irs::ExecuteTopK(reader, filter, scorer, k, true,
                                    std::span{hits})
                 : irs::ExecuteTopKWithCount(reader, filter, scorer, k,
                                             std::span{hits});
  };

  {
    SCOPED_TRACE("single term");
    auto filter = ParseQuery("topic:database");
    ASSERT_NE(nullptr, filter);
    EXPECT_LT(reached(*filter, true), reached(*filter, false));
  }

  {
    SCOPED_TRACE("terms that add up");
    irs::ByTerms filter;
    *filter.mutable_field_id() = ColumnIdFor("content");
    auto* options = filter.mutable_options();
    options->merge_type = irs::ScoreMergeType::Sum;
    options->terms.emplace(
      irs::ViewCast<irs::byte_type>(std::string_view{"index"}), irs::kNoBoost);
    options->terms.emplace(
      irs::ViewCast<irs::byte_type>(std::string_view{"search"}), irs::kNoBoost);
    EXPECT_LT(reached(filter, true), reached(filter, false));
  }
}

// A query that takes the best of its terms rather than adding them up must be
// scored that way whether or not it prunes. `MaxScoreIterator` bounds a sum,
// so it cannot serve a `Max` query: with `index` and `search` in the same
// documents, summing scores both differently and higher than taking the best.
TEST_P(ScorePruneScoringTestCase, MaxMergePrunedVsBaseline) {
  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto reader = CreateLargeIndex(scorer, 10);

  irs::ByTerms filter;
  *filter.mutable_field_id() = ColumnIdFor("content");
  auto* options = filter.mutable_options();
  options->merge_type = irs::ScoreMergeType::Max;
  options->terms.emplace(
    irs::ViewCast<irs::byte_type>(std::string_view{"index"}), irs::kNoBoost);
  options->terms.emplace(
    irs::ViewCast<irs::byte_type>(std::string_view{"search"}), irs::kNoBoost);

  ComparePrunedVsBaseline(reader, filter, scorer, 10);
}

// BM25 single-term, 4200 docs (~840 matching "search" = ~6 blocks)
TEST_P(ScorePruneScoringTestCase, Bm25PrunedVsBaseline) {
  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto reader = CreateLargeIndex(scorer, 10);

  auto filter = ParseQuery("topic:search");
  ASSERT_NE(nullptr, filter);

  ComparePrunedVsBaseline(reader, *filter, scorer, 15);
}

// Anti-correlated row filter: the highest-scoring docs all FAIL the filter and
// only lower-scoring docs pass. Block-max score pruning must NOT skip the
// (low-scoring) passing blocks just because high scorers dominate -- because
// the threshold is the kth-best *passing* score, the rejected high scorers
// never lift it. Proven by equality with the unpruned baseline (which cannot
// skip): if pruning wrongly skipped a passing block, its top-k would differ.
TEST_P(ScorePruneScoringTestCase, FilteredAntiCorrelatedKeepsLowScorers) {
  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto reader = CreateLargeIndex(scorer, 10);  // single segment, ~4200 docs
  ASSERT_EQ(1, reader.size());

  // `content` is space-tokenized with Freq | Norm, so a term's bm25 score
  // varies with document length -- unlike the single-term topic/category
  // fields, this gives the score cutoff below something to partition.
  auto filter = ParseQuery("content:quantum");
  ASSERT_NE(nullptr, filter);

  // 1. Identify the top scorers with a brute-force (unpruned) pass.
  constexpr size_t kReject = 150;  // > kBlockSize (128): rejects > a full block
  std::vector<irs::ScoreDoc> top(kReject);
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

  // 3. Filtered top-k: pruning (block-max skip ON) vs baseline (skip OFF),
  // both through the TableFilterDocIterator score filter.
  constexpr size_t kTopK = 10;
  std::vector<irs::ScoreDoc> pruned_hits(kTopK);
  std::vector<irs::ScoreDoc> base_hits(kTopK);

  const auto pruned_count = ExecuteTopKFiltered(
    reader, *filter, scorer, kTopK, true, cutoff, std::span{pruned_hits});
  const auto base_count = ExecuteTopKFiltered(
    reader, *filter, scorer, kTopK, false, cutoff, std::span{base_hits});

  const auto pruned_k = std::min<size_t>(pruned_count, kTopK);
  const auto base_k = std::min<size_t>(base_count, kTopK);

  ASSERT_GT(base_k, 0u)
    << "lower-scoring passing docs must exist below the top";
  ASSERT_EQ(base_k, pruned_k) << "pruned top-k size differs from baseline";

  // 4. Pruning must return exactly the baseline's filtered top-k, and none of
  //    the rejected high scorers (score >= cutoff) may leak through.
  for (size_t i = 0; i < base_k; ++i) {
    EXPECT_EQ(base_hits[i].doc, pruned_hits[i].doc)
      << "pruning dropped/reordered a passing doc at position " << i;
    EXPECT_FLOAT_EQ(base_hits[i].score, pruned_hits[i].score)
      << "score mismatch at position " << i;
    EXPECT_LT(pruned_hits[i].score, cutoff)
      << "a rejected high scorer leaked into the filtered top-k at position "
      << i;
  }
}

// BM25 with small k=3, 4200 docs (~2100 matching "tech" = ~16 blocks)
TEST_P(ScorePruneScoringTestCase, ScorePruneSmallK) {
  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto reader = CreateLargeIndex(scorer, 10);

  auto filter = ParseQuery("category:tech");
  ASSERT_NE(nullptr, filter);

  ComparePrunedVsBaseline(reader, *filter, scorer, 3);
}

// Score pruning with k larger than matches -- no pruning expected
TEST_P(ScorePruneScoringTestCase, ScorePruneLargeK) {
  auto scorer = irs::TFIDF{true};
  auto reader = CreateLargeIndex(scorer);

  auto filter = ParseQuery("topic:chemistry");
  ASSERT_NE(nullptr, filter);

  ComparePrunedVsBaseline(reader, *filter, scorer, 1000);
}

// BM15 (b=0), 4200 docs (~850 matching "physics" = ~6 blocks)
TEST_P(ScorePruneScoringTestCase, ScorePruneBm15) {
  auto scorer = irs::BM25{irs::BM25::K(), 0.0f};
  auto reader = CreateLargeIndex(scorer, 10);

  auto filter = ParseQuery("topic:physics");
  ASSERT_NE(nullptr, filter);

  ComparePrunedVsBaseline(reader, *filter, scorer, 10);
}

// k=1 -- aggressive threshold, 4200 docs
TEST_P(ScorePruneScoringTestCase, ScorePruneKOne) {
  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto reader = CreateLargeIndex(scorer, 10);

  auto filter = ParseQuery("category:tech");
  ASSERT_NE(nullptr, filter);

  ComparePrunedVsBaseline(reader, *filter, scorer, 1);
}

// Multi-segment TFIDF, 3 segments x 1400 docs each
TEST_P(ScorePruneScoringTestCase, ScorePruneMultisegTfidf) {
  auto scorer = irs::TFIDF{true};
  auto reader = CreateMultiSegmentIndex(scorer, 10);
  ASSERT_EQ(3, reader.size());

  auto filter = ParseQuery("topic:database");
  ASSERT_NE(nullptr, filter);

  ComparePrunedVsBaseline(reader, *filter, scorer, 15);
}

// Multi-segment BM25, 3 segments x 1400 docs each
TEST_P(ScorePruneScoringTestCase, ScorePruneMultisegBm25) {
  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto reader = CreateMultiSegmentIndex(scorer, 10);
  ASSERT_EQ(3, reader.size());

  auto filter = ParseQuery("topic:search");
  ASSERT_NE(nullptr, filter);

  ComparePrunedVsBaseline(reader, *filter, scorer, 20);
}

// Score pruning with empty result set
TEST_P(ScorePruneScoringTestCase, ScorePruneEmptyResults) {
  auto scorer = irs::TFIDF{true};
  auto reader = CreateLargeIndex(scorer);

  auto filter = ParseQuery("topic:xyznonexistent123");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;
  std::vector<irs::ScoreDoc> hits(kTopK);

  size_t count =
    irs::ExecuteTopK(reader, *filter, scorer, kTopK, true, std::span{hits});
  ASSERT_EQ(0, count);
}

// Verify score pruning returns valid results with correct scores
TEST_P(ScorePruneScoringTestCase, ScorePruneResultValues) {
  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto reader = CreateLargeIndex(scorer, 10);
  ASSERT_EQ(1, reader.size());

  auto filter = ParseQuery("topic:database");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;
  std::vector<irs::ScoreDoc> hits(kTopK);

  size_t count =
    irs::ExecuteTopK(reader, *filter, scorer, kTopK, true, std::span{hits});
  ASSERT_GT(count, 0);
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(hits, result_count);
}

// Multi-segment score pruning with result value verification
TEST_P(ScorePruneScoringTestCase, ScorePruneMultisegResultValues) {
  auto scorer = irs::TFIDF{true};
  auto reader = CreateMultiSegmentIndex(scorer, 10);
  ASSERT_EQ(3, reader.size());

  auto filter = ParseQuery("topic:physics");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;
  std::vector<irs::ScoreDoc> hits(kTopK);

  size_t count =
    irs::ExecuteTopK(reader, *filter, scorer, kTopK, true, std::span{hits});
  ASSERT_GT(count, 0);
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(hits, result_count);
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(ScorePruneScoringTest, ScorePruneScoringTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd")),
                         ScorePruneScoringTestCase::to_string);

}  // namespace

// The property every score bound rests on: the (freq, norm) pair a block stores
// must score at least as high as every document in that block. If it scores
// lower, `IsLessThanUpperBound` skips blocks that still hold qualifying rows
// and they vanish from the results. The end-to-end oracles observe that
// indirectly; this drives the producer and the scorer directly, over pairs
// chosen to stress the encoding -- notably documents whose length is below the
// block's maximum frequency, where the stored norm gets clamped up to the
// frequency because it is written as a delta above it.
namespace {

struct BoundAttrs final : irs::AttributeProvider {
  irs::Attribute* GetMutable(irs::TypeInfo::type_id id) noexcept final {
    if (irs::Type<irs::FreqBlockAttr>::id() == id) {
      return &freq_block;
    }
    if (irs::Type<irs::Norm>::id() == id) {
      return &norm;
    }
    return nullptr;
  }

  uint32_t freq{};
  irs::FreqBlockAttr freq_block{.value = &freq};
  irs::Norm norm;
};

struct NoNorms final : irs::NormProvider {
  irs::NormReader::ptr norms(irs::field_id) const final { return nullptr; }
};

struct Pair {
  uint32_t tf;
  uint32_t dl;
};

// One block's worth of documents, mixing very short postings with high
// frequencies so that min(dl) lands below max(tf).
constexpr Pair kBlock[]{{1, 1},   {30, 31},  {1, 201}, {12, 15},
                        {2, 2},   {25, 126}, {1, 4},   {7, 9},
                        {40, 40}, {3, 300},  {18, 20}, {5, 5}};

template<typename Scorer>
irs::bstring MakeStats(const Scorer& scorer) {
  irs::FieldCollector field;
  field.docs_with_field = 4096;
  field.total_term_freq = 65536;
  irs::TermCollector term;
  term.docs_with_term = 512;
  term.total_term_freq = 2048;

  irs::bstring stats(scorer.stats_size(), 0);
  scorer.collect(stats.data(), &field, &term);
  return stats;
}

template<typename Scorer>
irs::score_t ScoreAt(const Scorer& scorer, const irs::byte_type* stats,
                     BoundAttrs& attrs, uint32_t tf, uint32_t dl) {
  attrs.freq = tf;
  attrs.norm.value = dl;
  const NoNorms segment;
  auto fn = scorer.PrepareScorer({.segment = segment,
                                  .field = {},
                                  .doc_attrs = attrs,
                                  .fetcher = nullptr,
                                  .stats = stats,
                                  .boost = irs::kNoBoost});
  irs::score_t value{};
  fn.Score(&value, 1);
  return value;
}

// Drive the producer exactly as ScoreBoundWriterImpl does: fold every document
// of the block into one entry.
template<uint32_t Tag>
typename irs::FreqNormProducer<Tag>::Entry FoldBlock() {
  irs::FreqNormProducer<Tag> producer;
  typename irs::FreqNormProducer<Tag>::Entry bound{};
  for (const auto& [tf, dl] : kBlock) {
    typename irs::FreqNormProducer<Tag>::Entry doc{};
    doc.freq = tf;
    if constexpr (requires { doc.norm = dl; }) {
      doc.norm = dl;
    }
    producer.Produce(doc, bound);
  }
  return bound;
}

template<uint32_t Tag, typename Scorer>
void AssertBoundDominates(const Scorer& scorer, std::string_view name) {
  SCOPED_TRACE(name);
  const auto stats = MakeStats(scorer);
  BoundAttrs attrs;

  const auto bound = FoldBlock<Tag>();
  const uint32_t bound_dl = [&] {
    if constexpr (requires { static_cast<uint32_t>(bound.norm); }) {
      return static_cast<uint32_t>(bound.norm);
    } else {
      return uint32_t{1};
    }
  }();

  const auto bound_score =
    ScoreAt(scorer, stats.c_str(), attrs, bound.freq, bound_dl);
  auto lowest = std::numeric_limits<irs::score_t>::max();
  auto highest = std::numeric_limits<irs::score_t>::lowest();
  for (const auto& [tf, dl] : kBlock) {
    const auto doc_score = ScoreAt(scorer, stats.c_str(), attrs, tf, dl);
    lowest = std::min(lowest, doc_score);
    highest = std::max(highest, doc_score);
    EXPECT_GE(bound_score, doc_score)
      << "bound (tf=" << bound.freq << ", dl=" << bound_dl
      << ") scores below document (tf=" << tf << ", dl=" << dl << ")";
  }
  // Guard the guard: a block whose documents all score alike would satisfy the
  // comparison above no matter what the producer chose.
  EXPECT_LT(lowest, highest);
}

}  // namespace

TEST(score_bound_dominance_test, bound_scores_at_least_every_document) {
  AssertBoundDominates<irs::kScoreBoundMaxFreq>(irs::RawTF{}, "raw_tf");
  AssertBoundDominates<irs::kScoreBoundDivNorm>(irs::LMJelinekMercer{0.7f},
                                                "lm_jelinek_mercer");
  AssertBoundDominates<irs::kScoreBoundMinNorm>(irs::LMDirichlet{2000.f},
                                                "lm_dirichlet");
  AssertBoundDominates<irs::kScoreBoundMinNorm>(
    irs::DFI{irs::DFIMeasure::Standardized}, "dfi_standardized");
  AssertBoundDominates<irs::kScoreBoundMinNorm>(
    irs::DFI{irs::DFIMeasure::Saturated}, "dfi_saturated");
  AssertBoundDominates<irs::kScoreBoundMinNorm>(
    irs::DFI{irs::DFIMeasure::ChiSquared}, "dfi_chi_squared");
  AssertBoundDominates<irs::kScoreBoundDivNorm>(irs::TFIDF{true},
                                                "tfidf_with_norms");
  AssertBoundDominates<irs::kScoreBoundMaxFreq>(irs::TFIDF{false},
                                                "tfidf_plain");
}
