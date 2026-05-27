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

#include <gtest/gtest.h>

#include "index/index_tests.hpp"
#include "iresearch/analysis/delimited_tokenizer.hpp"
#include "iresearch/parser/parser.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/doc_collector.hpp"
#include "iresearch/search/mixed_boolean_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/tfidf.hpp"
#include "iresearch/types.hpp"
#include "tests_shared.hpp"

namespace {

// A field that tokenises its value by spaces (for multi-term documents).
class SpaceField : public tests::FieldBase {
 public:
  explicit SpaceField(std::string_view field_name)
    : _tokenizer{std::make_unique<irs::analysis::DelimitedTokenizer>(" ")} {
    name = field_name;
    index_features = irs::IndexFeatures::Freq | irs::IndexFeatures::Norm;
  }

  void value(std::string_view v) { _value = v; }

  irs::Tokenizer& GetTokens() const final {
    _tokenizer->reset(_value);
    return *_tokenizer;
  }

  bool Write(irs::DataOutput& o) const final {
    o.WriteByte(1);
    return true;
  }

 private:
  mutable std::unique_ptr<irs::analysis::DelimitedTokenizer> _tokenizer;
  std::string _value;
};

class BoostQueryTestCase : public tests::IndexTestBase {
 protected:
  // Collect all (score, doc_id) results sorted by score descending.
  std::vector<irs::ScoreDoc> CollectAll(const irs::DirectoryReader& reader,
                                        const irs::Filter& filter) {
    constexpr size_t kK = 1024;
    irs::TFIDF scorer{/*normalize=*/false};
    std::vector<irs::ScoreDoc> hits(irs::BlockSize(kK));
    size_t count =
      irs::ExecuteTopKWithCount(reader, filter, scorer, kK, std::span{hits});
    hits.resize(std::min(count, kK));
    return hits;
  }

  // Same shape as ExecuteTopKWithCount but takes an already-prepared Query
  // so the caller can choose between filter.prepare() and the
  // CreateBuffer+PrepareSegment+Compile pipeline.
  std::vector<irs::ScoreDoc> CollectAllUsingQuery(
    const irs::DirectoryReader& reader, const irs::Filter::Query& query,
    const irs::Scorer& scorer) {
    constexpr size_t kK = 1024;
    std::vector<irs::ScoreDoc> hits(irs::BlockSize(kK));
    irs::score_t threshold = std::numeric_limits<irs::score_t>::min();
    irs::NthPartitionScoreCollector collector{threshold, kK, std::span{hits}};
    irs::ColumnArgsFetcher fetcher;
    uint32_t seg_idx = 0;
    for (auto& segment : reader) {
      fetcher.Clear();
      collector.SetSegment(seg_idx++);
      auto it = query.execute({.segment = segment, .scorer = &scorer});
      auto score_func = it->PrepareScore({
        .scorer = &scorer,
        .segment = &segment,
        .fetcher = &fetcher,
      });
      it->Collect(score_func, fetcher, collector);
    }
    const auto count = collector.Finalize();
    hits.resize(std::min<size_t>(count, kK));
    return hits;
  }

  // Parse a Lucene query string into a MixedBooleanFilter.
  static irs::Filter::ptr ParseQuery(std::string_view query) {
    irs::analysis::DelimitedTokenizer tokenizer(" ");
    auto root = std::make_unique<irs::MixedBooleanFilter>();
    sdb::ParserContext ctx{*root, "content", tokenizer};
    EXPECT_TRUE(sdb::ParseQuery(ctx, query).ok());
    return root;
  }

  // Create a single-segment index with 4 documents:
  //   doc A - "open"                  (required only)
  //   doc B - "open source"           (required + 1 optional)
  //   doc C - "open source software"  (required + 2 optionals)
  //   doc D - "source software"       (no required term -> must be excluded)
  irs::DirectoryReader CreateIndex() {
    auto writer = open_writer(irs::kOmCreate);

    auto insert = [&](std::string_view content) {
      SpaceField f{"content"};
      f.value(content);
      auto batch = writer->GetBatch();
      auto d = batch.Insert();
      EXPECT_TRUE(d.Insert(f));
    };

    insert("open");
    insert("open source");
    insert("open source software");
    insert("source software");

    writer->RefreshCommit();

    auto reader = irs::DirectoryReader(dir(), codec());
    EXPECT_EQ(1, reader.size());
    return reader;
  }
};

// "+open source software" - only docs with "open" must be returned.
// Doc D ("source software") does NOT contain "open" and must be excluded.
TEST_P(BoostQueryTestCase, RequiredExcludesNonMatchingDocs) {
  auto reader = CreateIndex();
  auto filter = ParseQuery("+open source software");
  ASSERT_NE(nullptr, filter);

  auto hits = CollectAll(reader, *filter);
  ASSERT_EQ(3u, hits.size())
    << "Expected exactly 3 docs matching required 'open'";

  for (auto& [score, doc, _] : hits) {
    EXPECT_FALSE(irs::doc_limits::eof(doc));
    EXPECT_NE(irs::doc_limits::invalid(), doc);
    EXPECT_GT(score, 0.f);
  }
}

// Documents that match more optional terms must score higher.
// Score ordering (descending): doc C > doc B > doc A.
TEST_P(BoostQueryTestCase, MoreOptionalMatchesYieldHigherScore) {
  auto reader = CreateIndex();
  auto filter = ParseQuery("+open source software");
  ASSERT_NE(nullptr, filter);

  auto hits = CollectAll(reader, *filter);
  ASSERT_EQ(3u, hits.size());

  // hits[0] = doc C (2 optionals),
  // hits[1] = doc B (1 optional),
  // hits[2] = doc A (0)
  EXPECT_GT(hits[0].score, hits[1].score)
    << "doc with 2 optional matches should outscore doc with 1";
  EXPECT_GT(hits[1].score, hits[2].score)
    << "doc with 1 optional match should outscore doc with 0";
}

// "+open" with no optional terms: behaves like a simple term query.
TEST_P(BoostQueryTestCase, RequiredOnlyReturnsAllMatchingDocs) {
  auto reader = CreateIndex();
  auto filter = ParseQuery("+open");
  ASSERT_NE(nullptr, filter);

  auto hits = CollectAll(reader, *filter);
  ASSERT_EQ(3u, hits.size());
}

// "source software" with no required term: behaves like a plain OR disjunction.
TEST_P(BoostQueryTestCase, OptionalOnlyActsAsDisjunction) {
  auto reader = CreateIndex();
  auto filter = ParseQuery("source software");
  ASSERT_NE(nullptr, filter);

  auto hits = CollectAll(reader, *filter);
  // Docs B, C, D all contain at least one of "source" / "software"
  ASSERT_EQ(3u, hits.size());
}

// Regression: MixedBooleanFilter::Buffer must propagate the Or filter's
// own Boost() to every optional child -- the legacy BoostQuery::Prepare
// path does `opt_ctx = ctx.Boost(opt.Boost())`. The two paths must score
// each document identically.
TEST_P(BoostQueryTestCase, CreateBufferPreservesOrBoost) {
  auto reader = CreateIndex();

  auto build = []() {
    auto root = std::make_unique<irs::MixedBooleanFilter>();
    auto required = std::make_unique<irs::ByTerm>();
    *required->mutable_field() = "content";
    required->mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view{"open"});
    root->GetRequired().add(std::move(required));

    auto optional = std::make_unique<irs::ByTerm>();
    *optional->mutable_field() = "content";
    optional->mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view{"source"});
    root->GetOptional().add(std::move(optional));
    root->GetOptional().boost(2.5f);
    return root;
  };

  irs::TFIDF scorer{/*normalize=*/false};
  MaxMemoryCounter counter;
  const irs::PrepareContext ctx{
    .index = reader,
    .memory = counter,
    .scorer = &scorer,
  };

  auto filter_seq = build();
  auto query_seq = filter_seq->prepare(ctx);
  ASSERT_NE(nullptr, query_seq);

  auto filter_par = build();
  auto buf = filter_par->CreateBuffer(ctx);
  ASSERT_NE(nullptr, buf);
  for (auto& segment : reader) {
    buf->PrepareSegment(segment);
  }
  auto query_par = std::move(*buf).Compile(ctx);
  ASSERT_NE(nullptr, query_par);

  auto by_doc = [](auto& hits) {
    std::sort(hits.begin(), hits.end(),
              [](const auto& a, const auto& b) { return a.doc < b.doc; });
  };

  auto hits_seq = CollectAllUsingQuery(reader, *query_seq, scorer);
  auto hits_par = CollectAllUsingQuery(reader, *query_par, scorer);
  by_doc(hits_seq);
  by_doc(hits_par);

  ASSERT_EQ(hits_seq.size(), hits_par.size());
  for (size_t i = 0; i < hits_seq.size(); ++i) {
    EXPECT_EQ(hits_seq[i].doc, hits_par[i].doc);
    EXPECT_FLOAT_EQ(hits_seq[i].score, hits_par[i].score)
      << "doc " << hits_seq[i].doc
      << ": CreateBuffer path did not apply Or filter's Boost";
  }
}

// Programmatic construction of required/optional: verify same behavior as
// the parser path.
TEST_P(BoostQueryTestCase, ManualRequiredOptionalConstruction) {
  auto reader = CreateIndex();
  auto root = std::make_unique<irs::MixedBooleanFilter>();

  auto make_term = [](std::string_view value) {
    auto f = std::make_unique<irs::ByTerm>();
    *f->mutable_field() = "content";
    f->mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
    return f;
  };

  root->GetRequired().add(make_term("open"));
  root->GetOptional().add(make_term("source"));
  root->GetOptional().add(make_term("software"));

  auto hits = CollectAll(reader, *root);
  ASSERT_EQ(3u, hits.size());

  // Score ordering must be: C > B > A
  EXPECT_GT(hits[0].score, hits[1].score);
  EXPECT_GT(hits[1].score, hits[2].score);
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(boost_query_test, BoostQueryTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd")),
                         BoostQueryTestCase::to_string);

}  // namespace
