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

#include "index/index_tests.hpp"
#include "iresearch/analysis/analyzer.hpp"
#include "iresearch/analysis/delimited_tokenizer.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/parser/parser.h"
#include "iresearch/search/bm25.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/doc_collector.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/scorers.hpp"
#include "iresearch/search/tfidf.hpp"
#include "iresearch/types.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests;

void WandScoringFieldFactory(tests::Document& doc, const std::string& name,
                             const tests::JsonDocGenerator::JsonValue& data) {
  if (JsonDocGenerator::ValueType::STRING == data.vt) {
    doc.insert(std::make_shared<tests::StringField>(name, data.str,
                                                    irs::IndexFeatures::Norm));
  } else if (JsonDocGenerator::ValueType::NIL == data.vt) {
    doc.insert(std::make_shared<BinaryField>());
    auto& field = (doc.indexed.end() - 1).as<BinaryField>();
    field.Name(name);
    field.value(
      irs::ViewCast<irs::byte_type>(irs::NullTokenizer::value_null()));
  } else if (JsonDocGenerator::ValueType::BOOL == data.vt && data.b) {
    doc.insert(std::make_shared<BinaryField>());
    auto& field = (doc.indexed.end() - 1).as<BinaryField>();
    field.Name(name);
    field.value(
      irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value_true()));
  } else if (JsonDocGenerator::ValueType::BOOL == data.vt && !data.b) {
    doc.insert(std::make_shared<BinaryField>());
    auto& field = (doc.indexed.end() - 1).as<BinaryField>();
    field.Name(name);
    field.value(
      irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value_true()));
  } else if (data.is_number()) {
    doc.insert(std::make_shared<DoubleField>());
    auto& field = (doc.indexed.end() - 1).as<DoubleField>();
    field.Name(name);
    field.value(data.as_number<double_t>());
  }
}

class WandScoringTestCase : public IndexTestBase {
 protected:
  void WriteSegment(irs::IndexWriter& writer, auto& gens) {
    auto& index = const_cast<tests::index_t&>(this->index());
    for (auto& gen : gens) {
      index.emplace_back(writer.FeatureInfo());
      write_segment(writer, index.back(), gen);
    }
    writer.Commit();
  }

  // Single segment with multiplier * 420 docs.
  irs::DirectoryReader CreateLargeIndex(const irs::Scorer& scorer,
                                        size_t multiplier = 1) {
    const irs::Scorer* scorers[] = {&scorer};
    irs::IndexWriterOptions opts;
    opts.reader_options.scorers = scorers;
    opts.features = [](irs::IndexFeatures id) {
      irs::ColumnInfo info{irs::Type<irs::compression::None>::get(), {}, false};
      if (id == irs::IndexFeatures::Norm) {
        return std::make_pair(
          info, irs::FeatureWriterFactory{&irs::Norm::MakeWriter});
      }
      return std::make_pair(info, irs::FeatureWriterFactory{});
    };
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
    const irs::Scorer* scorers[] = {&scorer};
    irs::IndexWriterOptions opts;
    opts.reader_options.scorers = scorers;
    opts.features = [](irs::IndexFeatures id) {
      irs::ColumnInfo info{irs::Type<irs::compression::None>::get(), {}, false};
      if (id == irs::IndexFeatures::Norm) {
        return std::make_pair(
          info, irs::FeatureWriterFactory{&irs::Norm::MakeWriter});
      }
      return std::make_pair(info, irs::FeatureWriterFactory{});
    };
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
        index_ref.emplace_back(writer->FeatureInfo());
        write_segment(*writer, index_ref.back(), gen);
      }
      writer->Commit();
    }

    return writer->GetSnapshot();
  }

  irs::Filter::ptr ParseQuery(std::string_view query,
                              std::string_view default_field = "content") {
    if (!_tokenizer) {
      _tokenizer = std::make_unique<irs::analysis::DelimitedTokenizer>(" ");
    }
    auto root = std::make_unique<irs::MixedBooleanFilter>();
    sdb::ParserContext ctx{*root, default_field, *_tokenizer};
    auto result = sdb::ParseQuery(ctx, query);
    EXPECT_TRUE(result.ok()) << "Failed to parse query: " << query;
    return root;
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
    size_t wand_count =
      irs::ExecuteTopK(reader, filter, scorer, k, {.index = 0, .strict = true},
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
      EXPECT_EQ(std::get<irs::doc_id_t>(baseline_hits[i]),
                std::get<irs::doc_id_t>(wand_hits[i]))
        << "Doc ID mismatch at position " << i;
      EXPECT_FLOAT_EQ(std::get<irs::score_t>(baseline_hits[i]),
                      std::get<irs::score_t>(wand_hits[i]))
        << "Score mismatch at position " << i;
    }
  }

  void VerifyScoresAndDocs(auto docs, size_t result_count) {
    for (size_t i = 0; i < result_count; ++i) {
      EXPECT_GT(std::get<irs::score_t>(docs[i]), 0)
        << "Score at position " << i << " should be positive";
      if (i > 0) {
        EXPECT_GE(std::get<irs::score_t>(docs[i - 1]),
                  std::get<irs::score_t>(docs[i]))
          << "Scores should be in descending order at position " << i;
      }
      ASSERT_TRUE(!irs::doc_limits::eof(std::get<irs::doc_id_t>(docs[i])) &&
                  std::get<irs::doc_id_t>(docs[i]) !=
                    irs::doc_limits::invalid())
        << "Doc ID at position " << i << " should be valid, got " << docs[i];
    }
  }

  irs::analysis::Analyzer::ptr _tokenizer;
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

// BM25 with small k=3, 4200 docs (~2100 matching "tech" = ~16 blocks)
TEST_P(WandScoringTestCase, WandSmallK) {
  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto reader = CreateLargeIndex(scorer, 10);

  auto filter = ParseQuery("category:tech");
  ASSERT_NE(nullptr, filter);

  CompareWandVsNonWand(reader, *filter, scorer, 3);
}

// WAND with k larger than matches — no pruning expected
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

// k=1 — aggressive threshold, 4200 docs
TEST_P(WandScoringTestCase, WandKOne) {
  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto reader = CreateLargeIndex(scorer, 10);

  auto filter = ParseQuery("category:tech");
  ASSERT_NE(nullptr, filter);

  CompareWandVsNonWand(reader, *filter, scorer, 1);
}

// Multi-segment TFIDF, 3 segments × 1400 docs each
TEST_P(WandScoringTestCase, WandMultisegTfidf) {
  auto scorer = irs::TFIDF{true};
  auto reader = CreateMultiSegmentIndex(scorer, 10);
  ASSERT_EQ(3, reader.size());

  auto filter = ParseQuery("topic:database");
  ASSERT_NE(nullptr, filter);

  CompareWandVsNonWand(reader, *filter, scorer, 15);
}

// Multi-segment BM25, 3 segments × 1400 docs each
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
                     {.index = 0, .strict = true}, std::span{hits});
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
                     {.index = 0, .strict = true}, std::span{hits});
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
                     {.index = 0, .strict = true}, std::span{hits});
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
