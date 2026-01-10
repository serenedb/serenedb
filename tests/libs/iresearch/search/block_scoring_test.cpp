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
#include <iresearch/parser/parser.h>

#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/analysis/delimited_tokenizer.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/search/bm25.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/doc_collector.hpp>
#include <iresearch/search/scorers.hpp>
#include <iresearch/search/tfidf.hpp>
#include <iresearch/types.hpp>
#include <span>

#include "index/index_tests.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests;

// Field that uses delimiter tokenizer for comma-separated values
class DelimitedField : public tests::FieldBase {
 public:
  DelimitedField(std::string_view name, std::string_view delimiter)
    : _tokenizer(
        std::make_unique<irs::analysis::DelimitedTokenizer>(delimiter)) {
    this->name = name;
    index_features = irs::IndexFeatures::Freq;
  }

  void Value(std::string_view val) { _value = val; }

  irs::Tokenizer& GetTokens() const final {
    _tokenizer->reset(_value);
    return *_tokenizer;
  }

  bool Write(irs::DataOutput& o) const final {
    // TODO(gnusi): fix
    o.WriteByte(1);
    return true;
  }

 private:
  mutable std::unique_ptr<irs::analysis::DelimitedTokenizer> _tokenizer;
  std::string _value;
};

// Custom field factory that uses delimiter tokenizer for tags and content
// fields
void BlockScoringFieldFactory(tests::Document& doc, const std::string& name,
                              const tests::JsonDocGenerator::JsonValue& data) {
  if (JsonDocGenerator::ValueType::STRING == data.vt) {
    //    if (name == "tags") {
    //      // Use comma delimiter tokenizer for tags
    //      auto field = std::make_shared<DelimitedField>(name, ",");
    //      field->Value(data.str);
    //      doc.insert(std::move(field));
    //    } else if (name == "content") {
    //      // Use space delimiter tokenizer for content
    //      auto field = std::make_shared<DelimitedField>(name, " ");
    //      field->Value(data.str);
    //      doc.insert(std::move(field));
    //    } else {
    // Use standard string field for other fields
    doc.insert(std::make_shared<tests::StringField>(name, data.str));
    //    }
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
    // 'value' can be interpreted as a double
    doc.insert(std::make_shared<DoubleField>());
    auto& field = (doc.indexed.end() - 1).as<DoubleField>();
    field.Name(name);
    field.value(data.as_number<double_t>());
  }
}

class BlockScoringTestCase : public IndexTestBase {
 protected:
  void WriteSegment(irs::IndexWriter& writer, auto& gens) {
    auto& index = const_cast<tests::index_t&>(this->index());
    for (auto& gen : gens) {
      index.emplace_back(writer.FeatureInfo());
      write_segment(writer, index.back(), gen);
    }
    writer.Commit();
  }

  // Create single segment from multiple JSON files (420 total docs)
  void CreateLargeIndex() {
    auto writer = open_writer(irs::kOmCreate);

    std::vector<tests::JsonDocGenerator> gens;
    gens.emplace_back(resource("block_scoring_segment1.json"),
                      &BlockScoringFieldFactory);
    gens.emplace_back(resource("block_scoring_segment2.json"),
                      &BlockScoringFieldFactory);
    gens.emplace_back(resource("block_scoring_segment3.json"),
                      &BlockScoringFieldFactory);

    WriteSegment(*writer, gens);

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(1, reader.size()) << "Expected 1 segment";
  }

  // Create multi-segment index (3 segments with ~140 docs each)
  void CreateMultiSegmentIndex() {
    auto writer = open_writer(irs::kOmCreate);
    auto& index_ref = const_cast<tests::index_t&>(index());

    // Segment 1
    {
      tests::JsonDocGenerator gen(resource("block_scoring_segment1.json"),
                                  &BlockScoringFieldFactory);
      index_ref.emplace_back(writer->FeatureInfo());
      write_segment(*writer, index_ref.back(), gen);
      writer->Commit();
    }

    // Segment 2
    {
      tests::JsonDocGenerator gen(resource("block_scoring_segment2.json"),
                                  &BlockScoringFieldFactory);
      index_ref.emplace_back(writer->FeatureInfo());
      write_segment(*writer, index_ref.back(), gen);
      writer->Commit();
    }

    // Segment 3
    {
      tests::JsonDocGenerator gen(resource("block_scoring_segment3.json"),
                                  &BlockScoringFieldFactory);
      index_ref.emplace_back(writer->FeatureInfo());
      write_segment(*writer, index_ref.back(), gen);
      writer->Commit();
    }

    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_EQ(3, reader.size()) << "Expected 3 segments";
  }

  irs::Filter::ptr ParseQuery(std::string_view query,
                              std::string_view default_field = "content") {
    if (!_tokenizer) {
      _tokenizer = std::make_unique<irs::analysis::DelimitedTokenizer>(" ");
    }
    auto root = std::make_unique<irs::Or>();
    sdb::ParserContext ctx{*root, default_field, *_tokenizer};
    auto result = sdb::ParseQuery(ctx, query);
    EXPECT_TRUE(result.ok()) << "Failed to parse query: " << query;
    return root;
  }

  irs::analysis::Analyzer::ptr _tokenizer;
};

// Test TFIDF scorer with ByTerm filter using ExecuteTopKWithCount
TEST_P(BlockScoringTestCase, tfidf_byterm_block_scoring) {
  // CreateLargeIndex();
  CreateMultiSegmentIndex();

  auto scorer = irs::TFIDF{true};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  size_t total_docs = 0;
  for (auto& segment : reader) {
    total_docs += segment.docs_count();
  }
  ASSERT_GT(total_docs, 100);

  // Test filter for "database" in topic field using parser
  auto filter = ParseQuery("topic:database");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;
  std::vector<irs::score_t> scores(kTopK * 2);
  std::vector<irs::doc_id_t> docs(kTopK * 2);

  size_t count = irs::ExecuteTopKWithCount(
    reader, *filter, prepared_order, kTopK, std::span{scores}, std::span{docs});

  ASSERT_GT(count, 0) << "Expected matches for 'database' in topic field";
  auto result_count = std::min(count, kTopK);
  ASSERT_LE(result_count, kTopK);

  // Verify scores are positive and in descending order
  for (size_t i = 1; i < result_count; ++i) {
    EXPECT_GE(scores[i - 1], scores[i])
      << "Scores should be in descending order";
  }
  for (size_t i = 0; i < result_count; ++i) {
    EXPECT_GT(scores[i], 0) << "TFIDF scores should be positive";
  }
}

// Test TFIDF with topic field search (many matches)
TEST_P(BlockScoringTestCase, tfidf_topic_search) {
  CreateLargeIndex();

  auto scorer = irs::TFIDF{true};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());

  // Search for "physics" in topic field (has many matches)
  auto filter = ParseQuery("topic:physics");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 20;
  std::vector<irs::score_t> scores(kTopK * 2);
  std::vector<irs::doc_id_t> docs(kTopK * 2);

  size_t count = irs::ExecuteTopKWithCount(
    reader, *filter, prepared_order, kTopK, std::span{scores}, std::span{docs});

  ASSERT_GT(count, 5) << "Expected multiple matches for 'physics' in topic";
  auto result_count = std::min(count, kTopK);

  // Verify ordering
  for (size_t i = 1; i < result_count; ++i) {
    EXPECT_GE(scores[i - 1], scores[i]);
  }
}

// Test BM25 scorer with ByTerm filter using ExecuteTopKWithCount
TEST_P(BlockScoringTestCase, bm25_byterm_block_scoring) {
  CreateLargeIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());

  // Test filter for "search" in topic field using parser
  auto filter = ParseQuery("topic:search");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 15;
  std::vector<irs::score_t> scores(kTopK * 2);
  std::vector<irs::doc_id_t> docs(kTopK * 2);

  size_t count = irs::ExecuteTopKWithCount(
    reader, *filter, prepared_order, kTopK, std::span{scores}, std::span{docs});

  ASSERT_GT(count, 0) << "Expected matches for 'search' in topic field";
  auto result_count = std::min(count, kTopK);

  // Verify scores are in descending order
  for (size_t i = 1; i < result_count; ++i) {
    EXPECT_GE(scores[i - 1], scores[i])
      << "BM25 scores should be in descending order";
  }
}

// Test BM25 with chemistry topic (for document scoring)
TEST_P(BlockScoringTestCase, bm25_chemistry_search) {
  CreateLargeIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());

  // Search for "chemistry" in topic field
  auto filter = ParseQuery("topic:chemistry");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;
  std::vector<irs::score_t> scores(kTopK * 2);
  std::vector<irs::doc_id_t> docs(kTopK * 2);

  size_t count = irs::ExecuteTopKWithCount(
    reader, *filter, prepared_order, kTopK, std::span{scores}, std::span{docs});

  ASSERT_GT(count, 0);
  auto result_count = std::min(count, kTopK);

  // BM25 should produce scores - verify they are valid
  for (size_t i = 0; i < result_count; ++i) {
    EXPECT_GT(scores[i], 0) << "BM25 scores should be positive";
  }
  // Verify descending order
  for (size_t i = 1; i < result_count; ++i) {
    EXPECT_GE(scores[i - 1], scores[i]);
  }
}

// Test And filter with TFIDF using ExecuteTopKWithCount
TEST_P(BlockScoringTestCase, tfidf_and_filter_block_scoring) {
  CreateLargeIndex();

  auto scorer = irs::TFIDF{true};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());

  // Create AND filter using parser: category:tech AND topic:database
  auto filter = ParseQuery("+category:tech +topic:database");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;
  std::vector<irs::score_t> scores(kTopK * 2);
  std::vector<irs::doc_id_t> docs(kTopK * 2);

  size_t count = irs::ExecuteTopKWithCount(
    reader, *filter, prepared_order, kTopK, std::span{scores}, std::span{docs});

  ASSERT_GT(count, 0) << "Expected matches for tech AND database";
  auto result_count = std::min(count, kTopK);

  // Verify scores are in descending order
  for (size_t i = 1; i < result_count; ++i) {
    EXPECT_GE(scores[i - 1], scores[i]);
  }
}

// Test And filter with BM25 using ExecuteTopKWithCount
TEST_P(BlockScoringTestCase, bm25_and_filter_block_scoring) {
  CreateLargeIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());

  // Create AND filter using parser: category:science AND topic:physics
  auto filter = ParseQuery("+category:science +topic:physics");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 15;
  std::vector<irs::score_t> scores(kTopK * 2);
  std::vector<irs::doc_id_t> docs(kTopK * 2);

  size_t count = irs::ExecuteTopKWithCount(
    reader, *filter, prepared_order, kTopK, std::span{scores}, std::span{docs});

  ASSERT_GT(count, 0) << "Expected matches for science AND physics";
  auto result_count = std::min(count, kTopK);

  for (size_t i = 1; i < result_count; ++i) {
    EXPECT_GE(scores[i - 1], scores[i]);
  }
}

// Test block boundaries with small k
TEST_P(BlockScoringTestCase, block_boundary_small_k) {
  CreateLargeIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());

  // Use small k to force multiple block iterations
  auto filter = ParseQuery("category:tech");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 3;
  std::vector<irs::score_t> scores(kTopK * 2);
  std::vector<irs::doc_id_t> docs(kTopK * 2);

  size_t count = irs::ExecuteTopKWithCount(
    reader, *filter, prepared_order, kTopK, std::span{scores}, std::span{docs});

  // Should have many tech documents, k=3 should trigger multiple blocks
  ASSERT_GT(count, kTopK * 2)
    << "Expected many tech documents for block testing";
  auto result_count = std::min(count, kTopK);
  ASSERT_EQ(kTopK, result_count);

  for (size_t i = 1; i < result_count; ++i) {
    EXPECT_GE(scores[i - 1], scores[i]);
  }
}

// Test block boundaries with larger k
TEST_P(BlockScoringTestCase, block_boundary_large_k) {
  CreateLargeIndex();

  auto scorer = irs::TFIDF{true};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());

  // Use larger k
  auto filter = ParseQuery("category:science");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 50;
  std::vector<irs::score_t> scores(kTopK * 2);
  std::vector<irs::doc_id_t> docs(kTopK * 2);

  size_t count = irs::ExecuteTopKWithCount(
    reader, *filter, prepared_order, kTopK, std::span{scores}, std::span{docs});

  ASSERT_GT(count, 0);
  auto result_count = std::min(count, kTopK);

  for (size_t i = 1; i < result_count; ++i) {
    EXPECT_GE(scores[i - 1], scores[i]);
  }
}

// Test TFIDF vs BM25 score comparison
TEST_P(BlockScoringTestCase, tfidf_vs_bm25_comparison) {
  CreateLargeIndex();

  auto tfidf_scorer = irs::TFIDF{true};
  auto bm25_scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto tfidf_order = irs::Scorers::Prepare(tfidf_scorer);
  auto bm25_order = irs::Scorers::Prepare(bm25_scorer);

  auto reader = irs::DirectoryReader(dir(), codec());

  auto filter = ParseQuery("topic:search");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;

  // Get TFIDF results
  std::vector<irs::score_t> tfidf_scores(kTopK * 2);
  std::vector<irs::doc_id_t> tfidf_docs(kTopK * 2);
  size_t tfidf_count =
    irs::ExecuteTopKWithCount(reader, *filter, tfidf_order, kTopK,
                              std::span{tfidf_scores}, std::span{tfidf_docs});

  // Get BM25 results
  std::vector<irs::score_t> bm25_scores(kTopK * 2);
  std::vector<irs::doc_id_t> bm25_docs(kTopK * 2);
  size_t bm25_count =
    irs::ExecuteTopKWithCount(reader, *filter, bm25_order, kTopK,
                              std::span{bm25_scores}, std::span{bm25_docs});

  // Both should return the same number of matching documents
  ASSERT_EQ(tfidf_count, bm25_count);
  ASSERT_GT(tfidf_count, 0);

  auto result_count = std::min(tfidf_count, kTopK);

  // Both should produce valid sorted results
  for (size_t i = 1; i < result_count; ++i) {
    EXPECT_GE(tfidf_scores[i - 1], tfidf_scores[i]);
    EXPECT_GE(bm25_scores[i - 1], bm25_scores[i]);
  }
}

// Test with k larger than matching documents
TEST_P(BlockScoringTestCase, k_larger_than_matches) {
  CreateLargeIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());

  // Search for chemistry topic (fewer matches)
  auto filter = ParseQuery("topic:chemistry");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 1000;  // Much larger than chemistry documents
  std::vector<irs::score_t> scores(kTopK * 2);
  std::vector<irs::doc_id_t> docs(kTopK * 2);

  size_t count = irs::ExecuteTopKWithCount(
    reader, *filter, prepared_order, kTopK, std::span{scores}, std::span{docs});

  ASSERT_GT(count, 0);
  ASSERT_LT(count, kTopK) << "Should have fewer chemistry docs than k";
  auto result_count = std::min(count, kTopK);

  for (size_t i = 1; i < result_count; ++i) {
    EXPECT_GE(scores[i - 1], scores[i]);
  }
}

// Test empty result set
TEST_P(BlockScoringTestCase, empty_result_set) {
  CreateLargeIndex();

  auto scorer = irs::TFIDF{true};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());

  // Search for non-existent term
  auto filter = ParseQuery("xyznonexistent123");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;
  std::vector<irs::score_t> scores(kTopK * 2);
  std::vector<irs::doc_id_t> docs(kTopK * 2);

  size_t count = irs::ExecuteTopKWithCount(
    reader, *filter, prepared_order, kTopK, std::span{scores}, std::span{docs});

  ASSERT_EQ(0, count);
}

// Test And filter with three clauses
TEST_P(BlockScoringTestCase, and_filter_three_clauses) {
  CreateLargeIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());

  // Create AND filter with three clauses using parser
  auto filter = ParseQuery("+category:tech +topic:database +tags:index");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;
  std::vector<irs::score_t> scores(kTopK * 2);
  std::vector<irs::doc_id_t> docs(kTopK * 2);

  size_t count = irs::ExecuteTopKWithCount(
    reader, *filter, prepared_order, kTopK, std::span{scores}, std::span{docs});

  // Some documents should match all three conditions
  auto result_count = std::min(count, kTopK);

  for (size_t i = 1; i < result_count; ++i) {
    EXPECT_GE(scores[i - 1], scores[i]);
  }
}

// Test BM25 with different k and b parameters
TEST_P(BlockScoringTestCase, bm25_parameter_variations) {
  CreateLargeIndex();

  auto reader = irs::DirectoryReader(dir(), codec());

  auto filter = ParseQuery("content:database");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;

  // Test with default BM25 (k=1.2, b=0.75)
  {
    auto scorer = irs::BM25{};
    auto prepared_order = irs::Scorers::Prepare(scorer);

    std::vector<irs::score_t> scores(kTopK * 2);
    std::vector<irs::doc_id_t> docs(kTopK * 2);

    size_t count =
      irs::ExecuteTopKWithCount(reader, *filter, prepared_order, kTopK,
                                std::span{scores}, std::span{docs});

    ASSERT_GT(count, 0);
  }

  // Test with BM15 (b=0)
  {
    auto scorer = irs::BM25{irs::BM25::K(), 0.0f};
    auto prepared_order = irs::Scorers::Prepare(scorer);

    std::vector<irs::score_t> scores(kTopK * 2);
    std::vector<irs::doc_id_t> docs(kTopK * 2);

    size_t count =
      irs::ExecuteTopKWithCount(reader, *filter, prepared_order, kTopK,
                                std::span{scores}, std::span{docs});

    ASSERT_GT(count, 0);
    auto result_count = std::min(count, kTopK);

    for (size_t i = 1; i < result_count; ++i) {
      EXPECT_GE(scores[i - 1], scores[i]);
    }
  }

  // Test with BM11 (b=1)
  {
    auto scorer = irs::BM25{irs::BM25::K(), 1.0f};
    auto prepared_order = irs::Scorers::Prepare(scorer);

    std::vector<irs::score_t> scores(kTopK * 2);
    std::vector<irs::doc_id_t> docs(kTopK * 2);

    size_t count =
      irs::ExecuteTopKWithCount(reader, *filter, prepared_order, kTopK,
                                std::span{scores}, std::span{docs});

    ASSERT_GT(count, 0);
    auto result_count = std::min(count, kTopK);

    for (size_t i = 1; i < result_count; ++i) {
      EXPECT_GE(scores[i - 1], scores[i]);
    }
  }
}

// Test TFIDF with and without norms
TEST_P(BlockScoringTestCase, tfidf_with_without_norms) {
  CreateLargeIndex();

  auto reader = irs::DirectoryReader(dir(), codec());

  auto filter = ParseQuery("content:index");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;

  // Test with norms
  {
    auto scorer = irs::TFIDF{true};
    auto prepared_order = irs::Scorers::Prepare(scorer);

    std::vector<irs::score_t> scores_with_norms(kTopK * 2);
    std::vector<irs::doc_id_t> docs_with_norms(kTopK * 2);

    size_t count = irs::ExecuteTopKWithCount(
      reader, *filter, prepared_order, kTopK, std::span{scores_with_norms},
      std::span{docs_with_norms});

    ASSERT_GT(count, 0);
  }

  // Test without norms
  {
    auto scorer = irs::TFIDF{false};
    auto prepared_order = irs::Scorers::Prepare(scorer);

    std::vector<irs::score_t> scores_without_norms(kTopK * 2);
    std::vector<irs::doc_id_t> docs_without_norms(kTopK * 2);

    size_t count = irs::ExecuteTopKWithCount(
      reader, *filter, prepared_order, kTopK, std::span{scores_without_norms},
      std::span{docs_without_norms});

    ASSERT_GT(count, 0);
    auto result_count = std::min(count, kTopK);

    for (size_t i = 1; i < result_count; ++i) {
      EXPECT_GE(scores_without_norms[i - 1], scores_without_norms[i]);
    }
  }
}

// Multi-segment tests - TFIDF with ByTerm filter
TEST_P(BlockScoringTestCase, multiseg_tfidf_byterm) {
  CreateMultiSegmentIndex();

  auto scorer = irs::TFIDF{true};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());

  // Verify we have multiple segments
  ASSERT_EQ(3, reader.size()) << "Expected 3 segments";

  size_t total_docs = 0;
  for (auto& segment : reader) {
    total_docs += segment.docs_count();
  }
  ASSERT_EQ(420, total_docs) << "Expected 420 total documents";

  // Use parser to create query for "database"
  auto filter = ParseQuery("content:database");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 15;
  std::vector<irs::score_t> scores(kTopK * 2);
  std::vector<irs::doc_id_t> docs(kTopK * 2);

  size_t count = irs::ExecuteTopKWithCount(
    reader, *filter, prepared_order, kTopK, std::span{scores}, std::span{docs});

  ASSERT_GT(count, 0);
  auto result_count = std::min(count, kTopK);

  for (size_t i = 0; i < result_count; ++i) {
    EXPECT_GT(scores[i], 0) << "Scores should be positive";
  }
  for (size_t i = 1; i < result_count; ++i) {
    EXPECT_GE(scores[i - 1], scores[i]) << "Scores should be descending";
  }
}

// Multi-segment tests - BM25 with ByTerm filter
TEST_P(BlockScoringTestCase, multiseg_bm25_byterm) {
  CreateMultiSegmentIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(3, reader.size());

  // Use parser to create query for "search"
  auto filter = ParseQuery("topic:search");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 20;
  std::vector<irs::score_t> scores(kTopK * 2);
  std::vector<irs::doc_id_t> docs(kTopK * 2);

  size_t count = irs::ExecuteTopKWithCount(
    reader, *filter, prepared_order, kTopK, std::span{scores}, std::span{docs});

  ASSERT_GT(count, 0);
  auto result_count = std::min(count, kTopK);

  for (size_t i = 0; i < result_count; ++i) {
    EXPECT_GT(scores[i], 0);
  }
  for (size_t i = 1; i < result_count; ++i) {
    EXPECT_GE(scores[i - 1], scores[i]);
  }
}

// Multi-segment tests - And filter with TFIDF
TEST_P(BlockScoringTestCase, multiseg_tfidf_and_filter) {
  CreateMultiSegmentIndex();

  auto scorer = irs::TFIDF{true};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(3, reader.size());

  // AND filter: category:tech AND topic:database
  auto filter = ParseQuery("+category:tech +topic:database");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 15;
  std::vector<irs::score_t> scores(kTopK * 2);
  std::vector<irs::doc_id_t> docs(kTopK * 2);

  size_t count = irs::ExecuteTopKWithCount(
    reader, *filter, prepared_order, kTopK, std::span{scores}, std::span{docs});

  ASSERT_GT(count, 0) << "Expected matches for tech AND database";
  auto result_count = std::min(count, kTopK);

  for (size_t i = 1; i < result_count; ++i) {
    EXPECT_GE(scores[i - 1], scores[i]);
  }
}

// Multi-segment tests - And filter with BM25
TEST_P(BlockScoringTestCase, multiseg_bm25_and_filter) {
  CreateMultiSegmentIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(3, reader.size());

  // AND filter: category:science AND topic:physics
  auto filter = ParseQuery("+category:science +topic:physics");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 20;
  std::vector<irs::score_t> scores(kTopK * 2);
  std::vector<irs::doc_id_t> docs(kTopK * 2);

  size_t count = irs::ExecuteTopKWithCount(
    reader, *filter, prepared_order, kTopK, std::span{scores}, std::span{docs});

  ASSERT_GT(count, 0) << "Expected matches for science AND physics";
  auto result_count = std::min(count, kTopK);

  for (size_t i = 1; i < result_count; ++i) {
    EXPECT_GE(scores[i - 1], scores[i]);
  }
}

// Multi-segment tests - small k forcing multiple blocks across segments
TEST_P(BlockScoringTestCase, multiseg_small_k_block_boundaries) {
  CreateMultiSegmentIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(3, reader.size());

  // Use parser for query - "index" appears frequently
  auto filter = ParseQuery("content:index");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 5;  // Small k to test block boundaries
  std::vector<irs::score_t> scores(kTopK * 2);
  std::vector<irs::doc_id_t> docs(kTopK * 2);

  size_t count = irs::ExecuteTopKWithCount(
    reader, *filter, prepared_order, kTopK, std::span{scores}, std::span{docs});

  ASSERT_GT(count, kTopK * 2) << "Need many matches to test block boundaries";
  auto result_count = std::min(count, kTopK);
  ASSERT_EQ(kTopK, result_count);

  for (size_t i = 0; i < result_count; ++i) {
    EXPECT_GT(scores[i], 0);
  }
  for (size_t i = 1; i < result_count; ++i) {
    EXPECT_GE(scores[i - 1], scores[i]);
  }
}

// Multi-segment tests - verify results with quantum query
TEST_P(BlockScoringTestCase, multiseg_quantum_query) {
  CreateMultiSegmentIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(3, reader.size());

  auto filter = ParseQuery("content:quantum");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;
  std::vector<irs::score_t> scores(kTopK * 2);
  std::vector<irs::doc_id_t> docs(kTopK * 2);

  size_t count = irs::ExecuteTopKWithCount(
    reader, *filter, prepared_order, kTopK, std::span{scores}, std::span{docs});

  ASSERT_GT(count, 0);
  auto result_count = std::min(count, kTopK);

  // Verify multi-segment results are valid
  for (size_t i = 0; i < result_count; ++i) {
    EXPECT_GT(scores[i], 0) << "Multi-seg scores should be positive";
  }
  for (size_t i = 1; i < result_count; ++i) {
    EXPECT_GE(scores[i - 1], scores[i])
      << "Multi-seg scores should be descending";
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(block_scoring_test, BlockScoringTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd",
                                                              "1_5avx")),
                         BlockScoringTestCase::to_string);

}  // namespace
