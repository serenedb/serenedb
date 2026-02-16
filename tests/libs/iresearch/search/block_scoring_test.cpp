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
#include "iresearch/search/mixed_boolean_filter.hpp"
#include "iresearch/search/scorers.hpp"
#include "iresearch/search/tfidf.hpp"
#include "iresearch/types.hpp"
#include "tests_shared.hpp"

namespace {

// TODO(gnusi): try
// * Move collect from score to doc_iterator -> remove virtual call
// * Why does block disjunction work slower with bigger blocks?
// * Implement new score for small/heap disjunction
//   - Add collect
// * Fix block conjunction
// * Pass score_block via execution context to every iterator
//   - try to make it compile time or at least known per iterator -> optimize
//   loops
//   - use power of 2 blocks for scoring
// * Can we add special collec to to make block disjunction faster?
// * Don't use adapters
// * Simplify scorers (with templates)
// * Try merge scores in block disjunction without hash map

// TODO(gnusi):
// * Use delimited field
// * Check phrase
// * Check exact doc ids
// * Use norms
// * Check exact count number
// * Check exact score values (use custom scorers)
// * Bigger indexes for block disjunction (> 4096 docs)
// * Add test for nested cases

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
  // Helper to check if doc ID is valid
  static bool IsValidDoc(auto& doc) {
    return !irs::doc_limits::eof(doc.first) &&
           doc.first != irs::doc_limits::invalid();
  }

  // Helper to read string column value for a document
  std::string ReadColumnValue(const irs::SubReader& segment,
                              std::string_view column_name, auto& doc) {
    if (!IsValidDoc(doc)) {
      return {};
    }
    auto* column = segment.column(column_name);
    if (!column) {
      return {};
    }
    auto values = column->iterator(irs::ColumnHint::Normal);
    if (!values) {
      return {};
    }
    auto* payload = irs::get<irs::PayAttr>(*values);
    if (!payload) {
      return {};
    }
    if (doc.first != values->seek(doc.first)) {
      return {};
    }
    return irs::ToString<std::string>(payload->value.data());
  }

  // Helper to read double column value for a document (for "seq" field)
  std::optional<double> ReadDoubleColumn(const irs::SubReader& segment,
                                         std::string_view column_name,
                                         irs::doc_id_t doc) {
    auto* column = segment.column(column_name);
    if (!column) {
      return std::nullopt;
    }
    auto values = column->iterator(irs::ColumnHint::Normal);
    if (!values) {
      return std::nullopt;
    }
    auto* payload = irs::get<irs::PayAttr>(*values);
    if (!payload) {
      return std::nullopt;
    }
    if (doc != values->seek(doc)) {
      return std::nullopt;
    }
    // Double values are stored as 8 bytes
    if (payload->value.size() < sizeof(double)) {
      return std::nullopt;
    }
    double val;
    std::memcpy(&val, payload->value.data(), sizeof(double));
    return val;
  }

  // Helper to verify a doc has expected value in any segment (for multi-seg)
  bool VerifyDocValueInAnySegment(const irs::DirectoryReader& reader, auto& doc,
                                  std::string_view column_name,
                                  std::string_view expected_value) {
    if (!IsValidDoc(doc)) {
      return false;
    }
    for (auto& segment : reader) {
      auto value = ReadColumnValue(segment, column_name, doc);
      if (value == expected_value) {
        return true;
      }
    }
    return false;
  }

  // Helper to verify scores are positive, descending, and doc IDs are valid
  void VerifyScoresAndDocs(auto docs, size_t result_count) {
    for (size_t i = 0; i < result_count; ++i) {
      EXPECT_GT(docs[i].second, 0)
        << "Score at position " << i << " should be positive";
      if (i > 0) {
        EXPECT_GE(docs[i - 1].second, docs[i].second)
          << "Scores should be in descending order at position " << i;
      }
      ASSERT_TRUE(IsValidDoc(docs[i]))
        << "Doc ID at position " << i << " should be valid, got " << docs[i];
    }
  }

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
    auto root = std::make_unique<irs::MixedBooleanFilter>();
    sdb::ParserContext ctx{*root, default_field, *_tokenizer};
    auto result = sdb::ParseQuery(ctx, query);
    EXPECT_TRUE(result.ok()) << "Failed to parse query: " << query;
    return root;
  }

  irs::analysis::Analyzer::ptr _tokenizer;
};

// Test TFIDF scorer with ByTerm filter using ExecuteTopKWithCount
TEST_P(BlockScoringTestCase, TfidfBytermBlockScoring) {
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
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  ASSERT_GT(count, 0) << "Expected matches for 'database' in topic field";
  auto result_count = std::min(count, kTopK);
  ASSERT_LE(result_count, kTopK);

  VerifyScoresAndDocs(docs, result_count);
}

// Test TFIDF with topic field search (many matches) - verify actual values
TEST_P(BlockScoringTestCase, TfidfTopicSearch) {
  CreateLargeIndex();

  auto scorer = irs::TFIDF{true};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(1, reader.size()) << "Expected single segment for value checks";
  auto& segment = reader[0];

  // Search for "physics" in topic field (has many matches)
  auto filter = ParseQuery("topic:physics");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 20;
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  ASSERT_GT(count, 5) << "Expected multiple matches for 'physics' in topic";
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(docs, result_count);

  // Verify actual values - all returned docs should have topic="physics"
  for (size_t i = 0; i < result_count; ++i) {
    auto topic = ReadColumnValue(segment, "topic", docs[i]);
    EXPECT_EQ("physics", topic)
      << "Doc " << docs[i] << " should have topic='physics'";
  }
}

// Test BM25 scorer with ByTerm filter using ExecuteTopKWithCount
TEST_P(BlockScoringTestCase, Bm25BytermBlockScoring) {
  CreateLargeIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(1, reader.size()) << "Expected single segment for value checks";
  auto& segment = reader[0];

  // Test filter for "search" in topic field using parser
  auto filter = ParseQuery("topic:search");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 15;
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  ASSERT_GT(count, 0) << "Expected matches for 'search' in topic field";
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(docs, result_count);

  // Verify actual values - all returned docs should have topic="search"
  for (size_t i = 0; i < result_count; ++i) {
    auto topic = ReadColumnValue(segment, "topic", docs[i]);
    EXPECT_EQ("search", topic)
      << "Doc " << docs[i] << " should have topic='search'";
  }
}

// Test BM25 with chemistry topic (for document scoring) - verify actual values
TEST_P(BlockScoringTestCase, Bm25ChemistrySearch) {
  CreateLargeIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(1, reader.size()) << "Expected single segment for value checks";
  auto& segment = reader[0];

  // Search for "chemistry" in topic field
  auto filter = ParseQuery("topic:chemistry");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  ASSERT_GT(count, 0);
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(docs, result_count);

  // Verify actual values - all returned docs should have topic="chemistry"
  for (size_t i = 0; i < result_count; ++i) {
    auto topic = ReadColumnValue(segment, "topic", docs[i]);
    EXPECT_EQ("chemistry", topic)
      << "Doc " << docs[i] << " should have topic='chemistry'";
  }
}

// Test And filter with TFIDF using ExecuteTopKWithCount - verify both
// conditions
TEST_P(BlockScoringTestCase, TfidfAndFilterBlockScoring) {
  CreateLargeIndex();

  auto scorer = irs::TFIDF{true};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(1, reader.size()) << "Expected single segment for value checks";
  auto& segment = reader[0];

  // Create AND filter using parser: category:tech AND topic:database
  auto filter = ParseQuery("+category:tech +topic:database");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  ASSERT_GT(count, 0) << "Expected matches for tech AND database";
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(docs, result_count);

  // Verify actual values - all docs should match both conditions
  for (size_t i = 0; i < result_count; ++i) {
    auto category = ReadColumnValue(segment, "category", docs[i]);
    auto topic = ReadColumnValue(segment, "topic", docs[i]);
    EXPECT_EQ("tech", category)
      << "Doc " << docs[i] << " should have category='tech'";
    EXPECT_EQ("database", topic)
      << "Doc " << docs[i] << " should have topic='database'";
  }
}

// Test And filter with BM25 using ExecuteTopKWithCount - verify both conditions
TEST_P(BlockScoringTestCase, Bm25AndFilterBlockScoring) {
  CreateLargeIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(1, reader.size()) << "Expected single segment for value checks";
  auto& segment = reader[0];

  // Create AND filter using parser: category:science AND topic:physics
  auto filter = ParseQuery("+category:science +topic:physics");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 15;
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  ASSERT_GT(count, 0) << "Expected matches for science AND physics";
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(docs, result_count);

  // Verify actual values - all docs should match both conditions
  for (size_t i = 0; i < result_count; ++i) {
    auto category = ReadColumnValue(segment, "category", docs[i]);
    auto topic = ReadColumnValue(segment, "topic", docs[i]);
    EXPECT_EQ("science", category)
      << "Doc " << docs[i] << " should have category='science'";
    EXPECT_EQ("physics", topic)
      << "Doc " << docs[i] << " should have topic='physics'";
  }
}

// Test block boundaries with small k - verify values
TEST_P(BlockScoringTestCase, BlockBoundarySmallK) {
  CreateLargeIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(1, reader.size()) << "Expected single segment for value checks";
  auto& segment = reader[0];

  // Use small k to force multiple block iterations
  auto filter = ParseQuery("category:tech");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 3;
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  // Should have many tech documents, k=3 should trigger multiple blocks
  ASSERT_GT(count, kTopK * 2)
    << "Expected many tech documents for block testing";
  auto result_count = std::min(count, kTopK);
  ASSERT_EQ(kTopK, result_count);

  VerifyScoresAndDocs(docs, result_count);

  // Verify all returned docs have category='tech'
  for (size_t i = 0; i < result_count; ++i) {
    auto category = ReadColumnValue(segment, "category", docs[i]);
    EXPECT_EQ("tech", category)
      << "Doc " << docs[i] << " should have category='tech'";
  }
}

// Test block boundaries with larger k - verify values
TEST_P(BlockScoringTestCase, BlockBoundaryLargeK) {
  CreateLargeIndex();

  auto scorer = irs::TFIDF{true};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(1, reader.size()) << "Expected single segment for value checks";
  auto& segment = reader[0];

  // Use larger k
  auto filter = ParseQuery("category:science");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 50;
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  ASSERT_GT(count, 0);
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(docs, result_count);

  // Verify all returned docs have category='science'
  for (size_t i = 0; i < result_count; ++i) {
    auto category = ReadColumnValue(segment, "category", docs[i]);
    EXPECT_EQ("science", category)
      << "Doc " << docs[i] << " should have category='science'";
  }
}

// Test TFIDF vs BM25 score comparison
TEST_P(BlockScoringTestCase, TfidfVsBm25Comparison) {
  CreateLargeIndex();

  auto tfidf_scorer = irs::TFIDF{true};
  auto bm25_scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto tfidf_order = irs::Scorers::Prepare(tfidf_scorer);
  auto bm25_order = irs::Scorers::Prepare(bm25_scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(1, reader.size()) << "Expected single segment for value checks";
  auto& segment = reader[0];

  auto filter = ParseQuery("topic:search");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;

  // Get TFIDF results
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> tfidf_docs(
    irs::BlockSize(kTopK));
  size_t tfidf_count = irs::ExecuteTopKWithCount(reader, *filter, tfidf_order,
                                                 kTopK, std::span{tfidf_docs});

  // Get BM25 results
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> bm25_docs(
    irs::BlockSize(kTopK));
  size_t bm25_count = irs::ExecuteTopKWithCount(reader, *filter, bm25_order,
                                                kTopK, std::span{bm25_docs});

  // Both should return the same number of matching documents
  ASSERT_EQ(tfidf_count, bm25_count);
  ASSERT_GT(tfidf_count, 0);

  auto result_count = std::min(tfidf_count, kTopK);

  // Both should produce valid sorted results
  VerifyScoresAndDocs(tfidf_docs, result_count);
  VerifyScoresAndDocs(bm25_docs, result_count);

  // Verify actual values - all returned docs should have topic="search"
  for (size_t i = 0; i < result_count; ++i) {
    auto topic = ReadColumnValue(segment, "topic", tfidf_docs[i]);
    EXPECT_EQ("search", topic)
      << "TFIDF doc " << tfidf_docs[i] << " should have topic='search'";
  }
  for (size_t i = 0; i < result_count; ++i) {
    auto topic = ReadColumnValue(segment, "topic", bm25_docs[i]);
    EXPECT_EQ("search", topic)
      << "BM25 doc " << bm25_docs[i] << " should have topic='search'";
  }
}

// Test with k larger than matching documents - verify values
TEST_P(BlockScoringTestCase, KLargerThanMatches) {
  CreateLargeIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(1, reader.size()) << "Expected single segment for value checks";
  auto& segment = reader[0];

  // Search for chemistry topic (fewer matches)
  auto filter = ParseQuery("topic:chemistry");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 1000;  // Much larger than chemistry documents
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  ASSERT_GT(count, 0);
  ASSERT_LT(count, kTopK) << "Should have fewer chemistry docs than k";
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(docs, result_count);

  // Verify all returned docs have topic='chemistry'
  for (size_t i = 0; i < result_count; ++i) {
    auto topic = ReadColumnValue(segment, "topic", docs[i]);
    EXPECT_EQ("chemistry", topic)
      << "Doc " << docs[i] << " should have topic='chemistry'";
  }
}

// Test empty result set
TEST_P(BlockScoringTestCase, EmptyResultSet) {
  CreateLargeIndex();

  auto scorer = irs::TFIDF{true};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());

  // Search for non-existent term
  auto filter = ParseQuery("xyznonexistent123");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  ASSERT_EQ(0, count);
}

// Test And filter with three clauses - verify all three conditions
TEST_P(BlockScoringTestCase, AndFilterThreeClauses) {
  CreateLargeIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(1, reader.size()) << "Expected single segment for value checks";
  auto& segment = reader[0];

  // Create AND filter with three clauses using parser
  // Note: tags field stores full string (e.g., "search,index"), so we search
  // for exact match
  auto filter =
    ParseQuery("+category:tech +topic:database +tags:search +tags:index");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  // Some documents should match all three conditions
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(docs, result_count);

  // Verify actual values - all docs should match all three conditions
  for (size_t i = 0; i < result_count; ++i) {
    auto category = ReadColumnValue(segment, "category", docs[i]);
    auto topic = ReadColumnValue(segment, "topic", docs[i]);
    auto tags = ReadColumnValue(segment, "tags", docs[i]);
    EXPECT_EQ("tech", category)
      << "Doc " << docs[i] << " should have category='tech'";
    EXPECT_EQ("database", topic)
      << "Doc " << docs[i] << " should have topic='database'";
    EXPECT_EQ("search,index", tags)
      << "Doc " << docs[i] << " should have tags='search,index'";
  }
}

// Test BM25 with different k and b parameters
TEST_P(BlockScoringTestCase, Bm25ParameterVariations) {
  CreateLargeIndex();

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(1, reader.size()) << "Expected single segment for value checks";
  auto& segment = reader[0];

  // Use topic field which has single-word indexed values
  auto filter = ParseQuery("topic:database");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;

  // Test with default BM25 (k=1.2, b=0.75)
  {
    auto scorer = irs::BM25{};
    auto prepared_order = irs::Scorers::Prepare(scorer);

    std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
      irs::BlockSize(kTopK));

    size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                             kTopK, std::span{docs});

    ASSERT_GT(count, 0);
    auto result_count = std::min(count, kTopK);

    VerifyScoresAndDocs(docs, result_count);

    // Verify actual values
    for (size_t i = 0; i < result_count; ++i) {
      auto topic = ReadColumnValue(segment, "topic", docs[i]);
      EXPECT_EQ("database", topic)
        << "Doc " << docs[i] << " should have topic='database'";
    }
  }

  // Test with BM15 (b=0)
  {
    auto scorer = irs::BM25{irs::BM25::K(), 0.0f};
    auto prepared_order = irs::Scorers::Prepare(scorer);

    std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
      irs::BlockSize(kTopK));

    size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                             kTopK, std::span{docs});

    ASSERT_GT(count, 0);
    auto result_count = std::min(count, kTopK);

    VerifyScoresAndDocs(docs, result_count);

    // Verify actual values
    for (size_t i = 0; i < result_count; ++i) {
      auto topic = ReadColumnValue(segment, "topic", docs[i]);
      EXPECT_EQ("database", topic)
        << "Doc " << docs[i] << " should have topic='database'";
    }
  }

  // Test with BM11 (b=1)
  {
    auto scorer = irs::BM25{irs::BM25::K(), 1.0f};
    auto prepared_order = irs::Scorers::Prepare(scorer);

    std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
      irs::BlockSize(kTopK));

    size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                             kTopK, std::span{docs});

    ASSERT_GT(count, 0);
    auto result_count = std::min(count, kTopK);

    VerifyScoresAndDocs(docs, result_count);

    // Verify actual values
    for (size_t i = 0; i < result_count; ++i) {
      auto topic = ReadColumnValue(segment, "topic", docs[i]);
      EXPECT_EQ("database", topic)
        << "Doc " << docs[i] << " should have topic='database'";
    }
  }
}

// Test TFIDF with and without norms
TEST_P(BlockScoringTestCase, TfidfWithWithoutNorms) {
  CreateLargeIndex();

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(1, reader.size()) << "Expected single segment for value checks";
  auto& segment = reader[0];

  // Use topic field which has single-word indexed values
  auto filter = ParseQuery("topic:search");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;

  // Test with norms
  {
    auto scorer = irs::TFIDF{true};
    auto prepared_order = irs::Scorers::Prepare(scorer);

    std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs_with_norms(
      irs::BlockSize(kTopK));

    size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                             kTopK, std::span{docs_with_norms});

    ASSERT_GT(count, 0);
    auto result_count = std::min(count, kTopK);

    VerifyScoresAndDocs(docs_with_norms, result_count);

    // Verify actual values
    for (size_t i = 0; i < result_count; ++i) {
      auto topic = ReadColumnValue(segment, "topic", docs_with_norms[i]);
      EXPECT_EQ("search", topic)
        << "Doc " << docs_with_norms[i] << " should have topic='search'";
    }
  }

  // Test without norms
  {
    auto scorer = irs::TFIDF{false};
    auto prepared_order = irs::Scorers::Prepare(scorer);

    std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs_without_norms(
      irs::BlockSize(kTopK));

    size_t count = irs::ExecuteTopKWithCount(
      reader, *filter, prepared_order, kTopK, std::span{docs_without_norms});

    ASSERT_GT(count, 0);
    auto result_count = std::min(count, kTopK);

    VerifyScoresAndDocs(docs_without_norms, result_count);

    // Verify actual values
    for (size_t i = 0; i < result_count; ++i) {
      auto topic = ReadColumnValue(segment, "topic", docs_without_norms[i]);
      EXPECT_EQ("search", topic)
        << "Doc " << docs_without_norms[i] << " should have topic='search'";
    }
  }
}

// Multi-segment tests - TFIDF with ByTerm filter
TEST_P(BlockScoringTestCase, MultisegTfidfByterm) {
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

  // Use topic field which has single-word indexed values
  auto filter = ParseQuery("topic:database");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 15;
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  ASSERT_GT(count, 0);
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(docs, result_count);

  for (size_t i = 0; i < result_count; ++i) {
    EXPECT_TRUE(
      VerifyDocValueInAnySegment(reader, docs[i], "topic", "database"))
      << "Doc " << docs[i] << " should have topic='database'";
  }
}

// Multi-segment tests - BM25 with ByTerm filter
TEST_P(BlockScoringTestCase, MultisegBm25Byterm) {
  CreateMultiSegmentIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(3, reader.size());

  // Use parser to create query for "search"
  auto filter = ParseQuery("topic:search");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 20;
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  ASSERT_GT(count, 0);
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(docs, result_count);

  for (size_t i = 0; i < result_count; ++i) {
    EXPECT_TRUE(VerifyDocValueInAnySegment(reader, docs[i], "topic", "search"))
      << "Doc " << docs[i] << " should have topic='search'";
  }
}

// Multi-segment tests - And filter with TFIDF
TEST_P(BlockScoringTestCase, MultisegTfidfAndFilter) {
  CreateMultiSegmentIndex();

  auto scorer = irs::TFIDF{true};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(3, reader.size());

  // AND filter: category:tech AND topic:database
  auto filter = ParseQuery("+category:tech +topic:database");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 15;
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  ASSERT_GT(count, 0) << "Expected matches for tech AND database";
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(docs, result_count);

  for (size_t i = 0; i < result_count; ++i) {
    EXPECT_TRUE(VerifyDocValueInAnySegment(reader, docs[i], "category", "tech"))
      << "Doc " << docs[i] << " should have category='tech'";
    EXPECT_TRUE(
      VerifyDocValueInAnySegment(reader, docs[i], "topic", "database"))
      << "Doc " << docs[i] << " should have topic='database'";
  }
}

// Multi-segment tests - And filter with BM25
TEST_P(BlockScoringTestCase, MultisegBm25AndFilter) {
  CreateMultiSegmentIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(3, reader.size());

  // AND filter: category:science AND topic:physics
  auto filter = ParseQuery("+category:science +topic:physics");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 20;
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  ASSERT_GT(count, 0) << "Expected matches for science AND physics";
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(docs, result_count);

  for (size_t i = 0; i < result_count; ++i) {
    EXPECT_TRUE(
      VerifyDocValueInAnySegment(reader, docs[i], "category", "science"))
      << "Doc " << docs[i] << " should have category='science'";
    EXPECT_TRUE(VerifyDocValueInAnySegment(reader, docs[i], "topic", "physics"))
      << "Doc " << docs[i] << " should have topic='physics'";
  }
}

// Multi-segment tests - small k forcing multiple blocks across segments
TEST_P(BlockScoringTestCase, MultisegSmallKBlockBoundaries) {
  CreateMultiSegmentIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(3, reader.size());

  // Use category field - "tech" appears frequently
  auto filter = ParseQuery("category:tech");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 5;  // Small k to test block boundaries
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  ASSERT_GT(count, kTopK * 2) << "Need many matches to test block boundaries";
  auto result_count = std::min(count, kTopK);
  ASSERT_EQ(kTopK, result_count);

  VerifyScoresAndDocs(docs, result_count);

  for (size_t i = 0; i < result_count; ++i) {
    EXPECT_TRUE(VerifyDocValueInAnySegment(reader, docs[i], "category", "tech"))
      << "Doc " << docs[i] << " should have category='tech'";
  }
}

// Multi-segment tests - verify results with physics query
TEST_P(BlockScoringTestCase, MultisegQuantumQuery) {
  CreateMultiSegmentIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(3, reader.size());

  // Use topic field which has single-word indexed values
  auto filter = ParseQuery("topic:physics");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 10;
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  ASSERT_GT(count, 0);
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(docs, result_count);

  for (size_t i = 0; i < result_count; ++i) {
    EXPECT_TRUE(VerifyDocValueInAnySegment(reader, docs[i], "topic", "physics"))
      << "Doc " << docs[i] << " should have topic='physics'";
  }
}

// Test TFIDF with disjunction (OR) of two terms
TEST_P(BlockScoringTestCase, TfidfDisjunctionTwoTerms) {
  CreateLargeIndex();

  auto scorer = irs::TFIDF{true};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(1, reader.size()) << "Expected single segment for value checks";
  auto& segment = reader[0];

  // OR filter: topic:database OR topic:search
  auto filter = ParseQuery("topic:database topic:search");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 15;
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  ASSERT_GT(count, 0) << "Expected matches for database OR search";
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(docs, result_count);

  // Verify all returned docs have either topic='database' or topic='search'
  for (size_t i = 0; i < result_count; ++i) {
    auto topic = ReadColumnValue(segment, "topic", docs[i]);
    EXPECT_TRUE(topic == "database" || topic == "search")
      << "Doc " << docs[i] << " should have topic='database' or 'search', got '"
      << topic << "'";
  }
}

// Test BM25 with disjunction (OR) of two terms
TEST_P(BlockScoringTestCase, Bm25DisjunctionTwoTerms) {
  CreateLargeIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(1, reader.size()) << "Expected single segment for value checks";
  auto& segment = reader[0];

  // OR filter: category:tech OR category:science
  auto filter = ParseQuery("category:tech category:science");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 20;
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  ASSERT_GT(count, 0) << "Expected matches for tech OR science";
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(docs, result_count);

  // Verify all returned docs have either category='tech' or category='science'
  for (size_t i = 0; i < result_count; ++i) {
    auto category = ReadColumnValue(segment, "category", docs[i]);
    EXPECT_TRUE(category == "tech" || category == "science")
      << "Doc " << docs[i] << " should have category='tech' or 'science', got '"
      << category << "'";
  }
}

// Multi-segment test - TFIDF with disjunction of two terms
TEST_P(BlockScoringTestCase, MultisegTfidfDisjunction) {
  CreateMultiSegmentIndex();

  auto scorer = irs::TFIDF{true};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(3, reader.size()) << "Expected 3 segments";

  // OR filter: topic:physics OR topic:chemistry
  auto filter = ParseQuery("topic:physics topic:chemistry");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 15;
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  ASSERT_GT(count, 0) << "Expected matches for physics OR chemistry";
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(docs, result_count);

  // Verify all returned docs have either topic='physics' or topic='chemistry'
  for (size_t i = 0; i < result_count; ++i) {
    bool has_physics =
      VerifyDocValueInAnySegment(reader, docs[i], "topic", "physics");
    bool has_chemistry =
      VerifyDocValueInAnySegment(reader, docs[i], "topic", "chemistry");
    EXPECT_TRUE(has_physics || has_chemistry)
      << "Doc " << docs[i] << " should have topic='physics' or 'chemistry'";
  }
}

// Multi-segment test - BM25 with disjunction of two terms
TEST_P(BlockScoringTestCase, MultisegBm25Disjunction) {
  CreateMultiSegmentIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(3, reader.size()) << "Expected 3 segments";

  // OR filter: category:tech OR category:science
  auto filter = ParseQuery("category:tech category:science");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 20;
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  ASSERT_GT(count, 0) << "Expected matches for tech OR science";
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(docs, result_count);

  // Verify all returned docs have either category='tech' or category='science'
  for (size_t i = 0; i < result_count; ++i) {
    bool has_tech =
      VerifyDocValueInAnySegment(reader, docs[i], "category", "tech");
    bool has_science =
      VerifyDocValueInAnySegment(reader, docs[i], "category", "science");
    EXPECT_TRUE(has_tech || has_science)
      << "Doc " << docs[i] << " should have category='tech' or 'science'";
  }
}

// Test BM25 with disjunction (OR) of four terms
TEST_P(BlockScoringTestCase, Bm25DisjunctionFourTerms) {
  CreateLargeIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(1, reader.size()) << "Expected single segment for value checks";
  auto& segment = reader[0];

  // OR filter with 4 terms: database OR search OR physics OR chemistry
  auto filter =
    ParseQuery("topic:database topic:search topic:physics topic:chemistry");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 25;
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  ASSERT_GT(count, 0) << "Expected matches for 4-term disjunction";
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(docs, result_count);

  // Verify all returned docs have one of the four topics
  for (size_t i = 0; i < result_count; ++i) {
    auto topic = ReadColumnValue(segment, "topic", docs[i]);
    EXPECT_TRUE(topic == "database" || topic == "search" ||
                topic == "physics" || topic == "chemistry")
      << "Doc " << docs[i] << " should have one of the queried topics, got '"
      << topic << "'";
  }
}

// Multi-segment test - TFIDF with disjunction of five terms
TEST_P(BlockScoringTestCase, MultisegTfidfDisjunctionFiveTerms) {
  CreateMultiSegmentIndex();

  auto scorer = irs::TFIDF{true};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(3, reader.size()) << "Expected 3 segments";

  // OR filter with 5 terms across different fields
  auto filter = ParseQuery(
    "topic:database topic:search topic:physics topic:chemistry topic:biology");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 30;
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  ASSERT_GT(count, 0) << "Expected matches for 5-term disjunction";
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(docs, result_count);

  // Verify all returned docs have one of the five topics
  for (size_t i = 0; i < result_count; ++i) {
    bool has_database =
      VerifyDocValueInAnySegment(reader, docs[i], "topic", "database");
    bool has_search =
      VerifyDocValueInAnySegment(reader, docs[i], "topic", "search");
    bool has_physics =
      VerifyDocValueInAnySegment(reader, docs[i], "topic", "physics");
    bool has_chemistry =
      VerifyDocValueInAnySegment(reader, docs[i], "topic", "chemistry");
    bool has_biology =
      VerifyDocValueInAnySegment(reader, docs[i], "topic", "biology");
    EXPECT_TRUE(has_database || has_search || has_physics || has_chemistry ||
                has_biology)
      << "Doc " << docs[i] << " should have one of the queried topics";
  }
}

// Test BM25 with disjunction (OR) of three terms - different field
TEST_P(BlockScoringTestCase, Bm25DisjunctionThreeTermsSameField) {
  CreateLargeIndex();

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B()};
  auto prepared_order = irs::Scorers::Prepare(scorer);

  auto reader = irs::DirectoryReader(dir(), codec());
  ASSERT_EQ(1, reader.size()) << "Expected single segment for value checks";
  auto& segment = reader[0];

  // OR filter with 3 terms on topic field
  auto filter = ParseQuery("topic:database topic:physics topic:chemistry");
  ASSERT_NE(nullptr, filter);

  constexpr size_t kTopK = 20;
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> docs(
    irs::BlockSize(kTopK));

  size_t count = irs::ExecuteTopKWithCount(reader, *filter, prepared_order,
                                           kTopK, std::span{docs});

  ASSERT_GT(count, 0) << "Expected matches for 3-term disjunction";
  auto result_count = std::min(count, kTopK);

  VerifyScoresAndDocs(docs, result_count);

  // Verify all returned docs have one of the three topics
  for (size_t i = 0; i < result_count; ++i) {
    auto topic = ReadColumnValue(segment, "topic", docs[i]);
    EXPECT_TRUE(topic == "database" || topic == "physics" ||
                topic == "chemistry")
      << "Doc " << docs[i] << " should have one of the queried topics, got '"
      << topic << "'";
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(BlockScoringTest, BlockScoringTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd",
                                                              "1_5avx")),
                         BlockScoringTestCase::to_string);

}  // namespace
