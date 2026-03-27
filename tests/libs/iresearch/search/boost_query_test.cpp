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
#include "iresearch/parser/parser.h"
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

  // Parse a Lucene query string into a MixedBooleanFilter.
  static irs::Filter::ptr ParseQuery(std::string_view query) {
    irs::analysis::DelimitedTokenizer tokenizer(" ");
    auto root = std::make_unique<irs::MixedBooleanFilter>();
    sdb::ParserContext ctx{*root, "content", tokenizer};
    EXPECT_TRUE(sdb::ParseQuery(ctx, query).ok());
    return root;
  }

  // Create a single-segment index with 4 documents:
  //   doc A – "open"                  (required only)
  //   doc B – "open source"           (required + 1 optional)
  //   doc C – "open source software"  (required + 2 optionals)
  //   doc D – "source software"       (no required term → must be excluded)
  irs::DirectoryReader CreateIndex() {
    auto writer = open_writer(irs::kOmCreate);

    auto insert = [&](std::string_view content) {
      SpaceField f{"content"};
      f.value(content);
      auto batch = writer->GetBatch();
      auto d = batch.Insert();
      EXPECT_TRUE(d.Insert<irs::Action::INDEX>(f));
    };

    insert("open");
    insert("open source");
    insert("open source software");
    insert("source software");

    writer->Commit();

    auto reader = irs::DirectoryReader(dir(), codec());
    EXPECT_EQ(1, reader.size());
    return reader;
  }
};

// "+open source software" – only docs with "open" must be returned.
// Doc D ("source software") does NOT contain "open" and must be excluded.
TEST_P(BoostQueryTestCase, RequiredExcludesNonMatchingDocs) {
  auto reader = CreateIndex();
  auto filter = ParseQuery("+open source software");
  ASSERT_NE(nullptr, filter);

  auto hits = CollectAll(reader, *filter);
  ASSERT_EQ(3u, hits.size())
    << "Expected exactly 3 docs matching required 'open'";

  for (auto& [score, doc] : hits) {
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
  EXPECT_GT(std::get<irs::score_t>(hits[0]), std::get<irs::score_t>(hits[1]))
    << "doc with 2 optional matches should outscore doc with 1";
  EXPECT_GT(std::get<irs::score_t>(hits[1]), std::get<irs::score_t>(hits[2]))
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
  EXPECT_GT(std::get<irs::score_t>(hits[0]), std::get<irs::score_t>(hits[1]));
  EXPECT_GT(std::get<irs::score_t>(hits[1]), std::get<irs::score_t>(hits[2]));
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(boost_query_test, BoostQueryTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd")),
                         BoostQueryTestCase::to_string);

}  // namespace
