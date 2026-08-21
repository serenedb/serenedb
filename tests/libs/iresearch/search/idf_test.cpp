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

#include <cmath>
#include <map>

#include "filter_test_case_base.hpp"
#include "index/index_tests.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/search/idf.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/term_filter.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests;

inline constexpr irs::field_id kBodyId = 1;

irs::score_t ExpectedIdf(double docs_with_field, double docs_with_term) {
  return static_cast<irs::score_t>(std::log1p(
    (docs_with_field - docs_with_term + 0.5) / (docs_with_term + 0.5)));
}

TEST(idf_test, consts) { static_assert("idf" == irs::Type<irs::IDF>::name()); }

TEST(idf_test, load) {
  auto scorer = irs::IDF::Make(irs::IDF::Options{});
  ASSERT_NE(nullptr, scorer);
  ASSERT_EQ(irs::Type<irs::IDF>::id(), scorer->type());
  ASSERT_EQ(irs::IndexFeatures::None, scorer->GetIndexFeatures());
}

TEST(idf_test, equals) {
  auto a = std::make_unique<irs::IDF>();
  auto b = std::make_unique<irs::IDF>();
  ASSERT_TRUE(a->equals(*b));
}

// Fixture (4 documents, every one carrying the field):
//   doc1: "fox fox dog"  -- 'fox' twice, so term frequency is observable
//   doc2: "fox cat"
//   doc3: "dog rabbit"
//   doc4: "cat"
// 'fox' matches 2 of 4 documents, 'rabbit' matches 1 of 4.
class IdfIndexTest : public IndexTestBase {
 protected:
  void BuildAnalyzed();
  void BuildKeyword();

  std::map<irs::doc_id_t, irs::score_t> Score(std::string_view term);
};

void IdfIndexTest::BuildAnalyzed() {
  using TextField = tests::TextField<std::string>;

  auto make_body = [&](std::string value) {
    auto field =
      std::make_shared<TextField>("body", std::move(value),
                                  /*payload=*/false, irs::IndexFeatures::Norm);
    field->id = kBodyId;
    return field;
  };

  tests::Document doc1;
  doc1.insert(make_body("fox fox dog"), true, false);
  tests::Document doc2;
  doc2.insert(make_body("fox cat"), true, false);
  tests::Document doc3;
  doc3.insert(make_body("dog rabbit"), true, false);
  tests::Document doc4;
  doc4.insert(make_body("cat"), true, false);

  auto writer = open_writer(irs::kOmCreate, irs::IndexWriterOptions{});
  ASSERT_NE(nullptr, writer);
  ASSERT_TRUE(tests::Insert(*writer, doc1.indexed.begin(), doc1.indexed.end()));
  ASSERT_TRUE(tests::Insert(*writer, doc2.indexed.begin(), doc2.indexed.end()));
  ASSERT_TRUE(tests::Insert(*writer, doc3.indexed.begin(), doc3.indexed.end()));
  ASSERT_TRUE(tests::Insert(*writer, doc4.indexed.begin(), doc4.indexed.end()));
  writer->RefreshCommit();
}

// The same document frequencies, on a field carrying no index features at
// all: IDF needs none of them.
void IdfIndexTest::BuildKeyword() {
  auto make_tag = [&](std::string value) {
    auto field = std::make_shared<tests::StringField>("body", std::move(value));
    field->id = kBodyId;
    return field;
  };

  tests::Document doc1;
  doc1.insert(make_tag("fox"), true, false);
  tests::Document doc2;
  doc2.insert(make_tag("fox"), true, false);
  tests::Document doc3;
  doc3.insert(make_tag("rabbit"), true, false);
  tests::Document doc4;
  doc4.insert(make_tag("cat"), true, false);

  auto writer = open_writer(irs::kOmCreate, irs::IndexWriterOptions{});
  ASSERT_NE(nullptr, writer);
  ASSERT_TRUE(tests::Insert(*writer, doc1.indexed.begin(), doc1.indexed.end()));
  ASSERT_TRUE(tests::Insert(*writer, doc2.indexed.begin(), doc2.indexed.end()));
  ASSERT_TRUE(tests::Insert(*writer, doc3.indexed.begin(), doc3.indexed.end()));
  ASSERT_TRUE(tests::Insert(*writer, doc4.indexed.begin(), doc4.indexed.end()));
  writer->RefreshCommit();
}

std::map<irs::doc_id_t, irs::score_t> IdfIndexTest::Score(
  std::string_view term) {
  auto impl = std::make_unique<irs::IDF>();

  auto index = open_reader();
  EXPECT_EQ(1, index->size());
  auto& segment = *(index.begin());

  irs::ByTerm filter;
  *filter.mutable_field_id() = kBodyId;
  filter.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);

  MaxMemoryCounter counter;
  tests::PreparedFilter prepared{filter, *index, impl.get(), counter};

  irs::ColumnArgsFetcher fetcher;
  auto docs = prepared.Execute(0);
  auto score = docs->PrepareScore({
    .scorer = impl.get(),
    .segment = &segment,
    .fetcher = &fetcher,
  });

  std::map<irs::doc_id_t, irs::score_t> seen;
  while (!irs::doc_limits::eof(docs->advance())) {
    fetcher.Fetch(docs->value());
    docs->FetchScoreArgs(0);
    irs::score_t s{};
    score.Score(&s, 1);
    seen.emplace(docs->value(), s);
  }
  return seen;
}

TEST_P(IdfIndexTest, scores_idf_ignoring_term_frequency) {
  BuildAnalyzed();

  const auto fox = Score("fox");
  ASSERT_EQ(2u, fox.size());

  // doc1 carries 'fox' twice and doc2 once, yet both score the same: the
  // IDF-only scorer never looks at the frequency.
  const auto expected = ExpectedIdf(4, 2);
  for (const auto& [doc, value] : fox) {
    ASSERT_GT(value, 0.f);
    ASSERT_FLOAT_EQ(expected, value);
  }
}

TEST_P(IdfIndexTest, rarer_term_scores_higher) {
  BuildAnalyzed();

  const auto fox = Score("fox");
  const auto rabbit = Score("rabbit");
  ASSERT_EQ(2u, fox.size());
  ASSERT_EQ(1u, rabbit.size());

  ASSERT_FLOAT_EQ(ExpectedIdf(4, 2), fox.begin()->second);
  ASSERT_FLOAT_EQ(ExpectedIdf(4, 1), rabbit.begin()->second);
  ASSERT_GT(rabbit.begin()->second, fox.begin()->second);
}

TEST_P(IdfIndexTest, scores_field_without_frequency) {
  BuildKeyword();

  const auto fox = Score("fox");
  ASSERT_EQ(2u, fox.size());

  const auto expected = ExpectedIdf(4, 2);
  for (const auto& [doc, value] : fox) {
    ASSERT_GT(value, 0.f);
    ASSERT_FLOAT_EQ(expected, value);
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(idf_test, IdfIndexTest,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd")),
                         IdfIndexTest::to_string);

}  // namespace
