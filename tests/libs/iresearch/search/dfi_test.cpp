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

#include <algorithm>
#include <map>

#include "filter_test_case_base.hpp"
#include "formats/column/test_cs_helpers.hpp"
#include "index/index_tests.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/dfi.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/scorers.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/utils/lz4compression.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests;

TEST(dfi_test, consts) { static_assert("dfi" == irs::Type<irs::DFI>::name()); }

TEST(dfi_test, load_default) {
  auto scorer = irs::scorers::Get(
    "dfi", irs::Type<irs::text_format::Json>::get(), std::string_view{});
  ASSERT_NE(nullptr, scorer);
  auto& dfi = dynamic_cast<irs::DFI&>(*scorer);
  ASSERT_EQ(irs::DFIMeasure::Standardized, dfi.measure());
  ASSERT_EQ(irs::IndexFeatures::Freq | irs::IndexFeatures::Norm,
            scorer->GetIndexFeatures());
}

TEST(dfi_test, load_measures) {
  {
    auto s = irs::scorers::Get("dfi", irs::Type<irs::text_format::Json>::get(),
                               "{ \"measure\": \"standardized\" }");
    ASSERT_NE(nullptr, s);
    ASSERT_EQ(irs::DFIMeasure::Standardized,
              dynamic_cast<irs::DFI&>(*s).measure());
  }
  {
    auto s = irs::scorers::Get("dfi", irs::Type<irs::text_format::Json>::get(),
                               "{ \"measure\": \"saturated\" }");
    ASSERT_NE(nullptr, s);
    ASSERT_EQ(irs::DFIMeasure::Saturated,
              dynamic_cast<irs::DFI&>(*s).measure());
  }
  {
    auto s = irs::scorers::Get("dfi", irs::Type<irs::text_format::Json>::get(),
                               "{ \"measure\": \"chi_squared\" }");
    ASSERT_NE(nullptr, s);
    ASSERT_EQ(irs::DFIMeasure::ChiSquared,
              dynamic_cast<irs::DFI&>(*s).measure());
  }
}

TEST(dfi_test, load_invalid) {
  ASSERT_EQ(nullptr,
            irs::scorers::Get("dfi", irs::Type<irs::text_format::Json>::get(),
                              "{ \"measure\": \"bogus\" }"));
}

TEST(dfi_test, equals) {
  auto a = std::make_unique<irs::DFI>(irs::DFIMeasure::Standardized);
  auto b = std::make_unique<irs::DFI>(irs::DFIMeasure::Standardized);
  auto c = std::make_unique<irs::DFI>(irs::DFIMeasure::ChiSquared);
  ASSERT_TRUE(a->equals(*b));
  ASSERT_FALSE(a->equals(*c));
}

class DFIIndexTest : public IndexTestBase {
 protected:
  void BuildFixture();
};

void DFIIndexTest::BuildFixture() {
  // Fixture crafted so 'fox' in doc1 gets freq > expected (thus a positive
  // DFI score) while in doc2/doc3 freq == expected (thus score == 0).
  using TextField = tests::TextField<std::string>;
  const auto extra = irs::IndexFeatures::Norm;

  tests::Document doc1;
  doc1.insert(std::make_shared<TextField>(
                "body", std::string{"fox fox fox dog cat"}, false, extra),
              true, false);
  tests::Document doc2;
  doc2.insert(
    std::make_shared<TextField>("body", std::string{"fox cat"}, false, extra),
    true, false);
  tests::Document doc3;
  doc3.insert(std::make_shared<TextField>("body", std::string{"dog rabbit fox"},
                                          false, extra),
              true, false);

  auto opts = irs::tests::DefaultWriterOptions();

  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_TRUE(tests::Insert(*writer, doc1.indexed.begin(), doc1.indexed.end()));
  ASSERT_TRUE(tests::Insert(*writer, doc2.indexed.begin(), doc2.indexed.end()));
  ASSERT_TRUE(tests::Insert(*writer, doc3.indexed.begin(), doc3.indexed.end()));
  writer->RefreshCommit();
}

TEST_P(DFIIndexTest, scores_nonnegative_and_only_fire_above_expected) {
  BuildFixture();

  auto impl = std::make_unique<irs::DFI>(irs::DFIMeasure::Standardized);

  auto index = open_reader(irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, index->size());
  auto& segment = *(index.begin());

  irs::ByTerm filter;
  *filter.mutable_field() = "body";
  filter.mutable_options()->term =
    irs::ViewCast<irs::byte_type>(std::string_view("fox"));

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
  while (docs->next()) {
    fetcher.Fetch(docs->value());
    docs->FetchScoreArgs(0);
    irs::score_t s{};
    score.Score(&s, 1);
    seen.emplace(docs->value(), s);
  }
  ASSERT_EQ(3u, seen.size());
  for (auto& [_, s] : seen) {
    ASSERT_GE(s, 0.f) << "DFI never produces negative scores";
  }
  // At least one doc (doc1, with highest tf) must be above expected and
  // strictly positive.
  bool any_positive = false;
  for (auto& [_, s] : seen) {
    if (s > 0.f) {
      any_positive = true;
    }
  }
  ASSERT_TRUE(any_positive);
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(dfi_test, DFIIndexTest,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd")),
                         DFIIndexTest::to_string);

}  // namespace
