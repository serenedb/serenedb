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
#include <limits>
#include <map>

#include "filter_test_case_base.hpp"
#include "formats/column/test_cs_helpers.hpp"
#include "index/index_tests.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/lm_dirichlet.hpp"
#include "iresearch/search/lm_jelinek_mercer.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/term_filter.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests;

// ---------------------------------------------------------------------------
// LM Jelinek-Mercer factory tests.
// ---------------------------------------------------------------------------

TEST(lm_test, jm_consts) {
  static_assert("lm_jm" == irs::Type<irs::LMJelinekMercer>::name());
}

TEST(lm_test, jm_load_default) {
  auto scorer = irs::LMJelinekMercer::Make(irs::LMJelinekMercer::Options{});
  ASSERT_NE(nullptr, scorer);
  ASSERT_EQ(irs::Type<irs::LMJelinekMercer>::id(), scorer->type());
  auto& lm = dynamic_cast<irs::LMJelinekMercer&>(*scorer);
  ASSERT_EQ(irs::LMJelinekMercer::LAMBDA(), lm.lambda());
  ASSERT_EQ(irs::IndexFeatures::Freq | irs::IndexFeatures::Norm,
            scorer->GetIndexFeatures());
}

TEST(lm_test, jm_load_object) {
  auto scorer =
    irs::LMJelinekMercer::Make(irs::LMJelinekMercer::Options{.lambda = 0.7f});
  ASSERT_NE(nullptr, scorer);
  auto& lm = dynamic_cast<irs::LMJelinekMercer&>(*scorer);
  ASSERT_FLOAT_EQ(0.7f, lm.lambda());
}

TEST(lm_test, jm_load_array) {
  auto scorer =
    irs::LMJelinekMercer::Make(irs::LMJelinekMercer::Options{.lambda = 0.3f});
  ASSERT_NE(nullptr, scorer);
  auto& lm = dynamic_cast<irs::LMJelinekMercer&>(*scorer);
  ASSERT_FLOAT_EQ(0.3f, lm.lambda());
}

TEST(lm_test, jm_load_invalid) {
  // λ must lie in the open interval (0, 1] -- both `1-lambda` and `lambda`
  // must be positive for the smoothing formula to be defined.
  EXPECT_ANY_THROW(
    irs::LMJelinekMercer::Make(irs::LMJelinekMercer::Options{.lambda = 0.f}));
  EXPECT_ANY_THROW(
    irs::LMJelinekMercer::Make(irs::LMJelinekMercer::Options{.lambda = -0.1f}));
  EXPECT_ANY_THROW(
    irs::LMJelinekMercer::Make(irs::LMJelinekMercer::Options{.lambda = 1.5f}));
  // Boundary: λ = 1 is allowed (degenerate but mathematically defined).
  EXPECT_NE(nullptr, irs::LMJelinekMercer::Make(
                       irs::LMJelinekMercer::Options{.lambda = 1.f}));
}

TEST(lm_test, jm_equals) {
  auto a = std::make_unique<irs::LMJelinekMercer>(0.5f);
  auto b = std::make_unique<irs::LMJelinekMercer>(0.5f);
  auto c = std::make_unique<irs::LMJelinekMercer>(0.7f);
  ASSERT_TRUE(a->equals(*b));
  ASSERT_FALSE(a->equals(*c));
}

// ---------------------------------------------------------------------------
// LM Dirichlet factory tests.
// ---------------------------------------------------------------------------

TEST(lm_test, dirichlet_consts) {
  static_assert("lm_dirichlet" == irs::Type<irs::LMDirichlet>::name());
}

TEST(lm_test, dirichlet_load_default) {
  auto scorer = irs::LMDirichlet::Make(irs::LMDirichlet::Options{});
  ASSERT_NE(nullptr, scorer);
  ASSERT_EQ(irs::Type<irs::LMDirichlet>::id(), scorer->type());
  auto& lm = dynamic_cast<irs::LMDirichlet&>(*scorer);
  ASSERT_FLOAT_EQ(irs::LMDirichlet::MU(), lm.mu());
  ASSERT_EQ(irs::IndexFeatures::Freq | irs::IndexFeatures::Norm,
            scorer->GetIndexFeatures());
}

TEST(lm_test, dirichlet_load_object) {
  auto scorer = irs::LMDirichlet::Make(irs::LMDirichlet::Options{.mu = 500.f});
  ASSERT_NE(nullptr, scorer);
  auto& lm = dynamic_cast<irs::LMDirichlet&>(*scorer);
  ASSERT_FLOAT_EQ(500.f, lm.mu());
}

TEST(lm_test, dirichlet_load_invalid) {
  // μ must be non-negative -- it scales the collection prior and divides by
  // (dl + μ); a negative μ makes the score uninterpretable.
  EXPECT_ANY_THROW(
    irs::LMDirichlet::Make(irs::LMDirichlet::Options{.mu = -1.f}));
  EXPECT_ANY_THROW(
    irs::LMDirichlet::Make(irs::LMDirichlet::Options{.mu = -0.001f}));
  EXPECT_ANY_THROW(irs::LMDirichlet::Make(
    irs::LMDirichlet::Options{.mu = std::numeric_limits<float>::quiet_NaN()}));
  EXPECT_ANY_THROW(irs::LMDirichlet::Make(
    irs::LMDirichlet::Options{.mu = std::numeric_limits<float>::infinity()}));
  // Boundary: μ = 0 is allowed (degenerate -- falls back to MLE).
  EXPECT_NE(nullptr,
            irs::LMDirichlet::Make(irs::LMDirichlet::Options{.mu = 0.f}));
}

TEST(lm_test, dirichlet_equals) {
  auto a = std::make_unique<irs::LMDirichlet>(1000.f);
  auto b = std::make_unique<irs::LMDirichlet>(1000.f);
  auto c = std::make_unique<irs::LMDirichlet>(2000.f);
  ASSERT_TRUE(a->equals(*b));
  ASSERT_FALSE(a->equals(*c));
}

TEST(lm_test, jm_vs_dirichlet_not_equal) {
  auto jm = std::make_unique<irs::LMJelinekMercer>();
  auto dir = std::make_unique<irs::LMDirichlet>();
  ASSERT_FALSE(jm->equals(*dir));
}

// ---------------------------------------------------------------------------
// End-to-end scoring on a tiny fixture index.
//
// Fixture:
//   doc1: "fox fox dog"       -- freq(fox)=2, dl=3
//   doc2: "fox cat"           -- freq(fox)=1, dl=2
//   doc3: "dog rabbit fox"    -- freq(fox)=1, dl=3
//
// For term "fox":
//   ttf(fox) = 4   (total fox across docs)
//   docs_with_term(fox) = 3
// Field-wide:
//   sum_ttf = 8    (3 + 2 + 3)
//   docs_with_field = 3
//   P(fox|C) = (4 + 1) / (8 + 1) = 5/9
// ---------------------------------------------------------------------------

constexpr irs::field_id kBodyFieldId = 1;

class LMIndexTest : public IndexTestBase {
 protected:
  void BuildFixture();
};

void LMIndexTest::BuildFixture() {
  using TextField = tests::TextField<std::string>;
  const auto extra = irs::IndexFeatures::Norm;

  auto make_body = [&](std::string value) {
    auto f = std::make_shared<TextField>("body", std::move(value),
                                         /*payload=*/false, extra);
    f->id = kBodyFieldId;
    return f;
  };

  tests::Document doc1;
  doc1.insert(make_body(std::string{"fox fox dog"}), true, false);
  tests::Document doc2;
  doc2.insert(make_body(std::string{"fox cat"}), true, false);
  tests::Document doc3;
  doc3.insert(make_body(std::string{"dog rabbit fox"}), true, false);

  auto opts = irs::tests::DefaultWriterOptions();

  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_TRUE(tests::Insert(*writer, doc1.indexed.begin(), doc1.indexed.end()));
  ASSERT_TRUE(tests::Insert(*writer, doc2.indexed.begin(), doc2.indexed.end()));
  ASSERT_TRUE(tests::Insert(*writer, doc3.indexed.begin(), doc3.indexed.end()));
  writer->RefreshCommit();
}

namespace {

// Helper: run a filter with the given scorer and return {doc_id -> score}.
std::map<irs::doc_id_t, irs::score_t> RunQuery(irs::IndexReader& index,
                                               irs::Scorer& scorer) {
  auto& segment = *(index.begin());

  irs::ByTerm filter;
  *filter.mutable_field_id() = kBodyFieldId;
  filter.mutable_options()->term =
    irs::ViewCast<irs::byte_type>(std::string_view("fox"));

  MaxMemoryCounter counter;
  tests::PreparedFilter prepared{filter, index, &scorer, counter};

  irs::ColumnArgsFetcher fetcher;
  auto docs = prepared.Execute(0);
  auto score = docs->PrepareScore({
    .scorer = &scorer,
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
  return seen;
}

}  // namespace

TEST_P(LMIndexTest, jm_scores_positive_and_ordered) {
  BuildFixture();

  auto impl = std::make_unique<irs::LMJelinekMercer>(0.1f);
  auto index = open_reader(irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, index->size());

  auto seen = RunQuery(*index, *impl);
  ASSERT_EQ(3u, seen.size());
  for (auto& [_, s] : seen) {
    ASSERT_GT(s, 0.f) << "LM JM should produce positive scores for matches";
  }

  std::vector<irs::score_t> values;
  for (auto& [_, s] : seen) {
    values.push_back(s);
  }
  std::sort(values.begin(), values.end(), std::greater<>{});
  // The doc with freq=2, dl=3 has the highest freq/dl ratio (0.667).
  // The other two have ratio 0.5 (1/2) and 0.333 (1/3).
  ASSERT_GT(values[0], values[1]);
  ASSERT_GE(values[1], values[2]);
}

TEST_P(LMIndexTest, dirichlet_scores_nonnegative) {
  BuildFixture();

  // Small mu for sharper separation on this toy corpus.
  auto impl = std::make_unique<irs::LMDirichlet>(10.f);
  auto index = open_reader(irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, index->size());

  auto seen = RunQuery(*index, *impl);
  ASSERT_EQ(3u, seen.size());
  for (auto& [_, s] : seen) {
    ASSERT_GE(s, 0.f) << "LM Dirichlet floors at 0 per Lucene";
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(lm_test, LMIndexTest,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd")),
                         LMIndexTest::to_string);

}  // namespace
