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

#include "index/index_tests.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/indri_dirichlet.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/term_filter.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests;

TEST(indri_dirichlet_test, consts) {
  static_assert("indri_dirichlet" == irs::Type<irs::IndriDirichlet>::name());
}

TEST(indri_dirichlet_test, load_default) {
  auto scorer = irs::IndriDirichlet::Make(irs::IndriDirichlet::Options{});
  ASSERT_NE(nullptr, scorer);
  ASSERT_EQ(irs::Type<irs::IndriDirichlet>::id(), scorer->type());
  auto& lm = dynamic_cast<irs::IndriDirichlet&>(*scorer);
  ASSERT_FLOAT_EQ(irs::IndriDirichlet::MU(), lm.mu());
  ASSERT_EQ(irs::IndexFeatures::Freq | irs::IndexFeatures::Norm,
            scorer->GetIndexFeatures());
}

TEST(indri_dirichlet_test, load_object) {
  auto scorer =
    irs::IndriDirichlet::Make(irs::IndriDirichlet::Options{.mu = 500.f});
  ASSERT_NE(nullptr, scorer);
  auto& lm = dynamic_cast<irs::IndriDirichlet&>(*scorer);
  ASSERT_FLOAT_EQ(500.f, lm.mu());
}

TEST(indri_dirichlet_test, load_invalid) {
  // μ must be non-negative -- it scales the collection prior; a negative μ
  // produces a meaningless ratio.
  EXPECT_EQ(nullptr, irs::IndriDirichlet::Make(
                       irs::IndriDirichlet::Options{.mu = -1.f}));
  EXPECT_EQ(nullptr, irs::IndriDirichlet::Make(
                       irs::IndriDirichlet::Options{.mu = -0.001f}));
  // Boundary: μ = 0 is allowed (degenerate).
  EXPECT_NE(nullptr,
            irs::IndriDirichlet::Make(irs::IndriDirichlet::Options{.mu = 0.f}));
}

TEST(indri_dirichlet_test, equals) {
  auto a = std::make_unique<irs::IndriDirichlet>(1000.f);
  auto b = std::make_unique<irs::IndriDirichlet>(1000.f);
  auto c = std::make_unique<irs::IndriDirichlet>(2000.f);
  ASSERT_TRUE(a->equals(*b));
  ASSERT_FALSE(a->equals(*c));
}

class IndriDirichletIndexTest : public IndexTestBase {
 protected:
  void BuildFixture();
};

void IndriDirichletIndexTest::BuildFixture() {
  using TextField = tests::TextField<std::string>;
  const auto extra = irs::IndexFeatures::Norm;

  tests::Document doc1;
  doc1.insert(std::make_shared<TextField>("body", std::string{"fox fox dog"},
                                          false, extra),
              true, false);
  tests::Document doc2;
  doc2.insert(
    std::make_shared<TextField>("body", std::string{"fox cat"}, false, extra),
    true, false);
  tests::Document doc3;
  doc3.insert(std::make_shared<TextField>("body", std::string{"dog rabbit fox"},
                                          false, extra),
              true, false);

  irs::IndexWriterOptions opts;

  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_TRUE(tests::Insert(*writer, doc1.indexed.begin(), doc1.indexed.end()));
  ASSERT_TRUE(tests::Insert(*writer, doc2.indexed.begin(), doc2.indexed.end()));
  ASSERT_TRUE(tests::Insert(*writer, doc3.indexed.begin(), doc3.indexed.end()));
  writer->RefreshCommit();
}

TEST_P(IndriDirichletIndexTest, scores_are_finite) {
  BuildFixture();

  // With small mu the higher-tf doc should outrank the others; unlike the
  // floor-at-zero LMDirichlet, Indri can produce negative scores at large mu.
  auto impl = std::make_unique<irs::IndriDirichlet>(5.f);

  auto index = open_reader();
  ASSERT_EQ(1, index->size());
  auto& segment = *(index.begin());

  irs::ByTerm filter;
  *filter.mutable_field() = "body";
  filter.mutable_options()->term =
    irs::ViewCast<irs::byte_type>(std::string_view("fox"));

  MaxMemoryCounter counter;
  auto prepared = filter.prepare({
    .index = *index,
    .memory = counter,
    .scorer = impl.get(),
  });

  irs::ColumnArgsFetcher fetcher;
  auto docs = prepared->execute({
    .segment = segment,
    .scorer = impl.get(),
  });
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
    ASSERT_TRUE(std::isfinite(s));
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(indri_dirichlet_test, IndriDirichletIndexTest,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd")),
                         IndriDirichletIndexTest::to_string);

}  // namespace
