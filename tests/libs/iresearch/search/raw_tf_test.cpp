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
#include "index/index_tests.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/raw_tf.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/term_filter.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests;

inline constexpr irs::field_id kBodyId = 1;

// Factory / registration tests.

TEST(raw_tf_test, consts) {
  static_assert("raw_tf" == irs::Type<irs::RawTF>::name());
}

TEST(raw_tf_test, load) {
  auto scorer = irs::RawTF::Make(irs::RawTF::Options{});
  ASSERT_NE(nullptr, scorer);
  ASSERT_EQ(irs::Type<irs::RawTF>::id(), scorer->type());
  ASSERT_EQ(irs::IndexFeatures::Freq, scorer->GetIndexFeatures());
}

TEST(raw_tf_test, equals) {
  // RawTF has no params -- any two instances are equal.
  auto a = std::make_unique<irs::RawTF>();
  auto b = std::make_unique<irs::RawTF>();
  ASSERT_TRUE(a->equals(*b));
}

// End-to-end scoring on a tiny fixture index.
//
// Fixture:
//   doc1: "fox fox dog"       -- freq(fox)=2, dl=3
//   doc2: "fox cat"           -- freq(fox)=1, dl=2
//   doc3: "dog rabbit fox"    -- freq(fox)=1, dl=3

class RawTfIndexTest : public IndexTestBase {
 protected:
  void BuildFixture();
};

void RawTfIndexTest::BuildFixture() {
  using TextField = tests::TextField<std::string>;
  const auto extra = irs::IndexFeatures::Norm;

  auto make_body = [&](std::string value) {
    auto field = std::make_shared<TextField>("body", std::move(value),
                                             /*payload=*/false, extra);
    field->id = kBodyId;
    return field;
  };

  tests::Document doc1;
  doc1.insert(make_body("fox fox dog"), true, false);
  tests::Document doc2;
  doc2.insert(make_body("fox cat"), true, false);
  tests::Document doc3;
  doc3.insert(make_body("dog rabbit fox"), true, false);

  irs::IndexWriterOptions opts;

  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_TRUE(tests::Insert(*writer, doc1.indexed.begin(), doc1.indexed.end()));
  ASSERT_TRUE(tests::Insert(*writer, doc2.indexed.begin(), doc2.indexed.end()));
  ASSERT_TRUE(tests::Insert(*writer, doc3.indexed.begin(), doc3.indexed.end()));
  writer->RefreshCommit();
}

TEST_P(RawTfIndexTest, scores_match_freq) {
  BuildFixture();

  auto impl = std::make_unique<irs::RawTF>();

  auto index = open_reader();
  ASSERT_EQ(1, index->size());
  auto& segment = *(index.begin());

  irs::ByTerm filter;
  *filter.mutable_field_id() = kBodyId;
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
  while (!irs::doc_limits::eof(docs->advance())) {
    fetcher.Fetch(docs->value());
    docs->FetchScoreArgs(0);
    irs::score_t s{};
    score.Score(&s, 1);
    seen.emplace(docs->value(), s);
  }
  ASSERT_EQ(3u, seen.size());

  // The doc with freq=2 must outrank docs with freq=1.
  std::vector<irs::score_t> values;
  for (auto& [_, s] : seen) {
    values.push_back(s);
  }
  std::sort(values.begin(), values.end(), std::greater<>{});
  ASSERT_FLOAT_EQ(2.f, values[0]);
  ASSERT_FLOAT_EQ(1.f, values[1]);
  ASSERT_FLOAT_EQ(1.f, values[2]);
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(raw_tf_test, RawTfIndexTest,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd")),
                         RawTfIndexTest::to_string);

}  // namespace
