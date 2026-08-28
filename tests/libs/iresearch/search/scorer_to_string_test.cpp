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

#include "iresearch/search/bm25.hpp"
#include "iresearch/search/dfi.hpp"
#include "iresearch/search/idf.hpp"
#include "iresearch/search/indri_dirichlet.hpp"
#include "iresearch/search/lm_dirichlet.hpp"
#include "iresearch/search/lm_jelinek_mercer.hpp"
#include "iresearch/search/raw_boost.hpp"
#include "iresearch/search/raw_dl.hpp"
#include "iresearch/search/raw_tf.hpp"
#include "iresearch/search/tfidf.hpp"
#include "iresearch/search/unscored.hpp"
#include "tests_shared.hpp"

namespace {

TEST(scorer_to_string_test, parameters_are_rendered) {
  using irs::BM25;
  using irs::DFI;
  using irs::IndriDirichlet;
  using irs::LMDirichlet;
  using irs::LMJelinekMercer;
  using irs::TFIDF;

  ASSERT_EQ("bm25(k1=1.2, b=0.75)", BM25::Make(BM25::Options{})->ToString());
  ASSERT_EQ("bm25(k1=1.3, b=0.5)",
            BM25::Make(BM25::Options{.k1 = 1.3f, .b = 0.5f})->ToString());
  ASSERT_EQ("tfidf(with_norms=false)",
            TFIDF::Make(TFIDF::Options{})->ToString());
  ASSERT_EQ("tfidf(with_norms=true)",
            TFIDF::Make(TFIDF::Options{.with_norms = true})->ToString());
  ASSERT_EQ("lm_jm(lambda=0.5)",
            LMJelinekMercer::Make(LMJelinekMercer::Options{.lambda = 0.5f})
              ->ToString());
  ASSERT_EQ("lm_dirichlet(mu=1000)",
            LMDirichlet::Make(LMDirichlet::Options{.mu = 1000.f})->ToString());
  ASSERT_EQ(
    "indri_dirichlet(mu=500)",
    IndriDirichlet::Make(IndriDirichlet::Options{.mu = 500.f})->ToString());
  ASSERT_EQ("dfi(measure=chi_squared)",
            DFI::Make(DFI::Options{.measure = irs::DFIMeasure::ChiSquared})
              ->ToString());
}

TEST(scorer_to_string_test, parameterless_scorers_render_their_name) {
  ASSERT_EQ("raw_tf()", irs::RawTF::Make(irs::RawTF::Options{})->ToString());
  ASSERT_EQ("raw_boost()",
            irs::RawBoost::Make(irs::RawBoost::Options{})->ToString());
  ASSERT_EQ("raw_dl()", irs::RawDL::Make(irs::RawDL::Options{})->ToString());
  ASSERT_EQ("idf()", irs::IDF::Make(irs::IDF::Options{})->ToString());
}

TEST(scorer_to_string_test, unscored_reads_as_a_state) {
  ASSERT_EQ("unscored", irs::Unscored::Instance().ToString());
}

TEST(scorer_to_string_test, differing_scorers_print_differently) {
  const auto a = irs::BM25::Make(irs::BM25::Options{.k1 = 1.2f, .b = 0.75f});
  const auto b = irs::BM25::Make(irs::BM25::Options{.k1 = 1.3f, .b = 0.75f});
  ASSERT_FALSE(a->equals(*b));
  ASSERT_NE(a->ToString(), b->ToString());
}

}  // namespace
