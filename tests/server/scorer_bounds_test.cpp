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

#include <cstddef>
#include <type_traits>
#include <utility>
#include <variant>

#include "catalog/scorer_options.h"

namespace {

using Params = sdb::catalog::ScorerOptions::Params;
using BoundType = irs::Scorer::ScoreBoundType;

// A scorer that persists per-block bounds has to agree with itself in three
// places: the bound type its options classify to, the writer and source it
// builds, and the Compatible() answer the pruning gate asks for. Getting the
// last one half-right is the failure this pins -- comparing bound types but
// forgetting the parameterisation lets a scorer read a pair that is not its
// argmax and prune away qualifying rows.
template<typename P>
void CheckAlternative() {
  const sdb::catalog::ScorerOptions own{P{}};
  SCOPED_TRACE(own.Name());

  auto scorer = sdb::catalog::MakeScorer(own);
  ASSERT_TRUE(scorer);

  const bool bounded = irs::BoundTypeOf(own) != BoundType::None;

  EXPECT_EQ(bounded, static_cast<bool>(scorer->PrepareScoreBoundWriter(4)));
  EXPECT_EQ(bounded, static_cast<bool>(scorer->PrepareScoreBoundSource()));
  EXPECT_EQ(bounded, scorer->Compatible(own));

  // bm25 with 0 < b < 1 stores the argmax for that b under that avg_dl mode,
  // so nobody else may read it. Every alternative added to the variant is
  // checked here, which is the point: a new scorer cannot skip this.
  if constexpr (!std::is_same_v<P, irs::BM25::Options>) {
    const sdb::catalog::ScorerOptions bm25_min_norm{
      irs::BM25::Options{.k1 = 1.2f, .b = 0.75f}};
    ASSERT_EQ(BoundType::MinNorm, irs::BoundTypeOf(bm25_min_norm));
    EXPECT_FALSE(scorer->Compatible(bm25_min_norm));
  }
}

TEST(scorer_bounds_test, every_scorer_agrees_with_its_bound_classification) {
  []<std::size_t... I>(std::index_sequence<I...>) {
    (CheckAlternative<std::variant_alternative_t<I, Params>>(), ...);
  }(std::make_index_sequence<std::variant_size_v<Params>>{});
}

}  // namespace
