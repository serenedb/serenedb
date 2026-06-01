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

#pragma once

#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/lm_similarity.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/scorers.hpp"

namespace irs {

// Language model with Bayesian (Dirichlet) smoothing.
//
// LMDirichletSimilarity:
//   score(doc, term) = boost * (log(1 + tf / (mu * P(t|C))) +
//                               log(mu / (dl + mu)))
// clamped to [0, +inf) because the raw formula can yield negative
// contributions for terms that appear less often than the collection
// prior predicts.
//
// P(t|C) = (ttf_term + 1) / (ttf_field + 1)
class LMDirichlet final : public irs::ScorerBase<LMDirichlet, LMStats> {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "lm_dirichlet";
  }

  static constexpr score_t MU() noexcept { return 2000.f; }

  static void init();

  explicit LMDirichlet(score_t mu = MU()) noexcept : _mu{mu} {}

  void collect(byte_type* stats_buf, const irs::FieldCollector* field,
               const irs::TermCollector* term) const final;

  IndexFeatures GetIndexFeatures() const noexcept final {
    return IndexFeatures::Freq | IndexFeatures::Norm;
  }

  ScoreFunction PrepareScorer(const ScoreContext& ctx) const final;

  bool equals(const Scorer& other) const noexcept final;

  score_t mu() const noexcept { return _mu; }

 private:
  score_t _mu;
};

}  // namespace irs
