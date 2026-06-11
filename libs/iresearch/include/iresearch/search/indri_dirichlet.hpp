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

#include <cmath>

#include "basics/exceptions.h"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/lm_similarity.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {

// Bayesian smoothing using Dirichlet priors as implemented in the Indri
// search engine.
//
// IndriDirichletSimilarity:
//   score(doc, term) = boost * log((tf + mu * P(t | C)) / (dl + mu))
//
// Unlike LMDirichlet which clamps negative contributions to 0, Indri
// returns the raw log ratio -- documents where the observed tf is below
// the collection prior get a negative score, which matters when Indri
// combines scores across query terms via sum.
class IndriDirichlet final : public irs::ScorerBase<IndriDirichlet, LMStats> {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "indri_dirichlet";
  }

  static constexpr score_t MU() noexcept { return 2000.f; }

  struct Options {
    using Owner = IndriDirichlet;
    float mu = MU();
    bool operator==(const Options&) const = default;
  };

  static std::unique_ptr<IndriDirichlet> Make(const Options& opts) {
    if (opts.mu < 0.f || !std::isfinite(opts.mu)) {
      SDB_THROW(sdb::ERROR_BAD_PARAMETER,
                "indri_dirichlet: mu must be a non-negative finite value");
    }
    return std::make_unique<IndriDirichlet>(opts.mu);
  }

  explicit IndriDirichlet(score_t mu = MU()) noexcept : _mu{mu} {}

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
