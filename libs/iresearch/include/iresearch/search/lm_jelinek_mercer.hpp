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

// Language model with Jelinek-Mercer smoothing.
//
// LMJelinekMercerSimilarity:
//   score(doc, term) = boost * log(1 + ((1 - lambda) * tf / dl) /
//                                      (lambda * P(t | C)))
// where
//   P(t | C) = (total_term_freq_of_term + 1) /
//              (total_term_freq_of_field + 1)
//
// Parameters:
//   lambda in (0, 1]. We recommend ~0.1 for title queries
//   and ~0.7 for long queries.
class LMJelinekMercer final : public irs::ScorerBase<LMJelinekMercer, LMStats> {
 public:
  static constexpr std::string_view type_name() noexcept { return "lm_jm"; }

  static constexpr score_t LAMBDA() noexcept { return 0.1f; }

  static void init();

  explicit LMJelinekMercer(score_t lambda = LAMBDA()) noexcept
    : _lambda{lambda} {}

  void collect(byte_type* stats_buf, const irs::FieldCollector::Data* field,
               const irs::TermCollector* term) const final;

  IndexFeatures GetIndexFeatures() const noexcept final {
    return IndexFeatures::Freq | IndexFeatures::Norm;
  }

  TermCollector::ptr PrepareTermCollector() const final;

  ScoreFunction PrepareScorer(const ScoreContext& ctx) const final;

  bool equals(const Scorer& other) const noexcept final;

  score_t lambda() const noexcept { return _lambda; }

 private:
  score_t _lambda;
};

}  // namespace irs
