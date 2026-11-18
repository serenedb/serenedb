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

#include <absl/functional/function_ref.h>

#include <cstdint>
#include <optional>
#include <span>
#include <string_view>

#include "iresearch/search/explain/explanation.hpp"
#include "iresearch/search/explain/stats.hpp"
#include "iresearch/search/scorers/scorer.hpp"

namespace irs::explain {

using IdfOf = score_t (*)(uint64_t docs_with_field, uint64_t docs_with_term);

std::optional<uint32_t> FreqOf(const ScoreContext& ctx) noexcept;

void AddBoost(Node& out, score_t boost);

void AddScale(Node& out, const ScoreContext& ctx);

score_t AddFreq(Node& out, const ScoreContext& ctx);

uint32_t AddLength(Node& out, const ScoreContext& ctx);

void AddAverageLength(Node& out, score_t value, const SlotStats* stats);

void AddIdf(Node& out, score_t idf, const SlotStats* stats,
            std::string_view expression, IdfOf per_term);

void AddCollectionShare(Node& out, score_t value, const SlotStats* stats);

void AddShare(Node& out, score_t value, uint64_t ttf, uint64_t total);

using DescribeTerm =
  absl::FunctionRef<void(Node& part, const TermStats& term, score_t share)>;

void ExplainTerms(const SlotStats* stats, Node& out, DescribeTerm describe);

using TermFormula =
  absl::FunctionRef<score_t(score_t tf, score_t dl, score_t share)>;

void ExplainLanguageModel(const ScoreContext& ctx,
                          std::span<const score_t> shares,
                          const SlotStats* stats, Node parameter,
                          std::string_view expression, TermFormula score,
                          Node& out);

void ExplainNoFreq(const ScoreContext& ctx, bool boost_as_score, Node& out);

}  // namespace irs::explain
