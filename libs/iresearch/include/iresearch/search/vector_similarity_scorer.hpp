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

#include "iresearch/search/scorer.hpp"

namespace irs {

// Surfaces the raw per-doc distance computed by VectorDistanceIterator as the
// document score. The iterator publishes the value through a BoostBlockAttr and
// this scorer reads it back unchanged -- it does not negate. Whether nearest or
// farthest wins is imposed by the NthPartitionScoreCollector<Order> the caller
// selects, not here; the distance kernel lives next to the vector reads.
struct VectorSimilarityScorer final : ScorerBase<VectorSimilarityScorer, void> {
  static constexpr std::string_view type_name() noexcept {
    return "vector_similarity";
  }

  struct Options {
    using Owner = VectorSimilarityScorer;
    bool operator==(const Options&) const = default;
  };

  static std::unique_ptr<VectorSimilarityScorer> Make(const Options&) {
    return std::make_unique<VectorSimilarityScorer>();
  }

  ScoreFunction PrepareScorer(const ScoreContext& ctx) const final;

  IndexFeatures GetIndexFeatures() const noexcept final {
    return IndexFeatures::None;
  }
};

}  // namespace irs
