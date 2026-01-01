////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/attributes.hpp"

namespace irs {

// Represents a score related for the particular document
// min score set by document consumers
// max score set by document producers
struct ScoreAttr : Attribute, ScoreFunction {
  static const ScoreAttr kNoScore;

  static constexpr std::string_view type_name() noexcept { return "score"; }

  template<typename Provider>
  static const ScoreAttr& get(const Provider& attrs) {
    const auto* score = irs::get<irs::ScoreAttr>(attrs);
    return score ? *score : kNoScore;
  }

  using ScoreFunction::operator=;

  // For disjunction/conjunction it's just sum of sub-iterators max score
  // For iterator without score it depends on count of documents in iterator
  // For wanderator it's max score for whole skip-list
  // TODO(mbkkt) tail better here and not affect correctness
  //  but to support it we need to know max value in the tail blocks.
  //  Open question: how do it without read next blocks?
  // TODO(mbkkt) At least when iterator exhausted, we could set it to zero.
  struct UpperBounds {
    score_t tail = std::numeric_limits<score_t>::max();
    score_t leaf = std::numeric_limits<score_t>::max();
#ifdef SDB_GTEST
    std::span<const score_t> levels;  // levels.back() == leaf
#endif
  } max;
};

using ScoreFunctions = sdb::containers::SmallVector<ScoreFunction, 2>;

// Prepare scorer for each of the bucket.
ScoreFunctions PrepareScorers(std::span<const ScorerBucket> buckets,
                              const ColumnProvider& segment,
                              const TermReader& field, const byte_type* stats,
                              const AttributeProvider& doc, score_t boost);

// Compiles a set of prepared scorers into a single score function.
ScoreFunction CompileScorers(ScoreFunctions&& scorers);

void CompileScore(irs::ScoreAttr& score, std::span<const ScorerBucket> buckets,
                  const ColumnProvider& segment, const TermReader& field,
                  const byte_type* stats, const AttributeProvider& doc,
                  score_t boost);

// Prepare empty collectors, i.e. call collect(...) on each of the
// buckets without explicitly collecting field or term statistics,
// e.g. for 'all' filter.
void PrepareCollectors(std::span<const ScorerBucket> order, byte_type* stats);

}  // namespace irs
