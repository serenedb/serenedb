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

#include "score.hpp"

#include "basics/shared.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/field_meta.hpp"

namespace irs {

const ScoreAttr ScoreAttr::kNoScore;

ScoreFunctions PrepareScorers(std::span<const ScorerBucket> buckets,
                              const ColumnProvider& segment,
                              const TermReader& field,
                              const byte_type* stats_buf,
                              const AttributeProvider& doc, score_t boost) {
  ScoreFunctions scorers;
  scorers.reserve(buckets.size());

  for (const auto& entry : buckets) {
    const auto& bucket = *entry.bucket;

    if (!entry.bucket) [[unlikely]] {
      continue;
    }

    auto scorer = bucket.PrepareScorer(
      segment, field.meta(), stats_buf + entry.stats_offset, doc, boost);

    scorers.emplace_back(std::move(scorer));
  }

  return scorers;
}

static ScoreFunction CompileScorers(ScoreFunction&& wand,
                                    ScoreFunctions&& tail) {
  SDB_ASSERT(!tail.empty());
  switch (tail.size()) {
    case 0:
      return std::move(wand);
    default: {
      struct Ctx final : ScoreCtx {
        explicit Ctx(ScoreFunction&& wand, ScoreFunctions&& tail) noexcept
          : wand{std::move(wand)}, tail{std::move(tail)} {}

        ScoreFunction wand;
        ScoreFunctions tail;
      };

      return ScoreFunction::Make<Ctx>(
        [](ScoreCtx* ctx, score_t* res) noexcept {
          auto* scorers_ctx = static_cast<Ctx*>(ctx);
          SDB_ASSERT(res != nullptr);
          scorers_ctx->wand(res);
          for (auto& other : scorers_ctx->tail) {
            other.Score(++res);
          }
        },
        [](ScoreCtx* ctx, score_t arg) noexcept {
          auto* scorers_ctx = static_cast<Ctx*>(ctx);
          scorers_ctx->wand.Min(arg);
        },
        std::move(wand), std::move(tail));
    }
  }
}

ScoreFunction CompileScorers(ScoreFunctions&& scorers) {
  switch (scorers.size()) {
    case 0: {
      return ScoreFunction{};
    }
    case 1: {
      // The most important and frequent case when only
      // one scorer is provided.
      return std::move(scorers.front());
    }
    default: {
      struct Ctx final : ScoreCtx {
        explicit Ctx(ScoreFunctions&& scorers) noexcept
          : scorers{std::move(scorers)} {}

        ScoreFunctions scorers;
      };

      return ScoreFunction::Make<Ctx>(
        [](ScoreCtx* ctx, score_t* res) noexcept {
          auto& scorers = static_cast<Ctx*>(ctx)->scorers;
          for (auto& scorer : scorers) {
            scorer(res++);
          }
        },
        ScoreFunction::DefaultMin, std::move(scorers));
    }
  }
}

void PrepareCollectors(std::span<const ScorerBucket> order,
                       byte_type* stats_buf) {
  for (const auto& entry : order) {
    if (entry.bucket) [[likely]] {
      entry.bucket->collect(stats_buf + entry.stats_offset, nullptr, nullptr);
    }
  }
}

void CompileScore(irs::ScoreAttr& score, std::span<const ScorerBucket> buckets,
                  const ColumnProvider& segment, const TermReader& field,
                  const byte_type* stats, const AttributeProvider& doc,
                  score_t boost) {
  SDB_ASSERT(!buckets.empty());
  // wanderator could have score for first bucket and score upper bounds.
  if (score.IsDefault()) {
    auto scorers = PrepareScorers(buckets, segment, field, stats, doc, boost);
    // wanderator could have score upper bounds.
    if (score.max.tail == std::numeric_limits<score_t>::max()) {
      score.max.leaf = score.max.tail =
        scorers.empty() ? 0.f : scorers.front().Max();
    }
    score = CompileScorers(std::move(scorers));
  } else if (buckets.size() > 1) {
    auto scorers =
      PrepareScorers(buckets.subspan(1), segment, field, stats, doc, boost);
    score = CompileScorers(std::move(score), std::move(scorers));
    SDB_ASSERT(score.max.tail != std::numeric_limits<score_t>::max());
  }
  SDB_ASSERT(score.max.leaf <= score.max.tail);
}

}  // namespace irs
