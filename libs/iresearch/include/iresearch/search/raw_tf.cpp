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

#include "raw_tf.hpp"

#include "basics/empty.hpp"
#include "basics/misc.hpp"
#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {
namespace {

template<typename T>
constexpr const T* TryGetValue(const T* value) noexcept {
  return value;
}

constexpr std::nullptr_t TryGetValue(utils::Empty /*value*/) noexcept {
  return nullptr;
}

Scorer::ptr MakeJson(std::string_view /*args*/) {
  return std::make_unique<RawTF>();
}

Scorer::ptr MakeVPack(std::string_view /*args*/) {
  return std::make_unique<RawTF>();
}

template<ScoreMergeType MergeType, bool HasBoost>
IRS_FORCE_INLINE void RawTfImpl(
  score_t* IRS_RESTRICT res, scores_size_t n, const uint32_t* IRS_RESTRICT freq,
  [[maybe_unused]] const score_t* IRS_RESTRICT boost, score_t num) noexcept {
  for (scores_size_t i = 0; i != n; ++i) {
    const auto r = [&] IRS_FORCE_INLINE {
      if constexpr (HasBoost) {
        return boost[i] * num * static_cast<score_t>(freq[i]);
      } else {
        return num * static_cast<score_t>(freq[i]);
      }
    }();
    Merge<MergeType>(res[i], r);
  }
}

template<bool HasFilterBoost>
struct RawTfScore : public ScoreOperator {
  RawTfScore(score_t boost, const FreqBlockAttr* freq,
             const score_t* filter_boost) noexcept
    : freq{freq}, filter_boost{filter_boost}, boost{boost} {
    SDB_ASSERT(this->freq);
  }

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* IRS_RESTRICT res,
                                  scores_size_t n) const noexcept {
    RawTfImpl<MergeType, HasFilterBoost>(res, n, freq->value,
                                         TryGetValue(filter_boost), boost);
  }

  score_t Score() const noexcept final {
    score_t res{};
    ScoreImpl(&res, 1);
    return res;
  }

  void Score(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl(res, n);
  }
  void ScoreSum(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, n);
  }
  void ScoreMax(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, n);
  }

  void ScoreBlock(score_t* res) const noexcept final {
    ScoreImpl(res, kScoreBlock);
  }
  void ScoreSumBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, kScoreBlock);
  }
  void ScoreMaxBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, kScoreBlock);
  }

  void ScorePostingBlock(score_t* res) const noexcept final {
    ScoreImpl(res, kPostingBlock);
  }

  const FreqBlockAttr* freq;
  [[no_unique_address]] utils::Need<HasFilterBoost, const score_t*>
    filter_boost;
  score_t boost;
};

}  // namespace

ScoreFunction RawTF::PrepareScorer(const ScoreContext& ctx) const {
  auto* freq = irs::get<FreqBlockAttr>(ctx.doc_attrs);
  if (!freq) {
    // No frequency available (e.g. irs::all filter) -- score is 0
    // per RawTFSimilarity convention; defer to the default
    // zero scorer.
    if (0.f == ctx.boost) {
      return ScoreFunction::Default();
    }
    return ScoreFunction::Default();
  }

  auto* filter_boost = [&] {
    auto* attr = irs::get<BoostBlockAttr>(ctx.doc_attrs);
    return attr ? attr->value : nullptr;
  }();

  return ResolveBool(filter_boost != nullptr, [&]<bool HasBoost>() {
    return ScoreFunction::Make<RawTfScore<HasBoost>>(ctx.boost, freq,
                                                     filter_boost);
  });
}

void RawTF::init() {
  REGISTER_SCORER_JSON(RawTF, MakeJson);
  REGISTER_SCORER_VPACK(RawTF, MakeVPack);
}

}  // namespace irs
