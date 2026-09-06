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

#include "iresearch/search/common/score/make_probe.hpp"

#include <absl/base/optimization.h>

#include <bit>
#include <ranges>

#include "basics/bit_utils.hpp"
#include "basics/empty.hpp"
#include "basics/shared.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs::search {
namespace {

template<ScoreMergeType Inner, bool HasConst>
class ProbeScore : public ScoreOperator {
  static_assert(Inner != ScoreMergeType::Noop);

 public:
  ProbeScore(ScoreFunction&& required, std::vector<ScoreFunction>&& probed,
             uint32_t* held, score_t constant) noexcept
    : _required{std::move(required)},
      _probed{std::move(probed)},
      _held{held},
      _constant{constant} {
    SDB_ASSERT(_probed.empty() || _held != nullptr);
  }

  score_t Score() const noexcept final {
    score_t res = 0;
    bool covered = false;
    if (!_required.IsDefault()) {
      res = _required.Score();
      covered = true;
    }
    for (size_t i = 0, count = _probed.size(); i != count; ++i) {
      const auto held = _held[i];
      _held[i] = 0;
      if (!CheckBit(held, 0)) {
        continue;
      }
      const auto one = _probed[i].Score();
      if (covered) {
        irs::Merge<Inner>(res, one);
      } else {
        res = one;
        covered = true;
      }
    }
    if constexpr (HasConst) {
      if (covered) {
        irs::Merge<Inner>(res, _constant);
      } else {
        res = _constant;
      }
    }
    return res;
  }

  void Score(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Noop>(res, n);
  }
  void ScoreSum(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, n);
  }
  void ScoreMax(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, n);
  }

  void ScoreBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Noop>(res, kScoreBlock);
  }
  void ScoreSumBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, kScoreBlock);
  }
  void ScoreMaxBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, kScoreBlock);
  }

 private:
  template<ScoreMergeType Outer>
  IRS_FORCE_INLINE void ScoreImpl(score_t* IRS_RESTRICT res,
                                  scores_size_t n) const noexcept {
    SDB_ASSERT(n <= kScoreBlock);
    uint32_t covered = 0;
    if (!_required.IsDefault()) {
      _required.Score(_gathered, n);
      covered = ~uint32_t{0};
    }
    for (size_t i = 0, count = _probed.size(); i != count; ++i) {
      const auto held = _held[i];
      _held[i] = 0;
      if (held == 0) {
        continue;
      }
      _probed[i].Score(_scratch, n);
      auto fresh = held & ~covered;
      auto again = held & covered;
      covered |= held;
      while (fresh != 0) {
        const auto slot = static_cast<uint32_t>(std::countr_zero(fresh));
        fresh = PopBit(fresh);
        _gathered[slot] = _scratch[slot];
      }
      while (again != 0) {
        const auto slot = static_cast<uint32_t>(std::countr_zero(again));
        again = PopBit(again);
        irs::Merge<Inner>(_gathered[slot], _scratch[slot]);
      }
    }
    for (scores_size_t i = 0; i != n; ++i) {
      if (CheckBit(covered, i)) {
        auto value = _gathered[i];
        if constexpr (HasConst) {
          irs::Merge<Inner>(value, _constant);
        }
        irs::Merge<Outer>(res[i], value);
      } else if constexpr (HasConst) {
        irs::Merge<Outer>(res[i], _constant);
      } else if constexpr (Outer == ScoreMergeType::Noop) {
        res[i] = 0;
      }
    }
  }

  ScoreFunction _required;
  std::vector<ScoreFunction> _probed;
  uint32_t* IRS_RESTRICT _held;
  [[no_unique_address]] utils::Need<HasConst, score_t> _constant;
  ABSL_CACHELINE_ALIGNED mutable score_t _gathered[kScoreBlock];
  ABSL_CACHELINE_ALIGNED mutable score_t _scratch[kScoreBlock];
};

template<bool HasConst>
ScoreFunction Resolve(ScoreMergeType inner, ScoreFunction&& required,
                      std::vector<ScoreFunction>&& probed, uint32_t* held,
                      score_t constant) {
  if (inner == ScoreMergeType::Max) {
    return ScoreFunction::Make<ProbeScore<ScoreMergeType::Max, HasConst>>(
      std::move(required), std::move(probed), held, constant);
  }
  SDB_ASSERT(inner == ScoreMergeType::Sum);
  return ScoreFunction::Make<ProbeScore<ScoreMergeType::Sum, HasConst>>(
    std::move(required), std::move(probed), held, constant);
}

bool AnyProbed(const std::vector<ScoreFunction>& probed) noexcept {
  return std::ranges::any_of(
    probed, [](const ScoreFunction& s) { return !s.IsDefault(); });
}

}  // namespace

ScoreFunction MakeProbeScore(ScoreMergeType inner, ScoreFunction&& required,
                             std::vector<ScoreFunction>&& probed,
                             uint32_t* held, score_t constant) {
  if (!AnyProbed(probed)) {
    probed.clear();
    if (constant == 0) {
      return std::move(required);
    }
    if (required.IsDefault()) {
      return ScoreFunction::Constant(constant);
    }
  }
  if (constant != 0) {
    return Resolve<true>(inner, std::move(required), std::move(probed), held,
                         constant);
  }
  return Resolve<false>(inner, std::move(required), std::move(probed), held,
                        constant);
}

}  // namespace irs::search
