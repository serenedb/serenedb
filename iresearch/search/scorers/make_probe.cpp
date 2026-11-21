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

#include "iresearch/search/scorers/make_probe.hpp"

#include <absl/algorithm/container.h>
#include <absl/base/optimization.h>

#include <bit>

#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/containers/fixed.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::detail {
namespace {

template<ScoreMergeType Inner, bool HasConst>
class ProbeScore : public ScoreOperatorOf<ProbeScore<Inner, HasConst>, false> {
  static_assert(Inner != ScoreMergeType::Noop);

 public:
  ProbeScore(ScoreFunction&& required, std::vector<ScoreFunction>&& probed,
             ScoreMask* held, score_t constant) noexcept
    : _required{std::move(required)},
      _probed{probed.size(),
              [&](ScoreFunction& slot, size_t i) noexcept {
                slot = std::move(probed[i]);
              }},
      _held{held},
      _constant{constant} {
    SDB_ASSERT(_probed.empty() || _held != nullptr);
  }

  score_t ScoreOne() const noexcept {
    score_t res = 0;
    if (!_required.IsDefault()) {
      irs::Merge<Inner>(res, _required.Score());
    }
    for (size_t i = 0, count = _probed.size(); i != count; ++i) {
      const auto held = _held[i];
      _held[i] = 0;
      if (CheckBit(held, 0)) {
        irs::Merge<Inner>(res, _probed[i].Score());
      }
    }
    if constexpr (HasConst) {
      irs::Merge<Inner>(res, _constant);
    }
    return res;
  }

  template<ScoreMergeType Outer = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* IRS_RESTRICT res,
                                  scores_size_t n) const noexcept {
    SDB_ASSERT(n <= kScoreBlock);
    ScoreMask covered = 0;
    if (!_required.IsDefault()) {
      _required.Score(_gathered, n);
      for (scores_size_t i = 0; i != n; ++i) {
        _gathered[i] = FromDefault(_gathered[i]);
      }
      covered = ~ScoreMask{0};
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
        _gathered[slot] = FromDefault(_scratch[slot]);
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

 private:
  IRS_FORCE_INLINE static score_t FromDefault(score_t score) noexcept {
    if constexpr (Inner == ScoreMergeType::Max) {
      return std::max(score_t{0}, score);
    } else {
      return score;
    }
  }

  ScoreFunction _required;
  containers::Fixed<ScoreFunction> _probed;
  ScoreMask* IRS_RESTRICT _held;
  [[no_unique_address]] utils::Need<HasConst, score_t> _constant;
  ABSL_CACHELINE_ALIGNED mutable score_t _gathered[kScoreBlock];
  ABSL_CACHELINE_ALIGNED mutable score_t _scratch[kScoreBlock];
};

bool AnyProbed(const std::vector<ScoreFunction>& probed) noexcept {
  return absl::c_any_of(probed,
                        [](const ScoreFunction& s) { return !s.IsDefault(); });
}

}  // namespace

ScoreFunction MakeProbeScore(ScoreMergeType inner, ScoreFunction&& required,
                             std::vector<ScoreFunction>&& probed,
                             ScoreMask* held, score_t constant) {
  if (!AnyProbed(probed)) {
    probed.clear();
    if (constant == 0) {
      return std::move(required);
    }
    if (required.IsDefault()) {
      return ScoreFunction::Constant(constant);
    }
  }
  return Resolve(constant != 0, [&]<bool HasConst> {
    return Resolve(inner, [&]<ScoreMergeType M> {
      return ScoreFunction::Make<ProbeScore<M, HasConst>>(
        std::move(required), std::move(probed), held, constant);
    });
  });
}

}  // namespace irs::detail
