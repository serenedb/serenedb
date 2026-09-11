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

#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "basics/empty.hpp"
#include "basics/shared.hpp"
#include "iresearch/search/detail/exclude_block.hpp"
#include "iresearch/search/scorers/make_conjunction.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/score_policy.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::probe {

template<typename Optional>
consteval bool OptionalDecides() {
  if constexpr (requires { Optional::kDecides; }) {
    return Optional::kDecides;
  } else {
    return true;
  }
}

template<typename Musts, typename Optional, typename Excludes,
         typename Score = utils::Empty>
class BooleanSparse {
 public:
  static constexpr bool kMusts = !std::is_same_v<Musts, utils::Empty>;
  static constexpr bool kOptional = !std::is_same_v<Optional, utils::Empty>;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;
  static constexpr bool kScored = std::is_same_v<Score, detail::Scored>;
  static constexpr bool kInherited = std::is_same_v<Score, detail::Inherited>;
  static_assert(kMusts || kOptional);
  static_assert(!kInherited || (kMusts && !kOptional));

  template<typename MustsArgs, typename OptionalArgs, typename ExcludesArgs>
  BooleanSparse(std::piecewise_construct_t, MustsArgs&& musts,
                OptionalArgs&& optional, ExcludesArgs&& excludes,
                Score score = {})
    : _musts{std::make_from_tuple<Musts>(std::forward<MustsArgs>(musts))},
      _optional{
        std::make_from_tuple<Optional>(std::forward<OptionalArgs>(optional))},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))},
      _score{score} {}

  BooleanSparse(BooleanSparse&&) = delete;
  BooleanSparse& operator=(BooleanSparse&&) = delete;

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    if constexpr (kMusts) {
      if (const auto probe = _musts.Probe(target); probe != target) {
        return probe;
      }
    }
    if constexpr (kOptional) {
      if (const auto probe = _optional.Probe(target); probe != target) {
        return probe;
      }
    }
    if constexpr (kExcludes) {
      if (detail::IsExcluded(_excludes, target)) {
        return target + 1;
      }
    }
    return target;
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot)
    requires(kScored || kInherited)
  {
    if constexpr (kMusts) {
      _musts.FetchScoreArgs(slot);
    }
    if constexpr (kOptional) {
      _optional.FetchScoreArgs(slot);
    }
  }

  ScoreFunction PrepareScore()
    requires(kScored || kInherited)
  {
    if constexpr (kInherited) {
      return _musts.PrepareScore();
    } else if constexpr (!kMusts && OptionalDecides<Optional>()) {
      return _optional.PrepareScore(_score.inner, _score.absorbed);
    } else {
      std::vector<ScoreFunction> scorers;
      if constexpr (kMusts) {
        _musts.CollectScorers(scorers);
      }
      if constexpr (kOptional) {
        detail::AppendScorer(scorers, OptionalScore());
      }
      return detail::MakeConjunctionScore(_score.inner, std::move(scorers),
                                          _score.absorbed);
    }
  }

  void CollectScorers(std::vector<ScoreFunction>& out)
    requires(kScored || kInherited)
  {
    if constexpr (kInherited) {
      _musts.CollectScorers(out);
    } else {
      detail::AppendScorer(out, PrepareScore());
    }
  }

 private:
  ScoreFunction OptionalScore()
    requires kScored
  {
    if constexpr (requires { _optional.PrepareScore(_score.inner); }) {
      return _optional.PrepareScore(_score.inner);
    } else if constexpr (requires {
                           _optional.PrepareScore(_score.inner, score_t{0});
                         }) {
      return _optional.PrepareScore(_score.inner, score_t{0});
    } else {
      return _optional.PrepareScore();
    }
  }

  [[no_unique_address]] Musts _musts;
  [[no_unique_address]] Optional _optional;
  [[no_unique_address]] Excludes _excludes;
  [[no_unique_address]] Score _score;
};

}  // namespace irs::probe
