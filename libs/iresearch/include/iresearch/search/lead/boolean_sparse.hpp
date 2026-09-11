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
#include "iresearch/search/lead/concept.hpp"
#include "iresearch/search/scorers/make_conjunction.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/score_policy.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

template<Type Lead, typename Probes, typename Optional, typename Excludes,
         typename Score = utils::Empty>
class BooleanSparse {
 public:
  static constexpr bool kProbes = !std::is_same_v<Probes, utils::Empty>;
  static constexpr bool kOptional = !std::is_same_v<Optional, utils::Empty>;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;
  static constexpr bool kScored = std::is_same_v<Score, detail::Scored>;
  static constexpr bool kInherited = std::is_same_v<Score, detail::Inherited>;
  static_assert(kProbes || kOptional || kExcludes);
  static_assert(kScored || !kOptional);
  static_assert(!kInherited || (!kProbes && !kOptional));

  template<typename LeadArgs, typename ProbesArgs, typename OptionalArgs,
           typename ExcludesArgs>
  BooleanSparse(std::piecewise_construct_t, LeadArgs&& lead,
                ProbesArgs&& probes, OptionalArgs&& optional,
                ExcludesArgs&& excludes, Score score = {})
    : _lead{std::make_from_tuple<Lead>(std::forward<LeadArgs>(lead))},
      _probes{std::make_from_tuple<Probes>(std::forward<ProbesArgs>(probes))},
      _optional{
        std::make_from_tuple<Optional>(std::forward<OptionalArgs>(optional))},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))},
      _score{score} {}

  BooleanSparse(BooleanSparse&&) = delete;
  BooleanSparse& operator=(BooleanSparse&&) = delete;

  doc_id_t Next() { return Converge(_lead.Next()); }

  doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    return Converge(_lead.Seek(target));
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot)
    requires(kScored || kInherited)
  {
    _lead.FetchScoreArgs(slot);
    if constexpr (kProbes) {
      _probes.FetchScoreArgs(slot);
    }
    if constexpr (kOptional) {
      _optional.FetchScoreArgs(slot, _doc);
    }
  }

  ScoreFunction PrepareScore()
    requires(kScored || kInherited)
  {
    if constexpr (kInherited) {
      return _lead.PrepareScore();
    } else {
      auto required = Required();
      if constexpr (kOptional) {
        return _optional.PrepareScore(_score.inner, std::move(required),
                                      _score.absorbed);
      } else {
        return required;
      }
    }
  }

  void CollectScorers(std::vector<ScoreFunction>& out)
    requires(kScored || kInherited)
  {
    detail::AppendScorer(out, PrepareScore());
  }

 private:
  ScoreFunction Required()
    requires kScored
  {
    if constexpr (kProbes) {
      std::vector<ScoreFunction> scorers;
      detail::AppendScorer(scorers, _lead.PrepareScore());
      _probes.CollectScorers(scorers);
      return detail::MakeConjunctionScore(
        _score.inner, std::move(scorers),
        kOptional ? score_t{0} : _score.absorbed);
    } else {
      return _lead.PrepareScore();
    }
  }

  doc_id_t Converge(doc_id_t doc) {
    while (!doc_limits::eof(doc)) {
      if constexpr (kProbes) {
        const auto probe = _probes.Probe(doc);
        if (probe != doc) {
          doc = _lead.Seek(probe);
          continue;
        }
      }
      if constexpr (kExcludes) {
        if (detail::IsExcluded(_excludes, doc)) {
          doc = _lead.Next();
          continue;
        }
      }
      break;
    }
    return _doc = doc;
  }

  Lead _lead;
  [[no_unique_address]] Probes _probes;
  [[no_unique_address]] Optional _optional;
  [[no_unique_address]] Excludes _excludes;
  doc_id_t _doc = doc_limits::invalid();
  [[no_unique_address]] Score _score;
};

}  // namespace irs::lead
