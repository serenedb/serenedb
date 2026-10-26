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

#include "iresearch/search/detail/exclude_block.hpp"
#include "iresearch/search/lead/concept.hpp"
#include "iresearch/search/scorers/make_conjunction.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/score_policy.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

template<lead::Type Lead, typename Probes, typename Optional, typename Excludes>
class SparseCursor {
 public:
  static constexpr bool kProbes = !std::is_same_v<Probes, utils::Empty>;
  static constexpr bool kOptional = !std::is_same_v<Optional, utils::Empty>;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;
  static_assert(kProbes || kOptional || kExcludes);

  template<typename LeadArgs, typename ProbesArgs, typename OptionalArgs,
           typename ExcludesArgs>
  SparseCursor(Scored score, LeadArgs&& lead, ProbesArgs&& probes,
               OptionalArgs&& optional, ExcludesArgs&& excludes)
    : _lead{std::make_from_tuple<Lead>(std::forward<LeadArgs>(lead))},
      _probes{std::make_from_tuple<Probes>(std::forward<ProbesArgs>(probes))},
      _optional{
        std::make_from_tuple<Optional>(std::forward<OptionalArgs>(optional))},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))},
      _score{score} {}

  SparseCursor(SparseCursor&&) = delete;
  SparseCursor& operator=(SparseCursor&&) = delete;

  IRS_FORCE_INLINE doc_id_t Seek(doc_id_t target) {
    return Seek(target, doc_limits::eof());
  }

  IRS_FORCE_INLINE doc_id_t Next() { return Next(doc_limits::eof()); }

  IRS_FORCE_INLINE doc_id_t Seek(doc_id_t target, doc_id_t max) {
    return Converge(_lead.Seek(target), max);
  }

  IRS_FORCE_INLINE doc_id_t Next(doc_id_t max) {
    return Converge(_lead.Next(), max);
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) {
    _lead.FetchScoreArgs(slot);
    if constexpr (kProbes) {
      _probes.FetchScoreArgs(slot);
    }
    if constexpr (kOptional) {
      _optional.FetchScoreArgs(slot);
    }
  }

  ScoreFunction PrepareScore() {
    if constexpr (!kProbes && !kOptional) {
      return _lead.PrepareScore();
    } else {
      std::vector<ScoreFunction> scorers;
      AppendScorer(scorers, _lead.PrepareScore());
      if constexpr (kProbes) {
        _probes.CollectScorers(scorers);
      }
      if constexpr (kOptional) {
        if constexpr (requires { _optional.PrepareScore(_score.inner); }) {
          AppendScorer(scorers, _optional.PrepareScore(_score.inner));
        } else {
          AppendScorer(scorers, _optional.PrepareScore());
        }
      }
      return MakeConjunctionScore(_score.inner, std::move(scorers),
                                  _score.absorbed);
    }
  }

 private:
  IRS_FORCE_INLINE doc_id_t Converge(doc_id_t doc, doc_id_t max) {
    while (doc < max) {
      if constexpr (kProbes) {
        if (const auto probe = _probes.Probe(doc); probe != doc) {
          doc = _lead.Seek(probe);
          continue;
        }
      }
      if constexpr (kOptional) {
        if (const auto probe = _optional.Probe(doc); probe != doc) {
          doc = _lead.Seek(probe);
          continue;
        }
      }
      if (SkipExcluded(_excludes, _lead, doc)) {
        continue;
      }
      break;
    }
    return doc;
  }

  Lead _lead;
  [[no_unique_address]] Probes _probes;
  [[no_unique_address]] Optional _optional;
  [[no_unique_address]] Excludes _excludes;
  Scored _score;
};

}  // namespace irs::detail
