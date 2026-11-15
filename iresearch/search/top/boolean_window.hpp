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

#include <algorithm>
#include <tuple>
#include <type_traits>
#include <utility>

#include "iresearch/index/iterators.hpp"
#include "iresearch/search/detail/boolean_groups.hpp"
#include "iresearch/search/detail/boolean_window_base.hpp"
#include "iresearch/search/detail/window.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Lead, typename Optional, typename Excludes, typename Table>
class BooleanWindow
  : public Root,
    irs::detail::WindowClauses<Lead, utils::Empty, Optional, Excludes> {
  using Clauses =
    irs::detail::WindowClauses<Lead, utils::Empty, Optional, Excludes>;
  using Clauses::_excludes;
  using Clauses::_lead;
  using Clauses::_optional;

 public:
  using Clauses::kExcludes;
  using Clauses::kLead;
  using Clauses::kOptional;
  static constexpr bool kTally = kOptional && irs::detail::Tallies<Optional>();
  static constexpr bool kScoredLead = kLead && kOptional;
  static constexpr bool kConstant = kLead && !kOptional;
  static_assert(kLead || kOptional);
  static_assert(!kTally || !kExcludes);
  static_assert(!kScoredLead || (!kTally && !kExcludes));

  template<typename LeadArgs, typename OptionalArgs, typename ExcludesArgs>
  BooleanWindow(Table table, bool prune, std::piecewise_construct_t,
                LeadArgs&& lead, OptionalArgs&& optional,
                ExcludesArgs&& excludes, ScoreMergeType merge, score_t absorbed)
    : Clauses{std::piecewise_construct, std::forward<LeadArgs>(lead),
              std::tuple<>{}, std::forward<OptionalArgs>(optional),
              std::forward<ExcludesArgs>(excludes)},
      _score{merge, absorbed},
      _admit{table},
      _constant{merge == ScoreMergeType::Max ? std::max(score_t{0}, absorbed)
                                             : absorbed},
      _prune{prune} {}

  void Run(doc_id_t begin, doc_id_t end, LoserScoreCollector& collector) final {
    doc_id_t next = begin;
    for (;;) {
      if (!_admit.Skip(next) || next >= end) {
        break;
      }
      if constexpr (kConstant) {
        if (_prune && !collector.Admits(_constant)) {
          break;
        }
      }
      const auto min = next;
      const auto max = std::min<doc_id_t>(min + irs::detail::kWindowDocs, end);
      if constexpr (kScoredLead) {
        next = _lead.Fill(min, max, _mask, _window);
        _optional.AddMasked(min, max, _mask, _window);
      } else if constexpr (kLead) {
        next = _lead.FillOr(min, max, _mask);
        if constexpr (kExcludes) {
          _excludes.Remove(min, max, _mask);
        }
      } else if constexpr (kTally) {
        next = _optional.FillTouched(min, max, _mask, _window);
        Tally();
      } else {
        next = _optional.Fill(min, max, _mask, _window);
        if constexpr (kExcludes) {
          _excludes.Remove(min, max, _mask, _window, score_t{0});
        }
      }
      _score.Apply(_window, _mask, irs::detail::kWindowWords);
      _admit.Window(collector, _window, _mask, min, irs::detail::kWindowWords);
    }
    _admit.Flush(collector);
  }

 private:
  void Tally() {
    irs::detail::TallyMask(_mask, _mask, _optional.Counts(), _window,
                           _optional.MinMatch(), irs::detail::kWindowWords);
  }

  ABSL_CACHELINE_ALIGNED uint64_t _mask[irs::detail::kWindowWords]{};
  ABSL_CACHELINE_ALIGNED score_t _window[irs::detail::kWindowDocs]{};
  irs::detail::RootWindowScore _score;
  [[no_unique_address]] Admit<Table> _admit;
  [[no_unique_address]] utils::Need<kConstant, score_t> _constant;
  bool _prune;
};

}  // namespace irs::top
