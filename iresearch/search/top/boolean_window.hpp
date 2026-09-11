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

#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/detail/boolean_groups.hpp"
#include "iresearch/search/detail/window.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Lead, typename Optional, typename Excludes, typename Table>
class BooleanWindow : public Root {
 public:
  static constexpr bool kLead = !std::is_same_v<Lead, utils::Empty>;
  static constexpr bool kOptional = !std::is_same_v<Optional, utils::Empty>;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;
  static constexpr bool kTally = kOptional && irs::detail::Tallies<Optional>();
  static_assert(kLead != kOptional);
  static_assert(!kTally || !kExcludes);

  template<typename LeadArgs, typename OptionalArgs, typename ExcludesArgs>
  BooleanWindow(Table table, std::piecewise_construct_t, LeadArgs&& lead,
                OptionalArgs&& optional, ExcludesArgs&& excludes,
                ScoreMergeType merge, score_t absorbed)
    : _lead{std::make_from_tuple<Lead>(std::forward<LeadArgs>(lead))},
      _optional{
        std::make_from_tuple<Optional>(std::forward<OptionalArgs>(optional))},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))},
      _score{merge, absorbed},
      _admit{table} {}

  BooleanWindow(BooleanWindow&&) = delete;
  BooleanWindow& operator=(BooleanWindow&&) = delete;

  void Run(LoserScoreCollector& collector) final {
    doc_id_t next = doc_limits::min();
    while (!doc_limits::eof(next)) {
      if (!_admit.Skip(next)) {
        break;
      }
      const auto min = next;
      const auto max = min + irs::detail::kWindowDocs;
      if constexpr (kLead) {
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
  [[no_unique_address]] Lead _lead;
  [[no_unique_address]] Optional _optional;
  [[no_unique_address]] Excludes _excludes;
  irs::detail::RootWindowScore _score;
  [[no_unique_address]] Admit<Table> _admit;
};

}  // namespace irs::top
