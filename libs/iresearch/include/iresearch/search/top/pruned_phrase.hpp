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

#include <utility>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/common/score_provider.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Slots, typename Table>
class PrunedPhrase : public Root {
  static_assert(Slots::kHasFreqBound,
                "a phrase with no bound cannot be pruned");

 public:
  template<typename... Args>
  PrunedPhrase(Table table, const SubReader& segment, const TermReader& field,
               const search::ScoreArgs& args, Args&&... slots)
    : _slots{std::forward<Args>(slots)...},
      _fetcher{*args.fetcher},
      _admit{table} {
    _provider.freq.value = &_freq;
    SDB_ASSERT(args.scorer != nullptr);
    _score = args.scorer->PrepareScorer({
      .segment = segment,
      .field = field.meta(),
      .doc_attrs = _provider,
      .fetcher = args.fetcher,
      .stats = args.stats,
      .boost = args.boost,
    });
  }

  void Run(LoserScoreCollector& collector) final {
    for (auto doc = _slots.Next(doc_limits::invalid()); !doc_limits::eof(doc);
         doc = _slots.Next(doc)) {
      _freq = _slots.FreqBound();
      _fetcher.Fetch(doc);
      auto score = _score.Score();
      if (score <= collector.ScoreThreshold() || !_slots.MatchOrdered(doc)) {
        continue;
      }
      if (const auto freq = _slots.Freq(); freq != _freq) {
        SDB_ASSERT(freq < _freq);
        _freq = freq;
        score = _score.Score();
      }
      _admit.Add(collector, score, doc);
    }
    _admit.Flush(collector);
  }

 private:
  Slots _slots;
  search::LeafProvider _provider;
  ScoreFunction _score;
  ColumnArgsFetcher& _fetcher;
  uint32_t _freq = 0;
  [[no_unique_address]] Admit<Table> _admit;
};

}  // namespace irs::top
