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
#include <cstddef>
#include <utility>

#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/detail/score_filter.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/prune_leaves.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Lead, typename Optional, typename Table>
class PrunedReqOpt : public Root {
 public:
  static constexpr uint32_t kChunk = kScoreBlock;
  static constexpr uint64_t kLeapfrogCost = 2;

  template<typename Init>
  PrunedReqOpt(Table table, ColumnArgsFetcher& fetcher, size_t size,
               Init&& init)
    : _optional{fetcher, size - 1,
                [&](auto& leaf, size_t i) { init(leaf, i + 1); }},
      _admit{table} {
    SDB_ASSERT(size > 1);
    init(_lead, 0);
  }

  PrunedReqOpt(PrunedReqOpt&&) = delete;
  PrunedReqOpt& operator=(PrunedReqOpt&&) = delete;

  void Run(doc_id_t min, doc_id_t max, LoserScoreCollector& collector) final {
    for (auto doc = _lead.Seek(min); doc < max;) {
      const auto threshold = collector.ScoreThreshold();
      _optional.AdvanceTo(doc);
      auto last = _lead.BlockLast();
      if (last >= max) [[unlikely]] {
        last = max - 1;
      }
      const auto lead_max = _lead.MaxScore(last);
      const auto optional_max = _optional.OpenWindow(doc, last);
      if (lead_max + optional_max <= threshold) {
        doc = _lead.Seek(last + 1);
        continue;
      }
      if (const auto drivers = _optional.Drivers(lead_max, threshold);
          drivers != 0 &&
          _optional.DriverCost(drivers) * kLeapfrogCost < _lead.Cost()) {
        doc = OptionalFirst(doc, last, drivers, collector);
        continue;
      }
      FlushBatch(collector);
      ScoreWindow(last, optional_max, collector);
      doc = _lead.Value();
    }
    FlushBatch(collector);
    _admit.Flush(collector);
  }

 private:
  doc_id_t OptionalFirst(doc_id_t doc, doc_id_t last, size_t drivers,
                         LoserScoreCollector& collector) {
    auto target = _optional.FirstOf(drivers, doc);
    while (target <= last) {
      const auto lead = _lead.Seek(target);
      if (lead != target) {
        if (lead > last) {
          return lead;
        }
        target = _optional.FirstOf(drivers, lead);
        continue;
      }
      _batch.docs[_batch.size] = target;
      _batch.freqs[_batch.size] = _lead.Freq();
      _optional.Gather(target, _batch.size);
      if (++_batch.size == kChunk) {
        FlushBatch(collector);
      }
      target = _optional.FirstOf(drivers, target + 1);
    }
    return _lead.Seek(last + 1);
  }

  void FlushBatch(LoserScoreCollector& collector) {
    _batch.Flush(
      _lead, _optional,
      [&](score_t* scores, uint32_t n)
        IRS_FORCE_INLINE { _optional.ScoreHeld(scores, n); },
      _admit, collector);
  }

  void ScoreWindow(doc_id_t last, score_t optional_max,
                   LoserScoreCollector& collector) {
    auto threshold = collector.ScoreThreshold();
    _lead.ForEachScoredBlock(
      last + 1, [&](doc_id_t* docs, uint32_t len, score_t* scores) {
        if (const auto required = threshold - optional_max; required > 0) {
          len = irs::detail::FilterScores(docs, scores, len, required);
        }
        for (uint32_t off = 0; off < len; off += kChunk) {
          const auto n = std::min<uint32_t>(kChunk, len - off);
          const auto kept =
            _optional.AddOptional(docs + off, scores + off, n, threshold);
          if (kept != 0) {
            _admit.AddDocs(collector, docs + off, kept, scores + off);
            threshold = collector.ScoreThreshold();
          }
        }
      });
  }

  Lead _lead;
  Optional _optional;
  ScoreBatch _batch;
  [[no_unique_address]] Admit<Table> _admit;
};

}  // namespace irs::top
