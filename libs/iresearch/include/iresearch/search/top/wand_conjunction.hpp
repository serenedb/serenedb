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

#include <absl/base/optimization.h>

#include <algorithm>
#include <cstddef>
#include <utility>

#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/detail/prune_leaves.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Lead, typename Others, typename Table>
class WandConjunction : public Root {
 public:
  static constexpr uint32_t kChunk = kScoreBlock;
  static constexpr size_t kNarrowWindowClauses = 4;
  static constexpr uint32_t kNarrowFragment = 32;

  template<typename Init>
  WandConjunction(Table table, ColumnArgsFetcher& fetcher, size_t size,
                  Init&& init)
    : _others{fetcher, size - 1,
              [&](auto& leaf, size_t i) { init(leaf, i + 1); }},
      _admit{table} {
    SDB_ASSERT(size > 1);
    _narrow = size >= kNarrowWindowClauses;
    init(_lead, 0);
  }

  void Run(LoserScoreCollector& collector) final {
    for (auto doc = _lead.Advance(); !doc_limits::eof(doc);) {
      auto threshold = collector.ScoreThreshold();
      const auto others_end = _others.AdvanceTo(doc);
      const auto lead_last = _lead.BlockLast();
      auto last = lead_last;
      if (_narrow && doc < others_end && others_end < lead_last &&
          static_cast<uint64_t>(others_end - doc) * kNarrowFragment >=
            static_cast<uint64_t>(lead_last - doc)) {
        last = others_end;
      }
      const auto bound = _lead.MaxScore(last) + _others.OpenWindow(doc, last);

      if (bound <= threshold) {
        doc = _lead.Seek(last + 1);
        continue;
      }

      _lead.ForEachScoredBlock(
        last + 1, [&](doc_id_t* docs, uint32_t len, score_t* scores) {
          for (uint32_t off = 0; off < len; off += kChunk) {
            const auto n = std::min<uint32_t>(kChunk, len - off);
            const auto kept =
              _others.Apply(docs + off, scores + off, n, threshold);
            if (kept != 0) {
              _admit.AddDocs(collector, docs + off, kept, scores + off);
              threshold = collector.ScoreThreshold();
            }
          }
        });

      doc = _lead.Value();
    }
    _admit.Flush(collector);
  }

 private:
  Lead _lead;
  Others _others;
  bool _narrow = false;
  [[no_unique_address]] Admit<Table> _admit;
};

}  // namespace irs::top
