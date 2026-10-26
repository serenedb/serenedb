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

#include <span>
#include <utility>

#include "iresearch/index/iterators.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/detail/sparse_cursor.hpp"
#include "iresearch/search/lead/concept.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<lead::Type Lead, typename Probes, typename Optional, typename Excludes,
         typename Table>
class BooleanSparse : public Root {
 public:
  static constexpr uint32_t kBatch = kScoreBlock;

  template<typename LeadArgs, typename ProbesArgs, typename OptionalArgs,
           typename ExcludesArgs>
  BooleanSparse(Table table, std::piecewise_construct_t,
                ColumnArgsFetcher& fetcher, irs::detail::Scored score,
                LeadArgs&& lead, ProbesArgs&& probes, OptionalArgs&& optional,
                ExcludesArgs&& excludes)
    : _fetcher{fetcher},
      _cursor{score, std::forward<LeadArgs>(lead),
              std::forward<ProbesArgs>(probes),
              std::forward<OptionalArgs>(optional),
              std::forward<ExcludesArgs>(excludes)},
      _admit{table} {
    _score = _cursor.PrepareScore();
  }

  BooleanSparse(BooleanSparse&&) = delete;
  BooleanSparse& operator=(BooleanSparse&&) = delete;

  void Run(doc_id_t min, doc_id_t max, LoserScoreCollector& collector) final {
    ABSL_CACHELINE_ALIGNED doc_id_t docs[kBatch];
    ABSL_CACHELINE_ALIGNED score_t scores[kBatch];
    uint32_t batch = 0;
    for (auto doc = _cursor.Seek(min, max); doc < max;
         doc = _cursor.Next(max)) {
      docs[batch] = doc;
      _cursor.FetchScoreArgs(batch);
      if (++batch == kBatch) {
        FlushBatch(docs, scores, collector);
        batch = 0;
      }
    }
    if (batch != 0) {
      _fetcher.Fetch(std::span<const doc_id_t>{docs, batch});
      _score.Score(scores, static_cast<scores_size_t>(batch));
      _admit.AddDocs(collector, docs, batch, scores);
    }
    _admit.Flush(collector);
  }

  IRS_NO_INLINE void FlushBatch(const doc_id_t* IRS_RESTRICT docs,
                                score_t* IRS_RESTRICT scores,
                                LoserScoreCollector& collector) {
    _fetcher.FetchScoreBlock(
      std::span<const doc_id_t, kScoreBlock>{docs, kScoreBlock});
    _score.ScoreBlock(scores);
    _admit.AddDocs(collector, docs, kBatch, scores);
  }

 private:
  ColumnArgsFetcher& _fetcher;
  irs::detail::SparseCursor<Lead, Probes, Optional, Excludes> _cursor;
  ScoreFunction _score;
  [[no_unique_address]] Admit<Table> _admit;
};

}  // namespace irs::top
