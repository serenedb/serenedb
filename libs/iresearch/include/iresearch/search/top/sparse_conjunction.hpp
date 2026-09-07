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

#include <tuple>
#include <utility>

#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/detail/walk_block.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Head, typename Tail, typename Table>
class SparseConjunction : public Root {
 public:
  using Block = detail::WalkBlock<Head, Tail>;

  SparseConjunction(Table table, ColumnArgsFetcher& fetcher, score_t absorbed,
                    ScoreMergeType merge, Head&& head, Tail&& tail)
    : _block{fetcher, absorbed, merge, std::move(head), std::move(tail)},
      _admit{table} {}

  template<typename HeadArgs, typename TailArgs>
  SparseConjunction(Table table, std::piecewise_construct_t,
                    ColumnArgsFetcher& fetcher, score_t absorbed,
                    ScoreMergeType merge, HeadArgs&& head, TailArgs&& tail)
    : _block{std::piecewise_construct,
             fetcher,
             absorbed,
             merge,
             std::forward<HeadArgs>(head),
             std::forward<TailArgs>(tail)},
      _admit{table} {}

  void Run(LoserScoreCollector& collector) final {
    ABSL_CACHELINE_ALIGNED doc_id_t docs[Block::kFill];
    ABSL_CACHELINE_ALIGNED score_t scores[Block::kFill];

    for (;;) {
      const auto len = _block.Fill(docs, scores);
      if (len == 0) {
        break;
      }
      _admit.AddDocs(collector, docs, len, scores);
    }
    _admit.Flush(collector);
  }

 private:
  Block _block;
  [[no_unique_address]] Admit<Table> _admit;
};

}  // namespace irs::top
