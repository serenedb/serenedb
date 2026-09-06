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
#include <span>
#include <utility>

#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Table>
class All : public Root {
 public:
  static constexpr uint32_t kBatch = kScoreBlock;

  All(Table table, ColumnArgsFetcher& fetcher, doc_id_t count,
      score_t score = 0) noexcept
    : _end{doc_limits::min() + count},
      _score{ScoreFunction::Constant(score)},
      _fetcher{fetcher},
      _admit{table} {}

  All(Table table, ColumnArgsFetcher& fetcher, doc_id_t count,
      ScoreFunction score) noexcept
    : _end{doc_limits::min() + count},
      _score{std::move(score)},
      _fetcher{fetcher},
      _admit{table} {}

  void Run(LoserScoreCollector& collector) final {
    ABSL_CACHELINE_ALIGNED doc_id_t docs[kBatch];
    ABSL_CACHELINE_ALIGNED score_t scores[kBatch];

    for (auto doc = doc_limits::min(); doc < _end;) {
      const auto n = std::min<uint32_t>(kBatch, _end - doc);
      for (uint32_t i = 0; i != n; ++i) {
        docs[i] = doc + i;
      }
      _fetcher.Fetch(std::span<const doc_id_t>{docs, n});
      _score.Score(scores, static_cast<scores_size_t>(n));
      _admit.AddDocs(collector, docs, n, scores);
      doc += n;
    }
    _admit.Flush(collector);
  }

 private:
  doc_id_t _end;
  ScoreFunction _score;
  ColumnArgsFetcher& _fetcher;
  [[no_unique_address]] Admit<Table> _admit;
};

}  // namespace irs::top
