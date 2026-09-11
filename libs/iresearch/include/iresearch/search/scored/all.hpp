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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/detail/table_filter.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scored/root.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::scored {

template<typename Table>
class All : public Root {
 public:
  static constexpr bool kTable = !std::is_same_v<Table, utils::Empty>;

  All(Table table, ColumnArgsFetcher& fetcher, doc_id_t count,
      score_t score = 0) noexcept
    : _end{doc_limits::min() + count},
      _score{ScoreFunction::Constant(score)},
      _fetcher{fetcher},
      _table{table} {}

  void Prepare(const SubReader& segment, const detail::ScoreArgs& args) {
    if (detail::AllDocsConstant(args)) {
      _score = ScoreFunction::Constant(AllDocsScore(segment, args));
      return;
    }
    _score = detail::AllDocsScorer(segment, args);
  }

  uint32_t Run(doc_id_t* IRS_RESTRICT out, score_t* IRS_RESTRICT scores,
               uint32_t capacity) final {
    SDB_ASSERT(capacity >= doc_limits::kMinCapacity);
    if constexpr (kTable) {
      _doc = std::min(_table.Live(_doc), _end);
    }
    const auto n = std::min<uint32_t>(capacity, _end - _doc);
    for (uint32_t i = 0; i != n; ++i) {
      out[i] = _doc + i;
    }
    uint32_t offset = 0;
    for (; offset + kScoreBlock <= n; offset += kScoreBlock) {
      _fetcher.FetchScoreBlock(
        std::span<const doc_id_t, kScoreBlock>{out + offset, kScoreBlock});
      _score.ScoreBlock(scores + offset);
    }
    if (offset != n) {
      _fetcher.Fetch(std::span<const doc_id_t>{out + offset, n - offset});
      _score.Score(scores + offset, static_cast<scores_size_t>(n - offset));
    }
    _doc += n;
    return n;
  }

 private:
  doc_id_t _doc = doc_limits::min();
  doc_id_t _end;
  ScoreFunction _score;
  ColumnArgsFetcher& _fetcher;
  [[no_unique_address]] detail::Narrowing<Table> _table;
};

}  // namespace irs::scored
