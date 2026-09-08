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

#include <span>
#include <tuple>
#include <utility>

#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scored/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::scored {

template<typename Include, typename Excludes, typename Table>
class SparseExclusion : public Root {
 public:
  static constexpr bool kTable = !std::is_same_v<Table, utils::Empty>;
  static constexpr uint32_t kBatch = kScoreBlock;

  template<typename IncludeArgs, typename ExcludesArgs>
  SparseExclusion(Table table, std::piecewise_construct_t,
                  ColumnArgsFetcher& fetcher, IncludeArgs&& include,
                  ExcludesArgs&& excludes)
    : _fetcher{fetcher},
      _include{
        std::make_from_tuple<Include>(std::forward<IncludeArgs>(include))},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))},
      _table{table} {
    _score = _include.PrepareScore();
  }

  SparseExclusion(SparseExclusion&&) = delete;
  SparseExclusion& operator=(SparseExclusion&&) = delete;

  uint32_t Run(doc_id_t* IRS_RESTRICT out, score_t* IRS_RESTRICT scores,
               uint32_t capacity) final {
    SDB_ASSERT(capacity >= doc_limits::kMinCapacity);
    uint32_t n = 0;
    uint32_t batch = 0;
    auto doc = _include.Advance();

    while (!doc_limits::eof(doc)) {
      if constexpr (kTable) {
        if (const auto live = _table.Live(doc); live != doc) {
          doc = _include.Seek(live);
          continue;
        }
      }
      if (_excludes.Probe(doc) == doc) {
        doc = _include.Advance();
        continue;
      }
      out[n] = doc;
      _include.FetchScoreArgs(batch);
      ++n;
      if (++batch == kBatch) {
        ScoreFull(out + n - batch, scores + n - batch);
        batch = 0;
      }
      if (n == capacity) {
        break;
      }
      doc = _include.Advance();
    }

    if (batch != 0) {
      Score(out + n - batch, scores + n - batch, batch);
    }
    return n;
  }

 private:
  void ScoreFull(const doc_id_t* docs, score_t* scores) {
    _fetcher.FetchScoreBlock(
      std::span<const doc_id_t, kScoreBlock>{docs, kScoreBlock});
    _score.ScoreBlock(scores);
  }

  void Score(const doc_id_t* docs, score_t* scores, uint32_t len) {
    _fetcher.Fetch(std::span<const doc_id_t>{docs, len});
    _score.Score(scores, static_cast<scores_size_t>(len));
  }

  ColumnArgsFetcher& _fetcher;
  Include _include;
  Excludes _excludes;
  ScoreFunction _score;
  [[no_unique_address]] search::Narrowing<Table> _table;
};

}  // namespace irs::scored
