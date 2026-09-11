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

#include <algorithm>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/common/score_provider.hpp"
#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/hnsw_query.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scored/make.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs::scored {
namespace {

class HnswHits : public Root {
 public:
  HnswHits(std::vector<ScoreDoc>&& hits, const SubReader& segment,
           ColumnArgsFetcher& fetcher, search::DeadRuns* table,
           const search::ScoreArgs& args)
    : _hits{std::move(hits)}, _fetcher{fetcher}, _table{table} {
    SDB_ASSERT(args.scorer != nullptr);
    _provider.attr.value = _block;
    _score = args.scorer->PrepareScorer({
      .segment = segment,
      .field = search::NoField(),
      .doc_attrs = _provider,
      .fetcher = fetcher,
      .stats = args.stats,
      .boost = args.boost,
    });
  }

  uint32_t Run(doc_id_t* IRS_RESTRICT docs, score_t* IRS_RESTRICT scores,
               uint32_t capacity) final {
    const auto limit = std::min<uint32_t>(capacity, kScoreBlock);
    uint32_t n = 0;
    while (_pos != _hits.size() && n != limit) {
      const auto& hit = _hits[_pos++];
      if (_table != nullptr && _table->Live(hit.doc) != hit.doc) {
        continue;
      }
      docs[n] = hit.doc;
      _block[n] = hit.score;
      ++n;
    }
    if (n == 0) {
      return 0;
    }
    _fetcher.Fetch(std::span<const doc_id_t>{docs, n});
    _score.Score(scores, static_cast<scores_size_t>(n));
    return n;
  }

 private:
  std::vector<ScoreDoc> _hits;
  search::BoostProvider _provider;
  ScoreFunction _score;
  ColumnArgsFetcher& _fetcher;
  search::DeadRuns* _table;
  score_t _block[kScoreBlock];
  size_t _pos = 0;
};

}  // namespace

Root::ptr Make(const HnswQuery& query, const Context& ctx) {
  const auto record = query.Stats(ScoredOf(ctx));
  const search::ScoreArgs args{.scorer = record.scorer,
                               .stats = record.stats,
                               .fetcher = &ctx.fetcher,
                               .boost = query.Boost()};
  return memory::make_managed<HnswHits>(query.RunSearch(), query.Segment(),
                                        ctx.fetcher, ctx.table, args);
}

}  // namespace irs::scored
