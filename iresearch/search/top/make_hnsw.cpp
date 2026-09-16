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

#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/queries/hnsw_query.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/score_provider.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/search/top/make.hpp"
#include "iresearch/utils/containers/fixed.hpp"

namespace irs::top {
namespace {

class HnswHits : public Root {
 public:
  HnswHits(std::vector<ScoreDoc>&& hits, const SubReader& segment,
           ColumnArgsFetcher& fetcher, irs::detail::TableFilter* table,
           const irs::detail::ScoreArgs& args)
    : _hits{hits.size(),
            [&](ScoreDoc& slot, size_t i) noexcept { slot = hits[i]; }},
      _fetcher{fetcher},
      _table{table} {
    SDB_ASSERT(args.scorer != nullptr);
    _provider.attr.value = _block;
    _score = args.scorer->PrepareScorer({
      .segment = segment,
      .field = irs::detail::NoField(),
      .doc_attrs = _provider,
      .fetcher = fetcher,
      .stats = args.stats,
      .boost = args.boost,
    });
  }

  void Run(doc_id_t min, doc_id_t max, LoserScoreCollector& collector) final {
    SDB_ASSERT(min == doc_limits::min() && doc_limits::eof(max));
    for (size_t i = 0, total = _hits.size(); i < total;) {
      const auto n =
        static_cast<uint32_t>(std::min<size_t>(kScoreBlock, total - i));
      for (uint32_t j = 0; j < n; ++j) {
        _block[j] = _hits[i + j].score;
        _docs[j] = _hits[i + j].doc;
      }
      _fetcher.Fetch(std::span<const doc_id_t>{_docs, n});
      _score.Score(_scores, static_cast<scores_size_t>(n));
      // The hits are ascending by doc, so a block is the run the table
      // filter narrows in place.
      const auto live =
        _table == nullptr ? n : _table->Narrow(_docs, _scores, n);
      collector.AddDocs(_docs, live, _scores);
      i += n;
    }
  }

 private:
  containers::Fixed<ScoreDoc> _hits;
  irs::detail::ScaleProvider _provider;
  ScoreFunction _score;
  ColumnArgsFetcher& _fetcher;
  irs::detail::TableFilter* _table;
  score_t _block[kScoreBlock];
  score_t _scores[kScoreBlock];
  doc_id_t _docs[kScoreBlock];
};

}  // namespace

Root::ptr Make(const HnswQuery& query, const Context& ctx) {
  auto hits = query.RunSearch(ctx.table, ctx.part, ctx.parts);
  if (hits.empty()) {
    return {};
  }
  // A table that folded into the search has been applied; one that did not
  // (a predicate on the score) narrows the hits.
  auto* const table =
    ctx.table != nullptr && !ctx.table->Foldable() ? ctx.table : nullptr;
  const auto record = query.Stats(ScoredOf(ctx));
  const irs::detail::ScoreArgs args{.scorer = record.scorer,
                                    .stats = record.stats,
                                    .fetcher = &ctx.fetcher,
                                    .boost = query.Boost()};
  return memory::make_managed<HnswHits>(std::move(hits), query.Segment(),
                                        ctx.fetcher, table, args);
}

}  // namespace irs::top
