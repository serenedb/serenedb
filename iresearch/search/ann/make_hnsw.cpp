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

#include "iresearch/search/ann/family.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/queries/hnsw_query.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/score_provider.hpp"
#include "iresearch/search/scorers/scorer.hpp"

namespace irs::ann {
namespace {

class HnswHits : public Root {
 public:
  HnswHits(std::vector<ScoreDoc>&& hits, std::vector<score_t>&& spreads,
           const SubReader& segment, ColumnArgsFetcher& fetcher,
           irs::detail::TableFilter* table, const irs::detail::ScoreArgs& args)
    : _hits{std::move(hits)},
      _spreads{std::move(spreads)},
      _fetcher{fetcher},
      _table{table},
      _boost{args.boost} {
    _score = irs::detail::PrepareBlockScorer(
      args, segment, irs::detail::NoField(), _provider, _block);
  }

  void Run(Collector& collector) final {
    for (size_t i = 0, total = _hits.size(); i < total;) {
      const auto n =
        static_cast<uint32_t>(std::min<size_t>(kScoreBlock, total - i));
      for (uint32_t j = 0; j < n; ++j) {
        _block[j] = _hits[i + j].score;
        _docs[j] = _hits[i + j].doc;
        _spread[j] = _spreads.empty() ? 0.f : _boost * _spreads[i + j];
      }
      _fetcher.Fetch(std::span<const doc_id_t>{_docs, n});
      _score.Score(_scores, static_cast<scores_size_t>(n));
      auto live = n;
      if (_table) {
        std::copy_n(_docs, n, _all);
        live = _table->Narrow(_docs, _scores, n);
        for (uint32_t k = 0, j = 0; j < live; ++k) {
          if (_all[k] == _docs[j]) {
            _spread[j++] = _spread[k];
          }
        }
      }
      collector.Append(_docs, _scores, _spread, live);
      i += n;
    }
  }

 private:
  std::vector<ScoreDoc> _hits;
  std::vector<score_t> _spreads;
  irs::detail::ScaleProvider _provider;
  ScoreFunction _score;
  ColumnArgsFetcher& _fetcher;
  irs::detail::TableFilter* _table;
  score_t _boost;
  score_t _block[kScoreBlock];
  score_t _scores[kScoreBlock];
  score_t _spread[kScoreBlock];
  doc_id_t _docs[kScoreBlock];
  doc_id_t _all[kScoreBlock];
};

}  // namespace
}  // namespace irs::ann
namespace irs {

ann::Root::ptr HnswQuery::PlanAnn(const ann::Context& ctx) const {
  auto* const narrow =
    ctx.table && !ctx.table->Foldable() ? ctx.table : nullptr;
  std::vector<score_t> spreads;
  auto hits = RunSearch(ctx.table, &spreads);
  if (hits.empty()) {
    return ann::MakeEmpty();
  }
  const auto record = Stats(ann::ScoredOf(ctx));
  const detail::ScoreArgs args{.scorer = record.scorer,
                               .stats = record.stats,
                               .fetcher = &ctx.fetcher,
                               .boost = Boost()};
  return memory::make_managed<ann::HnswHits>(
    std::move(hits), std::move(spreads), Segment(), ctx.fetcher, narrow, args);
}

}  // namespace irs
