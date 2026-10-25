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

#include <absl/algorithm/container.h>

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
#include "iresearch/search/top/family.hpp"
#include "iresearch/search/top/make.hpp"
#include "iresearch/utils/containers/fixed.hpp"

namespace irs::top {
namespace {

class HnswHits : public Root {
 public:
  HnswHits(std::vector<ScoreDoc>&& hits, std::vector<score_t>&& spreads,
           const SubReader& segment, ColumnArgsFetcher& fetcher,
           irs::detail::TableFilter* table, const irs::detail::ScoreArgs& args)
    : _hits{hits.size(),
            [&](ScoreDoc& slot, size_t i) noexcept { slot = hits[i]; }},
      _spreads{std::move(spreads)},
      _fetcher{fetcher},
      _table{table},
      _boost{args.boost} {
    _score = irs::detail::PrepareBlockScorer(
      args, segment, irs::detail::NoField(), _provider, _block);
  }

  void Run(doc_id_t min, doc_id_t max, LoserScoreCollector& collector) final {
    const auto by_doc = [](const ScoreDoc& hit, doc_id_t doc) {
      return hit.doc < doc;
    };
    auto lo = absl::c_lower_bound(_hits, min, by_doc);
    const auto hi = doc_limits::eof(max)
                      ? _hits.end()
                      : std::lower_bound(lo, _hits.end(), max, by_doc);
    const size_t first = static_cast<size_t>(lo - _hits.begin());
    const size_t total = static_cast<size_t>(hi - _hits.begin());
    for (size_t i = first; i < total;) {
      const auto n =
        static_cast<uint32_t>(std::min<size_t>(kScoreBlock, total - i));
      for (uint32_t j = 0; j < n; ++j) {
        _block[j] = _hits[i + j].score;
        _docs[j] = _hits[i + j].doc;
      }
      _fetcher.Fetch(std::span<const doc_id_t>{_docs, n});
      _score.Score(_scores, static_cast<scores_size_t>(n));
      if (!_spreads.empty()) {
        std::copy_n(_docs, n, _all);
      }
      const auto live = _table ? _table->Narrow(_docs, _scores, n) : n;
      if (!_spreads.empty()) {
        for (uint32_t k = 0, j = 0; j < live; ++k) {
          if (_all[k] == _docs[j]) {
            _scores[j++] += _boost * _spreads[i + k];
          }
        }
      }
      collector.AddDocs(_docs, live, _scores);
      i += n;
    }
  }

 private:
  containers::Fixed<ScoreDoc> _hits;
  std::vector<score_t> _spreads;
  irs::detail::ScaleProvider _provider;
  ScoreFunction _score;
  ColumnArgsFetcher& _fetcher;
  irs::detail::TableFilter* _table;
  score_t _boost;
  score_t _block[kScoreBlock];
  score_t _scores[kScoreBlock];
  doc_id_t _docs[kScoreBlock];
  doc_id_t _all[kScoreBlock];
};

}  // namespace

Root::ptr Family::Make(const HnswQuery& query, const Context& ctx) {
  auto* const narrow =
    ctx.table && !ctx.table->Foldable() ? ctx.table : nullptr;
  std::vector<score_t> spreads;
  auto hits = query.RunSearch(ctx.table, &spreads);
  if (hits.empty()) {
    return MakeEmpty();
  }
  const auto record = query.Stats(ScoredOf(ctx));
  const irs::detail::ScoreArgs args{.scorer = record.scorer,
                                    .stats = record.stats,
                                    .fetcher = &ctx.fetcher,
                                    .boost = query.Boost()};
  return memory::make_managed<HnswHits>(std::move(hits), std::move(spreads),
                                        query.Segment(), ctx.fetcher, narrow,
                                        args);
}

}  // namespace irs::top
