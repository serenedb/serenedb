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
#include "iresearch/search/hits/make.hpp"
#include "iresearch/search/queries/hnsw_query.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/score_provider.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/containers/fixed.hpp"

namespace irs::hits {
namespace {

class HnswHits : public Root {
 public:
  HnswHits(std::vector<ScoreDoc>&& hits, const SubReader& segment,
           ColumnArgsFetcher& fetcher, const irs::detail::ScoreArgs& args)
    : _hits{hits.size(),
            [&](ScoreDoc& slot, size_t i) noexcept { slot = hits[i]; }},
      _fetcher{fetcher} {
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

  uint32_t Run(doc_id_t min, doc_id_t max, doc_id_t* IRS_RESTRICT docs,
               score_t* IRS_RESTRICT scores) final {
    uint32_t n = 0;
    for (;;) {
      scores_size_t m = 0;
      for (; _pos != _hits.size() && m != kScoreBlock; ++_pos) {
        const auto& hit = _hits[_pos];
        if (hit.doc >= max) {
          break;
        }
        if (hit.doc < min) {
          continue;
        }
        docs[n + m] = hit.doc;
        _block[m] = hit.score;
        ++m;
      }
      if (m == 0) {
        return n;
      }
      _fetcher.Fetch(std::span<const doc_id_t>{docs + n, m});
      _score.Score(scores + n, m);
      n += m;
    }
  }

 private:
  containers::Fixed<ScoreDoc> _hits;
  irs::detail::ScaleProvider _provider;
  ScoreFunction _score;
  ColumnArgsFetcher& _fetcher;
  score_t _block[kScoreBlock];
  size_t _pos = 0;
};

}  // namespace

Root::ptr Make(const HnswQuery& query, const Context& ctx) {
  const auto record = query.Stats(ScoredOf(ctx));
  const irs::detail::ScoreArgs args{.scorer = record.scorer,
                                    .stats = record.stats,
                                    .fetcher = &ctx.fetcher,
                                    .boost = query.Boost()};
  return memory::make_managed<HnswHits>(query.RunSearch(), query.Segment(),
                                        ctx.fetcher, args);
}

}  // namespace irs::hits
