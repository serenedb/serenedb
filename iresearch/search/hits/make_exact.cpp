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

#include "iresearch/search/detail/exact_scan.hpp"
#include "iresearch/search/hits/make.hpp"
#include "iresearch/search/scorers/score_provider.hpp"

namespace irs::hits {
namespace {

// Every row of the segment with its exact score, ascending by doc.
class ExactHits : public Root {
 public:
  ExactHits(const ExactVectorQuery& query, const Context& ctx,
            const irs::detail::ScoreArgs& args)
    : _scanner{query}, _fetcher{ctx.fetcher} {
    SDB_ASSERT(args.scorer != nullptr);
    _provider.attr.value = _block;
    _score = args.scorer->PrepareScorer({
      .segment = query.Segment(),
      .field = irs::detail::NoField(),
      .doc_attrs = _provider,
      .fetcher = ctx.fetcher,
      .stats = args.stats,
      .boost = args.boost,
    });
  }

  uint32_t Run(doc_id_t min, doc_id_t max, doc_id_t* IRS_RESTRICT docs,
               score_t* IRS_RESTRICT scores) final {
    _scanner.Reset(min, max);
    _pos = 0;
    _have = 0;
    uint32_t n = 0;
    for (;;) {
      if (_pos == _have) {
        _have = _scanner.Next(_docs, _dists);
        _pos = 0;
        if (_have == 0) {
          return n;
        }
      }
      const auto m =
        static_cast<scores_size_t>(std::min<uint32_t>(_have - _pos,
                                                      kScoreBlock));
      std::copy_n(_docs + _pos, m, docs + n);
      std::copy_n(_dists + _pos, m, _block);
      _pos += m;
      _fetcher.Fetch(std::span<const doc_id_t>{docs + n, m});
      _score.Score(scores + n, m);
      n += m;
    }
  }

 private:
  static constexpr uint32_t kRun = irs::detail::ExactScanner::kRun;

  irs::detail::ExactScanner _scanner;
  irs::detail::ScaleProvider _provider;
  ScoreFunction _score;
  ColumnArgsFetcher& _fetcher;
  score_t _block[kScoreBlock];
  doc_id_t _docs[kRun];
  score_t _dists[kRun];
  uint32_t _pos = 0;
  uint32_t _have = 0;
};

}  // namespace

Root::ptr Make(const ExactVectorQuery& query, const Context& ctx) {
  const auto record = query.Stats(ScoredOf(ctx));
  const irs::detail::ScoreArgs args{.scorer = record.scorer,
                                    .stats = record.stats,
                                    .fetcher = &ctx.fetcher,
                                    .boost = query.Boost()};
  return memory::make_managed<ExactHits>(query, ctx, args);
}

}  // namespace irs::hits
