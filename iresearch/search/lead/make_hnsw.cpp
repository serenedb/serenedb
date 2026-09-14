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

#include <utility>
#include <vector>

#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/queries/hnsw_query.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/score_provider.hpp"
#include "iresearch/search/scorers/scorer.hpp"

namespace irs::lead {
namespace {

class HnswHits : public Node {
 public:
  HnswHits(std::vector<ScoreDoc>&& hits, const SubReader& segment,
           const detail::ScoreArgs& args)
    : _hits{std::move(hits)} {
    SDB_ASSERT(args.scorer != nullptr);
    _provider.attr.value = _block;
    _score = args.scorer->PrepareScorer({
      .segment = segment,
      .field = detail::NoField(),
      .doc_attrs = _provider,
      .fetcher = *args.fetcher,
      .stats = args.stats,
      .boost = args.boost,
    });
  }

  HnswHits(HnswHits&&) = delete;
  HnswHits& operator=(HnswHits&&) = delete;

  doc_id_t Next() final {
    if (_pos == _hits.size()) {
      return _doc = doc_limits::eof();
    }
    return _doc = _hits[_pos++].doc;
  }

  doc_id_t Seek(doc_id_t target) final {
    if (target <= _doc) {
      return _doc;
    }
    while (_pos != _hits.size() && _hits[_pos].doc < target) {
      ++_pos;
    }
    return Next();
  }

  void FetchScoreArgs(uint32_t slot) final {
    SDB_ASSERT(_pos != 0 && slot < kScoreBlock);
    _block[slot] = _hits[_pos - 1].score;
  }

  ScoreFunction PrepareScore() final { return std::move(_score); }

 private:
  std::vector<ScoreDoc> _hits;
  detail::BoostProvider _provider;
  ScoreFunction _score;
  score_t _block[kScoreBlock];
  size_t _pos = 0;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace

Node::ptr Make(const HnswQuery& query, const detail::ScoredCtx& ctx) {
  const auto record = query.Stats(ctx);
  const detail::ScoreArgs args{.scorer = record.scorer,
                               .stats = record.stats,
                               .fetcher = ctx.fetcher,
                               .boost = query.Boost()};
  return memory::make_managed<HnswHits>(query.RunSearch(), query.Segment(),
                                        args);
}

}  // namespace irs::lead
