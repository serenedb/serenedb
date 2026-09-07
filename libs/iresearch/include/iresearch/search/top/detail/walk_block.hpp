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
#include <tuple>
#include <utility>
#include <vector>

#include "iresearch/formats/posting/common.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/common/score/make_conjunction.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top::detail {

template<typename Lead, typename Others>
class WalkBlock {
 public:
  static constexpr uint32_t kFill = kScoreBlock;

  WalkBlock(ColumnArgsFetcher& fetcher, score_t absorbed, ScoreMergeType merge,
            Lead&& lead, Others&& others)
    : _fetcher{fetcher}, _lead{std::move(lead)}, _others{std::move(others)} {
    Collect(absorbed, merge);
  }

  template<typename LeadArgs, typename OthersArgs>
  WalkBlock(std::piecewise_construct_t, ColumnArgsFetcher& fetcher,
            score_t absorbed, ScoreMergeType merge, LeadArgs&& lead,
            OthersArgs&& others)
    : _fetcher{fetcher},
      _lead{std::make_from_tuple<Lead>(std::forward<LeadArgs>(lead))},
      _others{std::make_from_tuple<Others>(std::forward<OthersArgs>(others))} {
    Collect(absorbed, merge);
  }

  uint32_t Fill(doc_id_t* IRS_RESTRICT docs, score_t* IRS_RESTRICT scores) {
    auto doc = doc_limits::valid(_doc) ? _doc : _lead.Advance();
    uint32_t batch = 0;

    while (!doc_limits::eof(doc)) {
      const auto probe = _others.Probe(doc);
      if (probe != doc) {
        doc = _lead.Seek(probe);
        continue;
      }
      docs[batch] = doc;
      _lead.FetchScoreArgs(batch);
      _others.FetchScoreArgs(batch);
      doc = _lead.Advance();
      if (++batch == kFill) {
        break;
      }
    }

    _doc = doc;
    if (batch == 0) {
      return 0;
    }
    if (batch == kFill) {
      ScoreFull(docs, scores);
    } else {
      Score(docs, scores, batch);
    }
    return batch;
  }

 private:
  void Collect(score_t absorbed, ScoreMergeType merge) {
    std::vector<ScoreFunction> scorers;
    search::AppendScorer(scorers, _lead.PrepareScore());
    _others.CollectScorers(scorers);
    _score = search::MakeConjunctionScore(merge, std::move(scorers), absorbed);
  }

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
  Lead _lead;
  Others _others;
  ScoreFunction _score;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::top::detail
