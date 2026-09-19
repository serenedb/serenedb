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
#include <utility>

#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/hits/root.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::hits {

template<typename Node>
class Walk : public Root {
 public:
  static constexpr uint32_t kBatch = kScoreBlock;

  template<typename... Args>
  explicit Walk(ColumnArgsFetcher& fetcher, Args&&... args)
    : _fetcher{fetcher}, _node{std::forward<Args>(args)...} {
    _score = _node.PrepareScore();
  }

  uint32_t Run(doc_id_t min, doc_id_t max, doc_id_t* IRS_RESTRICT out,
               score_t* IRS_RESTRICT scores) final {
    uint32_t n = 0;
    uint32_t batch = 0;

    auto doc = _node.Seek(min);
    while (doc < max) {
      out[n] = doc;
      _node.FetchScoreArgs(batch);
      ++n;
      if (++batch == kBatch) {
        ScoreFull(out + n - batch, scores + n - batch);
        batch = 0;
      }
      doc = _node.Next();
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
  Node _node;
  ScoreFunction _score;
};

template<typename Node>
class ConstantWalk : public Root {
 public:
  template<typename... Args>
  explicit ConstantWalk(score_t score, Args&&... args)
    : _node{std::forward<Args>(args)...}, _score{score} {}

  uint32_t Run(doc_id_t min, doc_id_t max, doc_id_t* IRS_RESTRICT out,
               score_t* IRS_RESTRICT scores) final {
    uint32_t n = 0;

    auto doc = _node.Seek(min);
    while (doc < max) {
      out[n++] = doc;
      doc = _node.Next();
    }
    std::fill_n(scores, n, _score);
    return n;
  }

 private:
  Node _node;
  score_t _score;
};

}  // namespace irs::hits
