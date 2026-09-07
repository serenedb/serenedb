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

#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top::detail {

template<typename Node, typename Table>
class Walk : public Root {
 public:
  template<typename... Args>
  explicit Walk(Table table, ColumnArgsFetcher& fetcher, Args&&... args)
    : _fetcher{fetcher}, _node{std::forward<Args>(args)...}, _admit{table} {
    _score = _node.PrepareScore();
  }

  void Run(LoserScoreCollector& collector) final {
    scores_size_t batch = 0;

    for (auto doc = _node.Advance(); !doc_limits::eof(doc);
         doc = _node.Advance()) {
      _docs[batch] = doc;
      _node.FetchScoreArgs(batch);
      if (++batch == kScoreBlock) {
        _fetcher.FetchScoreBlock(std::span<const doc_id_t, kScoreBlock>{_docs});
        _score.ScoreBlock(_scores);
        _admit.AddDocs(collector, _docs, kScoreBlock, _scores);
        batch = 0;
      }
    }

    if (batch != 0) {
      _fetcher.Fetch(std::span<const doc_id_t>{_docs, batch});
      _score.Score(_scores, static_cast<scores_size_t>(batch));
      _admit.AddDocs(collector, _docs, batch, _scores);
    }
    _admit.Flush(collector);
  }

 private:
  ABSL_CACHELINE_ALIGNED doc_id_t _docs[kScoreBlock];
  ABSL_CACHELINE_ALIGNED score_t _scores[kScoreBlock];
  ColumnArgsFetcher& _fetcher;
  Node _node;
  ScoreFunction _score;
  [[no_unique_address]] Admit<Table> _admit;
};

template<typename Node, typename Table>
class ConstantWalk : public Root {
 public:
  static constexpr uint32_t kBatch = kScoreBlock;

  template<typename... Args>
  explicit ConstantWalk(Table table, score_t score, Args&&... args)
    : _node{std::forward<Args>(args)...}, _score{score}, _admit{table} {}

  void Run(LoserScoreCollector& collector) final {
    ABSL_CACHELINE_ALIGNED doc_id_t docs[kBatch];
    ABSL_CACHELINE_ALIGNED score_t scores[kBatch];
    std::fill_n(scores, kBatch, _score);
    scores_size_t batch = 0;

    for (auto doc = _node.Advance(); !doc_limits::eof(doc);
         doc = _node.Advance()) {
      docs[batch] = doc;
      if (++batch == kBatch) {
        _admit.AddDocs(collector, docs, kBatch, scores);
        batch = 0;
      }
    }

    if (batch != 0) {
      _admit.AddDocs(collector, docs, batch, scores);
    }
    _admit.Flush(collector);
  }

 private:
  Node _node;
  score_t _score;
  [[no_unique_address]] Admit<Table> _admit;
};

}  // namespace irs::top::detail
