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
#include "iresearch/search/lead/concept.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Node, typename Table>
class Walk : public Root {
 public:
  template<typename... Args>
  explicit Walk(Table table, ColumnArgsFetcher& fetcher, Args&&... args)
    : _fetcher{fetcher}, _node{std::forward<Args>(args)...}, _admit{table} {
    _score = _node.PrepareScore();
  }

  void Run(doc_id_t min, doc_id_t max, LoserScoreCollector& collector) final {
    ABSL_CACHELINE_ALIGNED doc_id_t docs[kScoreBlock];
    ABSL_CACHELINE_ALIGNED score_t scores[kScoreBlock];
    uint32_t batch = 0;

    for (auto doc = lead::SeekBelow(_node, min, max); doc < max;
         doc = lead::NextBelow(_node, max)) {
      docs[batch] = doc;
      _node.FetchScoreArgs(batch);
      if (++batch == kScoreBlock) {
        AddBlock(docs, scores, collector);
        batch = 0;
      }
    }

    if (batch != 0) {
      FetchAndScore(_fetcher, _score, docs, scores, batch);
      _admit.AddDocs(collector, docs, batch, scores);
    }
    _admit.Flush(collector);
  }

 private:
  IRS_NO_INLINE void AddBlock(const doc_id_t* IRS_RESTRICT docs,
                              score_t* IRS_RESTRICT scores,
                              LoserScoreCollector& collector) {
    FetchAndScoreBlock(_fetcher, _score, docs, scores);
    _admit.AddDocs(collector, docs, kScoreBlock, scores);
  }

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
  explicit ConstantWalk(Table table, bool prune, score_t score, Args&&... args)
    : _node{std::forward<Args>(args)...},
      _score{score},
      _admit{table},
      _prune{prune} {}

  void Run(doc_id_t min, doc_id_t max, LoserScoreCollector& collector) final {
    if (_prune && !collector.Admits(_score)) {
      return;
    }
    ABSL_CACHELINE_ALIGNED doc_id_t docs[kBatch];
    ABSL_CACHELINE_ALIGNED score_t scores[kBatch];
    std::fill_n(scores, kBatch, _score);
    for (auto doc = lead::SeekBelow(_node, min, max); doc < max;) {
      const auto n = lead::Gather(_node, doc, max, docs, kBatch);
      _admit.AddDocs(collector, docs, n, scores);
      if (_prune) {
        _admit.Flush(collector);
        if (!collector.Admits(_score)) {
          return;
        }
      }
    }
    _admit.Flush(collector);
  }

 private:
  Node _node;
  score_t _score;
  [[no_unique_address]] Admit<Table> _admit;
  bool _prune;
};

}  // namespace irs::top
