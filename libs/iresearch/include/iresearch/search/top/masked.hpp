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

#include <span>
#include <utility>

#include "iresearch/index/index_meta.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename NodeType, typename Table>
class Masked : public Root {
 public:
  static constexpr uint32_t kBatch = kScoreBlock;

  template<typename... Args>
  Masked(Table table, ColumnArgsFetcher& fetcher, const DocumentMask& mask,
         Args&&... args)
    : _fetcher{fetcher},
      _mask{&mask},
      _node{std::forward<Args>(args)...},
      _admit{table} {
    _score = _node.PrepareScore();
  }

  void Run(LoserScoreCollector& collector) final {
    uint32_t batch = 0;

    for (auto doc = _node.Advance(); !doc_limits::eof(doc);
         doc = _node.Advance()) {
      if (_mask->contains(doc)) {
        continue;
      }
      _docs[batch] = doc;
      _node.FetchScoreArgs(batch);
      if (++batch == kBatch) {
        ScoreFull();
        _admit.AddDocs(collector, _docs, kBatch, _scores);
        batch = 0;
      }
    }

    if (batch != 0) {
      Score(batch);
      _admit.AddDocs(collector, _docs, batch, _scores);
    }
    _admit.Flush(collector);
  }

 private:
  void ScoreFull() {
    _fetcher.FetchScoreBlock(
      std::span<const doc_id_t, kScoreBlock>{_docs, kScoreBlock});
    _score.ScoreBlock(_scores);
  }

  void Score(uint32_t len) {
    _fetcher.Fetch(std::span<const doc_id_t>{_docs, len});
    _score.Score(_scores, static_cast<scores_size_t>(len));
  }

  ABSL_CACHELINE_ALIGNED doc_id_t _docs[kBatch];
  ABSL_CACHELINE_ALIGNED score_t _scores[kBatch];
  ColumnArgsFetcher& _fetcher;
  const DocumentMask* _mask;
  NodeType _node;
  ScoreFunction _score;
  [[no_unique_address]] Admit<Table> _admit;
};

}  // namespace irs::top
