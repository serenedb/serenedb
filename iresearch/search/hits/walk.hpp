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
    for (;;) {
      while (_held != _batched) {
        const auto doc = _batch[_held];
        if (doc >= max) {
          return n;
        }
        const auto score = _batch_scores[_held];
        ++_held;
        if (doc < min) [[unlikely]] {
          continue;
        }
        out[n] = doc;
        scores[n] = score;
        ++n;
      }
      if (_spent) {
        return n;
      }
      Gather(min);
    }
  }

 private:
  IRS_NO_INLINE void Gather(doc_id_t min) {
    _held = 0;
    _batched = 0;
    auto doc = _pos;
    if (!doc_limits::valid(doc) || doc < min) {
      doc = _node.Seek(min);
    }
    uint32_t batch = 0;
    while (!doc_limits::eof(doc) && batch != kBatch) {
      _batch[batch] = doc;
      _node.FetchScoreArgs(batch);
      ++batch;
      doc = _node.Next();
    }
    _pos = doc;
    _spent = doc_limits::eof(doc);
    if (batch == 0) {
      return;
    }
    if (batch == kBatch) {
      ScoreFull(_batch, _batch_scores);
    } else {
      Score(_batch, _batch_scores, batch);
    }
    _batched = batch;
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
  Node _node;
  ScoreFunction _score;
  ABSL_CACHELINE_ALIGNED doc_id_t _batch[kBatch];
  ABSL_CACHELINE_ALIGNED score_t _batch_scores[kBatch];
  uint32_t _batched = 0;
  uint32_t _held = 0;
  doc_id_t _pos = doc_limits::invalid();
  bool _spent = false;
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
