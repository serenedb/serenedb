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
#include <array>
#include <cstring>
#include <span>
#include <utility>

#include "iresearch/search/ann/root.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/filters/filter.hpp"
#include "iresearch/search/lead/two_phase.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::ann {

template<typename Node>
class Walk : public Root {
  static constexpr bool kSpreads =
    requires(const Node& node) { node.Spreads(); };

 public:
  static constexpr uint32_t kRun = 2048;

  template<typename... Args>
  Walk(detail::TableFilter* table, ColumnArgsFetcher& fetcher, Args&&... args)
    : _fetcher{fetcher}, _table{table}, _node{std::forward<Args>(args)...} {
    _score = _node.PrepareScore();
  }

  void Run(Collector& collector) final {
    scores_size_t batch = 0;
    for (auto doc = _node.Seek(doc_limits::min()); !doc_limits::eof(doc);
         doc = _node.Next()) {
      _docs[batch] = doc;
      _node.FetchScoreArgs(batch);
      if (++batch == kScoreBlock) {
        _fetcher.FetchScoreBlock(std::span<const doc_id_t, kScoreBlock>{_docs});
        _score.ScoreBlock(_scores);
        Stage(collector, kScoreBlock);
        batch = 0;
      }
    }
    if (batch != 0) {
      _fetcher.Fetch(std::span<const doc_id_t>{_docs, batch});
      _score.Score(_scores, batch);
      Stage(collector, batch);
    }
    if (_staged != 0) {
      Drain(collector);
    }
  }

 private:
  void Stage(Collector& collector, uint32_t n) {
    std::memcpy(_staged_docs.data() + _staged, _docs, n * sizeof(doc_id_t));
    std::memcpy(_staged_scores.data() + _staged, _scores, n * sizeof(score_t));
    if constexpr (kSpreads) {
      std::memcpy(_staged_spreads.data() + _staged, _node.Spreads(),
                  n * sizeof(score_t));
    } else {
      std::fill_n(_staged_spreads.data() + _staged, n, score_t{0});
    }
    _staged += n;
    if (_staged + kScoreBlock > kRun) {
      Drain(collector);
    }
  }

  void Drain(Collector& collector) {
    auto n = std::exchange(_staged, 0);
    if (_table) {
      std::memcpy(_all.data(), _staged_docs.data(), n * sizeof(doc_id_t));
      const auto live =
        _table->Narrow(_staged_docs.data(), _staged_scores.data(), n);
      for (uint32_t i = 0, j = 0; j < live; ++i) {
        if (_all[i] == _staged_docs[j]) {
          _staged_spreads[j++] = _staged_spreads[i];
        }
      }
      n = live;
    }
    collector.Append(_staged_docs.data(), _staged_scores.data(),
                     _staged_spreads.data(), n);
  }

  ABSL_CACHELINE_ALIGNED doc_id_t _docs[kScoreBlock];
  ABSL_CACHELINE_ALIGNED score_t _scores[kScoreBlock];
  std::array<doc_id_t, kRun> _staged_docs;
  std::array<score_t, kRun> _staged_scores;
  std::array<score_t, kRun> _staged_spreads;
  std::array<doc_id_t, kRun> _all;
  ColumnArgsFetcher& _fetcher;
  detail::TableFilter* _table;
  Node _node;
  ScoreFunction _score;
  uint32_t _staged = 0;
};

struct Family {
  using Result = Root::ptr;
  using ScoredContext = Context;

  template<typename Slots>
  using TwoPhaseScored = lead::TwoPhaseScored<Slots>;

  static detail::ScoredCtx Scoring(const Context& ctx) noexcept {
    return ScoredOf(ctx);
  }

  template<typename Node, typename... Args>
  static Result Scored(const Context& ctx, Args&&... args) {
    return memory::make_managed<Walk<Node>>(ctx.table, ctx.fetcher,
                                            std::forward<Args>(args)...);
  }
};

Root::ptr MakeEmpty();

Root::ptr MakeRoot(const QueryBuilder& query, const Context& ctx);

}  // namespace irs::ann
