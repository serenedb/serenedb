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
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/detail/exclude_block.hpp"
#include "iresearch/search/hits/root.hpp"
#include "iresearch/search/lead/concept.hpp"
#include "iresearch/search/scorers/make_conjunction.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/score_policy.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::hits {

template<lead::Type Lead, typename Probes, typename Optional, typename Excludes>
class BooleanSparse : public Root {
 public:
  static constexpr bool kProbes = !std::is_same_v<Probes, utils::Empty>;
  static constexpr bool kOptional = !std::is_same_v<Optional, utils::Empty>;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;
  static constexpr uint32_t kBatch = kScoreBlock;
  static_assert(kProbes || kOptional || kExcludes);

  template<typename LeadArgs, typename ProbesArgs, typename OptionalArgs,
           typename ExcludesArgs>
  BooleanSparse(std::piecewise_construct_t, ColumnArgsFetcher& fetcher,
                irs::detail::Scored score, LeadArgs&& lead, ProbesArgs&& probes,
                OptionalArgs&& optional, ExcludesArgs&& excludes)
    : _fetcher{fetcher},
      _lead{std::make_from_tuple<Lead>(std::forward<LeadArgs>(lead))},
      _probes{std::make_from_tuple<Probes>(std::forward<ProbesArgs>(probes))},
      _optional{
        std::make_from_tuple<Optional>(std::forward<OptionalArgs>(optional))},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))} {
    _score = Compose(score);
  }

  BooleanSparse(BooleanSparse&&) = delete;
  BooleanSparse& operator=(BooleanSparse&&) = delete;

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
      doc = _lead.Seek(min);
    }
    uint32_t batch = 0;
    while (!doc_limits::eof(doc) && batch != kBatch) {
      if constexpr (kProbes) {
        if (const auto probe = _probes.Probe(doc); probe != doc) {
          doc = _lead.Seek(probe);
          continue;
        }
      }
      if constexpr (kOptional) {
        if (const auto probe = _optional.Probe(doc); probe != doc) {
          doc = _lead.Seek(probe);
          continue;
        }
      }
      if constexpr (kExcludes) {
        if (irs::detail::IsExcluded(_excludes, doc)) {
          doc = _lead.Next();
          continue;
        }
      }
      _batch[batch] = doc;
      _lead.FetchScoreArgs(batch);
      if constexpr (kProbes) {
        _probes.FetchScoreArgs(batch);
      }
      if constexpr (kOptional) {
        _optional.FetchScoreArgs(batch);
      }
      ++batch;
      doc = _lead.Next();
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

  ScoreFunction Compose(irs::detail::Scored score) {
    if constexpr (!kProbes && !kOptional) {
      return _lead.PrepareScore();
    } else {
      std::vector<ScoreFunction> scorers;
      irs::detail::AppendScorer(scorers, _lead.PrepareScore());
      if constexpr (kProbes) {
        _probes.CollectScorers(scorers);
      }
      if constexpr (kOptional) {
        if constexpr (requires { _optional.PrepareScore(score.inner); }) {
          irs::detail::AppendScorer(scorers,
                                    _optional.PrepareScore(score.inner));
        } else {
          irs::detail::AppendScorer(scorers, _optional.PrepareScore());
        }
      }
      return irs::detail::MakeConjunctionScore(score.inner, std::move(scorers),
                                               score.absorbed);
    }
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
  [[no_unique_address]] Probes _probes;
  [[no_unique_address]] Optional _optional;
  [[no_unique_address]] Excludes _excludes;
  ScoreFunction _score;
  ABSL_CACHELINE_ALIGNED doc_id_t _batch[kBatch];
  ABSL_CACHELINE_ALIGNED score_t _batch_scores[kBatch];
  uint32_t _batched = 0;
  uint32_t _held = 0;
  doc_id_t _pos = doc_limits::invalid();
  bool _spent = false;
};

}  // namespace irs::hits
