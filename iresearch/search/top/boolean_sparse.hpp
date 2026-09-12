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

#include <absl/base/optimization.h>

#include <span>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/index/iterators.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/detail/exclude_block.hpp"
#include "iresearch/search/lead/concept.hpp"
#include "iresearch/search/scorers/make_conjunction.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/score_policy.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<lead::Type Lead, typename Probes, typename Optional, typename Excludes,
         typename Table>
class BooleanSparse : public Root {
 public:
  static constexpr bool kProbes = !std::is_same_v<Probes, utils::Empty>;
  static constexpr bool kOptional = !std::is_same_v<Optional, utils::Empty>;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;
  static constexpr uint32_t kBatch = kScoreBlock;
  static_assert(kProbes || kOptional || kExcludes);

  template<typename LeadArgs, typename ProbesArgs, typename OptionalArgs,
           typename ExcludesArgs>
  BooleanSparse(Table table, std::piecewise_construct_t,
                ColumnArgsFetcher& fetcher, irs::detail::Scored score,
                LeadArgs&& lead, ProbesArgs&& probes, OptionalArgs&& optional,
                ExcludesArgs&& excludes)
    : _fetcher{fetcher},
      _lead{std::make_from_tuple<Lead>(std::forward<LeadArgs>(lead))},
      _probes{std::make_from_tuple<Probes>(std::forward<ProbesArgs>(probes))},
      _optional{
        std::make_from_tuple<Optional>(std::forward<OptionalArgs>(optional))},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))},
      _admit{table} {
    _score = Compose(score);
  }

  BooleanSparse(BooleanSparse&&) = delete;
  BooleanSparse& operator=(BooleanSparse&&) = delete;

  void Run(LoserScoreCollector& collector) final {
    ABSL_CACHELINE_ALIGNED doc_id_t docs[kBatch];
    ABSL_CACHELINE_ALIGNED score_t scores[kBatch];
    uint32_t batch = 0;
    auto doc = _lead.Next();
    [[clang::code_align(64)]] while (!doc_limits::eof(doc)) {
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
      docs[batch] = doc;
      _lead.FetchScoreArgs(batch);
      if constexpr (kProbes) {
        _probes.FetchScoreArgs(batch);
      }
      if constexpr (kOptional) {
        _optional.FetchScoreArgs(batch);
      }
      if (++batch == kBatch) {
        _fetcher.FetchScoreBlock(
          std::span<const doc_id_t, kScoreBlock>{docs, kScoreBlock});
        _score.ScoreBlock(scores);
        _admit.AddDocs(collector, docs, kBatch, scores);
        batch = 0;
      }
      doc = _lead.Next();
    }
    if (batch != 0) {
      _fetcher.Fetch(std::span<const doc_id_t>{docs, batch});
      _score.Score(scores, static_cast<scores_size_t>(batch));
      _admit.AddDocs(collector, docs, batch, scores);
    }
    _admit.Flush(collector);
  }

 private:
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

  ColumnArgsFetcher& _fetcher;
  Lead _lead;
  [[no_unique_address]] Probes _probes;
  [[no_unique_address]] Optional _optional;
  [[no_unique_address]] Excludes _excludes;
  ScoreFunction _score;
  [[no_unique_address]] Admit<Table> _admit;
};

}  // namespace irs::top
