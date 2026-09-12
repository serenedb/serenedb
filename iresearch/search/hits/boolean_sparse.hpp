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
#include "iresearch/search/detail/table_filter.hpp"
#include "iresearch/search/hits/root.hpp"
#include "iresearch/search/lead/concept.hpp"
#include "iresearch/search/scorers/make_conjunction.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/score_policy.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::hits {

template<lead::Type Lead, typename Probes, typename Optional, typename Excludes,
         typename Table>
class BooleanSparse : public Root {
 public:
  static constexpr bool kProbes = !std::is_same_v<Probes, utils::Empty>;
  static constexpr bool kOptional = !std::is_same_v<Optional, utils::Empty>;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;
  static constexpr bool kTable = !std::is_same_v<Table, utils::Empty>;
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
      _table{table} {
    _score = Compose(score);
  }

  BooleanSparse(BooleanSparse&&) = delete;
  BooleanSparse& operator=(BooleanSparse&&) = delete;

  uint32_t Run(doc_id_t* IRS_RESTRICT out, score_t* IRS_RESTRICT scores,
               uint32_t capacity) final {
    SDB_ASSERT(capacity >= doc_limits::kMinCapacity);
    uint32_t n = 0;
    uint32_t batch = 0;
    auto doc = _lead.Next();
    while (!doc_limits::eof(doc)) {
      if constexpr (kTable) {
        if (const auto live = _table.Live(doc); live != doc) {
          doc = _lead.Seek(live);
          continue;
        }
      }
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
      out[n] = doc;
      _lead.FetchScoreArgs(batch);
      if constexpr (kProbes) {
        _probes.FetchScoreArgs(batch);
      }
      if constexpr (kOptional) {
        _optional.FetchScoreArgs(batch);
      }
      ++n;
      if (++batch == kBatch) {
        ScoreFull(out + n - batch, scores + n - batch);
        batch = 0;
      }
      if (n == capacity) {
        break;
      }
      doc = _lead.Next();
    }
    if (batch != 0) {
      Score(out + n - batch, scores + n - batch, batch);
    }
    return n;
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
  [[no_unique_address]] irs::detail::Narrowing<Table> _table;
};

}  // namespace irs::hits
