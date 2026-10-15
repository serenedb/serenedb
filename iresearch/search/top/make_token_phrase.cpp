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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/detail/token_phrase_of.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/two_phase_scored.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"
#include "iresearch/search/scorers/score_provider.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/make.hpp"
#include "iresearch/search/top/posting_pruned_lead.hpp"
#include "iresearch/search/top/walk.hpp"

namespace irs::top {
namespace {

template<typename Input, typename Table>
class PrunedTokenPhrase : public Root {
  static constexpr uint32_t kBatch = 64;

 public:
  PrunedTokenPhrase(Table table, const TokenPhraseQuery& query,
                    lead::Node::ptr&& approx,
                    const irs::detail::ScoreArgs& args)
    : _approx{std::move(approx)},
      _rows{query, true},
      _fetcher{*args.fetcher},
      _admit{table} {
    const auto& field = query.Reader();
    _anchor.Prepare(*query.Anchor(), *irs::detail::DocOf(field),
                    irs::detail::LayoutOf(field), query.Segment(), field, args);
    _provider.freq.value = &_freq;
    _score = args.scorer->PrepareScorer({
      .segment = query.Segment(),
      .field = field.meta(),
      .doc_attrs = _provider,
      .fetcher = _fetcher,
      .stats = args.stats,
      .boost = args.boost,
    });
  }

  void Run(doc_id_t min, doc_id_t max, LoserScoreCollector& collector) final {
    for (auto doc = _approx.Seek(min); doc < max;) {
      _anchor.Seek(doc);
      const auto last = std::min(_anchor.BlockLast(), max - 1);
      if (_anchor.MaxScore(last) <= collector.ScoreThreshold()) {
        doc = _approx.Seek(last + 1);
        continue;
      }
      for (; doc <= last; doc = _approx.Next()) {
        _anchor.Seek(doc);
        const auto freq = _anchor.Freq();
        const auto bound = Score(doc, freq);
        if (bound <= collector.ScoreThreshold()) {
          continue;
        }
        _docs[_size] = doc;
        _freqs[_size] = freq;
        _bounds[_size] = bound;
        if (++_size == kBatch) {
          Flush(collector);
        }
      }
    }
    Flush(collector);
    _admit.Flush(collector);
  }

 private:
  score_t Score(doc_id_t doc, uint32_t freq) {
    _freq = freq;
    _fetcher.Fetch(doc);
    return _score.Score();
  }

  void Flush(LoserScoreCollector& collector) {
    const auto rows = _rows.Gather({_docs, _size});
    for (size_t i = 0; i != rows; ++i) {
      if (_bounds[i] <= collector.ScoreThreshold()) {
        continue;
      }
      const auto verdict = _rows.Check(i, _freqs[i]);
      if (verdict.freq == 0) {
        continue;
      }
      auto score = _bounds[i];
      if (verdict.freq != _freqs[i]) {
        SDB_ASSERT(verdict.freq < _freqs[i]);
        score = Score(_docs[i], verdict.freq);
      }
      _admit.Add(collector, score, _docs[i]);
    }
    _size = 0;
  }

  doc_id_t _docs[kBatch];
  uint32_t _freqs[kBatch];
  score_t _bounds[kBatch];
  lead::Erased _approx;
  PostingPrunedLead<Input> _anchor;
  irs::detail::PhraseRows _rows;
  irs::detail::LeafProvider _provider;
  ScoreFunction _score;
  ColumnArgsFetcher& _fetcher;
  uint32_t _size = 0;
  uint32_t _freq = 0;
  [[no_unique_address]] Admit<Table> _admit;
};

bool FreqBoundedByAnchor(const TokenPhraseQuery& query) {
  const auto& phrase = query.Compiled();
  return phrase.slop.max == 0 &&
         absl::c_all_of(phrase.slots,
                        [](const CompiledPhrase::Slot& slot) {
                          return slot.offs_min == slot.offs_max;
                        }) &&
         !query.ExplicitPos();
}

Root::ptr MakePruned(const TokenPhraseQuery& query, const Context& ctx,
                     const irs::detail::ScoreArgs& args) {
  const auto* anchor = query.Anchor();
  const auto& field = query.Reader();
  if (!ctx.prune || !anchor || anchor->docs_count <= doc_limits::kBlockSize ||
      !irs::detail::BoundsOf(field) || !HasScoreBounds(args.scorer) ||
      !FreqBoundedByAnchor(query)) {
    return {};
  }
  auto approx = query.Approx().PlanLead({});
  if (!approx) {
    return {};
  }
  return irs::detail::ResolveInput(*irs::detail::DocOf(field),
                                   [&]<typename Input> -> Root::ptr {
                                     return MakeShape<PrunedTokenPhrase, Input>(
                                       ctx, query, std::move(approx), args);
                                   });
}

}  // namespace

Root::ptr Make(const TokenPhraseQuery& query, const Context& ctx) {
  const auto record = query.Stats(ScoredOf(ctx));
  const irs::detail::ScoreArgs args{.scorer = record.scorer,
                                    .stats = record.stats,
                                    .fetcher = &ctx.fetcher,
                                    .boost = query.Boost()};
  if (const auto value =
        irs::detail::ConstantOf(query.Segment(), query.Reader(), args)) {
    if (ctx.table) {
      return irs::detail::MakeTokenPhrase<FilteredConstantWalk, Root::ptr>(
        query, 0, ctx.table, *value);
    }
    return irs::detail::MakeTokenPhrase<PlainConstantWalk, Root::ptr>(
      query, 0, utils::Empty{}, *value);
  }
  if (auto pruned = MakePruned(query, ctx, args)) {
    return pruned;
  }
  if (ctx.table) {
    return irs::detail::MakeTokenPhrase<FilteredWalk, Root::ptr, true,
                                        lead::TwoPhaseScored>(
      query, 0, ctx.table, ctx.fetcher, query.Segment(), query.Reader(), args);
  }
  return irs::detail::MakeTokenPhrase<PlainWalk, Root::ptr, true,
                                      lead::TwoPhaseScored>(
    query, 0, utils::Empty{}, ctx.fetcher, query.Segment(), query.Reader(),
    args);
}

}  // namespace irs::top
