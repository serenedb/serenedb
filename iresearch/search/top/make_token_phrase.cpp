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
#include "iresearch/search/detail/score_filter.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/detail/token_phrase_of.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/two_phase.hpp"
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
    _score =
      irs::detail::PrepareScore(args, query.Segment(), field.meta(), _provider);
  }

  void Run(doc_id_t min, doc_id_t max, LoserScoreCollector& collector) final {
    Batch batch;
    uint32_t size = 0;
    uint32_t end = 0;
    for (auto doc = _approx.Seek(min); doc < max;) {
      _anchor.Seek(doc);
      const auto last = std::min(_anchor.BlockLast(), max - 1);
      if (_anchor.MaxScore(last) <= collector.ScoreThreshold()) {
        doc = _approx.Seek(last + 1);
        continue;
      }
      const auto least = _anchor.LeastFreq(collector.ScoreThreshold());
      for (; doc <= last; doc = _approx.Next()) {
        _anchor.Seek(doc);
        const auto freq = _anchor.Freq();
        if (freq < least) {
          continue;
        }
        batch.docs[end] = doc;
        batch.freqs[end] = freq;
        if (++end - size == kScoreBlock) {
          _provider.freq.value = batch.freqs + size;
          FetchAndScoreBlock(_fetcher, _score, batch.docs + size,
                             batch.bounds + size);
          size += irs::detail::FilterScores(
            batch.docs + size, batch.freqs + size, batch.bounds + size,
            kScoreBlock, collector.ScoreThreshold());
          if (size >= kBatch) {
            Flush(batch, std::exchange(size, 0), collector);
          }
          end = size;
        }
      }
    }
    if (end != size) {
      _provider.freq.value = batch.freqs + size;
      FetchAndScore(_fetcher, _score, batch.docs + size, batch.bounds + size,
                    end - size);
      size += irs::detail::FilterScores(batch.docs + size, batch.freqs + size,
                                        batch.bounds + size, end - size,
                                        collector.ScoreThreshold());
    }
    Flush(batch, size, collector);
    _admit.Flush(collector);
  }

 private:
  struct Batch {
    doc_id_t docs[kBatch + kScoreBlock];
    uint32_t freqs[kBatch + kScoreBlock];
    score_t bounds[kBatch + kScoreBlock];
  };

  void Flush(const Batch& batch, uint32_t size,
             LoserScoreCollector& collector) {
    const auto rows = _rows.Gather({batch.docs, size});
    for (size_t i = 0; i != rows; ++i) {
      if (batch.bounds[i] <= collector.ScoreThreshold()) {
        continue;
      }
      auto verdict = _rows.Check(i, batch.freqs[i]);
      if (verdict.freq == 0) {
        continue;
      }
      auto score = batch.bounds[i];
      if (verdict.freq != batch.freqs[i]) {
        SDB_ASSERT(verdict.freq < batch.freqs[i]);
        _provider.freq.value = &verdict.freq;
        _fetcher.Fetch(batch.docs[i]);
        score = _score.Score();
      }
      _admit.Add(collector, score, batch.docs[i]);
    }
  }

  lead::Erased _approx;
  PostingPrunedLead<Input> _anchor;
  irs::detail::PhraseRows _rows;
  irs::detail::LeafProvider _provider;
  ScoreFunction _score;
  ColumnArgsFetcher& _fetcher;
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

}  // namespace

Root::ptr MakeTokenPhrasePruned(const TokenPhraseQuery& query,
                                const Context& ctx) {
  const auto* anchor = query.Anchor();
  const auto& field = query.Reader();
  if (!anchor || anchor->docs_count <= doc_limits::kBlockSize ||
      !irs::detail::BoundsOf(field) || !FreqBoundedByAnchor(query)) {
    return {};
  }
  const auto record = query.Stats(ScoredOf(ctx));
  if (!record.stats || !HasScoreBounds(record.scorer)) {
    return {};
  }
  auto approx = query.Approx().PlanLead({});
  if (!approx) {
    return {};
  }
  const irs::detail::ScoreArgs args{.scorer = record.scorer,
                                    .stats = record.stats,
                                    .fetcher = &ctx.fetcher,
                                    .boost = query.Boost()};
  return irs::detail::ResolveInput(*irs::detail::DocOf(field),
                                   [&]<typename Input> -> Root::ptr {
                                     return MakeShape<PrunedTokenPhrase, Input>(
                                       ctx, query, std::move(approx), args);
                                   });
}

}  // namespace irs::top
