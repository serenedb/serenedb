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

#include <utility>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting/block_index.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_provider.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/phrase_bound.hpp"
#include "iresearch/search/top/pruned_leaf.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Slots, typename Table>
class PrunedPhrase : public Root {
  static_assert(Slots::kHasFreqBound,
                "a phrase with no bound cannot be pruned");

  static constexpr bool kHasScale = requires(const Slots& s) { s.Scale(); };

  struct Provider final : AttributeProvider {
    Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
      if (type == irs::Type<FreqBlockAttr>::id()) {
        return &freq;
      }
      if constexpr (kHasScale) {
        if (type == irs::Type<ScaleBlockAttr>::id()) {
          return &scale;
        }
      }
      return nullptr;
    }

    FreqBlockAttr freq;
    [[no_unique_address]] utils::Need<kHasScale, ScaleBlockAttr> scale;
  };

  static constexpr uint32_t kWindow = 64;
  static constexpr uint32_t kMaxInterval = 64;
  static constexpr uint32_t kPassShare = 2;
  static constexpr uint32_t kLeastFreqCap = 256;

 public:
  template<typename... Args>
  PrunedPhrase(Table table, const SubReader& segment, const TermReader& field,
               const irs::detail::ScoreArgs& args, bool stored, Args&&... slots)
    : _slots{std::forward<Args>(slots)...},
      _bound{segment, field, args, stored},
      _fetcher{*args.fetcher},
      _admit{table},
      _normed{field_limits::valid(field.meta().norm)} {
    _provider.freq.value = _freqs;
    if constexpr (kHasScale) {
      _provider.scale.value = _scales;
    }
    _score = irs::detail::PrepareScore(args, segment, field.meta(), _provider);
    _slots.Gate();
  }

  void Run(doc_id_t min, doc_id_t max, LoserScoreCollector& collector) final {
    Batch batch;
    auto doc = _slots.Seek(min);
    while (doc < max) {
      const auto threshold = collector.ScoreThreshold();
      if (threshold == std::numeric_limits<score_t>::lowest()) {
        doc = MatchFirst(doc, max, collector, batch);
        continue;
      }
      if (doc > _block_last) {
        doc = SkipBlocks(doc, threshold);
        continue;
      }
      const auto end = _block_last < max ? _block_last + 1 : max;
      if (_skip != 0) {
        --_skip;
        doc = MatchFirst(doc, end, collector, batch);
      } else {
        doc = BoundFirst(doc, end, collector, batch);
      }
    }
    FlushBatch(collector, batch);
    _admit.Flush(collector);
  }

 private:
  struct Batch {
    ABSL_CACHELINE_ALIGNED doc_id_t docs[kScoreBlock];
    ABSL_CACHELINE_ALIGNED score_t scores[kScoreBlock];
    uint32_t size = 0;
  };

  score_t RunScore(const BlockIndex& index, uint32_t r) {
    if (_run != r) {
      _run = r;
      _run_score = _bound.Score(_slots, index.RunBound(r));
    }
    return _run_score;
  }

  doc_id_t SkipBlocks(doc_id_t doc, score_t threshold) {
    const auto* const index = _bound.Index(_slots);
    if (!index) {
      _block_last = doc_limits::eof();
      return doc;
    }
    const auto first = _slots.BoundLeaf()->LeafBlock();
    const auto b = FirstCompetitive(
      *index, first, threshold, [&](uint32_t r) { return RunScore(*index, r); },
      [&](uint32_t k) { return _bound.Score(_slots, index->Bound(k)); });
    if (b == first) {
      _block_last = index->Last(b);
      return doc;
    }
    if (b == index->Size()) {
      return doc_limits::eof();
    }
    return _slots.Seek(index->Last(b - 1) + 1);
  }

  doc_id_t MatchFirst(doc_id_t doc, doc_id_t end,
                      LoserScoreCollector& collector, Batch& batch) {
    for (uint32_t seen = 0; doc < end && seen != kWindow;
         doc = _slots.Next(doc), ++seen) {
      if (!_slots.Match(doc)) {
        continue;
      }
      batch.docs[batch.size] = doc;
      _freqs[batch.size] = _slots.Freq();
      if constexpr (kHasScale) {
        _scales[batch.size] = _slots.Scale();
      }
      if (++batch.size == kScoreBlock) {
        FetchAndScoreBlock(_fetcher, _score, batch.docs, batch.scores);
        _admit.AddDocs(collector, batch.docs, kScoreBlock, batch.scores);
        batch.size = 0;
      }
    }
    return doc;
  }

  doc_id_t BoundFirst(doc_id_t doc, doc_id_t end,
                      LoserScoreCollector& collector, Batch& batch) {
    FlushBatch(collector, batch);
    doc = _slots.Gating() ? Bound<true>(doc, end, collector)
                          : Bound<false>(doc, end, collector);
    if (_seen >= kWindow) {
      if (_passed * kPassShare > _seen) {
        _interval = std::min(2 * _interval, kMaxInterval);
        _skip = _interval;
      } else {
        _interval = 1;
      }
      _seen = 0;
      _passed = 0;
    }
    return doc;
  }

  template<bool Gated>
  doc_id_t Bound(doc_id_t doc, doc_id_t end, LoserScoreCollector& collector) {
    auto fetched = doc_limits::invalid();
    const auto admit = [&](doc_id_t candidate, uint32_t bound) {
      if (bound < LeastFreq(collector.ScoreThreshold())) {
        ++_seen;
        return false;
      }
      if (fetched != candidate) {
        fetched = candidate;
        _fetcher.Fetch(candidate);
      }
      if (ScoreOne(bound) > collector.ScoreThreshold()) {
        return true;
      }
      ++_seen;
      return false;
    };
    const auto next = [&] {
      if constexpr (Gated) {
        return _slots.Next(doc, end, admit);
      } else {
        return _slots.Next(doc);
      }
    };
    auto threshold = collector.ScoreThreshold();
    auto least = LeastFreq(threshold);
    for (; doc < end && _seen < kWindow; doc = next()) {
      ++_seen;
      const auto bound = _slots.FreqBound();
      if (bound < least) {
        continue;
      }
      if (!Gated || fetched != doc) {
        _fetcher.Fetch(doc);
      }
      auto score = ScoreOne(bound);
      if (score <= threshold) {
        continue;
      }
      ++_passed;
      if (!_slots.MatchOrdered(doc)) {
        continue;
      }
      const auto freq = _slots.Freq();
      SDB_ASSERT(freq <= bound);
      if constexpr (kHasScale) {
        score = ScoreOne(freq, _slots.Scale());
      } else if (freq != bound) {
        score = ScoreOne(freq);
      }
      _admit.Add(collector, score, doc);
      threshold = collector.ScoreThreshold();
      least = LeastFreq(threshold);
    }
    if constexpr (Gated) {
      return _slots.Seek(doc);
    } else {
      return doc;
    }
  }

  IRS_FORCE_INLINE uint32_t LeastFreq(score_t threshold) {
    if (threshold >= _least_score) [[unlikely]] {
      RaiseLeastFreq(threshold);
    }
    return _least_freq;
  }

  IRS_NO_INLINE void RaiseLeastFreq(score_t threshold) {
    if (_bound.Bounded()) {
      for (; _least_freq != kLeastFreqCap; ++_least_freq) {
        _least_score = _bound.ScoreOf(
          {.freq = _least_freq, .norm = _normed ? _least_freq : 1});
        if (_least_score > threshold) {
          return;
        }
      }
    }
    _least_score = std::numeric_limits<score_t>::max();
  }

  score_t ScoreOne(uint32_t freq, [[maybe_unused]] score_t scale = kNoBoost) {
    _freqs[0] = freq;
    if constexpr (kHasScale) {
      _scales[0] = scale;
    }
    return _score.Score();
  }

  void FlushBatch(LoserScoreCollector& collector, Batch& batch) {
    if (batch.size != 0) {
      FetchAndScore(_fetcher, _score, batch.docs, batch.scores, batch.size);
      _admit.AddDocs(collector, batch.docs, batch.size, batch.scores);
      batch.size = 0;
    }
  }

  ABSL_CACHELINE_ALIGNED uint32_t _freqs[kScoreBlock];
  [[no_unique_address]] utils::Need<kHasScale, score_t[kScoreBlock]> _scales{};
  Slots _slots;
  Provider _provider;
  ScoreFunction _score;
  PhraseBound _bound;
  ColumnArgsFetcher& _fetcher;
  doc_id_t _block_last = doc_limits::invalid();
  uint32_t _run = std::numeric_limits<uint32_t>::max();
  score_t _run_score = 0;
  uint32_t _seen = 0;
  uint32_t _passed = 0;
  uint32_t _interval = 1;
  uint32_t _skip = 0;
  uint32_t _least_freq = 1;
  score_t _least_score = std::numeric_limits<score_t>::lowest();
  [[no_unique_address]] Admit<Table> _admit;
  bool _normed;
};

}  // namespace irs::top
