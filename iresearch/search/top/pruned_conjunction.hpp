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

#include <algorithm>
#include <cstddef>
#include <tuple>
#include <type_traits>
#include <utility>

#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/detail/exclude_block.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/prune_leaves.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Lead, typename Others, typename Excludes, typename Table>
class PrunedConjunction : public Root {
 public:
  static constexpr uint32_t kChunk = kScoreBlock;
  static constexpr size_t kNarrowWindowClauses = 4;
  static constexpr uint32_t kNarrowFragment = 32;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;

  template<typename Init, typename ExcludesArgs>
  PrunedConjunction(Table table, ColumnArgsFetcher& fetcher, size_t size,
                    Init&& init, ExcludesArgs&& excludes)
    : _others{fetcher, size - 1,
              [&](auto& leaf, size_t i) { init(leaf, i + 1); }},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))},
      _admit{table} {
    SDB_ASSERT(size > 1);
    _narrow = size >= kNarrowWindowClauses;
    init(_lead, 0);
  }

  template<typename LeadArgs, typename OthersArgs, typename ExcludesArgs>
  PrunedConjunction(Table table, ColumnArgsFetcher& fetcher, size_t size,
                    std::piecewise_construct_t, LeadArgs&& lead,
                    OthersArgs&& others, ExcludesArgs&& excludes)
    : _lead{std::make_from_tuple<Lead>(std::forward<LeadArgs>(lead))},
      _others{fetcher, size - 1, std::piecewise_construct,
              std::forward<OthersArgs>(others)},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))},
      _admit{table} {
    SDB_ASSERT(size > 1);
    _narrow = size >= kNarrowWindowClauses;
  }

  PrunedConjunction(PrunedConjunction&&) = delete;
  PrunedConjunction& operator=(PrunedConjunction&&) = delete;

  void Run(doc_id_t min, doc_id_t max, LoserScoreCollector& collector) final {
    for (auto doc = _lead.Seek(min); doc < max;) {
      const auto threshold = collector.ScoreThreshold();
      const auto others_end = _others.AdvanceTo(doc);
      const auto lead_last = _lead.BlockLast();
      auto last = lead_last;
      if (_narrow && doc < others_end && others_end < lead_last &&
          static_cast<uint64_t>(others_end - doc) * kNarrowFragment >=
            static_cast<uint64_t>(lead_last - doc)) {
        last = others_end;
      }
      if (last >= max) [[unlikely]] {
        last = max - 1;
      }
      const auto lead_max = _lead.MaxScore(last);
      const auto others_max = _others.OpenWindow(doc, last);

      if (lead_max + others_max <= threshold) {
        _span = 1;
        doc = _lead.Seek(last + 1);
        continue;
      }

      if constexpr (kDocFirst) {
        if (threshold <= others_max) {
          last = std::max(last, std::min(_lead.SpanLast(_span), max - 1));
          _span = std::min(_span * 2, kMaxSpan);
          doc = DocFirst(doc, last, others_max, collector);
          continue;
        }
        _span = 1;
        const auto ratio = (threshold - others_max) / lead_max;
        auto& bucket = _buckets[std::min<uint32_t>(
          kBuckets - 1, static_cast<uint32_t>(ratio * kBuckets))];
        const uint64_t range = last - doc + 1;
        _others.TakeReads();
        if (!ScoreFirstPays(bucket) && ++bucket.ticks < bucket.interval) {
          doc = DocFirst(doc, last, std::numeric_limits<score_t>::max(),
                         collector);
          bucket.doc_reads = bucket.doc_reads / 2 + _others.TakeReads();
          bucket.doc_range = bucket.doc_range / 2 + range;
          continue;
        }
        FlushBatch(collector);
        const auto seen = ScoreFirst(last, collector);
        bucket.sf_seen = bucket.sf_seen / 2 + seen;
        bucket.sf_dropped = bucket.sf_dropped / 2 + _others.TakeDropped();
        bucket.sf_reads = bucket.sf_reads / 2 + _others.TakeReads();
        bucket.sf_range = bucket.sf_range / 2 + range;
        if (bucket.ticks != 0) {
          bucket.ticks = 0;
          bucket.interval = ScoreFirstPays(bucket)
                              ? kScoreFirstSample
                              : std::min(bucket.interval * 2, kMaxInterval);
        }
        doc = _lead.Value();
        continue;
      }
      ScoreFirst(last, collector);
      doc = _lead.Value();
    }
    if constexpr (kDocFirst) {
      FlushBatch(collector);
    }
    _admit.Flush(collector);
  }

 private:
  static constexpr uint32_t kMaxSpan = 16;
  static constexpr uint32_t kBuckets = 8;
  static constexpr uint32_t kScoreFirstDrop = 3;
  static constexpr uint32_t kScoreFirstSample = 16;
  static constexpr uint32_t kMaxInterval = 256;
  static constexpr uint32_t kScoreFirstEvidence = 128;
  static constexpr uint64_t kDocsPerSavedRead = 32;
  static constexpr bool kDocFirst = requires(Lead& lead) { lead.Freq(); };

  struct Bucket {
    uint64_t sf_range = 0;
    uint64_t doc_range = 0;
    uint32_t sf_seen = 0;
    uint32_t sf_dropped = 0;
    uint32_t sf_reads = 0;
    uint32_t doc_reads = 0;
    uint32_t ticks = 0;
    uint32_t interval = kScoreFirstSample;
  };

  static bool ScoreFirstPays(const Bucket& bucket) noexcept {
    if (bucket.sf_seen < kScoreFirstEvidence ||
        bucket.sf_dropped * kScoreFirstDrop >= bucket.sf_seen) {
      return true;
    }
    if (bucket.doc_range == 0) {
      return false;
    }
    const auto doc_reads = bucket.doc_reads * bucket.sf_range;
    const auto sf_reads = bucket.sf_reads * bucket.doc_range;
    return doc_reads > sf_reads &&
           (doc_reads - sf_reads) * kDocsPerSavedRead >=
             bucket.sf_seen * bucket.doc_range;
  }

  uint32_t ScoreFirst(doc_id_t last, LoserScoreCollector& collector) {
    auto threshold = collector.ScoreThreshold();
    uint32_t seen = 0;
    _lead.ForEachScoredBlock(
      last + 1, [&](doc_id_t* docs, uint32_t len, score_t* scores) {
        if constexpr (kExcludes) {
          len = irs::detail::ExcludeBlock(_excludes, docs, scores, len);
        }
        seen += len;
        for (uint32_t off = 0; off < len; off += kChunk) {
          const auto n = std::min<uint32_t>(kChunk, len - off);
          const auto kept =
            _others.Apply(docs + off, scores + off, n, threshold);
          if (kept != 0) {
            _admit.AddDocs(collector, docs + off, kept, scores + off);
            threshold = collector.ScoreThreshold();
          }
        }
      });
    return seen;
  }

  doc_id_t DocFirst(doc_id_t doc, doc_id_t last, score_t stop,
                    LoserScoreCollector& collector) {
    [[clang::code_align(64)]] while (doc <= last) {
      if (const auto probe = _others.Probe(doc); probe != doc) {
        doc = _lead.Seek(probe);
        continue;
      }
      if constexpr (kExcludes) {
        if (irs::detail::IsExcluded(_excludes, doc)) {
          doc = _lead.Next();
          continue;
        }
      }
      _batch.docs[_batch.size] = doc;
      _batch.freqs[_batch.size] = _lead.Freq();
      _others.FetchScoreArgs(_batch.size);
      doc = _lead.Next();
      if (++_batch.size == kChunk) {
        FlushBatch(collector);
        if (collector.ScoreThreshold() > stop) {
          return doc;
        }
      }
    }
    return doc;
  }

  void FlushBatch(LoserScoreCollector& collector) {
    const auto size = _batch.size;
    if (size == 0) {
      return;
    }
    _batch.size = 0;
    _others.Fetch(_batch.docs, size);
    _lead.ScoreFreqs(_batch.freqs, _batch.scores, size);
    _others.Score(_batch.scores, size);
    _admit.AddDocs(collector, _batch.docs, size, _batch.scores);
  }

  struct Batch {
    ABSL_CACHELINE_ALIGNED doc_id_t docs[kChunk];
    ABSL_CACHELINE_ALIGNED uint32_t freqs[kChunk];
    ABSL_CACHELINE_ALIGNED score_t scores[kChunk];
    uint32_t size = 0;
  };

  Lead _lead;
  Others _others;
  [[no_unique_address]] Excludes _excludes;
  [[no_unique_address]] utils::Need<kDocFirst, Batch> _batch;
  [[no_unique_address]] utils::Need<kDocFirst, std::array<Bucket, kBuckets>>
    _buckets;
  bool _narrow = false;
  uint32_t _span = 1;
  [[no_unique_address]] Admit<Table> _admit;
};

}  // namespace irs::top
