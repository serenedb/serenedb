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
#include "iresearch/search/top/pruned_clause.hpp"
#include "iresearch/search/top/pruned_leaves.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Lead, typename Others, typename Joined, typename Excludes,
         typename Table>
class PrunedConjunction : public Root {
 public:
  static constexpr uint32_t kChunk = kScoreBlock;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;
  static constexpr bool kJoined = !std::is_same_v<Joined, utils::Empty>;

  template<typename Init, typename JoinedArgs, typename ExcludesArgs>
  PrunedConjunction(Table table, ColumnArgsFetcher& fetcher, bool exhaustive,
                    size_t size, Init&& init, JoinedArgs&& joined,
                    ExcludesArgs&& excludes)
    : _others{fetcher, size - 1,
              [&](auto& leaf, size_t i) { init(leaf, i + 1); }},
      _joined{std::make_from_tuple<Joined>(std::forward<JoinedArgs>(joined))},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))},
      _exhaustive{exhaustive},
      _admit{table} {
    SDB_ASSERT(size > 1);
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
  }

  PrunedConjunction(PrunedConjunction&&) = delete;
  PrunedConjunction& operator=(PrunedConjunction&&) = delete;

  void Run(doc_id_t min, doc_id_t max, LoserScoreCollector& collector) final {
    if constexpr (kDocFirst) {
      if (_exhaustive) {
        Walk(_lead.Seek(min), max - 1, std::numeric_limits<score_t>::max(),
             collector);
        FlushBatch(collector);
        _admit.Flush(collector);
        return;
      }
    }
    for (auto doc = _lead.Seek(min); doc < max;) {
      const auto threshold = collector.ScoreThreshold();
      _others.AdvanceTo(doc);
      auto last = _lead.BlockLast();
      if (last >= max) [[unlikely]] {
        last = max - 1;
      }
      const auto lead_max = _lead.MaxScore(last);
      const auto others_max = _others.OpenWindow(doc, last);

      if (lead_max + others_max <= threshold) {
        _span = 1;
        doc = SkipLead(last + 1, max, threshold - _others.RootMax());
        continue;
      }

      if constexpr (kDocFirst) {
        doc = _lead.Seek(doc);
        if (threshold <= others_max) {
          last = std::max(last, std::min(_lead.SpanLast(_span), max - 1));
          _span = std::min(_span * 2, kMaxSpan);
          doc = Walk(doc, last, others_max, collector);
          continue;
        }
        doc = ScoreOrWalk(doc, last, max, threshold, lead_max, others_max,
                          collector);
        continue;
      }
      ScoreFirst(last, others_max, collector);
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
  static constexpr bool kDocFirst = requires(Lead& lead) {
    lead.Freq();
  } && !std::is_same_v<typename Others::LeafType, ErasedClause>;

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
    return doc_reads > sf_reads && (doc_reads - sf_reads) * kDocsPerSavedRead >=
                                     bucket.sf_seen * bucket.doc_range;
  }

  doc_id_t ScoreOrWalk(doc_id_t doc, doc_id_t last, doc_id_t max,
                       score_t threshold, score_t lead_max, score_t others_max,
                       LoserScoreCollector& collector) {
    const auto ratio = (threshold - others_max) / lead_max;
    auto& bucket = _buckets[std::min<uint32_t>(
      kBuckets - 1, static_cast<uint32_t>(ratio * kBuckets))];
    _others.TakeReads();
    if (!ScoreFirstPays(bucket) && ++bucket.ticks < bucket.interval) {
      if constexpr (kJoined) {
        last = std::max(last, std::min(_lead.SpanLast(_span), max - 1));
        _span = std::min(_span * 2, kMaxSpan);
      } else {
        _span = 1;
      }
      const uint64_t range = last - doc + 1;
      doc = Walk(doc, last, std::numeric_limits<score_t>::max(), collector);
      bucket.doc_reads = bucket.doc_reads / 2 + _others.TakeReads();
      bucket.doc_range = bucket.doc_range / 2 + range;
      return doc;
    }
    _span = 1;
    const uint64_t range = last - doc + 1;
    FlushBatch(collector);
    const auto [seen, dropped] = ScoreFirst(last, others_max, collector);
    bucket.sf_seen = bucket.sf_seen / 2 + seen;
    bucket.sf_dropped = bucket.sf_dropped / 2 + dropped + _others.TakeDropped();
    bucket.sf_reads = bucket.sf_reads / 2 + _others.TakeReads();
    bucket.sf_range = bucket.sf_range / 2 + range;
    if (bucket.ticks != 0) {
      bucket.ticks = 0;
      bucket.interval = ScoreFirstPays(bucket)
                          ? kScoreFirstSample
                          : std::min(bucket.interval * 2, kMaxInterval);
    }
    return _lead.Value();
  }

  doc_id_t SkipLead(doc_id_t target, doc_id_t max, score_t threshold) {
    if constexpr (requires { _lead.SeekCompetitive(target, max, threshold); }) {
      return _lead.SeekCompetitive(target, max, threshold);
    } else {
      return _lead.Seek(target);
    }
  }

  std::pair<uint32_t, uint32_t> ScoreFirst(doc_id_t last, score_t others_max,
                                           LoserScoreCollector& collector) {
    auto threshold = collector.ScoreThreshold();
    uint32_t seen = 0;
    uint32_t dropped = 0;
    ForEachLeadBlock(
      last + 1, threshold - others_max,
      [&](doc_id_t* docs, uint32_t len, score_t* scores, uint32_t block) {
        dropped += block - len;
        seen += block - len;
        if (const auto required = threshold - others_max; required > 0) {
          const auto kept =
            irs::detail::FilterScores(docs, scores, len, required);
          dropped += len - kept;
          seen += len - kept;
          len = kept;
        }
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
    return {seen, dropped};
  }

  template<typename Visitor>
  void ForEachLeadBlock(doc_id_t max, score_t required, Visitor&& visit) {
    if constexpr (requires {
                    _lead.ForEachCompetitiveBlock(max, required, visit);
                  }) {
      _lead.ForEachCompetitiveBlock(max, required, visit);
    } else {
      _lead.ForEachScoredBlock(
        max, [&](doc_id_t* docs, uint32_t len, score_t* scores)
               IRS_FORCE_INLINE { visit(docs, len, scores, len); });
    }
  }

  doc_id_t Walk(doc_id_t doc, doc_id_t last, score_t stop,
                LoserScoreCollector& collector) {
    if constexpr (kJoined) {
      return WindowFirst(doc, last, stop, collector);
    } else {
      return DocFirst(doc, last, stop, collector);
    }
  }

  doc_id_t WindowFirst(doc_id_t doc, doc_id_t last, score_t stop,
                       LoserScoreCollector& collector) {
    while (doc <= last) {
      const auto n = WindowMembers(doc, last);
      for (uint32_t i = 0; i != n; ++i) {
        const auto member = _window_docs[i];
        if (_others.Probe(member) == member) {
          Batch(member, _window_freqs[i], collector);
        }
      }
      doc = _lead.Value();
      if (collector.ScoreThreshold() > stop) {
        return doc;
      }
    }
    return doc;
  }

  uint32_t WindowMembers(doc_id_t min, doc_id_t last) {
    const auto max = static_cast<doc_id_t>(std::min<uint64_t>(
      uint64_t{last} + 1, uint64_t{min} + irs::detail::kWindowDocs));
    uint32_t n = 0;
    _lead.ForEachDocs(max, [&](const doc_id_t* docs, const uint32_t* freqs,
                               uint32_t len) IRS_FORCE_INLINE {
      std::copy_n(docs, len, _window_docs + n);
      std::copy_n(freqs, len, _window_freqs + n);
      n += len;
    });
    const auto keep = [&](auto&& admits) IRS_FORCE_INLINE {
      uint32_t members = 0;
      for (uint32_t i = 0; i != n; ++i) {
        const auto member = _window_docs[i];
        _window_docs[members] = member;
        _window_freqs[members] = _window_freqs[i];
        members += static_cast<uint32_t>(admits(member));
      }
      return members;
    };
    const auto included = [&](doc_id_t member) IRS_FORCE_INLINE {
      if constexpr (kExcludes) {
        return !irs::detail::IsExcluded(_excludes, member);
      } else {
        return true;
      }
    };
    if (uint64_t{n} * irs::detail::kSparseInverse < max - min) {
      return kExcludes ? keep(included) : n;
    }
    irs::detail::PutDocs(min, _window_docs, n, _mask);
    _joined.Restrict(min, max, _mask);
    const auto members = keep([&](doc_id_t member) IRS_FORCE_INLINE {
      const auto offset = member - min;
      return CheckBit(_mask[offset / irs::detail::kWindowBits],
                      offset % irs::detail::kWindowBits) &&
             included(member);
    });
    irs::detail::Clear(_mask, irs::detail::WindowWords(min, max));
    return members;
  }

  IRS_FORCE_INLINE void Batch(doc_id_t doc, uint32_t freq,
                              LoserScoreCollector& collector) {
    _batch.docs[_batch.size] = doc;
    _batch.freqs[_batch.size] = freq;
    _others.FetchScoreArgs(_batch.size);
    if (++_batch.size == kChunk) {
      FlushBatch(collector);
    }
  }

  doc_id_t DocFirst(doc_id_t doc, doc_id_t last, score_t stop,
                    LoserScoreCollector& collector) {
    while (doc <= last) {
      if constexpr (kExcludes) {
        if (irs::detail::IsExcluded(_excludes, doc)) {
          doc = _lead.Next();
          continue;
        }
      }
      if (const auto probe = _others.Probe(doc); probe != doc) {
        doc = _lead.Seek(probe);
        continue;
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
    _batch.Flush(
      _lead, _others,
      [&](score_t* scores, uint32_t n)
        IRS_FORCE_INLINE { _others.Score(scores, n); },
      _admit, collector);
  }

  ABSL_CACHELINE_ALIGNED
  utils::Need<kJoined, uint64_t[irs::detail::kWindowWords]> _mask{};
  ABSL_CACHELINE_ALIGNED
  utils::Need<kJoined, doc_id_t[irs::detail::kWindowDocs]> _window_docs;
  ABSL_CACHELINE_ALIGNED
  utils::Need<kJoined, uint32_t[irs::detail::kWindowDocs]> _window_freqs;
  Lead _lead;
  Others _others;
  [[no_unique_address]] Joined _joined;
  [[no_unique_address]] Excludes _excludes;
  [[no_unique_address]] utils::Need<kDocFirst, ScoreBatch> _batch;
  [[no_unique_address]] utils::Need<kDocFirst, std::array<Bucket, kBuckets>>
    _buckets;
  uint32_t _span = 1;
  bool _exhaustive = false;
  [[no_unique_address]] Admit<Table> _admit;
};

}  // namespace irs::top
