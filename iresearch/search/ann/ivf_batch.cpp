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
#include <array>
#include <bit>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <tuple>
#include <vector>

#include "iresearch/formats/ivf/centroids.hpp"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/ann/ivf_scan.hpp"
#include "iresearch/search/detail/lazy_bitset.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/vector_filter_util.hpp"
#include "iresearch/search/detail/vector_slots.hpp"
#include "iresearch/search/lead/posting_docs.hpp"
#include "iresearch/utils/bit_utils.hpp"

namespace irs {
namespace ann {
namespace {

bool Admitted(const uint64_t* admitted, doc_id_t doc) noexcept {
  const auto offset = doc - detail::LazyBitset::kMin;
  return CheckBit(admitted[offset / detail::LazyBitset::kBits],
                  offset % detail::LazyBitset::kBits);
}

}  // namespace

IvfQueryScan::IvfQueryScan(std::unique_ptr<QuantizerReader> reader,
                           score_t boost, float width)
  : _reader{std::move(reader)},
    _scorer{*_reader},
    _boost{boost},
    _width{width} {
  if (_width > 0.f) {
    _scorer.TrackErrors();
  }
}

void IvfQueryScan::Score(const irs::detail::ScoreArgs& args,
                         const SubReader& segment, const FieldProperties& field,
                         irs::detail::TableFilter* table) {
  _fetcher = args.fetcher;
  _score = irs::detail::PrepareBlockScorer(args, segment, field, _provider,
                                           _kept.data());
  _table = table;
}

void IvfQueryScan::StartCluster(const float* centroid,
                                const irs::detail::ClusterLayout& layout) {
  _reader->StartCluster(centroid);
  _scorer.Start(layout);
  if (_table && _started) {
    _table->Rewind();
  }
  _started = true;
}

void IvfQueryScan::Collect(irs::detail::VectorBlockReader& pay,
                           Collector& collector, std::span<const doc_id_t> docs,
                           const uint64_t* admitted, uint32_t base) {
  const auto floor = collector.Floor();
  _scorer.SetLimit(floor);
  _scorer.Score(pay, base, static_cast<uint32_t>(docs.size()), _raw.data(),
                _err.data());
  Keep(collector, docs, admitted, _raw.data(),
       _width > 0.f ? _err.data() : nullptr, floor);
}

void IvfQueryScan::CollectScored(std::span<const byte_type> window,
                                 Collector& collector,
                                 std::span<const doc_id_t> docs,
                                 const uint64_t* admitted,
                                 std::span<score_t> scores, uint32_t offset,
                                 score_t floor) {
  const score_t* errors = nullptr;
  if (_width > 0.f) {
    _window_err.resize(scores.size());
    _reader->BlockErrors(window, scores, _window_err.data());
    errors = _window_err.data() + offset;
  }
  Keep(collector, docs, admitted, scores.data() + offset, errors, floor);
}

void IvfQueryScan::Keep(Collector& collector, std::span<const doc_id_t> docs,
                        const uint64_t* admitted, const score_t* scores,
                        const score_t* errors, score_t floor) {
  const auto n = static_cast<uint32_t>(docs.size());
  uint32_t kept = 0;
  uint32_t i = 0;
#ifdef __AVX2__
  if (!admitted) {
    const auto edge = _mm256_set1_ps(floor);
    const auto width = _mm256_set1_ps(_width);
    for (; i + 8 <= n; i += 8) {
      const auto score = _mm256_loadu_ps(scores + i);
      const auto spread = errors
                            ? _mm256_mul_ps(width, _mm256_loadu_ps(errors + i))
                            : _mm256_setzero_ps();
      const auto mask = static_cast<uint32_t>(_mm256_movemask_ps(
        _mm256_cmp_ps(_mm256_add_ps(score, spread), edge, _CMP_NLT_UQ)));
      if (mask == 0) {
        continue;
      }
      const auto control = LeftPackControl(mask);
      const auto ids =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(docs.data() + i));
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(_docs.data() + kept),
                          _mm256_permutevar8x32_epi32(ids, control));
      _mm256_storeu_ps(_kept.data() + kept,
                       _mm256_permutevar8x32_ps(score, control));
      _mm256_storeu_ps(_spreads.data() + kept,
                       _mm256_permutevar8x32_ps(spread, control));
      kept += static_cast<uint32_t>(std::popcount(mask));
    }
  }
#endif
  for (; i < n; ++i) {
    const auto spread = errors ? _width * errors[i] : 0.f;
    if (scores[i] + spread < floor ||
        (admitted && !Admitted(admitted, docs[i]))) {
      continue;
    }
    _docs[kept] = docs[i];
    _kept[kept] = scores[i];
    _spreads[kept] = spread;
    ++kept;
  }
  if (kept == 0) {
    return;
  }
  if (!_fetcher) {
    for (uint32_t j = 0; j < kept; ++j) {
      _kept[j] *= _boost;
      _spreads[j] *= _boost;
    }
    collector.Append(_docs.data(), _kept.data(), _spreads.data(), kept);
    return;
  }
  _fetcher->Fetch(std::span<const doc_id_t>{_docs.data(), kept});
  _score.Score(_scores.data(), static_cast<scores_size_t>(kept));
  kept = Narrow(kept);
  collector.Append(_docs.data(), _scores.data(), _spreads.data(), kept);
}

uint32_t IvfQueryScan::Narrow(uint32_t n) {
  if (!_table) {
    for (uint32_t i = 0; i < n; ++i) {
      _spreads[i] *= _boost;
    }
    return n;
  }
  std::copy_n(_docs.data(), n, _all.data());
  const auto live = _table->Narrow(_docs.data(), _scores.data(), n);
  for (uint32_t i = 0, j = 0; j < live; ++i) {
    if (_all[i] == _docs[j]) {
      _spreads[j++] = _boost * _spreads[i];
    }
  }
  return live;
}

void ScanIvfClusters(const IndexInput& doc_in, const IndexInput& payload,
                     const PayloadBlockSetting& setting,
                     std::span<const IvfCluster> clusters,
                     std::span<const IvfProbe> probes,
                     std::span<IvfQueryScan> scans,
                     std::span<Collector> collectors,
                     const uint64_t* admitted) {
  irs::detail::VectorBlockReader pay{payload.Dup(), setting};
  const uint32_t gs = setting.group_size;
  std::vector<QuantizerReader*> readers;
  std::vector<score_t> floors;
  std::vector<score_t> scores;
  irs::detail::ResolveInput(doc_in, [&]<typename InputType>() {
    for (const auto& cluster : clusters) {
      const auto group = probes.subspan(cluster.first, cluster.count);
      const irs::detail::ClusterLayout layout{setting, cluster.meta.pos_offset,
                                              cluster.meta.docs_count};
      readers.clear();
      for (const auto& probe : group) {
        scans[probe.query].StartCluster(probe.centroid, layout);
        readers.push_back(&scans[probe.query].Reader());
      }
      if (group.size() > 1) {
        readers.front()->StartBlocks(readers);
      }
      pay.Reset(cluster.meta.pay_start);
      irs::detail::PostingLead<InputType> list{cluster.meta, doc_in,
                                               IndexFeatures::None, false};
      uint32_t base = 0;
      for (auto doc = list.Next(); !doc_limits::eof(doc);
           doc = list.NextLeaf()) {
        const auto docs = list.Leaf();
        const auto len = static_cast<uint32_t>(docs.size());
        if (group.size() > 1) {
          floors.clear();
          for (const auto& probe : group) {
            floors.push_back(collectors[probe.query].Floor());
          }
          const uint32_t lane = layout.lane + base;
          const uint32_t first = lane / gs * gs;
          const uint32_t end =
            std::min(layout.records, (lane + len + gs - 1) / gs * gs);
          const uint32_t window = end - first;
          scores.resize(group.size() * size_t{window});
          const auto block = pay.Read(first, window);
          readers.front()->ComputeBlocks(readers, block, floors, scores.data());
          for (size_t i = 0; i < group.size(); ++i) {
            const auto q = group[i].query;
            scans[q].CollectScored(block, collectors[q], docs, admitted,
                                   {scores.data() + i * window, window},
                                   lane - first, floors[i]);
          }
        } else {
          for (const auto& probe : group) {
            scans[probe.query].Collect(pay, collectors[probe.query], docs,
                                       admitted, base);
          }
        }
        base += len;
      }
    }
  });
}

}  // namespace ann

void IvfIndex::SearchBatch(const SubReader& segment, const PrepareContext& ctx,
                           const VectorFilterOptions& opts,
                           const ann::QueryBatch& batch, uint32_t effort,
                           std::span<ann::Collector> collectors) const {
  const auto d = static_cast<uint32_t>(_tree.Dim());
  if (_tree.Empty() || batch.d != d || !field_limits::valid(opts.ann_id) ||
      batch.Size() == 0) {
    return;
  }
  const auto* postings = segment.field(opts.ann_id);
  auto idx_in = segment.ReopenAnn();
  if (!postings || !idx_in) {
    return;
  }
  auto payload = postings->ReopenPayload();
  const auto* doc_in = detail::DocOf(*postings);
  auto terms = postings->iterator();
  if (!payload || !doc_in || !terms) {
    return;
  }

  const std::span<const float> queries = batch.vectors;

  const auto nprobe =
    effort != 0
      ? effort
      : IvfAutoNprobe(segment.docs_count(), opts.posting_size, opts.top_k,
                      _tree.Levels() == 1 ? _tree.RootSize() : 0);
  const auto beam =
    IvfSearchBeam(nprobe, opts.min_search_fanout, opts.max_search_fanout);

  const auto stats = QuantStats(*idx_in, opts.quant, opts.metric);
  if (!stats) {
    return;
  }
  QueryBuilder::ptr inner;
  if (!PrepareInnerFilter(opts.inner, segment, ctx, inner)) {
    return;
  }
  std::optional<detail::LazyBitset> admitted_set;
  if (inner) {
    admitted_set.emplace(detail::MakeDocSet(
      inner.get(), static_cast<doc_id_t>(segment.docs_count())));
    admitted_set->Reach(admitted_set->End());
  }
  const uint64_t* admitted = admitted_set ? admitted_set->Words() : nullptr;
  std::vector<ann::IvfQueryScan> scans;
  scans.reserve(batch.Size());
  for (uint32_t q = 0; q < batch.Size(); ++q) {
    scans.emplace_back(MakeQuantizerReader(stats->MakeCodebook(
                         queries.subspan(size_t{q} * d, d))),
                       ctx.boost, opts.rescore_width);
  }
  const auto setting = scans.front().Reader().BlockSetting();
  SDB_ASSERT(setting.group_size != 0);
  const bool centroids = QuantizerNeedsCentroid(opts.quant);

  BatchRoutes routes;
  _tree.SearchBatch(queries, *idx_in, nprobe, beam, routes);
  auto& probes = routes.probes;
  absl::c_sort(probes, [](const CentroidProbe& l, const CentroidProbe& r) {
    return std::tie(l.cluster, r.score) < std::tie(r.cluster, l.score);
  });
  struct Group {
    uint32_t first;
    uint32_t count;
    float best;
  };
  std::vector<Group> groups;
  for (uint32_t i = 0; i < probes.size(); ++i) {
    if (groups.empty() ||
        probes[groups.back().first].cluster != probes[i].cluster) {
      groups.push_back({.first = i, .count = 0, .best = probes[i].score});
    }
    ++groups.back().count;
  }
  absl::c_stable_sort(
    groups, [](const Group& l, const Group& r) { return l.best > r.best; });

  std::vector<ann::IvfCluster> clusters;
  std::vector<ann::IvfProbe> refs;
  clusters.reserve(groups.size());
  refs.reserve(probes.size());
  std::array<byte_type, kCentroidTermWidth> term_buf{};
  for (const auto& group : groups) {
    if (!SeekClusterTerm(*terms, probes[group.first].cluster, term_buf)) {
      continue;
    }
    const auto meta = terms->cookie();
    if (meta.docs_count == 0) {
      continue;
    }
    clusters.push_back({.meta = meta,
                        .first = static_cast<uint32_t>(refs.size()),
                        .count = group.count});
    for (uint32_t i = group.first; i < group.first + group.count; ++i) {
      refs.push_back({.query = probes[i].query,
                      .centroid = centroids ? probes[i].centroid : nullptr});
    }
  }
  ann::ScanIvfClusters(*doc_in, *payload, setting, clusters, refs, scans,
                       collectors, admitted);
}

}  // namespace irs
