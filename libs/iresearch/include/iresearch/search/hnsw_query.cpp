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

#include "iresearch/search/hnsw_query.hpp"

#include <algorithm>
#include <optional>
#include <span>

#include "basics/misc.hpp"
#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/common/vector_of.hpp"
#include "pg/sql_exception_macro.h"

namespace irs {
namespace {

template<VectorMetric M>
struct HnswQueryDist {
  const float* base;
  uint32_t d;
  const float* q;

  const float* Row(uint32_t id) const noexcept {
    return base + static_cast<size_t>(id) * d;
  }

  void Batch(std::span<const uint32_t> ids, score_t* out,
             score_t = 0.f) const noexcept {
    HnswComputeDistances<M>(q, base, d, ids, out);
  }

  score_t One(uint32_t id) const noexcept {
    score_t s{};
    Batch({&id, 1}, &s);
    return s;
  }

  void Prefetch(uint32_t id) const noexcept {
    __builtin_prefetch(Row(id), 0, 3);
  }
};

template<VectorMetric M>
struct HnswCodeDist {
  const byte_type* codes;
  uint32_t record_size;
  QuantizerReader* qr;
  search::RawVectorReader* raw;

  const byte_type* Row(uint32_t id) const noexcept {
    return codes + static_cast<size_t>(id) * record_size;
  }

  score_t One(uint32_t id) {
    score_t out = .0f;
    Batch({&id, 1}, &out);
    return out;
  }

  void Batch(std::span<const uint32_t> ids, score_t* out,
             score_t threshold = kHnswNoThreshold) {
    qr->ComputeGathered(codes, record_size, ids, threshold, out);
  }

  void Prefetch(uint32_t id) const noexcept {
    __builtin_prefetch(Row(id), 0, 3);
  }

  score_t Exact(uint32_t id) {
    return raw->ComputeOne<M>(static_cast<doc_id_t>(id) + doc_limits::min());
  }
};

template<typename Fn>
void WithHnswDist(const HnswData& data, std::span<const float> query,
                  const std::shared_ptr<const QuantizerCodebook>& codebook,
                  VectorMetric metric, uint32_t d, uint32_t record_size,
                  search::RawVectorReader* raw, Fn&& fn) {
  if (codebook) {
    auto reader = MakeQuantizerReader(codebook);
    reader->StartCluster(data.centroid.empty() ? nullptr
                                               : data.centroid.data());
    ResolveEnum<VectorMetric>(metric, [&]<VectorMetric M>() {
      HnswCodeDist<M> dist{.codes = data.codes.data(),
                           .record_size = record_size,
                           .qr = reader.get(),
                           .raw = raw};
      ResolveBool(raw != nullptr,
                  [&]<bool Exact>() { fn.template operator()<Exact>(dist); });
    });
    return;
  }
  ResolveEnum<VectorMetric>(
    EffectiveQuantMetric(metric), [&]<VectorMetric M>() {
      HnswQueryDist<M> dist{
        .base = data.vectors.data(), .d = d, .q = query.data()};
      fn.template operator()<false>(dist);
    });
}

HnswSearchScratch& ThreadScratch() {
  static thread_local HnswSearchScratch scratch;
  return scratch;
}

std::vector<ScoreDoc> CollectHits(std::span<const HnswCandidate> found,
                                  const DocumentMask* mask) {
  std::vector<ScoreDoc> hits;
  hits.reserve(found.size());
  for (const auto& c : found) {
    const auto doc = static_cast<doc_id_t>(c.node) + doc_limits::min();
    if (mask != nullptr && mask->contains(doc)) {
      continue;
    }
    hits.push_back({.score = c.score, .doc = doc});
  }
  std::ranges::sort(
    hits, [](const ScoreDoc& l, const ScoreDoc& r) { return l.doc < r.doc; });
  return hits;
}

}  // namespace

void HnswRefuseFilter(const search::TableFilter* table) {
  if (table == nullptr) [[likely]] {
    return;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
    ERR_MSG("an hnsw vector index does not support filtered search: the graph "
            "walk cannot honour a predicate, so the filter would be silently "
            "dropped. Use an ivf vector index instead"));
}

std::vector<ScoreDoc> HnswQuery::RunSearch() const {
  auto& scratch = ThreadScratch();
  std::optional<search::RawVectorReader> raw;
  if (_exact_column != nullptr) {
    raw.emplace(*_exact_column, *Segment().GetColReader(), _d);
    raw->SetQuery(_query, _metric);
  }
  WithHnswDist(*_data, _query, _codebook, _metric, _d, _record_size,
               raw ? &*raw : nullptr, [&]<bool Exact>(auto& dist) {
                 if (_ef != 0) {
                   HnswSearchTopK<Exact>(_data->graph, dist, _ef, scratch);
                   return;
                 }
                 ResolveBool(_inclusive, [&]<bool Inclusive>() {
                   HnswSearchRadius<Inclusive>(_data->graph, dist, _threshold,
                                               _max_results, scratch);
                 });
               });
  return CollectHits(scratch.nearest, _segment.docs_mask());
}

}  // namespace irs
