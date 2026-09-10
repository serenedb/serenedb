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

#include "iresearch/formats/ivf/ivf_reader.hpp"

#include <cstring>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <limits>
#include <span>
#include <utility>
#include <vector>

#include "basics/assert.h"
#include "basics/memory.hpp"
#include "basics/misc.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/search/vector_filter_util.hpp"
#include "iresearch/search/vector_similarity_query.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

VectorDistanceFn ResolveScoringDistance(VectorMetric metric) {
  VectorDistanceFn fn = nullptr;
  ResolveEnum<VectorMetric>(
    metric, [&]<VectorMetric Metric>() { fn = &ComputeDistance<Metric>; });
  return fn;
}

bool VectorMetricIsAngular(VectorMetric metric) noexcept {
  return metric == VectorMetric::InnerProduct || metric == VectorMetric::Cosine;
}

IvfVectorReader::IvfVectorReader(const ColumnReader& vector_column,
                                 ReadContext& ctx)
  : _d{static_cast<uint32_t>(vector_column.ArraySize())},
    _reader{&vector_column},
    _ctx{&ctx},
    _scan{vector_column.InitScan(ctx)} {
  SDB_ASSERT(vector_column.Child());
}

void IvfVectorReader::Seek(uint64_t row) {
  if (row < _reader->GatherCursor(_scan)) {
    _scan = _reader->InitScan(*_ctx);
  }
  if (const uint64_t cur = _reader->GatherCursor(_scan); row > cur) {
    _reader->Skip(_scan, static_cast<duckdb::idx_t>(row - cur));
  }
}

const float* IvfVectorReader::ReadDocBatch(doc_id_t first, size_t count) {
  SDB_ASSERT(count >= 1);
  const uint64_t row0 = static_cast<uint64_t>(first) - doc_limits::min();
  if (count > _cap) {
    _cap = count;
    _cache =
      duckdb::VectorCache(duckdb::Allocator::DefaultAllocator(),
                          _reader->Type(), static_cast<duckdb::idx_t>(_cap));
  }
  duckdb::Vector out{_cache};
  Seek(row0);
  _reader->Scan(_scan, out, static_cast<duckdb::idx_t>(count));
  return duckdb::FlatVector::GetData<float>(
    duckdb::ArrayVector::GetChildMutable(out));
}

QueryBuilder::ptr IvfIndex::PrepareKnn(const SubReader& segment,
                                       const PrepareContext& ctx,
                                       const VectorFilterOptions& opts,
                                       uint32_t effort) const {
  VectorState state{ctx.memory};
  QueryBuilder::ptr inner;
  if (!PrepareVectorState(_tree, segment, ctx, opts, effort, state, inner,
                          opts.max_search_fanout)) {
    return QueryBuilder::Empty();
  }

  auto query = memory::make_tracked<KnnVectorQuery>(
    ctx.memory, segment, std::move(state), std::span<const float>{opts.query},
    opts.metric, ctx.boost, std::move(inner));
  query->SetStats(ctx.Record());
  return query;
}

QueryBuilder::ptr IvfIndex::PrepareRange(const SubReader& segment,
                                         const PrepareContext& ctx,
                                         const VectorFilterOptions& opts,
                                         float radius, bool inclusive,
                                         uint32_t /*effort*/) const {
  VectorState state{ctx.memory};
  QueryBuilder::ptr inner;
  if (!PrepareVectorState(_tree, segment, ctx, opts,
                          std::numeric_limits<uint32_t>::max(), state, inner)) {
    return QueryBuilder::Empty();
  }

  auto query = memory::make_tracked<RangeVectorQuery>(
    ctx.memory, segment, std::move(state), std::span<const float>{opts.query},
    opts.metric, radius, inclusive, ctx.boost, std::move(inner));
  query->SetStats(ctx.Record());
  return query;
}

}  // namespace irs
