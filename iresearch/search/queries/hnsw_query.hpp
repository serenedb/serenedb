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

#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/formats/hnsw/hnsw_bases.hpp"
#include "iresearch/formats/hnsw/hnsw_reader.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/detail/table_filter.hpp"
#include "iresearch/search/queries/query_builder_impl.hpp"
#include "iresearch/utils/containers/fixed.hpp"

namespace irs {

class HnswQuery : public QueryBuilderImpl<HnswQuery> {
 public:
  HnswQuery(const SubReader& segment, std::shared_ptr<const HnswData> data,
            const VectorFilterOptions& opts, uint32_t record_size,
            HnswSearchLimits limits, score_t boost, QueryBuilder::ptr&& inner,
            AnnQueryCache* cache);

  std::vector<ScoreDoc> RunSearch(detail::TableFilter* table = nullptr) const;

  const QueryBuilder* Inner() const noexcept { return _inner.get(); }

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

 private:
  template<typename Dist>
  void RunRadius(Dist& dist, detail::TableFilter* table,
                 HnswSearchScratch& scratch) const;

  template<typename Dist>
  void RunFiltered(Dist& dist, detail::TableFilter* table,
                   HnswSearchScratch& scratch) const;

  void PrepareDistance() const;

  bool ScansByColumn() const;

  void ScanColumn(HnswSearchScratch& scratch) const;

  std::shared_ptr<const HnswData> _data;
  mutable std::shared_ptr<const QuantizerCodebook> _codebook;
  mutable std::shared_ptr<const PanoramaQuery> _panorama;
  mutable absl::once_flag _distance_prepared;
  AnnQueryCache* _cache;
  QueryBuilder::ptr _inner;
  const ColumnReader* _exact;
  containers::Fixed<float> _query;
  VectorMetric _metric;
  uint32_t _d;
  uint32_t _record_size;
  uint32_t _ef;
  score_t _threshold;
  size_t _max_results;
  score_t _boost;
  HnswFilterMode _filter_mode;
  HnswColumnFilter _column_filter;
  bool _inclusive;
};

extern template class QueryBuilderImpl<HnswQuery>;

}  // namespace irs
