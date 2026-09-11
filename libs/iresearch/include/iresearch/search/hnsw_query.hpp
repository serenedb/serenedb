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
#include <utility>
#include <vector>

#include "iresearch/formats/hnsw/hnsw_reader.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/query_builder_impl.hpp"

namespace irs {

class HnswQuery : public QueryBuilderImpl<HnswQuery> {
 public:
  HnswQuery(const SubReader& segment, std::shared_ptr<const HnswData> data,
            std::shared_ptr<const QuantizerCodebook> codebook,
            std::vector<float> query, VectorMetric metric, uint32_t d,
            uint32_t record_size, uint32_t ef, score_t threshold,
            size_t max_results, bool inclusive, score_t boost)
    : QueryBuilderImpl{segment},
      _data{std::move(data)},
      _codebook{std::move(codebook)},
      _query{std::move(query)},
      _metric{metric},
      _d{d},
      _record_size{record_size},
      _ef{ef},
      _threshold{threshold},
      _max_results{max_results},
      _boost{boost},
      _inclusive{inclusive} {}

  std::vector<ScoreDoc> RunSearch() const;

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

 private:
  std::shared_ptr<const HnswData> _data;
  std::shared_ptr<const QuantizerCodebook> _codebook;
  std::vector<float> _query;
  VectorMetric _metric;
  uint32_t _d;
  uint32_t _record_size;
  uint32_t _ef;
  score_t _threshold;
  size_t _max_results;
  score_t _boost;
  bool _inclusive;
};

void HnswRefuseFilter(const detail::TableFilter* table);

}  // namespace irs
