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
            std::shared_ptr<const QuantizerCodebook> codebook,
            std::vector<float> query, VectorMetric metric, uint32_t d,
            uint32_t record_size, uint32_t ef, score_t threshold,
            size_t max_results, bool inclusive, score_t boost,
            QueryBuilder::ptr&& inner = nullptr,
            HnswFilterMode filter_mode = HnswFilterMode::Auto)
    : QueryBuilderImpl{segment},
      _data{std::move(data)},
      _codebook{std::move(codebook)},
      _inner{std::move(inner)},
      _query{query.size(),
             [&](float& slot, size_t i) noexcept { slot = query[i]; }},
      _metric{metric},
      _d{d},
      _record_size{record_size},
      _ef{ef},
      _threshold{threshold},
      _max_results{max_results},
      _boost{boost},
      _filter_mode{filter_mode},
      _inclusive{inclusive} {}

  // The hits of the graph search, ascending by doc, deleted docs dropped.
  // With an inner predicate, or a table filter whose predicates fold into a
  // set (TableFilter::Foldable), only docs they admit are returned: the walk
  // keeps moving through every node but admits what the set passes, and a
  // set too sparse for the graph is answered by scanning its docs. A table
  // that does not fold is left to the caller to apply to the hits.
  // `parts` splits the segment's rows into equal doc ranges for concurrent
  // callers, each running one `part`: only a scan can be split that way, so a
  // split query scans (the caller asked for it because ScanCandidates said a
  // scan is how this query answers its filter).
  std::vector<ScoreDoc> RunSearch(detail::TableFilter* table = nullptr,
                                  uint32_t part = 0, uint32_t parts = 1) const;

  // How many docs a scan would score, when scanning is how this query would
  // answer its inner filter; nullopt when it would walk the graph, has no
  // inner filter, or is a radius search.
  // `parallel` is how many workers a scan may spread over: a scan that splits
  // is cheaper per worker, so it wins the comparison at selectivities where a
  // single-threaded scan would not. `table_rows` is an upper bound on what the
  // caller's table filter admits, for a query whose predicate lives there.
  std::optional<uint64_t> ScanCandidates(
    uint32_t parallel = 1,
    std::optional<uint64_t> table_rows = std::nullopt) const;

  const QueryBuilder* Inner() const noexcept { return _inner.get(); }

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

 private:
  template<typename Dist>
  void RunFiltered(Dist& dist, detail::TableFilter* table,
                   HnswSearchScratch& scratch, uint32_t part,
                   uint32_t parts) const;

  // The docs the predicate admits, folded once for all the parts of a split
  // scan: every part would otherwise fold the set from the first doc up to the
  // end of its own range, which is most of the work of the scan itself.
  std::span<const uint64_t> FoldOnce(detail::TableFilter* table,
                                     doc_id_t docs_count) const;

  mutable std::mutex _fold_lock;
  mutable std::vector<uint64_t> _folded;
  mutable bool _folded_done = false;

  std::shared_ptr<const HnswData> _data;
  std::shared_ptr<const QuantizerCodebook> _codebook;
  QueryBuilder::ptr _inner;
  containers::Fixed<float> _query;
  VectorMetric _metric;
  uint32_t _d;
  uint32_t _record_size;
  uint32_t _ef;
  score_t _threshold;
  size_t _max_results;
  score_t _boost;
  HnswFilterMode _filter_mode;
  bool _inclusive;
};

}  // namespace irs
