////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <velox/connectors/Connector.h>

#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/search/column_collector.hpp>
#include <iresearch/search/filter.hpp>
#include <iresearch/search/score_function.hpp>
#include <iresearch/search/scorer.hpp>
#include <string>
#include <vector>

#include "basics/fwd.h"
#include "catalog/table_options.h"
#include "connector/offsets_collector.hpp"
#include "iresearch/index/index_reader.hpp"

namespace sdb::connector {

template<typename Materializer>
class SearchDataSource final : public velox::connector::DataSource {
 public:
  SearchDataSource(
    velox::memory::MemoryPool& memory_pool, Materializer materializer,
    const irs::IndexReader& reader, const irs::Filter::Query& query,
    const irs::Scorer* scorer,
    std::vector<catalog::Column::OffsetsFieldRequest> offsets_fields);

  void addSplit(std::shared_ptr<velox::connector::ConnectorSplit> split) final;
  std::optional<velox::RowVectorPtr> next(uint64_t size,
                                          velox::ContinueFuture& future) final;
  void addDynamicFilter(
    velox::column_index_t output_channel,
    const std::shared_ptr<velox::common::Filter>& filter) final;
  uint64_t getCompletedBytes() final;
  uint64_t getCompletedRows() final;
  std::unordered_map<std::string, velox::RuntimeMetric> getRuntimeStats() final;
  void cancel() final;

 private:
  velox::memory::MemoryPool& _memory_pool;
  Materializer _materializer;
  std::shared_ptr<velox::connector::ConnectorSplit> _current_split;
  const irs::IndexReader& _reader;
  // TODO(Dronplane) when we have sorted indexes we will need Merge reader for
  // all segments. Only sequential for now.
  size_t _current_segment{0};
  const irs::Filter::Query& _query;
  irs::DocIterator::ptr _pk_iterator;
  const irs::PayAttr* _pk_value;
  irs::DocIterator::ptr _doc;
  uint64_t _produced = 0;
  const irs::Scorer* _scorer = nullptr;
  irs::ColumnArgsFetcher _fetcher;
  irs::ScoreFunction _score_function;
  // One entry per requested OFFSETS() column, in the same order as
  // offsets_fields passed to the constructor.
  std::vector<catalog::Column::OffsetsFieldRequest> _offsets_fields;
  // 8-byte big-endian encoding of each column ID + mangle byte, matching
  // IResearch field names.
  std::vector<std::string> _binary_field_names;
  // Per-field offset state, rebuilt on each segment transition.
  std::vector<PerFieldState> _offsets_field_state;
  // Pointer to the segment currently being scanned; valid between next_segment
  // and the following _doc.reset().
  const irs::SubReader* _current_seg = nullptr;

  void ResetDocOffsets(const irs::SubReader& segment);
  void CollectDocOffsets(
    irs::doc_id_t doc_id,
    std::vector<std::vector<std::vector<int64_t>>>& offsets_data);
  std::vector<velox::VectorPtr> BuildOffsetsColumns(
    const std::vector<std::vector<std::vector<int64_t>>>& offsets_data) const;
};

}  // namespace sdb::connector
