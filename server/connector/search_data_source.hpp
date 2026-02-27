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

#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/search/filter.hpp>

#include "basics/fwd.h"
#include "data_materializer.hpp"
#include "iresearch/index/index_reader.hpp"

namespace sdb::connector::search {

class SearchDataSource final : public velox::connector::DataSource,
                               public Materializer {
 public:
  SearchDataSource(velox::memory::MemoryPool& memory_pool,
                   // Search source always uses snapshot synced with index state
                   // to avoid materialization failures
                   const rocksdb::Snapshot* snapshot, rocksdb::DB& db,
                   rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr row_type,
                   std::vector<catalog::Column::Id> column_ids,
                   catalog::Column::Id effective_column_id, ObjectId object_key,
                   const irs::IndexReader& reader,
                   const irs::Filter::Query& query);

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
  std::shared_ptr<velox::connector::ConnectorSplit> _current_split;
  const irs::IndexReader& _reader;
  // TODO(Dronplane) when we have sorted indexes we will need Merge reader for
  // all segments. Only sequential for now.
  size_t _current_segment{0};
  const irs::Filter::Query& _query;
  irs::DocIterator::ptr _pk_iterator;
  const irs::PayAttr* _pk_value;
  irs::DocIterator::ptr _doc;
  uint64_t _produced{0};
};

}  // namespace sdb::connector::search
