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

#include "search_scan_data_source.hpp"

#include "connector/parquet_materializer.hpp"
#include "connector/primary_key.hpp"
#include "connector/rocksdb_materializer.hpp"
#include "connector/search_remove_filter.hpp"
#include "velox/core/PlanNode.h"

namespace sdb::connector {

template<typename Materializer>
SearchDataSource<Materializer>::SearchDataSource(
  velox::memory::MemoryPool& memory_pool, Materializer materializer,
  const irs::IndexReader& reader, const irs::Filter::Query& query)
  : _memory_pool{memory_pool},
    _materializer{std::move(materializer)},
    _reader{reader},
    _query{query} {}

template<typename Materializer>
void SearchDataSource<Materializer>::addSplit(
  std::shared_ptr<velox::connector::ConnectorSplit> split) {
  SDB_ENSURE(split, ERROR_INTERNAL, "SearchScanDataSource: split is null");
  if (_current_split) {
    SDB_THROW(ERROR_INTERNAL,
              "SearchScanDataSource: a split is already being processed");
  }
  _current_split = std::move(split);
  _current_segment = 0;
  _doc.reset();
}

template<typename Materializer>
std::optional<velox::RowVectorPtr> SearchDataSource<Materializer>::next(
  uint64_t size, velox::ContinueFuture& future) {
  SDB_ASSERT(size);
  SDB_ASSERT(_current_split,
             "RocksDBDataSource: inconsistent state, addSplit call missing");
  auto next_segment = [&] {
    SDB_ASSERT(!_doc);
    if (_current_segment < _reader.size()) {
      auto& segment = _reader[_current_segment++];
      _doc = segment.mask(_query.execute({.segment = segment}));
      const auto* pk_column = segment.column(connector::kPkFieldName);
      _pk_iterator = pk_column->iterator(irs::ColumnHint::Normal);
      SDB_ASSERT(_pk_iterator);
      _pk_value = irs::get<irs::PayAttr>(*_pk_iterator);
      SDB_ASSERT(_pk_value);
    }
  };

  primary_key::Keys index_keys{_memory_pool};
  index_keys.reserve(size);
  while (size) {
    if (!_doc) {
      next_segment();
      if (!_doc) {
        // index exhausted
        break;
      }
    }
    irs::doc_id_t doc_id;
    while (size && !irs::doc_limits::eof(doc_id = _doc->advance())) {
      SDB_ENSURE(doc_id == _pk_iterator->seek(doc_id), ERROR_INTERNAL,
                 "PK column missing document");
      index_keys.emplace_back(irs::ViewCast<char>(_pk_value->value));
      --size;
    }
    if (irs::doc_limits::eof(doc_id)) {
      _doc.reset();
    }
  }
  if (index_keys.empty()) {
    _current_split.reset();
    return nullptr;
  }

  // batch ready - materialize it
  return _materializer.ReadRows(index_keys);
}

template<typename Materializer>
void SearchDataSource<Materializer>::addDynamicFilter(
  velox::column_index_t output_channel,
  const std::shared_ptr<velox::common::Filter>& filter) {
  VELOX_UNSUPPORTED();
}

template<typename Materializer>
uint64_t SearchDataSource<Materializer>::getCompletedBytes() {
  // TODO: implement completed bytes tracking
  return 0;
}

template<typename Materializer>
uint64_t SearchDataSource<Materializer>::getCompletedRows() {
  return _produced;
}

template<typename Materializer>
std::unordered_map<std::string, velox::RuntimeMetric>
SearchDataSource<Materializer>::getRuntimeStats() {
  // TODO: implement runtime stats reporting
  return {};
}

template<typename Materializer>
void SearchDataSource<Materializer>::cancel() {
  // TODO: implement cancellation logic
}

template class SearchDataSource<RocksDBMaterializer>;
template class SearchDataSource<ParquetMaterializer>;

}  // namespace sdb::connector
