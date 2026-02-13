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

#include "search_data_source.hpp"

#include "connector/primary_key.hpp"
#include "connector/search_remove_filter.hpp"
#include "velox/core/PlanNode.h"

namespace sdb::connector::search {

SearchDataSource::SearchDataSource(
  velox::memory::MemoryPool& memory_pool,
  // use just snapshot for now. But maybe we will need to have
  // this class template (or use some wrapper) to work with
  // WriteBatchWithindex or plain DB with snapshot
  const rocksdb::Snapshot* snapshot, rocksdb::DB& db,
  rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr row_type,
  std::vector<catalog::Column::Id> column_ids,
  catalog::Column::Id effective_column_id, ObjectId object_key,
  irs::IndexReader& reader, const irs::Filter::Query& query)
  : Materializer{memory_pool,
                 snapshot,
                 &db,
                 nullptr,
                 cf,
                 row_type,
                 std::move(column_ids),
                 effective_column_id,
                 object_key},
    _reader{reader},
    _query{query} {}

void SearchDataSource::addSplit(
  std::shared_ptr<velox::connector::ConnectorSplit> split) {
  SDB_ENSURE(split, ERROR_INTERNAL, "SearchDataSource: split is null");
  if (_current_split) {
    SDB_THROW(ERROR_INTERNAL,
              "SearchDataSource: a split is already being processed");
  }
  _current_split = std::move(split);
  // let sequential read make new try
  _is_range = true;
  _current_segment = 0;
  _doc.reset();
}

std::optional<velox::RowVectorPtr> SearchDataSource::next(
  uint64_t size, velox::ContinueFuture& future) {
  SDB_ASSERT(size);
  SDB_ASSERT(_current_split,
             "RocksDBDataSource: inconsistent state, addSplit call missing");
  auto next_segment = [&] {
    SDB_ASSERT(!_doc);
    if (_current_segment < _reader.size()) {
      auto& segment = _reader[_current_segment++];
      _doc = _query.execute({.segment = segment});
      const auto* pk_column =
        segment.column(sdb::connector::search::kPkFieldName);
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
  return ReadRows(index_keys);
}

void SearchDataSource::addDynamicFilter(
  velox::column_index_t output_channel,
  const std::shared_ptr<velox::common::Filter>& filter) {
  VELOX_UNSUPPORTED();
}

uint64_t SearchDataSource::getCompletedBytes() {
  // TODO: implement completed bytes tracking
  return 0;
}

uint64_t SearchDataSource::getCompletedRows() { return _produced; }

std::unordered_map<std::string, velox::RuntimeMetric>
SearchDataSource::getRuntimeStats() {
  // TODO: implement runtime stats reporting
  return {};
}

void SearchDataSource::cancel() {
  // TODO: implement cancellation logic
}

}  // namespace sdb::connector::search
