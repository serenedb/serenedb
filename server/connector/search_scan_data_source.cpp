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

#include <velox/vector/FlatVector.h>

#include "connector/primary_key.hpp"
#include "connector/search_remove_filter.hpp"
#include "velox/core/PlanNode.h"

namespace sdb::connector {

SearchScanDataSource::SearchScanDataSource(
  velox::memory::MemoryPool& memory_pool,
  // use just snapshot for now. But maybe we will need to have
  // this class template (or use some wrapper) to work with
  // WriteBatchWithindex or plain DB with snapshot
  const rocksdb::Snapshot* snapshot, rocksdb::DB& db,
  rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr output_type,
  std::vector<catalog::Column::Id> column_ids,
  catalog::Column::Id effective_column_id, ObjectId object_key,
  const irs::IndexReader& reader, const irs::Filter::Query& query,
  const irs::Scorer* scorer)
  : Materializer{memory_pool,
                 snapshot,
                 &db,
                 nullptr,
                 cf,
                 std::move(output_type),
                 std::move(column_ids),
                 effective_column_id,
                 object_key},
    _reader{reader},
    _query{query},
    _scorer{scorer} {}

void SearchScanDataSource::addSplit(
  std::shared_ptr<velox::connector::ConnectorSplit> split) {
  SDB_ENSURE(split, ERROR_INTERNAL, "SearchScanDataSource: split is null");
  if (_current_split) {
    SDB_THROW(ERROR_INTERNAL,
              "SearchScanDataSource: a split is already being processed");
  }
  _current_split = std::move(split);
  // let sequential read make new try
  _is_range = true;
  _current_segment = 0;
  _doc.reset();
}

std::optional<velox::RowVectorPtr> SearchScanDataSource::next(
  uint64_t size, velox::ContinueFuture& future) {
  SDB_ASSERT(size);
  SDB_ASSERT(_current_split,
             "RocksDBDataSource: inconsistent state, addSplit call missing");

  velox::VectorPtr scores;
  float* score_raw = nullptr;
  size_t score_idx = 0;
  if (_scorer) {
    scores = velox::BaseVector::create<velox::FlatVector<float>>(
      velox::REAL(), size, &_memory_pool);
    score_raw = scores->asFlatVector<float>()->mutableRawValues();
  }

  std::array<irs::doc_id_t, irs::kScoreBlock> block_docs;
  irs::scores_size_t block_count = 0;
  auto flush_score_block = [&] {
    if (block_count == 0) {
      return;
    }
    _fetcher.Fetch({block_docs.data(), block_count});
    _score_function.Score(score_raw + score_idx, block_count);
    score_idx += block_count;
    block_count = 0;
  };

  auto next_segment = [&] {
    SDB_ASSERT(!_doc);
    if (_current_segment < _reader.size()) {
      auto& segment = _reader[_current_segment++];
      _doc = segment.mask(_query.execute({
        .segment = segment,
        .scorer = _scorer,
      }));
      const auto* pk_column = segment.column(connector::kPkFieldName);
      _pk_iterator = pk_column->iterator(irs::ColumnHint::Normal);
      SDB_ASSERT(_pk_iterator);
      _pk_value = irs::get<irs::PayAttr>(*_pk_iterator);
      SDB_ASSERT(_pk_value);
      if (_scorer) {
        _fetcher.Clear();
        _score_function = _doc->PrepareScore({
          .scorer = _scorer,
          .segment = &segment,
          .fetcher = &_fetcher,
        });
      }
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
      if (_scorer) {
        _doc->FetchScoreArgs(block_count);
        block_docs[block_count++] = doc_id;
        if (block_count == irs::kScoreBlock) {
          _fetcher.Fetch(block_docs);
          _score_function.ScoreBlock(score_raw + score_idx);
          score_idx += irs::kScoreBlock;
          block_count = 0;
        }
      }
      --size;
    }
    if (irs::doc_limits::eof(doc_id)) {
      if (_scorer) {
        flush_score_block();
      }
      _doc.reset();
    }
  }

  if (index_keys.empty()) {
    _current_split.reset();
    return nullptr;
  }

  if (_scorer) {
    flush_score_block();
    scores->resize(index_keys.size());
  }

  return ReadRows(index_keys, std::move(scores));
}

void SearchScanDataSource::addDynamicFilter(
  velox::column_index_t output_channel,
  const std::shared_ptr<velox::common::Filter>& filter) {
  VELOX_UNSUPPORTED();
}

uint64_t SearchScanDataSource::getCompletedBytes() {
  // TODO: implement completed bytes tracking
  return 0;
}

uint64_t SearchScanDataSource::getCompletedRows() { return _produced; }

std::unordered_map<std::string, velox::RuntimeMetric>
SearchScanDataSource::getRuntimeStats() {
  // TODO: implement runtime stats reporting
  return {};
}

void SearchScanDataSource::cancel() {
  // TODO: implement cancellation logic
}

}  // namespace sdb::connector
