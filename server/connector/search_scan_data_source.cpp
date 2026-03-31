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

#include "connector/parquet_materializer.hpp"
#include "connector/primary_key.hpp"
#include "connector/rocksdb_materializer.hpp"
#include "connector/search_remove_filter.hpp"
#include "connector/text_materializer.hpp"
#include "velox/core/PlanNode.h"

namespace sdb::connector {

template<typename Materializer>
SearchDataSource<Materializer>::SearchDataSource(
  velox::memory::MemoryPool& memory_pool, Materializer materializer,
  const irs::IndexReader& reader, const irs::Filter::Query& query,
  const irs::Scorer* scorer,
  std::vector<catalog::Column::Id> offsets_column_ids)
  : _memory_pool{memory_pool},
    _materializer{std::move(materializer)},
    _reader{reader},
    _query{query},
    _scorer{scorer},
    _offsets_column_ids{std::move(offsets_column_ids)} {
  _pos_attrs.resize(_offsets_column_ids.size(), nullptr);
  _offs_attrs.resize(_offsets_column_ids.size(), nullptr);
}

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

  velox::VectorPtr scores;
  float* score_raw = nullptr;
  size_t score_idx = 0;
  if (_scorer) {
    scores = velox::BaseVector::create<velox::FlatVector<float>>(
      velox::REAL(), size, &_memory_pool);
    score_raw = scores->asFlatVector<float>()->mutableRawValues();
  }

  // Per-field, per-document flat int64 values: interleaved start,end pairs.
  // offsets_data[field_idx][doc_idx] = {start0, end0, start1, end1, ...}
  const size_t n_offsets_fields = _offsets_column_ids.size();
  std::vector<std::vector<std::vector<int64_t>>> offsets_data(n_offsets_fields);
  for (auto& field_data : offsets_data) {
    field_data.reserve(size);
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
      for (size_t fi = 0; fi < n_offsets_fields; ++fi) {
        _pos_attrs[fi] = irs::GetMutable<irs::PosAttr>(_doc.get());
        _offs_attrs[fi] =
          _pos_attrs[fi] ? irs::get<irs::OffsAttr>(*_pos_attrs[fi]) : nullptr;
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
      for (size_t fi = 0; fi < n_offsets_fields; ++fi) {
        auto& doc_offsets = offsets_data[fi].emplace_back();
        // Stub: emit column_id as start/end pair for each requested field.
        doc_offsets.push_back(static_cast<int64_t>(_offsets_column_ids[fi]));
        doc_offsets.push_back(static_cast<int64_t>(_offsets_column_ids[fi]));
      }
      --size;
    }
    if (irs::doc_limits::eof(doc_id)) {
      if (_scorer) {
        flush_score_block();
        _score_function = {};
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

  // Build one ArrayVector per requested offsets field.
  std::vector<velox::VectorPtr> offsets_per_field;
  if (n_offsets_fields > 0) {
    const auto n_docs =
      static_cast<velox::vector_size_t>(offsets_data[0].size());
    auto offsets_type = catalog::Column::OffsetsType();

    for (size_t fi = 0; fi < n_offsets_fields; ++fi) {
      const auto& field_data = offsets_data[fi];

      velox::vector_size_t total_elements = 0;
      for (const auto& doc_offs : field_data) {
        total_elements += static_cast<velox::vector_size_t>(doc_offs.size());
      }

      auto elements = velox::BaseVector::create<velox::FlatVector<int64_t>>(
        velox::BIGINT(), total_elements, &_memory_pool);
      int64_t* elements_raw =
        elements->asFlatVector<int64_t>()->mutableRawValues();
      velox::vector_size_t elem_idx = 0;
      for (const auto& doc_offs : field_data) {
        for (int64_t v : doc_offs) {
          elements_raw[elem_idx++] = v;
        }
      }

      auto offsets_buf = velox::AlignedBuffer::allocate<velox::vector_size_t>(
        n_docs, &_memory_pool);
      auto* offsets_buf_raw = offsets_buf->asMutable<velox::vector_size_t>();
      auto sizes_buf = velox::AlignedBuffer::allocate<velox::vector_size_t>(
        n_docs, &_memory_pool);
      auto* sizes_buf_raw = sizes_buf->asMutable<velox::vector_size_t>();
      velox::vector_size_t running = 0;
      for (velox::vector_size_t i = 0; i < n_docs; ++i) {
        offsets_buf_raw[i] = running;
        sizes_buf_raw[i] =
          static_cast<velox::vector_size_t>(field_data[i].size());
        running += sizes_buf_raw[i];
      }

      offsets_per_field.push_back(std::make_shared<velox::ArrayVector>(
        &_memory_pool, offsets_type, velox::BufferPtr{nullptr}, n_docs,
        std::move(offsets_buf), std::move(sizes_buf), std::move(elements)));
    }
  }

  // batch ready - materialize it
  return _materializer.ReadRows(index_keys, std::move(scores),
                                std::move(offsets_per_field));
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
template class SearchDataSource<TextMaterializer>;

}  // namespace sdb::connector
