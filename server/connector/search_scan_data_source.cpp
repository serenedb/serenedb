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

#include <algorithm>
#include <array>

#include "basics/containers/flat_hash_set.h"
#include "catalog/mangling.h"
#include "connector/parquet_materializer.hpp"
#include "connector/primary_key.hpp"
#include "connector/rocksdb_materializer.hpp"
#include "connector/search_filter_builder.hpp"
#include "connector/search_remove_filter.hpp"
#include "connector/text_materializer.hpp"
#include "velox/core/PlanNode.h"

namespace sdb::connector {

template<typename Materializer>
SearchDataSource<Materializer>::SearchDataSource(
  velox::memory::MemoryPool& memory_pool, Materializer materializer,
  const irs::IndexReader& reader, const irs::Filter::Query& query,
  const irs::Scorer* scorer,
  std::vector<catalog::Column::OffsetsFieldRequest> offsets_fields)
  : _memory_pool{memory_pool},
    _materializer{std::move(materializer)},
    _reader{reader},
    _query{query},
    _scorer{scorer},
    _offsets_fields{std::move(offsets_fields)} {
  for (const auto& req : _offsets_fields) {
    std::string name;
    MakeFieldName(req.column_id, name);
    search::mangling::MangleString(name);
    _binary_field_names.push_back(std::move(name));
  }
  _offsets_field_state.resize(_offsets_fields.size());
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
  _current_seg = nullptr;
  _doc.reset();
}

template<typename Materializer>
void SearchDataSource<Materializer>::ResetDocOffsets(
  const irs::SubReader& segment) {
  for (auto& fs : _offsets_field_state) {
    fs.Clear();
  }
  OffsetsCollector visitor(_binary_field_names, _offsets_field_state);
  _query.visit(segment, visitor, irs::kNoBoost);
}

template<typename Materializer>
void SearchDataSource<Materializer>::CollectDocOffsets(
  irs::doc_id_t doc_id,
  std::vector<std::vector<std::vector<int64_t>>>& offsets_data) {
  constexpr auto kFeatures = irs::IndexFeatures::Freq |
                             irs::IndexFeatures::Pos | irs::IndexFeatures::Offs;

  for (size_t fi = 0; fi < offsets_data.size(); ++fi) {
    auto& doc_offsets = offsets_data[fi].emplace_back();
    containers::FlatHashSet<uint64_t> seen_offsets;

    const size_t max_offsets = _offsets_fields[fi].limit;

    for (auto& fe : _offsets_field_state[fi].entries) {
      if (doc_offsets.size() / 2 >= max_offsets) {
        break;
      }
      // Lazy iterator creation: once per segment, reused for all docs.
      if (!fe.docs) {
        SDB_ASSERT(_current_seg);
        fe.docs = std::visit(
          [&]<typename T>(const T* ptr) -> irs::DocIterator::ptr {
            if constexpr (std::is_same_v<T, irs::SeekCookie>) {
              SDB_ASSERT(_offsets_field_state[fi].reader);
              return _offsets_field_state[fi].reader->Iterator(
                kFeatures, irs::PostingCookie{.cookie = ptr});
            } else {
              return ptr->ExecuteWithOffsets(*_current_seg);
            }
          },
          fe.filter);
        if (!fe.docs || irs::doc_limits::eof(fe.docs->value())) {
          fe.docs.reset();
          continue;
        }
        fe.pos = irs::GetMutable<irs::PosAttr>(fe.docs.get());
        if (!fe.pos) {
          fe.docs.reset();
          continue;
        }
        fe.offs = irs::get<irs::OffsAttr>(*fe.pos);
        if (!fe.offs) {
          fe.docs.reset();
          continue;
        }
      }

      if (fe.docs->seek(doc_id) != doc_id) {
        continue;
      }
      while (fe.pos->next()) {
        if (doc_offsets.size() / 2 >= max_offsets) {
          break;
        }
        const uint64_t key = (uint64_t{fe.offs->start} << 32) | fe.offs->end;
        if (!seen_offsets.insert(key).second) {
          continue;
        }
        doc_offsets.push_back(static_cast<int64_t>(fe.offs->start));
        doc_offsets.push_back(static_cast<int64_t>(fe.offs->end));
      }
    }

    // Sort pairs by start ascending.
    if (doc_offsets.size() > 2) {
      using Pair = std::array<int64_t, 2>;
      auto* pairs = reinterpret_cast<Pair*>(doc_offsets.data());
      std::sort(pairs, pairs + doc_offsets.size() / 2,
                [](const Pair& a, const Pair& b) { return a[0] < b[0]; });
    }
  }
}

template<typename Materializer>
std::vector<velox::VectorPtr>
SearchDataSource<Materializer>::BuildOffsetsColumns(
  const std::vector<std::vector<std::vector<int64_t>>>& offsets_data) const {
  const auto n_docs = static_cast<velox::vector_size_t>(offsets_data[0].size());
  auto offsets_type = catalog::Column::MakeOffsetsType();

  std::vector<velox::VectorPtr> offsets_per_field;
  for (size_t fi = 0; fi < offsets_data.size(); ++fi) {
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
  return offsets_per_field;
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
  const size_t n_offsets_fields = _offsets_fields.size();
  std::vector<std::vector<std::vector<int64_t>>> offsets_data;
  if (n_offsets_fields) {
    offsets_data.resize(n_offsets_fields);
    for (auto& field_data : offsets_data) {
      field_data.reserve(size);
    }
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
      _current_seg = &segment;
      if (n_offsets_fields) {
        ResetDocOffsets(segment);
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
      if (n_offsets_fields) {
        CollectDocOffsets(doc_id, offsets_data);
      }
      --size;
    }
    if (irs::doc_limits::eof(doc_id)) {
      if (_scorer) {
        flush_score_block();
        _score_function = {};
      }
      _doc.reset();
      _current_seg = nullptr;
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
  if (n_offsets_fields) {
    offsets_per_field = BuildOffsetsColumns(offsets_data);
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
