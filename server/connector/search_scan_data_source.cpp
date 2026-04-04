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
  const irs::Scorer* scorer, size_t topk_limit)
  : _memory_pool{memory_pool},
    _materializer{std::move(materializer)},
    _reader{reader},
    _query{query},
    _scorer{scorer},
    _topk_limit{topk_limit} {}

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
void SearchDataSource<Materializer>::collectTopK() {
  SDB_ASSERT(_topk_limit > 0);
  SDB_ASSERT(_scorer);

  const size_t k = _topk_limit;

  // Single collector across all segments. The segment_index is set before
  // each segment's Collect call, so Push stores it in each ScoreDoc.
  // The threshold naturally rises across segments, pruning more docs.
  irs::score_t score_threshold = std::numeric_limits<irs::score_t>::min();
  std::vector<irs::ScoreDoc> hits(irs::BlockSize(k));
  irs::NthPartitionScoreCollector collector{score_threshold, k,
                                            std::span{hits}};

  irs::ColumnArgsFetcher fetcher;

  for (size_t seg_idx = 0; seg_idx < _reader.size(); ++seg_idx) {
    auto& segment = _reader[seg_idx];

    collector.SetSegmentIndex(static_cast<uint32_t>(seg_idx));
    fetcher.Clear();

    auto it = segment.mask(_query.execute({
      .segment = segment,
      .scorer = _scorer,
    }));

    auto score_func = it->PrepareScore({
      .scorer = _scorer,
      .segment = &segment,
      .fetcher = &fetcher,
    });

    it->Collect(score_func, fetcher, collector);
  }

  auto total_count = collector.Finalize();
  auto result_count = std::min<size_t>(total_count, k);

  // Resolve PKs: group by segment for efficient sequential seeks.
  struct RankedDoc {
    uint32_t segment_index;
    irs::doc_id_t doc_id;
    irs::score_t score;
    size_t rank;  // original position in score-sorted order
  };
  std::vector<RankedDoc> ranked;
  ranked.reserve(result_count);
  for (size_t i = 0; i < result_count; ++i) {
    ranked.push_back(
      {hits[i].segment_index, hits[i].doc_id, hits[i].score, i});
  }

  // Sort by (segment_index, doc_id) for sequential PK column seeks.
  std::sort(ranked.begin(), ranked.end(), [](const auto& a, const auto& b) {
    return std::tie(a.segment_index, a.doc_id) <
           std::tie(b.segment_index, b.doc_id);
  });

  _topk_results.resize(result_count);
  uint32_t prev_seg = std::numeric_limits<uint32_t>::max();
  irs::DocIterator::ptr pk_iterator;
  const irs::PayAttr* pk_value = nullptr;

  for (const auto& rd : ranked) {
    if (rd.segment_index != prev_seg) {
      auto& segment = _reader[rd.segment_index];
      const auto* pk_column = segment.column(connector::kPkFieldName);
      pk_iterator = pk_column->iterator(irs::ColumnHint::Normal);
      SDB_ASSERT(pk_iterator);
      pk_value = irs::get<irs::PayAttr>(*pk_iterator);
      SDB_ASSERT(pk_value);
      prev_seg = rd.segment_index;
    }

    SDB_ENSURE(rd.doc_id == pk_iterator->seek(rd.doc_id), ERROR_INTERNAL,
               "PK column missing document in topk result");
    _topk_results[rd.rank] = {
      std::string{irs::ViewCast<char>(pk_value->value)},
      rd.score,
    };
  }

  _topk_collected = true;
}

template<typename Materializer>
std::optional<velox::RowVectorPtr> SearchDataSource<Materializer>::next(
  uint64_t size, velox::ContinueFuture& future) {
  SDB_ASSERT(size);
  SDB_ASSERT(_current_split,
             "RocksDBDataSource: inconsistent state, addSplit call missing");

  // Top-k path: collect all top-k results upfront, then serve batches.
  if (_topk_limit > 0 && _scorer) {
    if (!_topk_collected) {
      collectTopK();
    }

    if (_topk_cursor >= _topk_results.size()) {
      _current_split.reset();
      return nullptr;
    }

    auto batch_size =
      std::min<size_t>(size, _topk_results.size() - _topk_cursor);

    velox::VectorPtr scores =
      velox::BaseVector::create<velox::FlatVector<float>>(
        velox::REAL(), batch_size, &_memory_pool);
    auto* score_raw = scores->asFlatVector<float>()->mutableRawValues();

    primary_key::Keys index_keys{_memory_pool};
    index_keys.reserve(batch_size);

    for (size_t i = 0; i < batch_size; ++i) {
      const auto& result = _topk_results[_topk_cursor + i];
      index_keys.emplace_back(result.pk);
      score_raw[i] = result.score;
    }
    _topk_cursor += batch_size;

    return _materializer.ReadRows(index_keys, std::move(scores));
  }

  // Normal path: iterate all documents.
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

  // batch ready - materialize it
  return _materializer.ReadRows(index_keys, std::move(scores));
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
