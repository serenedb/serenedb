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

#include <cstdint>
#include <duckdb.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/search/filter.hpp>
#include <iresearch/search/score_function.hpp>
#include <iresearch/search/scorer.hpp>
#include <memory>
#include <optional>
#include <vector>

#include "connector/duckdb_scan_base.hpp"
#include "connector/index_source.h"
#include "connector/offsets_collector.hpp"
#include "connector/search_pk_lookup.h"

namespace sdb::connector {

struct SearchFullScanGlobalState : public CommonScanGlobalState {
  SearchFullScanGlobalState() noexcept = default;

  // Parallel top-K coordination. `hits` is pre-sized to
  // MaxThreads * BlockSize(top_k); each thread carves its own
  // BlockSize(top_k) slice (fetch_add on next_slice_idx). Threads
  // CAS-tighten global_kth_score so others can prune harder.
  size_t total_segments = 0;
  std::atomic_uint32_t next_segment = 0;
  std::atomic<irs::score_t> global_kth_score{
    std::numeric_limits<irs::score_t>::lowest()};
  std::atomic<size_t> next_slice_idx{0};
  bool parallel_topk = false;

  duckdb::idx_t MaxThreads() const override {
    if (parallel_topk) {
      return std::max<duckdb::idx_t>(1, total_segments / 2);
    }
    return std::max<duckdb::idx_t>(1, total_segments);
  }

  // Prepared once in InitGlobal with scorer attached so IDF/norm stats
  // get collected up front (re-preparing later would mis-handle filters
  // that mutate options(), e.g. GeoFilter).
  irs::Filter::Query::ptr query;
  // Read-only after construction; shared across all threads.
  std::unique_ptr<irs::Scorer> scorer_obj;

  std::vector<irs::ScoreDoc> hits;

  // Match-all + INCLUDE-only + no deletes shortcut: each thread reads
  // a segment's columnstore vector-at-a-time via ColumnSegment::Scan.
  bool bulk_scan_active = false;
};

// Per-thread state for all three BM25 paths (top-K / streaming / bulk).
struct SearchFullScanLocalState : public CommonScanLocalState {
  // ----- Top-K -----
  std::span<irs::ScoreDoc> hit_slice;  // into gstate.hits
  irs::score_t local_threshold = std::numeric_limits<irs::score_t>::lowest();
  irs::ColumnArgsFetcher score_fetcher;
  std::optional<irs::NthPartitionScoreCollector> collector;
  std::vector<irs::ScoreDoc> top_hits;
  std::vector<float> topk_scores;
  std::vector<SegDoc> seg_docs;
  size_t current_idx = 0;
  PrimaryKeyBatch pk_batch;
  std::shared_ptr<IndexSource> index_source;
  std::vector<uint32_t> lookup_scratch;
  const SereneDBScanBindData* bind_data = nullptr;

  // Per-thread offsets state (shared between top-K and streaming).
  std::vector<FieldEntry> offsets_entries;
  std::vector<highlight::HitRange> offsets_doc_scratch;

  // ----- Streaming -----
  irs::DocIterator::ptr streaming_doc;
  uint32_t streaming_seg_idx = 0;
  std::unique_ptr<SegmentPkSequentialFetcher> streaming_segment_pk;
  irs::ScoreFunction streaming_score_function;
  std::unique_ptr<duckdb::Vector> streaming_pk_vec;
  std::vector<irs::doc_id_t> streaming_chunk_doc_ids;

  // ----- Bulk-cs-scan cursor -----
  uint32_t bulk_seg_idx = 0;
  uint64_t bulk_doc_in_seg = 0;
  uint64_t bulk_seg_doc_count = 0;

  // Skeleton hooks; dispatch on parallel_topk / bulk_scan_active.
  void OnSegment(duckdb::ClientContext& ctx, const irs::SubReader& seg,
                 uint32_t seg_idx, SearchFullScanGlobalState& g);
  void OnSegmentsExhausted(duckdb::ClientContext& ctx,
                           SearchFullScanGlobalState& g);
  EmitOutput EmitNextChunk(duckdb::ClientContext& ctx,
                           SearchFullScanGlobalState& g,
                           duckdb::DataChunk& output);

 private:
  void OnSegmentTopK(const irs::SubReader& seg, uint32_t seg_idx,
                     SearchFullScanGlobalState& g);
  void OnSegmentsExhaustedTopK(duckdb::ClientContext& ctx,
                               SearchFullScanGlobalState& g);
  EmitOutput EmitNextChunkTopK(duckdb::ClientContext& ctx,
                               SearchFullScanGlobalState& g,
                               duckdb::DataChunk& output);

  void OnSegmentStreaming(duckdb::ClientContext& ctx, const irs::SubReader& seg,
                          uint32_t seg_idx, SearchFullScanGlobalState& g);
  EmitOutput EmitNextChunkStreaming(duckdb::ClientContext& ctx,
                                    SearchFullScanGlobalState& g,
                                    duckdb::DataChunk& output);

  void OnSegmentBulk(const irs::SubReader& seg, uint32_t seg_idx,
                     SearchFullScanGlobalState& g);
  EmitOutput EmitNextChunkBulk(SearchFullScanGlobalState& g,
                               duckdb::DataChunk& output);
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchFullScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input);

duckdb::unique_ptr<duckdb::LocalTableFunctionState> SearchFullScanInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* global_state);

void SearchFullScanFunction(duckdb::ClientContext& context,
                            duckdb::TableFunctionInput& data,
                            duckdb::DataChunk& output);

// DuckDB TopNHintPushdown hook. Validates DESC on the synthetic score
// column and stamps `score_top_k` so WAND can prune below the K-th score.
bool SereneDBSetTopNHint(duckdb::ClientContext& context,
                         duckdb::LogicalTopN& top_n, duckdb::LogicalGet& get,
                         duckdb::optional_ptr<duckdb::FunctionData> bind_data);

}  // namespace sdb::connector
