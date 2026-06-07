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

  std::atomic<irs::score_t> global_kth_score{
    std::numeric_limits<irs::score_t>::lowest()};
  bool parallel_topk = false;

  duckdb::idx_t MaxThreads() const override {
    if (count_star && !query) {
      return 1;
    }
    return std::max<duckdb::idx_t>(1, total_segments);
  }

  irs::Filter::Query::ptr query;
  std::unique_ptr<irs::Scorer> scorer_obj;

  bool bulk_scan_active = false;
  bool count_star = false;
};

struct SearchFullScanTopKLocalState : public SegDocBufferedScanLocalState {
  std::vector<irs::ScoreDoc> hit_buf;
  std::span<irs::ScoreDoc> hit_slice;
  // min() (not lowest()): SetScoreThreshold asserts new <= old, and WAND
  // seeds the iterator-local threshold at min().
  irs::score_t local_threshold = std::numeric_limits<irs::score_t>::min();
  irs::ColumnArgsFetcher score_fetcher;
  std::optional<irs::NthPartitionScoreCollector> collector;
  std::span<const irs::ScoreDoc> top_hits;
  std::vector<FieldEntry> offsets_entries;
  std::vector<highlight::HitRange> offsets_doc_scratch;
  uint32_t offsets_prepped_seg = std::numeric_limits<uint32_t>::max();

  void OnSegment(duckdb::ClientContext& ctx, const irs::SubReader& seg,
                 uint32_t seg_idx, SearchFullScanGlobalState& g);
  bool OnSegmentsExhausted(duckdb::ClientContext& ctx,
                           SearchFullScanGlobalState& g,
                           duckdb::DataChunk& output);

 private:
  bool prepped = false;
  void PrepEmitBuffer(duckdb::ClientContext& ctx, SearchFullScanGlobalState& g);
};

struct SearchFullScanStreamingLocalState : public SegDocBufferedScanLocalState {
  irs::DocIterator::ptr streaming_doc;
  uint32_t streaming_seg_idx = 0;
  irs::ScoreFunction streaming_score_function;
  irs::ColumnArgsFetcher score_fetcher;
  std::vector<irs::doc_id_t> chunk_hits;
  std::vector<float> chunk_scores;
  std::vector<FieldEntry> offsets_entries;
  std::vector<highlight::HitRange> offsets_doc_scratch;
  uint32_t offsets_prepped_seg = std::numeric_limits<uint32_t>::max();

  void OnSegment(duckdb::ClientContext& ctx, const irs::SubReader& seg,
                 uint32_t seg_idx, SearchFullScanGlobalState& g);
  bool EmitNextChunk(duckdb::ClientContext& ctx, SearchFullScanGlobalState& g,
                     duckdb::DataChunk& output);

 private:
  void AdvanceChunk(SearchFullScanGlobalState& g);
};

struct SearchFullScanCountLocalState : public CommonScanLocalState {
  uint64_t local_count = 0;
  uint64_t local_emitted = 0;

  void OnSegment(duckdb::ClientContext& ctx, const irs::SubReader& seg,
                 uint32_t seg_idx, SearchFullScanGlobalState& g);
  bool OnSegmentsExhausted(duckdb::ClientContext& ctx,
                           SearchFullScanGlobalState& g,
                           duckdb::DataChunk& output);
};

struct SearchFullScanBulkLocalState : public CommonScanLocalState {
  uint32_t bulk_seg_idx = 0;
  uint64_t bulk_doc_in_seg = 0;
  uint64_t bulk_seg_doc_count = 0;

  void OnSegment(duckdb::ClientContext& ctx, const irs::SubReader& seg,
                 uint32_t seg_idx, SearchFullScanGlobalState& g);
  bool EmitNextChunk(duckdb::ClientContext& ctx, SearchFullScanGlobalState& g,
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

void IResearchSetScanOrder(
  duckdb::unique_ptr<duckdb::RowGroupOrderOptions> options,
  duckdb::optional_ptr<duckdb::FunctionData> bind_data);

}  // namespace sdb::connector
