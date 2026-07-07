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

#include <absl/synchronization/notification.h>

#include <array>
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
#include "connector/duckdb_table_function.h"
#include "connector/index_source.h"
#include "connector/offsets_collector.hpp"
#include "connector/search_pk_lookup.h"

namespace sdb::connector {

struct SearchFullScanGlobalState : public CommonScanGlobalState {
  SearchFullScanGlobalState() noexcept = default;

  std::atomic<irs::score_t> global_kth_score{
    std::numeric_limits<irs::score_t>::lowest()};
  bool parallel_topk = false;

  duckdb::idx_t MaxThreads() const final {
    if (count_only && queries.empty()) {
      return 1;
    }
    if (!scan_units.empty()) {
      return std::max<duckdb::idx_t>(1, scan_units.size());
    }
    return std::max<duckdb::idx_t>(1, total_segments);
  }

  const irs::Filter* filter = nullptr;

  struct ScanUnit {
    uint32_t seg;
    uint64_t begin;
    uint64_t count;
    bool bulk;
  };
  std::vector<ScanUnit> scan_units;
  std::atomic_uint32_t next_unit{0};
  std::vector<irs::PrepareCollector::ptr> collectors;
  std::vector<irs::QueryBuilder::ptr> queries;
  std::optional<irs::StatsBuffer> stats;

  absl::Notification prepare_finished;
  std::atomic_uint32_t prepare_segment = 0;
  std::atomic_uint32_t prepare_count = 0;
  std::atomic_uint32_t collector_slots = 0;

  std::unique_ptr<irs::Scorer> scorer_obj;
  const SearchScan* scan = nullptr;

  bool count_only = false;

  bool BulkChunkEligible() const {
    return has_real_column && !scan_score && !has_external_projections &&
           scan->IsMatchAll() && !scan->EmitOffsets();
  }
};

struct SearchFullScanTopKLocalState : public SegDocBufferedScanLocalState {
  std::vector<irs::ScoreDoc> hit_buf;
  std::span<irs::ScoreDoc> hit_slice;
  irs::score_t local_threshold = std::numeric_limits<irs::score_t>::min();
  irs::ColumnArgsFetcher score_fetcher;
  std::optional<irs::NthPartitionScoreCollector> collector;
  std::span<const irs::ScoreDoc> top_hits;
  std::vector<FieldEntry> offsets_entries;
  std::vector<highlight::HitRange> offsets_doc_scratch;
  uint32_t offsets_prepped_seg = std::numeric_limits<uint32_t>::max();

  void OnSegment(duckdb::ClientContext& ctx, const irs::SubReader& seg,
                 uint32_t seg_idx, CommonScanGlobalState& g) override;
  bool OnSegmentsExhausted(duckdb::ClientContext& ctx, CommonScanGlobalState& g,
                           duckdb::DataChunk& output) override;
  void EmitRowOffsets(CommonScanGlobalState& g, const HitsChunk& view,
                      duckdb::DataChunk& output) override;

 private:
  bool _prepared = false;
  void PrepareEmitBuffer(duckdb::ClientContext& ctx,
                         SearchFullScanGlobalState& g);
};

struct SearchFullScanScanLocalState : public SegDocBufferedScanLocalState {
  uint32_t current_seg_idx = 0;
  uint64_t bulk_doc_in_seg = 0;
  uint64_t bulk_seg_doc_count = 0;

  irs::DocIterator::ptr streaming_doc;
  irs::ScoreFunction streaming_score_function;
  irs::ColumnArgsFetcher score_fetcher;
  std::array<float, STANDARD_VECTOR_SIZE> score_window;

  std::vector<FieldEntry> offsets_entries;
  std::vector<highlight::HitRange> offsets_doc_scratch;
  uint32_t offsets_prepped_seg = std::numeric_limits<uint32_t>::max();

  void StartSegment(duckdb::ClientContext& ctx, const irs::SubReader& seg,
                    uint32_t seg_idx, SearchFullScanGlobalState& g);
  void StartUnit(duckdb::ClientContext& ctx,
                 const SearchFullScanGlobalState::ScanUnit& unit,
                 SearchFullScanGlobalState& g);
  duckdb::idx_t EmitChunk(duckdb::ClientContext& ctx,
                          SearchFullScanGlobalState& g,
                          duckdb::DataChunk& output);
  void EmitRowOffsets(CommonScanGlobalState& g, const HitsChunk& view,
                      duckdb::DataChunk& output) override;

 private:
  void PushHits(SearchFullScanGlobalState& g);
};

struct SearchFullScanCountLocalState : public CommonScanLocalState {
  uint64_t local_count = 0;
  uint64_t local_emitted = 0;

  void OnSegment(duckdb::ClientContext& ctx, const irs::SubReader& seg,
                 uint32_t seg_idx, CommonScanGlobalState& g) override;
  bool OnSegmentsExhausted(duckdb::ClientContext& ctx, CommonScanGlobalState& g,
                           duckdb::DataChunk& output) override;
};

void RunStreamingScan(duckdb::ClientContext& ctx, SearchFullScanGlobalState& g,
                      SearchFullScanScanLocalState& l,
                      duckdb::DataChunk& output);

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchFullScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input);

duckdb::unique_ptr<duckdb::LocalTableFunctionState> SearchFullScanInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* global_state);

void SearchFullScanFunction(duckdb::ClientContext& context,
                            duckdb::TableFunctionInput& data,
                            duckdb::DataChunk& output);

void IResearchSetScanOrder(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::RowGroupOrderOptions> options,
  duckdb::optional_ptr<duckdb::FunctionData> bind_data);

}  // namespace sdb::connector
