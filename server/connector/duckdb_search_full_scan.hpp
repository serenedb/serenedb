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
#include <iresearch/index/index_source.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/filter.hpp>
#include <iresearch/search/score_function.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/terms_filter.hpp>
#include <iresearch/utils/string.hpp>
#include <memory>
#include <optional>
#include <span>
#include <variant>
#include <vector>

#include "connector/duckdb_scan_base.hpp"
#include "connector/duckdb_table_function.h"
#include "connector/offsets_collector.hpp"

namespace sdb::connector {

struct SearchFullScanGlobalState : public CommonScanGlobalState {
  SearchFullScanGlobalState() noexcept = default;

  std::atomic<irs::score_t> global_kth_score{
    std::numeric_limits<irs::score_t>::lowest()};
  bool parallel_topk = false;
  uint32_t rerank_pool = 0;

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
  irs::Filter::ptr owned_filter;

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

  bool ts_dict_mode = false;
};

struct SearchFullScanTopKLocalState : public SegDocBufferedScanLocalState {
  std::vector<irs::ScoreDoc> hit_buf;
  std::span<irs::ScoreDoc> hit_slice;
  irs::score_t local_threshold = std::numeric_limits<irs::score_t>::min();
  irs::ColumnArgsFetcher score_fetcher;
  // One alternative is engaged per scan, chosen from the score order and
  // whether the query pushed a table filter; the plain (unfiltered)
  // collectors carry zero filter overhead, the filtered ones gate hits
  // through the scan filter.
  using CollectorDesc = irs::NthPartitionScoreCollector<irs::Order::DESC>;
  using CollectorAsc = irs::NthPartitionScoreCollector<irs::Order::ASC>;
  using FCollectorDesc =
    irs::NthPartitionFilteredScoreCollector<irs::Order::DESC>;
  using FCollectorAsc =
    irs::NthPartitionFilteredScoreCollector<irs::Order::ASC>;
  std::variant<std::monostate, CollectorDesc, CollectorAsc, FCollectorDesc,
               FCollectorAsc>
    collector;
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

  // Filter path only: candidates collected from streaming_doc (via the scan
  // filter's NextUnpruned) for one chunk, then compacted in place to the
  // rows that pass; chunk_cursor tracks how many survivors are already
  // staged into hit_batcher across EmitChunk calls.
  std::vector<irs::doc_id_t> chunk_hits;
  std::vector<float> chunk_scores;
  size_t chunk_cursor = 0;

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
  void AdvanceChunk(SearchFullScanGlobalState& g, duckdb::idx_t budget);
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

struct TsDictLocalState : public CommonScanLocalState {
  // Per enumerated field: its output column slots.
  struct FieldState {
    irs::field_id field_id = irs::field_limits::invalid();
    irs::field_id null_field_id = irs::field_limits::invalid();
    duckdb::idx_t term_slot = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t term_raw_slot = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t count_slot = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t freq_slot = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t score_slot = duckdb::DConstants::INVALID_INDEX;
    TsDictTermUses term_uses = TsDictTermUses::kNone;
    const irs::Filter* having_filter = nullptr;
  };

  enum class CountMode { kMeta, kMasked, kWhere };

  std::vector<FieldState> fields;
  CountMode _count_mode = CountMode::kMeta;
  const irs::QueryBuilder* _where_query = nullptr;

  void StartSegment(duckdb::ClientContext& ctx, const irs::SubReader& seg,
                    uint32_t seg_idx, SearchFullScanGlobalState& g);
  duckdb::idx_t EmitChunk(duckdb::ClientContext& ctx,
                          SearchFullScanGlobalState& g,
                          duckdb::DataChunk& output,
                          duckdb::idx_t output_start);

 private:
  bool NextField();
  irs::TermIterator::ptr MakeTermSource(const FieldState& field,
                                        const irs::TermReader& reader);
  duckdb::idx_t EmitField(duckdb::DataChunk& output, duckdb::idx_t output_start,
                          duckdb::idx_t capacity);
  duckdb::idx_t AppendNullRow(duckdb::DataChunk& output,
                              const FieldState& field, duckdb::idx_t row);

  const irs::SubReader* _seg = nullptr;
  bool _null_pending = false;
  const FieldState* _field = nullptr;
  const FieldState* _next_field = nullptr;
  CountMode _cursor_mode = CountMode::kMeta;
  irs::TermIterator::ptr _cursor;
};

void RunStreamingScan(duckdb::ClientContext& ctx, SearchFullScanGlobalState& g,
                      TsDictLocalState& l, duckdb::DataChunk& output);

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
