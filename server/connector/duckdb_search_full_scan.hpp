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

#include <cstdint>
#include <duckdb.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/filter.hpp>
#include <iresearch/search/levenshtein_filter.hpp>
#include <iresearch/search/prefix_filter.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/terms_filter.hpp>
#include <iresearch/utils/string.hpp>
#include <iresearch/search/score_function.hpp>
#include <iresearch/search/scorer.hpp>
#include <memory>
#include <optional>
#include <span>
#include <variant>
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
    return std::max<duckdb::idx_t>(1, total_segments);
  }

  const irs::Filter* filter = nullptr;
  std::vector<irs::PrepareCollector::ptr> collectors;
  std::vector<irs::QueryBuilder::ptr> queries;
  std::optional<irs::StatsBuffer> stats;

  absl::Notification prepare_finished;
  std::atomic_uint32_t prepare_segment = 0;
  std::atomic_uint32_t prepare_count = 0;
  std::atomic_uint32_t collector_slots = 0;

  // Scorer state. `scorer_obj` is non-null iff the plan attached BM25 /
  // TFIDF / DFI / LM-* via the projection or ORDER BY rewrite.
  std::unique_ptr<irs::Scorer> scorer_obj;
  const SearchScan* scan = nullptr;

  bool count_only = false;

  bool ts_dict_mode = false;
  bool TsDictMode() const { return ts_dict_mode; }
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
  void OnSegmentPrepare(duckdb::ClientContext& ctx, const irs::SubReader& seg,
                        uint32_t seg_idx, SearchFullScanGlobalState& g);
  bool OnSegmentsExhausted(duckdb::ClientContext& ctx,
                           SearchFullScanGlobalState& g,
                           duckdb::DataChunk& output);

 private:
  bool prepped = false;
  void PrepEmitBuffer(duckdb::ClientContext& ctx, SearchFullScanGlobalState& g);
};

struct SearchFullScanScanLocalState : public SegDocBufferedScanLocalState {
  uint32_t current_seg_idx = 0;
  uint64_t bulk_doc_in_seg = 0;
  uint64_t bulk_seg_doc_count = 0;

  irs::DocIterator::ptr streaming_doc;
  irs::ScoreFunction streaming_score_function;
  irs::ColumnArgsFetcher score_fetcher;
  std::vector<irs::doc_id_t> chunk_hits;
  std::vector<float> chunk_scores;

  std::vector<FieldEntry> offsets_entries;
  std::vector<highlight::HitRange> offsets_doc_scratch;
  uint32_t offsets_prepped_seg = std::numeric_limits<uint32_t>::max();

  void StartSegment(duckdb::ClientContext& ctx, const irs::SubReader& seg,
                    uint32_t seg_idx, SearchFullScanGlobalState& g);
  void OnSegmentPrepare(duckdb::ClientContext& ctx, const irs::SubReader& seg,
                        uint32_t seg_idx, SearchFullScanGlobalState& g);
  duckdb::idx_t EmitChunk(duckdb::ClientContext& ctx,
                          SearchFullScanGlobalState& g,
                          duckdb::DataChunk& output,
                          duckdb::idx_t output_start);

 private:
  void AdvanceChunk(SearchFullScanGlobalState& g, duckdb::idx_t budget);
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

struct TsDictLocalState : public CommonScanLocalState {
  // Per enumerated field: its output column slots.
  struct FieldState {
    irs::field_id field_id = irs::field_limits::invalid();
    duckdb::idx_t term_slot = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t term_raw_slot = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t count_slot = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t freq_slot = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t score_slot = duckdb::DConstants::INVALID_INDEX;
  };

  std::vector<FieldState> fields;
  // Single-field WHERE constraint (term_filter) -- only set when fields.size()
  // == 1; null for global / multi-field enumeration.
  const irs::Filter* term_filter = nullptr;

  void StartSegment(duckdb::ClientContext& ctx, const irs::SubReader& seg,
                    uint32_t seg_idx, SearchFullScanGlobalState& g);
  duckdb::idx_t EmitChunk(duckdb::ClientContext& ctx,
                          SearchFullScanGlobalState& g,
                          duckdb::DataChunk& output,
                          duckdb::idx_t output_start);

 private:
  void OpenField(size_t field_idx);
  duckdb::idx_t EmitField(duckdb::DataChunk& output, duckdb::idx_t output_start,
                          duckdb::idx_t budget);

  const irs::SubReader* _seg = nullptr;
  size_t _cur_field = 0;
  std::variant<irs::AllTermIterator, irs::ByTermIterator,
               irs::ByPrefixIterator, irs::ByRangeIterator,
               irs::ByTermsIterator, irs::LevenshteinIterator>
    _cursor;
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
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::RowGroupOrderOptions> options,
  duckdb::optional_ptr<duckdb::FunctionData> bind_data);

}  // namespace sdb::connector
