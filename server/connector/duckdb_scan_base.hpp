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

#include <absl/functional/overload.h>

#include <algorithm>
#include <atomic>
#include <duckdb.hpp>
#include <duckdb/common/types/vector_cache.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/function/table_function.hpp>
#include <iresearch/index/index_source.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/index/pk_batch_helpers.hpp>
#include <iresearch/index/scan_filter.hpp>
#include <iresearch/search/filter.hpp>
#include <iresearch/types.hpp>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "catalog/table_options.h"
#include "connector/duckdb_table_function.h"
#include "connector/full_scanner.h"
#include "connector/index_source_factory.h"
#include "connector/offsets_collector.hpp"
#include "connector/offsets_writer.hpp"
#include "connector/search_pk_lookup.h"
#include "iresearch/index/column_extract.hpp"
#include "iresearch/index/hit_batcher.hpp"

namespace irs {

class IndexReader;

}  // namespace irs
namespace sdb::connector {

struct SereneDBScanBindData;

struct CommonScanGlobalState : public duckdb::GlobalTableFunctionState {
  std::vector<duckdb::idx_t> projected_columns;
  std::vector<duckdb::LogicalType> projected_types;
  std::vector<duckdb::ColumnIndex> projected_column_indexes;
  duckdb::ClientContext* client_context = nullptr;

  std::vector<duckdb::idx_t> external_projected_columns;
  std::vector<ColumnstoreProjection> cs_projections;
  bool has_external_projections = false;
  bool has_real_column = false;

  bool scan_tableoid = false;
  duckdb::idx_t tableoid_output_idx = 0;
  int64_t tableoid_value = 0;

  bool scan_score = false;
  duckdb::idx_t score_output_idx = 0;
  const VectorScorerOptions* vector_scorer = nullptr;

  // Output slot for a search-table generated PK (rowid), materialized from
  // `.col` as a covered column; INVALID_INDEX when not projected.
  duckdb::idx_t generated_pk_output_idx = duckdb::DConstants::INVALID_INDEX;

  bool finished = false;

  const irs::IndexReader* reader = nullptr;
  size_t total_segments = 0;
  std::atomic_uint32_t next_segment{0};

  std::atomic<duckdb::idx_t> produced_rows{0};

  // Pushed filters the scan enforces itself (filter_pushdown = true means
  // nothing above re-checks). Only `.col` (INCLUDE) column filters live here --
  // applied on the columnstore before materialize. Lookup-column filters go to
  // the source's native lookup scan (see `pushed_filters`), not here. Filters
  // over virtual/extract output slots land in `row_expr`, enforceable only on
  // the materialized chunk (ApplyRowFilter); a lookup or row_expr filter
  // demotes top-k/bulk to streaming.
  irs::TableFilter table_filter;
  duckdb::unique_ptr<duckdb::Expression> row_expr;

  // The scan's pushed filters (as duckdb hands them to us), forwarded verbatim
  // to the lookup source so its native scan evaluates lookup-column filters
  // (FilterSelection + late materialization). Lives for the query.
  const duckdb::TableFilterSet* pushed_filters = nullptr;
  // A pushed filter targets a lookup (source-only) column: it can only be
  // applied during the source lookup, so it forbids the fast collector/bulk
  // paths (which never run the lookup per candidate) -- forces streaming.
  bool has_lookup_filter = false;

  // filter_prune: when set, the scanned column_ids include filter-only columns
  // the output must not emit. These are indexes into the scanned columns that
  // form the output (a reorder/narrow); empty = emit the scanned columns as-is.
  duckdb::vector<duckdb::idx_t> output_projection_ids;

  duckdb::idx_t MaxThreads() const override { return 1; }
};

struct HitsChunk;

struct CommonScanLocalState : public duckdb::LocalTableFunctionState {
  bool segments_exhausted = false;
  // Holds the scanned column_ids when output_projection_ids is set; the output
  // then references the projected subset out of this (a reorder, not a copy).
  duckdb::DataChunk scan_chunk;

  irs::PrepareCollector* prepare_collector = nullptr;
  bool prepared = false;

  std::vector<std::unique_ptr<FullScanner>> full_scanners;

  irs::ScanFilter filter;
  duckdb::unique_ptr<duckdb::ExpressionExecutor> row_expr_executor;
  std::unique_ptr<duckdb::SelectionVector> row_expr_sel;

  virtual void OnSegment(duckdb::ClientContext&, const irs::SubReader&,
                         uint32_t, CommonScanGlobalState&) {
    SDB_UNREACHABLE();
  }
  virtual bool OnSegmentsExhausted(duckdb::ClientContext&,
                                   CommonScanGlobalState&, duckdb::DataChunk&) {
    SDB_UNREACHABLE();
  }
  virtual void EmitRowOffsets(CommonScanGlobalState&, const HitsChunk&,
                              duckdb::DataChunk&) {}
};

struct SegDocBufferedScanLocalState : public CommonScanLocalState {
  PrimaryKeyBatch pk_batch;
  std::shared_ptr<IndexSource> index_source;
  size_t current_idx = 0;
  std::unique_ptr<HitBatcher> hit_batcher;
  const SereneDBScanBindData* bind_data = nullptr;

  std::pair<const irs::ColReader*, const irs::ColumnReader*> PkColumnFor(
    const irs::IndexReader& reader, uint32_t seg_idx) {
    if (seg_idx != _pk_col_cached_seg) {
      std::tie(_pk_col_reader, _pk_column) = SegmentPkColumn(reader, seg_idx);
      _pk_col_cached_seg = seg_idx;
    }
    return {_pk_col_reader, _pk_column};
  }

 private:
  const irs::ColReader* _pk_col_reader = nullptr;
  const irs::ColumnReader* _pk_column = nullptr;
  uint32_t _pk_col_cached_seg = std::numeric_limits<uint32_t>::max();
};

void InitCommonState(CommonScanGlobalState& state,
                     duckdb::ClientContext& context,
                     const SereneDBScanBindData& bind_data,
                     duckdb::TableFunctionInitInput& input);

void BuildTableFilter(CommonScanGlobalState& state,
                      duckdb::ClientContext& context,
                      const SereneDBScanBindData& bind_data,
                      const duckdb::TableFilterSet& filters);

// Binds the shared table filter into this thread's ScanFilter.
void InitLocalFilter(CommonScanLocalState& lstate,
                     const CommonScanGlobalState& gstate);

// Enforce `row_expr` (filters over virtual/extract slots) on a materialized
// chunk; slices it to survivors.
inline duckdb::idx_t ApplyRowFilter(duckdb::ClientContext& ctx,
                                    const CommonScanGlobalState& g,
                                    CommonScanLocalState& l,
                                    duckdb::DataChunk& chunk,
                                    duckdb::idx_t count) {
  if (!g.row_expr || count == 0) {
    return count;
  }
  if (!l.row_expr_executor) {
    l.row_expr_executor =
      duckdb::make_uniq<duckdb::ExpressionExecutor>(ctx, *g.row_expr);
    l.row_expr_sel =
      std::make_unique<duckdb::SelectionVector>(STANDARD_VECTOR_SIZE);
  }
  chunk.SetCardinality(count);
  const auto survivors =
    l.row_expr_executor->SelectExpression(chunk, *l.row_expr_sel);
  if (survivors != count) {
    chunk.Slice(*l.row_expr_sel, survivors);
  }
  return survivors;
}

// Decode a pushdown-extract ColumnIndex into its dotted field-path components.
// Struct steps carry a numeric index resolved against `root_type`; variant
// steps carry the field name directly. `column_index` must be a pushdown
// extract with children. Components are appended to `out` and borrow from the
// type/index (valid for the scan's lifetime).
void DecodeExtractPath(const duckdb::ColumnIndex& column_index,
                       const duckdb::LogicalType& root_type,
                       std::vector<std::string_view>& out);

void ClassifyColumnstoreProjections(CommonScanGlobalState& state,
                                    const SereneDBScanBindData& bind_data);

FullScanner* GetOrOpenSegmentFullScanner(CommonScanLocalState& lstate,
                                         const CommonScanGlobalState& gstate,
                                         const irs::IndexReader& reader,
                                         size_t seg_idx);

struct HitsChunk {
  std::span<const irs::doc_id_t> docs;
  std::span<const float> scores;
  std::span<const uint32_t> segs;
  uint32_t fixed_seg = 0;

  size_t size() const noexcept { return docs.size(); }
  uint32_t seg(size_t i) const noexcept {
    return segs.empty() ? fixed_seg : segs[i];
  }
};

inline void SortScoreDocsBySegDoc(std::span<irs::ScoreDoc> hits) {
  std::ranges::sort(hits, [](const irs::ScoreDoc& l, const irs::ScoreDoc& r) {
    return std::pair{l.segment_idx, l.doc} < std::pair{r.segment_idx, r.doc};
  });
}

void CommonScanGetMetrics(duckdb::TableFunctionGetMetricsInput& input);

duckdb::unique_ptr<duckdb::LocalTableFunctionState> CommonScanInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* global_state);

void AccountAndWriteVirtualColumns(CommonScanGlobalState& gstate,
                                   duckdb::idx_t num_rows,
                                   std::span<const float> scores,
                                   duckdb::DataChunk& output);

duckdb::idx_t EmitReadyBatch(duckdb::ClientContext& ctx,
                             CommonScanGlobalState& g,
                             SegDocBufferedScanLocalState& l,
                             duckdb::DataChunk& output);

duckdb::idx_t FinalizeBatch(duckdb::ClientContext& ctx,
                            CommonScanGlobalState& g,
                            SegDocBufferedScanLocalState& l,
                            duckdb::DataChunk& output, duckdb::idx_t collected);

bool EmitBufferedScoreDocs(duckdb::ClientContext& ctx, CommonScanGlobalState& g,
                           SegDocBufferedScanLocalState& l,
                           std::span<const irs::ScoreDoc> hits,
                           size_t& current_idx, duckdb::DataChunk& output);

void RunCollectThenEmitScan(duckdb::ClientContext& ctx,
                            CommonScanGlobalState& g, CommonScanLocalState& l,
                            duckdb::DataChunk& output);

}  // namespace sdb::connector
