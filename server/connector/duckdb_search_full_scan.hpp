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

#include <algorithm>
#include <atomic>
#include <duckdb.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/filter/table_filter_functions.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/storage/table/row_group_reorderer.hpp>
#include <iresearch/index/column_extract.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/search/filter.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/types.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include "connector/duckdb_table_function.h"

namespace irs {

class IndexReader;

}  // namespace irs
namespace sdb::connector {

struct SereneDBScanBindData;

// How the scan executes, decided once in IResearchScanInitGlobal
// (DecideScanMode): the fastest mode that can apply every pushed filter.
enum class ScanMode : uint8_t {
  // ts_dict_agg() term enumeration.
  TsDict,
  // count(*) with no predicate and no pushed filters: the whole-reader
  // live_docs_count answers without touching a segment.
  CountFast,
  // count(*): per-segment count() -- whole-file statistics settle or kill the
  // pushed `.col` filters, the rest apply through the TableFilterDocIterator
  // wrapper. Score/lookup filters cannot run here (no scoring, no source
  // materialization), so their plans take Stream instead.
  Count,
  // ORDER BY score LIMIT k: parallel top-k collectors.
  TopK,
  // Match-all with every needed column covered (the FullScanner case): bulk
  // work units read `.col` directly; a segment with deletes falls back to the
  // masked streaming walk. Never scores, never touches the lookup source.
  ColScan,
  // Streaming DocIterator -> HitBatcher (WAND-seeded when eligible). The only
  // mode that materializes through the lookup source, engaged if and only if
  // a lookup column is needed -- for a filter or for the output.
  Stream,
};

struct IResearchScanGlobalState : public duckdb::GlobalTableFunctionState {
  std::vector<duckdb::idx_t> projected_columns;
  std::vector<duckdb::LogicalType> projected_types;
  std::vector<duckdb::ColumnIndex> projected_column_indexes;
  duckdb::ClientContext* client_context = nullptr;

  // Disposition of the scanned real columns (ClassifyColumnstoreProjections):
  // covered columns materialize from `.col` (`cs_projections`), the rest stay
  // in `lookup_projected_columns` for the lookup source. `needs_lookup` is set
  // if and only if a lookup column is needed -- in the output or by a pushed
  // filter (the source applies it natively during materialization); a column
  // needed by neither (left dangling by a statistics-eliminated filter) is
  // read nowhere.
  std::vector<duckdb::idx_t> lookup_projected_columns;
  std::vector<ColumnstoreProjection> cs_projections;
  bool needs_lookup = false;
  bool has_real_column = false;
  // Any output column that emits values (not the empty virtual column): false
  // means the scan only reports row counts (count(*) shapes).
  bool has_output_column = false;

  duckdb::idx_t tableoid_output_idx = duckdb::DConstants::INVALID_INDEX;
  int64_t tableoid_value = 0;

  bool scan_score = false;
  duckdb::idx_t score_output_idx = 0;
  const VectorScorerOptions* vector_scorer = nullptr;

  // Output slot for a search-table generated PK (rowid), materialized from
  // `.col` as a covered column; INVALID_INDEX when not projected.
  duckdb::idx_t generated_pk_output_idx = duckdb::DConstants::INVALID_INDEX;

  const irs::IndexReader* reader = nullptr;
  size_t total_segments = 0;
  std::atomic_uint32_t next_segment{0};
  // The claimable segments: claimed slots in [0, claimable_segments) map
  // through `segment_order` -- empty = identity over all segments. Init-time
  // whole-file classification against the static pushed filters shrinks the
  // list to survivors (a dynamic bound is still uninitialized at init, so it
  // classifies NO_PRUNING and never excludes; survivors still classify at
  // claim, where dynamic bounds have tightened), and ORDER BY <covered column>
  // LIMIT (bind_data.scan_order) permutes it best-first (scheduling only --
  // the TopN above still sorts). The scorer prepare phase walks every segment
  // regardless (corpus-level term stats).
  std::vector<uint32_t> segment_order;
  uint32_t claimable_segments = 0;

  uint32_t SegmentAt(uint32_t claimed) const {
    return segment_order.empty() ? claimed : segment_order[claimed];
  }

  std::atomic<duckdb::idx_t> produced_rows{0};

  // The scan's pushed filters (as duckdb hands them to us), forwarded verbatim
  // to the lookup source so its native scan evaluates lookup-column filters
  // (FilterSelection + late materialization). Lives for the query.
  const duckdb::TableFilterSet* pushed_filters = nullptr;
  // A pushed filter targets a lookup (source-only) column: it can only be
  // applied during the source lookup, so it forbids the fast collector/bulk
  // paths (which never run the lookup per candidate) -- forces streaming.
  bool has_lookup_filter = false;

  // Covered (INCLUDE'd) `.col` filters, applied in-scan by wrapping the search
  // DocIterator in a TableFilterDocIterator (codec Filter + zonemap). `field`
  // keys the segment columnstore; `filter` is the pushed ExpressionFilter.
  // Empty => no `.col` filtering, no wrapper, zero cost.
  struct ColFilter {
    irs::field_id field;
    const duckdb::TableFilter* filter;
    // A filter on the computed score column (not a `.col` field): applied on
    // the score vector after scoring instead of via the columnstore codec.
    bool is_score = false;
    // Per-filter invariants, computed once at pushdown (see ColFilterSpec).
    bool is_dynamic = false;
    bool zonemap_only = false;
    irs::NullCheckKind null_check = irs::NullCheckKind::None;
    // The filtered column's type (a segment lacking the column classifies by
    // evaluating the filter on a NULL of this type).
    duckdb::LogicalType type;
    // IS NOT NULL replacement for segments whose statistics say TRUE_OR_NULL
    // (every non-null row passes) -- built once at pushdown, duckdb
    // propagate_get-style. Null when the filter shape can't be replaced.
    duckdb::unique_ptr<duckdb::TableFilter> not_null;
  };
  std::vector<ColFilter> col_filters;

  // Streaming text-score WAND. `score_dynamic_filter` is the shared runtime
  // bound TOP_N updates (captured from the pushed dynamic score filter; null
  // when none). When `wand_streaming`, the streaming DocIterator runs with WAND
  // and its ScoreThresholdAttr is seeded from that bound before each emit, so
  // below-threshold blocks are skipped -- the HitBatcher score filter still
  // enforces the exact boundary on the docs that are produced.
  duckdb::shared_ptr<duckdb::DynamicFilterData> score_dynamic_filter;
  bool wand_streaming = false;
  // Lower bound implied by pushed static score filters (`score > c`), Lucene
  // min_score-style: seeds the WAND threshold (streaming) and the top-k
  // collector so below-bound blocks are skipped from the first window; the
  // pushed filter still enforces the exact bound. lowest() = no bound.
  float score_static_floor = std::numeric_limits<float>::lowest();

  // filter_prune: when set, the scanned column_ids include filter-only columns
  // the output must not emit. These are indexes into the scanned columns that
  // form the output (a reorder/narrow); empty = emit the scanned columns as-is.
  duckdb::vector<duckdb::idx_t> output_projection_ids;

  // The search predicate (`@@` / vector query), the scorer, and the scan-source
  // shape. `owned_filter` backs `filter` for vector/match-all queries.
  const irs::Filter* filter = nullptr;
  irs::Filter::ptr owned_filter;
  std::unique_ptr<irs::Scorer> scorer_obj;
  const SereneDBScanBindData* scan = nullptr;
  ScanMode mode = ScanMode::Stream;

  // Top-k (ORDER BY score LIMIT k): cross-thread k-th score for WAND pruning.
  std::atomic<irs::score_t> global_kth_score =
    std::numeric_limits<irs::score_t>::lowest();
  uint32_t rerank_pool = 0;

  // ColScan work units: `bulk` slices of all-live segments read `.col`
  // directly; a segment with deletes becomes one non-bulk unit taking the
  // masked streaming walk.
  struct ScanUnit {
    uint32_t seg;
    uint64_t begin;
    uint64_t count;
    bool bulk;
  };
  std::vector<ScanUnit> scan_units;
  std::atomic_uint32_t next_unit{0};

  // Prepare phase: per-segment queries + merged term statistics for scoring.
  std::vector<irs::PrepareCollector::ptr> collectors;
  std::vector<irs::QueryBuilder::ptr> queries;
  std::optional<irs::StatsBuffer> stats;
  absl::Notification prepare_finished;
  std::atomic_uint32_t prepare_segment = 0;
  std::atomic_uint32_t prepare_count = 0;
  std::atomic_uint32_t collector_slots = 0;

  duckdb::idx_t MaxThreads() const final {
    switch (mode) {
      case ScanMode::CountFast:
        return 1;
      case ScanMode::ColScan:
        return std::max<duckdb::idx_t>(1, scan_units.size());
      default:
        // The scorer prepare phase walks every segment (corpus-level term
        // statistics), even ones the whole-file classification excluded.
        return std::max<duckdb::idx_t>(
          1, scorer_obj ? total_segments : claimable_segments);
    }
  }
};

// Lower bound implied by a static score filter (`score > c` / `score >= c`,
// AND-conjunctions take the max): the largest float T such that every score
// passing the filter exceeds T. `exact` is set when the whole expression IS
// that bound, so enforcing `score > T` replaces evaluating the filter.
// lowest() when the expression implies no usable lower bound.
float StaticScoreFloor(const duckdb::Expression& expr, bool& exact);

// Decode a pushdown-extract ColumnIndex into its dotted field-path components.
// Struct steps carry a numeric index resolved against `root_type`; variant
// steps carry the field name directly. `column_index` must be a pushdown
// extract with children. Components are appended to `out` and borrow from the
// type/index (valid for the scan's lifetime).
void DecodeExtractPath(const duckdb::ColumnIndex& column_index,
                       const duckdb::LogicalType& root_type,
                       std::vector<std::string_view>& out);

void IResearchScanGetMetrics(duckdb::TableFunctionGetMetricsInput& input);

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> IResearchScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input);

duckdb::unique_ptr<duckdb::LocalTableFunctionState> IResearchScanInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* global_state);

void IResearchScanFunction(duckdb::ClientContext& context,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output);

void IResearchSetScanOrder(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::RowGroupOrderOptions> options,
  duckdb::optional_ptr<duckdb::FunctionData> bind_data);

}  // namespace sdb::connector
