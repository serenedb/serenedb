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
#include <duckdb/common/types/vector_cache.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/filter/table_filter_functions.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <iresearch/index/index_source.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/search/filter.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/types.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "basics/system-compiler.h"
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

struct IResearchScanGlobalState : public duckdb::GlobalTableFunctionState {
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

  const irs::IndexReader* reader = nullptr;
  size_t total_segments = 0;
  std::atomic_uint32_t next_segment{0};

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
  bool count_only = false;
  bool ts_dict_mode = false;

  // Top-k (ORDER BY score LIMIT k): cross-thread k-th score for WAND pruning.
  std::atomic<irs::score_t> global_kth_score =
    std::numeric_limits<irs::score_t>::lowest();
  bool parallel_topk = false;
  uint32_t rerank_pool = 0;

  // Bulk columnstore fast-path work units (match-all, covered, no filter).
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

  bool BulkChunkEligible() const {
    return has_real_column && !scan_score && !has_external_projections &&
           scan->IsMatchAll() && !scan->EmitOffsets();
  }

  duckdb::idx_t MaxThreads() const final {
    if (count_only && queries.empty()) {
      return 1;
    }
    if (!scan_units.empty()) {
      return std::max<duckdb::idx_t>(1, scan_units.size());
    }
    return std::max<duckdb::idx_t>(1, total_segments);
  }
};

struct IResearchScanLocalState : public duckdb::LocalTableFunctionState {
  bool segments_exhausted = false;
  // Holds the scanned column_ids when output_projection_ids is set; the output
  // then references the projected subset out of this (a reorder, not a copy).
  duckdb::DataChunk scan_chunk;

  irs::PrepareCollector* prepare_collector = nullptr;
  bool prepared = false;

  std::vector<std::unique_ptr<FullScanner>> full_scanners;
};

struct SegDocBufferedScanLocalState : public IResearchScanLocalState {
  PrimaryKeyBatch pk_batch;
  std::shared_ptr<IndexSource> index_source;
  size_t current_idx = 0;
  std::unique_ptr<HitBatcher> hit_batcher;
  const SereneDBScanBindData* bind_data = nullptr;

  // ts_offsets() output state (streaming + top-k paths); empty when not
  // requested.
  std::vector<FieldEntry> offsets_entries;
  std::vector<highlight::HitRange> offsets_doc_scratch;
  uint32_t offsets_prepped_seg = std::numeric_limits<uint32_t>::max();

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

void InitScanState(IResearchScanGlobalState& state,
                   duckdb::ClientContext& context,
                   const SereneDBScanBindData& bind_data,
                   duckdb::TableFunctionInitInput& input);

void BuildTableFilter(IResearchScanGlobalState& state,
                      duckdb::ClientContext& context,
                      const SereneDBScanBindData& bind_data,
                      const duckdb::TableFilterSet& filters);

// Decode a pushdown-extract ColumnIndex into its dotted field-path components.
// Struct steps carry a numeric index resolved against `root_type`; variant
// steps carry the field name directly. `column_index` must be a pushdown
// extract with children. Components are appended to `out` and borrow from the
// type/index (valid for the scan's lifetime).
void DecodeExtractPath(const duckdb::ColumnIndex& column_index,
                       const duckdb::LogicalType& root_type,
                       std::vector<std::string_view>& out);

void ClassifyColumnstoreProjections(IResearchScanGlobalState& state,
                                    const SereneDBScanBindData& bind_data);

FullScanner* GetOrOpenSegmentFullScanner(IResearchScanLocalState& lstate,
                                         const IResearchScanGlobalState& gstate,
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

void IResearchScanGetMetrics(duckdb::TableFunctionGetMetricsInput& input);

// Maps `n` raw per-doc scores to the user-facing value in place (no-op for text
// scorers). Called at the emit boundary so the score-column table filter, any
// WAND threshold and the output vector all operate on one user-facing value.
void ApplyScoreEmit(const IResearchScanGlobalState& gstate, float* scores,
                    duckdb::idx_t n);

void AccountAndWriteVirtualColumns(IResearchScanGlobalState& gstate,
                                   duckdb::idx_t num_rows,
                                   std::span<const float> scores,
                                   duckdb::DataChunk& output);

}  // namespace sdb::connector
