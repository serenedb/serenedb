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
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/enums/filter_propagate_result.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/planner/expression.hpp>
#include <duckdb/planner/table_filter_set.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/index/hit_batcher.hpp>
#include <iresearch/types.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace duckdb {

class ExpressionFilter;

}  // namespace duckdb
namespace irs {

class DocIterator;
struct SubReader;

// The pushed table filters of one scan, split by how they are evaluated:
// stored (INCLUDE) columns straight from the segment columnstore with
// zonemap pruning, everything else by fetching the filter columns from the
// row store. Immutable after the scan init builds it; shared by all scan
// threads. Per-thread execution lives in ScanFilter.
class TableFilter {
 public:
  // One pushed filter over an index-stored (INCLUDE) column, evaluated the
  // way duckdb evaluates a pushed ExpressionFilter: zonemap prune via
  // CheckStatistics, then ColumnSegment::FilterSelection over the scanned
  // column vector (per-thread TableFilterState in ScanFilter). is_optional
  // entries are zonemap verdicts only.
  struct ColumnFilter {
    field_id column_id;
    const duckdb::ExpressionFilter* filter;
    // duckdb's optional filter: a hint its inventing operator re-checks;
    // may prune, must never row-reject.
    bool is_optional = false;
    // A dynamic filter's bound tightens mid-scan: never cache or drop its
    // ALWAYS_TRUE verdicts.
    bool is_dynamic = false;
    // The filter evaluated against NULL; decides all-NULL segments and rows
    // past the column's RowCount.
    bool passes_null = false;
  };

  // One pushed filter over a column the index does not store (a lookup
  // column). The emit's single Materialize pass produces the column at
  // `output_slot`; the filter runs there via FilterSelection (no separate
  // row-fetch pass).
  struct RowFetchFilter {
    duckdb::idx_t output_slot;
    duckdb::LogicalType type;
    const duckdb::ExpressionFilter* filter;
    bool passes_null = false;
  };

  void AddColumn(field_id column_id, const duckdb::ExpressionFilter& filter,
                 bool is_optional, bool is_dynamic, bool passes_null);
  void AddRowFetch(duckdb::idx_t output_slot, const duckdb::LogicalType& type,
                   const duckdb::ExpressionFilter& filter, bool passes_null);
  // Sort stored filters cheapest-first; call once after the last Add*.
  void Seal();

  bool Empty() const noexcept { return _columns.empty() && _row_fetch.empty(); }
  bool HasRowFetch() const noexcept { return !_row_fetch.empty(); }

 private:
  friend class ScanFilter;

  std::vector<ColumnFilter> _columns;
  std::vector<RowFetchFilter> _row_fetch;
};

// Per-thread executable filter over one scan's TableFilter. MatchesBatch
// reads ONLY the filter columns; callers materialize survivors afterwards.
// Row-level calls may throw (row fetch hits storage) -- the collector call
// chain is deliberately not noexcept.
class ScanFilter {
 public:
  void Init(const TableFilter& filter) noexcept { _table_filter = &filter; }

  // Segment verdict, in duckdb zonemap terms:
  //   FILTER_ALWAYS_FALSE  no row can pass -- skip the segment
  //   FILTER_ALWAYS_TRUE   every row passes -- scan without per-row checks
  //   NO_PRUNING_POSSIBLE  per-row checks required
  duckdb::FilterPropagateResult StartSegment(duckdb::ClientContext& ctx,
                                             const SubReader& seg);

  // No per-row `.col` check for the current segment. Lookup-column filters are
  // applied later, post-materialize, via FilterLookupColumns.
  bool Empty() const noexcept { return _cols.empty(); }

  // Streaming iterator tier: advance `it`, seeking past row groups whose
  // zonemaps reject the filters.
  doc_id_t NextUnpruned(DocIterator& it);

  // Exact row checks; docs ascending within and across calls per segment
  // (forward-only cursors), docs.size() <= STANDARD_VECTOR_SIZE.
  void MatchesBatch(std::span<const doc_id_t> docs, bool* out);

  // Streaming: MatchesBatch over the chunk, then aligned in-place compaction
  // of hits (+scores when non-empty); returns the survivor count -- the
  // caller shrinks its containers to it.
  duckdb::idx_t FilterHits(std::span<doc_id_t> hits, std::span<float> scores);

  // Apply the lookup-column filters on the materialized output chunk (the
  // columns the emit's single Materialize produced, at each filter's
  // output_slot) via ColumnSegment::FilterSelection, then slice `output` to
  // survivors in place. Returns the survivor count. No separate source fetch.
  duckdb::idx_t FilterLookupColumns(duckdb::DataChunk& output,
                                    duckdb::idx_t count);

 private:
  // Reads one stored column's values for the survivor rows so the pushed
  // filter can be applied to them. Created once per scan thread on first use
  // (chunk is segment-independent); only the cursors re-bind per segment.
  // Verdict-only columns never allocate it.
  struct RowEval {
    RowEval(field_id column_id, const duckdb::LogicalType& type)
      : projection{.output_slot = 0, .column_id = column_id} {
      chunk.Initialize(duckdb::Allocator::DefaultAllocator(),
                       duckdb::vector<duckdb::LogicalType>{type});
      batcher = std::make_unique<sdb::connector::HitBatcher>(
        std::span<const sdb::connector::ColumnstoreProjection>{&projection, 1},
        field_limits::invalid(), /*track_scores=*/false);
    }

    void BindSegment(const ColReader& col_reader) {
      batcher->BeginSegment(0, &col_reader, nullptr);
    }

    // Single-column projection the batcher reads; the batcher keeps a span
    // into it, so it must outlive `batcher` and never move (RowEvals live
    // behind unique_ptr in _evals).
    sdb::connector::ColumnstoreProjection projection;
    std::unique_ptr<sdb::connector::HitBatcher> batcher;
    duckdb::DataChunk chunk;
  };

  struct Column {
    const ColumnReader* reader;
    const TableFilter::ColumnFilter* column_filter;
    BlockWindow window;
    // Zonemap verdict for the row-group window this column is currently
    // positioned on. ALWAYS_TRUE lets the row-level check skip the
    // read+eval entirely.
    duckdb::FilterPropagateResult verdict =
      duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE;
    // Points into _evals; null until this segment's first row-level check
    // binds the cursors.
    RowEval* eval = nullptr;
  };

  duckdb::FilterPropagateResult RgWindowVerdict(Column& col, uint64_t row_pos);
  // When `doc` sits in a row group whose zonemaps prove no row can pass,
  // the first doc past that row group; nullopt when `doc` must be checked.
  std::optional<doc_id_t> SkipTarget(doc_id_t doc);
  void BindEval(Column& col);
  // Materialize `col`'s values for the ascending `rows` via its managed
  // per-column batcher and evaluate the pushed filter into pass[0,
  // rows.size()).
  void ReadAndEval(Column& col, std::span<const uint64_t> rows, bool* pass);
  // Per-thread duckdb filter state (a cached ExpressionExecutor over the
  // pushed ExpressionFilter) that ColumnSegment::FilterSelection runs; lazily
  // built and reused across segments, indexed by filter position.
  duckdb::TableFilterState& ColumnFilterState(size_t filter_idx);
  duckdb::TableFilterState& RowFetchFilterState(size_t filter_idx);

  const TableFilter* _table_filter = nullptr;
  std::vector<Column> _cols;
  // Row position below which SkipTarget cannot prune (nearest window end at
  // the last no-prune answer); reset per segment.
  uint64_t _skip_horizon = 0;
  // Per-filter row-eval state, reused across segments; indexed by the
  // filter's position in the table filter.
  std::vector<std::unique_ptr<RowEval>> _evals;
  duckdb::ClientContext* _ctx = nullptr;
  const ColReader* _col_reader = nullptr;
  duckdb::DataChunk _rf_eval_chunk;
  std::vector<duckdb::unique_ptr<duckdb::TableFilterState>> _col_filter_states;
  std::vector<duckdb::unique_ptr<duckdb::TableFilterState>> _rf_filter_states;
};

}  // namespace irs
