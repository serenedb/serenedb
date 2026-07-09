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

#include "iresearch/index/scan_filter.hpp"

#include <algorithm>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <functional>

#include "basics/assert.h"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/index/pk_batch_helpers.hpp"

namespace irs {
namespace {

// Applies one pushed ExpressionFilter to the single filter column at
// `chunk.data[0]` (n valid rows) exactly the way duckdb applies a pushed
// filter during a scan: ColumnSegment::FilterSelection runs the filter
// state's cached ExpressionExecutor over the whole vector at once and hands
// back the surviving row positions. Writes pass[0, n).
void SelectFilterRows(duckdb::TableFilterState& filter_state,
                      duckdb::DataChunk& chunk, size_t n, bool* pass) {
  chunk.SetCardinality(n);
  // Unset sel -> FilterSelection uses an identity input over [0, n) and
  // rewrites it with the survivors. When the gathered filter column is a
  // dictionary vector (low-cardinality columnstore column), duckdb's
  // ExpressionExecutor evaluates the predicate once over the dictionary
  // entries and maps rows -- the compression-framework dict-level filter,
  // for free, via ColumnSegment::FilterSelection.
  duckdb::SelectionVector sel;
  duckdb::idx_t approved = n;
  duckdb::ColumnSegment::FilterSelection(sel, chunk.data[0], filter_state, n,
                                         approved);
  std::fill_n(pass, n, false);
  for (duckdb::idx_t j = 0; j < approved; ++j) {
    pass[sel.get_index(j)] = true;
  }
}

// Stage ascending row positions `rows` into `batcher` through its batched
// window API -- never the slow per-doc Push -- and hand each ready batch to
// `emit`, which drains it before more rows are staged.
template<typename Emit>
void GatherRows(sdb::connector::HitBatcher& batcher,
                std::span<const uint64_t> rows, Emit&& emit) {
  size_t i = 0;
  for (;;) {
    while (!batcher.Ready() && i < rows.size()) {
      const uint64_t row = rows[i];
      const auto span = batcher.OpenWindow(row);
      if (span == 0) {
        break;
      }
      doc_id_t* const head = batcher.WindowHead();
      const uint64_t win_end = row + span;
      size_t k = 0;
      while (i < rows.size() && rows[i] < win_end) {
        head[k++] = static_cast<doc_id_t>(rows[i] + doc_limits::min());
        ++i;
      }
      batcher.CommitWindow(k);
    }
    if (!batcher.Ready()) {
      if (batcher.Empty()) {
        break;
      }
      batcher.Finalize();
      if (!batcher.Ready()) {
        break;
      }
    }
    emit();
  }
}

}  // namespace

void TableFilter::AddColumn(field_id column_id,
                            const duckdb::ExpressionFilter& filter,
                            bool is_optional, bool is_dynamic,
                            bool passes_null) {
  ColumnFilter column_filter;
  column_filter.column_id = column_id;
  column_filter.filter = &filter;
  column_filter.is_optional = is_optional;
  column_filter.is_dynamic = is_dynamic;
  column_filter.passes_null = is_optional || passes_null;
  _columns.push_back(std::move(column_filter));
}

void TableFilter::AddRowFetch(duckdb::idx_t bind_index,
                              const duckdb::LogicalType& type,
                              const duckdb::ExpressionFilter& filter,
                              bool passes_null) {
  RowFetchFilter rf;
  rf.bind_index = bind_index;
  rf.type = type;
  rf.filter = &filter;
  rf.passes_null = passes_null;
  _row_fetch.push_back(std::move(rf));
}

void TableFilter::Seal() {
  // Verdict-only (optional) filters first: they prune whole segments/row
  // groups from zonemaps with no per-row read, so running them ahead of the
  // read-and-FilterSelection columns lets those see fewer survivors.
  std::stable_sort(_columns.begin(), _columns.end(),
                   [](const ColumnFilter& a, const ColumnFilter& b) {
                     return a.is_optional && !b.is_optional;
                   });
  // Row-fetch filters, as a duckdb TableFilterSet keyed by row-fetch position
  // (matching the RowFetcher source's projection order), for reader-side
  // pushdown by pushdown-capable lookup sources. Copies detach from the pushed
  // ExpressionFilters (which the scan's TableFilterSet owns).
  auto rf_filter_set = duckdb::make_uniq<duckdb::TableFilterSet>();
  bool any_pushable = false;
  for (size_t i = 0; i < _row_fetch.size(); ++i) {
    if (_row_fetch[i].passes_null) {
      continue;
    }
    rf_filter_set->PushFilter(duckdb::ProjectionIndex(i),
                              _row_fetch[i].filter->Copy());
    any_pushable = true;
  }
  if (any_pushable) {
    _rf_filter_set = std::move(rf_filter_set);
  }
}

void RowFetcher::SetSource(std::unique_ptr<sdb::connector::IndexSource> source,
                           duckdb::vector<duckdb::LogicalType> chunk_types) {
  SDB_ASSERT(source);
  _source = std::move(source);
  _pk_batch.kind = _source->PkKind();
  _chunk.Initialize(duckdb::Allocator::DefaultAllocator(), chunk_types);
}

void RowFetcher::StartSegment(const ColReader& col_reader,
                              field_id pk_field_id) {
  if (!_pk_batcher) {
    _pk_batcher = std::make_unique<sdb::connector::HitBatcher>(
      std::span<const sdb::connector::ColumnstoreProjection>{}, pk_field_id,
      /*track_scores=*/false);
  }
  _pk_batcher->BeginSegment(0, &col_reader, nullptr);
}

duckdb::DataChunk& RowFetcher::FetchRows(duckdb::ClientContext& ctx,
                                         std::span<const uint64_t> rows) {
  SDB_ASSERT(_source);
  SDB_ASSERT(_pk_batcher);
  SDB_ASSERT(rows.size() <= STANDARD_VECTOR_SIZE);
  _pk_batch.Reset();
  GatherRows(*_pk_batcher, rows, [&] {
    const auto batch = _pk_batcher->Emit(_sink);
    SDB_ASSERT(batch.pk != nullptr);
    batch.pk->Flatten(batch.count);
    sdb::connector::AppendPrimaryKeysFromVector(_pk_batch, *batch.pk,
                                                batch.count);
  });
  SDB_ASSERT(_pk_batch.Size() == rows.size());
  _chunk.Reset();
  _source->Materialize(ctx, _pk_batch, 0, rows.size(), _chunk);
  return _chunk;
}

duckdb::FilterPropagateResult ScanFilter::StartSegment(
  duckdb::ClientContext& ctx, const SubReader& seg) {
  // Init() always precedes use: every scan local state binds the table
  // filter in its init_local.
  SDB_ASSERT(_table_filter);
  _ctx = &ctx;
  _cols.clear();
  _skip_horizon = 0;
  _col_reader = seg.GetColReader();
  SDB_ASSERT(_col_reader);
  // _columns is pre-sorted cheapest-first; _cols inherits that order.
  for (const auto& column_filter : _table_filter->_columns) {
    const auto* reader = _col_reader->Column(column_filter.column_id);
    if (!reader || reader->DataRgCount() == 0) {
      // An index-stored column with no data in this segment: every row reads
      // as NULL, so the filter's NULL verdict decides the whole segment.
      if (!column_filter.passes_null) {
        _cols.clear();
        return duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE;
      }
      continue;
    }
    switch (column_filter.filter->CheckStatistics(reader->MergedStatistics())) {
      case duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE:
      // FALSE_OR_NULL rejects every row under WHERE semantics just like
      // ALWAYS_FALSE (duckdb's zonemap check prunes both the same way).
      case duckdb::FilterPropagateResult::FILTER_FALSE_OR_NULL:
        // No row in this segment can pass. Safe for optional filters (they
        // may prune) and for dynamic ones (bounds only tighten).
        _cols.clear();
        return duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE;
      case duckdb::FilterPropagateResult::FILTER_ALWAYS_TRUE:
        // A dynamic filter's bound tightens mid-scan: its ALWAYS_TRUE holds
        // only for the current (weakest) bound and may rot, so the column
        // must stay for per-row-group re-checks.
        if (!column_filter.is_dynamic) {
          continue;
        }
        break;
      default:
        break;
    }
    _cols.push_back({.reader = reader, .column_filter = &column_filter});
  }
  if (!_table_filter->_row_fetch.empty()) {
    const auto* pk_col = _col_reader->Column(_table_filter->_pk_column);
    if (!pk_col) {
      _cols.clear();
      return duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE;
    }
    _rows.StartSegment(*_col_reader, _table_filter->_pk_column);
  }
  return Empty() ? duckdb::FilterPropagateResult::FILTER_ALWAYS_TRUE
                 : duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

duckdb::FilterPropagateResult ScanFilter::RgWindowVerdict(Column& col,
                                                          uint64_t row_pos) {
  const bool new_window =
    row_pos < col.window.begin || row_pos >= col.window.end;
  if (new_window) {
    col.window = col.reader->Locate(row_pos, col.window);
    auto verdict = col.column_filter->filter->CheckStatistics(
      col.reader->RowGroupStatistics(col.window.block));
    if (verdict == duckdb::FilterPropagateResult::FILTER_ALWAYS_TRUE &&
        col.column_filter->is_dynamic) {
      // Dynamic filter state tightens mid-scan; never cache an all-pass.
      verdict = duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE;
    }
    if (verdict == duckdb::FilterPropagateResult::FILTER_FALSE_OR_NULL) {
      // Rejects every row under WHERE semantics, same as ALWAYS_FALSE.
      verdict = duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE;
    }
    col.verdict = verdict;
  }
  return col.verdict;
}

doc_id_t ScanFilter::NextUnpruned(DocIterator& it) {
  auto doc = it.advance();
  if (_cols.empty()) {
    return doc;
  }
  while (!doc_limits::eof(doc)) {
    const auto skip_to = SkipTarget(doc);
    if (!skip_to) {
      break;
    }
    doc = it.seek(*skip_to);
  }
  return doc;
}

std::optional<doc_id_t> ScanFilter::SkipTarget(doc_id_t doc) {
  const uint64_t row_pos = doc - doc_limits::min();
  // Verdicts only refresh on window crossings, so below the nearest window
  // end of the last no-prune answer nothing can start pruning.
  if (row_pos < _skip_horizon) {
    return std::nullopt;
  }
  // Every ALWAYS_FALSE column rules out [doc, its window end) on its own,
  // so skip to the furthest such end.
  uint64_t skip_end = 0;
  uint64_t horizon = std::numeric_limits<uint64_t>::max();
  for (auto& col : _cols) {
    if (row_pos >= col.reader->RowCount()) {
      continue;
    }
    if (RgWindowVerdict(col, row_pos) ==
        duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE) {
      if (col.window.end > skip_end) {
        skip_end = col.window.end;
      }
    } else if (col.window.end < horizon) {
      horizon = col.window.end;
    }
  }
  if (skip_end == 0) {
    _skip_horizon = horizon;
    return std::nullopt;
  }
  return skip_end + doc_limits::min();
}

void ScanFilter::BindEval(Column& col) {
  if (_evals.size() < _table_filter->_columns.size()) {
    _evals.resize(_table_filter->_columns.size());
  }
  const size_t fi = col.column_filter - _table_filter->_columns.data();
  auto& slot = _evals[fi];
  if (!slot) {
    slot = std::make_unique<RowEval>(col.column_filter->column_id,
                                     col.reader->Type());
  }
  slot->BindSegment(*_col_reader);
  col.eval = slot.get();
}

duckdb::TableFilterState& ScanFilter::ColumnFilterState(size_t filter_idx) {
  if (_col_filter_states.size() < _table_filter->_columns.size()) {
    _col_filter_states.resize(_table_filter->_columns.size());
  }
  auto& slot = _col_filter_states[filter_idx];
  if (!slot) {
    slot = duckdb::TableFilterState::Initialize(
      *_ctx, *_table_filter->_columns[filter_idx].filter);
  }
  return *slot;
}

duckdb::TableFilterState& ScanFilter::RowFetchFilterState(size_t filter_idx) {
  if (_rf_filter_states.size() < _table_filter->_row_fetch.size()) {
    _rf_filter_states.resize(_table_filter->_row_fetch.size());
  }
  auto& slot = _rf_filter_states[filter_idx];
  if (!slot) {
    slot = duckdb::TableFilterState::Initialize(
      *_ctx, *_table_filter->_row_fetch[filter_idx].filter);
  }
  return *slot;
}

void ScanFilter::ReadAndEval(Column& col, std::span<const uint64_t> rows,
                             bool* pass) {
  auto& eval = *col.eval;
  auto& filter_state =
    ColumnFilterState(col.column_filter - _table_filter->_columns.data());
  size_t done = 0;
  // Each emitted fragment covers the staged rows in order, so its values land
  // contiguously at pass[done, done + count). Reset before every Emit: the
  // gather writes into chunk.data[0], and a fragment can leave it a dictionary
  // vector -- reusing it would resize a non-flat vector. (This is what duckdb
  // does per scan output; it is NOT flattening the values.)
  GatherRows(*eval.batcher, rows, [&] {
    eval.chunk.Reset();
    const auto batch = eval.batcher->Emit(eval.chunk);
    SelectFilterRows(filter_state, eval.chunk, batch.count, pass + done);
    done += batch.count;
  });
  SDB_ASSERT(done == rows.size());
}

void ScanFilter::MatchesBatch(std::span<const doc_id_t> docs, bool* out) {
  const size_t n = docs.size();
  SDB_ASSERT(n <= STANDARD_VECTOR_SIZE);
  std::fill_n(out, n, true);
  uint64_t rowpos[STANDARD_VECTOR_SIZE];
  size_t need[STANDARD_VECTOR_SIZE];
  bool pass[STANDARD_VECTOR_SIZE];
  for (auto& col : _cols) {
    if (&col != &_cols.front() && std::none_of(out, out + n, std::identity{})) {
      return;
    }
    const uint64_t row_count = col.reader->RowCount();
    // Collect the survivors this column must actually read: rows past the
    // column's data resolve by its NULL verdict, and per-block zonemap
    // verdicts reject or wave through whole blocks with no read at all. The
    // ascending survivors go to the managed batcher, which picks dense vs.
    // scattered gather itself.
    size_t nn = 0;
    for (size_t i = 0; i < n; ++i) {
      if (!out[i]) {
        continue;
      }
      const uint64_t rp = docs[i] - doc_limits::min();
      if (rp >= row_count) {
        out[i] = col.column_filter->passes_null;
        continue;
      }
      const auto verdict = RgWindowVerdict(col, rp);
      if (verdict == duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE) {
        out[i] = false;
        continue;
      }
      if (verdict == duckdb::FilterPropagateResult::FILTER_ALWAYS_TRUE ||
          col.column_filter->is_optional) {
        // An optional filter never earns a read: its inventing operator
        // re-checks rows, so past the zonemap verdict it always passes.
        continue;
      }
      rowpos[nn] = rp;
      need[nn] = i;
      ++nn;
    }
    if (nn == 0) {
      continue;
    }
    if (!col.eval) {
      BindEval(col);
    }
    ReadAndEval(col, std::span<const uint64_t>{rowpos, nn}, pass);
    for (size_t j = 0; j < nn; ++j) {
      out[need[j]] = pass[j];
    }
  }
  if (!_table_filter->_row_fetch.empty()) {
    RowFetchBatch(docs, out);
  }
}

void ScanFilter::RowFetchBatch(std::span<const doc_id_t> docs, bool* out) {
  const size_t n = docs.size();
  uint64_t rowpos[STANDARD_VECTOR_SIZE];
  size_t need[STANDARD_VECTOR_SIZE];
  bool pass[STANDARD_VECTOR_SIZE];
  size_t nn = 0;
  for (size_t i = 0; i < n; ++i) {
    if (!out[i]) {
      continue;
    }
    rowpos[nn] = docs[i] - doc_limits::min();
    need[nn] = i;
    ++nn;
  }
  if (nn == 0) {
    return;
  }
  auto& fetched = _rows.FetchRows(*_ctx, std::span<const uint64_t>{rowpos, nn});
  if (_rf_eval_chunk.data.empty()) {
    _rf_eval_chunk.InitializeEmpty(duckdb::vector<duckdb::LogicalType>{
      _table_filter->_row_fetch.front().type});
  }
  for (size_t fi = 0; fi < _table_filter->_row_fetch.size(); ++fi) {
    _rf_eval_chunk.data[0].Reference(fetched.data[fi]);
    SelectFilterRows(RowFetchFilterState(fi), _rf_eval_chunk, nn, pass);
    for (size_t j = 0; j < nn; ++j) {
      out[need[j]] = out[need[j]] && pass[j];
    }
  }
}

duckdb::idx_t ScanFilter::FilterHits(std::span<doc_id_t> hits,
                                     std::span<float> scores) {
  SDB_ASSERT(scores.empty() || scores.size() == hits.size());
  if (hits.empty() || Empty()) {
    return hits.size();
  }
  bool pass[STANDARD_VECTOR_SIZE];
  SDB_ASSERT(hits.size() <= STANDARD_VECTOR_SIZE);
  MatchesBatch(hits, pass);
  size_t w = 0;
  for (size_t i = 0; i < hits.size(); ++i) {
    if (!pass[i]) {
      continue;
    }
    hits[w] = hits[i];
    if (!scores.empty()) {
      scores[w] = scores[i];
    }
    ++w;
  }
  return w;
}

}  // namespace irs
