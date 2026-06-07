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

#include <algorithm>
#include <atomic>
#include <duckdb.hpp>
#include <duckdb/function/table_function.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/search/filter.hpp>
#include <iresearch/types.hpp>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "catalog/table_options.h"
#include "connector/columnstore_materializer.h"
#include "connector/index_source.h"
#include "connector/index_source_factory.h"
#include "connector/offsets_collector.hpp"
#include "connector/offsets_writer.hpp"
#include "connector/pk_batch_helpers.h"
#include "connector/search_pk_lookup.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/utilities/transaction.h"

namespace irs {

class IndexReader;

}  // namespace irs
namespace sdb::connector {

struct SereneDBScanBindData;

struct ColumnstoreProjection {
  duckdb::idx_t output_slot;
  catalog::Column::Id column_id;
};

struct CommonScanGlobalState : public duckdb::GlobalTableFunctionState {
  // Isolation: exactly one of txn / snapshot is set.
  rocksdb::Transaction* txn = nullptr;
  const rocksdb::Snapshot* snapshot = nullptr;

  // Maps output column index -> bind_data column index.
  // INVALID_INDEX marks virtual columns (rowid, tableoid, score).
  std::vector<duckdb::idx_t> projected_columns;
  std::vector<duckdb::LogicalType> projected_types;

  // Same shape as `projected_columns` but with INCLUDE'd columnstore
  // slots reset to INVALID_INDEX. Pass this (not `projected_columns`)
  // to `MakeIndexSource` so RocksDB does not double-materialize columns
  // the cs overlay will overwrite. Default: copy of projected_columns
  // (no behavior change until `ClassifyColumnstoreProjections` runs).
  std::vector<duckdb::idx_t> external_projected_columns;

  // INCLUDE'd projections served by the columnstore overlay.
  // Populated by `ClassifyColumnstoreProjections`.
  std::vector<ColumnstoreProjection> cs_projections;
  // Parallel arrays derived from `cs_projections` once at init time --
  // bulk + streaming + score-ordered materializer paths all want them
  // in this shape and would otherwise rebuild per batch.
  std::vector<irs::field_id> cs_field_ids;
  std::vector<duckdb::idx_t> cs_output_slots;
  // True iff at least one projected real column is NOT an INCLUDE
  // column (i.e. IndexSource still has work to do).
  bool has_external_projections = false;

  // Rowid virtual column
  bool has_generated_pk = false;
  bool scan_rowid = false;
  duckdb::idx_t rowid_output_idx = 0;

  // Tableoid virtual column
  bool scan_tableoid = false;
  duckdb::idx_t tableoid_output_idx = 0;
  int64_t tableoid_value = 0;

  // Score virtual column (set only for search scans with BM25/TFIDF scorer)
  bool scan_score = false;
  duckdb::idx_t score_output_idx = 0;

  bool finished = false;

  const irs::IndexReader* reader = nullptr;
  size_t total_segments = 0;
  std::atomic_uint32_t next_segment{0};

  std::atomic<duckdb::idx_t> produced_rows{0};

  // Cached IndexSource adapter for the table being scanned: holds whatever
  // bind/session state the per-source materialiser needs (file-backed: a
  // bound MultiFileBindData + lookup gstate; rocksdb: small projection +
  // snapshot/txn). Lazy-built via MakeIndexSource on first use; reused
  // across batches.
  // shared_ptr (not unique_ptr) so derived-state destructors don't need
  // IndexSource's complete type -- the deleter is type-erased.
  std::shared_ptr<IndexSource> index_source;

  duckdb::idx_t MaxThreads() const override { return 1; }

  ~CommonScanGlobalState() override;
};

struct CommonScanLocalState : public duckdb::LocalTableFunctionState {
  bool segments_exhausted = false;

  std::vector<std::unique_ptr<ColumnstoreMaterializer>> cs_materializers;
};

struct SegDocBufferedScanLocalState : public CommonScanLocalState {
  PrimaryKeyBatch pk_batch;
  std::shared_ptr<IndexSource> index_source;
  PkLookupBuffers pk_lookup;
  size_t current_idx = 0;
  const SereneDBScanBindData* bind_data = nullptr;
};

struct PKScanGlobalState : public CommonScanGlobalState {
  std::string upper_bound_data;
  std::vector<rocksdb::Slice> upper_bound_slices;
};

void InitCommonState(CommonScanGlobalState& state,
                     duckdb::ClientContext& context,
                     const SereneDBScanBindData& bind_data,
                     duckdb::TableFunctionInitInput& input);

void ClassifyColumnstoreProjections(CommonScanGlobalState& state,
                                    const SereneDBScanBindData& bind_data);

ColumnstoreMaterializer* GetOrOpenSegmentMaterializer(
  CommonScanLocalState& lstate, const CommonScanGlobalState& gstate,
  const irs::IndexReader& reader, size_t seg_idx);

struct ScoreDocsView {
  std::span<const irs::ScoreDoc> hits;

  size_t size() const noexcept { return hits.size(); }
  irs::doc_id_t doc(size_t i) const noexcept { return hits[i].doc; }
  uint32_t seg(size_t i) const noexcept { return hits[i].segment_idx; }
  float score(size_t i) const noexcept { return hits[i].score; }
  uint64_t operator[](size_t i) const noexcept {
    return doc(i) - irs::doc_limits::min();
  }
  bool IsSorted() const noexcept {
    for (size_t i = 1; i < size(); ++i) {
      if ((*this)[i] < (*this)[i - 1]) {
        return false;
      }
    }
    return true;
  }
  ScoreDocsView subview(size_t off, size_t n) const noexcept {
    return {hits.subspan(off, n)};
  }
};

struct StreamingHitsView {
  std::span<const irs::doc_id_t> docs;
  std::span<const float> scores;  // empty if scan_score is false
  uint32_t fixed_seg = 0;

  size_t size() const noexcept { return docs.size(); }
  irs::doc_id_t doc(size_t i) const noexcept { return docs[i]; }
  uint32_t seg(size_t /*i*/) const noexcept { return fixed_seg; }
  float score(size_t i) const noexcept { return scores[i]; }
  uint64_t operator[](size_t i) const noexcept {
    return doc(i) - irs::doc_limits::min();
  }
  bool IsSorted() const noexcept {
    for (size_t i = 1; i < size(); ++i) {
      if ((*this)[i] < (*this)[i - 1]) {
        return false;
      }
    }
    return true;
  }
  StreamingHitsView subview(size_t off, size_t n) const noexcept {
    return {docs.subspan(off, n),
            scores.empty() ? std::span<const float>{} : scores.subspan(off, n),
            fixed_seg};
  }
};

template<class View, class F>
void ForEachSegmentRun(const View& view, F&& body) {
  size_t i = 0;
  while (i < view.size()) {
    const uint32_t seg_id = view.seg(i);
    const size_t begin = i;
    while (i < view.size() && view.seg(i) == seg_id) {
      ++i;
    }
    body(seg_id, begin, view.subview(begin, i - begin));
  }
}

template<class View>
void MaterializeIncludeColumnsBatched(CommonScanLocalState& lstate,
                                      const CommonScanGlobalState& gstate,
                                      const irs::IndexReader& reader,
                                      const View& view,
                                      duckdb::DataChunk& output) {
  if (view.size() == 0 || gstate.cs_projections.empty()) {
    return;
  }
  ForEachSegmentRun(view, [&](uint32_t seg_id, size_t begin, auto slice) {
    auto* mat = GetOrOpenSegmentMaterializer(lstate, gstate, reader, seg_id);
    if (mat && mat->HasAny()) {
      mat->SelectByDocIds(slice, output, begin);
    }
  });
}

// Read generated PK int64 values from RocksDB iterator keys into output.
// Key format: [ObjectId(8)][ColumnId(8)][PK int64 big-endian XOR 0x80].
duckdb::idx_t ReadGeneratedPKFromKeys(rocksdb::Iterator& it,
                                      duckdb::Vector& output,
                                      duckdb::idx_t max_rows);

// DuckDB get_metrics callback: writes produced_rows from CommonScanGlobalState
// into input.operator_metrics.rows_scanned.
void CommonScanGetMetrics(duckdb::TableFunctionGetMetricsInput& input);

// DuckDB init_local callback: creates an empty CommonScanLocalState.
duckdb::unique_ptr<duckdb::LocalTableFunctionState> CommonScanInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* global_state);

template<class View>
void WriteVirtualColumns(CommonScanGlobalState& gstate, duckdb::idx_t row_base,
                         duckdb::idx_t num_rows, const View& view,
                         duckdb::DataChunk& output) {
  SDB_ASSERT(!gstate.scan_score || view.size() == num_rows);
  for (duckdb::idx_t proj = 0; proj < gstate.projected_columns.size(); ++proj) {
    if (gstate.projected_columns[proj] != duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    if (gstate.scan_tableoid && proj == gstate.tableoid_output_idx) {
      output.data[proj].Reference(duckdb::Value::BIGINT(gstate.tableoid_value),
                                  duckdb::count_t(num_rows));
    } else if (gstate.scan_score && proj == gstate.score_output_idx) {
      auto* score_data =
        duckdb::FlatVector::GetDataMutable<float>(output.data[proj]);
      for (duckdb::idx_t i = 0; i < num_rows; ++i) {
        score_data[i] = view.score(i);
      }
    } else if (gstate.scan_rowid && proj == gstate.rowid_output_idx) {
      auto* data =
        duckdb::FlatVector::GetDataMutable<int64_t>(output.data[proj]);
      for (duckdb::idx_t i = 0; i < num_rows; ++i) {
        data[i] = row_base + i;
      }
    }
  }
}

template<class Lstate, class View>
void WriteChunkOffsets(Lstate& lstate, const irs::Filter::Query& query,
                       const irs::IndexReader& reader, const View& view,
                       duckdb::DataChunk& output) {
  for (const auto& entry : lstate.offsets_entries) {
    auto& list_vec = output.data[entry.output_idx];
    list_vec.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    duckdb::ListVector::SetListSize(list_vec, 0);
    auto& child = duckdb::ListVector::GetChildMutable(list_vec);
    child.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  }
  for (size_t i = 0; i < view.size(); ++i) {
    const uint32_t seg_idx = view.seg(i);
    if (seg_idx != lstate.offsets_prepped_seg) {
      for (auto& entry : lstate.offsets_entries) {
        entry.state.Clear();
      }
      OffsetsCollector visitor{lstate.offsets_entries};
      query.visit(reader[seg_idx], visitor, irs::kNoBoost);
      lstate.offsets_prepped_seg = seg_idx;
    }
    const auto& seg = reader[seg_idx];
    for (auto& entry : lstate.offsets_entries) {
      FillRowOffsets(entry.state, seg, view.doc(i), entry.limit,
                     lstate.offsets_doc_scratch);
      WriteRowOffsets(output.data[entry.output_idx],
                      static_cast<duckdb::idx_t>(i),
                      lstate.offsets_doc_scratch);
    }
  }
}

template<class Pk, class Lstate, class Gstate, class View>
void ReadIResearchSegments(Pk& pk, Lstate& l, Gstate& g, const View& view,
                           bool include_cs, duckdb::DataChunk& output) {
  ForEachSegmentRun(view, [&](uint32_t seg_id, size_t begin, auto slice) {
    SDB_ASSERT(slice.size() <= STANDARD_VECTOR_SIZE);

    const auto [cs_reader, pk_col] = SegmentPkColumn(*g.reader, seg_id);
    SDB_ASSERT(pk_col);
    if (!l.pk_lookup.seg_pk_vec) {
      l.pk_lookup.seg_pk_vec = std::make_unique<duckdb::Vector>(
        duckdb::LogicalType::BLOB, STANDARD_VECTOR_SIZE);
    }
    if (!l.pk_lookup.fetcher) {
      l.pk_lookup.fetcher =
        std::make_unique<SegmentPkSequentialFetcher>(*cs_reader, *pk_col);
    } else if (l.pk_lookup.last_seg_idx != seg_id) {
      l.pk_lookup.fetcher->Reset(*cs_reader, *pk_col);
    }
    l.pk_lookup.last_seg_idx = seg_id;
    l.pk_lookup.fetcher->Fetch(slice, *l.pk_lookup.seg_pk_vec);
    auto* data =
      duckdb::FlatVector::GetData<duckdb::string_t>(*l.pk_lookup.seg_pk_vec);
    // TODO: we can probably remove this cycle by removing redundant buffer.
    for (size_t k = 0; k < slice.size(); ++k) {
      std::string_view bytes{data[k].GetData(), data[k].GetSize()};
      SetPrimaryKey(pk, begin + k, bytes);
    }

    if (include_cs) {
      auto* mat = GetOrOpenSegmentMaterializer(l, g, *g.reader, seg_id);
      if (mat && mat->HasAny()) {
        mat->SelectByDocIds(slice, output, begin);
      }
    }
  });
}

template<class Gstate, class Lstate, class View>
duckdb::idx_t MaterializeChunk(duckdb::ClientContext& ctx, Gstate& g, Lstate& l,
                               const View& view, duckdb::DataChunk& output) {
  const auto num_rows = static_cast<duckdb::idx_t>(view.size());
  if (num_rows == 0) {
    return 0;
  }
  const bool need_cs = !g.cs_projections.empty();
  if (g.has_external_projections) {
    SDB_ASSERT(l.bind_data);
    if (!l.index_source) {
      l.index_source = MakeIndexSource(
        ctx, *l.bind_data, g.snapshot, g.txn, g.external_projected_columns,
        g.projected_types, l.bind_data->column_ids);
    }
    if (std::holds_alternative<std::monostate>(l.pk_batch)) {
      l.pk_batch = l.index_source->CreatePkBatch();
    }
    std::visit(
      [&](auto& pk) {
        using T = std::decay_t<decltype(pk)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          SDB_ASSERT(false, "pk_batch must be initialised");
        } else {
          pk.Reset();
          if constexpr (std::is_same_v<T, PrimaryKeysBytes>) {
            pk.EnsureInit(duckdb::Allocator::Get(ctx));
          }
          PkResize(pk, num_rows);
          ReadIResearchSegments(pk, l, g, view, need_cs, output);
        }
      },
      l.pk_batch);
    l.index_source->Materialize(ctx, l.pk_batch, 0, num_rows, output);
  } else if (need_cs) {
    MaterializeIncludeColumnsBatched(l, g, *g.reader, view, output);
  }
  if constexpr (requires { l.offsets_entries; }) {
    if (!l.offsets_entries.empty()) {
      SDB_ASSERT(g.query);
      WriteChunkOffsets(l, *g.query, *g.reader, view, output);
    }
  }

  const auto row_base =
    g.produced_rows.fetch_add(num_rows, std::memory_order_relaxed);
  WriteVirtualColumns(g, row_base, num_rows, view, output);
  output.SetChildCardinality(num_rows);
  return num_rows;
}

template<class Gstate, class Lstate>
void RunCollectThenEmitScan(duckdb::ClientContext& ctx, Gstate& g, Lstate& l,
                            duckdb::DataChunk& output) {
  while (!l.segments_exhausted) {
    const auto seg = g.next_segment.fetch_add(1, std::memory_order_relaxed);
    if (seg < g.total_segments) {
      SDB_ASSERT((*g.reader)[seg].live_docs_count() != 0);
      l.OnSegment(ctx, (*g.reader)[seg], seg, g);
    } else {
      l.segments_exhausted = true;
    }
  }
  if (l.OnSegmentsExhausted(ctx, g, output)) {
    return;
  }
  output.SetChildCardinality(0);
}

template<class Gstate, class Lstate>
void RunStreamingScan(duckdb::ClientContext& ctx, Gstate& g, Lstate& l,
                      duckdb::DataChunk& output) {
  while (true) {
    if (l.EmitNextChunk(ctx, g, output)) {
      return;
    }
    const auto seg = g.next_segment.fetch_add(1, std::memory_order_relaxed);
    if (seg >= g.total_segments) {
      output.SetChildCardinality(0);
      return;
    }
    SDB_ASSERT((*g.reader)[seg].live_docs_count() != 0);
    l.OnSegment(ctx, (*g.reader)[seg], seg, g);
  }
}

// Builds the scan column list and populates state.upper_bound_data and
// state.upper_bound_slices. Must be called after InitCommonState.
// Returns per-column prefix keys (one per scan column, same order).
std::vector<std::string> InitPKScanColumns(
  PKScanGlobalState& state, const SereneDBScanBindData& bind_data);

// Shared scan body for PK full-scan and range-scan functions.
// `iterators` is passed explicitly (not stored in PKScanGlobalState) so each
// derived state can declare it last and guarantee correct destruction order.
void PKScanFunctionImpl(
  CommonScanGlobalState& gstate,
  std::vector<std::unique_ptr<rocksdb::Iterator>>& iterators,
  duckdb::DataChunk& output);

}  // namespace sdb::connector
