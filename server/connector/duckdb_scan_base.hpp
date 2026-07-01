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
#include <duckdb/function/table_function.hpp>
#include <iresearch/index/iterators.hpp>
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
#include "connector/columnstore_materializer.h"
#include "connector/index_source.h"
#include "connector/index_source_factory.h"
#include "connector/offsets_collector.hpp"
#include "connector/offsets_writer.hpp"
#include "connector/pk_batch_helpers.h"
#include "connector/search_pk_lookup.h"

namespace irs {

class IndexReader;

}  // namespace irs
namespace sdb::connector {

struct SereneDBScanBindData;

// Common state inherited by all per-scan global states.
// Holds the fields shared across every scan strategy: isolation context,
// projection mapping, virtual column metadata, and the termination flag.
struct CommonScanGlobalState : public duckdb::GlobalTableFunctionState {
  // Maps output column index -> bind_data column index.
  // INVALID_INDEX marks virtual columns (rowid, tableoid, score).
  std::vector<duckdb::idx_t> projected_columns;
  std::vector<duckdb::LogicalType> projected_types;
  std::vector<duckdb::ColumnIndex> projected_column_indexes;
  duckdb::ClientContext* client_context = nullptr;

  // Same shape as `projected_columns` but with INCLUDE'd columnstore
  // slots reset to INVALID_INDEX. Pass this (not `projected_columns`)
  // to `MakeIndexSource` so the source does not double-materialize columns
  // the cs overlay will overwrite. Default: copy of projected_columns
  // (no behavior change until `ClassifyColumnstoreProjections` runs).
  std::vector<duckdb::idx_t> external_projected_columns;

  // INCLUDE'd projections served by the columnstore overlay.
  // Populated by `ClassifyColumnstoreProjections`.
  std::vector<ColumnstoreProjection> cs_projections;
  // True iff at least one projected real column is NOT an INCLUDE
  // column (i.e. IndexSource still has work to do).
  bool has_external_projections = false;
  bool has_real_column = false;

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

  duckdb::idx_t MaxThreads() const override { return 1; }
};

struct CommonScanLocalState : public duckdb::LocalTableFunctionState {
  bool segments_exhausted = false;

  irs::PrepareCollector* prepare_collector = nullptr;
  bool prepared = false;

  std::vector<std::unique_ptr<ColumnstoreMaterializer>> cs_materializers;
};

struct SegDocBufferedScanLocalState : public CommonScanLocalState {
  PrimaryKeyBatch pk_batch;
  std::shared_ptr<IndexSource> index_source;
  duckdb::VectorCache pk_vec_cache;
  PkLookupBuffers pk_lookup;
  size_t current_idx = 0;
  const SereneDBScanBindData* bind_data = nullptr;

  std::pair<const irs::ColReader*, const irs::ColumnReader*> PkColumnFor(
    const irs::IndexReader& reader, uint32_t seg_idx) {
    if (seg_idx != pk_col_cached_seg) {
      std::tie(pk_col_reader, pk_column) = SegmentPkColumn(reader, seg_idx);
      pk_col_cached_seg = seg_idx;
    }
    return {pk_col_reader, pk_column};
  }

 private:
  const irs::ColReader* pk_col_reader = nullptr;
  const irs::ColumnReader* pk_column = nullptr;
  uint32_t pk_col_cached_seg = std::numeric_limits<uint32_t>::max();
};

void InitCommonState(CommonScanGlobalState& state,
                     duckdb::ClientContext& context,
                     const SereneDBScanBindData& bind_data,
                     duckdb::TableFunctionInitInput& input);

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
  ScoreDocsView subview(size_t off, size_t n) const noexcept {
    return {hits.subspan(off, n)};
  }
};

inline void SortScoreDocsBySegDoc(std::span<irs::ScoreDoc> hits) {
  std::ranges::sort(hits, [](const irs::ScoreDoc& l, const irs::ScoreDoc& r) {
    return std::pair{l.segment_idx, l.doc} < std::pair{r.segment_idx, r.doc};
  });
}

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
  StreamingHitsView subview(size_t off, size_t n) const noexcept {
    return {docs.subspan(off, n),
            scores.empty() ? std::span<const float>{} : scores.subspan(off, n),
            fixed_seg};
  }
};

template<typename View, typename F>
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

// DuckDB get_metrics callback: writes produced_rows from CommonScanGlobalState
// into input.operator_metrics.rows_scanned.
void CommonScanGetMetrics(duckdb::TableFunctionGetMetricsInput& input);

// DuckDB init_local callback: creates an empty CommonScanLocalState.
duckdb::unique_ptr<duckdb::LocalTableFunctionState> CommonScanInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* global_state);

template<typename View>
void WriteVirtualColumns(CommonScanGlobalState& gstate, duckdb::idx_t num_rows,
                         const View& view, duckdb::DataChunk& output,
                         duckdb::idx_t output_start) {
  if (gstate.scan_tableoid) {
    auto* tableoid_data = duckdb::FlatVector::GetDataMutable<int64_t>(
      output.data[gstate.tableoid_output_idx]);
    for (duckdb::idx_t i = 0; i < num_rows; ++i) {
      tableoid_data[output_start + i] = gstate.tableoid_value;
    }
  }
  SDB_ASSERT(!gstate.scan_score || view.size() == num_rows);
  if (!gstate.scan_score) {
    return;
  }
  auto* score_data = duckdb::FlatVector::GetDataMutable<float>(
    output.data[gstate.score_output_idx]);
  for (duckdb::idx_t i = 0; i < num_rows; ++i) {
    score_data[output_start + i] = view.score(i);
  }
}

template<typename View>
void AccountAndWriteVirtualColumns(CommonScanGlobalState& gstate,
                                   duckdb::idx_t num_rows, const View& view,
                                   duckdb::DataChunk& output,
                                   duckdb::idx_t output_start) {
  gstate.produced_rows.fetch_add(num_rows, std::memory_order_relaxed);
  WriteVirtualColumns(gstate, num_rows, view, output, output_start);
}

template<typename Lstate, typename Gstate, typename View>
void WriteChunkOffsets(Lstate& lstate, const Gstate& g,
                       const irs::IndexReader& reader, const View& view,
                       duckdb::DataChunk& output, duckdb::idx_t output_start) {
  if (output_start == 0) {
    for (const auto& entry : lstate.offsets_entries) {
      auto& list_vec = output.data[entry.output_idx];
      list_vec.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
      duckdb::ListVector::SetListSize(list_vec, 0);
      auto& child = duckdb::ListVector::GetChildMutable(list_vec);
      child.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    }
  }
  const irs::SubReader* cached_seg = nullptr;
  for (size_t i = 0; i < view.size(); ++i) {
    const uint32_t seg_idx = view.seg(i);
    if (seg_idx != lstate.offsets_prepped_seg) {
      for (auto& entry : lstate.offsets_entries) {
        entry.state.Clear();
      }
      cached_seg = &reader[seg_idx];
      OffsetsCollector visitor{lstate.offsets_entries};
      const auto& seg_query = g.queries[seg_idx];
      SDB_ASSERT(seg_query);
      seg_query->Visit(visitor, irs::kNoBoost);
      lstate.offsets_prepped_seg = seg_idx;
    }
    for (auto& entry : lstate.offsets_entries) {
      FillRowOffsets(entry.state, *cached_seg, view.doc(i), entry.limit,
                     lstate.offsets_doc_scratch);
      WriteRowOffsets(output.data[entry.output_idx],
                      output_start + static_cast<duckdb::idx_t>(i),
                      lstate.offsets_doc_scratch);
    }
  }
}

template<typename Lstate, typename Gstate, typename View>
void ReadIResearchSegments(Lstate& l, Gstate& g, const View& view,
                           bool include_cs, duckdb::DataChunk& output,
                           duckdb::idx_t output_start) {
  ForEachSegmentRun(view, [&](uint32_t seg_id, size_t begin, auto slice) {
    SDB_ASSERT(slice.size() <= STANDARD_VECTOR_SIZE);

    std::visit(
      absl::Overload{
        [](std::monostate) {},
        [&](auto& pk) {
          if (!l.pk_lookup.seg_pk_vec) {
            l.pk_vec_cache =
              duckdb::VectorCache(duckdb::Allocator::Get(*g.client_context),
                                  duckdb::LogicalType::BLOB);
            l.pk_lookup.seg_pk_vec =
              std::make_unique<duckdb::Vector>(l.pk_vec_cache);
          }
          if (!l.pk_lookup.fetcher || l.pk_lookup.last_seg_idx != seg_id) {
            const auto [col_reader, pk_col] = l.PkColumnFor(*g.reader, seg_id);
            SDB_ASSERT(pk_col);
            if (!l.pk_lookup.fetcher) {
              l.pk_lookup.fetcher =
                std::make_unique<SegmentPkSequentialFetcher>(*col_reader,
                                                             *pk_col);
            } else {
              l.pk_lookup.fetcher->Reset(*col_reader, *pk_col);
            }
            l.pk_lookup.last_seg_idx = seg_id;
          }
          l.pk_lookup.seg_pk_vec->ResetFromCache(l.pk_vec_cache);
          l.pk_lookup.fetcher->Fetch(slice, *l.pk_lookup.seg_pk_vec);
          const auto* data = duckdb::FlatVector::GetData<duckdb::string_t>(
            *l.pk_lookup.seg_pk_vec);
          for (size_t k = 0; k < slice.size(); ++k) {
            AppendPrimaryKey(
              pk, std::string_view{data[k].GetData(), data[k].GetSize()});
          }
        }},
      l.pk_batch);

    if (include_cs) {
      auto* mat = GetOrOpenSegmentMaterializer(l, g, *g.reader, seg_id);
      if (mat && mat->HasAny()) {
        mat->SelectByDocIds(slice, output, output_start + begin);
      }
    }
  });
}

template<typename Gstate, typename Lstate, typename View>
duckdb::idx_t MaterializeChunk(duckdb::ClientContext& ctx, Gstate& g, Lstate& l,
                               const View& view, duckdb::DataChunk& output,
                               duckdb::idx_t output_start) {
  const auto num_rows = static_cast<duckdb::idx_t>(view.size());
  if (num_rows == 0) {
    return 0;
  }
  const bool need_cs = !g.cs_projections.empty();
  if (g.has_external_projections) {
    SDB_ASSERT(l.bind_data);
    if (!l.index_source) {
      l.index_source =
        MakeIndexSource(ctx, *l.bind_data, g.external_projected_columns,
                        g.projected_types, l.bind_data->column_ids);
    }
    if (std::holds_alternative<std::monostate>(l.pk_batch)) {
      l.pk_batch = l.index_source->CreatePkBatch();
    }
    if (output_start == 0) {
      std::visit(absl::Overload{[](std::monostate) {},
                                [&](PrimaryKeysBytes& pk) {
                                  pk.Reset();
                                  pk.EnsureInit(duckdb::Allocator::Get(ctx));
                                },
                                [](PrimaryKeyI64& pk) { pk.Reset(); },
                                [](PrimaryKeyI64I64& pk) { pk.Reset(); }},
                 l.pk_batch);
    }
  }
  if (g.has_external_projections || need_cs) {
    ReadIResearchSegments(l, g, view, need_cs, output, output_start);
  }
  if constexpr (requires { l.offsets_entries; }) {
    if (!l.offsets_entries.empty()) {
      WriteChunkOffsets(l, g, *g.reader, view, output, output_start);
    }
  }

  AccountAndWriteVirtualColumns(g, num_rows, view, output, output_start);
  return num_rows;
}

// Returns the number of rows in `output`. Inverted scans are eventually
// consistent: matched rows (as of the last refresh) are never dropped for
// visibility; non-INCLUDE columns are read from the store at the current
// snapshot and are NULL if the row is gone.
template<typename Gstate, typename Lstate>
duckdb::idx_t FinalizeBatch(duckdb::ClientContext& ctx, Gstate& g, Lstate& l,
                            duckdb::DataChunk& output,
                            duckdb::idx_t collected) {
  if (collected == 0 || !g.has_external_projections) {
    return collected;
  }
  SDB_ASSERT(l.index_source);
  SDB_ASSERT(std::visit(
    absl::Overload{[](std::monostate) { return false; },
                   [&](auto& pk) { return PrimaryKeysSize(pk) == collected; }},
    l.pk_batch));
  return l.index_source->Materialize(ctx, l.pk_batch, 0, collected, output);
}

template<typename Gstate, typename Lstate>
bool EmitBufferedScoreDocs(duckdb::ClientContext& ctx, Gstate& g, Lstate& l,
                           std::span<const irs::ScoreDoc> hits,
                           size_t& current_idx, duckdb::DataChunk& output) {
  const size_t remaining = hits.size() - current_idx;
  if (remaining == 0) {
    return false;
  }
  const auto take = std::min<size_t>(remaining, STANDARD_VECTOR_SIZE);
  SDB_IF_FAILURE("SearchLookupFault") {
    if (g.has_external_projections) {
      SDB_THROW(ERROR_DEBUG);
    }
  }
  const ScoreDocsView view{hits.subspan(current_idx, take)};
  const auto emitted = MaterializeChunk(ctx, g, l, view, output, 0);
  const auto visible = FinalizeBatch(ctx, g, l, output, emitted);
  current_idx += take;
  output.SetChildCardinality(visible);
  return emitted > 0;
}

template<typename Gstate, typename Lstate>
void PreparePhase(duckdb::ClientContext& ctx, Gstate& g, Lstate& l) {
  for (;;) {
    const auto seg = g.prepare_segment.fetch_add(1, std::memory_order_relaxed);
    if (seg >= g.total_segments) {
      break;
    }
    SDB_ASSERT((*g.reader)[seg].live_docs_count() != 0);
    l.OnSegmentPrepare(ctx, (*g.reader)[seg], seg, g);
    if (g.prepare_count.fetch_add(1, std::memory_order_acq_rel) + 1 ==
        g.total_segments) {
      const uint32_t used = g.collector_slots.load(std::memory_order_relaxed);
      auto& merged = *g.collectors[0];
      merged.MergeAll([&](irs::PrepareCollector::MergeSink sink) {
        for (uint32_t i = 1; i < used; ++i) {
          sink(*g.collectors[i]);
        }
      });
      g.stats.emplace(merged.Finish(irs::IResourceManager::gNoop));
      g.prepare_finished.Notify();
      return;
    }
  }
  g.prepare_finished.WaitForNotification();
}

template<typename Gstate, typename Lstate>
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

template<typename Gstate, typename Lstate>
void RunStreamingScan(duckdb::ClientContext& ctx, Gstate& g, Lstate& l,
                      duckdb::DataChunk& output) {
  duckdb::idx_t collected = 0;
  while (collected < STANDARD_VECTOR_SIZE) {
    const auto added = l.EmitChunk(ctx, g, output, collected);
    SDB_ASSERT(collected + added <= STANDARD_VECTOR_SIZE);
    collected += added;
    if (added != 0) {
      continue;
    }
    if (collected != 0 && l.EmitsZeroCopyContiguous()) {
      break;
    }
    const auto seg_idx = g.next_segment.fetch_add(1, std::memory_order_relaxed);
    if (seg_idx >= g.total_segments) {
      break;
    }
    const auto& seg = (*g.reader)[seg_idx];
    SDB_ASSERT(seg.live_docs_count() != 0);
    l.StartSegment(ctx, seg, seg_idx, g);
  }
  collected = FinalizeBatch(ctx, g, l, output, collected);
  output.SetChildCardinality(collected);
}

}  // namespace sdb::connector
