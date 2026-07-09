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

#include "connector/duckdb_scan_base.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <duckdb.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/filter/table_filter_functions.hpp>
#include <duckdb/planner/table_filter_set.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/directory_reader_impl.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/store/directory.hpp>
#include <limits>
#include <numeric>
#include <ranges>
#include <string_view>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/duckdb_engine.h"
#include "basics/string_utils.h"
#include "catalog/inverted_index.h"
#include "catalog/table_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_entry.h"
#include "connector/duckdb_table_function.h"
#include "connector/full_scanner.h"
#include "iresearch/index/column_extract.hpp"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {

void InitCommonState(CommonScanGlobalState& state,
                     duckdb::ClientContext& context,
                     const SereneDBScanBindData& bind_data,
                     duckdb::TableFunctionInitInput& input) {
  // Determine which columns DuckDB actually wants (projection pushdown).
  const auto num_bind_columns = bind_data.column_ids.size();
  for (auto col_id : input.column_ids) {
    if (col_id == kColumnIdentifierGeneratedPk) {
      if (!bind_data.IsSearchTableEntry()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG("projecting the rowid through an inverted-index scan is not "
                  "supported"));
      }
      // Search table: the rowid IS the generated PK, materialized from `.col`.
      state.generated_pk_output_idx = state.projected_columns.size();
      state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
      state.projected_types.push_back(duckdb::LogicalType::ROW_TYPE);
    } else if (col_id == kColumnIdentifierTableOid) {
      state.scan_tableoid = true;
      state.tableoid_output_idx = state.projected_columns.size();
      state.tableoid_value = bind_data.RelationId().id();
      state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
      state.projected_types.push_back(duckdb::LogicalType::BIGINT);
    } else if (col_id == duckdb::COLUMN_IDENTIFIER_EMPTY) {
      state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
      state.projected_types.push_back(duckdb::LogicalType::BOOLEAN);
    } else if (col_id >= duckdb::VIRTUAL_COLUMN_START) {
      SDB_ASSERT(!bind_data.IsViewBacked(),
                 "virtual PK columns are not used for view-backed scans");
      auto cat_idx = SereneDBTableEntry::VirtualToPKColumnIndex(col_id);
      SDB_ASSERT(cat_idx != duckdb::DConstants::INVALID_INDEX);
      const auto& tbd = bind_data.As<TableScanBindData>();
      const auto& catalog_cols = tbd.table->Columns();
      SDB_ASSERT(cat_idx < catalog_cols.size());
      const auto catalog_col_id = catalog_cols[cat_idx].GetId();
      duckdb::idx_t bind_idx = duckdb::DConstants::INVALID_INDEX;
      for (duckdb::idx_t i = 0; i < bind_data.column_ids.size(); ++i) {
        if (bind_data.column_ids[i] == catalog_col_id) {
          bind_idx = i;
          break;
        }
      }
      SDB_ASSERT(bind_idx != duckdb::DConstants::INVALID_INDEX);
      state.projected_columns.push_back(bind_idx);
      state.projected_types.push_back(bind_data.column_types[bind_idx]);
    } else if (col_id < num_bind_columns) {
      const auto catalog_col_id = bind_data.column_ids[col_id];
      if (catalog_col_id == catalog::Column::kInvertedIndexScoreId) {
        state.scan_score = true;
        state.score_output_idx = state.projected_columns.size();
        state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        state.projected_types.push_back(duckdb::LogicalType::FLOAT);
      } else if (catalog_col_id == catalog::Column::kInvertedIndexOffsetsId) {
        state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        state.projected_types.push_back(catalog::Column::MakeOffsetsType());
      } else if (catalog_col_id == catalog::Column::kInvertedIndexTermId) {
        state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        state.projected_types.push_back(duckdb::LogicalType::VARCHAR);
      } else if (catalog_col_id == catalog::Column::kInvertedIndexTermRawId) {
        state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        state.projected_types.push_back(duckdb::LogicalType::BLOB);
      } else if (catalog_col_id == catalog::Column::kInvertedIndexTermCountId) {
        state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        state.projected_types.push_back(duckdb::LogicalType::INTEGER);
      } else if (catalog_col_id == catalog::Column::kInvertedIndexTermFreqId) {
        state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        state.projected_types.push_back(duckdb::LogicalType::BIGINT);
      } else if (catalog_col_id == catalog::Column::kInvertedIndexTermScoreId) {
        state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        state.projected_types.push_back(duckdb::LogicalType::FLOAT);
      } else {
        state.projected_columns.push_back(col_id);
        state.projected_types.push_back(bind_data.column_types[col_id]);
      }
    }
  }

  state.external_projected_columns = state.projected_columns;
  state.has_real_column = absl::c_any_of(state.projected_columns, [](auto p) {
    return p != duckdb::DConstants::INVALID_INDEX;
  });

  state.client_context = &context;
  state.projected_column_indexes = input.column_indexes;
  SDB_ASSERT(state.projected_column_indexes.size() ==
             state.projected_columns.size());

  if (input.CanRemoveFilterColumns()) {
    state.output_projection_ids = input.projection_ids;
  }

  if (input.filters && input.filters->HasFilters()) {
    BuildTableFilter(state, context, bind_data, *input.filters);
  }
}

// Splits the pushed filters three ways: index-stored columns -> stored
// entries (zonemaps + columnstore row eval), other real columns -> row-fetch
// entries (evaluated on values fetched from the store), virtual/extract
// output slots -> the row expression, enforceable only on the materialized
// chunk. Optional filters never join the row expression and are dropped
// entirely when their column has no zonemaps.
void BuildTableFilter(CommonScanGlobalState& state,
                      duckdb::ClientContext& context,
                      const SereneDBScanBindData& bind_data,
                      const duckdb::TableFilterSet& filters) {
  const catalog::InvertedIndex* index_meta =
    bind_data.IsInvertedIndexEntry() ? bind_data.inverted_index.get() : nullptr;
  // ANN scans carry no scan-side enforcement: every non-optional filter
  // lands in row_expr, which demotes ANN top-k to the range shape whose
  // driver enforces it on the materialized chunk.
  const bool ann_scan =
    bind_data.scan_source->Cast<SearchScan>().vector_scorer.has_value();
  for (const auto& entry : filters) {
    const duckdb::idx_t idx = entry.GetIndex();
    SDB_ASSERT(idx < state.projected_columns.size());
    const bool is_optional =
      duckdb::ExpressionFilter::IsOptionalFilter(entry.Filter());
    const auto bind_index = state.projected_columns[idx];
    const bool is_extract =
      idx < state.projected_column_indexes.size() &&
      state.projected_column_indexes[idx].IsPushdownExtract() &&
      state.projected_column_indexes[idx].HasChildren();

    const auto slot_type = state.projected_types[idx];
    const auto& expr_filter = duckdb::ExpressionFilter::GetExpressionFilter(
      entry.Filter(), "BuildTableFilter");

    if (bind_index == duckdb::DConstants::INVALID_INDEX || is_extract ||
        ann_scan) {
      if (is_optional) {
        continue;
      }
      auto slot_ref =
        duckdb::make_uniq<duckdb::BoundReferenceExpression>(slot_type, idx);
      auto expr = entry.Filter().ToExpression(*slot_ref);
      if (!state.row_expr) {
        state.row_expr = std::move(expr);
      } else {
        state.row_expr = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
          duckdb::ExpressionType::CONJUNCTION_AND, std::move(state.row_expr),
          std::move(expr));
      }
      continue;
    }

    const auto col_id = bind_data.column_ids[bind_index];
    const auto* info =
      index_meta ? index_meta->FindColumnInfo(col_id) : nullptr;
    const bool index_stored = !index_meta || (info && info->IsStored());

    if (index_stored) {
      const bool is_dynamic =
        duckdb::ExpressionFilter::ContainsInternalFunction(
          *expr_filter.expr, duckdb::DynamicFilterScalarFun::NAME);
      const bool passes_null =
        expr_filter.EvaluateWithConstant(context, duckdb::Value(slot_type));
      state.table_filter.AddColumn(col_id.id(), expr_filter, is_optional,
                                   is_dynamic, passes_null);
    } else if (!is_optional) {
      // Lookup-column filter: the emit's single Materialize pass fetches the
      // filter and output columns together, so evaluate on the materialized
      // chunk (row_expr) -- no separate row-fetch pass, and (since row_expr
      // demotes top-k to streaming) no per-candidate over-fetch in a collector.
      auto slot_ref =
        duckdb::make_uniq<duckdb::BoundReferenceExpression>(slot_type, idx);
      auto expr = entry.Filter().ToExpression(*slot_ref);
      if (!state.row_expr) {
        state.row_expr = std::move(expr);
      } else {
        state.row_expr = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
          duckdb::ExpressionType::CONJUNCTION_AND, std::move(state.row_expr),
          std::move(expr));
      }
    }
  }
  state.table_filter.SetPkColumn(catalog::Column::kGeneratedPKId.id());
  state.table_filter.Seal();
}

void InitLocalFilter(CommonScanLocalState& lstate,
                     const CommonScanGlobalState& gstate,
                     duckdb::ClientContext& context,
                     const SereneDBScanBindData& bind_data) {
  lstate.filter.Init(gstate.table_filter);
  if (gstate.table_filter.HasRowFetch()) {
    // Push the row-fetch filters into the lookup source: pushdown-capable
    // readers (parquet) prune reads; others ignore it. Excluded rows come
    // back NULL and ScanFilter re-checks, so results are unchanged.
    lstate.filter.Rows().SetSource(
      MakeIndexSource(context, bind_data, gstate.rf_columns, gstate.rf_types,
                      bind_data.column_ids,
                      gstate.table_filter.RowFetchFilterSet()),
      gstate.rf_types);
  }
}

void DecodeExtractPath(const duckdb::ColumnIndex& column_index,
                       const duckdb::LogicalType& root_type,
                       std::vector<std::string_view>& out) {
  // col->field1 -- exactly one child assumed.
  SDB_ASSERT(column_index.ChildIndexCount() == 1);
  const duckdb::ColumnIndex* node = &column_index.GetChildIndex(0);
  const duckdb::LogicalType* cur_type = &root_type;
  while (true) {
    if (node->HasPrimaryIndex()) {
      SDB_ASSERT(cur_type->id() == duckdb::LogicalTypeId::STRUCT,
                 "Numeric identifiers are only in structs");
      const auto node_index = node->GetPrimaryIndex();
      const auto& children = duckdb::StructType::GetChildTypes(*cur_type);
      SDB_ASSERT(node_index < children.size(), "Invalid index node");
      out.emplace_back(children[node_index].first.GetIdentifierName());
      cur_type = &children[node_index].second;
    } else {
      out.emplace_back(node->GetFieldName());
    }
    if (!node->HasChildren()) {
      break;
    }
    // col->field1->field2->... etc. Each indirection has exactly one child.
    SDB_ASSERT(node->ChildIndexCount() == 1);
    node = &node->GetChildIndex(0);
  }
}

void ClassifyColumnstoreProjections(CommonScanGlobalState& state,
                                    const SereneDBScanBindData& bind_data) {
  if (bind_data.IsSearchTableEntry()) {
    // Search table: every column lives in `.col`, so all projections are
    // covered and there is no lookup source.
    std::vector<std::string_view> path;
    for (duckdb::idx_t proj = 0; proj < state.projected_columns.size();
         ++proj) {
      const auto bind_col = state.projected_columns[proj];
      if (bind_col == duckdb::DConstants::INVALID_INDEX) {
        continue;
      }
      const auto col_id = bind_data.column_ids[bind_col];
      ColumnstoreProjection cp{.output_slot = proj, .column_id = col_id.id()};
      if (proj < state.projected_column_indexes.size()) {
        const auto& column_index = state.projected_column_indexes[proj];
        if (column_index.IsPushdownExtract() && column_index.HasChildren()) {
          path.clear();
          DecodeExtractPath(column_index, bind_data.column_types[bind_col],
                            path);
          if (!path.empty()) {
            cp.extract_path = std::move(path);
            cp.extract_scan_type = column_index.GetScanType();
          }
        }
      }
      state.cs_projections.emplace_back(std::move(cp));
      state.external_projected_columns[proj] =
        duckdb::DConstants::INVALID_INDEX;
    }
    if (state.generated_pk_output_idx != duckdb::DConstants::INVALID_INDEX) {
      state.cs_projections.emplace_back(ColumnstoreProjection{
        .output_slot = state.generated_pk_output_idx,
        .column_id = catalog::Column::kGeneratedPKId.id()});
    }
    state.has_external_projections = false;
    return;
  }
  if (!bind_data.IsInvertedIndexEntry() || !bind_data.inverted_index) {
    state.has_external_projections = state.has_real_column;
    return;
  }

  std::vector<std::string_view> path;
  for (duckdb::idx_t proj = 0; proj < state.projected_columns.size(); ++proj) {
    const auto bind_col = state.projected_columns[proj];
    if (bind_col == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    const auto col_id = bind_data.column_ids[bind_col];
    const auto* info = bind_data.inverted_index->FindColumnInfo(col_id);
    if (info && info->IsStored()) {
      ColumnstoreProjection cp{.output_slot = proj, .column_id = col_id.id()};
      if (info && info->store_values &&
          proj < state.projected_column_indexes.size()) {
        const auto& column_index = state.projected_column_indexes[proj];
        if (column_index.IsPushdownExtract() && column_index.HasChildren()) {
          path.clear();
          DecodeExtractPath(column_index, bind_data.column_types[bind_col],
                            path);
          if (!path.empty()) {
            cp.extract_path = std::move(path);
            cp.extract_scan_type = column_index.GetScanType();
          }
        }
      }
      state.cs_projections.emplace_back(std::move(cp));
      state.external_projected_columns[proj] =
        duckdb::DConstants::INVALID_INDEX;
      continue;
    }
    state.has_external_projections = true;
  }
  if (state.has_external_projections) {
    const auto pk_kind = bind_data.inverted_index->GetOptions().pk_column;
    if (pk_kind == catalog::PkColumnKind::None) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("inverted index \"", bind_data.inverted_index->GetName(),
                "\" was created WITH (store_pk = 'none'), so it does not store "
                "row PKs and hits cannot be mapped back to source rows; select "
                "only INCLUDE'd columns, counts or scores through this index"));
    }
    if (pk_kind == catalog::PkColumnKind::Unable) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("materialising real columns from this view-backed inverted "
                "index is not yet supported -- view body must be a simple "
                "`SELECT * FROM <reader>(literal_args)` over a recognised "
                "fast-path source (read_parquet/csv/json/...)"));
    }
  }
}

FullScanner* GetOrOpenSegmentFullScanner(CommonScanLocalState& lstate,
                                         const CommonScanGlobalState& gstate,
                                         const irs::IndexReader& reader,
                                         size_t seg_idx) {
  if (gstate.cs_projections.empty() || seg_idx >= reader.size()) {
    return nullptr;
  }
  if (lstate.full_scanners.size() < reader.size()) {
    lstate.full_scanners.resize(reader.size());
  }
  auto& slot = lstate.full_scanners[seg_idx];
  if (!slot) {
    const auto* col_reader = reader[seg_idx].GetColReader();
    if (!col_reader) {
      return nullptr;
    }
    slot = std::make_unique<FullScanner>(*col_reader, gstate.cs_projections,
                                         gstate.client_context);
  }
  return slot.get();
}

void CommonScanGetMetrics(duckdb::TableFunctionGetMetricsInput& input) {
  auto& gstate = input.global_state->Cast<CommonScanGlobalState>();
  input.operator_metrics.rows_scanned =
    gstate.produced_rows.load(std::memory_order_relaxed);
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> CommonScanInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* global_state) {
  auto lstate = duckdb::make_uniq<CommonScanLocalState>();
  InitLocalFilter(*lstate, global_state->Cast<CommonScanGlobalState>(),
                  context.client,
                  input.bind_data->Cast<SereneDBScanBindData>());
  return lstate;
}

void AccountAndWriteVirtualColumns(CommonScanGlobalState& gstate,
                                   duckdb::idx_t num_rows,
                                   std::span<const float> scores,
                                   duckdb::DataChunk& output) {
  gstate.produced_rows.fetch_add(num_rows, std::memory_order_relaxed);
  if (gstate.scan_tableoid) {
    auto* tableoid_data = duckdb::FlatVector::GetDataMutable<int64_t>(
      output.data[gstate.tableoid_output_idx]);
    for (duckdb::idx_t i = 0; i < num_rows; ++i) {
      tableoid_data[i] = gstate.tableoid_value;
    }
  }
  if (!gstate.scan_score) {
    return;
  }
  SDB_ASSERT(scores.size() == num_rows);
  auto* score_data = duckdb::FlatVector::GetDataMutable<float>(
    output.data[gstate.score_output_idx]);
  if (gstate.vector_scorer) {
    // TODO(codeworse): benchmark not-optimized distances, and maybe remove this
    switch (gstate.vector_scorer->score_emit) {
      case ScoreEmit::Identity:
        std::memcpy(score_data, scores.data(), num_rows * sizeof(float));
        break;
      case ScoreEmit::Sqrt:
        for (duckdb::idx_t i = 0; i < num_rows; ++i) {
          score_data[i] = std::sqrt(scores[i]);
        }
        break;
      case ScoreEmit::OneMinus:
        for (duckdb::idx_t i = 0; i < num_rows; ++i) {
          score_data[i] = 1.0f - scores[i];
        }
        break;
      case ScoreEmit::Negate:
        for (duckdb::idx_t i = 0; i < num_rows; ++i) {
          score_data[i] = -scores[i];
        }
        break;
    }
  } else {
    std::memcpy(score_data, scores.data(), num_rows * sizeof(float));
  }
}

duckdb::idx_t EmitReadyBatch(duckdb::ClientContext& ctx,
                             CommonScanGlobalState& g,
                             SegDocBufferedScanLocalState& l,
                             duckdb::DataChunk& output) {
  if (!g.cs_projections.empty()) {
    SDB_IF_FAILURE("SearchIncludeFetchFault") { SDB_THROW(ERROR_DEBUG); }
  }
  if (g.has_external_projections) {
    SDB_ASSERT(l.bind_data);
    if (!l.index_source) {
      l.index_source =
        MakeIndexSource(ctx, *l.bind_data, g.external_projected_columns,
                        g.projected_types, l.bind_data->column_ids);
    }
    if (l.pk_batch.kind == PrimaryKeyBatch::Kind::None) {
      l.pk_batch.kind = l.index_source->PkKind();
    }
    l.pk_batch.Reset();
  }
  const auto batch = l.hit_batcher->Emit(output);
  if (batch.pk != nullptr) {
    SDB_IF_FAILURE("SearchPkFetchFault") { SDB_THROW(ERROR_DEBUG); }
    batch.pk->Flatten(batch.count);
    AppendPrimaryKeysFromVector(l.pk_batch, *batch.pk, batch.count);
  }
  const HitsChunk view{
    .docs = batch.docs,
    .scores = batch.scores,
    .fixed_seg = batch.seg,
  };
  l.EmitRowOffsets(g, view, output);
  AccountAndWriteVirtualColumns(g, batch.count, batch.scores, output);
  return batch.count;
}

void FinalizeBatch(duckdb::ClientContext& ctx, CommonScanGlobalState& g,
                   SegDocBufferedScanLocalState& l, duckdb::DataChunk& output,
                   duckdb::idx_t collected) {
  if (collected == 0 || !g.has_external_projections) {
    return;
  }
  SDB_ASSERT(l.index_source);
  SDB_ASSERT(l.pk_batch.Size() == collected);
  l.index_source->Materialize(ctx, l.pk_batch, 0, collected, output);
}

bool EmitBufferedScoreDocs(duckdb::ClientContext& ctx, CommonScanGlobalState& g,
                           SegDocBufferedScanLocalState& l,
                           std::span<const irs::ScoreDoc> hits,
                           size_t& current_idx, duckdb::DataChunk& output) {
  if (!l.hit_batcher) {
    l.hit_batcher = std::make_unique<HitBatcher>(
      g.cs_projections,
      g.has_external_projections ? catalog::term_dict::kPKFieldId
                                 : irs::field_limits::invalid(),
      g.scan_score);
    l.hit_batcher->BeginSegment(std::numeric_limits<uint32_t>::max(), nullptr,
                                g.client_context);
  }
  SDB_IF_FAILURE("SearchLookupFault") {
    if (g.has_external_projections) {
      SDB_THROW(ERROR_DEBUG);
    }
  }
  auto& batcher = *l.hit_batcher;
  // Emit one materialized batch at a time; a batch the row expression rejects
  // in full (filters over virtual/extract slots -- ANN routes all its filters
  // here) loops for the next one instead of surfacing an empty chunk.
  for (;;) {
    while (!batcher.Ready()) {
      if (current_idx == hits.size()) {
        if (batcher.Empty()) {
          return false;
        }
        batcher.Finalize();
        if (!batcher.Ready()) {
          return false;
        }
        break;
      }
      const auto& hit = hits[current_idx];
      // Hits are sorted by (segment, doc); a segment change drains the batch.
      if (hit.segment_idx != batcher.Segment()) {
        if (!batcher.Empty()) {
          batcher.Finalize();
          continue;
        }
        batcher.BeginSegment(hit.segment_idx,
                             (*g.reader)[hit.segment_idx].GetColReader(),
                             g.client_context);
      }
      batcher.Push(hit.doc);
      if (g.scan_score) {
        batcher.ScoreSlots(1)[0] = hit.score;
      }
      ++current_idx;
    }
    const auto emitted = EmitReadyBatch(ctx, g, l, output);
    FinalizeBatch(ctx, g, l, output, emitted);
    const auto survivors = ApplyRowFilter(ctx, g, l, output, emitted);
    if (survivors != 0) {
      output.SetChildCardinality(survivors);
      return true;
    }
    output.Reset();
  }
}

void RunCollectThenEmitScan(duckdb::ClientContext& ctx,
                            CommonScanGlobalState& g, CommonScanLocalState& l,
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

}  // namespace sdb::connector
