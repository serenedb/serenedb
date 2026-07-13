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
#include "connector/column_extract.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_entry.h"
#include "connector/duckdb_table_function.h"
#include "connector/full_scanner.h"
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
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("projecting the rowid through an inverted-index scan is not "
                "supported"));
    } else if (col_id == kColumnIdentifierTableOid) {
      state.scan_tableoid = true;
      state.tableoid_output_idx = state.projected_columns.size();
      state.tableoid_value = static_cast<int64_t>(bind_data.RelationId().id());
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
      ColumnstoreProjection cp{.output_slot = proj, .column_id = col_id};
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
  duckdb::ExecutionContext& /*context*/,
  duckdb::TableFunctionInitInput& /*input*/,
  duckdb::GlobalTableFunctionState* /*global_state*/) {
  return duckdb::make_uniq<CommonScanLocalState>();
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
    SDB_IF_FAILURE("SearchIncludeFetchFault") {
      THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
    }
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
    SDB_IF_FAILURE("SearchPkFetchFault") {
      THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
    }
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
      g.cs_projections, g.has_external_projections, g.scan_score);
    l.hit_batcher->BeginSegment(std::numeric_limits<uint32_t>::max(), nullptr,
                                g.client_context);
  }
  SDB_IF_FAILURE("SearchLookupFault") {
    if (g.has_external_projections) {
      THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
    }
  }
  auto& batcher = *l.hit_batcher;
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
  output.SetChildCardinality(emitted);
  return emitted > 0;
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
