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
#include <duckdb/planner/filter/expression_filter.hpp>
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

void InitScanState(IResearchScanGlobalState& state,
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
        const auto& col_index =
          input.column_indexes[state.projected_columns.size() - 1];
        if (col_index.IsPushdownExtract() && col_index.HasChildren()) {
          state.projected_types.push_back(col_index.GetScanType());
        } else {
          state.projected_types.push_back(bind_data.column_types[col_id]);
        }
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

  if (!input.projection_ids.empty()) {
    state.output_projection_ids = input.projection_ids;
  }

  state.pushed_filters = input.filters.get();
  if (input.filters && input.filters->HasFilters()) {
    BuildTableFilter(state, context, bind_data, *input.filters);
  }
}

// Flags whether any pushed filter targets a lookup (source-only) column. Such
// filters are forwarded to the source's native lookup scan (via
// `pushed_filters`) and force the streaming path (the lookup can't run per
// collector candidate).
// `.col` (covered) and virtual/extract filters are declined in
// supports_pushdown_type and re-hosted in a Filter node above the scan, so they
// never reach here.
void BuildTableFilter(IResearchScanGlobalState& state,
                      duckdb::ClientContext& /*context*/,
                      const SereneDBScanBindData& bind_data,
                      const duckdb::TableFilterSet& filters) {
  const catalog::InvertedIndex* index_meta =
    bind_data.IsInvertedIndexEntry() ? bind_data.inverted_index.get() : nullptr;
  // Push a score-column filter (applied on the computed score vector) and, if
  // it is the optional dynamic TOP_N boundary, capture its shared runtime bound
  // so the streaming WAND path can seed its threshold from it.
  const auto push_score_filter = [&](const duckdb::TableFilter& filter) {
    state.col_filters.push_back(
      {.field = 0, .filter = &filter, .is_score = true});
    if (auto dyn =
          duckdb::ExpressionFilter::GetRootOptionalDynamicFilterData(filter)) {
      state.score_dynamic_filter = std::move(dyn);
    }
  };
  for (const auto& entry : filters) {
    const duckdb::idx_t proj_idx = entry.GetIndex();
    // A filter on the computed score column is applied on the score vector by
    // TableFilterDocIterator (no columnstore field). Detect it either as the
    // projected score slot or -- when the score is filter-only -- by its
    // resolved column id.
    if (state.scan_score && proj_idx == state.score_output_idx) {
      push_score_filter(entry.Filter());
      continue;
    }
    const auto bind_index = state.projected_columns[proj_idx];
    if (bind_index == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    const auto col_id = bind_data.column_ids[bind_index];
    if (col_id == catalog::Column::kInvertedIndexScoreId) {
      push_score_filter(entry.Filter());
      continue;
    }
    const auto* info =
      index_meta ? index_meta->FindColumnInfo(col_id) : nullptr;
    const bool index_stored = !index_meta || (info && info->IsStored());
    if (!index_stored) {
      state.has_lookup_filter = true;
    } else if (index_meta != nullptr || bind_data.IsSearchTableEntry()) {
      // Covered columnstore column filtered in-scan (codec Filter + zonemap),
      // keyed by the columnstore field id: an INCLUDE'd inverted-index column,
      // or -- for a search table, where every column lives in `.col` -- any
      // column.
      state.col_filters.push_back({
        .field = static_cast<irs::field_id>(col_id.id()),
        .filter = &entry.Filter(),
      });
    }
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

void ClassifyColumnstoreProjections(IResearchScanGlobalState& state,
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

FullScanner* GetOrOpenSegmentFullScanner(IResearchScanLocalState& lstate,
                                         const IResearchScanGlobalState& gstate,
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
    std::vector<TableFilterDocIterator::FilterSpec> specs;
    specs.reserve(gstate.col_filters.size());
    for (const auto& cf : gstate.col_filters) {
      specs.push_back(
        {.field = cf.field, .filter = cf.filter, .is_score = cf.is_score});
    }
    slot = std::make_unique<FullScanner>(*col_reader, gstate.cs_projections,
                                         specs, gstate.client_context);
  }
  return slot.get();
}

void IResearchScanGetMetrics(duckdb::TableFunctionGetMetricsInput& input) {
  auto& gstate = input.global_state->Cast<IResearchScanGlobalState>();
  input.operator_metrics.rows_scanned =
    gstate.produced_rows.load(std::memory_order_relaxed);
}

void ApplyScoreEmit(const IResearchScanGlobalState& gstate, float* scores,
                    duckdb::idx_t n) {
  if (!gstate.vector_scorer) {
    // Text scores are already the user-facing value (Identity).
    return;
  }
  // The raw score is "larger = nearer" (ResolveScoringDistance negates distance
  // kernels); map it in place to the user value at the emit boundary, so the
  // score-column filter, the output vector (and any WAND threshold) all see the
  // one user-facing value.
  switch (gstate.vector_scorer->score_emit) {
    case ScoreEmit::Identity:
      break;
    case ScoreEmit::SqrtNeg:
      for (duckdb::idx_t i = 0; i < n; ++i) {
        scores[i] = std::sqrt(-scores[i]);
      }
      break;
    case ScoreEmit::OneMinus:
      for (duckdb::idx_t i = 0; i < n; ++i) {
        scores[i] = 1.0f - scores[i];
      }
      break;
    case ScoreEmit::Negate:
      for (duckdb::idx_t i = 0; i < n; ++i) {
        scores[i] = -scores[i];
      }
      break;
  }
}

void AccountAndWriteVirtualColumns(IResearchScanGlobalState& gstate,
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
  // Scores arrive already mapped to the user-facing value (ApplyScoreEmit runs
  // at the emit boundary), so the write is a straight copy.
  auto* score_data = duckdb::FlatVector::GetDataMutable<float>(
    output.data[gstate.score_output_idx]);
  std::memcpy(score_data, scores.data(), num_rows * sizeof(float));
}

}  // namespace sdb::connector
